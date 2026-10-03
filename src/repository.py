from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .audit import make_entry, utc_now
from .domain import ConflictError, NotFoundError
from .rules import ALL_STATUSES, ID_PREFIX, STATES, UNEXECUTED_STATES

DEFAULT_OBSERVATION_NOTE = "系统初始化默认水情观测"


class Repository:
    def __init__(self, db_path: str):
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA journal_mode = WAL")
        self._create_schema()

    # ------------------------------------------------------------------ schema
    def _create_schema(self) -> None:
        statuses = ",".join("'" + s.replace("'", "''") + "'" for s in ALL_STATUSES)
        with self._lock, self.conn:
            self.conn.executescript(f"""
                CREATE TABLE IF NOT EXISTS items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    description TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    quantity REAL NOT NULL DEFAULT 0,
                    threshold REAL NOT NULL DEFAULT 1,
                    status TEXT NOT NULL CHECK(status IN ({statuses})),
                    version INTEGER NOT NULL DEFAULT 1,
                    external_ref TEXT,
                    basis_obs_version INTEGER,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS ux_items_external_ref
                    ON items(external_ref) WHERE external_ref IS NOT NULL;
                CREATE TABLE IF NOT EXISTS records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
                    kind TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'open'
                        CHECK(status IN ('open','closed')),
                    external_ref TEXT,
                    basis_obs_version INTEGER,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(item_id, external_ref)
                );
                CREATE TABLE IF NOT EXISTS observations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version INTEGER NOT NULL UNIQUE,
                    reservoir_level REAL NOT NULL,
                    inflow REAL NOT NULL,
                    note TEXT,
                    recorded_by TEXT NOT NULL,
                    recorded_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS idempotency_keys (
                    request_no TEXT NOT NULL,
                    action TEXT NOT NULL,
                    entity_type TEXT NOT NULL,
                    entity_id INTEGER NOT NULL,
                    response TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (request_no, action)
                );
                CREATE TABLE IF NOT EXISTS audit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    action TEXT NOT NULL,
                    entity_type TEXT NOT NULL,
                    entity_id INTEGER NOT NULL,
                    actor TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    previous_hash TEXT NOT NULL,
                    entry_hash TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL
                );
            """)
        self._migrate_legacy()

    def _columns(self, table: str) -> List[str]:
        with self._lock:
            rows = self.conn.execute(f"PRAGMA table_info({table})").fetchall()
        return [r["name"] for r in rows]

    def _migrate_legacy(self) -> None:
        """补齐旧库缺失的依据列，并把缺依据的未执行指令升级为待补核。"""
        item_cols = self._columns("items")
        record_cols = self._columns("records")
        if "basis_obs_version" not in item_cols:
            self._rebuild_items_table()
        if "basis_obs_version" not in record_cols:
            with self._lock, self.conn:
                self.conn.execute("ALTER TABLE records ADD COLUMN basis_obs_version INTEGER")
        # 历史数据中缺依据的未执行指令 -> 待补核(supplement)
        with self._lock, self.conn:
            self.conn.execute(
                """UPDATE items SET status='supplement'
                   WHERE basis_obs_version IS NULL
                     AND status IN ('draft','checked','authorized')"""
            )

    def _rebuild_items_table(self) -> None:
        statuses = ",".join("'" + s.replace("'", "''") + "'" for s in ALL_STATUSES)
        old_level = self.conn.isolation_level
        self.conn.isolation_level = None
        try:
            self.conn.execute("PRAGMA foreign_keys = OFF")
            self.conn.execute(f"""
                CREATE TABLE items_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    description TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    quantity REAL NOT NULL DEFAULT 0,
                    threshold REAL NOT NULL DEFAULT 1,
                    status TEXT NOT NULL CHECK(status IN ({statuses})),
                    version INTEGER NOT NULL DEFAULT 1,
                    external_ref TEXT,
                    basis_obs_version INTEGER,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)
            self.conn.execute("""
                INSERT INTO items_new
                    (id, title, description, severity, quantity, threshold,
                     status, version, external_ref, created_by, created_at, updated_at)
                SELECT id, title, description, severity, quantity, threshold,
                       status, version, external_ref, created_by, created_at, updated_at
                FROM items
            """)
            self.conn.execute("DROP TABLE items")
            self.conn.execute("ALTER TABLE items_new RENAME TO items")
            self.conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS ux_items_external_ref
                    ON items(external_ref) WHERE external_ref IS NOT NULL
            """)
            self.conn.execute("PRAGMA foreign_keys = ON")
        finally:
            self.conn.isolation_level = old_level

    # ------------------------------------------------------------- helpers
    @staticmethod
    def _item(row: sqlite3.Row) -> Dict[str, Any]:
        return dict(row)

    def _get_idempotency(self, request_no: str, action: str) -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            "SELECT * FROM idempotency_keys WHERE request_no=? AND action=?",
            (request_no, action),
        ).fetchone()
        return dict(row) if row else None

    def _store_idempotency(self, request_no: str, action: str, entity_type: str,
                           entity_id: int, response: Dict[str, Any]) -> None:
        self.conn.execute(
            """INSERT INTO idempotency_keys(request_no, action, entity_type, entity_id,
               response, created_at) VALUES(?,?,?,?,?,?)""",
            (request_no, action, entity_type, entity_id,
             json.dumps(response, ensure_ascii=False), utc_now()),
        )

    def _replay(self, request_no: Optional[str], action: str) -> Optional[Dict[str, Any]]:
        if not request_no:
            return None
        existing = self._get_idempotency(request_no, action)
        if existing is None:
            return None
        return json.loads(existing["response"])

    def get_idempotency_response(self, request_no: Optional[str],
                                 action: str) -> Optional[Dict[str, Any]]:
        if not request_no:
            return None
        with self._lock:
            existing = self._get_idempotency(request_no, action)
        if existing is None:
            return None
        return json.loads(existing["response"])

    def _append_audit(self, action: str, entity_type: str, entity_id: int,
                      actor: str, detail: dict) -> Dict[str, Any]:
        """在当前事务内追加审计事件（不单独开事务）。"""
        row = self.conn.execute(
            "SELECT entry_hash FROM audit_events ORDER BY id DESC LIMIT 1"
        ).fetchone()
        previous = row["entry_hash"] if row else "GENESIS"
        event = make_entry(action, entity_type, entity_id, actor, detail, previous)
        cur = self.conn.execute(
            """INSERT INTO audit_events(action, entity_type, entity_id, actor, detail,
               previous_hash, entry_hash, created_at) VALUES(?,?,?,?,?,?,?,?)""",
            (event["action"], event["entity_type"], event["entity_id"], event["actor"],
             json.dumps(event["detail"], ensure_ascii=False, sort_keys=True),
             event["previous_hash"], event["entry_hash"], event["created_at"]),
        )
        event["id"] = int(cur.lastrowid)
        return event

    # ------------------------------------------------------- observations
    def current_observation(self) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM observations ORDER BY version DESC LIMIT 1"
            ).fetchone()
        return dict(row) if row else None

    def ensure_default_observation(self, actor: str) -> Dict[str, Any]:
        existing = self.current_observation()
        if existing is not None:
            return existing
        obs, _ = self.create_observation(0.0, 0.0, DEFAULT_OBSERVATION_NOTE, actor)
        return obs

    def list_observations(self) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM observations ORDER BY version DESC"
            ).fetchall()
        return [dict(row) for row in rows]

    def create_observation(self, reservoir_level: float, inflow: float,
                           note: Optional[str], actor: str
                           ) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """新增一个水情观测版本，并让所有未执行指令的旧复核失效、退回待复核。"""
        now = utc_now()
        with self._lock, self.conn:
            row = self.conn.execute(
                "SELECT COALESCE(MAX(version),0)+1 AS v FROM observations"
            ).fetchone()
            version = int(row["v"])
            cur = self.conn.execute(
                """INSERT INTO observations(version, reservoir_level, inflow, note,
                   recorded_by, recorded_at) VALUES(?,?,?,?,?,?)""",
                (version, reservoir_level, inflow, note, actor, now),
            )
            obs_id = int(cur.lastrowid)
            obs = dict(self.conn.execute(
                "SELECT * FROM observations WHERE id=?", (obs_id,)
            ).fetchone())

            stale = self.conn.execute(
                """SELECT * FROM items
                   WHERE status IN ('draft','checked','authorized')
                     AND (basis_obs_version IS NULL OR basis_obs_version < ?)""",
                (version,),
            ).fetchall()
            affected: List[Dict[str, Any]] = []
            for r in stale:
                item = dict(r)
                old_basis = item["basis_obs_version"]
                reverted = item["status"] in ("checked", "authorized")
                self.conn.execute(
                    "UPDATE items SET status='draft', basis_obs_version=?, updated_at=? WHERE id=?",
                    (version, now, item["id"]),
                )
                self._append_audit("observation_superseded", "items", item["id"], actor, {
                    "observation_id": obs_id,
                    "old_basis": old_basis,
                    "new_basis": version,
                    "old_status": item["status"],
                    "reverted": reverted,
                })
                affected.append({
                    "id": item["id"], "old_status": item["status"],
                    "old_basis": old_basis, "reverted": reverted,
                })

            self._append_audit("observation", "observation", obs_id, actor, {
                "version": version,
                "reservoir_level": reservoir_level,
                "inflow": inflow,
                "note": note,
                "affected_items": len(affected),
            })
        return obs, affected

    # -------------------------------------------------------------- items
    def create_item(self, title: str, description: str, severity: str,
                    quantity: float, threshold: float, external_ref: Optional[str],
                    basis_obs_version: int, request_no: Optional[str], actor: str
                    ) -> Tuple[Dict[str, Any], bool]:
        now = utc_now()
        with self._lock, self.conn:
            replayed = self._replay(request_no, "create_item")
            if replayed is not None:
                return replayed, True
            try:
                cur = self.conn.execute(
                    """INSERT INTO items(title, description, severity, quantity, threshold,
                       status, version, external_ref, basis_obs_version,
                       created_by, created_at, updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (title, description, severity, quantity, threshold, STATES[0], 1,
                     external_ref, basis_obs_version, actor, now, now),
                )
                item_id = int(cur.lastrowid)
            except sqlite3.IntegrityError as exc:
                replayed = self._replay(request_no, "create_item")
                if replayed is not None:
                    return replayed, True
                raise ConflictError("external_ref已存在") from exc
            item = self._item(self.conn.execute(
                "SELECT * FROM items WHERE id=?", (item_id,)
            ).fetchone())
            self._append_audit("create", "items", item_id, actor, {
                "title": title, "severity": severity, "quantity": quantity,
                "threshold": threshold, "basis_obs_version": basis_obs_version,
            })
            if request_no:
                self._store_idempotency(request_no, "create_item", "items", item_id, item)
        return item, False

    def get_item(self, item_id: int) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
        if row is None:
            raise NotFoundError("项目不存在")
        return self._item(row)

    def list_items(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM items"
        params: tuple = ()
        if status:
            sql += " WHERE status=?"
            params = (status,)
        sql += " ORDER BY id DESC"
        with self._lock:
            rows = self.conn.execute(sql, params).fetchall()
        return [self._item(row) for row in rows]

    def transition_item(self, item_id: int, target: str, expected_version: int,
                        actor: str, basis_obs_version: int,
                        request_no: Optional[str]
                        ) -> Tuple[Dict[str, Any], bool]:
        now = utc_now()
        with self._lock, self.conn:
            replayed = self._replay(request_no, "transition")
            if replayed is not None:
                return replayed, True
            row = self.conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
            if row is None:
                raise NotFoundError("项目不存在")
            item = dict(row)
            if item["version"] != expected_version:
                raise ConflictError("版本冲突，请刷新后重试")
            cur = self.conn.execute(
                """UPDATE items SET status=?, version=version+1, updated_at=?,
                   basis_obs_version=? WHERE id=? AND version=?""",
                (target, now, basis_obs_version, item_id, expected_version),
            )
            if cur.rowcount == 0:
                raise ConflictError("版本冲突，请刷新后重试")
            updated = dict(self.conn.execute(
                "SELECT * FROM items WHERE id=?", (item_id,)
            ).fetchone())
            detail: Dict[str, Any] = {
                "from": item["status"], "to": target,
                "basis_obs_version": basis_obs_version,
            }
            if target == "authorized":
                open_rows = self.conn.execute(
                    """SELECT id, kind FROM records
                       WHERE item_id=? AND status='open' ORDER BY id""",
                    (item_id,),
                ).fetchall()
                detail["open_records"] = [
                    {"id": r["id"], "kind": r["kind"]} for r in open_rows
                ]
                detail["open_record_count"] = len(open_rows)
            self._append_audit("transition", "items", item_id, actor, detail)
            if request_no:
                self._store_idempotency(request_no, "transition", "items", item_id, updated)
        return updated, False

    def supplement_basis(self, item_id: int, basis_obs_version: int,
                          actor: str) -> Dict[str, Any]:
        """历史缺依据指令补齐依据并复核(supplement -> checked)。"""
        now = utc_now()
        with self._lock, self.conn:
            row = self.conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
            if row is None:
                raise NotFoundError("项目不存在")
            item = dict(row)
            if item["status"] != "supplement":
                raise ConflictError("仅待补核指令可补充依据")
            self.conn.execute(
                """UPDATE items SET status='checked', version=version+1,
                   basis_obs_version=?, updated_at=? WHERE id=?""",
                (basis_obs_version, now, item_id),
            )
            updated = dict(self.conn.execute(
                "SELECT * FROM items WHERE id=?", (item_id,)
            ).fetchone())
            self._append_audit("supplement", "items", item_id, actor, {
                "basis_obs_version": basis_obs_version,
                "from_status": "supplement", "to_status": "checked",
            })
        return updated

    # ------------------------------------------------------------ records
    def add_record(self, item_id: int, kind: str, detail: str, status: str,
                   external_ref: Optional[str], basis_obs_version: int,
                   request_no: Optional[str], actor: str
                   ) -> Tuple[Dict[str, Any], bool]:
        now = utc_now()
        self.get_item(item_id)
        with self._lock, self.conn:
            replayed = self._replay(request_no, "add_record")
            if replayed is not None:
                return replayed, True
            try:
                cur = self.conn.execute(
                    """INSERT INTO records(item_id, kind, detail, status, external_ref,
                       basis_obs_version, created_by, created_at)
                       VALUES(?,?,?,?,?,?,?,?)""",
                    (item_id, kind, detail, status, external_ref,
                     basis_obs_version, actor, now),
                )
                record_id = int(cur.lastrowid)
            except sqlite3.IntegrityError as exc:
                replayed = self._replay(request_no, "add_record")
                if replayed is not None:
                    return replayed, True
                raise ConflictError("记录唯一标识已存在") from exc
            record = dict(self.conn.execute(
                "SELECT * FROM records WHERE id=?", (record_id,)
            ).fetchone())
            self._append_audit("record", "items", item_id, actor, {
                "record_id": record_id, "kind": kind, "status": status,
                "basis_obs_version": basis_obs_version,
            })
            if request_no:
                self._store_idempotency(request_no, "add_record", "record", record_id, record)
        return record, False

    def list_records(self, item_id: int) -> List[Dict[str, Any]]:
        self.get_item(item_id)
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM records WHERE item_id=? ORDER BY id", (item_id,)
            ).fetchall()
        return [dict(row) for row in rows]

    def open_record_count(self, item_id: int) -> int:
        with self._lock:
            row = self.conn.execute(
                "SELECT COUNT(*) AS n FROM records WHERE item_id=? AND status='open'",
                (item_id,),
            ).fetchone()
        return int(row["n"])

    # -------------------------------------------------------------- audit
    def append_audit(self, action: str, entity_type: str, entity_id: int,
                     actor: str, detail: dict) -> Dict[str, Any]:
        with self._lock, self.conn:
            return self._append_audit(action, entity_type, entity_id, actor, detail)

    def list_audit(self, entity_id: Optional[int] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM audit_events"
        params: tuple = ()
        if entity_id is not None:
            sql += " WHERE entity_id=?"
            params = (entity_id,)
        sql += " ORDER BY id"
        with self._lock:
            rows = self.conn.execute(sql, params).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["detail"] = json.loads(item["detail"])
            result.append(item)
        return result

    def verify_audit_chain(self) -> bool:
        from .audit import calculate_hash
        with self._lock:
            rows = self.conn.execute("SELECT * FROM audit_events ORDER BY id").fetchall()
        previous = "GENESIS"
        for row in rows:
            if row["previous_hash"] != previous:
                return False
            payload = {
                "action": row["action"], "entity_type": row["entity_type"],
                "entity_id": row["entity_id"], "actor": row["actor"],
                "detail": json.loads(row["detail"]), "created_at": row["created_at"],
            }
            if calculate_hash(previous, payload) != row["entry_hash"]:
                return False
            previous = row["entry_hash"]
        return True

    def close(self) -> None:
        with self._lock:
            self.conn.close()
