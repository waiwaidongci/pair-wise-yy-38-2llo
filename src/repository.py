from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from .audit import make_entry, utc_now
from .domain import ConflictError, NotFoundError
from .rules import ALL_STATES, ENTITY, ID_PREFIX, INVALIDATABLE_STATES, OBSERVATION_ENTITY, RECHECK_STATE, SUP_STATE, STATES


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
        self._migrate_legacy()

    def _create_schema(self) -> None:
        statuses = ",".join("'" + s.replace("'", "''") + "'" for s in ALL_STATES)
        with self.conn:
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
                    basis_version INTEGER,
                    basis_stale INTEGER NOT NULL DEFAULT 0,
                    prior_status TEXT,
                    auth_snapshot TEXT,
                    external_ref TEXT,
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
                    basis_version INTEGER,
                    external_ref TEXT,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(item_id, external_ref)
                );
                CREATE TABLE IF NOT EXISTS observations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    basis_version INTEGER NOT NULL UNIQUE,
                    water_level REAL NOT NULL,
                    inflow REAL NOT NULL,
                    downstream_guard REAL NOT NULL DEFAULT 0,
                    note TEXT,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS idempotent_requests (
                    request_id TEXT PRIMARY KEY,
                    action TEXT NOT NULL,
                    status_code INTEGER NOT NULL,
                    response TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    created_at TEXT NOT NULL
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

    # ---------- 迁移：历史指令缺依据版本 → 待补核 ----------
    def _table_columns(self, table: str) -> set:
        rows = self.conn.execute(f"PRAGMA table_info({table})").fetchall()
        return {r["name"] for r in rows}

    def _migrate_legacy(self) -> None:
        with self._lock, self.conn:
            version = int(self.conn.execute("PRAGMA user_version").fetchone()[0])
            if version >= 2:
                return
            item_cols = self._table_columns("items")
            migrated: List[Dict[str, Any]] = []
            if "basis_version" not in item_cols:
                self.conn.executescript(
                    "ALTER TABLE items ADD COLUMN basis_version INTEGER;"
                    "ALTER TABLE items ADD COLUMN basis_stale INTEGER NOT NULL DEFAULT 0;"
                    "ALTER TABLE items ADD COLUMN prior_status TEXT;"
                    "ALTER TABLE items ADD COLUMN auth_snapshot TEXT;"
                )
            if "basis_version" not in self._table_columns("records"):
                self.conn.execute("ALTER TABLE records ADD COLUMN basis_version INTEGER")
            # 旧 CHECK 只允许 STATES；重建 items 以放宽到 ALL_STATES
            row = self.conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='items'"
            ).fetchone()
            if row and SUP_STATE not in (row["sql"] or ""):
                statuses = ",".join("'" + s + "'" for s in ALL_STATES)
                self.conn.executescript(f"""
                    ALTER TABLE items RENAME TO items_legacy;
                    CREATE TABLE items (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        title TEXT NOT NULL,
                        description TEXT NOT NULL,
                        severity TEXT NOT NULL,
                        quantity REAL NOT NULL DEFAULT 0,
                        threshold REAL NOT NULL DEFAULT 1,
                        status TEXT NOT NULL CHECK(status IN ({statuses})),
                        version INTEGER NOT NULL DEFAULT 1,
                        basis_version INTEGER,
                        basis_stale INTEGER NOT NULL DEFAULT 0,
                        prior_status TEXT,
                        auth_snapshot TEXT,
                        external_ref TEXT,
                        created_by TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );
                    INSERT INTO items(id,title,description,severity,quantity,
                        threshold,status,version,basis_version,basis_stale,
                        prior_status,auth_snapshot,external_ref,created_by,
                        created_at,updated_at)
                    SELECT id,title,description,severity,quantity,threshold,
                        status,version,NULL AS basis_version,
                        0 AS basis_stale,NULL AS prior_status,
                        NULL AS auth_snapshot,external_ref,created_by,
                        created_at,updated_at FROM items_legacy;
                    DROP TABLE items_legacy;
                    CREATE UNIQUE INDEX IF NOT EXISTS ux_items_external_ref
                        ON items(external_ref) WHERE external_ref IS NOT NULL;
                """)
            legacy_rows = self.conn.execute(
                "SELECT id, status FROM items WHERE basis_version IS NULL"
            ).fetchall()
            for r in legacy_rows:
                self.conn.execute(
                    "UPDATE items SET status=?, prior_status=?, basis_stale=1, updated_at=? WHERE id=?",
                    (SUP_STATE, r["status"], utc_now(), r["id"]),
                )
                migrated.append({"id": r["id"], "from": r["status"]})
            for info in migrated:
                self._insert_audit_locked("basis_migration", ENTITY, info["id"], "system", {
                    "reason": "历史指令缺少依据版本",
                    "from": info["from"], "to": SUP_STATE,
                })
            self.conn.execute("PRAGMA user_version = 2")

    # ---------- 审计 ----------
    def _insert_audit_locked(self, action: str, entity_type: str, entity_id: int,
                             actor: str, detail: dict) -> Dict[str, Any]:
        """调用方必须持有 self._lock 且处于写事务中，保证与业务写入同提交。"""
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

    def append_audit(self, action: str, entity_type: str, entity_id: int,
                     actor: str, detail: dict) -> Dict[str, Any]:
        with self._lock, self.conn:
            return self._insert_audit_locked(action, entity_type, entity_id, actor, detail)

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

    # ---------- 幂等 ----------
    def idempotent_call(self, request_id: Optional[str], action: str, actor: str,
                        worker: Callable[[], Any]) -> Any:
        """凭原请求号重试：同一 request_id 只执行一次 worker，重试返回首次结果。

        worker 在持有全局锁的单一事务内执行，必须只调用 *_locked 变体；
        其全部业务写入与审计写入同提交，失败整体回滚，审计不会重复。
        worker 必须返回 (status_code, payload)。
        """
        if request_id is None:
            # 无请求号时由仓储自行开事务执行
            with self._lock, self.conn:
                status_code, payload = worker()
                return payload
        request_id = request_id.strip()
        with self._lock:
            existing = self.conn.execute(
                "SELECT status_code, response FROM idempotent_requests WHERE request_id=?",
                (request_id,),
            ).fetchone()
            if existing is not None:
                # 重试（含写入失败后的凭号重试）：直接回放首次结果，不再写业务或审计
                return json.loads(existing["response"])
            with self.conn:
                # 同一事务内先占位再执行业务，由全局锁串行化：
                # 两名值班员并发提交同一请求号时，只有先到者能插入
                self.conn.execute(
                    """INSERT INTO idempotent_requests(request_id, action, status_code,
                       response, actor, created_at) VALUES(?,?,?,?,?,?)""",
                    (request_id, action, 0, "{}", actor, utc_now()),
                )
                # worker 必须使用 *_locked 变体，只使用当前事务，不再自行提交；
                # 任何失败整体回滚（含占位行与审计），客户端可凭原请求号安全重试
                status_code, payload = worker()
                self.conn.execute(
                    "UPDATE idempotent_requests SET status_code=?, response=? WHERE request_id=?",
                    (status_code, json.dumps(payload, ensure_ascii=False, default=str),
                     request_id),
                )
                return payload

    # ---------- 观测 / 依据版本 ----------
    def latest_basis_version(self) -> Optional[int]:
        with self._lock:
            row = self.conn.execute(
                "SELECT basis_version FROM observations ORDER BY basis_version DESC LIMIT 1"
            ).fetchone()
        return int(row["basis_version"]) if row else None

    def ensure_seed_observation(self, actor: str = "system") -> int:
        """无任何观测时播种初始依据版本（兼容历史用法），返回最新依据版本。"""
        with self._lock, self.conn:
            return self._ensure_seed_observation_locked(actor)

    def _ensure_seed_observation_locked(self, actor: str = "system") -> int:
        row = self.conn.execute(
            "SELECT basis_version FROM observations ORDER BY basis_version DESC LIMIT 1"
        ).fetchone()
        if row is not None:
            return int(row["basis_version"])
        now = utc_now()
        cur = self.conn.execute(
            """INSERT INTO observations(basis_version, water_level, inflow,
               downstream_guard, note, created_by, created_at)
               VALUES(1,0,0,0,?,'system',?)""",
            ("初始播种依据版本", now),
        )
        observation_id = int(cur.lastrowid)
        self._insert_audit_locked("observation", OBSERVATION_ENTITY, observation_id,
                                  actor, {
            "basis_version": 1, "water_level": 0, "inflow": 0,
            "downstream_guard": 0, "invalidated_items": [],
            "seed": True,
        })
        return 1

    def get_observation(self, basis_version: int) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM observations WHERE basis_version=?", (basis_version,)
            ).fetchone()
        if row is None:
            raise NotFoundError("依据版本不存在")
        return dict(row)

    def list_observations(self) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM observations ORDER BY basis_version DESC"
            ).fetchall()
        return [dict(r) for r in rows]

    def submit_observation(self, water_level: float, inflow: float,
                           downstream_guard: float, note: Optional[str],
                           actor: str) -> Dict[str, Any]:
        """新观测入库（basis_version 递增），并作废依据旧的未执行指令复核。

        返回 {"observation": ..., "invalidated_items": [...]}，
        审计事件在同一事务写入。
        """
        with self._lock, self.conn:
            basis_version, invalidated = self._submit_observation_locked(
                water_level, inflow, downstream_guard, note, actor)
        return {
            "observation": self.get_observation(basis_version),
            "invalidated_items": invalidated,
        }

    def _submit_observation_locked(self, water_level: float, inflow: float,
                                   downstream_guard: float, note: Optional[str],
                                   actor: str):
        now = utc_now()
        row = self.conn.execute(
            "SELECT COALESCE(MAX(basis_version),0) AS v FROM observations"
        ).fetchone()
        basis_version = int(row["v"]) + 1
        cur = self.conn.execute(
            """INSERT INTO observations(basis_version, water_level, inflow,
               downstream_guard, note, created_by, created_at)
               VALUES(?,?,?,?,?,?,?)""",
            (basis_version, water_level, inflow, downstream_guard, note, actor, now),
        )
        observation_id = int(cur.lastrowid)
        # 未执行指令（checked/authorized）依据更旧或无依据 → 旧复核失效，退回待复核
        stale = self.conn.execute(
            f"""SELECT id, status FROM items
                WHERE status IN ({','.join('?' for _ in INVALIDATABLE_STATES)})
                  AND (basis_version IS NULL OR basis_version < ?)""",
            tuple(sorted(INVALIDATABLE_STATES)) + (basis_version,),
        ).fetchall()
        invalidated = [int(r["id"]) for r in stale]
        for item_id in invalidated:
            self.conn.execute(
                """UPDATE items SET status=?, basis_stale=1,
                   version=version+1, updated_at=? WHERE id=?""",
                (RECHECK_STATE, now, item_id),
            )
            self._insert_audit_locked("basis_invalidated", ENTITY, item_id, actor, {
                "new_basis_version": basis_version,
                "reason": "水情观测更新，旧复核失效，退回待复核",
            })
        self._insert_audit_locked("observation", OBSERVATION_ENTITY, observation_id,
                                  actor, {
            "basis_version": basis_version, "water_level": water_level,
            "inflow": inflow, "downstream_guard": downstream_guard,
            "invalidated_items": invalidated,
        })
        return basis_version, invalidated

    # ---------- 指令 ----------
    @staticmethod
    def _item(row: sqlite3.Row) -> Dict[str, Any]:
        return dict(row)

    def create_item(self, title: str, description: str, severity: str,
                    quantity: float, threshold: float, external_ref: Optional[str],
                    actor: str, basis_version: Optional[int]) -> Dict[str, Any]:
        with self._lock, self.conn:
            item_id = self._create_item_locked(
                title, description, severity, quantity, threshold, external_ref,
                actor, basis_version)
        return self.get_item(item_id)

    def _create_item_locked(self, title: str, description: str, severity: str,
                            quantity: float, threshold: float,
                            external_ref: Optional[str], actor: str,
                            basis_version: Optional[int]) -> int:
        now = utc_now()
        cur = self.conn.execute(
            """INSERT INTO items(title, description, severity, quantity, threshold,
               status, version, basis_version, external_ref, created_by,
               created_at, updated_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
            (title, description, severity, quantity, threshold, STATES[0], 1,
             basis_version, external_ref, actor, now, now),
        )
        item_id = int(cur.lastrowid)
        self._insert_audit_locked("create", ENTITY, item_id, actor, {
            "title": title, "severity": severity, "quantity": quantity,
            "basis_version": basis_version,
        })
        return item_id

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
                        actor: str, basis_version: Optional[int] = None,
                        auth_snapshot: Optional[dict] = None,
                        clear_stale: bool = False,
                        audit_detail: Optional[dict] = None) -> Dict[str, Any]:
        with self._lock, self.conn:
            self._transition_item_locked(
                item_id, target, expected_version, actor, basis_version,
                auth_snapshot, clear_stale, audit_detail)
        return self.get_item(item_id)

    def _transition_item_locked(self, item_id: int, target: str,
                                expected_version: int, actor: str,
                                basis_version: Optional[int],
                                auth_snapshot: Optional[dict],
                                clear_stale: bool,
                                audit_detail: Optional[dict]) -> None:
        now = utc_now()
        cur = self.conn.execute(
            """UPDATE items SET status=?, version=version+1, updated_at=?,
                   basis_version=COALESCE(?, basis_version),
                   auth_snapshot=COALESCE(?, auth_snapshot),
                   basis_stale=CASE WHEN ?=1 THEN 0 ELSE basis_stale END
               WHERE id=? AND version=?""",
            (target, now, basis_version,
             json.dumps(auth_snapshot, ensure_ascii=False, default=str)
             if auth_snapshot is not None else None,
             1 if clear_stale else 0, item_id, expected_version),
        )
        if cur.rowcount == 0:
            exists = self.conn.execute("SELECT 1 FROM items WHERE id=?", (item_id,)).fetchone()
            if exists is None:
                raise NotFoundError("项目不存在")
            raise ConflictError("版本冲突，请刷新后重试")
        if audit_detail is not None:
            self._insert_audit_locked("transition", ENTITY, item_id, actor,
                                      audit_detail)

    def supplement_basis(self, item_id: int, basis_version: int, actor: str) -> Dict[str, Any]:
        """历史指令补核：绑定依据版本，回到迁移前状态，清除待补核标记。"""
        with self._lock, self.conn:
            self._supplement_basis_locked(item_id, basis_version, actor)
        return self.get_item(item_id)

    def _supplement_basis_locked(self, item_id: int, basis_version: int,
                                 actor: str) -> None:
        now = utc_now()
        row = self.conn.execute(
            "SELECT prior_status FROM items WHERE id=? AND status=?",
            (item_id, SUP_STATE),
        ).fetchone()
        if row is None:
            exists = self.conn.execute("SELECT 1 FROM items WHERE id=?", (item_id,)).fetchone()
            if exists is None:
                raise NotFoundError("项目不存在")
            raise ConflictError("指令不在待补核状态")
        target = row["prior_status"] or STATES[0]
        self.conn.execute(
            """UPDATE items SET status=?, basis_version=?, basis_stale=0,
               prior_status=NULL, version=version+1, updated_at=? WHERE id=?""",
            (target, basis_version, now, item_id),
        )
        self._insert_audit_locked("basis_supplemented", ENTITY, item_id, actor, {
            "basis_version": basis_version, "to": target,
        })

    def recheck_item(self, item_id: int, expected_version: int, basis_version: int,
                     actor: str) -> Dict[str, Any]:
        """旧复核失效后的重新复核：依据最新观测，回到 checked。"""
        with self._lock, self.conn:
            self._recheck_item_locked(item_id, expected_version, basis_version, actor)
        return self.get_item(item_id)

    def _recheck_item_locked(self, item_id: int, expected_version: int,
                             basis_version: int, actor: str) -> None:
        now = utc_now()
        cur = self.conn.execute(
            """UPDATE items SET status='checked', basis_version=?, basis_stale=0,
                   version=version+1, updated_at=? WHERE id=? AND version=?
                   AND status=?""",
            (basis_version, now, item_id, expected_version, RECHECK_STATE),
        )
        if cur.rowcount == 0:
            exists = self.conn.execute("SELECT 1 FROM items WHERE id=?", (item_id,)).fetchone()
            if exists is None:
                raise NotFoundError("项目不存在")
            raise ConflictError("版本冲突或指令不在待复核状态")
        self._insert_audit_locked("recheck", ENTITY, item_id, actor, {
            "basis_version": basis_version,
        })

    # ---------- 操作记录 ----------
    def add_record(self, item_id: int, kind: str, detail: str, status: str,
                   external_ref: Optional[str], actor: str,
                   basis_version: Optional[int]) -> Dict[str, Any]:
        with self._lock, self.conn:
            record_id = self._add_record_locked(
                item_id, kind, detail, status, external_ref, actor, basis_version)
        with self._lock:
            row = self.conn.execute("SELECT * FROM records WHERE id=?", (record_id,)).fetchone()
        return dict(row)

    def _add_record_locked(self, item_id: int, kind: str, detail: str,
                           status: str, external_ref: Optional[str], actor: str,
                           basis_version: Optional[int]) -> int:
        now = utc_now()
        lock_row = self.conn.execute(
            "SELECT id FROM items WHERE id=?", (item_id,)).fetchone()
        if lock_row is None:
            raise NotFoundError("项目不存在")
        try:
            cur = self.conn.execute(
                """INSERT INTO records(item_id, kind, detail, status, basis_version,
                   external_ref, created_by, created_at) VALUES(?,?,?,?,?,?,?,?)""",
                (item_id, kind, detail, status, basis_version,
                 external_ref, actor, now),
            )
        except sqlite3.IntegrityError as exc:
            raise ConflictError("记录唯一标识已存在") from exc
        record_id = int(cur.lastrowid)
        self._insert_audit_locked("record", ENTITY, item_id, actor, {
            "record_id": record_id, "kind": kind, "status": status,
            "basis_version": basis_version,
        })
        return record_id

    def list_records(self, item_id: int) -> List[Dict[str, Any]]:
        self.get_item(item_id)
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM records WHERE item_id=? ORDER BY id", (item_id,)
            ).fetchall()
        return [dict(row) for row in rows]

    def open_records(self, item_id: int) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT id, kind, detail, basis_version FROM records "
                "WHERE item_id=? AND status='open' ORDER BY id",
                (item_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def open_record_count(self, item_id: int) -> int:
        with self._lock:
            row = self.conn.execute(
                "SELECT COUNT(*) AS n FROM records WHERE item_id=? AND status='open'",
                (item_id,),
            ).fetchone()
        return int(row["n"])

    def close(self) -> None:
        with self._lock:
            self.conn.close()
