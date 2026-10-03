import json
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

from src.domain import ConflictError
from src.repository import Repository
from src.service import Service
from src.rules import STATES, TRANSITION_ROLES


class ObservationBasisTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = Repository(str(Path(self.tmp.name) / "test.db"))
        self.service = Service(self.repo)

    def tearDown(self):
        self.repo.close()
        self.tmp.cleanup()

    def _create_item(self, ref="ITEM-1", severity="urgent", quantity=12, threshold=6):
        return self.service.create_item({
            "title": "调度指令", "description": "泄洪", "severity": severity,
            "quantity": quantity, "threshold": threshold, "external_ref": ref,
        }, "creator", "duty_officer")

    def test_item_binds_to_current_observation(self):
        # 尚无观测时自动初始化默认观测，指令绑定其版本
        item = self._create_item()
        obs = self.service.list_observations("viewer")
        self.assertEqual(len(obs), 1)
        self.assertEqual(item["basis_obs_version"], obs[0]["version"])
        self.assertEqual(item["basis_obs_version"], 1)

    def test_observation_update_reverts_unexecuted_instruction(self):
        item = self._create_item()
        # 复核到 checked
        item = self.service.transition(item["id"], "checked", item["version"],
                                       "reviewer", "duty_officer")
        self.assertEqual(item["status"], "checked")
        # 新增水情观测
        result = self.service.record_observation({
            "reservoir_level": 118.5, "inflow": 3200, "note": "洪峰到达",
        }, "observer", "duty_officer")
        self.assertEqual(result["observation"]["version"], 2)
        # 未执行指令退回待复核，依据更新为新版本
        updated = self.service.get_item(item["id"], "viewer")
        self.assertEqual(updated["status"], "draft")
        self.assertEqual(updated["basis_obs_version"], 2)
        # 可重新复核
        again = self.service.transition(updated["id"], "checked", updated["version"],
                                        "reviewer", "duty_officer")
        self.assertEqual(again["status"], "checked")
        self.assertEqual(again["basis_obs_version"], 2)

    def test_executed_instruction_not_reverted(self):
        item = self._create_item()
        for target in ("checked", "authorized", "executed"):
            item = self.service.transition(item["id"], target, item["version"],
                                           "reviewer", TRANSITION_ROLES[target][0])
        self.assertEqual(item["status"], "executed")
        self.service.record_observation({"reservoir_level": 120, "inflow": 4000},
                                        "observer", "duty_officer")
        updated = self.service.get_item(item["id"], "viewer")
        self.assertEqual(updated["status"], "executed")
        self.assertEqual(updated["basis_obs_version"], 1)

    def test_authorization_records_observation_and_open_records(self):
        item = self._create_item()
        # 一条未关闭现场记录
        self.service.add_record(item["id"], {
            "kind": "gate", "detail": "闸门开启", "status": "open",
        }, "recorder", "dispatcher")
        item = self.service.transition(item["id"], "checked", item["version"],
                                       "reviewer", "duty_officer")
        item = self.service.transition(item["id"], "authorized", item["version"],
                                       "approver", "chief_engineer")
        events = self.service.audit("viewer", item["id"])
        auth = [e for e in events if e["action"] == "transition" and e["detail"]["to"] == "authorized"][0]
        self.assertEqual(auth["detail"]["basis_obs_version"], item["basis_obs_version"])
        self.assertEqual(auth["detail"]["open_record_count"], 1)
        self.assertEqual(len(auth["detail"]["open_records"]), 1)
        self.assertEqual(auth["detail"]["open_records"][0]["kind"], "gate")

    def test_record_binds_to_observation(self):
        item = self._create_item()
        rec = self.service.add_record(item["id"], {
            "kind": "evidence", "detail": "现场照片", "status": "closed",
        }, "recorder", "dispatcher")
        self.assertEqual(rec["basis_obs_version"], 1)
        self.service.record_observation({"reservoir_level": 119, "inflow": 3500},
                                        "observer", "duty_officer")
        rec2 = self.service.add_record(item["id"], {
            "kind": "evidence", "detail": "二次复核", "status": "open",
        }, "recorder", "dispatcher")
        self.assertEqual(rec2["basis_obs_version"], 2)

    def test_audit_chain_valid(self):
        item = self._create_item()
        self.service.record_observation({"reservoir_level": 118, "inflow": 3000},
                                        "observer", "duty_officer")
        item = self.service.transition(item["id"], "checked", item["version"],
                                       "reviewer", "duty_officer")
        self.assertTrue(self.repo.verify_audit_chain())


class IdempotencyTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = Repository(str(Path(self.tmp.name) / "test.db"))
        self.service = Service(self.repo)

    def tearDown(self):
        self.repo.close()
        self.tmp.cleanup()

    def test_same_request_no_replays_without_duplicate_audit(self):
        first = self.service.create_item({
            "title": "指令A", "description": "d", "severity": "urgent",
            "quantity": 5, "threshold": 10,
        }, "creator", "duty_officer", request_no="REQ-1")
        second = self.service.create_item({
            "title": "指令A", "description": "d", "severity": "urgent",
            "quantity": 5, "threshold": 10,
        }, "creator", "duty_officer", request_no="REQ-1")
        self.assertEqual(first["id"], second["id"])
        events = self.service.audit("viewer", first["id"])
        creates = [e for e in events if e["action"] == "create"]
        self.assertEqual(len(creates), 1)

    def test_concurrent_same_request_no_only_one_wins(self):
        results = []
        errors = []

        def worker():
            try:
                results.append(self.service.create_item({
                    "title": "并发指令", "description": "d", "severity": "urgent",
                    "quantity": 5, "threshold": 10,
                }, "creator", "duty_officer", request_no="REQ-CONC"))
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len(errors), 0)
        ids = {r["id"] for r in results}
        self.assertEqual(len(ids), 1)
        events = self.service.audit("viewer", results[0]["id"])
        self.assertEqual(len([e for e in events if e["action"] == "create"]), 1)

    def test_different_request_no_same_external_ref_conflicts(self):
        payload = {
            "title": "指令B", "description": "d", "severity": "urgent",
            "quantity": 5, "threshold": 10, "external_ref": "SAME-REF",
        }
        self.service.create_item(payload, "creator", "duty_officer", request_no="REQ-2")
        with self.assertRaises(ConflictError):
            self.service.create_item(payload, "creator", "duty_officer", request_no="REQ-3")

    def test_retry_after_write_failure_no_duplicate_audit(self):
        # 模拟写入失败后凭原请求号重试：首次成功，重试不重复写审计
        first = self.service.create_item({
            "title": "指令C", "description": "d", "severity": "urgent",
            "quantity": 5, "threshold": 10,
        }, "creator", "duty_officer", request_no="REQ-4")
        # 模拟客户端未收到响应，凭同一请求号重试
        retried = self.service.create_item({
            "title": "指令C", "description": "d", "severity": "urgent",
            "quantity": 5, "threshold": 10,
        }, "creator", "duty_officer", request_no="REQ-4")
        self.assertEqual(first["id"], retried["id"])
        events = self.service.audit("viewer", first["id"])
        self.assertEqual(len([e for e in events if e["action"] == "create"]), 1)

    def test_record_request_no_idempotent(self):
        item = self.service.create_item({
            "title": "指令D", "description": "d", "severity": "routine",
            "quantity": 1, "threshold": 10,
        }, "creator", "duty_officer")
        r1 = self.service.add_record(item["id"], {
            "kind": "k", "detail": "d", "status": "open",
        }, "recorder", "dispatcher", request_no="REC-1")
        r2 = self.service.add_record(item["id"], {
            "kind": "k", "detail": "d", "status": "open",
        }, "recorder", "dispatcher", request_no="REC-1")
        self.assertEqual(r1["id"], r2["id"])
        events = self.service.audit("viewer", item["id"])
        self.assertEqual(len([e for e in events if e["action"] == "record"]), 1)


class LegacyMigrationTest(unittest.TestCase):
    def _make_old_db(self, path):
        conn = sqlite3.connect(path)
        conn.executescript("""
            CREATE TABLE items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                severity TEXT NOT NULL,
                quantity REAL NOT NULL DEFAULT 0,
                threshold REAL NOT NULL DEFAULT 1,
                status TEXT NOT NULL CHECK(status IN ('draft','checked','authorized','executed','closed')),
                version INTEGER NOT NULL DEFAULT 1,
                external_ref TEXT,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
                kind TEXT NOT NULL,
                detail TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open' CHECK(status IN ('open','closed')),
                external_ref TEXT,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(item_id, external_ref)
            );
        """)
        conn.execute(
            """INSERT INTO items(title, description, severity, quantity, threshold,
               status, version, external_ref, created_by, created_at, updated_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
            ("历史指令", "旧库", "urgent", 10, 5, "checked", 2, "OLD-1",
             "olduser", "2026-01-01T00:00:00+00:00", "2026-01-01T00:00:00+00:00"),
        )
        conn.commit()
        conn.close()

    def test_legacy_item_upgraded_to_supplement(self):
        tmp = tempfile.TemporaryDirectory()
        db_path = str(Path(tmp.name) / "legacy.db")
        self._make_old_db(db_path)
        repo = Repository(db_path)
        service = Service(repo)
        item = service.get_item(1, "viewer")
        self.assertEqual(item["status"], "supplement")
        self.assertIsNone(item["basis_obs_version"])
        repo.close()
        tmp.cleanup()

    def test_supplement_binds_basis_and_reviews(self):
        tmp = tempfile.TemporaryDirectory()
        db_path = str(Path(tmp.name) / "legacy.db")
        self._make_old_db(db_path)
        repo = Repository(db_path)
        service = Service(repo)
        item = service.get_item(1, "viewer")
        self.assertEqual(item["status"], "supplement")
        updated = service.supplement_basis(1, "reviewer", "duty_officer")
        self.assertEqual(updated["status"], "checked")
        self.assertIsNotNone(updated["basis_obs_version"])
        # 补齐后可继续授权
        authorized = service.transition(1, "authorized", updated["version"],
                                        "approver", "chief_engineer")
        self.assertEqual(authorized["status"], "authorized")
        repo.close()
        tmp.cleanup()

    def test_migration_is_idempotent(self):
        tmp = tempfile.TemporaryDirectory()
        db_path = str(Path(tmp.name) / "legacy.db")
        self._make_old_db(db_path)
        repo = Repository(db_path)
        service = Service(repo)
        # 再次执行迁移不应报错或重复升级
        repo._migrate_legacy()
        item = service.get_item(1, "viewer")
        self.assertEqual(item["status"], "supplement")
        repo.close()
        tmp.cleanup()


if __name__ == "__main__":
    unittest.main()
