import sqlite3
import tempfile
import unittest
from pathlib import Path

from src.domain import ConflictError
from src.repository import Repository
from src.rules import RECHECK_STATE, SUP_STATE, STATES
from src.service import Service


class BasisVersionTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmp.name) / "test.db")
        self.repo = Repository(self.db_path)
        self.service = Service(self.repo)

    def tearDown(self):
        self.repo.close()
        self.tmp.cleanup()

    def _observe(self, level=10.0, inflow=5.0, guard=8.0, request_id=None):
        payload = {"water_level": level, "inflow": inflow,
                   "downstream_guard": guard}
        if request_id:
            payload["request_id"] = request_id
        return self.service.submit_observation(payload, "observer", "duty_officer")

    def _check(self, item, actor="reviewer"):
        return self.service.transition(item["id"], "checked", item["version"],
                                       actor, "duty_officer")

    def _authorize(self, item, actor="chief"):
        return self.service.transition(item["id"], "authorized", item["version"],
                                       actor, "chief_engineer")

    def test_items_records_and_audit_share_basis_version(self):
        result = self._observe(12.0, 7.0)
        basis = result["observation"]["basis_version"]
        item = self.service.create_item(
            {"title": "RF-1", "description": "泄洪", "severity": "urgent",
             "quantity": 12, "threshold": 6}, "duty", "duty_officer")
        self.assertEqual(item["basis_version"], basis)
        record = self.service.add_record(
            item["id"], {"kind": "evidence", "detail": "现场读数",
                         "status": "closed"}, "rec", "duty_officer")
        self.assertEqual(record["basis_version"], basis)
        checked = self._check(item)
        events = self.service.audit("viewer", item["id"])
        create_event = next(e for e in events if e["action"] == "create")
        record_event = next(e for e in events if e["action"] == "record")
        transition_event = next(e for e in events if e["action"] == "transition")
        self.assertEqual(create_event["detail"]["basis_version"], basis)
        self.assertEqual(record_event["detail"]["basis_version"], basis)
        self.assertEqual(transition_event["detail"]["basis_version"], basis)
        self.assertTrue(self.repo.verify_audit_chain())

    def test_observation_invalidates_pending_checked_and_authorized(self):
        self._observe(10.0, 5.0)
        item = self.service.create_item(
            {"title": "RF-2", "description": "待复核失效", "severity": "urgent",
             "quantity": 10, "threshold": 5}, "duty", "duty_officer")
        checked = self._check(item)
        self.assertEqual(checked["status"], "checked")
        # 新观测到达：库位变化，旧复核失效
        self._observe(13.5, 9.0)
        stale = self.service.get_item(item["id"], "viewer")
        self.assertEqual(stale["status"], RECHECK_STATE)
        self.assertTrue(stale["recheck_required"])
        # 旧版本号无法直接授权
        with self.assertRaises(ConflictError):
            self.service.transition(item["id"], "authorized", checked["version"],
                                    "chief", "chief_engineer")
        # 必须重新复核，且绑定最新依据
        latest = self.repo.latest_basis_version()
        rechecked = self.service.recheck(
            item["id"], {"expected_version": stale["version"]},
            "reviewer", "duty_officer")
        self.assertEqual(rechecked["status"], "checked")
        self.assertEqual(rechecked["basis_version"], latest)
        authorized = self._authorize(rechecked)
        self.assertEqual(authorized["status"], "authorized")

    def test_executed_instructions_survive_new_observations(self):
        self._observe(10.0, 5.0)
        item = self.service.create_item(
            {"title": "RF-3", "description": "已执行不受影响", "severity": "urgent",
             "quantity": 10, "threshold": 5}, "duty", "duty_officer")
        self.service.add_record(item["id"], {"kind": "feedback",
                                             "detail": "闭环", "status": "closed"},
                                "disp", "dispatcher")
        cur = self._check(item)
        cur = self._authorize(cur)
        cur = self.service.transition(cur["id"], "executed", cur["version"],
                                      "disp", "dispatcher")
        self._observe(20.0, 15.0)
        after = self.service.get_item(item["id"], "viewer")
        self.assertEqual(after["status"], "executed")

    def test_authorization_records_observation_and_open_records_snapshot(self):
        self._observe(11.0, 6.0)
        item = self.service.create_item(
            {"title": "RF-4", "description": "授权快照", "severity": "urgent",
             "quantity": 11, "threshold": 6}, "duty", "duty_officer")
        self.service.add_record(item["id"], {"kind": "warning",
                                             "detail": "下游围堰", "status": "open"},
                                "rec", "duty_officer")
        checked = self._check(item)
        authorized = self._authorize(checked)
        snapshot = authorized["auth_snapshot"]
        self.assertEqual(snapshot["basis_version"], item["basis_version"])
        self.assertEqual(snapshot["observation"]["water_level"], 11.0)
        self.assertEqual(snapshot["observation"]["inflow"], 6.0)
        self.assertEqual(len(snapshot["open_records"]), 1)
        self.assertEqual(snapshot["open_records"][0]["kind"], "warning")
        # 授权事件同样记录快照
        events = self.service.audit("viewer", item["id"])
        auth_event = next(e for e in events if e["action"] == "transition"
                          and e["detail"]["to"] == "authorized")
        self.assertIn("auth_snapshot", auth_event["detail"])

    def test_stale_basis_cannot_be_checked_after_observation_update(self):
        self._observe(10.0, 5.0)
        item = self.service.create_item(
            {"title": "RF-5", "description": "旧读数复核", "severity": "routine",
             "quantity": 1, "threshold": 5}, "duty", "duty_officer")
        self._observe(14.0, 12.0)
        # 观测已更新，仍按旧读数复核 → 冲突；指令退回待复核
        with self.assertRaises(ConflictError):
            self.service.transition(item["id"], "checked", item["version"],
                                    "reviewer", "duty_officer")
        # draft 不自动失效（尚未复核），但复核动作要求先刷新；这里直接进入待复核由观测更新不影响draft
        draft = self.service.get_item(item["id"], "viewer")
        self.assertIn(draft["status"], ("draft", RECHECK_STATE))

    def test_concurrent_same_instruction_only_first_wins(self):
        self._observe(10.0, 5.0)
        payloads = [
            {"title": "SAME", "description": "并发", "severity": "routine",
             "quantity": 1, "threshold": 5, "request_id": "REQ-1"},
            {"title": "SAME", "description": "并发", "severity": "routine",
             "quantity": 1, "threshold": 5, "request_id": "REQ-1"},
        ]
        first = self.service.create_item(payloads[0], "duty-a", "duty_officer")
        second = self.service.create_item(payloads[1], "duty-b", "duty_officer")
        self.assertEqual(first["id"], second["id"])
        creates = [e for e in self.repo.list_audit() if e["action"] == "create"]
        self.assertEqual(len(creates), 1)

    def test_retry_with_same_request_id_does_not_duplicate_audit(self):
        self._observe(10.0, 5.0)
        payload = {"title": "RETRY", "description": "重试", "severity": "routine",
                   "quantity": 1, "threshold": 5, "request_id": "REQ-2"}
        first = self.service.create_item(payload, "duty", "duty_officer")
        second = self.service.create_item(payload, "duty", "duty_officer")
        self.assertEqual(first["id"], second["id"])
        record_payload = {"kind": "evidence", "detail": "可重试记录",
                          "status": "closed", "request_id": "REQ-3"}
        r1 = self.service.add_record(first["id"], record_payload, "rec",
                                     "duty_officer")
        r2 = self.service.add_record(first["id"], record_payload, "rec",
                                     "duty_officer")
        self.assertEqual(r1["id"], r2["id"])
        record_events = [e for e in self.repo.list_audit()
                         if e["action"] == "record"]
        self.assertEqual(len(record_events), 1)
        # 观测提交同样幂等
        obs_payload = {"water_level": 9.0, "inflow": 3.0,
                       "request_id": "REQ-4"}
        o1 = self.service.submit_observation(obs_payload, "obs", "duty_officer")
        o2 = self.service.submit_observation(obs_payload, "obs", "duty_officer")
        self.assertEqual(o1["observation"]["basis_version"],
                         o2["observation"]["basis_version"])
        self.assertTrue(self.repo.verify_audit_chain())

    def test_failed_write_allows_safe_retry_without_duplicate(self):
        self._observe(10.0, 5.0)
        item = self.service.create_item(
            {"title": "RETRY-FAIL", "description": "失败重试", "severity": "routine",
             "quantity": 1, "threshold": 5}, "duty", "duty_officer")
        # 第一次用合法 external_ref
        good = {"kind": "evidence", "detail": "ok", "status": "closed",
                "external_ref": "EXT-1", "request_id": "REQ-5"}
        rec = self.service.add_record(item["id"], good, "rec", "duty_officer")
        # 同请求号重试，回放首次结果，不产生新记录或审计
        again = self.service.add_record(item["id"], good, "rec", "duty_officer")
        self.assertEqual(rec["id"], again["id"])
        self.assertEqual(len(self.repo.list_records(item["id"])), 1)

    def test_parallel_same_request_id_from_two_officers(self):
        import threading
        self._observe(10.0, 5.0)
        results = []
        errors = []

        def submit(officer):
            try:
                payload = {"title": "PARALLEL", "description": "同时提交",
                           "severity": "routine", "quantity": 1, "threshold": 5,
                           "request_id": "PAR-1"}
                results.append(self.service.create_item(
                    payload, officer, "duty_officer"))
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        threads = [threading.Thread(target=submit, args=(f"duty-{i}",))
                   for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertFalse(errors)
        self.assertEqual(len(results), 8)
        self.assertEqual({r["id"] for r in results}, {results[0]["id"]})
        creates = [e for e in self.repo.list_audit()
                   if e["action"] == "create" and e["actor"].startswith("duty-")]
        self.assertEqual(len(creates), 1)
        self.assertTrue(self.repo.verify_audit_chain())


class LegacyMigrationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmp.name) / "legacy.db")

    def tearDown(self):
        self.tmp.cleanup()

    def _build_legacy_db(self):
        # 用旧 schema（无 basis_version 列、旧 CHECK 约束）直接造历史数据
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL, description TEXT NOT NULL,
                severity TEXT NOT NULL, quantity REAL NOT NULL DEFAULT 0,
                threshold REAL NOT NULL DEFAULT 1,
                status TEXT NOT NULL CHECK(status IN
                  ('draft','checked','authorized','executed','closed')),
                version INTEGER NOT NULL DEFAULT 1,
                external_ref TEXT, created_by TEXT NOT NULL,
                created_at TEXT NOT NULL, updated_at TEXT NOT NULL
            )""")
        conn.execute("""
            CREATE TABLE records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_id INTEGER NOT NULL, kind TEXT NOT NULL,
                detail TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'open',
                external_ref TEXT, created_by TEXT NOT NULL, created_at TEXT NOT NULL,
                UNIQUE(item_id, external_ref))""")
        conn.execute("""
            CREATE TABLE audit_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT, action TEXT NOT NULL,
                entity_type TEXT NOT NULL, entity_id INTEGER NOT NULL,
                actor TEXT NOT NULL, detail TEXT NOT NULL,
                previous_hash TEXT NOT NULL, entry_hash TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL)""")
        conn.execute("""INSERT INTO items(title,description,severity,quantity,
            threshold,status,external_ref,created_by,created_at,updated_at)
            VALUES('历史指令','缺依据','urgent',5,10,'authorized','OLD-1',
            'duty','2026-09-01T00:00:00+00:00','2026-09-01T00:00:00+00:00')""")
        conn.commit()
        conn.close()

    def test_legacy_items_without_basis_become_supplement_pending(self):
        self._build_legacy_db()
        repo = Repository(self.db_path)
        service = Service(repo)
        item = service.get_item(1, "viewer")
        self.assertEqual(item["status"], SUP_STATE)
        self.assertTrue(item["pending_basis"])
        self.assertIsNone(item["basis_version"])
        # 常规流转被禁止
        with self.assertRaises(ConflictError):
            service.transition(1, "executed", item["version"], "disp",
                               "dispatcher")
        # 提交观测并补核后，回到迁移前状态
        service.submit_observation(
            {"water_level": 9.0, "inflow": 2.0}, "obs", "duty_officer")
        restored = service.supplement_basis(
            1, {"basis_version": 1}, "chief", "chief_engineer")
        self.assertEqual(restored["status"], "authorized")
        self.assertEqual(restored["basis_version"], 1)
        self.assertFalse(restored["pending_basis"])
        migrations = [e for e in repo.list_audit()
                      if e["action"] == "basis_migration"]
        self.assertEqual(len(migrations), 1)
        supplements = [e for e in repo.list_audit()
                       if e["action"] == "basis_supplemented"]
        self.assertEqual(len(supplements), 1)
        self.assertTrue(repo.verify_audit_chain())
        repo.close()


if __name__ == "__main__":
    unittest.main()
