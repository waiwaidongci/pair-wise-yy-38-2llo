from __future__ import annotations

from typing import Any, Dict, Optional

from .domain import (ConflictError, ensure_role, normalize_severity,
                     require_number, require_text)
from .repository import Repository
from .rules import (AUDIT_ROLES, CREATE_ROLES, ENTITY, OBSERVATION_ENTITY,
                    OBSERVATION_ROLES, RECHECK_ROLES, RECHECK_STATE,
                    RECORD_ROLES, SUP_STATE, SUPPLEMENT_ROLES, VIEW_ROLES,
                    completion_blockers, escalation_required, priority_score,
                    response_deadline_hours, role_for_transition,
                    validate_transition)


class Service:
    def __init__(self, repository: Repository):
        self.repository = repository

    def _view(self, role: str) -> None:
        ensure_role(role, VIEW_ROLES)

    @staticmethod
    def _request_id(payload: Dict[str, Any]) -> Optional[str]:
        request_id = payload.get("request_id")
        if request_id is None:
            return None
        return require_text(request_id, "request_id", 100)

    def _require_basis(self, allow_seed: bool = False) -> int:
        basis_version = self.repository.latest_basis_version()
        if basis_version is None:
            if allow_seed:
                return self.repository.ensure_seed_observation(actor="system")
            raise ConflictError("尚无任何水情观测依据，请先提交观测")
        return basis_version

    # ---------- 水情观测 ----------
    def submit_observation(self, payload: Dict[str, Any], actor: str,
                           role: str) -> Dict[str, Any]:
        ensure_role(role, OBSERVATION_ROLES)
        actor = require_text(actor, "actor", 100)
        water_level = require_number(payload.get("water_level"), "water_level")
        inflow = require_number(payload.get("inflow"), "inflow")
        downstream_guard = require_number(payload.get("downstream_guard", 0),
                                          "downstream_guard")
        note = payload.get("note")
        if note is not None:
            note = require_text(note, "note", 2000)
        request_id = self._request_id(payload)

        def worker():
            basis_version, invalidated = self.repository._submit_observation_locked(
                water_level, inflow, downstream_guard, note, actor)
            body = {
                "observation": self.repository.get_observation(basis_version),
                "invalidated_items": invalidated,
            }
            return 201, body

        if request_id is None:
            return self.repository.idempotent_call(None, "observation", actor, worker)
        return self.repository.idempotent_call(
            request_id, "observation", actor, worker)

    def list_observations(self, role: str) -> list:
        self._view(role)
        return self.repository.list_observations()

    # ---------- 调度指令 ----------
    def create_item(self, payload: Dict[str, Any], actor: str, role: str) -> Dict[str, Any]:
        ensure_role(role, CREATE_ROLES)
        actor = require_text(actor, "actor", 100)
        title = require_text(payload.get("title"), "title", 200)
        description = require_text(payload.get("description"), "description")
        severity = normalize_severity(payload.get("severity"))
        quantity = require_number(payload.get("quantity", 0), "quantity")
        threshold = require_number(payload.get("threshold", 1), "threshold", 0.000001)
        external_ref = payload.get("external_ref")
        if external_ref is not None:
            external_ref = require_text(external_ref, "external_ref", 100)
        # 新建指令默认绑定最新依据版本；尚无观测时播种初始依据（兼容历史流程）
        basis_version = self._require_basis(allow_seed=True)
        request_id = self._request_id(payload)

        def worker():
            item_id = self.repository._create_item_locked(
                title, description, severity, quantity, threshold, external_ref,
                actor, basis_version)
            item = self.repository.get_item(item_id)
            result = self.enrich(item)
            return 201, result

        if request_id is None:
            return self.repository.idempotent_call(None, "create", actor, worker)
        return self.repository.idempotent_call(request_id, "create", actor, worker)

    def add_record(self, item_id: int, payload: Dict[str, Any], actor: str,
                   role: str) -> Dict[str, Any]:
        ensure_role(role, RECORD_ROLES)
        actor = require_text(actor, "actor", 100)
        kind = require_text(payload.get("kind"), "kind", 100)
        detail = require_text(payload.get("detail"), "detail")
        status = payload.get("status", "open")
        if status not in ("open", "closed"):
            from .domain import ValidationError
            raise ValidationError("status必须是open或closed")
        external_ref = payload.get("external_ref")
        if external_ref is not None:
            external_ref = require_text(external_ref, "external_ref", 100)
        item = self.repository.get_item(item_id)
        basis_version = item["basis_version"] or self._require_basis()
        request_id = self._request_id(payload)

        def worker():
            record_id = self.repository._add_record_locked(
                item_id, kind, detail, status, external_ref, actor, basis_version)
            with self.repository._lock:
                row = self.repository.conn.execute(
                    "SELECT * FROM records WHERE id=?", (record_id,)).fetchone()
            return 201, dict(row)

        if request_id is None:
            return self.repository.idempotent_call(None, "record", actor, worker)
        return self.repository.idempotent_call(request_id, "record", actor, worker)

    def transition(self, item_id: int, target: str, expected_version: int,
                   actor: str, role: str,
                   request_id: Optional[str] = None) -> Dict[str, Any]:
        actor = require_text(actor, "actor", 100)
        # 角色不依赖可变状态，可在事务外判定
        ensure_role(role, role_for_transition(target))
        if not isinstance(expected_version, int) or expected_version < 1:
            from .domain import ValidationError
            raise ValidationError("expected_version必须是正整数")
        # 先确认指令存在
        self.repository.get_item(item_id)

        def worker():
            # 全部基于可变状态的判定都在持锁事务内重读，避免与观测更新竞态：
            # 观测一旦提交即串行在前，旧读数复核/授权不可能溜过去
            repo = self.repository
            item = repo.get_item(item_id)
            validate_transition(item["status"], target)
            blockers = completion_blockers(target, repo.open_record_count(item_id))
            if blockers:
                raise ConflictError("；".join(blockers))
            basis_version = item["basis_version"]
            auth_snapshot = None
            audit_detail = {
                "from": item["status"], "to": target,
                "escalation_required": escalation_required(
                    item["severity"], item["quantity"], item["threshold"]),
                "basis_version": basis_version,
            }
            update_basis: Optional[int] = None
            if target == "checked":
                latest = repo.latest_basis_version()
                if latest is None:
                    latest = repo._ensure_seed_observation_locked("system")
                if basis_version is None:
                    update_basis = latest
                elif basis_version < latest:
                    raise ConflictError(
                        "依据版本已过期，观测已更新，请基于最新读数重新提交复核")
            elif target == "authorized":
                latest = repo.latest_basis_version()
                if latest is None:
                    raise ConflictError("尚无任何水情观测依据，请先提交观测")
                if basis_version is not None and basis_version < latest:
                    raise ConflictError("依据版本已过期，不能依据旧观测授权")
                basis_version = basis_version or latest
                update_basis = basis_version
                # 总工授权：记下所依据观测与未关闭记录快照
                auth_snapshot = {
                    "basis_version": basis_version,
                    "observation": repo.get_observation(basis_version),
                    "open_records": repo.open_records(item_id),
                }
                audit_detail["auth_snapshot"] = auth_snapshot
            repo._transition_item_locked(
                item_id, target, expected_version, actor,
                update_basis, auth_snapshot, True, audit_detail)
            return 200, self.enrich(repo.get_item(item_id))

        if request_id is None:
            return self.repository.idempotent_call(None, "transition", actor, worker)
        return self.repository.idempotent_call(
            request_id, "transition", actor, worker)

    def recheck(self, item_id: int, payload: Dict[str, Any], actor: str,
                role: str) -> Dict[str, Any]:
        """观测更新导致旧复核失效（recheck_pending）后，值班员基于最新观测重新复核。"""
        ensure_role(role, RECHECK_ROLES)
        actor = require_text(actor, "actor", 100)
        expected_version = payload.get("expected_version")
        if not isinstance(expected_version, int) or expected_version < 1:
            from .domain import ValidationError
            raise ValidationError("expected_version必须是正整数")
        latest = self._require_basis()
        request_id = self._request_id(payload)

        def worker():
            self.repository._recheck_item_locked(
                item_id, expected_version, latest, actor)
            return 200, self.enrich(self.repository.get_item(item_id))

        if request_id is None:
            return self.repository.idempotent_call(None, "recheck", actor, worker)
        return self.repository.idempotent_call(
            request_id, "recheck", actor, worker)

    def supplement_basis(self, item_id: int, payload: Dict[str, Any], actor: str,
                         role: str) -> Dict[str, Any]:
        """历史指令缺依据版本（待补核）：值班员/总工补绑观测依据后回到原状态。"""
        ensure_role(role, SUPPLEMENT_ROLES)
        actor = require_text(actor, "actor", 100)
        basis_version = payload.get("basis_version")
        if not isinstance(basis_version, int) or basis_version < 1:
            from .domain import ValidationError
            raise ValidationError("basis_version必须是正整数")
        self.repository.get_observation(basis_version)
        request_id = self._request_id(payload)

        def worker():
            self.repository._supplement_basis_locked(item_id, basis_version, actor)
            return 200, self.enrich(self.repository.get_item(item_id))

        if request_id is None:
            return self.repository.idempotent_call(
                None, "supplement_basis", actor, worker)
        return self.repository.idempotent_call(
            request_id, "supplement_basis", actor, worker)

    def get_item(self, item_id: int, role: str) -> Dict[str, Any]:
        self._view(role)
        return self.enrich(self.repository.get_item(item_id))

    def list_items(self, role: str, status: Optional[str] = None) -> list:
        self._view(role)
        return [self.enrich(item) for item in self.repository.list_items(status)]

    def list_records(self, item_id: int, role: str) -> list:
        self._view(role)
        return self.repository.list_records(item_id)

    def audit(self, role: str, item_id: Optional[int] = None) -> list:
        ensure_role(role, AUDIT_ROLES)
        return self.repository.list_audit(item_id)

    @staticmethod
    def enrich(item: Dict[str, Any]) -> Dict[str, Any]:
        result = dict(item)
        if item.get("auth_snapshot"):
            import json
            try:
                result["auth_snapshot"] = json.loads(item["auth_snapshot"]) \
                    if isinstance(item["auth_snapshot"], str) else item["auth_snapshot"]
            except (TypeError, ValueError):
                pass
        result["priority"] = priority_score(
            item["severity"], item["quantity"], item["threshold"])
        result["deadline_hours"] = response_deadline_hours(
            item["severity"], item["quantity"], item["threshold"])
        result["escalation_required"] = escalation_required(
            item["severity"], item["quantity"], item["threshold"])
        result["pending_basis"] = item["status"] == SUP_STATE
        result["recheck_required"] = item["status"] == RECHECK_STATE
        return result
