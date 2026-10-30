"""설정 저장소 (장비 공통) — 웹이 값의 원본, 장비는 마지막으로 받은 값을 로컬에 보관한다.

파일(설정 폴더 안)
- config_active.json : {"version", "schemaHash", "savedAt", "values"(비밀값 제외), "contentHash"}
- config_prev.json   : 직전 버전(같은 형식)
- local_override.json: {"values": {...}} — 노트북 비상용 덮어쓰기. scope 가 install·secret 인 키만.
                       비밀값(웹 주소·토큰)은 여기에만 둔다.
  읽기는 utf-8-sig — 사람이 메모장·PowerShell 5(Set-Content -Encoding UTF8)로 저장해 BOM 이 붙어도 받는다(5-14). 쓰기는 BOM 없이.

읽기(load)
- active 를 검사(파일 해시·ParamDef 타입·범위·키 사이 규칙). 실패하면 prev. 둘 다 실패면 ConfigError — 시작 거부.
  기본값으로 조용히 돌지 않는다. 단, 두 파일이 모두 없으면(새 설치) 기본값 버전 0 — RUN_MODE 기본값이 sim 이다.
- 코드의 파라미터 정의가 바뀌어 schemaHash 가 다르면: 새 키는 기본값으로 채우고 없어진 키는 버리며 notes 에 남긴다.
- 덮어쓰기 파일이 깨졌거나 허용되지 않는 키가 있으면 ConfigError.
- 저장 값(stored) = config_active 의 값(없으면 기본값), 실행 값(values) = 저장 값 + 덮어쓰기(+ 대기 중 키는 이전 값).
  웹은 저장 값을 원본으로 두고 CONFIG_APPLY 에 저장 값을 보낸다(묶음 params[].stored·overridden).

적용(apply) — CONFIG_APPLY
- 스키마 해시·버전·비밀값 포함 여부 → 전체 검사 → 하나라도 실패면 버전 전체 거부(부분 적용 없음).
- 웹 잠금 키(RUN_MODE·EQUIPMENT_ID …)는 받은 값이 저장 값과 같아야 한다. 덮어쓰기가 정한 잠금 키는 실행 값(덮어쓰기 값)도
  받아 주되 저장은 저장 값 그대로(예전 웹이 묶음 current 를 보내던 것) — 그 밖의 값은 CONFIG_LOCKED_KEY.
- 공정·히터 운전 중(busy)이면 H 등급 변경이 하나라도 있으면 거부(CONFIG_H_IN_PROCESS, 안전 규칙 S48).
- 통과하면 파일에 원자적으로 저장(임시 파일 → fsync → 바꿔치기, 이전 active 는 prev 로).
- 적용 시점: live 는 즉시, idle 은 busy 면 대기(on_idle 에서 적용), restart 는 재시작까지 대기.
  즉시 적용분만 반영한 실행 값이 키 사이 규칙을 어기면 그 live 키도 idle 대기로 미룬다.

이력·되돌리기(사용자 지시 5-4 ④)
- config_history.jsonl : 적용 요청마다 한 줄 {at, by, kind(apply·rollback), version, status, reason, changes[{key, old, new}]}
  (old = 그때까지 저장된 웹 값, 비밀값은 원래 저장하지 않으므로 이력에도 없다). 거부된 요청도 사유와 함께 남긴다.
- rollback(busy, by, version): config_prev.json(직전 버전)의 값을 새 버전으로 다시 적용 — 같은 검사·S48·적용 시점 규칙.
  새 버전 번호는 웹이 정한다(version — 지금 버전보다 커야 함, 아니면 CONFIG_VERSION_STALE). 없으면 지금 버전 + 1.
  웹은 config_ack(kind=rollback, values)로 받아 자기 DB 를 맞춘다(값의 원본은 계속 웹). 이전 버전이 없으면 CONFIG_NO_PREVIOUS.
"""
from __future__ import annotations

import datetime as _dt
import hashlib
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from v2.common.paramdef import Apply, CrossRule, ParamDef, Safety, Scope, as_float, check_config, check_value, defaults

ACTIVE = "config_active.json"
PREV = "config_prev.json"
OVERRIDE = "local_override.json"
HISTORY = "config_history.jsonl"


class ConfigError(RuntimeError):
    """설정을 쓸 수 없음 — 시작 거부."""


@dataclass
class ApplyResult:
    status: str                      # applied / pending_idle / pending_restart / rejected
    reason: Optional[str] = None     # 사유 코드(rejected 일 때)
    errors: list = field(default_factory=list)
    applied_keys: list = field(default_factory=list)
    pending_keys: list = field(default_factory=list)
    overridden_keys: list = field(default_factory=list)   # 노트북 덮어쓰기에 가려진 키
    version: Optional[int] = None                          # 이 요청의 버전(되돌리기는 새로 매긴 버전)
    kind: str = "apply"                                    # apply / rollback
    values: Optional[dict] = None                          # 되돌리기: 새 버전의 값 전체(웹이 DB 를 맞춘다)
    changes: list = field(default_factory=list)            # [{key, old, new}]


def _content_hash(version: int, schema_hash: str, values: Mapping[str, Any]) -> str:
    raw = json.dumps({"version": version, "schemaHash": schema_hash, "values": values},
                     ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class ConfigStore:
    def __init__(self, directory: Path, defs: Sequence[ParamDef], rules: Sequence[CrossRule],
                 schema_hash: str) -> None:
        self.dir = Path(directory)
        self.defs = tuple(defs)
        self.rules = tuple(rules)
        self.schema_hash = schema_hash
        self.by_key = {p.key: p for p in self.defs}
        self.secret_keys = {p.key for p in self.defs if p.scope is Scope.SECRET}
        self.version = 0                     # 저장된(웹에서 받은) 최신 버전
        self.persisted: dict = {}            # 저장된 값(비밀값 제외)
        self.override: dict = {}
        self.values: dict = {}               # 지금 실행에 쓰는 값(대기 중 키는 이전 값)
        self.pending_idle: dict = {}
        self.pending_restart: dict = {}
        self.source = ""
        self.notes: list = []

    # ───────── 파일 ─────────
    @property
    def active_path(self) -> Path:
        return self.dir / ACTIVE

    @property
    def prev_path(self) -> Path:
        return self.dir / PREV

    @property
    def override_path(self) -> Path:
        return self.dir / OVERRIDE

    @property
    def history_path(self) -> Path:
        return self.dir / HISTORY

    def _read_override(self) -> dict:
        if not self.override_path.exists():
            return {}
        try:
            obj = json.loads(self.override_path.read_text(encoding="utf-8-sig"))
            vals = obj["values"]
            if not isinstance(vals, dict):
                raise ValueError("values 가 객체가 아님")
        except Exception as e:
            raise ConfigError(f"{OVERRIDE} 를 읽을 수 없음: {e!r}") from e
        errs = []
        for k, v in vals.items():
            p = self.by_key.get(k)
            if p is None:
                errs.append(f"{k}: 정의되지 않은 키")
            elif p.scope not in (Scope.INSTALL, Scope.SECRET):
                errs.append(f"{k}: 덮어쓰기는 install·secret 키만")
            else:
                e = check_value(p, v)
                if e:
                    errs.append(e)
        if errs:
            raise ConfigError(f"{OVERRIDE} 오류: {errs}")
        return as_float(self.defs, vals)

    def _effective(self, persisted: Mapping[str, Any]) -> dict:
        base = defaults(self.defs)
        eff = {k: base[k] for k in self.secret_keys}
        eff.update({k: v for k, v in persisted.items() if k not in self.secret_keys})
        eff.update(self.override)
        return eff

    def stored(self) -> dict:
        """저장 값(config_active 의 값, 없는 키는 기본값) — 비밀값 제외."""
        base = defaults(self.defs)
        return {p.key: self.persisted.get(p.key, base[p.key]) for p in self.defs if p.key not in self.secret_keys}

    def overridden(self) -> list:
        """덮어쓰기(local_override.json)가 정한 키 — 비밀값 제외."""
        return sorted(k for k in self.override if k not in self.secret_keys)

    def _read_file(self, path: Path) -> tuple:
        """(version, persisted 값, notes). 실패하면 예외."""
        obj = json.loads(path.read_text(encoding="utf-8-sig"))
        version, sh, vals, ch = obj["version"], obj["schemaHash"], obj["values"], obj["contentHash"]
        if not isinstance(version, int) or not isinstance(vals, dict):
            raise ValueError("형식 오류")
        if _content_hash(version, sh, vals) != ch:
            raise ValueError("파일 해시 불일치(손상)")
        vals = as_float(self.defs, vals)                # 정수로 저장된 float 키(예전 파일·웹 JSON 700)도 float
        notes = []
        if sh != self.schema_hash:
            base = defaults(self.defs)
            known = {p.key for p in self.defs if p.scope is not Scope.SECRET}
            added = sorted(known - set(vals))
            dropped = sorted(set(vals) - known)
            vals = {k: (vals[k] if k in vals else base[k]) for k in known}
            notes.append(f"{path.name}: 파라미터 정의가 바뀜 — 새 키 기본값 {added}, 없어진 키 버림 {dropped}")
        errs = check_config(self.defs, self.rules, self._effective(vals))
        if errs:
            raise ValueError(f"검사 실패: {errs[:5]}")
        return version, dict(vals), notes

    # ───────── 읽기 ─────────
    def load(self) -> dict:
        self.override = self._read_override()
        self.notes = []
        tried = []
        for name, path in (("active", self.active_path), ("prev", self.prev_path)):
            if not path.exists():
                tried.append(f"{path.name} 없음")
                continue
            try:
                version, vals, notes = self._read_file(path)
            except Exception as e:
                tried.append(f"{path.name} 사용 불가: {e}")
                continue
            self.version, self.persisted, self.source = version, vals, name
            self.notes = tried + notes
            break
        else:
            if self.active_path.exists() or self.prev_path.exists():
                raise ConfigError("설정 파일을 쓸 수 없음 — 시작 거부: " + "; ".join(tried))
            base = defaults(self.defs)
            self.version, self.persisted, self.source = 0, {k: v for k, v in base.items() if k not in self.secret_keys}, "defaults"
            self.notes = ["설정 파일 없음 — 기본값(버전 0)"]
        eff = self._effective(self.persisted)
        errs = check_config(self.defs, self.rules, eff)
        if errs:   # 덮어쓰기 때문에 깨진 경우
            raise ConfigError(f"덮어쓰기 적용 뒤 검사 실패 — 시작 거부: {errs[:5]}")
        self.values = eff
        self.pending_idle, self.pending_restart = {}, {}
        return dict(self.values)

    # ───────── 적용 ─────────
    def apply(self, version: int, values: Mapping[str, Any], schema_hash: str, busy: bool, by: str = "",
              kind: str = "apply") -> ApplyResult:
        """CONFIG_APPLY — 결과와 함께 이력 한 줄(누가·언제·이전 → 새 값). float 형식 키는 float 로 바꿔 검사·저장."""
        values = as_float(self.defs, values) if isinstance(values, Mapping) else values
        if isinstance(values, Mapping):
            values = self._keep_locked(values)
        before = dict(self.persisted)
        res = self._apply(version, values, schema_hash, busy)
        keys = sorted((set(before) | set(values)) - self.secret_keys)
        res.version, res.kind = version, kind
        res.changes = [{"key": k, "old": before.get(k), "new": values.get(k)} for k in keys if before.get(k) != values.get(k)]
        self._log({"at": _dt.datetime.now().astimezone().isoformat(timespec="seconds"), "by": by, "kind": kind,
                   "version": version, "status": res.status, "reason": res.reason, "changes": res.changes})
        return res

    def _keep_locked(self, values: Mapping[str, Any]) -> dict:
        """덮어쓰기가 정한 웹 잠금 키에 실행 값(덮어쓰기 값)이 왔으면 저장 값으로 — 덮어쓰기 값이 config_active 에 들어가지 않게."""
        out, st = dict(values), self.stored()
        for k in self.override:
            p = self.by_key.get(k)
            if p is not None and p.web_locked and k in out and out[k] != st.get(k) and out[k] == self.values.get(k):
                out[k] = st.get(k)
        return out

    def rollback(self, busy: bool, by: str = "", version: Optional[int] = None) -> ApplyResult:
        """이전 버전으로 되돌리기 — config_prev.json 의 값을 새 버전으로 적용(검사·S48·적용 시점은 CONFIG_APPLY 와 같다).
        version = 웹이 정한 새 버전 번호(지금 버전보다 커야 함 — 아니면 CONFIG_VERSION_STALE). 없으면 지금 + 1."""
        if version is not None and (not isinstance(version, int) or isinstance(version, bool) or version <= self.version):
            res = ApplyResult("rejected", "CONFIG_VERSION_STALE", [f"버전 {version} ≤ 저장된 {self.version}"],
                              version=version, kind="rollback")
            self._log({"at": _dt.datetime.now().astimezone().isoformat(timespec="seconds"), "by": by, "kind": "rollback",
                       "version": version, "status": res.status, "reason": res.reason, "changes": []})
            return res
        if not self.prev_path.exists():
            return ApplyResult("rejected", "CONFIG_NO_PREVIOUS", ["이전 버전 없음"], kind="rollback")
        try:
            prev_version, vals, _ = self._read_file(self.prev_path)
        except Exception as e:
            return ApplyResult("rejected", "CONFIG_NO_PREVIOUS", [f"이전 버전을 쓸 수 없음: {e}"], kind="rollback")
        res = self.apply(self.version + 1 if version is None else version, vals, self.schema_hash, busy, by=by,
                         kind=f"rollback:{prev_version}")
        res.kind = "rollback"
        if res.status != "rejected":
            res.values = dict(vals)
        return res

    def history(self, limit: int = 100) -> list:
        """최근 이력(새 것이 뒤). 깨진 줄은 건너뛴다."""
        if not self.history_path.exists():
            return []
        out = []
        for line in self.history_path.read_text(encoding="utf-8").splitlines():
            try:
                out.append(json.loads(line))
            except ValueError:
                continue
        return out[-limit:]

    def _log(self, entry: dict) -> None:
        try:
            self.dir.mkdir(parents=True, exist_ok=True)
            with open(self.history_path, "a", encoding="utf-8", newline="\n") as f:
                f.write(json.dumps(entry, ensure_ascii=False, sort_keys=True) + "\n")
                f.flush()
                os.fsync(f.fileno())
        except OSError:
            pass                                   # 이력 실패가 적용을 막지 않는다(적용 결과는 config_ack 로 웹에)

    def _apply(self, version: int, values: Mapping[str, Any], schema_hash: str, busy: bool) -> ApplyResult:
        if schema_hash != self.schema_hash:
            return ApplyResult("rejected", "CONFIG_HASH_MISMATCH", ["스키마 해시 불일치"])
        if not isinstance(version, int) or isinstance(version, bool) or version <= self.version:
            return ApplyResult("rejected", "CONFIG_VERSION_STALE", [f"버전 {version} ≤ 저장된 {self.version}"])
        secrets = sorted(set(values) & self.secret_keys)
        if secrets:
            return ApplyResult("rejected", "CONFIG_SECRET_FROM_WEB", [f"비밀값 포함: {secrets}"])
        cand = self._effective(values)
        st = self.stored()
        locked = [f"{k}: 저장 {st.get(k)!r} ≠ 받은 {values.get(k, cand.get(k))!r}" for k in sorted(self.by_key)
                  if self.by_key[k].web_locked and st.get(k) != values.get(k, cand.get(k))]
        if locked:                                  # 실행 모드·장비 ID — 덮어쓰기 파일에 없어도 웹에서는 못 바꾼다
            return ApplyResult("rejected", "CONFIG_LOCKED_KEY", [f"웹에서 바꿀 수 없는 키: {'; '.join(locked)}"])
        errs = check_config(self.defs, self.rules, cand)
        if errs:
            return ApplyResult("rejected", "CONFIG_INVALID", errs)
        changed = sorted(k for k in cand if cand[k] != self.values.get(k))
        if busy:
            hs = [k for k in changed if self.by_key[k].safety is Safety.H]
            if hs:
                return ApplyResult("rejected", "CONFIG_H_IN_PROCESS", [f"공정 중 H 등급 변경: {hs}"])

        self.dir.mkdir(parents=True, exist_ok=True)          # 새 설치(설정 폴더 없음)에서 첫 적용
        atomic_write_json_pair(self.active_path, self.prev_path, {
            "version": version, "schemaHash": schema_hash,
            "savedAt": _dt.datetime.now().astimezone().isoformat(timespec="seconds"),
            "values": dict(values), "contentHash": _content_hash(version, schema_hash, dict(values)),
        })
        self.version, self.persisted = version, dict(values)

        overridden = sorted(k for k in values if k in self.override and values[k] != self.override[k])
        now_keys, idle_keys, restart_keys = [], [], []
        for k in changed:
            a = self.by_key[k].apply
            if a is Apply.RESTART:
                restart_keys.append(k)
            elif a is Apply.IDLE and busy:
                idle_keys.append(k)
            else:
                now_keys.append(k)
        trial = dict(self.values)
        trial.update({k: cand[k] for k in now_keys})
        if check_config(self.defs, self.rules, trial):     # 일부만 바꾼 값이 규칙을 어기면 모두 대기로
            idle_keys += now_keys
            now_keys = []
            trial = dict(self.values)
        self.values = trial
        self.pending_idle = {k: cand[k] for k in idle_keys}
        self.pending_restart = {k: cand[k] for k in restart_keys}
        status = "pending_restart" if restart_keys else ("pending_idle" if idle_keys else "applied")
        return ApplyResult(status, None, [], now_keys, sorted(idle_keys + restart_keys), overridden)

    def on_idle(self) -> list:
        """공정·히터가 멈췄을 때 대기 중 idle 키 적용. 적용한 키 목록."""
        if not self.pending_idle:
            return []
        trial = dict(self.values)
        trial.update(self.pending_idle)
        if check_config(self.defs, self.rules, trial):
            return []      # restart 대기 키와 함께여야 규칙을 만족 — 재시작까지 둔다
        keys = sorted(self.pending_idle)
        self.values = trial
        self.pending_idle = {}
        return keys

    def pending_summary(self) -> dict:
        return {"idle": sorted(self.pending_idle), "restart": sorted(self.pending_restart)}


def atomic_write_json_pair(active: Path, prev: Path, obj: Any) -> None:
    """새 active 를 임시 파일에 쓴 뒤, 지금 active 를 prev 로, 임시 파일을 active 로 바꾼다.
    중간에 멈추면 active 가 없고 prev 가 남는다 — load 는 prev 를 쓴다."""
    tmp = active.with_name(active.name + ".tmp")
    data = json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=1)
    with open(tmp, "w", encoding="utf-8", newline="\n") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    if active.exists():
        os.replace(active, prev)
    os.replace(tmp, active)
