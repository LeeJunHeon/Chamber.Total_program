"""챔버12 웹 설정 적용(CONFIG_APPLY·CONFIG_ROLLBACK) — 챔버K v2/chk/system.py ConfigApply 그대로(ConfigStore 와 실행 cfg 사전을 잇는다).

busy = 어느 챔버든 공정·정지 순서·플라즈마 클리닝·TSP·히터 운전 중 = system.busy()(엔진 하나라도 busy — 엔진이 busy 를 정한다)
  + 안전 정지 순서 진행 중(system.safety.stopping — 4단계, 있으면).
  busy 면 H 등급 변경이 하나라도 있으면 버전 전체 거부(CONFIG_H_IN_PROCESS, S48), idle 키는 대기했다가 busy 가 풀린 첫 1초 행에서 적용.
설치·비밀 키(ConfigStore 규칙): 비밀값은 웹에서 받지 않고(CONFIG_SECRET_FROM_WEB), RUN_MODE·EQUIPMENT_ID·UPLINK_IN_SIM 은 저장 값과
  다르면 거부(CONFIG_LOCKED_KEY — 덮어쓰기에 없어도), local_override 가 정한 키는 웹 값보다 덮어쓰기가 이긴다(overriddenKeys).
  install 키는 restart 대기. CONFIG_ROLLBACK version = 웹이 정한 새 버전 번호(없으면 지금 + 1).
결과는 config_ack 메시지(on_ack — 허브). 명령 처리(core, 4단계)는 CONFIG_APPLY → system.config.apply(args, by),
CONFIG_ROLLBACK → system.config.rollback(args, by) 를 부르고 돌려받은 사유 코드(None = 받아들임)로 cmd_result 를 정한다.

잠금: 챔버12 는 공정 잠금이 챔버마다라 하나가 없다 — 값 바꾸기는 이 객체의 잠금(또는 system.cfg_lock 이 있으면 그것) 아래에서
사전 항목을 하나씩 바꾼다(모든 구성 요소가 같은 사전을 본다 — 복사하지 않음).
붙이기: attach(system) — system.config = ConfigApply, 1초 행 구독자(허브 state 보다 앞)에 tick(idle 키 적용).
"""
from __future__ import annotations

import threading
from typing import Any, Callable, Dict, Optional

from v2.common.config_store import ApplyResult, ConfigStore


def _is_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


class ConfigApply:
    def __init__(self, store: ConfigStore, cfg: Dict[str, Any], busy: Callable[[], bool], lock=None,
                 on_ack: Optional[Callable[[dict], None]] = None) -> None:
        self.store, self.cfg, self.busy = store, cfg, busy
        self.lock = lock if lock is not None else threading.RLock()
        self.on_ack = on_ack or (lambda d: None)
        self.acks = 0

    def _push(self, keys) -> None:
        with self.lock:                                   # 모든 구성 요소가 같은 사전을 본다
            for k in keys:
                self.cfg[k] = self.store.values[k]

    def _ack(self, res: ApplyResult, by: str) -> dict:
        d = {"version": res.version, "status": res.status, "reasonCode": res.reason, "errors": list(res.errors),
             "appliedKeys": list(res.applied_keys), "pendingKeys": list(res.pending_keys),
             "overriddenKeys": list(res.overridden_keys), "kind": res.kind, "by": by, "changes": list(res.changes),
             "values": res.values}
        self.acks += 1
        self.on_ack(d)
        return d

    def apply(self, a: dict, by: str) -> Optional[str]:
        """CONFIG_APPLY {version, values, schemaHash} → 사유 코드(None = 적용·대기). 결과는 config_ack 로."""
        a = a if isinstance(a, dict) else {}
        v, vals, sh = a.get("version"), a.get("values"), a.get("schemaHash")
        if not _is_int(v) or not isinstance(vals, dict) or not isinstance(sh, str):
            return "BAD_ARGS"
        res = self.store.apply(v, vals, sh, bool(self.busy()), by=by)
        if res.status != "rejected":
            self._push(res.applied_keys)
        self._ack(res, by)
        return res.reason

    def rollback(self, a: dict, by: str) -> Optional[str]:
        """CONFIG_ROLLBACK {version?} → 사유 코드(None = 적용·대기)."""
        v = (a if isinstance(a, dict) else {}).get("version")
        if v is not None and not _is_int(v):
            return "BAD_ARGS"
        res = self.store.rollback(bool(self.busy()), by=by, version=v)
        if res.status != "rejected":
            self._push(res.applied_keys)
        self._ack(res, by)
        return res.reason

    def tick(self, row: Any = None) -> None:
        """1초 행마다 — idle 이 되면 대기 중 idle 키 적용(ConfigStore.on_idle) → config_ack(kind idle)."""
        if not self.store.pending_idle or self.busy():
            return
        keys = self.store.on_idle()
        if keys:
            self._push(keys)
            self.acks += 1
            self.on_ack({"version": self.store.version, "status": "applied", "reasonCode": None, "errors": [],
                         "appliedKeys": keys, "pendingKeys": sorted(self.store.pending_restart), "overriddenKeys": [],
                         "kind": "idle", "by": "system", "changes": [], "values": None})


def system_busy(system) -> bool:
    """설정 적용의 busy — 엔진 busy(공정·정지 순서·PC·TSP·히터) 또는 안전 정지 순서 중."""
    b = bool(system.busy()) if hasattr(system, "busy") else False
    return b or bool(getattr(getattr(system, "safety", None), "stopping", None))


def attach(system) -> Optional[ConfigApply]:
    """조립에 설정 적용을 붙인다 — 저장소가 없으면(시험) None. on_ack 는 허브(있으면) config_ack."""
    store = getattr(system, "store", None)
    if store is None:
        return None

    def on_ack(d: dict) -> None:
        hub = getattr(system, "hub", None)
        if hub is not None:
            hub.config_ack(d)

    ca = ConfigApply(store, system.cfg, lambda: system_busy(system), getattr(system, "cfg_lock", None), on_ack)
    system.config = ca
    hub = getattr(system, "hub", None)
    lst = system.row_listeners
    at = lst.index(hub.on_row) if hub is not None and hub.on_row in lst else len(lst)
    lst.insert(at, ca.tick)                               # 허브 state 보다 앞(이 행의 configPending 이 맞게)
    return ca
