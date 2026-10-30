"""챔버12 설정 적용(config_apply.py — 챔버K system.ConfigApply 그대로) — 실제 ConfigStore(tmp 폴더) + 같은 cfg 사전.

- live 키는 바로 같은 사전에(config_ack applied), install 키는 restart 대기, 비밀값·잠금 키·스키마 해시·버전·형식 거부.
- busy(어느 엔진이든 busy·안전 정지 순서 중)면 H 등급 변경 거부(CONFIG_H_IN_PROCESS), idle 키는 대기 → busy 풀린 첫 행에서 적용.
- 덮어쓰기(local_override)가 정한 키는 덮어쓰기가 이긴다(overriddenKeys). 되돌리기(CONFIG_ROLLBACK version).
- attach: system.config·1초 행 구독(허브 state 보다 앞 — 같은 행의 configPending 이 맞다).
"""
import json

import pytest

from v2.common.clock import VirtualClock
from v2.ch12 import config_apply as CA
from v2.ch12 import hub as H
from v2.ch12.params import PARAMS
from v2.ch12.sim.rig import Ch12Rig
from v2.ch12.system import new_store, param_schema_hash

SECRETS = {p.key for p in PARAMS if p.scope.value == "secret"}
LIVE, IDLE, HKEY, INSTALL = "UPLINK_CLOCK_SKEW_SEC", "SHUTDOWN_STEP_GAP_MS", "CHAMBER_GATE_RECHECK_COUNT", "LOCAL_HMI_PORT"


class Box:
    def __init__(self, tmp_path, override=None):
        d = tmp_path / "config"
        d.mkdir(parents=True, exist_ok=True)
        if override:
            (d / "local_override.json").write_text(json.dumps({"values": override}), encoding="utf-8")
        self.store = new_store(d)
        self.cfg = dict(self.store.load())
        self.is_busy = False
        self.acks = []
        self.ca = CA.ConfigApply(self.store, self.cfg, lambda: self.is_busy, on_ack=self.acks.append)

    def vals(self, **change):
        v = {k: x for k, x in self.store.stored().items() if k not in SECRETS}
        v.update(change)
        return v

    def apply(self, version, by="admin@web", **change):
        return self.ca.apply({"version": version, "values": self.vals(**change), "schemaHash": param_schema_hash()}, by)


def test_live_idle_install_and_ack(tmp_path):
    b = Box(tmp_path)
    assert b.apply(1, **{LIVE: 7.0}) is None
    a = b.acks[-1]
    assert (a["status"], a["kind"], a["by"], a["appliedKeys"], a["version"]) == ("applied", "apply", "admin@web", [LIVE], 1)
    assert b.cfg[LIVE] == 7.0 and a["changes"] == [{"key": LIVE, "old": 5.0, "new": 7.0}]
    assert b.apply(2, **{IDLE: 900}) is None and b.cfg[IDLE] == 900                 # idle 키도 busy 아니면 바로
    assert b.apply(3, **{INSTALL: 8800}) is None
    a = b.acks[-1]
    assert a["status"] == "pending_restart" and a["pendingKeys"] == [INSTALL] and b.cfg[INSTALL] == 8712
    assert set(a) == {"version", "status", "reasonCode", "errors", "appliedKeys", "pendingKeys", "overriddenKeys", "kind",
                      "by", "changes", "values"}
    assert json.loads((tmp_path / "config" / "config_active.json").read_text(encoding="utf-8"))["version"] == 3


def test_busy_rejects_h_and_defers_idle(tmp_path):
    b = Box(tmp_path)
    b.is_busy = True
    assert b.apply(1, **{HKEY: 6}) == "CONFIG_H_IN_PROCESS" and b.cfg[HKEY] == 5
    assert b.acks[-1]["status"] == "rejected" and b.store.version == 0               # 버전 전체 거부(저장 안 함)
    assert b.apply(1, **{IDLE: 900, LIVE: 6.0}) is None
    a = b.acks[-1]
    assert a["status"] == "pending_idle" and a["appliedKeys"] == [LIVE] and a["pendingKeys"] == [IDLE]
    assert b.cfg[LIVE] == 6.0 and b.cfg[IDLE] == 500
    b.ca.tick()                                                                    # 아직 busy
    assert b.cfg[IDLE] == 500 and len(b.acks) == 2
    b.is_busy = False
    b.ca.tick({"seq": 1})
    a = b.acks[-1]
    assert (a["kind"], a["status"], a["appliedKeys"], a["by"]) == ("idle", "applied", [IDLE], "system") and b.cfg[IDLE] == 900
    b.ca.tick()
    assert len(b.acks) == 3                                                        # 한 번만


def test_rejections_secret_locked_hash_version_args(tmp_path):
    b = Box(tmp_path)
    assert b.ca.apply({"version": 1, "values": dict(b.vals(), UPLINK_TOKEN="x"), "schemaHash": param_schema_hash()},
                      "a") == "CONFIG_SECRET_FROM_WEB"
    assert b.apply(1, RUN_MODE="prod") == "CONFIG_LOCKED_KEY"
    assert b.apply(1, EQUIPMENT_ID="CH12-SIM") == "CONFIG_LOCKED_KEY"
    assert b.ca.apply({"version": 1, "values": b.vals(), "schemaHash": "0" * 64}, "a") == "CONFIG_HASH_MISMATCH"
    assert b.apply(1) is None and b.apply(1) == "CONFIG_VERSION_STALE"
    assert b.apply(2, **{LIVE: 1000.0}) == "CONFIG_INVALID"
    assert [a["reasonCode"] for a in b.acks] == ["CONFIG_SECRET_FROM_WEB", "CONFIG_LOCKED_KEY", "CONFIG_LOCKED_KEY",
                                                 "CONFIG_HASH_MISMATCH", None, "CONFIG_VERSION_STALE", "CONFIG_INVALID"]
    n = len(b.acks)
    for bad in ({}, {"version": True, "values": {}, "schemaHash": "x"}, {"version": 2, "values": [], "schemaHash": "x"},
                {"version": 2, "values": {}}, None):
        assert b.ca.apply(bad, "a") == "BAD_ARGS"
    assert b.ca.rollback({"version": "3"}, "a") == "BAD_ARGS" and len(b.acks) == n   # 형식 오류는 config_ack 없음


def test_override_wins_and_rollback(tmp_path):
    b = Box(tmp_path, override={"LOCAL_HMI_STATIC_DIR": "D:/hmi", "EQUIPMENT_ID": "CH12-SIM"})
    assert b.cfg["LOCAL_HMI_STATIC_DIR"] == "D:/hmi" and b.cfg["EQUIPMENT_ID"] == "CH12-SIM"
    assert b.apply(1, LOCAL_HMI_STATIC_DIR="E:/web") is None
    a = b.acks[-1]
    assert a["overriddenKeys"] == ["EQUIPMENT_ID", "LOCAL_HMI_STATIC_DIR"] and b.cfg["LOCAL_HMI_STATIC_DIR"] == "D:/hmi"   # 덮어쓰기가 이긴다
    assert b.ca.apply({"version": 2, "values": b.vals(EQUIPMENT_ID="CH12-SIM"), "schemaHash": param_schema_hash()},
                      "a") is None                                                   # 덮어쓰기 값(실행 값)은 받아 줌
    assert b.store.stored()["EQUIPMENT_ID"] == "CH12"                                # 저장은 저장 값 그대로
    assert b.apply(3, **{IDLE: 800}) is None and b.cfg[IDLE] == 800
    assert b.ca.rollback({"version": 2}, "r@web") == "CONFIG_VERSION_STALE"
    assert b.ca.rollback({"version": 9}, "r@web") is None
    a = b.acks[-1]
    assert (a["kind"], a["version"], a["by"], a["values"][IDLE]) == ("rollback", 9, "r@web", 500) and b.cfg[IDLE] == 500
    assert b.ca.rollback({}, "r@web") is None and b.store.version == 10


@pytest.fixture
def rig(tmp_path):
    r = Ch12Rig(VirtualClock(start_wall=1_790_000_000.0), tmp_path)
    yield r
    r.outputs.stop()


def test_attach_busy_from_engines_and_safety_and_row_order(rig):
    hub = H.attach(rig)
    ca = CA.attach(rig)
    assert rig.config is ca and rig.row_listeners.index(ca.tick) == rig.row_listeners.index(hub.on_row) - 1
    assert ca.cfg is rig.cfg                                                          # 같은 사전(복사 아님)

    class Eng:
        busy = False
    e = Eng()
    rig.register_engine("ch1", e)
    assert not CA.system_busy(rig)
    e.busy = True
    assert CA.system_busy(rig)
    e.busy = False

    class Safety:
        stopping = "stop"
    rig.safety = Safety()
    assert CA.system_busy(rig)
    rig.safety = None
    e.busy = True
    vals = {k: x for k, x in rig.store.stored().items() if k not in SECRETS}
    assert ca.apply({"version": 1, "values": dict(vals, **{IDLE: 900}), "schemaHash": param_schema_hash()}, "w") is None
    acks = [x["data"] for x in hub.recent if x["type"] == "config_ack"]
    assert acks[-1]["status"] == "pending_idle"
    rig._on_row({"seq": 1})
    assert hub.last_state["data"]["configPending"] == {"idle": {IDLE: 900}, "restart": {}} and rig.cfg[IDLE] == 500
    e.busy = False
    rig._on_row({"seq": 2})                                                           # tick → 적용 → 같은 행 state
    acks = [x["data"] for x in hub.recent if x["type"] == "config_ack"]
    assert acks[-1]["kind"] == "idle" and rig.cfg[IDLE] == 900 and hub.last_state["data"]["configPending"] is None


def test_attach_without_store_or_hub(tmp_path):
    class S:
        store = None
    assert CA.attach(S()) is None
    r = Ch12Rig(VirtualClock(start_wall=1_790_000_000.0), tmp_path)
    try:
        ca = CA.attach(r)                                                             # 허브 없이 — 끝에, ack 는 버림
        assert r.row_listeners[-1] == ca.tick
        vals = {k: x for k, x in r.store.stored().items() if k not in SECRETS}
        assert ca.apply({"version": 1, "values": vals, "schemaHash": param_schema_hash()}, "w") is None and ca.acks == 1
    finally:
        r.outputs.stop()
