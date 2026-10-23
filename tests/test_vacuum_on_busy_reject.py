# -*- coding: utf-8 -*-
"""VACUUM_ON 진행 중(또는 VACUUM_OFF 진행 중)에 들어온 VACUUM_ON — 바로 E321 거절, PLC 에 아무것도 쓰지 않는다.

예전: 거절 검사가 try 안에 있어 finally 의 실패 원복(_stop_roughing: L_R_V_SW OFF → 5초 → L_R_P_SW OFF)이
거절된 요청에서도 돌았다 → 진행 중인 펌프다운의 러핑이 꺼져 첫 요청도 E312("원인 미확정")로 실패,
거절 응답은 5초 늦게 나갔다. VACUUM_OFF 는 원래 try 밖이라 문제없었다.
자기 실패 때의 러핑 OFF 원복은 그대로다. 장치·네트워크 접근 없음(가짜 PLC).
"""
import os
import sys
import time
import asyncio
import contextlib

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

from host.context import HostContext     # noqa: E402
from host.handlers import HostHandlers   # noqa: E402

BITS = {
    "G_V_1_OPEN_LAMP": False, "G_V_1_CLOSE_LAMP": True,
    "G_V_2_OPEN_LAMP": False, "G_V_2_CLOSE_LAMP": True,
    "L_VAC_READY_SW": False, "L_VAC_NOT_READY": False, "LP_STEP1": False, "LP_STEP2": False,
    "L_R_P_SW": False, "L_R_V_SW": False, "L_VENT_SW": False, "L_R_P_OFF_TIMER": False,
    "L_R_V_인터락": True, "L_VENT_인터락": True, "L_ATM": True,
}
ROUGHING = ("L_R_P_SW", "L_R_V_SW")


class _PLC:
    def __init__(self, **over):
        self.bits = dict(BITS, **over)
        self.writes = []                       # (요청 이름, 비트, 값)
        self.who = "?"

    async def read_bit(self, name):
        await asyncio.sleep(0)
        if name not in self.bits:
            raise KeyError(name)
        return bool(self.bits[name])

    async def write_switch(self, name, on, **kw):
        await asyncio.sleep(0)
        self.writes.append((self.who, name, bool(on)))
        self.bits[name] = bool(on)


def _handlers(plc):
    ctx = HostContext(log=lambda t, m: None, plc=plc, ch1=None, ch2=None, pc=None, runtime_state=None,
                      lock_plc=asyncio.Lock(), lock_ch1=asyncio.Lock(), lock_ch2=asyncio.Lock(),
                      chat=None, popup=None)
    return HostHandlers(ctx)


async def _second(h, plc, coro_fn):
    """두 번째 요청 — 응답 시간과, 그 요청이 PLC 에 쓴 것만 돌려준다."""
    plc.who = "second"
    n0 = len(plc.writes)
    t0 = time.monotonic()
    res = await coro_fn({})
    dt = time.monotonic() - t0
    writes = [w for w in plc.writes[n0:] if w[0] == "second"]
    plc.who = "first"
    return res, dt, writes


def test_duplicate_vacuum_on_rejected_without_touching_roughing():
    async def go():
        plc = _PLC()
        h = _handlers(plc)
        plc.who = "first"
        first = asyncio.create_task(h.vacuum_on({"timeout_s": 60}))
        for _ in range(200):                                       # 러핑펌프·밸브 ON 까지(5초 대기 포함)
            await asyncio.sleep(0.05)
            if plc.bits["L_R_P_SW"] and plc.bits["L_R_V_SW"]:
                break
        assert plc.bits["L_R_P_SW"] and plc.bits["L_R_V_SW"]

        res, dt, writes = await _second(h, plc, h.vacuum_on)
        assert res["result"] == "fail" and res["error_code"] == "E321", res
        assert "VACUUM_ON 진행 중" in str(res.get("detail") or res.get("message")), res
        assert dt < 1.0, dt                                        # 예전: 5초(_stop_roughing 대기)
        assert writes == []                                        # 예전: L_R_V_SW·L_R_P_SW OFF
        assert plc.bits["L_R_P_SW"] and plc.bits["L_R_V_SW"]       # 펌프다운 계속

        # 래더 L_PUMPING 정상 완료 흉내 → 첫 요청은 성공
        plc.bits.update(L_R_V_SW=False, L_VAC_READY_SW=True, L_R_P_SW=False)
        r1 = await asyncio.wait_for(first, timeout=20)
        assert r1["result"] == "success", r1
    asyncio.run(go())


def test_vacuum_on_during_vacuum_off_rejected_without_writes():
    async def go():
        plc = _PLC(L_VAC_READY_SW=True, L_ATM=False)
        h = _handlers(plc)
        plc.who = "first"
        off = asyncio.create_task(h.vacuum_off({"timeout_s": 60}))
        for _ in range(300):                                       # 러핑 OFF(5초) 뒤 벤트 ON 까지
            await asyncio.sleep(0.05)
            if plc.bits["L_VENT_SW"]:
                break
        assert plc.bits["L_VENT_SW"]

        res, dt, writes = await _second(h, plc, h.vacuum_on)
        assert res["result"] == "fail" and res["error_code"] == "E321", res
        assert dt < 1.0 and writes == [], (dt, writes)

        plc.bits["L_ATM"] = True
        r = await asyncio.wait_for(off, timeout=20)
        assert r["result"] == "success", r
    asyncio.run(go())


def test_own_failure_still_turns_roughing_off():
    """(그대로) 자기 실패 — 러핑 중 게이트가 열리면 E301 + 러핑밸브 OFF → 5초 → 러핑펌프 OFF."""
    async def go():
        plc = _PLC()
        h = _handlers(plc)
        plc.who = "first"
        task = asyncio.create_task(h.vacuum_on({"timeout_s": 60}))
        for _ in range(200):
            await asyncio.sleep(0.05)
            if plc.bits["L_R_P_SW"] and plc.bits["L_R_V_SW"]:
                break
        plc.bits.update(G_V_1_OPEN_LAMP=True, G_V_1_CLOSE_LAMP=False)
        r = await asyncio.wait_for(task, timeout=30)
        assert r["result"] == "fail" and r["error_code"] == "E301", r
        offs = [(n, v) for _, n, v in plc.writes if n in ROUGHING and v is False]
        assert offs == [("L_R_V_SW", False), ("L_R_P_SW", False)], plc.writes
        assert not plc.bits["L_R_P_SW"] and not plc.bits["L_R_V_SW"]
    asyncio.run(go())
