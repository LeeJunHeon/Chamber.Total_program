# -*- coding: utf-8 -*-
"""공유 MFC(MFC1 = CH1 챔버 Ar/O2 + CH2 Plasma Cleaning N2) 동시 시작/동시 종료 최악 경우 검증.

실제 AsyncMFC 를 쓰고 TCP 대신 '가짜 장비'(쓰기 기록 + R60/R5 응답)를 붙인다. 네트워크 없음.
수정 전 재현된 문제:
 · PC 종료 정리가 CH1 의 대기 명령을 폐기하고 CH1 의 FLOW_ON 안정화를 통지 없이 끊음(CH1 무한 대기)
 · 동시 종료 시 CH1 FLOW_OFF 가 폐기됐는데도 '확인'으로 처리
 · 안정화 슬롯 1개 → 한쪽 FLOW_ON 이 다른 쪽 안정화를 끊고, 확인/실패 이벤트가 양쪽에 다 전달됨
"""
import os
import re
import sys
import asyncio
import contextlib
from types import SimpleNamespace

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                        # noqa: E402
import device.mfc as MFCMOD                                          # noqa: E402
from device.mfc import AsyncMFC, MFCEvent                            # noqa: E402
from controller.runtime_state import runtime_state, RuntimeState     # noqa: E402
from runtime.chamber_runtime import ChamberRuntime                   # noqa: E402
from runtime.plasma_cleaning_runtime import PlasmaCleaningRuntime    # noqa: E402

FAST = SimpleNamespace(
    MFC_GAP_MS=5, MFC_DELAY_MS=20, MFC_DELAY_MS_VALVE=30, MFC_STABILIZATION_INTERVAL_MS=10,
    MFC_TIMEOUT=300, MFC_ALLOW_NO_REPLY_DRAIN_MS=1, MFC_POST_OPEN_QUIET_MS=0, MFC_ZEROING_GAP_MS=5,
    MFC_POLLING_INTERVAL_MS=50, MFC_SCALE_FACTORS={1: 1.0, 2: 1.0, 3: 1.0},
)


class FakeDevice:
    """MFC 장비 흉내: 쓴 명령 기록, L{ch}1/0 → ON/OFF, Q{ch} v → 설정값, R60/R5 응답."""

    def __init__(self, m: AsyncMFC):
        self.m = m
        self.sent: list[str] = []
        self.setp = {1: 0.0, 2: 0.0, 3: 0.0}
        self.on = {1: False, 2: False, 3: False}
        self.reach = {1: True, 2: True, 3: True}      # False 면 그 채널 유량이 0 에 머문다(안정화 미도달)

    def write(self, payload: bytes):
        line = payload.decode("ascii", "ignore").strip()
        self.sent.append(line)
        mm = re.match(r"^L([123])([01])$", line)
        if mm:
            self.on[int(mm.group(1))] = (mm.group(2) == "1")
        mm = re.match(r"^Q([123]) ([0-9.+-]+)$", line)
        if mm:
            self.setp[int(mm.group(1))] = float(mm.group(2))
        if line == "R60":
            vals = [self.setp[c] if (self.on[c] and self.reach[c]) else 0.0 for c in (1, 2, 3)]
            self.m._on_line_from_tcp("Q0 " + " ".join(f"+{v:.2f}" for v in vals))
        elif line == "R5":
            self.m._on_line_from_tcp("P +0.500")

    async def drain(self):
        return None

    def is_closing(self):
        return False

    def close(self):
        pass

    async def wait_closed(self):
        return None


def _new_mfc(key="MFC1"):
    m = AsyncMFC(enable_verify=False, enable_stabilization=True, cfg=FAST)
    m.resource_key = key
    dev = FakeDevice(m)
    m._want_connected = True
    m._connected = True
    m._writer = dev
    worker = asyncio.get_running_loop().create_task(m._cmd_worker_loop())
    evs: list[MFCEvent] = []

    async def _collect():
        async for ev in m.events():
            evs.append(ev)
    col = asyncio.get_running_loop().create_task(_collect())
    return m, dev, evs, (worker, col)


async def _stop(*tasks):
    for t in tasks:
        t.cancel()
    for t in tasks:
        with contextlib.suppress(BaseException):
            await t


def _results(evs, *, owner=None, cmd=None, kind=None):
    out = []
    for e in evs:
        if e.kind not in ("command_confirmed", "command_failed"):
            continue
        if owner is not None and e.owner != owner:
            continue
        if cmd is not None and e.cmd != cmd:
            continue
        if kind is not None and e.kind != kind:
            continue
        out.append(e)
    return out


def _msgs(evs):
    return [e.message for e in evs if e.kind == "status" and e.message]


async def _until(pred, timeout=5.0):
    t_end = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < t_end:
        if pred():
            return True
        await asyncio.sleep(0.01)
    return pred()


def _pc(ch, gas, pressure):
    p = PlasmaCleaningRuntime.__new__(PlasmaCleaningRuntime)
    p._selected_ch = ch
    p.mfc_gas = gas
    p.mfc_pressure = pressure
    p._mfc_claims = {}
    p.logs = []
    p.append_log = lambda src, msg: p.logs.append(f"[{src}] {msg}")
    return p


def _chamber(ch, mfc):
    c = ChamberRuntime.__new__(ChamberRuntime)
    c.ch = ch
    c.mfc = mfc
    c.logs = []
    c.append_log = lambda src, msg: c.logs.append(f"[{src}] {msg}")
    c._u = lambda name: None
    return c


@pytest.fixture(autouse=True)
def _clean_state():
    def _clr():
        for k in ("MFC1", "MFC2"):
            for o in ("chamber1", "chamber2", "pc1", "pc2"):
                runtime_state.release_shared(k, o)
        for o in ("chamber1", "chamber2", "pc1", "pc2"):
            runtime_state.end_run_use(o)
    _clr()
    yield
    _clr()


# ───────────────────────── runtime_state: 런 사용자 집합 ─────────────────────────
def test_r_run_users_api():
    rs = RuntimeState()
    rs.begin_run_use("pc2", ["MFC1", "MFC2"])
    rs.begin_run_use("chamber1", ["MFC1"])
    assert rs.run_users("MFC1") == {"pc2", "chamber1"} and rs.run_users("MFC2") == {"pc2"}
    rs.acquire_shared("MFC1", "chamber1")
    assert rs.other_users("MFC1", "pc2") == {"chamber1"}
    assert rs.other_users("MFC2", "pc2") == set()
    assert rs.shared_snapshot() == {"MFC1": ["chamber1"]}               # 폴링 집합은 그대로
    assert rs.snapshot()["run_users"] == {"chamber1": ["MFC1"], "pc2": ["MFC1", "MFC2"]}
    rs.begin_run_use("pc2", ["MFC1"])                                   # 재등록 = 교체(멱등)
    assert rs.run_users("MFC2") == set()
    rs.end_run_use("pc2")
    rs.end_run_use("pc2")                                               # 없던 owner 해제도 안전
    assert rs.run_users("MFC1") == {"chamber1"}


# ───────────────────────── T1 동시 시작: 채널별 안정화 + 결과는 주인에게만 ─────────────────────────
def test_t1_start_together_stabilization_isolated():
    async def _main():
        m, dev, evs, tasks = _new_mfc()
        dev.reach[1] = False                         # CH1 Ar 는 아직 목표 미도달
        await m.set_flow(1, 20.0, owner="chamber1")
        await m.flow_on(1, owner="chamber1")         # CH1 Ar 안정화 시작
        await m.gas_select(3, owner="pc2")
        await m.flow_set_on(30.0, owner="pc2")       # PC N2 시작 → 이전처럼 CH1 안정화를 끊으면 안 된다
        assert await _until(lambda: _results(evs, owner="pc2", cmd="FLOW_ON"))
        assert [e.kind for e in _results(evs, owner="pc2", cmd="FLOW_ON")] == ["command_confirmed"]
        assert _results(evs, owner="chamber1", cmd="FLOW_ON") == []          # PC 확인이 CH1 것으로 가지 않음
        assert 1 in m._stab_jobs and m._stab_jobs[1].owner == "chamber1"   # CH1 안정화 유지
        dev.reach[1] = True
        assert await _until(lambda: _results(evs, owner="chamber1", cmd="FLOW_ON"))
        assert [e.kind for e in _results(evs, owner="chamber1", cmd="FLOW_ON")] == ["command_confirmed"]
        # 확인 이벤트에는 채널이 실린다
        assert _results(evs, owner="chamber1", cmd="FLOW_ON")[0].channel == 1
        await _stop(*tasks)
    asyncio.run(_main())


# ───────────────────────── T2 한쪽 실패가 다른 쪽으로 가지 않는다(주인 필터) ─────────────────────────
class _PCtl:
    def __init__(self):
        self.ok, self.fail = [], []

    def on_mfc_confirmed(self, cmd):
        self.ok.append(cmd)

    def on_mfc_failed(self, cmd, why, **k):
        self.fail.append((cmd, why))


class _FeedMFC:
    def __init__(self, evs):
        self._evs = evs

    async def events(self):
        for e in self._evs:
            yield e


_FEED = [
    MFCEvent(kind="command_failed", cmd="FLOW_ON", reason="GAS 안정화 시간 초과", owner="pc2", channel=3),
    MFCEvent(kind="command_confirmed", cmd="FLOW_ON", owner="pc2", channel=3),
    MFCEvent(kind="command_confirmed", cmd="FLOW_ON", owner="chamber1", channel=1),
    MFCEvent(kind="command_failed", cmd="WAIT_PRESSURE", reason="압력 안정화 실패", owner="chamber1"),
    MFCEvent(kind="command_confirmed", cmd="FLOW_SET"),                 # owner 없음 = 공용(기존 호환)
]


def test_t2_chamber_pump_takes_only_own_results():
    async def _main():
        c = _chamber(1, _FeedMFC(_FEED))
        c.process_controller = _PCtl()
        c._last_polling_targets = {}
        await c._pump_mfc_events()
        assert c.process_controller.ok == ["FLOW_ON", "FLOW_SET"]
        assert c.process_controller.fail == [("WAIT_PRESSURE", "압력 안정화 실패")]
    asyncio.run(_main())


def test_t2b_pc_pump_takes_only_own_results():
    async def _main():
        feed = _FeedMFC(_FEED)
        p = _pc(2, feed, None)
        p._run_mfc_owner = "pc2"
        p.pc = _PCtl()
        await p._pump_mfc_events(feed, "MFC(GAS)")
        assert p.pc.ok == ["FLOW_ON", "FLOW_SET"]
        assert p.pc.fail == [("FLOW_ON", "GAS 안정화 시간 초과")]           # 챔버 WAIT_PRESSURE 실패로 PC 가 죽지 않음
    asyncio.run(_main())


# ───────────────────────── T3 CH1 가스 준비 중 PC 종료(재현됐던 무한 대기) ─────────────────────────
def test_t3_pc_end_during_ch1_gas_on_keeps_ch1():
    async def _main():
        m1, dev, evs, tasks = _new_mfc("MFC1")
        m2 = SimpleNamespace(resource_key="MFC2", calls=[])
        m2.set_poll_mask = lambda **k: m2.calls.append(("mask", k))
        m2.on_process_cleanup = lambda: m2.calls.append(("cleanup",))
        runtime_state.begin_run_use("chamber1", ["MFC1"])         # CH1 런 진행 중(폴링 구간 아님)
        p = _pc(2, m1, m2)
        p._run_mfc_owner = "pc2"
        runtime_state.begin_run_use("pc2", ["MFC1", "MFC2"])
        p._acquire_mfcs()
        dev.reach[1] = False
        await m1.set_flow(1, 20.0, owner="chamber1")
        await m1.flow_on(1, owner="chamber1")                       # CH1 FLOW_ON 안정화 중
        assert 1 in m1._stab_jobs

        p._release_mfcs_and_finalize()                              # PC 종료 정리
        await asyncio.sleep(0.05)
        assert 1 in m1._stab_jobs, "PC 종료가 CH1 안정화를 끊었다"
        assert m1.last_setpoints[1] > 0
        assert not any("(cleanup)" in s for s in _msgs(evs)), "PC 종료가 공유 대기열을 폐기했다"
        assert any("MFC1 대기열/상태 유지" in s for s in p.logs), p.logs
        assert m2.calls == [("mask", {"gas": True, "pressure": True}), ("cleanup",)]   # MFC2 는 PC 단독 → 정리
        assert runtime_state.run_users("MFC1") == {"chamber1"}
        dev.reach[1] = True
        assert await _until(lambda: _results(evs, owner="chamber1", cmd="FLOW_ON"))
        assert _results(evs, owner="chamber1", cmd="FLOW_ON")[0].kind == "command_confirmed"
        await _stop(*tasks)
    asyncio.run(_main())


# ───────────────────────── T4 동시 종료: CH1 FLOW_OFF 는 폐기되지 않고 실제 전송 ─────────────────────────
def test_t4_end_together_ch1_flow_off_really_sent():
    async def _main():
        m1, dev, evs, tasks = _new_mfc("MFC1")
        runtime_state.begin_run_use("chamber1", ["MFC1"])         # CH1 종료 절차 진행 중
        p = _pc(2, m1, None)
        p._run_mfc_owner = "pc2"
        runtime_state.begin_run_use("pc2", ["MFC1"])
        p._acquire_mfcs()
        t = asyncio.create_task(m1.flow_off(1, owner="chamber1"))
        await asyncio.sleep(0)                                      # FLOW_OFF 대기열 등록 직후
        p._release_mfcs_and_finalize()                              # 같은 순간 PC 종료
        await t
        assert "L10" in dev.sent
        assert await _until(lambda: _results(evs, owner="chamber1", cmd="FLOW_OFF"))
        assert [e.kind for e in _results(evs, owner="chamber1", cmd="FLOW_OFF")] == ["command_confirmed"]
        assert not any("폐기" in s and "FLOW_OFF" in s for s in _msgs(evs))
        await _stop(*tasks)
    asyncio.run(_main())


# ───────────────────────── T5 PC 진행 중 CH1 종료: PC 몫은 그대로 ─────────────────────────
def test_t5_ch1_end_during_pc_keeps_pc():
    async def _main():
        m1, dev, evs, tasks = _new_mfc("MFC1")
        runtime_state.begin_run_use("pc2", ["MFC1", "MFC2"])
        runtime_state.acquire_shared("MFC1", "pc2")
        c = _chamber(1, m1)
        dev.reach[3] = False
        await m1.gas_select(3, owner="pc2")
        await m1.flow_set_on(30.0, owner="pc2")                      # PC N2 안정화 중
        await m1.set_flow(1, 20.0, owner="chamber1")
        c._on_process_status_changed(True)
        assert runtime_state.run_users("MFC1") == {"pc2", "chamber1"}
        c._on_process_status_changed(False)                          # CH1 종료(상태 이벤트)
        assert runtime_state.run_users("MFC1") == {"pc2"}
        assert c._skip_mfc_finalize_due_to_pc() is True               # PC 가 쓰는 중 → 전체 정리 생략
        c._mfc_release_own("finished")                               # 이 챔버 몫만 정리
        assert m1.last_setpoints[1] == 0.0                            # CH1 채널만 초기화
        assert 3 in m1._stab_jobs and m1.last_setpoints[3] > 0 and m1._selected_ch == 3
        dev.reach[3] = True
        assert await _until(lambda: _results(evs, owner="pc2", cmd="FLOW_ON"))
        assert _results(evs, owner="pc2", cmd="FLOW_ON")[0].kind == "command_confirmed"
        await _stop(*tasks)
    asyncio.run(_main())


# ───────────────────────── T6 동시 종료 순서 무관: 나중 쪽이 반드시 전체 정리 ─────────────────────────
@pytest.mark.parametrize("order", ["pc_first", "chamber_first"])
def test_t6_last_one_finalizes(order):
    async def _main():
        m1, dev, evs, tasks = _new_mfc("MFC1")
        c = _chamber(1, m1)
        p = _pc(2, m1, None)
        p._run_mfc_owner = "pc2"
        runtime_state.begin_run_use("pc2", ["MFC1"])
        p._acquire_mfcs()
        c._on_process_status_changed(True)

        def _chamber_end():
            c._on_process_status_changed(False)
            if c._skip_mfc_finalize_due_to_pc():
                c._mfc_release_own("finished")
                return "release"
            m1.on_process_finished(True)
            return "full"

        if order == "pc_first":
            p._release_mfcs_and_finalize()
            assert not any("(cleanup)" in s for s in _msgs(evs))
            assert _chamber_end() == "full"
        else:
            assert _chamber_end() == "release"
            p._release_mfcs_and_finalize()
        await asyncio.sleep(0.02)
        full = [s for s in _msgs(evs) if "대기 중 명령" in s and "폐기" in s]
        assert len(full) == 1, full
        assert runtime_state.run_users("MFC1") == set() and runtime_state.shared_users("MFC1") == set()
        await _stop(*tasks)
    asyncio.run(_main())


# ───────────────────────── T7 폐기된 no-reply 는 '실패'(가짜 확인 금지) ─────────────────────────
def test_t7_purged_noreply_reports_failed():
    async def _main():
        m1, dev, evs, tasks = _new_mfc("MFC1")
        m1._connected = False                                        # 링크 끊김 → 대기열에 머묾
        t1 = asyncio.create_task(m1.handle_command("FLOW_OFF", {"channel": 1}, owner="chamber1"))
        t2 = asyncio.create_task(m1.handle_command("PS_ZEROING", {}, owner="chamber1"))
        t3 = asyncio.create_task(m1.handle_command("MFC_ZEROING", {"channel": 2}, owner="chamber1"))
        await asyncio.sleep(0.05)
        m1.on_process_cleanup()                                       # 전체 정리 → 폐기
        await asyncio.gather(t1, t2, t3)
        await asyncio.sleep(0.05)
        for cmd in ("FLOW_OFF", "PS_ZEROING", "MFC_ZEROING"):
            r = _results(evs, owner="chamber1", cmd=cmd)
            assert [e.kind for e in r] == ["command_failed"], (cmd, r)
            assert "폐기" in (r[0].reason or ""), r[0].reason
        assert dev.sent == []
        await _stop(*tasks)
    asyncio.run(_main())


# ───────────────────────── T8 전송 대기 상한: 켜는 명령은 철회, 끄는 명령은 남김 ─────────────────────────
def test_t8_send_bound_withdraw_on_off(monkeypatch):
    monkeypatch.setattr(MFCMOD, "SEND_CONFIRM_TIMEOUT_S", 0.3)

    async def _main():
        m1, dev, evs, tasks = _new_mfc("MFC1")
        m1._connected = False
        await m1.flow_on(1, owner="chamber1")
        assert await _until(lambda: _results(evs, owner="chamber1", cmd="FLOW_ON"))
        r = _results(evs, owner="chamber1", cmd="FLOW_ON")
        assert [e.kind for e in r] == ["command_failed"] and "철회" in r[0].reason
        assert not any(c.tag == "[FLOW_ON ch1]" for c in m1._cmd_q)  # 대기열에서 빠졌다
        assert m1._flow_on_flags[1] is False
        await m1.flow_off(1, owner="chamber1")
        assert await _until(lambda: _results(evs, owner="chamber1", cmd="FLOW_OFF"))
        r = _results(evs, owner="chamber1", cmd="FLOW_OFF")
        assert [e.kind for e in r] == ["command_failed"]
        assert any(c.tag == "[FLOW_OFF ch1]" for c in m1._cmd_q)     # OFF 는 남겨 둔다
        m1._connected = True                                          # 링크 복구 → OFF 가 실제로 나간다
        assert await _until(lambda: "L10" in dev.sent)
        assert "L11" not in dev.sent                                  # 철회한 ON 은 나가지 않는다
        await _stop(*tasks)
    asyncio.run(_main())


# ───────────────────────── T9 유량 감시: PC 가스 선택이 CH1 감시를 끄지 않는다 ─────────────────────────
def test_t9_monitor_flow_by_flags_and_selected_cleared():
    async def _main():
        m1, dev, evs, tasks = _new_mfc("MFC1")
        await m1.gas_select(3, owner="pc2")
        m1._flow_on_flags[1] = True
        m1.last_setpoints[1] = 20.0
        for _ in range(3):
            m1._monitor_flow(1, 0.0)
        await asyncio.sleep(0.02)
        assert any("Ch1 GAS 불안정" in s for s in _msgs(evs)), _msgs(evs)
        m1.release_owner("pc2", reason="test")
        assert m1._selected_ch is None
        m1.on_process_finished(True)
        assert m1._selected_ch is None and m1._ch_owner == {1: None, 2: None, 3: None}
        await _stop(*tasks)
    asyncio.run(_main())


# ───────────────────────── T10 전체 정리 시 진행 중 안정화는 실패로 통지(무한 대기 방지) ─────────────────────────
def test_t10_finalize_notifies_pending_stabilization():
    async def _main():
        m1, dev, evs, tasks = _new_mfc("MFC1")
        dev.reach[1] = False
        await m1.set_flow(1, 20.0, owner="chamber1")
        await m1.flow_on(1, owner="chamber1")
        assert 1 in m1._stab_jobs
        m1.on_process_cleanup()
        await asyncio.sleep(0.02)
        r = _results(evs, owner="chamber1", cmd="FLOW_ON")
        assert [e.kind for e in r] == ["command_failed"], r
        assert m1._stab_jobs == {} and m1._stab_task is None
        await _stop(*tasks)
    asyncio.run(_main())


# ───────────────────────── T11 연결 종료 ↔ 시작 경합(동시 종료·시작) ─────────────────────────
def test_t11_cleanup_and_start_serialized():
    async def _main():
        m = AsyncMFC(enable_verify=False, enable_stabilization=True, cfg=FAST)
        m.resource_key = "MFC1"
        order = []

        async def _fake_wd():
            while True:
                await asyncio.sleep(1)
        m._watchdog_loop = _fake_wd

        class _SlowWriter(FakeDevice):
            async def wait_closed(self):
                order.append("closing")
                await asyncio.sleep(0.2)
                order.append("closed")
        m._writer = _SlowWriter(m)
        m._connected = True
        await m.start()
        assert m.is_connected()
        t_clean = asyncio.create_task(m.cleanup())
        await asyncio.sleep(0.05)
        assert m.is_connected() is False                              # 종료 중에는 '연결됨'으로 보이지 않는다
        await m.start()                                               # 다른 런타임의 시작 → 종료 끝날 때까지 대기
        order.append("started")
        await t_clean
        assert order == ["closing", "closed", "started"], order
        assert m._want_connected is True and m._cmd_worker_task and not m._cmd_worker_task.done()
        await m.cleanup()
    asyncio.run(_main())


def test_t11b_chamber_mfc_cleanup_rechecks_users():
    async def _main():
        calls = []

        class _M:
            resource_key = "MFC1"

            async def cleanup(self):
                calls.append("cleanup")
        mm = _M()
        c = _chamber(1, mm)
        runtime_state.begin_run_use("pc2", ["MFC1"])                 # 판단 뒤 PC 가 시작했다고 가정
        await c._mfc_cleanup_if_unshared(mm)
        assert calls == [] and any("재확인" in s for s in c.logs)
        runtime_state.end_run_use("pc2")
        await c._mfc_cleanup_if_unshared(mm)
        assert calls == ["cleanup"]
    asyncio.run(_main())


# ───────────────────────── T12 같은 채널을 다른 주체가 다시 켜면 이전 주체에게 실패 통지 ─────────────────────────
def test_t12_same_channel_other_owner_notified():
    async def _main():
        m1, dev, evs, tasks = _new_mfc("MFC1")
        dev.reach[2] = False
        await m1.set_flow(2, 10.0, owner="chamber1")
        await m1.flow_on(2, owner="chamber1")
        await m1.set_flow(2, 10.0, owner="pc2")
        await m1.flow_on(2, owner="pc2")
        r = _results(evs, owner="chamber1", cmd="FLOW_ON")
        assert [e.kind for e in r] == ["command_failed"], r
        assert m1._stab_jobs[2].owner == "pc2"
        await _stop(*tasks)
    asyncio.run(_main())


# ───────────────────────── 소스: PC 정리 순서(가스 OFF 뒤 해제) ─────────────────────────
def test_s_pc_final_cleanup_releases_after_gas_off():
    src = open(os.path.join(_ROOT, "runtime", "plasma_cleaning_runtime.py"), encoding="utf-8").read()
    i = src.index("    async def _final_cleanup(self) -> None:")
    j = src.index("    def _apply_button_state(", i)
    blk = src[i:j]
    i_rf = blk.index("await self._safe_rf_stop()")
    i_rel = blk.index("self._release_mfcs_and_finalize()")
    assert i_rf < i_rel, "PC 가 자기 가스 OFF 전에 공유 MFC 를 해제한다"
    k = src.index("    async def _shutdown_rest_devices(self) -> None:")
    blk2 = src[k:src.index("    async def _disconnect_selected_devices(", k)]
    assert blk2.index("flow_off_selected(") < blk2.index("self._release_mfcs_and_finalize()")


# ───────────────────────── T13 cleanup() 가 끝까지 실행되고, 상주 이벤트 펌프는 계속 받는다 ─────────────────────────
def test_t13_cleanup_completes_and_keepalive_pump_survives():
    """장비 로그: 'MFC 종료 절차 시작' 57회 / 'MFC 연결 종료됨' 0회 — _cancel_task 가 CancelledError 를 못 잡아
    워커 취소 직후 cleanup 이 끊겼다(TCP 미종료 → 약 55초 뒤 원격에서 끊김, 그 사이 '연결됨'인데 워커는 죽은 상태)."""
    async def _main():
        m = AsyncMFC(enable_verify=False, enable_stabilization=True, cfg=FAST)
        m.resource_key = "MFC1"
        got = []

        async def _pump():                       # 챔버의 상주 펌프 흉내(공정 사이에도 살아 있음)
            async for ev in m.events():
                got.append(ev)
        pump = asyncio.create_task(_pump())

        async def _fake_wd():                    # 워치독 대신: 연결을 즉시 붙인다
            m._writer = FakeDevice(m)
            m._connected = True
            while True:
                await asyncio.sleep(1)
        m._watchdog_loop = _fake_wd
        await m.start()
        await asyncio.sleep(0.02)
        assert m.is_connected()
        await m.cleanup()
        await asyncio.sleep(0.02)
        msgs = [e.message for e in got if e.kind == "status"]
        assert "MFC 연결 종료됨" in msgs, msgs
        assert any("폐기 (shutdown)" in s for s in msgs), msgs
        assert m._connected is False and m._writer is None and m.is_connected() is False
        assert m._cmd_worker_task is None and m._watchdog_task is None
        # 다음 공정: 다시 시작 → 같은 펌프가 확인 이벤트를 받는다
        await m.start()
        await asyncio.sleep(0.02)
        assert await m.set_flow(1, 5.0, owner="chamber1")
        assert await _until(lambda: any(e.kind == "command_confirmed" and e.cmd == "FLOW_SET" for e in got))
        await m.cleanup()
        await _stop(pump)
    asyncio.run(_main())


# ───────────────────────── T14 실제 PC _final_cleanup 경로: CH1 가스 준비 중 PC 종료 ─────────────────────────
def test_t14_pc_final_cleanup_real_path_during_ch1_gas_on():
    async def _main():
        m1, dev1, evs1, t1 = _new_mfc("MFC1")
        m2, dev2, evs2, t2 = _new_mfc("MFC2")
        p = _pc(2, m1, m2)
        p._cleanup_started = False
        p._test_mode_active = False
        p.ig = None
        p.rf = None                                   # _safe_rf_stop → _shutdown_rest_devices 바로
        p._event_tasks, p._bg_tasks = [], []
        p._disconnect_on_finish = False
        p._close_run_log = lambda: None
        p._process_timer_active = False
        p._run_mfc_owner = "pc2"
        p._mfc_release_done = False
        runtime_state.begin_run_use("pc2", ["MFC1", "MFC2"])
        p._acquire_mfcs()
        await m1.gas_select(3, owner="pc2")
        await m1.flow_set_on(30.0, owner="pc2")        # PC N2 켜짐(도달)
        assert await _until(lambda: _results(evs1, owner="pc2", cmd="FLOW_ON"))

        runtime_state.begin_run_use("chamber1", ["MFC1"])   # CH1 공정 준비 중(폴링 구간 아님)
        dev1.reach[1] = False
        await m1.set_flow(1, 20.0, owner="chamber1")
        await m1.flow_on(1, owner="chamber1")          # CH1 Ar 안정화 대기 중

        await p._final_cleanup()                       # PC 정상 종료 정리(실제 코드 경로)
        assert "L30" in dev1.sent                      # PC 자기 N2 OFF 는 나갔다
        assert dev2.sent.count("O") == 1               # SP4 쪽 밸브 OPEN
        assert 1 in m1._stab_jobs and m1.last_setpoints[1] > 0      # CH1 안정화/설정 유지
        assert not any("(cleanup)" in s for s in _msgs(evs1))       # MFC1 대기열 폐기 없음
        assert any("(cleanup)" in s for s in _msgs(evs2))           # MFC2 는 PC 단독 → 전체 정리
        assert runtime_state.run_users("MFC1") == {"chamber1"} and runtime_state.run_users("MFC2") == set()
        assert getattr(p, "_run_mfc_owner", None) is None
        assert m1._selected_ch is None                 # PC 선택 채널 해제
        dev1.reach[1] = True
        assert await _until(lambda: _results(evs1, owner="chamber1", cmd="FLOW_ON"))
        assert _results(evs1, owner="chamber1", cmd="FLOW_ON")[0].kind == "command_confirmed"
        await _stop(*t1, *t2)
    asyncio.run(_main())


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q", "-s"]))
