# -*- coding: utf-8 -*-
"""같은 CH 챔버 ↔ Plasma Cleaning(PC) 상호 차단 + 공유 IG 보호 + IG 이벤트 브로드캐스트 + IG 정리 중단 버그.

실제 AsyncIG 에 '가짜 IG 서버'(127.0.0.1 TCP, SIG→OK / RDI→압력)를 붙이고,
실제 ChamberRuntime / PlasmaCleaningRuntime 메서드를 최소 속성으로 실행한다.
수정 전 재현된 문제:
 · IG 이벤트 큐가 하나라 챔버 상주 펌프와 PC 펌프가 나눠 받음 → PC 로그에서 Base 도달/압력 누락
 · AsyncIG.cleanup()/pause_watchdog() 이 취소한 자식 태스크의 CancelledError 로 중간에 끊김
   (연결 미종료·워커만 죽음, Base 대기 재시작 경로 사망, 주소 변경 후 예전 주소에 남음)
 · 챔버 UI Start 가 같은 CH PC 실행 중에도 진행 → 게이트 체크 실패 정리가 PC 의 IG 대기를 끊음
 · 챔버 리스트 delay 중(running 표시 꺼짐) 같은 CH PC 시작이 수락됨
"""
import os
import sys
import asyncio
import contextlib
from collections import deque
from types import SimpleNamespace

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                        # noqa: E402
import runtime.chamber_runtime as CR                                 # noqa: E402
import runtime.plasma_cleaning_runtime as PCR                        # noqa: E402
from device.ig import AsyncIG                                        # noqa: E402
from controller.runtime_state import RuntimeState                    # noqa: E402
from runtime.chamber_runtime import ChamberRuntime                   # noqa: E402
from runtime.plasma_cleaning_runtime import PlasmaCleaningRuntime    # noqa: E402

FASTIG = SimpleNamespace(
    IG_TIMEOUT_MS=1000, IG_GAP_MS=10, IG_FIRST_READ_DELAY_MS=50, IG_POLLING_INTERVAL_MS=50,
    IG_WAIT_TIMEOUT=10, IG_RECONNECT_BACKOFF_START_MS=50, IG_RECONNECT_BACKOFF_MAX_MS=200,
    IG_WATCHDOG_INTERVAL_MS=50, IG_CONNECT_TIMEOUT_S=1.0, IG_DRAIN_TIMEOUT_S=1.0,
    IG_INACTIVITY_REOPEN_S=0.0, IG_REIGNITE_MAX_ATTEMPTS=3, IG_TX_EOL=b"\r", IG_SKIP_ECHO=True,
)


class FakeIGServer:
    """IG(MOXA TCP) 흉내: 'SIG 1/0' → 'OK', 'RDI' → pressures 에서 하나씩(없으면 default)."""

    def __init__(self, pressures=None, default="5.0x10E-5"):
        self.pressures = list(pressures or [])
        self.default = default
        self.lines: list[str] = []
        self.open = 0
        self.total = 0
        self.srv = None
        self.port = 0

    async def _handle(self, reader, writer):
        self.open += 1
        self.total += 1
        try:
            while True:
                line = await reader.readuntil(b"\r")
                s = line.decode("ascii", "ignore").strip().upper()
                self.lines.append(s)
                if s.startswith("SIG"):
                    writer.write(b"OK\r")
                elif s == "RDI":
                    p = self.pressures.pop(0) if self.pressures else self.default
                    writer.write((p + "\r").encode())
                await writer.drain()
        except Exception:
            pass
        finally:
            self.open -= 1
            with contextlib.suppress(Exception):
                writer.close()

    async def start(self):
        self.srv = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self.srv.sockets[0].getsockname()[1]
        return self

    async def stop(self):
        if self.srv is None:
            return
        self.srv.close()
        cc = getattr(self.srv, "close_clients", None)
        if callable(cc):
            with contextlib.suppress(Exception):
                cc()
        with contextlib.suppress(BaseException):
            await asyncio.wait_for(self.srv.wait_closed(), timeout=1.0)


async def _until(pred, timeout=5.0):
    t_end = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < t_end:
        if pred():
            return True
        await asyncio.sleep(0.01)
    return pred()


async def _stop(*tasks):
    for t in tasks:
        t.cancel()
    for t in tasks:
        with contextlib.suppress(BaseException):
            await t


def _collector(ig, out: list):
    async def _run():
        async for ev in ig.events():
            out.append(ev)
    return asyncio.get_running_loop().create_task(_run())


def _msgs(evs):
    return [e.message for e in evs if e.kind == "status" and e.message]


def _sig(evs):
    return [(e.kind, e.message, e.pressure) for e in evs]


async def _connected_ig(srv):
    ig = AsyncIG(host="127.0.0.1", port=srv.port, cfg=FASTIG)
    await ig.start()
    assert await _until(ig.is_connected, 3.0)
    return ig


async def _teardown_ig(ig):
    with contextlib.suppress(BaseException):
        await asyncio.wait_for(ig.cleanup(), timeout=3.0)
    for name in ("_event_broadcast_task", "_polling_task", "_bg_poll_task", "_cmd_worker_task", "_watchdog_task", "_reader_task"):
        t = getattr(ig, name, None)
        if isinstance(t, asyncio.Task):
            await _stop(t)


# ───────────────────────── IG 1: 두 구독자가 같은 이벤트를 모두 받는다(실제 Base 대기) ─────────────────────────
def test_i1_broadcast_both_subscribers_get_every_event_in_real_base_wait():
    async def _main():
        srv = await FakeIGServer(pressures=["5.0x10E-5", "2.0x10E-5", "1.0x10E-6"]).start()
        ig = await _connected_ig(srv)
        a, b = [], []
        ta, tb = _collector(ig, a), _collector(ig, b)
        await asyncio.sleep(0.01)
        assert len(ig._event_subscribers) == 2

        ok = await asyncio.wait_for(ig.wait_for_base_pressure(1e-5, interval_ms=50), timeout=10)
        assert ok is True
        # Base 도달 뒤 자동 정리가 끝까지 간다(연결 종료 줄) — 양쪽 모두
        assert await _until(lambda: "IG 연결 종료됨" in _msgs(a) and "IG 연결 종료됨" in _msgs(b))
        assert _sig(a) == _sig(b)                                       # 순서·내용 동일
        assert [e.pressure for e in a if e.kind == "pressure"] == [5e-5, 2e-5, 1e-6]
        assert sum(1 for e in a if e.kind == "base_reached") == 1
        assert "목표 압력 도달" in _msgs(a)
        assert await _until(lambda: srv.open == 0)                      # 연결이 실제로 닫혔다
        assert ig.is_connected() is False
        await _stop(ta, tb)
        await _teardown_ig(ig)
        await srv.stop()
    asyncio.run(_main())


# ───────────────────────── IG 2: 첫 구독자는 밀린 이벤트를 받고, 나중 구독자는 새 이벤트만 / 해제 ─────────────────────────
def test_i2_backlog_to_first_subscriber_new_only_to_later_and_unsubscribe():
    async def _main():
        ig = AsyncIG(host="127.0.0.1", port=9, cfg=FASTIG)
        await ig._emit_status("구독 전 이벤트")
        a = []
        ta = _collector(ig, a)
        assert await _until(lambda: "구독 전 이벤트" in _msgs(a))       # 기존처럼 첫 소비자가 받는다
        b = []
        tb = _collector(ig, b)
        await asyncio.sleep(0.01)
        await ig._emit_status("둘 다")
        await ig._emit_pressure(3.0e-6)
        assert await _until(lambda: "둘 다" in _msgs(a) and "둘 다" in _msgs(b))
        assert "구독 전 이벤트" not in _msgs(b)
        assert await _until(lambda: any(e.kind == "pressure" for e in b))
        await _stop(tb)                                                 # PC 펌프 종료처럼 취소
        assert await _until(lambda: len(ig._event_subscribers) == 1)
        await ig._emit_status("남은 쪽만")
        assert await _until(lambda: "남은 쪽만" in _msgs(a))
        assert not ig._event_broadcast_task.done()
        await _stop(ta)
        assert await _until(lambda: len(ig._event_subscribers) == 0)
        await _stop(ig._event_broadcast_task)
    asyncio.run(_main())


# ───────────────────────── IG 3: cleanup 이 끝까지 간다 + 구독은 유지 ─────────────────────────
def test_i3_cleanup_completes_closes_connection_and_keeps_subscribers():
    async def _main():
        srv = await FakeIGServer().start()
        ig = await _connected_ig(srv)
        a = []
        ta = _collector(ig, a)
        await asyncio.sleep(0.01)
        assert ig._cmd_worker_task is not None and not ig._cmd_worker_task.done()

        await asyncio.wait_for(ig.cleanup(), timeout=5.0)               # 수정 전: CancelledError 로 중단
        assert ig.is_connected() is False
        assert ig._cmd_worker_task is None and ig._watchdog_task is None
        assert await _until(lambda: "IG 연결 종료됨" in _msgs(a))
        assert any("폐기 (shutdown)" in s for s in _msgs(a))
        assert await _until(lambda: srv.open == 0)
        # 상주 펌프 구독 유지 → 다시 시작하면 같은 구독자가 새 이벤트를 받는다
        assert len(ig._event_subscribers) == 1 and not ig._event_broadcast_task.done()
        n = len(a)
        await ig.start()
        assert await _until(ig.is_connected, 3.0)
        assert await _until(lambda: any("연결 성공" in s for s in _msgs(a[n:])))
        await _stop(ta)
        await _teardown_ig(ig)
        await srv.stop()
    asyncio.run(_main())


# ───────────────────────── IG 4: 이미 대기 중일 때 다시 대기 요청(재시작 경로) ─────────────────────────
def test_i4_wait_restart_path_does_not_die():
    async def _main():
        srv = await FakeIGServer(default="5.0x10E-5").start()            # 처음엔 목표 미도달
        ig = await _connected_ig(srv)
        t1 = asyncio.get_running_loop().create_task(ig.wait_for_base_pressure(1e-5, interval_ms=50))
        assert await _until(lambda: ig._waiting_active and ig._polling_task is not None)
        srv.default = "1.0x10E-6"                                       # 이제 도달
        ok2 = await asyncio.wait_for(ig.wait_for_base_pressure(1e-5, interval_ms=50), timeout=10)  # 수정 전: CancelledError
        assert ok2 is True
        ok1 = await asyncio.wait_for(t1, timeout=5)
        assert ok1 is False                                             # 먼저 것은 정리돼 False
        await _teardown_ig(ig)
        await srv.stop()
    asyncio.run(_main())


# ───────────────────────── IG 5: 주소 변경 재연결(_bounce_connection) — 새 포트로 옮겨 붙는다 ─────────────────────────
def test_i5_endpoint_bounce_reconnects_to_new_port():
    async def _main():
        srv1 = await FakeIGServer().start()
        srv2 = await FakeIGServer().start()
        ig = await _connected_ig(srv1)
        ig._override_port = srv2.port
        await asyncio.wait_for(ig._bounce_connection(), timeout=5.0)     # 수정 전: CancelledError 로 중단
        assert await _until(lambda: ig.is_connected() and srv2.open == 1, 3.0)
        assert ig._writer.get_extra_info("peername")[1] == srv2.port
        assert await _until(lambda: srv1.open == 0)
        assert ig._want_connected is True and ig._watchdog_task is not None and not ig._watchdog_task.done()
        await _teardown_ig(ig)
        await srv1.stop()
        await srv2.stop()
    asyncio.run(_main())


# ───────────────────────── IG 6: _cancel_and_wait — 자식 취소만 삼키고 바깥 취소는 올린다 ─────────────────────────
def test_i6_cancel_and_wait_semantics():
    async def _main():
        ig = AsyncIG(host="127.0.0.1", port=9, cfg=FASTIG)
        loop = asyncio.get_running_loop()

        # (a) 자식 취소 → 삼킨다
        child = loop.create_task(asyncio.sleep(10))
        await asyncio.sleep(0)
        await ig._cancel_and_wait(child)
        assert child.cancelled()

        # (b) 기다리는 동안 바깥이 취소되면 → 다시 올린다
        async def _slow_child():
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                await asyncio.sleep(0.3)
                raise

        async def _outer():
            c = loop.create_task(_slow_child())
            await asyncio.sleep(0)
            await ig._cancel_and_wait(c)
            return "끝까지 감"
        o = loop.create_task(_outer())
        await asyncio.sleep(0.05)
        o.cancel()
        with pytest.raises(asyncio.CancelledError):
            await o
        assert o.cancelled()

        # (c) 이미 취소를 한 번 받아 처리한 태스크(cancelling()>0)에서도 자식 취소는 삼킨다(TSP finally 경로)
        async def _after_handled_cancel():
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                pass
            c = loop.create_task(asyncio.sleep(10))
            await asyncio.sleep(0)
            await ig._cancel_and_wait(c)
            return "정리 계속"
        h = loop.create_task(_after_handled_cancel())
        await asyncio.sleep(0.01)
        h.cancel()
        assert await asyncio.wait_for(h, timeout=2.0) == "정리 계속"

        # (d) 이미 끝난 태스크/None/자기 자신
        await ig._cancel_and_wait(None)
        done = loop.create_task(asyncio.sleep(0))
        await done
        await ig._cancel_and_wait(done)
    asyncio.run(_main())


# ───────────────────────── IG 7: TSP 종료 정리 패턴 — IG cleanup 뒤 코드가 실행된다 ─────────────────────────
def test_i7_code_after_ig_cleanup_runs_tsp_finally_pattern():
    """tsp_runtime._run finally: suppress(Exception) 안에서 ig.ensure_off() → ig.cleanup() 뒤 UI 기본값 복원.
    수정 전에는 cleanup 의 CancelledError 가 suppress(Exception) 를 뚫고 나가 그 뒤가 실행되지 않았다."""
    async def _main():
        srv = await FakeIGServer().start()
        ig = await _connected_ig(srv)
        await ig.ensure_on()
        after = []

        async def _finally_like():
            with contextlib.suppress(Exception):
                await ig.ensure_off()
            with contextlib.suppress(Exception):
                await ig.cleanup()
            after.append("UI 기본값 복원")
        await asyncio.wait_for(_finally_like(), timeout=5.0)
        assert after == ["UI 기본값 복원"]
        assert await _until(lambda: srv.open == 0)
        await _teardown_ig(ig)
        await srv.stop()
    asyncio.run(_main())


# ───────────────────────── 챔버 Start 입구(A) ─────────────────────────
def _start_chamber(ch, rs, monkeypatch):
    monkeypatch.setattr(CR, "runtime_state", rs)
    c = ChamberRuntime.__new__(ChamberRuntime)
    c.ch = ch
    c.logs, c.warns, c.host, c.puts = [], [], [], []
    c.append_log = lambda s, m: c.logs.append(f"[{s}] {m}")
    c._ensure_runner_started = lambda: None
    c._host_report_start = lambda ok, reason="": c.host.append((bool(ok), reason))
    c._post_warning = lambda title, text, **k: c.warns.append((title, text))
    c._pending_device_cleanup = False
    c._runner_state = "IDLE"
    c.process_controller = SimpleNamespace(is_running=False)
    c._auto_connect_enabled = False
    c.process_queue = [{"Process_name": "x"}]
    c.current_process_index = -1
    c._host_run_begin = lambda origin, meta: None
    c._runner_put = lambda cmd: c.puts.append(cmd.kind)
    c._handle_start_clicked(False)
    return c


def test_a1_chamber_start_rejected_when_same_ch_pc_running(monkeypatch):
    for ch in (1, 2):
        rs = RuntimeState()
        rs.mark_started("pc", ch)
        c = _start_chamber(ch, rs, monkeypatch)
        assert c.puts == [], (ch, c.puts)                                # 러너에 아무것도 안 넣음
        assert c._auto_connect_enabled is False                          # 장치 자동연결도 안 켬
        assert c.host == [(False, f"CH{ch} Plasma Cleaning 실행 중")], c.host
        assert c.warns and "Plasma Cleaning이 실행 중" in c.warns[-1][1]
        assert rs.is_running("chamber", ch) is False and not rs.has_error("chamber", ch)


def test_a2_chamber_start_allowed_when_other_ch_pc_running_or_no_pc(monkeypatch):
    for ch, pc_ch in ((1, 2), (2, 1), (1, None)):
        rs = RuntimeState()
        if pc_ch:
            rs.mark_started("pc", pc_ch)                                 # CH1 공정 + CH2 PC 동시 진행은 그대로
        c = _start_chamber(ch, rs, monkeypatch)
        assert c.puts == ["START_QUEUE"], (ch, pc_ch, c.puts)
        assert c.warns == [] and c._auto_connect_enabled is True


# ───────────────────────── PC Start 입구(B) ─────────────────────────
LIVE = {
    "_cleanup_started": True,
    "_forced_fail": True,
    "_forced_fail_reason": "RF OFF 미확인",
    "_rf_off_unconfirmed_reason": "강제 0W 쓰기 실패",
}


class _RS:
    """PC 쪽 runtime_state 스텁(test_pc_start_flags 와 같은 모양) — 쿨다운 없음, 아무것도 실행 중 아님."""

    def __init__(self):
        self.started = []

    def pc_block_reason(self, ch, cooldown_s=60.0):
        return (True, 0.0, "")

    def is_running(self, kind, ch=None):
        return False

    def mark_started(self, kind, ch=None):
        self.started.append((kind, ch))
        raise _Stop()


class _Stop(BaseException):
    """수락 뒤 진행을 멈추는 신호."""


def _pc_start(ch, probe, monkeypatch, *, set_probe=True):
    rs = _RS()
    monkeypatch.setattr(PCR, "runtime_state", rs)
    p = PlasmaCleaningRuntime.__new__(PlasmaCleaningRuntime)
    p._selected_ch = ch
    p._running = False
    p._loaded_recipe_row = {}
    p.warns, p.host = [], []
    p._post_warning = lambda title, text, **k: p.warns.append((title, text))
    p._host_report_start = lambda ok, reason="": p.host.append((bool(ok), reason))
    for k, v in LIVE.items():
        setattr(p, k, v)
    if set_probe:
        p.set_chamber_busy_probe(probe)
    accepted = False
    try:
        asyncio.run(p._on_click_start())
    except _Stop:
        accepted = True
    return p, rs, accepted


def test_b1_pc_start_rejected_when_same_ch_chamber_busy(monkeypatch):
    for ch in (1, 2):
        p, rs, accepted = _pc_start(ch, lambda c, _ch=ch: c == _ch, monkeypatch)
        assert accepted is False and rs.started == []
        assert p.host == [(False, f"CH{ch}는 이미 다른 공정이 실행 중입니다.")], p.host
        assert p.warns and p.warns[-1][0] == "실행 오류"
        assert {k: getattr(p, k) for k in LIVE} == LIVE                   # 진행 중 런 플래그 보존


def test_b2_pc_start_allowed_other_ch_busy_no_probe_or_probe_error(monkeypatch):
    def _boom(ch):
        raise RuntimeError("probe 실패")
    cases = [
        (1, lambda c: c == 2, True),         # 다른 CH 챔버만 바쁨
        (2, lambda c: c == 1, True),
        (1, None, False),                    # probe 미주입(기존 동작)
        (1, _boom, True),                    # probe 예외 → 바쁘지 않음으로 취급
    ]
    for ch, probe, set_probe in cases:
        p, rs, accepted = _pc_start(ch, probe, monkeypatch, set_probe=set_probe)
        assert accepted is True and rs.started == [("pc", ch)], (ch, p.host)
        assert p.host == [] and p.warns == []


def test_b3_real_chamber_is_busy_covers_queue_gap_states(monkeypatch):
    """실제 ChamberRuntime.is_busy 를 probe 로 — 리스트 행 사이(COOLDOWN)·delay·정리 중이면 거절, IDLE 이면 수락."""
    c = ChamberRuntime.__new__(ChamberRuntime)
    c.process_controller = SimpleNamespace(is_running=False)
    c._runner_cmd_start_enqueued = False
    for st, expect_reject in (("COOLDOWN", True), ("DELAY", True), ("CLEANUP", True), ("IDLE", False)):
        c._runner_state = st
        p, rs, accepted = _pc_start(2, lambda ch: bool(c.is_busy) if ch == 2 else False, monkeypatch)
        assert accepted is (not expect_reject), (st, p.host)
    c._runner_state = "IDLE"
    c._runner_cmd_start_enqueued = True                                   # Start 접수 직후(러너가 아직 안 꺼냄)
    p, rs, accepted = _pc_start(2, lambda ch: bool(c.is_busy), monkeypatch)
    assert accepted is False


def test_b4_main_injects_chamber_busy_probe():
    src = open(os.path.join(_ROOT, "main.py"), encoding="utf-8").read()
    i = src.index("self.pc.set_pc_done_callback(self._on_pc_done)")
    blk = src[i:i + 900]
    assert "def _chamber_busy(ch: int) -> bool:" in blk
    assert 'rt = self.ch1 if int(ch) == 1 else self.ch2' in blk
    assert 'getattr(rt, "is_busy", False)' in blk
    assert "self.pc.set_chamber_busy_probe(_chamber_busy)" in blk
    j = blk.index("self.pc.set_chamber_busy_probe(_chamber_busy)")
    assert "try:" in blk[:j] and "except Exception" in blk[j:]           # 주입 실패가 PC 를 끄지 않게


def test_b5_pc_source_order_busy_check_before_flag_reset():
    """새 거절(2-3)은 기존 거절들 뒤, 플래그 초기화/mark_started 앞(test_pc_start_flags 와 같은 원칙)."""
    src = open(os.path.join(_ROOT, "runtime", "plasma_cleaning_runtime.py"), encoding="utf-8").read()
    i_def = src.index("async def _on_click_start(")
    i_chamber = src.index('is_running("chamber", ch)', i_def)
    i_busy = src.index("if self._chamber_busy(ch):", i_def)
    i_init = src.index("self._cleanup_started = False", i_def)
    i_mark = src.index('runtime_state.mark_started("pc", ch)', i_def)
    assert i_chamber < i_busy < i_init < i_mark


# ───────────────────────── 챔버 정리가 같은 CH PC 의 IG 를 건드리지 않음(C) ─────────────────────────
class _FakeIG:
    def __init__(self, on_cancel=None):
        self.calls = []
        self._on_cancel = on_cancel

    async def cancel_wait(self, *, reason="user cancel / stop"):
        self.calls.append(("cancel_wait", reason))
        if self._on_cancel:
            self._on_cancel()

    async def cleanup(self):
        self.calls.append(("cleanup",))

    def set_process_status(self, on):
        self.calls.append(("set_process_status", on))


class _FakeMFC:
    resource_key = "MFC2"

    def __init__(self):
        self.calls = []

    def set_process_status(self, on):
        self.calls.append(("set_process_status", on))

    def on_process_cleanup(self):
        self.calls.append(("on_process_cleanup",))

    def release_owner(self, owner, *, reason=None):
        self.calls.append(("release_owner", owner))

    async def cleanup(self):
        self.calls.append(("cleanup",))


def _heavy_chamber(ch, ig, mfc, rs, monkeypatch):
    monkeypatch.setattr(CR, "runtime_state", rs)
    c = ChamberRuntime.__new__(ChamberRuntime)
    c.ch = ch
    c.ig, c.mfc = ig, mfc
    c.dc_pulse = c.rf_pulse = c.dc_power = c.dc_power2 = c.rf_power = c.oes = c.rga = None
    c.chat = None
    c._bg_tasks = []
    c._cleanup_timed_out = False
    c._keepalive_tasks = {}
    c.logs = []
    c.append_log = lambda s, m: c.logs.append(f"[{s}] {m}")
    c._loop_from_anywhere = lambda: asyncio.get_running_loop()
    c._log_file_path = None
    c._prestart_buf = deque()
    c._close_run_log = lambda: None

    async def _noop(*a, **k):
        return None
    c._shutdown_log_writer = _noop
    return c


def test_c1_heavy_cleanup_skips_ig_only_for_same_ch_pc(monkeypatch):
    async def _main():
        # 같은 CH PC 실행 중 → IG 는 대기 취소/정리 모두 생략, MFC(단독)는 정상 정리
        rs = RuntimeState()
        rs.mark_started("pc", 2)
        ig, mfc = _FakeIG(), _FakeMFC()
        c = _heavy_chamber(2, ig, mfc, rs, monkeypatch)
        await c._stop_device_watchdogs(light=False)
        assert ig.calls == [], ig.calls
        assert any("IG 대기 취소/정리 생략" in s for s in c.logs), c.logs
        assert ("on_process_cleanup",) in mfc.calls and ("cleanup",) in mfc.calls

        # 다른 CH PC 실행 중 / PC 없음 → 기존과 똑같이 IG 대기 취소(cleanup 라벨) + 정리
        for pc_ch in (1, None):
            rs = RuntimeState()
            if pc_ch:
                rs.mark_started("pc", pc_ch)
            ig, mfc = _FakeIG(), _FakeMFC()
            c = _heavy_chamber(2, ig, mfc, rs, monkeypatch)
            await c._stop_device_watchdogs(light=False)
            assert ig.calls == [("cancel_wait", "cleanup"), ("cleanup",)], (pc_ch, ig.calls)
    asyncio.run(_main())


def test_c2_heavy_cleanup_rechecks_right_before_ig_cleanup(monkeypatch):
    """정리 도중(대기 취소 뒤) 같은 CH PC 가 시작하면 연결 종료는 하지 않는다."""
    async def _main():
        rs = RuntimeState()
        ig = _FakeIG(on_cancel=lambda: rs.mark_started("pc", 1))
        c = _heavy_chamber(1, ig, _FakeMFC(), rs, monkeypatch)
        await c._stop_device_watchdogs(light=False)
        assert ig.calls == [("cancel_wait", "cleanup")], ig.calls
        assert any("연결 종료 직전 재확인" in s and "ig cleanup 생략" in s for s in c.logs), c.logs
    asyncio.run(_main())


def test_c3_preflight_fail_path_does_not_reset_ig_of_same_ch_pc(monkeypatch):
    async def _run(pc_running: bool):
        rs = RuntimeState()
        if pc_running:
            rs.mark_started("pc", 2)
        ig, mfc = _FakeIG(), _FakeMFC()
        c = _heavy_chamber(2, ig, mfc, rs, monkeypatch)
        c._active_run_gen = 7
        c.supports_rf_pulse = c.supports_dc_pulse = False
        c.cfg = SimpleNamespace(_get=lambda k, d=None: d)
        c._ensure_background_started = lambda: None
        c._kick_oes_init_background = lambda force=False: None
        c._on_process_status_changed = lambda running: None
        c._post_critical = lambda *a, **k: None
        c._host_report_start = lambda *a, **k: None
        c.stopped = 0

        async def _stop_wd(*, light=False):
            c.stopped += 1
        c._stop_device_watchdogs = _stop_wd

        async def _pre(params, timeout_s=0.0):
            return (False, ["IG"])
        c._preflight_connect = _pre
        with pytest.raises(RuntimeError):
            await c._start_after_preflight({"process_note": "t"}, 7)
        return ig, c

    async def _main():
        ig, c = await _run(pc_running=True)
        assert ("set_process_status", False) not in ig.calls and c.stopped >= 1
        ig, c = await _run(pc_running=False)
        assert ("set_process_status", False) in ig.calls                  # PC 없으면 기존처럼 리셋
    asyncio.run(_main())


class _FakeWriter:
    def __init__(self):
        self.closed = 0

    def close(self):
        self.closed += 1


def test_c4_force_recovery_keeps_shared_ig_and_mfc(monkeypatch):
    async def _run(*, pc_ch=None, mfc_other=False):
        rs = RuntimeState()
        if pc_ch:
            rs.mark_started("pc", pc_ch)
        if mfc_other:
            rs.begin_run_use("pc1", ["MFC2"])                               # 다른 런이 이 챔버 MFC 사용
        ig, mfc = _FakeIG(), _FakeMFC()
        ig._writer, mfc._writer = _FakeWriter(), _FakeWriter()
        c = _heavy_chamber(2, ig, mfc, rs, monkeypatch)
        c._force_recover_count = 0
        c._apply_polling_targets = lambda targets: None
        c._starter_threads = None
        c._log_writer_task = None
        c._log_io_exec = None
        ok = await c._force_recover_after_cleanup_timeout(["Cleanup.X"])
        with contextlib.suppress(Exception):
            c._log_io_exec.shutdown(wait=False)
        return ok, ig, mfc, c

    async def _main():
        ok, ig, mfc, c = await _run(pc_ch=2, mfc_other=True)
        assert ok is True
        assert ig._writer.closed == 0 and mfc._writer.closed == 0
        assert any("공유 장치는 닫지 않음 → IG, MFC" in s for s in c.logs), c.logs
        ok, ig, mfc, c = await _run(pc_ch=1, mfc_other=False)            # 다른 CH PC → 둘 다 닫는다(기존)
        assert ig._writer.closed == 1 and mfc._writer.closed == 1
        assert not any("공유 장치는 닫지 않음" in s for s in c.logs)
    asyncio.run(_main())


# ───────────────────────── 챔버 IG 펌프(D) ─────────────────────────
class _PCtl:
    def __init__(self):
        self.calls = []

    def on_ig_ok(self):
        self.calls.append("ok")

    def on_ig_failed(self, src, why, *, code=None, meta=None):
        self.calls.append(("failed", why))


class _DL:
    def __init__(self):
        self.p = []

    def log_ig_pressure(self, v):
        self.p.append(v)


def _pump_chamber(ch, ig, rs, monkeypatch):
    monkeypatch.setattr(CR, "runtime_state", rs)
    c = ChamberRuntime.__new__(ChamberRuntime)
    c.ch = ch
    c.ig = ig
    c.logs = []
    c.append_log = lambda s, m: c.logs.append(f"[{s}] {m}")
    c.process_controller = _PCtl()
    c.data_logger = _DL()
    c._dl_fire_and_forget = lambda fn, *a, **k: fn(*a, **k)
    return c


def test_d1_chamber_ig_pump_quiet_while_same_ch_pc_but_forwards_base_events(monkeypatch):
    async def _main():
        rs = RuntimeState()
        ig = AsyncIG(host="127.0.0.1", port=9, cfg=FASTIG)
        c = _pump_chamber(2, ig, rs, monkeypatch)
        pump = asyncio.get_running_loop().create_task(c._pump_ig_events())
        await asyncio.sleep(0.01)

        rs.mark_started("pc", 2)                                         # 같은 CH PC 가 IG 사용 중
        await ig._emit_status("Base Pressure 대기 시작")
        await ig._emit_pressure(4.0e-5)
        await ig._emit_base_reached()
        await ig._emit_failed("HardTimeout")
        assert await _until(lambda: len(c.process_controller.calls) == 2)
        assert c.process_controller.calls == ["ok", ("failed", "HardTimeout")]   # 공정 쪽 전달은 그대로
        assert c.logs == [] and c.data_logger.p == []                    # 로그/데이터는 PC 쪽만

        rs.mark_finished("pc", 2)                                        # PC 끝 → 챔버가 다시 기록
        await ig._emit_status("재연결 시도 예약... (1000 ms)")
        await ig._emit_pressure(1.0e-6)
        assert await _until(lambda: c.data_logger.p == [1.0e-6])
        assert any("재연결 시도 예약" in s for s in c.logs)

        rs.mark_started("pc", 1)                                         # 다른 CH PC 는 상관없음
        await ig._emit_status("다른 CH PC 중")
        assert await _until(lambda: any("다른 CH PC 중" in s for s in c.logs))
        await _stop(pump, ig._event_broadcast_task)
    asyncio.run(_main())


def test_d2_end_to_end_pc_gets_every_ig_event_with_chamber_pump_alive(monkeypatch):
    """9/29 08:38 CH2 PC 재현: 챔버 상주 IG 펌프가 살아 있는 상태에서 PC 가 Base 대기.
    수정 전: 이벤트를 나눠 받아 PC 로그에 '목표 압력 도달'/'Base pressure reached'/압력 일부가 빠짐."""
    async def _main():
        rs = RuntimeState()
        monkeypatch.setattr(PCR, "runtime_state", rs)
        rs.mark_started("pc", 2)                                         # PC 는 시작 수락 즉시 running(프리플라이트 전)
        srv = await FakeIGServer(pressures=["6.3x10E-5", "4.9x10E-5", "3.2x10E-5", "2.8x10E-5"]).start()
        ig = await _connected_ig(srv)

        c = _pump_chamber(2, ig, rs, monkeypatch)                        # 챔버 상주 펌프(공정 없음)
        cp = asyncio.get_running_loop().create_task(c._pump_ig_events())

        p = PlasmaCleaningRuntime.__new__(PlasmaCleaningRuntime)         # PC 펌프(런 동안)
        p.ig = ig
        p._running = True
        p._pc_ig_readings = []
        p.logs = []
        p.append_log = lambda s, m: p.logs.append(f"[{s}] {m}")
        pp = asyncio.get_running_loop().create_task(p._pump_ig_events("IG-CH2"))
        await asyncio.sleep(0.01)

        ok = await asyncio.wait_for(ig.wait_for_base_pressure(3.0e-5, interval_ms=50), timeout=10)
        assert ok is True
        assert await _until(lambda: any("IG 연결 종료됨" in s for s in p.logs))
        assert p._pc_ig_readings == [6.3e-5, 4.9e-5, 3.2e-5, 2.8e-5]     # 압력 전부(GDrive base_pressure = 마지막 값)
        assert any("Base pressure reached" in s for s in p.logs)
        assert any("목표 압력 도달" in s for s in p.logs)
        assert any("Base Pressure 대기 시작" in s for s in p.logs)
        assert c.logs == [] and c.data_logger.p == []                    # 챔버 로그(→ 다음 런 로그 앞부분)에 안 섞임
        assert c.process_controller.calls == ["ok"]                      # 챔버 공정 쪽 전달은 기존 그대로(공정 없으면 무시)
        await _stop(cp, pp)
        await _teardown_ig(ig)
        await srv.stop()
    asyncio.run(_main())


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q", "-s"]))
