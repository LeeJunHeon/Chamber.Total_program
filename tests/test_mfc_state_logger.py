# -*- coding: utf-8 -*-
"""GPC3000(MFC) 압력·밸브 위치 상시 CSV 기록 — 저우선(low_priority)·조용한(quiet) 읽기와 기록기.
실제 AsyncMFC + 가짜 장비(tests/test_mfc_shared_run.py 의 FakeDevice 방식). 네트워크·파일은 tmp_path 만."""
import os
import sys
import csv
import asyncio
import contextlib
import datetime as _dt
from pathlib import Path
from types import SimpleNamespace

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
sys.path.insert(0, _ROOT)
sys.path.insert(0, _HERE)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                        # noqa: E402
import device.mfc as MFCMOD                                          # noqa: E402
from device.mfc import AsyncMFC                                      # noqa: E402
from controller import mfc_state_logger as MSL                       # noqa: E402
from controller.mfc_state_logger import MfcStateLogger, HEADER       # noqa: E402
from test_mfc_shared_run import FakeDevice, FAST, _stop, _until     # noqa: E402

LOG_CFG = SimpleNamespace(MFC_STATE_LOG_INTERVAL_S=0.05, MFC_STATE_LOG_GAP_MS=5, MFC_STATE_LOG_TIMEOUT_MS=100)


class Dev(FakeDevice):
    """FakeDevice + R6("V+35.2") 응답, 응답 보류/지연, 응답 직전 훅."""

    def __init__(self, m):
        super().__init__(m)
        self.answer = {"R5": True, "R6": True}
        self.delay = {"R5": 0.0, "R6": 0.0}
        self.before_reply = None
        self.pressure = "P +0.500"

    def write(self, payload: bytes):
        line = payload.decode("ascii", "ignore").strip()
        if line in ("R5", "R6"):
            self.sent.append(line)
            if not self.answer[line]:
                return
            resp = self.pressure if line == "R5" else "V+35.2"

            def _reply():
                if self.before_reply:
                    self.before_reply(line)
                self.m._on_line_from_tcp(resp)
            d = self.delay[line]
            if d > 0:
                asyncio.get_running_loop().call_later(d, _reply)
            else:
                _reply()
            return
        super().write(payload)


def _mfc():
    m = AsyncMFC(enable_verify=False, enable_stabilization=True, cfg=FAST)
    m.resource_key = "MFC1"
    dev = Dev(m)
    m._want_connected = True
    m._connected = True
    m._writer = dev
    worker = asyncio.get_running_loop().create_task(m._cmd_worker_loop())
    evs = []

    async def _collect():
        async for ev in m.events():
            evs.append(ev)
    col = asyncio.get_running_loop().create_task(_collect())
    return m, dev, evs, (worker, col)


class _Wall:
    def __init__(self, dt):
        self.dt = dt

    def now(self):
        return self.dt


def _rows(path):
    with open(path, encoding="utf-8-sig", newline="") as fp:
        return list(csv.DictReader(fp))


def _status(evs):
    return [e.message for e in evs if e.kind == "status" and e.message]


# ───────────────────────── 1) 끔 = 기존과 동일 ─────────────────────────
def test_disabled_by_default_and_normal_queue_unchanged():
    # 소스 기본값은 False(키가 없거나 false 면 main 이 기록기를 만들지 않는다).
    # 모듈 속성이 아니라 소스를 보는 이유: 다른 테스트가 settings.json(true)을 config 모듈에 적용한 채 남길 수 있다.
    src = (Path(_ROOT) / "lib" / "config_common.py").read_text(encoding="utf-8")
    assert "MFC_STATE_LOG_ENABLED = False" in src

    async def go():
        m, dev, evs, tasks = _mfc()
        t_send = {}
        orig = dev.write

        def _w(payload):
            t_send.setdefault(len(dev.sent), asyncio.get_running_loop().time())
            orig(payload)
        dev.write = _w
        await m.set_flow(1, 10.0, owner="chamber1")
        await m.flow_on(1, owner="chamber1")
        await m.sp1_set(5.0, owner="chamber1")
        m.set_process_status(True)
        await _until(lambda: dev.sent.count("R5") >= 1 and dev.sent.count("R60") >= 2, 3.0)
        m.set_process_status(False)
        await _stop(*tasks)
        # 대표 시퀀스: 설정 → ON → SP1 → 폴링(R60, R5) 순서 그대로, low_priority 명령은 하나도 없다
        head = [c for c in dev.sent if not c.startswith("R6")][:3]
        assert head[0].startswith("Q1 ") and head[1] == "L11", dev.sent
        assert "S1" in head[2] or head[2].startswith("S1"), dev.sent
        assert dev.sent.index("R60") < dev.sent.index("R5")
        # 일반 명령끼리 간격은 MFC_GAP_MS(5ms) 이상으로 유지된다
        ts = sorted(t_send.values())
        assert all((b - a) >= 0.004 for a, b in zip(ts, ts[1:]))
    asyncio.run(go())


def test_pop_next_equals_popleft_without_low_priority():
    m = AsyncMFC(enable_verify=False, cfg=FAST)
    a = m._enqueue("R60", None, tag="a")
    b = m._enqueue("R5", None, tag="b")
    c = m._enqueue("L11", None, tag="c", allow_no_reply=True)
    assert [m._pop_next_cmd() for _ in range(3)] == [a, b, c]
    assert not m._cmd_q
    assert a.low_priority is False and a.quiet is False


# ───────────────────────── 2) 우선순위 ─────────────────────────
def test_normal_commands_go_before_queued_low_priority_reads():
    async def go():
        m, dev, evs, tasks = _mfc()
        m._connected = False                                        # 워커가 못 보내게 잠시 막고 대기열을 쌓는다
        await asyncio.sleep(0.03)
        lows = [m._enqueue("R5", None, tag=f"[LOG R5]#{i}", low_priority=True, quiet=True, retries_left=0)
                for i in range(3)]
        n1 = m._enqueue("R60", None, tag="[READ R60]")
        n2 = m._enqueue("L11", None, tag="[FLOW_ON]", allow_no_reply=True)
        assert list(m._cmd_q) == lows + [n1, n2]
        m._connected = True
        await _until(lambda: len(dev.sent) >= 5, 3.0)
        await _stop(*tasks)
        assert dev.sent[:2] == ["R60", "L11"] and dev.sent[2:5] == ["R5", "R5", "R5"]
    asyncio.run(go())


def test_normal_command_extra_wait_bounded_by_one_low_read():
    async def go():
        m, dev, evs, tasks = _mfc()
        dev.answer["R5"] = False                                    # 기록 읽기가 타임아웃까지 간다
        loop = asyncio.get_running_loop()
        fut = loop.create_task(m._read_low_priority_line("R5", tag="[LOG R5]", timeout_ms=100, gap_ms=5,
                                                         expect_prefixes=("P",)))
        await _until(lambda: m._inflight is not None and m._inflight.low_priority, 1.0)
        t0 = loop.time()
        m._enqueue("R60", None, tag="[READ R60]")
        await _until(lambda: "R60" in dev.sent, 2.0)
        waited = loop.time() - t0
        await fut
        await _stop(*tasks)
        assert waited < (0.100 + 0.005) + 0.25, waited              # 진행 중인 기록 읽기 1개(타임아웃) + 그 gap + 여유
    asyncio.run(go())


# ───────────────────────── 3) 기록 읽기 타임아웃 ─────────────────────────
def test_low_priority_timeout_no_retry_no_disconnect_no_mixing():
    async def go():
        m, dev, evs, tasks = _mfc()
        calls = []
        orig = m._on_tcp_disconnected
        m._on_tcp_disconnected = lambda: calls.append(1) or orig()
        dev.answer["R5"] = False
        line = await m._read_low_priority_line("R5", tag="[LOG R5]", timeout_ms=100, gap_ms=5, expect_prefixes=("P",))
        assert line is None
        assert dev.sent.count("R5") == 1 and calls == [] and m._connected is True
        # 늦게 도착한 R5 응답은 다음 일반 명령(R60) 응답에 섞이지 않는다
        m._on_line_from_tcp("P +0.999")
        vals = await m._read_r60_values(tag="[READ R60]")
        await _stop(*tasks)
        assert vals is not None and len(vals) == 3
        assert "P +0.999" not in [e.message for e in evs if e.kind == "status" and e.message and "[RECV]" in e.message]
    asyncio.run(go())


# ───────────────────────── 4) 폴링은 기록에 양보하지 않는다 ─────────────────────────
def test_poll_loop_ignores_low_priority_reads():
    async def go():
        m, dev, evs, tasks = _mfc()
        m._writer = None                                            # 워커만 멈춘다(연결 상태는 유지 → 폴링 루프는 돈다)
        await asyncio.sleep(0.03)
        for i in range(2):
            m._enqueue("R6", None, tag="[LOG R6]", low_priority=True, quiet=True, retries_left=0)
        assert m._has_pending_non_poll_cmds() is False and m._has_pending_poll_reads() is False
        m.set_process_status(True)
        await asyncio.sleep(0.05)
        assert any(c.tag == "[POLL R60]" for c in m._cmd_q)            # 폴링이 양보하지 않고 바로 등록했다
        m._writer = dev
        await _until(lambda: "R60" in dev.sent, 2.0)
        m.set_process_status(False)
        await _stop(*tasks)
        assert dev.sent[0] == "R60"
    asyncio.run(go())


# ───────────────────────── 5) quiet ─────────────────────────
def test_quiet_reads_emit_no_status_or_pressure_events():
    async def go():
        m, dev, evs, tasks = _mfc()
        p, v, at = await m.read_state_quiet(timeout_ms=100, gap_ms=5)
        await asyncio.sleep(0.05)
        await _stop(*tasks)
        assert p == pytest.approx(5.0) and v == pytest.approx(35.2) and isinstance(at, _dt.datetime)
        assert dev.sent == ["R5", "R6"]
        assert not [s for s in _status(evs) if "[SEND]" in s or "[RECV]" in s or "[TIMEOUT]" in s]
        assert not [e for e in evs if e.kind == "pressure"]
    asyncio.run(go())


def test_read_state_quiet_skips_when_normal_pending():
    async def go():
        m, dev, evs, tasks = _mfc()
        m._connected = False
        await asyncio.sleep(0.03)
        m._enqueue("R60", None, tag="[READ R60]")
        p, v, at = await m.read_state_quiet(timeout_ms=100, gap_ms=5)
        m._connected = True
        await _until(lambda: "R60" in dev.sent, 2.0)
        await _stop(*tasks)
        assert (p, v, at) == (None, None, None) and "R5" not in dev.sent and "R6" not in dev.sent
    asyncio.run(go())


# ───────────────────────── 6) CSV ─────────────────────────
def test_csv_rows_blank_on_failure_and_new_file_on_date_change(tmp_path, monkeypatch):
    async def go():
        m, dev, evs, tasks = _mfc()
        wall = _Wall(_dt.datetime(2026, 11, 10, 23, 59, 59))
        monkeypatch.setattr(MFCMOD, "datetime", SimpleNamespace(now=wall.now))
        lg = MfcStateLogger(m, "CH1", cfg=LOG_CFG, wall=wall.now, log_dir=str(tmp_path))
        lg.start(asyncio.get_running_loop())
        await _until(lambda: lg.stats["rows"] >= 2, 3.0)
        dev.answer["R6"] = False                                    # 밸브 읽기 실패 → 빈 칸
        n = lg.stats["rows"]
        await _until(lambda: lg.stats["rows"] >= n + 1 and lg.stats["partial"] >= 1, 3.0)
        dev.answer["R6"] = True
        wall.dt = _dt.datetime(2026, 11, 11, 0, 0, 1)               # 자정 → 새 파일
        n = lg.stats["rows"]
        await _until(lambda: lg.stats["rows"] >= n + 2, 3.0)
        await lg.stop()
        await _stop(*tasks)
    asyncio.run(go())
    files = sorted(p.name for p in tmp_path.glob("CH1_*.csv"))
    assert files == ["CH1_20261110.csv", "CH1_20261111.csv"]
    r1 = _rows(tmp_path / "CH1_20261110.csv")
    assert list(r1[0].keys()) == HEADER
    assert r1[0]["Timestamp"] == "2026-11-10 23:59:59" and r1[0]["Pressure_mTorr"] == "5.000" and r1[0]["Valve_pct"] == "35.2"
    assert any(r["Valve_pct"] == "" and r["Pressure_mTorr"] == "5.000" for r in r1)
    r2 = _rows(tmp_path / "CH1_20261111.csv")
    assert r2 and r2[0]["Timestamp"].startswith("2026-11-11 00:00:01") and r2[0]["Valve_pct"] == "35.2"


def test_csv_timestamp_is_r5_reply_time_not_tick_start(tmp_path, monkeypatch):
    """공정 중 기록 읽기가 앞 명령 뒤로 밀려도 Timestamp 는 실제로 값을 읽은 시각(R5 응답 시각)이다."""
    async def go():
        m, dev, evs, tasks = _mfc()
        wall = _Wall(_dt.datetime(2026, 11, 10, 12, 0, 0))
        monkeypatch.setattr(MFCMOD, "datetime", SimpleNamespace(now=wall.now))
        dev.delay["R5"] = 0.03

        def _advance(cmd):
            if cmd == "R5":
                wall.dt = _dt.datetime(2026, 11, 10, 12, 0, 2)       # 응답이 오는 시점에는 2초 지났다
        dev.before_reply = _advance
        lg = MfcStateLogger(m, "CH2", cfg=LOG_CFG, wall=wall.now, log_dir=str(tmp_path))
        lg.start(asyncio.get_running_loop())
        await _until(lambda: lg.stats["rows"] >= 1, 3.0)
        await lg.stop()
        await _stop(*tasks)
    asyncio.run(go())
    rows = _rows(next(tmp_path.glob("CH2_*.csv")))
    assert rows[0]["Timestamp"] == "2026-11-10 12:00:02"


def test_csv_no_row_when_both_fail_or_disconnected(tmp_path):
    async def go():
        m, dev, evs, tasks = _mfc()
        dev.answer["R5"] = dev.answer["R6"] = False
        lg = MfcStateLogger(m, "CH1", cfg=LOG_CFG, wall=lambda: _dt.datetime(2026, 11, 10, 9, 0, 0),
                            log_dir=str(tmp_path))
        lg.start(asyncio.get_running_loop())
        await _until(lambda: lg.stats["empty"] >= 2, 3.0)
        m._connected = False
        await _until(lambda: lg.stats["disconnected"] >= 2, 3.0)
        await lg.stop()
        await _stop(*tasks)
        assert lg.stats["rows"] == 0
    asyncio.run(go())
    assert list(tmp_path.glob("*.csv")) == []


# ───────────────────────── 7) 연결 유지 ─────────────────────────
def test_revive_after_cleanup_not_during_closing_not_after_stop(tmp_path, monkeypatch):
    async def go():
        m = AsyncMFC(enable_verify=False, enable_stabilization=True, cfg=FAST)
        dev = Dev(m)

        async def fake_wd():                                        # 실제 TCP 대신 가짜 장비에 '연결'
            m._connected = True
            m._ever_connected = True
            m._writer = dev
            while m._want_connected:
                await asyncio.sleep(0.05)
        m._watchdog_loop = fake_wd
        clk = {"t": 1000.0}
        probes = []

        async def probe():
            probes.append(1)
            return True
        lg = MfcStateLogger(m, "CH1", cfg=LOG_CFG, clock=lambda: clk["t"], log_dir=str(tmp_path))
        lg._probe = probe
        lg.start(asyncio.get_running_loop())
        # (a) 처음 내려가 있음 → REVIVE_AFTER_S 전에는 올리지 않는다
        await asyncio.sleep(0.2)
        assert m._watchdog_task is None and lg.stats["revive"] == 0
        clk["t"] += MSL.REVIVE_AFTER_S + 1
        await _until(lambda: m.is_connected(), 2.0)
        assert lg.stats["revive"] == 1 and probes == [1]
        await _until(lambda: lg.stats["rows"] >= 1, 2.0)
        # (b) 공정 종료 정리처럼 cleanup → 내려감 → 5초 뒤 다시 올림
        await m.cleanup()
        assert m._watchdog_task is None and not m.is_connected()
        await asyncio.sleep(0.2)
        assert lg.stats["revive"] == 1                              # 아직
        clk["t"] += MSL.REVIVE_AFTER_S + 1
        await _until(lambda: m.is_connected(), 2.0)
        assert lg.stats["revive"] == 2
        # (c) cleanup 진행 중(_closing) 에는 끼어들지 않는다
        await m.cleanup()
        m._closing = True
        clk["t"] += MSL.REVIVE_AFTER_S + 1
        await asyncio.sleep(0.2)
        assert lg.stats["revive"] == 2 and m._watchdog_task is None
        m._closing = False
        # (d) stop() 뒤에는 다시 올리지 않는다
        await lg.stop()
        clk["t"] += MSL.REVIVE_AFTER_S + 1
        await asyncio.sleep(0.2)
        assert lg.stats["revive"] == 2 and m._watchdog_task is None
        await m.cleanup()
    asyncio.run(go())


def test_revive_backs_off_when_probe_fails(tmp_path):
    async def go():
        m = AsyncMFC(enable_verify=False, cfg=FAST)
        started = []
        m.start = lambda: started.append(1) or asyncio.sleep(0)
        clk = {"t": 0.0}
        lg = MfcStateLogger(m, "CH1", cfg=LOG_CFG, clock=lambda: clk["t"], log_dir=str(tmp_path))

        async def probe():
            return False
        lg._probe = probe
        lg.start(asyncio.get_running_loop())
        await asyncio.sleep(0.15)                                   # 첫 tick 이 '내려감' 시작 시각을 잡게
        clk["t"] += MSL.REVIVE_AFTER_S + 1
        await _until(lambda: lg.stats["probe_fail"] >= 1, 2.0)
        await asyncio.sleep(0.2)
        assert lg.stats["probe_fail"] == 1 and started == []        # 백오프 동안 다시 확인하지 않고 start 도 없다
        clk["t"] += MSL.PROBE_BACKOFF_S[0] + 1
        await _until(lambda: lg.stats["probe_fail"] >= 2, 2.0)
        await lg.stop()
        assert started == []
    asyncio.run(go())


# ───────────────────────── 8) 예외 격리 ─────────────────────────
def test_logger_exceptions_do_not_propagate(tmp_path):
    async def go():
        m, dev, evs, tasks = _mfc()

        async def boom(**k):
            raise RuntimeError("boom")
        m.read_state_quiet = boom
        lg = MfcStateLogger(m, "CH1", cfg=LOG_CFG, log_dir=str(tmp_path))
        lg.start(asyncio.get_running_loop())
        await _until(lambda: lg.stats["exc"] >= 3, 3.0)
        assert lg._task is not None and not lg._task.done()
        # 파일 쓰기 예외도 삼킨다
        del m.read_state_quiet

        async def bad_thread(fn, *a, **k):
            raise OSError("disk")
        lg._to_thread = bad_thread
        await _until(lambda: lg.stats["write_failed"] >= 2, 3.0)
        assert not lg._task.done() and m._connected is True
        lg._to_thread = asyncio.to_thread
        await lg.stop()
        await _stop(*tasks)
    asyncio.run(go())


def test_summary_written_only_when_problems(tmp_path):
    async def go():
        m, dev, evs, tasks = _mfc()
        clk = {"t": 0.0}
        lg = MfcStateLogger(m, "CH1", cfg=LOG_CFG, clock=lambda: clk["t"],
                            wall=lambda: _dt.datetime(2026, 11, 10, 9, 0, 0), log_dir=str(tmp_path))
        lg.start(asyncio.get_running_loop())
        await _until(lambda: lg.stats["rows"] >= 2, 3.0)
        clk["t"] += MSL.SUMMARY_PERIOD_S + 1                       # 문제 없음 → 요약 없음
        await _until(lambda: lg._last_summary_t >= MSL.SUMMARY_PERIOD_S, 2.0)
        await asyncio.sleep(0.1)
        assert list(tmp_path.glob("*.log")) == []
        dev.answer["R5"] = dev.answer["R6"] = False
        await _until(lambda: lg.stats["empty"] >= 2, 3.0)
        clk["t"] += MSL.SUMMARY_PERIOD_S + 1
        await _until(lambda: list(tmp_path.glob("CH1_20261110.log")), 3.0)
        await lg.stop()
        await _stop(*tasks)
    asyncio.run(go())
    txt = (tmp_path / "CH1_20261110.log").read_text(encoding="utf-8").splitlines()
    assert len(txt) == 1 and "empty" in txt[0] and "[CH1]" in txt[0]


# ───────────────────────── settings.json ─────────────────────────
def test_repo_settings_json_has_keys_and_passes_strict_check():
    from lib import _config_loader as L
    from lib import config_common, config_ch1, config_ch2
    mods = (config_common, config_ch1, config_ch2)
    snap = [dict(vars(md)) for md in mods]
    try:
        res = L.check_and_load_settings(Path(_ROOT) / "config" / "settings.json")
        assert not res.problems, L.format_problems(res)
        assert config_common.MFC_STATE_LOG_ENABLED is True
        assert config_common.MFC_STATE_LOG_INTERVAL_S == 1.0 and config_common.MFC_STATE_LOG_GAP_MS == 100
        assert config_common.MFC_STATE_LOG_TIMEOUT_MS == 500
        assert config_common.MFC_STATE_LOG_DIR == r"C:\VanaM_Logs\CH1&2\CH1&2_MFC"
        import re as _re                                            # ch1/ch2 섹션에는 넣지 않는다(common 한 곳만)
        text = (Path(_ROOT) / "config" / "settings.json").read_text(encoding="utf-8-sig")
        assert len(_re.findall(r'"MFC_STATE_LOG_ENABLED"', text)) == 1
        assert text.index('"MFC_STATE_LOG_ENABLED"') < text.index('"ch1"')
    finally:
        for md, d in zip(mods, snap):
            for k in [k for k in vars(md) if k not in d]:
                delattr(md, k)
            for k, v in d.items():
                setattr(md, k, v)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
