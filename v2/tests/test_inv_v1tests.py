"""v1 시험(tests/ 93개 파일·904 함수 — 전수 목록 영역 test, v2/ch12/spec/inv_test.py)이 고정하던 기대값 가운데
v2 시험에 아직 옮겨지지 않은 것을 옮긴 곳.

규칙
- v1 을 import 하지 않는다(test_isolation). 기대값(문구·숫자)은 v1 시험·코드에서 그대로 옮기고 근거를 'v1 파일:줄' 로 적는다.
- 시험 이름 = test_<항목 번호>_<무엇>(예 test_t019_dc_pulse_overcurrent_card) — 항목은 inv_test.py 의 TEST-<번호>.
- 대역(fake)은 v2 모듈이 실제로 부르는 속성만 갖춘다. 장비·파일은 가상 시계·시뮬레이터·tmp_path 만 쓴다.
"""
from __future__ import annotations

import os
import re
from pathlib import Path
from types import SimpleNamespace

import pytest


# ═════════════════════════ 공통 대역 ═════════════════════════
class _Eng:
    """ProcessEventsMixin 이 보는 엔진 자리 — chat·run_warnings·stats·busy."""

    def __init__(self):
        self.chats, self.run_warnings, self.busy, self.run_id, self.params = [], [], True, "r1", {}

        class _St:
            def __getattr__(self, _n):
                return lambda *a, **k: None
        self.stats = _St()

    def chat(self, text):
        self.chats.append(text)

    def ig_in_use_by_pc(self):
        return False


class _Runner:
    def __init__(self):
        self.calls = []

    def __getattr__(self, name):
        return lambda *a, **k: self.calls.append((name, a, k))


def _events_host(n=2):
    from v2.ch12.process_events import ProcessEventsMixin

    class H(ProcessEventsMixin):
        pass
    h = H()
    h.n, h.e, h.runner, h.owner = n, _Eng(), _Runner(), f"chamber{n}"
    h.logs, h.fails, h.matches = [], [], []
    h.dc_failed = {1: False, 2: False}
    h.log = lambda src, msg: h.logs.append((src, msg))
    h._fail = lambda src, why, **k: h.fails.append((src, why, k))
    h._match = lambda *a: h.matches.append(a)
    return h


def _ev(**kw):
    base = dict(cmd=None, detail=None, reason=None, data={}, code=None, owner=None)
    base.update(kw)
    return SimpleNamespace(**base)


# ═════════════ TEST-019 전원 이상 공정 중단 카드 문구(v1 tests/test_dc_overcurrent.py:411-468) ═════════════
def test_t019_dc_pulse_overcurrent_low_current_and_other_cards():
    """runtime/chamber_runtime.py:2755-2762 — AUTO_STOP 사유별 머리말 + 사유 원문, 공정 실패로 넘김(on_dc_pulse_failed)."""
    reason = "overcurrent: I=2.710A > 1.000A (설정 190W 기준, 1회 연속)"
    h = _events_host(2)
    h._ev_dcp("command_failed", _ev(cmd="AUTO_STOP", detail=reason))
    assert len(h.e.chats) == 1
    msg = h.e.chats[0]
    assert "과전류" in msg and reason in msg and "CH2" in msg
    assert msg == f"⚠️ CH2 DC Pulse 과전류 차단 → 전체 공정 중단 | {reason}"
    assert [c[0] for c in h.runner.calls] == ["step_failed"]
    h = _events_host(2)
    h._ev_dcp("command_failed", _ev(cmd="AUTO_STOP", detail="low_current: I <= 0.050A (3회 연속)"))
    assert "저전류" in h.e.chats[0] and "과전류" not in h.e.chats[0]
    h = _events_host(2)
    h._ev_dcp("command_failed", _ev(cmd="AUTO_STOP", detail="target_failed"))
    assert "세트포인트 이탈(연속) 또는 P=0W 감지" in h.e.chats[0] and "target_failed" in h.e.chats[0]


@pytest.mark.parametrize("unit", ["DC1", "DC2"])
def test_t019_dc_overcurrent_and_low_power_cards(unit):
    """chamber_runtime.py:2374-2440 — DC/DC2 target_failed: 과전류면 '과전류 차단', 저출력이면 '출력 이상'(과전류 문구 없음);
    공정 실패의 첫 인자는 원래 메시지 그대로, 실패 플래그(_dc_failed_flag) 켜짐."""
    m = "DC 과전류: meas=1.500A > 한계 1.000A (목표 300.0W 기준, 1회 연속) → 공정 중단 | I_scale=0.00097405 (raw≈1540)"
    h = _events_host(1)
    h._ev_dc("target_failed", _ev(detail=m, data={"unit": unit}))
    u = 1 if unit == "DC1" else 2
    assert h.dc_failed[u] is True and len(h.e.chats) == 1
    assert "과전류 차단" in h.e.chats[0] and m in h.e.chats[0]
    assert h.fails[0][0] == ("DC Power" if u == 1 else "DC2 Power") and h.fails[0][1] == m
    m2 = "저전력 연속 3/3 (meas=0.0W ≤ 1.0W)"
    h = _events_host(1)
    h._ev_dc("target_failed", _ev(detail=m2, data={"unit": unit}))
    assert "출력 이상" in h.e.chats[0] and "과전류" not in h.e.chats[0] and m2 in h.e.chats[0]


# ═════════════ TEST-040 next_step_message 는 읽기 전용(v1 tests/test_heater_cards.py:318) ═════════════
def test_t040_next_step_message_read_only():
    """controller/process_controller.py next_step_message — 지금 스텝 다음 것의 message, 마지막이면 ''. 위치는 그대로."""
    from v2.ch12.process_engine import ProcessEngine
    from v2.ch12.process_steps import ProcessStep
    steps = [ProcessStep(action="HEATER_WAIT", message="히터 승온 대기", value=200.0),
             ProcessStep(action="DELAY", message="다음 스텝", duration=1000)]
    fake = SimpleNamespace(runner=SimpleNamespace(steps=steps, idx=0))
    assert ProcessEngine._next_step_message(fake) == "다음 스텝" and fake.runner.idx == 0
    fake.runner.idx = 1
    assert ProcessEngine._next_step_message(fake) == ""


# ═════════════ TEST-053·167 엑셀 히터·두께 그룹 머리글(v1 tests/test_heater_process_logs.py:135, test_thickness_logs.py:74) ═════════════
def _ws_with(cols, grp, starts):
    from openpyxl import Workbook
    from v2.ch12.outputs import excel_style as XS
    wb = Workbook()
    ws = wb.active
    XS._build_headers(ws, cols, grp, starts)
    return wb, ws


def test_t053_t167_excel_heater_then_thickness_groups():
    """util/gdrive_logger.py:51-180 — CH2 = 38열 + Heater 5열(39~43, 'SP Temp…Avg DAC', 단위 °C·count) + Thickness 11열(44~54,
    그룹 머리 AR1 'Thickness', 'Material' 53열·'Factor Source' 54열). CH1 은 38열 그대로(두께 없을 때)."""
    from v2.ch12.outputs import excel_style as XS
    c1, _, _ = XS._main_cols(1)
    c2, g2, _ = XS._main_cols(2)
    c2t, g2t, s2t = XS._main_cols(2, thickness=True)
    assert len(c1) == 38 and len(c2) == 43 and len(c2t) == 54
    assert [c[0] for c in c2[38:]] == ["SP Temp", "Avg Temp", "Min Temp", "Max Temp", "Avg DAC"]
    assert [c[1] for c in c2[38:]] == ["°C", "°C", "°C", "°C", "count"]
    assert g2["Heater"] == [39, 40, 41, 42, 43] and g2t["Thickness"] == list(range(44, 55))
    assert c2t[52][0] == "Material" and c2t[53][0] == "Factor Source"
    wb, ws = _ws_with(c2t, g2t, s2t)
    assert ws["AM1"].value == "Heater" and ws["AR1"].value == "Thickness"
    assert "AM1:AQ1" in [str(r) for r in ws.merged_cells.ranges] and "AR1:BB1" in [str(r) for r in ws.merged_cells.ranges]


def test_t053_t167_old_files_get_only_new_headers():
    """gdrive_logger.py:276-318 _extend_headers — 옛 38열 CH2 파일 → 히터·두께 머리글만 덧붙임(기존 행 그대로), 히터까지(43열)
    파일 → 두께 11열, Material·Factor Source 없던 옛 Thickness 그룹(52열) → 그 2열만; CH1·머리글이 안 맞는 파일은 0."""
    from v2.ch12.outputs import excel_style as XS
    c1, g1, s1 = XS._main_cols(1)
    c2, g2, s2 = XS._main_cols(2)
    c2t, g2t, s2t = XS._main_cols(2, thickness=True)
    wb, ws = _ws_with(c1, g1, s1)                       # 옛 CH2 파일(38열) + 데이터 행 1개
    ws.cell(row=4, column=1, value="old row")
    assert XS._extend_headers(ws, c2, g2, s2) == 5
    assert [ws.cell(row=2, column=c).value for c in range(39, 44)] == ["SP Temp", "Avg Temp", "Min Temp", "Max Temp", "Avg DAC"]
    assert ws.cell(row=1, column=39).value == "Heater" and ws.cell(row=4, column=1).value == "old row"
    assert XS._extend_headers(ws, c2t, g2t, s2t) == 11 and ws.cell(row=1, column=44).value == "Thickness"
    assert ws.cell(row=4, column=44).value is None
    old_thk = c2t[:52]                                     # Material·Factor Source 없던 두께 그룹
    wb, ws = _ws_with(old_thk, *XS._group_index(old_thk))
    assert XS._extend_headers(ws, c2t, g2t, s2t) == 2
    assert ws.cell(row=2, column=53).value == "Material" and ws.cell(row=2, column=54).value == "Factor Source"
    wb, ws = _ws_with(c1, g1, s1)
    assert XS._extend_headers(ws, c1, g1, s1) == 0          # CH1 은 그대로
    ws.cell(row=2, column=5, value="손으로 고친 머리글")
    assert XS._extend_headers(ws, c2, g2, s2) == 0          # 앞부분이 다르면 손대지 않음


# ═════════════ TEST-004 Calib CSV 는 기록할 때만 열고 바로 닫음(v1 tests/test_calib_csv_release.py:36) ═════════════
def _open_paths():
    out = set()
    for fd in os.listdir("/proc/self/fd"):
        try:
            out.add(os.readlink(f"/proc/self/fd/{fd}"))
        except OSError:
            pass
    return out


@pytest.mark.skipif(not Path("/proc/self/fd").is_dir(), reason="리눅스 /proc 필요")
def test_t004_calib_csv_not_held_open(tmp_path):
    """controller/data_logger.py:156-163·472 — 시작·기록 뒤 파일 손잡이가 열려 있지 않다(G: 가 열린 파일을 올리지 않음).
    v2 는 D07(로컬 정본 먼저 → 사본 스레드가 G: 로) — 두 파일 모두 기록 뒤 닫혀 있다. 로그 '1행 기록 완료 (NAS)'."""
    from v2.ch12.outputs.base import QueueWriter
    from v2.ch12.outputs.calib_csv import CalibCsv
    from v2.ch12.outputs.paths import CopySender
    from v2.ch12.outputs.run_stats import RunStats
    from v2.tests.outputs_golden import Clock, make_root, v2_cfg
    from v2.tools import v1golden_outputs_scen as SC
    root = make_root(tmp_path)
    cfg = v2_cfg(root, SC.BASE_CFG)
    w, s, logs = QueueWriter("t"), CopySender(cfg, retry_s=0.05), []
    c = CalibCsv(1, cfg, log_func=logs.append, writer=w, sender=s)
    try:
        w.flush()
        assert not [p for p in _open_paths() if p.endswith("_log.csv")]
        for t in ("2026-10-30T11:00:00", "2026-10-30T11:10:00"):
            st = RunStats(now=Clock(t).now)
            st.apply_events([["start", dict(SC.P_CH1, t0_wall=t)]])
            c.record(st, True)
            w.flush()
            assert s.flush(2.0)
            assert not [p for p in _open_paths() if p.endswith("_log.csv") or p.endswith("_pending.csv")]
        files = list(root.rglob("Ch1_log.csv"))
        assert len(files) == 2 and all(f.read_bytes().count(b"\r\n") == 3 for f in files)
        assert any("1행 기록 완료 (NAS)" in x for x in logs)
    finally:
        w.stop()
        s.stop()


# ═════════════════════════ DC Pulse·DC 과전류(v1 tests/test_dc_overcurrent*.py, test_dcp_*.py) ═════════════════════════
class _Clk:
    def __init__(self, t=1000.0):
        self.t = t

    def monotonic(self):
        return self.t


def _dcp(cfg=None, chamber="ch2", t=1000.0):
    """I/O 없이 판정 mixin 만 쓰는 DC Pulse 드라이버(worker 없음)."""
    from v2.ch12.drivers import dcpulse as D
    from v2.ch12.params import dcpulse as PD
    from v2.common.paramdef import defaults
    ev = []
    base = dict(defaults(PD.PARAMS), DC_OVERCURRENT_ENABLE=True, DC_OVERCURRENT_P1_W=300.0, DC_OVERCURRENT_I1_A=1.0,
                DC_OVERCURRENT_P2_W=600.0, DC_OVERCURRENT_I2_A=2.0, DC_OVERCURRENT_STREAK_N=1,
                DC_OVERCURRENT_EXTRAPOLATE=True)
    d = D.DcPulseDriver(_Clk(t), None, dict(base, **(cfg or {})), True, on_event=ev.append, chamber=chamber)
    d.ev = ev
    return d


def _st(d):
    return [e.detail for e in d.ev if e.kind == "status"]


def _res(p, i, v=None, raw=None):
    v = (p / i) if (v is None and i) else (v or 0.0)
    return {"raw": raw or {"P": int(p / 10), "I": int(round(i * 100)), "V": int(v)}, "eng": {"P_W": p, "V_V": v, "I_A": i}}


def test_t015_overcurrent_limit_both_implementations():
    """lib/power_limits.py:20 dc_overcurrent_limit_a (v1 tests/test_dc_overcurrent.py:39-66) — DC Pulse(proto)·DC 연속(dc) 같은 값."""
    from v2.ch12.drivers import dc as DC
    from v2.ch12.drivers import dcpulse_proto as P
    lim = lambda w, **k: P.overcurrent_limit_a(w, k.get("p1", 300.0), k.get("i1", 1.0), k.get("p2", 600.0),  # noqa: E731
                                                k.get("i2", 2.0), k.get("ext", True))
    assert [lim(p) for p in (0, 100, 300)] == [1.0, 1.0, 1.0]
    assert lim(450) == pytest.approx(1.5) and lim(600) == pytest.approx(2.0)
    assert lim(900) == pytest.approx(3.0) and lim(900, ext=False) == pytest.approx(2.0)
    assert lim(450, p1=600, p2=300) == 1.0 and lim(450, p1=300, p2=300) == 1.0
    assert lim(450, i2=float("nan")) == 1.0 and lim(450, i1=-5, i2=-1) == 1.0 and lim("abc") == 1.0
    assert lim(None) == 1.0 and lim(-50) == 1.0 and lim(None, i1=0.7) == 0.7
    cfg = {"DC_OVERCURRENT_P1_W": 300.0, "DC_OVERCURRENT_I1_A": 1.0, "DC_OVERCURRENT_P2_W": 600.0,
           "DC_OVERCURRENT_I2_A": 2.0, "DC_OVERCURRENT_EXTRAPOLATE": True}
    for w in (None, -50, 0, 300, 450, 600, 900):
        assert DC.dc_overcurrent_limit_a(cfg, w) == pytest.approx(lim(w))
    assert DC.dc_overcurrent_limit_a(dict(cfg, DC_OVERCURRENT_EXTRAPOLATE=False), 900) == pytest.approx(2.0)


def test_t016_pulse_overcurrent_check_rules():
    """device/dc_pulse.py:2485-2535 _check_overcurrent (v1 tests/test_dc_overcurrent.py:122-192)."""
    d = _dcp()
    d._last_ref_power_w = 190.0
    out = d._mon_cycle(_res(190.0, 2.71, 70.0, {"P": 19, "I": 271, "V": 70}), 1000.0)
    assert out is not None and out[0].startswith("overcurrent") and "설정 190W" in out[0] and "1회 연속" in out[0]
    assert out[1] == "[AUTO-STOP] 과전류 → OUTPUT_OFF & stop polling"
    assert any(s.startswith("[overcurrent]") and "raw P/I/V=19/271/70" in s for s in _st(d))
    d = _dcp()
    d._last_ref_power_w = 190.0
    assert d._mon_overcurrent(190.0, 211.0, 0.9, None) is None
    d._last_ref_power_w = 600.0
    assert d._mon_overcurrent(600.0, 300.0, 1.9, None) is None and d._mon_overcurrent(600.0, 300.0, 2.1, None)
    d = _dcp()                                       # 점화 고속창 안에서도 판정(창 안 저전류·이탈은 멈춤)
    d._last_ref_power_w, d._arc_run_start_ts = 190.0, 999.0
    assert d._in_arc_fast_window(1000.0) and d._mon_cycle(_res(190.0, 2.71), 1000.0) is not None
    d = _dcp({"DC_OVERCURRENT_ENABLE": False})
    d._last_ref_power_w = 190.0
    assert d._mon_overcurrent(190.0, 70.0, 2.71, None) is None
    d = _dcp({"DC_OVERCURRENT_STREAK_N": 2})
    d._last_ref_power_w = 190.0
    assert d._mon_overcurrent(190.0, 70.0, 2.71, None) is None and d._overcurr_n == 1
    assert "[WARN] 과전류 감지: I=2.710 A > 1.000 A (1/2)" in _st(d)
    why = d._mon_overcurrent(190.0, 70.0, 2.71, None)
    assert d._overcurr_n == 2 and "2회 연속" in why
    d._overcurr_n = 1
    assert d._mon_overcurrent(190.0, 70.0, 0.5, None) is None and d._overcurr_n == 0      # 정상 표본 → 0
    d._overcurr_n = 1
    d._mon_overcurrent(190.0, 70.0, float("nan"), None)
    d._mon_overcurrent(190.0, 70.0, None, None)
    assert d._overcurr_n == 1                                                             # NaN·None 은 그대로


def test_t016_pulse_overcurrent_trip_in_polling_sends_one_off():
    """폴링(점화 감시 끔)에서 과전류 → AUTO_STOP 'overcurrent' 1회 + OUTPUT_OFF 프레임 1개 + '[AUTO-STOP] 과전류'."""
    from v2.tests.test_dcpulse import Rig
    r = Rig(cfg={"DC_OVERCURRENT_ENABLE": True, "DC_OVERCURRENT_ARM_ON_START": False})
    assert r.start(190.0, freq=50, off_time_us=5.0) == (None, True)
    n0 = len(r.sim.received)
    r.sim.i_override_a = 2.71
    r.rows(10)
    stop = [e for e in r.ev("command_failed") if e.cmd == "AUTO_STOP"]
    assert len(stop) == 1 and stop[0].detail.startswith("overcurrent: I=2.710A > 1.000A (설정 190W 기준, 1회 연속)")
    assert any(e.detail == "[AUTO-STOP] 과전류 → OUTPUT_OFF & stop polling" for e in r.ev("status"))
    assert r.sent(n0).count("02 80 00 02 03 87") == 1 and r.sim.out is False
    r.done()


def test_t018_t032_activation_watch_arm_off_and_on():
    """dc_pulse.py:2537-2605 _watch_overcurrent_during_activation (v1 tests/test_dc_overcurrent.py:310-398,
    test_dcp_arc_log_raw.py:239) — ARM 끔: 대기 5 s 동안 PIV(0x9A) 안 읽음, 'ON 직후' SAT/ANT 기록은 함;
    ARM 켬: '활성화 대기 5.0초 구간 감시 시작 (0.5초 간격)', 0.5 s 간격 ≤ 10회, '[telemetry·점화] … (raw P/I/V=…) +0.0 s' 로그만
    (telemetry 이벤트 아님)."""
    from v2.tests.test_dcpulse import Rig
    r = Rig(cfg={"DC_OVERCURRENT_ENABLE": True, "DC_OVERCURRENT_ARM_ON_START": False})
    assert r.start(100.0, freq=50, off_time_us=5.0) == (None, True)
    fr = [bytes.fromhex(f) for f in r.sent()]
    i_on = max(i for i, b in enumerate(fr) if b[1] == 0x80 and b[3] == 1)
    assert not any(b[1] == 0x9A for b in fr[i_on:])
    st = [e.detail for e in r.ev("status")]
    assert any(s.startswith("[arc] ON 직후 장비 SAT=") for s in st) and not any("활성화 대기" in s for s in st)
    r.done()
    r = Rig(cfg={"DC_OVERCURRENT_ENABLE": True})
    t0 = r.c.monotonic()
    assert r.start(100.0, freq=50, off_time_us=5.0) == (None, True)
    st = [e.detail for e in r.ev("status")]
    assert "[overcurrent] 활성화 대기 5.0초 구간 감시 시작 (0.5초 간격)" in st
    tl = [s for s in st if s.startswith("[telemetry·점화]")]
    assert 1 <= len(tl) <= 10 and re.search(r" \+0\.\d s$", tl[0]) and "(raw P/I/V=10/25/400)" in tl[0]
    assert not any(e.kind == "telemetry" for e in r.events)
    assert r.c.monotonic() - t0 < 30.0
    r.done()


def test_t020_overcurrent_keys_are_common_only():
    """lib/_config_loader.py:474 + dc_pulse.py:404-411 — 과전류 설정은 공통 키만(채널 키는 보지 않음): CH2 드라이버도 공통
    ENABLE=false 면 끔, STREAK_N=2 면 첫 초과는 '(1/2)' 경고. DC 연속도 공통 ENABLE_CONT."""
    d = _dcp({"DC_OVERCURRENT_ENABLE": False, "CH2_DC_OVERCURRENT_ENABLE": True}, chamber="ch2")
    d._last_ref_power_w = 190.0
    assert d._mon_overcurrent(190.0, 70.0, 2.71, None) is None
    assert d._overcurrent_arm_status().startswith("[overcurrent] ⚠ 인터락 OFF (settings.json common.DC_OVERCURRENT_ENABLE=false)")
    d = _dcp({"DC_OVERCURRENT_STREAK_N": 2, "CH2_DC_OVERCURRENT_STREAK_N": 1}, chamber="ch2")
    d._last_ref_power_w = 190.0
    assert d._mon_overcurrent(190.0, 70.0, 2.71, None) is None and "(1/2)" in _st(d)[-1]


def test_t021_pulse_start_log_line():
    """dc_pulse.py:436 공정 시작 1줄 — '[overcurrent] 인터락 ON — 한계 1.50A (설정 450W, 연속 2회, 점화 감시 OFF)'."""
    d = _dcp({"DC_OVERCURRENT_STREAK_N": 2, "DC_OVERCURRENT_ARM_ON_START": False})
    d._last_ref_power_w = 450.0
    assert d._overcurrent_arm_status() == "[overcurrent] 인터락 ON — 한계 1.50A (설정 450W, 연속 2회, 점화 감시 OFF)"
    from v2.tests.test_dcpulse import Rig
    r = Rig(cfg={"DC_OVERCURRENT_ENABLE": False})
    assert r.start(100.0, freq=50, off_time_us=5.0) == (None, True)
    oc = [e.detail for e in r.ev("status") if e.detail.startswith("[overcurrent]") and "인터락" in e.detail]
    assert len(oc) == 1 and "인터락 OFF (settings.json common.DC_OVERCURRENT_ENABLE=false)" in oc[0]
    r.done()


def _dc_rig(cfg):
    from v2.ch12.sim.plc_rig import PlcRig
    return PlcRig(dc=True, cfg=dict({"CH2_SUPPORTS_HEATER": False}, **cfg))


def test_t017_t021_dc_cont_overcurrent_disabled_and_start_line():
    """device/dc_power.py:289·306 — ENABLE_CONT=false 면 300 W·1.5 A 에도 계속(target_failed 없음), 시작 1줄
    '[overcurrent] ⚠ 인터락 OFF (settings.json common.DC_OVERCURRENT_ENABLE_CONT=false) …'. 켜져 있을 때의 차단 문구·정리 1회는
    v1 실행 골든(dev_plc.json dc_overcurrent_trip — test_dc.py::test_dc_matches_v1_golden)."""
    from v2.tests.test_dc import run_script
    r = _dc_rig({"DC_OVERCURRENT_ENABLE_CONT": False})
    run_script(r, r.dc1, 300.0, [(900, 1540)] * 3, (0, 1))
    assert not [e for e in r.pevents if e.kind == "target_failed" and "과전류" in (e.detail or "")]
    st = [e.detail for e in r.pevents if e.kind == "status" and e.data.get("unit") == "DC1"]
    assert any(s.startswith("[overcurrent] ⚠ 인터락 OFF (settings.json common.DC_OVERCURRENT_ENABLE_CONT=false)") for s in st)
    r.done()


def test_t022_cleanup_rules():
    """dc_pulse.py:544-600 cleanup (v1 tests/test_dc_pulse_cleanup_off.py:94-230) — 켜진 출력이면 '정리 전 DC Pulse 출력 OFF 확인'
    + OFF 1회, 진행 중 OFF 가 있으면 새로 안 보내고 기다림, 워커 없는(이번 런에 안 쓴) 장치는 아무것도 안 보냄·경고 없음."""
    from v2.tests.test_dcpulse import Rig
    r = Rig()
    assert r.start(100.0, freq=50, off_time_us=5.0) == (None, True)
    n0 = len(r.sim.received)
    r.d.output_off()                                      # 진행 중 OFF
    box = []
    r.d.cleanup(on_done=lambda u: (box.append(u), r.c.notify()))
    r.c.wait_until(None, lambda: bool(box))
    assert box == [False] and r.sent(n0).count("02 80 00 02 03 87") == 1
    assert not any(e.detail == "정리 전 DC Pulse 출력 OFF 확인" for e in r.ev("status"))
    r.done()
    r = Rig()
    assert r.start(100.0, freq=50, off_time_us=5.0) == (None, True)
    assert r.d._output_maybe_on is True                    # OUTPUT_ON 쓰기 직전 True — OFF 확인 때까지 유지
    box = []
    r.d.cleanup(on_done=lambda u: (box.append(u), r.c.notify()))
    r.c.wait_until(None, lambda: bool(box))
    assert any(e.detail == "정리 전 DC Pulse 출력 OFF 확인" for e in r.ev("status")) and box == [False]
    assert r.d._output_maybe_on is False                   # OFF 확인 → False
    r.done()
    d = _dcp()
    d._out_on = True
    got = []
    d.cleanup(on_done=got.append)
    assert got == [False] and d.ev == [] and d.cleanup_off_unconfirmed is False


def test_t023_confirm_off_needs_hv_off_when_status_readable():
    """dc_pulse.py:2047 _confirm_off_quick — STATUS 를 읽었고 HV On 이면 P=0 이어도 꺼짐 아님, STATUS 를 못 읽을 때만 P==0 보조판정."""
    d = _dcp()

    def run(flags, p):
        def g_flags(kind):
            return flags
            yield  # noqa: B901 — 생성기

        def g_piv(kind):
            return {"eng": {"P_W": p, "V_V": 0.0, "I_A": 0.0}}
            yield
        d._g_read_status_flags, d._g_read_piv = g_flags, g_piv
        g = d._g_confirm_off_quick("safety")
        try:
            next(g)
        except StopIteration as e:
            return e.value
    assert run(0x0001, 0.0) is False                      # HV On — P=0 이어도 아님
    assert run(0x0000, 50.0) is True                      # HV Off
    assert run(None, 0.0) is True and run(None, 5.0) is False


def test_t024_t029_arc_reset_reads_before_on_and_labels():
    """dc_pulse.py:678·1219·1486·1570 — SAT/ANT 2쌍(0x8C 리셋 전·후)을 OUTPUT_ON 전에 읽고 '[arc] 0x8C 리셋: … (동작=…)',
    옛 '[arc] baseline SAT=' 줄 없음; 3값 판정(15050→0·15050→12 예, 15050→15050 아니오, 못 읽음 확인불가), arc_summary 의
    reset_state·reset_ok(= yes 일 때만)."""
    from v2.ch12.drivers import dcpulse_proto as P
    assert P.arc_reset_state(15050, 0, 0, 0) == "yes" and P.arc_reset_state(15050, 0, 12, 0) == "yes"
    assert P.arc_reset_state(15050, 0, 15050, 0) == "no" and P.arc_reset_state(None, None, 0, 0) == "unknown"
    assert P.ARC_RESET_LABEL == {"yes": "예", "no": "아니오", "unknown": "확인불가"}
    from v2.tests.test_dcpulse import Rig
    r = Rig()
    r.sim.sat = 15050.0
    assert r.start(100.0, freq=50, off_time_us=5.0) == (None, True)
    fr = [bytes.fromhex(f) for f in r.sent()]
    i_on = min(i for i, b in enumerate(fr) if b[1] == 0x80 and len(b) == 6 and b[3] == 1)
    i_rst = min(i for i, b in enumerate(fr) if b[1] == 0x8C)
    pre = [b[1] for b in fr[:i_rst] if b[1] in (0x96, 0x99)]
    post = [b[1] for b in fr[i_rst:i_on] if b[1] in (0x96, 0x99)]
    assert pre[-2:] == [0x96, 0x99] and post[:2] == [0x96, 0x99]
    st = [e.detail for e in r.ev("status")]
    line = [s for s in st if s.startswith("[arc] 0x8C 리셋:")]
    # 시뮬레이터는 실측대로 출력 OFF 중 0x8C 효과 없음(15050→15050) → '아니오', reset_ok False
    assert len(line) == 1 and "전 SAT=15050" in line[0] and line[0].endswith("(동작=아니오)")
    assert not any(s.startswith("[arc] baseline SAT=") for s in st)
    s = r.d.arc_summary
    assert s["reset_state"] == "no" and s["reset_ok"] is False
    r.done()


def test_t025_fast_window_period_and_frozen_counters():
    """dc_pulse.py:1587-1602 — 점화 창(30 s) 안 판정 주기 1.0 s, 밖 5.0 s; 창 안에서는 저전류·이탈 카운터를 올리지도 0 으로
    되돌리지도 않음(v1 tests/test_dcp_arc_accounting.py:97-150)."""
    d = _dcp()
    d._arc_run_start_ts = 1000.0
    assert d._effective_poll_period(1010.0) == 1.0 and d._effective_poll_period(1031.0) == 5.0
    d._last_ref_power_w = 100.0
    d._low_curr_n, d._spdev_n = 2, 2
    assert d._mon_cycle(_res(0.0, 0.0, 0.0), 1010.0) is None
    assert (d._low_curr_n, d._spdev_n) == (2, 2)
    d._mon_cycle(_res(100.0, 0.25, 400.0), 1010.0)        # 정상 값이어도 0 으로 안 되돌림
    assert (d._low_curr_n, d._spdev_n) == (2, 2)
    d._mon_cycle(_res(100.0, 0.25, 400.0), 1031.0)
    assert (d._low_curr_n, d._spdev_n) == (0, 0)


def test_t026_off_reads_final_arcs_first_and_off_survives_read_failure():
    """dc_pulse.py:1246·1495 — output_off: 0x96/0x99 읽어 종료값 확정('[arc] 종료 확정:') 뒤 0x80=0x0002; 종료값 읽기가 응답
    없어도 OUTPUT_OFF 는 3 s 안에 나감."""
    from v2.tests.test_dcpulse import Rig
    r = Rig()
    assert r.start(100.0, freq=50, off_time_us=5.0) == (None, True)
    r.d.polling = False
    r.c.wait_until(r.c.monotonic() + 1.0)
    n0 = len(r.sim.received)
    assert r.call(r.d.output_off) == (None, True)
    fr = [bytes.fromhex(f)[1] for f in r.sent(n0)]
    assert fr[:3] == [0x96, 0x99, 0x80] and any(e.detail.startswith("[arc] 종료 확정:") for e in r.ev("status"))
    r.done()
    r = Rig()
    assert r.start(100.0, freq=50, off_time_us=5.0) == (None, True)
    r.d.polling = False
    r.c.wait_until(r.c.monotonic() + 1.0)
    before = r.d.arc_counts
    t = r.c.monotonic()
    r.sim.add_fault("no_response", t, t + 1.4)           # 종료값 읽기 무응답 — 1.5 s 한도에서 포기
    box = []
    r.d.output_off(on_done=lambda *x: (box.append(x), r.c.notify()))
    r.c.wait_until(None, lambda: bool(box))
    assert box[0] == (None, True) and r.sim.out is False
    assert r.d._arc_final_soft is None and r.d.arc_counts == before          # 확정 실패 → 폴링 값 유지
    r.done()


def test_t027_t030_t031_arc_accounting_units():
    """dc_pulse.py:230·2668-2985 (v1 tests/test_dcp_arc_accounting.py:247, test_dcp_arc_log_raw.py:135-200) —
    점화/증착 분해, 장비 자체 리셋(raw<기준) → 기준 0 + 런당 1회 문구, 경고 = rate·ign·depo(런 누적 단독은 아님 — data by_total
    False·run_total_limit), 장비 AE/AF·거대 rate 는 판정에 안 씀(계산 rate 만), 주기 로그 형식(첫 줄 '| 계산 -', 못 읽은 rate '-')."""
    from v2.ch12.drivers import dcpulse_proto as P
    s = P.arc_split(4000, 100, 3700, 100)
    assert (s["ign_total"], s["depo_total"], s["run_total"], s["ign_closed"]) == (3800, 300, 4100, True)
    assert P.arc_split(10, 0, None, None)["ign_closed"] is False
    d = _dcp({"DCP_ARC_IGN_WARN": 0, "DCP_ARC_DEPO_WARN": 0})
    d._arc_run_start_ts = 900.0
    d._arc_baseline_soft, d._arc_baseline_hard = 2, 0
    d._mon_arc(3, 0, 0, 1, 1000.0)
    assert _st(d)[-1].startswith("[arc] 장비 SAT=3 ANT=0 AE=0 AF=1 | 런 soft=1 hard=0 (기준 SAT=2 ANT=0) | 점화=")
    assert _st(d)[-1].endswith("| 계산 -")
    d._mon_arc(3, 0, 60000, 60000, 1005.0)                # 카운트 그대로 + 거대 장비 rate → 무시
    assert not [e for e in d.ev if e.kind == "arc_threshold_reached"] and "계산 0.0/s" in _st(d)[-1]
    d._mon_arc(3, 0, None, None, 1010.0)
    assert "AE=- AF=-" in _st(d)[-1]
    d._mon_arc(1, 0, None, None, 1015.0)                  # 장비가 다시 셈(raw < 기준) → 기준 0, 1회만
    d._mon_arc(0, 0, None, None, 1020.0)
    zr = [x for x in _st(d) if x.startswith("[arc] 장비 카운터 감소")]
    assert zr == ["[arc] 장비 카운터 감소 SAT 2→1, ANT 0→0 — 장비가 카운터를 다시 셈 → 기준값을 0 으로 변경"]
    assert d._arc_baseline_soft == 0
    d2 = _dcp({"DCP_ARC_ALERT_RATE_PER_S": 0.0})          # 런 누적만 커져도(≥1000) 경고 없음 — by_total 폐지
    d2._arc_run_start_ts = 1.0
    d2._mon_arc(5000, 0, 0, 0, 1000.0)
    al = [e for e in d2.ev if e.kind == "arc_threshold_reached"]
    assert len(al) == 1 and al[0].data["by_ign"] and al[0].data["by_total"] is False and al[0].data["run_total_limit"] == 1000
    d3 = _dcp({"DCP_ARC_IGN_WARN": 0, "DCP_ARC_DEPO_WARN": 0})
    d3._arc_run_start_ts = 900.0
    for k, t in enumerate((1000.0, 1001.0, 1002.0)):     # 실제 누계 증가 100/s → 2회 연속이면 경고
        d3._mon_arc(100 * k, 0, 0, 1, t)
    al = [e for e in d3.ev if e.kind == "arc_threshold_reached"]
    assert len(al) == 1 and al[0].data["by_rate"] and al[0].data["calc_rate"] >= 20


def test_t028_run_baseline_after_on():
    """dc_pulse.py:2606 _decide_arc_baseline_after_on (v1 tests/test_dcp_arc_baseline_on.py:108-260)."""
    d = _dcp()
    d._arc_baseline_soft, d._arc_baseline_hard, d._arc_on_ref = 2, 0, (2, 0)
    d._arc_after_on(0, 0)
    assert (d._arc_baseline_soft, d._arc_baseline_hard) == (0, 0)
    assert _st(d)[-1] == "[arc] 런 기준값: SAT=0 ANT=0 (ON 직후 초기화 확인)"
    assert _st(d)[-2] == "[arc] ON 직후 장비 SAT=0 ANT=0 (ON 전 기준 SAT=2 ANT=0)"
    d = _dcp()
    d._arc_baseline_soft, d._arc_baseline_hard = 3, 0
    d._arc_after_on(3, 0)
    assert d._arc_baseline_soft == 3
    assert "[arc] ⚠ ON 직후 카운터가 초기화되지 않음 SAT 3→3, ANT 0→0 — 기준값 유지(기존 방식)" in _st(d)
    assert sum(x.startswith("[arc] ⚠") for x in _st(d)) == 1
    d = _dcp()
    d._arc_baseline_soft, d._arc_baseline_hard = 3, 1
    d._arc_after_on(None, None)
    assert _st(d)[-1] == "[arc] 런 기준값: SAT=3 ANT=1 (확인 실패 → 기존 방식)" and _st(d)[-2].endswith("(ON 전 기준 SAT=- ANT=-)")
    d._mon_arc(1, 0, None, None, 1000.0)                  # 그 뒤 감소면 0 보정
    assert d._arc_baseline_soft == 0


def test_t033_t034_piv_constants_and_scale_check():
    """lib/config_common.py:399-406 + dc_pulse.py:51-63 (v1 tests/test_dcp_piv_scale.py:66-120) — V 1.0 V/LSB(옛 1.468815 아님)·
    I 0.01 A·P 10 W·V 설정 1.0 V; |P−V·I|/P ≤ 15 % (115.0 OK, 115.01 NG), P·I 0 이면 None."""
    from v2.ch12.drivers import dcpulse_proto as P
    from v2.ch12.params import dcpulse as PD
    from v2.common.paramdef import defaults
    c = defaults(PD.PARAMS)
    assert (c["DCP_V_MEAS_V_PER_LSB"], c["DCP_I_MEAS_A_PER_LSB"], c["DCP_P_MEAS_W_PER_LSB"], c["DCP_V_SET_STEP_V"]) == \
        (1.0, 0.01, 10.0, 1.0)
    assert P.PIV_SCALE_ERR_MAX_PCT == 15.0
    assert P.piv_scale_check(100.0, 1.0, 115.0)[0] is True and P.piv_scale_check(100.0, 1.0, 115.01)[0] is False
    assert P.piv_scale_check(0.0, 1.0, 100.0) is None and P.piv_scale_check(100.0, 0.0, 100.0) is None
    raw = {"P": 125, "I": 100, "V": 170}                  # 실측 표본(1250 W, 1.00 A, 170 V)은 15 % 안이 아님 → 의심 경고
    e = P.piv_scale(raw, 10.0, 0.01, 1.0)
    assert (e["P_W"], e["I_A"], e["V_V"]) == (1250.0, 1.0, 170.0)
    for p, i, v in ((150.0, 0.48, 310.0), (300.0, 0.97, 320.0)):
        ok, err = P.piv_scale_check(p, i, v)
        assert ok and err <= 8.0


# ═════════════ TEST-003·089 '[arc] 런 요약'(v1 tests/test_arc_summary_reset_label.py:66, test_log_gates.py:43) ═════════════
_ARC_BASE = {"run_total": 4, "run_soft": 3, "run_hard": 1, "ign_total": 2, "ign_soft": 2, "ign_hard": 0, "depo_total": 2,
             "depo_soft": 1, "depo_hard": 1, "final_read": True, "ran": True}


@pytest.mark.parametrize("extra,label", [
    ({"reset_state": "unknown", "reset_ok": False}, "확인불가"), ({"reset_state": "no", "reset_ok": False}, "아니오"),
    ({"reset_state": "yes", "reset_ok": True}, "예"), ({"reset_ok": True}, "예"), ({"reset_ok": False}, "아니오"),
    ({"reset_state": "???", "reset_ok": True}, "예")])
def test_t003_arc_run_summary_reset_label(extra, label):
    """runtime/chamber_runtime.py:1886-1903 — 3값 문구(dc_pulse _ARC_RESET_LABEL 과 같음), 옛 요약(reset_state 없음)은 reset_ok."""
    from v2.ch12.drivers import dcpulse_proto as P
    from v2.ch12.process_records import _RESET_LABEL, arc_summary_line
    msg = arc_summary_line(dict(_ARC_BASE, **extra))
    assert msg.startswith("[arc] 런 요약: 합계=4 (soft=3 hard=1) | 점화=2 (soft=2 hard=0) | 증착=2 (soft=1 hard=1) | ")
    assert msg.endswith(f"0x8C 동작={label}, 종료값 확정=예")
    assert _RESET_LABEL == P.ARC_RESET_LABEL


def test_t003_t089_arc_summary_only_when_ran():
    """ran=False(RF Pulse 전용 런 등 DC Pulse 출력을 안 켬)면 찍지 않음 — '합계=0 … 아니오' 오독 방지(v1 test_log_gates.py:43-100)."""
    from v2.ch12.process_records import arc_summary_line
    assert arc_summary_line(dict(_ARC_BASE, ran=False, reset_state="unknown")) is None
    d = _dcp()
    assert d.arc_summary["ran"] is False                   # 켠 적 없는 드라이버
    d._arc_run_start_ts = 5.0
    assert d.arc_summary["ran"] is True


@pytest.mark.slow
def test_t003_engine_logs_arc_summary_on_success(tmp_path):
    """성공 공정 끝(엑셀 기록 직전)에 DCPulse 로그 한 줄 — 엑셀 soft/hard_arc_count 값은 그대로(v2 엔진 실제 실행)."""
    from v2.tests import proc_rig as R
    from v2.tests.test_process_engine import _run_single
    rig, e, _ = _run_single(tmp_path, "ch2_dcp_change")
    assert R.ends(e) == [("done", None)]
    line = [x for x in R.logs(e) if "[arc] 런 요약: 합계=" in x]
    assert len(line) == 1 and "0x8C 동작=" in line[0] and "종료값 확정=예" in line[0]
    s, h = e.actions.dcp.arc_counts
    assert (e.stats.params["soft_arc_count"], e.stats.params["hard_arc_count"]) == (s, h)


# ═════════════ PLC 링크 카드·억제·진단(v1 tests/test_chat_link_suppress.py, test_plc_link_cards.py, test_plc_link_policy.py) ═════════════
def _link():
    from v2.ch12.drivers.plc_io import LinkState
    from v2.common.clock import VirtualClock
    c = VirtualClock(start_wall=1_760_000_000.0)
    out = []
    return LinkState(c, lambda k, d, data: out.append((k, d, data)), "192.168.1.2:502"), out, c


def test_t010_suppressed_counts_consume_and_format():
    """lib/link_state.py:90-136 — note_suppressed 로 건수만, consume_suppressed()==(40, {'E401':20,'E402':20}) 뒤 (0, {}),
    format_suppressed → 'E401 20건, E402 20건'."""
    from v2.ch12.drivers.plc_io import format_suppressed
    ls, out, _ = _link()
    ls.note_down("reset", "read_coils")
    for _ in range(20):
        ls.note_suppressed("E401")
        ls.note_suppressed("e402")
    assert ls.consume_suppressed() == (40, {"E401": 20, "E402": 20})
    assert ls.consume_suppressed() == (0, {})
    assert format_suppressed(40, {"E401": 20, "E402": 20}) == "E401 20건, E402 20건" and format_suppressed(0, {}) == ""


def test_t011_t113_one_outage_is_exactly_two_cards():
    """main.py:286-293 + lib/link_state.py:30-103 — 끊김 1회 = 'PLC 연결 끊김' FAIL 1 + 'PLC 재연결' SUCCESS 1, 복구 부제에
    '억제된 연결 알림 80건 (E401 40건, E402 40건)'; 긴 끊김(억제 200건)도 2장, 깜빡임 3번 = 6장, 이미 down 이면 down 카드 없음,
    부팅(처음부터 up)·종료 때 카드 없음."""
    ls, out, c = _link()
    assert ls.note_up() is False and out == []                    # 부팅 — 끊긴 적 없음
    ls.note_down("reset", "read_coils")
    assert ls.note_down("again") is False                         # 첫 감지 즉시 down — 두 번째는 없음
    for _ in range(40):
        ls.note_suppressed("E401")
        ls.note_suppressed("E402")
    ls.note_up()
    assert [k for k, *_ in out] == ["link_down", "link_up"]
    down, up = out[0][2]["card"], out[1][2]["card"]
    assert (down["title"], down["status"], up["title"], up["status"]) == ("PLC 연결 끊김", "FAIL", "PLC 재연결", "SUCCESS")
    assert "[CH1&2] PLC 연결 끊김 (192.168.1.2:502)" in down["subtitle"] and "사유=reset" in down["subtitle"]
    assert "끊긴 동안 억제된 연결 알림 80건 (E401 40건, E402 40건)" in up["subtitle"]
    out.clear()
    ls.note_down("x")
    for _ in range(200):
        ls.note_suppressed("E401")
    ls.note_up()
    assert len(out) == 2 and "억제된 연결 알림 200건 (E401 200건)" in out[1][1]
    out.clear()
    for _ in range(3):
        ls.note_down("flap")
        ls.note_up()
    assert [k for k, *_ in out] == ["link_down", "link_up"] * 3


def test_t113_single_e402_is_no_card_and_not_down():
    """device/plc.py:1252 — 고우선 타임아웃 1회(E402)는 링크 down 아님·카드 없음(같은 소켓 재시도)."""
    from v2.common.worker import Status
    from v2.ch12.sim.plc_rig import PlcRig
    r = PlcRig()
    r.sim.drop_next = 1
    res = r.call(r.d.read_reg, 8)
    assert res.status == Status.OK
    assert "link_down" not in r.kinds() and "link_up" not in r.kinds() and r.d.link.down is False
    r.done()


def test_t116_diag_text_states_once_per_interval_and_errno_table(monkeypatch):
    """device/plc.py:1000-1094 — 분류 문구: 값 없음 '미확인', 진행 중 '진단중', 묵은 값(> 60 s)은 '미확인'; 진단은
    PLC_DIAG_MIN_INTERVAL_S(60 s)에 1회; _probe_connect errno 표(10061 → '접속 거부(RST, errno=10061)')."""
    import socket
    import threading
    from v2.ch12.drivers import plc_io
    from v2.ch12.sim.plc_rig import PlcRig
    r = PlcRig()
    assert r.d.diag_text() == "미확인"
    gate, calls = threading.Event(), []

    def probe(host, port, tmo):
        calls.append(host)
        gate.wait(2.0)
        return "SYN 무응답(타임아웃)"
    r.d.diag_probe = probe
    r.d._run_diag()
    assert r.d.diag_text() == "진단중"
    gate.set()
    for _ in range(200):
        if not r.d._diag_running:
            break
        threading.Event().wait(0.01)
    assert r.d.diag_text() == "SYN 무응답(타임아웃)"
    r.d._run_diag()                                               # 60 s 안 — 다시 안 함
    assert len(calls) == 1
    r.after(61.0)
    assert r.d.diag_text() == "미확인"                              # 묵은 값은 쓰지 않음
    r.done()

    class _S:
        def __init__(self, *a):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def settimeout(self, t):
            pass

        def connect_ex(self, addr):
            return self.rc
    for rc, want in ((10061, "접속 거부(RST, errno=10061)"), (111, "접속 거부(RST, errno=111)"), (0, "접속됨"),
                     (10060, "SYN 무응답(타임아웃, errno=10060)"), (10013, "차단됨(방화벽/권한, errno=10013)"),
                     (99999, "미분류(errno=99999)")):
        _S.rc = rc
        monkeypatch.setattr(socket, "socket", _S)
        assert plc_io.probe_connect("127.0.0.1", 502, 1.0) == want


# ═════════════ TEST-008 플라즈마 클리닝 카메라 owner(v1 tests/test_camera_owner.py:170-230) ═════════════
def test_t008_pc_camera_owner_fixed_at_start_and_cleared_after_stop(tmp_path):
    """runtime/plasma_cleaning_runtime.py:1967-1972·2271 — 시작 때 정한 owner(pc1)로 끝냄(실행 중 채널 바꾸기는 거부돼 owner 그대로),
    끝나면 _cam_owner_active = '' 이고 참여자에서 빠짐."""
    from v2.tests.test_plasma_cleaning import UI, make, run_to_end, ticks
    r, eng = make(tmp_path)
    try:
        assert eng.start(params=dict(UI, process_time="0.1")).accepted
        ticks(r, lambda: eng.pc.step == "process")
        assert eng.cam_owner_active == "pc1" and r.shared.camera.users == {"pc1"}
        assert eng.select_channel(2)[0] is False and eng.cam_owner_active == "pc1"
        run_to_end(r, eng)
        assert eng.cam_owner_active == "" and r.shared.camera.users == set()
    finally:
        r.stop()


# ═════════════ Robot CSV 기록 이벤트·끊긴 줄·메모리 큐(v1 tests/test_client_process_chat.py:117, test_host_log_line_end.py, test_leak_guards.py:230) ═════════════
def _robot(tmp_path):
    from v2.ch12.outputs.base import QueueWriter
    from v2.tests.outputs_golden import Clock, make_root, v2_cfg
    from v2.tests.test_outputs_robot import _mk
    from v2.tools import v1golden_outputs_scen as SC
    root = make_root(tmp_path)
    cfg = v2_cfg(root, SC.BASE_CFG)
    clock = Clock(SC.T0)
    w = QueueWriter("t")
    return _mk(cfg, w, clock), w, clock, cfg


def test_t012_robot_csv_events_once_each(tmp_path):
    """util/host_process_log.py:236-523 — 요청 수신은 통지 없음, started 는 시작 시각 첫 기록 때 1회(info 키), finalize 는 기록키당
    finished 1회(멱등 재호출 통지 없음), reject → finished(거절, started None), 재시작 복구 → finished(중단(재시작)) 사유
    '프로그램 재시작 (공정 중)'/'(시작 전)', 수신자 예외는 기록을 막지 않음, 수신자 없으면 상태(_final_emitted)를 쌓지 않음."""
    r, w, clock, cfg = _robot(tmp_path)
    got = []
    r.set_event_sink(lambda kind, info: got.append((kind, dict(info))))
    k = r.request(target="CH1", request_id="R1", peer="p", received_at=clock.now(), recipe_name="a.csv")
    assert got == []
    r.update(k, row_count=2, process_names="A / B")
    r.mark_started(k, clock.now(), "CH1_A_1.txt")
    r.mark_started(k, clock.now(), "CH1_B_2.txt")
    assert [g[0] for g in got] == ["started"]
    info = got[0][1]
    assert set(info) >= {"key", "target", "request_id", "recipe_name", "row_count", "process_names", "received_at",
                         "started_at", "log_files"}
    assert info["process_names"] == "A / B" and info["target"] == "CH1" and info["key"] == k
    assert r.finalize(k, "성공", "", clock.now()) is True and r.finalize(k, "성공", "", clock.now()) is False
    assert [g[0] for g in got] == ["started", "finished"] and got[1][1]["result"] == "성공"
    k2 = r.request(target="CH2", request_id="R2", peer="p", received_at=clock.now())
    r.reject(k2, "E205 busy")
    assert got[-1][0] == "finished" and got[-1][1]["result"] == "거절" and got[-1][1]["started_at"] is None
    k3 = r.request(target="CH1", request_id="R3", peer="p", received_at=clock.now())
    r.mark_started(k3, clock.now(), "x.txt")
    r.request(target="CH1", request_id="R4", peer="p", received_at=clock.now())
    w.flush()

    def boom(kind, info):
        raise RuntimeError("sink 고장")
    from v2.tests.test_outputs_robot import _mk
    r2 = _mk(cfg, w, clock)
    seen = []
    r2.set_event_sink(lambda kind, info: seen.append((kind, info["result"], info["reason"])))
    r2.startup_recover()
    w.flush()
    assert sorted(seen) == [("finished", "중단(재시작)", "프로그램 재시작 (공정 중)"),
                            ("finished", "중단(재시작)", "프로그램 재시작 (시작 전)")]
    r3 = _mk(cfg, w, clock)
    r3.set_event_sink(boom)
    k5 = r3.request(target="CH1", request_id="R5", peer="p", received_at=clock.now())
    r3.mark_started(k5, clock.now(), "y.txt")
    assert r3.finalize(k5, "성공", "", clock.now()) is True
    w.flush()
    pend = (Path(cfg["HOST_LOG_LOCAL_DIR"]) / "pending_sputter.csv").read_text(encoding="utf-8")
    assert k5 in pend                                                      # 수신자 예외에도 기록됨
    r4 = _mk(cfg, w, clock)
    k6 = r4.request(target="CH1", request_id="R6", peer="p", received_at=clock.now())
    r4.finalize(k6, "성공", "", clock.now())
    assert len(r4._final_emitted) == 0                                     # 수신자 없음 — 상태 안 쌓음
    for x in (r, r2, r3, r4):
        x.close()
    w.stop()


def test_t074_ensure_line_end_rules(tmp_path):
    """host_process_log.py:116 _ensure_line_end — 끊긴 파일은 CRLF 먼저(끊긴 행은 그대로 별도 줄), 정상·빈·BOM 만·없는 파일은
    그대로(없는 파일은 만들지 않음), 보정 실패(폴더 등)는 예외 없이 넘어감."""
    r, w, clock, cfg = _robot(tmp_path)
    cut, ok, empty, bom = (tmp_path / n for n in ("cut.csv", "ok.csv", "empty.csv", "bom.csv"))
    cut.write_bytes(b"a,b\r\nALD1,09:01:00,09:01:05,,,\xec\xa7\x84\xed\x96\x89")
    ok.write_bytes(b"a,b\r\n1,2\r\n")
    empty.write_bytes(b"")
    bom.write_bytes(b"\xef\xbb\xbf")
    for p in (cut, ok, empty, bom, tmp_path / "none.csv", tmp_path):
        r._ensure_line_end(p)
    assert cut.read_bytes().endswith("진행\r\n".encode("utf-8"))
    assert ok.read_bytes() == b"a,b\r\n1,2\r\n" and empty.read_bytes() == b"" and bom.read_bytes() == b"\xef\xbb\xbf"
    assert not (tmp_path / "none.csv").exists()
    r.close()
    w.stop()


def test_t085_mem_queue_cap_drops_oldest_and_warns_once(tmp_path, monkeypatch):
    """host_process_log.py:152·604 — 메모리 큐 상한(기본 10000, 시험 100) 넘으면 가장 오래된 것부터 버림(250 → 150 버림), 경고 1회."""
    from v2.ch12.outputs import robot_csv as RC
    assert RC.MEM_QUEUE_MAX == 10000
    monkeypatch.setattr(RC, "MEM_QUEUE_MAX", 100)
    r, w, clock, cfg = _robot(tmp_path)
    logs = []
    r._syslog = SimpleNamespace(append=lambda src, msg: logs.append(msg))
    for i in range(250):
        r._mem_queue.append(f"line{i}\r\n")
        r._trim_mem_queue()
    assert len(r._mem_queue) == 100 and r._mem_queue[0] == "line150\r\n" and r._mem_queue_dropped == 150
    assert sum("메모리 큐 상한" in m for m in logs) == 1
    r.close()
    w.stop()


def test_t075_midnight_row_goes_to_request_day_file(tmp_path):
    """host_process_log.py:40-44·285 — 파일 날짜 = 요청 수신 날짜(자정을 넘겨 끝나도), 소요 = (종료−시작)/60 소수 1자리."""
    from datetime import datetime
    r, w, clock, cfg = _robot(tmp_path)
    k = r.request(target="CH1", request_id="night", peer="p", received_at=datetime(2026, 10, 30, 23, 59, 50))
    r.mark_started(k, datetime(2026, 10, 30, 23, 59, 55), "a.txt")
    assert r.finalize(k, "성공", "무시", datetime(2026, 10, 31, 0, 10, 0))
    w.flush()
    r.flush_once()
    nas = Path(cfg["HOST_LOG_NAS_DIR"])
    assert (nas / "Robot_20261030.csv").exists() and not (nas / "Robot_20261031.csv").exists()
    line = [x for x in (nas / "Robot_20261030.csv").read_text(encoding="utf-8-sig").splitlines() if k in x][0]
    f = line.split(",")
    assert f[:7] == ["CH1", "23:59:50", "23:59:55", "00:10:00", "10.1", "성공", ""]
    r.close()
    w.stop()


# ═════════════ 레시피 열·히터 SOAK·빈 행·Factor 열(v1 tests/test_heater_recipe_soak.py, test_recipe_template.py, test_recipe_blank_rows.py) ═════════════
def _caps2():
    from v2.ch12 import process_steps as ps
    return ps.Caps(dc=True, dc2=False, rf=True, dcp=True, rfp=True, heater=True, thickness=True)


def _rrow(**kw):
    from v2.ch12.recipe import TEMPLATE_COLUMNS
    r = {c: "" for c in TEMPLATE_COLUMNS}
    r.update(kw)
    return r


def test_t060_heater_soak_normalize_and_check():
    """runtime/chamber_runtime.py:6159·8653·8805 — '60' → 60.0, 공란·열 없음(옛 레시피) → None, use_heater=F 면 None + 로그
    'heater_soak=60.0 은 무시', 숫자 아니면 오류 1건(heater_soak·숫자), 0~480분."""
    from v2.ch12.recipe import normalize_row
    from v2.ch12.recipe_check import HEATER_SOAK_MAX_MIN, check_raw_rows
    c2 = _caps2()
    assert normalize_row({"use_heater": "T", "heater_temp": "400", "heater_soak": "60"}, 2, c2)["heater_soak"] == 60.0
    assert normalize_row({"use_heater": "T", "heater_temp": "400", "heater_soak": ""}, 2, c2)["heater_soak"] is None
    assert normalize_row({"use_heater": "T", "heater_temp": "400"}, 2, c2)["heater_soak"] is None
    log = []
    p = normalize_row({"use_heater": "F", "heater_temp": "400", "heater_soak": "60"}, 2, c2, log=log)
    assert p["heater_soak"] is None and any("heater_soak=60.0 은 무시" in m for _s, m in log)
    errs = check_raw_rows([_rrow(Process_name="a", heater_soak="한 시간")])
    assert len(errs) == 1 and "heater_soak" in errs[0] and "숫자" in errs[0]
    assert HEATER_SOAK_MAX_MIN == 480.0


def test_t061_heater_wait_step_carries_soak():
    """controller/process_controller.py:2153 — 시퀀스 앞 HEATER_SET·HEATER_WAIT·IG_CMD, SOAK 있으면 HEATER_WAIT params (3600.0,)·
    '히터 400°C 도달·안정 대기 (SOAK 60분)', 공란이면 params None·'히터 400°C 도달·안정 대기'(나머지 스텝 같음)."""
    from v2.ch12 import process_steps as ps
    from v2.ch12.blocks import expand, template_blocks
    p = dict(base_pressure=7e-6, working_pressure=2.0, process_time=0.5, shutter_delay=0.1, use_ar=True, ar_flow=20,
             use_dc_power=True, dc_power=100, use_ms=True, use_g1=True, use_heater=True, heater_temp=400.0)
    caps = _caps2()
    out = {}
    for soak in (60.0, None):
        b, _ = template_blocks(dict(p, heater_soak=soak), 2, {}, caps=caps)
        out[soak], _ = expand(b, {}, 2, caps=caps)
    a, b = out[60.0], out[None]
    assert [s.action for s in a[:3]] == ["HEATER_SET", "HEATER_WAIT", "IG_CMD"]
    assert a[1].params == (3600.0,) and a[1].message == "히터 400°C 도달·안정 대기 (SOAK 60분)"
    assert b[1].params is None and b[1].message == "히터 400°C 도달·안정 대기"
    assert ps.to_golden(a[2:]) == ps.to_golden(b[2:]) and a[1].value == b[1].value == 400.0


def test_t063_t138_template_column_order():
    """recipes/Recipe_Template.csv:1 (v1 tests/test_recipe_template.py:50) — 48열, 마지막 4열 use_heater·heater_temp·heater_ramp·
    heater_soak, thickness 앞 dep_rate·뒤 tooling·density·z_factor, power_change_time·change_power_value 짝; 모든 열에 설명(COLUMN_DOC
    = v1 설명 시트), heater_soak 단위 분."""
    from v2.ch12.recipe import COLUMN_DOC, TEMPLATE_COLUMNS as T
    assert len(T) == 48 and T[-4:] == ("use_heater", "heater_temp", "heater_ramp", "heater_soak")
    i = T.index("thickness")
    assert T[i - 1] == "dep_rate" and T[i + 1:i + 4] == ("tooling", "density", "z_factor")
    j = T.index("power_change_time")
    assert T[j + 1] == "change_power_value"
    assert set(T) <= set(COLUMN_DOC) and COLUMN_DOC["heater_soak"][1] == "분"


def test_t139_factor_column_value_checks():
    """runtime/chamber_runtime.py:8653 _validate_raw_rows — tooling 10~399 %, density 0.5~99.99 g/cm3, z_factor 0.1~9.999,
    세 칸 비우면 통과(Factor 값 사용)."""
    from v2.ch12.recipe_check import check_raw_rows
    e = check_raw_rows([_rrow(Process_name="a", tooling="5", density="abc", z_factor="10")])
    assert "2행('a') tooling='5' → 범위 10~399 % 밖입니다" in e
    assert "2행('a') density='abc' → 숫자가 아닙니다" in e
    assert any(x.startswith("2행('a') z_factor='10' → 범위 0.1~9.999") for x in e)
    assert check_raw_rows([_rrow(Process_name="a")]) == []


def test_t140_loader_reads_new_columns(tmp_path):
    """v1 tests/test_recipe_template.py:173 — 템플릿 머리글 그대로 dep_rate 0.05·thickness 100.0, process_time 공란이면
    thickness/dep_rate/60 분, power_change_time '2m'·change_power_value '100' 문자열 그대로, tooling·density·z_factor 공란 None·
    값 있으면 (120.0, 2.73, 1.08)."""
    from v2.ch12.recipe import TEMPLATE_COLUMNS, load_recipe_file, normalize_row
    p = tmp_path / "r.csv"
    rows = [_rrow(**{"#": "1", "Process_name": "t1", "dep_rate": "0.05", "thickness": "100", "power_change_time": "2m",
                     "change_power_value": "100", "use_rf_power": "T", "rf_power": "100", "Ar": "T", "Ar_flow": "20"}),
            _rrow(**{"#": "2", "Process_name": "t2", "tooling": "120", "density": "2.73", "z_factor": "1.08",
                     "process_time": "5", "use_rf_power": "T", "rf_power": "100"})]
    with open(p, "w", encoding="utf-8-sig", newline="") as f:
        f.write(",".join(TEMPLATE_COLUMNS) + "\r\n")
        for r in rows:
            f.write(",".join(r[c] for c in TEMPLATE_COLUMNS) + "\r\n")
    r = load_recipe_file(str(p))
    assert r.ok, r.message
    a, b = (normalize_row(x, 2, _caps2()) for x in r.rows)
    assert a["dep_rate"] == 0.05 and a["thickness"] == 100.0 and a["process_time"] == pytest.approx(100 / 0.05 / 60)
    assert a["power_change_time"] == "2m" and a["change_power_value"] == "100"
    assert (a["tooling"], a["density"], a["z_factor"]) == (None, None, None)
    assert (b["tooling"], b["density"], b["z_factor"]) == (120.0, 2.73, 1.08)


def test_t134_blank_rows_skipped_and_row_numbers_kept(tmp_path):
    """runtime/chamber_runtime.py:187·7991·3359 — '쉼표만' 행은 건너뜀(로그 '빈 행 4개 건너뜀', 호스트 '(총 7개)'·UI '총 7개 공정
    읽음.'), 오류 행 번호는 원래 파일 행 번호('4행('Pre-sputter') rf_pulse_power='abc'')."""
    from v2.ch12.recipe import TEMPLATE_COLUMNS, load_recipe_file
    names = ["Pre-sputter", "", "delay 5m", "", "Pre-sputter", "delay 5m", "", "Pre-sputter", "delay 5m", "", "Pre-sputter"]
    p = tmp_path / "Pre-sputter_RF (30+5)x4_2hr.csv"
    lines = [",".join(TEMPLATE_COLUMNS)]
    for n in names:
        if not n:
            lines.append("," * (len(TEMPLATE_COLUMNS) - 1))
        else:
            r = _rrow(Process_name=n, use_rf_pulse="T" if n == "Pre-sputter" else "", rf_pulse_power="100" if n == "Pre-sputter" else "")
            lines.append(",".join(r[c] for c in TEMPLATE_COLUMNS))
    p.write_text("\r\n".join(lines) + "\r\n", encoding="utf-8-sig")
    r = load_recipe_file(str(p))
    assert r.ok and [x["Process_name"] for x in r.rows] == ["Pre-sputter", "delay 5m"] * 3 + ["Pre-sputter"]
    assert ("File", "빈 행 4개 건너뜀 (모든 칸이 빈 행)") in r.logs and r.logs[-1][1].endswith("(총 7개)")
    assert load_recipe_file(str(p), origin="ui").logs[-1] == ("File", "총 7개 공정 읽음.")
    bad = lines[:]
    r4 = _rrow(Process_name="Pre-sputter", use_rf_pulse="T", rf_pulse_power="abc")
    bad[3] = ",".join(r4[c] for c in TEMPLATE_COLUMNS)                     # 파일 4행(머리글 1행 + 빈 행 포함)
    p.write_text("\r\n".join(bad) + "\r\n", encoding="utf-8-sig")
    r = load_recipe_file(str(p))
    assert r.code == "E230" and "4행('Pre-sputter') rf_pulse_power='abc'" in r.message


def test_t052_heater_calib_row_off_and_no_samples():
    """controller/data_logger.py:17·421-466 — CH2 만 히터 5열(36열 뒤), 히터 꺼짐 공정은 목표·MV 공란·PV 만, 표본 없으면 5칸 공란·요약
    없음, CH1 은 36열 그대로; 요약 '히터 요약(메인 공정 구간) — 히터 꺼짐 · PV 평균 46.0°C'."""
    from datetime import datetime
    from v2.ch12.outputs.calib_csv import HEADER, HEATER_COLUMNS, build_row, header_for
    from v2.ch12.outputs.run_stats import RunStats
    assert header_for(1, False) == HEADER and len(HEADER) == 36 and header_for(2, False) == HEADER + HEATER_COLUMNS
    st = RunStats(now=lambda: datetime(2026, 10, 30, 9, 0, 0))
    st.start({"use_ms": False, "process_name": "T2"})
    for pv in (45.0, 47.0):
        st.add_heater(pv, 0.0, 0, False)
    assert st.heater_summary_text().startswith("히터 요약(메인 공정 구간) — 히터 꺼짐 · PV 평균 46.0°C")
    row = build_row(2, st, True, False)
    assert [row[c] for c in HEATER_COLUMNS] == ["", "46.0", "45.0", "47.0", ""]
    st2 = RunStats(now=lambda: datetime(2026, 10, 30, 9, 0, 0))
    st2.start({"use_ms": True})
    assert st2.heater_stats() is None and st2.heater_summary_text() == ""
    assert [build_row(2, st2, True, False)[c] for c in HEATER_COLUMNS] == [""] * 5
    assert list(build_row(1, st2, True, False)) == HEADER


# ═════════════ 두께 CSV·Calib 두께 열(v1 tests/test_thickness_service.py:350, test_thickness_logs.py:47) ═════════════
def test_t185_thickness_csv_only_while_recording_then_appends(tmp_path):
    """lib/thickness_logger.py — 공정 중(set_recording)만 1초 1행, 공정 없으면 파일·행 없음, 끝나면 멈춤·닫힘, 다음 공정은 같은 날
    같은 파일에 이어쓰기('Run B (Time 모드)')."""
    from datetime import datetime
    from v2.ch12.outputs.base import QueueWriter
    from v2.ch12.outputs.thickness_csv import COLUMNS, ThicknessCsv
    from v2.tests.outputs_golden import make_root, v2_cfg
    from v2.tools import v1golden_outputs_scen as SC
    root = make_root(tmp_path)
    cfg = v2_cfg(root, SC.BASE_CFG, {"THICKNESS_LOG_DIR": "@/로컬/Thickness"})
    w = QueueWriter("t")
    t = ThicknessCsv(2, cfg, writer=w)
    st = {"connected": True, "thickness_nm": 123.4, "rate_nm_s": 0.1, "freq_hz": 5900000, "life_pct": 12.4,
          "crystal_ok": True, "stale": False}
    for s in range(3):
        t.on_tick(st, datetime(2026, 10, 30, 9, 0, s))
    w.flush()
    assert not list(root.rglob("Thickness_*.csv"))
    t.set_recording(True, "Run A")
    for s in range(3, 6):
        t.on_tick(st, datetime(2026, 10, 30, 9, 0, s))
    t.set_recording(False)
    t.on_tick(st, datetime(2026, 10, 30, 9, 0, 7))
    w.flush()
    f = list(root.rglob("Thickness_20261030.csv"))
    assert len(f) == 1
    if Path("/proc/self/fd").is_dir():
        assert not [p for p in _open_paths() if p.endswith("Thickness_20261030.csv")]
    t.set_recording(True, "Run B (Time 모드)")
    t.on_tick(st, datetime(2026, 10, 30, 9, 10, 0))
    t.stop()
    w.stop()
    rows = f[0].read_text(encoding="utf-8-sig").splitlines()
    assert rows[0].split(",") == COLUMNS and len(rows) == 1 + 3 + 1
    assert rows[1].startswith("2026-10-30 09:00:03,1,123.40,") and rows[1].endswith(",1,Run A")
    assert rows[-1].endswith(",1,Run B (Time 모드)")


def test_t166_calib_thickness_columns_tail():
    """controller/data_logger.py THICKNESS_COLUMNS — 끝 3열 Jump Count·Material·Factor Source(계단 'Step' 열 없음), CH2 순서는
    36 + 히터 5 + 두께 13."""
    from v2.ch12.outputs.calib_csv import HEADER, HEATER_COLUMNS, THICKNESS_COLUMNS, header_for
    assert THICKNESS_COLUMNS[-3:] == ["Thickness: Jump Count", "Thickness: Material", "Thickness: Factor Source"]
    assert not any("Step" in c for c in THICKNESS_COLUMNS)
    assert header_for(2, True) == HEADER + HEATER_COLUMNS + THICKNESS_COLUMNS and header_for(1, True) == HEADER + THICKNESS_COLUMNS


# ═════════════ 두께 공정 스텝·ETA(v1 tests/test_thickness_process_steps.py:82-170, tests/data/thickness_time_mode_golden.json) ═════════════
_THK_ETA_ROWS = [
    {"Process_name": "a", "shutter_delay": "5", "process_time": "10"},
    {"Process_name": "heat", "delay": "20m"},
    {"Process_name": "b", "shutter_delay": "1", "process_time": "", "thickness": "100", "dep_rate": "0.05"},
    {"Process_name": "c", "shutter_delay": "2", "process_time": "7", "thickness": "40", "dep_rate": "0.05"},
]


def test_t175_t193_eta_rows_time_mode_golden_and_thickness_supported():
    """골든 eta_rows 4790(두께 변경 전 컨트롤러, 두께 미지원), 두께 지원 채널은 thickness 행에서 메인 시간을 빼고
    (5+10)×60 + 1200 + 60 + 120 + 90 = 2370, time 행만이면 그대로."""
    import json
    from v2.ch12 import eta as E
    root = Path(__file__).resolve().parents[2]
    gold = json.loads((root / "tests" / "data" / "thickness_time_mode_golden.json").read_text(encoding="utf-8"))
    assert E.estimate_min_total_s(_THK_ETA_ROWS, {}, 2) == gold["eta_rows"] == 4790
    assert E.estimate_min_total_s(_THK_ETA_ROWS, {}, 2, thickness_supported=True) == (5 + 10) * 60 + 1200 + 60 + 120 + 90
    t = _THK_ETA_ROWS[:2]
    assert E.estimate_min_total_s(t, {}, 2, thickness_supported=True) == E.estimate_min_total_s(t, {}, 2)
    assert E.eta_row_min_seconds(_THK_ETA_ROWS[2], True) == 60.0


def _thk_steps(**kw):
    from v2.ch12 import process_steps as ps
    from v2.ch12.blocks import expand, template_blocks
    p = dict(base_pressure=7e-6, working_pressure=5, process_time=100 / 0.05 / 60, shutter_delay=5, thickness=100.0,
             dep_rate=0.05, use_ar=True, ar_flow=20, use_dc_pulse=True, dc_pulse_power=80, dc_pulse_freq=20,
             dc_pulse_off_time=10.0, use_ms=True, use_g1=True)
    p.update(kw)
    caps = ps.Caps(dc=True, dc2=False, rf=True, dcp=True, rfp=True, heater=False, thickness=True)
    b, _ = template_blocks(dict(p), 2, {}, caps=caps)
    steps, _ = expand(b, {}, 2, caps=caps)
    return steps


def test_t175_thickness_mode_replaces_main_delay():
    """Shutter Delay → THICKNESS_ZERO(params (5.0,), 폴링 아님) → PLC 'MS' → OES_RUN(상한 4000 s, no_wait, 'OES 측정 시작 (메인 공정
    끝까지, 최대 66.67분') → THICKNESS_WAIT(value 100, 폴링, '메인 공정 진행…', params (2000, 4000)); Time 모드엔 ZERO 스텝 없음;
    use_ms=F 면 ZERO 바로 뒤 OES_RUN·THICKNESS_WAIT(MS 없음)."""
    st = _thk_steps(thickness_mode=True)
    names = [s.action for s in st]
    i, z = names.index("THICKNESS_WAIT"), names.index("THICKNESS_ZERO")
    assert names[i - 1] == "OES_RUN" and names[i - 2] == "PLC_CMD" and st[i - 2].params[0] == "MS" and z == i - 3
    assert st[z - 1].message.startswith("Shutter Delay") and st[z].params == (5.0,) and not st[z].polling
    w = st[i]
    assert w.value == 100.0 and w.polling and w.message.startswith("메인 공정 진행")
    assert w.params == (pytest.approx(2000.0), pytest.approx(4000.0))
    assert st[i - 1].params[0] == pytest.approx(4000.0) and st[i - 1].no_wait
    assert st[i - 1].message.startswith("OES 측정 시작 (메인 공정 끝까지, 최대 66.67분")
    assert "THICKNESS_ZERO" not in [s.action for s in _thk_steps()]
    n2 = [s.action for s in _thk_steps(thickness_mode=True, use_ms=False)]
    k = n2.index("THICKNESS_ZERO")
    assert n2[k + 1:k + 3] == ["OES_RUN", "THICKNESS_WAIT"]


@pytest.mark.slow
def test_t177_limit_is_failure_e848_and_closes_shutter_first(tmp_path):
    """v1 tests/test_thickness_process_steps.py:275-330 — 상한 도달 = 공정 실패 E848(error_source THICKNESS, '두께 목표 미도달 —
    최대 시간 초과: 유효 … / 목표 … nm', errors 에 THICKNESS_WAIT·E848), 종료 절차 첫 단계 Main Shutter 닫기 → 전원 OFF,
    actual_process_time_min > 0. (v2 엔진 + 두께 엔진 + STM-100 시뮬레이터 실제 실행 — 실제 증착률이 레시피보다 훨씬 느림)"""
    from v2.ch12.process_engine import ProcessEngine
    from v2.ch12.thickness import ThicknessEngine
    from v2.tests import proc_rig as R
    rig = R.make(tmp_path, chambers=())
    th = ThicknessEngine(rig.clock, rig.cfg, "ch2", driver=rig.stm["ch2"], settings_dir=tmp_path / "set")
    e = ProcessEngine(rig, "ch2", thickness=th, excel_main_dir=tmp_path / "xl")
    rig.register_engine("proc.ch2", e)
    p = dict(base_pressure=1e-5, working_pressure=5, process_time=0, shutter_delay=0.05, thickness=1000.0, dep_rate=50.0,
             use_ar=True, ar_flow=20, use_dc_pulse=True, dc_pulse_power=80, dc_pulse_freq=20, use_ms=True, use_g1=True,
             tooling=100, density=2.0, z_factor=1.0, process_note="thk")
    R.run(rig, R.idle(e), max_ticks=1500, hooks=[R.at(2, lambda: e.start(params=p))])
    assert R.ends(e) == [("error", "E848")]
    d = e.last_result
    assert d["error_source"] == "THICKNESS" and d["error_detail"].startswith("두께 목표 미도달 — 최대 시간 초과: 유효 ")
    assert any("THICKNESS_WAIT" in x and "E848" in x for x in d["errors"]) and d["actual_process_time_min"] > 0
    sd = [(a, m) for _t, _i, a, m, s in e.traces[-1] if s]
    assert sd[0][0] == "PLC_CMD" and sd[0][1].startswith("Main Shutter 닫기") and sd[1][0] == "DC_PULSE_STOP"
    assert R.has_log(e, "완료 — 종료 방식 상한")


def test_t135_ui_path_rejects_out_of_range_factor_value(tmp_path):
    """runtime/chamber_runtime.py:3359·8653 — 화면 파일 선택 경로도 tooling·density·z_factor 범위 밖이면 다른 레시피 오류처럼
    경고하고 불러오지 않음('density='150' → 범위 0.5~99.99 g/cm3 밖')."""
    from v2.ch12.recipe import TEMPLATE_COLUMNS, load_recipe_file
    r = _rrow(Process_name="p1", density="150", process_time="1", use_rf_power="T", rf_power="100")
    p = tmp_path / "ui.csv"
    p.write_text(",".join(TEMPLATE_COLUMNS) + "\r\n" + ",".join(r[c] for c in TEMPLATE_COLUMNS) + "\r\n", encoding="utf-8-sig")
    res = load_recipe_file(str(p), origin="ui")
    assert not res.ok and res.code == "E230" and "density='150' → 범위 0.5~99.99 g/cm3 밖" in res.message


# ═════════════ TEST-088 오류 팝업 닫기 = 에러 해제(v1 tests/test_loadlock_state.py:123) ═════════════
def test_t088_error_clear_commands_clear_latch_regardless_of_result(tmp_path):
    """runtime/plasma_cleaning_runtime.py:2926-2935·chamber_runtime.py:8487-8513 — 팝업을 확인/X/ESC 어느 것으로 닫아도
    clear_error('pc', ch)·clear_error('chamber', ch). v2 는 웹·로컬 화면의 '오류 표시 해제' 명령(PC_ERROR_CLEAR·CHAMBER_ERROR_CLEAR)."""
    from v2.common.commands import Source
    from v2.tests.test_core import done as core_done, make as core_make
    rig = core_make(tmp_path)
    try:
        rs = rig.asm.run_state if hasattr(rig, "asm") and hasattr(rig.asm, "run_state") else rig.run_state
        rs.set_error("pc", 2, "GV 인터락")
        rs.set_error("chamber", 1, "E301")
        rig.asm.proc["ch1"].error = "E301"
        assert rig.command("PC_ERROR_CLEAR", {"ch": 2}, source=Source.HMI).ok
        assert rig.command("CHAMBER_ERROR_CLEAR", {"ch": 1}, source=Source.WEB).ok
        assert not rs.has_error("pc", 2) and not rs.has_error("chamber", 1) and rig.asm.proc["ch1"].error is None
    finally:
        core_done(rig)


# ═════════════ TEST-087 Loadlock_Chamber = 마지막으로 끝난 PC 결과(v1 tests/test_loadlock_state.py:48-120) ═════════════
@pytest.mark.parametrize("last,want", [
    ({"finished": {1: 1.0, 2: 2.0}, "error": {1: True, 2: False}}, None),     # a 사고: ch1 실패 뒤 ch2 성공
    ({"finished": {1: 2.0, 2: 1.0}, "error": {1: True, 2: False}}, 1),        # b
    ({"finished": {1: 1.0}, "error": {1: True}}, 1),                          # c
    ({"finished": {1: 1.0, 2: 2.0}, "error": {}}, None),                      # d
    ({}, None),                                                               # e 이력 없음
    ({"finished": {1: 3.0, 2: 2.0}, "error": {1: True, 2: True}}, 1),         # f 둘 다 실패 → 더 최근
    ({"finished": {}, "error": {2: True}}, 2),                                # f2 종료 이력 없이 에러만 → 보수적
])
def test_t087_loadlock_error_channel(last, want):
    """host/handlers.py:45-67 loadlock_error_channel."""
    from v2.ch12.robot_engines import loadlock_error_channel
    assert loadlock_error_channel(last) == want


def test_t087_loadlock_status_running_first_and_exception_is_error():
    """host/handlers.py:586-665 — 실행 중(또는 CH1 IG 대기)이면 에러 판정보다 'running' 먼저, 조회 예외는 'error'."""
    from v2.ch12.robot_base import RobotBase

    class Eng:
        def __init__(self, ps, last, boom=False):
            self.ps, self.last, self.boom = ps, last, boom

        def pc_state(self):
            if self.boom:
                raise RuntimeError("조회 실패")
            return self.ps

        def last_pc_result(self):
            return self.last
    bad = {"finished": {1: 2.0}, "error": {1: True}}
    f = lambda eng: RobotBase.loadlock_status(SimpleNamespace(engines=eng))  # noqa: E731
    assert f(Eng({"running": {1: True}}, bad)) == "running"
    assert f(Eng({"running": {}}, bad)) == "error"
    assert f(Eng({"running": {}}, {})) == "idle"
    assert f(Eng({"running": {}, "ch1_ig_wait": True}, {})) == "running"
    assert f(Eng({}, {}, boom=True)) == "error"


# ═════════════ TEST-190 VACUUM_ON 중복(v1 tests/test_vacuum_on_busy_reject.py:71) ═════════════
def test_t190_duplicate_vacuum_on_rejected_without_touching_roughing(tmp_path):
    """host/handlers.py — VACUUM_ON 진행 중 들어온 VACUUM_ON 은 즉시(<1 s) E321 '… VACUUM_ON 진행 중' 거절, PLC 쓰기 없음
    (진행 중 펌프다운의 L_R_P_SW·L_R_V_SW 를 원복하지 않음 — 코일 쓰기가 단독 실행과 같다). VACUUM_OFF 중 VACUUM_ON 거절·자기 실패
    (러핑 중 게이트 열림 E301 + 원복)는 v1 실행 골든 v_off_then_on·von_gate_opens."""
    import json as _json
    from v2.ch12.sim import robot_rig as RR
    eng = {"rs": {}, "rt": {"1": {}, "2": {}}, "pc": {"selected_ch": 1, "is_running": False}}
    one = RR.RobotRig(out_dir=tmp_path / "a")
    try:
        solo = one.run_case({"name": "solo", "cmd": "VACUUM_ON", "data": {"timeout_s": 20}, "eng": eng})
    finally:
        one.done()
    two = RR.RobotRig(out_dir=tmp_path / "b")
    try:
        both = two.run_case({"name": "dup", "cmd": "VACUUM_ON", "data": {}, "eng": eng,
                             "jobs": [[0.0, "VACUUM_ON", {"timeout_s": 20}], [2.0, "VACUUM_ON", {}]]})
    finally:
        two.done()
    second = both["results"][1]
    body = second["body"] if isinstance(second["body"], dict) else _json.loads(second["body"])
    assert body["error_code"] == "E321" and "VACUUM_ON 진행 중" in body.get("detail", "") + body.get("message", "")
    assert second["elapsed"] < 1.0
    assert both["writes"] == solo["writes"] and both["results"][0]["body"] == solo["results"][0]["body"]


def test_t118_classify_log_level():
    """util/app_logging.py:399 (v1 tests/test_plc_log_route.py:128) — 시각·태그 머리를 떼고 판정."""
    import logging
    from v2.ch12.outputs.app_log import classify_log_level as c
    W, E, I = logging.WARNING, logging.ERROR, logging.INFO
    assert c("PLC_REMOTE", "2026-10-22 10:00:00 [GET_SPUTTER_STATUS] WARN in-lock IO 2.5s") == W
    assert c("PLC_REMOTE", "2026-10-22 10:00:00 WARN lock-wait 3.0s") == W
    assert c("PLC_REMOTE", "2026-10-22 10:00:00 [X] ❌ fail") == E
    assert c("PLC_REMOTE", "2026-10-22 10:00:00 [CH1_GATE_OPEN] read L_ATM -> True") == I
    assert c("PLC", "WARN lock-wait 1.2s") == W and c("PLC", "[CH1&2] PLC 연결 끊김") == I
    assert c("ERROR/HOST", "x") == E and c("CH1", "⚠ 경고") == W and c("CH1", "ERROR: x") == E and c("CH1", "정상 진행") == I


# ═════════════ 전원 OFF(v1 tests/test_power_off_plc.py:137-404, test_power_off_owner.py:230) ═════════════
def test_t123_dc_never_started_and_zero_retry_then_success():
    """device/dc_power.py:435-471 — 안 켠 DC 는 쓰기 0·finished 1, 0 W 쓰기가 잠깐 실패하면 재시도 뒤 0 W → SET OFF → finished."""
    from v2.ch12.sim.plc_rig import PlcRig
    r = PlcRig(dc=True, cfg={"CH2_SUPPORTS_HEATER": False})
    t0 = r.c.monotonic()
    box = []
    r.dc1.stop(on_done=box.append)
    r.wait(lambda: bool(box), 5.0)
    assert box == [None] and [p for p in r.pdus(t0) if p[0] in (5, 6)] == []
    assert [e.kind for e in r.pevents if e.kind.startswith("power_off")] == ["power_off_finished"]
    st = []
    r.dc1.start(50.0, on_done=st.append)
    r.wait(lambda: bool(st), 5.0)
    r.after(1.0)
    t = r.c.monotonic()
    r.sim.add_fault("exception", t, t + 1.2)
    n_fin = sum(e.kind == "power_off_finished" for e in r.pevents)
    stop = []
    r.dc1.stop(on_done=stop.append)
    r.wait(lambda: bool(stop), 30.0)
    zeros = [p for p in r.pdus(t) if p == bytes.fromhex("0600040000")]
    assert stop == [None] and len(zeros) >= 2 and not r.sim.coils[80] and r.dc1.output_off_unconfirmed is False
    assert sum(e.kind == "power_off_finished" for e in r.pevents) == n_fin + 1
    assert not [e for e in r.pevents if e.kind == "power_off_failed"]
    r.done()


def test_t124_rf_cont_never_started_and_zero_always_fails_then_next_ok():
    """device/rf_power.py:631-667 — 안 켠 RF 는 오경보 없음(power_off_failed 없음); 0 W 가 끝내 안 되면 power_off_failed·SET OFF
    안 함·output_off_unconfirmed, 다음 정지는 성공."""
    from v2.ch12.sim.plc_rig import PlcRig
    r = PlcRig(rf=True)
    box = []
    r.rf.stop(on_done=box.append)
    r.wait(lambda: bool(box), 10.0)
    assert box == [None] and not [e for e in r.pevents if e.kind == "power_off_failed"]
    st = []
    r.rf.start(5.0, on_done=st.append)
    r.wait(lambda: bool(st), 5.0)
    r.after(3.0)
    t = r.c.monotonic()
    r.sim.add_fault("exception", t, t + 60.0)
    stop = []
    r.rf.stop(on_done=stop.append)
    r.wait(lambda: bool(stop), 90.0)
    assert stop == ["POWER_OFF_UNCONFIRMED"] and r.rf.output_off_unconfirmed
    assert [e for e in r.pevents if e.kind == "power_off_failed"] and r.sim.coils[82]       # SET OFF 하지 않음
    r.after(61.0)
    again = []
    r.rf.stop(on_done=again.append)
    r.wait(lambda: bool(again), 90.0)
    assert again == [None] and not r.sim.coils[82] and not r.rf.output_off_unconfirmed
    r.done()


def test_t122_event_pumps_pass_source_and_off_kind():
    """runtime/chamber_runtime.py:2348·2755 — DC/DC2/RF/DC Pulse/RF Pulse OFF 실패가 on_power_off_failed(source, off_kind) 로:
    ('DC Power','DC_OFF')·('DC2 Power','DC2_OFF')·('RF Power','RF_OFF')·('DCPulse','DCPULSE_OFF')·('RFPulse','RFPULSE_OFF'),
    safety 'output_state_unconfirmed', DC 실패 플래그 리셋, '⚠️ … OFF 실패 - 출력 상태 미확인' 경고·run_warnings."""
    h = _events_host(2)
    h.dc_failed = {1: True, 2: True}
    h._ev_dc("power_off_failed", _ev(detail="DC1 0W 실패", data={"unit": "DC1"}))
    h._ev_dc("power_off_failed", _ev(detail="DC2 0W 실패", data={"unit": "DC2"}))
    h._ev_rf("power_off_failed", _ev(detail="RF 0W 실패", data={}))
    h._ev_dcp("command_failed", _ev(cmd="OUTPUT_OFF", detail="x"))
    h._ev_rfp("command_failed", _ev(cmd="RF_OFF", detail="y"))
    got = [(c[1][0], c[1][1], c[2]["meta"].get("safety")) for c in h.runner.calls if c[0] == "power_off_failed"]
    assert got == [("DC Power", "DC_OFF", "output_state_unconfirmed"), ("DC2 Power", "DC2_OFF", "output_state_unconfirmed"),
                   ("RF Power", "RF_OFF", "output_state_unconfirmed"), ("DCPulse", "DCPULSE_OFF", "output_state_unconfirmed"),
                   ("RFPulse", "RFPULSE_OFF", "output_state_unconfirmed")]
    assert h.dc_failed == {1: False, 2: False} and len(h.e.run_warnings) == 5
    assert all("OFF 실패 - 출력 상태 미확인" in m for m in h.e.chats)


def test_t121_power_off_failure_ends_only_its_own_step():
    """controller/process_controller.py:962-982 on_power_off_failed — 종료 절차 중 자기 단계(DC_PULSE_STOP 에 DCPulse 실패)면
    그 대기를 바로 끝냄(오류 기록·끝까지 진행), 정상 공정 중이면 종료 절차 시작, 실행 중 아니면 무동작."""
    from v2.ch12.process_runner import StepRunner
    from v2.tests.test_process_stop import Host, _go
    h = Host(delays={"DC_PULSE_STOP": 30.0})

    def act(r):
        r.request_stop()
        h.clock.wait_until(h.clock.monotonic() + 1.0, None)          # DC_PULSE_STOP 대기 중
        r.power_off_failed("DCPulse", "DCPULSE_OFF", "OUTPUT_OFF 미확인")
    r, res = _go(h, at=5.0, act=act)
    sd = [x for x in r.trace if x[4]]
    i = next(k for k, x in enumerate(sd) if x[2] == "DC_PULSE_STOP")
    assert sd[i + 1][0] - sd[i][0] < 5.0                              # 30 s 를 기다리지 않음
    assert [x[2] for x in sd] == [s.action for s in h.shutdown_steps()] and res.detail["result"] in ("STOP", "FAIL")
    h2 = Host()
    r2, res2 = _go(h2, at=100.0, act=lambda r: r.power_off_failed("DC Power", "DC_OFF", "DC1 0W 실패"))
    assert res2.detail["result"] == "FAIL" and any("종료 절차를 시작합니다" in l for l in h2.lines)
    h3 = Host()
    StepRunner(h3).power_off_failed("RF Power", "RF_OFF", "x")      # 실행 중 아님 → 무동작
    assert h3.lines == []
    h3.clock.close()


# ═════════════ 펄스 '켠 적 없음' OFF·RF Pulse OFF(v1 tests/test_pulse_unused_off.py, test_rf_pulse_off.py) ═════════════
def test_t131_dc_pulse_unused_off_ack_and_truth_table():
    """device/dc_pulse.py:1236-1290 — off_needs_confirm: 켠 적 없으면 False, 켜려고 보냈거나 켰거나 미확인이면 True; output_off_unused
    ACK → 0.5 s 안에 OUTPUT_OFF_UNUSED 확인·경고 없음·'켠 적 없음 … (ACK)'·STOP_GUARD(뒤 쓰기 차단)."""
    d = _dcp()
    assert d.off_needs_confirm() is False
    for attr in ("_out_on", "_output_maybe_on", "output_off_unconfirmed"):
        d = _dcp()
        setattr(d, attr, True)
        assert d.off_needs_confirm() is True, attr
    from v2.tests.test_dcpulse import Rig
    r = Rig()
    t0 = r.c.monotonic()
    assert r.call(r.d.output_off_unused) == (None, True)
    assert r.c.monotonic() - t0 < 0.5 + 0.2 and r.sent() == ["02 80 00 02 03 87"]
    assert [e.cmd for e in r.ev("command_confirmed")] == ["OUTPUT_OFF_UNUSED"] and not r.ev("command_failed")
    assert any("켠 적 없음" in e.detail and "(ACK)" in e.detail for e in r.ev("status"))
    assert r.d._stop_guard is True
    r.done()


def test_t132_rf_pulse_unused_and_needs_confirm_and_double_stop():
    """device/rf_pulse.py:768·904-914 — 정상 장비 stop_unused 는 RF_OFF 1회·power_off_finished 1('켠 적 없음 … CSR 0'), RF_ON 뒤엔
    확인 OFF 까지 needs_confirm True·확인 뒤 False, 두 번 stop 은 시퀀스 하나(결과는 둘 다 받음)."""
    from v2.tests.test_rfpulse import Rig
    r = Rig()
    assert r.d.off_needs_confirm() is False
    assert r.call(r.d.stop_unused) == (None, True)
    assert r.sent() == ["08 01 09"] and len(r.ev("power_off_finished")) == 1
    assert any("켠 적 없음" in e.detail and "CSR 0" in e.detail for e in r.ev("status"))
    r.done()
    r = Rig()
    assert r.call(r.d.start_pulse_process, 100.0) == (None, True)
    assert r.d.off_needs_confirm() is True
    n0 = len(r.sim.received)
    box = []
    r.d.stop_process(on_done=lambda *x: (box.append(x), r.c.notify()))
    r.d.stop_process(on_done=lambda *x: (box.append(x), r.c.notify()))
    r.c.wait_until(None, lambda: len(box) == 2)
    assert box == [(None, True), (None, True)] and len(r.ev("power_off_finished")) == 1
    assert sum(1 for f in r.sent(n0) if bytes.fromhex(f)[1] == 1) == 1                 # RF_OFF(cmd 1) 한 번
    assert r.d.off_needs_confirm() is False
    r.done()


def test_t133_engine_routes_pulse_stop_by_needs_confirm():
    """runtime/chamber_runtime.py:1264·1350 — 정지 단계: 켠 적 없으면 1회 OFF(output_off_unused·stop_unused), 아니면 확인형;
    DC Pulse OUTPUT_OFF_UNUSED 확인은 정지 단계 완료(DCPULSE_OFF) 로."""
    from v2.ch12.process_actions import ProcessActions
    calls = []

    class D:
        def __init__(self, need):
            self.need = need

        def off_needs_confirm(self):
            return self.need

        def __getattr__(self, n):
            return lambda *a, **k: calls.append(n)
    for need, want in ((False, ["output_off_unused", "stop_unused"]), (True, ["output_off", "stop_process"])):
        calls.clear()
        host = SimpleNamespace(dcp=D(need), rfp=D(need))
        ProcessActions._a_dc_pulse_stop(host, None)
        ProcessActions._a_rf_pulse_stop(host, None)
        assert calls == want
    h = _events_host(2)
    h._ev_dcp("command_confirmed", _ev(cmd="OUTPUT_OFF_UNUSED"))
    assert h.matches == [("DCPULSE_OFF",)] and h.e.chats == []
    from v2.ch12.params import dcpulse as PD, rfpulse as PR
    from v2.common.paramdef import defaults
    assert defaults(PD.PARAMS)["DCP_UNUSED_OFF_WAIT_S"] == 3.0 and defaults(PR.PARAMS)["RFPULSE_UNUSED_OFF_WAIT_S"] == 3.0


def test_t142_t143_rf_pulse_off_server_down_cleanup_and_start_stop():
    """device/rf_pulse.py:344·443·728·888·1987 — 서버 다운이면 command_failed RF_OFF·output_off_unconfirmed; 시작 중 stop 이면 RF_ON
    안 보냄('시작 시퀀스 중단'); stop 없이 cleanup 이면 RF_OFF 확인(_output_maybe_on False), 워커 없으면 연결 없이 끝(켰을 수 있으면
    output_off_unconfirmed 로 남김)."""
    from v2.tests.test_rfpulse import Rig
    r = Rig()
    assert r.call(r.d.start_pulse_process, 100.0) == (None, True)
    r.sim.add_fault("disconnect", r.c.monotonic(), r.c.monotonic() + 100.0)
    reason, ok = r.call(r.d.stop_process)
    assert ok is False and r.d.output_off_unconfirmed and [e for e in r.ev("command_failed") if e.cmd == "RF_OFF"]
    r.done()
    r = Rig()
    box = []
    r.d.start_pulse_process(100.0, on_done=lambda *x: box.append(x))
    r.c.wait_until(r.c.monotonic() + 0.3)
    assert r.call(r.d.stop_process)[1] is True
    r.c.wait_until(r.c.monotonic() + 20.0)
    assert box and box[0][1] is False and 2 not in [bytes.fromhex(f)[1] for f in r.sent()]
    assert not r.ev("target_reached")
    r.done()
    r = Rig()
    assert r.call(r.d.start_pulse_process, 100.0) == (None, True)
    got = []
    r.d.cleanup(on_done=lambda u: (got.append(u), r.c.notify()))
    r.c.wait_until(None, lambda: bool(got))
    assert got == [False] and r.d._output_maybe_on is False and not r.sim.out
    r.done()
    from v2.ch12.drivers import rfpulse as RP
    from v2.ch12.params import rfpulse as PR
    from v2.common.paramdef import defaults
    d = RP.RfPulseDriver(_Clk(), None, defaults(PR.PARAMS), True, on_event=lambda e: None, chamber="ch1")
    d._output_maybe_on = True
    out = []
    d.cleanup(on_done=out.append)
    assert out == [False] and d.output_off_unconfirmed is True and d.cleanup_off_unconfirmed is False


# ═════════════ 정리 라벨·공유 IG·MFC 사용자(v1 tests/test_run_log_close.py:224, test_ig_pc_same_ch.py, test_shared_resource_owner.py) ═════════════
def test_t147_mfc_cleanup_label_and_ig_cancel_labels():
    """device/mfc.py:1218 on_process_cleanup — '대기 중 명령 0개 폐기 (cleanup)'·setpoint 0·flow flag False·오류 카운터 0,
    on_process_finished 라벨 '(process finished (ok))'/'(fail)'; device/ig.py:475 cancel_wait 라벨 '(user cancel / stop)'·'(cleanup)'."""
    from v2.tests.test_ig import Rig as IgRig
    from v2.tests.test_mfc import Rig as MfcRig
    r = MfcRig("ch1")
    r.d.last_setpoints = {1: 5.0, 2: 3.0, 3: 1.0}
    r.d.flow_on_flags = {1: True, 2: True, 3: False}
    r.d.on_process_cleanup()
    r.c.wait_until(r.c.monotonic() + 0.5)
    assert any("대기 중 명령 0개 폐기 (cleanup)" in e.detail for e in r.kinds("status"))
    assert r.d.last_setpoints == {1: 0.0, 2: 0.0, 3: 0.0} and r.d.flow_on_flags == {1: False, 2: False, 3: False}
    if hasattr(r.d, "flow_error_counters"):
        assert set(r.d.flow_error_counters.values()) <= {0}
    for ok, label in ((True, "(process finished (ok))"), (False, "(process finished (fail))")):
        r.d.on_process_finished(ok)
        r.c.wait_until(r.c.monotonic() + 0.5)
        assert any(label in e.detail for e in r.kinds("status")), label
    r.done()
    g = IgRig("ch1")
    for reason, label in ((None, "(user cancel / stop)"), ("cleanup", "(cleanup)")):
        n0 = len(g.events)
        (g.d.cancel_wait() if reason is None else g.d.cancel_wait(reason))
        g.c.wait_until(g.c.monotonic() + 2.0)
        assert any(label in (e.detail or "") for e in g.events[n0:] if e.kind == "status"), reason
    g.done()


def test_t080_chamber_ig_pump_quiet_while_same_ch_pc_but_forwards_base_events():
    """runtime/chamber_runtime.py:2253 — 같은 CH PC 가 IG 를 쓰는 동안 챔버 IG 펌프는 로그·압력 기록을 조용히(공정 결과 전달은 유지)."""
    h = _events_host(1)
    h.e.ig_in_use_by_pc = lambda: True
    added = []
    h.e.stats = SimpleNamespace(add_ig=added.append)
    h._ev_ig("status", _ev(detail="Base Pressure 대기 시작"))
    h._ev_ig("pressure", SimpleNamespace(pressure=6.3e-5))
    assert h.logs == [] and added == []
    h._ev_ig("base_reached", _ev())
    assert h.matches == [("IG_OK",)]
    h.e.ig_in_use_by_pc = lambda: False
    h._ev_ig("pressure", SimpleNamespace(pressure=2.8e-5))
    assert added == [2.8e-5]


def test_t155_shared_poll_users_counts():
    """controller/runtime_state.py:145-170 acquire_shared/release_shared — 남은 사용자 수 반환, 같은 owner 재획득 멱등, 모르는 owner
    해제는 집합 유지, 마지막 해제 0(v2 는 MFC 드라이버 poll_users 가 주인 — shared.MfcPoll)."""
    from v2.ch12.shared import MfcPoll
    from v2.tests.test_mfc import Rig as MfcRig
    r = MfcRig("ch1")
    m = MfcPoll(r.d)
    m.acquire("chamber1")
    m.acquire("pc2")
    m.acquire("pc2")
    assert m.users == {"chamber1", "pc2"}
    assert m.release("nobody") == 2 and m.users == {"chamber1", "pc2"}
    assert m.release("pc2") == 1 and m.release("chamber1") == 0 and m.users == set()
    r.done()


# ═════════════ OES 워커 DLL 호출·일련번호·연결 감시(v1 tests/test_oes_worker_cyusb.py — 가짜 DLL 객체로) ═════════════
_LIST_A = [r"\\?\usb#vid_0547&pid_1002#a#{ae18aa60-7f6a-11d4-97dd-00010229b959}",
           r"\\?\usb#vid_0547&pid_1002#b#{ae18aa60-7f6a-11d4-97dd-00010229b959}"]


class _OesFakeDll:
    """호출을 (이름, 인자값) 으로 기록하는 가짜 SPdbUSBm.dll(v1 시험의 FakeDLL 과 같음)."""

    def __init__(self, *, read_ok=lambda n: True, serial="SN-A1", tests=2):
        self.calls, self.reads, self._read_ok, self._serial, self._tests = [], 0, read_ok, serial, tests

    def _read(self, buf, ch):
        self.reads += 1
        if not self._read_ok(self.reads):
            return -1
        for i in range(len(buf)):
            buf[i] = self.reads
        return 0

    def _dev_info(self, mb, sb, ch):
        import ctypes
        if self._serial is None:
            return -1
        ctypes.memmove(mb, b"SM303\x00", 6)
        s = self._serial.encode() + b"\x00"
        ctypes.memmove(sb, s, len(s))
        return 0

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        impl = {"spReadDataEx": self._read, "spDevInfo": self._dev_info,
                "spTestAllChannels": lambda *_a: self._tests, "spGetWLTable": lambda *_a: -1}.get(name)
        dll = self

        def fn(*args):
            dll.calls.append((name, tuple(getattr(a, "value", a) for a in args)))
            return impl(*args) if impl else 0
        object.__setattr__(self, name, fn)
        return fn

    def names(self):
        return [c[0] for c in self.calls]


@pytest.fixture
def oes_env(tmp_path, monkeypatch):
    pytest.importorskip("numpy")
    from v2.ch12.workers import oes_common as C
    from v2.ch12.workers import oes_device as OD
    out = []
    monkeypatch.setattr(C, "_print_json", out.append)
    monkeypatch.setattr(C, "_runlog", lambda m: None)
    monkeypatch.setattr(OD, "_runlog", lambda m: None)
    C.set_home(str(tmp_path))
    monkeypatch.setattr(OD, "_DDL_LOCK", None)                 # asyncio.run 마다 새 루프(v1 시험도 W._DDL_LOCK = None)
    env = SimpleNamespace(tmp=tmp_path, out=out, C=C, OD=OD, mp=monkeypatch,
                          statuses=lambda: [o.get("message", "") for o in out if o.get("kind") == "status"])
    yield env
    C.set_home(None)


def _oes_ready(env, dll, avg=1):
    o = env.OD.OESAsync(chamber=1, usb_index=0, save_directory=str(env.tmp / "save"), avg_count=avg)
    o.sp_dll = dll
    o._bind_functions()
    o.sChannel, o._npix = 0, 1056
    o._roi_start, o._roi_end = env.OD.ROI_START_DEFAULT, env.OD.ROI_END_DEFAULT
    dll.calls.clear()
    return o


def test_t104_slice_avg_reads_only_and_settings_without_baseline(oes_env):
    """apps/oes_service/oes_api.py:165·560 — 슬라이스 평균은 spReadDataEx×3 만(spSetupGivenChannel 없음), 설정 적용은
    spSetTrgEx(11)·spSetTEC(1)·spSetDblIntEx(50.0) 만(spSetBaseLineCorrection 없음)."""
    dll = _OesFakeDll()
    o = _oes_ready(oes_env, dll, avg=3)
    x, y = o._acquire_one_slice_avg()
    assert x is not None and y is not None and dll.names() == ["spReadDataEx"] * 3
    dll.calls.clear()
    o._apply_device_settings_blocking(0, 50)
    assert dll.calls == [("spSetTrgEx", (11, 0)), ("spSetTEC", (1, 0)), ("spSetDblIntEx", (50.0, 0))]


def test_t104_link_ok_rules(oes_env):
    """oes_api.py:1940 link_ok — 기준 없으면(옛 드라이버) 늘 정상, 목록 조회 일시 실패(None)도 정상, 순서만 다르면 정상, 빠지면 비정상."""
    o = _oes_ready(oes_env, _OesFakeDll())
    o._link_baseline = None
    assert o.link_ok() is True
    o._link_baseline = sorted(_LIST_A)
    for cur, want in ((None, True), (list(reversed(_LIST_A)), True), ([], False), (_LIST_A[:1], False)):
        oes_env.mp.setattr(oes_env.OD, "_list_cyusb_interfaces", lambda c=cur: c)
        assert o.link_ok() is want


def _oes_init(env, dll, *, config=None, lists=None):
    import asyncio
    import json as _json
    if config is not None:
        (env.tmp / "oes_config.json").write_text(_json.dumps(config), encoding="utf-8")
    seq = list(lists or [_LIST_A])

    def _fake_list():
        cur = seq.pop(0) if len(seq) > 1 else seq[0]
        return None if cur is None else list(cur)
    o = env.OD.OESAsync(chamber=1, usb_index=0, save_directory=str(env.tmp / "save"))

    def _load():
        o.sp_dll = dll
        o._bind_functions()
    o._load_and_bind_blocking = _load
    env.mp.setattr(env.OD, "_list_cyusb_interfaces", _fake_list)
    env.OD._DDL_LOCK = None
    ok = asyncio.run(o.initialize_device())
    return o, ok


def test_t105_expected_serial_rules(oes_env):
    """oes_api.py:857·1160 — 기대 일련번호 불일치 실패('분광기 일련번호 불일치: CH1 기대=sn-a1, 실제=SN-B2'), 대소문자·공백 무시 일치,
    읽기 실패면 '실제=(읽기 실패)' 실패, 기대값 없으면 통과 + '[init] 분광기 확인 USB0: 모델=SM303(PDA), 일련번호=SN-A1, 연결 목록=…'
    (기준 목록 저장), 초기화 중 연결 목록이 바뀌면 '초기화 중 분광기 연결 변화'."""
    o, ok = _oes_init(oes_env, _OesFakeDll(serial="SN-B2"), config={"enabled": False, "expected_serial": {"1": " sn-a1 ", "2": "SN-B2"}})
    assert ok is False and o._last_error == "분광기 일련번호 불일치: CH1 기대=sn-a1, 실제=SN-B2"
    o, ok = _oes_init(oes_env, _OesFakeDll(serial="SN-A1"), config={"expected_serial": {"1": " sn-a1 "}})
    assert ok is True and o._serial == "SN-A1"
    o, ok = _oes_init(oes_env, _OesFakeDll(serial=None), config={"expected_serial": {"1": "SN-A1"}})
    assert ok is False and "실제=(읽기 실패)" in o._last_error
    oes_env.out.clear()
    o, ok = _oes_init(oes_env, _OesFakeDll(serial="SN-A1"), config={"enabled": True})
    assert ok is True and o._link_baseline == sorted(_LIST_A)
    assert any(m.startswith("[init] 분광기 확인 USB0: 모델=SM303(PDA), 일련번호=SN-A1, 연결 목록=") for m in oes_env.statuses())
    (oes_env.tmp / "oes_config.json").unlink()
    o, ok = _oes_init(oes_env, _OesFakeDll(serial=None))
    assert ok is True and o._serial == ""
    o, ok = _oes_init(oes_env, _OesFakeDll(), lists=[_LIST_A, _LIST_A[:1]])
    assert ok is False and o._last_error == "초기화 중 분광기 연결 변화"
    o, ok = _oes_init(oes_env, _OesFakeDll(), lists=[None])                 # 옛 드라이버 — 감시 끔
    assert ok is True and o._link_baseline is None and o.link_ok() is True


def test_t106_config_read_failure_turns_serial_check_off(oes_env):
    """oes_api.py:857 — oes_config.json 을 못 읽으면 '[init] oes_config.json 읽기 실패 → 일련번호 확인 꺼짐', 없으면 '확인=꺼짐(expected_serial 없음)'."""
    (oes_env.tmp / "oes_config.json").write_text("{깨진", encoding="utf-8")
    o, ok = _oes_init(oes_env, _OesFakeDll(serial="SN-A1"))
    assert ok is True and "[init] oes_config.json 읽기 실패 → 일련번호 확인 꺼짐" in oes_env.statuses()
    assert any("확인=꺼짐(expected_serial 없음)" in m for m in oes_env.statuses())


def test_t107_cleanup_close_rules(oes_env):
    """oes_api.py:1070 cleanup — 연결 목록이 기준과 다르면 '[cleanup] 연결 목록이 기준과 다름 → 닫기 명령 생략', 같거나 조회 실패(None)·
    감시 꺼짐이면 spCloseGivenChannel(0) 1회; 끝나면 sChannel −1·sp_dll None."""
    import asyncio
    logs = []
    oes_env.mp.setattr(oes_env.OD, "_runlog", logs.append)

    def run(listing, baseline=True):
        dll = _OesFakeDll()
        o = _oes_ready(oes_env, dll)
        o._link_baseline = sorted(_LIST_A) if baseline else None
        oes_env.mp.setattr(oes_env.OD, "_list_cyusb_interfaces", listing)
        oes_env.OD._DDL_LOCK = None
        asyncio.run(o.cleanup())
        assert o.sChannel == -1 and o.sp_dll is None
        return dll
    assert run(lambda: list(_LIST_A[:1])).names().count("spCloseGivenChannel") == 0
    assert "[cleanup] 연결 목록이 기준과 다름 → 닫기 명령 생략" in logs
    assert run(lambda: list(_LIST_A)).calls == [("spCloseGivenChannel", (0,))]
    assert run(lambda: None).calls == [("spCloseGivenChannel", (0,))]
    assert run(lambda: list(_LIST_A[:1]), baseline=False).calls == [("spCloseGivenChannel", (0,))]


def test_t106_other_chamber_only_fails_before_reset_and_pre_read_once(oes_env):
    """oes_api.py:857·1252 — 다른 챔버 분광기만 꽂혀 있으면(사전 읽기에서 불일치) 리셋·초기화 전에 실패(spDevInfo·spTestAllChannels 만),
    사전 읽기가 맞으면 spDevInfo 1회(채널 설정 전)·'읽은 시점=사전, 확인=일치(기대 SN-A1)'."""
    dll = _OesFakeDll(serial="SN-B2", tests=1)
    o, ok = _oes_init(oes_env, dll, config={"expected_serial": {"1": "SN-A1"}}, lists=[_LIST_A[:1]])
    assert ok is False and o.sChannel == -1 and o._last_error == "분광기 일련번호 불일치: CH1 기대=SN-A1, 실제=SN-B2"
    assert sorted(set(dll.names())) == ["spDevInfo", "spTestAllChannels"]
    oes_env.out.clear()
    dll = _OesFakeDll(serial="SN-A1")
    o, ok = _oes_init(oes_env, dll, config={"expected_serial": {"1": "SN-A1"}})
    assert ok is True and o.sChannel == 0 and dll.names().count("spDevInfo") == 1
    assert dll.names().index("spDevInfo") < dll.names().index("spSetupGivenChannel")
    assert any("읽은 시점=사전, 확인=일치(기대 SN-A1)" in m for m in oes_env.statuses())


def test_t106_measure_blocked_until_link_ok_sends_no_commands(oes_env):
    """oes_api.py:1252 _LinkGuard.ensure_at_start — 측정 시작 때 목록이 기준과 다르면(대기 한도 안에 안 돌아옴) USB 명령 0건·CSV 없음·'[link]'."""
    import asyncio
    from v2.ch12.workers import oes_daemon as ODm, oes_guard as OG
    dll = _OesFakeDll()
    o = _oes_ready(oes_env, dll)
    o._link_baseline = sorted(_LIST_A)
    oes_env.mp.setattr(oes_env.OD, "_list_cyusb_interfaces", lambda: list(_LIST_A[:1]))
    oes_env.mp.setattr(OG, "_list_cyusb_interfaces", lambda: list(_LIST_A[:1]))
    for k, v in (("_LINK_WAIT_AT_START_S", 0.2), ("_LINK_POLL_S", 0.01), ("_LINK_SETTLE_S", 0.01)):
        oes_env.mp.setattr(OG, k, v)
    oes_env.OD._DDL_LOCK = None
    out_csv = oes_env.tmp / "a" / "x.csv"
    with pytest.raises(RuntimeError, match=r"\[link\]"):
        asyncio.run(ODm._daemon_measure_once(oes=o, ch=1, usb=0, duration_s=1.0, integration_ms=50, sample_interval_s=0.05,
                                             avg_count=1, out_dir=None, out_csv=out_csv))
    assert dll.calls == [] and not out_csv.exists()


# ═════════════ OES 저장 위치·완료 문구(v1 tests/test_oes_local_dir.py) ═════════════
def test_t099_worker_never_copies_to_g_and_keeps_local():
    """apps/oes_service/oes_api.py:134 _copy_csv_to_nas → (False, None, None, False) — G: 경로·robocopy 없음, 로컬 CSV 유지."""
    import asyncio
    from v2.ch12.workers import oes_common as C
    assert asyncio.run(C._copy_csv_to_nas(Path("x.csv"), 1)) == (False, None, None, False)


@pytest.mark.parametrize("obj,msg,nas", [
    ({"ok": True, "nas_ok": False}, "OES 측정 완료(로컬 저장)", False),
    ({"ok": True, "nas_ok": False, "nas_error": "robocopy failed rc=8"}, "OES 측정 완료(로컬 저장, NAS 실패: robocopy failed rc=8)", False),
    ({"ok": True, "nas_ok": True, "nas_csv": r"G:\공유 드라이브\OES\CH1\a.csv", "local_deleted": False}, "OES 측정 완료(NAS 저장)", True),
])
def test_t100_daemon_finish_messages(tmp_path, obj, msg, nas):
    """device/oes.py:648·1328 — 새 워커는 로컬 저장 문구·로컬 CSV 유지, 재빌드 전 옛 워커(G: 복사 성공·로컬 못 지움)면 'NAS 저장'·
    out_csv = G: 경로·로컬 삭제, NAS 실패면 '(로컬 저장, NAS 실패: …)'."""
    import itertools
    from v2.common.clock import VirtualClock
    from v2.ch12.drivers import oes_link as OL
    out = tmp_path / "OES_Data_1.csv"
    out.write_text("Time\n", encoding="utf-8")
    c = VirtualClock()
    c.register()
    f = SimpleNamespace(clock=c, _ids=itertools.count(1), _daemon=object(), _rows_seen=3, is_running=False, _waiter=None)
    f._ensure_daemon = lambda t, force: True
    f._f = lambda k, d: d
    f._c = lambda k, d=None: d
    f._new_measure = lambda: out
    f.local_dir = lambda: tmp_path
    f._clear_local_flags = f._start_tail = f._stop_tail = lambda *a: None
    f._status = lambda m: None
    f._measure_timeout = lambda d: 60.0

    def send(_d, m):
        f._waiter.result = dict(obj, rows=5, elapsed_s=2.0)
        return True
    f._send = send
    res = OL.OesLink._run_daemon(f, 2.0, 50)
    c.close()
    assert res.success and res.message == msg and res.rows == 5
    assert (res.out_csv == obj["nas_csv"]) if nas else (res.out_csv == str(out))
    assert out.exists() is (not nas)


# ═════════════ RGA(v1 tests/test_oes_rga_fixes.py:87, test_rga_worker_ascii_json.py:43) ═════════════
def test_t102_rga_step_timeout_follows_settings():
    """lib/config_common.py:100-125 rga_step_timeout_ms = n×t×1000 + (n−1)×d×1000 + 5000 — 기본 187000, 설정 (2, 30, 0.5) → 65500
    (실행기는 상수 대신 이 함수를 그때 부름)."""
    import inspect
    from v2.ch12 import process_runner
    from v2.ch12.drivers import rga_link as rl
    assert rl.step_timeout_ms({}) == 187000
    assert rl.step_timeout_ms({"RGA_MAX_ATTEMPTS": 2, "RGA_WORKER_TIMEOUT_S": 30.0, "RGA_RETRY_DELAY_S": 0.5}) == 65500
    assert "step_timeout_ms(self.cfg)" in inspect.getsource(process_runner) and "RGA_STEP_TIMEOUT_MS" not in inspect.getsource(process_runner)


def test_t145_rga_worker_json_is_ascii_and_korean_survives(capsys):
    """apps/rga_service/rga_api.py:94 — JSON 은 ASCII 만(한글은 \\uXXXX) → 메인(UTF-8)이 워커 인코딩과 무관하게 한글 경로·오류를 받음."""
    import json as _json
    from v2.ch12.workers import rga_worker as RW
    path = r"C:\VanaM_Logs\RGA\한글 폴더\CH1.csv"
    RW._emit({"kind": "result", "ok": False, "csv": path, "error": "RuntimeError: RGA 연결 실패: 응답 없음",
              "csv_error": "primary(권한 없음)/fallback(디스크 가득)", "pressures": [0.5, 0.6]})
    line = capsys.readouterr().out.strip()
    assert line.isascii() and "\\u" in line
    got = _json.loads(line.encode("cp949", errors="strict").decode("utf-8"))
    assert got["csv"] == path and got["error"] == "RuntimeError: RGA 연결 실패: 응답 없음"
    assert got["csv_error"] == "primary(권한 없음)/fallback(디스크 가득)" and got["pressures"] == [0.5, 0.6]


# ═════════════ TEST-086 코일 로그 요약 low_fail(v1 tests/test_leak_guards.py:373) ═════════════
def test_t086_coil_log_summary_low_fail_is_interval_counter():
    """device/plc.py:1932-2003 — 코일 스냅샷 실패 N 회 뒤 요약 'plc_error=N … low_fail=N', 요약 뒤 0. v2 는 1초 행의 plc.q=fail 이 그 실패."""
    from datetime import datetime, timedelta
    from v2.ch12.outputs.base import QueueWriter
    from v2.ch12.outputs.plc_coil_csv import PlcCoilCsv
    from v2.ch12.outputs.row_feed import plc_coil_args
    assert plc_coil_args({"plc.q": "fail"}, True) == {"connected": True, "coils": None, "plc_error": True}
    assert plc_coil_args({"plc.q": "late"}, True) == {"connected": True, "coils": None}
    logs = []
    w = QueueWriter("t")
    c = PlcCoilCsv({"PLC_COIL_LOG_INTERVAL_S": 1.0, "PLC_COIL_LOG_SUMMARY_S": 10.0}, writer=w, log=logs.append)
    t0 = datetime(2026, 10, 30, 9, 0, 0)
    try:
        for k in range(4):                                     # 0~3 s 실패 4회
            c.on_tick(t0 + timedelta(seconds=k), mono=float(k), **plc_coil_args({"plc.q": "fail"}, True))
        c.on_tick(t0 + timedelta(seconds=10), mono=10.0, connected=False)          # 요약 1
        c.on_tick(t0 + timedelta(seconds=20), mono=20.0, connected=False)          # 요약 2(리셋 뒤)
    finally:
        w.stop()
    summ = [m for m in logs if "PLC COIL LOG summary" in m]
    assert len(summ) == 2
    assert "plc_error=4" in summ[0] and summ[0].endswith("low_fail=4") and "disconnected=1" in summ[0]
    assert summ[1].endswith("low_fail=0") and "plc_error=0" in summ[1]


# ═════════════ 펄스 로그 게이트·시작 순서(v1 tests/test_log_gates.py:105-180) ═════════════
def test_t090_rf_pulse_raw_log_gate():
    """device/rf_pulse.py:1100-1107 — RFPULSE_RAW_LOG=false 면 '[POLL …]' 태그만 원시 프레임 문구 생략(그 밖·None·'' 은 남김), true 면 전부."""
    from v2.ch12.drivers import rfpulse as RP
    from v2.ch12.params import rfpulse as PR
    from v2.common.paramdef import defaults
    d = RP.RfPulseDriver(_Clk(), None, defaults(PR.PARAMS), True, on_event=lambda e: None, chamber="ch1")
    assert d._raw_on("[POLL FWD]") is False and d._raw_on("[POLL WAKE]") is False
    assert d._raw_on("[RF ON]") is True and d._raw_on(None) is True and d._raw_on("") is True
    d.cfg["RFPULSE_RAW_LOG"] = True
    assert d._raw_on("[POLL FWD]") is True


def test_t091_dc_pulse_start_order_counters():
    """device/dc_pulse.py:1039-1065·678 — 과전류 카운터 0 은 OUTPUT_ON 보내기 전(이전 런 값이 남아 있어도), 점화 창 시작 시각·PIV 정합성
    검사 플래그는 ON(HV 확인) 뒤; 활성화 감시가 센 과전류 수를 폴링이 0 으로 밀지 않음(STREAK 2 → 활성화 중 2회째에 차단)."""
    from v2.tests.test_dcpulse import Rig
    r = Rig(cfg={"DC_OVERCURRENT_ENABLE": True})
    r.d._overcurr_n = 5
    r.d._piv_scale_checked = True
    t0 = r.c.monotonic()
    assert r.start(100.0, freq=50, off_time_us=5.0) == (None, True)
    assert r.d._overcurr_n == 0 and r.d._arc_run_start_ts > t0
    on_sent = [ts for ts, f in list(r.sim.received) if f[1] == 0x80 and len(f) == 6 and f[3] == 1][0]
    assert r.d._arc_run_start_ts >= on_sent
    r.done()
    r = Rig(cfg={"DC_OVERCURRENT_ENABLE": True, "DC_OVERCURRENT_STREAK_N": 2})
    r.sim.i_override_a = 2.71
    reason, ok = r.start(190.0, freq=50, off_time_us=5.0)
    stop = [e for e in r.ev("command_failed") if e.cmd == "AUTO_STOP"]
    assert ok is False and stop and "2회 연속" in stop[0].detail
    assert any("(1/2)" in e.detail for e in r.ev("status") if e.detail.startswith("[WARN] 과전류 감지"))
    r.done()


# ═════════════ TEST-126 프리스퍼터 예약 루프(v1 tests/test_pre_sputter_loop_error.py:31) ═════════════
def test_t126_once_exception_ends_normally_and_cancel_ends_loop(tmp_path):
    """runtime/pre_sputter_runtime.py:256-312 — 1회 예약(반복 아님)은 회차 예외여도 '1회 예약 실행 완료(반복 없음)'·상태 '실행 오류'로
    끝나고 루프 종료, 취소는 실행 0·예외 로그 없이 루프 종료(매일 반복 쪽은 test_pre_sputter.py::test_round_exception_keeps_daily_reservation)."""
    from v2.ch12 import pre_sputter as PS
    from v2.tests.test_pre_sputter import FakeEng, _clock
    c = _clock()
    e = FakeEng(c)
    e.snapshot_recipe_queue = lambda: (_ for _ in ()).throw(RuntimeError("boom"))
    p = PS.PreSputter(c, e, settings_dir=tmp_path, hh=6, mm=0)
    p.start_daily()
    p._repeat = False                                   # v1 시험처럼 반복 끔(1회 예약)
    c.wait_until(c.monotonic() + 300, lambda: any("1회 예약 실행 완료(반복 없음)" in l for l in p.lines))
    assert any("실행 중 예외: RuntimeError('boom')" in l for l in p.lines) and p.status_text == "실행 오류"
    assert not any("다음 반복 예약" in l for l in p.lines)
    p.close()
    e2 = FakeEng(c)
    p2 = PS.PreSputter(c, e2, settings_dir=tmp_path / "b", hh=7, mm=0)
    p2.start_daily()
    c.wait_until(c.monotonic() + 5)
    p2.stop()
    c.wait_until(c.monotonic() + 7200)
    assert e2.calls == [] and not any("예외" in l for l in p2.lines) and not p2.is_reserved
    p2.close()
    c.close()


# ═════════════ TEST-128 히터 공정 흐름(v1 tests/test_process_heater_steps.py:212-330 — 엔진 실제 실행 + 히터 대역) ═════════════
def _heater_flow(tmp_path, heater, hooks=()):
    from v2.tests import proc_rig as R
    from v2.tests.test_process_list import H2
    rig = R.make(tmp_path, chambers=("ch2",), ch2={"heater": heater})
    e = rig.eng["ch2"]
    assert e.load([dict(H2)]) is None
    R.run(rig, R.idle(e), max_ticks=600, hooks=[R.at(2, lambda: e.start(source="local")), *hooks])
    return R, e


@pytest.mark.slow
def test_t128_heater_flow_order_timeout_hold_abort_fault_and_stop(tmp_path):
    """controller/process_controller.py:1018-1027 — 정상 heater_set → heater_wait → IG …, 전원 ON 뒤 유지 측정(hold), 끝에 정리;
    대기 시간 초과 E831 실패; 유지 중단 E833 은 실패(종료 절차 — DC Pulse OFF 수행); 대기 중 STOP 은 결과 STOP·대기 취소(heater_cancel);
    공정 중 히터 이상 E832 는 FAIL."""
    from v2.tests.test_process_list import FakeHeater

    h = FakeHeater()
    R, e = _heater_flow(tmp_path / "ok", h)
    names = [c[0] for c in h.calls]
    assert R.ends(e) == [("done", None)] and names.index("set") < names.index("wait") < names.index("hold")
    acts = [a for _t, _i, a, _m, sd in e.traces[-1] if not sd]
    assert acts[:3] == ["HEATER_SET", "HEATER_WAIT", "IG_CMD"]
    assert acts.index("HEATER_HOLD") > next(i for i, a in enumerate(acts) if a == "DC_PULSE_START")

    class Timeout(FakeHeater):
        def heater_wait(self, target, soak_s=None):
            self.calls.append(("wait", target, soak_s))
            self.on_process("failed", False, "히터 도달 시간 초과", "E831")
    h = Timeout()
    R, e = _heater_flow(tmp_path / "to", h)
    assert R.ends(e) == [("error", "E831")] and ("cancel",) in h.calls

    class HoldAbort(FakeHeater):
        def heater_hold(self):
            self.calls.append(("hold",))
            self.on_process("failed", False, "유지 측정 실패 — 포기", "E833")
    h = HoldAbort()
    R, e = _heater_flow(tmp_path / "hold", h)
    sd = [a for _t, _i, a, _m, s in e.traces[-1] if s]
    assert R.ends(e) == [("error", "E833")] and "DC_PULSE_STOP" in sd

    class Slow(FakeHeater):
        def heater_wait(self, target, soak_s=None):
            self.calls.append(("wait", target, soak_s))             # 끝나지 않는 대기
    from v2.tests import proc_rig as R2
    from v2.tests.test_process_list import H2
    h = Slow()
    rig = R2.make(tmp_path / "stop2", chambers=("ch2",), ch2={"heater": h})
    e = rig.eng["ch2"]
    e.load([dict(H2)])
    R2.run(rig, R2.idle(e), max_ticks=600, hooks=[R2.at(2, lambda: e.start(source="local")),
                                                  R2.when(lambda: any(c[0] == "wait" for c in h.calls), lambda: e.stop())])
    assert R2.ends(e) == [("aborted", "USER_STOP")] and ("cancel",) in h.calls

    class Fault(FakeHeater):
        def heater_hold(self):
            self.calls.append(("hold",))
            self._ok("hold_ok")
            self.on_process("failed", False, "공정 중 히터 이상", "E832")
    h = Fault()
    R, e = _heater_flow(tmp_path / "fault", h)
    assert R.ends(e) == [("error", "E832")]


# ═════════════ TEST-094 공정 로그 쓰기 실패 — 사유와 본문 보존(v1 tests/test_log_writer_reason.py:80) ═════════════
def test_t094_writer_failure_emergency_file_keeps_reason_and_text(tmp_path):
    """runtime/chamber_runtime.py:7428-7568 — 쓰기 실패면 로컬 폴더에 비상 파일 1개('CH{n}_…_writer_pending.txt'): 머리 '# [log-writer
    pending]'·'# source_path = …'·'# reason = RuntimeError('boom')' + 본문 그대로, 화면 '세션 로그 긴급 보관 파일 생성'. 사유는 값으로
    (예외 객체를 클로저로 잡지 않음 — 쓰기 스레드가 끝난 뒤에도 같은 문구)."""
    from v2.ch12.outputs.base import QueueWriter
    from v2.ch12.outputs.process_txt import ProcessTxtLog
    from v2.tests.outputs_golden import Clock, make_root, v2_cfg
    from v2.tools import v1golden_outputs_scen as SC
    root = make_root(tmp_path)
    ui = []
    w = QueueWriter("t")
    t = ProcessTxtLog(2, v2_cfg(root, SC.BASE_CFG), writer=w, now=Clock("2026-10-30T10:00:00").now, on_ui=ui.append)

    def boom(text):
        raise RuntimeError("boom")
    t._appender.write = boom
    p = t.open_run({"process_note": "x"})
    t.log("P", "row-A")
    w.flush()
    t._appender.close()
    files = list(t._local_dir.glob("CH2_*_writer_pending.txt"))
    assert len(files) == 1
    body = files[0].read_text(encoding="utf-8")
    assert body.startswith(f"# [log-writer pending]\n# source_path = {p}\n# reason = RuntimeError('boom')\n")
    assert "# ==== Sputter Run ====\n" in body and "[CH2:P] row-A\n" in body
    assert any("세션 로그 긴급 보관 파일 생성" in m for m in ui)
    w.stop()


# ═════════════ TEST-082 서버 페이지 일별 로그 버퍼(v1 tests/test_leak_guards.py:66) ═════════════
def test_t082_server_daily_log_buffer_cap_and_flag_reset(tmp_path, monkeypatch):
    """runtime/server_page.py:392-432 — 상한의 2배를 넣어도 버퍼는 상한 이하(넘침 경고 1회), flush 성공 뒤 경고 플래그 리셋. v2 로봇 서버는
    flush 스레드가 버퍼를 통째로 가져가 쓰므로 v1 의 'submit 실패 → inflight 복구'는 생기지 않는다."""
    from v2.robot_server.log import ServerLog
    monkeypatch.setattr(ServerLog, "HARD_MAX", 100)
    lg = ServerLog(tmp_path / "logs", tmp_path / "fb", now=lambda: __import__("datetime").datetime(2026, 10, 30, 9, 0, 0))
    for i in range(200):
        lg.log("CLIENT", f"line {i}")
    assert len(lg._buf) <= 101 and lg._overflow_logged is True
    lg.log("CLIENT", "x")
    lg._dir = lambda: (_ for _ in ()).throw(OSError("nas down"))         # 실패 경로 — 플래그 유지
    lg.flush()
    assert lg._overflow_logged is True
    del lg._dir
    lg.log("CLIENT", "y")
    lg.flush()
    assert lg._overflow_logged is False and lg._buf == []
    lg.close()
    assert lg.path_today().read_text(encoding="utf-8").endswith("[CLIENT] y\n")
