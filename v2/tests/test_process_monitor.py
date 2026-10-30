"""공정 모니터(v2/ch12/process_monitor.py) — v1 tests/test_process_monitor_events.py 의 기대값 그대로 + v2 연결(이벤트 카드).

v1 시험은 log_func 로 받은 문구를 셌다. 여기서는 같은 문구가 로그('Monitor')와 카드(monitor_card 이벤트) 둘 다로 나오는지 본다.
연결 시험: 가짜 조립(system.emit·event_listeners)과 가짜 엔진으로 MFC flow·pressure 이벤트(v1 폴링 주기 값)·셔터·run_start/end·
Arc·REFP 경로, 그리고 실제 Ch12Rig 공정 1회(셔터 열린 동안 Ar 유량을 틀어 첫 알림 카드·셔터 닫힘 요약).
"""
from __future__ import annotations

import math
from types import SimpleNamespace

import pytest

from v2.ch12 import process_monitor as PM
from v2.ch12.process_monitor import ProcessMonitor, arc_log_text, fmt_dur, parse_refp

PARAMS = {"use_ms": True, "use_ar": True, "ar_flow": 20.0, "use_o2": True, "o2_flow": 5.0, "working_pressure": 3.0}


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def _mon(repeat_s=300.0, recover_s=30.0, cfg=None):
    clk = Clock()
    msgs, cards = [], []
    m = ProcessMonitor(ch=2, cfg=cfg or {}, log_func=lambda src, msg: msgs.append((src, msg)),
                       on_card=lambda text, info: cards.append((text, info)),
                       repeat_s=repeat_s, recover_s=recover_s, now_func=clk)
    m.activate(PARAMS)
    m.notify_shutter_open()
    m.cards = cards
    return m, clk, msgs


def _feed(m, clk, gas, values, step=3.0):
    for v in values:
        m.check_flow(gas, v)
        clk.t += step


def _texts(msgs, m=None):
    assert all(src == "Monitor" for src, _ in msgs)
    t = [x for _, x in msgs]
    if m is not None:
        assert [c[0] for c in m.cards] == t           # 카드 = 로그 문구(같은 계기)
    return t


# ───────────────────────── v1 tests/test_process_monitor_events.py 그대로 ─────────────────────────

def test_first_alert_immediate_with_old_text_then_quiet():
    m, clk, msgs = _mon()
    _feed(m, clk, "Ar", [20.5] + [20.6] * 90)
    assert _texts(msgs, m) == ["⚠️ CH2 Ar flow 편차\n세팅: 20.00 sccm → 실측: 20.50 sccm (편차: +0.50)"]
    assert m.cards[0][1] == {"item": "Ar", "phase": "first"}


def test_pressure_first_alert_old_text():
    m, clk, msgs = _mon()
    m.check_pressure(3.4)
    m.check_pressure(3.5)
    assert _texts(msgs, m) == ["⚠️ CH2 Pressure 편차\n세팅: 3.000 mTorr → 실측: 3.400 mTorr (편차: +0.400)"]


def test_repeat_summary_every_interval():
    m, clk, msgs = _mon(repeat_s=300.0)
    _feed(m, clk, "Ar", [20.5] * 201)
    t = _texts(msgs, m)
    assert len(t) == 3
    assert t[1].startswith("⚠️ CH2 Ar flow 편차 계속 (벗어난 지 5분 0초)\n")
    assert t[2].startswith("⚠️ CH2 Ar flow 편차 계속 (벗어난 지 10분 0초)\n")
    assert "최대 편차: +0.50" in t[1] and "범위 밖 101회" in t[1]


def test_repeat_zero_disables_summary():
    m, clk, msgs = _mon(repeat_s=0)
    _feed(m, clk, "Ar", [20.5] * 400)
    assert len(msgs) == 1


def test_recovery_after_hold_time():
    m, clk, msgs = _mon(recover_s=30.0)
    _feed(m, clk, "Ar", [20.5, 20.7, 20.4])
    _feed(m, clk, "Ar", [20.1] * 10)
    assert len(msgs) == 1
    _feed(m, clk, "Ar", [20.1])
    t = _texts(msgs, m)
    assert len(t) == 2
    assert t[1] == ("✅ CH2 Ar flow 정상 복귀 (벗어나 있던 시간 9초)\n"
                    "세팅: 20.00 sccm → 실측: 20.10 sccm · 최대 편차: +0.70 · 범위 밖 3회")
    _feed(m, clk, "Ar", [20.1] * 20)
    _feed(m, clk, "Ar", [19.6])
    t = _texts(msgs, m)
    assert len(t) == 3 and t[2].startswith("⚠️ CH2 Ar flow 편차\n") and "(편차: -0.40)" in t[2]


def test_brief_return_is_same_excursion():
    m, clk, msgs = _mon(recover_s=30.0)
    _feed(m, clk, "Ar", [20.5])
    _feed(m, clk, "Ar", [20.0] * 5)
    _feed(m, clk, "Ar", [20.6])
    _feed(m, clk, "Ar", [20.0] * 11)
    t = _texts(msgs, m)
    assert len(t) == 2
    assert t[1].startswith("✅ CH2 Ar flow 정상 복귀 (벗어나 있던 시간 21초)\n")
    assert "최대 편차: +0.60 · 범위 밖 2회" in t[1]


def test_recover_zero_means_first_in_range_sample():
    m, clk, msgs = _mon(recover_s=0)
    _feed(m, clk, "Ar", [20.5, 20.0])
    t = _texts(msgs, m)
    assert len(t) == 2 and t[1].startswith("✅ CH2 Ar flow 정상 복귀 (벗어나 있던 시간 3초)\n")


def test_shutter_close_summary_once_then_deactivate_silent():
    m, clk, msgs = _mon()
    _feed(m, clk, "Ar", [20.5] * 40)
    m.check_pressure(3.0)
    m.notify_shutter_close()
    t = _texts(msgs, m)
    assert len(t) == 2
    assert t[1] == ("⚠️ CH2 Ar flow 편차 — 셔터 닫힐 때까지 계속 (벗어난 지 2분 0초)\n"
                    "세팅: 20.00 sccm → 마지막 실측: 20.50 sccm (편차: +0.50) · 최대 편차: +0.50 · 범위 밖 40회")
    assert m.cards[1][1]["phase"] == "flush"
    assert not m.is_active
    m.check_flow("Ar", 25.0)
    m.deactivate()
    assert len(msgs) == 2


def test_deactivate_summarizes_open_excursions():
    m, clk, msgs = _mon()
    _feed(m, clk, "O2", [5.5])
    m.check_pressure(2.5)
    clk.t += 60
    m.check_pressure(2.5)
    m.deactivate()
    t = _texts(msgs, m)
    assert len(t) == 4
    assert t[2].startswith("⚠️ CH2 O2 flow 편차 — 공정 끝날 때까지 계속 (벗어난 지 1분 3초)\n")
    assert t[3].startswith("⚠️ CH2 Pressure 편차 — 공정 끝날 때까지 계속 (벗어난 지 1분 0초)\n")
    assert "마지막 실측: 2.500 mTorr (편차: -0.500)" in t[3]


def test_close_while_returning_reports_recovery():
    m, clk, msgs = _mon(recover_s=30.0)
    _feed(m, clk, "Ar", [20.5, 20.5])
    _feed(m, clk, "Ar", [20.0] * 3)
    m.notify_shutter_close()
    t = _texts(msgs, m)
    assert len(t) == 2 and t[1].startswith("✅ CH2 Ar flow 정상 복귀 (벗어나 있던 시간 6초)\n")


def test_keys_are_independent():
    m, clk, msgs = _mon()
    for _ in range(2):
        m.check_flow("Ar", 20.5)
        m.check_flow("O2", 5.5)
        m.check_pressure(3.5)
        clk.t += 3
    assert [x.split("\n")[0] for x in _texts(msgs, m)] == [
        "⚠️ CH2 Ar flow 편차", "⚠️ CH2 O2 flow 편차", "⚠️ CH2 Pressure 편차"]


def test_inactive_and_unknown_gas_ignored():
    m, clk, msgs = _mon()
    m.check_flow("N2", 99.0)
    m.notify_shutter_close()
    m.check_flow("Ar", 99.0)
    m.activate({"use_ms": False})
    m.notify_shutter_open()
    m.check_pressure(99.0)
    assert msgs == [] and not m.is_active


def test_activate_resets_open_excursion():
    m, clk, msgs = _mon()
    m.check_flow("Ar", 20.5)
    m.activate(PARAMS)
    m.notify_shutter_open()
    m.check_flow("Ar", 20.5)
    assert len(msgs) == 2 and msgs[1][1].startswith("⚠️ CH2 Ar flow 편차\n")


def test_arc_and_refp_stay_immediate():
    m, clk, msgs = _mon()
    for _ in range(3):
        m.notify_arc_warning(3, 2, process_name="P")
        m.notify_refp_warning(25.0, 20.0, process_name="P")
    t = _texts(msgs, m)
    assert len(t) == 6
    assert t[0] == "⚠️ CH2 DC Pulse Arc 경고\n공정: P\nSoft Arc: 3회  Hard Arc: 2회  합계: 5회"
    assert t[1] == "⚠️ CH2 REFP 경고 (공정 계속 진행)\n공정: P\nREFP: 25.0W  ≥  기준 20.0W"
    m.notify_refp_warning(2.1, 5.0, recovered=True, process_name="P")
    assert msgs[-1][1] == "✅ CH2 REFP 정상 복귀\n공정: P\nREFP: 2.1W  <  기준 5.0W"


@pytest.mark.parametrize("bad", ["abc", -5, None, True, float("nan"), float("inf")])
def test_invalid_interval_falls_back_to_default(bad):
    m = ProcessMonitor(ch=1, repeat_s=bad, recover_s=bad)
    assert m._repeat_s() == 300.0 and m._recover_s() == 30.0
    m2 = ProcessMonitor(ch=1, cfg={"PROCESS_MONITOR_REPEAT_S": bad, "PROCESS_MONITOR_RECOVER_S": bad})
    assert m2._repeat_s() == 300.0 and m2._recover_s() == 30.0


def test_settings_read_at_runtime():
    """v1: 생성 뒤 settings 로드해도 반영(실행 시점 읽기) → v2: 같은 cfg 사전을 부를 때마다 읽는다(파라미터 apply=live)."""
    cfg = {}
    m = ProcessMonitor(ch=2, cfg=cfg)
    assert m._repeat_s() == 300.0 and m._recover_s() == 30.0 and m._flow_tol() == 0.3 and m._pressure_tol() == 0.3
    cfg.update(PROCESS_MONITOR_REPEAT_S=120, PROCESS_MONITOR_RECOVER_S=15,
               PROCESS_MONITOR_TOLERANCE_FLOW_SCCM=1.0, PROCESS_MONITOR_TOLERANCE_PRESSURE_MTORR=0.05)
    assert m._repeat_s() == 120.0 and m._recover_s() == 15.0 and m._flow_tol() == 1.0 and m._pressure_tol() == 0.05


def test_param_defaults_match_v1():
    from v2.ch12.params import core
    d = {p.key: p.default for p in core.PARAMS}
    assert d["PROCESS_MONITOR_REPEAT_S"] == 300.0 and d["PROCESS_MONITOR_RECOVER_S"] == 30.0
    assert d["PROCESS_MONITOR_TOLERANCE_FLOW_SCCM"] == 0.3 and d["PROCESS_MONITOR_TOLERANCE_PRESSURE_MTORR"] == 0.3


def test_tolerance_from_cfg_boundary_is_out():
    m, clk, msgs = _mon(cfg={"PROCESS_MONITOR_TOLERANCE_FLOW_SCCM": 1.0})
    m.check_flow("Ar", 20.9)                         # 0.9 < 1.0 → 안
    assert msgs == []
    m.check_flow("Ar", 21.0)                         # |편차| ≥ 허용 → 밖(v1 >=)
    assert len(msgs) == 1


def test_dev_mode_logs_only():
    m, clk, msgs = _mon(cfg={"DEV_MODE": True})
    m.check_flow("Ar", 20.5)
    assert len(msgs) == 1 and m.cards == []


def test_activate_targets_rules():
    m = ProcessMonitor(ch=1)
    m.activate({"use_ms": True, "use_ar": True, "ar_flow": 0, "use_o2": False, "o2_flow": 5, "use_n2": True,
                "n2_flow": 2.5, "working_pressure": 0})
    assert m.targets == {"N2": 2.5}
    m.notify_shutter_open()
    assert m.is_active


def test_helpers():
    assert [fmt_dur(x) for x in (0, 59.4, 60, 3599, 3600, 7322)] == ["0초", "59초", "1분 0초", "59분 59초", "1시간 0분",
                                                                     "2시간 2분"]
    assert parse_refp("⚠ REFP_WARN: REFP=5.3W ≥ 5.0W (공정은 계속 진행)") == (5.3, 5.0, False)
    assert parse_refp("REFP 정상 복귀: REFP=2.1W < 5.0W") == (2.1, 5.0, True)
    assert parse_refp("RF ON") is None
    d = {"elapsed_s": 75.0, "by_rate": True, "by_depo": True, "ign_total": 12, "ign_limit": 3000, "depo_total": 600,
         "depo_limit": 500, "rate": 3.5, "calc_rate": None, "rate_limit": 20.0, "rate_n": 2, "raw_soft": 9, "raw_hard": 1}
    assert arc_log_text(2, d, "P1", 400, 200) == (
        "⚠️ CH2 DC Pulse Arc 경고 (아크율, 증착 구간 과다)\n공정: P1\n출력 ON 후 1분15초 — 런 누적 Soft 400 / Hard 200 (합 600)\n"
        "점화 12 (기준 3000) / 증착 600 (기준 500)\n최근 아크율 장비 3.5/s, 계산 -/s (기준 20.0/s x2)\n장비 원시값 SAT=9 ANT=1")
    assert arc_log_text(1, {"elapsed_s": 5}, "P", 0, 0).startswith("⚠️ CH1 DC Pulse Arc 경고 (-)\n공정: P\n출력 ON 후 5초")


# ───────────────────────── 연결(가짜 조립·엔진) ─────────────────────────

class FakeSys:
    def __init__(self, cfg=None):
        self.cfg = cfg or {}
        self.t = 0.0
        self.clock = SimpleNamespace(monotonic=lambda: self.t)
        self.event_listeners = []
        self.emitted = []

    def emit(self, source, kind, detail="", level="info", code=None, reason=None, event=None):
        self.emitted.append(SimpleNamespace(source=source, kind=kind, detail=detail, level=level, event=event))

    def send(self, source, kind, ev, detail=""):
        lg = SimpleNamespace(source=source, kind=kind, event=ev, detail=detail)
        for f in list(self.event_listeners):
            f(lg)


class FakeEngine:
    def __init__(self, ch="ch1"):
        self.ch, self.n = ch, int(ch[-1])
        self.lines = []
        self.actions = SimpleNamespace(mfc_polling=False)
        self.params = {}
        self.run_id = None
        self.shutter_listeners = []

    def log(self, src, msg):
        self.lines.append((src, msg))


def _rig(cfg=None, ch="ch1"):
    s, e = FakeSys(cfg), FakeEngine(ch)
    mon = PM.attach(s, e)
    return s, e, mon


def _start(s, e, params, run_id="r1"):
    e.run_id, e.params = run_id, dict(params, process_name="P-1")
    s.send(f"engine:{e.ch}", "run_start", SimpleNamespace(data={"run_id": run_id, "params": e.params}))


def _cards(s):
    return [x for x in s.emitted if x.kind == "monitor_card"]


def test_attach_flow_pressure_events_only_while_polling_and_shutter_open():
    s, e, mon = _rig()
    _start(s, e, {"use_ms": True, "use_ar": True, "ar_flow": 20.0, "working_pressure": 3.0})
    flow = lambda v: s.send("ch1.mfc", "flow", SimpleNamespace(gas="Ar", value=v))       # noqa: E731
    flow(25.0)                                                    # 셔터 전 → 무시
    for f in e.shutter_listeners:
        f(True)
    flow(25.0)                                                    # 폴링 구간 아님 → 무시(v1 _last_polling_targets['mfc'])
    assert _cards(s) == []
    e.actions.mfc_polling = True
    s.send("ch2.mfc", "flow", SimpleNamespace(gas="Ar", value=25.0))   # 다른 챔버 → 무시
    flow(20.5)
    s.send("ch1.mfc", "pressure", SimpleNamespace(value=None, text=""))
    s.send("ch1.mfc", "pressure", SimpleNamespace(value=3.5, text="3.500"))
    c = _cards(s)
    assert [x.detail.split("\n")[0] for x in c] == ["⚠️ CH1 Ar flow 편차", "⚠️ CH1 Pressure 편차"]
    assert c[0].source == "process:ch1" and c[0].level == "warn"
    assert c[0].event == {"chamber": "ch1", "card": "MONITOR", "text": c[0].detail, "item": "Ar", "phase": "first",
                          "run_id": "r1"}
    assert ("Monitor", c[0].detail) in e.lines
    for f in e.shutter_listeners:
        f(False)                                                  # 셔터 닫힘 → 요약 2건
    assert [x.event["phase"] for x in _cards(s)][2:] == ["flush", "flush"]
    assert "셔터 닫힐 때까지 계속" in _cards(s)[2].detail
    s.send("engine:ch1", "run_end", SimpleNamespace(data={}))
    assert len(_cards(s)) == 4 and not mon.mon.is_active


def test_attach_run_end_flushes_open_excursion():
    s, e, mon = _rig()
    _start(s, e, {"use_ms": True, "use_o2": True, "o2_flow": 5.0})
    e.actions.mfc_polling = True
    for f in e.shutter_listeners:
        f(True)
    s.send("ch1.mfc", "flow", SimpleNamespace(gas="O2", value=6.0))
    s.t += 63
    s.send("engine:ch1", "run_end", SimpleNamespace(data={}))
    c = _cards(s)
    assert len(c) == 2 and c[1].detail.startswith("⚠️ CH1 O2 flow 편차 — 공정 끝날 때까지 계속 (벗어난 지 1분 3초)\n")


def test_attach_arc_once_per_run_and_refp_each_time():
    s, e, mon = _rig(ch="ch2")
    _start(s, e, {"use_ms": False})
    arc = SimpleNamespace(data={"run_soft": 7, "run_hard": 3})
    s.send("ch2.dcpulse", "arc_threshold_reached", arc)
    s.send("ch2.dcpulse", "arc_threshold_reached", arc)            # 같은 공정 → 1회만
    s.send("ch2.rfpulse", "status", SimpleNamespace(detail="⚠ REFP_WARN: REFP=5.3W ≥ 5.0W (공정은 계속 진행)"))
    s.send("ch2.rfpulse", "status", SimpleNamespace(detail="RF Pulse 연결됨"))
    s.send("ch2.rfpulse", "status", SimpleNamespace(detail="REFP 정상 복귀: REFP=2.1W < 5.0W"))
    _start(s, e, {"use_ms": False}, run_id="r2")
    s.send("ch2.dcpulse", "arc_threshold_reached", arc)            # 새 공정 → 다시
    t = [x.detail for x in _cards(s)]
    assert t == ["⚠️ CH2 DC Pulse Arc 경고\n공정: P-1\nSoft Arc: 7회  Hard Arc: 3회  합계: 10회",
                 "⚠️ CH2 REFP 경고 (공정 계속 진행)\n공정: P-1\nREFP: 5.3W  ≥  기준 5.0W",
                 "✅ CH2 REFP 정상 복귀\n공정: P-1\nREFP: 2.1W  <  기준 5.0W",
                 "⚠️ CH2 DC Pulse Arc 경고\n공정: P-1\nSoft Arc: 7회  Hard Arc: 3회  합계: 10회"]
    assert [x.level for x in _cards(s)] == ["warn", "warn", "info", "warn"]


def test_attach_detach_and_handler_errors_do_not_raise():
    s, e, mon = _rig()
    s.send("ch1.mfc", "flow", SimpleNamespace(gas="Ar", value="x"))    # 처리 예외 → 로그만
    e.actions.mfc_polling = True
    _start(s, e, {"use_ms": True, "use_ar": True, "ar_flow": 20.0})
    for f in e.shutter_listeners:
        f(True)
    s.send("ch1.mfc", "flow", SimpleNamespace(gas="Ar", value="bad"))
    assert any("공정 모니터 처리 예외" in m for _s, m in e.lines)
    mon.detach()
    assert s.event_listeners == [] and e.shutter_listeners == []
    assert e.monitor is mon


# ───────────────────────── 실제 공정(Ch12Rig — 가상 시계) ─────────────────────────

def test_rig_run_flow_deviation_card_and_shutter_summary(tmp_path):
    from v2.tests import proc_rig as R
    rig = R.make(tmp_path, sp=(3.0, 5.0), chambers=("ch1",))
    e = rig.eng["ch1"]
    mon = PM.attach(rig, e)
    p = dict(base_pressure=1e-5, working_pressure=3, process_time=0.2, shutter_delay=0.1, use_ar=True, ar_flow=20,
             use_rf_pulse=True, rf_pulse_power=120, rf_pulse_freq=0.5, rf_pulse_duty=40, use_ms=True,
             process_note="mon")
    sim = rig.sims["ch1.mfc"]
    bumped = []

    def bump(t):
        if mon.mon.is_active and e.actions.mfc_polling and not bumped:
            bumped.append(t)
            sim.set_pct[0] = sim.set_pct[0] * 1.2                   # Ar 실제 유량을 틀어 놓는다(셔터 열린 메인 구간)

    R.run(rig, R.idle(e), max_ticks=1500, hooks=[R.at(2, lambda: e.start(params=p, owner="local", source="local")), bump])
    assert R.ends(e) == [("done", None)], R.logs(e)[-20:]
    assert bumped
    cards = [lg for lg in rig.events if lg.kind == "monitor_card"]
    first = [lg for lg in cards if lg.event["phase"] == "first"]
    assert first and first[0].detail.startswith("⚠️ CH1 Ar flow 편차\n세팅: 20.00 sccm → 실측: ")
    assert first[0].source == "process:ch1" and first[0].event["run_id"] == e.run_id
    assert any(lg.event["phase"] in ("flush", "recovered") for lg in cards)
    assert any("셔터 닫힐 때까지 계속" in lg.detail for lg in cards)
    assert R.has_log(e, "Ar flow 편차")
    assert not mon.mon.is_active and math.isclose(mon.mon.targets.get("pressure", 0) or 0, 0)
