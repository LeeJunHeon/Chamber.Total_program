"""히터 담당이 넘긴 조합·기록 항목(HEAT-083~085·091~096 — winshell 몫 HEAT-086 제외)과 DC2·SUPPORTS_* 조합.

- HEAT-083 v1 settings.json ch2 히터 운전값이 v2 설정(CH2_*)으로 그대로 옮겨지고 히터 설정으로 읽힌다.
- HEAT-084 히터 초기화 규칙(heater_setup.init_ch2_heater): 설정 읽기 실패·꺼짐·만들기 실패 문구.
- HEAT-085·DC2: CH2 DC2 는 SUPPORTS_DC2 이고 히터가 없을 때만(조립·스텝 caps·레시피 검사 'DC2 폐지').
- HEAT-091·092 Calib CSV·엑셀 히터 열 표본(메인 공정 폴링·셔터 열림)과 공정 끝 히터 요약 한 줄(공정 txt).
- HEAT-093·094 레시피 히터 열 정규화·값 검사. HEAT-095 스텝(골든은 test_process_steps_golden). HEAT-096 value 필수·E830.
"""
from __future__ import annotations

import csv
from pathlib import Path

import pytest

from v2.ch12 import process_steps as ps
from v2.ch12.heater_profile import load_heater_cfg
from v2.ch12.heater_setup import init_ch2_heater
from v2.ch12.recipe import normalize_row
from v2.ch12.recipe_check import check_norm_params
from v2.tests import proc_rig as R

ROOT = Path(__file__).resolve().parents[2]


# ───────────────────────── HEAT-083 ─────────────────────────

def test_v1_ch2_heater_values_convert_and_load():
    from v2.tools import convert_v1_config as C
    v1 = C.read_json_v1(ROOT / "config" / "settings.json")
    res = C.convert(ROOT / "config" / "settings.json", ROOT, allow_pending=True)
    heater = {k: v for k, v in v1["ch2"].items() if "HEATER" in k}
    assert len(heater) >= 40
    for k, v in heater.items():
        assert res.values[f"CH2_{k}"] == v, k
    hc, warns = load_heater_cfg(res.values, "CH2_")
    assert warns == []
    assert (hc.enabled, hc.temp_scale, hc.poll_interval_s, hc.mv_limit, hc.run_log_period_s) == (True, 0.1, 1.0, 1200, 10.0)
    off, _ = load_heater_cfg({}, "CH2_")                 # 코드 기본값으로도 읽힌다(설정 없음)
    assert isinstance(off.enabled, bool)


# ───────────────────────── HEAT-084 ─────────────────────────

class _Eng:
    def __init__(self):
        self.started = 0

    def start(self):
        self.started += 1


def test_init_ch2_heater_rules(monkeypatch):
    logs = []
    log = lambda s, m: logs.append((s, m))                 # noqa: E731
    assert init_ch2_heater({"CH2_SUPPORTS_HEATER": False}, lambda: 1 / 0, log) is None and logs == []
    eng = init_ch2_heater({"CH2_SUPPORTS_HEATER": True}, _Eng, log)
    assert isinstance(eng, _Eng) and eng.started == 1 and logs == []
    assert init_ch2_heater({"CH2_SUPPORTS_HEATER": True}, lambda: 1 / 0, log) is None
    assert logs == [("HEATER", "CH2 히터 초기화 실패: ZeroDivisionError('division by zero')")]
    import v2.ch12.heater_setup as HS
    monkeypatch.setattr(HS, "load_heater_cfg", lambda cfg, p: (_ for _ in ()).throw(ValueError("bad")))
    assert init_ch2_heater({"CH2_SUPPORTS_HEATER": True}, _Eng, log) is None
    assert logs[-1] == ("HEATER", "히터 설정 읽기 실패 — 히터 기능 끔: ValueError('bad')")


# ───────────────────────── HEAT-085 · DC2 · SUPPORTS_* ─────────────────────────

@pytest.mark.parametrize("dc2,heater,want", [(True, True, False), (True, False, True), (False, False, False),
                                             (False, True, False)])
def test_dc2_only_without_heater(tmp_path, dc2, heater, want):
    cfg = {"CH2_SUPPORTS_DC2": dc2, "CH2_SUPPORTS_HEATER": heater}
    caps = ps.caps_from_cfg(cfg, 2)
    assert caps.dc2 is want and caps.heater is heater
    assert ps.caps_from_cfg(cfg, 1).dc2 is False and ps.caps_from_cfg(cfg, 1).heater is False
    rig = R.make(tmp_path, cfg=cfg, chambers=())
    try:
        assert (rig.dc[2] is not None) is want
        p = dict(use_dc_power2=True, dc_power2=50, use_ar=True, ar_flow=20, process_time=1, working_pressure=3,
                 base_pressure=1e-5)
        errs = check_norm_params(p, 2, caps, cfg, heater_available=heater)
        assert ("DC2 Power 는 사용할 수 없습니다(DC2 폐지)" in errs) is (not want)
    finally:
        rig.clock.close()


# ───────────────────────── HEAT-093 · 094 ─────────────────────────

def test_recipe_heater_columns_and_checks():
    caps2 = ps.Caps(dc=True, dc2=False, rf=True, dcp=True, rfp=True, heater=True, thickness=False)
    log = []
    p = normalize_row({"use_heater": "F", "heater_temp": "200", "heater_ramp": "5"}, 2, caps2, log=log)
    assert p["use_heater"] is False and p["heater_temp"] is None and p["heater_ramp"] is None
    assert any("use_heater=F 이므로 히터를 쓰지 않습니다" in m for _s, m in log)
    p = normalize_row({"use_heater": "T", "heater_temp": "250", "heater_ramp": "", "heater_soak": "10"}, 2, caps2)
    assert (p["use_heater"], p["heater_temp"], p["heater_ramp"], p["heater_soak"]) == (True, 250.0, None, 10.0)
    base = dict(use_dc_power=True, dc_power=50, use_ar=True, ar_flow=20, process_time=1, working_pressure=3,
                base_pressure=1e-5, use_heater=True)
    cfg = {"CH2_SUPPORTS_HEATER": True}
    assert "CH1 에는 히터가 없습니다 (use_heater=T)" in check_norm_params(dict(base, heater_temp=100), 1, caps2, cfg)
    off = check_norm_params(dict(base, heater_temp=100), 2, caps2, cfg, heater_available=False)
    assert any(e.startswith("히터 기능이 꺼져 있어 use_heater=T 공정을 실행할 수 없습니다") for e in off)
    bad = check_norm_params(dict(base, heater_temp=0, heater_ramp=61, heater_soak=481), 2, caps2, cfg,
                            heater_available=True)
    assert any(e.startswith("heater_temp 는 0 초과") for e in bad)
    assert any(e.startswith("heater_ramp 는 0 초과 60°C/min 이하여야 합니다") for e in bad)
    assert any(e.startswith("heater_soak 는 0~480분") for e in bad)
    assert not [e for e in check_norm_params(dict(base, heater_temp=300, heater_ramp=60, heater_soak=480), 2, caps2,
                                             cfg, heater_available=True) if "heater" in e]


# ───────────────────────── HEAT-095 · 096 ─────────────────────────

def test_heater_steps_value_required_and_message():
    with pytest.raises(ValueError, match="HEATER_SET 액션은 value가 필요합니다"):
        ps.ProcessStep(action="HEATER_SET", message="x")
    with pytest.raises(ValueError, match="HEATER_WAIT 액션은 value가 필요합니다"):
        ps.ProcessStep(action="HEATER_WAIT", message="x")
    assert ps.ProcessStep(action="HEATER_HOLD", message="x").value is None


class RecHeater:
    """히터 엔진 대역 — 공정 API(결과 즉시) + 공정 진행 표시 기록. 메인 폴링 중이면 tick 마다 표본을 낸다(heater_link 처럼)."""

    def __init__(self):
        self.on_process = None
        self.on_sample = None
        self.owner = None
        self.stop_user = True
        self.flags = []
        self.calls = []

    def _ok(self, kind):
        self.on_process(kind, True, "", None)

    def heater_set(self, target, rate=None, context=None):
        self.owner = "process"
        self._ok("set_ok")

    def heater_wait(self, target, soak_s=None):
        self._ok("ready")

    def heater_hold(self):
        self._ok("hold_ok")

    def heater_cancel(self):
        pass

    def process_started(self, uses):
        pass

    def set_process_flags(self, running, polling=False, log_open=False):
        self.flags.append((running, polling, log_open))

    def after_row(self, ok, stopped, detail=None, queue_mode=False, next_uses_heater=False, on_done=None):
        self.owner = None
        if on_done:
            on_done()

    def queue_end(self):
        pass

    def tick(self, t):
        if self.flags and self.flags[-1][0] and self.flags[-1][1] and self.on_sample is not None:
            self.on_sample(None if len(self.calls) % 5 == 4 else 200.0 + len(self.calls) % 3, 250.0, 800, True)
            self.calls.append(t)


H2 = {"Process_name": "H", "Ar": "T", "Ar_flow": "20", "base_pressure": "1e-5", "working_pressure": "5",
      "shutter_delay": "0", "process_time": "0.1", "use_dc_pulse": "T", "dc_pulse_power": "80", "dc_pulse_freq": "20",
      "gun1": "T", "main_shutter": "T", "use_heater": "T", "heater_temp": "250"}


def test_heater_columns_and_summary_from_main_polling(tmp_path):
    h = RecHeater()
    rig = R.make(tmp_path, chambers=("ch2",), ch2={"heater": h})
    e = rig.eng["ch2"]
    assert h.on_sample is not None
    assert e.load([dict(H2)]) is None
    R.run(rig, R.idle(e), max_ticks=900, hooks=[R.at(2, lambda: e.start(source="local")), h.tick])
    assert R.ends(e) == [("done", None)], R.logs(e)[-20:]
    assert h.flags[0] == (True, False, True) and h.flags[-1] == (False, False, False)
    assert (True, True, True) in h.flags and h.calls
    pv = [200.0 + i % 3 for i in range(len(h.calls)) if i % 5 != 4]
    line = [m for m in R.logs(e) if "히터 요약(메인 공정 구간)" in m]
    want = (f"히터 요약(메인 공정 구간) — 목표 250.0°C · PV 평균 {sum(pv) / len(pv):.1f}°C (최소 {min(pv):.1f} · "
            f"최대 {max(pv):.1f}) · MV 평균 800 · 표본 {len(pv)}개")
    assert line and line[-1].endswith(want), line
    calib = list(tmp_path.rglob("Ch2_log.csv"))
    assert calib
    with open(calib[0], encoding="utf-8-sig", newline="") as f:
        row = list(csv.DictReader(f))[-1]
    assert row["Heater: SV"] == "250.0" and row["Heater: PV Avg"] == f"{sum(pv) / len(pv):.1f}"
    assert row["Heater: PV Min"] == f"{min(pv):.1f}" and row["Heater: PV Max"] == f"{max(pv):.1f}"
    assert row["Heater: MV Avg"] == "800"


def test_no_heater_no_summary(tmp_path):
    rig = R.make(tmp_path, chambers=("ch1",))
    e = rig.eng["ch1"]
    p = dict(base_pressure=1e-5, working_pressure=3, process_time=0.05, shutter_delay=0, use_ar=True, ar_flow=20,
             use_rf_pulse=True, rf_pulse_power=100, rf_pulse_freq=0.5, rf_pulse_duty=50, use_ms=True, process_note="x")
    R.run(rig, R.idle(e), max_ticks=600, hooks=[R.at(2, lambda: e.start(params=p, owner="local", source="local"))])
    assert R.ends(e) == [("done", None)]
    assert not R.has_log(e, "히터 요약")
