"""sim 압력 모델 보정(2026-11-19 D424) — sim/chamber.py 상수만(TORR_PER_SCCM·THROTTLE_MIN), 실장비 경로는 그대로.

이전 상수(유량 1 sccm = 1e-4 Torr, 스로틀 최소 유효 개방 0.02)로는 다 열어도 압력 = 총 유량 × 0.1 mTorr 라, 30 sccm(Ar 20 + O2 10)이면
3 mTorr 아래로 못 내려가 작업 압력 2 mTorr 를 못 잡았다(메인 공정 내내 +1 mTorr — 편차 카드). 지금(1.5e-5·0.002):
- 총 유량 5~60 sccm 에서 SP3·SP4 30 mTorr(스로틀 거의 닫힘 — 5 sccm 이면 닫힘 끝 37.5 mTorr)와 작업 압력 1~5 mTorr(60 sccm 다 열면
  0.9 mTorr) 모두 정상 상태로 잡힌다(모델 단위 — 가상 MFC 밸브 제어 + 챔버).
- 압력 대기는 수십 초(1차 지연 τ 5 s·밸브 τ 2 s 그대로) — 기준 안(±0.3 mTorr)에 드는 시간.
- 시험대(실제 조립 — Ch12FullRig, 기본 설정 SP3·SP4 30): CH1 Ar 20·CH1 Ar 20 + O2 1.5·CH2 Ar 20 + O2 10 레시피가 압력 제어 30 → SP2 부스트
  10 → SP1 2 mTorr → 메인 공정, 메인 공정(셔터 열림) 압력이 2 ± 0.3 안, 공정 모니터 편차 카드 0.
- 실장비 경로 영향 없음: 상수는 sim 패키지(sim/chamber.py·sim/mfc_sim.py)만 읽는다.
"""
import ast
from pathlib import Path

import pytest

from v2.common.clock import VirtualClock
from v2.ch12.sim import chamber as CM
from v2.ch12.sim import mfc_sim as MS
from v2.ch12.sim.rig import Ch12FullRig

ROOT = Path(__file__).resolve().parents[2]
FLOWS = (5.0, 10.0, 20.0, 21.5, 30.0, 45.0, 60.0)


def _settle(flow_sccm: float, target_mtorr: float, start_mtorr: float, secs: float = 150.0):
    """가상 MFC(Ar 채널 flow_sccm, SP1 = target 밸브 제어) + 챔버를 start 압력에서 secs 초 돌림 → (끝 압력, 기준 ±0.3 안에 처음 든 시각)."""
    ch = CM.Chamber()
    ch.pressure = start_mtorr / 1000.0
    m = MS.MfcSim(chamber=ch, flow_tau_s=0.0)
    m.on[0], m.set_pct[0] = True, flow_sccm
    m.sp[0], m.sp_sel, m.valve_mode = target_mtorr * MS.PRESSURE_SCALE, 1, "control"
    t_in = None
    t = 0.0
    while t < secs:
        t += 0.5
        m._advance(t)
        if t_in is None and abs(ch.pressure_mtorr - target_mtorr) < 0.3:
            t_in = t
    return ch.pressure_mtorr, t_in


def test_constants_cover_5_to_60_sccm_for_30_and_1_to_5_mtorr():
    lo = max(FLOWS) * CM.TORR_PER_SCCM * 1000.0                   # 다 열었을 때 가장 낮은 압력(60 sccm)
    hi = min(FLOWS) * CM.TORR_PER_SCCM / CM.THROTTLE_MIN * 1000.0  # 다 닫았을 때 가장 높은 압력(5 sccm)
    assert lo < 1.0 and hi > 30.0, (lo, hi)
    assert CM.BASE_TORR == 5e-7 and CM.IGNITE_MIN_MTORR <= 1.0 and CM.IGNITE_MAX_MTORR >= 30.0


@pytest.mark.parametrize("flow", FLOWS)
@pytest.mark.parametrize("target", (30.0, 10.0, 5.0, 3.0, 2.0, 1.0))
def test_every_flow_reaches_every_target_and_wait_is_tens_of_seconds(flow, target):
    start = 0.3 if target >= 10 else 10.0                          # 30·10 은 가스 주입 뒤(밸브 열림)에서, 작업 압력은 부스트 10 에서
    p, t_in = _settle(flow, target, start)
    assert p == pytest.approx(target, abs=0.02), (flow, target, p)
    assert t_in is not None and t_in <= 60.0, (flow, target, t_in)     # 압력 대기 한도(MFC_PRESSURE_TIMEOUT_SEC 60) 안


def test_old_constants_could_not_reach_2_mtorr_at_30_sccm(monkeypatch):
    monkeypatch.setattr(CM, "TORR_PER_SCCM", 1e-4)
    monkeypatch.setattr(MS, "TORR_PER_SCCM", 1e-4)
    monkeypatch.setattr(CM, "THROTTLE_MIN", 0.02)
    monkeypatch.setattr(MS, "THROTTLE_MIN", 0.02)
    p, t_in = _settle(30.0, 2.0, 10.0)
    assert p == pytest.approx(3.0, abs=0.02) and t_in is None          # 이전 문제 재현: 다 열어도 3 mTorr


def test_only_the_sim_reads_the_model_constants():
    users = []
    for p in (ROOT / "v2").rglob("*.py"):
        if "tests" in p.parts:
            continue
        tree = ast.parse(p.read_text(encoding="utf-8"))
        names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)} | \
                {a.name for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) for a in n.names}
        if names & {"TORR_PER_SCCM", "THROTTLE_MIN"}:
            users.append(str(p.relative_to(ROOT)))
    assert sorted(users) == ["v2/ch12/sim/chamber.py", "v2/ch12/sim/mfc_sim.py"], users


def _row(**kw):
    from v2.ch12.recipe import TEMPLATE_COLUMNS
    r = {k: "" for k in TEMPLATE_COLUMNS}
    r.update(kw)
    return r


@pytest.mark.parametrize("ch,name,gas", [
    (1, "CH1 Ar 20", dict(Ar="T", Ar_flow="20")),
    (1, "CH1 Ar 20 + O2 1.5", dict(Ar="T", Ar_flow="20", O2="T", O2_flow="1.5")),
    (2, "CH2 Ar 20 + O2 10", dict(Ar="T", Ar_flow="20", O2="T", O2_flow="10")),
])
def test_recipe_30_boost_10_sp1_2_main_within_tolerance_no_monitor_card(tmp_path, ch, name, gas):
    from v2.common.commands import Source
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    rig = Ch12FullRig(c, Path(tmp_path), cfg={"CAMERA_MIN_FREE_GB": 0.0}, robot_api=False)
    power = (dict(use_rf_pulse="T", rf_pulse_power="120", rf_pulse_freq="0.5", rf_pulse_duty_cycle="40") if ch == 1 else
             dict(use_dc_pulse="T", dc_pulse_power="60", dc_pulse_freq="50", dc_pulse_off_time="5", gun1="T"))
    row = _row(Process_name=name, base_pressure="7.00E-06", working_pressure="2", shutter_delay="1", process_time="1",
               main_shutter="T", **gas, **power)
    blocks, press = {}, []
    eng = rig.eng[f"ch{ch}"] if hasattr(rig, "eng") else None

    def on(lg):
        ev = getattr(lg, "event", None)
        if lg.source == f"engine:ch{ch}" and lg.kind == "block" and ev.data.get("phase") in ("start", "end"):
            blocks.setdefault((ev.data.get("type"), ev.data["phase"]), c.monotonic())
        elif lg.source == f"ch{ch}.mfc" and lg.kind == "pressure" and getattr(ev, "value", None) is not None:
            press.append((c.monotonic(), float(ev.value)))
    rig.event_listeners.append(on)
    try:
        assert rig.start()
        rig.loop.run(2)
        assert rig.command("RECIPE_LOAD", {"ch": ch, "rows": [row], "name": "p2"}, source=Source.HMI).ok
        assert rig.command("PROCESS_START", {"ch": ch}, source=Source.HMI).ok
        for _ in range(1200):
            rig.loop.run(1)
            if any(e["type"] == "run_end" and e["data"].get("chamber") == f"ch{ch}" for e in rig.hub.recent):
                break
        ends = [e["data"] for e in rig.hub.recent if e["type"] == "run_end" and e["data"].get("chamber") == f"ch{ch}"]
        assert ends and ends[-1].get("result") in ("done", "success", None) and not ends[-1].get("reasonCode"), ends
        wait30 = blocks[("pressure_ctrl", "end")] - blocks[("pressure_ctrl", "start")]
        boost = blocks[("pressure_boost", "end")] - blocks[("pressure_boost", "start")]
        assert 5.0 <= wait30 <= 90.0 and 5.0 <= boost <= 90.0, (wait30, boost)          # 압력 대기 수십 초
        t0, t1 = blocks[("main", "start")], blocks[("main", "end")]
        main_p = [v for t, v in press if t0 <= t <= t1]
        assert len(main_p) >= 10 and max(abs(v - 2.0) for v in main_p) < 0.3, (min(main_p), max(main_p))
        cards = [lg for lg in rig.events if lg.kind == "monitor_card"]
        assert cards == [], [x.detail for x in cards]
    finally:
        rig.stop()
        c.close()
