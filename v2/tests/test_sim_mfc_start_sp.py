"""sim 가상 MFC 시작 설정점(2026-11-18 D415, 2026-11-19 D421 — 기본 30) — 설정 SIM_MFC_START_SP_MTORR(기본 '0,0,30,30,0' mTorr).

- sim 이 켜질 때 가상 MFC(sim/mfc_sim)가 SP1~SP5 를 이 값으로 들고 있어, 압력 대기(CH1 SP3·CH2 SP4 setpoint 읽기)가
  'setpoint 읽기 실패'로 멈추지 않는다(이전에는 0 이라 sim 공정이 압력 대기에서 모두 멈췄다).
- 실장비 경로에는 영향이 없다: 이 설정은 sim 시뮬레이터 만들기(sim/rig.make_sims — app 은 RUN_MODE sim 일 때만 부른다)만 읽고,
  드라이버·엔진·실장비 연결(tcp_transports)은 읽지 않는다. 실장비는 패널에 저장된 SP 를 R1~R4 로 읽는다(값 미확인 — UNVERIFIED).
"""
import ast
from pathlib import Path

import pytest

from v2.common.clock import VirtualClock
from v2.common.paramdef import check_config, defaults
from v2.ch12.params import PARAMS, CROSS_RULES
from v2.ch12.sim import mfc_sim as MS
from v2.ch12.sim.rig import Ch12FullRig, make_sims, start_sp_of
from v2.ch12.spec import devices as DV

ROOT = Path(__file__).resolve().parents[2]
KEY = "SIM_MFC_START_SP_MTORR"


def test_default_is_sp3_sp4_30_mtorr_as_device_values():
    assert {p.key: p.default for p in PARAMS}[KEY] == "0,0,30,30,0"
    assert start_sp_of({}) == [0.0, 0.0, 30 * MS.PRESSURE_SCALE, 30 * MS.PRESSURE_SCALE, 0.0]
    assert start_sp_of({KEY: "1, 2,3 ,4,5"}) == [v * MS.PRESSURE_SCALE for v in (1, 2, 3, 4, 5)]
    assert MS.MfcSim().sp == [0.0] * 5                                   # 시뮬레이터 자체 기본은 그대로(단위 시험이 직접 만든 것)


def test_bad_format_is_rejected_by_config_check_not_fixed_up():
    vals = defaults(PARAMS)
    assert check_config(PARAMS, CROSS_RULES, vals) == []
    for bad in ("2,2", "a,b,c,d,e", "0,0,2,2,0,9", "-1,0,0,0,0"):
        assert check_config(PARAMS, CROSS_RULES, dict(vals, **{KEY: bad})), bad   # 고쳐 쓰지 않고 거부(D11)


def test_make_sims_applies_the_setting_to_both_chambers():
    _, sims = make_sims(0.0, dict(defaults(PARAMS), **{KEY: "0,0,3,4,0"}))
    for n in ("ch1", "ch2"):
        assert sims[f"{n}.mfc"].sp == [0.0, 0.0, pytest.approx(0.3), pytest.approx(0.4), 0.0]


def test_full_rig_reads_sp3_sp4_from_the_virtual_mfc(tmp_path):
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    rig = Ch12FullRig(c, Path(tmp_path), cfg={"CAMERA_MIN_FREE_GB": 0.0}, robot_api=False)
    try:
        assert rig.start()
        rig.loop.run(8)                                                   # 돌아가며 읽는 SP 값까지 한 바퀴
        snap = rig.hub.last_state["data"]["snapshot"]
        assert snap["ch1.mfc.sp3_mtorr"] == pytest.approx(30.0) and snap["ch2.mfc.sp4_mtorr"] == pytest.approx(30.0)
    finally:
        rig.stop()
        c.close()


def test_only_the_sim_reads_the_setting():
    """설정을 읽는 곳은 sim/rig.py 하나(params 정의 제외, 문서 spec/ 제외) — 드라이버·엔진·app 의 실장비 경로에는 없다."""
    def uses(p: Path) -> bool:                       # 코드에서 키 글자 그대로를 쓰는 곳(설명문·머리글 속 언급은 빼고)
        src = p.read_text(encoding="utf-8")
        return KEY in src and any(isinstance(n, ast.Constant) and n.value == KEY for n in ast.walk(ast.parse(src)))
    users = sorted(str(p.relative_to(ROOT)) for p in (ROOT / "v2").rglob("*.py") if "tests" not in p.parts and uses(p))
    assert users == ["v2/ch12/params/mfc.py", "v2/ch12/sim/rig.py"], users
    # app 은 RUN_MODE sim 일 때만 make_sims 를 부른다(실장비는 tcp_transports)
    src = (ROOT / "v2" / "ch12" / "app.py").read_text(encoding="utf-8")
    fn = next(n for n in ast.walk(ast.parse(src)) if isinstance(n, ast.FunctionDef) and n.name == "build")
    sim_if = next(n for n in ast.walk(fn) if isinstance(n, ast.If) and "RunMode.SIM" in ast.unparse(n.test))
    assert "make_sims" in ast.unparse(sim_if.body[-3]) or any("make_sims" in ast.unparse(x) for x in sim_if.body)
    assert not any("make_sims" in ast.unparse(x) for x in sim_if.orelse)


def test_unverified_lists_the_real_panel_setpoints():
    assert any(u.device == "mfc" and "SP3" in u.item and "SP4" in u.item for u in DV.UNVERIFIED)


def _row(**kw):
    from v2.ch12.recipe import TEMPLATE_COLUMNS
    r = {k: "" for k in TEMPLATE_COLUMNS}
    r.update(kw)
    return r


@pytest.mark.parametrize("ch,name,gas", [
    (1, "CH1 Ar 20", dict(Ar="T", Ar_flow="20")),
    (1, "CH1 Ar 20 + O2 1.5", dict(Ar="T", Ar_flow="20", O2="T", O2_flow="1.5")),
    (2, "CH2 Ar 20 + O2 10(합 30 sccm, O2 단계 9.0·9.5)", dict(Ar="T", Ar_flow="20", O2="T", O2_flow="10")),
])
def test_default_sp30_is_reached_then_sp2_boost_and_sp1_then_main(tmp_path, ch, name, gas):
    """기본 SP3·SP4 = 30 mTorr(D421)로 일반 레시피가 압력 제어(SP setpoint 30 기준 도달) → SP2 부스트(10 mTorr 도달) → SP1(작업 압력)을
    지나 메인 공정에 들어간다 — sim 압력 모델(유량 × 1.5e-5 Torr ÷ 스로틀 유효 개방 — D424)로 30 mTorr 가 잡힌다(Ar 20 → 스로틀 약 0.8 %)."""
    from v2.common.commands import Source
    from v2.ch12.recipe import TEMPLATE_COLUMNS  # noqa: F401
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    rig = Ch12FullRig(c, Path(tmp_path), cfg={"CAMERA_MIN_FREE_GB": 0.0}, robot_api=False)
    power = (dict(use_rf_pulse="T", rf_pulse_power="120", rf_pulse_freq="0.5", rf_pulse_duty_cycle="40") if ch == 1 else
             dict(use_dc_pulse="T", dc_pulse_power="60", dc_pulse_freq="50", dc_pulse_off_time="5", gun1="T"))
    row = _row(Process_name=name, base_pressure="7.00E-06", working_pressure="2", shutter_delay="1", process_time="0.2",
               main_shutter="T", **gas, **power)
    try:
        assert rig.start()
        rig.loop.run(2)
        assert rig.command("RECIPE_LOAD", {"ch": ch, "rows": [row], "name": "sp30"}, source=Source.HMI).ok
        assert rig.command("PROCESS_START", {"ch": ch}, source=Source.HMI).ok
        seen = {"pc": None, "boost": None, "sp1": None, "main": None}
        for _ in range(400):
            rig.loop.run(1)
            for e in list(rig.hub.recent):
                ctx = (e.get("data") or {}).get("context") or {}
                d = ctx.get("data") or {}
                if e["type"] == "event" and ctx.get("kind") == "block" and ctx.get("chamber") == f"ch{ch}" and d.get("phase") == "start":
                    key = {"pressure_ctrl": "pc", "pressure_boost": "boost", "sp1": "sp1", "main": "main"}.get(d.get("type"))
                    if key and seen[key] is None:
                        seen[key] = d.get("no")
            ends = [e for e in rig.hub.recent if e["type"] == "run_end" and e["data"].get("chamber") == f"ch{ch}"]
            if seen["main"] is not None or ends:
                break
        assert seen["main"] is not None, (seen, [e["data"] for e in rig.hub.recent if e["type"] == "run_end"])
        assert seen["pc"] is not None and seen["boost"] is not None and seen["pc"] < seen["boost"] < seen["main"], seen
        snap = rig.hub.last_state["data"]["snapshot"]
        assert snap[f"ch{ch}.mfc.sp{2 + ch}_mtorr"] == pytest.approx(30.0)
        logs = [str((e["data"] or {}).get("message") or "") for e in rig.hub.recent if e["type"] == "event"]
        assert not any("압력 안정화 실패" in m or "setpoint 읽기 실패" in m for m in logs)
    finally:
        rig.stop()
        c.close()
