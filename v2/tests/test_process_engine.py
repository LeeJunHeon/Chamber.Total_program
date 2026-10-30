"""공정 엔진 실제 실행(가상 시계 + Ch12Rig — 시뮬레이터·실제 드라이버) — 골든 시나리오 6개 + 열 조합 일부.

확인: 실행한 스텝 순서 = blocks.expand(같은 params 의 기본 템플릿) 결과(골든 8개 필드 그대로 — v1 골든은
test_process_steps_golden 이 이미 expand 와 비교한다), 시뮬레이터가 받은 MFC 명령·PLC 코일 쓰기가 스텝과 같은 순서,
결과(run_end done), v1 로그 문구(공정 시작·[공정 i/N]·PLC 요청/완료·ETA 확정·폴링), 기록 출력(공정 txt·Calib CSV).
시간은 골든보다 짧게(Shutter Delay·메인 수 초) — 순서·조건은 같은 템플릿 함수.
"""
import csv

import pytest

from v2.ch12.blocks import expand, template_blocks
from v2.ch12.process_steps import caps_from_cfg, to_golden
from v2.tests import proc_rig as R

SCEN = {
    "ch1_dcp_change_sd": (1, dict(base_pressure=1e-5, working_pressure=12, process_time=0.15, shutter_delay=0.2, use_ar=True,
                                  ar_flow=30, use_dc_pulse=True, dc_pulse_power=60, dc_pulse_freq=50, dc_pulse_off_time="DC",
                                  use_ms=True, power_change_time="6s", change_power_value="90"), (12.0, 5.0)),
    "ch1_rfp": (1, dict(base_pressure=1e-5, working_pressure=3, process_time=0.1, shutter_delay=0.1, use_ar=True, use_o2=True,
                        ar_flow=20, o2_flow=1, use_rf_pulse=True, rf_pulse_power=120, rf_pulse_freq=0.5, rf_pulse_duty=40,
                        use_ms=True), (3.0, 5.0)),
    "ch2_dc_rf_boost": (2, dict(base_pressure=7e-6, working_pressure=2.0, process_time=0.1, shutter_delay=0.1, use_ar=True,
                                ar_flow=20, use_dc_power=True, dc_power=100, use_rf_power=True, rf_power=80, use_ms=True,
                                use_g1=True, use_g2=True), (3.0, 2.0)),
    "ch2_dcp_change": (2, dict(base_pressure=7e-6, working_pressure=60, process_time=0.3, shutter_delay=0.1, use_ar=True,
                               ar_flow=50, use_dc_pulse=True, dc_pulse_power=80, dc_pulse_freq=20, dc_pulse_off_time=10.0,
                               use_ms=True, use_g1=True, power_change_time="10s", change_power_value="100"), (3.0, 60.0)),
    "ch2_rfp_ps": (2, dict(base_pressure=1e-5, working_pressure=5, process_time=0.1, shutter_delay=0, use_ar=True, use_o2=True,
                           ar_flow=20, o2_flow=2, use_rf_pulse=True, rf_pulse_power=100, rf_pulse_freq=1.0, rf_pulse_duty=50,
                           use_power_select=True, use_g3=True), (3.0, 5.0)),
    "ch2_test": (2, dict(test_mode=True, test_duration_sec=30), (3.0, 5.0)),
}


def _run_single(tmp_path, name):
    n, p, sp = SCEN[name]
    ch = f"ch{n}"
    rig = R.make(tmp_path, sp=sp, chambers=(ch,))
    e = rig.eng[ch]
    p = dict(p, process_note=name)
    want, _ = template_blocks(dict(p), n, rig.cfg, caps=caps_from_cfg(rig.cfg, n))
    steps, _ = expand(want, rig.cfg, n, caps=caps_from_cfg(rig.cfg, n))
    R.run(rig, R.idle(e), max_ticks=1500, hooks=[R.at(2, lambda: e.start(params=p, owner="local", source="local"))])
    return rig, e, steps


@pytest.mark.parametrize("name", list(SCEN))
def test_golden_scenario_runs_like_expand(tmp_path, name):
    rig, e, steps = _run_single(tmp_path, name)
    assert R.ends(e) == [("done", None)], R.logs(e)[-30:]
    tr = e.traces[-1]
    ran = [(i, a, m) for _t, i, a, m, sd in tr if not sd]
    assert [x[1:] for x in ran] == [(s.action, s.message) for s in steps]
    assert [x[0] for x in ran] == list(range(len(steps)))
    assert to_golden(e.runner.steps) == to_golden(steps)                       # 8개 필드(값·params·병렬·폴링·no_wait)
    if name == "ch2_test":
        assert R.has_log(e, "[TEST MODE] 'ch2_test' 장비 제어 스킵 / 30.0s 시뮬레이션")
        assert tr[-1][0] - tr[0][0] < 1.0 and not rig.sims["ch2.mfc"].set_commands_seen()
        return
    n = e.n
    assert R.prefix_subsequence(R.mfc_prefixes(steps), rig.sims[f"ch{n}.mfc"].set_commands_seen())
    assert R.subsequence(R.plc_expected(steps, n), R.plc_writes(rig.sims["plc"]))
    assert R.has_log(e, f"=== '{name}' 공정 시작 (총 {len(steps)}단계) ===")
    assert R.has_log(e, f"[공정 1/{len(steps)}] 베이스 압력(")
    assert R.has_log(e, f"[CH{n}] 요청: MV -> True (raw='MV', ch={n})") and R.has_log(e, f"[CH{n}] 완료: MV -> True (")
    assert R.has_log(e, "[ETA] 종료 예정 확정:") and R.has_log(e, "폴링 시작") and R.has_log(e, "폴링 중지")
    assert R.has_log(e, "=== 공정이 성공적으로 완료되었습니다 ===")
    assert R.has_log(e, "Sputter Calib CSV 기록 요청 (ok=True, result=성공)")
    # 기록 출력 — 공정 txt(머리 줄·END)와 Calib CSV 1행(Result SUCCESS)
    e.writer.flush(10)
    txt = sorted((tmp_path / "logs" / f"CH{n}").glob(f"CH{n}_{name}_*.txt"))
    body = txt[-1].read_text(encoding="utf-8")
    assert body.startswith("# ==== Sputter Run ====\n") and body.rstrip().endswith("# ==== END ====")
    assert "[CH%d:Process] [공정 2/%d] RGA 스캔 및 그래프 출력 대기" % (n, len(steps)) in body
    e.calib.sender.flush(10) if hasattr(e.calib.sender, "flush") else None
    loc = e.calib.local_file
    with open(loc, encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    assert rows and rows[-1]["Result"] == "SUCCESS"


def test_parallel_block_dc_and_rf_sent_together(tmp_path):
    """병렬 블록: DC_POWER_SET 과 RF_POWER_SET 사이에 SW_POWER_SELECT OFF 가 끼어 블록이 나뉜다(골든 ch2_dc_rf_boost)."""
    rig, e, steps = _run_single(tmp_path, "ch2_dc_rf_boost")
    par = [s.action for s in steps if s.parallel]
    assert par == ["DC_POWER_SET", "RF_POWER_SET"]
    t = {a: tt for tt, _i, a, _m, sd in e.traces[-1] if not sd}
    assert t["RF_POWER_SET"] >= t["DC_POWER_SET"]


def test_snapshot_fields_columns_and_targets(tmp_path):
    from v2.ch12 import snapshot_fields as SF
    n, p, sp = SCEN["ch1_rfp"]
    rig = R.make(tmp_path, sp=sp, chambers=("ch1",))
    e = rig.eng["ch1"]
    rows = []
    rig.row_listeners.append(rows.append)
    R.run(rig, R.idle(e), hooks=[R.at(2, lambda: e.start(params=dict(p, process_note="snap"), owner="web"))])
    assert not rig.unknown_fields and rig.internal_errors == 0
    run = [r for r in rows if r.get("ch1.proc.run_id")]
    assert run and {r["ch1.proc.owner"] for r in run} == {"web"}
    assert {r["ch1.proc.block_type"] for r in run} >= {"base_wait", "gas_intro", "power_on", "main", "end_stop"}
    assert all(r["ch1.proc.block_count"] == len(e.seq.blocks) if e.seq else True for r in run)
    main = [r for r in run if r["ch1.proc.block_type"] == "main" and r["ch1.proc.remain_s"] is not None]
    assert main and all(0 <= r["ch1.proc.remain_s"] <= 6.0 for r in main)
    assert {r["ch1.set.rfpulse_w"] for r in run} == {120.0} and {r["ch1.set.ar_sccm"] for r in run} == {20.0}
    assert all(r["ch1.proc.min_total_s"] is None for r in run)                  # 단일 시작 — 큐 없음
    eta = [r["ch1.proc.eta_remaining_s"] for r in run]
    first = next(i for i, v in enumerate(eta) if v is not None)
    assert all(v is None for v in eta[:first]) and eta[first] > 100            # 확정 전 null, Shutter Delay 진입 때 확정
    assert rows[-1]["ch1.proc.state"] == "IDLE" and rows[-1].get("ch1.proc.run_id") is None
    assert set(k for r in rows for k in r if k.startswith("ch1.proc.") or k.startswith("ch1.set.")) <= SF.ENGINE_COLUMNS


def test_pulse_port_same_chamber_other_kind_rejected(tmp_path):
    """같은 챔버라도 종류가 다르면 펄스 주소 점유 거부(v1 문구) → run_end error PULSE_PORT_BUSY."""
    n, p, sp = SCEN["ch1_rfp"]
    rig = R.make(tmp_path, sp=sp, chambers=("ch1",))
    e = rig.eng["ch1"]
    ok, _m, _o = rig.shared.pulse.claim("dcpulse", "ch1")
    assert ok
    R.run(rig, R.idle(e), max_ticks=60, hooks=[R.at(2, lambda: e.start(params=dict(p), owner="local"))])
    assert R.ends(e) == [("error", "PULSE_PORT_BUSY")]
    assert R.has_log(e, "RF-Pulse 시작 불가: 동일 주소(192.168.1.50:4007) 장비를 CH1(DC-Pulse) 공정이 사용 중입니다.")
    assert any(x.kind == "popup" and x.data.get("title") == "공유 장비 사용 중" for x in e.events)


def test_gate_open_blocks_start(tmp_path):
    n, p, sp = SCEN["ch1_rfp"]
    rig = R.make(tmp_path, sp=sp, chambers=("ch1",))
    e = rig.eng["ch1"]
    rig.sims["plc"].gate[1] = 1.0                                               # 시뮬레이터 게이트 열림
    R.run(rig, R.idle(e), max_ticks=60, hooks=[R.at(2, lambda: e.start(params=dict(p), owner="local"))])
    assert R.ends(e) == [("error", "GATE_CHECK_FAILED")]
    assert R.has_log(e, "[CH1] Gate 상태=OPEN (open=True, close=False)")
    assert R.has_log(e, "[CH1] Gate 체크 실패 (Gate OPEN 상태) → 공정 시작 차단")
    assert any(x.kind == "chat" and x.code == "E301" for x in e.events)
    assert rig.shared.pulse.owner("rfpulse", "ch1") is None                    # 점유 해제
    assert not e.busy and e.error == "GATE_CHECK_FAILED"


def test_thickness_mode_run_ends_on_target(tmp_path):
    """열 조합: Thickness 모드(thickness·dep_rate, 두께 모니터 CH2) — 영점 → Main Shutter → THICKNESS_WAIT 목표 도달로 끝.
    ETA 는 확정하지 않는다(remaining_s=null)."""
    from v2.ch12.process_engine import ProcessEngine
    from v2.ch12.thickness import ThicknessEngine
    rig = R.make(tmp_path, chambers=())
    th = ThicknessEngine(rig.clock, rig.cfg, "ch2", driver=rig.stm["ch2"], settings_dir=tmp_path / "set")
    e = ProcessEngine(rig, "ch2", thickness=th, excel_main_dir=tmp_path / "xl")
    rig.register_engine("proc.ch2", e)
    p = dict(base_pressure=1e-5, working_pressure=5, process_time=0, shutter_delay=0.05, thickness=3.0, dep_rate=0.05,
             use_ar=True, ar_flow=20, use_dc_pulse=True, dc_pulse_power=80, dc_pulse_freq=20, use_ms=True, use_g1=True,
             tooling=100, density=2.0, z_factor=1.0, process_note="thk")
    etas = []
    R.run(rig, R.idle(e), max_ticks=900, hooks=[R.at(2, lambda: e.start(params=p)),
                                                lambda t: etas.append(e.eta_remaining_s())])
    assert R.ends(e) == [("done", None)]
    acts = [a for _t, _i, a, _m, sd in e.traces[-1] if not sd]
    assert acts.index("THICKNESS_ZERO") < acts.index("PLC_CMD", acts.index("THICKNESS_ZERO")) < acts.index("THICKNESS_WAIT")
    assert R.has_log(e, "완료 — 종료 방식 Thickness") and R.has_log(e, "[ETA] Thickness 모드")
    assert all(v is None for v in etas) and not R.has_log(e, "[ETA] 종료 예정 확정")
    assert e.stats.thickness_record                                              # 두께 기록 열(Calib CSV)


def test_single_start_invalid_inputs_rejected_with_popup(tmp_path):
    """단일(화면) 시작 값 검사 실패 — 공정 txt 는 먼저 열리고(v1 LOG-017) '입력값 확인' 경고, 장비 명령 없음."""
    rig = R.make(tmp_path, chambers=("ch1",))
    e = rig.eng["ch1"]
    host = []
    e.on_host_start = lambda ok, msg: host.append((ok, msg))
    bad = dict(base_pressure=1e-5, working_pressure=3, process_time=0.1, use_rf_pulse=True, rf_pulse_power=120,
               process_note="bad")                                              # 가스 없음
    R.run(rig, R.idle(e), max_ticks=30, hooks=[R.at(2, lambda: e.start(params=bad))])
    assert R.ends(e) == [("error", "RECIPE_PARAM_ERROR")] and host and host[0][0] is False
    assert any(x.kind == "popup" and x.data.get("title") == "입력값 확인" for x in e.events)
    assert not R.has_log(e, "입력 검증 통과") and not rig.sims["ch1.mfc"].set_commands_seen()
    assert sorted((tmp_path / "logs" / "CH1").glob("CH1_bad_*.txt"))
