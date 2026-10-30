"""전수 목록 heat 영역(inv_heat.py)에서 기존 시험이 직접 확인하지 않던 동작 — 실제 실행(v1 import 없음, 근거 = v1 파일:줄).

- HEAT-006 히터 주소는 PLC_REG_MAP·PLC_COIL_MAP 밖(controller/heater_service.py:5-8)
- HEAT-016 발행 상태 키(controller/heater_service.py:646-654·702-708)
- HEAT-020 HEATER_PUSH_CONFIG=false 면 설정 쓰기 없이 세션 마커만(controller/heater_service.py:922-954)
- HEAT-022 HEATER 프로그램 없음 감지 1회(controller/heater_service.py:1039-1045)
- HEAT-025 이미 운전 중이면 ON 거부(controller/heater_service.py:266-297)
- HEAT-036 프로그램 종료 shutdown — RUN=0·로그·CSV '프로그램 종료' 행, 링크 없으면 경고(controller/heater_service.py:433-466)
- HEAT-111 엔진 시작 로그 '연동 준비'·'서비스 시작'(runtime/chamber_runtime.py:6946-6953, main.py:1257-1292)
- HEAT-115 패널 [리셋] — RUN 이면 '리셋 불가'(ui/heater_panel.py:389-401)
- HEAT-116~120 래더 모델(tests/heater_ladder_sim.py — v2/ch12/sim/plc_ladder.HeaterLadder)
"""
from __future__ import annotations

import pytest

from v2.ch12.drivers import plc_map as PM
from v2.ch12.drivers.heater import float_to_words
from v2.ch12.sim.heater_rig import FastHeaterRig
from v2.ch12.sim.plc_ladder import HeaterLadder, VAC_ITL_COIL

STATUS_KEYS = {"ok", "pv", "sv", "sv_limit", "cur_sv", "pid_err", "mv", "mv_pct", "mv_limit", "sv_ramp", "ramp_rate",
               "holdback", "ot_limit", "slow_zone", "slow_rate", "kp_act", "ti_act", "marker", "run", "itl", "fault", "ot",
               "tc_err", "wd_err", "pid_run", "dac_en", "tc_ok", "da_ok", "water_ok", "vac_ok", "rst", "output_dead",
               "noresp", "stale", "age_s", "t"}


def test_heat006_heater_addresses_not_in_plc_maps():
    assert not (set(range(110, 161)) & set(PM.REG_MAP.values()))
    assert not (set(range(640, 659)) & set(PM.COIL_MAP.values()))


def test_heat016_published_keys():
    r = FastHeaterRig()
    pub = r.listen()
    r.run(3)
    assert pub and all(not k.startswith("_") for k in pub[-1])
    assert STATUS_KEYS <= set(pub[-1]) and set(pub[-1]) - STATUS_KEYS <= {"residual_run"}
    assert set(r.eng.status()) == set(pub[-1])


def test_heat020_push_config_false_writes_marker_only():
    r = FastHeaterRig(push_config=False)
    r.run(2)
    w = [c for c in r.plc.writes() if c[0] == "wr"]
    assert [c[1] for c in w] == [160] and 0x1000 <= w[0][2] <= 0xFFFE
    assert r.eng.is_ready() == (True, "")


def test_heat022_no_heater_program_warned_once():
    r = FastHeaterRig()
    r.plc.forced = {146: 0, 147: 0}

    def broken(rr):
        rr.plc.M[653] = rr.plc.M[656] = False                     # TC1_OK·DA_OK 꺼짐
    for _ in range(5):
        r.plc.scan(1.0)
        broken(r)
        r.clk.t += 1.0
        r.plc.read()
        r.eng.poll_once()
        r.eng._flush()
    msgs = r.log_texts("HEATER 프로그램이 없는 것 같음")
    assert len(msgs) == 1 and msgs[0].startswith("⚠ ")


def test_heat025_turn_on_while_running_rejected():
    r = FastHeaterRig()
    r.run(3)
    assert r.call(r.eng.turn_on, 150.0)[0]
    r.run(3)
    ok, why = r.call(r.eng.turn_on, 160.0)
    assert not ok and why == "이미 운전 중입니다 — 목표를 바꾸려면 [적용]을 쓰십시오"
    assert r.log_texts("운전 ON 요청 — 목표 150.0°C (수동)")


def test_heat036_shutdown_turns_run_off_and_closes_csv(tmp_path):
    from v2.ch12.heater_profile import HeaterCfg, cfg_to_dict
    from v2.ch12.outputs.heater_csv import HeaterCsv
    cfg = cfg_to_dict(HeaterCfg(enabled=True, log_enabled=True, log_dir=str(tmp_path)))
    r = FastHeaterRig(cfg=cfg)
    sink = HeaterCsv(cfg, monotonic=r.clk.monotonic)
    r.eng.csv = sink
    r.run(3)
    assert r.call(r.eng.turn_on, 150.0)[0]
    r.run(10)
    done = []
    r.eng.shutdown(on_done=lambda: done.append(1))
    assert done and r.plc.writes()[-1] == ("wc", 640, False) and not r.plc.M[640]
    assert r.log_texts("프로그램 종료 — 히터 RUN OFF")
    sink.shutdown()
    lines = sorted(tmp_path.glob("CH2_HEATER_*"))[0].read_text(encoding="utf-8-sig").strip().splitlines()
    assert lines[-1].endswith(",프로그램 종료")
    # 링크가 없으면 보내지 못했다는 경고(워치독이 30 s 뒤 차단)
    r2 = FastHeaterRig()
    r2.run(3)
    assert r2.call(r2.eng.turn_on, 150.0)[0]
    r2.run(3)
    r2.plc.connected = False
    r2.eng.shutdown()
    assert r2.log_texts("⚠ 종료 시 히터 OFF 를 보내지 못함 — PLC 워치독이 30초 뒤 차단")


def test_heat111_start_logs():
    r = FastHeaterRig(ramp_rate=3.0)
    r.eng.start()
    lg = r.log_texts()
    assert lg[-2:] == ["CH2 히터 공정 연동 준비(레시피 use_heater)", "CH2 히터 서비스 시작"]
    assert any(m.startswith("⚠ 히터 설정: ") for m in lg[:-2])                 # 설정 경고(램프 < 6 → 6)


def test_heat115_panel_reset_refused_while_run():
    r = FastHeaterRig()
    r.run(3)
    assert r.call(r.eng.turn_on, 150.0)[0]
    r.run(3)
    ok, why = r.call(r.eng.panel_reset)
    assert not ok and why == "히터 RUN 이 켜져 있습니다.\n먼저 히터를 OFF 한 뒤 리셋하십시오."
    assert ("wc", 643, True) not in r.plc.calls


# ═════════════════════ 래더 모델(HEAT-116~120) ═════════════════════
def _lad(**kw):
    D, M = [0] * 512, [False] * 4096
    M[VAC_ITL_COIL] = True
    lad = HeaterLadder(D, M, **kw)
    lad.scan(0.0)
    return lad, D, M


def test_heat116_ladder_watchdog_30s():
    lad, D, M = _lad()
    D[112] = 1000
    M[640] = True
    for i in range(29):
        lad.scan(1.0)
    assert not M[646] and M[640]
    lad.scan(1.0)
    assert M[646] and M[642] and not M[640]                                  # WD_ERR → FAULT → RUN 리셋
    assert D[141] == 0
    lad2, D2, M2 = _lad()
    M2[640] = True
    for i in range(60):
        D2[114] = i + 1                                                      # 바뀌면 D115=D114, 타이머 0
        lad2.scan(1.0)
    assert not M2[646] and D2[115] == 60


def test_heat117_ladder_h0_defaults_and_clamps():
    lad, D, M = _lad()
    assert (D[120], D[121], D[113], D[126], D[128], D[118]) == (2, 100, 5000, 5500, 10, 520)
    D[113], D[126], D[118] = 9999, 9999, 2000
    lad.scan(0.0)
    assert (D[113], D[126], D[118]) == (9000, 9500, 1600)
    D[113], D[126] = 6000, 6000
    lad.scan(0.0)
    assert D[126] == 6300                                                    # OT ≤ SV 상한 → +300


def test_heat118_ladder_h1_h5():
    lad, D, M = _lad()
    assert M[641] and not M[642]                                             # ITL = TC·DA·WATER·VAC·¬FAULT
    lad.water_ok = False
    lad.scan(0.0)
    assert not M[641]
    lad.water_ok = True
    M[VAC_ITL_COIL] = False
    lad.scan(0.0)
    assert not M[641]
    M[VAC_ITL_COIL] = True
    lad.T = 600.0                                                            # PV > D126(550 °C)
    lad.scan(0.0)
    assert M[644] and M[642] and not M[641]
    lad.T = 25.0
    M[643] = True                                                            # RST → 래치 해제(자기 리셋)
    lad.scan(0.0)
    assert not M[644] and not M[643]
    lad.scan(0.0)                                                            # FAULT·ITL 은 다음 스캔에 다시 계산
    assert not M[644] and not M[642] and not M[643] and M[641]
    lad.tc_fail = True
    M[640] = True
    lad.scan(0.0)
    assert M[645] and M[642] and not M[640]                                  # RUN & ¬TC_OK → TC_ERR


def test_heat119_ladder_ramp_pid_and_cutoff():
    lad, D, M = _lad()
    D[112], D[120], D[121] = 3000, 2, 1000                                   # 300 °C, 2 카운트/s, 홀드백 100 °C
    M[640] = True
    lad.scan(0.0)
    sv0 = D[119]
    lad.scan(1.0)
    assert D[119] - sv0 == 2                                                 # 1초 펄스마다 D120 카운트
    D[127], D[128] = 3000, 1                                                 # 감속 구간 안이면 D128
    lad.scan(1.0)
    assert D[119] - sv0 == 3
    assert 400 <= D[141] <= D[118] and M[652] and M[649]                     # PID 출력 400~D118
    D[142], D[143] = float_to_words(60.0)                                    # Kp 범위 밖 → 3.0 사용
    lad.scan(0.0)
    assert D[147] == float_to_words(3.0)[1]
    M[640] = False
    lad.scan(1.0)
    assert D[141] == 0 and not M[652] and D[119] == D[110]                   # ¬RUN → 출력 0, D119 = PV
    lad.gain = 0.0                                                           # PV 고정(25.0 °C)
    M[640] = True
    D[121] = 1                                                               # 홀드백 0.1 °C — PV 보다 1 카운트까지만 앞섬
    lad.scan(1.0)
    lad.scan(1.0)
    lad.scan(1.0)
    assert D[119] - D[110] == 1


def test_heat120_thermal_model():
    lad, D, M = _lad()
    D[112], D[118] = 9000, 1600
    M[640] = True
    lad.scan(0.0)
    lad._mv = 1600.0
    D[141] = 1600
    t0 = lad.T
    f = (min(D[141], D[118]) - 400) / 1200.0
    lad._h7_pid = lambda run, dt: None                                       # 출력 고정(모델만)
    lad.scan(1.0)
    assert lad.T - t0 == pytest.approx((2250.0 * f - (t0 - 25.0)) / 900.0, rel=1e-9)
    lad.gas_loss, lad.plasma_heat = 0.25, 20.0
    t1 = lad.T
    lad.scan(1.0)
    assert lad.T - t1 == pytest.approx((2250.0 * f + 20.0 - (t1 - 25.0) * 1.25) / 900.0, rel=1e-9)
