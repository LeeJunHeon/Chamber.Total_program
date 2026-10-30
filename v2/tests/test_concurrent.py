"""CH1·CH2 동시 공정(사용자 지시 3장 (3) — 동시 운전 시험 필수). 엔진 두 개가 같은 system(공유 PLC·카메라·펄스 포트)에서.

- 두 챔버가 서로 막지 않는다: 두 공정이 겹친 시간에 진행되고 둘 다 done, 각자 자기 MFC·자기 펄스 포트(CH1 4007·CH2 4005).
- 공유 장비 충돌 없음: PLC 한 worker 로 두 챔버 코일 쓰기가 섞여도 각자 순서대로(시뮬레이터 FC5 순서), 카메라는 두 RF 공정이
  같이 참여(모드 ALL) → 둘 다 떠나면 멈춤, 펄스 포트 점유는 챔버마다(공정 끝 해제).
- 한 챔버 STOP 은 다른 챔버를 멈추지 않는다.
"""
from v2.ch12.blocks import expand, template_blocks
from v2.ch12.process_steps import caps_from_cfg
from v2.tests import proc_rig as R

P1 = dict(base_pressure=1e-5, working_pressure=3, process_time=0.3, shutter_delay=0.1, use_ar=True, ar_flow=20,
          use_rf_pulse=True, rf_pulse_power=120, rf_pulse_freq=0.5, rf_pulse_duty=40, use_ms=True, process_note="c1")
P2 = dict(base_pressure=1e-5, working_pressure=5, process_time=0.3, shutter_delay=0.1, use_ar=True, ar_flow=20,
          use_rf_pulse=True, rf_pulse_power=100, rf_pulse_freq=1.0, rf_pulse_duty=50, use_ms=True, use_g1=True,
          process_note="c2")
P2_DCP = dict(base_pressure=1e-5, working_pressure=5, process_time=0.3, shutter_delay=0.1, use_ar=True, ar_flow=20,
              use_dc_pulse=True, dc_pulse_power=80, dc_pulse_freq=20, use_ms=True, use_g1=True, process_note="c2d")


def _span(e):
    st = next(x for x in e.events if x.kind == "run_start")
    en = next(x for x in e.events if x.kind == "run_end")
    tr = e.traces[-1]
    return tr[0][0], tr[-1][0], st, en


def test_ch1_ch2_run_together_rf_camera_shared(tmp_path):
    rig = R.make(tmp_path)
    e1, e2 = rig.eng["ch1"], rig.eng["ch2"]
    seen = {"cam": set(), "own": []}

    def watch(t):
        seen["cam"].add(tuple(sorted(rig.shared.camera.users)))
        seen["own"].append(rig.shared.snapshot())
    R.run(rig, R.idle(e1, e2), hooks=[R.at(2, lambda: (e1.start(params=dict(P1)), e2.start(params=dict(P2)))), watch])
    assert R.ends(e1) == [("done", None)] and R.ends(e2) == [("done", None)]
    a0, a1, _, _ = _span(e1)
    b0, b1, _, _ = _span(e2)
    assert a0 < b1 and b0 < a1                                                  # 겹쳐서 진행
    assert ("chamber1", "chamber2") in seen["cam"]                              # 카메라 1대 — 두 RF 공정 참여(ALL)
    assert any(o.get("pulse 192.168.1.50:4007") == "ch1.rfpulse" and o.get("pulse 192.168.1.50:4005") == "ch2.rfpulse"
               for o in seen["own"])
    assert rig.shared.camera.users == set() and all(p.owner is None for p in rig.shared.pulse.ports.values())
    for e, n, p in ((e1, 1, P1), (e2, 2, P2)):
        bl, _ = template_blocks(dict(p), n, rig.cfg, caps=caps_from_cfg(rig.cfg, n))
        steps, _ = expand(bl, rig.cfg, n, caps=caps_from_cfg(rig.cfg, n))
        assert R.prefix_subsequence(R.mfc_prefixes(steps), rig.sims[f"ch{n}.mfc"].set_commands_seen())
        assert R.subsequence(R.plc_expected(steps, n), R.plc_writes(rig.sims["plc"]))
    assert rig.internal_errors == 0


def test_stop_one_chamber_other_continues(tmp_path):
    rig = R.make(tmp_path)
    e1, e2 = rig.eng["ch1"], rig.eng["ch2"]
    main2 = lambda: e2.cur_inst is not None and e2.cur_inst.type == "main" and e2.runner.cd is not None   # noqa: E731
    R.run(rig, R.idle(e1, e2), hooks=[R.at(2, lambda: (e1.start(params=dict(P1)), e2.start(params=dict(P2_DCP)))),
                                      R.when(main2, lambda: e2.stop(source="web"))])
    assert R.ends(e2) == [("aborted", "USER_STOP")] and R.ends(e1) == [("done", None)]
    assert not rig.sims["plc"].coil("MAIN_SHUTTER_2_SW") and not rig.sims["plc"].coil("MAIN_SHUTTER_1_SW")


def test_shared_mfc_poll_users_and_plc_proxies(tmp_path):
    rig = R.make(tmp_path)
    e1, e2 = rig.eng["ch1"], rig.eng["ch2"]
    users = []
    R.run(rig, R.idle(e1, e2), hooks=[R.at(2, lambda: (e1.start(params=dict(P1)), e2.start(params=dict(P2_DCP)))),
                                      lambda t: users.append((tuple(sorted(rig.shared.mfc["ch1"].users)),
                                                              tuple(sorted(rig.shared.mfc["ch2"].users))))])
    assert R.ends(e1) == [("done", None)] and R.ends(e2) == [("done", None)], (R.logs(e1)[-8:], R.logs(e2)[-8:])
    assert ("chamber1",) in {u[0] for u in users} and ("chamber2",) in {u[1] for u in users}   # 챔버마다 자기 MFC 폴링
    assert users[-1] == ((), ())                                                # 공정 끝 — 폴링 사용자 해제
