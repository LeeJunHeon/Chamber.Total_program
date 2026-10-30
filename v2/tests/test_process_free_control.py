"""공정 중 자유 제어 4종(사용자 지시 3장 (3)) — 실제 시험대(Ch12Rig)에서 블록 종류별로.

- PROCESS_ADJUST: 지금 블록이 쓰는 값은 바로 드라이버에(MFC FLOW_SET → 시뮬레이터가 새 Q 명령을 받음 → confirmed), 뒤 블록이 쓰는
  값은 예약(scheduled — 그 스텝 값이 바뀜), 메인 남은 시간 수정, 안 쓰는 항목·범위 밖·공정 밖 거부.
- 블록 건너뛰기: 정의가 skippable 인 블록만(대기·압력 대기·메인), 정리 후 다음 블록. 아니면 SKIP_NOT_ALLOWED.
- 일시정지·재개: 대기 = 타이머 정지(남은 시간 유지·ETA 늦춤), 파워 켜기 = 다음 명령 보류, 못 멈추는 블록 거부.
- 시퀀스 편집: 아직 시작 안 한 블록만(판 +1·재검사), 끝 정지를 지워도 런 끝 정지 순서(자동 끝 정지)는 남는다.
"""
from v2.tests import proc_rig as R

P = dict(base_pressure=1e-5, working_pressure=3, process_time=1.0, shutter_delay=0.2, use_ar=True, ar_flow=20,
         use_rf_pulse=True, rf_pulse_power=120, rf_pulse_freq=0.5, rf_pulse_duty=40, use_ms=True, process_note="fc")


def _blk(e, t):
    return lambda: e.cur_inst is not None and e.cur_inst.type == t and e.runner.is_running and not e.runner.shutdown


def _rig(tmp_path):
    rig = R.make(tmp_path, chambers=("ch1",))
    return rig, rig.eng["ch1"]


def _adj(e):
    return [(x.data["device"] + "." + x.data["item"], x.data["result"], x.code) for x in e.events if x.kind == "adjust"]


def _main_secs(e):
    tr = [x for x in e.traces[-1] if not x[4]]
    i = next(k for k, x in enumerate(tr) if x[3].startswith("메인 공정 진행"))
    return tr[i + 1][0] - tr[i][0]


def test_adjust_live_scheduled_and_rejects(tmp_path):
    rig, e = _rig(tmp_path)
    got = {}

    def in_sd():
        got["not_used"] = e.adjust("dcp", "power", 80)
        got["range"] = e.adjust("mfc", "ar_flow", -1)
        got["sched"] = e.adjust("process", "main_remaining", 8)                  # 아직 안 온 메인 → 예약
        got["state"] = {(a["device"], a["item"]): a["scope"] for a in e.adjust_state()}

    def in_main():
        got["live"] = e.adjust("mfc", "ar_flow", 25.0, source="web", by="op1")
    got["idle"] = e.adjust("mfc", "ar_flow", 10)
    R.run(rig, R.idle(e), hooks=[R.at(2, lambda: e.start(params=dict(P))), R.when(_blk(e, "wait"), in_sd),
                                 R.when(lambda: _blk(e, "main")() and e.runner.cd is not None, in_main)])
    assert got["idle"] == "PROCESS_NOT_RUNNING" and got["not_used"] == "ADJUST_NOT_USED"
    assert got["range"] == "ADJUST_OUT_OF_RANGE" and got["sched"] is None and got["live"] is None
    assert got["state"][("process", "main_remaining")] == "future" and got["state"][("mfc", "ar_flow")] == "live"
    a = _adj(e)
    assert ("process.main_remaining", "scheduled", None) in a
    assert ("mfc.ar_flow", "applied", None) in a and ("mfc.ar_flow", "confirmed", None) in a
    assert "Q1 25.0" in rig.sims["ch1.mfc"].set_commands_seen()
    assert 7.5 < _main_secs(e) < 9.0                                            # 60 s 메인이 예약한 8 s 로
    assert R.ends(e) == [("done", None)]


def test_adjust_main_remaining_live(tmp_path):
    rig, e = _rig(tmp_path)
    R.run(rig, R.idle(e), hooks=[R.at(2, lambda: e.start(params=dict(P))),
                                 R.when(lambda: _blk(e, "main")() and e.runner.cd is not None
                                        and e.runner.remaining_s() < 55, lambda: e.adjust("process", "main_remaining", 3))])
    assert 7.0 < _main_secs(e) < 10.0 and R.ends(e) == [("done", None)]


def test_skip_wait_block_and_reject_non_skippable(tmp_path):
    rig, e = _rig(tmp_path)
    got = {}

    def in_gas():
        got["gas"] = e.skip_block()
        got["gas_pause"] = e.pause()
    R.run(rig, R.idle(e), hooks=[R.at(2, lambda: e.start(params=dict(P))), R.when(_blk(e, "gas_intro"), in_gas),
                                 R.when(_blk(e, "wait"), lambda: got.setdefault("wait", e.skip_block(source="local"))),
                                 R.when(_blk(e, "main"), lambda: got.setdefault("main", e.skip_block()))])
    assert got == {"gas": "SKIP_NOT_ALLOWED", "gas_pause": "PAUSE_NOT_ALLOWED", "wait": None, "main": None}
    end = [x for x in e.events if x.kind == "run_end"][-1]
    ex = [(r["type"], r["label"], r["result"]) for r in end.data["executed"]]
    assert ("wait", "Power Delay 20초", "skipped") in ex and ("main", "", "skipped") in ex
    assert ("wait", "Shutter Delay 0.2분", "done") in ex
    assert R.ends(e) == [("done", None)] and _main_secs(e) < 3.0
    assert R.has_log(e, "Power Delay 20초 중단됨")
    assert any(x[2] == "RF_PULSE_STOP" for x in e.traces[-1])                    # 끝 정지(파워 끄기)는 그대로


def test_skip_pressure_wait_cancels_mfc_wait(tmp_path):
    rig, e = _rig(tmp_path)
    rig.sims["ch1.mfc"].sp[2] = 9.0                                             # 도달하지 않을 SP3(90 mTorr)
    R.run(rig, R.idle(e), hooks=[R.at(2, lambda: e.start(params=dict(P, process_time=0.05, shutter_delay=0))),
                                 R.when(lambda: _blk(e, "pressure_ctrl")() and e.runner.idx == e.cur_inst.first_step + 1
                                        and e.clock.monotonic() > 0, lambda: e.skip_block())])
    assert R.ends(e) == [("done", None)], R.logs(e)[-15:]
    assert R.has_log(e, "[SKIP] 블록") and not R.has_log(e, "오류 발생: [MFC")


def test_pause_resume_timer_and_power_hold(tmp_path):
    rig, e = _rig(tmp_path)
    got = {}

    def pause_main():
        got["p"] = e.pause()
        got["t"] = e.clock.monotonic()

    def resume():
        got["r"] = e.resume_pause()
    paused_long = lambda: "t" in got and "r" not in got and e.clock.monotonic() - got["t"] >= 10.0   # noqa: E731
    R.run(rig, R.idle(e), hooks=[R.at(2, lambda: e.start(params=dict(P, process_time=0.2))),
                                 R.when(lambda: _blk(e, "main")() and e.runner.cd is not None, pause_main),
                                 R.when(paused_long, resume)])
    assert got["p"] is None and got["r"] is None
    assert 21.5 < _main_secs(e) < 24.0                                          # 12 s 메인 + 10 s 정지
    assert [x.data["on"] for x in e.events if x.kind == "pause"] == [True, False]


def test_pause_power_on_holds_next_commands(tmp_path):
    rig, e = _rig(tmp_path)
    got = {}

    def pause():
        got["p"] = e.pause()
        got["t"] = e.clock.monotonic()
    R.run(rig, R.idle(e), hooks=[R.at(2, lambda: e.start(params=dict(P, process_time=0.05))),
                                 R.when(_blk(e, "power_on"), pause),
                                 R.when(lambda: "t" in got and e.clock.monotonic() - got["t"] > 40, e.resume_pause)])
    assert got["p"] is None and R.ends(e) == [("done", None)]
    tr = [x for x in e.traces[-1] if not x[4]]
    i = next(k for k, x in enumerate(tr) if x[3].startswith("Power Delay"))
    assert tr[i + 1][0] - got["t"] > 40.0                                       # Power Delay 는 재개 뒤에야 끝남


def test_edit_insert_wait_and_delete_end_stop(tmp_path):
    rig, e = _rig(tmp_path)
    got = {}

    def edit():
        n = len(e.blocks)
        main_at = next(i for i, b in enumerate(e.blocks) if b["type"] == "main")
        got["started"] = e.edit_sequence({"op": "delete", "at": 0})
        got["ins"] = e.edit_sequence({"op": "insert", "at": main_at,
                                      "block": {"type": "wait", "params": {"sec": 4}, "label": "추가 대기"}}, by="op1")
        got["del"] = e.edit_sequence({"op": "delete", "at": n})                 # 끝 정지(이제 마지막) 삭제
        got["rev"] = e.seq.rev
    R.run(rig, R.idle(e), hooks=[R.at(2, lambda: e.start(params=dict(P, process_time=0.05))),
                                 R.when(_blk(e, "wait"), edit)])
    assert got["started"] == "EDIT_STARTED_BLOCK" and got["ins"] is None and got["del"] is None and got["rev"] == 2
    assert R.ends(e) == [("done", None)]
    end = [x for x in e.events if x.kind == "run_end"][-1]
    ex = [r["label"] or r["type"] for r in end.data["executed"]]
    assert "추가 대기" in ex and ex[-1] == "자동 끝 정지"
    rec = end.data["recipe"]
    assert rec["rev"] == 2 and len(rec["original"]) == len(rec["blocks"]) and rec["history"][0]["ok"] is False
    assert any(x[2] == "RF_PULSE_STOP" for x in e.traces[-1]) and R.has_log(e, "추가 대기 완료")
