"""TSP 운전 엔진(v2/ch12/tsp_engine.py·tsp_cycle.py) — 가상 시계 + Ch12Rig(TSP 시뮬레이터·CH1 IG 시뮬레이터) 실제 실행.

기대값 근거는 v1 runtime/tsp_runtime.py·controller/tsp_controller.py(파일:줄은 각 시험 독스트링)와 전수 목록 PC-120~151,
v1 시험 tests/test_ig_pc_same_ch.py:312(test_i7 — TSP finally 의 IG ensure_off → cleanup 뒤 'UI 기본값 복원' 이 실행됨, TEST-078).
시간은 짧게(TSP_ON_SEC 12·TSP_OFF_SEC 15 — 폴링 10 s·첫 확인 5 s 는 v1 기본값) 바꿔 돌린다(규칙은 같다).
"""
from __future__ import annotations

from pathlib import Path

import pytest

from v2.common.clock import VirtualClock
from v2.ch12 import snapshot_fields as SF
from v2.ch12 import tsp_cycle as TC
from v2.ch12 import tsp_engine as TE
from v2.ch12.drivers import tsp as T
from v2.ch12.run_state import RunState
from v2.ch12.sim.rig import Ch12Rig

FAST = {"CAMERA_MIN_FREE_GB": 0.0, "TSP_ON_SEC": 12.0, "TSP_OFF_SEC": 15.0, "TSP_POLL_SEC": 10.0,
        "TSP_FIRST_CHECK_DELAY_SEC": 5.0}
ON_FRAME = T.build_frame("011", True, "1").hex()
OFF_FRAME = T.build_frame("011", True, "0").hex()


def make(tmp_path, cfg=None, rs=None):
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    rig = Ch12Rig(c, tmp_path, cfg=dict(FAST, **(cfg or {})))
    rig.rows = []
    rig.row_listeners.append(rig.rows.append)
    assert rig.start()
    rs = rs or RunState(c)
    eng = TE.attach(rig, rs)
    return c, rig, rs, eng


@pytest.fixture
def env(tmp_path):
    c, rig, rs, eng = make(tmp_path)
    yield c, rig, rs, eng
    rig.stop()


def run_until_idle(rig, eng, max_ticks=600):
    n = 0
    while eng.busy and n < max_ticks:
        rig.loop.run(2)
        n += 2
    assert not eng.busy, f"{max_ticks} tick 안에 끝나지 않음 — {eng.state()}"
    rig.loop.run(1)
    rig.outputs.writer.flush()
    return n


def run_until(rig, pred, max_ticks=300):
    n = 0
    while not pred() and n < max_ticks:
        rig.loop.run(1)
        n += 1
    assert pred()


def logs(eng):
    return [e.detail for e in eng.events if e.kind == "log"]


def cards(eng, name="finished"):
    return [e.data for e in eng.events if e.kind == "card" and e.detail == name]


def popups(eng):
    return [(e.data["title"], e.data["text"]) for e in eng.events if e.kind == "popup"]


def tsp_frames(rig):
    return [f.hex() for f in rig.sims["tsp"].frames if f.hex() in (ON_FRAME, OFF_FRAME)]


def test_cycles_exhausted_is_cycles_completed_card_and_full_cleanup(env, tmp_path):
    """controller/tsp_controller.py:123-171 순서·문구, tsp_runtime.py:576-581 시작 줄, 615-647 cycles_exhausted → 'cycles_completed'
    '목표 사이클 완료' ok True(PC-134 — 카드 그림은 '정상 종료', D175), finally(668-705): TSP OFF 가 컨트롤러·런타임에서 두 번,
    SIG 0 이 ctrl ensure_off·runtime ensure_off·cleanup 으로 세 번, UI 기본값 복원(TEST-078 i7)."""
    c, rig, rs, eng = env
    rig.chambers["ch1"].pressure_override = 1.0e-5
    r = eng.start({"target": "2.5e-7", "cycles": "2"}, "web", by="op")
    assert r["ok"] and eng.busy and eng.state()["display"] == {"now_cycle": "0", "base_pressure": ""}
    run_until_idle(rig, eng)
    lg = logs(eng)
    assert lg[0] == ("=== TSP 공정 시작 === host=192.168.1.50 ig=4001 tsp=4004 target=2.5e-07 cycles=2 "
                     "on=12.0s off=15.0s poll=10.0s first_wait=5.0s")
    assert lg[1:6] == ["[STATE] prepare",
                       "[TSP] 공정 시작: target=2.5e-07, cycles=2, on=12.0s, off=15.0s, poll=10.0s, first_wait=5.0s",
                       "[STATE] ig_on", "[IG] ensure_on 완료", "[IG] P=1.000e-05"]
    assert lg[6] == "[IG] 초기 판정 P0=1.000e-05"
    order = [x for x in lg if x.startswith(("[STATE]", "[TSP] ON", "[TSP] OFF", "[TSP] cycle", "==="))]
    assert order[1:] == ["[STATE] prepare", "[STATE] ig_on", "[STATE] tsp_on", "[TSP] ON (cycle 1/2)", "[STATE] tsp_off",
                         "[TSP] OFF (cycle 1/2)", "[TSP] cycle 1/2 완료", "[STATE] tsp_on", "[TSP] ON (cycle 2/2)",
                         "[STATE] tsp_off", "[TSP] OFF (cycle 2/2)", "[TSP] cycle 2/2 완료", "[STATE] cleanup",
                         "[STATE] finished",
                         "=== RESULT === ok=False final_P=1.000e-05 cycles_done=2 reason=cycles_exhausted"]
    on_polls = lg[lg.index("[TSP] ON (cycle 1/2)"):lg.index("[STATE] tsp_off")]
    assert 2 <= sum(x.startswith("[IG] P=") for x in on_polls) <= 3          # 진입 즉시 1회 + 10 s 간격 + 마감(12 s)
    st = cards(eng, "started")[0]
    assert st["params"] == {"process_note": "TSP run: target=2.500e-07, cycles=2", "tsp_target": 2.5e-07, "tsp_cycles": 2}
    fin = cards(eng)[0]
    assert fin["ok"] is True and fin["detail"] == {"process_name": "TSP", "status": "cycles_completed",
                                                   "status_label": "목표 사이클 완료", "final_pressure": 1e-05,
                                                   "cycles_done": 2, "reason": "cycles_exhausted"}
    assert tsp_frames(rig) == [ON_FRAME, OFF_FRAME, ON_FRAME, OFF_FRAME, OFF_FRAME, OFF_FRAME]
    cmds = rig.sims["ch1.ig"].commands_seen()
    assert cmds.count("SIG 1") == 1 and cmds.count("SIG 0") == 3
    s = eng.state()
    assert s["phase"] == "idle" and s["inputs"] == {"target": "2.5e-07", "cycles": "10"}       # TSP_UI_DEFAULT_*
    assert s["display"] == {"now_cycle": "0", "base_pressure": ""} and s["result"]["outcome"] == "cycles_completed"
    assert rig.shared.tsp_ig.owner is None and not rig.tsp.in_use and not rs.is_running("tsp")
    # 로그 파일 — '[TSP] [TSP]' 접두어 중복(PC-151), {LOG_ROOT_DIR}/TSP/TSP_YYYYmmdd_HHMMSS.txt
    p = Path(s["log_file"])
    assert p.parent == tmp_path / "logs" / "TSP" and p.name.startswith("TSP_") and p.suffix == ".txt"
    text = p.read_text(encoding="utf-8")
    assert "] [TSP] [TSP] ON (cycle 1/2)\n" in text and "] [TSP] [STATE] finished\n" in text
    assert text.splitlines()[0].endswith("[TSP] === TSP 공정 시작 === host=192.168.1.50 ig=4001 tsp=4004 target=2.5e-07 "
                                         "cycles=2 on=12.0s off=15.0s poll=10.0s first_wait=5.0s")
    assert rig.internal_errors == 0 and not rig.unknown_fields


def test_target_reached_during_on_and_initial_below(tmp_path):
    """ON 중 도달 → reached_target_on, cycles_done 은 i-1(PC-140 비고). 첫 판정 P0 ≤ 목표 → initial_below_target(TSP ON 없음,
    finally OFF 두 번은 그대로 — tsp_controller.py:138-142·184-194, tsp_runtime.py:676-678)."""
    c, rig, rs, eng = make(tmp_path, cfg={"TSP_COOLDOWN_S": 0.0})
    try:
        ch = rig.chambers["ch1"]
        ch.pressure_override = 1.0e-5
        assert eng.start({"target": 2.5e-7, "cycles": 3})["ok"]
        run_until(rig, lambda: "[TSP] ON (cycle 2/3)" in logs(eng))
        ch.pressure_override = 1.0e-7
        run_until_idle(rig, eng)
        lg = logs(eng)
        assert lg[-1] == "=== RESULT === ok=True final_P=1.000e-07 cycles_done=1 reason=reached_target_on"
        assert cards(eng)[-1]["detail"]["status"] == "normal_completed" and cards(eng)[-1]["ok"] is True
        assert cards(eng)[-1]["detail"]["status_label"] == "정상 종료"
        rig.sims["tsp"].frames.clear()
        n0 = len(eng.events)
        assert eng.start({"target": "2.5e-7", "cycles": "3"})["ok"]          # 쿨다운 0
        run_until_idle(rig, eng)
        lg = [e.detail for e in list(eng.events)[n0:] if e.kind == "log"]
        assert "[IG] 초기 판정 P0=1.000e-07" in lg and not any(x.startswith("[TSP] ON") for x in lg)
        assert lg[-1] == "=== RESULT === ok=True final_P=1.000e-07 cycles_done=0 reason=initial_below_target"
        assert tsp_frames(rig) == [OFF_FRAME, OFF_FRAME]
    finally:
        rig.stop()


def test_ig_failures(tmp_path):
    """IG 점등 실패 → '[TSP] 예외: RuntimeError('SIG 1 failed (ensure_on)')'·exception:… 오류 카드(device/ig.py:1142-1153,
    tsp_controller.py:177-179). RDI 가 숫자가 아니면 '[IG] 초기 판정 RDI 실패'·'[IG] 압력 읽기 오류' 로그 뒤 계속(PC-139·141)."""
    c, rig, rs, eng = make(tmp_path, cfg={"TSP_COOLDOWN_S": 0.0})
    try:
        ig = rig.sims["ch1.ig"]
        ig.sig1_reply = "ERR"
        assert eng.start({"target": "2.5e-7", "cycles": "1"})["ok"]
        run_until_idle(rig, eng, 400)
        lg = logs(eng)
        assert "[TSP] 예외: RuntimeError('SIG 1 failed (ensure_on)')" in lg
        assert lg[-1] == ("=== RESULT === ok=False final_P=NaN cycles_done=0 "
                          "reason=exception:RuntimeError('SIG 1 failed (ensure_on)')")
        fin = cards(eng)[-1]
        assert fin["ok"] is False and fin["detail"]["status"] == "error" and fin["detail"]["status_label"] == "오류로 공정 종료"
        assert fin["detail"]["errors"] == ["exception:RuntimeError('SIG 1 failed (ensure_on)')"]
        assert fin["detail"]["final_pressure"] is None                       # v1 NaN
        assert ig.commands_seen().count("SIG 1") == 5                         # retries=5
        ig.sig1_reply = "OK"
        ig.rdi_script = ["IG OFF"]
        n0 = len(eng.events)
        assert eng.start({"target": "2.5e-7", "cycles": "1"})["ok"]
        run_until_idle(rig, eng)
        lg = [e.detail for e in list(eng.events)[n0:] if e.kind == "log"]
        assert "[IG] 초기 판정 RDI 실패: RuntimeError(\"parse error: 'IG OFF'\")" in lg
        assert "[IG] 압력 읽기 오류: RuntimeError(\"parse error: 'IG OFF'\")" in lg
        assert lg[-1] == "=== RESULT === ok=False final_P=NaN cycles_done=1 reason=cycles_exhausted"
    finally:
        rig.stop()


def test_tsp_no_response_and_interlock_codes(tmp_path):
    """TSP 무응답 → v1 asyncio.TimeoutError → '[TSP] 예외: TimeoutError()'. ON 확인 시간초과(인터락 — 상태 2) →
    TSPError('상태 확인 시간초과(target=on, last=2)')·코드 E808(D17)."""
    c, rig, rs, eng = make(tmp_path, cfg={"TSP_COOLDOWN_S": 0.0})
    try:
        rig.chambers["ch1"].pressure_override = 1.0e-5
        sim = rig.sims["tsp"]
        sim.add_fault("no_response", 0.0, c.monotonic() + 3600.0)
        assert eng.start({"target": "2.5e-7", "cycles": "2"})["ok"]
        run_until_idle(rig, eng)
        lg = logs(eng)
        assert "[TSP] 예외: TimeoutError()" in lg
        assert lg[-1] == "=== RESULT === ok=False final_P=1.000e-05 cycles_done=0 reason=exception:TimeoutError()"
        assert cards(eng)[-1]["detail"]["errors"] == ["exception:TimeoutError()"]
        sim.faults.clear()
        sim.interlock = True
        n0 = len(eng.events)
        assert eng.start({"target": "2.5e-7", "cycles": "2"})["ok"]
        run_until_idle(rig, eng)
        lg = [e.detail for e in list(eng.events)[n0:] if e.kind == "log"]
        assert "[TSP] 예외: TSPError('상태 확인 시간초과(target=on, last=2)')" in lg
        fin = cards(eng)[-1]
        assert fin["code"] == "E808" and fin["ok"] is False and eng.state()["result"]["code"] == "E808"
        assert any(e.kind == "command_result" and e.code == "E808" for e in rig.events if e.source == "tsp")
    finally:
        rig.stop()


def test_user_stop_python311_semantics(env, tmp_path):
    """STOP(PC-137·142·135·150): '[TSP] 중단 요청'(파일 '[TSP] [TSP] 중단 요청') → 컨트롤러 '[TSP] run() 취소됨'·cleanup·OFF·
    finished → '[사용자 중단]'·카드 stopped(3.11 — D112). RESULT 줄 없음. 끝나면 TSP 쿨다운(mark_finished)."""
    c, rig, rs, eng = env
    rig.chambers["ch1"].pressure_override = 1.0e-5
    assert eng.stop()["reason"] == "PROCESS_NOT_RUNNING"
    assert eng.start({"target": "2.5e-7", "cycles": "3"})["ok"]
    run_until(rig, lambda: "[TSP] ON (cycle 1/3)" in logs(eng))
    rig.loop.run(3)
    assert eng.stop("web")["ok"]
    run_until_idle(rig, eng)
    lg = logs(eng)
    i = lg.index("[TSP] 중단 요청")
    assert lg[i + 1:] == ["[TSP] run() 취소됨", "[STATE] cleanup", "[STATE] finished", "[사용자 중단]"]
    assert not any(x.startswith("=== RESULT") for x in lg)
    assert cards(eng)[-1] == {"ok": False, "detail": {"process_name": "TSP", "stopped": True}, "run_id": eng.run_id,
                              "card": "finished"}
    assert eng.state()["result"]["outcome"] == "stopped"
    assert tsp_frames(rig) == [ON_FRAME, OFF_FRAME, OFF_FRAME]                # ctrl finally + runtime finally
    assert "] [TSP] [TSP] 중단 요청\n" in Path(eng.log_file).read_text(encoding="utf-8")
    assert rs.remaining_cooldown("tsp", 0, 60.0) > 55.0


def test_total_timeout_is_cancelled_result_not_timed_out(tmp_path):
    """전체 상한 cycles×(on+off)+margin(tsp_runtime.py:592-599): 넘으면 컨트롤러가 취소를 결과 'cancelled' 로 삼켜
    '=== RESULT === ok=False … reason=cancelled'·오류 카드 errors ['cancelled'] — 'TSP run timed out' RuntimeError 는 나지 않음
    (PC-132·150 — 3.11·3.12·3.13 모두 같음, D112)."""
    c, rig, rs, eng = make(tmp_path, cfg={"TSP_TOTAL_TIMEOUT_MARGIN_S": 0.0})
    try:
        rig.chambers["ch1"].pressure_override = 1.0e-5
        assert eng.start({"target": "2.5e-7", "cycles": "1"})["ok"]
        t0 = c.monotonic()
        run_until(rig, lambda: eng.state()["deadline_left_s"] is not None)
        assert eng.state()["deadline_left_s"] <= 27.0
        run_until_idle(rig, eng)
        lg = logs(eng)
        assert "[TSP] run() 취소됨" in lg and not any("timed out" in x for x in lg)
        assert lg[-1].startswith("=== RESULT === ok=False final_P=1.000e-05 cycles_done=0 reason=cancelled")
        assert cards(eng)[-1]["detail"]["errors"] == ["cancelled"] and cards(eng)[-1]["ok"] is False
        assert c.monotonic() - t0 < 27.0 + 15.0
    finally:
        rig.stop()


def test_start_guards_cooldown_and_parse(env):
    """시작 가드 순서·문구(tsp_runtime.py:394-443, PC-123~125): 실행 중 → '이미 실행 중입니다.'; CH1 챔버·CH1 PC 실행 중 →
    '실행 오류'/'CH1는 이미 다른 공정이 실행 중입니다.'(CH2 는 막지 않음); TSP 쿨다운 '대기 필요'; 입력 오류 문구.
    종료 뒤 60 s 동안 CH1 챔버·CH1 PC 도 TSP 쿨다운을 본다(runtime_state chamber/pc_block_reason)."""
    c, rig, rs, eng = env
    r = eng.start({"target": "", "cycles": "1"})
    assert r == {"ok": False, "reason": "BAD_ARGS", "message": "입력값 파싱 실패(TSP_targetPressure_edit 가 비어있습니다.)"}
    assert logs(eng)[-1] == "[ERROR] 입력 파싱 실패: TSP_targetPressure_edit 가 비어있습니다."
    assert eng.start({"target": "2.5e-7", "cycles": "0"})["message"] == "입력값 파싱 실패(반복 횟수는 1 이상이어야 합니다.)"
    assert eng.start({"target": "abc", "cycles": "1"})["message"] == \
        "입력값 파싱 실패(could not convert string to float: 'abc')"
    rs.mark_started("chamber", 1)
    assert eng.start({"target": "2.5e-7", "cycles": "1"})["reason"] == "CH1_PROCESS_RUNNING"
    assert popups(eng)[-1] == ("실행 오류", "CH1는 이미 다른 공정이 실행 중입니다.")
    rs.mark_finished("chamber", 1)
    rs._last_finish["chamber"].clear()
    rs.mark_started("chamber", 2)                                                # CH2 공정은 TSP 를 막지 않는다
    rig.chambers["ch1"].pressure_override = 1.0e-7
    assert eng.start({"target": "2.5e-7", "cycles": "1"}, "robot")["ok"]
    assert eng.start({"target": "2.5e-7", "cycles": "1"}) == {"ok": False, "reason": "PROCESS_RUNNING",
                                                               "message": "TSP 공정이 이미 실행 중"}
    assert logs(eng)[-1] == "이미 실행 중입니다." or "이미 실행 중입니다." in logs(eng)
    assert not rs.chamber_block_reason(1)[0] and rs.chamber_block_reason(1)[2] == "TSP 실행 중"
    run_until_idle(rig, eng)
    ok, remain, why = rs.chamber_block_reason(1)
    assert not ok and why.startswith("쿨다운 대기: TSP ") and 55.0 < remain <= 60.0
    assert not rs.pc_block_reason(1)[0] and rs.pc_block_reason(2)[0]
    r = eng.start({"target": "2.5e-7", "cycles": "1"})
    secs = int(rs.remaining_cooldown("tsp", 0, 60.0) + 0.999)
    assert r["reason"] == "COOLDOWN" and r["message"] == f"이전 공정 종료 후 쿨다운 60초 미경과(잔여 {secs}초)"
    assert popups(eng)[-1] == ("대기 필요", f"이전 공정 종료 후 60초 대기 필요합니다.\n{secs}초 후에 시작하십시오.")


def test_race_recheck_starts_cooldown_without_card(tmp_path):
    """실행 직전 재확인(tsp_runtime.py:513-519)에서 걸리면 팝업만 — 카드 없음, finally 의 mark_finished 로 TSP 쿨다운 시작
    (PC-148, D110 — v1 그대로)."""
    class Racy(RunState):
        calls = 0

        def is_running(self, kind, channel=None):
            if kind == "chamber":
                Racy.calls += 1
                return Racy.calls > 1                     # start() 의 첫 확인은 통과, 스레드의 재확인에서 걸림
            return super().is_running(kind, channel)
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    rig = Ch12Rig(c, tmp_path, cfg=FAST)
    assert rig.start()
    eng = TE.attach(rig, Racy(c))
    try:
        assert eng.start({"target": "2.5e-7", "cycles": "1"})["ok"]
        run_until_idle(rig, eng, 20)
        assert popups(eng) == [("실행 오류", "CH1는 이미 다른 공정이 실행 중입니다.")]
        assert cards(eng, "started") == [] and cards(eng) == []
        assert eng.rs.remaining_cooldown("tsp", 0, 60.0) > 55.0 and not rig.sims["tsp"].frames
    finally:
        rig.stop()


def test_ch1_ig_shared_row_and_no_second_connection(env):
    """D67: TSP 게이지는 CH1 IG worker 공유 — 두 번째 연결 없음(tsp.ig 시뮬레이터는 아무것도 받지 않음), 잡은 동안 ch1.ig 1초 열은
    TSP 쪽 값(tsp.ig 와 같은 값), tick 에 RDI 하나. 놓으면 ch1.ig 는 CH1 드라이버로, tsp.ig 는 off."""
    c, rig, rs, eng = env
    rig.chambers["ch1"].pressure_override = 1.0e-5
    assert eng.start({"target": "2.5e-7", "cycles": "1"})["ok"]
    run_until(rig, lambda: "[TSP] ON (cycle 1/1)" in logs(eng))
    ig = rig.sims["ch1.ig"]
    n0, k0 = len(ig.commands_seen()), len(rig.rows)
    rig.loop.run(8)
    for r in rig.rows[k0 + 1:]:
        assert r["tsp.ig.q"] == "ok" and r["ch1.ig.q"] == "ok"
        assert r["tsp.ig.pressure_torr"] == r["ch1.ig.pressure_torr"] == 1.0e-5
        assert r["tsp.proc.state"] in ("tsp_on", "tsp_off") and r["tsp.set.base_torr"] == 2.5e-7
        assert r["tsp.proc.block_label"].startswith("TSP ")
    assert len(ig.commands_seen()) - n0 <= 8 + 1                                 # tick RDI + 엔진 폴링(10 s 주기)
    assert rig.shared.tsp_ig.owner == "tsp" and rig.sources["tsp.ig"].worker is rig.workers["ch1.ig"]
    run_until_idle(rig, eng)
    assert rig.sims["tsp.ig"].commands_seen() == []
    assert rig.shared.tsp_ig.owner is None and rig.sources["tsp.ig"].worker is None
    rig.loop.run(3)
    last = rig.rows[-1]
    assert last["tsp.ig.q"] == "off" and last["ch1.ig.q"] == "ok" and last["tsp.proc.state"] == "idle"
    assert not any(e.kind == "connected" and e.source == "tsp.ig" for e in rig.events)


def test_log_between_runs_goes_to_next_file(tmp_path):
    """PC-129·149: 첫 실행 전 줄은 프리버퍼 + SYSTEM 로그, 첫 파일 앞에. 실행 사이의 줄은 SYSTEM 로그에 안 남고 다음 실행 파일 앞에."""
    c, rig, rs, eng = make(tmp_path, cfg={"TSP_COOLDOWN_S": 0.0})
    try:
        rig.chambers["ch1"].pressure_override = 1.0e-7
        eng.start({"target": "", "cycles": "1"})                                # 프리버퍼 줄
        assert eng.start({"target": "2.5e-7", "cycles": "1"})["ok"]
        run_until_idle(rig, eng)
        first = Path(eng.log_file)
        assert first.read_text(encoding="utf-8").splitlines()[0].endswith(
            "[TSP] [ERROR] 입력 파싱 실패: TSP_targetPressure_edit 가 비어있습니다.")
        sysdir = tmp_path / "logs" / "SYSTEM"
        sys_text = "".join(p.read_text(encoding="utf-8") for p in sysdir.glob("*.log"))
        assert "[TSP] [ERROR] 입력 파싱 실패: TSP_targetPressure_edit 가 비어있습니다." in sys_text
        rig.loop.run(2)
        eng.start({"target": "x", "cycles": "1"})                               # 실행 사이 줄
        rig.loop.run(2)
        assert eng.start({"target": "2.5e-7", "cycles": "1"})["ok"]
        run_until_idle(rig, eng)
        second = Path(eng.log_file)
        assert second != first
        lines = second.read_text(encoding="utf-8").splitlines()
        assert lines[0].endswith("[TSP] [ERROR] 입력 파싱 실패: could not convert string to float: 'x'")
        assert "=== TSP 공정 시작 ===" in lines[1]
        sys_text = "".join(p.read_text(encoding="utf-8") for p in sysdir.glob("*.log"))
        assert "could not convert" not in sys_text
        assert "could not convert" not in first.read_text(encoding="utf-8")
    finally:
        rig.stop()


def test_abort_journal_and_snapshot_fields(env):
    """abort(안전 규칙 — v2): 예외 경로 '[ERROR] 실행 실패: RuntimeError('abort: …')'·오류 카드. 저널: 운전 중 resumable False,
    resume_checks 는 ok False(이어가기 안 함 — 상태 유지 블록 없음). snapshot_fields 는 1초 행 열(ENGINE_COLUMNS)만."""
    c, rig, rs, eng = env
    assert eng.resume_checks(eng.state_for_journal()) == [] and not eng.abort("S99")
    rig.chambers["ch1"].pressure_override = 1.0e-5
    assert eng.start({"target": "2.5e-7", "cycles": "2"})["ok"]
    run_until(rig, lambda: eng.state()["phase"] == "tsp_on")
    j = eng.state_for_journal()
    assert j["busy"] and j["phase"] == "tsp_on" and j["resumable"] is False and j["cycles"] == 2
    chk = eng.resume_checks(j)
    assert len(chk) == 1 and chk[0]["name"] == "tsp.phase" and chk[0]["ok"] is False
    f = eng.snapshot_fields()
    assert set(f) <= SF.ENGINE_COLUMNS and f["tsp.proc.block_no"] == 1 and f["tsp.proc.block_count"] == 2
    assert all(TE.PHASES[p]["resumable"] is False for p in TE.PHASES)
    assert eng.abort("S99 RF reflected")
    run_until_idle(rig, eng)
    lg = logs(eng)
    assert lg[lg.index("[TSP] 이상 중단 요청: S99 RF reflected") + 1] == "[TSP] run() 취소됨"
    assert lg[-1] == "[ERROR] 실행 실패: RuntimeError('abort: S99 RF reflected')"
    assert cards(eng)[-1]["detail"] == {"process_name": "TSP", "errors": ["abort: S99 RF reflected"]}
    assert eng.snapshot_fields() == {"tsp.proc.state": "idle"} and rig.busy() is False


def test_v1_exception_shapes():
    """drivers.tsp 결과 → v1 예외 repr(device/tsp.py:51-63·412-431)."""
    D = T.TspDone
    e = TC.v1_exception(D(False, "on", "VERIFY_MISMATCH", "E808", "TspVerifyTimeout('상태 확인 시간초과(target=on, last=None)')"))
    assert repr(e) == "TSPError('상태 확인 시간초과(target=on, last=None)')" and e.code == "E808"
    assert repr(TC.v1_exception(D(False, "on", "DEVICE_REJECTED", "E806", "TspNackError('NACK')"))) == "TSPNackError('NACK')"
    assert repr(TC.v1_exception(D(False, "off", "DEVICE_REJECTED", "E806", "TspWindowError('Unknown Window')"))) == \
        "TSPWindowError('Unknown Window')"
    assert repr(TC.v1_exception(D(False, "on", "DEVICE_NO_RESPONSE", None, "TspNoResponse('응답 없음(부분 -)')"))) == \
        "TimeoutError()"
    assert repr(TC.v1_exception(D(False, "on", "DEVICE_REJECTED", "E807", "TspUnexpectedReply('예상치 못한 응답: RAW 00')"))) \
        == "TSPProtocolError('예상치 못한 응답: RAW 00')"
    assert repr(TC.v1_exception(D(False, "on", "BENCH_READ_ONLY", None, "bench 모드 — TSP 쓰기 금지"))) == \
        "RuntimeError('bench 모드 — TSP 쓰기 금지')"


def test_system_stop_mid_run_turns_off_and_releases(tmp_path):
    """조립 정지(system.stop → engine.close) — 실행 중이면 이상 중단(PROGRAM_EXIT) 후 정리(TSP OFF·SIG 0·게이지 놓기)까지 기다린다.
    (v1 은 앱 종료 정리 목록에 TSP 가 없었다 — main.py:1395-1399, PC-137 비고. v2 는 끝낼 때 끈다.)"""
    c, rig, rs, eng = make(tmp_path)
    rig.chambers["ch1"].pressure_override = 1.0e-5
    assert eng.start({"target": "2.5e-7", "cycles": "3"})["ok"]
    run_until(rig, lambda: "[TSP] ON (cycle 1/3)" in logs(eng))
    rig.loop.run(2)
    rig.stop()
    assert not eng.busy and rig.shared.tsp_ig.owner is None
    assert logs(eng)[-1] == "[ERROR] 실행 실패: RuntimeError('abort: PROGRAM_EXIT')"
    assert tsp_frames(rig)[-2:] == [OFF_FRAME, OFF_FRAME] and not rs.is_running("tsp")
