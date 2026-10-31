"""TSP 매일 예약(v2/ch12/tsp_schedule.py) — 가상 시계 + Ch12Rig 실제 실행. 근거 v1 runtime/tsp_runtime.py:111-135·251-307·
460-505·751-843(PC-143~146). 예약 대기 중에는 1초 tick 을 돌리지 않고 시험 스레드가 시계를 기다린다(엔진은 tick 없이 돈다).
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

from v2.common.clock import VirtualClock
from v2.ch12 import tsp_engine as TE
from v2.ch12.run_state import RunState
from v2.ch12.sim.rig import Ch12Rig

FAST = {"CAMERA_MIN_FREE_GB": 0.0, "TSP_ON_SEC": 12.0, "TSP_OFF_SEC": 15.0, "TSP_POLL_SEC": 10.0}


def clock_at(y, mo, d, h, mi, s=0):
    c = VirtualClock(start_wall=datetime(y, mo, d, h, mi, s).timestamp())
    c.register()
    return c


def make(c, out: Path, cfg=None, settings=None, rs=None):
    rig = Ch12Rig(c, out, cfg=dict(FAST, **(cfg or {})))
    assert rig.start()
    rig.chambers["ch1"].pressure_override = 1.0e-7                  # 첫 판정에서 끝나는 짧은 실행
    rs = rs or RunState(c)
    eng = TE.attach(rig, rs, settings_dir=settings)
    return rig, rs, eng


def logs(eng):
    return [e.detail for e in eng.events if e.kind == "log"]


def wait_wall(c, eng, when: datetime, extra_s: float = 0.0):
    c.wait_until(c.monotonic() + (when.timestamp() - c.wall()) + extra_s)


def wait_idle(c, eng, limit_s=120.0):
    assert c.wait_until(c.monotonic() + limit_s, lambda: not eng.busy)


def test_boot_daily_crosses_midnight_and_repeats(tmp_path):
    """기동 때 TSP_DAILY_ENABLE → 입력창 비면 설정 HH:MM 으로 채우고 매일 예약(PC-143). 23:58 에 00:01 이면 다음날 00:01 —
    그 시각에 화면 입력값으로 시작, '다음 반복 예약' 하루 뒤. 등록 줄은 첫 실행 전이라 SYSTEM 로그와 첫 실행 파일 앞에(PC-129)."""
    c = clock_at(2025, 10, 9, 23, 58, 0)
    rig, rs, eng = make(c, tmp_path, cfg={"TSP_DAILY_HH": 0, "TSP_DAILY_MM": 1})
    try:
        sch = eng.schedule
        assert sch.time_text == "00:01" and sch.checked
        assert logs(eng)[0] == "[TSP] 예약 실행 등록: 2025-10-10 00:01:00 (매일=True)"
        assert sch.text == "Daily 00:01 / Next 10-10"
        wait_wall(c, eng, datetime(2025, 10, 10, 0, 0, 59))
        assert not eng.busy
        # busy 는 '자리 잡기' 순간 먼저 참이 된다(start: 자리 잡기 → 검사 → by·cycles 채움 → 실행 스레드) — 값은 스레드가 생긴 뒤 본다
        assert c.wait_until(c.monotonic() + 5.0, lambda: eng.busy and eng._thread is not None)
        assert eng.by == "schedule" and eng.cycles == 10 and eng.target == 2.5e-07       # 화면 기본값(TSP_UI_DEFAULT_*)
        wait_idle(c, eng)
        lg = logs(eng)
        assert "[TSP] 다음 반복 예약: 2025-10-11 00:01:00" in lg and sch.text == "Daily 00:01 / Next 10-11"
        assert "=== RESULT === ok=True final_P=1.000e-07 cycles_done=0 reason=initial_below_target" in lg
        rig.outputs.writer.flush()
        first = Path(eng.log_file).read_text(encoding="utf-8").splitlines()[0]
        assert first.endswith("[TSP] [TSP] 예약 실행 등록: 2025-10-10 00:01:00 (매일=True)")       # PC-151 접두어 중복
        sys_text = "".join(p.read_text(encoding="utf-8") for p in (tmp_path / "logs" / "SYSTEM").glob("*.log"))
        assert "[TSP] [TSP] 예약 실행 등록: 2025-10-10 00:01:00 (매일=True)" in sys_text
    finally:
        rig.stop()


def test_busy_at_schedule_time_and_blocked_start(tmp_path):
    """그 시각에 CH1 챔버가 돌면 기다리지 않고 '리소스(Chamber/PC/TSP) 사용 중 → 오늘 예약 취소', TSP 실행 중이면 'TSP 사용 중'
    (PC-146). 시작 가드가 거절하면(쿨다운) '⚠ 예약(mm-dd HH:MM) 실행 못함 → 오늘 건너뜀 (사유: …)'. 1회 예약 끝."""
    c = clock_at(2025, 10, 9, 10, 0, 0)
    rig, rs, eng = make(c, tmp_path, cfg={"TSP_DAILY_ENABLE": False})
    try:
        sch = eng.schedule
        assert not sch.checked and sch.text == "No schedule" and sch.time_text == "05:00"
        rs.mark_started("chamber", 1)
        sch.schedule_run_at(datetime(2025, 10, 9, 10, 1, 0), repeat_daily=True)
        wait_wall(c, eng, datetime(2025, 10, 9, 10, 1, 0), 1.0)
        lg = logs(eng)
        assert lg[-2:] == ["[TSP] 예약 시각 도달했지만 리소스(Chamber/PC/TSP) 사용 중 → 오늘 예약 취소",
                           "[TSP] 다음 반복 예약: 2025-10-10 10:01:00"]
        assert not eng.busy and sch.text == "Daily 10:01 / Next 10-10"
        rs.mark_finished("chamber", 1)
        rs._last_finish["chamber"].clear()
        # TSP 실행 중(사이클 도중)에 1회 예약 시각
        rig.chambers["ch1"].pressure_override = 1.0e-5
        assert eng.start({"target": "2.5e-7", "cycles": "3"})["ok"]
        sch.schedule_run_at(datetime(2025, 10, 9, 10, 1, 30), repeat_daily=False)
        assert "[TSP] 예약 실행 등록: 2025-10-09 10:01:30 (매일=False)" in logs(eng)
        wait_wall(c, eng, datetime(2025, 10, 9, 10, 1, 30), 1.0)
        lg = logs(eng)
        i = lg.index("[TSP] 예약 시각 도달했지만 TSP 사용 중 → 오늘 예약 취소")
        assert lg[i + 1] == "[TSP] 1회 예약이므로 더 이상 재시도하지 않습니다." and eng.busy
        assert c.wait_until(c.monotonic() + 2.0, lambda: not sch.checked) and sch.text == "No schedule"
        eng.stop()                                                   # STOP 은 예약을 건드리지 않는다(예약은 이미 끝)
        wait_idle(c, eng)
        # 끝난 직후(쿨다운 60 s 안) 예약 시각 → 시작 가드 거절
        when = (datetime.fromtimestamp(c.wall()) + timedelta(seconds=20)).replace(microsecond=0)
        sch.schedule_run_at(when, repeat_daily=False)
        wait_wall(c, eng, when, 1.0)
        lg = logs(eng)
        msg = [x for x in lg if x.startswith(f"[TSP] ⚠ 예약({when:%m-%d %H:%M}) 실행 못함 → 오늘 건너뜀 (사유: ")]
        assert msg and "이전 공정 종료 후 쿨다운 60초 미경과(잔여 " in msg[0]
        assert lg[-1] == "[TSP] 1회 예약 실행 완료(반복 없음)."
        assert not eng.busy and not sch.checked
    finally:
        rig.stop()


def test_toggle_save_and_restore_after_restart(tmp_path):
    """체크박스(PC-144): 입력 '6시30분' → '06:30' 정규화·매일 예약, 형식 오류면 '예약 시각 형식 오류(예: 05:00) → 기본값 05:00 사용'.
    v2: 예약 상태를 설정 폴더 tsp_schedule.json 에 — 재시작 뒤 복원(지난 시각이면 다음날), 꺼 두었으면 TSP_DAILY_ENABLE 이어도
    예약 없음. 종료(close)는 저장을 바꾸지 않는다."""
    settings = tmp_path / "settings"
    c = clock_at(2025, 10, 9, 6, 0, 0)
    rig, rs, eng = make(c, tmp_path / "a", settings=settings)
    sch = eng.schedule
    assert logs(eng) == ["[TSP] 예약 실행 등록: 2025-10-10 05:00:00 (매일=True)"]           # 저장 없음 → v1 기동
    assert not (settings / "tsp_schedule.json").exists()
    sch.set_time_text("25:99")
    assert sch.toggle(True)
    assert logs(eng)[-2:] == ["예약 시각 형식 오류(예: 05:00) → 기본값 05:00 사용",
                              "[TSP] 예약 실행 등록: 2025-10-10 05:00:00 (매일=True)"]
    sch.set_time_text("6시30분")
    assert sch.toggle(True) and sch.time_text == "06:30" and sch.text == "Daily 06:30 / Next 10-09"
    d = json.loads((settings / "tsp_schedule.json").read_text(encoding="utf-8"))
    assert d == {"tsp/reserved": True, "tsp/reserve_hhmm": "06:30", "tsp/repeat_daily": True}
    rig.stop()                                                       # 프로그램 종료 — 저장 유지
    assert json.loads((settings / "tsp_schedule.json").read_text(encoding="utf-8"))["tsp/reserved"] is True
    c.wait_until(c.monotonic() + 3600.0)                             # 07:00 — 06:30 은 지남(놓친 회차는 따라잡지 않음)
    rig, rs, eng = make(c, tmp_path / "b", settings=settings)
    try:
        sch = eng.schedule
        assert logs(eng) == ["[TSP] 예약 실행 등록: 2025-10-10 06:30:00 (매일=True)"]
        assert any(e.kind == "schedule_restore" and e.detail == "TSP 예약 복원: 매일 06:30" for e in rig.events)
        assert sch.time_text == "06:30" and not eng.busy
        assert not sch.toggle(False) and logs(eng)[-1] == "[TSP] 예약 실행 취소됨" and sch.text == "No schedule"
        assert json.loads((settings / "tsp_schedule.json").read_text(encoding="utf-8"))["tsp/reserved"] is False
    finally:
        rig.stop()
    rig, rs, eng = make(c, tmp_path / "c", settings=settings)        # TSP_DAILY_ENABLE(기본 True)여도 꺼 둔 상태 유지
    try:
        assert not eng.schedule.checked and logs(eng) == []
        assert any(e.kind == "schedule_restore" and e.detail == "TSP 예약 복원: 예약 없음" for e in rig.events)
    finally:
        rig.stop()
