"""Z — 영점 대기 표시(사용자 지시 2026-11-27, 챔버K W-23 state.run.waitText 와 같은 뜻).

영점 조건(유량 꺼짐·MFC 밸브 열림이 ZERO_MIN_IDLE_SEC · PLC 가스 밸브 닫힘)을 기다리는 동안 그 챔버 상태에 '영점 대기 n s'(기다린 초):
1초 행 <c>.proc.wait_text · 엔진 화면 상태 wait_text · 웹 state chambers.<c>.waitText. 기다림이 끝나면(보냄·건너뜀) 바로, STOP(대기열
폐기)이면 기다리던 명령이 깨기 전에도 바로 지운다. 기다리지 않는 공정은 늘 비어 있다(지금과 같아 보임).
"""
from v2.tests import proc_rig as R
from v2.tests.test_zero_guard import _bus_run, _rig


# ── 드라이버(가상 시계 MFC 시험대) ──
def test_driver_wait_text_counts_up_then_clears_when_sent():
    r = _rig()
    _bus_run(r, 15)
    t_open = r.c.monotonic() + 6.0
    r.d.zero_plc = lambda: (True, "") if r.c.monotonic() >= t_open else (False, "Ar 밸브")
    assert r.d.zero_wait_text() is None
    box, seen = [], []
    t0 = r.c.monotonic()
    r.d.ps_zeroing(on_done=lambda k, rs, t: box.append(rs))
    for k in range(1, 6):
        r.c.wait_until(t0 + k + 0.2)
        seen.append(r.d.zero_wait_text())
    assert seen == [f"영점 대기 {k} s" for k in range(1, 6)], seen
    r.c.wait_until(t0 + 9.0)
    assert box == [None] and "Z1" in r.sent(t0) and r.d.zero_wait_text() is None and r.d.zero_wait is None
    r.done()


def test_driver_wait_text_clears_on_skip():
    r = _rig(ZERO_WAIT_MAX_SEC=3.0)
    _bus_run(r, 15)
    r.d.zero_plc = lambda: (False, "Ar 밸브")
    box = []
    t0 = r.c.monotonic()
    r.d.ps_zeroing(on_done=lambda k, rs, t: box.append(rs))
    r.c.wait_until(t0 + 1.5)
    assert r.d.zero_wait_text() == "영점 대기 1 s"
    r.c.wait_until(t0 + 5.0)
    assert box == [None] and "Z1" not in r.sent(t0) and r.d.zero_wait_text() is None      # 한도 넘겨 건너뜀 — 지움
    r.done()


def test_driver_wait_text_clears_on_stop_before_the_waiting_command_wakes():
    r = _rig(ZERO_MIN_IDLE_SEC=60.0)                       # 시간만 모자람 — 남은 약 45 s 를 한 번에 잔다
    _bus_run(r, 15)
    box = []
    t0 = r.c.monotonic()
    r.d.ps_zeroing(on_done=lambda k, rs, t: box.append(rs))
    r.c.wait_until(t0 + 2.3)
    assert r.d.zero_wait_text() == "영점 대기 2 s" and not box
    r.d.on_process_finished(False, reason="STOP")          # 정지 — 대기열 폐기(MFC worker 에서 곧바로)
    r.c.wait_until(t0 + 2.5)
    assert r.d.zero_wait_text() is None and not box        # 기다리던 명령은 아직 자는 중 — 글은 이미 지움
    r.c.wait_until(t0 + 50.0)
    assert box == ["PURGED"] and "Z1" not in r.sent(t0) and r.d.zero_wait is None
    r.done()


def test_driver_no_wait_never_sets_text():
    r = _rig()
    _bus_run(r, 15)
    marks = []
    orig = type(r.d).zero_check

    def spy(self, now):
        marks.append(self.zero_wait)
        return orig(self, now)
    type(r.d).zero_check = spy
    try:
        assert r.call(r.d.ps_zeroing)[1] is None and r.call(r.d.mfc_zeroing, 1)[1] is None
    finally:
        type(r.d).zero_check = orig
    assert marks and all(m is None for m in marks) and r.d.zero_wait_text() is None
    r.done()


# ── 공정(엔진·1초 행·웹 state) ──
P2 = dict(base_pressure=7e-6, working_pressure=5, process_time=0.1, shutter_delay=0.2, use_ar=True, ar_flow=20,
          use_rf_power=True, rf_power=100, use_ms=True, use_g1=True, process_note="zero-wait")


def _watch(rig, e, out):
    def hook(_t):
        row = e.snapshot_fields()
        out.append((e.runner.is_running, e.step_label, row.get("ch2.proc.wait_text"), e.state_dict()["wait_text"],
                    "ch2.proc.wait_text" in row))
    return hook


def test_process_that_waits_shows_wait_text_next_to_step_then_clears(tmp_path):
    rig = R.make(tmp_path, cfg={"ZERO_MIN_IDLE_SEC": 40.0}, chambers=("ch2",))
    e = rig.eng["ch2"]
    seen = []
    R.run(rig, R.idle(e), max_ticks=3000, hooks=[R.at(2, lambda: e.start(params=dict(P2))), _watch(rig, e, seen)])
    texts = [w for _r, _l, w, _s, _k in seen if w]
    assert texts and texts[0].startswith("영점 대기 ") and texts[0].endswith(" s"), texts[:3]
    assert all(w == s for _r, _l, w, s, _k in seen)                                   # 1초 행 = 엔진 화면 상태
    n = [int(t.split()[2]) for t in texts]
    assert n == sorted(n) and n[-1] - n[0] >= 20                                     # 기다린 초가 올라간다
    assert all(lab for r, lab, w, _s, _k in seen if w)                               # 단계 표시와 같이(단계 이름이 있음)
    i_last = max(i for i, x in enumerate(seen) if x[2])
    assert seen[i_last + 1][2] is None                                               # 기다림이 끝난 다음 행에서 바로 비움
    assert R.ends(e)[-1][0] == "done"
    assert e.state_dict()["wait_text"] is None


def test_stop_while_waiting_clears_at_once(tmp_path):
    rig = R.make(tmp_path, cfg={"ZERO_MIN_IDLE_SEC": 40.0}, chambers=("ch2",))
    e = rig.eng["ch2"]
    seen, mark = [], {}

    def waiting():
        return bool(e.snapshot_fields().get("ch2.proc.wait_text")) and int(e.wait_text().split()[2]) >= 5

    def stop():
        e.stop()
        mark["after"] = (e.wait_text(), e.snapshot_fields().get("ch2.proc.wait_text"))
    R.run(rig, R.idle(e), max_ticks=3000, hooks=[R.at(2, lambda: e.start(params=dict(P2))), R.when(waiting, stop),
                                                  _watch(rig, e, seen)])
    assert mark["after"] == (None, None)
    i = next(i for i, x in enumerate(seen) if x[2])
    assert not any(x[2] for x in seen[i + 7:])


def test_process_that_does_not_wait_looks_the_same(tmp_path):
    rig = R.make(tmp_path, chambers=("ch2",))
    e = rig.eng["ch2"]
    seen = []
    R.run(rig, R.idle(e), max_ticks=3000, hooks=[R.at(2, lambda: e.start(params=dict(P2))), _watch(rig, e, seen)])
    assert R.ends(e)[-1][0] == "done"
    assert seen and not any(w or s for _r, _l, w, s, _k in seen)
    assert not [x for x in e.events if x.kind == "zero_wait"] and not [x for x in rig.events if getattr(x, "kind", "") == "zero_wait"]


def test_hub_chamber_state_carries_wait_text():
    from v2.ch12.hub import MessageHub as Hub
    st = Hub.chamber_state(None, "ch2", {"ch2.proc.state": "RUNNING", "ch2.proc.step_label": "MFC 영점",
                                         "ch2.proc.wait_text": "영점 대기 3 s"})
    assert st["waitText"] == "영점 대기 3 s" and st["stepLabel"] == "MFC 영점"
    assert Hub.chamber_state(None, "ch2", {"ch2.proc.state": "IDLE"})["waitText"] is None
