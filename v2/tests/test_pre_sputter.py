"""프리스퍼터 예약(pre_sputter.py) — v1 runtime/pre_sputter_runtime.py·main.py:1630-1805 동작(가상 시계).

예약 시각(지난 시각은 내일)·바쁘면 그날 PASS·시작 확인 최대 60 s·실행 동안 사용자 레시피 큐 보관 → 되돌림(아직 바쁘면 버림)·
전용 레시피 경로·예약 상태 저장(v1 QSettings 키 이름 그대로 json)·알려진 이상 동작 UI-143(가드 거절에도 True → 60 s 뒤 실패).
"""
from datetime import datetime, timedelta

from v2.common.clock import VirtualClock
from v2.ch12 import pre_sputter as PS


class FakeEng:
    ch, n = "ch1", 1

    def __init__(self, clock, run_s=30.0, accept=True):
        self.clock, self.run_s, self.accept = clock, run_s, accept
        self.until = -1.0
        self.calls, self.snaps, self.restored, self.lines = [], 0, [], []
        self.loaded = None
        self.is_running = False

    @property
    def busy(self):
        return self.clock.monotonic() < self.until

    def start(self, **kw):
        self.calls.append(kw)
        if self.accept:
            self.until = self.clock.monotonic() + self.run_s
        return None if self.accept else "COOLDOWN"

    def load(self, rows, **kw):
        self.loaded = rows
        return None

    def snapshot_recipe_queue(self):
        self.snaps += 1
        return ("q", 0)

    def restore_recipe_queue(self, s):
        self.restored.append(s)

    def log(self, src, msg):
        self.lines.append(msg)


def _clock():
    c = VirtualClock(start_wall=datetime(2025, 10, 9, 5, 58, 0).timestamp())
    c.register()
    return c


def test_next_time_and_parse():
    now = datetime(2025, 10, 9, 6, 0, 0)
    assert PS.next_time_at(now, 6, 0) == datetime(2025, 10, 10, 6, 0)            # 같은 시각 = 지남 → 내일
    assert PS.next_time_at(now, 6, 1) == datetime(2025, 10, 9, 6, 1)
    assert PS.parse_hhmm("08:30") == (8, 30) and PS.parse_hhmm("8시30분") == (8, 30)
    assert PS.parse_hhmm("25:00") == (None, None) and PS.parse_hhmm("x") == (None, None)


def test_daily_run_isolates_queue_and_reschedules(tmp_path):
    c = _clock()
    e = FakeEng(c)
    p = PS.PreSputter(c, e, settings_dir=tmp_path, ui_params=lambda: {"process_note": "pre"}, hh=6, mm=0)
    p.start_daily()
    assert p.status_text == "예약됨 · 매일 06:00" and p.left_text == "00:02:00"
    assert p.lines[-1].endswith("[PreSputter] CH1 [PreSputter] 예약 등록: 2025-10-09 06:00:00 (매일 반복)")
    c.wait_until(c.monotonic() + 200, lambda: p.status_text.startswith("완료"))
    assert p.status_text == "완료 · 다음 06:00" and e.calls[0]["origin"] == "presputter" and e.calls[0]["params"]
    assert e.snaps == 1 and e.restored == [("q", 0)]
    assert any("UI 현재값으로 실행" in l for l in p.lines) and any(l.endswith("CH1 [PreSputter] CH1 완료") for l in p.lines)
    c.wait_until(c.monotonic() + 5, lambda: any("다음 반복 예약: 2025-10-10 06:00:00" in l for l in p.lines))
    assert p.when == datetime(2025, 10, 10, 6, 0)
    p.stop()
    assert p.status_text == "예약 없음" and p.lines[-1].endswith("예약 취소됨")
    c.close()


def test_busy_at_time_passes_today(tmp_path):
    c = _clock()
    e = FakeEng(c)
    e.until = c.monotonic() + 600                                               # 다른 공정이 돌고 있음
    p = PS.PreSputter(c, e, settings_dir=tmp_path, hh=6, mm=0)
    p.start_daily()
    c.wait_until(c.monotonic() + 200, lambda: p.status_text.startswith("PASS"))
    assert p.status_text == "PASS(공정 중) · 다음 06:00" and not e.calls
    assert any("해당 챔버가 이미 공정 중 → 이번 예약 PASS" in l for l in p.lines)
    p.close()
    c.close()


def test_ui143_guard_reject_waits_60s_then_fails(tmp_path):
    c = _clock()
    e = FakeEng(c, accept=False)                                                # 시작 가드가 거절(쿨다운 등)
    p = PS.PreSputter(c, e, settings_dir=tmp_path, hh=6, mm=0)
    p.start_daily()
    t0 = c.monotonic()
    c.wait_until(c.monotonic() + 400, lambda: p.status_text.startswith("실행 실패"))
    assert c.monotonic() - t0 >= 120 + 60 - 1                                   # 예약 시각 + 60 s 대기
    assert any("CH1 시작 신호 미확인 (timeout)" in l for l in p.lines) and e.restored == [("q", 0)]
    p.close()
    c.close()


def test_recipe_path_runs_file(tmp_path):
    c = _clock()
    e = FakeEng(c, run_s=10)
    f = tmp_path / "pre.csv"
    f.write_text("Process_name,delay\nwarm,1m\n", encoding="utf-8-sig")
    p = PS.PreSputter(c, e, settings_dir=tmp_path, hh=6, mm=0)
    p.set_recipe_path(str(f))
    assert p.recipe_display() == "pre.csv"
    p.run_once_now()
    assert e.loaded == [{"Process_name": "warm", "delay": "1m"}] and e.calls[0]["origin"] == "presputter"
    assert any("레시피 실행: pre.csv" in l for l in p.lines)
    c.close()


def test_settings_saved_with_v1_keys_and_restored(tmp_path):
    c = _clock()
    e = FakeEng(c)
    p = PS.PreSputter(c, e, settings_dir=tmp_path)
    p.set_recipe_path(r"\\NAS\pre.csv")
    assert p.schedule("07:15")
    d = p._read()
    assert d["presputter/recipe_ch1"] == r"\\NAS\pre.csv" and d["presputter/reserved_ch1"] is True
    assert d["presputter/reserve_hhmm_ch1"] == "07:15"
    p.close()
    q = PS.PreSputter(c, e, settings_dir=tmp_path)
    assert q.restore() == "PreSputter CH1 예약 복원: 매일 07:15" and q.recipe_display() == "pre.csv (파일 없음)"
    q.close()
    (tmp_path / PS.SETTINGS_FILE).write_text('{"presputter/reserved_ch1": true, "presputter/reserve_hhmm_ch1": "99:00"}',
                                             encoding="utf-8")
    assert PS.PreSputter(c, e, settings_dir=tmp_path).restore() == "PreSputter CH1 예약 복원 실패: 범위 초과('99:00')"
    c.close()


def test_engine_queue_snapshot_restore_and_discard_when_busy(tmp_path):
    from v2.tests import proc_rig as R
    rig = R.make(tmp_path, chambers=("ch1",))
    e = rig.eng["ch1"]
    e.load([{"Process_name": "a", "delay": "1m"}, {"Process_name": "b", "delay": "2m"}], source="local")
    snap = e.snapshot_recipe_queue()
    assert e.rows is None and e.runs == []
    e.restore_recipe_queue(snap)
    assert len(e.rows) == 2 and R.has_log(e, "[CH1] 레시피 큐 2건 복구 완료")
    snap = e.snapshot_recipe_queue()
    e._cmd = ("START", None)                                                    # 바쁨(프리플라이트 중 등)
    e.restore_recipe_queue(snap)
    e._cmd = None
    assert e.rows is None and R.has_log(e, "보관분 2건은 파기됨")
    e.close()
    rig.outputs.stop(1.0)
    rig.clock.close()


def test_round_exception_keeps_daily_reservation(tmp_path):
    """TEST-126: 회차 실행 예외 → '실행 중 예외: … — 이번 회차 실패, 다음 예약 유지', 상태 '실행 오류 · 다음 06:00', 하루 뒤 예약."""
    c = _clock()
    e = FakeEng(c)
    e.snapshot_recipe_queue = lambda: (_ for _ in ()).throw(RuntimeError("boom"))
    p = PS.PreSputter(c, e, settings_dir=tmp_path, hh=6, mm=0)
    p.start_daily()
    c.wait_until(c.monotonic() + 200, lambda: any("다음 반복 예약" in l for l in p.lines))
    assert any("[PreSputter] 실행 중 예외: RuntimeError('boom') — 이번 회차 실패, 다음 예약 유지" in l for l in p.lines)
    assert p.status_text == "실행 오류 · 다음 06:00" and p.is_reserved
    p.close()
    c.close()
