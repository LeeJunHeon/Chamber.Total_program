"""터보 꺼짐 지속 알림(v1 main a6e8d46, tests/test_turbo_watch.py 와 같은 판정) — 판정기와 1초 행 연결·이벤트 카드."""
from datetime import datetime

from v2.common.clock import VirtualClock
from v2.ch12.outputs.row_feed import RowFeed
from v2.ch12.turbo_watch import TurboOffWatch, card_text, fmt_duration


def test_fmt_duration():
    assert [fmt_duration(x) for x in (5, 60, 3600, 3660, 7200)] == ["5초", "1분", "1시간", "1시간 1분", "2시간"]


def test_off_long_once_then_back_on():
    w = TurboOffWatch(100.0, wall=lambda: 1000.0)
    assert w.update(1, True, 0.0) is None
    assert w.update(1, False, 10.0) is None
    assert w.update(1, None, 50.0) is None                       # 읽기 실패 — 타이머 그대로
    assert w.update(1, False, 109.0) is None
    ev = w.update(1, False, 110.0)
    assert ev["kind"] == "off_long" and ev["ch"] == 1 and ev["off_at"] == 1000.0 and ev["off_s"] == 100.0
    assert w.update(1, False, 500.0) is None                     # 같은 꺼짐 구간에서 한 번만
    back = w.update(1, True, 600.0)
    assert back["kind"] == "back_on" and back["off_s"] == 590.0
    assert w.update(1, True, 700.0) is None


def test_short_off_is_silent_and_first_value_off_counts_and_disabled():
    w = TurboOffWatch(100.0)
    assert w.update(2, False, 0.0) is None and w.update(2, True, 50.0) is None      # off_long 전에 켜짐 — 아무것도 없음
    assert w.update(2, False, 60.0) is None
    assert w.update(2, False, 160.0)["kind"] == "off_long"        # 시작 뒤 첫 값 꺼짐부터도 센다(채널별)
    off = TurboOffWatch(0.0)
    assert off.update(1, False, 0.0) is None and off.update(1, False, 1e6) is None


def test_card_text():
    t, sub, st = card_text({"kind": "off_long", "ch": 2, "off_at": datetime(2026, 1, 1, 9, 30, 5).timestamp(), "alert_s": 3600})
    assert (t, st) == ("터보 꺼짐 지속", "FAIL") and sub == "[CH2] 터보 펌프 꺼짐 1시간 경과 | 꺼진 시각 09:30:05 (PLC 기동 출력 기준)"
    t, sub, st = card_text({"kind": "back_on", "ch": 1, "off_s": 5400})
    assert (t, sub, st) == ("터보 다시 켜짐", "[CH1] 터보 펌프 다시 켜짐 | 꺼져 있던 시간 1시간 30분", "SUCCESS")


def test_row_feed_emits_from_one_second_rows(tmp_path):
    from v2.common.paramdef import defaults
    from v2.ch12.params import PARAMS
    cfg = dict(defaults(PARAMS), TURBO_OFF_ALERT_S=3.0, LOG_ROOT_DIR=str(tmp_path), PLC_COIL_LOG_NAS_DIR=str(tmp_path),
               PLC_COIL_LOG_LOCAL_DIR=str(tmp_path), LOCAL_FALLBACK_ROOT=str(tmp_path))
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    got = []
    f = RowFeed(cfg, c, plc_connected=lambda: True, on_turbo=lambda k, t, ev: got.append((k, ev["ch"])))

    def row(sw, il, q="ok"):
        return {"ts": datetime.fromtimestamp(c.wall()).isoformat(), "plc.q": q,
                "plc.turbo_1_sw": sw, "plc.turbo_1_인터락": il, "plc.turbo_2_sw": True, "plc.turbo_2_인터락": True}
    for sw, il, q in ((True, True, "ok"), (True, False, "ok"), (True, False, "fail"), (True, False, "ok"), (True, False, "ok"),
                      (True, False, "ok"), (True, False, "ok"), (True, True, "ok")):
        f.on_row(row(sw, il, q))
        c.wait_until(c.monotonic() + 1.0)
    assert got == [("off_long", 1), ("back_on", 1)]               # SW and 인터락 — 인터락이 꺼지면 꺼짐, 실패 행은 건너뜀
    f.stop()
