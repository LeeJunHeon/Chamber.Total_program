# -*- coding: utf-8 -*-
"""터보 꺼짐 지속 알림 — lib/turbo_watch.TurboOffWatch 판정, 카드 문구, PLC 코일 로거 경로(가짜 스냅샷으로 실제 루프 구동),
발송기 예외가 로거를 멈추지 않음, settings.json common 적용."""
from __future__ import annotations

import asyncio
import contextlib
import os
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE.parent))
sys.path.insert(0, str(_HERE))
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                    # noqa: E402

from lib import config_common as cfgc                           # noqa: E402
from lib import turbo_watch                                      # noqa: E402
from lib.turbo_watch import TurboOffWatch, card_text, fmt_duration  # noqa: E402

H = 3600.0


def _w(alert=H):
    return TurboOffWatch(alert, wall=lambda: 1_700_000_000.0)


# ═══════════════ 판정 ═══════════════
def test_stays_on_no_event():
    w = _w()
    assert all(w.update(1, True, t) is None for t in range(0, 20000, 60))


def test_off_shorter_than_alert_then_on_no_event():
    w = _w()
    assert w.update(1, True, 0.0) is None
    assert w.update(1, False, 10.0) is None
    assert w.update(1, False, 10.0 + H - 1) is None
    assert w.update(1, True, 10.0 + H - 0.5) is None                 # off_long 전에 켜짐 → 아무것도 없음
    assert w.update(1, False, 20000.0) is None                       # 새 꺼짐 구간은 다시 잰다
    assert w.update(1, False, 20000.0 + H - 1) is None


def test_off_long_once_then_back_on_once():
    w = _w()
    w.update(2, True, 0.0)
    assert w.update(2, False, 100.0) is None
    ev = w.update(2, False, 100.0 + H)
    assert ev["kind"] == "off_long" and ev["ch"] == 2 and ev["off_at"] == 1_700_000_000.0 and ev["alert_s"] == H
    assert all(w.update(2, False, 100.0 + H + k) is None for k in range(1, 5000, 100))    # 한 구간에 한 번만
    ev = w.update(2, True, 100.0 + 2 * H + 5 * 60)
    assert ev["kind"] == "back_on" and ev["off_s"] == pytest.approx(2 * H + 300)
    assert w.update(2, True, 100.0 + 3 * H) is None                  # back_on 도 한 번만
    assert w.update(1, False, 0.0) is None                           # 채널 독립


def test_none_keeps_state_and_timer():
    w = _w()
    w.update(1, True, 0.0)
    w.update(1, False, 10.0)
    for t in range(20, 3000, 30):
        assert w.update(1, None, float(t)) is None                   # 끊김 — 상태·타이머 그대로
    assert w.update(1, None, 10.0 + H) is None                       # None 으로는 이벤트도 내지 않는다
    ev = w.update(1, False, 10.0 + H + 1)
    assert ev["kind"] == "off_long" and ev["off_s"] == pytest.approx(H + 1)    # 꺼진 시각부터 잼(None 구간 포함)
    assert w.update(1, None, 10.0 + 2 * H) is None
    assert w.update(1, True, 10.0 + 2 * H)["kind"] == "back_on"


def test_first_value_off_counts_from_start():
    w = _w()
    assert w.update(1, False, 50.0) is None                          # 시작 뒤 첫 값이 꺼짐 → 그 시각부터
    assert w.update(1, False, 50.0 + H - 1) is None
    assert w.update(1, False, 50.0 + H)["kind"] == "off_long"


@pytest.mark.parametrize("alert", [0.0, -1.0])
def test_disabled_when_zero_or_less(alert):
    w = _w(alert)
    w.update(1, True, 0.0)
    assert all(w.update(1, False, t) is None for t in (1.0, 1e6, 1e7))
    assert w.update(1, True, 2e7) is None


def test_alert_setting_read_each_update():
    cur = {"v": 0.0}
    w = TurboOffWatch(lambda: cur["v"], wall=lambda: 0.0)
    assert w.update(1, False, 0.0) is None                          # 끔
    cur["v"] = 60.0                                                  # Config 팝업으로 켬 → 그때부터 잰다
    assert w.update(1, False, 10.0) is None
    assert w.update(1, False, 70.0)["kind"] == "off_long"


# ═══════════════ 카드 문구 ═══════════════
def test_card_text_follows_setting():
    import time as _t
    at = _t.mktime((2026, 11, 2, 9, 5, 7, 0, 0, -1))
    t, sub, st = card_text({"kind": "off_long", "ch": 1, "off_at": at, "alert_s": H})
    assert (t, st) == ("터보 꺼짐 지속", "FAIL")
    assert sub == "[CH1] 터보 펌프 꺼짐 1시간 경과 | 꺼진 시각 09:05:07 (PLC 기동 출력 기준)"
    assert "꺼짐 30분 경과" in card_text({"kind": "off_long", "ch": 2, "off_at": at, "alert_s": 1800.0})[1]
    assert "꺼짐 1시간 30분 경과" in card_text({"kind": "off_long", "ch": 2, "off_at": at, "alert_s": 5400.0})[1]
    t, sub, st = card_text({"kind": "back_on", "ch": 2, "off_s": 2 * H + 15 * 60})
    assert (t, st, sub) == ("터보 다시 켜짐", "SUCCESS", "[CH2] 터보 펌프 다시 켜짐 | 꺼져 있던 시간 2시간 15분")
    assert (fmt_duration(3600), fmt_duration(59), fmt_duration(61)) == ("1시간", "59초", "1분")


# ═══════════════ PLC 코일 로거 경로 ═══════════════
class _Writer:
    def __init__(self):
        self.rows = []

    def append_row(self, *, dt, header, row):
        self.rows.append((list(header), list(row)))

    def consume_switched_flag(self):
        return False


def _run_logger(snaps, emitter, alert_s=0.05, run_s=0.6):
    import device.plc as PLC
    from test_plc_link_policy import _mk, _reset_cfg
    _reset_cfg()
    p, logs = _mk()
    asyncio.run(p.read_coil(1))                                      # 연결 상태
    p._consec_timeouts = p._consec_timeouts_low = 0
    calls = []
    it = iter(snaps)

    async def _snap(keys=None):
        calls.append(1)
        v = next(it, snaps[-1])
        if isinstance(v, Exception):
            raise v
        return dict(v)
    p.snapshot_all_coils_fast = _snap                                 # type: ignore[assignment]
    p._plc_coil_log_stop = asyncio.Event()
    p._plc_coil_log_interval = 0.01
    p._plc_coil_log_keys = ["TURBO_1_SW", "TURBO_1_인터락", "TURBO_2_SW", "TURBO_2_인터락"]
    p._plc_reg_log_keys = []
    p._plc_coil_csv_writer = _Writer()
    p._note_coil_plan_result = lambda addrs, ok: None                # type: ignore[assignment]
    old = getattr(cfgc, "TURBO_OFF_ALERT_S", None)
    cfgc.TURBO_OFF_ALERT_S = alert_s
    turbo_watch.set_emitter(emitter)

    async def _go():
        t = asyncio.create_task(p._plc_coil_log_loop())
        await asyncio.sleep(run_s)
        p._plc_coil_log_stop.set()
        with contextlib.suppress(Exception):
            await asyncio.wait_for(t, timeout=1.0)
    try:
        asyncio.run(_go())
    finally:
        turbo_watch.set_emitter(None)
        cfgc.TURBO_OFF_ALERT_S = old
        _reset_cfg()
    assert set(PLC.PLC_COIL_MAP) >= set(p._plc_coil_log_keys)
    return p, logs, calls


ON = {"TURBO_1_SW": True, "TURBO_1_인터락": True, "TURBO_2_SW": True, "TURBO_2_인터락": True}


def test_logger_feeds_watch_and_emits_once():
    off1 = dict(ON, TURBO_1_인터락=False)                             # CH1: SW 켜짐 · 인터락 꺼짐 → 기동 출력 꺼짐
    snaps = [ON, off1, RuntimeError("snapshot fail"), {}] + [off1] * 30 + [ON] * 10
    got = []
    p, logs, calls = _run_logger(snaps, lambda kind, info: got.append((kind, info["ch"])), run_s=0.8)
    assert got == [("off_long", 1), ("back_on", 1)], got
    assert len(p._plc_coil_csv_writer.rows) >= 40                     # CSV 행은 그대로 기록
    assert p._plc_coil_csv_writer.rows[0][0] == ["Timestamp", *p._plc_coil_log_keys]
    tl = [m for m in logs if "TURBO WATCH" in m]
    assert len(tl) == 2 and "터보 꺼짐 지속" in tl[0] and "[CH1]" in tl[0] and "터보 다시 켜짐" in tl[1]


def test_emitter_exception_does_not_stop_logger():
    off2 = dict(ON, TURBO_2_SW=False)
    snaps = [off2] * 200

    def boom(kind, info):
        raise RuntimeError("발송 실패")
    p, logs, calls = _run_logger(snaps, boom, alert_s=0.02, run_s=0.5)
    n = len(calls)
    assert n >= 10 and len(p._plc_coil_csv_writer.rows) == n           # 이벤트 뒤에도 계속 돈다
    assert sum("TURBO WATCH" in m for m in logs) == 1


def test_watch_feed_swallows_bad_snapshot():
    import device.plc as PLC
    p = PLC.AsyncPLC.__new__(PLC.AsyncPLC)

    class Bad(dict):
        def get(self, *a, **k):
            raise RuntimeError("bad")
    p._turbo_watch_feed(Bad())                                        # 예외를 올리지 않는다


# ═══════════════ 설정 ═══════════════
def test_settings_json_common_applies_turbo_off_alert():
    from lib import _config_loader
    old = cfgc.TURBO_OFF_ALERT_S
    try:
        assert old == 3600.0
        cfgc.TURBO_OFF_ALERT_S = 1.0
        _config_loader.load_settings()
        assert cfgc.TURBO_OFF_ALERT_S == 3600.0                       # plc.py 가 읽는 config_common 에 common 이 들어간다
    finally:
        cfgc.TURBO_OFF_ALERT_S = old
