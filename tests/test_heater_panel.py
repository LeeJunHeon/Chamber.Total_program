# -*- coding: utf-8 -*-
"""ui/heater_panel.py — 상태별 표시(offscreen). pytest / 직접 실행 모두 가능."""
import os
import sys
import contextlib

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                   # noqa: E402
from PySide6.QtWidgets import QApplication                      # noqa: E402
from lib.heater_config import HeaterConfig                      # noqa: E402
from ui.heater_panel import HeaterPanel, PANEL_W                # noqa: E402

_APP = QApplication.instance() or QApplication(sys.argv)


class FakeSvc:
    def __init__(self):
        self.owner = None
        self.ramp = (False, None, 0.0, None)
        self.hold = {"holding": False, "value": None}

    def ramp_info(self):
        return self.ramp

    def hold_snapshot(self):
        return dict(self.hold)


def _st(**kw):
    d = {"ok": True, "stale": False, "age_s": 0.0, "pv": 250.0, "sv": 300.0, "cur_sv": 260.0,
         "mv": 470, "mv_limit": 520, "run": False, "itl": True, "fault": False,
         "ot": False, "tc_err": False, "wd_err": False, "tc_ok": True, "da_ok": True,
         "water_ok": True, "vac_ok": True, "sv_limit": 500.0}
    d.update(kw)
    return d


def _mk():
    svc = FakeSvc()
    p = HeaterPanel(svc, HeaterConfig(enabled=True))
    return p, svc


def test_create_and_width():
    p, _ = _mk()
    assert p.width() == PANEL_W == 240
    assert 300 <= p.height() <= 420
    assert p.status_label.text().startswith("PLC 응답 없음")    # 처음엔 stale


def test_stop_view():
    p, _ = _mk()
    p.update_status(_st())
    assert p.badge.text() == "STOP" and p.status_label.text() == "정지"
    assert p.onoff_button.text() == "ON" and not p.onoff_button.isChecked()
    assert p.onoff_button.isEnabled() and p.apply_button.isEnabled()
    assert not p.reset_button.isVisibleTo(p)
    assert p.sv_label.text() == "300.0"                          # 정지 중엔 D112
    assert p.mv_bar.format() == "MV 470/520"


def test_fault_shows_reset():
    p, _ = _mk()
    p.update_status(_st(fault=True, ot=True, itl=False))
    assert p.reset_button.isVisibleTo(p)
    assert p.status_label.text() == "과온 트립" and p.badge.text() == "FAULT"


def test_stale_disables_buttons():
    p, _ = _mk()
    p.update_status(_st(stale=True, age_s=12.0, fault=True))
    assert not p.onoff_button.isEnabled() and not p.apply_button.isEnabled()
    assert not p.reset_button.isEnabled()


def test_run_shows_off_and_ramp_line():
    p, svc = _mk()
    svc.ramp = (True, 300.0, 12.0, 4.5)
    p.update_status(_st(run=True))
    assert p.onoff_button.text() == "OFF" and p.onoff_button.isChecked()
    assert p.badge.text() == "RUN"
    assert p.sv_label.text() == "260.0"                          # 운전 중엔 D116
    assert p.state_label.text().startswith("램프 12°C/min → 300.0°C")
    assert p.dev_label.text() == "Δ-10.0"
    svc.ramp = (False, None, 0.0, None)
    svc.hold = {"holding": True, "value": 470}
    p.update_status(_st(run=True))
    assert p.state_label.text() == "유지 · DAC 상한 470 고정"
    assert p.status_label.text() == "운전 중 · 유지"


def test_owner_process_blocks_on_apply():
    p, svc = _mk()
    svc.owner = "process"
    p.update_status(_st())
    assert not p.onoff_button.isEnabled() and not p.apply_button.isEnabled()
    assert p.onoff_button.toolTip() == "공정이 히터를 제어 중"
    assert p.owner_label.text() == "공정"


def test_interlock_text():
    p, _ = _mk()
    p.update_status(_st(itl=False, tc_ok=False))
    assert p.status_label.text() == "인터락 — TC 꺼짐" and p.badge.text() == "ITL"


def test_lamps_two_rows_inside_panel():
    """램프 8개(4열 × 2줄) — 윗줄 인터락 조건(TC·D/A·냉각수·진공), 아랫줄 결과·이상. 패널 안, 서로 겹치지 않음."""
    p, _ = _mk()
    assert list(p.lamps) == ["tc_ok", "da_ok", "water_ok", "vac_ok", "itl", "ot", "tc_err", "wd_err"]
    rects = [w.geometry() for w, _kind in p.lamps.values()]
    assert {r.top() for r in rects[:4]} == {170} and {r.top() for r in rects[4:]} == {194}
    assert all(r.left() >= 10 and r.right() <= 230 and r.bottom() < p.target_edit.geometry().top() for r in rects)
    assert all(not a.intersects(b) for i, a in enumerate(rects) for b in rects[i + 1:])


def test_interlock_water_vacuum_view():
    p, _ = _mk()
    p.update_status(_st(itl=False, vac_ok=False))
    assert p.status_label.text() == "인터락 — 진공(메인밸브 인터락) 꺼짐" and p.badge.text() == "ITL"
    assert "color: #2e7d32" in p.lamps["water_ok"][0].styleSheet()      # 켜진 조건: 초록
    assert "color: #9aa3ab" in p.lamps["vac_ok"][0].styleSheet()        # 빠진 조건: 회색
    p.update_status(_st(itl=False, water_ok=False, vac_ok=False))
    assert p.status_label.text() == "인터락 — 냉각수, 진공(메인밸브 인터락) 꺼짐"


def test_click_toggle_restored_to_run_view():
    """버튼은 RUN 의 표시 — 클릭으로 바뀐 체크 상태는 즉시 RUN 에 맞춰 되돌린다(목표 없음 → 경고만)."""
    p, svc = _mk()
    p.update_status(_st())
    p.onoff_button.click()
    assert not p.onoff_button.isChecked() and p.onoff_button.text() == "ON"



def test_disabled_button_style():
    p, _ = _mk()
    css = p.onoff_button.styleSheet()
    assert "QPushButton:disabled {background: #f3f4f6; color: #9aa3ab; border: 1px solid #e0e3e7;}" in css
    assert "QPushButton:checked:disabled {background: #c8e6c9; color: #6b7280;}" in css


def test_stale_badge_and_delta_grey():
    p, _ = _mk()
    p.update_status(_st(run=True, stale=True, age_s=15.0))
    assert p.badge.text() == "RUN"                                # 글자는 그대로
    assert "color: #9aa3ab" in p.badge.styleSheet() and "background: #eef1f4" in p.badge.styleSheet()
    assert "color: #9aa3ab" in p.dev_label.styleSheet()
    p.update_status(_st(run=True))
    assert "color: #2e7d32" in p.badge.styleSheet()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
