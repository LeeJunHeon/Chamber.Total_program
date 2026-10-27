# -*- coding: utf-8 -*-
"""ui/thickness_panel.py · ui/thickness_factor_dialog.py · main._layout_side_panels — 오프스크린.
pytest / 직접 실행 모두 가능."""
import contextlib
import json
import os
import sys
import tempfile
from pathlib import Path

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                    # noqa: E402
from PySide6.QtCore import QRect                                 # noqa: E402
from PySide6.QtWidgets import QApplication, QFrame, QTableWidgetItem, QWidget  # noqa: E402

from lib.thickness_config import ThicknessConfig                 # noqa: E402
from ui.thickness_factor_dialog import ThicknessFactorDialog     # noqa: E402
from ui.thickness_panel import PANEL_H, PANEL_W, ThicknessPanel  # noqa: E402

_APP = QApplication.instance() or QApplication(sys.argv)


class FakeSvc:
    def __init__(self):
        self.reloaded = 0

    def reload_factors(self):
        self.reloaded += 1
        return []

    async def zero(self, mode="B", why=""):
        return True, ""


_OK = {"connected": True, "stale": False, "thickness_nm": 123.44, "rate_nm_s": 0.123, "rate_a_s": 1.23,
       "freq_hz": 5912345, "life_pct": 12.4, "crystal_ok": True, "density": 2.7, "z_factor": 1.08,
       "tooling": 100.0, "reset_seen": 0, "last_error": "", "process_running": False}


def _panel():
    cfg = ThicknessConfig(enabled=True, ch=2, port=4008)
    return ThicknessPanel(FakeSvc(), cfg)


def test_panel_initial_and_values():
    p = _panel()
    assert p.width() == PANEL_W and p.height() == PANEL_H
    assert p.title.text() == "CH2 두께 모니터"
    assert p.thk_label.text() == "---" and not p.zero_button.isEnabled()
    assert p.lamps["conn"].text() == "끊김" and p.error_label.text() == "연결 대기"
    assert p.progress.format() == "대기" and p.mode_label.text() == "종료 방식 대기"
    p.update_status(_OK)
    assert p.thk_label.text() == "123.4"
    assert p.rate_label.text() == "0.123 nm/s (1.2 Å/s)"
    assert p.freq_label.text() == "주파수 5,912,345 Hz" and p.life_label.text() == "수명 12.4 %"
    assert p.material_label.text() == "적용 ρ 2.7 · Z 1.080 · T 100.0%"
    assert p.lamps["conn"].text() == "연결" and p.lamps["xtal"].text() == "XTAL" and p.lamps["resp"].text() == "응답"
    assert "#2e7d32" in p.lamps["resp"].styleSheet()
    assert p.zero_button.isEnabled()


def test_panel_stale_xtal_fail_process_and_reset():
    p = _panel()
    p.update_status(dict(_OK, crystal_ok=False))
    assert p.lamps["xtal"].text() == "XTAL FAIL" and "#c62828" in p.lamps["xtal"].styleSheet()
    p.update_status(dict(_OK, stale=True, age_s=12.0, crystal_ok=False, last_error="응답 없음 — 응답 없음 'S' (3회)"))
    assert p.lamps["xtal"].text() == "XTAL" and "#9aa3ab" in p.lamps["xtal"].styleSheet()   # stale → 회색
    assert p.lamps["resp"].text() == "응답 없음" and "#9aa3ab" in p.lamps["resp"].styleSheet()
    assert "#9aa3ab" in p.thk_label.styleSheet() and p.thk_label.text() == "123.4"          # 마지막 값, 회색
    assert not p.zero_button.isEnabled() and p.error_label.toolTip() == "응답 없음 — 응답 없음 'S' (3회)"
    p.update_status(dict(_OK, process_running=True))
    assert not p.zero_button.isEnabled() and "공정 중" in p.zero_button.toolTip()
    p.update_status(dict(_OK, reset_seen=2))
    assert p.reset_label.text() == "리셋 2회"


def test_panel_style_only_on_change():
    p = _panel()
    p.update_status(_OK)
    calls = []
    orig = p.thk_label.setStyleSheet
    p.thk_label.setStyleSheet = lambda css: (calls.append(css), orig(css))
    p.update_status(dict(_OK, thickness_nm=200.0))
    p.update_status(dict(_OK, thickness_nm=201.0))
    assert calls == []                                            # 값만 바뀌면 스타일은 다시 넣지 않는다
    p.update_status(dict(_OK, stale=True))
    assert len(calls) == 1


def test_process_view_placeholder():
    p = _panel()
    p.set_process_view("Thickness", "100.0 nm", 42.4, "두께 42.4 / 100.0 nm")
    assert p.progress.value() == 42 and p.mode_label.text() == "종료 방식 Thickness"
    p.set_process_view()
    assert p.progress.format() == "대기" and p.proc_text.text() == "대기"


def test_factor_dialog_validate_and_save():
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "한글" / "thickness_factors_ch2.json"
        saved = []
        dlg = ThicknessFactorDialog(str(path), ch=2, on_saved=lambda: saved.append(1))
        dlg.tool_table.setItem(0, 0, QTableWidgetItem("120"))
        dlg.tool_table.setItem(1, 1, QTableWidgetItem("5"))           # 범위 밖
        dlg.mat_table.setItem(0, 0, QTableWidgetItem("2.7"))
        dlg.mat_table.setItem(0, 1, QTableWidgetItem("x"))            # 숫자 아님
        dlg.mat_table.setItem(0, 2, QTableWidgetItem("Al"))
        fac, errs = dlg.collect()
        assert len(errs) == 2 and fac.tooling["up"]["G1"] == 120.0 and fac.tooling["mid"]["G2"] is None
        assert dlg.tool_table.item(1, 1).background().color().name() == "#ffcdd2"
        dlg.tool_table.setItem(1, 1, QTableWidgetItem(""))
        dlg.mat_table.setItem(0, 1, QTableWidgetItem("1.08"))
        dlg._on_save()
        assert saved == [1] and path.exists()
        data = json.loads(path.read_text(encoding="utf-8"))
        assert data["tooling"]["up"]["G1"] == 120.0 and data["tooling"]["mid"]["G2"] is None
        assert data["material"]["G1"] == {"density": 2.7, "z_factor": 1.08, "memo": "Al"}
        dlg2 = ThicknessFactorDialog(str(path), ch=2)                # 다시 열면 저장한 값
        assert dlg2.tool_table.item(0, 0).text() == "120" and dlg2.mat_table.item(0, 2).text() == "Al"


def test_layout_side_panels():
    import main

    class Win:
        def __init__(self, heater, thick):
            self.root = QWidget()
            self.heater_panel = None
            if heater:
                self.heater_panel = QFrame(self.root)
                self.heater_panel.setGeometry(QRect(1086, 10, 240, 340))
            self.thickness_panels = {2: ThicknessPanel(FakeSvc(), ThicknessConfig(ch=2), self.root)} if thick else {}
            self.sizes = []

        def resize(self, w, h):
            self.sizes.append((w, h))

        def _broadcast_log(self, *_a):
            raise AssertionError(_a)

    w = Win(True, True)
    main.MainWindow._layout_side_panels(w)
    assert w.thickness_panels[2].geometry() == QRect(1086, 10 + 340 + 10, PANEL_W, PANEL_H)
    assert w.sizes == [(1086 + 240 + 12, 1010)]
    w = Win(True, False)                                           # 히터만 — _init_ch2_heater 의 크기 그대로(다시 안 바꿈)
    main.MainWindow._layout_side_panels(w)
    assert w.sizes == []
    w = Win(False, True)                                           # 두께만
    main.MainWindow._layout_side_panels(w)
    assert w.thickness_panels[2].geometry().topLeft().y() == 10 and w.sizes == [(1086 + PANEL_W + 12, 1010)]
    w = Win(False, False)                                          # 둘 다 없으면 창 크기 그대로
    main.MainWindow._layout_side_panels(w)
    assert w.sizes == []


def test_init_thickness_channels_and_unreachable_monitor(monkeypatch):
    """main._init_thickness: CH1(SUPPORTS_THICKNESS=false)은 아무것도 만들지 않고, CH2 는 서비스+패널.
    모니터가 없어도(아무도 듣지 않는 포트) 예외 없이 시작·종료되고 공정 로그(append_log)는 쓰지 않는다."""
    import asyncio
    import socket
    import types
    import main

    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    with tempfile.TemporaryDirectory() as tmp:
        monkeypatch.setattr(main, "config_ch1", types.SimpleNamespace(SUPPORTS_THICKNESS=False))
        monkeypatch.setattr(main, "config_ch2", types.SimpleNamespace(
            SUPPORTS_THICKNESS=True, THICKNESS_TCP_HOST="127.0.0.1", THICKNESS_TCP_PORT=port,
            THICKNESS_LOG_DIR=str(Path(tmp) / "logs"), THICKNESS_FACTOR_FILE=str(Path(tmp) / "f.json")))

        class RT:
            def __init__(self):
                self.side, self.proc = [], []
                self.is_running = self.is_busy = False
                self.process_controller = types.SimpleNamespace(current_params={})

            def append_side_log(self, src, msg):
                self.side.append(msg)

            def append_log(self, src, msg):
                self.proc.append(msg)

        loop = asyncio.new_event_loop()
        win = types.SimpleNamespace(ch1=RT(), ch2=RT(), _loop=loop, heater_panel=None,
                                    thickness_services={}, thickness_panels={}, sizes=[], root=QWidget())
        win._broadcast_log = lambda *a: (_ for _ in ()).throw(AssertionError(a))
        win.resize = lambda w, h: win.sizes.append((w, h))
        import ui.thickness_panel as tp_mod
        real = tp_mod.ThicknessPanel
        monkeypatch.setattr(tp_mod, "ThicknessPanel", lambda svc, cfg, parent=None: real(svc, cfg, win.root))
        main.MainWindow._init_thickness(win, 1)
        main.MainWindow._init_thickness(win, 2)
        main.MainWindow._layout_side_panels(win)
        assert list(win.thickness_services) == [2] and list(win.thickness_panels) == [2]
        assert win.ch1.side == [] and win.ch1.proc == []
        assert win.sizes == [(1086 + PANEL_W + 12, 1010)]
        svc = win.thickness_services[2]

        async def run():
            await asyncio.sleep(1.5)
            st = svc.status()
            assert not st["connected"] and st["stale"] and st["seq"] == 0
            await svc.shutdown(timeout=1.0)
        loop.run_until_complete(run())
        loop.close()
        assert win.ch2.proc == []                                  # 공정 로그·프리스타트 버퍼에는 아무것도
        assert any("서비스 시작" in m for m in win.ch2.side)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
