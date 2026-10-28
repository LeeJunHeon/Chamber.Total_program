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

    def find_material(self, name):
        from lib.thickness_materials import Material, find_material
        return find_material([Material("Aluminum Oxide", "Al2O3", 3.97, 1.0)], name)

    def reload_factors(self):
        self.reloaded += 1
        return []

    async def zero(self, mode="B", why=""):
        return True, ""


_OK = {"connected": True, "stale": False, "thickness_nm": 123.44, "rate_nm_s": 0.123,
       "freq_hz": 5912345, "life_pct": 12.4, "crystal_ok": True, "density": 2.7, "z_factor": 1.08,
       "tooling": 100.0, "reset_seen": 0, "last_error": "", "process_running": False}


def _one_line(t):
    """적용값 줄 — 폭에 따라 두 줄로 나뉜 것을 한 줄로 되돌려 비교(글꼴 폭은 플랫폼마다 다름)."""
    return t.replace("\nTooling", " · Tooling").replace("\n", " · ")


def _panel():
    cfg = ThicknessConfig(enabled=True, ch=2, port=4008)
    return ThicknessPanel(FakeSvc(), cfg)


def test_panel_initial_and_values():
    p = _panel()
    assert p.width() == PANEL_W and p.height() == PANEL_H
    assert p.title.text() == "CH2 Thickness Monitor"
    assert p.thk_label.text() == "---" and p.rate_label.text() == "--- nm/s"   # 값 없음도 단위 표시
    assert not p.zero_button.isEnabled() and p.factor_button.isEnabled()
    assert p.lamps["conn"].text() == "끊김" and p.error_label.text() == ""     # 연결 상태는 램프로만
    # 공정 없음 — 한 번만, 막대는 비우고 글자 없음
    texts = [p.mode_label.text(), p.target_label.text(), p.proc_text.text()]
    assert texts.count("공정 없음") == 1 and texts == ["공정 없음", "", ""]
    assert p.progress.value() == 0 and not p.progress.isTextVisible()
    assert _one_line(p.material_label.text()) == "Density --- · Z --- · Tooling --- %" and p.material_label.wordWrap()
    p.update_status(_OK)
    assert p.thk_label.text() == "123.4"
    assert p.rate_label.text() == "0.123 nm/s"                          # nm 만(Å/s 병기 없음)
    assert p.freq_label.text() == "주파수 5,912,345 Hz" and p.life_label.text() == "수명 12.4 %"
    assert _one_line(p.material_label.text()) == "Density 2.7 · Z 1.080 · Tooling 100 %"
    assert p.lamps["conn"].text() == "연결" and p.lamps["xtal"].text() == "XTAL" and p.lamps["resp"].text() == "응답"
    assert "#2e7d32" in p.lamps["resp"].styleSheet()
    assert p.zero_button.isEnabled() and p.error_label.text() == ""


def test_error_line_only_when_connected_without_response():
    p = _panel()
    p.update_status(dict(_OK, last_error="10:00:01 체크섬 오류 — x"))
    assert p.error_label.text() == ""                                   # 연결·정상이면 비움(지나간 오류는 표시 안 함)
    p.update_status(dict(_OK, stale=True, age_s=12.0, last_error="10:00:01 응답 없음 — S"))
    assert p.error_label.toolTip() == "응답 없음 12초 · 10:00:01 응답 없음 — S"
    p.update_status(dict(_OK, connected=False, stale=True, age_s=30.0, last_error="x"))
    assert p.error_label.text() == "" and p.lamps["conn"].text() == "끊김"   # 미연결은 램프로만


def test_zero_button_enable_rules_and_disabled_style():
    p = _panel()
    p.update_status(_OK)
    assert p.zero_button.isEnabled()
    for st in (dict(_OK, connected=False), dict(_OK, stale=True), dict(_OK, process_running=True)):
        p.update_status(st)
        assert not p.zero_button.isEnabled() and p.factor_button.isEnabled()
    assert "QPushButton:disabled" in p.zero_button.styleSheet() and "#b0b7be" in p.zero_button.styleSheet()
    p.update_status(dict(_OK, process_running=True))
    assert "공정 중" in p.zero_button.toolTip()


def test_panel_stale_xtal_fail_process_and_reset():
    p = _panel()
    p.update_status(dict(_OK, crystal_ok=False))
    assert p.lamps["xtal"].text() == "XTAL FAIL" and "#c62828" in p.lamps["xtal"].styleSheet()
    p.update_status(dict(_OK, stale=True, age_s=12.0, crystal_ok=False))
    assert p.lamps["xtal"].text() == "XTAL" and "#9aa3ab" in p.lamps["xtal"].styleSheet()   # stale → 회색
    assert p.lamps["resp"].text() == "응답 없음" and "#9aa3ab" in p.lamps["resp"].styleSheet()
    assert "#9aa3ab" in p.thk_label.styleSheet() and p.thk_label.text() == "123.4"          # 마지막 값, 회색
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


def test_process_view_running_end_and_idle():
    p = _panel()
    p.set_process_view("Thickness", "100.0 nm", 42.4, "두께 42.4 / 100.0 nm")
    assert p.progress.value() == 42 and p.progress.isTextVisible() and p.progress.format() == "42%"
    assert p.mode_label.text() == "종료 방식 Thickness" and p.target_label.text() == "목표 100.0 nm"
    p.set_process_view("Time", "—", None, "Time 모드 공정(두께는 기록만)")
    assert p.target_label.text() == "" and p.progress.value() == 0 and not p.progress.isTextVisible()
    p.set_process_view("Thickness", "100.0 nm", 100.1, "종료(Thickness) — 100.1 nm")      # 종료 뒤 결과 유지
    assert p.mode_label.text() == "종료 방식 Thickness" and p.proc_text.toolTip() == "종료(Thickness) — 100.1 nm"
    p.set_idle()
    assert [p.mode_label.text(), p.target_label.text(), p.proc_text.text()] == ["공정 없음", "", ""]


def _materials(tmp):
    from lib.thickness_materials import Material, save_materials
    mp = Path(tmp) / "materials.json"
    save_materials(mp, [Material("Aluminum", "Al", 2.73, 1.08), Material("Aluminum Oxide", "Al2O3", 3.97, 1.0, "매뉴얼 Z 미기재"),
                        Material("Titanium", "Ti", 4.5, 0.628)])
    return mp


def test_factor_dialog_material_dropdown_and_save():
    with tempfile.TemporaryDirectory() as tmp:
        mp = _materials(tmp)
        path = Path(tmp) / "한글" / "thickness_factors_ch2.json"
        saved = []
        dlg = ThicknessFactorDialog(str(path), ch=2, on_saved=lambda: saved.append(1), materials_path=str(mp))
        cb = dlg.combos["G1"]
        assert cb.itemText(0) == "(선택 안 함)" and cb.count() == 4 and cb.itemText(2) == "Al2O3 — Aluminum Oxide"
        assert cb.isEditable() and dlg.values["G1"].text() == "—"
        cb.setCurrentIndex(cb.findData("Aluminum Oxide"))
        assert dlg.values["G1"].text() == "ρ 3.97 · Z 1.000"                    # 고른 재료의 density·Z(읽기 전용)
        dlg.combos["G2"].setEditText("ti — titanium")                          # 입력해서 찾기(대소문자 무시)
        dlg.combos["G2"].setEditText("Titanium")
        assert dlg.values["G2"].text() == "ρ 4.5 · Z 0.628"
        dlg.combos["G3"].setEditText("Unobtainium")                            # 목록에 없는 재료
        dlg.tool_table.setItem(0, 0, QTableWidgetItem("120"))
        dlg.tool_table.setItem(1, 1, QTableWidgetItem("5"))                    # 범위 밖
        fac, errs = dlg.collect()
        assert len(errs) == 2 and any("Unobtainium" in e for e in errs)
        assert dlg.tool_table.item(1, 1).background().color().name() == "#ffcdd2"
        dlg.tool_table.setItem(1, 1, QTableWidgetItem(""))
        dlg.combos["G3"].setCurrentIndex(0)
        dlg._on_save()
        assert saved == [1] and path.exists()
        data = json.loads(path.read_text(encoding="utf-8"))
        assert data["tooling"]["up"]["G1"] == 120.0 and data["tooling"]["mid"]["G2"] is None
        assert data["material"] == {"G1": {"material": "Aluminum Oxide"}, "G2": {"material": "Titanium"},
                                    "G3": {"material": ""}}
        dlg2 = ThicknessFactorDialog(str(path), ch=2, materials_path=str(mp))  # 다시 열면 저장한 값
        assert dlg2.tool_table.item(0, 0).text() == "120"
        assert dlg2.combos["G1"].currentData() == "Aluminum Oxide" and dlg2.values["G2"].text() == "ρ 4.5 · Z 0.628"
        assert "다음 공정 시작 때 적용" in dlg2.info.text()


def test_material_dialog_edit_validate_and_refresh_factor_dialog():
    from ui.thickness_material_dialog import ThicknessMaterialDialog
    from lib.thickness_materials import load_materials
    with tempfile.TemporaryDirectory() as tmp:
        mp = _materials(tmp)
        fpath = Path(tmp) / "f.json"
        saved = []
        fdlg = ThicknessFactorDialog(str(fpath), ch=2, on_saved=lambda: saved.append(1), materials_path=str(mp))
        fdlg.combos["G1"].setCurrentIndex(fdlg.combos["G1"].findData("Titanium"))
        mdlg = ThicknessMaterialDialog(str(mp), used_names=["Titanium"], on_saved=fdlg._materials_saved)
        assert mdlg.table.rowCount() == 3
        r = mdlg._append_row()
        for c, v in enumerate(("Copper", "Cu", "8.93", "0.437", "새 재료")):
            mdlg.table.setItem(r, c, QTableWidgetItem(v))
        mdlg.table.setItem(0, 2, QTableWidgetItem("200"))                     # density 범위 밖
        r2 = mdlg._append_row()
        for c, v in enumerate(("titanium", "Ti2", "4.5", "0.6", "")):        # 이름 중복(대소문자 무시)
            mdlg.table.setItem(r2, c, QTableWidgetItem(v))
        mats, errs = mdlg.collect()
        assert len(errs) == 2 and mdlg.table.item(0, 2).background().color().name() == "#ffcdd2"
        assert mdlg.table.item(r2, 0).background().color().name() == "#ffcdd2"
        mdlg.table.setItem(0, 2, QTableWidgetItem("2.70"))
        mdlg.table.removeRow(r2)
        asked = []
        mdlg.confirm = lambda text: (asked.append(text), False)[1]           # 사용 중 재료 삭제 — 확인창에서 '아니오'
        assert mdlg.delete_rows([2]) is False and asked and "Titanium" in asked[0] and mdlg.table.rowCount() == 4
        assert mdlg.delete_rows([1]) is True and mdlg.table.rowCount() == 3  # 안 쓰는 재료는 바로 삭제(확인 없음)
        assert len(asked) == 1
        mdlg._on_save()
        back, w = load_materials(mp)
        assert w == [] and [m.name for m in back] == ["Aluminum", "Titanium", "Copper"] and back[0].density == 2.7
        # Factor 설정창 드롭다운·표시값이 바로 갱신되고 고른 재료는 그대로
        assert fdlg.combos["G1"].count() == 4 and fdlg.combos["G1"].findData("Copper") > 0
        assert fdlg.combos["G1"].currentData() == "Titanium" and saved == [1]


def test_panel_shows_applied_material_name():
    p = _panel()
    p.update_status(dict(_OK, material="Aluminum Oxide", density=3.97, z_factor=1.0, tooling=100.0))
    assert _one_line(p.material_label.text()) == "Al2O3 · Density 3.97 · Z 1.000 · Tooling 100 %"      # 목록에 있으면 기호
    assert p.material_label.toolTip().startswith("Aluminum Oxide · Density 3.97")
    p.update_status(dict(_OK, material="Unknown Stuff"))
    assert _one_line(p.material_label.text()).startswith("Unknown Stuff · Density 2.7")            # 목록에 없으면 이름
    long = dict(_OK, material="Very Long Material Name For Wrapping", density=19.3, z_factor=0.381, tooling=399.0)
    p.update_status(long)
    assert p.material_label.text() == "Very Long Material Name For Wrapping\nDensity 19.3 · Z 0.381 · Tooling 399 %"  # 재료 / 값 두 줄


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
    w = Win(True, True)                                            # 히터 패널을 숨기면 두께 패널이 그 자리로
    w.heater_panel.hide()
    main.MainWindow._layout_side_panels(w)
    assert w.thickness_panels[2].geometry().topLeft().y() == 10
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

            def attach_thickness(self, svc):
                self.attached = svc

            def set_thickness_view(self, cb):
                self.view = cb

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
        assert getattr(win.ch2, "attached", None) is win.thickness_services[2] and not hasattr(win.ch1, "attached")
        assert win.ch2.view == win.thickness_panels[2].set_process_view
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
