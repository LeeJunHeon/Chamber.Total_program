# -*- coding: utf-8 -*-
"""use_thickness — 두께 모니터로 끝낼지(Thickness 모드)를 체크·레시피 열로 명시한다.
모드 판정·시작 판정(E849)·정규화·원본 검증·ETA, 화면 Process Time 자동 계산(사람이 넣은 값은 덮어쓰지 않음),
체크박스 반영·초기화·Start, 패널 체크박스, main 등록, 오류 정의."""
import asyncio
import contextlib
import csv
import os
import socket
import sys
import tempfile
import types
from pathlib import Path

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
sys.path.insert(0, _ROOT)
sys.path.insert(0, _HERE)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                     # noqa: E402
from PySide6.QtCore import QRect                                  # noqa: E402
from PySide6.QtWidgets import QApplication, QCheckBox, QPlainTextEdit, QWidget  # noqa: E402

from controller.process_controller import eta_row_min_seconds     # noqa: E402
from controller.thickness_process import thickness_mode_of, use_thickness_on  # noqa: E402
from lib.thickness_config import ThicknessConfig                  # noqa: E402
from runtime.chamber_runtime import ChamberRuntime, _CfgAdapter   # noqa: E402
from ui.thickness_panel import PANEL_H, PANEL_W, ThicknessPanel   # noqa: E402

_APP = QApplication.instance() or QApplication(sys.argv)


# ═══════════════════════ 모드 판정 ═══════════════════════
@pytest.mark.parametrize("v, supported, want", [
    ("T", True, True), (True, True, True), ("F", True, False), ("", True, False), (None, True, False),
    ("T", False, False), ("t", True, True), ("yes", True, True), (False, True, False), ("100", True, False),
])
def test_thickness_mode_of(v, supported, want):
    assert thickness_mode_of(v, supported) is want
    assert use_thickness_on(v) is (v in ("T", True, "t", "yes"))


ETA_ROW = {"shutter_delay": "1", "process_time": "", "thickness": "100", "dep_rate": "0.05"}


def test_eta_row_min_seconds_uses_use_thickness():
    assert eta_row_min_seconds(dict(ETA_ROW), True) == pytest.approx(2060.0)            # 열 없음 → 시간
    assert eta_row_min_seconds(dict(ETA_ROW, use_thickness="T"), True) == 60.0         # shutter_delay 만
    assert eta_row_min_seconds(dict(ETA_ROW, use_thickness="T"), False) == pytest.approx(2060.0)
    assert eta_row_min_seconds(dict(ETA_ROW, use_thickness="F"), True) == pytest.approx(2060.0)


# ═══════════════════════ 시작 판정(_thickness_start_plan) ═══════════════════════
def _plan_rt(ch=2, cfg_on=True, svc=True):
    rt = ChamberRuntime.__new__(ChamberRuntime)
    rt.ch = ch
    rt.cfg = _CfgAdapter(types.SimpleNamespace(SUPPORTS_THICKNESS=cfg_on), ch)
    rt._thickness = object() if svc else None
    return rt


@pytest.mark.parametrize("ut", ["T", True, "TRUE"])
def test_plan_supported_and_on(ut):
    assert _plan_rt()._thickness_start_plan({"use_thickness": ut, "thickness": 100.0}) == (True, "", "")


@pytest.mark.parametrize("p", [{"use_thickness": "F", "thickness": 100.0}, {"use_thickness": "", "thickness": 100.0},
                               {"use_thickness": False, "thickness": 100.0}, {"thickness": 100.0}, {}])
def test_plan_supported_but_off_is_time(p):
    assert _plan_rt()._thickness_start_plan(p) == (False, "", "")


def test_plan_rejects_unsupported_channel_e849():
    mode, code, msg = _plan_rt(ch=1, cfg_on=False, svc=False)._thickness_start_plan({"use_thickness": "T"})
    assert (mode, code) == (False, "E849")
    assert "SUPPORTS_THICKNESS" in msg and "use_thickness" in msg and "CH1" in msg and "시간으로 진행" in msg
    mode2, code2, msg2 = _plan_rt(ch=2, cfg_on=True, svc=False)._thickness_start_plan({"use_thickness": True})
    assert (mode2, code2) == (False, "E849")
    assert msg2 != msg and "서비스" in msg2 and "THICKNESS 로그" in msg2 and "use_thickness" in msg2 and "시간으로 진행" in msg2


@pytest.mark.parametrize("cfg_on, svc", [(False, False), (True, False)])
@pytest.mark.parametrize("ut", ["F", "", None])
def test_plan_unsupported_and_off_no_reject(cfg_on, svc, ut):
    p = {"thickness": 100.0} if ut is None else {"use_thickness": ut, "thickness": 100.0}
    assert _plan_rt(cfg_on=cfg_on, svc=svc)._thickness_start_plan(p) == (False, "", "")


# ═══════════════════════ 정규화·원본 검증 ═══════════════════════
def _norm_rt():
    rt = ChamberRuntime.__new__(ChamberRuntime)
    rt.ch = 2
    rt.logs = []
    rt.append_log = lambda s, m: rt.logs.append((s, m))
    rt._get_text = lambda name: ""
    rt.supports_rf_pulse = rt.supports_dc_pulse = rt.supports_dc_cont = rt.supports_rf_cont = True
    rt.supports_dc_cont2 = False
    return rt


def _row(**kw):
    with open(os.path.join(_ROOT, "recipes", "Recipe_Template.csv"), encoding="utf-8-sig", newline="") as fp:
        head = next(csv.reader(fp))
    row = {h: "" for h in head if h != "use_thickness"}       # 열 없음(기존 레시피)
    row.update({"#": "1", "Process_name": "t", "base_pressure": "7e-6", "working_pressure": "5",
                "shutter_delay": "5", "gun1": "T", "Ar": "T", "Ar_flow": "20",
                "use_dc_pulse": "T", "dc_pulse_power": "80"})
    row.update(kw)
    return row


def test_normalize_use_thickness():
    rt = _norm_rt()
    n = rt._normalize_params_for_process(_row(dep_rate="0.05", thickness="100"))
    assert n["use_thickness"] is False and n["process_time"] == pytest.approx(33.333, abs=1e-3)
    n = rt._normalize_params_for_process(_row(use_thickness="T", dep_rate="0.05", thickness="100"))
    assert n["use_thickness"] is True and n["process_time"] == pytest.approx(33.333, abs=1e-3)
    n = rt._normalize_params_for_process(_row(use_thickness="F", process_time="10", dep_rate="0.05", thickness="100"))
    assert n["use_thickness"] is False and n["process_time"] == 10.0            # 입력한 process_time 우선
    n = rt._normalize_params_for_process(_row(use_thickness="", process_time="10"))
    assert n["use_thickness"] is False and n["process_time"] == 10.0


def test_validate_raw_rows_use_thickness():
    rt = ChamberRuntime.__new__(ChamberRuntime)
    ok = {"Process_name": "t", "thickness": "100"}
    for v in ("T", "F", "", "t", "true"):
        assert rt._validate_raw_rows([dict(ok, use_thickness=v)]) == [], v
    errs = rt._validate_raw_rows([dict(ok, use_thickness="TT")])
    assert len(errs) == 1 and "use_thickness='TT' → T/F 값이 아닙니다" in errs[0]


# ═══════════════════════ 화면(Process Time 자동 계산·체크박스·Start) ═══════════════════════
class _UI:
    pass


def _ui_rt(ch=2, with_checkbox=True):
    """실제 Qt 위젯 몇 개만 둔 런타임 — Dep.Rate·Thickness textChanged 를 _recalc_process_time 에 연결(실제 화면과 같게)."""
    rt = ChamberRuntime.__new__(ChamberRuntime)
    rt.ch = ch
    rt.prefix = f"ch{ch}_"
    rt.ui = _UI()
    rt.logs = []
    rt.append_log = lambda s, m: rt.logs.append((s, m))
    rt._w_log = None
    rt._root = QWidget()
    for leaf, cls in (("depRate_edit", QPlainTextEdit), ("thickness_edit", QPlainTextEdit),
                      ("processTime_edit", QPlainTextEdit)):
        setattr(rt.ui, f"{rt.prefix}{leaf}", cls(rt._root))
    if with_checkbox:
        setattr(rt.ui, f"{rt.prefix}useThickness_checkbox", QCheckBox(rt._root))
    rt._u("depRate_edit").textChanged.connect(rt._recalc_process_time)
    rt._u("thickness_edit").textChanged.connect(rt._recalc_process_time)
    # Pulse·타겟 등 이 시험과 무관한 화면 함수는 막는다
    rt._set_pulse_radio = lambda *a, **k: None
    rt._bind_pulse_selector_signals = lambda *a, **k: None
    rt._sync_pulse_duty_label = lambda *a, **k: None
    rt._dcp_off_text_for_ui = lambda *a, **k: ""
    rt._apply_ui_lockouts = lambda *a, **k: None
    return rt


def _pt(rt):
    return rt._get_text("processTime_edit")


def _type(rt, leaf, text):
    rt._u(leaf).setPlainText(text)


def test_recalc_cases_1_to_4():
    rt = _ui_rt()
    _type(rt, "depRate_edit", "0.05")
    _type(rt, "thickness_edit", "100")
    assert _pt(rt) == "33.333"                                   # 1. 빈 칸 → 계산값
    _type(rt, "thickness_edit", "200")
    assert _pt(rt) == "66.667"                                   # 2. 자동 값은 계속 따라간다
    _type(rt, "processTime_edit", "40")
    _type(rt, "thickness_edit", "300")
    assert _pt(rt) == "40"                                       # 3. 사람이 넣은 값 유지
    rt._recalc_process_time()                                    # Start 의 강제 반영도 덮어쓰지 않는다
    assert _pt(rt) == "40"
    _type(rt, "processTime_edit", "")
    _type(rt, "depRate_edit", "0.1")
    assert _pt(rt) == "50.000"                                   # 4. 칸을 지우면 다시 계산값
    _type(rt, "processTime_edit", "0")
    _type(rt, "thickness_edit", "600")
    assert _pt(rt) == "100.000"                                  # 0 이하도 빈 칸 취급


def test_recalc_case_5_ch1_default_25():
    rt = _ui_rt(ch=1, with_checkbox=False)
    rt._set_default_ui_values()
    assert _pt(rt) == "25"
    _type(rt, "depRate_edit", "0.05")
    _type(rt, "thickness_edit", "100")
    assert _pt(rt) == "33.333"                                   # 5. 프로그램 기본값 25 는 계산값으로 바뀐다


def _recipe(**kw):
    p = {"Process_name": "r", "G1 Target": "x"}
    p.update(kw)
    return p


def test_update_ui_cases_6_and_7_and_start():
    rt = _ui_rt()
    rt._update_ui_from_params(_recipe(process_time="10", dep_rate="0.05", thickness="100"))
    assert _pt(rt) == "10"                                       # 6. 행 process_time 그대로
    rt._recalc_process_time()
    assert _pt(rt) == "10"                                       # Start 의 강제 반영 뒤에도 10
    _type(rt, "processTime_edit", "40")                          # 사람이 넣은 값
    rt._update_ui_from_params(_recipe(process_time="", dep_rate="0.05", thickness="100"))
    assert _pt(rt) == "33.333"                                   # 7. 행 process_time 공란 → 계산값
    rt._update_ui_from_params(_recipe(process_time="", dep_rate="", thickness=""))
    assert _pt(rt) == ""                                         # 값이 없으면 빈 칸(실제로 쓰는 시간과 같게)


def _start_rt(monkeypatch, ch=2, with_checkbox=True):
    from runtime import chamber_runtime as mod
    monkeypatch.setattr(mod.runtime_state, "remaining_cooldown", lambda *a, **k: 0.0)
    monkeypatch.setattr(mod.runtime_state, "is_running", lambda *a, **k: False)
    rt = _ui_rt(ch=ch, with_checkbox=with_checkbox)
    rt.put = []
    rt.reports = []
    rt._ensure_runner_started = lambda: None
    rt._host_report_start = lambda ok, msg="": rt.reports.append((ok, msg))
    rt._post_warning = lambda *a: rt.reports.append(("warn", a))
    rt._post_critical = lambda *a: rt.reports.append(("crit", a))
    rt._pending_device_cleanup = False
    rt._runner_state = "IDLE"
    rt.process_queue = []
    rt.process_controller = types.SimpleNamespace(is_running=False)
    rt._validate_single_run_inputs = lambda: {}
    rt._log_file_path = "x"
    rt._validate_norm_params = lambda p: []
    rt._host_run_begin = lambda *a: None
    rt._runner_put = lambda cmd: rt.put.append(cmd)
    return rt


@pytest.mark.parametrize("checked", [True, False])
def test_start_reads_checkbox(monkeypatch, checked):
    rt = _start_rt(monkeypatch)
    rt._update_ui_from_params(_recipe(process_time="10", dep_rate="0.05", thickness="100"))
    rt._u("useThickness_checkbox").setChecked(checked)
    rt._handle_start_clicked(False)
    assert len(rt.put) == 1, rt.reports
    p = rt.put[0].params
    assert p["use_thickness"] is checked and p["process_time"] == 10.0     # 6. Start → process_time 10


def test_start_without_checkbox_is_false(monkeypatch):
    rt = _start_rt(monkeypatch, ch=1, with_checkbox=False)
    rt._handle_start_clicked(False)
    assert len(rt.put) == 1, rt.reports
    assert rt.put[0].params["use_thickness"] is False


@pytest.mark.parametrize("v, want", [("T", True), ("t", True), (True, True), ("F", False), ("", False), (None, False)])
def test_update_ui_sets_checkbox(v, want):
    rt = _ui_rt()
    cb = rt._u("useThickness_checkbox")
    cb.setChecked(not want)
    rt._update_ui_from_params(_recipe() if v is None else _recipe(use_thickness=v))
    assert cb.isChecked() is want


def test_default_ui_values_unchecks():
    rt = _ui_rt()
    rt._u("useThickness_checkbox").setChecked(True)
    rt._set_default_ui_values()
    assert rt._u("useThickness_checkbox").isChecked() is False


# ═══════════════════════ 패널 체크박스 ═══════════════════════
def _panel():
    class Svc:
        def find_material(self, name):
            return None
    return ThicknessPanel(Svc(), ThicknessConfig(enabled=True, ch=2, port=4008))


def test_panel_checkbox_default_disabled_in_process_and_layout():
    p = _panel()
    cb = p.use_checkbox
    assert isinstance(cb, QCheckBox) and not cb.isChecked() and cb.isEnabled()
    assert "공정에 사용" in cb.text() and "Dep.Rate" in cb.toolTip() and "Process Time" in cb.toolTip()
    g = cb.geometry()
    assert QRect(0, 0, PANEL_W, PANEL_H).contains(g) and p.height() == PANEL_H
    for b in (p.zero_button, p.factor_button):
        assert not g.intersects(b.geometry())
        assert g.top() > b.geometry().bottom()                      # 버튼 줄 밑
    p.update_status({"connected": True, "stale": False, "process_running": True})
    assert not cb.isEnabled()
    p.update_status({"connected": True, "stale": False, "process_running": False})
    assert cb.isEnabled()


# ═══════════════════════ main 등록 ═══════════════════════
class _RT:
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


class _BadUI:
    def __setattr__(self, k, v):
        raise RuntimeError("no setattr")


@pytest.mark.parametrize("ui_kind", ["ui", "none", "bad"])
def test_main_registers_checkbox(monkeypatch, ui_kind):
    import main
    import ui.thickness_panel as tp_mod

    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    with tempfile.TemporaryDirectory() as tmp:
        monkeypatch.setattr(main, "config_ch2", types.SimpleNamespace(
            SUPPORTS_THICKNESS=True, THICKNESS_TCP_HOST="127.0.0.1", THICKNESS_TCP_PORT=port,
            THICKNESS_LOG_DIR=str(Path(tmp) / "logs"), THICKNESS_FACTOR_FILE=str(Path(tmp) / "f.json")))
        loop = asyncio.new_event_loop()
        root = QWidget()
        win = types.SimpleNamespace(ch1=_RT(), ch2=_RT(), _loop=loop, heater_panel=None,
                                    thickness_services={}, thickness_panels={}, root=root)
        if ui_kind == "ui":
            win.ui = types.SimpleNamespace()
        elif ui_kind == "bad":
            win.ui = _BadUI()
        win._broadcast_log = lambda *a: (_ for _ in ()).throw(AssertionError(a))
        real = tp_mod.ThicknessPanel
        monkeypatch.setattr(tp_mod, "ThicknessPanel", lambda svc, cfg, parent=None: real(svc, cfg, root))
        main.MainWindow._init_thickness(win, 2)
        try:
            assert list(win.thickness_services) == [2] and list(win.thickness_panels) == [2]   # 패널·서비스는 그대로
            assert getattr(win.ch2, "attached", None) is win.thickness_services[2]
            panel = win.thickness_panels[2]
            if ui_kind == "ui":
                assert win.ui.ch2_useThickness_checkbox is panel.use_checkbox
            elif ui_kind == "bad":
                assert any("체크박스 등록 실패" in m for m in win.ch2.side)
            else:
                assert not hasattr(win, "ui")
        finally:
            loop.run_until_complete(win.thickness_services[2].shutdown(timeout=1.0))
            loop.close()


# ═══════════════════════ 오류 정의 ═══════════════════════
def test_e849_registered():
    from errors.error_registry import ACTION_STOP, SEV_ERROR, get_error_definition
    d = get_error_definition("E849")
    assert d.code == "E849" and d.title == "Thickness 모드를 쓸 수 없는 채널"
    assert d.category == "THICKNESS" and d.client_action == ACTION_STOP and d.severity == SEV_ERROR
    assert "SUPPORTS_THICKNESS" in d.cause and "use_thickness" in d.fix


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
