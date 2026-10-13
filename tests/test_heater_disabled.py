# -*- coding: utf-8 -*-
"""main.MainWindow._init_ch2_heater — SUPPORTS_HEATER=False 면 서비스·패널·resize 를 만들지 않는다.
+ DC2 폐지 반영(runtime/chamber_runtime.py: _validate_norm_params / _apply_ui_lockouts).

main.py 는 import 할 때 이벤트 루프 정책 등을 바꾸므로 메서드 소스만 떼어 스텁 객체로 실행한다.
pytest / 직접 실행 모두 가능.
"""
import os
import sys
import ast
import asyncio
import contextlib
import textwrap
from types import SimpleNamespace

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                   # noqa: E402
from PySide6.QtCore import QRect                                # noqa: E402
from PySide6.QtWidgets import QApplication, QWidget             # noqa: E402

_APP = QApplication.instance() or QApplication(sys.argv)


def _method(path, cls, name):
    src = open(path, encoding="utf-8").read()
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, ast.ClassDef) and node.name == cls:
            for f in node.body:
                if isinstance(f, (ast.FunctionDef, ast.AsyncFunctionDef)) and f.name == name:
                    return textwrap.dedent(ast.get_source_segment(src, f))
    raise AssertionError(f"{cls}.{name} 없음")


def _init_fn(cfg_mod):
    ns = {"config_ch2": cfg_mod, "contextlib": contextlib, "QRect": QRect}
    exec(compile(_method(os.path.join(_ROOT, "main.py"), "MainWindow", "_init_ch2_heater"), "main.py", "exec"), ns)
    return ns["_init_ch2_heater"]


class _Stub(QWidget):
    def __init__(self, loop=None):
        super().__init__()
        self.resized = []
        self.logs = []
        self._plc_ch2 = SimpleNamespace(is_connected=lambda: False)
        self.chat_ch2 = None
        self._loop = loop
        self.ch2 = SimpleNamespace(append_log=lambda s, m: self.logs.append((s, m)))
        self._broadcast_log = lambda s, m: self.logs.append((s, m))

    def resize(self, *a):                                       # noqa: D401
        self.resized.append(a)
        super().resize(*a)


def test_disabled_creates_nothing():
    from lib import config_ch2
    assert getattr(config_ch2, "SUPPORTS_HEATER", False) is False   # 기본 꺼짐
    stub = _Stub()
    _init_fn(config_ch2)(stub)
    assert stub.heater_service is None and stub.heater_panel is None
    assert stub.resized == [] and stub.children() == []
    assert all("히터" not in m for _s, m in stub.logs)


def test_enabled_creates_service_panel_and_resizes():
    async def _main():
        stub = _Stub(asyncio.get_running_loop())
        _init_fn(SimpleNamespace(SUPPORTS_HEATER=True, HEATER_HOLD_MODE="tc2"))(stub)
        try:
            assert stub.heater_service is not None and stub.heater_panel is not None
            assert stub.resized == [(1086 + 240 + 12, 1010)]
            g = stub.heater_panel.geometry()
            assert (g.x(), g.y(), g.width()) == (1086, 10, 240)
            assert any("tc2" in m for _s, m in stub.logs)        # 설정 경고가 CH2 로그로
            names = {t.get_name() for t in asyncio.all_tasks()}
            assert {"CH2HeaterPoll", "CH2HeaterWD"} <= names
        finally:
            await stub.heater_service.shutdown()
        return stub
    asyncio.run(_main())


# ───────────────────────── DC2 폐지 ─────────────────────────
def _chamber(ch=2, dc2=False):
    from runtime.chamber_runtime import ChamberRuntime
    c = ChamberRuntime.__new__(ChamberRuntime)
    c.ch = ch
    c.prefix = f"ch{ch}_"
    c.supports_dc_cont = True
    c.supports_dc_cont2 = dc2
    c.supports_rf_cont = True
    c.supports_dc_pulse = True
    c.supports_rf_pulse = True
    return c


def _params(**kw):
    p = {"use_ar": True, "ar_flow": 10.0, "use_o2": False, "o2_flow": 0, "use_n2": False, "n2_flow": 0,
         "use_dc_power": False, "dc_power": 0.0, "use_dc_power2": True, "dc_power2": 100.0,
         "use_rf_power": False, "use_dc_pulse": False, "use_rf_pulse": False}
    p.update(kw)
    return p


def test_dc2_rejected_when_unsupported():
    errs = _chamber(dc2=False)._validate_norm_params(_params())
    assert "DC2 Power 는 사용할 수 없습니다(DC2 폐지)" in errs
    errs = _chamber(dc2=True)._validate_norm_params(_params())
    assert "DC2 Power 는 사용할 수 없습니다(DC2 폐지)" not in errs
    errs = _chamber(dc2=False)._validate_norm_params(_params(use_dc_power2=False, use_dc_power=True, dc_power=100.0))
    assert "DC2 Power 는 사용할 수 없습니다(DC2 폐지)" not in errs


class _W:
    def __init__(self):
        self.enabled = True
        self.tip = ""

    def setEnabled(self, v):
        self.enabled = bool(v)

    def setToolTip(self, t):
        self.tip = t


def test_dc2_widgets_locked():
    c = _chamber(dc2=False)
    c.ui = SimpleNamespace(ch2_dcPower2_checkbox=_W(), ch2_dcPower2_edit=_W(),
                           ch2_rfPulsePower_radio=_W(), ch2_dcPulsePower_radio=_W())
    c._apply_ui_lockouts()
    for w in (c.ui.ch2_dcPower2_checkbox, c.ui.ch2_dcPower2_edit):
        assert w.enabled is False and w.tip == "DC2 미사용 — D/A CH3 는 CH2 히터 출력"
    c2 = _chamber(dc2=True)
    c2.ui = SimpleNamespace(ch2_dcPower2_checkbox=_W(), ch2_dcPower2_edit=_W())
    c2._apply_ui_lockouts()
    assert c2.ui.ch2_dcPower2_checkbox.enabled is True


def test_settings_json_ch2_flags_load():
    """값은 배포 상황마다 바뀌므로 고정하지 않는다 — 로더가 bool 로 읽는지만 본다."""
    from lib import _config_loader, config_ch2
    _config_loader.load_settings()
    assert isinstance(config_ch2.SUPPORTS_DC2, bool)
    assert isinstance(config_ch2.SUPPORTS_HEATER, bool)


def test_dc2_and_heater_exclusive():
    from runtime.chamber_runtime import _dc2_allowed
    assert _dc2_allowed(2, True, False) is True
    assert _dc2_allowed(2, True, True) is False      # 히터 켜면 DC2 끔(D/A CH3 공유)
    assert _dc2_allowed(2, False, False) is False
    assert _dc2_allowed(1, True, False) is False     # DC2 는 CH2 전용


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
