# -*- coding: utf-8 -*-
"""Plasma Cleaning 시작의 GV 인터락 FALSE → 응답 코드 E302.

로봇 GATE_OPEN 이 같은 PLC 비트(G_V_n_인터락)가 FALSE 일 때 E302 를 보내므로 같은 코드로 맞춘다
(로봇은 E302 에 진공 재확보 VACUUM_ON 을 다시 시도한다). 예전: 기본 코드(항목 8 이전 E999, 이후 E511).
인터락을 읽지 못한 경우(PLC 통신 문제 — 진공과 무관)는 그대로 기본 코드 E511. 장치·네트워크 접근 없음.
"""
import os
import sys
import asyncio
import contextlib

import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import runtime.plasma_cleaning_runtime as PCR                        # noqa: E402
from runtime.plasma_cleaning_runtime import PlasmaCleaningRuntime    # noqa: E402
from errors.app_error import CodedRuntimeError                       # noqa: E402
from host.context import HostContext                                 # noqa: E402
from host.handlers import HostHandlers                               # noqa: E402

PC_HEADER = "#,time,rf_power,working_pressure,Ar_flow"
PC_ROW = "PC1,10m,100,3,20"


class _RuntimeState:
    """runtime_state 대역 — 시작 검사(쿨다운·실행 중)는 통과, 나머지 기록은 하지 않는다."""

    def pc_block_reason(self, ch, cooldown_s=60.0):
        return True, 0.0, None

    def is_running(self, kind, ch=None):
        return False

    def __getattr__(self, name):
        return lambda *a, **k: None


class _PLC:
    def __init__(self, value=False, exc=None):
        self.value, self.exc, self.reads = value, exc, []

    async def read_bit(self, key):
        self.reads.append(key)
        if self.exc is not None:
            raise self.exc
        return self.value


def _pc(monkeypatch, plc, ch=1):
    """실제 _on_click_start 를 프리플라이트(장치 연결) 직후 GV 인터락 검사까지 타게 만든다."""
    monkeypatch.setattr(PCR, "runtime_state", _RuntimeState())
    pc = PlasmaCleaningRuntime.__new__(PlasmaCleaningRuntime)
    pc.logs = []
    pc.append_log = lambda s, m: pc.logs.append((s, m))
    pc.criticals = []
    pc._post_critical = lambda title, msg, **k: pc.criticals.append((title, msg))
    pc._post_warning = lambda *a, **k: None
    pc._apply_recipe_row_to_ui = lambda row: None
    pc._loaded_recipe_row = {}
    pc._selected_ch = ch
    pc._running = False
    pc._chamber_busy = lambda c: False
    pc.plc = plc

    async def _noop(*a, **k):
        return None
    pc._preflight_connect = _noop
    pc._final_cleanup = _noop
    pc._cleanup_tasks_only = _noop

    class _Cfgm:
        PC_HOST_START_WAIT_TIMEOUT_S = 5.0
    pc._cfg_mod = _Cfgm()
    pc._host_start_future = None
    return pc


async def _raises(coro):
    try:
        await coro
    except Exception as e:        # noqa: BLE001
        return e
    raise AssertionError("예외가 나지 않음")


@pytest.mark.parametrize("ch", [1, 2])
def test_gv_interlock_false_is_e302(monkeypatch, ch):
    plc = _PLC(value=False)
    pc = _pc(monkeypatch, plc, ch)

    async def go():
        return await _raises(pc.start_with_recipe_string("", origin="host"))
    e = asyncio.run(go())
    msg = f"게이트밸브 인터락 FALSE (G_V_{ch}_인터락=FALSE) → LoadLock의 압력을 확인하십시오."
    assert isinstance(e, CodedRuntimeError) and e.code == "E302" and str(e) == msg
    assert plc.reads == [f"G_V_{ch}_인터락"]
    assert pc.criticals == [("게이트밸브 인터락", msg)]                # 화면 경고는 그대로
    assert pc._running is False and pc._host_start_code is None


def test_gv_interlock_read_failure_keeps_default_code(monkeypatch):
    plc = _PLC(exc=RuntimeError("Modbus 응답 없음"))
    pc = _pc(monkeypatch, plc, 1)

    async def go():
        return await _raises(pc.start_with_recipe_string("", origin="host"))
    e = asyncio.run(go())
    assert type(e) is RuntimeError and getattr(e, "code", None) is None   # 핸들러 기본 코드(E511)
    assert str(e) == "게이트밸브 인터락 상태 확인 실패: Modbus 응답 없음"


def _ctx(pc):
    return HostContext(log=lambda t, m: None, plc=None, ch1=None, ch2=None, pc=pc, runtime_state=None,
                       lock_plc=asyncio.Lock(), lock_ch1=asyncio.Lock(), lock_ch2=asyncio.Lock())


@pytest.mark.parametrize("plc_kw,code", [
    (dict(value=False), "E302"),                                   # 인터락 FALSE → GATE_OPEN 과 같은 코드
    (dict(exc=RuntimeError("Modbus 응답 없음")), "E511"),          # 읽기 실패 → 기본 코드 그대로
])
def test_handler_response_code(monkeypatch, tmp_path, plc_kw, code):
    path = tmp_path / "pc.csv"
    path.write_text(PC_HEADER + "\r\n" + PC_ROW + "\r\n", encoding="utf-8-sig")
    pc = _pc(monkeypatch, _PLC(**plc_kw), 1)

    async def go():
        return await HostHandlers(_ctx(pc))._start_plasma_cleaning_impl({"recipe": str(path)})
    res = asyncio.run(go())
    assert res["result"] == "fail" and res["error_code"] == code and res["client_action"] == "STOP", res
