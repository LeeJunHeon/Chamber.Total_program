# -*- coding: utf-8 -*-
"""호스트 START 의 레시피 파일 오류·PC 시작 거절 응답 코드.

예전: 파일 없음·데이터 없음·형식 미지원·NAS 30초 초과가 모두 챔버 E511(사전 연결 점검 실패)/PC E420(없는 코드 → 응답 E999)로,
인코딩(엑셀 기본 CSV = CP949)·잠김 같은 읽기 실패는 챔버 E110(핸들러 내부 예외)·PC '데이터 없음'(E999)으로 나갔다.
지금: E207(파일 없음)·E208(데이터 없음)·E209(형식 미지원·읽기 실패)·E227(읽기 시간 초과),
PC 는 공정 실행 중 거절 E205·중복 시작 E206·그 밖의 거절 기본값 E511. 쿨다운(E700)·레시피 값 오류(E230)는 그대로.
장치·네트워크 접근 없음.
"""
import os
import sys
import asyncio
import contextlib

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                        # noqa: E402
import runtime.plasma_cleaning_runtime as PCR                        # noqa: E402
from runtime.chamber_runtime import ChamberRuntime                   # noqa: E402
from runtime.plasma_cleaning_runtime import PlasmaCleaningRuntime    # noqa: E402
from errors.app_error import CodedRuntimeError                       # noqa: E402
from host.context import HostContext                                 # noqa: E402
from host.handlers import HostHandlers                               # noqa: E402

CH_HEADER = "Process_name,delay,shutter_delay,process_time,use_rf_pulse,rf_pulse_power,base_pressure,working_pressure,Ar_flow"
CH_ROW = "공정A,,0,30,T,100,5e-6,3,20"
PC_HEADER = "#,time,rf_power,working_pressure,Ar_flow"
PC_ROW = "PC1,10m,100,3,20"


def _write(path, text, encoding="utf-8-sig"):
    with open(path, "w", encoding=encoding, newline="") as f:
        f.write(text)
    return str(path)


# ───────────────────────── 하네스 ─────────────────────────
class _Cfg:
    def _get(self, name, default=None):
        return default


def _mk_ch():
    c = ChamberRuntime.__new__(ChamberRuntime)
    c.ch = 1
    c.cfg = _Cfg()
    c.logs = []
    c.append_log = lambda src, msg: c.logs.append((src, msg))
    c._post_warning = lambda *a, **k: None
    c._update_ui_from_params = lambda p: None
    c.process_queue = []
    c.current_process_index = -1
    c._w_log = None
    c._host_start_future = None
    c._hostlog = lambda: None

    def _handle_start_clicked(_flag, *, origin="ui", origin_meta=None):
        fut = c._host_start_future
        if fut is not None and not fut.done():
            fut.set_result((True, "ok"))
    c._handle_start_clicked = _handle_start_clicked
    return c


def _mk_pc(report=(True, "ok")):
    pc = PlasmaCleaningRuntime.__new__(PlasmaCleaningRuntime)
    pc.logs = []
    pc.append_log = lambda s, m: pc.logs.append((s, m))
    pc._apply_recipe_row_to_ui = lambda row: None

    class _Cfgm:
        PC_HOST_START_WAIT_TIMEOUT_S = 5.0
    pc._cfg_mod = _Cfgm()
    pc._host_start_future = None

    async def _on_click_start(*, origin="ui", origin_meta=None):
        pc._host_report_start(*report)
    pc._on_click_start = _on_click_start
    return pc


async def _raises(coro):
    try:
        await coro
    except Exception as e:        # noqa: BLE001
        return e
    raise AssertionError("예외가 나지 않음")


@contextlib.contextmanager
def _nas_timeout():
    """recipe 로드의 wait_for(..., timeout=30.0) 만 즉시 시간 초과로 만든다(그 밖의 wait_for 는 그대로)."""
    orig = asyncio.wait_for

    async def fake(aw, timeout=None, **kw):
        if timeout == 30.0:
            with contextlib.suppress(Exception):
                aw.cancel()
            raise asyncio.TimeoutError()
        return await orig(aw, timeout, **kw)
    asyncio.wait_for = fake
    try:
        yield
    finally:
        asyncio.wait_for = orig


# ───────────────────────── 챔버(START_SPUTTER) — 런타임 예외 코드 ─────────────────────────
@pytest.mark.parametrize("kind,code,msg_start", [
    ("missing", "E207", "레시피 파일을 찾을 수 없습니다: "),
    ("header_only", "E208", "CSV에 공정 데이터가 없습니다."),
    ("txt", "E209", "지원하지 않는 레시피 형식입니다."),
    ("cp949", "E209", "레시피 파일을 읽지 못했습니다(UnicodeDecodeError: "),
    ("bad_xlsx", "E209", "레시피 파일을 읽지 못했습니다("),
    ("timeout", "E227", "CSV 로드 30초 timeout (NAS 응답 지연): "),
])
def test_chamber_recipe_errors_carry_code(tmp_path, kind, code, msg_start):
    if kind == "missing":
        path = str(tmp_path / "없음.csv")
    elif kind == "header_only":
        path = _write(tmp_path / "h.csv", CH_HEADER + "\r\n")
    elif kind == "txt":
        path = _write(tmp_path / "r.txt", CH_HEADER + "\r\n" + CH_ROW + "\r\n")
    elif kind == "cp949":
        path = _write(tmp_path / "k.csv", CH_HEADER + "\r\n" + CH_ROW + "\r\n", encoding="cp949")
    elif kind == "bad_xlsx":
        path = _write(tmp_path / "b.xlsx", "이건 엑셀이 아님")
    else:
        path = _write(tmp_path / "ok.csv", CH_HEADER + "\r\n" + CH_ROW + "\r\n")

    async def go():
        c = _mk_ch()
        if kind == "timeout":
            with _nas_timeout():
                return await _raises(c.start_with_recipe_string(path, origin="host"))
        return await _raises(c.start_with_recipe_string(path, origin="host"))
    e = asyncio.run(go())
    assert isinstance(e, CodedRuntimeError) and isinstance(e, RuntimeError)
    assert e.code == code and str(e).startswith(msg_start), (e.code, str(e))
    if kind in ("cp949", "bad_xlsx"):
        assert "CSV UTF-8" in str(e) and path in str(e)


def test_chamber_value_error_still_e230_path(tmp_path):
    """레시피 값 오류는 그대로 코드 없는 RuntimeError("레시피 오류 …") — 핸들러가 E230 으로 분류."""
    path = _write(tmp_path / "v.csv", CH_HEADER + "\r\n" + "공정A,,0,30,T,abc,5e-6,3,20\r\n")

    async def go():
        return await _raises(_mk_ch().start_with_recipe_string(path, origin="host"))
    e = asyncio.run(go())
    assert type(e) is RuntimeError and str(e).startswith("레시피 오류") and getattr(e, "code", None) is None


@pytest.mark.parametrize("val", ["500", "abc"])
def test_chamber_factor_value_out_of_range_is_e230_path(tmp_path, val):
    """tooling 범위 밖·숫자 아님 — 다른 레시피 값 오류와 같은 '레시피 오류'(핸들러 E230)."""
    path = _write(tmp_path / "f.csv", CH_HEADER + ",thickness,tooling\r\n" + CH_ROW + f",100,{val}\r\n")

    async def go():
        return await _raises(_mk_ch().start_with_recipe_string(path, origin="host"))
    e = asyncio.run(go())
    assert type(e) is RuntimeError and str(e).startswith("레시피 오류") and "tooling" in str(e)


def test_chamber_ok_path_unchanged(tmp_path):
    path = _write(tmp_path / "ok.csv", CH_HEADER + "\r\n" + CH_ROW + "\r\n")

    async def go():
        c = _mk_ch()
        await c.start_with_recipe_string(path, origin="host")
        return c
    c = asyncio.run(go())
    assert [r.get("Process_name") for r in c.process_queue] == ["공정A"]


# ───────────────────────── PC(START_PLASMA_CLEANING) — 런타임 예외 코드 ─────────────────────────
@pytest.mark.parametrize("kind,code,msg_start", [
    ("missing", "E207", "CSV 파일을 찾을 수 없습니다: "),
    ("header_only", "E208", "CSV에 데이터 행이 없습니다."),
    ("xlsx", "E209", "지원하지 않는 레시피 형식입니다. CSV 파일 경로만 허용됩니다."),
    ("cp949", "E209", "레시피 파일을 읽지 못했습니다(UnicodeDecodeError: "),
    ("timeout", "E227", "CSV 로드 30초 timeout (NAS 응답 지연): "),
])
def test_pc_recipe_errors_carry_code(tmp_path, kind, code, msg_start):
    if kind == "missing":
        path = str(tmp_path / "없음.csv")
    elif kind == "header_only":
        path = _write(tmp_path / "h.csv", PC_HEADER + "\r\n")
    elif kind == "xlsx":
        path = str(tmp_path / "r.xlsx")
    elif kind == "cp949":
        path = _write(tmp_path / "k.csv", PC_HEADER + "\r\n" + "세정1,10m,100,3,20\r\n", encoding="cp949")
    else:
        path = _write(tmp_path / "ok.csv", PC_HEADER + "\r\n" + PC_ROW + "\r\n")

    async def go():
        pc = _mk_pc()
        if kind == "timeout":
            with _nas_timeout():
                e = await _raises(pc.start_with_recipe_string(path, origin="host"))
        else:
            e = await _raises(pc.start_with_recipe_string(path, origin="host"))
        return pc, e
    pc, e = asyncio.run(go())
    assert isinstance(e, CodedRuntimeError) and e.code == code and str(e).startswith(msg_start), (e, getattr(e, "code", None))
    assert pc._host_start_future is None and pc._host_start_code is None


def test_pc_duplicate_start_e206(tmp_path):
    path = _write(tmp_path / "ok.csv", PC_HEADER + "\r\n" + PC_ROW + "\r\n")

    async def go():
        pc = _mk_pc()
        pc._host_start_future = asyncio.get_running_loop().create_future()   # 앞 요청이 아직 대기 중
        return await _raises(pc.start_with_recipe_string(path, origin="host"))
    e = asyncio.run(go())
    assert isinstance(e, CodedRuntimeError) and e.code == "E206"
    assert str(e) == "다른 Plasma Cleaning start 요청이 이미 처리 중입니다."


def _busy_pc(monkeypatch, *, running=False, pc_running=False, ch_running=False, ch_busy=False):
    """실제 _on_click_start 의 앞부분(실행 중·쿨다운·교차 실행 검사)만 타게 만든다."""
    pc = PlasmaCleaningRuntime.__new__(PlasmaCleaningRuntime)
    pc.logs = []
    pc.append_log = lambda s, m: pc.logs.append((s, m))
    pc._post_warning = lambda *a, **k: None
    pc._apply_recipe_row_to_ui = lambda row: None
    pc._loaded_recipe_row = {}
    pc._selected_ch = 1
    pc._running = running
    pc._chamber_busy = lambda ch: ch_busy

    class _Cfgm:
        PC_HOST_START_WAIT_TIMEOUT_S = 5.0
    pc._cfg_mod = _Cfgm()
    pc._host_start_future = None
    monkeypatch.setattr(PCR.runtime_state, "pc_block_reason", lambda ch, cooldown_s=60.0: (True, 0.0, None))
    monkeypatch.setattr(PCR.runtime_state, "is_running",
                        lambda kind, ch=None: (pc_running if kind == "pc" else ch_running if kind == "chamber" else False))
    return pc


@pytest.mark.parametrize("flags,msg", [
    (dict(running=True), "이미 Plasma Cleaning이 실행 중입니다."),
    (dict(pc_running=True), "CH1 Plasma Cleaning이 이미 실행 중입니다."),
    (dict(ch_running=True), "CH1는 이미 다른 공정이 실행 중입니다."),
    (dict(ch_busy=True), "CH1는 이미 다른 공정이 실행 중입니다."),
])
def test_pc_busy_rejections_e205(monkeypatch, flags, msg):
    pc = _busy_pc(monkeypatch, **flags)

    async def go():
        return await _raises(pc.start_with_recipe_string("", origin="host"))
    e = asyncio.run(go())
    assert isinstance(e, CodedRuntimeError) and e.code == "E205" and str(e) == msg
    assert pc._host_start_code is None


def test_pc_patched_report_start_signature_still_works(monkeypatch):
    """기존 테스트처럼 _host_report_start(ok, reason="") 를 바꿔 끼워도 _host_reject 가 같은 인자로 부른다."""
    pc = _busy_pc(monkeypatch, ch_running=True)
    seen = []
    pc._host_report_start = lambda ok, reason="": seen.append((bool(ok), reason))
    asyncio.run(pc._on_click_start(origin="ui"))
    assert seen == [(False, "CH1는 이미 다른 공정이 실행 중입니다.")]


# ───────────────────────── 핸들러 응답(로봇이 받는 코드) ─────────────────────────
def _ctx(ch1=None, pc=None):
    logs = []
    ctx = HostContext(log=lambda t, m: logs.append((t, m)), plc=None, ch1=ch1, ch2=None, pc=pc,
                      runtime_state=None, lock_plc=asyncio.Lock(), lock_ch1=asyncio.Lock(),
                      lock_ch2=asyncio.Lock())
    return ctx


@pytest.mark.parametrize("report,code,action", [
    ((False, "장치 연결에 실패했습니다: MFC timeout"), "E511", "STOP"),      # 예전 E420 → 응답 E999
    ((False, "이전 공정 종료 후 1분 대기 필요합니다. 약 30초 후에 다시 시도하십시오."), "E700", "RETRY_LATER"),
])
def test_pc_handler_default_and_cooldown(tmp_path, report, code, action):
    path = _write(tmp_path / "ok.csv", PC_HEADER + "\r\n" + PC_ROW + "\r\n")

    async def go():
        h = HostHandlers(_ctx(pc=_mk_pc(report=report)))
        return await h._start_plasma_cleaning_impl({"recipe": path})
    res = asyncio.run(go())
    assert res["result"] == "fail" and res["error_code"] == code and res["client_action"] == action


def test_pc_handler_busy_is_retry_e205(tmp_path, monkeypatch):
    path = _write(tmp_path / "ok.csv", PC_HEADER + "\r\n" + PC_ROW + "\r\n")

    async def go():
        h = HostHandlers(_ctx(pc=_busy_pc(monkeypatch, ch_running=True)))
        return await h._start_plasma_cleaning_impl({"recipe": path})
    res = asyncio.run(go())
    assert res["error_code"] == "E205" and res["client_action"] == "RETRY"
    assert res["detail"] == "CH1는 이미 다른 공정이 실행 중입니다."


@pytest.mark.parametrize("kind,code", [("missing", "E207"), ("cp949", "E209"), ("header_only", "E208")])
def test_pc_handler_recipe_codes(tmp_path, kind, code):
    if kind == "missing":
        path = str(tmp_path / "없음.csv")
    elif kind == "cp949":
        path = _write(tmp_path / "k.csv", PC_HEADER + "\r\n" + "세정1,10m,100,3,20\r\n", encoding="cp949")
    else:
        path = _write(tmp_path / "h.csv", PC_HEADER + "\r\n")

    async def go():
        h = HostHandlers(_ctx(pc=_mk_pc()))
        return await h._start_plasma_cleaning_impl({"recipe": path})
    res = asyncio.run(go())
    assert res["error_code"] == code and res["client_action"] == "STOP"


@pytest.mark.parametrize("kind,code,action", [
    ("missing", "E207", "STOP"), ("header_only", "E208", "STOP"), ("txt", "E209", "STOP"),
    ("cp949", "E209", "STOP"), ("timeout", "E227", "RETRY"), ("value", "E230", "STOP"),
    ("tooling", "E230", "STOP"),
])
def test_sputter_handler_recipe_codes(tmp_path, kind, code, action):
    if kind == "missing":
        path = str(tmp_path / "없음.csv")
    elif kind == "header_only":
        path = _write(tmp_path / "h.csv", CH_HEADER + "\r\n")
    elif kind == "txt":
        path = _write(tmp_path / "r.txt", CH_HEADER + "\r\n" + CH_ROW + "\r\n")
    elif kind == "cp949":
        path = _write(tmp_path / "k.csv", CH_HEADER + "\r\n" + CH_ROW + "\r\n", encoding="cp949")
    elif kind == "value":
        path = _write(tmp_path / "v.csv", CH_HEADER + "\r\n" + "공정A,,0,30,T,abc,5e-6,3,20\r\n")
    elif kind == "tooling":
        path = _write(tmp_path / "t.csv", CH_HEADER + ",tooling\r\n" + CH_ROW + ",5\r\n")
    else:
        path = _write(tmp_path / "ok.csv", CH_HEADER + "\r\n" + CH_ROW + "\r\n")

    async def go():
        c = _mk_ch()
        c._runner_state = "IDLE"
        h = HostHandlers(_ctx(ch1=c))

        async def _gate(ch):
            return {"state": "closed"}

        async def _mv(ch):
            return {"state": "open", "sw": True, "interlock": True}
        h._read_gate_state = _gate
        h._read_main_valve_state = _mv
        if kind == "timeout":
            with _nas_timeout():
                return await h._start_sputter_impl({"ch": 1, "recipe": path})
        return await h._start_sputter_impl({"ch": 1, "recipe": path})
    res = asyncio.run(go())
    assert res["result"] == "fail" and res["error_code"] == code and res["client_action"] == action, res


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
