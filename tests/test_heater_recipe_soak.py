# -*- coding: utf-8 -*-
"""레시피 heater_soak(분) — 공정 행이 히터 RAMP → 목표 도달 → SOAK N분(히터 유지) → 베이스 압력 … 순서로 돈다.

공란·열 없음이면 예전처럼 설정 HEATER_SOAK_TIME_SEC(60초). 목표 ±HEATER_SOAK_TOLERANCE 를 벗어나면 SOAK 을 처음부터 센다.
대기 한도 = 예상 RAMP + SOAK + 30분(기본값보다 짧으면 기본값). 상한 12시간은 RAMP 쪽 — SOAK 은 그 위에 더한다.
템플릿(CSV·엑셀)에 heater_soak 열, 히터 설명은 현재 설정값(목표 상한 900·램프 기본 6°C/min).
실제 HeaterService + 래더·열 에뮬레이터(tests/heater_ladder_sim.py, 가상 시계). 장치·네트워크 접근 없음.
"""
import os
import sys
import csv
import asyncio
import contextlib

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
sys.path.insert(0, _ROOT)
sys.path.insert(0, _HERE)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                          # noqa: E402
from openpyxl import load_workbook                                     # noqa: E402
import heater_ladder_sim as sim                                        # noqa: E402
from lib import config_common as cfgc                                  # noqa: E402
from lib.heater_profile import ramp_minutes                            # noqa: E402
import runtime.chamber_runtime as cr                                   # noqa: E402
from controller.process_controller import ProcessController, ActionType             # noqa: E402
from controller.heater_process import (                                # noqa: E402
    HeaterSoakWaiter, heater_wait_timeout_s, WAIT_TIMEOUT_MAX_SEC, WAIT_TIMEOUT_MARGIN_SEC,
)

CSV_PATH = os.path.join(_ROOT, "recipes", "Recipe_Template.csv")
XLSX_PATH = os.path.join(_ROOT, "recipes", "Recipe_Template.xlsx")
# 장비 settings.json ch2 와 같은 히터 값(에뮬레이터 기본값 대신)
REAL = dict(mv_limit=1200, mv_abs_max=1300, ramp_rate=6.0, max_temp=900.0, sv_limit_c=900.0, ot_limit_c=950.0,
            hold_mode="off")


class _PCRec:
    """공정 컨트롤러 대신 on_heater_* 호출만 기록."""

    def __init__(self):
        self.calls = []

    def __getattr__(self, name):
        if name.startswith("on_heater"):
            return lambda *a, **k: self.calls.append((name, a, k))
        raise AttributeError(name)

    def names(self):
        return [c[0] for c in self.calls]


class _FieldChat(sim.FakeChat):
    """에뮬레이터 가짜 챗 + 카드 필드까지 남긴다."""

    def __init__(self, clk):
        super().__init__(clk)
        self.full = []

    def _post_card(self, title, subtitle="", status="INFO", fields=None, urgent=False):
        super()._post_card(title, subtitle, status, fields, urgent)
        self.full.append((title, status, dict(fields or {})))


def _rt():
    """런타임 최소 인스턴스 + 실제 _bind_process_controller 콜백. (rt, 콜백을 가진 진짜 PC, 기록기)"""
    rt = cr.ChamberRuntime.__new__(cr.ChamberRuntime)
    rt.ch = 2
    rt.logs = []
    rt.append_log = lambda s, m: rt.logs.append((None, s, m))
    rt._get_text = lambda name: ""
    rt.supports_rf_pulse = rt.supports_dc_pulse = rt.supports_dc_cont = rt.supports_rf_cont = True
    rt.supports_dc_cont2 = False
    rt._loop = asyncio.get_running_loop()
    rt._ensure_task_alive = lambda *a, **k: None
    rt._heater = None
    rt._heater_gen = 0
    rt._heater_job = None
    rt._heater_watch = False
    rt._heater_rate = None
    rt._runner_queue_mode = True
    rt.process_queue = []
    rt.current_process_index = -1
    rt._bind_process_controller()
    pc = rt.process_controller
    rec = _PCRec()
    rt.process_controller = rec
    return rt, pc, rec


def _logs(rt, word):
    return [(t, m) for t, s, m in rt.logs if word in m]


async def _tick(n=5):
    for _ in range(n):
        await asyncio.sleep(0)


async def _heat(target, soak_s, **cfg):
    """HEATER_SET → 승온 대기(soak_s=None 이면 인자 1개 — 예전 호출) 시작까지. (rt, rec, svc, plc, clk, chat, t0)"""
    rt, pc, rec = _rt()
    svc, plc, clk, _logs_, _chat = sim.make(**cfg)
    chat = _FieldChat(clk)
    svc._chat = chat
    rt.append_log = lambda s, m: rt.logs.append((clk.t, s, m))
    rt.attach_heater(svc)
    await sim.run(svc, plc, clk, 5)
    pc._heater_set(float(target), None)
    await _tick()
    await sim.run(svc, plc, clk, 2)
    assert rec.names() == ["on_heater_set_ok"], rec.calls
    t0 = clk.t
    if soak_s is None:
        pc._heater_wait(float(target))
    else:
        pc._heater_wait(float(target), float(soak_s))
    return rt, rec, svc, plc, clk, chat, t0


async def _run_until(svc, plc, clk, cond, limit_s, on_step=None):
    t0 = clk.t
    while not cond():
        await sim.run(svc, plc, clk, 1, on_step=on_step)
        await _tick()
        assert clk.t - t0 < limit_s, "시간 초과"


def _done(rec):
    return lambda: any(n in ("on_heater_ready", "on_heater_failed") for n in rec.names())


# ───────────────────────── 레시피 정규화·검증 ─────────────────────────
def test_normalize_validate_heater_soak():
    async def go():
        rt, _pc, _rec = _rt()
        svc, plc, clk, logs, chat = sim.make(**REAL)
        raw = {"Ar": "T", "Ar_flow": "10", "gun1": "T", "use_heater": "T", "heater_temp": "400"}
        assert rt._normalize_params_for_process(dict(raw, heater_soak="60"))["heater_soak"] == 60.0
        assert rt._normalize_params_for_process(dict(raw, heater_soak=""))["heater_soak"] is None
        assert rt._normalize_params_for_process(dict(raw))["heater_soak"] is None          # 열이 없는 예전 레시피
        n = rt._normalize_params_for_process(dict(raw, use_heater="F", heater_soak="60"))
        assert n["heater_soak"] is None and _logs(rt, "heater_soak=60.0 은 무시")

        rt._heater = svc
        base = dict(use_ar=True, ar_flow=10, use_g1=True, use_dc_pulse=True, dc_pulse_power=50,
                    use_heater=True, heater_temp=400.0)
        for ok in (None, 0.0, 0.5, 60.0, cr.HEATER_SOAK_MAX_MIN):
            assert rt._validate_norm_params(dict(base, heater_soak=ok)) == [], ok
        for bad in (-1.0, cr.HEATER_SOAK_MAX_MIN + 1, float("nan")):
            errs = rt._validate_norm_params(dict(base, heater_soak=bad))
            assert len(errs) == 1 and "heater_soak" in errs[0] and "480" in errs[0], (bad, errs)
        errs = rt._validate_raw_rows([{"use_heater": "T", "heater_temp": "400", "heater_soak": "1h"}])
        assert len(errs) == 1 and "heater_soak" in errs[0] and "숫자" in errs[0], errs
        assert rt._validate_raw_rows([{"use_heater": "T", "heater_temp": "400", "heater_soak": "60"}]) == []
    asyncio.run(go())


# ───────────────────────── 공정 단계 ─────────────────────────
def _bare_pc(**cb):
    def nop(*_a, **_k):
        return None
    kw = dict(send_plc=nop, send_mfc=nop, send_dc_power=nop, stop_dc_power=nop, send_rf_power=nop,
              stop_rf_power=nop, send_dc_power2=nop, stop_dc_power2=nop, start_dc_pulse=nop, stop_dc_pulse=nop,
              start_rf_pulse=nop, stop_rf_pulse=nop, ig_wait=nop, cancel_ig=nop, rga_scan=nop, oes_run=nop,
              ch=2, supports_dc_cont=True, supports_rf_cont=True, supports_dc_pulse=True, supports_rf_pulse=True)
    kw.update(cb)
    return ProcessController(**kw)


def test_wait_step_carries_soak_and_blank_keeps_one_arg_call():
    async def go():
        calls = []
        pc = _bare_pc(heater_set=lambda t, r: None, heater_wait=lambda *a: calls.append(a))
        p = dict(base_pressure=7e-6, working_pressure=60, process_time=1, shutter_delay=1, use_ar=True, ar_flow=50,
                 use_dc_pulse=True, dc_pulse_power=80, use_ms=True, use_g1=True, use_heater=True, heater_temp=400.0)
        seq = pc._create_process_sequence(dict(p, heater_soak=60.0))
        assert [s.action for s in seq[:3]] == [ActionType.HEATER_SET, ActionType.HEATER_WAIT, ActionType.IG_CMD]
        assert seq[1].params == (3600.0,) and seq[1].message == "히터 400°C 도달·안정 대기 (SOAK 60분)"
        old = pc._create_process_sequence(dict(p))                       # 공란 → 예전 그대로
        assert old[1].params is None and old[1].message == "히터 400°C 도달·안정 대기"
        assert [(s.action, s.message, s.value) for s in old[2:]] == [(s.action, s.message, s.value) for s in seq[2:]]

        assert pc._send_and_collect_tokens(seq[1])[0].kind == "HEATER_READY"
        assert pc._send_and_collect_tokens(old[1])[0].kind == "HEATER_READY"
        assert calls == [(400.0, 3600.0), (400.0,)]                       # 공란이면 인자 1개(예전 호출 그대로)
        zero = pc._create_process_sequence(dict(p, heater_soak=0.0))[1]   # 0분 = 도달 즉시(공란과 다름)
        assert zero.params == (0.0,)
    asyncio.run(go())


# ───────────────────────── 400°C · SOAK 60분 (에뮬레이터) ─────────────────────────
def test_400c_soak_60min_waits_after_arrival_with_heater_on():
    async def go():
        rt, rec, svc, plc, clk, chat, t0 = await _heat(400.0, 3600.0, **REAL)
        runs = []
        await _run_until(svc, plc, clk, _done(rec), 4 * 3600,
                         on_step=lambda s, p: runs.append(bool(s.status().get("run"))))
        assert rec.names() == ["on_heater_set_ok", "on_heater_ready"], rec.calls
        assert all(runs)                                                    # 히터는 대기 내내 켜짐
        st = svc.status()
        assert st["run"] and svc.owner == "process" and abs(st["pv"] - 400.0) <= 2.0

        (t_in, m_in), = _logs(rt, "목표 도달 — SOAK 60분 시작")
        (t_ok, m_ok), = _logs(rt, "히터 준비 완료")
        assert 3600 <= t_ok - t_in <= 3601                                  # 도달 뒤 60분
        assert 50 * 60 <= t_in - t0 <= 80 * 60                              # 25→400°C RAMP(6°C/min, 감속 접근)
        assert "(SOAK 60분 포함)" in m_ok
        (_t, m_start), = _logs(rt, "를 SOAK 60분(레시피 heater_soak) 유지하면 진행")
        assert "한도 156분" in m_start and "+ SOAK 60분 + 여유 30분" in m_start
        prog = _logs(rt, "히터 SOAK — ")
        assert len(prog) >= 55 and all(t_in <= t <= t_ok for t, _m in prog)
        assert not _logs(rt, "SOAK 중단")

        reach = [c for c in chat.full if c[0] == "[CH2] 히터 도달"]
        assert len(reach) == 1 and reach[0][1] == "SUCCESS"
        f = reach[0][2]
        assert list(f) == ["목표", "도달 PV", "승온 소요", "SOAK"]
        assert f["SOAK"] == "60분 0초"
        ramp = int(t_in - t0)
        assert f["승온 소요"] == f"{ramp // 60}분 {ramp % 60}초"             # SOAK 을 뺀 시간
    asyncio.run(go())


def test_blank_soak_uses_setting_and_card_unchanged():
    async def go():
        rt, rec, svc, plc, clk, chat, t0 = await _heat(200.0, None, mv_limit=700)
        await _run_until(svc, plc, clk, _done(rec), 3 * 3600)
        assert rec.names() == ["on_heater_set_ok", "on_heater_ready"], rec.calls
        assert svc.cfg.soak_time_s == 60
        (t_in, _m), = _logs(rt, "히터 목표 도달 —")
        (t_ok, _m2), = _logs(rt, "히터 준비 완료")
        assert 60 <= t_ok - t_in <= 61
        assert _logs(rt, "±2°C 를 60초 유지하면 진행")
        reach = [c for c in chat.full if c[0] == "[CH2] 히터 도달"]
        assert list(reach[0][2]) == ["목표", "도달 PV", "승온 소요"]           # 예전 카드 그대로
    asyncio.run(go())


def test_soak_restarts_when_pv_leaves_band():
    async def go():
        rt, rec, svc, plc, clk, chat, t0 = await _heat(200.0, 600.0, mv_limit=700)
        await _run_until(svc, plc, clk, lambda: bool(_logs(rt, "SOAK 10분 시작")), 3 * 3600)
        await sim.run(svc, plc, clk, 300)
        await _tick()
        plc.tc_offset = 5.0                                                 # TC 가 5°C 높게 읽힘 → ±2°C 밖
        await sim.run(svc, plc, clk, 10)
        await _tick()
        plc.tc_offset = 0.0
        await _run_until(svc, plc, clk, _done(rec), 3600)
        assert rec.names() == ["on_heater_set_ok", "on_heater_ready"], rec.calls
        (t_out, m_out), = _logs(rt, "SOAK 중단")
        assert "목표 ±2°C 밖" in m_out and "처음부터" in m_out
        (t_re, _m), = _logs(rt, "SOAK 다시 시작(2번째)")
        (t_ok, _m2), = _logs(rt, "히터 준비 완료")
        assert t_re > t_out and 600 <= t_ok - t_re <= 601                   # 다시 들어온 뒤 10분 전부
    asyncio.run(go())


# ───────────────────────── 판정기·한도 (순수 함수) ─────────────────────────
class _Clk:
    def __init__(self):
        self.t = 1000.0                     # 실제 시계(monotonic)처럼 0 이 아닌 값에서 시작

    def __call__(self):
        return self.t


def test_waiter_restart_logs_and_timeout_message():
    clk, logs = _Clk(), []
    w = HeaterSoakWaiter(200.0, tol=2.0, soak_sec=600, timeout_s=900, clock=clk, log=logs.append)
    st = {"run": True, "fault": False, "stale": False, "pv": 200.3, "sv": 200.0, "sv_ramp": 200.0}
    assert w.tick(st, False) is None
    assert logs[-1].startswith("히터 목표 도달 — SOAK 10분 시작 (PV 200.3°C")
    clk.t = 1300
    assert w.tick(st, False) is None
    assert any(m.startswith("히터 SOAK — 5분 / 10분") for m in logs)
    clk.t = 1301
    assert w.tick({**st, "pv": 203.0}, False) is None
    assert logs[-1] == "히터 SOAK 중단 — PV 203.0°C 가 목표 ±2°C 밖, 5분 유지 후 → 다시 들어오면 처음부터"
    clk.t = 1310
    assert w.tick(st, False) is None and logs[-1] == "히터 SOAK 다시 시작(2번째) — PV 200.3°C"
    clk.t = 1900
    ok, msg, code = w.tick(st, False)
    assert ok is False and code == "E831" and "(SOAK 2번 시작했으나 10분 유지 못 함)" in msg


def test_timeout_adds_soak_and_cap_applies_to_ramp_only():
    kw = dict(base_timeout_s=5400, approach_zone=20.0, approach_min_rate=1.0)
    ramp = ramp_minutes(375.0, 6.0, 20.0, 1.0) * 60.0
    t, why = heater_wait_timeout_s(25.0, 400.0, 6.0, soak_sec=3600, **kw)
    assert t == pytest.approx(ramp + 3600 + WAIT_TIMEOUT_MARGIN_SEC) and round(t / 60) == 156
    assert "예상 승온 66분" in why and "+ SOAK 60분" in why
    t60, _w = heater_wait_timeout_s(25.0, 400.0, 6.0, soak_sec=60, **kw)       # 설정값 60초 — 예전과 같은 값
    assert t60 == pytest.approx(ramp + 60 + WAIT_TIMEOUT_MARGIN_SEC)
    slow = ramp_minutes(875.0, 2.0, 20.0, 1.0) * 60.0                          # 900°C · 2°C/min · SOAK 8시간
    t, why = heater_wait_timeout_s(25.0, 900.0, 2.0, soak_sec=28800, **kw)
    assert t == pytest.approx(slow + 28800 + WAIT_TIMEOUT_MARGIN_SEC) and t > WAIT_TIMEOUT_MAX_SEC   # 예전: 12시간에 잘림
    t, why = heater_wait_timeout_s(25.0, 900.0, 0.1, soak_sec=600, **kw)        # RAMP 만으로 12시간 넘음
    assert t == WAIT_TIMEOUT_MAX_SEC + 600 and "상한 12시간 + SOAK" in why
    t, why = heater_wait_timeout_s(None, 400.0, 6.0, soak_sec=3600, **kw)       # 현재 온도 미상 — 예전 그대로
    assert t == 5400 and "기본값" in why


# ───────────────────────── 템플릿 ─────────────────────────
def test_template_has_soak_column_and_current_heater_values():
    with open(CSV_PATH, encoding="utf-8-sig", newline="") as fp:
        rows = list(csv.reader(fp))
    assert rows[0][-1] == "heater_soak" and all(len(r) == len(rows[0]) for r in rows)

    wb = load_workbook(XLSX_PATH)
    ws, desc = wb["Recipe"], wb["설명"]
    assert ws["AO1"].value == "heater_soak" and ws["AO1"].comment.text.startswith("단위: min\n")
    for r in (1, 2, 61):                                                        # 서식은 옆 열(heater_ramp)과 같게
        assert tuple(ws.cell(r, 41)._style) == tuple(ws.cell(r, 40)._style)
    dvs = {str(dv.sqref): dv for dv in ws.data_validations.dataValidation}
    dv = dvs["AO2:AO61"]
    assert (dv.type, dv.formula1, float(dv.formula2)) == ("decimal", "0", cr.HEATER_SOAK_MAX_MIN)
    assert dv.promptTitle == "heater_soak [min]"
    assert [desc.cell(45, c).value for c in (1, 2)] == ["heater_soak", "min"]

    # 설명 문구가 코드·설정 기본값과 맞는지(예전: 목표 1~500, 램프 기본 12, '60초 머물면')
    lim = min(cfgc.HEATER_MAX_TEMP, cfgc.HEATER_SV_LIMIT_C)
    assert float(dvs["AM2:AM61"].formula2) == lim and f"1~{lim:g}" in dvs["AM2:AM61"].prompt
    assert f"HEATER_RAMP_RATE_C_PER_MIN, {cfgc.HEATER_RAMP_RATE_C_PER_MIN:g})" in dvs["AN2:AN61"].prompt
    assert f"HEATER_SOAK_TIME_SEC, {cfgc.HEATER_SOAK_TIME_SEC:g}초)" in dv.prompt
    assert f"±{cfgc.HEATER_SOAK_TOLERANCE:g}°C" in dv.prompt
    texts = [ws.cell(1, c).comment.text for c in range(1, ws.max_column + 1)]
    texts += [str(d.prompt) for d in dvs.values()] + [str(d.error) for d in dvs.values()]
    texts += [str(desc.cell(r, c).value) for r in range(1, desc.max_row + 1) for c in range(1, 5)]
    for old in ("1~500", "RATE_C_PER_MIN, 12)", "60초 머물면", "'예열 대기'"):
        assert not [t for t in texts if old in t], old


def test_template_rows_load_and_normalize():
    """엑셀 템플릿을 로더처럼 읽어(첫 시트, 1행 머리글) 전수 검증·정규화 — heater_soak 열이 있어도 그대로 통과."""
    async def go():
        rt, _pc, _rec = _rt()
        wb = load_workbook(XLSX_PATH, read_only=True, data_only=True)
        try:
            it = wb.worksheets[0].iter_rows(values_only=True)
            head = [str(h).strip() if h is not None else "" for h in next(it)]
            rows = []
            for vals in it:
                row = {h: ("" if v is None else str(v).strip()) for h, v in zip(head, vals) if h}
                if any(row.values()):
                    rows.append(row)
        finally:
            wb.close()
        assert "heater_soak" in head and len(rows) == 2
        assert rt._validate_raw_rows(rows) == []
        n = rt._normalize_params_for_process(rows[1])
        assert n["use_heater"] is False and n["heater_soak"] is None
    asyncio.run(go())


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
