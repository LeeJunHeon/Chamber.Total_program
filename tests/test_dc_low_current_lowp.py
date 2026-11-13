# -*- coding: utf-8 -*-
"""DC Power(PLC 경유) 저전류 감시 — 설정 파워 50 W 이하는 기준 0.01 A, 초과는 기존 기준(DC_LOW_CURRENT_THRESH_A).

배경(2026-11-13 CH2 'Si_35W'): 정상 전류가 0.05 A 근처인 저파워 공정이
"저전류 연속 3/3 (meas=0.049A ≤ 0.050A)" 로 중단됐다(오탐). 장치·PLC 접근 없음.

A. lib/power_limits.dc_low_current_thresh_a 순수 함수
B. DCPowerAsync._control_loop — 상태 읽기 스텁으로 측정값 주입
C. 런 시작 상태 로그 1줄
D. settings.json(실제 로더) · 워커 모드
"""
import ast
import os
import re
import sys
import asyncio
import contextlib
import inspect
from pathlib import Path

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                   # noqa: E402
from lib import config_common as cfgc, config_ch1, config_ch2   # noqa: E402
from lib import _config_loader as L                             # noqa: E402
from lib.power_limits import dc_low_current_thresh_a, dc_low_current_lowp_cfg   # noqa: E402
from device.dc_power import DCPowerAsync                        # noqa: E402

BASE = 0.05
NAN = float("nan")
INF = float("inf")
REPO_JSON = Path(_ROOT) / "config" / "settings.json"


@pytest.fixture(autouse=True)
def _defaults(monkeypatch):
    """다른 테스트가 settings.json 을 적용한 채 남겨도 순서 무관하게 — 이 파일이 기대하는 값으로 고정."""
    for k, v in dict(DC_LOW_CURRENT_THRESH_A=0.05, DC_LOW_CURRENT_STREAK_N=3,
                     DC_LOW_CURRENT_LOWP_MAX_W=50.0, DC_LOW_CURRENT_LOWP_THRESH_A=0.01,
                     DC_LOW_W_THRESH=1.0, DC_LOW_STREAK_N=3,
                     DC_OVERCURRENT_ENABLE_CONT=True, DC_OVERCURRENT_P1_W=300.0, DC_OVERCURRENT_I1_A=1.0,
                     DC_OVERCURRENT_P2_W=600.0, DC_OVERCURRENT_I2_A=2.0, DC_OVERCURRENT_STREAK_N=1).items():
        monkeypatch.setattr(cfgc, k, v, raising=False)
    yield monkeypatch


@pytest.fixture
def restore_cfg():
    """load_settings 가 바꾼 모듈 값을 테스트 뒤 원복."""
    mods = (cfgc, config_ch1, config_ch2)
    snap = [dict(vars(md)) for md in mods]
    yield
    for md, d in zip(mods, snap):
        for k in [k for k in vars(md) if k not in d]:
            delattr(md, k)
        for k, v in d.items():
            setattr(md, k, v)


# ───────────────────────── A: 순수 함수 ─────────────────────────
@pytest.mark.parametrize("p", [35, 35.0, 0.1, 50, 50.0])
def test_a1_low_power_uses_lowp_thresh(p):
    assert dc_low_current_thresh_a(p, BASE) == 0.01


@pytest.mark.parametrize("p", [50.1, 51, 300, 1000])
def test_a2_above_max_is_base(p):
    assert dc_low_current_thresh_a(p, BASE) == BASE


@pytest.mark.parametrize("p", [None, 0, 0.0, -1, -35.0, NAN, INF, "abc", object()])
def test_a3_bad_setpoint_is_base(p):
    assert dc_low_current_thresh_a(p, BASE) == BASE


@pytest.mark.parametrize("max_w", [0, 0.0, -10.0, NAN, INF, "x", None])
def test_a4_feature_off_or_bad_max_is_base(_defaults, max_w):
    _defaults.setattr(cfgc, "DC_LOW_CURRENT_LOWP_MAX_W", max_w, raising=False)
    assert dc_low_current_thresh_a(35, BASE) == BASE
    assert dc_low_current_lowp_cfg() is None


@pytest.mark.parametrize("thr", [-0.01, NAN, INF, -INF, "x", None])
def test_a5_bad_lowp_thresh_is_base(_defaults, thr):
    _defaults.setattr(cfgc, "DC_LOW_CURRENT_LOWP_THRESH_A", thr, raising=False)
    assert dc_low_current_thresh_a(35, BASE) == BASE


def test_a6_base_passthrough_and_runtime_change(_defaults):
    assert dc_low_current_thresh_a(300, 0.07) == 0.07           # base 는 받은 값 그대로
    _defaults.setattr(cfgc, "DC_LOW_CURRENT_LOWP_MAX_W", 100.0, raising=False)
    _defaults.setattr(cfgc, "DC_LOW_CURRENT_LOWP_THRESH_A", 0.02, raising=False)
    assert dc_low_current_thresh_a(80, BASE) == 0.02            # 매 호출 때 config_common 을 읽는다
    _defaults.setattr(cfgc, "DC_LOW_CURRENT_LOWP_THRESH_A", 0.0, raising=False)
    assert dc_low_current_thresh_a(80, BASE) == 0.0             # 0 은 유효(전류가 0 일 때만 저전류)


def test_a7_reads_config_common_not_channel_copies(_defaults):
    """채널 모듈(import 시점 복사본)에 같은 이름이 있어도 보지 않는다."""
    for m in (config_ch1, config_ch2):
        _defaults.setattr(m, "DC_LOW_CURRENT_LOWP_MAX_W", 0.0, raising=False)
        _defaults.setattr(m, "DC_LOW_CURRENT_LOWP_THRESH_A", 0.04, raising=False)
    assert dc_low_current_thresh_a(35, BASE) == 0.01


# ───────────────────────── B: 드라이버 ─────────────────────────
def _mk(target_w, current_a, *, reached=True):
    """폴링 루프에 상태 읽기 스텁을 넣어 update_measurements 경로로 측정값을 주입."""
    async def _read():
        return (float(target_w), 300.0, float(current_a))

    async def _toggle(on):
        return None

    async def _send(w):
        return None

    d = DCPowerAsync(send_dc_power=_send, send_dc_power_unverified=_send,
                     request_status_read=_read, toggle_enable=_toggle, name="DCTEST")
    d._dc_interval_ms = 50
    d.target_power = float(target_w)
    d._is_running = True
    d._polling_enabled = True
    d._sent_target_reached = reached
    d.cleaned = []

    async def _cleanup():
        d.cleaned.append(1)
        d._is_running = False

    async def _adjust_once():
        return None

    d.cleanup = _cleanup
    d._adjust_once = _adjust_once
    return d


def _run_loop(d, seconds=0.4):
    async def _main():
        t = asyncio.create_task(d._control_loop())
        await asyncio.sleep(seconds)
        d._is_running = False
        with contextlib.suppress(Exception):
            await asyncio.wait_for(t, timeout=1.0)
        await asyncio.sleep(0.05)
    asyncio.run(_main())
    evs = []
    while not d._event_q.empty():
        evs.append(d._event_q.get_nowait())
    return evs


def _failed(evs):
    return [e for e in evs if e.kind == "target_failed"]


def _status(evs):
    return [str(e.message) for e in evs if e.kind == "status"]


def test_b1_35w_0p049a_does_not_stop():
    """11-13 오탐 재현 조건 — 이제 중단되지 않고 저전류 감시 메시지도 없다."""
    d = _mk(35.0, 0.049)
    assert d._dc_low_current_thresh_a == 0.05 and d._dc_low_current_streak_n == 3
    evs = _run_loop(d)
    assert _failed(evs) == [] and d.cleaned == []
    assert d._low_current_streak == 0
    assert not any("저전류" in m for m in _status(evs))


def test_b2_35w_0p005a_stops_with_lowp_thresh_in_message():
    d = _mk(35.0, 0.005)
    evs = _run_loop(d)
    tf = _failed(evs)
    assert len(tf) == 1 and tf[0].message == "저전류 연속 3/3 (meas=0.005A ≤ 0.010A)", tf
    assert d.cleaned == [1]
    st = _status(evs)
    assert any("저전류 감시(목표 도달 후): 1/3 (meas=0.005A ≤ 0.010A)" in m for m in st), st
    assert any("DC 전류가 목표 도달 후 3회 연속 ≤ 0.010A → 공정 중단" in m for m in st), st
    assert not any("0.050A" in m for m in st)


def test_b3_300w_0p049a_stops_as_before():
    d = _mk(300.0, 0.049)
    evs = _run_loop(d)
    tf = _failed(evs)
    assert len(tf) == 1 and tf[0].message == "저전류 연속 3/3 (meas=0.049A ≤ 0.050A)", tf
    assert d.cleaned == [1]
    st = _status(evs)
    assert any("저전류 감시(목표 도달 후): 1/3 (meas=0.049A ≤ 0.050A)" in m for m in st), st
    assert any("DC 전류가 목표 도달 후 3회 연속 ≤ 0.050A → 공정 중단" in m for m in st), st


def test_b4_boundaries():
    assert _failed(_run_loop(_mk(50.0, 0.049))) == []                        # 50 W 는 저파워 구간
    assert "≤ 0.050A" in _failed(_run_loop(_mk(50.1, 0.049)))[0].message     # 50 W 초과 → 기존 기준
    assert "≤ 0.010A" in _failed(_run_loop(_mk(35.0, 0.010)))[0].message     # 기준과 같으면 저전류(≤)


def test_b5_feature_off_restores_old_behavior(_defaults):
    _defaults.setattr(cfgc, "DC_LOW_CURRENT_LOWP_MAX_W", 0.0, raising=False)
    tf = _failed(_run_loop(_mk(35.0, 0.049)))
    assert len(tf) == 1 and tf[0].message == "저전류 연속 3/3 (meas=0.049A ≤ 0.050A)"


def test_b6_target_reached_gate_unchanged():
    """목표 도달 전에는 저파워 구간이어도 저전류로 세지 않는다(게이트 그대로)."""
    d = _mk(35.0, 0.005, reached=False)
    evs = _run_loop(d)
    assert _failed(evs) == [] and d._low_current_streak == 0


# ───────────────────────── C: 런 시작 로그 ─────────────────────────
def _start_log(target_w):
    d = _mk(target_w, 0.1)
    out = []

    async def _emit(msg):
        out.append(msg)

    d._emit_status = _emit
    asyncio.run(d._emit_low_current_thresh_status())
    return out


def test_c1_start_log_lines(_defaults):
    assert _start_log(35.0) == ["저전류 감시 기준: 0.010 A (설정 35.0 W ≤ 50.0 W 저파워 구간)"]
    assert _start_log(50.0) == ["저전류 감시 기준: 0.010 A (설정 50.0 W ≤ 50.0 W 저파워 구간)"]
    assert _start_log(300.0) == ["저전류 감시 기준: 0.050 A"]
    _defaults.setattr(cfgc, "DC_LOW_CURRENT_LOWP_MAX_W", 0.0, raising=False)
    assert _start_log(35.0) == ["저전류 감시 기준: 0.050 A"]


def test_c2_start_process_emits_it_right_after_target_power():
    src = inspect.getsource(DCPowerAsync.start_process)
    i_tp = src.index("self.target_power = ")
    i_log = src.index("await self._emit_low_current_thresh_status()")
    i_toggle = src.index("if not self._toggle_enable")
    assert i_tp < i_log < i_toggle


def test_c3_dc_pulse_untouched():
    src = (Path(_ROOT) / "device" / "dc_pulse.py").read_text(encoding="utf-8")
    assert "dc_low_current_thresh_a" not in src and "DC_LOW_CURRENT_LOWP" not in src


# ───────────────────────── D: settings.json ─────────────────────────
def test_d1_repo_settings_loads_and_applies_new_keys(_defaults, restore_cfg):
    _defaults.setattr(cfgc, "DC_LOW_CURRENT_LOWP_MAX_W", 7.0, raising=False)
    _defaults.setattr(cfgc, "DC_LOW_CURRENT_LOWP_THRESH_A", 0.5, raising=False)
    res = L.check_and_load_settings(REPO_JSON)
    assert res.problems == [], res.problems
    assert cfgc.DC_LOW_CURRENT_LOWP_MAX_W == 50.0
    assert cfgc.DC_LOW_CURRENT_LOWP_THRESH_A == 0.01
    assert cfgc.DC_LOW_CURRENT_THRESH_A == 0.05 and cfgc.DC_LOW_CURRENT_STREAK_N == 3
    assert cfgc.DC_OVERCURRENT_I1_A == 1.5                        # 장비 노트북 값
    assert dc_low_current_thresh_a(35, cfgc.DC_LOW_CURRENT_THRESH_A) == 0.01
    assert dc_low_current_thresh_a(300, cfgc.DC_LOW_CURRENT_THRESH_A) == 0.05


def test_d2_code_defaults():
    """settings.json 이 없을 때의 기본값(소스 기준) — 새 키 2개, 과전류 I1 기본값은 1.0 그대로."""
    src = (Path(_ROOT) / "lib" / "config_common.py").read_text(encoding="utf-8")
    assert re.search(r"^DC_LOW_CURRENT_LOWP_MAX_W\s*=\s*50\.0\b", src, re.M)
    assert re.search(r"^DC_LOW_CURRENT_LOWP_THRESH_A\s*=\s*0\.01\b", src, re.M)
    assert re.search(r"^DC_OVERCURRENT_I1_A\s*=\s*1\.0\b", src, re.M)


def _worker_keys(rel):
    tree = ast.parse((Path(_ROOT) / rel).read_text(encoding="utf-8"))
    for n in tree.body:
        if isinstance(n, ast.Assign) and any(getattr(t, "id", "") == "SETTINGS_KEYS" for t in n.targets):
            return tuple(ast.literal_eval(n.value))
    raise AssertionError(f"{rel}: SETTINGS_KEYS 없음")


@pytest.mark.parametrize("rel", ["apps/log_sync_service/log_sync.py", "apps/rf_service/rf_worker.py"])
def test_d3_workers_do_not_stop_on_new_common_keys(restore_cfg, rel):
    keys = _worker_keys(rel)
    assert not any(k.startswith("DC_LOW_CURRENT") for k in keys)       # 워커는 이 키를 쓰지 않는다
    res = L.check_and_load_settings(REPO_JSON, worker_keys=keys)
    assert res.problems == [], (rel, res.problems)


@pytest.mark.parametrize("rel", ["apps/log_sync_service/log_sync.py", "apps/rf_service/rf_worker.py"])
def test_d4_old_worker_build_ignores_new_keys(restore_cfg, rel):
    """새 키를 모르는(예전 빌드) 워커 — config_common 에 이름이 없어도 멈추지 않고 무시한다."""
    delattr(cfgc, "DC_LOW_CURRENT_LOWP_MAX_W")
    delattr(cfgc, "DC_LOW_CURRENT_LOWP_THRESH_A")
    res = L.check_and_load_settings(REPO_JSON, worker_keys=_worker_keys(rel))
    assert res.problems == [], (rel, res.problems)
    assert sorted(k for k in res.ignored if "LOWP" in k) == [
        "common.DC_LOW_CURRENT_LOWP_MAX_W", "common.DC_LOW_CURRENT_LOWP_THRESH_A"]
    assert not hasattr(cfgc, "DC_LOW_CURRENT_LOWP_MAX_W")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
