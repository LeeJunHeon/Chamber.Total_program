# -*- coding: utf-8 -*-
"""DC 과전류 인터락 설정 출처 검증 — settings.json common 단일 출처 (장치·PLC·네트워크 접근 없음).

config_ch1/ch2 는 `from .config_common import *` 로 import 시점에 값을 복사하고,
lib._config_loader.load_settings() 는 그 뒤 config_common 만 바꾼다. 그래서 드라이버가 채널 모듈을
먼저 보면 common 의 on/off·연속횟수·점화감시 변경이 반영되지 않았다(실측: common=False → 드라이버 판정 True).
여기서는 monkeypatch 로 config_common 만 바꾸지 않고, 임시 settings.json 으로 실제 load_settings() 를 돌린다.

(a) common.DC_OVERCURRENT_ENABLE=false → cfg=config_ch2 DC Pulse 가 2.71A@190W 에서 차단하지 않음
(b) common true → 같은 조건에서 차단
(c) common.DC_OVERCURRENT_STREAK_N=2 → 첫 초과는 경고, 두 번째에 차단
(d) common.DC_OVERCURRENT_ARM_ON_START=false → 활성화 대기 감시가 PIV 를 읽지 않음
(e) common.DC_OVERCURRENT_ENABLE_CONT=false → cfg=config_ch2 DC Power 가 차단하지 않음
(f) 런 시작 로그 — ON/OFF 문구가 _emit_status 로 1회

pytest 로도, `python tests/test_dc_overcurrent_cfg_source.py` 로도 실행된다.
"""
import os
import sys
import json
import asyncio
import tempfile
import contextlib
from pathlib import Path

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                              # noqa: E402
from lib import config_common as cfgc                      # noqa: E402
from lib import config_ch1, config_ch2                     # noqa: E402
from lib import _config_loader                             # noqa: E402
from device.dc_pulse import AsyncDCPulse                   # noqa: E402
from device.dc_power import DCPowerAsync                   # noqa: E402

_MODS = (cfgc, config_ch1, config_ch2)

# 이 테스트의 기준값(프로젝트 기본값과 같음). 각 테스트는 여기서 바꿀 키만 덮어쓴다.
_BASE = {
    "DC_OVERCURRENT_ENABLE": True, "DC_OVERCURRENT_ENABLE_CONT": True,
    "DC_OVERCURRENT_P1_W": 300.0, "DC_OVERCURRENT_I1_A": 1.0,
    "DC_OVERCURRENT_P2_W": 600.0, "DC_OVERCURRENT_I2_A": 2.0,
    "DC_OVERCURRENT_STREAK_N": 1, "DC_OVERCURRENT_EXTRAPOLATE": True,
    "DC_OVERCURRENT_ARM_ON_START": True, "DC_OVERCURRENT_ARM_INTERVAL_S": 0.5,
}


@pytest.fixture(autouse=True)
def _restore_modules():
    """load_settings 가 바꾼 모듈 값을 테스트 뒤 원복(모든 DC_OVERCURRENT_* — common/ch1/ch2)."""
    snap = {m: {k: getattr(m, k) for k in dir(m) if k.startswith("DC_OVERCURRENT_")} for m in _MODS}
    yield
    for m, kv in snap.items():
        for k in [k for k in dir(m) if k.startswith("DC_OVERCURRENT_")]:
            if k not in kv:
                delattr(m, k)
        for k, v in kv.items():
            setattr(m, k, v)


def _load_common(**over):
    """임시 settings.json(JSONC, common 섹션만)을 만들어 실제 load_settings(path) 로 적용."""
    common = dict(_BASE)
    common.update(over)
    body = ("{\n  // 테스트용 임시 settings.json — common 섹션만\n  \"common\": "
            + json.dumps(common, ensure_ascii=False) + "\n}\n")
    d = Path(tempfile.mkdtemp())
    p = d / "settings.json"
    p.write_text(body, encoding="utf-8")
    data = _config_loader.load_settings(p)
    assert isinstance(data, dict) and data.get("common"), "load_settings 실패"
    for k, v in over.items():
        assert getattr(cfgc, k) == v, (k, getattr(cfgc, k))      # common 에는 적용됐다
    return data


def _mk_dcp():
    """cfg=config_ch2 를 쓰는 실제 AsyncDCPulse — _cfg_* 는 실제 메서드 그대로."""
    d = AsyncDCPulse.__new__(AsyncDCPulse)
    d._cfg = config_ch2
    d._overcurr_n = 0
    d._last_ref_power_w = None
    d.status, d.offs, d.events_out = [], [], []

    async def _emit(msg):
        d.status.append(msg)

    async def _off():
        d.offs.append(1)

    async def _fault():
        return 0

    d._emit_status = _emit
    d.output_off = _off
    d.read_fault_code = _fault
    d._ev_nowait = lambda ev: d.events_out.append(ev)
    return d


def _check(d, ref_w=190.0, i=2.71):
    d._last_ref_power_w = ref_w
    p = float(ref_w)
    v = p / i
    return asyncio.run(d._check_overcurrent(p, v, i, {"P_W": p, "V_V": v, "I_A": i}, None))


def _stops(d):
    return [e for e in d.events_out if e.kind == "command_failed" and e.cmd == "AUTO_STOP"]


# ───────────────────────── (a)(b) DC Pulse on/off ─────────────────────────
def test_a_common_disable_is_honored_by_ch2_driver():
    _load_common(DC_OVERCURRENT_ENABLE=False)
    # 함정 재현: 채널 모듈에는 import 시점 값(True)이 남아 있다 — 드라이버는 이것을 보면 안 된다
    assert config_ch2.DC_OVERCURRENT_ENABLE is True
    d = _mk_dcp()
    assert _check(d) is False
    assert d.offs == [] and _stops(d) == []


def test_b_common_enable_trips():
    _load_common(DC_OVERCURRENT_ENABLE=True)
    d = _mk_dcp()
    assert _check(d) is True
    assert d.offs == [1] and len(_stops(d)) == 1


# ───────────────────────── (c) STREAK_N ─────────────────────────
def test_c_common_streak_n_two():
    _load_common(DC_OVERCURRENT_STREAK_N=2)
    assert config_ch2.DC_OVERCURRENT_STREAK_N == 1                 # 채널 모듈은 옛 값
    d = _mk_dcp()
    assert _check(d) is False                                      # 첫 초과 → 경고만
    assert d.offs == [] and any("[WARN] 과전류 감지" in m and "(1/2)" in m for m in d.status), d.status
    assert _check(d) is True                                       # 두 번째 → 차단
    assert d.offs == [1] and len(_stops(d)) == 1


# ───────────────────────── (d) ARM_ON_START ─────────────────────────
def test_d_common_arm_off_no_piv_reads():
    _load_common(DC_OVERCURRENT_ARM_ON_START=False)
    assert config_ch2.DC_OVERCURRENT_ARM_ON_START is True
    d = _mk_dcp()
    d._activation_check_delay_s = 0.05
    d._last_ref_power_w = 190.0
    reads = []

    async def _read():
        reads.append(1)
        return {"raw": {}, "eng": {"P_W": 190.0, "V_V": 70.0, "I_A": 2.71}}
    d.read_output_piv = _read
    assert asyncio.run(d._watch_overcurrent_during_activation()) is False
    assert reads == [] and d.offs == []

    # 대조: common true 면 읽고 차단한다
    _load_common(DC_OVERCURRENT_ARM_ON_START=True)
    d2 = _mk_dcp()
    d2._activation_check_delay_s = 0.05
    d2._last_ref_power_w = 190.0
    d2.read_output_piv = _read
    assert asyncio.run(d2._watch_overcurrent_during_activation()) is True
    assert reads and d2.offs == [1]


# ───────────────────────── (e) DC Power ENABLE_CONT ─────────────────────────
def _mk_dcpower():
    async def _noop(*a, **k):
        return None
    d = DCPowerAsync(send_dc_power=_noop, send_dc_power_unverified=_noop,
                     request_status_read=None, toggle_enable=_noop, cfg=config_ch2, name="DCTEST")
    d.status = []

    async def _emit(msg):
        d.status.append(msg)
    d._emit_status = _emit
    d.events_out = []
    d._ev_nowait = lambda ev: d.events_out.append(ev)
    return d


def test_e_common_disable_cont_is_honored_by_ch2_driver():
    _load_common(DC_OVERCURRENT_ENABLE_CONT=False)
    assert config_ch2.DC_OVERCURRENT_ENABLE_CONT is True
    d = _mk_dcpower()
    d.target_power = 300.0
    d.current_a = 1.5                                              # 300W 한계 1.0A 초과
    assert asyncio.run(d._check_overcurrent()) is False
    assert d.events_out == []

    _load_common(DC_OVERCURRENT_ENABLE_CONT=True, DC_OVERCURRENT_STREAK_N=1)
    d2 = _mk_dcpower()
    d2.target_power = 300.0
    d2.current_a = 1.5
    assert asyncio.run(d2._check_overcurrent()) is True
    assert [e.kind for e in d2.events_out] == ["target_failed"]


# ───────────────────────── (f) 런 시작 로그 ─────────────────────────
def test_f1_dc_pulse_start_log_on_off():
    _load_common(DC_OVERCURRENT_ENABLE=True, DC_OVERCURRENT_STREAK_N=2, DC_OVERCURRENT_ARM_ON_START=False)
    d = _mk_dcp()
    d._last_ref_power_w = 450.0
    asyncio.run(d._emit_overcurrent_arm_status())
    oc = [m for m in d.status if m.startswith("[overcurrent]")]
    assert oc == ["[overcurrent] 인터락 ON — 한계 1.50A (설정 450W, 연속 2회, 점화 감시 OFF)"], d.status

    _load_common(DC_OVERCURRENT_ENABLE=False)
    d = _mk_dcp()
    d._last_ref_power_w = 190.0
    asyncio.run(d._emit_overcurrent_arm_status())
    oc = [m for m in d.status if m.startswith("[overcurrent]")]
    assert oc == ["[overcurrent] ⚠ 인터락 OFF (settings.json common.DC_OVERCURRENT_ENABLE=false) "
                  "— 과전류로 정지하지 않음"], d.status

    # 로그 출력이 실패해도 예외를 내지 않는다(공정 진행)
    async def _boom(msg):
        raise RuntimeError("log down")
    d._emit_status = _boom
    asyncio.run(d._emit_overcurrent_arm_status())

    # 위치: _prepare_and_start_impl 안에서 OUTPUT_ON 바로 앞(세트포인트 설정 뒤)
    src = open(os.path.join(_ROOT, "device", "dc_pulse.py"), encoding="utf-8").read()
    i0 = src.index("async def _prepare_and_start_impl(")
    i_ref = src.index("ok = await self.set_reference_power(power_w)", i0)
    i_log = src.index("await self._emit_overcurrent_arm_status()", i0)
    i_on = src.index("ok2 = await self.output_on()", i0)
    assert i_ref < i_log < i_on


def test_f2_dc_power_start_process_logs_once():
    async def _run(**over):
        _load_common(**over)
        d = _mk_dcpower()
        await d.start_process(300.0)
        with contextlib.suppress(Exception):
            d._is_running = False
            if d._adjust_task:
                d._adjust_task.cancel()
                with contextlib.suppress(BaseException):
                    await d._adjust_task
        return [m for m in d.status if m.startswith("[overcurrent]")]

    oc = asyncio.run(_run(DC_OVERCURRENT_ENABLE_CONT=True, DC_OVERCURRENT_STREAK_N=3))
    assert oc == ["[overcurrent] 인터락 ON — 한계 1.00A (설정 300W, 연속 3회)"], oc
    oc = asyncio.run(_run(DC_OVERCURRENT_ENABLE_CONT=False))
    assert oc == ["[overcurrent] ⚠ 인터락 OFF (settings.json common.DC_OVERCURRENT_ENABLE_CONT=false) "
                  "— 과전류로 정지하지 않음"], oc


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
