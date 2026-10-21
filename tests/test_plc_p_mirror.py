# -*- coding: utf-8 -*-
"""PLC P 영역 미러(P0000~P0004 → M0050~M0054, 2026-10-16 래더) — 주소 변환·이름 해석·쓰기 거절,
코일 로그 열/블록 계획 불변, 로드락 진공 진단이 P 신호 5개를 bool 로 읽는지."""
from __future__ import annotations

import asyncio
import hashlib
import os
import sys
import types

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from device.plc import (AsyncPLC, PLCError, PLC_COIL_MAP, PLC_P_MIRROR_MAP,   # noqa: E402
                        p_mirror_coil)
from host.handlers import HostHandlers                                        # noqa: E402

P_NAMES = {"L_GAUGE_A": 803, "L_ATM_SENSOR": 809, "L_R_P_OUT": 848, "L_R_V_OUT": 849, "L_VENT_OUT": 852}


def _plc() -> AsyncPLC:
    return AsyncPLC(ip="127.0.0.1", port=1502, unit=1, timeout_s=0.5)


@pytest.mark.parametrize("p, coil", [
    ("P00003", 803), ("P00009", 809), ("P00030", 848), ("P00031", 849), ("P00034", 852),
    ("P00000", 800), ("P0004F", 879),
])
def test_p_mirror_coil(p, coil):
    assert p_mirror_coil(p) == coil


@pytest.mark.parametrize("p", ["P00050", "P00060", "P0006F", "M00030", "P"])
def test_p_mirror_coil_out_of_range(p):
    with pytest.raises(KeyError):
        p_mirror_coil(p)


def test_mirror_map_values():
    assert PLC_P_MIRROR_MAP == P_NAMES
    assert not set(PLC_P_MIRROR_MAP) & set(PLC_COIL_MAP)


def test_addr_resolves_mirror_names():
    p = _plc()
    for name, coil in P_NAMES.items():
        assert p._addr(name) == coil
    with pytest.raises(KeyError):
        p._addr("L_GAUGE_B")


@pytest.mark.parametrize("addr", [800, 803, 848, 852, 879])
def test_write_to_mirror_coil_rejected(addr):
    p = _plc()
    calls = []

    async def _no_io(*a, **k):
        calls.append(a)
        raise AssertionError("미러 코일 쓰기가 Modbus 까지 갔다")

    p._run_io = _no_io
    with pytest.raises(PLCError) as ei:
        asyncio.run(p.write_coil(addr, True))
    assert ei.value.addr == addr and "읽기 전용" in str(ei.value)
    assert calls == []


def test_write_switch_mirror_name_rejected():
    p = _plc()
    p._run_io = None   # 쓰기 거절이 먼저라 불리지 않는다
    with pytest.raises(PLCError):
        asyncio.run(p.write_switch("L_R_P_OUT", True))


@pytest.mark.parametrize("addr", [799, 880])
def test_write_next_to_mirror_not_rejected(addr):
    p = _plc()
    seen = []

    async def _io(op, a, fn, *args, **k):
        seen.append((fn, args))
        return types.SimpleNamespace(isError=lambda: False)

    p._run_io = _io
    p._ensure_ok = lambda *a, **k: None
    asyncio.run(p.write_coil(addr, True))
    assert seen == [("write_coil", (addr, True))]


# ── 코일 로그 열·블록 계획은 수정 전과 같다 ──
KEYS_N, KEYS_SHA = 124, "5de4dd2492cb"
PLANS_BEFORE = {
    0: [(1, 1600), (2400, 641)],
    1: [(1, 481), (1600, 1), (2400, 481), (3040, 1)],
    2: [(1, 239), (320, 74), (480, 2), (1600, 1), (2400, 1), (2560, 1), (2720, 1), (2880, 1), (3040, 1)],
    3: [(1, 3), (33, 20), (65, 19), (96, 5), (112, 6), (128, 6), (160, 1), (176, 6), (192, 48), (320, 1),
        (356, 6), (370, 3), (388, 6), (480, 2), (1600, 1), (2400, 1), (2560, 1), (2720, 1), (2880, 1), (3040, 1)],
}


def _sha(keys):
    return hashlib.sha1("|".join(keys).encode()).hexdigest()[:12]


def test_coil_map_keys_unchanged():
    keys = list(PLC_COIL_MAP)
    assert (len(keys), _sha(keys)) == (KEYS_N, KEYS_SHA)
    assert not any(k in PLC_COIL_MAP for k in P_NAMES)


def test_coil_log_default_keys_unchanged():
    import inspect
    src = inspect.getsource(AsyncPLC)
    # 코일 로그 기본 키 = PLC_COIL_MAP 전체(미러 이름표는 들어가지 않는다)
    assert "self._plc_coil_log_keys = list(keys) if keys is not None else list(PLC_COIL_MAP.keys())" in src
    keys = list(PLC_COIL_MAP.keys())
    assert _sha(keys) == KEYS_SHA
    assert not set(keys) & set(P_NAMES)


@pytest.mark.parametrize("idx", [0, 1, 2, 3])
def test_coil_log_block_plan_unchanged(idx):
    p = _plc()
    addrs = [PLC_COIL_MAP[k] for k in PLC_COIL_MAP]
    used, ranges = p._coil_ranges_for_plan(addrs, idx)
    assert used == idx
    assert [tuple(r) for r in ranges] == PLANS_BEFORE[idx]


# ── 로드락 진공 진단: P 신호 5개가 bool 로 ──
def test_loadlock_vacuum_diag_reads_mirror_bits():
    p = _plc()
    coils = {803: True, 809: False, 848: True, 849: False, 852: True}
    read = []

    async def _read_coil(addr):
        read.append(addr)
        return bool(coils.get(addr, False))

    async def _nop():
        return None

    p.read_coil = _read_coil
    p.pause_watchdog = _nop
    p.resume_watchdog = _nop

    h = HostHandlers.__new__(HostHandlers)
    h.ctx = types.SimpleNamespace(plc=p, lock_plc=asyncio.Lock())
    h._plc_file_logger = lambda *a, **k: None

    async def _go():
        h.ctx.lock_plc = asyncio.Lock()
        return await h._read_loadlock_vacuum_diag()

    res = asyncio.run(_go())
    for name, coil in P_NAMES.items():
        assert res[name] is coils[coil], (name, res[name])
        assert coil in read
    assert "미매핑" not in res.values()
