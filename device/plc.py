# -*- coding: utf-8 -*-
# plc.py
"""
plc.py — Modbus-TCP PLC 컨트롤러 — 단일 클래스 통합판

개요
----
- 코일 래치/타이머 동작을 고려한 안전 IO
- 동기 pymodbus 클라이언트를 asyncio에서 안전하게 사용(직렬화, 스레드 위임, 간격 보장, 하트비트, 자동 재연결).
- pymodbus 2.x / 3.x 호환: 'unit' / 'slave' 자동 판별.
- 고수준 API(door/gate/main_shutter/vent/turbo/lift_pin/gas) + 저수준 IO를 **AsyncPLC 하나**로 제공.
"""

from __future__ import annotations

import asyncio
import inspect
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Iterable
from contextlib import asynccontextmanager

from pymodbus.pdu import ExceptionResponse
from pymodbus.client import ModbusTcpClient
from pymodbus.exceptions import ModbusException

# ======================================================
# 주소 맵 (단독 CLI에서 사용한 것과 동일)
# ======================================================

# === M (Coils) — FC1/FC5 ===
PLC_COIL_MAP: Dict[str, int] = {
    # 인터락/기타
    "R_P_1_OFF_TIMER": 1,        # M00001
    "R_P_2_OFF_TIMER": 2,        # M00002
    "L_R_P_OFF_TIMER": 3,        # M00003
    "R_V_1_인터락": 33,          # M00021
    "F_V_1_인터락": 34,          # M00022
    "M_V_1_인터락": 35,          # M00023
    "VENT_1_인터락": 36,         # M00024
    "TURBO_1_인터락": 37,        # M00025
    "DOOR_1_인터락": 38,         # M00026
    "G_V_1_인터락": 40,          # M00028
    "MAIN_SHUTTER_1_인터락": 42, # M0002A
    "Ar_1_GAS_인터락": 43,       # M0002B
    "O2_1_GAS_인터락": 44,       # M0002C
    "MAIN_1_GAS_인터락": 45,     # M0002D
    "L_R_V_인터락": 49,          # M00031
    "L_PIN_인터락": 50,          # M00032
    "L_VENT_인터락": 52,         # M00034
    "R_V_2_인터락": 65,          # M00041
    "F_V_2_인터락": 66,          # M00042
    "M_V_2_인터락": 67,          # M00043
    "VENT_2_인터락": 68,         # M00044
    "TURBO_2_인터락": 69,        # M00045
    "DOOR_2_인터락": 70,         # M00046
    "G_V_2_인터락": 72,          # M00048
    "MAIN_SHUTTER_2_인터락": 74, # M0004A
    "Ar_2_GAS_인터락": 75,       # M0004B
    "O2_2_GAS_인터락": 76,       # M0004C
    "N2_2_GAS_인터락": 77,       # M0004D
    "MAIN_2_GAS_인터락": 78,     # M0004E

    # Z/MID 영역 — 워드/비트 재매핑 적용
    "Z_M_P_1_STOP_SW": 176,    # M00100
    "Z_M_P_1_MID_SW":  192,    # M00110
    "Z_M_P_1_MID_CW":  177,    # M00111
    "Z_M_P_1_MID_CCW": 178,    # M00112
    "Z_M_P_2_MID_SW":  179,    # M00113
    "Z_M_P_2_MID_CW":  180,    # M00114
    "Z_M_P_2_MID_CCW": 181,    # M00115

    # ===== SW(명령) 영역 =====
    # M0012y
    "R_P_1_SW":          208,  # M00120
    "R_V_1_SW":          193,  # M00121
    "F_V_1_SW":          194,  # M00122
    "M_V_1_SW":          195,  # M00123
    "VENT_1_SW":         196,  # M00124
    "TURBO_1_SW":        197,  # M00125
    "DOOR_1_OPEN_SW":    198,  # M00126
    "DOOR_1_CLOSE_SW":   199,  # M00127
    "G_V_1_OPEN_SW":     200,  # M00128
    "G_V_1_CLOSE_SW":    201,  # M00129
    "MAIN_SHUTTER_1_SW": 202,  # M0012A
    "Ar_1_GAS_SW":       203,  # M0012B
    "O2_1_GAS_SW":       204,  # M0012C
    "MAIN_1_GAS_SW":     205,  # M0012D

    # M0013y
    "L_R_P_SW":          224,  # M00130
    "L_R_V_SW":          209,  # M00131
    "L_PIN_UP_SW":       210,  # M00132
    "L_PIN_DOWN_SW":     211,  # M00133
    "L_VENT_SW":         212,  # M00134
    "SW_RF_SELECT":      214,  # M00136
    "SHUTTER_2_SW":      216,  # M00138
    "SHUTTER_3_SW":      217,  # M00139
    "Z_M_P_1_SW":        218,  # M0013A
    "Z_M_P_1_CW_SW":     219,  # M0013B
    "Z_M_P_1_CCW_SW":    220,  # M0013C
    "Z_M_P_2_SW":        221,  # M0013D
    "Z_M_P_2_CW_SW":     222,  # M0013E
    "Z_M_P_2_CCW_SW":    223,  # M0013F

    # M0014y
    "R_P_2_SW":          240,  # M00140
    "R_V_2_SW":          225,  # M00141
    "F_V_2_SW":          226,  # M00142
    "M_V_2_SW":          227,  # M00143
    "VENT_2_SW":         228,  # M00144
    "TURBO_2_SW":        229,  # M00145
    "DOOR_2_OPEN_SW":    230,  # M00146
    "DOOR_2_CLOSE_SW":   231,  # M00147
    "G_V_2_OPEN_SW":     232,  # M00148
    "G_V_2_CLOSE_SW":    233,  # M00149
    "MAIN_SHUTTER_2_SW": 234,  # M0014A
    "Ar_2_GAS_SW":       235,  # M0014B
    "O2_2_GAS_SW":       236,  # M0014C
    "N2_2_GAS_SW":       237,  # M0014D
    "MAIN_2_GAS_SW":     238,  # M0014E
    "SHUTTER_1_SW":      239,  # M0014F

    # 램프/기타
    "Z_M_P_2_STOP_SW":   336,   # M00200
    "VENT_1_LAMP":       356,   # M00224
    "DOOR_1_OPEN_LAMP":  358,   # M00226
    "DOOR_1_CLOSE_LAMP": 359,   # M00227
    "G_V_1_OPEN_LAMP":   360,   # M00228
    "G_V_1_CLOSE_LAMP":  361,   # M00229
    "L_PIN_UP_LAMP":     370,   # M00232
    "L_PIN_DOWN_LAMP":   371,   # M00233
    "L_VENT_LAMP":       372,   # M00234
    "VENT_2_LAMP":       388,   # M00244
    "DOOR_2_OPEN_LAMP":  390,   # M00246
    "DOOR_2_CLOSE_LAMP": 391,   # M00247
    "G_V_2_OPEN_LAMP":   392,   # M00248
    "G_V_2_CLOSE_LAMP":  393,   # M00249

    "BUZZER_STOP_SW":      1616,  # M01000
    "GAUGE_1_A_INTERLOCK": 2416,  # M01500
    "GAUGE_1_B_INTERLOCK": 2576,  # M01600
    "L_GAUGE_A_INTERLOCK": 2736,  # M01700
    "GAUGE_2_A_INTERLOCK": 2896,  # M01800
    "GAUGE_2_B_INTERLOCK": 3056,  # M01900
    
    # ─────────── Power SET (M00050~M00053) ───────────
    "DCV_SET_0": 80,  # M00050 (0x50)
    "DCV_SET_1": 81,  # M00051 (0x51)
    "DCV_SET_2": 82,  # M00052 (0x52)
    "DCV_SET_3": 83,  # M00053 (0x53)
}

# === D (Holding Registers) — FC3/FC6 ===
PLC_REG_MAP: Dict[str, int] = {
    "DCV_READ_0": 0,
    "DCV_READ_1": 1,
    "DCV_READ_2": 2,
    "DCV_READ_3": 3,
    "DCV_WRITE_0": 4,
    "DCV_WRITE_1": 5,
    "DCV_WRITE_2": 6,
    "DCV_WRITE_3": 7,
}

# === T (Timers as registers) — FC3/FC6 ===
PLC_TIMER_MAP: Dict[str, int] = {
    "AIR_ALARM_TIMER": 0,
    "Z_M_P_1_UP_DELAY": 1,
    "Z_M_P_1_DOWN_DELAY": 2,
    "Z_M_P_1_MID_DELAY": 3,
    "Z_M_P_2_UP_DELAY": 4,
    "Z_M_P_2_DOWN_DELAY": 5,
    "Z_M_P_2_MID_DELAY": 6,
    "VENT_1_EXT_TIME": 7,
    "VENT_2_EXT_TIME": 8,
    "L_VENT_EXT_TIME": 9,
    "GAUGE_1_SP1_ON_DELAY": 16,
    "GAUGE_1_SP1_OFF_DELAY": 17,
    "GAUGE_1_SP2_ON_DELAY": 18,
    "GAUGE_1_SP2_OFF_DELAY": 19,
    "GAUGE_1_SP3_ON_DELAY": 20,
    "GAUGE_1_SP3_OFF_DELAY": 21,
    "GAUGE_2_SP1_ON_DELAY": 22,
    "GAUGE_2_SP1_OFF_DELAY": 23,
    "GAUGE_2_SP2_ON_DELAY": 24,
    "GAUGE_2_SP2_OFF_DELAY": 25,
    "GAUGE_2_SP3_ON_DELAY": 26,
    "GAUGE_2_SP3_OFF_DELAY": 27,
    "TURBO_1_TIMER_인터락": 32,
    "TURBO_2_TIMER_인터락": 33,
    "ATM_ALARM_TIMER": 34,
}

# ======================================================
# 설정
# ======================================================

@dataclass
class PLCConfig:
    ip: str = "192.168.1.2"
    port: int = 502
    unit: int = 1
    timeout_s: float = 2.0
    inter_cmd_gap_s: float = 0.15
    heartbeat_s: float = 15.0
    pulse_ms: int = 180  # momentary 기본 펄스폭(ms)

    # ⬇️ 추가: 성능/경합 모니터링 임계치(ms)
    lock_warn_ms: float = 1000.0   # 락 획득 대기시간 경고 임계
    io_warn_ms: float   = 1500.0   # 락 내부 I/O 소요시간 경고 임계

    # ── DC Power 설정 ───────────────────────────────────────────
    # 원하는 파워[W] → DAC 코드 변환용. 직선 스케일(0~FULL)
    dc_power_min_w: float = 0.0
    dc_power_max_w: float = 1000.0      # 장비 정격에 맞춰 수정
    dc_dac_full_scale: int = 4000      # DAC 스케일 4000
    dc_dac_offset: int = 0             # 필요 시 오프셋

    # WRITE 인덱스(D00004=WRITE_0)를 기본으로 사용
    dc_write_index: int = 0            # 0→D00004, 1→D00005 ...

    # READ 스케일: 0..4000 카운트 → 0..2000 V, 0..4 A
    # (필요시 현장 값에 맞게 두 수치만 조정하세요:  dc_v_scale=V_FS/4000, dc_i_scale=I_FS/4000)
    dc_v_scale: float = 0.5    # 2000 V / 4000 ct  = 0.5 V/ct
    dc_i_scale: float = 0.001  # 4 A / 4000 ct     = 0.001 A/ct

# ======================================================
# 단일 클래스: AsyncPLC (저수준+고수준)
# ======================================================

class AsyncPLC:
    """
    Modbus 저수준 + 고수준 제어를 하나로 제공.
    - 저수준: connect/close, read/write coil/reg, 직렬화, 하트비트, 재연결
    - 고수준: door/gate/main_shutter/vent/turbo/lift_pin/gas, snapshot 등
    - DI용: set(name,on,ch=1)로 'MV/MS/AR/O2/N2/MAIN/G1/G2/G3' 논리명 처리
    """

    def __init__(self, ip: str = "192.168.1.2", port: int = 502, unit: int = 1,
                 timeout_s: float = 2.0, inter_cmd_gap_s: float = 0.15,
                 heartbeat_s: float = 15.0, pulse_ms: int = 180, logger=None):
        self.cfg = PLCConfig(ip=ip, port=port, unit=unit, timeout_s=timeout_s,
                             inter_cmd_gap_s=inter_cmd_gap_s, heartbeat_s=heartbeat_s,
                             pulse_ms=pulse_ms)
        self._client: Optional[ModbusTcpClient] = None
        self._uid_kw: Optional[str] = None  # 'unit' 또는 'slave'
        self._lock = asyncio.Lock()
        self._last_io_ts = 0.0
        self._hb_task: Optional[asyncio.Task] = None
        self._closed = False
        self._hb_paused: bool = False   # ← 추가
        self.log = logger or (lambda *a, **k: None)

        # 혼합 대/소문자/논리명 별칭
        self._ALIASES: Dict[str, str] = {}
        for ch in (1, 2):
            self._ALIASES[f"AR_{ch}_GAS_SW"] = f"Ar_{ch}_GAS_SW"
            self._ALIASES[f"AR_{ch}_GAS_인터락"] = f"Ar_{ch}_GAS_인터락"
        # 논리명 → 실제 코일 이름
        self._LOGICAL: Dict[str, str] = {
            "MV@1": "M_V_1_SW",
            "MV@2": "M_V_2_SW",
            "MS@1": "MAIN_SHUTTER_1_SW",
            "MS@2": "MAIN_SHUTTER_2_SW",
            "G1": "SHUTTER_1_SW",
            "G2": "SHUTTER_2_SW",
            "G3": "SHUTTER_3_SW",
        }

    # ---------- 연결/수명주기 ----------
    async def connect(self) -> None:
        self._closed = False
        async with self._io_lock("connect"):  # 🔒 I/O 및 하트비트와 직렬화
            await asyncio.to_thread(self._connect_sync)
        self.log("TCP 연결 성공: %s:%s (unit=%s)", self.cfg.ip, self.cfg.port, self.cfg.unit)
        if self._hb_task is None or self._hb_task.done():
            self._hb_task = asyncio.create_task(self._heartbeat_loop(), name="PLCHeartbeat")

    async def close(self) -> None:
        self._closed = True
        # 하트비트 먼저 중지
        if self._hb_task:
            self._hb_task.cancel()
            try:
                await self._hb_task
            except Exception:
                pass
            self._hb_task = None
        # 🔒 모든 I/O와 동기화하여 안전 종료
        async with self._io_lock("close"):
            await asyncio.to_thread(self._close_sync)
        self.log("TCP 연결 종료")

    def is_connected(self) -> bool:
        try:
            return bool(self._client) and self._is_connected()
        except Exception:
            return False

    # ---------- 내부 저수준 헬퍼 ----------
    def _is_connected(self) -> bool:
        c = self._client
        if not c:
            return False
        if hasattr(c, "connected"):
            try:
                return bool(getattr(c, "connected"))
            except Exception:
                pass
        if hasattr(c, "is_socket_open"):
            try:
                return bool(c.is_socket_open())  # type: ignore[attr-defined]
            except Exception:
                pass
        return False

    def _detect_uid_kw(self, method) -> Optional[str]:
        try:
            params = inspect.signature(method).parameters
            if "slave" in params:
                return "slave"
            if "unit" in params:
                return "unit"
        except Exception:
            pass
        return None

    def _uid_kwargs(self) -> dict:
        if self._uid_kw:
            return {self._uid_kw: self.cfg.unit}
        return {}

    def _ensure_ok(self, resp):
        if resp is None:
            raise ModbusException("응답 없음(None)")
        if isinstance(resp, ExceptionResponse):
            raise ModbusException(f"Modbus Exception: {resp}")
        if hasattr(resp, "isError") and resp.isError():
            raise ModbusException(str(resp))
        return resp

    def _is_reset_err(self, e: Exception) -> bool:
        s = str(e).lower()
        return ("10054" in s) or ("reset by peer" in s) or ("connectionreseterror" in s)

    async def _throttle_and_heartbeat(self):
        now = time.monotonic()
        delta = now - self._last_io_ts
        if delta < self.cfg.inter_cmd_gap_s:
            await asyncio.sleep(self.cfg.inter_cmd_gap_s - delta)
        if delta > self.cfg.heartbeat_s and self._client is not None:
            try:
                await asyncio.to_thread(self._client.read_coils, address=0, count=1, **self._uid_kwargs())
            except Exception:
                pass
        self._last_io_ts = time.monotonic()

    def _connect_sync(self) -> None:
        if self._client is None:
            self._client = ModbusTcpClient(self.cfg.ip, port=self.cfg.port, timeout=self.cfg.timeout_s)
        if not self._is_connected():
            if not self._client.connect():
                raise RuntimeError("Modbus TCP 연결 실패")
            # OS keepalive
            try:
                import socket
                self._client.socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            except Exception:
                pass
            self._last_io_ts = time.monotonic()
        if self._uid_kw is None:
            self._uid_kw = self._detect_uid_kw(self._client.read_coils)

    def _close_sync(self):
        if self._client is not None:
            try:
                self._client.close()
            finally:
                self._client = None
                self._uid_kw = None

    async def _heartbeat_loop(self):
        try:
            while not self._closed:
                await asyncio.sleep(max(1.0, self.cfg.heartbeat_s * 0.75))
                try:
                    await self.read_coil(0)
                except Exception:
                    # 🔒 재연결도 I/O와 직렬화
                    try:
                        async with self._io_lock("reconnect"):
                            await asyncio.to_thread(self._close_sync)
                            await asyncio.to_thread(self._connect_sync)
                    except Exception:
                        pass
        except asyncio.CancelledError:
            return

    # ---------- 저수준 IO(직렬화) ----------
    async def read_coil(self, addr: int) -> bool:
        async with self._io_lock("read_coil", addr=addr):
            await asyncio.to_thread(self._connect_sync)
            await self._throttle_and_heartbeat()
            try:
                resp = await asyncio.to_thread(self._client.read_coils, addr, count=1, **self._uid_kwargs())
            except Exception as e:
                if self._is_reset_err(e):
                    await asyncio.to_thread(self._close_sync)
                    await asyncio.to_thread(self._connect_sync)
                    await self._throttle_and_heartbeat()
                    resp = await asyncio.to_thread(self._client.read_coils, addr, count=1, **self._uid_kwargs())
                else:
                    raise
            self._ensure_ok(resp)
            return bool(resp.bits[0])

    async def write_coil(self, addr: int, state: bool) -> None:
        async with self._io_lock("write_coil", addr=addr):
            await asyncio.to_thread(self._connect_sync)
            await self._throttle_and_heartbeat()
            try:
                resp = await asyncio.to_thread(self._client.write_coil, addr, bool(state), **self._uid_kwargs())
            except Exception as e:
                if self._is_reset_err(e):
                    await asyncio.to_thread(self._close_sync)
                    await asyncio.to_thread(self._connect_sync)
                    await self._throttle_and_heartbeat()
                    resp = await asyncio.to_thread(self._client.write_coil, addr, bool(state), **self._uid_kwargs())
                else:
                    raise
            self._ensure_ok(resp)

    async def read_reg(self, addr: int) -> int:
        async with self._io_lock("read_reg", addr=addr):
            await asyncio.to_thread(self._connect_sync)
            await self._throttle_and_heartbeat()
            try:
                resp = await asyncio.to_thread(self._client.read_holding_registers, addr, count=1, **self._uid_kwargs())
            except Exception as e:
                if self._is_reset_err(e):
                    await asyncio.to_thread(self._close_sync)
                    await asyncio.to_thread(self._connect_sync)
                    await self._throttle_and_heartbeat()
                    resp = await asyncio.to_thread(self._client.read_holding_registers, addr, count=1, **self._uid_kwargs())
                else:
                    raise
            self._ensure_ok(resp)
            return int(resp.registers[0])

    async def write_reg(self, addr: int, value: int) -> None:
        async with self._io_lock("write_reg", addr=addr):
            await asyncio.to_thread(self._connect_sync)
            await self._throttle_and_heartbeat()
            try:
                resp = await asyncio.to_thread(self._client.write_register, addr, int(value), **self._uid_kwargs())
            except Exception as e:
                if self._is_reset_err(e):
                    await asyncio.to_thread(self._close_sync)
                    await asyncio.to_thread(self._connect_sync)
                    await self._throttle_and_heartbeat()
                    resp = await asyncio.to_thread(self._client.write_register, addr, int(value), **self._uid_kwargs())
                else:
                    raise
            self._ensure_ok(resp)

    async def read_coils(self, addrs: Iterable[int]) -> Dict[int, bool]:
        out: Dict[int, bool] = {}
        for a in addrs:
            out[a] = await self.read_coil(a)
        return out

    async def pulse(self, addr: int, ms: Optional[int] = None) -> None:
        ms = self.cfg.pulse_ms if ms is None else ms
        await self.write_coil(addr, True)
        await asyncio.sleep(max(0.03, ms / 1000.0))
        await self.write_coil(addr, False)

    # ---------- 이름/주소 해석 ----------
    def _addr(self, name_or_addr: Any) -> int:
        # 정수 주소 직접 지원
        if isinstance(name_or_addr, int):
            return name_or_addr
        # 별칭 보정(혼합 대/소문자 등)
        key = str(name_or_addr)
        if key in self._ALIASES:
            key = self._ALIASES[key]
        # 맵 조회
        if key in PLC_COIL_MAP:
            return PLC_COIL_MAP[key]
        if key in PLC_REG_MAP:
            return PLC_REG_MAP[key]
        raise KeyError(f"알 수 없는 주소/이름: {name_or_addr}")

    # ---------- 공통 고수준 ----------
    async def write_switch(self, name_or_addr: Any, on: bool,
                           *, momentary: bool = False, pulse_ms: Optional[int] = None) -> None:
        addr = self._addr(name_or_addr)
        if momentary:
            self.log("pulse %s (addr=%d, %sms)", name_or_addr, addr, str(pulse_ms or self.cfg.pulse_ms))
            await self.pulse(addr, ms=pulse_ms)
        else:
            self.log("set %s <- %s (addr=%d)", name_or_addr, on, addr)
            await self.write_coil(addr, bool(on))

    async def press_switch(self, name_or_addr: Any, pulse_ms: Optional[int] = None) -> None:
        await self.write_switch(name_or_addr, True, momentary=True, pulse_ms=pulse_ms)

    async def read_bit(self, name_or_addr: Any) -> bool:
        addr = self._addr(name_or_addr)
        v = await self.read_coil(addr)
        self.log("read %s (addr=%d) -> %s", name_or_addr, addr, v)
        return v

    async def write_reg_name(self, name_or_addr: Any, value: int) -> None:
        addr = self._addr(name_or_addr)
        await self.write_reg(addr, int(value))
        self.log("write reg %s (addr=%d) <- %d", name_or_addr, addr, value)

    async def read_reg_name(self, name_or_addr: Any) -> int:
        addr = self._addr(name_or_addr)
        v = await self.read_reg(addr)
        self.log("read reg %s (addr=%d) -> %d", name_or_addr, addr, v)
        return v

    # ---------- Faduino 스타일 고수준 ----------
    async def door(self, chamber: int, *, open: bool, momentary: bool = False) -> None:
        name = f"DOOR_{chamber}_{'OPEN' if open else 'CLOSE'}_SW"
        await self.write_switch(name, True, momentary=momentary)
        self.log("Door%d %s", chamber, "OPEN" if open else "CLOSE")

    async def gate_valve(self, chamber: int, *, open: bool, momentary: bool = False) -> None:
        name = f"G_V_{chamber}_{'OPEN' if open else 'CLOSE'}_SW"
        await self.write_switch(name, True, momentary=momentary)
        self.log("GV%d %s", chamber, "OPEN" if open else "CLOSE")

    async def main_shutter(self, chamber: int, *, open: bool, momentary: bool = False) -> None:
        name = f"MAIN_SHUTTER_{chamber}_SW"
        await self.write_switch(name, bool(open), momentary=momentary)
        self.log("MainShutter%d %s", chamber, "OPEN(ON)" if open else "CLOSE(OFF)")

    async def vent(self, chamber: int, *, on: bool, momentary: bool = False) -> None:
        name = f"VENT_{chamber}_SW"
        await self.write_switch(name, bool(on), momentary=momentary)
        self.log("Vent%d %s", chamber, "ON" if on else "OFF")

    async def turbo(self, chamber: int, *, on: bool, momentary: bool = False) -> None:
        name = f"TURBO_{chamber}_SW"
        await self.write_switch(name, bool(on), momentary=momentary)
        self.log("Turbo%d %s", chamber, "ON" if on else "OFF")

    async def lift_pin(self, *, up: bool, momentary: bool = False) -> None:
        await self.write_switch("L_PIN_UP_SW" if up else "L_PIN_DOWN_SW", True, momentary=momentary)
        self.log("LiftPin %s", "UP" if up else "DOWN")

    async def lift_pin_lamp(self, *, up: bool) -> bool:
        return await self.read_bit("L_PIN_UP_LAMP" if up else "L_PIN_DOWN_LAMP")

    async def lr_valve(self, *, on: bool, momentary: bool = False) -> None:
        await self.write_switch("L_R_V_SW", bool(on), momentary=momentary)
        self.log("L_R_V %s", "ON" if on else "OFF")

    async def rf_select(self, *, rf_mode: bool, momentary: bool = False) -> None:
        await self.write_switch("SW_RF_SELECT", bool(rf_mode), momentary=momentary)
        self.log("RF_SELECT <- %s", rf_mode)

    async def gas(self, chamber: int, gas: str, *, on: bool, momentary: bool = False) -> None:
        g = (gas or "").strip().upper()
        valid = {"AR", "O2", "N2", "MAIN"}
        if g not in valid:
            raise ValueError(f"gas must be one of {valid}")
        if g == "AR":
            key = f"Ar_{chamber}_GAS_SW"   # 혼합 표기
        elif g == "MAIN":
            key = f"MAIN_{chamber}_GAS_SW"
        else:
            key = f"{g}_{chamber}_GAS_SW"
        await self.write_switch(key, bool(on), momentary=momentary)
        self.log("Gas %s@Ch%d %s", g, chamber, "ON" if on else "OFF")

    async def buzzer_stop(self, *, momentary: bool = False) -> None:
        await self.write_switch("BUZZER_STOP_SW", True, momentary=momentary)

    async def read_interlocks(self) -> Dict[str, bool]:
        keys = [k for k in PLC_COIL_MAP.keys() if "인터락" in k]
        res: Dict[str, bool] = {}
        for k in keys:
            res[k] = await self.read_bit(k)
        return res

    async def read_lamps(self) -> Dict[str, bool]:
        keys = [k for k in PLC_COIL_MAP.keys() if k.endswith("_LAMP")]
        res: Dict[str, bool] = {}
        for k in keys:
            res[k] = await self.read_bit(k)
        return res

    async def snapshot_status(self) -> Dict[str, Any]:
        interlocks = await self.read_interlocks()
        lamps = await self.read_lamps()
        return {"interlocks": interlocks, "lamps": lamps}

    # ---------- 레거시/편의 ----------
    async def main_shutter_open(self, chamber: int = 1, *, momentary: bool = False):
        await self.main_shutter(chamber, open=True, momentary=momentary)

    async def main_shutter_close(self, chamber: int = 1, *, momentary: bool = False):
        await self.main_shutter(chamber, open=False, momentary=momentary)

    async def gv_open(self, chamber: int = 1, *, momentary: bool = False):
        await self.gate_valve(chamber, open=True, momentary=momentary)

    async def gv_close(self, chamber: int = 1, *, momentary: bool = False):
        await self.gate_valve(chamber, open=False, momentary=momentary)

    async def door_open(self, chamber: int = 1, *, momentary: bool = False):
        await self.door(chamber, open=True, momentary=momentary)

    async def door_close(self, chamber: int = 1, *, momentary: bool = False):
        await self.door(chamber, open=False, momentary=momentary)

    async def vent_on(self, chamber: int = 1, *, momentary: bool = False):
        await self.vent(chamber, on=True, momentary=momentary)

    async def vent_off(self, chamber: int = 1, *, momentary: bool = False):
        await self.vent(chamber, on=False, momentary=momentary)

    # ──────────────────────────────────────────────────────────
    # Power 공통 (DC/RF 등): family + index 기반
    # ──────────────────────────────────────────────────────────

    def _clamp(self, v: float, lo: float, hi: float) -> float:
        return lo if v < lo else hi if v > hi else v

    def _linear_to_dac(self, value: float, *, vmin: float, vmax: float,
                       full_scale: int, offset: int = 0) -> int:
        v = self._clamp(float(value), vmin, vmax)
        span = max(1e-9, (vmax - vmin))
        code = int(round(((v - vmin) / span) * full_scale + offset))
        return max(0, min(full_scale, code))

    async def power_enable(self, on: bool = True, *, family: str = "DCV",
                        set_idx: Optional[int] = None, set_key: Optional[str] = None) -> None:
        """
        SET 코일 래치 ON/OFF.
        - 기본: family='DCV', set_idx=cfg.dc_write_index → 'DCV_SET_{idx}'
        - RF 등은 set_key='RFP_SET_0' 처럼 직접 키를 지정해도 됨.
        """
        idx = int(self.cfg.dc_write_index) if set_idx is None else int(set_idx)
        key = set_key or f"{family}_SET_{idx}"
        if key not in PLC_COIL_MAP:
            raise KeyError(f"PLC_COIL_MAP에 '{key}' 없음 (family={family}, idx={idx})")
        self.log("POWER SET (%s)[%d] <- %s", family, idx, on)
        await self.write_switch(key, bool(on), momentary=False)

    async def power_write(self, power_w: float, *, family: str = "DCV",
                        write_idx: Optional[int] = None, write_key: Optional[str] = None,
                        vmin: Optional[float] = None, vmax: Optional[float] = None,
                        full_scale: Optional[int] = None, offset: Optional[int] = None) -> int:
        """
        원하는 파워[W] → DAC 코드 → WRITE 레지스터 기록.
        - 기본: family='DCV', write_idx=cfg.dc_write_index → 'DCV_WRITE_{idx}'(=D00004+idx)
        - RF 등은 write_key='RFP_WRITE_0' 식으로 직접 키 지정 가능.
        """
        vmin  = self.cfg.dc_power_min_w if vmin  is None else float(vmin)
        vmax  = self.cfg.dc_power_max_w if vmax  is None else float(vmax)
        fs    = self.cfg.dc_dac_full_scale if full_scale is None else int(full_scale)
        off   = self.cfg.dc_dac_offset if offset is None else int(offset)

        code = self._linear_to_dac(power_w, vmin=vmin, vmax=vmax, full_scale=fs, offset=off)

        if write_key is None:
            idx = int(self.cfg.dc_write_index) if write_idx is None else int(write_idx)
            write_key = f"{family}_WRITE_{idx}"
        if write_key not in PLC_REG_MAP:
            raise KeyError(f"PLC_REG_MAP에 '{write_key}' 없음 (family={family})")
        await self.write_reg_name(write_key, code)
        self.log("POWER WRITE (%s) %s <- W=%.3f (DAC=%d)", family, write_key, power_w, code)
        return code

    async def power_apply(self, power_w: float, *, family: str = "DCV",
                        channel: Optional[int] = None, ensure_set: bool = True,
                        vmin: Optional[float] = None, vmax: Optional[float] = None,
                        full_scale: Optional[int] = None, offset: Optional[int] = None) -> int:
        """
        (추천) 한 번에: SET(선택) → WRITE. channel=None이면 cfg.dc_write_index.
        """
        idx = int(self.cfg.dc_write_index) if channel is None else int(channel)
        if ensure_set:
            await self.power_enable(True, family=family, set_idx=idx)
        return await self.power_write(power_w, family=family, write_idx=idx,
                                      vmin=vmin, vmax=vmax, full_scale=full_scale, offset=offset)

    async def power_read(self, *, family: str = "DCV",
                        v_idx: Optional[int] = None, i_idx: Optional[int] = None,
                        v_key: Optional[str] = None, i_key: Optional[str] = None,
                        v_scale: Optional[float] = None,
                        i_scale: Optional[float] = None) -> tuple[float, float, float]:
        """
        V/I 읽고 스케일 적용 → (P[W], V[V], I[A]).
        - 기본: family='DCV', v_idx=0 → 'DCV_READ_0'(D00000), i_idx=1 → 'DCV_READ_1'(D00001)
        - RF 등은 v_key/i_key로 직접 레지스터 키 지정 가능.
        """
        if v_key is None:
            vi = 0 if v_idx is None else int(v_idx)
            v_key = f"{family}_READ_{vi}"
        if i_key is None:
            ii = 1 if i_idx is None else int(i_idx)
            i_key = f"{family}_READ_{ii}"
        if v_key not in PLC_REG_MAP:
            raise KeyError(f"PLC_REG_MAP에 '{v_key}' 없음 (family={family})")
        if i_key not in PLC_REG_MAP:
            raise KeyError(f"PLC_REG_MAP에 '{i_key}' 없음 (family={family})")

        v_raw = await self.read_reg_name(v_key)
        i_raw = await self.read_reg_name(i_key)

        V = float(v_raw) * (self.cfg.dc_v_scale if v_scale is None else float(v_scale))
        I = float(i_raw) * (self.cfg.dc_i_scale if i_scale is None else float(i_scale))
        P = V * I
        self.log("POWER READ (%s) V=%.3f, I=%.3f, P=%.3f (keys=%s/%s)", family, V, I, P, v_key, i_key)
        return P, V, I

    # ---------- DI용 논리명 셋(set) ----------
    async def set(self, name: str, on: bool, *, ch: int = 1, momentary: bool = False) -> None:
        """
        process_ch2.py에서의 PLC_CMD용 범용 엔드포인트.
        name:
          - 'MV', 'MS'        : 메인 밸브 / 메인 셔터 (ch 사용)
          - 'AR','O2','N2','MAIN' : 가스 라인 (ch 사용)
          - 'G1','G2','G3'    : 건 셔터 1/2/3
          - (그 외) PLC_COIL_MAP 키를 직접 주면 그대로 동작
        """
        if not name:
            raise ValueError("name required")

        key = name.strip().upper().replace(" ", "").replace("_", "")
        # 가스
        if key in {"AR", "O2", "N2", "MAIN"}:
            await self.gas(int(ch), key, on=bool(on), momentary=momentary)
            return
        # 메인 밸브/셔터
        if key == "MV":
            await self.write_switch(self._LOGICAL.get(f"MV@{int(ch)}", "M_V_1_SW"), bool(on), momentary=momentary)
            return
        if key == "MS":
            await self.write_switch(self._LOGICAL.get(f"MS@{int(ch)}", "MAIN_SHUTTER_1_SW"), bool(on), momentary=momentary)
            return
        # 건 셔터들
        if key in {"G1", "G2", "G3"}:
            await self.write_switch(self._LOGICAL[key], bool(on), momentary=momentary)
            return
        # 맵 이름 직접
        try:
            await self.write_switch(name, bool(on), momentary=momentary)
        except KeyError:
            raise KeyError(f"지원하지 않는 PLC 논리명/키: {name}")
        
    # =============== 유틸 ===============
    @asynccontextmanager
    async def _io_lock(self, op: str, *, addr: Optional[int] = None):
        """
        락 획득 대기(wait)와 락 내부 실행(in-lock) 시간을 분리 계측하고,
        임계치 초과 시 self.log로 WARN을 남긴다.
        """
        loop = asyncio.get_running_loop()
        t_wait_start = loop.time()
        await self._lock.acquire()
        waited_ms = (loop.time() - t_wait_start) * 1000.0

        try:
            if waited_ms >= self.cfg.lock_warn_ms:
                if addr is None:
                    self.log("WARN lock-wait %.0f ms (op=%s)", waited_ms, op)
                else:
                    self.log("WARN lock-wait %.0f ms (op=%s, addr=%s)", waited_ms, op, addr)

            t_in_start = loop.time()
            yield  # 🔒 임계구역 시작

            io_ms = (loop.time() - t_in_start) * 1000.0
            if io_ms >= self.cfg.io_warn_ms:
                if addr is None:
                    self.log("WARN in-lock IO %.0f ms (op=%s)", io_ms, op)
                else:
                    self.log("WARN in-lock IO %.0f ms (op=%s, addr=%s)", io_ms, op, addr)
        finally:
            try:
                self._lock.release()
            except RuntimeError:
                pass
    # =============== 유틸 ===============

    # =============== chamber_runtime.py 호환용 함수 ===============
    # chamber_runtime: start()/connect 호환
    async def start(self) -> None:
        """connect() 별칭 — 장치 생명주기 통일."""
        await self.connect()

    # chamber_runtime: cleanup()/cleanup_quick 호환
    async def cleanup(self) -> None:
        """close() 별칭 — 정상 종료."""
        await self.close()

    async def cleanup_quick(self) -> None:
        """빠른 종료(현재는 close와 동일)."""
        await self.close()

    # IG와 대칭되는 워치독 일시중지/재개
    async def pause_watchdog(self) -> None:
        """하트비트/자동 재연결 워치독 중지 (연결은 유지)."""
        self._hb_paused = True
        t = self._hb_task
        if t and not t.done():
            t.cancel()
            try:
                await t
            except Exception:
                pass
        self._hb_task = None

    async def resume_watchdog(self) -> None:
        """pause_watchdog 이후 워치독 재개."""
        self._hb_paused = False
        if (not self._closed) and self.is_connected() and (self._hb_task is None or self._hb_task.done()):
            self._hb_task = asyncio.create_task(self._heartbeat_loop(), name="PLCHeartbeat")

    # chamber_runtime: 공정 on/off 신호에 맞춰 폴링/워치독 제어(옵션)
    def set_process_status(self, should_poll: bool) -> None:
        """
        공정 상태 알림 훅.
        False면 워치독(하트비트/자동재연결) 잠시 멈춰 로그 소음/경합 줄임.
        True면 다시 재개.
        """
        loop = asyncio.get_running_loop()
        if not should_poll:
            loop.create_task(self.pause_watchdog())
        else:
            loop.create_task(self.resume_watchdog())

    # IG의 set_endpoint와 대칭 — ip/port 런타임 변경
    async def set_endpoint(self, ip: str, port: int, *, reconnect: bool = True) -> None:
        """
        런타임에서 PLC 엔드포인트 변경. reconnect=True면 즉시 재연결.
        """
        self.cfg.ip = str(ip)
        self.cfg.port = int(port)
        if reconnect:
            # 하트비트 일시 정지 후 재연결
            await self.pause_watchdog()
            try:
                await self.close()
            except Exception:
                pass
            await self.connect()
            await self.resume_watchdog()
    # =============== chamber_runtime.py 호환용 함수 ===============

__all__ = [
    "PLC_COIL_MAP", "PLC_REG_MAP", "PLC_TIMER_MAP",
    "PLCConfig", "AsyncPLC",
]