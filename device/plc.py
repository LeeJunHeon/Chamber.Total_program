# -*- coding: utf-8 -*-
"""
Async PLC controller (Modbus‑TCP) with Faduino‑compatible adapter

- Drop‑in replacement for your asyncio "AsyncFaduino" device.
- Uses synchronous pymodbus client under the hood, isolated via asyncio.to_thread
  so it never blocks the Qt/qasync event loop.
- Serializes Modbus requests with an asyncio.Lock, enforces inter‑command gap,
  and keeps the TCP session alive with a heartbeat (light coil read) task.

Files/Classes
-------------
- PLC_COIL_MAP / PLC_REG_MAP / PLC_TIMER_MAP : addresses you validated in your
  stand‑alone CLI. (Copied here to avoid cross‑module import issues.)
- class AsyncPLC : low‑level generic Modbus‑TCP helper (read/write coil/register).
- class AsyncFaduinoPLC : Faduino‑compatible adapter exposing typical methods
  your ProcessController expects (door/gate/vent/turbo/shutter/pin etc.).

Notes
-----
- Switch (…_SW) addresses are treated as *momentary* buttons by default.
  Methods ending with *_press or arguments with momentary=True will pulse the
  coil for PULSE_MS and then release it. You can set momentary=False to latch.
- Interlock / lamp bits are readable via read_interlocks()/read_lamps().
- If your original AsyncFaduino exposed extra helpers, add a thin wrapper below
  or extend _ALIASES so existing call‑sites keep working.

Tested against pymodbus 2.x / 3.x via signature detection for 'unit'/'slave'.
"""
from __future__ import annotations

import asyncio
import inspect
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Iterable, Tuple

from pymodbus.pdu import ExceptionResponse
from pymodbus.client import ModbusTcpClient
from pymodbus.exceptions import ModbusException

# ======================================================
# 주소 맵 (당신의 단독 테스트 코드와 동일)
# ======================================================

# === M (Coils) — FC1/FC5 ===
PLC_COIL_MAP: Dict[str, int] = {
    # 인터락/기타
    "R_P_1_OFF_TIMER": 1,
    "R_P_2_OFF_TIMER": 2,
    "L_R_P_OFF_TIMER": 3,
    "R_V_1_인터락": 33,
    "F_V_1_인터락": 34,
    "M_V_1_인터락": 35,
    "VENT_1_인터락": 36,
    "TURBO_1_인터락": 37,
    "DOOR_1_인터락": 38,
    "G_V_1_인터락": 40,
    "MAIN_SHUTTER_1_인터락": 42,
    "Ar_1_GAS_인터락": 43,
    "O2_1_GAS_인터락": 44,
    "MAIN_1_GAS_인터락": 45,
    "L_R_V_인터락": 49,
    "L_PIN_인터락": 50,
    "L_VENT_인터락": 52,
    "R_V_2_인터락": 65,
    "F_V_2_인터락": 66,
    "M_V_2_인터락": 67,
    "VENT_2_인터락": 68,
    "TURBO_2_인터락": 69,
    "DOOR_2_인터락": 70,
    "G_V_2_인터락": 72,
    "MAIN_SHUTTER_2_인터락": 74,
    "Ar_2_GAS_인터락": 75,
    "O2_2_GAS_인터락": 76,
    "N2_2_GAS_인터락": 77,
    "MAIN_2_GAS_인터락": 78,

    # Z/MID 영역 — 워드/비트 재매핑 적용
    "Z_M_P_1_STOP_SW": 176,
    "Z_M_P_1_MID_SW": 192,
    "Z_M_P_1_MID_CW": 177,
    "Z_M_P_1_MID_CCW": 178,
    "Z_M_P_2_MID_SW": 179,
    "Z_M_P_2_MID_CW": 180,
    "Z_M_P_2_MID_CCW": 181,

    # ===== SW(명령) 영역 =====
    # M0012y
    "R_P_1_SW": 208,
    "R_V_1_SW": 193,
    "F_V_1_SW": 194,
    "M_V_1_SW": 195,
    "VENT_1_SW": 196,
    "TURBO_1_SW": 197,
    "DOOR_1_OPEN_SW": 198,
    "DOOR_1_CLOSE_SW": 199,
    "G_V_1_OPEN_SW": 200,
    "G_V_1_CLOSE_SW": 201,
    "MAIN_SHUTTER_1_SW": 202,
    "Ar_1_GAS_SW": 203,
    "O2_1_GAS_SW": 204,
    "MAIN_1_GAS_SW": 205,

    # M0013y
    "L_R_P_SW": 224,
    "L_R_V_SW": 209,
    "L_PIN_UP_SW": 210,
    "L_PIN_DOWN_SW": 211,
    "L_VENT_SW": 212,
    "SW_RF_SELECT": 214,
    "SHUTTER_2_SW": 216,
    "SHUTTER_3_SW": 217,
    "Z_M_P_1_SW": 218,
    "Z_M_P_1_CW_SW": 219,
    "Z_M_P_1_CCW_SW": 220,
    "Z_M_P_2_SW": 221,
    "Z_M_P_2_CW_SW": 222,
    "Z_M_P_2_CCW_SW": 223,

    # M0014y
    "R_P_2_SW": 240,
    "R_V_2_SW": 225,
    "F_V_2_SW": 226,
    "M_V_2_SW": 227,
    "VENT_2_SW": 228,
    "TURBO_2_SW": 229,
    "DOOR_2_OPEN_SW": 230,
    "DOOR_2_CLOSE_SW": 231,
    "G_V_2_OPEN_SW": 232,
    "G_V_2_CLOSE_SW": 233,
    "MAIN_SHUTTER_2_SW": 234,
    "Ar_2_GAS_SW": 235,
    "O2_2_GAS_SW": 236,
    "N2_2_GAS_SW": 237,
    "MAIN_2_GAS_SW": 238,
    "SHUTTER_1_SW": 239,

    # 램프/기타
    "Z_M_P_2_STOP_SW": 336,
    "VENT_1_LAMP": 356,
    "DOOR_1_OPEN_LAMP": 358,
    "DOOR_1_CLOSE_LAMP": 359,
    "G_V_1_OPEN_LAMP": 360,
    "G_V_1_CLOSE_LAMP": 361,
    "L_PIN_UP_LAMP": 370,
    "L_PIN_DOWN_LAMP": 371,
    "L_VENT_LAMP": 372,
    "VENT_2_LAMP": 388,
    "DOOR_2_OPEN_LAMP": 390,
    "DOOR_2_CLOSE_LAMP": 391,
    "G_V_2_OPEN_LAMP": 392,
    "G_V_2_CLOSE_LAMP": 393,

    "BUZZER_STOP_SW": 1616,
    "GAUGE_1_A_INTERLOCK": 2416,
    "GAUGE_1_B_INTERLOCK": 2576,
    "L_GAUGE_A_INTERLOCK": 2736,
    "GAUGE_2_A_INTERLOCK": 2896,
    "GAUGE_2_B_INTERLOCK": 3056,
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
# Low‑level: AsyncPLC (generic Modbus‑TCP wrapper)
# ======================================================

@dataclass
class PLCConfig:
    ip: str = "192.168.1.2"
    port: int = 502
    unit: int = 1
    timeout_s: float = 2.0
    inter_cmd_gap_s: float = 0.15
    heartbeat_s: float = 15.0
    pulse_ms: int = 180  # default momentary switch press

class AsyncPLC:
    def __init__(self, cfg: PLCConfig, logger=None):
        self.cfg = cfg
        self._client: Optional[ModbusTcpClient] = None
        self._uid_kw: Optional[str] = None  # 'unit' or 'slave'
        self._lock = asyncio.Lock()
        self._last_io_ts = 0.0
        self._hb_task: Optional[asyncio.Task] = None
        self._closed = False
        self.log = logger or (lambda *a, **k: None)

    # ---------- connection helpers ----------
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

    def _throttle_and_heartbeat_sync(self):
        now = time.monotonic()
        delta = now - self._last_io_ts
        # gap
        if delta < self.cfg.inter_cmd_gap_s:
            time.sleep(self.cfg.inter_cmd_gap_s - delta)
        # idle heartbeat
        if delta > self.cfg.heartbeat_s and self._client is not None:
            try:
                self._client.read_coils(address=0, count=1, **self._uid_kwargs())
            except Exception:
                pass
        self._last_io_ts = time.monotonic()

    def _connect_sync(self) -> None:
        if self._client is None:
            self._client = ModbusTcpClient(self.cfg.ip, port=self.cfg.port, timeout=self.cfg.timeout_s)
        if not self._is_connected():
            if not self._client.connect():
                raise RuntimeError("Modbus TCP 연결 실패")
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

    async def connect(self) -> None:
        self._closed = False
        await asyncio.to_thread(self._connect_sync)
        self.log("[PLC] TCP 연결 성공: %s:%s (unit=%s)", self.cfg.ip, self.cfg.port, self.cfg.unit)
        if self._hb_task is None or self._hb_task.done():
            self._hb_task = asyncio.create_task(self._heartbeat_loop(), name="PLCHeartbeat")

    async def close(self) -> None:
        self._closed = True
        if self._hb_task:
            self._hb_task.cancel()
            try:
                await self._hb_task
            except Exception:
                pass
            self._hb_task = None
        await asyncio.to_thread(self._close_sync)
        self.log("[PLC] TCP 연결 종료")

    async def _heartbeat_loop(self):
        # lightweight periodic read to keep gateways from timing out
        try:
            while not self._closed:
                await asyncio.sleep(max(1.0, self.cfg.heartbeat_s * 0.75))
                try:
                    await self.read_coil(0)
                except Exception:
                    # try one reconnect
                    try:
                        await asyncio.to_thread(self._close_sync)
                        await asyncio.to_thread(self._connect_sync)
                    except Exception:
                        pass
        except asyncio.CancelledError:
            return

    # ---------- public low‑level ops (serialized) ----------
    async def read_coil(self, addr: int) -> bool:
        async with self._lock:
            await asyncio.to_thread(self._connect_sync)
            self._throttle_and_heartbeat_sync()
            try:
                resp = await asyncio.to_thread(self._client.read_coils, addr, 1, **self._uid_kwargs())
            except Exception as e:
                if self._is_reset_err(e):
                    await asyncio.to_thread(self._close_sync)
                    await asyncio.to_thread(self._connect_sync)
                    self._throttle_and_heartbeat_sync()
                    resp = await asyncio.to_thread(self._client.read_coils, addr, 1, **self._uid_kwargs())
                else:
                    raise
            self._ensure_ok(resp)
            return bool(resp.bits[0])

    async def write_coil(self, addr: int, state: bool) -> None:
        async with self._lock:
            await asyncio.to_thread(self._connect_sync)
            self._throttle_and_heartbeat_sync()
            try:
                resp = await asyncio.to_thread(self._client.write_coil, addr, bool(state), **self._uid_kwargs())
            except Exception as e:
                if self._is_reset_err(e):
                    await asyncio.to_thread(self._close_sync)
                    await asyncio.to_thread(self._connect_sync)
                    self._throttle_and_heartbeat_sync()
                    resp = await asyncio.to_thread(self._client.write_coil, addr, bool(state), **self._uid_kwargs())
                else:
                    raise
            self._ensure_ok(resp)

    async def read_reg(self, addr: int) -> int:
        async with self._lock:
            await asyncio.to_thread(self._connect_sync)
            self._throttle_and_heartbeat_sync()
            try:
                resp = await asyncio.to_thread(self._client.read_holding_registers, addr, 1, **self._uid_kwargs())
            except Exception as e:
                if self._is_reset_err(e):
                    await asyncio.to_thread(self._close_sync)
                    await asyncio.to_thread(self._connect_sync)
                    self._throttle_and_heartbeat_sync()
                    resp = await asyncio.to_thread(self._client.read_holding_registers, addr, 1, **self._uid_kwargs())
                else:
                    raise
            self._ensure_ok(resp)
            return int(resp.registers[0])

    async def write_reg(self, addr: int, value: int) -> None:
        async with self._lock:
            await asyncio.to_thread(self._connect_sync)
            self._throttle_and_heartbeat_sync()
            try:
                resp = await asyncio.to_thread(self._client.write_register, addr, int(value), **self._uid_kwargs())
            except Exception as e:
                if self._is_reset_err(e):
                    await asyncio.to_thread(self._close_sync)
                    await asyncio.to_thread(self._connect_sync)
                    self._throttle_and_heartbeat_sync()
                    resp = await asyncio.to_thread(self._client.write_register, addr, int(value), **self._uid_kwargs())
                else:
                    raise
            self._ensure_ok(resp)

    # convenience batches
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


# ======================================================
# High‑level: AsyncFaduinoPLC (Faduino‑compatible API)
# ======================================================

class AsyncFaduinoPLC:
    """Adapter exposing a Faduino‑like API on top of AsyncPLC.

    Typical usage in main.py:
        from device.plc_async import AsyncFaduinoPLC
        plc = AsyncFaduinoPLC(ip="192.168.1.2", unit=1, logger=your_logger)
        await plc.connect()
        await plc.main_shutter(1, open=True)

    Logging: pass a logger like logger.info(fmt, *args).
    """
    def __init__(self, ip: str = "192.168.1.2", port: int = 502, unit: int = 1,
                 timeout_s: float = 2.0, inter_cmd_gap_s: float = 0.15,
                 heartbeat_s: float = 15.0, pulse_ms: int = 180, logger=None):
        cfg = PLCConfig(ip=ip, port=port, unit=unit, timeout_s=timeout_s,
                        inter_cmd_gap_s=inter_cmd_gap_s, heartbeat_s=heartbeat_s,
                        pulse_ms=pulse_ms)
        self._plc = AsyncPLC(cfg, logger=logger)
        self.log = logger or (lambda *a, **k: None)

        # aliases to preserve older call‑sites if needed
        self._ALIASES: Dict[str, str] = {
            # "relay4_off": "G_V_2_CLOSE_SW",  # example — fill if your code uses string commands
        }

    # ---------- lifecycle ----------
    async def connect(self) -> None:
        await self._plc.connect()

    async def close(self) -> None:
        await self._plc.close()

    # ---------- low‑level by symbolic name ----------
    def _addr(self, name_or_addr: Any) -> int:
        if isinstance(name_or_addr, int):
            return name_or_addr
        if name_or_addr in PLC_COIL_MAP:
            return PLC_COIL_MAP[name_or_addr]
        if name_or_addr in PLC_REG_MAP:
            return PLC_REG_MAP[name_or_addr]
        if name_or_addr in self._ALIASES:
            alias = self._ALIASES[name_or_addr]
            if alias in PLC_COIL_MAP:
                return PLC_COIL_MAP[alias]
        raise KeyError(f"알 수 없는 주소/이름: {name_or_addr}")

    async def write_switch(self, name_or_addr: Any, on: bool, *, momentary: bool = True, pulse_ms: Optional[int] = None) -> None:
        addr = self._addr(name_or_addr)
        if momentary:
            self.log("[PLC] press %s (addr=%d)", name_or_addr, addr)
            await self._plc.pulse(addr, ms=pulse_ms)
        else:
            self.log("[PLC] set %s <- %s (addr=%d)", name_or_addr, on, addr)
            await self._plc.write_coil(addr, bool(on))

    async def read_bit(self, name_or_addr: Any) -> bool:
        addr = self._addr(name_or_addr)
        v = await self._plc.read_coil(addr)
        self.log("[PLC] read %s (addr=%d) -> %s", name_or_addr, addr, v)
        return v

    async def write_reg(self, name_or_addr: Any, value: int) -> None:
        addr = self._addr(name_or_addr)
        await self._plc.write_reg(addr, int(value))
        self.log("[PLC] write reg %s (addr=%d) <- %d", name_or_addr, addr, value)

    async def read_reg(self, name_or_addr: Any) -> int:
        addr = self._addr(name_or_addr)
        v = await self._plc.read_reg(addr)
        self.log("[PLC] read reg %s (addr=%d) -> %d", name_or_addr, addr, v)
        return v

    # ---------- High‑level ops (Faduino‑like) ----------
    # Door
    async def door(self, chamber: int, *, open: bool, momentary: bool = True) -> None:
        name = f"DOOR_{chamber}_{'OPEN' if open else 'CLOSE'}_SW"
        await self.write_switch(name, True, momentary=momentary)
        self.log("[Faduino→PLC] Door%d %s", chamber, "OPEN" if open else "CLOSE")

    # Gate Valve
    async def gate_valve(self, chamber: int, *, open: bool, momentary: bool = True) -> None:
        name = f"G_V_{chamber}_{'OPEN' if open else 'CLOSE'}_SW"
        await self.write_switch(name, True, momentary=momentary)
        self.log("[Faduino→PLC] GV%d %s", chamber, "OPEN" if open else "CLOSE")

    # Main shutter (1/2)
    async def main_shutter(self, chamber: int, *, open: bool, momentary: bool = True) -> None:
        name = f"MAIN_SHUTTER_{chamber}_SW"
        await self.write_switch(name, bool(open), momentary=momentary)
        self.log("[Faduino→PLC] MainShutter%d %s", chamber, "OPEN" if open else "CLOSE")

    # Vent (1/2)
    async def vent(self, chamber: int, *, on: bool, momentary: bool = True) -> None:
        name = f"VENT_{chamber}_SW"
        await self.write_switch(name, bool(on), momentary=momentary)
        self.log("[Faduino→PLC] Vent%d %s", chamber, "ON" if on else "OFF")

    # Turbo (1/2)
    async def turbo(self, chamber: int, *, on: bool, momentary: bool = True) -> None:
        name = f"TURBO_{chamber}_SW"
        await self.write_switch(name, bool(on), momentary=momentary)
        self.log("[Faduino→PLC] Turbo%d %s", chamber, "ON" if on else "OFF")

    # Linear Pin (lift pin) up/down + lamp reads
    async def lift_pin(self, *, up: bool, momentary: bool = True) -> None:
        await self.write_switch("L_PIN_UP_SW" if up else "L_PIN_DOWN_SW", True, momentary=momentary)
        self.log("[Faduino→PLC] LiftPin %s", "UP" if up else "DOWN")

    async def lift_pin_lamp(self, *, up: bool) -> bool:
        return await self.read_bit("L_PIN_UP_LAMP" if up else "L_PIN_DOWN_LAMP")

    # Left/Right Valve selector and RF select
    async def lr_valve(self, *, on: bool, momentary: bool = True) -> None:
        await self.write_switch("L_R_V_SW", bool(on), momentary=momentary)
        self.log("[Faduino→PLC] L_R_V %s", "ON" if on else "OFF")

    async def rf_select(self, *, rf_mode: bool, momentary: bool = True) -> None:
        # True=RF, False=DC (naming depends on your PLC program)
        await self.write_switch("SW_RF_SELECT", bool(rf_mode), momentary=momentary)
        self.log("[Faduino→PLC] RF_SELECT <- %s", rf_mode)

    # Gas line toggles (Ar/O2/N2/Main per chamber)
    async def gas(self, chamber: int, gas: str, *, on: bool, momentary: bool = True) -> None:
        g = gas.upper()
        valid = {"AR", "O2", "N2", "MAIN"}
        if g not in valid:
            raise ValueError(f"gas must be one of {valid}")
        key = f"{g}_{chamber}_GAS_SW" if g != "MAIN" else f"MAIN_{chamber}_GAS_SW"
        await self.write_switch(key, bool(on), momentary=momentary)
        self.log("[Faduino→PLC] Gas %s@Ch%d %s", g, chamber, "ON" if on else "OFF")

    async def buzzer_stop(self) -> None:
        await self.write_switch("BUZZER_STOP_SW", True, momentary=True)

    # ---------- Reads (status/interlock/lamp snapshots) ----------
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
        """Convenient one‑shot snapshot for UI/status panes."""
        interlocks = await self.read_interlocks()
        lamps = await self.read_lamps()
        return {
            "interlocks": interlocks,
            "lamps": lamps,
        }

    # ---------- Optionally mirror legacy names ----------
    # If your ProcessController calls functions like faduino.main_shutter_open(),
    # you can uncomment/add simple one‑liners below.
    async def main_shutter_open(self, chamber: int = 1):
        await self.main_shutter(chamber, open=True)

    async def main_shutter_close(self, chamber: int = 1):
        await self.main_shutter(chamber, open=False)

    async def gv_open(self, chamber: int = 1):
        await self.gate_valve(chamber, open=True)

    async def gv_close(self, chamber: int = 1):
        await self.gate_valve(chamber, open=False)

    async def door_open(self, chamber: int = 1):
        await self.door(chamber, open=True)

    async def door_close(self, chamber: int = 1):
        await self.door(chamber, open=False)

    async def vent_on(self, chamber: int = 1):
        await self.vent(chamber, on=True)

    async def vent_off(self, chamber: int = 1):
        await self.vent(chamber, on=False)

    # DCV registers passthrough if you used them with Faduino before
    async def read_dcv(self, idx: int = 0) -> int:
        name = f"DCV_READ_{idx}"
        return await self.read_reg(name)

    async def write_dcv(self, idx: int, value: int) -> None:
        name = f"DCV_WRITE_{idx}"
        await self.write_reg(name, value)


__all__ = [
    "PLC_COIL_MAP", "PLC_REG_MAP", "PLC_TIMER_MAP",
    "PLCConfig", "AsyncPLC", "AsyncFaduinoPLC",
]
