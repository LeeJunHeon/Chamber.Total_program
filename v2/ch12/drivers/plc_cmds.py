"""PLC 고수준 명령(챔버12) — v1 device/plc.py:1747-1830(door/gate/main_shutter/vent/turbo/lift_pin/lr_valve/rf_select/
power_select/buzzer_stop/gas/set)·2084-2218(power_enable/write/apply/read, rf_enable/write_w/apply/read_fwd_ref).

PlcDriver 가 이 믹스인을 물려받는다. 모두 worker 요청(write_switch → write_coil/pulse, 전원 → submit)이고 출처 태그(source)·
on_done·kind 를 받는다. 이름·코일·로그 문구는 v1 그대로.
"""
from __future__ import annotations

from typing import Callable, Optional

from v2.ch12.drivers import plc_map as PM
from v2.ch12.drivers.plc_conv import dac_code, dc_scale, rf_regs, rf_scale


class PlcCommands:
    # 고수준(device/plc.py:1747-1830)
    def door(self, chamber: int, *, open: bool, momentary: bool = False, source: str = "", on_done=None):
        return self.write_switch(f"DOOR_{chamber}_{'OPEN' if open else 'CLOSE'}_SW", True, momentary=momentary,
                                 source=source, on_done=on_done)

    def gate_valve(self, chamber: int, *, open: bool, momentary: bool = False, source: str = "", on_done=None):
        return self.write_switch(f"G_V_{chamber}_{'OPEN' if open else 'CLOSE'}_SW", True, momentary=momentary,
                                 source=source, on_done=on_done)

    def main_shutter(self, chamber: int, *, open: bool, momentary: bool = False, source: str = "", on_done=None):
        return self.write_switch(f"MAIN_SHUTTER_{chamber}_SW", bool(open), momentary=momentary, source=source,
                                 on_done=on_done)

    def vent(self, chamber: int, *, on: bool, momentary: bool = False, source: str = "", on_done=None):
        return self.write_switch(f"VENT_{chamber}_SW", bool(on), momentary=momentary, source=source, on_done=on_done)

    def turbo(self, chamber: int, *, on: bool, momentary: bool = False, source: str = "", on_done=None):
        return self.write_switch(f"TURBO_{chamber}_SW", bool(on), momentary=momentary, source=source, on_done=on_done)

    def lift_pin(self, *, up: bool, momentary: bool = False, source: str = "", on_done=None):
        return self.write_switch("L_PIN_UP_SW" if up else "L_PIN_DOWN_SW", True, momentary=momentary, source=source,
                                 on_done=on_done)

    def lr_valve(self, *, on: bool, momentary: bool = False, source: str = "", on_done=None):
        return self.write_switch("L_R_V_SW", bool(on), momentary=momentary, source=source, on_done=on_done)

    def rf_select(self, *, rf_mode: bool, momentary: bool = False, source: str = "", on_done=None):
        """SW_RF_SELECT 214(M00136) — 공정 Power Select(use_power_select, CH2)·종료 시 OFF(controller/process_controller.py:2431-2435)."""
        return self.write_switch("SW_RF_SELECT", bool(rf_mode), momentary=momentary, source=source, on_done=on_done)

    def power_select(self, *, on: bool, momentary: bool = False, source: str = "", on_done=None):
        """SW_POWER_SELECT 215(M00137) — RF Pulse 경로(CH2 use_rf_pulse 때 ON, RF 연속 전 OFF)·화면 Power Select 버튼(main.py:1241)."""
        return self.write_switch("SW_POWER_SELECT", bool(on), momentary=momentary, source=source, on_done=on_done)

    def buzzer_stop(self, *, momentary: bool = False, source: str = "", on_done=None):
        return self.write_switch("BUZZER_STOP_SW", True, momentary=momentary, source=source, on_done=on_done)

    def gas(self, chamber: int, gas: str, *, on: bool, momentary: bool = False, source: str = "", on_done=None):
        g = (gas or "").strip().upper()
        if g not in {"AR", "O2", "N2", "MAIN"}:
            raise ValueError(f"gas must be one of {{'AR', 'O2', 'N2', 'MAIN'}}")
        key = f"Ar_{chamber}_GAS_SW" if g == "AR" else f"{g}_{chamber}_GAS_SW"
        if key not in PM.COIL_MAP:
            raise ValueError(f"가스 '{g}'는 CH{chamber}에 존재하지 않습니다 (키: {key})")
        return self.write_switch(key, bool(on), momentary=momentary, source=source, on_done=on_done)

    def set(self, name: str, on: bool, *, ch: int = 1, momentary: bool = False, source: str = "", on_done=None):
        """논리명 MV/MS/AR/O2/N2/MAIN/G1~G3 또는 맵 이름(device/plc.py:2221-2253)."""
        if not name:
            raise ValueError("name required")
        key = name.strip().upper().replace(" ", "").replace("_", "")
        if key in {"AR", "O2", "N2", "MAIN"}:
            return self.gas(int(ch), key, on=on, momentary=momentary, source=source, on_done=on_done)
        if key in ("MV", "MS"):
            return self.write_switch(PM.LOGICAL.get(f"{key}@{int(ch)}", f"{'M_V' if key == 'MV' else 'MAIN_SHUTTER'}_1_SW"),
                                     on, momentary=momentary, source=source, on_done=on_done)
        if key in {"G1", "G2", "G3"}:
            return self.write_switch(PM.LOGICAL[key], on, momentary=momentary, source=source, on_done=on_done)
        PM.resolve(name)
        return self.write_switch(name, on, momentary=momentary, source=source, on_done=on_done)

    # ───────── 전원(DC·RF) 공통(device/plc.py:2084-2218) ─────────
    def power_enable(self, on: bool = True, *, family: str = "DCV", set_idx: Optional[int] = None,
                     set_key: Optional[str] = None, source: str = "", on_done=None, kind: str = "write",
                     not_before: Optional[float] = None):
        idx = int(self.cfg["PLC_DC_WRITE_INDEX"]) if set_idx is None else int(set_idx)
        key = set_key or f"{family}_SET_{idx}"
        if key not in PM.COIL_MAP:
            raise KeyError(f"PLC_COIL_MAP에 '{key}' 없음 (family={family}, idx={idx})")
        a = PM.COIL_MAP[key]

        def fn(s):
            self._log(source, f"POWER SET ({family})[{idx}] <- {bool(on)}")
            self.io_write_coil(s, a, bool(on))
            self._log(source, f"set {key} <- {bool(on)} (addr={a})")
        return self.submit(f"{key}={int(bool(on))}", fn, kind=kind, source=source, on_done=on_done, not_before=not_before)

    def power_write(self, power_w: float, *, family: str = "DCV", write_idx: Optional[int] = None,
                    write_key: Optional[str] = None, source: str = "", on_done=None, kind: str = "write",
                    guard: Optional[Callable[[], bool]] = None, not_before: Optional[float] = None):
        """W → DAC → WRITE 레지스터(D4+idx). value = DAC 코드. guard() 가 False 면 실행 때 건너뜀(value None)."""
        code = dac_code(self.cfg, power_w)
        if write_key is None:
            idx = int(self.cfg["PLC_DC_WRITE_INDEX"]) if write_idx is None else int(write_idx)
            write_key = f"{family}_WRITE_{idx}"
        if write_key not in PM.REG_MAP:
            raise KeyError(f"PLC_REG_MAP에 '{write_key}' 없음 (family={family})")
        a = PM.REG_MAP[write_key]

        def fn(s):
            if guard is not None and not guard():
                return None
            self.io_write_reg(s, a, code)
            self._log(source, f"write reg {write_key} (addr={a}) <- {code}")
            self._log(source, f"POWER WRITE ({family}) {write_key} <- W={float(power_w):.3f} (DAC={code})")
            return code
        return self.submit(f"{write_key}={code}", fn, kind=kind, source=source, on_done=on_done, not_before=not_before)

    def power_apply(self, power_w: float, *, family: str = "DCV", channel: Optional[int] = None, ensure_set: bool = True,
                    source: str = "", on_done=None):
        idx = int(self.cfg["PLC_DC_WRITE_INDEX"]) if channel is None else int(channel)
        if ensure_set:
            self.power_enable(True, family=family, set_idx=idx, source=source)
        return self.power_write(power_w, family=family, write_idx=idx, source=source, on_done=on_done)

    def power_read(self, *, family: str = "DCV", v_idx: Optional[int] = None, i_idx: Optional[int] = None,
                   source: str = "", on_done=None):
        """FC3 D(v) 후 FC3 D(i) 따로 2번 → (P, V, I)."""
        vk, ik = f"{family}_READ_{0 if v_idx is None else int(v_idx)}", f"{family}_READ_{1 if i_idx is None else int(i_idx)}"
        for k in (vk, ik):
            if k not in PM.REG_MAP:
                raise KeyError(f"PLC_REG_MAP에 '{k}' 없음 (family={family})")

        def fn(s):
            return dc_scale(self.cfg, self.io_read_reg(s, PM.REG_MAP[vk]), self.io_read_reg(s, PM.REG_MAP[ik]))
        return self.submit(f"power_read {vk}/{ik}", fn, kind="read", source=source, on_done=on_done)

    @staticmethod
    def _rf_ch(rf_ch: int) -> int:
        return 1 if int(rf_ch) != 2 else 2

    def rf_enable(self, on: bool = True, *, rf_ch: int = 1, source: str = "", on_done=None, kind: str = "write",
                  not_before: Optional[float] = None):
        return self.power_enable(on, family="DCV", set_idx=self._rf_ch(rf_ch), source=source, on_done=on_done, kind=kind,
                                 not_before=not_before)

    def rf_write_w(self, power_w: float, *, rf_ch: int = 1, source: str = "", on_done=None, kind: str = "write",
                   guard=None, not_before: Optional[float] = None):
        return self.power_write(power_w, family="DCV", write_idx=self._rf_ch(rf_ch), source=source, on_done=on_done,
                                kind=kind, guard=guard, not_before=not_before)

    def rf_apply(self, power_w: float, *, ensure_set: bool = True, rf_ch: int = 1, source: str = "", on_done=None,
                 kind: str = "write", guard=None):
        ch = self._rf_ch(rf_ch)
        if ensure_set:
            self.rf_enable(True, rf_ch=ch, source=source)
        return self.rf_write_w(power_w, rf_ch=ch, source=source, on_done=on_done, kind=kind, guard=guard)

    def rf_read_fwd_ref(self, *, rf_ch: int = 1, zeroing: Optional[bool] = None, source: str = "", on_done=None):
        fk, rk = rf_regs(rf_ch)

        def fn(s):
            f_raw = self.io_read_reg(s, PM.REG_MAP[fk])
            r_raw = self.io_read_reg(s, PM.REG_MAP[rk])
            return rf_scale(self.cfg, f_raw, r_raw, rf_ch, zeroing)
        return self.submit(f"rf_read ch{rf_ch}", fn, kind="read", source=source, on_done=on_done)
