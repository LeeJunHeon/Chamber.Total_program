"""로드락 진공·벤트(VACUUM_ON·VACUUM_OFF) — v1 host/handlers.py:1062-1774 동작 그대로(검사 순서·코일·대기·재확인·문구).

VACUUM_ON: 전환 잠금(진행 중이면 E321, 원복 없음) → CH1·CH2 게이트 closed(E301) → 6비트 스냅샷 — 이미 READY 면 러핑 OFF 확인/정리
  → L_VENT_SW OFF → 0.3 s → L_R_P_OFF_TIMER(E309) → LP_STEP1/2 가 꺼져 있을 때만 L_R_P_SW ON → 5 s → L_R_V_인터락(E310) →
  L_R_V_SW ON → timeout_s(660) 까지 1 s 간격 폴링(READY 성공 우선·NOT_READY 실패·러핑 중 게이트 감시 0.5 s 재확인 E301·
  stop-sequence grace 5 s·LP_STEP 15 s·둘 다 OFF grace 5 s) → 실패·예외면 finally 에서 러핑 V OFF → 5 s → P OFF(응답이 5 s 늦다).
VACUUM_OFF: 전환 잠금 → 게이트 closed → 이미 L_ATM 이면 벤트·러핑 정리 → 러핑 V OFF → 5 s → P OFF → L_VENT_인터락(E311) →
  L_VENT_SW ON → 0.5 s 간격 L_ATM(timeout_s 240, E313) → 실패면 finally L_VENT_SW OFF.
timeout_s 가 숫자가 아니면 try 밖 ValueError → 처리 경계의 E999(v1 그대로).
"""
from __future__ import annotations

import contextlib
from typing import Dict, Optional, Tuple, Union

from v2.ch12.robot_base import Json
from v2.ch12.robot_status import StatusOps

_TRANSITION_BITS = ("L_VAC_READY_SW", "L_VAC_NOT_READY", "LP_STEP1", "LP_STEP2", "L_R_P_SW", "L_R_V_SW")
_DIAG_NAMES = ("L_GAUGE_A", "L_GAUGE_A_INTERLOCK", "L_R_P_OUT", "L_R_V_OUT", "L_VENT_OUT", "L_ATM_SENSOR",
               "L_R_P_OFF_TIMER", "L_R_V_인터락", "G_V_1_OPEN_LAMP", "G_V_1_CLOSE_LAMP", "G_V_2_OPEN_LAMP",
               "G_V_2_CLOSE_LAMP")


class VacuumOps(StatusOps):
    def _require_gates_closed(self) -> Tuple[bool, str, Optional[str]]:
        for ch in (1, 2):
            try:
                st = self._read_gate_state(ch)
            except KeyError as e:
                return False, f"PLC 주소맵에 gate lamp 키가 없습니다: {e}", "E411"
            except Exception as e:
                return False, f"Gate 상태 조회 실패: {type(e).__name__}: {e}", (getattr(e, "code", None) or "E412")
            if st.get("state") != "closed":
                return False, f"VACUUM_ON/OFF 불가 — CH{ch} gate가 CLOSED가 아님({st.get('state')})", "E301"
        return True, "CH1/CH2 gate 모두 CLOSED", None

    def _read_loadlock_vacuum_transition_bits(self) -> Dict[str, bool]:
        with self._plc_call():
            return {n: bool(self.read_bit(n)) for n in _TRANSITION_BITS}

    def _read_loadlock_vacuum_diag(self) -> Dict[str, Union[bool, str]]:
        result: Dict[str, Union[bool, str]] = {}
        try:
            with self._plc_call():
                for name in _DIAG_NAMES:
                    if not self.is_mapped(name):
                        result[name] = "미매핑"
                        continue
                    try:
                        result[name] = bool(self.read_bit(name))
                    except Exception:
                        result[name] = "읽기실패"
        except Exception:
            for name in _DIAG_NAMES:
                result.setdefault(name, "미매핑" if not self.is_mapped(name) else "읽기실패")
        return result

    # ───────── VACUUM_ON ─────────
    def vacuum_on(self, data: Json) -> Json:
        timeout_s = float(data.get("timeout_s", 660.0))
        with self._plc_command("VACUUM_ON"):
            st = {"success": False, "cleanup_done": False}

            def stop_roughing(delay_s: float = 5.0) -> None:
                with contextlib.suppress(Exception):
                    with self._plc_call():
                        self.write_switch("L_R_V_SW", False)
                self.sleep(delay_s)
                with contextlib.suppress(Exception):
                    with self._plc_call():
                        self.write_switch("L_R_P_SW", False)
                st["cleanup_done"] = True

            busy = self._fail_if_loadlock_transition_busy("VACUUM_ON")
            if busy is not None:
                return busy
            try:
                with self._loadlock_transition_lock:
                    self._loadlock_transition_tag = "VACUUM_ON"
                    try:
                        return self._vacuum_on_body(timeout_s, st, stop_roughing)
                    finally:
                        self._loadlock_transition_tag = None
            except Exception as e:
                return self._fail(e)
            finally:
                if (not st["success"]) and (not st["cleanup_done"]):
                    stop_roughing(delay_s=5.0)

    def _vacuum_on_body(self, timeout_s: float, st: dict, stop_roughing) -> Json:
        ok, msg, code = self._require_gates_closed()
        if not ok:
            return self._fail(msg, code=code)
        snap0 = self._read_loadlock_vacuum_transition_bits()
        if snap0["L_VAC_READY_SW"]:
            if (not snap0["L_R_P_SW"]) and (not snap0["L_R_V_SW"]):
                st["success"] = True
                return self._ok("VACUUM_ON: 이미 L_VAC_READY_SW=TRUE 상태")
            stop_roughing(delay_s=5.0)
            off_deadline = self.monotonic() + 10.0
            while self.monotonic() < off_deadline:
                with self._plc_call():
                    pump_sw2 = bool(self.read_bit("L_R_P_SW"))
                    valve_sw2 = bool(self.read_bit("L_R_V_SW"))
                if (not pump_sw2) and (not valve_sw2):
                    st["success"] = True
                    return self._ok("VACUUM_ON: 이미 L_VAC_READY_SW=TRUE였고 "
                                    "L_R_V_SW OFF → 5초 → L_R_P_SW OFF 정리 완료")
                self.sleep(1.0)
            return self._fail("VACUUM_ON 실패 — L_VAC_READY_SW=TRUE였지만 "
                              "L_R_P_SW/L_R_V_SW OFF 완료 확인 실패", code="E312")

        with self._plc_call():
            self.write_switch("L_VENT_SW", False)
        self.sleep(0.3)
        with self._plc_call():
            if self.read_bit("L_R_P_OFF_TIMER"):
                return self._fail("러핑펌프 OFF 타이머 진행 중 → 잠시 후 재시도", code="E309")
        with self._plc_call():
            lp1_active = bool(self.read_bit("LP_STEP1"))
            lp2_active = bool(self.read_bit("LP_STEP2"))
        if not (lp1_active or lp2_active):
            with self._plc_call():
                self.write_switch("L_R_P_SW", True)
            self.sleep(5.0)
            with self._plc_call():
                rv_interlock = bool(self.read_bit("L_R_V_인터락"))
            if not rv_interlock:
                stop_roughing(delay_s=5.0)
                return self._fail("L_R_V_인터락=FALSE → 러핑밸브 개방 불가 (L_R_P_SW/L_R_V_SW OFF 처리)", code="E310")
            with self._plc_call():
                self.write_switch("L_R_V_SW", True)
        return self._vacuum_on_poll(timeout_s, st, stop_roughing)

    def _vacuum_on_poll(self, timeout_s: float, st: dict, stop_roughing) -> Json:
        deadline = self.monotonic() + timeout_s
        transition_deadline: Optional[float] = None
        both_off_deadline: Optional[float] = None
        lp_step_off_deadline: Optional[float] = None
        transition_grace_s = 5.0
        both_off_grace_s = 5.0
        while self.monotonic() < deadline:
            snap = self._read_loadlock_vacuum_transition_bits()
            vac_ready, vac_not_ready = snap["L_VAC_READY_SW"], snap["L_VAC_NOT_READY"]
            lp_step1, lp_step2 = snap["LP_STEP1"], snap["LP_STEP2"]
            pump_sw, valve_sw = snap["L_R_P_SW"], snap["L_R_V_SW"]

            if vac_ready:
                off_deadline = self.monotonic() + 5.0
                while self.monotonic() < off_deadline:
                    snap2 = self._read_loadlock_vacuum_transition_bits()
                    if (not snap2["L_R_P_SW"]) and (not snap2["L_R_V_SW"]):
                        st["success"] = True
                        return self._ok("VACUUM_ON 완료 — PLC의 L_VAC_READY_SW=TRUE 및 러핑 OFF 완료 확인")
                    self.sleep(0.2)
                stop_roughing(delay_s=5.0)
                off_deadline = self.monotonic() + 10.0
                while self.monotonic() < off_deadline:
                    snap2 = self._read_loadlock_vacuum_transition_bits()
                    if (not snap2["L_R_P_SW"]) and (not snap2["L_R_V_SW"]):
                        st["success"] = True
                        return self._ok("VACUUM_ON 완료 — L_VAC_READY_SW=TRUE 확인 후 "
                                        "fallback으로 L_R_V_SW/L_R_P_SW OFF 정리 완료")
                    self.sleep(1.0)
                return self._fail("VACUUM_ON 실패 — L_VAC_READY_SW=TRUE였지만 "
                                  "L_R_P_SW/L_R_V_SW OFF 완료 확인 실패", code="E312")

            if vac_not_ready:
                return self._fail("VACUUM_ON 실패 — PLC가 L_VAC_NOT_READY=TRUE로 판정 "
                                  f"(LP_STEP1={lp_step1}, LP_STEP2={lp_step2}, "
                                  f"L_R_P_SW={pump_sw}, L_R_V_SW={valve_sw})", code="E312")

            gate_bad = None
            try:
                for gch in (1, 2):
                    gst = self._read_gate_state(gch)
                    if gst.get("state") != "closed":
                        gate_bad = gst
                        break
            except Exception:
                gate_bad = None
            if gate_bad is not None:
                self.sleep(0.5)
                confirm = None
                with contextlib.suppress(Exception):
                    confirm = self._read_gate_state(int(gate_bad["ch"]))
                if confirm is not None and confirm.get("state") != "closed":
                    diag = self._read_loadlock_vacuum_diag()
                    self.log("PLC_REMOTE", f"[VACUUM_ON_DIAG/gate_not_closed] gate={confirm}, snap={snap}, diag={diag}")
                    return self._fail(f"VACUUM_ON 중단 — 러핑 진행 중 CH{confirm['ch']} gate가 "
                                      f"CLOSED가 아님(state={confirm['state']}, "
                                      f"OPEN_LAMP={confirm['open_lamp']}, "
                                      f"CLOSE_LAMP={confirm['close_lamp']})", code="E301")

            if pump_sw and valve_sw:
                transition_deadline = None
                both_off_deadline = None
            elif pump_sw and (not valve_sw):
                now = self.monotonic()
                both_off_deadline = None
                if transition_deadline is None:
                    transition_deadline = now + transition_grace_s
                elif now >= transition_deadline:
                    diag = self._read_loadlock_vacuum_diag()
                    self.log("PLC_REMOTE", f"[VACUUM_ON_DIAG/stop_seq_stall] snap={snap}, diag={diag}")
                    if diag.get("L_R_P_OFF_TIMER") is True:
                        return self._fail("VACUUM_ON 실패 — 러핑 중 에어압 알람"
                                          "(L_R_P_OFF_TIMER=TRUE)으로 러핑밸브 인터락이 해제됨 "
                                          "→ AIR 공급/러핑펌프 하드웨어 점검 필요 "
                                          f"(LP_STEP1={lp_step1}, LP_STEP2={lp_step2}, diag={diag})", code="E312")
                    if lp_step1 and (not lp_step2):
                        return self._fail("VACUUM_ON 실패 — LP_STEP1 유지 상태에서 T0050(60s) "
                                          "정상 경로가 아닌데 L_R_V_SW가 OFF됨. "
                                          f"(LP_STEP1={lp_step1}, LP_STEP2={lp_step2}, diag={diag})", code="E312")
                    return self._fail("VACUUM_ON 실패 — stop-sequence 진입 후 "
                                      "L_VAC_READY_SW가 유예시간 내 들어오지 않음 "
                                      f"(LP_STEP1={lp_step1}, LP_STEP2={lp_step2}, "
                                      f"L_R_P_SW={pump_sw}, L_R_V_SW={valve_sw}, diag={diag})", code="E312")
            elif (not pump_sw) and (not valve_sw):
                now = self.monotonic()
                transition_deadline = None
                if lp_step1 or lp_step2:
                    if lp_step_off_deadline is None:
                        lp_step_off_deadline = now + 15.0
                    elif now >= lp_step_off_deadline:
                        diag = self._read_loadlock_vacuum_diag()
                        self.log("PLC_REMOTE", f"[VACUUM_ON_DIAG/lp_step_stall] snap={snap}, diag={diag}")
                        return self._fail("VACUUM_ON 실패 — LP_STEP 진행 중 러핑 스위치가 "
                                          "OFF된 채 L_VAC_READY_SW 미도달 (15s 초과). "
                                          f"(LP_STEP1={lp_step1}, LP_STEP2={lp_step2}, diag={diag})", code="E312")
                    both_off_deadline = None
                    self.sleep(1.0)
                    continue
                lp_step_off_deadline = None
                if both_off_deadline is None:
                    both_off_deadline = now + both_off_grace_s
                elif now >= both_off_deadline:
                    snap2 = self._read_loadlock_vacuum_transition_bits()
                    if snap2["L_VAC_READY_SW"]:
                        self.sleep(0.2)
                        continue
                    try:
                        diag = self._read_loadlock_vacuum_diag()
                    except Exception:
                        diag = {}
                    self.log("PLC_REMOTE", f"[VACUUM_ON_DIAG/both_off] diag={diag}, snap2={snap2}")
                    if snap2["L_VAC_NOT_READY"]:
                        return self._fail("VACUUM_ON 실패 — 러핑 OFF 후 PLC가 L_VAC_NOT_READY=TRUE로 판정", code="E312")
                    gi = diag.get("L_GAUGE_A_INTERLOCK")
                    gi_txt = f", L_GAUGE_A_INTERLOCK={gi}" if gi != "미매핑" else ""
                    return self._fail("VACUUM_ON 실패 — L_VAC_READY_SW/L_VAC_NOT_READY 없이 "
                                      "L_R_P_SW와 L_R_V_SW가 모두 OFF로 전환됨 "
                                      f"(L_VAC_READY_SW={snap2['L_VAC_READY_SW']}, "
                                      f"L_VAC_NOT_READY={snap2['L_VAC_NOT_READY']}, "
                                      f"LP_STEP1={snap2['LP_STEP1']}, "
                                      f"LP_STEP2={snap2['LP_STEP2']}, "
                                      f"L_R_P_SW={snap2['L_R_P_SW']}, "
                                      f"L_R_V_SW={snap2['L_R_V_SW']}"
                                      f"{gi_txt}) "
                                      "원인 미확정 — 상세 진단은 로그의 "
                                      "VACUUM_ON_DIAG/both_off 참조", code="E312")
            elif (not pump_sw) and valve_sw:
                diag = self._read_loadlock_vacuum_diag()
                self.log("PLC_REMOTE", f"[VACUUM_ON_DIAG/pump_off_valve_on] snap={snap}, diag={diag}")
                return self._fail("VACUUM_ON 실패 — READY 전 러핑펌프가 먼저 OFF됨 "
                                  f"(LP_STEP1={lp_step1}, LP_STEP2={lp_step2}, "
                                  f"L_R_P_SW={pump_sw}, L_R_V_SW={valve_sw}, diag={diag})", code="E312")
            self.sleep(1.0)

        try:
            snap_timeout = self._read_loadlock_vacuum_transition_bits()
        except Exception:
            snap_timeout = None
        diag = self._read_loadlock_vacuum_diag()
        self.log("PLC_REMOTE", f"[VACUUM_ON_DIAG/timeout] snap={snap_timeout}, diag={diag}")
        if snap_timeout and snap_timeout["L_VAC_NOT_READY"]:
            return self._fail(f"VACUUM_ON 실패 — {int(timeout_s)}s 타임아웃, "
                              "PLC가 L_VAC_NOT_READY=TRUE로 판정 "
                              "(진공 게이지 인터락 미충족) "
                              f"(L_GAUGE_A_INTERLOCK={diag.get('L_GAUGE_A_INTERLOCK')})", code="E312")
        if snap_timeout:
            return self._fail(f"VACUUM_ON 타임아웃 — {int(timeout_s)}s 내 진공 미도달 "
                              "(진공 게이지 인터락/펌프/누설 점검 필요) "
                              f"(L_GAUGE_A_INTERLOCK={diag.get('L_GAUGE_A_INTERLOCK')}, "
                              f"L_R_P_SW={snap_timeout['L_R_P_SW']}, "
                              f"L_R_V_SW={snap_timeout['L_R_V_SW']})", code="E312")
        return self._fail(f"VACUUM_ON 타임아웃 — {int(timeout_s)}s 내 진공 미도달 (상태 스냅샷 읽기 실패)", code="E312")

    # ───────── VACUUM_OFF ─────────
    def vacuum_off(self, data: Json) -> Json:
        timeout_s = float(data.get("timeout_s", 240.0))
        with self._plc_command("VACUUM_OFF"):
            st = {"success": False}
            busy = self._fail_if_loadlock_transition_busy("VACUUM_OFF")
            if busy is not None:
                return busy
            try:
                with self._loadlock_transition_lock:
                    self._loadlock_transition_tag = "VACUUM_OFF"
                    try:
                        return self._vacuum_off_body(timeout_s, st)
                    finally:
                        self._loadlock_transition_tag = None
            except Exception as e:
                return self._fail(e)
            finally:
                if not st["success"]:
                    with contextlib.suppress(Exception):
                        with self._plc_call():
                            self.write_switch("L_VENT_SW", False)

    def _vacuum_off_body(self, timeout_s: float, st: dict) -> Json:
        ok, msg, code = self._require_gates_closed()
        if not ok:
            return self._fail(msg, code=code)
        with self._plc_call():
            atm_now = bool(self.read_bit("L_ATM"))
        if atm_now:
            with self._plc_call():
                self.write_switch("L_VENT_SW", False)
                self.write_switch("L_R_V_SW", False)
            self.sleep(5.0)
            with self._plc_call():
                self.write_switch("L_R_P_SW", False)
            st["success"] = True
            return self._ok("VACUUM_OFF: 이미 대기압 상태 (L_ATM=TRUE)")
        with self._plc_call():
            self.write_switch("L_R_V_SW", False)
        self.sleep(5.0)
        with self._plc_call():
            self.write_switch("L_R_P_SW", False)
        with self._plc_call():
            if not self.read_bit("L_VENT_인터락"):
                return self._fail("L_VENT_인터락=FALSE → 벤트 불가", code="E311")
        with self._plc_call():
            self.write_switch("L_VENT_SW", True)
        deadline = self.monotonic() + timeout_s
        while self.monotonic() < deadline:
            with self._plc_call():
                atm = self.read_bit("L_ATM")
            if atm:
                with self._plc_call():
                    self.write_switch("L_VENT_SW", False)
                st["success"] = True
                return self._ok("VACUUM_OFF 완료 (L_ATM=TRUE, L_VENT_SW=FALSE)")
            self.sleep(0.5)
        with contextlib.suppress(Exception):
            with self._plc_call():
                self.write_switch("L_VENT_SW", False)
        return self._fail(f"VACUUM_OFF 타임아웃: {int(timeout_s)}s 내 L_ATM TRUE 미도달 (N2 gas 부족)", code="E313")
