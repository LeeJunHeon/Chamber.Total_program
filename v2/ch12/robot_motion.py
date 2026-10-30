"""게이트·척·4핀 이동 — v1 host/handlers.py:1080-1104(게이트 열기 전 로드락 검사)·1777-2327 동작 그대로.

- 4PIN_UP/DOWN: L_PIN_인터락(E314/E315) → 스위치 펄스 → settle_s(5) → 램프를 poll_s(1) 간격으로 wait_s(20) 까지(E316/E317).
  busy·채널 잠금·게이트·로드락 검사 없음.
- CHx_GATE_OPEN: busy E205 → 로드락 전환 E321 → lock_chN → busy → 게이트 잠금 → 전환 E321 → 태그 GATE_OPEN_CHn →
  램프 둘 다 TRUE E306 → 이미 open(CH2 는 MAIN_SHUTTER_2_인터락 E330 → 셔터 ON) → L_VENT_SW·L_R_P_SW·L_R_V_SW·L_ATM 하나라도
  TRUE E321 → 반대 게이트 closed 아님 E303 → G_V_n_인터락 E302 → OPEN_SW 펄스 → wait_s(5) → OPEN_LAMP(E304) → CH2 셔터.
  예외는 e.code 또는 E412(KeyError 도 E412 — 태그 블록 밖이라 meta 없음).
- CHx_GATE_CLOSE: busy E205 → lock_chN → busy → 게이트 잠금 → 태그 GATE_CLOSE_CHn → 이미 closed(CH2 셔터 OFF) → E306 →
  LOADING_n_SENSOR_LAMP(E305, 읽기 KeyError E411·그 밖 E412) → CLOSE_SW 펄스 → wait_s → 램프·센서 → CH2 셔터 OFF(실패 E331).
  VACUUM 진행 여부는 보지 않는다.
- CHx_CHUCK_UP = MID 이동(위치 모르면 1 s 간격 CHUCK_POSITION_RESOLVE_TIMEOUT_S 90 s 확정 대기, 끝까지 모르면 E318),
  CHx_CHUCK_DOWN = 최하단. Z-POWER ON → 0.2 s → 방향 ON → 1 s 간격 목표 램프 → 같은 PLC 구간에서 둘 다 OFF. 시간 초과 E318,
  예외 E412(둘 다 OFF 보장). lock_chN 과 busy 만 본다.
"""
from __future__ import annotations

import contextlib
from typing import Tuple

from v2.ch12.robot_base import Json
from v2.ch12.robot_vacuum import VacuumOps


class MotionOps(VacuumOps):
    # ───────── 4PIN ─────────
    def _poll_bit_until_true(self, bit_name: str, *, timeout_s: float, interval_s: float = 1.0) -> bool:
        deadline = self.monotonic() + float(timeout_s)
        while True:
            with self._plc_call():
                v = bool(self.read_bit(bit_name))
            if v:
                return True
            now = self.monotonic()
            if now >= deadline:
                return False
            self.sleep(min(float(interval_s), deadline - now))

    def _four_pin(self, data: Json, up: bool) -> Json:
        wait_s = float(data.get("wait_s", 20.0))
        poll_s = float(data.get("poll_s", 1.0))
        settle_s = float(data.get("settle_s", 5.0))
        name = "4PIN_UP" if up else "4PIN_DOWN"
        lamp = "L_PIN_UP_LAMP" if up else "L_PIN_DOWN_LAMP"
        try:
            with self._plc_command(name):
                with self._plc_call():
                    if not self.read_bit("L_PIN_인터락"):
                        return self._fail(f"L_PIN_인터락=FALSE → {name} 불가", code="E314" if up else "E315")
                with self._plc_call():
                    self.press_switch("L_PIN_UP_SW" if up else "L_PIN_DOWN_SW")
                self.sleep(settle_s)
                lamp_ok = self._poll_bit_until_true(lamp, timeout_s=wait_s, interval_s=poll_s)
                if lamp_ok:
                    return self._ok(f"{name} 완료 — {lamp}=TRUE (timeout {int(wait_s)}s, poll {poll_s:.1f}s)")
                return self._fail(f"{name} 실패 — {int(wait_s)}s 내 {lamp}=TRUE 미도달 (poll {poll_s:.1f}s)",
                                  code="E316" if up else "E317")
        except Exception as e:
            return self._fail(e)

    def four_pin_up(self, data: Json) -> Json:
        return self._four_pin(data, True)

    def four_pin_down(self, data: Json) -> Json:
        return self._four_pin(data, False)

    # ───────── 게이트 ─────────
    def _require_loadlock_safe_for_gate_open(self) -> Tuple[bool, str]:
        with self._plc_call():
            s = {n: bool(self.read_bit(n)) for n in ("L_VENT_SW", "L_R_P_SW", "L_R_V_SW", "L_ATM")}
        bad = [k for k, v in s.items() if v]
        if bad:
            return False, f"Loadlock 상태로 인해 GATE_OPEN 불가 ({', '.join(f'{k}=TRUE' for k in bad)})"
        return True, "Loadlock 상태 OK"

    def gate_open(self, data: Json) -> Json:
        ch = int(data.get("ch", 1))
        wait_s = float(data.get("wait_s", 5.0))
        busy = self._fail_if_ch_busy(ch, f"CH{ch}_GATE_OPEN")
        if busy is not None:
            return busy
        busy_ll = self._fail_if_loadlock_transition_busy(f"CH{ch}_GATE_OPEN")
        if busy_ll is not None:
            return busy_ll
        if ch not in (1, 2):
            return self._fail(f"지원하지 않는 CH: {ch}", code="E201")
        interlock, sw, lamp = f"G_V_{ch}_인터락", f"G_V_{ch}_OPEN_SW", f"G_V_{ch}_OPEN_LAMP"
        try:
            with self.lock_ch[ch]:
                busy = self._fail_if_ch_busy(ch, f"CH{ch}_GATE_OPEN")
                if busy is not None:
                    return busy
                with self._loadlock_gate_lock:
                    busy_ll = self._fail_if_loadlock_transition_busy(f"CH{ch}_GATE_OPEN")
                    if busy_ll is not None:
                        return busy_ll
                    with self._plc_command(f"GATE_OPEN_CH{ch}"):
                        return self._gate_open_body(ch, wait_s, interlock, sw, lamp)
        except Exception as e:
            return self._fail(e, code=getattr(e, "code", None) or "E412")

    def _gate_open_body(self, ch: int, wait_s: float, interlock: str, sw: str, lamp: str) -> Json:
        cur_st = self._read_gate_state(ch)
        if cur_st["state"] == "invalid_both_true":
            return self._fail(f"CH{ch} gate lamp 이상(OPEN/CLOSE 모두 TRUE): {cur_st}", code="E306")
        if cur_st["state"] == "open":
            if ch == 2:
                with self._plc_call():
                    if not self.read_bit("MAIN_SHUTTER_2_인터락"):
                        return self._fail("MAIN_SHUTTER_2_인터락=FALSE → MAIN_SHUTTER_OPEN 불가", code="E330")
                    self.main_shutter(2, open=True)
                return self._ok("CH2_GATE_OPEN: 이미 OPEN + MAIN_SHUTTER_OPEN", current=cur_st)
            return self._ok(f"CH{ch}_GATE_OPEN: 이미 OPEN 상태", current=cur_st)
        ok_ll, msg_ll = self._require_loadlock_safe_for_gate_open()
        if not ok_ll:
            return self._fail(msg_ll, code="E321")
        other = 2 if ch == 1 else 1
        other_st = self._read_gate_state(other)
        if other_st["state"] != "closed":
            return self._fail(f"다른 챔버 Gate가 CLOSED가 아님: CH{other}={other_st['state']} → CH{ch}_GATE_OPEN 불가",
                              code="E303")
        with self._plc_call():
            il = self.read_bit(interlock)
        if not il:
            return self._fail(f"{interlock}=FALSE → CH{ch}_GATE_OPEN 불가", code="E302")
        with self._plc_call():
            self.press_switch(sw)
        self.sleep(wait_s)
        with self._plc_call():
            ok = self.read_bit(lamp)
        if not ok:
            return self._fail(f"CH{ch}_GATE_OPEN 실패 — {lamp}=FALSE (대기 {int(wait_s)}s)", code="E304")
        if ch == 2:
            with self._plc_call():
                if not self.read_bit("MAIN_SHUTTER_2_인터락"):
                    return self._fail("MAIN_SHUTTER_2_인터락=FALSE → MAIN_SHUTTER_OPEN 불가", code="E330")
                self.main_shutter(2, open=True)
            return self._ok(f"CH2_GATE_OPEN 완료 — {lamp}=TRUE + MAIN_SHUTTER_OPEN (대기 {int(wait_s)}s)")
        return self._ok(f"CH{ch}_GATE_OPEN 완료 — {lamp}=TRUE (대기 {int(wait_s)}s)")

    def gate_close(self, data: Json) -> Json:
        ch = int(data.get("ch", 1))
        wait_s = float(data.get("wait_s", 5.0))
        busy = self._fail_if_ch_busy(ch, f"CH{ch}_GATE_CLOSE")
        if busy is not None:
            return busy
        if ch not in (1, 2):
            return self._fail(f"지원하지 않는 CH: {ch}", code="E201")
        sw, lamp, sensor_lamp = f"G_V_{ch}_CLOSE_SW", f"G_V_{ch}_CLOSE_LAMP", f"LOADING_{ch}_SENSOR_LAMP"
        with self.lock_ch[ch]:
            busy = self._fail_if_ch_busy(ch, f"CH{ch}_GATE_CLOSE")
            if busy is not None:
                return busy
            with self._loadlock_gate_lock:
                with self._plc_command(f"GATE_CLOSE_CH{ch}"):
                    try:
                        return self._gate_close_body(ch, wait_s, sw, lamp, sensor_lamp)
                    except Exception as e:
                        return self._fail(e)

    def _gate_close_body(self, ch: int, wait_s: float, sw: str, lamp: str, sensor_lamp: str) -> Json:
        cur_st = self._read_gate_state(ch)
        if cur_st["state"] == "closed":
            if ch == 2:
                with self._plc_call():
                    self.main_shutter(2, open=False)
                return self._ok("CH2_GATE_CLOSE: 이미 CLOSED + MAIN_SHUTTER_CLOSE", current=cur_st)
            return self._ok(f"CH{ch}_GATE_CLOSE: 이미 CLOSED 상태", current=cur_st)
        if cur_st["state"] == "invalid_both_true":
            return self._fail(f"CH{ch} gate lamp 이상(OPEN/CLOSE 모두 TRUE): {cur_st}", code="E306")
        try:
            with self._plc_call():
                sensor_ok = bool(self.read_bit(sensor_lamp))
        except KeyError as e:
            return self._fail(f"PLC 주소맵에 {sensor_lamp} 키가 없습니다: {e}", code="E411")
        except Exception as e:
            return self._fail(e, code=getattr(e, "code", None) or "E412")
        if not sensor_ok:
            return self._fail(f"CH{ch}_GATE_CLOSE 불가 — {sensor_lamp}=FALSE "
                              f"(arm/loading sensor 미감지, gate close 명령 미전송)", code="E305")
        with self._plc_call():
            self.press_switch(sw)
        self.sleep(wait_s)
        with self._plc_call():
            ok = self.read_bit(lamp)
            sensor_after = bool(self.read_bit(sensor_lamp))
        ms_err = None
        if ch == 2:
            try:
                with self._plc_call():
                    self.main_shutter(2, open=False)
            except Exception as e:
                ms_err = e
        if ok:
            if ch == 2 and ms_err is not None:
                return self._fail(f"CH2_GATE_CLOSE는 성공했지만 MAIN_SHUTTER_CLOSE 실패: {type(ms_err).__name__}: {ms_err}",
                                  code="E331")
            if ch == 2:
                return self._ok(f"CH2_GATE_CLOSE 완료 — {lamp}=TRUE + MAIN_SHUTTER_CLOSE (대기 {int(wait_s)}s)")
            return self._ok(f"CH{ch}_GATE_CLOSE 완료 — {lamp}=TRUE (대기 {int(wait_s)}s)")
        if ch == 2 and ms_err is None:
            return self._fail(f"CH2_GATE_CLOSE 실패 — {lamp}=FALSE, {sensor_lamp}={sensor_after} (대기 {int(wait_s)}s) "
                              f"+ MAIN_SHUTTER_CLOSE 시도 완료", code="E305")
        if ch == 2 and ms_err is not None:
            return self._fail(f"CH2_GATE_CLOSE 실패 — {lamp}=FALSE, {sensor_lamp}={sensor_after} (대기 {int(wait_s)}s) "
                              f"+ MAIN_SHUTTER_CLOSE도 실패: {type(ms_err).__name__}: {ms_err}", code="E331")
        return self._fail(f"CH{ch}_GATE_CLOSE 실패 — {lamp}=FALSE, {sensor_lamp}={sensor_after} (대기 {int(wait_s)}s)",
                          code="E305")

    # ───────── 척 ─────────
    def chuck_up(self, data: Json) -> Json:
        ch = int(data.get("ch", 1))
        timeout_s = float(data.get("wait_s", 90.0))
        busy = self._fail_if_ch_busy(ch, f"CH{ch}_CHUCK_UP")
        if busy is not None:
            return busy
        if ch not in (1, 2):
            return self._fail(f"지원하지 않는 CH: {ch}", code="E201")
        return self._move_chuck(ch, f"Z_M_P_{ch}_SW", f"Z_M_P_{ch}_MID_SW", f"Z{ch}_MID_LOCATION", "mid", timeout_s)

    def chuck_down(self, data: Json) -> Json:
        ch = int(data.get("ch", 1))
        timeout_s = float(data.get("wait_s", 90.0))
        busy = self._fail_if_ch_busy(ch, f"CH{ch}_CHUCK_DOWN")
        if busy is not None:
            return busy
        if ch not in (1, 2):
            return self._fail(f"지원하지 않는 CH: {ch}", code="E201")
        return self._move_chuck(ch, f"Z_M_P_{ch}_SW", f"Z_M_P_{ch}_CCW_SW", f"Z{ch}_DOWN_LOCATION", "down", timeout_s)

    def _read_chuck_position(self, ch: int) -> Json:
        if ch not in (1, 2):
            raise ValueError(f"지원하지 않는 CH: {ch}")
        with self._plc_call():
            up = bool(self.read_bit(f"Z{ch}_UP_LOCATION"))
            mid = bool(self.read_bit(f"Z{ch}_MID_LOCATION"))
            dn = bool(self.read_bit(f"Z{ch}_DOWN_LOCATION"))
        pos = "unknown"
        if int(up) + int(mid) + int(dn) == 1:
            pos = "up" if up else ("mid" if mid else "down")
        return {"position": pos, "lamp": {"up": up, "mid": mid, "down": dn}}

    def _move_chuck(self, ch: int, power_sw: str, move_sw: str, target_lamp: str, target_name: str,
                    timeout_s: float = 60.0) -> Json:
        with self.lock_ch[ch]:
            busy = self._fail_if_ch_busy(ch, f"CH{ch}_CHUCK_{target_name.upper()}")
            if busy is not None:
                return busy
            with self._plc_command(f"CHUCK_{target_name.upper()}_CH{ch}"):
                try:
                    cur = self._read_chuck_position(ch)
                except Exception as e:
                    return self._fail(e, code="E412")
                resolve_timeout_s = 0.0
                if target_name == "mid" and cur["position"] == "unknown":
                    resolve_timeout_s = float(self.cfg.get("CHUCK_POSITION_RESOLVE_TIMEOUT_S", 90.0))
                    resolve_deadline = self.monotonic() + resolve_timeout_s
                    while self.monotonic() < resolve_deadline:
                        self.sleep(1.0)
                        with contextlib.suppress(Exception):
                            cur = self._read_chuck_position(ch)
                        if cur["position"] != "unknown":
                            break
                if cur["position"] == target_name:
                    return self._ok(f"CH{ch} Chuck OK — 이미 {target_name.upper()} 위치", current=cur)
                if target_name == "mid" and cur["position"] == "unknown":
                    return self._fail(f"CH{ch} Chuck 위치 불명 상태 지속({int(resolve_timeout_s)}s 대기 후에도 "
                                      f"UP/MID/DOWN 모두 OFF 또는 중복 ON) → MID 이동 불가. "
                                      f"먼저 CH{ch}_CHUCK_DOWN 등으로 위치를 확정한 뒤 재시도. snapshot={cur}", code="E318")
                try:
                    with self._plc_call():
                        self.write_switch(power_sw, True)
                    self.sleep(0.2)
                    with self._plc_call():
                        self.write_switch(move_sw, True)
                    deadline = self.monotonic() + float(timeout_s)
                    while self.monotonic() < deadline:
                        with self._plc_call():
                            lamp_on = bool(self.read_bit(target_lamp))
                            if lamp_on:
                                self.write_switch(move_sw, False)
                                self.write_switch(power_sw, False)
                        if lamp_on:
                            cur = self._read_chuck_position(ch)
                            return self._ok(f"CH{ch} Chuck {target_name.upper()} 도달", current=cur)
                        self.sleep(1)
                    with self._plc_call():
                        self.write_switch(move_sw, False)
                        self.write_switch(power_sw, False)
                    cur = self._read_chuck_position(ch)
                    return self._fail(f"CH{ch} Chuck {target_name.upper()} 타임아웃({int(timeout_s)}s) — "
                                      f"{target_lamp}=FALSE, snapshot={cur}", code="E318")
                except Exception as e:
                    with contextlib.suppress(Exception):
                        with self._plc_call():
                            self.write_switch(move_sw, False)
                            self.write_switch(power_sw, False)
                    return self._fail(e, code="E412")
