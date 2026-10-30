"""DC Pulse 동작 단계(챔버12) — v1 device/dc_pulse.py 의 쓰기·복구·시작 순서를 generator 로 다시 쓴 것(DcPulseDriver mixin).

generator 는 pulse_port.IO/Sleep/WaitLink 를 내고(yield) 결과를 받는다. I/O 하나 = worker 요청 하나.
v1 근거(파일:줄은 device/dc_pulse.py)
- _write_cmd_data(1789-1967): 1회 전송 → 실패면 fault 판단(_recover_and_prepare_retry 1675-1744) → 다시, 모두
  DCP_RECOVER_MAX_ATTEMPTS(5)회. 쓰기 성공 = 1 B ACK 06 만. STOP_GUARD(OFF 시작 뒤)는 OUTPUT_OFF 외 쓰기를 막는다.
  OUTPUT_ON: ACK 면 확인 → 활성화 대기 5 s(0.5 s 마다 PIV, 과전류만) → 폴링 시작. ACK 아니면 0.08 s 뒤 0x90 HV 비트로 확인.
  OUTPUT_OFF: ACK, 아니면 0x90 HV Off, 아니면 (0x90 못 읽을 때만) P == 0 으로 확인. 끝내 못 하면 '출력 상태 미확인'.
- _read_raw(1442-1456): 읽기는 워커 재시도 2(무응답이면 세션 재시작 뒤 다시). 응답은 같은 CMD 프레임 또는 ERR 04.
- fault_reset(1632-1683)·_reopen_session_for_retry(1652-1673)·read_status_flags(1989-2039)·_confirm_off_quick(2046-2077).
- prepare_and_start(667-1092): 연결 대기 → MASTER HOST ×3 + 0.2 s → 0x9C(HOST 아니면 1 s 뒤 재확인) → 0x9E(≠0 이면 0x6F)
  → 펄스 값 검사(전송 전) → 0xA5(Ext 면 0x65=0) → 0x66 → (주파수 바꿨으면 DCP_FREQ_SETTLE_MS) 0x67 → 0xA6/0xA7 되읽기
  (다르면 0.5 s → 재기록 → 0.5 s, 그래도 다르면 진단 뒤 실패) → 0x81=3 → 0x83 → 0x96/0x99 → 0x8C=2 → 0x96/0x99 기준선
  → 0x80=1 → 0x90 HV 확인(아니면 0x9E·0xBB~0xBD 읽고 실패).
"""
from __future__ import annotations

from typing import Optional

from v2.ch12.drivers import dcpulse_proto as P
from v2.ch12.drivers.pulse_port import IO, Sleep, WaitLink
from v2.common.transport import TransportError

ON, OFF = "OUTPUT_ON", "OUTPUT_OFF"


def _hx(b: Optional[bytes]) -> str:
    return b.hex() if b else "-"


class DcpOpsMixin:
    # ───────── 한 번 주고받기 ─────────
    def _g_send(self, payload: bytes, label: str, is_read: bool, kind: str):
        self._emit("status", f"[SEND] {label} → {payload.hex(' ')}")
        r = yield IO(lambda s: self._xfer(s, payload, label, is_read), self._io_limit(), kind=kind, name=label)
        if r.ok:
            resp = r.value
            name = {0x06: "ACK", 0x04: "ERR"}.get(resp[0]) if len(resp) == 1 else None
            self._emit("status", f"[RECV] {label} ← {name}({resp.hex(' ')})" if name else f"[RECV] {label} ← {resp.hex(' ')}")
            return resp
        self._emit("status", f"[TIMEOUT] {label} → 세션 재시작")
        return None

    def _g_read_raw(self, code: int, kind: str):
        """v1 _read_raw: 워커 재시도 2 — 무응답·끊김이면 재연결을 기다려 다시(호출자 한도 안에서)."""
        label = P.READ_LABELS.get(code, f"READ_{code:02X}")
        t_end = self.clock.monotonic() + 3 * (self._timeout_s() + 2.0 + self._gap_s()) + self._extra_s()
        for attempt in range(3):
            resp = yield from self._g_send(P.pack_read(code), label, True, kind)
            if resp is not None:
                return resp
            left = t_end - self.clock.monotonic()
            if attempt == 2 or left <= 0:
                break
            if not (yield WaitLink(left)):
                break
        return None

    def _g_read_once(self, code: int, kind: str, limit_s: float):
        label = P.READ_LABELS.get(code, "")
        r = yield IO(lambda s: self._xfer(s, P.pack_read(code), label, True), max(0.05, limit_s), kind=kind, name=label)
        return r.value if r.ok else None

    # ───────── 읽기 ─────────
    def _g_read_u16(self, code: int, kind: str):
        return P.u16_tail((yield from self._g_read_raw(code, kind)))

    def _g_read_piv(self, kind: str):
        resp = yield from self._g_read_raw(P.R_PIV, kind)
        if not resp:
            self._emit("status", "READ_PIV: 응답 없음")
            return None
        if len(resp) == 1 and resp[0] == P.ERR:
            self._emit("status", "READ_PIV: 장비가 읽기 불가 상태(ERR)")
            return None
        raw = P.piv_raw(resp)
        if raw is None:
            self._emit("status", f"READ_PIV: 응답 길이 부족: {resp!r}")
            return None
        return {"raw": raw, "eng": self._scale(raw)}

    def _g_read_control_mode(self, kind: str):
        resp = yield from self._g_read_raw(P.R_CTRL_MODE, kind)
        if not resp or len(resp) < 2:
            self._failed("READ_CTRL_MODE", f"응답 길이 부족: {resp!r}")
            return None
        if resp[0] != P.R_CTRL_MODE:
            self._failed("READ_CTRL_MODE", f"형식 오류: raw={resp.hex(' ')}")
            return None
        val = resp[-1] & 0xFF
        return P.CTRL_MODES.get(val, f"UNKNOWN({val})")

    def _g_read_fault_code(self, kind: str):
        resp = yield from self._g_read_raw(P.R_FAULT, kind)
        if not resp or len(resp) < 2:
            self._failed("READ_FAULT", f"응답 길이 부족: {resp!r}")
            return None
        if resp[0] != P.R_FAULT:
            self._failed("READ_FAULT", f"형식 오류: raw={resp.hex(' ')}")
            return None
        data = resp[1:]
        return ((data[-2] << 8) | data[-1]) if len(data) >= 2 else data[-1]

    def _g_read_status_flags(self, kind: str):
        for attempt in range(3):
            resp = yield from self._g_read_raw(P.R_STATUS, kind)
            if not resp or len(resp) < 2:
                self._emit("status", f"READ_STATUS: 응답 없음/길이 부족({resp!r}) → 재시도({2 - attempt})")
                yield Sleep(0.03)
                continue
            if resp[0] == P.R_STATUS:
                flags = P.flags16(resp[1:])
                if flags is None:
                    self._emit("status", f"READ_STATUS: 데이터 없음: raw={resp.hex(' ')}")
                return flags
            self._emit("status", f"READ_STATUS: 다른 프레임(0x{resp[0]:02X}) 수신 → 무시하고 재시도")
            yield Sleep(0.03)
        self._emit("status", "READ_STATUS: 연속 실패로 STATUS 플래그 확인 불가 (non-fatal)")
        return None

    def _g_read_sync(self, kind: str):
        return (yield from self._g_read_raw(P.R_SYNC, kind))

    # ───────── 복구 ─────────
    def _g_reopen(self, label: str, kind: str):
        def drop(s):
            raise TransportError(f"[{label}] 세션 재시작(복구)")
        yield IO(drop, 0.5, kind=kind, confirms_link=False, name="reopen")
        if not (yield WaitLink(3.0)):
            self._emit("status", f"[{label}] 세션 재연결 실패")
            return False
        self._tokens.clear()
        yield Sleep(0.25)
        self._emit("status", f"[{label}] 세션 재연결 완료 → 재시도")
        return True

    def _g_fault_reset(self, kind: str):
        label = "FAULT_RESET"
        if not (yield WaitLink(1.5)):
            self._failed(label, "연결 안됨")
            return False
        resp = yield from self._g_send(P.pack_write(P.FAULT_RESET, 0x0001, 2), label, False, kind)
        if resp != b"\x06":
            self._failed(label, f"ACK 미수신: {resp!r}")
            return False
        self._confirmed(label)
        f = yield from self._g_read_fault_code(kind)
        if f is None:
            self._emit("status", "FAULT_RESET: fault 재확인 실패(통신)")
            return True
        if f != 0:
            self._failed(label, f"fault 남음: 0x{f:04X}")
            return False
        return True

    def _g_recover(self, label: str, resp: Optional[bytes], kind: str):
        if resp is None:
            if not (yield WaitLink(self._connect_timeout_s())):
                self._emit("status", f"[{label}] 실패 후 재연결 안됨 → 복구 중단")
                return False
        self._tokens.clear()
        fault = yield from self._g_read_fault_code(kind)
        if fault is None:
            if label == OFF:
                self._emit("status", f"[{label}] 실패 후 fault 조회 실패 → 세션 재연결 후 재전송")
                return (yield from self._g_reopen(label, kind))
            self._emit("status", f"[{label}] 실패 후 fault 조회 실패 → 1초 대기 후 재전송")
            yield Sleep(1.0)
            return True
        if fault == 0:
            if label == OFF and resp == b"\x04":
                self._emit("status", f"[{label}] fault=0 이지만 ERR(04) 지속 → 세션 재연결 후 재전송")
                return (yield from self._g_reopen(label, kind))
            self._emit("status", f"[{label}] 실패 후 fault=0 → 1초 대기 후 재전송")
            yield Sleep(1.0)
            return True
        self._emit("status", f"[{label}] 실패 후 fault=0x{fault:04X} → FAULT_RESET 후 재전송")
        if not (yield from self._g_fault_reset(kind)):
            self._emit("status", f"[{label}] FAULT_RESET 실패 → 재전송 중단")
            return False
        yield Sleep(1.0)
        return True

    # ───────── 쓰기(v1 _write_cmd_data) ─────────
    def _g_write(self, cmd: int, value: int, label: str, kind: str = "write"):
        base = label
        if base in (ON, OFF):
            self.polling = False
        payload = P.pack_write(cmd, value, 2)
        if self._stop_guard and base != OFF:
            self._emit("status", f"[{base}] STOP_GUARD active → skip write")
            return False
        n = self._recover_attempts()
        last = None
        for attempt in range(1, n + 1):
            if self._stop_guard and base != OFF:
                self._emit("status", f"[{base}] STOP_GUARD active → abort attempts")
                return False
            resp = yield from self._g_send(payload, f"{base}[{attempt}/{n}]", False, kind)
            last = resp
            ack = resp == b"\x06"
            if base == ON:
                if ack or (yield from self._g_on_verify()):
                    self._out_on = True
                    self._confirmed(base if ack else base + "_VERIFIED")
                    if (yield from self._g_activation_watch()):
                        return False
                    self._start_polling()
                    return True
                if self._fault_recover() and not (yield from self._g_recover(base, resp, kind)):
                    self._failed(base, "FAULT_RESET 실패/복구 불가")
                    self.polling = False
                    return False
                continue
            if base == OFF:
                how = base if ack else None
                if how is None:
                    flags = yield from self._g_read_status_flags(kind)
                    if flags is not None and not P.hv_on(flags):
                        how = base + "_VERIFIED"
                if how is None and (yield from self._g_confirm_off_quick(kind)):
                    how = base + "_VERIFIED"
                if how is not None:
                    self._out_on = self._output_maybe_on = self.output_off_unconfirmed = False
                    if ack:
                        self._last_ref_power_w = None
                    self._confirmed(how)
                    self.polling = False
                    return True
                if self._fault_recover() and not (yield from self._g_recover(base, resp, kind)):
                    self.output_off_unconfirmed = True
                    self._failed(base, "OUTPUT_OFF 미확인 — 복구 불가(재연결 실패/정리 중 등) (출력 상태 미확인)")
                    self.polling = False
                    return False
                continue
            if ack:
                self._confirmed(base)
                return True
            if self._fault_recover() and not (yield from self._g_recover(base, resp, kind)):
                self._failed(base, "FAULT_RESET 실패/복구 불가")
                return False
        if base == OFF:
            self.output_off_unconfirmed = True
            self._failed(base, f"OUTPUT_OFF 미확인 — 총 {n}회 시도, last={last!r} (출력 상태 미확인)")
        else:
            self._failed(base, f"응답 없음/실패 — 총 {n}회 시도, last={last!r}")
        if base in (ON, OFF):
            self.polling = False
        return False

    def _g_on_verify(self):
        yield Sleep(0.08)
        flags = yield from self._g_read_status_flags("write")
        return flags is not None and P.hv_on(flags)

    def _g_confirm_off_quick(self, kind: str):
        flags = yield from self._g_read_status_flags(kind)
        if flags is not None:
            return not P.hv_on(flags)
        piv = yield from self._g_read_piv(kind)
        return piv is not None and float(piv["eng"]["P_W"]) == 0.0

    # ───────── OUTPUT_ON 직후 ─────────
    def _g_arc_after_on(self):
        s_now = yield from self._g_read_u16(P.R_ARC_SOFT, "write")
        h_now = yield from self._g_read_u16(P.R_ARC_HARD, "write")
        self._arc_after_on(s_now, h_now)

    def _g_activation_watch(self):
        """v1 _watch_overcurrent_during_activation — 차단했으면 True."""
        delay = max(0.0, float(self._c("DCP_ACTIVATION_CHECK_DELAY_S", 5.0)))
        armed = bool(self._oc("DC_OVERCURRENT_ARM_ON_START", True))
        interval = max(0.05, float(self._oc("DC_OVERCURRENT_ARM_INTERVAL_S", 0.5)))
        t_on = self.clock.monotonic()
        if not armed or delay <= 0.0:
            if delay > 0.0:
                yield from self._g_arc_after_on()
            remain = t_on + delay - self.clock.monotonic()
            if remain > 0.0:
                yield Sleep(remain)
            return False
        t_end = t_on + delay
        self._emit("status", f"[overcurrent] 활성화 대기 {delay:.1f}초 구간 감시 시작 ({interval:.1f}초 간격)")
        first = True
        while self.clock.monotonic() < t_end:
            res = yield from self._g_read_piv("write")
            if res:
                e, rr = res["eng"], res["raw"]
                p, v, i = float(e["P_W"]), float(e["V_V"]), float(e["I_A"])
                self._emit("status", f"[telemetry·점화] P={p:.1f} W, V={v:.2f} V, I={i:.3f} A "
                                     f"(raw P/I/V={rr.get('P')}/{rr.get('I')}/{rr.get('V')}) +{self.clock.monotonic() - t_on:.1f} s")
                why = self._mon_overcurrent(p, v, i, rr)
                if why:
                    yield from self._g_auto_stop(why, "[AUTO-STOP] 과전류 → OUTPUT_OFF & stop polling", p, v, i, e)
                    return True
            if first:
                first = False
                yield from self._g_arc_after_on()
            remain = t_end - self.clock.monotonic()
            if remain <= 0.0:
                break
            yield Sleep(min(interval, remain))
        return False

    def _g_auto_stop(self, reason: str, msg: str, p, v, i, eng):
        fault = yield from self._g_read_fault_code("safety")
        if fault:
            reason += f", fault=0x{fault:04X}"
        self._emit("command_failed", reason, cmd="AUTO_STOP", reason="DCP_AUTO_STOP",
                   data={"power": p, "voltage": v, "current": i, "eng": eng})
        self._emit("status", msg)
        yield from self._g_off_impl()

    # ───────── OUTPUT_OFF(v1 _output_off_impl) ─────────
    def _g_off_impl(self):
        if self._out_on and self._arc_final_soft is None:          # 끄기 직전 아크 종료값(1.5 s 안, 실패는 조용히)
            t_end = self.clock.monotonic() + 1.5
            fs = yield from self._g_read_once(P.R_ARC_SOFT, "safety", t_end - self.clock.monotonic())
            fh = None
            if t_end - self.clock.monotonic() > 0.05:
                fh = yield from self._g_read_once(P.R_ARC_HARD, "safety", t_end - self.clock.monotonic())
            self._arc_final(P.u16_tail(fs), P.u16_tail(fh))
        self._stop_guard = True
        self._tokens.clear()
        return (yield from self._g_write(P.OUTPUT, 0x0002, OFF, "safety"))

    # ───────── 시작(v1 _prepare_and_start_impl) ─────────
    def _g_prepare(self, power_w: float, freq, off_time_us, duty, sync, master: str):
        self.last_failure = None
        if not (yield WaitLink(self._connect_timeout_s())):
            self._failed("CONNECT", "연결 준비 실패", phase="prepare")
            return False
        self._stop_guard = False
        self._last_ref_power_w = None
        self._soft_arc_total = self._hard_arc_total = 0
        if master == "host":
            for cmd, name in ((P.MASTER_ONOFF, "MASTER_ONOFF"), (P.MASTER_REFER, "MASTER_REFER"), (P.MASTER_MODE, "MASTER_MODE")):
                if not (yield from self._g_write(cmd, 0x0003, name)):
                    self._failed(name, "HOST master 강제 실패")
                    self._failed("PRECHECK", "ONOFF/REFER/MODE master를 HOST로 강제하지 못함", phase="prepare")
                    return False
            yield Sleep(0.2)
        self.polling = False
        self._tokens.clear()
        ctrl = yield from self._g_read_control_mode("write")
        if ctrl is None:
            self._failed("PRECHECK", "READ_CTRL_MODE(0x9C) 실패 → OUTPUT_ON 시퀀스 중단", phase="prepare")
            return False
        if ctrl not in ("HOST", "REMOTE", "LOCAL"):
            self._failed("PRECHECK", f"Control mode={ctrl} → OUTPUT_ON 시퀀스 중단", phase="prepare")
            return False
        if self._c("DCP_REQUIRE_HOST_MODE", True):
            if ctrl != "HOST":
                self._emit("status", f"[PRECHECK] Control mode={ctrl} → 1초 후 재확인")
                yield Sleep(1.0)
                ctrl2 = yield from self._g_read_control_mode("write")
                if ctrl2 != "HOST":
                    raw = {"REMOTE": "0x02", "LOCAL": "0x04"}.get(str(ctrl2 or ctrl), "?")
                    self._failed("PRECHECK", f"DC Pulse 패널 Operation Mode={ctrl2 or ctrl}(0x9C={raw}) — "
                                             "매뉴얼 p.18: 시리얼 제어 불가, 전면 패널에서 HOST로 전환 후 재시작", phase="prepare")
                    return False
        elif ctrl == "LOCAL":
            self._failed("PRECHECK", "Control mode=LOCAL (패널에서 HOST/REMOTE 전환 필요)", phase="prepare")
            return False
        fault = yield from self._g_read_fault_code("write")
        if fault is None:
            self._failed("PRECHECK", "READ_FAULT(0x9E) 실패 → OUTPUT_ON 시퀀스 중단", phase="prepare")
            return False
        if fault != 0:
            self._emit("status", f"[PRECHECK] fault=0x{fault:04X} → FAULT_RESET(0x6F) 시도")
            if not (yield from self._g_fault_reset("write")):
                self._failed("PRECHECK", "FAULT_RESET 실패(인터락/점화/케이블/진공 상태 확인 필요)", phase="prepare")
                return False
        ok = yield from self._g_pulse_params(freq, off_time_us, duty, sync)
        if not ok:
            return False
        f_for_check, want_off_raw = ok if isinstance(ok, tuple) else (None, None)
        if not (yield from self._g_write(P.REGULATION, 3, "REG_POWER")):
            self._failed("REG_POWER", "제어 모드=Power 설정 실패 → OUTPUT_ON 시퀀스 중단", phase="prepare")
            return False
        if not (yield from self._g_set_ref(power_w)):
            self._failed("REF_POWER", f"출력 Setpoint({power_w:g}W) 설정 실패 → OUTPUT_ON 생략", phase="prepare")
            return False
        pre_s = yield from self._g_read_u16(P.R_ARC_SOFT, "write")
        pre_h = yield from self._g_read_u16(P.R_ARC_HARD, "write")
        if not (yield from self._g_write(P.ARC_COUNT_RESET, 2, "ARC_COUNT_RESET")):
            self._emit("status", "[WARN] Arc Count Reset(0x8C=2) ACK 실패 → baseline 차감으로 대체")
        post_s = yield from self._g_read_u16(P.R_ARC_SOFT, "write")
        post_h = yield from self._g_read_u16(P.R_ARC_HARD, "write")
        self._arc_baseline_soft, self._arc_baseline_hard = int(post_s or 0), int(post_h or 0)
        self._arc_reset_state = P.arc_reset_state(pre_s, pre_h, post_s, post_h)
        self._arc_reset_ok = self._arc_reset_state == "yes"
        self._arc_on_ref = (post_s, post_h)
        d = (lambda x: x if x is not None else "-")
        self._emit("status", f"[arc] 0x8C 리셋: 전 SAT={d(pre_s)} ANT={d(pre_h)} → 후 SAT={d(post_s)} ANT={d(post_h)} "
                             f"(동작={P.ARC_RESET_LABEL.get(self._arc_reset_state, '확인불가')})")
        self._overcurr_n = 0
        self._emit("status", self._overcurrent_arm_status())
        self._spdev_n = self._low_curr_n = 0
        self._tokens.clear()
        if not (yield from self._g_write(P.OUTPUT, 0x0001, ON)):
            return False
        flags = yield from self._g_read_status_flags("write")
        if flags is not None and not P.hv_on(flags):
            f2 = yield from self._g_read_fault_code("write")
            m = []
            for c in (P.R_MASTER_ONOFF, P.R_MASTER_REFER, P.R_MASTER_MODE):
                m.append((yield from self._g_read_raw(c, "write")))
            self._failed(ON, "ACK 수신했지만 HV Off 상태 — 장비가 ON을 무시함 "
                             f"(fault={('0x%04X' % f2) if f2 is not None else '조회실패'}, "
                             f"master ONOFF/REFER/MODE={_hx(m[0])}/{_hx(m[1])}/{_hx(m[2])})", phase="prepare")
            return False
        self._arc_run_start_ts = self.clock.monotonic()            # v1: HV 확인 뒤(점화 창 시작)
        self._arc_rate_hit_n, self._arc_alert_sent = 0, False
        self._arc_ign_soft = self._arc_ign_hard = self._arc_final_soft = self._arc_final_hard = None
        self._arc_prev_raw_s = self._arc_prev_raw_h = None
        self._arc_prev_ts, self._arc_zero_reset_logged = 0.0, False
        self._soft_arc_total = self._hard_arc_total = 0
        self._piv_scale_checked = False
        return True

    def _g_set_ref(self, value_w: float):
        step = self._p_step()
        raw = max(0, min(int(self._max_power_w() // step), int(round(float(value_w) / step))))
        ok = yield from self._g_write(P.REF_LEVEL, raw, f"REF_POWER({value_w:.0f}W)")
        if ok:
            self._last_ref_power_w = float(value_w)
            self._spdev_n = self._low_curr_n = 0
        return ok

    # ───────── 펄스 값(v1 2-a ~ 2-f, 되읽기) ─────────
    def _g_pulse_params(self, freq, off_time_us, duty, sync):
        """성공이면 (검사 주파수, 기대 Off raw), 실패면 False. 값 검사가 실패하면 장비에 아무것도 쓰지 않는다."""
        want_off_raw: Optional[int] = None
        freq_given = freq is not None and not P.is_keep(freq)
        off_given = off_time_us is not None and not P.is_keep(off_time_us)
        if not off_given and freq_given and duty is not None and not P.is_keep(duty):
            off_time_us = P.duty_to_off_time_us(float(freq), float(duty))
            off_given = True
            self._emit("status", f"[호환] duty {float(duty):g}% @{float(freq):g}kHz → Off {float(off_time_us):.1f}us")
        f_chk: Optional[float] = None
        if freq_given:
            fv = float(freq)
            if fv != int(round(fv)):
                self._failed("PULSE_PARAM", f"Pulse Freq {fv}kHz는 정수가 아닙니다 — 매뉴얼 p.50: 0x66은 정수 kHz만 허용", phase="prepare")
                return False
            fi = int(round(fv))
            if not (P.FREQ_MIN_KHZ <= fi <= P.FREQ_MAX_KHZ):
                self._failed("PULSE_PARAM", f"Pulse Freq {fi}kHz는 허용 범위 {P.FREQ_MIN_KHZ}~{P.FREQ_MAX_KHZ}kHz 밖"
                                            "(매뉴얼 p.12/p.50) — 레시피 수정 필요", phase="prepare")
                return False
            f_chk = float(fi)
        if off_given and not P.is_dc_mode(off_time_us):
            ov = round(float(off_time_us), 1)
            if f_chk is None:
                cur = yield from self._g_read_u16(P.R_FREQ, "write")
                if cur is None:
                    self._failed("PULSE_PARAM", "Off Time 검증용 현재 주파수(0xA6) 읽기 실패 — Off Time 상한을 확인할 수 없음", phase="prepare")
                    return False
                f_chk = float(int(cur))
                self._emit("status", f"[PARAM] freq=keep → 장비 현재 주파수 {f_chk:g}kHz 기준으로 Off Time 검증")
            mx = P.max_off_time_us(f_chk)
            if ov < P.OFF_MIN_US:
                self._failed("PULSE_PARAM", f"Off Time {ov:.1f}us는 최소 {P.OFF_MIN_US:.1f}us 미만(매뉴얼 p.12/13) — 레시피 수정 필요", phase="prepare")
                return False
            if ov > mx:
                self._failed("PULSE_PARAM", f"Off Time {ov:.1f}us는 {f_chk:g}kHz 상한 {mx:.1f}us 초과(매뉴얼 그림3) — 레시피 수정 필요", phase="prepare")
                return False
            off_time_us = ov
        if freq_given or off_given:
            sr = yield from self._g_read_sync("write")
            if sr is None:
                self._emit("status", "[WARN] Pulse Sync(0xA5) 읽기 실패 → 확인 생략")
            else:
                sv = (sr[-1] & 0xFF) if len(sr) > 1 else None
                if sv is not None and sv != 0:
                    self._emit("status", f"[PARAM] Pulse Sync=Ext({sv}) 감지 → Int(0x65=0)로 전환 (매뉴얼 p.49)")
                    yield from self._g_write(P.PULSE_SYNC, 0, "PULSE_SYNC(INT)")
        if sync is not None:
            yield from self._g_write(P.PULSE_SYNC, 0 if sync == "int" else 1, f"PULSE_SYNC({str(sync).upper()})")
        if freq_given:
            if not (yield from self._g_write(P.PULSE_FREQ, int(round(f_chk)), f"PULSE_FREQ({int(round(f_chk))}kHz)")):
                self._failed("PULSE_FREQ", "0x66 설정 실패 → OUTPUT_ON 시퀀스 중단", phase="prepare")
                return False
        if off_given:
            if freq_given:
                settle = float(int(self._c("DCP_FREQ_SETTLE_MS", 500))) / 1000.0
                if settle > 0:
                    yield Sleep(settle)
            want_off_raw = P.OFF_RAW_DC if P.is_dc_mode(off_time_us) else P.off_time_to_raw(float(off_time_us))
            if not (yield from self._g_write_off(want_off_raw)):
                self._failed("OFF_TIME", "0x67 설정 실패 → OUTPUT_ON 시퀀스 중단", phase="prepare")
                return False
        if freq_given:
            want = int(round(f_chk))
            applied = False
            for attempt in (1, 2):
                got = yield from self._g_read_u16(P.R_FREQ, "write")
                if got is None:
                    self._emit("status", "[VERIFY] PULSE_FREQ read-back 실패(통신) → 검증 생략")
                    applied = True
                    break
                self.freq_khz = int(got)
                if int(got) == want:
                    applied = True
                    break
                if attempt == 1:
                    self._emit("status", f"[VERIFY] PULSE_FREQ 미적용 감지 (요청 {want}kHz ≠ 장비 {int(got)}kHz) → 0.5s 후 재기록")
                    yield Sleep(0.5)
                    if not (yield from self._g_write(P.PULSE_FREQ, want, f"PULSE_FREQ({want}kHz)")):
                        break
                    yield Sleep(0.5)
            if not applied:
                self._failed("PULSE_FREQ", f"쓰기 ACK 후에도 미적용 지속 (요청 {want}kHz) — 장비 마스터/패널 상태 점검 필요", phase="prepare")
                return False
        if want_off_raw is not None:
            off_applied, got_raw = False, None
            for attempt in (1, 2):
                got_raw = yield from self._g_read_u16(P.R_OFF, "write")
                if got_raw is None:
                    self._emit("status", "[VERIFY] OFF_TIME read-back 실패(통신) → 검증 생략")
                    off_applied = True
                    break
                self.off_raw = int(got_raw)
                if int(got_raw) == int(want_off_raw):
                    off_applied = True
                    break
                if attempt == 1:
                    self._emit("status", f"[VERIFY] OFF_TIME 미적용 감지 (요청 raw={want_off_raw} ≠ 장비 raw={got_raw}) → 0.5s 후 재기록")
                    yield Sleep(0.5)
                    if not (yield from self._g_write_off(want_off_raw)):
                        break
                    yield Sleep(0.5)
            if not off_applied:
                yield from self._g_off_time_diag(want_off_raw, got_raw, f_chk)
                return False
        return (f_chk, want_off_raw)

    def _g_write_off(self, raw: int):
        if raw == P.OFF_RAW_DC:
            return (yield from self._g_write(P.OFF_TIME, P.OFF_RAW_DC, "OFF_TIME(DC)"))
        return (yield from self._g_write(P.OFF_TIME, int(raw), f"OFF_TIME({int(raw) / 10.0:.1f}us)"))

    def _g_off_time_diag(self, want_raw: int, got_raw: Optional[int], f_khz: Optional[float]):
        """v1 _emit_failed_off_time — 진단(0xA5·0x91·0xBB~0xBD 읽기, 요청 −0.1 us 시험 기록) 뒤 실패 사유."""
        fmt = (lambda r: "조회실패" if r is None else ("DC" if int(r) == P.OFF_RAW_DC else f"{int(r) / 10.0:.1f}us"))
        wd = gd = None
        if f_khz and want_raw != P.OFF_RAW_DC:
            wd = P.off_time_to_duty_pct(f_khz, want_raw / 10.0)
        if f_khz and got_raw is not None and int(got_raw) != P.OFF_RAW_DC:
            gd = P.off_time_to_duty_pct(f_khz, int(got_raw) / 10.0)
        sync_txt = "조회실패"
        r = yield from self._g_read_sync("write")
        if r and len(r) > 1:
            sync_txt = "Int" if (r[-1] & 0xFF) == 0 else f"Ext({r[-1] & 0xFF})"
        r = yield from self._g_read_raw(P.R_OPERATION, "write")
        oper = r.hex(" ") if r else "-"
        m = []
        for c in (P.R_MASTER_ONOFF, P.R_MASTER_REFER, P.R_MASTER_MODE):
            m.append((yield from self._g_read_raw(c, "write")))
        probe = "생략"
        if want_raw != P.OFF_RAW_DC:
            pr = max(int(P.OFF_MIN_US * 10), int(want_raw) - 1)
            if pr != int(want_raw):
                yield from self._g_write_off(pr)
                yield Sleep(0.3)
                back = yield from self._g_read_u16(P.R_OFF, "write")
                probe = f"{pr / 10.0:.1f}us 요청→장비 {fmt(back)}"
        mx = f"{P.max_off_time_us(f_khz):.1f}us" if f_khz else "?"
        self._failed("OFF_TIME", f"DC Pulse Off Time 미적용: 요청 {fmt(want_raw)}"
                                 + (f"({f_khz:g}kHz" if f_khz else "(") + (f", 듀티 {wd:g}%)" if wd is not None else ")")
                                 + f" → 장비 {fmt(got_raw)} 유지" + (f"(듀티 {gd:g}%)" if gd is not None else "")
                                 + f". 매뉴얼 상한 {mx} 이내인데 거부됨. 진단: {probe} / PulseSync={sync_txt} / "
                                 + f"마스터 ONOFF={_hx(m[0])} REF={_hx(m[1])} MODE={_hx(m[2])} / 0x91={oper}"
                                 + " — 패널 Off Time·Key Lock 확인 필요", phase="prepare")
