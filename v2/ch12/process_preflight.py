"""챔버12 공정 시작 전 단계 — v1 runtime/chamber_runtime.py 의 _start_after_preflight(3799-4119) 중 장비 부분.

순서(v1 그대로): 이번 런 펄스 선택(_run_select — use_*_pulse ∧ 지원만, 값으로 추정하지 않음) → 펄스 주소 점유(재연결·장비
기동보다 먼저 — 같은 주소를 다른 챔버나 같은 챔버의 다른 종류가 쓰면 거부) → 펄스 재연결(reconnect_pulses — 옛 세션이 닫히고 새
worker 가 돌 때까지 CHAMBER_PULSE_RECONNECT_TIMEOUT_S 2 s, 넘으면 시작 실패) → 장비 기동 순서 PLC → MFC → IG → DC Pulse → RF Pulse
(GAPB-001 — v2 는 PLC·MFC·IG 가 상시 연결이라 '이미 연결' 이면 'already connected → skip', 펄스는 점유가 곧 연결) →
연결 확인(PLC·MFC·IG + 고른 펄스, 0.2 s 간격, 8 s·펄스 10 s, 1초마다 '연결 대기 중: …') → Gate(닫힘일 때만) → Main Valve
(SW ∧ 인터락) → [두께 기본 점검] → Chuck 위치(실패해도 계속 — 경고 목록) → [두께 재료값] → 시작.
Gate·Main Valve 판정(4단계 안전 규칙 표의 S 항목이 될 것)과 Chuck 이동(6단계 robot_ops 와 같은 동작)은 v1 코드를 그대로 여기
두었다 — 4·6단계가 같은 함수를 부르거나 엔진의 hooks 로 바꿔 끼운다(engine.preflight_hooks).
모든 대기는 엔진 러너 스레드에서 clock.wait_until — 취소(STOP)는 cancelled() 로 바로 끝난다(StageCancelled).
"""
from __future__ import annotations

from typing import Any, Callable, List, Optional, Tuple

from v2.common.worker import Status


class StageCancelled(Exception):
    """STOP·새 시작으로 단계가 취소됨(v1 asyncio CancelledError)."""


def plc_call(eng, submit: Callable[[Callable], Any], timeout_s: float):
    """PLC 요청 하나를 보내고 결과(Result)를 기다린다. 시간 초과 → TimeoutError, 취소 → StageCancelled.
    한도는 v1 그대로(Gate·Main Valve 0.6 s) — PLC 1초 묶음 읽기가 블록 사이에서 기다리는 요청에 양보하므로(drivers/plc_poll,
    결정 D120) 줄 서기 여유를 더하지 않는다(D84 를 되돌림 — CH1·CH2 동시 시작을 tick 의 20 시점에서 재 본 최대 0.30 s)."""
    box: dict = {}

    def done(res) -> None:
        box["r"] = res
        eng.clock.notify()
    submit(done)
    ok = eng.clock.wait_until(eng.clock.monotonic() + timeout_s,
                              lambda: "r" in box or eng.stage_cancelled())
    if eng.stage_cancelled():
        raise StageCancelled()
    if not ok:
        raise TimeoutError(f"PLC 응답 없음({timeout_s:.1f}s)")
    return box["r"]


def read_bit(eng, name: str, timeout_s: float = 3.0) -> bool:
    from v2.ch12.drivers import plc_map as PM
    if name not in PM.COIL_MAP:
        raise KeyError(name)
    r = plc_call(eng, lambda cb: eng.plc.read_bit(name, on_done=cb), timeout_s)
    if r.status != Status.OK:
        raise RuntimeError(r.error or r.reason or r.status)
    return bool(r.value)


def write_switch(eng, name: str, on: bool, timeout_s: float = 3.0) -> None:
    r = plc_call(eng, lambda cb: eng.plc.write_switch(name, on, on_done=cb), timeout_s)
    if r.status != Status.OK:
        raise RuntimeError(r.error or r.reason or r.status)


def sleep(eng, sec: float) -> None:
    eng.clock.wait_until(eng.clock.monotonic() + max(0.0, sec), eng.stage_cancelled)
    if eng.stage_cancelled():
        raise StageCancelled()


# ───────────── 펄스 점유·장비 기동 ─────────────
KIND_LABEL = {"dcpulse": "DC-Pulse", "rfpulse": "RF-Pulse"}


def claim_pulses(eng, sel: dict) -> Optional[str]:
    """고른 펄스 장비의 주소 점유(3864-3894). 거부 문구 또는 None. 부분 점유는 부르는 쪽이 release_chamber 로 푼다."""
    for kind in ("dcpulse", "rfpulse"):
        if not sel.get(kind):
            continue
        ok, msg, _owner = eng.sys.shared.pulse.claim(kind, eng.ch)
        if not ok:
            return msg
    return None


def reconnect_pulses(eng, sel: dict) -> None:
    """펄스 재연결(3896-3927 · _pulse_reconnect_safe 3760-3797) — 점유 뒤, 장비 기동 전. 고른 펄스마다(RF 는 RFPULSE_TCP 주소가
    있을 때만 — v1 cfg.RFPULSE_TCP) '[Runner] {label} reconnect 시작: host:port' → 재연결이 끝날 때까지(옛 세션 Worker 가 멈추고
    새 Worker 가 돈다 — PulsePorts.settled, v1 set_endpoint_reconnect = 감시 멈춤·끊기·새 워치독 시작) 최대
    CHAMBER_PULSE_RECONNECT_TIMEOUT_S(2 s) → '[Runner] {label} reconnect 완료'. 넘으면 RuntimeError('{label} reconnect timeout(2.0s)')
    — 부르는 쪽이 v1 과 같이 "오류: '{note}' 시작 실패. (…)"·PREFLIGHT_FAILED. 연결 성립은 v1 처럼 다음 연결 확인(8/10 s)에서 본다.
    STOP 취소는 StageCancelled 그대로(v1 의도한 취소), 그 밖의 예외는 '{label} reconnect failed: {e!r}'."""
    from v2.ch12.drivers.pulse_port import endpoint
    timeout_s = float(eng.cget("CHAMBER_PULSE_RECONNECT_TIMEOUT_S", 2.0))
    ports = eng.sys.shared.pulse
    for kind in ("dcpulse", "rfpulse"):
        if not sel.get(kind):
            continue
        label = KIND_LABEL[kind]
        host, port = endpoint(eng.cfg, eng.ch, kind)
        if not host or not port:
            continue
        eng.log("MAIN", f"[Runner] {label} reconnect 시작: {host}:{port}")
        try:
            t0 = eng.clock.monotonic()
            while not ports.settled(kind, eng.ch):
                if eng.clock.monotonic() - t0 >= timeout_s:
                    raise TimeoutError()
                sleep(eng, 0.05)
        except StageCancelled:
            raise
        except TimeoutError:
            raise RuntimeError(f"{label} reconnect timeout({timeout_s:.1f}s)")
        except Exception as e:
            raise RuntimeError(f"{label} reconnect failed: {e!r}")
        eng.log("MAIN", f"[Runner] {label} reconnect 완료")


def start_devices(eng, sel: dict) -> None:
    """_start_devices_task(2974-3048) 의 순서·로그. PLC 로그는 PLC 소유 챔버만(_owns_plc — CH1)."""
    a = eng.actions
    items = [("PLC", lambda: eng.sys.workers["plc"].connected, eng.n == 1),
             ("MFC", a.mfc.is_connected, True), ("IG", a.ig.is_connected, True)]
    if sel.get("dcpulse") and a.dcp is not None:
        items.append(("DCPulse", lambda: bool(a.dcp.worker and a.dcp.worker.connected), True))
    if sel.get("rfpulse") and a.rfp is not None:
        items.append(("RFPulse", lambda: bool(a.rfp.worker and a.rfp.worker.connected), True))
    for label, conn, log in items:
        if conn() and log:
            eng.log(label, "already connected → skip")
        elif log:
            eng.log(label, "start 호출 완료")          # v2: worker 가 이미 연결을 시도 중(상시 재연결)


def wait_connected(eng, sel: dict, timeout_s: float) -> List[str]:
    """_preflight_connect(4175-4208) — 실패한 장비 이름 목록."""
    a = eng.actions
    need: List[Tuple[str, Callable[[], bool]]] = [("PLC", lambda: eng.sys.workers["plc"].connected),
                                                  ("MFC", a.mfc.is_connected), ("IG", a.ig.is_connected)]
    if sel.get("dcpulse") and a.dcp is not None:
        need.append(("DC-Pulse", lambda: bool(a.dcp.worker and a.dcp.worker.connected)))
    if sel.get("rfpulse") and a.rfp is not None:
        need.append(("RF-Pulse", lambda: bool(a.rfp.worker and a.rfp.worker.connected)))
    c = eng.clock
    t0 = c.monotonic()
    nxt_log = t0
    while True:
        now = c.monotonic()
        missing = [n for n, f in need if not f()]
        if now >= nxt_log:
            eng.log("MAIN", f"연결 대기 중: {', '.join(missing) if missing else '모두 연결됨'}")
            nxt_log += 1.0
        if not missing:
            return []
        if now - t0 >= timeout_s:
            for n in missing:
                eng.log(n, f"연결 확인 실패(타임아웃 {timeout_s:.1f}s)")
            return missing
        sleep(eng, 0.2)


# ───────────── Gate·Main Valve(3622-3758) ─────────────
def gate_check(eng) -> Tuple[bool, str]:
    n, cg = eng.n, eng.cget
    tries, rd_t, iv = int(cg("CHAMBER_GATE_RECHECK_COUNT", 5)), float(cg("CHAMBER_GATE_READ_TIMEOUT_S", 0.6)), \
        float(cg("CHAMBER_GATE_RECHECK_INTERVAL_S", 0.2))
    for _ in range(tries):
        try:
            op = read_bit(eng, f"G_V_{n}_OPEN_LAMP", rd_t)
            cl = read_bit(eng, f"G_V_{n}_CLOSE_LAMP", rd_t)
        except KeyError as e:
            eng.log("MAIN", f"[CH{n}] PLC 주소맵에 gate lamp 키 없음: {e} → 시작 차단")
            return False, f"PLC 주소맵 키 없음 ({e})"
        except StageCancelled:
            raise
        except Exception as e:
            eng.log("MAIN", f"[CH{n}] Gate lamp 읽기 실패: {type(e).__name__}: {e} → 재시도 중")
            sleep(eng, iv)
            continue
        if cl and not op:
            return True, ""
        if op and not cl:
            eng.log("MAIN", f"[CH{n}] Gate 상태=OPEN (open={op}, close={cl})")
            return False, "Gate OPEN 상태"
        if op and cl:
            eng.log("MAIN", f"[CH{n}] Gate lamp 이상(OPEN/CLOSE 모두 TRUE) (open={op}, close={cl})")
            return False, "Gate lamp 이상 (OPEN/CLOSE 모두 True)"
        sleep(eng, iv)
    eng.log("MAIN", f"[CH{n}] Gate 상태=moving_or_unknown (OPEN/CLOSE 모두 FALSE) → 시작 차단")
    return False, "Gate 이동 중 / 상태 불명 (재시도 초과)"


def main_valve_check(eng) -> Tuple[bool, str]:
    n, cg = eng.n, eng.cget
    tries, rd_t, iv = int(cg("CHAMBER_MAINVALVE_RECHECK_COUNT", 5)), float(cg("CHAMBER_MAINVALVE_READ_TIMEOUT_S", 0.6)), \
        float(cg("CHAMBER_MAINVALVE_RECHECK_INTERVAL_S", 0.2))
    last_sw = last_it = False
    for _ in range(tries):
        try:
            sw = read_bit(eng, f"M_V_{n}_SW", rd_t)
            it = read_bit(eng, f"M_V_{n}_인터락", rd_t)
        except KeyError as e:
            eng.log("MAIN", f"[CH{n}] PLC 주소맵에 Main Valve 키 없음: {e} → 시작 차단")
            return False, f"PLC 주소맵 키 없음 ({e})"
        except StageCancelled:
            raise
        except Exception as e:
            eng.log("MAIN", f"[CH{n}] Main Valve 읽기 실패: {type(e).__name__}: {e} → 재시도 중")
            sleep(eng, iv)
            continue
        if sw and it:
            return True, ""
        last_sw, last_it = sw, it
        sleep(eng, iv)
    if not last_it:
        why = "Main Valve 인터락 불충족 (벤트 상태이거나 터보/러프펌프/압력 조건 미충족)"
    elif not last_sw:
        why = "Main Valve가 열려있지 않음 (인터락 충족, 밸브 미개방)"
    else:
        why = "Main Valve 상태 불명"
    eng.log("MAIN", f"[CH{n}] Main Valve 미개방 (sw={last_sw}, interlock={last_it}) → 시작 차단")
    return False, why


# ───────────── Chuck(4210-4354) ─────────────
def read_chuck_pos(eng) -> str:
    n = eng.n
    try:
        up, mid, dn = (read_bit(eng, f"Z{n}_{x}_LOCATION") for x in ("UP", "MID", "DOWN"))
        if int(up) + int(mid) + int(dn) == 1:
            return "up" if up else ("mid" if mid else "down")
    except StageCancelled:
        raise
    except Exception:
        pass
    return "unknown"


def chuck_move(eng, params: dict) -> bool:
    """레시피 chuck_position 이 있을 때만 1회. 실패·타임아웃이면 params chuck_position = 실측, False(공정은 계속)."""
    pos = str(params.get("chuck_position") or "").strip().lower()
    if not pos:
        return True
    n = eng.n
    if pos not in ("up", "mid", "down"):
        eng.log("PLC", f"[CH{n}] 알 수 없는 chuck_position='{pos}' → 스킵")
        return True
    power_sw = f"Z_M_P_{n}_SW"
    move_sw = {"up": f"Z_M_P_{n}_CW_SW", "mid": f"Z_M_P_{n}_MID_SW", "down": f"Z_M_P_{n}_CCW_SW"}[pos]
    lamp = f"Z{n}_{pos.upper()}_LOCATION"
    timeout_s = float(eng.cget("CHAMBER_CHUCK_MOVE_TIMEOUT_S", 60.0))
    settle_s = float(eng.cget("CHAMBER_CHUCK_POWER_ON_SETTLE_S", 0.2))
    poll_s = float(eng.cget("CHAMBER_CHUCK_POLL_INTERVAL_S", 0.3))

    def off() -> None:
        for sw in (move_sw, power_sw):
            try:
                write_switch(eng, sw, False)
            except StageCancelled:
                raise
            except Exception:
                pass
    try:
        try:
            already = read_bit(eng, lamp)
        except StageCancelled:
            raise
        except Exception:
            already = False
        if already:
            eng.log("PLC", f"[CH{n}] Chuck '{pos}' 이미 목표 위치 ({lamp}=True) → 이동 생략")
            return True
        eng.log("PLC", f"[CH{n}] Chuck '{pos}' 이동 시작: {power_sw} → {move_sw} → {lamp} 폴링")
        eng.state(f"Chuck {pos.upper()} 이동 중…")
        write_switch(eng, power_sw, True)
        sleep(eng, settle_s)
        write_switch(eng, move_sw, True)
        deadline = eng.clock.monotonic() + timeout_s
        while eng.clock.monotonic() < deadline:
            try:
                ok = read_bit(eng, lamp)
            except StageCancelled:
                raise
            except Exception:
                ok = False
            if ok:
                off()
                eng.log("PLC", f"[CH{n}] Chuck '{pos}' 이동 성공 ({lamp}=True)")
                return True
            sleep(eng, poll_s)
        off()
        actual = read_chuck_pos(eng)
        eng.log("PLC", f"[CH{n}] Chuck '{pos}' 타임아웃({int(timeout_s)}s) — 실측 위치: {actual}")
        params["chuck_position"] = actual
        return False
    except StageCancelled:
        try:
            eng.plc.write_switch(move_sw, False)
            eng.plc.write_switch(power_sw, False)
        except Exception:
            pass
        raise
    except Exception as e:
        off()
        actual = read_chuck_pos(eng)
        eng.log("PLC", f"[CH{n}] Chuck '{pos}' 이동 중 예외: {e!r} — 실측 위치: {actual}")
        params["chuck_position"] = actual
        return False


class PreflightMixin:
    """ProcessEngine 의 시작 전 단계(_start_after_preflight 3799-4119) — 위 함수들을 v1 순서로 부른다."""

    def _preflight(self, run, params: dict) -> bool:
        """_start_after_preflight(3799-4119). False = 시작 실패(정리까지 끝냄)."""
        if run.kind == "test" or params.get("test_mode"):
            dur = float(params.get("test_duration_sec", 0.0) or 0.0) or float(params.get("process_time", 0) or 0) * 60.0
            dur = max(1.0, dur)
            params.setdefault("process_note", params.get("Process_name") or "TEST")
            self.log("MAIN", f"[TEST MODE] '{params.get('process_note') or 'TEST'}' 장비 제어 스킵 / {dur:.1f}s 시뮬레이션")
            self._host(True, f"TEST MODE: {str(params.get('time', '')).strip() or f'{dur:.0f}s'}")
            return True
        sel = {"dcpulse": bool(params.get("use_dc_pulse")) and self.caps.dcp,
               "rfpulse": bool(params.get("use_rf_pulse")) and self.caps.rfp}
        why = claim_pulses(self, sel)
        if why:
            self.log("MAIN", why)
            self.popup("공유 장비 사용 중", why)
            return self._start_fail(why, "PULSE_PORT_BUSY", cleanup=False)
        reconnect_pulses(self, sel)
        start_devices(self, sel)
        self.actions.oes_init_background()
        self.actions.run_use(True)                       # v1 _on_process_status_changed(True) — 공유 MFC 런 사용자
        t = float(self.cget("CHAMBER_PREFLIGHT_TIMEOUT_WITH_PULSE_S" if any(sel.values()) else "CHAMBER_PREFLIGHT_TIMEOUT_S",
                            10.0 if any(sel.values()) else 8.0))
        failed = wait_connected(self, sel, t)
        if failed:
            fl = ", ".join(failed)
            self._host(False, "장비 연결 실패: " + fl)
            self.log("MAIN", f"필수 장비 연결 실패: {fl} → 시작 중단")
            self.popup("장비 연결 실패", f"다음 장비 연결을 확인하지 못했습니다:\n - {fl}\n\n케이블/전원/포트 설정 확인 후 재시도",
                       level="error")
            return self._start_fail(f"preflight connect failed: {fl}", "PREFLIGHT_FAILED")
        self._host(True, "preflight OK")
        note = params.get("process_note") or params.get("Process_name") or "알 수 없음"
        for key, label, code in (("gate", "Gate", "GATE_CHECK_FAILED"), ("main_valve", "Main Valve", "MAIN_VALVE_CHECK_FAILED")):
            ok, why = self.preflight_hooks[key](self)
            if not ok:
                self.log("MAIN", f"[CH{self.n}] {label} 체크 실패 ({why}) → 공정 시작 차단")
                self.emit("chat", f"{label} 체크 실패: {why} → 공정 시작 차단 (공정: {note})", "E301", level="error")
                return self._start_fail(f"{label.lower().replace(' ', ' ')} check failed: {why}", code)
        thk = self.thickness is not None and self.thickness.thickness_mode(params)
        if thk:
            code, msg = self.thickness.check_basic(params)
            if code:
                return self._thickness_reject(params, code, msg)
        self.run_warnings = []
        pos = str(params.get("chuck_position") or "").strip().lower()     # v1 _run_chuck_position — 이동 전 레시피 값(4051)
        chuck = self.preflight_hooks["chuck"](self, params)
        if not chuck:
            warn = f"Chuck 위치 이동 실패 (target='{pos}')" if pos else "Chuck 위치 이동 실패"
            self.log("MAIN", f"⚠️ {warn} → 공정은 계속 진행")
            self.run_warnings.append(warn)
        if not params.get("chuck_position"):
            actual = read_chuck_pos(self)
            if actual != "unknown":
                params["chuck_position"] = actual
        if thk:
            box: dict = {}
            self.thickness.apply_material(params, bool(chuck), lambda r: (box.setdefault("r", r), self.clock.notify()))
            self.clock.wait_until(self.clock.monotonic() + 30.0, lambda: "r" in box or self.stage_cancelled())
            if self.stage_cancelled():
                raise StageCancelled()
            code, msg = box.get("r", ("E846", "재료값 쓰기 응답 없음"))
            if code:
                return self._thickness_reject(params, code, msg)
            params["thickness_mode"] = True
        self.log("MAIN", "장비 연결 확인 완료 → 공정 시작")
        if self.heater is not None:
            self.heater.process_started(bool(params.get("use_heater")))
        return True

    def _preflight_exception(self, params: dict, ex: Exception) -> RuntimeError:
        """_start_after_preflight 의 except(4097-4119): 문구 로그·'오류' 창·호스트 실패 응답·장비 정리 → RuntimeError(문구)."""
        note = params.get("process_note", "알 수 없는")
        msg = f"오류: '{note}' 시작 실패. ({ex})"
        self.log("MAIN", msg)
        self.popup("오류", msg, level="error")
        self._host(False, msg)
        try:
            self.actions.cleanup_devices()
        except Exception:
            pass
        return RuntimeError(msg)

    def _start_exception(self, ex: Exception, queue: bool) -> None:
        """_runner_stage_start_single / _runner_stage_advance_queue 의 except(4964-4976·5232-5244): 로그 → 호스트 실패 응답(큐) →
        장비 정리 → 결과(run_end error PREFLIGHT_FAILED, 문구 '시작 실패: …' / '공정 시작 실패: …' — Robot CSV '실패' 사유) → 리셋."""
        if queue:
            self.log("MAIN", f"[Runner] ADVANCE_QUEUE failed: {ex!r}")
            why = f"공정 시작 실패: {ex}"
            self._host(False, why)
        else:
            self.log("MAIN", f"[Runner] START_SINGLE failed: {ex!r}")
            why = f"시작 실패: {ex}"
        self.queue_mode = False
        try:
            self.actions.cleanup_devices()
        except Exception:
            pass
        self.error = "PREFLIGHT_FAILED"
        self.last_end_t = self.clock.monotonic()
        self.emit("run_end", why, "PREFLIGHT_FAILED", level="error", result="error")
        self._reset_after_list()

    def _thickness_reject(self, params, code: str, msg: str) -> bool:
        log_txt, chat_txt = self.thickness.reject_texts(code, msg, params)
        self.log("MAIN", log_txt)
        self.emit("chat", chat_txt, code, level="error")
        return self._start_fail(msg, code)

    def _start_fail(self, why: str, code: str, cleanup: bool = True) -> bool:
        self._host(False, why)
        if cleanup:
            self.actions.cleanup_devices()
        else:
            self.sys.shared.pulse.release_chamber(self.ch)
        self.error = code
        self.last_end_t = self.clock.monotonic()
        self.emit("run_end", why, code, level="error", result="error")
        self.queue_mode = False
        return False
