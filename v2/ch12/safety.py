"""챔버12 안전 규칙 실행 — safety_rules.py 표 중 안전 엔진 몫(S30 ALL STOP·S31 시작 때 남은 파워 정리·S32 의도적 종료 파워만)을
드라이버·엔진 호출로 잇는다. 나머지 규칙(v1 공정 중단 조건·시작 가드·정지 순서)은 엔진·드라이버가 v1 처럼 이미 처리하고, 표의 impl
(IMPLEMENTED_IN)에 위치를 적었다 — 여기서 같은 판정을 한 번 더 하지 않는다(판정이 두 곳에 생기지 않게, 새 규칙 금지).

연결(assembly.build 가 붙인다): system.safety = Ch12Safety — Ch12System 이 1초 행마다 on_row(RowView)(기록 다음·엔진 앞)를 부른다.
이벤트: SafetyEvent(kind fired·released·step·info, rule S번호, code, detail, level) → system.emit("safety", …, event=…) — 허브가
  ruleId 를 실어 웹·로컬 화면에(fired/released 는 alarms 에).

S30 ALL STOP(결정 D05): CH1 STOP + CH2 STOP + 플라즈마 클리닝 STOP + TSP STOP 을 한꺼번에 — 각 엔진의 v1 정지 순서 그대로(보내기만,
  서로 기다리지 않음). 넷 다 다시 idle 이 되면 released. 진행 중이면 stopping 에 보인다.
S31 시작 때 남은 파워(지시 3장 (6), 챔버K S51): 프로그램이 켜진 뒤 첫 정상 PLC 행(재연결 때는 다시 하지 않음)에서 공정·운전이 없는데
  PLC D/A 전원(CH2 DC1·DC2, CH2 RF 연속, PC RF)의 SET 래치가 켜져 있거나 WRITE 값이 0 이 아니면 그 전원만 끈다(드라이버 정지 경로 —
  0 W 확인 → SET OFF, RF 는 v1 STOP 과 같은 램프다운). 펌프·밸브·셔터·가스는 건드리지 않는다. 공정 중이면 알림만. bench 는 쓰지 않는다.
  DC Pulse·RF Pulse 는 포트를 공정이 고를 때만 열어(v1 GAPB-001) 시작 때 읽지 않는다 — 확인 못 함('사용자 확인 필요' 후보).
  히터 남은 RUN 은 히터 엔진 첫 폴링(S21)이 정리한다.
S37 통신 끊김 공정 중단(v1 에 없음 — 사용자 결정 2026-11-26): comm_track 이 1초 행마다 PLC·chN.mfc 의 '정상으로 읽히지 않은 시간'을
  센다 — 행 품질 fail(또는 링크가 끊긴 채 stale)인 행이면 앞 행부터의 시간만큼 늘고, ok·late 행이면 0, 명령에 양보해 건너뛴 stale(링크
  살아 있음)·suppressed·off 행은 그대로(세지도 지우지도 않음 — 로봇 PLC 명령이 길게 이어져도 중단 안 됨). comm_loss 는 챔버 공정이
  전원 켜기 단계(POWER_ON_ACTIONS)를 시작한 뒤·정지 순서 전(power_armed)에만 PLC ≥ PLC_COMM_LOSS_ABORT_SEC 또는 그 챔버 MFC ≥
  MFC_COMM_LOSS_ABORT_SEC 면 경고 사건 1번 + engine.abort(PLC_COMM_LOST·MFC_COMM_LOSS) — 지금 abort 경로(정지 순서) 그대로.
  Plasma Cleaning 은 PC RF 가 켜진 동안(pc_armed) 같은 규칙(PC 가 쓰는 챔버 MFC) → pc.abort. 0 = 끔.
S39 펄스 전원 통신 끊김 중단(v1 에 없음 — 사용자 결정 2026-11-27, P · S37 과 같은 방식): comm_track 이 chN.rfpulse·chN.dcpulse 의
  '정상으로 읽히지 않은 시간'도 센다(쓰지 않는 펄스 장비 — 포트를 잡지 않아 off 품질 — 는 0). 챔버 공정이 그 전원의 출력을 켠 뒤
  (RF_PULSE_START·DC_PULSE_START 단계 시작)·정지 순서 전에만 RFPULSE_COMM_LOSS_ABORT_SEC·DCPULSE_COMM_LOSS_ABORT_SEC 이상이면 경고 사건
  1번 + engine.abort(RFP_COMM_LOST·DCP_COMM_LOST). 정지 순서는 그 링크가 아직 끊겨 있으면 그 전원의 꺼짐 확인을 기다리지 않는다
  (process_runner._lost_skip — 살아 있는 장치 끄기가 판정 뒤 20 s 안에). 0 = 끔.
S40 펄스 전원 재연결 뒤 끄기(같은 결정): S39 로 멈춘 (챔버, 종류)는 공정·운전(챔버·PC·TSP)이 모두 없을 때 그 포트를 안전 엔진이 잡아
  (PulsePorts.claim hold — 공정이 잡으면 넘겨준다) 연결을 기다리고, 연결되면 그 전원에 끄기를 다시 보낸다(정보 사건 1번 — 보낸 뒤
  잡기를 푼다). 공정 중이면 아무것도 하지 않는다(공정이 같은 전원을 잡아 연결되면 공정이 끄므로 다시 보내지 않음).
S38 PLC 재연결 뒤 남은 파워(v1 에 없음 — 같은 결정): PLC 링크가 끊긴 행(fail 이고 링크 끊김)을 본 뒤, 공정·운전이 없는 첫 정상 PLC
  행에서 S31 과 같은 확인으로 남은 PLC D/A 전원만 끈다(reconnect_read). 공정·운전 중이면 아무것도 하지 않고(알림 없음) 모두 대기가
  될 때까지 미룬다(S37 중단 뒤 정지 정리 중에 PLC 가 돌아온 경우 — PC RF 가 켜진 채 남지 않게).
S32 의도적 종료(챔버K 결정 4): power_off_only(reason) — 챔버 공정을 멈추고(새 장비 명령을 보내지 않게 붙잡음 — 정지 순서를 돌리지 않음:
  돌리면 MV·가스 밸브를 닫는다, run_end error PROGRAM_EXIT), 확인을 기다리지 않는 OFF 를 먼저 다 보낸 뒤 deadline_s 까지 결과를 기다린다:
  DC1·DC2 OFF · RF(CH2·PC) 0 W → SET OFF(램프다운 없음) · 연결된 DC Pulse OUTPUT OFF · RF Pulse RF OFF · 히터 RUN=0 ·
  TSP 운전 중이면 이상 중단(TSP OFF). 플라즈마 클리닝이 돌고 있으면 챔버 공정처럼 먼저 붙잡고(PC 엔진 program_exit — 끝 정리의
  RF 안전 정지·가스 FLOW OFF·SP4 VALVE OPEN 을 보내지 않음, 결과 실패 PROGRAM_EXIT) PC RF 도 다른 RF 처럼 0 W → SET OFF
  (rf_zero_off — 붙잡은 뒤라 PC 램프다운 쓰기와 엇갈리지 않음). PLC 릴레이·MFC·IG 는 그대로('MFC 그대로' — 챔버K 결정 4).
"""
from __future__ import annotations

import collections
import threading
from typing import Any, Callable, Dict, List, Optional

from v2.common.safety_engine import SafetyEvent
from v2.ch12 import process_states as PS

FRESH = ("ok", "late")
POWER_ON_ACTIONS = frozenset({"DC_POWER_SET", "DC2_POWER_SET", "DC_PULSE_START", "RF_PULSE_START", "RF_POWER_SET"})
PULSE_DEVS = ("ch1.rfpulse", "ch1.dcpulse", "ch2.rfpulse", "ch2.dcpulse")
COMM_DEVS = ("plc", "ch1.mfc", "ch2.mfc") + PULSE_DEVS
# S39 — 펄스 종류마다 (켜기 단계, 설정, 사유, 화면 이름)
PULSE_RULE = {"rfpulse": ("RF_PULSE_START", "RFPULSE_COMM_LOSS_ABORT_SEC", "RFP_COMM_LOST", "RF Pulse"),
              "dcpulse": ("DC_PULSE_START", "DCPULSE_COMM_LOSS_ABORT_SEC", "DCP_COMM_LOST", "DC Pulse")}
PULSE_CODE_KIND = {v[2]: k for k, v in PULSE_RULE.items()}
FORCE_DEADLINE_S = 4.0            # 의도적 종료 — 콘솔 창 닫기는 약 5 s 안에 끝나야 한다(챔버K 와 같은 여유)
DC_UNITS = ((1, "ch2.dc.dc1_enable", "plc.dcv_write_0"), (2, "ch2.dc.dc2_enable", "plc.dcv_write_3"))
RF_UNITS = (("ch2", "ch2.rf.enable", "plc.dcv_write_2", "ch2.rf.fwd_w"), ("pc", "pc.rf.enable", "plc.dcv_write_1", "pc.rf.fwd_w"))


def rf_zero_off(drv, on_done: Callable[[Optional[str]], None]) -> None:
    """RF 연속(PLC D/A) 즉시 끄기 — 램프다운 없이 0 W 확인 → SET OFF(plc_power.ZeroThenOff, RfPower 의 끝 단계 그대로).
    의도적 종료에서만(창 닫기 5 s 안). 드라이버 안 상태(동작·폴링·램프)를 먼저 멈춰 뒤에 다른 쓰기가 끼지 않게 한다."""
    drv.running = False
    drv._gen += 1
    drv.polling, drv._due = False, None
    drv.enabled = True                       # 이번 런에 안 켰어도 SET OFF 까지(남은 래치)
    drv.ramping_down = True
    drv._stop_done.append(on_done)
    drv._finish_power_off(lambda ok: drv._end(ok, msg="RF 파워 즉시 OFF(프로그램 종료)", idle=True))


def pulse_armed(e, action: str) -> bool:
    """S39 판정 구간 — 이번 공정이 그 펄스 전원의 켜기 단계(action)를 시작했고 정지 순서는 아직."""
    r = getattr(e, "runner", None)
    if r is None or not r.is_running or r.shutdown or r.failed or r.stop_requested:
        return False
    return any(t[2] == action and not t[4] for t in list(r.trace))


def power_armed(e) -> bool:
    """S37 판정 구간 — 이번 공정(행)이 전원 켜기 단계를 시작했고 정지 순서는 아직(러너가 돌고 shutdown·실패·STOP 요청 없음)."""
    r = getattr(e, "runner", None)
    if r is None or not r.is_running or r.shutdown or r.failed or r.stop_requested:
        return False
    return any(t[2] in POWER_ON_ACTIONS and not t[4] for t in list(r.trace))


class Ch12Safety:
    def __init__(self, system, proc: Dict[str, Any], pc=None, tsp=None, heater=None) -> None:
        self.sys, self.clock, self.cfg = system, system.clock, system.cfg
        self.proc = dict(proc)                    # {"ch1": ProcessEngine, "ch2": …}
        self.pc, self.tsp, self.heater = pc, tsp, heater
        self.bench = not bool(system.policy.device_writes)
        self.events: "collections.deque[SafetyEvent]" = collections.deque(maxlen=5000)
        self.first_done = False
        self.residual: List[tuple] = []           # S31 조치 기록 (장비, 조치) — 실행당 한 번뿐
        self.all_stop_active = False
        self.all_stop_t: Optional[float] = None
        self.all_stops = 0
        self.forced: Optional[dict] = None
        self._forced_lk = threading.Lock()
        self._held: set = set()
        self.comm_lost_s: Dict[str, float] = {k: 0.0 for k in COMM_DEVS}   # S37 — 정상으로 읽히지 않은 시간(s)
        self._comm_prev_t: Optional[float] = None
        self._comm_fired: Dict[str, str] = {}      # 대상(ch1·ch2·pc) → 사유 — 판정 구간마다 한 번
        self._plc_was_down = False                 # S38 — PLC 링크가 끊긴 행을 봄
        self.pulse_off_pending: Dict[tuple, dict] = {}   # S40 — (챔버, 종류) → {code, sending} — S39 로 멈춘 펄스 전원

    # ───────── 이벤트 ─────────
    def _emit(self, kind: str, rule: str, code: str, detail: str, level: str = "info") -> SafetyEvent:
        e = SafetyEvent(kind, rule, code, detail, level, self.clock.wall())
        self.events.append(e)
        try:
            self.sys.emit("safety", kind, detail, level=level, code=code, event=e)
        except Exception:
            pass
        return e

    # ───────── 상태 ─────────
    def engine_busy(self) -> Dict[str, bool]:
        out = {ch: bool(e.busy) for ch, e in self.proc.items() if e is not None}
        if self.pc is not None:
            out["pc"] = bool(self.pc.busy)
        if self.tsp is not None:
            out["tsp"] = bool(self.tsp.busy)
        return out

    def any_busy(self) -> bool:
        return any(self.engine_busy().values())

    @property
    def stopping(self) -> Optional[dict]:
        """지금 정지 순서 중인 것(허브 state.stopping) — 없으면 None."""
        out: Dict[str, Any] = {}
        for ch, e in self.proc.items():
            if e is not None and PS.chamber_state(e) == "STOPPING":
                out[ch] = "STOPPING"
        if self.pc is not None and getattr(self.pc, "phase", "") == "stopping":
            out["pc"] = "stopping"
        if self.tsp is not None and getattr(self.tsp, "phase", "") in ("cleanup", "closing"):
            out["tsp"] = self.tsp.phase
        if self.all_stop_active:
            out["allStop"] = True
        return out or None

    # ───────── S30 ALL STOP ─────────
    def all_stop(self, source: str = "web", by: Optional[str] = None) -> List[str]:
        """넷을 한꺼번에 STOP(보내기만). 보낸 대상 목록."""
        self.all_stops += 1
        self.all_stop_active = True
        self.all_stop_t = self.clock.monotonic()
        self._emit("fired", "S30", "ALL_STOP", f"ALL STOP({source}{'·' + by if by else ''}) — CH1·CH2·PC·TSP STOP", "warn")
        sent: List[str] = []
        for ch in ("ch1", "ch2"):
            e = self.proc.get(ch)
            if e is not None:
                self._safe(lambda e=e: e.stop(source=source, by=by, user=True))
                sent.append(ch)
        if self.pc is not None:
            self._safe(lambda: self.pc.stop(source=source))
            sent.append("pc")
        if self.tsp is not None:
            self._safe(lambda: self.tsp.stop(source=source))
            sent.append("tsp")
        rs = getattr(self.sys, "resumer", None)
        if rs is not None and not rs.done:                   # 재시작 이어가기 판단 중 — 이어가지 않고 정지 순서(D05)
            self._safe(lambda: rs.request_stop_all(source, by))
            sent.append("resume")
        self._emit("step", "S30", "ALL_STOP", "보냄: " + ", ".join(sent))
        return sent

    def _safe(self, fn) -> Any:
        try:
            return fn()
        except Exception as e:                    # 하나의 실패가 나머지 STOP 을 막지 않는다
            self._emit("error", "S30", "INTERNAL_ERROR", repr(e), "error")
            return None

    def _all_stop_tick(self) -> None:
        if self.all_stop_active and not self.any_busy():
            self.all_stop_active = False
            self._emit("released", "S30", "ALL_STOP", "ALL STOP 끝 — 모두 대기")

    # ───────── 1초 행 ─────────
    def on_row(self, view) -> None:
        self.comm_track(view)
        if not self.first_done and view.q("plc") in FRESH:
            self.first_done = True
            self._plc_was_down = False
            self.first_read(view)
        elif self._plc_was_down and view.q("plc") in FRESH and not self.any_busy():
            self._plc_was_down = False                # 공정·운전 중이면 기다림(아무것도 하지 않음) — 모두 대기인 첫 정상 행에서
            self.reconnect_read(view)
        self.comm_loss(view)
        self.pulse_reconnect()
        self._all_stop_tick()

    # ───────── S37 통신 끊김 공정 중단 ─────────
    def _pulse_drv(self, ch: str, kind: str):
        return (getattr(self.sys, kind, {}) or {}).get(ch)

    def _link_up(self, dev: str) -> Optional[bool]:
        if dev in PULSE_DEVS:
            ch, kind = dev.split(".")
            d = self._pulse_drv(ch, kind)
        else:
            d = getattr(self.sys, "plc", None) if dev == "plc" else (getattr(self.sys, "mfc", {}) or {}).get(dev.split(".")[0])
        w = getattr(d, "worker", None)
        try:
            return None if w is None else bool(w.connected)
        except Exception:
            return None

    def comm_track(self, view) -> None:
        """1초 행 품질로 장치마다 '정상으로 읽히지 않은 시간'을 센다(머리말 S37)."""
        now = self.clock.monotonic()
        dt = 0.0 if self._comm_prev_t is None else max(0.0, now - self._comm_prev_t)
        self._comm_prev_t = now
        for dev in COMM_DEVS:
            q = view.q(dev)
            if q in FRESH or (dev in PULSE_DEVS and q in (None, "off")):    # 펄스는 포트를 놓으면 off — 쓰지 않음(0)
                self.comm_lost_s[dev] = 0.0
            elif q == "fail" or (q == "stale" and self._link_up(dev) is False):
                self.comm_lost_s[dev] += dt
                if dev == "plc" and self._link_up("plc") is False:
                    self._plc_was_down = True

    def _comm_limit(self, key: str) -> float:
        try:
            return max(0.0, float(self.cfg.get(key, 0.0) or 0.0))
        except Exception:
            return 0.0

    def _comm_hit(self, ch: Optional[str]) -> Optional[tuple]:
        """(사유, 장치 이름, 끊긴 시간, 기준) 또는 None — PLC 먼저."""
        lim = self._comm_limit("PLC_COMM_LOSS_ABORT_SEC")
        if lim > 0 and self.comm_lost_s["plc"] >= lim:
            return "PLC_COMM_LOST", "PLC", self.comm_lost_s["plc"], lim
        lim = self._comm_limit("MFC_COMM_LOSS_ABORT_SEC")
        dev = f"{ch}.mfc"
        if ch and lim > 0 and dev in self.comm_lost_s and self.comm_lost_s[dev] >= lim:
            return "MFC_COMM_LOSS", f"{ch.upper()} MFC", self.comm_lost_s[dev], lim
        return None

    def _pulse_hit(self, ch: str, e) -> Optional[tuple]:
        """S39 — (사유, 장치 이름, 끊긴 시간, 기준, 종류) 또는 None. 그 전원의 켜기 단계를 시작한 뒤·정지 순서 전에만."""
        for kind, (action, key, code, name) in PULSE_RULE.items():
            lim = self._comm_limit(key)
            dev = f"{ch}.{kind}"
            if lim > 0 and dev in self.comm_lost_s and self.comm_lost_s[dev] >= lim and pulse_armed(e, action):
                return code, f"{ch.upper()} {name}", self.comm_lost_s[dev], lim, kind
        return None

    def pc_armed(self) -> Optional[str]:
        """Plasma Cleaning 이 돌고(정지 요청 없음) PC RF 가 켜져 있으면 PC 가 쓰는 챔버(ch1·ch2), 아니면 None."""
        pc = self.pc
        if pc is None or not getattr(pc, "running", False) or getattr(pc, "stop_requested", False) \
                or getattr(pc, "abort_reason", None):
            return None
        d = (getattr(self.sys, "rf", {}) or {}).get("pc")
        if d is None or not getattr(d, "running", False) or getattr(d, "ramping_down", False):
            return None
        return f"ch{int(getattr(pc, 'ch', 1) or 1)}"

    def comm_loss(self, view=None) -> None:
        targets = [(ch, e, ch if power_armed(e) else None) for ch, e in self.proc.items() if e is not None]
        if self.pc is not None:
            targets.append(("pc", self.pc, self.pc_armed()))
        for key, eng, mfc_ch in targets:
            pulse = None if key == "pc" else self._pulse_hit(key, eng)        # S39 — 펄스 전원(그 출력을 켠 뒤)
            if mfc_ch is None and pulse is None:
                self._comm_fired.pop(key, None)
                continue
            if key in self._comm_fired:
                continue
            hit = (self._comm_hit(mfc_ch) if mfc_ch is not None else None) or (pulse[:4] if pulse else None)
            if hit is None:
                continue
            code, dev, secs, lim = hit
            self._comm_fired[key] = code
            who = "Plasma Cleaning" if key == "pc" else key.upper()
            detail = f"{who} · {dev} 가 {secs:.0f} s 동안 읽히지 않아 공정을 멈춥니다(기준 {lim:g} s)"
            rule = "S39" if code in PULSE_CODE_KIND else "S37"
            self._emit("fired", rule, code, detail, "warn")
            if rule == "S39":
                self.pulse_off_pending[(key, PULSE_CODE_KIND[code])] = {"code": code, "sending": False}
            try:
                if key == "pc":
                    eng.abort(code)
                else:
                    eng.abort(code, detail, source="SAFETY")
            except Exception as ex:
                self._emit("error", "S37", "INTERNAL_ERROR", f"{who}: {ex!r}", "error")

    # ───────── S40 펄스 전원 재연결 뒤 끄기 ─────────
    def pulse_reconnect(self) -> None:
        """S39 로 멈춘 펄스 전원 — 공정·운전이 모두 없을 때 포트를 잡아 연결되면 끄기를 다시 보낸다(머리말 S40)."""
        if not self.pulse_off_pending:
            return
        ports = getattr(self.sys, "pulse", None)
        busy = self.any_busy()
        for key in list(self.pulse_off_pending):
            ch, kind = key
            st = self.pulse_off_pending[key]
            drv = self._pulse_drv(ch, kind)
            if drv is None or ports is None or self.bench:
                self.pulse_off_pending.pop(key, None)
                continue
            if st["sending"]:
                continue
            try:
                owner, held = ports.owner(kind, ch), ports.held(kind, ch)
            except Exception:
                self.pulse_off_pending.pop(key, None)
                continue
            if busy:                                    # 공정 중이면 아무것도 하지 않는다
                if owner == (ch, kind) and not held and self._link_up(f"{ch}.{kind}"):
                    self.pulse_off_pending.pop(key, None)   # 공정이 그 전원을 잡아 연결됨 — 공정이 끈다
                continue
            if owner is None:
                self._try(lambda: ports.claim(kind, ch, hold=True))   # 연결을 기다린다(다음 행부터 확인)
                continue
            if owner != (ch, kind) or not self._link_up(f"{ch}.{kind}"):
                continue
            st["sending"] = True
            name = PULSE_RULE[kind][3]
            self._emit("info", "S40", "RESIDUAL_POWER_OFF", f"{ch.upper()} {name} 연결이 돌아와 끄기를 다시 보냅니다", "info")

            def done(*_a, _key=key, _kind=kind, _ch=ch) -> None:
                self.pulse_off_pending.pop(_key, None)
                self._try(lambda: ports.release_hold(_kind, _ch))
            if kind == "rfpulse":
                off = drv.stop_process if drv.off_needs_confirm() else drv.stop_unused
            else:
                off = drv.output_off if drv.off_needs_confirm() else drv.output_off_unused
            try:
                off(on_done=done)
            except Exception as ex:
                done()
                self._emit("error", "S40", "INTERNAL_ERROR", f"{ch.upper()} {name}: {ex!r}", "error")

    # ───────── S31 시작 때 남은 파워 ─────────
    def residual_found(self, view) -> List[tuple]:
        """[(장비 이름, 드라이버, 종류 dc|rf, 값 설명, 측정 W)] — PLC 행으로 본 남은 D/A 전원."""
        out = []
        dc = getattr(self.sys, "dc", {}) or {}
        for unit, en_col, wr_col in DC_UNITS:
            d = dc.get(unit)
            if d is None:
                continue
            en, wr = view.get(en_col), view.get(wr_col) or 0
            if en or wr:
                out.append((f"DC{unit}", d, "dc", f"SET={bool(en)} WRITE={wr}", None))
        rf = getattr(self.sys, "rf", {}) or {}
        for mode, en_col, wr_col, fwd_col in RF_UNITS:
            d = rf.get(mode)
            if d is None:
                continue
            en, wr = view.get(en_col), view.get(wr_col) or 0
            if en or wr:
                out.append(("RF(CH2)" if mode == "ch2" else "RF(PC)", d, "rf", f"SET={bool(en)} WRITE={wr}", view.get(fwd_col)))
        return out

    def first_read(self, view) -> None:
        found = self.residual_found(view)
        if not found:
            return
        busy = [k for k, v in self.engine_busy().items() if v]
        for name, drv, kind, txt, fwd in found:
            if busy:
                self._emit("fired", "S31", "RESIDUAL_POWER_KEPT", f"{name} {txt} — 공정·운전 중({','.join(busy)})이라 끄지 않음",
                           "warn")
                continue
            if self.bench:
                self._emit("fired", "S31", "RESIDUAL_POWER_OFF", f"{name} {txt} — bench(읽기 전용)라 보내지 않음", "info")
                continue
            self._emit("fired", "S31", "RESIDUAL_POWER_OFF", f"시작 때 {name} 출력이 남아 있음({txt}) — 파워만 끔", "warn")
            self._residual_off("S31", name, drv, kind, fwd)

    def _residual_off(self, rule: str, name: str, drv, kind: str, fwd) -> None:
        self.residual.append((name, kind))
        done = (lambda r, n=name: self._emit("released", rule, "RESIDUAL_POWER_OFF",
                                              f"{n} 꺼짐" if r is None else f"{n} 끄기 확인 실패({r})",
                                              "info" if r is None else "error"))
        try:
            drv.enabled = True                    # 이번 실행에 켜지 않았어도 0 W → SET OFF 까지
            if kind == "rf":
                drv.last_sent = max(0.0, float(fwd or 0.0))   # v1 STOP 과 같은 램프다운(측정 W 부터)
            drv.stop(on_done=done)
        except Exception as e:
            self._emit("error", rule, "INTERNAL_ERROR", f"{name}: {e!r}", "error")

    # ───────── S38 PLC 재연결 뒤 남은 파워 ─────────
    def reconnect_read(self, view) -> None:
        """PLC 가 다시 연결된 뒤 첫 정상 행 — 공정·운전이 없을 때만 S31 과 같은 확인으로 남은 D/A 전원만 끈다(있으면 알림 없음)."""
        if self.any_busy() or self.bench:
            return
        for name, drv, kind, txt, fwd in self.residual_found(view):
            self._emit("fired", "S38", "RESIDUAL_POWER_OFF", f"PLC 재연결 뒤 {name} 출력이 남아 있어 파워만 끕니다({txt})", "warn")
            self._residual_off("S38", name, drv, kind, fwd)

    # ───────── S32 의도적 종료 — 파워 출력만 ─────────
    def hold_process(self, e, reason: str) -> bool:
        """챔버 엔진을 붙잡는다 — 정지 순서 없이 멈추고 run_end error PROGRAM_EXIT(엔진 program_exit — 5단계가 만든 길).
        새 장비 명령도 보내지 않게 actions.send 를 막는다(끄기 명령 뒤 다음 블록이 다시 켜지 않게). 공정 중이었으면 True."""
        was = bool(e.busy)
        if hasattr(e, "program_exit"):
            e.program_exit()
        elif was:
            e.emit("run_end", f"프로그램 종료 — 파워 출력만 끔({reason})", "PROGRAM_EXIT", level="error", result="error",
                   run_id=e.run_id)
            e.error = "PROGRAM_EXIT"
        e.actions.send = lambda step, runner: None
        with e.lk:
            e._closing = True
            e._cancel = True
            e.queue_mode = False
            e.lk.notify_all()
        self._held.add(e.ch)
        return was

    def power_off_only(self, reason: str = "PROGRAM_EXIT", deadline_s: float = FORCE_DEADLINE_S) -> Optional[dict]:
        """결정 4 — 파워 출력만 끈다(확인을 기다리지 않는 명령을 먼저 다 보냄). 두 번째 호출은 None."""
        if not self._forced_lk.acquire(blocking=False):
            return None
        clk = self.clock
        t0 = clk.monotonic()
        done: Dict[str, bool] = {}
        sent: List[str] = []
        was: List[str] = []

        def cb(name: str):
            done[name] = False
            sent.append(name)

            def fin(*a) -> None:
                done[name] = True
                clk.notify()
            return fin
        self._emit("fired", "S32", "POWER_OFF_ONLY", f"의도적 종료({reason}) — 파워 출력만 끔", "warn")
        for ch, e in self.proc.items():
            if e is not None and self.hold_process(e, reason):
                was.append(ch)
        pc_held = self._hold_pc(reason)                   # PC 도 붙잡는다(정지 정리 없이) — 뒤의 PC RF 즉시 끄기와 엇갈리지 않게
        if pc_held:
            was.append("pc")
        if not self.bench:
            s = self.sys
            for unit, d in sorted((getattr(s, "dc", {}) or {}).items()):
                if d is not None:
                    d.enabled = True
                    self._try(lambda d=d, u=unit: d.stop(on_done=cb(f"DC{u} OFF")))
            for mode, d in (getattr(s, "rf", {}) or {}).items():
                if d is None:
                    continue
                self._try(lambda d=d, m=mode: rf_zero_off(d, cb(f"RF({m}) 0 W·SET OFF")))
            for ch in ("ch1", "ch2"):
                dp = (getattr(s, "dcpulse", {}) or {}).get(ch)
                if dp is not None and dp.worker is not None:
                    self._try(lambda dp=dp, ch=ch: dp.output_off(on_done=cb(f"{ch} DC Pulse OFF")))
                rp = (getattr(s, "rfpulse", {}) or {}).get(ch)
                if rp is not None and rp.worker is not None:
                    self._try(lambda rp=rp, ch=ch: rp.stop_process(on_done=cb(f"{ch} RF Pulse OFF")))
            if self.heater is not None:
                self._try(lambda: self.heater.shutdown(on_done=cb("히터 RUN=0")))
            if pc_held:
                sent.append("플라즈마 클리닝 붙잡음(PC RF 만 끔 — 가스·밸브 그대로)")
            if self.tsp is not None and self.tsp.busy:
                self._try(lambda: self.tsp.abort("PROGRAM_EXIT"))
                sent.append("TSP 이상 중단(TSP OFF)")
                was.append("tsp")
        t_sent = clk.monotonic() - t0
        if done:
            clk.wait_until(t0 + float(deadline_s), lambda: all(done.values()))
        res = {"why": reason, "sent": sent, "done": [k for k, v in done.items() if v], "sent_s": round(t_sent, 3),
               "elapsed_s": round(clk.monotonic() - t0, 3), "process_was_running": was, "bench": self.bench}
        self.forced = res
        self._emit("released", "S32", "POWER_OFF_ONLY",
                   f"보낸 것 {len(sent)} · 끝난 것 {len(res['done'])} · {res['elapsed_s']:.2f} s", "info")
        return res

    def _hold_pc(self, reason: str) -> bool:
        """플라즈마 클리닝을 정지 순서 없이 붙잡는다(엔진 program_exit — 끝 정리에서 RF 안전 정지·가스 FLOW OFF·SP4 VALVE OPEN 을
        보내지 않음, 결과 실패 PROGRAM_EXIT). 그런 길이 없는 엔진이면 v1 RF 안전 정지(abort). 돌고 있었으면 True."""
        pc = self.pc
        if pc is None or not bool(getattr(pc, "busy", False)):
            return False
        f = getattr(pc, "program_exit", None)
        held = [False]

        def go() -> None:
            held[0] = bool(f(reason) if callable(f) else pc.abort(reason))
        self._try(go)
        return held[0]

    def _try(self, fn) -> None:
        try:
            fn()
        except Exception as e:
            self._emit("error", "S32", "INTERNAL_ERROR", repr(e), "error")

    # ───────── 보기 ─────────
    def public_state(self) -> dict:
        return {"stopping": self.stopping, "allStops": self.all_stops, "residual": [list(x) for x in self.residual],
                "forced": self.forced, "firstRead": self.first_done,
                "commLostS": {k: round(v, 1) for k, v in self.comm_lost_s.items()}}
