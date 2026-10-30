"""로봇 명령 처리 바탕 — v1 host/handlers.py 의 공통 부분(잠금·명령 태그·PLC 구간·PLC 로그·응답 모양·busy 판정).

v1 은 asyncio 코루틴(요청마다 태스크), v2 는 요청마다 스레드(robot_api 의 ThreadingHTTPServer)라 같은 뜻을 이렇게 옮긴다.
- 잠금(lock_plc·lock_ch1·lock_ch2·로드락 전환 잠금·게이트 잠금): ClockLock — 시계 조건 변수로 기다린다(가상 시계 시험에서
  기다리는 스레드가 '기다림'으로 보여 시각이 넘어간다). locked() 는 asyncio.Lock.locked() 와 같은 뜻.
- 명령 태그(_cmd_tag_var — 코루틴별): ContextVar(스레드마다 따로). _plc_command(tag) 블록 안 실패에만 meta.cmd 가 붙는다.
- 요청 범위(_scope): 이 요청이 남긴 로그 줄(robot_api 가 응답 log 로 돌려준다)·요청 정보(request_id·peer·received_at).
- PLC 구간(_plc_call — v1 handlers.py:261-293): lock_plc 를 잡고 하트비트를 쉰다(shared.PlcLink.begin_robot/end_robot — 겹치면
  참여 수를 센다). 이 구간에서 부른 PLC 연산만 PLC_REMOTE 로그 '{시각} [{태그}] {문구}'(v1 _plc_file_logger + plc.log_route).
- PLC 연산(read_bit·write_switch·press_switch·main_shutter·is_mapped)은 v1 AsyncPLC 와 같은 순서·문구로 로그를 남기고
  PLC 드라이버(출처 ROBOT proxy)에 요청 하나씩 넣어 결과를 기다린다. 실패는 PLCError(v1 이름 그대로 — 'Gate 상태 조회 실패:
  PLCError: ..' 문구) 와 v1 코드(E401 연결·E402 무응답·E403 거부). 문구(plc_error_text)는 v2 드라이버 문구다 — v1 은 pymodbus
  예외 문구라 바이트가 같을 수 없다(시험은 자리표로 가린다, differences 에 기록).
- 응답(_ok·_fail)·실패 보고(log 'ERROR/HOST' + on_report — v1 chat_host·popup 자리, 결정 D09)는 v1 _ok·_fail·notify_* 그대로.
"""
from __future__ import annotations

import contextlib
import contextvars
import datetime as _dt
import json
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

from v2.common.worker import Status
from v2.ch12 import errors as ER
from v2.ch12.drivers import plc_map as PM
from v2.ch12.robot_engines import loadlock_error_channel

Json = Dict[str, Any]
LogFn = Callable[[str, str], None]
PLC_WAIT_LIMIT_S = 120.0          # PLC 요청 하나의 결과를 기다리는 한도(드라이버 요청 마감이 먼저 끝난다 — 안전망)

_STATE_MSG = {"PREFLIGHT": "공정 시작 준비 중", "RUNNING": "공정 실행 중", "CLEANUP": "공정 종료 처리 중",
              "STOPPING": "공정 정지 중", "COOLDOWN": "다음 공정 대기 중", "DELAY": "공정 간 대기 중"}
_STAGE_MSG = {"ADVANCE_QUEUE": "리스트 공정 대기 중 (다음 공정 예약됨)", "PROCESS": "공정 실행 중",
              "PREFLIGHT": "공정 시작 준비 중", "CLEANUP": "공정 종료 처리 중"}


class PLCError(RuntimeError):
    """v1 device/plc.py PLCError — code(E401/E402/E403)·op·addr."""

    def __init__(self, code: str, message: str, *, op: Optional[str] = None, addr: Optional[int] = None) -> None:
        super().__init__(message)
        self.code, self.op, self.addr = code, op, addr


_RESULT_CODE = {Status.TIMEOUT: "E402", Status.LINK_DOWN: "E401", Status.EXPIRED: "E402", Status.CANCELLED: "E402",
                Status.REJECTED: "E403"}


def plc_error_code(res) -> str:
    if res.status == Status.FAIL and res.reason == "LINK_DOWN":
        return "E401"
    return _RESULT_CODE.get(res.status, "E403")


def plc_error_text(code: str, res, op: str, addr: int) -> str:
    """PLCError 문구 — v2 드라이버가 남긴 오류(시험은 이 함수를 바꿔 자리표로 가린다)."""
    why = res.error or res.reason or res.status
    return f"PLC 요청 실패({res.status}): {why} (op={op}, addr={addr})"


# ───────────────────────── 시계 잠금 ─────────────────────────
class ClockLock:
    """시계 조건 변수로 기다리는 잠금(asyncio.Lock 자리). 같은 스레드 다시 잡기는 안 된다(asyncio.Lock 과 같다)."""

    def __init__(self, clock, name: str = "") -> None:
        self.clock, self.name = clock, name
        self._held = False

    def locked(self) -> bool:
        return self._held

    def acquire(self) -> None:
        with self.clock.cond:
            self.clock.wait_until(None, lambda: not self._held)
            self._held = True

    def release(self) -> None:
        with self.clock.cond:
            self._held = False
            self.clock.cond.notify_all()

    def __enter__(self) -> "ClockLock":
        self.acquire()
        return self

    def __exit__(self, *exc) -> bool:
        self.release()
        return False


# ───────────────────────── 요청 범위 ─────────────────────────
@dataclass
class Scope:
    request_id: str = ""
    peer: str = ""                                  # v1 str(peername) — "('127.0.0.1', 5000)"
    received_at: Optional[_dt.datetime] = None
    lines: List[Tuple[str, str]] = field(default_factory=list)
    plc_route: int = 0                              # _plc_call 깊이(PLC 로그를 남기는 구간)


_scope_var: contextvars.ContextVar[Optional[Scope]] = contextvars.ContextVar("robot_scope", default=None)
_tag_var: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("robot_cmd_tag", default=None)


def peer_text(peer: str) -> str:
    """로봇 서버의 'ip:port' → v1 str(peername) "('ip', port)". 모양이 다르면 그대로."""
    s = str(peer or "")
    host, sep, port = s.rpartition(":")
    if sep and host and port.isdigit() and ":" not in host:
        return str((host, int(port)))
    return s


def parse_received_at(v: Any) -> Optional[_dt.datetime]:
    if isinstance(v, _dt.datetime):
        return v
    try:
        return _dt.datetime.fromisoformat(str(v)) if v else None
    except Exception:
        return None


class RobotBase:
    def __init__(self, clock, plc_link, cfg: Dict[str, Any], engines=None, robot_csv=None,
                 on_report: Optional[Callable[[Json, str], None]] = None, log: Optional[LogFn] = None) -> None:
        self.clock, self.link, self.cfg = clock, plc_link, cfg
        self.engines = engines
        self.robot_csv = robot_csv
        self.on_report = on_report
        self.global_log = log
        self.plc = plc_link.proxy("ROBOT")
        self.lock_plc = ClockLock(clock, "plc")
        self.lock_ch = {1: ClockLock(clock, "ch1"), 2: ClockLock(clock, "ch2")}
        self._loadlock_transition_lock = ClockLock(clock, "loadlock_transition")
        self._loadlock_transition_tag: Optional[str] = None
        self._loadlock_gate_lock = ClockLock(clock, "loadlock_gate")
        self._pc_start_pending = False            # v1 PlasmaCleaningRuntime._host_start_future 대기 중(E206)
        self._req_scope: Dict[int, Tuple[Scope, Optional[str]]] = {}   # 진행 중 PLC 요청 → (요청 범위, 태그)
        self._tap_driver_log(getattr(plc_link, "plc", None))

    def _tap_driver_log(self, drv) -> None:
        """PLC 드라이버 저수준 로그(소켓 재생성 'WARN PLC 소켓 재생성 #..' 등)가 이 요청의 PLC 연산 중에 나면 그 요청 로그에도
        — v1 은 log_route 로 그 작업의 PLC 로그가 모두 PLC_REMOTE 로 갔다(device/plc.py:992-998). 드라이버 로그는 그대로 둔다."""
        io = getattr(drv, "io", None)
        worker = getattr(drv, "worker", None)
        if io is None or worker is None or not callable(getattr(io, "log", None)):
            return
        orig = io.log

        def tapped(msg: str) -> None:
            orig(msg)
            got = self._req_scope.get(id(getattr(worker, "inflight", None)))
            if got is not None:
                sc, tag = got
                with contextlib.suppress(Exception):
                    self._plc_log(str(msg), sc, tag)
        io.log = tapped

    # ───────── 시간 ─────────
    def monotonic(self) -> float:
        return self.clock.monotonic()

    def sleep(self, dt: float) -> None:
        if dt > 0:
            self.clock.wait_until(self.clock.monotonic() + float(dt))

    def now_text(self) -> str:
        return _dt.datetime.fromtimestamp(self.clock.wall()).strftime("%Y-%m-%d %H:%M:%S")

    # ───────── 로그 ─────────
    @staticmethod
    def scope() -> Scope:
        s = _scope_var.get()
        if s is None:
            s = Scope()
            _scope_var.set(s)
        return s

    def log(self, tag: str, text: str) -> None:
        self.scope().lines.append((tag, text))
        if self.global_log is not None:
            with contextlib.suppress(Exception):
                self.global_log(tag, text)

    def _plc_log(self, msg: str, sc: Optional[Scope] = None, tag: Optional[str] = None) -> None:
        """v1 _plc_file_logger — PLC 구간 안에서만(plc.log_route). sc·tag 를 주면 그 요청으로(다른 스레드에서)."""
        if sc is None:
            sc, tag = self.scope(), _tag_var.get()
        if sc.plc_route <= 0:
            return
        tag = (tag or "").strip()
        line = f"{self.now_text()} [{tag}] {msg}" if tag else f"{self.now_text()} {msg}"
        sc.lines.append(("PLC_REMOTE", line))
        if self.global_log is not None:
            with contextlib.suppress(Exception):
                self.global_log("PLC_REMOTE", line)

    # ───────── 명령 태그·PLC 구간 ─────────
    @property
    def current_tag(self) -> Optional[str]:
        return _tag_var.get()

    @contextlib.contextmanager
    def _plc_command(self, tag: str) -> Iterator[None]:
        tok = _tag_var.set(tag)
        try:
            yield
        finally:
            _tag_var.reset(tok)

    @contextlib.contextmanager
    def _plc_call(self) -> Iterator[None]:
        sc = self.scope()
        with self.lock_plc:
            sc.plc_route += 1
            self.link.begin_robot()
            try:
                yield
            finally:
                with contextlib.suppress(Exception):
                    self.link.end_robot()
                sc.plc_route -= 1

    # ───────── PLC 연산(v1 AsyncPLC 문구·순서) ─────────
    def _wait_result(self, submit: Callable[[Callable], Any], op: str, addr: int):
        box: list = []

        def done(r) -> None:
            box.append(r)
            self.clock.notify()
        t_sub = self.clock.monotonic()
        sc_tag = (self.scope(), _tag_var.get())
        with self.clock.cond:                      # 요청이 바로 실행돼도 로그 범위를 먼저 알도록
            req = submit(done)
            if req is not None:
                self._req_scope[id(req)] = sc_tag
        try:
            if not self.clock.wait_until(self.clock.monotonic() + PLC_WAIT_LIMIT_S, lambda: bool(box)):
                raise PLCError("E402", f"PLC 요청 결과 없음({PLC_WAIT_LIMIT_S:.0f}s) (op={op}, addr={addr})",
                               op=op, addr=addr)
        finally:
            if req is not None:
                self._req_scope.pop(id(req), None)
        r = box[0]
        # v1 AsyncPLC._io_lock 경고(device/plc.py:2257-2301) — 이 요청의 PLC 로그로(대기 = worker 큐, 실행 = 간격 + 입출력)
        if r.started is not None:
            waited = (r.started - t_sub) * 1000.0
            if waited >= float(self.cfg.get("PLC_LOCK_WARN_MS", 2500.0)):
                self._plc_log(f"WARN lock-wait {waited:.0f} ms (op={op}, addr={addr})")
            if r.finished is not None:
                io_ms = (r.finished - r.started) * 1000.0
                if io_ms >= float(self.cfg.get("PLC_IO_WARN_MS", 2500.0)):
                    self._plc_log(f"WARN in-lock IO {io_ms:.0f} ms (op={op}, addr={addr})")
        if r.status != Status.OK:
            code = plc_error_code(r)
            raise PLCError(code, plc_error_text(code, r, op, addr), op=op, addr=addr)
        return r.value

    def _write_coil(self, addr: int, on: bool) -> None:
        self._wait_result(lambda cb: self.plc.write_coil(addr, bool(on), on_done=cb), "write_coil", addr)

    def read_bit(self, name: Any) -> bool:
        addr = PM.resolve(name)                   # KeyError '알 수 없는 주소/이름: ..'(v1 _addr)
        v = bool(self._wait_result(lambda cb: self.plc.read_coil(addr, on_done=cb), "read_coil", addr))
        self._plc_log(f"read {name} (addr={addr}) -> {v}")
        return v

    def write_switch(self, name: Any, on: bool) -> None:
        addr = PM.resolve(name)
        self._plc_log(f"set {name} <- {bool(on)} (addr={addr})")
        self._write_coil(addr, bool(on))

    def press_switch(self, name: Any) -> None:
        """v1 write_switch(momentary=True) → pulse: ON → max(0.03, ms/1000) s → OFF(device/plc.py:1633-1662)."""
        addr = PM.resolve(name)
        ms = int(self.cfg.get("PLC_CMD_PULSE_MS", 180))
        self._plc_log(f"pulse {name} (addr={addr}, {ms}ms)")
        self._write_coil(addr, True)
        self.sleep(max(0.03, ms / 1000.0))
        self._write_coil(addr, False)

    def main_shutter(self, ch: int, *, open: bool) -> None:
        self.write_switch(f"MAIN_SHUTTER_{ch}_SW", bool(open))
        self._plc_log(f"MainShutter{ch} {'OPEN(ON)' if open else 'CLOSE(OFF)'}")

    @staticmethod
    def is_mapped(name: str) -> bool:
        try:
            PM.resolve(name)
            return True
        except Exception:
            return False

    # ───────── 응답(v1 _ok·_fail·notify_*) ─────────
    def _report(self, payload: Json, src: str = "HOST") -> Json:
        with contextlib.suppress(Exception):
            self.log(f"ERROR/{src}", ER.format_log_text(payload))
        if self.on_report is not None:
            with contextlib.suppress(Exception):
                self.on_report(payload, src)
        return payload

    def notify_code(self, code: str, detail: str = "", meta: Optional[Json] = None, src: str = "HOST") -> Json:
        return self._report(ER.build_error_payload_from_code(code, detail=detail, meta=meta), src)

    def notify_unknown(self, e: BaseException, meta: Optional[Json] = None, src: str = "HOST") -> Json:
        return self._report(ER.build_unknown_error_payload(e, meta=meta), src)

    @staticmethod
    def _ok(msg: str = "OK", **extra) -> Json:
        return {"result": "success", "message": msg, **extra}

    def _fail(self, e: Any, *, code: Optional[str] = None, src: str = "HOST") -> Json:
        if isinstance(e, dict) and e.get("result") == "fail":
            return e
        if code is None and isinstance(e, BaseException):
            code = getattr(e, "code", None) or getattr(e, "error_code", None)
            if code is None and isinstance(e, KeyError):
                code = "E411"
        if isinstance(e, BaseException):
            detail = getattr(e, "detail", None) or str(e)
        else:
            detail = "" if e is None else str(e)
        meta: Json = {}
        if isinstance(e, BaseException):
            em = getattr(e, "meta", None)
            if isinstance(em, dict):
                meta.update(em)
        tag = _tag_var.get()
        if tag:
            if "cmd" in meta and meta["cmd"] != tag:
                meta["host_cmd"] = tag
            else:
                meta["cmd"] = tag
        if code:
            return self.notify_code(code, detail=detail, meta=meta or None, src=src)
        if isinstance(e, BaseException):
            return self._report(ER.build_handler_error_payload(e, meta=meta or None), src)
        return self.notify_code("E110", detail=detail or "Handler failure", meta=meta or None, src=src)

    # ───────── 공정 쪽 사실(어댑터) ─────────
    def ch_facts(self, ch: int) -> Optional[Json]:
        """chamber_state(ch) — 어댑터가 없으면 None(v1 runtime_state·ctx.chN 이 없음)."""
        if self.engines is None:
            return None
        return dict(self.engines.chamber_state(int(ch)) or {})

    def _fail_if_ch_busy(self, ch: int, action: str) -> Optional[Json]:
        """v1 _fail_if_ch_busy(host/handlers.py:389-474) — 사실을 읽다 예외가 나면 통과."""
        try:
            f = self.ch_facts(ch)
            if f is None:
                return None
            reasons: List[str] = []
            if f.get("chamber_running"):
                reasons.append(f"CH{ch} 스퍼터 공정 실행 중")
            if f.get("pc_running"):
                reasons.append(f"CH{ch} Plasma Cleaning 실행 중")
            if int(ch) == 1 and f.get("tsp_running"):
                reasons.append("TSP 공정 실행 중")
            st = f.get("runner_state")
            if isinstance(st, str) and st and st.upper() != "IDLE":
                m = _STATE_MSG.get(st.upper())
                reasons.append(f"CH{ch} {m}" if m else f"CH{ch} 처리 중 ({st})")
            if f.get("stage_alive"):
                k = f.get("stage_kind")
                if k:
                    m = _STAGE_MSG.get(k)
                    reasons.append(f"CH{ch} {m}" if m else f"CH{ch} 처리 중 ({k})")
                else:
                    reasons.append(f"CH{ch} 처리 중")
            if reasons:
                return self._fail(f"{action} 불가 — " + " / ".join(reasons), code="E205")
        except Exception:
            return None
        return None

    def _fail_if_loadlock_transition_busy(self, action: str) -> Optional[Json]:
        if self._loadlock_transition_lock.locked():
            tag = self._loadlock_transition_tag or "LOADLOCK_TRANSITION"
            return self._fail(f"{action} 불가 — {tag} 진행 중", code="E321")
        return None

    def chamber_status(self, ch: int) -> str:
        """GET_SPUTTER_STATUS 의 Chamber_n(v1 _ch_state, host/handlers.py:543-584)."""
        try:
            f = self.ch_facts(ch)
        except Exception:
            return "error"
        if f is None:
            return "idle"
        running = bool(f.get("chamber_running") or f.get("pc_running"))
        st = f.get("runner_state")
        if isinstance(st, str) and st and st.upper() != "IDLE":
            running = True
        elif f.get("stage_alive"):
            running = True
        if not running and f.get("has_error"):
            return "error"
        return "running" if running else "idle"

    def loadlock_status(self) -> str:
        """Loadlock_Chamber(v1 _loadlock_state, host/handlers.py:620-665)."""
        if self.engines is None:
            return "idle"
        try:
            ps = dict(self.engines.pc_state() or {})
            run = ps.get("running") or {}
            if any(bool(run.get(ch)) for ch in (1, 2)):
                return "running"
            if loadlock_error_channel(dict(self.engines.last_pc_result() or {})) is not None:
                return "error"
        except Exception:
            return "error"
        if ps.get("ch1_ig_wait"):
            return "running"
        cleaning = ps.get("cleaning")
        if cleaning is None:
            return "idle"
        return "running" if bool(cleaning) else "idle"

    def build_eta(self) -> Json:
        out: Json = {}
        for ch in (1, 2):
            v = None
            if self.engines is not None:
                with contextlib.suppress(Exception):
                    v = self.engines.eta(ch)
            out[f"CH{ch}"] = {"remaining_s": v}
        return out

    # ───────── Robot CSV(v1 _hostlog_*) ─────────
    def _hostlog_begin(self, target: str, recipe: str) -> str:
        import os
        hl = self.robot_csv
        if hl is None:
            return ""
        try:
            sc = self.scope()
            rx = sc.received_at or _dt.datetime.fromtimestamp(self.clock.wall())
            return hl.request(target=str(target), request_id=sc.request_id, peer=sc.peer, received_at=rx,
                              recipe_name=os.path.basename(str(recipe or "")))
        except Exception:
            return ""

    def _hostlog_reject_if_unowned(self, key: str, reason: str) -> None:
        hl = self.robot_csv
        if hl is None or not key:
            return
        with contextlib.suppress(Exception):
            if not hl.is_owned(key):
                hl.reject(key, reason)

    @staticmethod
    def _hostlog_reason_from_res(res: Any) -> str:
        try:
            if isinstance(res, dict):
                return f"{res.get('error_code', '') or ''} {res.get('message', '') or ''}".strip()
        except Exception:
            pass
        return "요청 거절"

    def request_meta(self, key: str) -> Json:
        sc = self.scope()
        return {"key": key, "request_id": sc.request_id, "peer": sc.peer}


def cfg_tuple(v: Any) -> tuple:
    """설정의 목록 값(JSON 문자열 '["CH1", ...]' 도) → tuple(v1 tuple(getattr(cfg, ..)))."""
    if isinstance(v, str):
        try:
            v = json.loads(v)
        except Exception:
            return (v,)
    return tuple(v)
