"""v1 식 ASCII 줄 장비(MFC·IG — MOXA NPort TCP, 명령 + CR) 공용 — 줄 입출력과 동작 단위(op) 실행기.

v1 device/mfc.py·device/ig.py 는 asyncio 단일 명령 큐(_cmd_q) + 워커(_cmd_worker_loop) 위에서 '고수준 동작'(FLOW_ON →
실제 전송 확인 → 대기 → 안정화 …)을 코루틴으로 돌렸다. v2 는 포트 독점 worker(v2.common.worker) 위에서 같은 순서를
생성기(generator)로 돌린다 — 생성기가 아래 항목을 하나씩 내면(yield) 실행기가 그 I/O 하나를 worker 요청 하나로 넣고,
결과를 생성기에 돌려준다. 그 사이 1초 읽기(스냅샷 묶기)가 끼어들 수 있다(드라이버가 언제 끼어들지 정한다).

생성기가 내는 항목
- Send(cmd, gap_s, …)  : 응답 없는 명령(no-reply). 결과 ("sent", 보낸 시각) / ("fail", v1 문구, 사유 코드).
                         v1 _await_sent(device/mfc.py:2032-2064)와 같게 '실제로 보냈는지'를 최대 bound_s(30 s) 기다린다 —
                         링크가 없으면 다시 넣어 기다리고(v1 은 큐에서 기다림), 넘으면 withdraw=True 는 철회 후 실패,
                         False(끄는 계열)는 실패를 알리되 명령은 남겨 순서대로 보낸다(유령 전송 — 보낼 때까지 또는 purge 까지).
- Query(cmd, …)        : 응답 한 줄 질의. 결과 = 줄(str) 또는 None(v1 콜백 None — 시간 초과·폐기).
                         caller_s = v1 호출 쪽 대기(_send_and_wait_line 의 wait_for 한도) — 그 안에 못 받으면 None.
                         무응답(ReadTimeout)이고 retries 가 남으면 v1 처럼 다시 넣는다(재연결 뒤 다시 보냄).
- Sleep(s) / Until(t)  : 기다림(포트를 잡지 않음 — 그 사이 다른 요청이 돈다).
- Disconnect(why)      : v1 _on_tcp_disconnected() 처럼 연결을 끊는다(worker 가 재연결).
실행기의 모든 생성기 진행(send)은 worker 스레드에서 한다(드라이버 상태를 한 스레드만 바꾼다).
"""
from __future__ import annotations

import collections
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from v2.common.transport import ReadTimeout, TransportError
from v2.common.worker import Request, Result, Status, Worker

SEND_CONFIRM_TIMEOUT_S = 30.0      # v1 device/mfc.py:64 — no-reply 실제 전송 확인 최대 대기
LINK_RETRY_S = 0.2                 # 링크가 없을 때 다시 넣는 간격(v1 워커는 0.05 s 마다 연결을 봄 — 너무 잦지 않게)
GHOST_MAX_S = 3600.0               # 끄는 명령(유령 전송)을 보내려고 버티는 최대 시간 — 그 뒤엔 포기하고 이벤트
RETRY = object()                   # io_query 가 '연결 유지 재시도'(v1 연결 직후 무응답)를 알릴 때 돌려주는 값
CALLER_TIMEOUT = object()          # 호출 쪽 wait_for 한도 초과(Query.mark_timeout 일 때만 — 아니면 None)


@dataclass
class Send:
    cmd: str
    gap_s: float
    bound_s: float = SEND_CONFIRM_TIMEOUT_S
    withdraw: bool = True
    retries: int = 5               # v1 _enqueue retries_left=5 — 전송(write) 오류 재시도
    tag: str = ""


@dataclass
class Query:
    cmd: str
    timeout_s: float
    caller_s: float
    prefixes: tuple = ()
    retries: int = 1               # v1 워커 재시도(_send_and_wait_line retries)
    tag: str = ""
    mark_timeout: bool = False     # True 면 호출 쪽 한도 초과를 None 이 아닌 CALLER_TIMEOUT 으로 알린다(IG — v1 은 그때만 재시도)


@dataclass
class Sleep:
    seconds: float


@dataclass
class Until:
    t: float


@dataclass
class Disconnect:
    why: str


@dataclass
class _Op:
    name: str
    gen: Any
    on_done: Optional[Callable[["_Op", Optional[str], str], None]]
    kind: str = "write"
    done: bool = False
    next_io: Optional[float] = None       # 다음 I/O 예정 시각(대기 중이면 지금)
    live: Optional[dict] = None           # 지금 진행 중인 항목 상태
    meta: Dict[str, Any] = field(default_factory=dict)


class OpFailed(Exception):
    """생성기가 실패로 끝낼 때 — (사유 코드, v1 문구)."""

    def __init__(self, reason: str, detail: str) -> None:
        super().__init__(detail)
        self.reason, self.detail = reason, detail


class OpRunner:
    """생성기 op 실행기. dev = io_send(session, Send) → 보낸 시각, io_query(session, Query, retries_left) → 줄(또는 RETRY),
    exec_timeout_s(item) → 요청 하나의 실행 한도(간격 대기 + 응답 한도 + 여유), on_withdraw·on_ghost_sent·on_ghost_gave_up 를 가진 드라이버."""

    def __init__(self, dev, worker: Worker, clock) -> None:
        self.dev, self.worker, self.clock = dev, worker, clock
        self.ops: List[_Op] = []
        self.last_end = -1e9                  # 마지막 op 가 끝난 시각(1초 읽기 양보 판단)
        self.ghosts: List[dict] = []          # 철회하지 않고 남겨 둔 끄는 명령(보낼 때까지 다시 넣음)

    # ───────── 상태 ─────────
    def busy_until(self) -> Optional[float]:
        """진행 중 op 의 가장 이른 다음 I/O 시각(없으면 None). 대기 중(큐에 있음)이면 지금."""
        ts = [o.next_io for o in self.ops if not o.done and o.next_io is not None]
        return min(ts) if ts else None

    def active(self) -> bool:
        return any(not o.done for o in self.ops)

    # ───────── 시작 ─────────
    def start(self, name: str, gen, on_done=None, kind: str = "write", **meta) -> _Op:
        op = _Op(name, gen, on_done, kind=kind, meta=dict(meta))
        op.next_io = self.clock.monotonic()
        self.ops.append(op)
        # 첫 진행도 worker 스레드에서 — 빈 요청 하나로(링크·bench 와 무관하게 실행된다)
        self.worker.submit(Request(kind if kind == "read" else "write", lambda s: None,
                                   deadline=None, on_done=lambda r: self._advance(op, None),
                                   name=f"{name}:start", confirms_link=False))
        return op

    def call_in_worker(self, fn: Callable[[], None], name: str = "ctl") -> None:
        """드라이버 상태만 바꾸는 호출을 worker 스레드에서(결과 상태와 무관하게 fn 실행)."""
        self.worker.submit(Request("write", lambda s: None, on_done=lambda r: fn(), name=name, confirms_link=False))

    # ───────── 진행 ─────────
    def _finish(self, op: _Op, reason: Optional[str], detail: str) -> None:
        if op.done:
            return
        op.done, op.next_io, op.live = True, None, None
        if op in self.ops:
            self.ops.remove(op)
        self.last_end = self.clock.monotonic()
        if op.on_done:
            op.on_done(op, reason, detail)

    def _advance(self, op: _Op, value) -> None:
        if op.done:
            return
        try:
            item = op.gen.send(value)
        except StopIteration as e:
            v = e.value
            if isinstance(v, tuple):
                self._finish(op, v[0], v[1])
            else:
                self._finish(op, None, "" if v is None else str(v))
            return
        except OpFailed as e:
            self._finish(op, e.reason, e.detail)
            return
        except Exception as e:             # 생성기 내부 오류 — v1 handle_command 의 '예외: …'
            self._finish(op, "INTERNAL_ERROR", f"예외: {e}")
            return
        now = self.clock.monotonic()
        if isinstance(item, Sleep):
            self._wake(op, now + max(0.0, float(item.seconds)))
        elif isinstance(item, Until):
            self._wake(op, float(item.t))
        elif isinstance(item, Send):
            op.live = {"item": item, "t_enq": now, "tries": 0, "reported": False, "cancel": False}
            self._issue_send(op, op.live, None)
        elif isinstance(item, Query):
            op.live = {"item": item, "t_enq": now, "deadline": now + item.caller_s, "retries": item.retries,
                       "cancel": False, "answered": False}
            self._issue_query(op, op.live, None)
            self._deadline_wake(op, op.live)
        elif isinstance(item, Disconnect):
            op.next_io = now
            why = item.why

            def fn(session, why=why):
                raise TransportError(why)
            self.worker.submit(Request("write", fn, on_done=lambda r: self._advance(op, None),
                                       name=f"{op.name}:disconnect", confirms_link=False))
        else:
            self._finish(op, "INTERNAL_ERROR", f"예외: 알 수 없는 항목 {item!r}")

    def _wake(self, op: _Op, t: float) -> None:
        op.next_io = t
        self.worker.submit(Request("write" if op.kind != "read" else "read", lambda s: None,
                                   on_done=lambda r: self._advance(op, None), name=f"{op.name}:wait",
                                   not_before=t, confirms_link=False))

    # ───────── no-reply 전송 ─────────
    def _issue_send(self, op: Optional[_Op], st: dict, not_before: Optional[float]) -> None:
        item: Send = st["item"]
        if op is not None:
            op.next_io = not_before or self.clock.monotonic()

        def fn(session):
            if st["cancel"]:
                return None                    # 철회·폐기된 명령 — 보내지 않는다
            return self.dev.io_send(session, item)

        self.worker.submit(Request("write", fn, on_done=lambda r: self._send_result(op, st, r),
                                   name=f"{getattr(op, 'name', 'ghost')}:{item.cmd}", not_before=not_before,
                                   exec_timeout_s=self.dev.exec_timeout_s(item), confirms_link=False))

    def _send_result(self, op: Optional[_Op], st: dict, r: Result) -> None:
        if st["cancel"]:
            return
        item: Send = st["item"]
        now = self.clock.monotonic()
        ghost = op is None or op.live is not st
        if r.status == Status.OK and r.value is not None:
            st["cancel"] = True
            if ghost:
                self._drop_ghost(st)
                self.dev.on_ghost_sent(item, r.value)
            else:
                self._advance(op, ("sent", float(r.value)))
            return
        if r.status == Status.REJECTED:
            st["cancel"] = True
            self._drop_ghost(st)
            if not ghost:
                self._advance(op, ("fail", "bench — 장비 쓰기 금지", r.reason or "BENCH_READ_ONLY"))
            return
        if r.status == Status.CANCELLED:            # worker safety purge — 보내기 전에 버려짐
            st["cancel"] = True
            self._drop_ghost(st)
            if not ghost:
                self._advance(op, ("fail", "전송 전에 대기열에서 폐기됨(또는 전송 실패)", "PURGED"))
            return
        if r.status == Status.FAIL and r.reason != "LINK_DOWN":
            st["tries"] += 1                         # 드라이버 오류(가드 등) — 재시도해도 같음
            st["cancel"] = True
            self._drop_ghost(st)
            if not ghost:
                self._advance(op, ("fail", "전송 전에 대기열에서 폐기됨(또는 전송 실패)", r.reason or "DEVICE_REJECTED"))
            return
        if r.status == Status.FAIL:                  # 쓰는 중 끊김 — v1 retries_left 만큼 다시
            st["tries"] += 1
            if st["tries"] > item.retries:
                st["cancel"] = True
                self._drop_ghost(st)
                if not ghost:
                    self._advance(op, ("fail", "전송 전에 대기열에서 폐기됨(또는 전송 실패)", "LINK_DOWN"))
                return
        waited = now - st["t_enq"]
        if not ghost and waited >= item.bound_s:
            b = f"{item.bound_s:.0f}"
            if item.withdraw:
                st["cancel"] = True
                self.dev.on_withdraw(item, f"{b}초 전송 대기 초과")
                self._advance(op, ("fail", f"{b}초 안에 전송되지 못해 대기열에서 철회", "SEND_TIMEOUT"))
                return
            # 끄는 계열 — 실패를 알리고 명령은 남긴다(유령 전송)
            self.ghosts.append(st)
            self._advance(op, ("fail", f"{b}초 안에 전송 확인 못 함(명령은 대기열에 남아 순서대로 전송)", "SEND_TIMEOUT"))
        if ghost and waited >= GHOST_MAX_S:
            st["cancel"] = True
            self._drop_ghost(st)
            self.dev.on_ghost_gave_up(item)
            return
        self._issue_send(None if ghost or (op is not None and op.live is not st) else op, st,
                         now + LINK_RETRY_S)

    def _drop_ghost(self, st: dict) -> None:
        if st in self.ghosts:
            self.ghosts.remove(st)

    # ───────── 질의 ─────────
    def _issue_query(self, op: _Op, st: dict, not_before: Optional[float]) -> None:
        item: Query = st["item"]
        op.next_io = not_before or self.clock.monotonic()

        def fn(session):
            if st["cancel"] or st["answered"]:
                return None
            return self.dev.io_query(session, item, retries_left=st["retries"])

        self.worker.submit(Request("write" if op.kind != "read" else "read", fn,
                                   on_done=lambda r: self._query_result(op, st, r),
                                   name=f"{op.name}:{item.cmd}", not_before=not_before,
                                   exec_timeout_s=self.dev.exec_timeout_s(item), confirms_link=True))

    def _answer(self, op: _Op, st: dict, line: Optional[str]) -> None:
        if st["answered"] or st["cancel"]:
            return
        st["answered"] = True
        if op.live is st:
            self._advance(op, line)

    def _query_result(self, op: _Op, st: dict, r: Result) -> None:
        if st["answered"] or st["cancel"]:
            return
        now = self.clock.monotonic()
        if r.status == Status.OK and r.value is RETRY:      # v1: 연결 직후 무응답 — 재시도 1회 쓰고 바로 다시
            st["retries"] -= 1
            self._issue_query(op, st, None)
            return
        if r.status == Status.OK:
            self._answer(op, st, r.value)
            return
        if r.status in (Status.CANCELLED, Status.REJECTED):
            self._answer(op, st, None)                  # v1: 폐기 → 콜백 None
            return
        if now >= st["deadline"]:
            self._answer(op, st, CALLER_TIMEOUT if st["item"].mark_timeout else None)
            return
        if r.status == Status.TIMEOUT:
            if st["retries"] <= 0:
                self._answer(op, st, None)              # v1: 재시도 소진 → 콜백 None
                return
            st["retries"] -= 1                          # v1: 큐 앞에 되돌림(재연결 뒤 다시 보냄)
        elif r.status == Status.FAIL and r.reason != "LINK_DOWN":
            self._answer(op, st, None)                  # 드라이버 오류(형식 등)
            return
        self._issue_query(op, st, now + LINK_RETRY_S)

    def _deadline_wake(self, op: _Op, st: dict) -> None:
        """v1 호출 쪽 wait_for 한도 — 그때까지 답이 없으면 None(큐에 남은 질의는 그대로 둔다)."""
        def fn(session):
            return None
        val = CALLER_TIMEOUT if st["item"].mark_timeout else None
        self.worker.submit(Request("write" if op.kind != "read" else "read", fn,
                                   on_done=lambda r: (self._answer(op, st, val)
                                                      if self.clock.monotonic() >= st["deadline"] else None),
                                   name=f"{op.name}:caller_deadline", not_before=st["deadline"], confirms_link=False))

    # ───────── 폐기 ─────────
    def purge(self, keep: Callable[[_Op], bool] = lambda o: False) -> List[str]:
        """v1 _purge_pending — 아직 안 보낸 명령은 '전송 전 폐기', 기다리던 질의는 None. 폐기한 명령 이름 목록."""
        tags = []
        for st in list(self.ghosts):
            st["cancel"] = True
            tags.append(st["item"].tag or st["item"].cmd)
        self.ghosts = []
        for op in list(self.ops):
            if op.done or keep(op):
                continue
            st = op.live
            if not st:
                continue
            item = st["item"]
            if isinstance(item, Send) and not st["cancel"]:
                st["cancel"] = True
                tags.append(item.tag or item.cmd)
                self._advance(op, ("fail", "전송 전에 대기열에서 폐기됨(또는 전송 실패)", "PURGED"))
            elif isinstance(item, Query) and not st["answered"]:
                tags.append(item.tag or item.cmd)
                self._answer(op, st, None)
        return tags

    def cancel(self, op: _Op, reason: str, detail: str) -> None:
        """op 를 그 자리에서 끝낸다(진행 중 항목은 버린다)."""
        if op.done:
            return
        if op.live:
            op.live["cancel"] = True
        self._finish(op, reason, detail)


# ───────────── 줄 입출력(v1 _tcp_reader_loop 와 같은 줄 나누기) ─────────────
LINE_MAX = 512


class LineReader:
    """CR 또는 LF 로 줄을 나눈다(CRLF·LFCR 은 한 구분자), 줄 512 B, ascii 디코드·strip, 빈 줄 버림
    (v1 device/mfc.py:1477-1540, device/ig.py:565-616)."""

    def __init__(self) -> None:
        self.buf = b""

    def clear(self) -> None:
        self.buf = b""

    def _split(self) -> Optional[str]:
        while True:
            i_cr, i_lf = self.buf.find(b"\r"), self.buf.find(b"\n")
            if i_cr == -1 and i_lf == -1:
                return None
            idx = i_cr if i_lf == -1 else (i_lf if i_cr == -1 else min(i_cr, i_lf))
            raw = self.buf[:idx][:LINE_MAX]
            drop = idx + 1
            if drop < len(self.buf):
                a, b = self.buf[idx], self.buf[idx + 1]
                if (a == 13 and b == 10) or (a == 10 and b == 13):
                    drop += 1
            self.buf = self.buf[drop:]
            line = raw.decode("ascii", "ignore").strip()
            if line:
                return line

    def read_line(self, session, deadline: float) -> str:
        """다음 빈 줄 아닌 줄. deadline 까지 없으면 ReadTimeout(받다 만 바이트 포함)."""
        while True:
            line = self._split()
            if line is not None:
                return line
            try:
                self.buf += session.read_until(size=1, deadline=deadline)
            except ReadTimeout as e:
                partial, self.buf = self.buf + e.partial, b""
                raise ReadTimeout(partial)


def echo_queue() -> "collections.deque":
    """v1 _skip_echos — no-reply 명령 에코를 1회 버리는 대기열(최대 64)."""
    return collections.deque(maxlen=64)
