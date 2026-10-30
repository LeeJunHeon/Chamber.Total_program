"""PLC 1초 묶음 읽기 한 tick(챔버12) — 블록 단위로 진행하는 상태(PollRun)와 나눠 읽기(start_split — SnapshotBus Source.start. 결정 후보: 1초 묶음은 PLC 를 통째로 잡지 않는다)
  v1 코일 로거·레지스터 스냅샷은 priority='low' 로, 블록마다 '기다리는 high 요청(외부 read/write 전부)이 없을 때까지' 양보한 뒤
  다음 블록을 읽었다(device/plc.py:1485-1494 _yield_to_priority, 1538-1541, 1733 · _io_lock 의 _priority_waiters 2257-2310).
  v2 worker 는 요청 하나가 끝나야 다음 요청을 꺼내므로, 묶음을 한 요청으로 읽으면 PLC 를 0.6~0.75 s 통째로 잡아 tick 앞부분에 온
  짧은 마감 요청(PC GV 인터락 0.6 s·게이트·메인밸브 확인 0.6 s·히터·로봇 명령)이 시간 초과했다.
  그래서 블록 하나 = worker 요청 하나(kind 'low' — worker 우선순위 맨 뒤, 마감 = 소스 마감). 다음 블록은 앞 블록이 끝난 뒤
  not_before = 직전 원시 연산 시작 + PLC_CMD_GAP_MS 로 넣는다 — 명령 간격 동안 worker 는 비어 있고, 기다리는 요청(safety·write·read)
  이 있으면 언제나 그것이 먼저 돈다(v1 양보와 같음). 끼어든 요청이 시간을 쓰면 마감 규칙대로 뒤 블록이 이번 tick 에서 빠진다
  (빈 칸 — 다음 블록이 마감 안에 못 들 것이 미리 보이면 기다리지 않고 바로 끝낸다). 첫 블록 뒤의 블록이 큐에서 마감을 넘긴
  것(EXPIRED)도 같은 '빈 칸'. 첫 블록이 시작도 못 하고 마감을 넘기면 결과 EXPIRED(묶기는 stale — 장비 실패 아님).
  취소(safety purge·worker 정지)·링크 끊김은 한 요청일 때와 같은 결과(CANCELLED·LINK_DOWN).
  묶음 결과(Result)의 started 는 첫 블록 시작, finished 는 마지막 블록 끝.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Tuple

from v2.common.worker import Request, Result, Status
from v2.ch12.drivers.plc_io import code_of

MORE = object()            # step: 이번 블록 성공, 다음 블록이 있음


class NoRoom(Exception):
    """나눠 읽기 첫 블록이 마감 안에 못 든다(worker 가 다른 요청으로 바빴음) — 묶음 결과는 EXPIRED(장비에 보내지 않음)."""


class PollRun:
    """한 tick 읽기 상태. drv = PlcDriver(_read_block·_coil_ranges·_note_plan·_demote·_fail_decoders·coil_log_backoff)."""

    def __init__(self, drv, split: bool = False) -> None:
        from v2.ch12.drivers import plc as P
        self.d = drv
        self.split = split                 # 나눠 읽기(첫 블록도 시작 전에 마감 규칙)
        self.no_room = False
        self.P = P
        self.pre: List[Tuple[str, int, int]] = [("regs",) + P.DCV_REGS]
        if drv.heater:
            self.pre += [("regs",) + P.HEATER_REGS, ("coils",) + P.HEATER_COILS]
        self.coil_steps: Optional[List[Tuple[int, int]]] = None
        self.i = 0                         # 다음 블록 번호(pre 다음 coil_steps)
        self.started = False
        self.finished = False              # 값을 만들었거나(해독기까지) 실패 처리를 끝냄
        self.t_wall = self.t_mono = 0.0
        self.skip_coils = False
        self.regs: Dict[int, int] = {}
        self.coils: Dict[int, bool] = {}
        self.err: Optional[BaseException] = None

    @property
    def in_pre(self) -> bool:
        return self.i < len(self.pre)

    def _begin(self) -> None:
        self.started = True
        self.t_wall, self.t_mono = self.d.clock.wall(), self.d.clock.monotonic()
        self.skip_coils = self.d.coil_log_backoff()      # v1 코일 로거처럼 이번 tick 시작 때 판단(device/plc.py:1976-1985)

    def _plan(self) -> None:
        d = self.d
        if self.skip_coils:
            d.plan_ok_streak = 0
            self.coil_steps = []
            return
        always, rot = d._coil_ranges()
        pick = [rot[(d._rot_i + k) % len(rot)] for k in range(min(self.P.ROTATE_PER_TICK, len(rot)))]
        d._rot_i = (d._rot_i + len(pick)) % max(1, len(rot))
        self.coil_steps = always + pick

    def _room(self, session) -> bool:
        """이 블록이 마감 안에 드는가 — 남은 시간 ≥ 직전 원시 연산 뒤 남은 명령 간격 + 응답 여유(READ_MARGIN_S)."""
        d = self.d
        now = d.clock.monotonic()
        io = d.io
        wait = 0.0 if io.last_io is None else max(0.0, io.last_io + io.gap_s - now)
        return session.deadline - now >= wait + self.P.READ_MARGIN_S

    def step(self, session) -> Any:
        """블록 하나. 다음 블록이 있으면 MORE, 끝나면 값 사전. 실패는 예외(한 요청 읽기와 같은 분류)."""
        d = self.d
        if not self.started:
            if self.split and not self._room(session):
                self.finished = self.no_room = True
                raise NoRoom("1초 읽기 첫 블록이 마감 안에 못 듦")
            self._begin()
        if self.in_pre:
            if self.i > 0 and not self._room(session):
                return self.finish()         # 히터 블록이 마감 안에 못 든다 — 이번 tick 히터·코일 몫 빈 칸(실패 아님)
            kind, st, n = self.pre[self.i]
            try:
                d._read_block(session, kind, st, n, self.regs if kind == "regs" else self.coils)
            except Exception as e:
                self.finished = True
                d._fail_decoders(f"{code_of(e)} {e}")
                raise
            self.i += 1
            if self.in_pre:
                return MORE
            self._plan()
            return MORE if self.coil_steps else self._finish_coils_ok()
        k = self.i - len(self.pre)
        if not self._room(session):
            return self._finish_coils_ok()   # 이번 tick 마감 안에 못 든다 — 남은 블록은 빈 칸(다음 tick)
        st, n = self.coil_steps[k]
        try:
            d._read_block(session, "coils", st, n, self.coils)
        except self.P.PlcModbusError as e:
            d._demote(str(e))
            self.err = e
            return self.finish()
        except Exception as e:
            d.plan_ok_streak = 0
            self.err = e
            return self.finish()
        self.i += 1
        if k + 1 < len(self.coil_steps):
            return MORE
        return self._finish_coils_ok()

    def _finish_coils_ok(self) -> Dict[str, Any]:
        if not self.skip_coils:
            self.d._note_plan(True)
        return self.finish()

    def finish(self) -> Dict[str, Any]:
        """값 사전을 만들고 last_poll·해독기. 코일 몫 실패가 있었으면 해독기 뒤 그 예외."""
        P, PM, d = self.P, self.P.PM, self.d
        self.finished = True
        vals: Dict[str, Any] = {f"plc.{PM.field_name(n)}": self.regs.get(a) for n, a in PM.REG_MAP.items()}
        if self.skip_coils:
            vals.update({f"plc.{PM.field_name(n)}": None for n, a in PM.COIL_MAP.items() if a < P.ROTATE_MIN_ADDR})
        else:
            for n, a in list(PM.COIL_MAP.items()) + list(PM.P_MIRROR_MAP.items()):
                if a in self.coils:
                    vals[f"plc.{PM.field_name(n)}"] = bool(self.coils[a])
                elif a < P.ROTATE_MIN_ADDR:
                    vals[f"plc.{PM.field_name(n)}"] = None          # 매 tick 필드는 못 읽었으면 빈 칸(묵은 값 아님)
        vals["plc.coil_plan"] = d.plan_idx
        poll = P.PlcPoll(self.t_wall, self.t_mono, self.coils, self.regs, vals)
        d.last_poll = poll
        for blocks_, decode, _on_fail in d.decoders:
            try:
                vals.update(decode(poll) or {})
            except Exception as e:                     # 해독 오류가 읽기를 깨지 않게
                d._emit("log", f"WARN 해독 오류 {blocks_}: {e!r}")
        if self.err is not None:
            raise self.err
        return vals

    def run_session(self, session) -> Dict[str, Any]:
        """한 세션 안에서 끝까지(source_read)."""
        while True:
            out = self.step(session)
            if out is not MORE:
                return out

    def no_room_at(self, start_at: float, deadline: float) -> bool:
        """첫 블록 뒤의 다음 블록이 start_at 에 시작해도 마감 안에 못 든다 — 요청을 넣지 않고 여기서 '빈 칸'으로 끝낼 때."""
        return (self.started and not self.finished and (self.i > 0 or not self.in_pre)
                and deadline - start_at < self.P.READ_MARGIN_S)

    def cut(self) -> Dict[str, Any]:
        """마감 때문에 남은 블록을 이번 tick 에서 뺀다 — 코일 몫이면 v1 처럼 성공으로 셈(계획 승격 연속), 히터 블록이면 그대로."""
        return self.finish() if self.in_pre else self._finish_coils_ok()

    def not_started(self, res: Result) -> Optional[Dict[str, Any]]:
        """나눠 읽기에서 블록 요청이 실행되지 못하고 끝남(EXPIRED·큐에서 취소·LINK_DOWN·거부) — 한 요청 읽기와 같게 마무리.
        첫 블록 뒤의 마감 지남(EXPIRED)은 '빈 칸'(값 사전 반환 → 성공). 첫 블록이 마감 지남이면 None(EXPIRED 그대로 —
        묶기는 '새 값 없음' stale), 그 밖은 None(그 결과 그대로 실패)."""
        d = self.d
        if self.finished:
            return None
        if res.status == Status.EXPIRED and self.started:
            return self.cut()
        if self.in_pre:
            self.finished = True
            if self.started:                       # 앞 블록을 읽다 만 tick — 해독기에 실패를 알림
                d._fail_decoders(f"{res.reason or res.status}")
            return None
        d.plan_ok_streak = 0
        try:
            self.finish()
        except Exception:
            pass
        return None


def start_split(drv, seq: int, deadline: float, on_done: Callable[[Result], None], name: str = "read:plc") -> PollRun:
    """나눠 읽기 시작 — 블록마다 worker 요청 하나. 끝나면 on_done(Result) 한 번(값 = 값 사전)."""
    run = PollRun(drv, split=True)
    w = drv.worker
    first: Dict[str, Optional[float]] = {"started": None}
    box = {"k": 0}

    def submit(not_before: Optional[float]) -> None:
        box["k"] += 1
        w.submit(Request("low", run.step, deadline=deadline, tick_seq=seq, on_done=done,
                         name=f"{name}#{box['k']}", not_before=not_before))

    def end(res: Result, status: str, value: Any = None, finished: Optional[float] = None) -> None:
        reason = "EXPIRED" if status == Status.EXPIRED else res.reason
        on_done(Result(res.request, status, value, None if status == Status.OK else reason,
                       "" if status == Status.OK else res.error,
                       first["started"] if first["started"] is not None else res.started,
                       res.finished if finished is None else finished))

    def done(res: Result) -> None:
        if first["started"] is None and res.started is not None and res.status not in (Status.EXPIRED,
                                                                                      Status.LINK_DOWN):
            first["started"] = res.started
        if res.status == Status.OK:
            first["last_ok"] = res.finished
        if res.status == Status.OK and res.value is MORE:
            if not w.running:                        # worker 가 멈추는 중 — 더 넣지 않는다(stop 이 큐를 비운 뒤일 수 있음)
                run.finished = True
                return end(res, Status.CANCELLED)
            io = drv.io
            now = drv.clock.monotonic()
            nb = None if io.last_io is None else io.last_io + io.gap_s
            if run.no_room_at(max(now, nb or now), deadline):
                return end(res, Status.OK, run.cut())     # 남은 블록은 이번 tick 빈 칸(기다리지 않고 바로)
            submit(nb if nb is not None and nb > now else None)
            return
        if res.status == Status.OK:
            return end(res, Status.OK, res.value)
        if run.no_room:
            return end(res, Status.EXPIRED)
        vals = run.not_started(res)
        if vals is not None:                         # 뒤 블록이 큐에서 마감을 넘김 — 값은 마지막으로 읽은 블록 끝 시각의 것
            return end(res, Status.OK, vals, finished=first.get("last_ok"))
        end(res, res.status)

    submit(None)
    return run
