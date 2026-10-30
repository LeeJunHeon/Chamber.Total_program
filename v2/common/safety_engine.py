"""안전 규칙 실행기 (장비 공통) — 조건·지속 시간·조치·해제·우선순위·정지 순서.

규칙(LiveRule)은 장비 쪽(chk)이 표(safety_rules.py)를 보고 만든다. 이 모듈은 장비를 모른다.

평가
- evaluate(where, t, view, ctx): where("plc250"·"tick"·이벤트 이름)가 같은 규칙만. 적용 안 되는 상태면 판정을 초기화.
  조건 값 None = 모름(stale·fail·suppressed) → judge.Persist 가 판정을 바꾸지 않는다(시간 기반, ok·late 값만).
- 성립하면 한 번 발동(활성 동안 다시 안 함 — 같은 알림 폭주 억제 S44), 조건이 거짓이 되면 해제(이벤트 1회).
- 같은 평가에서 여러 규칙: ALL STOP > 공정 중단(abort) > OFF > 알림 순서로 실행.
  run_end 사유 코드는 이번 공정에서 처음 걸린 중단 하나 — 나머지 중단은 이벤트(SAFETY_ALSO)로만.
- 정지 중(STOP·ALL STOP·중단 순서 실행 중)에는 skip_when_stopping 규칙(MV 닫힘·인터록 — S4)을 평가하지 않는다.

조치
- 규칙 조치는 handlers[act.kind](args, done) 로 바로 실행(드라이버가 safety 우선순위로 큐에 넣는다 — tick 을 기다리지 않음).
  조치 실패는 error. 단 bench(장비 쓰기 금지 — 장비 쪽이 bench=True 로 둔다)에서 사유가 BENCH_READ_ONLY 면 실패가 아니라
  bench_skip("bench — 보내지 않음(읽기 전용)") — bench_info_rules 의 규칙(종료 등)은 info, 그 밖은 warn(규칙이 실제 값으로
  발동했다는 사실은 fired 로 남는다).
- PROCESS_ABORT 는 엔진이 처리: 공정 쪽 on_abort(reason) 를 부르고 정지 순서를 실행.
- 정지 순서(StepRunner): 단계마다 결과를 이벤트로. 단계에서 예외·실패가 나면 must=False 단계는 건너뛰고,
  must=True 단계는 반드시 실행한다. 단계 한도(timeout)를 넘으면 기다리지 않고 다음 단계로.
  nowait 단계는 보내기만 하고(결과 "sent") 바로 다음 단계로 — 결과는 뒤의 AWAIT 단계(Act("AWAIT", {"no": n}))가
  그 단계 한도까지 기다려 받는다(v1 정지 순서: DC OFF·RF Pulse OFF 를 보내 놓고 RF 램프다운, 그 뒤 RF Pulse 완료 대기).
"""
from __future__ import annotations

import collections
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence

from v2.common.judge import Persist

# 우선순위(작을수록 먼저)
PRIO = {"all_stop": 0, "abort": 1, "off": 2, "notify": 3, "maintain": 4}
EVENTS_KEEP = 20000                # 최근 기록(길이 제한 링 버퍼)


@dataclass(frozen=True)
class Act:
    kind: str
    args: dict = field(default_factory=dict)


@dataclass
class SafetyEvent:
    kind: str                  # fired · released · blip · also · step · seq_done · error · bench_skip
    rule: str = ""
    code: str = ""
    detail: str = ""
    level: str = "info"
    t: float = 0.0


@dataclass
class LiveRule:
    id: str
    cls: str                                          # abort · off · notify · maintain
    where: str                                        # plc250 · tick · 이벤트 이름
    cond: Callable[[Any, Any], Optional[bool]]        # (view, ctx) → 참/거짓/모름(None)
    acts: Callable[[Any, Any], List[Act]]             # (view, ctx) → 조치(발동 때)
    hold_s: Callable[[Dict[str, Any]], float] = lambda cfg: 0.0
    states: Optional[frozenset] = None                # None = 모든 상태
    applies: Callable[[Any], bool] = lambda ctx: True # 상태 밖 적용 조건(장비 사용 여부 등)
    skip_when_stopping: bool = False
    on_release: Callable[[Any, Any], List[Act]] = lambda view, ctx: []
    blip_code: str = ""                               # 지속 시간 전에 풀리면 이 코드로 info 이벤트(S3)
    block: str = ""                                   # 표본 시각을 이 블록 값의 시각으로(view.t_of) — late 값은 자기 시각
    order: int = 0


@dataclass
class Step:
    no: int
    act: Act
    timeout_s: float = 5.0
    must: bool = True
    when: Callable[[], bool] = lambda: True
    desc: str = ""
    nowait: bool = False                              # 보내기만 하고 다음 단계로 — 결과는 AWAIT 단계가 받는다


class StepRunner:
    """정지 순서 실행 — 단계마다 handlers[act.kind](args, done) 를 부르고 done(err) 또는 한도까지 기다린다."""

    def __init__(self, clock, handlers: Dict[str, Callable], emit: Callable[[SafetyEvent], None]) -> None:
        self.clock, self.handlers, self.emit = clock, handlers, emit

    def _status(self, box: List[Optional[str]], got: bool) -> str:
        if not got:
            return "timeout"
        return "ok" if box[0] is None else f"error:{box[0]}"

    def run(self, name: str, steps: Sequence[Step]) -> List[tuple]:
        results: List[tuple] = []
        failed = False
        sent: Dict[int, List[Optional[str]]] = {}         # nowait 단계 번호 → 결과 상자
        for s in steps:
            if s.act.kind == "AWAIT":
                no = int(s.act.args.get("no", -1))
                box_ = sent.get(no)
                if box_ is None:
                    results.append((s.no, "AWAIT", "not_needed"))
                    self.emit(SafetyEvent("step", name, "AWAIT", f"{s.no}. {no}단계를 보내지 않음 — 건너뜀", "info",
                                          self.clock.wall()))
                    continue
                got = self.clock.wait_until(self.clock.monotonic() + s.timeout_s, lambda b=box_: bool(b))
                status = self._status(box_, got)
                if status != "ok":
                    failed = True
                results.append((s.no, "AWAIT", status))
                self.emit(SafetyEvent("step", name, "AWAIT", f"{s.no}. {s.desc or f'{no}단계 결과'} → {status}",
                                      "info" if status == "ok" else "error", self.clock.wall()))
                continue
            if failed and not s.must:
                results.append((s.no, s.act.kind, "skipped"))
                self.emit(SafetyEvent("step", name, s.act.kind, f"{s.no}. 건너뜀(앞 단계 실패, 필수 아님)", "warn",
                                      self.clock.wall()))
                continue
            try:
                if not s.when():
                    results.append((s.no, s.act.kind, "not_needed"))
                    self.emit(SafetyEvent("step", name, s.act.kind, f"{s.no}. 조건 아님 — 건너뜀", "info", self.clock.wall()))
                    continue
            except Exception as e:                       # 조건 계산 실패 — 안전 쪽으로 실행
                self.emit(SafetyEvent("error", name, s.act.kind, f"{s.no}. 조건 계산 실패 {e!r} — 실행", "warn",
                                      self.clock.wall()))
            box: List[Optional[str]] = []

            def done(err: Optional[str] = None, _box=box) -> None:
                with self.clock.cond:
                    if not _box:
                        _box.append(err)
                    self.clock.cond.notify_all()
            try:
                h = self.handlers[s.act.kind]
                h(dict(s.act.args), done)
                if s.nowait:
                    sent[s.no] = box
                    status = "sent"
                else:
                    got = self.clock.wait_until(self.clock.monotonic() + s.timeout_s, lambda: bool(box))
                    status = self._status(box, got)
            except Exception as e:
                status = f"error:{type(e).__name__}: {e}"
            if status not in ("ok", "sent"):
                failed = True
            results.append((s.no, s.act.kind, status))
            self.emit(SafetyEvent("step", name, s.act.kind, f"{s.no}. {s.desc or s.act.kind} → {status}",
                                  "info" if status in ("ok", "sent") else "error", self.clock.wall()))
        self.emit(SafetyEvent("seq_done", name, "", ", ".join(f"{n}:{st}" for n, _, st in results),
                              "info" if not failed else "warn", self.clock.wall()))
        return results


class SafetyEngine:
    def __init__(self, clock, rules: Iterable[LiveRule], cfg: Dict[str, Any], handlers: Dict[str, Callable],
                 process, on_event: Optional[Callable[[SafetyEvent], None]] = None) -> None:
        """process: on_abort(reason)·on_stop_done(kind)·state·active 를 가진 공정 쪽(ProcessEngine 또는 시험용)."""
        self.clock = clock
        self.cfg = cfg
        self.rules = list(rules)
        for i, r in enumerate(self.rules):
            r.order = i
        self.handlers = handlers
        self.process = process
        self.on_event = on_event
        self.events: "collections.deque[SafetyEvent]" = collections.deque(maxlen=EVENTS_KEEP)
        self._p: Dict[str, Persist] = {}
        self._fired: Dict[str, bool] = {}
        self._lock = threading.Lock()
        self.run_reason: Optional[str] = None      # 이번 공정의 run_end 사유(처음 걸린 중단)
        self.stopping: Optional[str] = None        # None · "stop" · "abort" · "all_stop"
        self.runner = StepRunner(clock, handlers, self._emit)
        self.last_fire_t: Dict[str, float] = {}
        self.seq_results: Dict[str, List[tuple]] = {}
        self._threads: "collections.deque[Any]" = collections.deque(maxlen=64)   # 최근 정지 순서 스레드
        self.bench = False                         # bench 모드(장비 쓰기 금지) — 장비 쪽이 정한다
        self.bench_info_rules: frozenset = frozenset()   # bench_skip 을 info 로 남길 규칙(나머지는 warn)

    # ───────── 이벤트 ─────────
    def _emit(self, e: SafetyEvent) -> None:
        self.events.append(e)
        if self.on_event:
            try:
                self.on_event(e)
            except Exception:
                pass

    def fired(self, rule_id: str) -> bool:
        return self._fired.get(rule_id, False)

    # ───────── 평가 ─────────
    def evaluate(self, where: str, t: float, view: Any, ctx: Any) -> List[str]:
        """where 규칙을 평가하고 발동한 규칙 id 를 우선순위 순으로 돌려준다(조치는 이 안에서 실행)."""
        fire: List[LiveRule] = []
        release: List[LiveRule] = []
        with self._lock:
            state = self.process.state
            for r in self.rules:
                if r.where != where:
                    continue
                p = self._p.get(r.id)
                if p is None:
                    p = self._p[r.id] = Persist(r.hold_s(self.cfg))
                ok_state = (r.states is None or state in r.states) and r.applies(ctx)
                if not ok_state or (r.skip_when_stopping and self.stopping):
                    p.reset()
                    self._fired[r.id] = False
                    continue
                try:
                    c = r.cond(view, ctx)
                except Exception as e:
                    self._emit(SafetyEvent("error", r.id, "", f"조건 계산 실패 {e!r}", "warn", t))
                    c = None
                was_since = p.since
                p.hold_s = r.hold_s(self.cfg)
                ts = view.t_of(r.block) if (r.block and hasattr(view, "t_of")) else t
                active = p.update(ts, c)
                if active and not self._fired.get(r.id):
                    self._fired[r.id] = True
                    fire.append(r)
                elif c is False and self._fired.get(r.id):
                    self._fired[r.id] = False
                    release.append(r)
                elif c is False and was_since is not None and not self._fired.get(r.id) and r.blip_code:
                    self._emit(SafetyEvent("blip", r.id, r.blip_code, f"{t - was_since:.2f} s 만에 풀림(기록만)", "info", t))
        fire.sort(key=lambda r: (PRIO[r.cls], r.order))
        for r in fire:
            self._fire(r, t, view, ctx)
        for r in release:
            self._emit(SafetyEvent("released", r.id, "", "", "info", t))
            for a in r.on_release(view, ctx):
                self._do(a, r.id, t)
        return [r.id for r in fire]

    def reset_rule(self, rule_id: str) -> None:
        with self._lock:
            if rule_id in self._p:
                self._p[rule_id].reset()
            self._fired[rule_id] = False

    # ───────── 조치 ─────────
    def _fire(self, r: LiveRule, t: float, view: Any, ctx: Any) -> None:
        acts = r.acts(view, ctx)
        self.last_fire_t[r.id] = self.clock.monotonic()
        aborts = [a for a in acts if a.kind == "PROCESS_ABORT"]
        others = [a for a in acts if a.kind != "PROCESS_ABORT"]
        code = aborts[0].args.get("reason", "") if aborts else next(
            (a.args.get("code", "") for a in acts if a.kind == "NOTIFY"), "")
        level = {"abort": "error", "off": "error", "notify": "warn", "maintain": "info"}[r.cls]
        self._emit(SafetyEvent("fired", r.id, code, ", ".join(a.kind for a in acts), level, t))
        for a in others:                       # OFF 류는 바로(safety 우선순위 — 드라이버가 큐 앞에)
            self._do(a, r.id, t)
        for a in aborts:
            self.abort(a.args.get("reason", r.id), rule=r.id, t=t)

    def _do(self, a: Act, rule: str, t: float) -> None:
        h = self.handlers.get(a.kind)
        if h is None:
            self._emit(SafetyEvent("error", rule, a.kind, "조치 처리기 없음", "error", t))
            return
        try:
            h(dict(a.args), lambda err=None: None if err is None else self._failed(rule, a.kind, err))
        except Exception as e:
            self._emit(SafetyEvent("error", rule, a.kind, f"조치 예외 {e!r}", "error", t))

    def _failed(self, rule: str, kind: str, err: str) -> None:
        if self.bench and err == "BENCH_READ_ONLY":
            self._emit(SafetyEvent("bench_skip", rule, kind, "bench — 보내지 않음(읽기 전용)",
                                   "info" if rule in self.bench_info_rules else "warn", self.clock.wall()))
            return
        self._emit(SafetyEvent("error", rule, kind, f"조치 실패: {err}", "error", self.clock.wall()))

    def abort(self, reason: str, rule: str = "", t: Optional[float] = None) -> bool:
        """공정 중단 — 공정이 진행 중이고 정지 중이 아니면 첫 사유로 기록하고 정지 순서. 아니면 이벤트(SAFETY_ALSO)만."""
        t = self.clock.wall() if t is None else t
        with self._lock:
            first = self.process.active and self.stopping is None
            if first:
                self.stopping = "abort"
                if self.run_reason is None:
                    self.run_reason = reason
        if not first:
            self._emit(SafetyEvent("also", rule, reason, f"이미 정지 중({self.stopping}) 또는 공정 없음 — 이벤트만", "warn", t))
            late = getattr(self.process, "late_stop", None)
            if late is not None:                       # 정지 순서 중의 중단 — 리스트를 끝내도록 공정 쪽에 알린다
                late("error", reason)
            return False
        self._emit(SafetyEvent("abort", rule, reason, "공정 중단", "error", t))
        if not getattr(self.process, "needs_stop_sequence", True):
            # 켜진 장비가 없는 상태(딜레이 행 등) — 정지 순서 없이 공정 쪽이 바로 정리한다
            with self._lock:
                self.stopping = None
            self.process.on_abort(reason)
            return True
        self.process.on_abort(reason)
        self._start_seq("abort", self.stop_steps())
        return True

    def end_row(self, skip=()) -> bool:
        """레시피 행 정상 끝(정상 끝 정지 블록) — 같은 정지 순서(가스·전원 정리)를 '정상 끝'으로 실행
        (v1: 모든 스텝 완료 → stop_process). skip = 이 블록에서 끈 단계 번호."""
        with self._lock:
            if not self.process.active or self.stopping is not None:
                return False
            self.stopping = "row_end"
        self._start_seq("row_end", self.stop_steps(skip) if skip else self.stop_steps())
        return True

    def run_seq(self, name: str, steps: List[Step]) -> None:
        """공정 쪽이 이어서 내는 정지 순서(정지 중 표시를 이 이름으로 — 그동안 STOP·중단은 late_stop 으로 간다)."""
        with self._lock:
            if self.stopping is None:
                self.stopping = name
        self._start_seq(name, steps)

    def stop(self) -> bool:
        with self._lock:
            if not self.process.active or self.stopping is not None:
                return False
            self.stopping = "stop"
        on_stop = getattr(self.process, "on_stop", None)
        if on_stop:
            on_stop()
        self._start_seq("stop", self.stop_steps())
        return True

    def all_stop(self) -> None:
        """ALL STOP — 공정 여부와 무관. 진행 중인 정지 순서가 있어도 따로 실행(각 단계 독립)."""
        with self._lock:
            prev = self.stopping
            self.stopping = "all_stop"
            if self.process.active and self.run_reason is None:
                self.run_reason = "ALL_STOP"
        self._emit(SafetyEvent("fired", "ALL_STOP", "ALL_STOP", f"이전 정지 상태 {prev}", "error", self.clock.wall()))
        self._start_seq("all_stop", self.all_stop_steps())

    # 장비 쪽이 덮어쓴다
    def stop_steps(self, skip=()) -> List[Step]:
        return []

    def all_stop_steps(self) -> List[Step]:
        return []

    def _start_seq(self, name: str, steps: List[Step]) -> None:
        self._emit(SafetyEvent("seq_start", name, "", f"{len(steps)}단계", "info", self.clock.wall()))

        def run():
            res = self.runner.run(name, steps)
            self.seq_results[name] = res
            with self._lock:
                if self.stopping == name:
                    self.stopping = None
            try:
                self.process.on_stop_done(name)
            except Exception as e:
                self._emit(SafetyEvent("error", name, "", f"on_stop_done 예외 {e!r}", "error", self.clock.wall()))
            finally:
                self.clock.notify()           # 순서가 끝났음을 기다리는 쪽을 깨운다
        self._threads.append(self.clock.start_thread(run, f"safety-{name}"))

    def new_run(self) -> None:
        """공정 시작 때 — 사유 초기화."""
        with self._lock:
            self.run_reason = None

    @property
    def busy(self) -> bool:
        return self.stopping is not None
