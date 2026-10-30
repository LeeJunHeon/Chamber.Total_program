"""스냅샷 묶기 (장비 공통) — tick 마다 모든 worker 에 읽기를 보내고 마감까지 모아 1행을 만든다.

한 tick(정각 t)
1) begin: 켜진 소스마다 read(t) 요청(마감 = 보낸 시각 + 소스 마감 ms). 앞 tick 요청이 아직 진행 중이면 보내지 않는다.
2) close: t + TICK_DEADLINE 까지(또는 진행 중인 요청이 하나도 없으면 바로) 기다린 뒤 행을 닫는다. 행 시각은 t.
   그때 마감이 지난 요청의 결과는 넣고(무응답이면 그 행은 fail), 마감이 남은 요청은 다음 행으로 넘긴다.

품질(장비 단위 <블록>.q, 값마다 own_quality 필드는 <블록>.<필드>.q)
- off        : 소스가 꺼짐(설정·worker 없음) — 값 빈 칸
- ok         : 이번 tick 요청이 마감 안에 성공
- suppressed : worker 가 억제 구간(예정된 무응답)이고 이번 tick 에 새 값 없음, 또는 안전 요청(STOP·OFF)이 이 tick 읽기를 취소함
- fail       : 이번 tick 요청이 실패(무응답·형식 오류·링크 끊김)
- late       : 이번 행에 쓰는 값이 앞 tick 요청의 '마감 뒤 도착'한 값(그 값이 처음 쓰이는 행)
- stale      : 새 값 없음 — 마지막 값과 나이
나이(age_s) = 행 시각 − 그 값을 만든 요청의 tick 시각. ok 는 0.
번갈아 읽는 필드: 소스가 이번 tick 에 돌려주지 않은 own_quality 필드는 stale(자기 나이), 장비 품질은 요청 기준.
"""
from __future__ import annotations

import collections
import datetime as _dt
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional, Sequence

from v2.common.clock import Tick, TickScheduler
from v2.common.snapshot import DeviceBlock, FieldDef, Quality, row_columns
from v2.common.worker import Request, Result, Status, Worker

STATS_KEEP = 21600                 # tick 통계(jitter·행 닫기 지연) 최근 6시간분 — 링 버퍼


@dataclass
class Source:
    name: str
    worker: Optional[Worker]
    read: Callable[[Any, int], Dict[str, Any]]    # (session, tick_seq) → {"블록.필드": 값}
    blocks: tuple                                 # 이 소스가 채우는 블록 이름(품질 공유)
    deadline_ms: int = 900
    enabled: bool = True


@dataclass
class _SrcState:
    inflight_seq: Optional[int] = None
    inflight_deadline: Optional[float] = None
    sent_this_tick: bool = False
    result: Optional[Result] = None               # 이번 tick 요청 결과(마감 안 도착)
    fields: Dict[str, tuple] = field(default_factory=dict)   # 열 → (값, 요청 tick 시각, 늦게 도착 & 아직 안 쓰임)
    newest_t: Optional[float] = None
    newest_late_unused: bool = False
    late_fail: bool = False          # 행이 닫힌 뒤 실패로 끝난 요청 — 다음 행에 fail


def iso_local(t_wall: float) -> str:
    return _dt.datetime.fromtimestamp(t_wall).isoformat(timespec="seconds")


class SnapshotBus:
    # 행 닫힘 이전이 마감인 요청의 결과를 기다리는 여유. 설정으로 두지 않는 이유: 장비·공정과 무관한
    # 스레드 전달 지연(수 ms) 흡수용이고, 키우면 행 닫힘이 늦어져 다음 tick 에 닿을 수 있다.
    SETTLE_S = 0.05

    def __init__(self, clock, blocks: Sequence[DeviceBlock], sources: Sequence[Source],
                 tick_deadline_ms: int = 900, extra_fields: Sequence[FieldDef] = ()) -> None:
        self.clock = clock
        self.blocks = tuple(blocks)
        self.sources = tuple(sources)
        self.tick_deadline_ms = tick_deadline_ms
        self.extra = tuple(extra_fields)
        self.columns = row_columns(self.blocks, self.extra)
        self._block_src = {}
        for s in self.sources:
            for b in s.blocks:
                if b in self._block_src:
                    raise ValueError(f"블록 {b} 를 두 소스가 채움")
                self._block_src[b] = s.name
        missing = [b.name for b in self.blocks if b.name not in self._block_src]
        if missing:
            raise ValueError(f"소스 없는 블록: {missing}")
        self._st = {s.name: _SrcState() for s in self.sources}
        self._open_tick: Optional[Tick] = None

    # ───────── tick ─────────
    def begin(self, tick: Tick) -> None:
        with self.clock.cond:
            self._open_tick = tick
            for s in self.sources:
                st = self._st[s.name]
                st.sent_this_tick, st.result = False, None
        for s in self.sources:
            st = self._st[s.name]
            if not s.enabled or s.worker is None or st.inflight_seq is not None:
                continue
            with self.clock.cond:
                st.inflight_seq, st.sent_this_tick = tick.seq, True
            # 소스 마감 = 보낸 시각 + 소스 마감 — tick 이 늦게 시작해도(jitter) 읽기 시간을 빼앗지 않는다.
            dl = self.clock.monotonic() + s.deadline_ms / 1000.0
            with self.clock.cond:
                st.inflight_deadline = dl
            req = Request("read", (lambda sess, s=s, n=tick.seq: s.read(sess, n)),
                          deadline=dl, tick_seq=tick.seq,
                          on_done=self._done_cb(s.name, tick), name=f"read:{s.name}")
            s.worker.submit(req)

    def _done_cb(self, name: str, tick: Tick) -> Callable[[Result], None]:
        def cb(res: Result) -> None:
            with self.clock.cond:
                st = self._st[name]
                st.inflight_seq = None
                on_time = (self._open_tick is not None and self._open_tick.seq == tick.seq
                           and res.finished is not None
                           and res.finished <= tick.t_mono + self.tick_deadline_ms / 1000.0)
                if res.status == Status.OK and isinstance(res.value, dict):
                    for k, v in res.value.items():
                        st.fields[k] = (v, tick.t_wall, not on_time)
                    if st.newest_t is None or tick.t_wall >= st.newest_t:
                        st.newest_t = tick.t_wall
                        st.newest_late_unused = not on_time
                if on_time:
                    st.result = res
                elif res.status not in (Status.OK, Status.CANCELLED):
                    st.late_fail = True
                self.clock.cond.notify_all()
        return cb

    def _all_back(self) -> bool:
        # 이번 tick 요청뿐 아니라 앞 tick 의 늦은 요청까지 모두 돌아왔을 때만 마감 전에 닫는다
        return all(st.inflight_seq is None for st in self._st.values())

    def close(self, tick: Tick, extra_values: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        close_t = tick.t_mono + self.tick_deadline_ms / 1000.0
        if not self.clock.wait_until(close_t, self._all_back):
            # 행 닫힘 이전이 마감인 요청은 결과가 반드시 온다 — 그 결과까지 넣고 닫는다(같은 순간 경쟁 없음).
            # 마감이 더 늦은 요청은 기다리지 않는다 — 도착하면 다음 행에 late.
            self.clock.wait_until(close_t + self.SETTLE_S, lambda: all(
                st.inflight_seq is None or (st.inflight_deadline or 0) > close_t for st in self._st.values()))
        with self.clock.cond:
            row = self._build(tick, extra_values or {})
            self._open_tick = None
        return row

    # ───────── 행 만들기 ─────────
    def _build(self, tick: Tick, extra: Dict[str, Any]) -> Dict[str, Any]:
        row: Dict[str, Any] = {c: None for c in self.columns}
        row.update({"ts": iso_local(tick.t_wall), "seq": tick.seq, "jitter_ms": int(round(tick.jitter_ms)),
                    "skipped": tick.skipped})
        for k, v in extra.items():
            if k in row:
                row[k] = v
        now = self.clock.monotonic()
        src_by = {s.name: s for s in self.sources}
        dev_q: Dict[str, tuple] = {}
        for s in self.sources:
            st = self._st[s.name]
            age = None if st.newest_t is None else round(tick.t_wall - st.newest_t, 3)
            if not s.enabled or s.worker is None:
                q = Quality.OFF
            elif st.result is not None and st.result.status == Status.OK:
                q = Quality.OK
            elif s.worker.suppressed(now) or (st.result is not None and st.result.status == Status.CANCELLED):
                q = Quality.SUPPRESSED            # 예정된 무응답, 또는 안전 요청이 앞질러 취소한 읽기(장비 실패 아님)
            elif (st.result is not None or st.late_fail
                  or (not s.worker.connected and not st.sent_this_tick)):
                q = Quality.FAIL
            elif st.newest_late_unused:
                q = Quality.LATE
            else:
                q = Quality.STALE
            dev_q[s.name] = (q, age)
        for b in self.blocks:
            s = src_by[self._block_src[b.name]]
            st = self._st[s.name]
            q, age = dev_q[s.name]
            row[f"{b.name}.q"] = q.value
            row[f"{b.name}.age_s"] = None if q is Quality.OFF else age
            fresh_keys = set(st.result.value) if (st.result is not None and st.result.status == Status.OK
                                                  and isinstance(st.result.value, dict)) else set()
            for f in b.fields:
                col = f"{b.name}.{f.name}"
                if q is Quality.OFF or col in extra:
                    continue                  # core 가 채운 열(예: heater.hold)은 장비 값으로 덮지 않는다
                val = st.fields.get(col)
                row[col] = None if val is None else val[0]
                if f.own_quality:
                    fage = None if val is None else round(tick.t_wall - val[1], 3)
                    if col in fresh_keys:
                        fq = Quality.OK
                    elif q in (Quality.FAIL, Quality.SUPPRESSED):
                        fq = q
                    elif val is not None and val[2]:
                        fq = Quality.LATE
                    else:
                        fq = Quality.STALE
                    row[f"{col}.q"] = fq.value
                    row[f"{col}.age_s"] = fage
        # 늦게 도착한 값은 처음 쓰인 이 행에서만 late — 다음 행부터 stale
        for st in self._st.values():
            st.newest_late_unused = False
            st.late_fail = False
            st.fields = {k: (v[0], v[1], False) for k, v in st.fields.items()}
        return row


class TickLoop:
    """시계 → tick → 묶기 → 받는 곳(sink). 놓친 tick·jitter 를 모은다."""

    def __init__(self, clock, bus: SnapshotBus, sinks: Sequence[Callable[[Dict[str, Any]], None]] = (),
                 extra_fn: Optional[Callable[[Tick], Dict[str, Any]]] = None, period: float = 1.0,
                 tick_hooks: Sequence[Callable[[Tick], None]] = ()) -> None:
        self.clock = clock
        self.bus = bus
        self.sinks = list(sinks)
        self.tick_hooks = list(tick_hooks)      # 읽기를 보낸 직후 tick 으로 부른다(하트비트·CURR 쓰기 예약 등)
        self.extra_fn = extra_fn
        self.scheduler = TickScheduler(clock, period)
        self.jitters: "collections.deque[float]" = collections.deque(maxlen=STATS_KEEP)
        self.close_lags: "collections.deque[float]" = collections.deque(maxlen=STATS_KEEP)   # 행을 닫은 시각 − tick 시각(ms)
        self.missed = 0
        self.rows = 0
        self._stop = False

    def stop(self) -> None:
        self._stop = True

    def run(self, count: Optional[int] = None) -> None:
        n = 0
        while not self._stop and (count is None or n < count):
            t = self.scheduler.next_tick()
            self.jitters.append(t.jitter_ms)
            self.missed += t.skipped
            self.bus.begin(t)
            for h in self.tick_hooks:
                h(t)
            row = self.bus.close(t, self.extra_fn(t) if self.extra_fn else None)
            self.close_lags.append((self.clock.monotonic() - t.t_mono) * 1000.0)
            for s in self.sinks:
                s(row)
            self.rows += 1
            n += 1
