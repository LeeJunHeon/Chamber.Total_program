"""챔버12 웹 연결 보관함·요약 — 챔버K v2/chk/uplink.py 의 Spool·Summary·priority 그대로(파일 크기 때문에 uplink.py 에서 나눔).

보관함(LOCAL_DATA_DIR/uplink): state 를 뺀 모든 메시지를 받자마자 디스크 조각 파일에 — acked 된 id 만 지운다(끊겨도·다시 켜도
잃지 않는다). 상한 UPLINK_SPOOL_MAX_MB — 차면 LOW(info event·snapshot_summary)부터 오래된 것을 버린다.
snapshot_summary: UPLINK_SUMMARY_PERIOD_SEC 정각 구간 요약 — 공정 중 구간만(구간 안 행에 run id 가 하나라도 있을 때).
챔버12 는 공정 id 가 챔버마다(<c>.proc.run_id) — 구간 안 어느 챔버든 run id 가 있으면 내고 runId 는 마지막으로 본 것
(runIds = 챔버별 마지막 run id 를 더 싣는다 — 챔버K 웹은 runId 만 읽는다).
"""
from __future__ import annotations

import collections
import heapq
import json
import math
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from v2.common.messages import dumps_json
from v2.ch12.hub import utc_iso

SEG_BYTES = 256 * 1024                 # 보관함 조각 파일 크기
HIGH, LOW = 1, 0


def priority(env: dict) -> int:
    """보관함이 차면 LOW(info event·snapshot_summary)부터 버린다 — run_*·cmd_result·config_ack·warn/error event 는 마지막까지."""
    t = env.get("type")
    if t == "snapshot_summary":
        return LOW
    if t == "event" and (env.get("data") or {}).get("level") not in ("warn", "error"):
        return LOW
    return HIGH


# ───────────── 보관함 ─────────────
class Spool:
    """디스크 보관함 — 조각 파일(seg-N.jsonl)에 한 줄씩 덧붙이고, 지운(acked·버린) id 는 그 조각의 seg-N.ack 에 적는다
    (다시 켰을 때 지운 것이 되살아나지 않게). 조각의 메시지가 모두 지워지면 두 파일을 지운다(평소: 1 초 안에 acked →
    조각이 쌓이지 않음).
    디스크 크기가 상한의 두 배를 넘으면(조각마다 몇 개씩 남은 경우) 남은 것만 새 조각으로 다시 쓴다(순서 유지).
    다시 켜면 남은 조각을 읽어 이어 올린다(지난 실행 것 포함 — envelope 의 instance 로 구분)."""

    def __init__(self, directory: Path, max_bytes: int) -> None:
        self.dir = Path(directory)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.max_bytes = int(max_bytes)
        self._lock = threading.Lock()
        self._n = 0
        self._q: Dict[int, "collections.OrderedDict[str, list]"] = {HIGH: collections.OrderedDict(),
                                                                    LOW: collections.OrderedDict()}
        self._seg_live: Dict[int, int] = {}
        self._seg_size: Dict[int, int] = {}
        self.live_bytes = 0
        self.dropped = 0                       # 상한으로 버린 수(누적)
        self.dropped_by_type: Dict[str, int] = collections.Counter()
        self._cur: Optional[int] = None
        self._fh = None
        self._next_seg = 1
        self._tombs: Dict[int, List[str]] = {}
        self.loaded = self._load()

    # ── 파일 ──
    def _path(self, seg: int) -> Path:
        return self.dir / f"seg-{seg:08d}.jsonl"

    def _ack_path(self, seg: int) -> Path:
        return self.dir / f"seg-{seg:08d}.ack"

    def _load(self) -> int:
        n = 0
        segs = sorted(int(p.stem[4:]) for p in self.dir.glob("seg-*.jsonl") if p.stem[4:].isdigit())
        for seg in segs:
            data = self._path(seg).read_bytes()
            ap = self._ack_path(seg)
            gone = set(ap.read_text(encoding="utf-8").split()) if ap.exists() else set()
            self._seg_size[seg] = len(data)
            self._seg_live[seg] = 0
            off = 0
            for line in data.split(b"\n"):
                ln = len(line) + 1
                if line.strip():
                    try:
                        env = json.loads(line)
                        eid = env["id"]
                    except (ValueError, KeyError, TypeError):
                        env = None                       # 끝이 잘린 줄(쓰는 중에 꺼짐) — 버림
                    if env is not None and eid not in gone and not self._has(eid):
                        self._add(eid, seg, off, len(line), priority(env), env.get("type", "?"))
                        n += 1
                off += ln
            if self._seg_live[seg] == 0:
                self._unlink(seg)
        for ap in self.dir.glob("seg-*.ack"):                     # 조각 없이 남은 ack 파일
            if not ap.with_suffix(".jsonl").exists():
                try:
                    ap.unlink()
                except OSError:
                    pass
        self._next_seg = (segs[-1] + 1) if segs else 1
        return n

    def _has(self, eid: str) -> bool:
        return eid in self._q[HIGH] or eid in self._q[LOW]

    def _add(self, eid: str, seg: int, off: int, length: int, prio: int, type_: str) -> None:
        self._n += 1
        self._q[prio][eid] = [self._n, seg, off, length, type_, prio]
        self._seg_live[seg] = self._seg_live.get(seg, 0) + 1
        self.live_bytes += length

    def _unlink(self, seg: int) -> None:
        if seg == self._cur:
            self._fh.close()
            self._fh, self._cur = None, None
        for f in (self._path(seg), self._ack_path(seg)):
            try:
                f.unlink()
            except OSError:
                pass
        self._seg_live.pop(seg, None)
        self._seg_size.pop(seg, None)
        self._tombs.pop(seg, None)

    def _remove(self, eid: str) -> bool:
        for q in self._q.values():
            e = q.pop(eid, None)
            if e is not None:
                seg = e[1]
                self.live_bytes -= e[3]
                self._seg_live[seg] -= 1
                if self._seg_live[seg] == 0:
                    self._unlink(seg)
                else:
                    self._tombs.setdefault(seg, []).append(eid)
                return True
        return False

    def _flush_tombs(self) -> None:
        for seg, ids in self._tombs.items():
            try:
                with open(self._ack_path(seg), "a", encoding="utf-8") as f:
                    f.write("\n".join(ids) + "\n")
            except OSError:
                pass                                       # 다시 켜면 다시 올라갈 뿐(웹은 id 로 한 번 저장)
        self._tombs = {}

    def _append(self, line: bytes) -> Tuple[int, int]:
        if self._cur is None or self._seg_size[self._cur] >= SEG_BYTES:
            if self._fh is not None:
                self._fh.close()
            self._cur, self._next_seg = self._next_seg, self._next_seg + 1
            self._fh = open(self._path(self._cur), "ab")
            self._seg_size[self._cur] = 0
            self._seg_live.setdefault(self._cur, 0)
        off = self._seg_size[self._cur]
        self._fh.write(line + b"\n")
        self._fh.flush()
        self._seg_size[self._cur] += len(line) + 1
        return self._cur, off

    def _read(self, entries: List[list]) -> List[bytes]:
        out: List[bytes] = []
        fhs: Dict[int, Any] = {}
        try:
            for e in entries:
                seg = e[1]
                if seg == self._cur:
                    self._fh.flush()
                f = fhs.get(seg)
                if f is None:
                    f = fhs[seg] = open(self._path(seg), "rb")
                f.seek(e[2])
                out.append(f.read(e[3]))
        finally:
            for f in fhs.values():
                f.close()
        return out

    def _ordered(self):
        return heapq.merge(((e[0], k, e) for k, e in self._q[HIGH].items()),
                           ((e[0], k, e) for k, e in self._q[LOW].items()))

    def _compact(self) -> None:
        items = [(k, e) for _, k, e in self._ordered()]
        lines = self._read([e for _, e in items])
        old = list(self._seg_size)
        if self._fh is not None:
            self._fh.close()
        self._fh, self._cur = None, None
        for q in self._q.values():
            q.clear()
        self._seg_live, self._seg_size, self.live_bytes = {}, {}, 0
        for (k, e), line in zip(items, lines):
            seg, off = self._append(line)
            self._add(k, seg, off, len(line), e[5], e[4])
        self._tombs = {}
        for seg in old:
            for f in (self._path(seg), self._ack_path(seg)):
                try:
                    f.unlink()
                except OSError:
                    pass

    # ── 쓰기·읽기 ──
    def put(self, env: dict) -> int:
        """넣기 — 상한을 넘으면 낮은 것부터(같으면 오래된 것부터) 버린다. 버린 수를 돌려준다."""
        line = dumps_json(env)
        prio = priority(env)
        dropped = 0
        with self._lock:
            if len(line) + 1 > self.max_bytes:
                self.dropped += 1
                self.dropped_by_type[env.get("type", "?")] += 1
                return 1
            while self.live_bytes + len(line) > self.max_bytes:
                q = self._q[LOW] if self._q[LOW] else self._q[HIGH]
                if not q:
                    break
                if q is self._q[HIGH] and prio == LOW:
                    self.dropped += 1                          # 넣으려는 것이 더 낮다 — 이것을 버린다
                    self.dropped_by_type[env.get("type", "?")] += 1
                    return dropped + 1
                eid, e = next(iter(q.items()))
                self._remove(eid)
                self.dropped += 1
                self.dropped_by_type[e[4]] += 1
                dropped += 1
            seg, off = self._append(line)
            self._add(env["id"], seg, off, len(line), prio, env.get("type", "?"))
            if sum(self._seg_size.values()) > 2 * self.max_bytes + SEG_BYTES:
                self._compact()
            self._flush_tombs()
        return dropped

    def batch(self, max_count: int, max_bytes: int) -> List[Tuple[str, bytes]]:
        """오래된 것부터 (id, 줄) — 개수·바이트 상한(첫 하나는 상한보다 커도 넣는다 — 413 이면 나눠서)."""
        with self._lock:
            picked, size = [], 0
            for _, k, e in self._ordered():
                if len(picked) >= max_count or (picked and size + e[3] > max_bytes):
                    break
                picked.append((k, e))
                size += e[3] + 1
            lines = self._read([e for _, e in picked])
        return [(k, ln) for (k, _), ln in zip(picked, lines)]

    def ack(self, ids) -> int:
        with self._lock:
            n = sum(1 for i in ids if isinstance(i, str) and self._remove(i))
            self._flush_tombs()
            return n

    def drop(self, eid: str) -> None:
        with self._lock:
            if self._remove(eid):
                self.dropped += 1
            self._flush_tombs()

    @property
    def count(self) -> int:
        return len(self._q[HIGH]) + len(self._q[LOW])

    def disk_bytes(self) -> int:
        with self._lock:
            return sum(self._seg_size.values())

    def files(self) -> int:
        return len(list(self.dir.glob("seg-*")))

    def close(self) -> None:
        with self._lock:
            if self._fh is not None:
                self._fh.close()
                self._fh, self._cur = None, None


# ───────────── snapshot_summary ─────────────
class Summary:
    """정각에 맞춘 구간(UPLINK_SUMMARY_PERIOD_SEC)마다 숫자 열의 last·mean·min·max, 장비별 ok 비율·최대 나이, 행 수.
    공정 중에만 — 구간 안 행에 run id(챔버12: <c>.proc.run_id, 챔버K 모양 proc.run_id 도)가 하나라도 있을 때만 낸다
    (runId = 구간의 마지막 run id, 챔버K 사용자 결정 2026-10-22 — runIds = 챔버별 마지막 run id)."""

    RUN_ID_COLS = (("ch1", "ch1.proc.run_id"), ("ch2", "ch2.proc.run_id"), ("pc", "pc.proc.run_id"),
                   ("tsp", "tsp.proc.run_id"), ("", "proc.run_id"))

    SKIP = ("seq",)

    def __init__(self) -> None:
        self.start: Optional[float] = None
        self.period = 60
        self._reset()

    def _reset(self) -> None:
        self.rows = 0
        self.run_id: Optional[str] = None
        self.run_ids: Dict[str, str] = {}
        self.vals: Dict[str, list] = {}           # 열 → [last, sum, n, min, max]
        self.q: Dict[str, list] = {}              # 장비 → [ok, n, max_age]

    def add(self, t_wall: float, row: Dict[str, Any], period: int) -> Optional[dict]:
        start = math.floor(t_wall / period) * period
        out = None
        if self.start is not None and (start != self.start or period != self.period):
            out = self.finish()
        if self.start is None or out is not None:
            self.start, self.period = start, period
        self.rows += 1
        for c, col in self.RUN_ID_COLS:
            rid = row.get(col)
            if rid:
                self.run_id = str(rid)
                if c:
                    self.run_ids[c] = str(rid)
        for k, v in row.items():
            if k.endswith(".q"):
                dev = k[:-2]
                qq = self.q.setdefault(dev, [0, 0, None])
                qq[0] += v == "ok"
                qq[1] += 1
                age = row.get(f"{dev}.age_s")
                if isinstance(age, (int, float)) and not isinstance(age, bool):
                    qq[2] = age if qq[2] is None else max(qq[2], age)
                continue
            if k in self.SKIP or k.endswith(".age_s") or isinstance(v, bool) or not isinstance(v, (int, float)):
                continue
            if isinstance(v, float) and not math.isfinite(v):
                continue
            a = self.vals.get(k)
            if a is None:
                self.vals[k] = [v, v, 1, v, v]
            else:
                a[0], a[1], a[2] = v, a[1] + v, a[2] + 1
                a[3], a[4] = min(a[3], v), max(a[4], v)
        return out

    def finish(self) -> Optional[dict]:
        if self.start is None or not self.rows or self.run_id is None:
            self.start = None
            self._reset()
            return None
        r = lambda x: round(x, 6) if isinstance(x, float) else x
        d = {"runId": self.run_id, "runIds": dict(self.run_ids), "from": utc_iso(self.start), "to": utc_iso(self.start + self.period), "rows": self.rows,
             "values": {k: {"last": r(a[0]), "mean": r(a[1] / a[2]), "min": r(a[3]), "max": r(a[4])}
                        for k, a in self.vals.items()},
             "quality": {k: {"okRatio": round(q[0] / q[1], 4) if q[1] else None, "maxAge": q[2]}
                         for k, q in self.q.items()}}
        self.start = None
        self._reset()
        return d
