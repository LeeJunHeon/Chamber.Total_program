"""챔버12 장비 메시지(MessageHub, 사용자 지시 3장 (7)) — 내부 이벤트를 messages.py 형식(envelope id·instance·seq·ts(UTC)·
type·data)으로 바꾸는 곳 하나. 챔버K v2/chk/hub.py 를 챔버12 용으로 옮긴 것(envelope·seq·instance·type·소비자 규칙 그대로).
instance = 이 실행의 id(프로그램을 켤 때마다 새로) — seq 는 실행 안에서만 이어진다.

조립(Ch12System)에 붙는 법 — system.py 를 고치지 않고 attach(system) 가 꽂는다:
  system.event_listeners 에 on_logged(내부 이벤트 Logged — 드라이버·worker·엔진·안전·히터 모두 같은 길),
  system.row_listeners 에 on_row(1초 행이 닫힐 때 — 기록·안전·엔진·v1 기록 출력 뒤), system.hub = 이 허브.

type
- run_start · run_end(챔버별 — data.chamber) · cmd_result · config_ack · hello 는 그 type 으로.
- state 는 1초 행마다 하나(최신 1개만 보관 — 링 버퍼에 넣지 않음).
- 나머지 이벤트는 event {level, code, ruleId, message, context}.
  context = {source(process·safety·heater·uplink·장비 이름 — mfc·ig·plc·dcpulse …), chamber(ch1·ch2·pc·tsp·shared),
             kind, data, origin(조립의 원래 출처 문자열 — ch1.mfc·engine:ch1 …)} (+ dict 이벤트의 키 — uplink 의 fromSeq 등).
  챔버·출처 정하기(chamber_of·origin_of): 이벤트 객체의 chamber 속성(1·2 도 받음)·data.chamber → 출처 문자열의 첫 조각
  (ch1.mfc → ch1, process:ch2 → ch2, engine:pc → pc) → heater 는 ch2(히터는 CH2 만) → 나머지 shared(plc·camera·pulse …).
  출처: ProcEvent·process·engine·proc → process, SafetyEvent·safety → safety, HeaterEvent·heater → heater, 그 밖은 장비 이름
  (챔버 조각을 뺀 나머지 — ch1.mfc → mfc, tsp.ig → ig, 챔버 이름만이면 process, 단 tsp 만이면 TSP 드라이버 tsp).
  엔진이 낼 때의 약속: system.emit(source="process:<ch1|ch2|pc|tsp>", kind, …, event=dict 또는 .data 가 dict 인 객체).
  run_start·run_end 는 kind 로 알아본다 — data 키 run_id·recipe_name·by(owner)·name·step_no(process_index)·process_total
  (→ processTotal, 리스트 '(i/n)')·params·blocks / run_id·status(result)·reason(이벤트 code)·summary(없으면 {name} +
  엔진 result_detail → summary.result_detail). 챔버 공정(ch1·ch2)·플라즈마 클리닝(pc)·TSP(tsp) 모두 같은 길. run_end 는 run_start 를 보낸 공정에만(아니면 event — 시작 전 거부 등).

OES 스펙트럼(링크 이벤트 kind='spectrum' — oes_spectrum.payload, 0.5 s 쓰로틀·512점 솎기)은 event 로 보내지 않고 state.spectra[챔버]
  에 마지막 것만(1초 state 마다 같이 감 — 사건 링 버퍼·uplink 보관함·기록에 쌓이지 않게). data.clear(측정 시작)·그 챔버 run_start·
  run_end 에 비운다(v1 graph.reset — 공정 시작·끝, clear_oes_plot — 측정 시작).
명령 결과 값을 화면이 쓰는 명령(VALUE_EVENTS — THICKNESS_WINDOW: 두께 Factor·재료 창 값)은 받으면 cmd_result 뒤에 event 하나를 더 낸다
  (context {source, chamber, kind, data: 결과 value, cmdId} — 웹 cmd_result 에는 value 칸이 없어서 웹·로컬 화면 모두 이 사건을 읽는다).
명령: run_command(kind, args, source, cid, by) — system.command(...) 를 부르고 그 결과로 cmd_result 를 낸다(로컬 서버·웹 연결·
  로봇 API 가 같이 쓴다). 챔버K 는 core 가 on_result 로 냈지만 챔버12 는 여기서 낸다 — core 는 cmd_result 를 내지 않는다.
  system.command 가 없으면(명령 처리 4단계 전) CORE_NOT_READY, 예외면 INTERNAL_ERROR(로컬 서버·웹 연결 스레드가 죽지 않게).

소비자(로컬 서버, uplink)는 subscribe() 로 받는다. 소비자마다 길이 제한 큐 — 넘치면 그 소비자만 끊는다
(tick·worker 를 막지 않는다: 넣기는 put_nowait 뿐). 연결 때 최신 state 하나 + 최근 메시지(링 버퍼, last_seq 뒤부터).
last_seq 가 없거나 지금 seq 보다 크면(지난 실행의 번호) 처음부터: 마지막 hello + 최신 state + 최근 메시지 전부.
링 버퍼에서 이미 밀려난 메시지가 last_seq 뒤에 있으면 sub.gap = (첫 seq, 끝 seq).
"""
from __future__ import annotations

import collections
import datetime as _dt
import importlib
import queue
import re
import threading
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from v2.common.commands import Source

RECENT_KEEP = 500                  # 최근 메시지(state 제외) 링 버퍼
SUB_QUEUE = 1000                   # 소비자 큐 길이 — 넘치면 그 소비자를 끊는다
CLOSED = object()                  # 큐 끝 표시
CONTEXTS = ("ch1", "ch2", "pc", "tsp")
SHARED = "shared"
_RULE_ID = re.compile(r"S\d")
_PROCESS_WORDS = ("process", "engine", "proc")
_ORIGIN_CLASS = {"ProcEvent": "process", "SafetyEvent": "safety", "HeaterEvent": "heater"}
SPECTRUM_KIND = "spectrum"
# 결과 value 를 사건으로도 내는 명령 → (context.source, context.kind, 챔버 인자 기본값)
VALUE_EVENTS = {"THICKNESS_WINDOW": ("thickness", "window", 2)}


@dataclass(frozen=True)
class CmdResult:
    """명령 결과 — core 결과와 같은 모양(ok·reason·detail). 허브·웹 연결이 직접 거부할 때 쓴다."""
    ok: bool
    reason: Optional[str] = None
    detail: str = ""


def utc_iso(t_wall: Optional[float] = None) -> str:
    d = (_dt.datetime.now(_dt.timezone.utc) if t_wall is None
         else _dt.datetime.fromtimestamp(t_wall, _dt.timezone.utc))
    return d.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def optional_module(name: str):
    """아직 없을 수 있는 모듈(4단계 commands·safety_rules, 3단계 process_states) — 없으면 None. 있는데 import 가 깨지면 그대로 올린다."""
    try:
        return importlib.import_module(name)
    except ModuleNotFoundError as e:
        if e.name == name:
            return None
        raise


def _event_data(ev: Any) -> Optional[dict]:
    if isinstance(ev, dict):
        return ev
    d = getattr(ev, "data", None)
    return d if isinstance(d, dict) else None


def chamber_of(source: str, ev: Any = None) -> str:
    """이벤트의 챔버 — ch1·ch2·pc·tsp·shared."""
    for v in (getattr(ev, "chamber", None), (_event_data(ev) or {}).get("chamber")):
        if v in (1, 2) and not isinstance(v, bool):
            return f"ch{v}"
        if isinstance(v, str) and v.lower() in CONTEXTS:
            return v.lower()
    parts = [p for p in re.split(r"[:.]", str(source or "").lower()) if p]
    for p in parts:
        if p in CONTEXTS:
            return p
    if "heater" in parts or type(ev).__name__ == "HeaterEvent":
        return "ch2"
    return SHARED


def origin_of(source: str, ev: Any = None) -> str:
    """이벤트의 출처 — process·safety·heater·uplink·장비 이름."""
    by_class = _ORIGIN_CLASS.get(type(ev).__name__)
    if by_class:
        return by_class
    parts = [p for p in re.split(r"[:.]", str(source or "").lower()) if p]
    if not parts:
        return "unknown"
    for words, name in ((_PROCESS_WORDS, "process"), (("safety",), "safety"), (("heater",), "heater")):
        if any(p in words for p in parts):
            return name
    rest = [p for p in parts if p not in CONTEXTS]
    if rest:
        return ".".join(rest)
    return "tsp" if parts == ["tsp"] else "process"


class Subscriber:
    def __init__(self, hub: "MessageHub", maxsize: int, name: str) -> None:
        self.hub, self.name = hub, name
        self.q: "queue.Queue" = queue.Queue(maxsize=maxsize)
        self.backlog: "collections.deque[dict]" = collections.deque()   # 연결 때 다시 보내는 것(큐와 따로)
        self.closed = False
        self.reason = ""
        self.gap: Optional[tuple] = None          # (놓친 첫 seq, 놓친 끝 seq) — 링 버퍼보다 많이 놓쳤을 때

    def put(self, env: dict) -> bool:
        if self.closed:
            return False
        try:
            self.q.put_nowait(env)
            return True
        except queue.Full:
            self.close("slow")                   # 느린 소비자 — 이 소비자만 끊는다
            return False

    def get(self, timeout: float):
        """다음 메시지(다시 보내기 먼저), 시간 안에 없으면 None, 끊겼으면 CLOSED."""
        if self.backlog:
            return self.backlog.popleft()
        if self.closed and self.q.empty():
            return CLOSED
        try:
            return self.q.get(timeout=timeout)
        except queue.Empty:
            return CLOSED if self.closed else None

    def close(self, reason: str = "closed") -> None:
        if not self.closed:
            self.closed, self.reason = True, reason
            self.hub._drop(self)
            try:
                self.q.put_nowait(CLOSED)
            except queue.Full:
                pass


class MessageHub:
    def __init__(self, system, recent_keep: int = RECENT_KEEP) -> None:
        self.sys = system
        self._lock = threading.Lock()
        self.seq = 0
        self.instance = uuid.uuid4().hex
        self.recent: "collections.deque[dict]" = collections.deque(maxlen=recent_keep)
        self.last_state: Optional[dict] = None
        self.last_hello: Optional[dict] = None
        self.evicted_seq = 0                         # 링 버퍼에서 밀려난 마지막 seq
        self.subs: List[Subscriber] = []
        self.dropped_subs = 0
        self.alarm_codes: Dict[str, dict] = {}       # 규칙 id → 마지막 발동(code·since·chamber)
        self.errors = 0                              # 메시지 만들기 실패(제어는 막지 않음)
        self._web_hash: Optional[str] = None
        self._open_runs: Dict[str, Any] = {}         # 챔버 → run_start 를 보낸 공정 id(run_end 는 이것만)
        self.spectra: Dict[str, Optional[dict]] = {"ch1": None, "ch2": None}   # 챔버 → 마지막 OES 스펙트럼(state.spectra)

    # ── 조립에 붙기 ──
    def attach(self) -> "MessageHub":
        s = self.sys
        s.hub = self
        if self.on_logged not in s.event_listeners:
            s.event_listeners.append(self.on_logged)
        if self.on_row not in s.row_listeners:
            s.row_listeners.append(self.on_row)
        return self

    def detach(self) -> None:
        s = self.sys
        for lst, f in ((s.event_listeners, self.on_logged), (s.row_listeners, self.on_row)):
            while f in lst:
                lst.remove(f)

    # ── 보내기 ──
    def _wall(self) -> float:
        return self.sys.clock.wall()

    def publish(self, type_: str, data: Dict[str, Any]) -> dict:
        with self._lock:
            self.seq += 1
            env = {"id": uuid.uuid4().hex, "instance": self.instance, "seq": self.seq, "ts": utc_iso(self._wall()),
                   "type": type_, "data": data}
            if type_ == "state":
                self.last_state = env
            else:
                if type_ == "hello":
                    self.last_hello = env
                if len(self.recent) == self.recent.maxlen:
                    self.evicted_seq = self.recent[0]["seq"]
                self.recent.append(env)
            subs = list(self.subs)
        for s in subs:
            s.put(env)
        return env

    def envelope(self, type_: str, data: Dict[str, Any]) -> dict:
        """seq 를 받지만 링 버퍼·소비자에 넣지 않는 envelope(uplink 가 연결마다 맨 앞에 싣는 hello)."""
        with self._lock:
            self.seq += 1
            return {"id": uuid.uuid4().hex, "instance": self.instance, "seq": self.seq, "ts": utc_iso(self._wall()),
                    "type": type_, "data": data}

    def subscribe(self, last_seq: Optional[int] = None, maxsize: int = SUB_QUEUE, name: str = "") -> Subscriber:
        """새 소비자 — 최신 state 하나 + 최근 메시지(last_seq 가 있으면 그 뒤부터)를 먼저 넣는다.
        last_seq 가 지금 seq 보다 크면(지난 실행의 번호) 없는 것으로 — 마지막 hello 부터 처음부터 보낸다."""
        sub = Subscriber(self, maxsize, name)
        with self._lock:
            if last_seq is not None and last_seq > self.seq:
                last_seq = None
            first = []
            if last_seq is None and self.last_hello is not None:
                first.append(self.last_hello)                     # 링 버퍼에서 밀려났어도 hello 는 맨 앞에
            if self.last_state is not None:
                first.append(self.last_state)
            first += [e for e in self.recent if (last_seq is None or e["seq"] > last_seq) and e is not self.last_hello]
            if last_seq is not None and self.evicted_seq > last_seq:
                sub.gap = (last_seq + 1, self.evicted_seq)
            sub.backlog.extend(first)
            self.subs.append(sub)
        return sub

    def _drop(self, sub: Subscriber) -> None:
        with self._lock:
            if sub in self.subs:
                self.subs.remove(sub)
                if sub.reason == "slow":
                    self.dropped_subs += 1

    def close(self) -> None:
        for s in list(self.subs):
            s.close("shutdown")

    # ── hello ──
    @property
    def web_schema_hash(self) -> str:
        if self._web_hash is None:
            from v2.ch12 import web_schema as WS
            self._web_hash = WS.content_hash()
        return self._web_hash

    def command_kinds(self, source: Source = Source.HMI) -> List[str]:
        C = optional_module("v2.ch12.commands")
        if C is None:
            return []
        return [c.kind for c in getattr(C, "COMMANDS", ()) if source in getattr(c, "sources", ())]

    def hello_data(self) -> dict:
        s = self.sys
        st = getattr(s, "store", None)
        return {"equipment": s.cfg.get("EQUIPMENT_ID"), "instance": self.instance, "program": s.program,
                "version": s.version, "runMode": s.mode, "schemaHash": param_hash(),
                "configVersion": config_version(s), "webSchemaHash": self.web_schema_hash,
                "overriddenKeys": st.overridden() if st is not None else [],
                "snapshotColumns": list(s.bus.columns),
                "commandKinds": self.command_kinds(Source.HMI),
                "writesAllowed": bool(s.policy.device_writes), "deviceUtc": utc_iso(self._wall()),
                "chambers": list(CONTEXTS)}

    def hello(self) -> dict:
        return self.publish("hello", self.hello_data())

    # ── 명령·설정 결과 ──
    @staticmethod
    def cmd_result_data(cid: Any, kind: str, res, source: Any = None, by: Optional[str] = None) -> dict:
        src = getattr(source, "value", source)
        return {"cmdId": None if cid is None else str(cid), "kind": kind, "status": "accepted" if res.ok else "rejected",
                "reasonCode": res.reason, "source": src, "by": by,
                "detail": (getattr(res, "detail", "") or "")[:300]}

    def cmd_result(self, cid: Any, kind: str, res, source: Any = None, by: Optional[str] = None) -> dict:
        return self.publish("cmd_result", self.cmd_result_data(cid, kind, res, source, by))

    def run_command(self, kind: str, args: Optional[dict], source: Source, cid: Any = None,
                    by: Optional[str] = None):
        """명령 처리 한 곳(system.command)을 부르고 cmd_result 를 낸다. 결과 객체(.ok·.reason·.detail)를 돌려준다."""
        fn = getattr(self.sys, "command", None)
        if fn is None:
            res = CmdResult(False, "CORE_NOT_READY", "명령 처리(core)가 아직 없음")
        else:
            try:
                res = fn(kind, dict(args or {}), source=source, cid=cid, by=by)
                if res is None or not hasattr(res, "ok"):
                    res = CmdResult(False, "INTERNAL_ERROR", f"명령 결과 형식 오류: {type(res).__name__}")
            except Exception as e:                          # 명령 하나의 오류가 서버·웹 연결 스레드를 멈추지 않는다
                res = CmdResult(False, "INTERNAL_ERROR", repr(e)[:300])
                self._safe_emit("command", "internal_error", repr(e), "error", "INTERNAL_ERROR")
        self.cmd_result(cid, kind, res, source, by)
        if res.ok and kind in VALUE_EVENTS and getattr(res, "value", None) is not None:
            src_name, ev_kind, ch_default = VALUE_EVENTS[kind]
            ch = f"ch{int((args or {}).get('ch') or ch_default)}"
            self._event("info", kind, None, "", {"source": src_name, "chamber": ch, "kind": ev_kind,
                                                  "origin": "command", "data": res.value,
                                                  "cmdId": None if cid is None else str(cid)})
        return res

    def config_ack(self, d: dict) -> None:
        self.publish("config_ack", dict(d))

    def _safe_emit(self, source, kind, detail, level, code) -> None:
        try:
            self.sys.emit(source, kind, detail, level=level, code=code)
        except Exception:
            self.errors += 1

    # ── 내부 이벤트 → 메시지 ──
    def on_logged(self, e) -> None:
        """system.event_listeners — Logged(t_wall, source, kind, detail, reason, code, level, event)."""
        try:
            self._on_logged(e)
        except Exception:                                   # 메시지 만들기 실패가 제어를 막지 않는다
            self.errors += 1

    def _on_logged(self, e) -> None:
        ev = getattr(e, "event", None)
        src = str(getattr(e, "source", "") or "")
        kind = str(getattr(e, "kind", "") or "?")
        ch = chamber_of(src, ev)
        org = origin_of(src, ev)
        data = _event_data(ev)
        if kind == SPECTRUM_KIND and org == "oes":
            if ch in self.spectra:
                self.spectra[ch] = None if not data or data.get("clear") else dict(data)
            return
        if org == "process" and kind in ("run_start", "run_end") and self._run(kind, ch, dict(data or {}), e):
            if ch in self.spectra:
                self.spectra[ch] = None                   # v1 graph.reset(공정 시작·끝)
            return
        code = getattr(e, "code", None) or getattr(e, "reason", None) or kind.upper()
        level = getattr(e, "level", None) or "info"
        rule = None
        if org == "safety":
            r = getattr(ev, "rule", None) or (data or {}).get("rule")
            rule = r if _RULE_ID.match(str(r or "")) else None
            if rule and kind == "fired":
                self.alarm_codes[rule] = {"code": code, "since": utc_iso(getattr(e, "t_wall", None) or self._wall()),
                                          "chamber": ch}
            elif rule and kind == "released":
                self.alarm_codes.pop(rule, None)
        ctx: Dict[str, Any] = {"source": org, "chamber": ch, "kind": kind, "origin": src}
        if isinstance(ev, dict):
            ctx.update({k: v for k, v in ev.items() if k not in ctx})
        elif data:
            ctx["data"] = data
        self._event(str(level), str(code).upper(), rule, str(getattr(e, "detail", "") or ""), ctx)

    def _event(self, level: str, code: str, rule: Optional[str], message: str, context: dict) -> None:
        self.publish("event", {"level": level, "code": code, "ruleId": rule, "message": (message or "")[:500],
                               "context": context})

    def _run(self, kind: str, ch: str, d: dict, e: Any) -> bool:
        """run_start·run_end 메시지(챔버별). run_end 는 run_start 를 보낸 공정에만(그 챔버에 열린 공정이 없으면 False —
        시작 전 거부 등은 event 로). 엔진 data 키: run_id·recipe_name·by(없으면 owner)·name(없으면 문구)·step_no(없으면
        process_index)·params·blocks·recipe_rev / run_id·status(또는 result)·reason(없으면 이벤트 code)·summary."""
        name = d.get("name") or getattr(e, "detail", None) or None
        if kind == "run_start":
            rid = d.get("run_id")
            self._open_runs[ch] = rid
            self.publish("run_start", {"runId": rid, "chamber": ch, "recipeName": d.get("recipe_name"),
                                       "by": d.get("by") or d.get("owner"), "name": name,
                                       "stepNo": d.get("step_no") or d.get("process_index") or 1,
                                       "params": d.get("params") or {}, "blocks": d.get("blocks"),
                                       "configVersion": config_version(self.sys), "owner": d.get("owner"),
                                       "recipeRev": d.get("recipe_rev"), "processTotal": d.get("process_total")})
            return True
        if ch not in self._open_runs:
            return False
        rid = d.get("run_id") or self._open_runs.pop(ch)
        self._open_runs.pop(ch, None)
        summary = dict(d.get("summary") or {"name": name})
        if "result_detail" not in summary and isinstance(d.get("result_detail"), dict):
            summary["result_detail"] = d["result_detail"]          # 엔진 run_end 상세(errors·warnings·Arc·chuck·두께)
        self.publish("run_end", {"runId": rid, "chamber": ch, "result": d.get("status") or d.get("result"),
                                 "reasonCode": d.get("reason") or getattr(e, "code", None) or getattr(e, "reason", None),
                                 "summary": summary})
        return True

    # ── state(1초 행마다) ──
    def on_row(self, row: Dict[str, Any]) -> None:
        try:
            data = self.state_data(row)
        except Exception:
            self.errors += 1
            data = {"procState": "UNKNOWN", "snapshot": row, "heater": {}, "alarms": [], "links": {},
                    "configVersion": config_version(self.sys), "left": {}, "uplink": {"enabled": False}}
        self.publish("state", data)

    def chamber_state(self, c: str, row: Dict[str, Any]) -> dict:
        p = f"{c}.proc."
        g = lambda k: row.get(p + k)
        step = None
        if g("block_no") is not None:
            step = {"no": g("block_no"), "total": g("block_count"), "type": g("block_type"), "label": g("block_label"),
                    "elapsedS": g("block_elapsed_s"), "remainS": g("remain_s")}
        al = g("alarms")
        return {"procState": g("state"), "runId": g("run_id"), "owner": g("owner"), "recipeRev": g("recipe_rev"),
                "step": step, "remainS": g("remain_s"), "etaRemainingS": g("eta_remaining_s"), "minTotalS": g("min_total_s"),
                "paused": g("paused"), "alarms": [a for a in str(al).split(",") if a] if al else []}

    def state_data(self, row: Dict[str, Any]) -> dict:
        s = self.sys
        chambers = {c: self.chamber_state(c, row) for c in CONTEXTS}
        engines: Dict[str, Any] = {}
        for n, e in list(getattr(s, "engines", {}).items()):
            f = getattr(e, "web_state", None)
            if callable(f):
                try:
                    engines[n] = f()
                except Exception:
                    self.errors += 1
        h = engines.get("heater")
        alarms = [dict({"ruleId": r}, **v) for r, v in sorted(self.alarm_codes.items())]
        seen = {a["ruleId"] for a in alarms}
        for c, cs in chambers.items():
            alarms += [{"ruleId": r, "chamber": c} for r in cs["alarms"] if r not in seen]
        blocks = getattr(s.bus, "blocks", None) or []
        links = {b.name: {"q": row.get(f"{b.name}.q"), "age": row.get(f"{b.name}.age_s")} for b in blocks}
        st = getattr(s, "store", None)
        safety = getattr(s, "safety", None)
        up = getattr(s, "uplink", None)
        busy = s.busy() if hasattr(s, "busy") else False
        return {"procState": "BUSY" if busy else "IDLE", "runId": None, "chambers": chambers, "engines": engines,
                "heater": h if isinstance(h, dict) else {}, "alarms": alarms, "links": links,
                "configVersion": config_version(s),
                "configPending": ({"idle": dict(st.pending_idle), "restart": dict(st.pending_restart)}
                                  if st is not None and (st.pending_idle or st.pending_restart) else None),
                "snapshot": row, "left": {}, "stopping": getattr(safety, "stopping", None) or None,
                "spectra": dict(self.spectra),
                "uplink": up.status() if up is not None else {"enabled": False}}


def config_version(s) -> int:
    st = getattr(s, "store", None)
    return st.version if st is not None else 0


def param_hash() -> str:
    from v2.ch12.params import table
    return table().schema_hash()


def attach(system, recent_keep: int = RECENT_KEEP) -> MessageHub:
    """조립에 허브를 붙인다(system.hub·event_listeners·row_listeners)."""
    return MessageHub(system, recent_keep).attach()


def split(e) -> Tuple[str, str]:
    """(챔버, 출처) — 시험·로그용."""
    return chamber_of(getattr(e, "source", ""), getattr(e, "event", None)), origin_of(getattr(e, "source", ""),
                                                                                       getattr(e, "event", None))
