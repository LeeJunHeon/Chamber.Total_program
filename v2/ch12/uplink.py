"""챔버12 웹 상시 연결(uplink, 사용자 지시 3장 (7)) — 챔버K v2/chk/uplink.py 그대로(계약: 경로·헤더·JSON·응답 코드·명령·보관함은
v2.common.messages 독스트링 — 챔버K 와 같은 계약, 웹은 같은 받는 쪽을 장비 ID 로 나눈다). 장비 ID 는 CH12, sim 은 CH12-SIM
(UPLINK_IN_SIM 일 때 -SIM 으로 끝나야 — app 이 시작 때 검사).

스레드 셋(데몬, 실제 시간 — 시스템 시계는 envelope 시각·명령 만료 판정에만):
  uplink-in   MessageHub 소비자. state 는 최신 하나만(+ snapshot_summary 계산 — 공정 중 구간만), hello 는 건너뜀(연결마다 새로 만든다),
              나머지는 받자마자 보관함(Spool)에 — HIGH 면 uplink-out 을 깨운다(직전 POST 성공·200 ms 지남이면 바로 POST). 느린 소비자로
              끊기면 마지막 seq 로 다시 붙어 링 버퍼에서 이어 받는다(링 버퍼보다 많이 놓쳤으면 UPLINK_GAP).
  uplink-out  POST {URL}/ingest — UPLINK_INTERVAL_MS 마다 [hello(연결 뒤 첫 POST)] + 보관함(오래된 것부터, 개수·바이트 상한) + 최신 state.
              acked id 만 보관함에서 지운다. 응답 commands 는 SSE 와 같게 처리. needWebSchema 면 webSchema 를 실은 hello 를 한 번 더.
  uplink-sse  GET {URL}/stream — 명령 받기(주 경로). 끊기면 다시 붙는다.
tick·worker·로컬 서버를 막지 않는다 — 허브는 put_nowait, 보관함·명령 기억은 상한, 네트워크는 자기 스레드·시간 제한 UPLINK_TIMEOUT_SEC.
끊겨도 공정·안전은 그대로(기록·화면에만 — UPLINK_DOWN·state.uplink).
UPLINK_URL·UPLINK_TOKEN 은 로그·메시지에 쓰지 않는다(주소는 호스트만).

챔버12 에서 다른 것(연결부만 — 계약은 같다)
- 명령 실행: hub.run_command(kind, args, Source.WEB, cid, by=requestedBy) → system.command(...) — cmd_result 는 허브가 낸다.
- 알림(UPLINK_*): system.emit("uplink", 종류, 문구, level, code, event={값}) — 기록·로그·허브(event, context.source uplink)가 같은 길.
- 명령 id 기억(같은 id 는 한 번만): to_state()/from_state(d) — 5단계 저널이 1초마다 저장했다가 재시작 뒤 넣어
  재시작 전에 실행한 명령이 다시 와도 실행하지 않고 같은 cmd_result 를 다시 보낸다.
"""
from __future__ import annotations

import collections
import datetime as _dt
import http.client
import json
import random
import ssl
import threading
import time
from pathlib import Path
from typing import Any, Callable, List, Optional
from urllib.parse import urlencode, urlsplit

from v2.common.commands import Source
from v2.common.messages import dumps_json
from v2.ch12.hub import CLOSED, SUB_QUEUE, CmdResult, utc_iso
from v2.ch12.uplink_spool import HIGH, LOW, SEG_BYTES, Spool, Summary, priority  # noqa: F401 — 시험·저널이 uplink. 으로 쓴다

UPLINK_BATCH_MAX = 500                 # 한 POST 에 보관함 메시지 최대 개수
UPLINK_BATCH_BYTES = 512 * 1024        # 한 POST 본문 최대 바이트(대략)
AUTH_RETRY_S = 60.0                    # 401
CONFLICT_RETRY_S = 10.0                # 409
BACKOFF_MAX_S = 30.0                   # 그 밖 오류: 1→2→4…→30 s
GROW_AFTER_413_S = 60.0                # 413 로 줄인 묶음 크기를 다시 늘리기까지
SSE_IDLE_S = 45.0                      # 웹 keep-alive 15 s — 세 번 못 받으면 끊고 다시
CMD_KEEP = 2000                        # 기억하는 명령 id(같은 id 는 한 번만 실행)
SPOOL_FULL_WARN_S = 60.0               # UPLINK_SPOOL_FULL 경고 간격(그동안 버린 수를 모아서)
WAKE_GAP_S = 0.2                       # HIGH 메시지로 주기 밖 POST — 직전 POST 끝에서 이만큼은 띄운다


def retry_delay(code: Optional[int], fails: int, rnd: Callable[[], float] = random.random) -> float:
    """다시 시도까지 초 — 401 60 s, 409 10 s, 그 밖 1→2→4…→30 s(±20 % 흔들림, 30 s 넘지 않음)."""
    if code == 401:
        return AUTH_RETRY_S
    if code == 409:
        return CONFLICT_RETRY_S
    base = min(BACKOFF_MAX_S, 2.0 ** max(0, fails - 1))
    return min(BACKOFF_MAX_S, base * (0.8 + 0.4 * rnd()))


def parse_iso(s: Any) -> Optional[float]:
    if not isinstance(s, str) or not s:
        return None
    try:
        d = _dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=_dt.timezone.utc)
    return d.timestamp()


# ───────────── 연결 ─────────────
class HttpError(Exception):
    def __init__(self, status: Optional[int], what: str) -> None:
        super().__init__(what)
        self.status, self.what = status, what


class Uplink:
    def __init__(self, system, url: str, token: str, data_dir: Path) -> None:
        self.sys = system
        self.hub = system.hub
        self.cfg = system.cfg
        u = urlsplit(url)
        self.scheme, self.host, self.port = u.scheme, u.hostname or "", u.port
        self.base = u.path.rstrip("/")
        self._auth = f"Bearer {token}"
        self.spool = Spool(Path(data_dir) / "uplink", int(self.cfg["UPLINK_SPOOL_MAX_MB"]) * 1024 * 1024)
        self.summary = Summary()
        self.latest_state: Optional[dict] = None
        self._stop = threading.Event()
        self._wake = threading.Event()                      # HIGH 메시지 → 주기 밖 POST(stop 도 깨운다)
        self._last_post_end: Optional[float] = None
        self.early_posts = 0
        self._threads: List[threading.Thread] = []
        self._sse_sock = None
        self._ssl = ssl.create_default_context() if self.scheme == "https" else None
        # 보내기 상태
        self.need_hello = True
        self.want_schema = False
        self.batch_n = UPLINK_BATCH_MAX
        self._last_413 = -1e9
        self.fails = 0
        self.fail_since: Optional[float] = None
        self.down_reported = False
        self.last_ok_wall: Optional[float] = None
        self.last_error: Optional[str] = None
        self.sse_connected = False
        self.sse_fails = 0
        self._warned: set = set()
        self.posts = 0
        # 명령
        self._cmd_lock = threading.Lock()
        self.done_cmds: "collections.OrderedDict[str, dict]" = collections.OrderedDict()
        self.dup_cmds = 0
        # 소비자
        self.last_seq: Optional[int] = None
        self.resubscribes = 0
        self.sub_queue = SUB_QUEUE
        self._full_pending = 0
        self._full_warn_t: Optional[float] = None

    @property
    def where(self) -> str:
        """로그·화면에 쓰는 주소 — 호스트만(경로·토큰 없음)."""
        return self.host

    # ── 생명주기 ──
    def start(self) -> None:
        if self.spool.loaded:
            self._note("info", "UPLINK_SPOOL_LOADED", f"지난 실행이 남긴 웹 미전송 메시지 {self.spool.loaded}개 — 이어서 올림",
                       count=self.spool.loaded)
        sub = self.hub.subscribe(None, maxsize=self.sub_queue, name="uplink")   # 시작 전에 붙어 첫 메시지부터
        for target, name in ((lambda: self._consume(sub), "uplink-in"), (self._send_loop, "uplink-out"),
                             (self._sse_loop, "uplink-sse")):
            t = threading.Thread(target=target, name=name, daemon=True)
            t.start()
            self._threads.append(t)

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        self._wake.set()
        sk = self._sse_sock
        if sk is not None:
            try:
                sk.shutdown(2)                                 # 읽기 중인 SSE 를 깨운다
            except OSError:
                pass
        t_end = time.monotonic() + timeout
        for t in self._threads:
            t.join(max(0.0, t_end - time.monotonic()))
        self.spool.close()

    def status(self) -> dict:
        return {"enabled": True, "host": self.where, "connected": self.sse_connected,
                "lastOkAt": utc_iso(self.last_ok_wall) if self.last_ok_wall is not None else None,
                "lastError": self.last_error, "queued": self.spool.count, "spoolBytes": self.spool.live_bytes,
                "dropped": self.spool.dropped}

    def _note(self, level: str, code: str, msg: str, **ctx) -> None:
        try:
            self.sys.emit("uplink", code.lower(), msg, level=level, code=code, event=dict(ctx))
        except Exception:
            pass

    def _warn_once(self, key: str, code: str, msg: str, **ctx) -> None:
        if key not in self._warned:
            self._warned.add(key)
            self._note("warn", code, msg, **ctx)

    # ── 소비자(uplink-in) ──
    def _consume(self, sub) -> None:
        while not self._stop.is_set():
            if sub is None:
                self.resubscribes += 1
                sub = self.hub.subscribe(self.last_seq, maxsize=self.sub_queue, name="uplink")
                if sub.gap:
                    self._note("warn", "UPLINK_GAP", f"웹으로 올릴 메시지를 놓침(seq {sub.gap[0]}~{sub.gap[1]} 중 링 버퍼에서 밀려난 것)",
                               fromSeq=sub.gap[0], toSeq=sub.gap[1])
            while True:
                env = sub.get(0.5)
                if env is None:
                    if self._stop.is_set():
                        return
                    self._maybe_full_warn()
                    continue
                if env is CLOSED:
                    break
                try:
                    self._take(env)
                except Exception as e:                          # 보관함 쓰기 실패 — 버리고 계속(공정에 영향 없음)
                    self._note("warn", "UPLINK_SPOOL_ERROR", f"보관함 쓰기 실패: {type(e).__name__}")
            if sub.reason == "shutdown":
                return
            sub = None

    def _take(self, env: dict) -> None:
        t = env["type"]
        if t == "state":
            self.latest_state = env
            row = (env.get("data") or {}).get("snapshot") or {}
            out = self.summary.add(parse_iso(env["ts"]) or self.sys.clock.wall(), row,
                                   int(self.cfg["UPLINK_SUMMARY_PERIOD_SEC"]))
            if out is not None:
                self.hub.publish("snapshot_summary", out)
            return
        if self.last_seq is not None and env["seq"] <= self.last_seq:
            return                                              # 다시 붙을 때 겹친 것
        self.last_seq = env["seq"]
        if t == "hello":
            return                                              # 연결마다 새로 만든다(uplink-out)
        n = self.spool.put(env)
        if priority(env) == HIGH:
            self._wake.set()
        if n:
            self._full_pending += n
            self._maybe_full_warn()

    def _maybe_full_warn(self) -> None:
        if not self._full_pending:
            return
        now = time.monotonic()
        if self._full_warn_t is not None and now - self._full_warn_t < SPOOL_FULL_WARN_S:
            return
        n, self._full_pending, self._full_warn_t = self._full_pending, 0, now
        self._note("warn", "UPLINK_SPOOL_FULL",
                   f"웹 미전송 보관함 가득(UPLINK_SPOOL_MAX_MB {self.cfg['UPLINK_SPOOL_MAX_MB']}) — {n}개 버림(낮은 것부터)",
                   dropped=n, total=self.spool.dropped)

    # ── HTTP ──
    def _conn(self, timeout: float) -> http.client.HTTPConnection:
        if self.scheme == "https":
            return http.client.HTTPSConnection(self.host, self.port, timeout=timeout, context=self._ssl)
        return http.client.HTTPConnection(self.host, self.port, timeout=timeout)

    def _headers(self, **extra) -> dict:
        return dict({"Authorization": self._auth, "User-Agent": "ch12-v2"}, **extra)

    def post_ingest(self, body: bytes) -> dict:
        c = self._conn(float(self.cfg["UPLINK_TIMEOUT_SEC"]))
        try:
            c.request("POST", self.base + "/ingest", body=body,
                      headers=self._headers(**{"Content-Type": "application/json"}))
            r = c.getresponse()
            data = r.read(4_000_000)
            if r.status != 200:
                raise HttpError(r.status, f"HTTP {r.status}")
            try:
                out = json.loads(data.decode("utf-8") or "{}")
            except ValueError:
                raise HttpError(None, "bad_response")
            if not isinstance(out, dict):
                raise HttpError(None, "bad_response")
            return out
        except (OSError, http.client.HTTPException) as e:
            raise HttpError(None, "timeout" if isinstance(e, TimeoutError) else type(e).__name__)
        finally:
            c.close()

    # ── 보내기(uplink-out) ──
    def _hello(self) -> dict:
        d = self.hub.hello_data()
        if self.want_schema:
            from v2.ch12 import web_schema as WS
            d["webSchema"] = WS.bundle_for(dict(self.cfg), getattr(self.sys, "store", None), d["configVersion"])
        return self.hub.envelope("hello", d)

    def _send_loop(self) -> None:
        next_at = time.monotonic()
        while not self._stop.is_set():
            now = time.monotonic()
            if now < next_at:
                self._check_down(now)
                wait = min(0.5, next_at - now)
                if self.fails or self._last_post_end is None:
                    self._stop.wait(wait)                       # 실패 뒤 대기(401·409·백오프)는 깨우지 않는다
                    continue
                if self._wake.is_set():                         # HIGH 메시지 — 직전 POST 성공, 200 ms 지나면 바로
                    early = self._last_post_end + WAKE_GAP_S
                    if now >= early:
                        next_at = now
                        self.early_posts += 1
                    else:
                        self._stop.wait(min(wait, early - now))
                    continue
                self._wake.wait(wait)
                continue
            self._wake.clear()                                  # 이 POST 에 실린다 — 이 뒤에 온 것만 다시 깨운다
            try:
                more = self.send_once()
                next_at = time.monotonic() + (0.05 if more else float(self.cfg["UPLINK_INTERVAL_MS"]) / 1000.0)
            except HttpError as e:
                next_at = time.monotonic() + self._failed(e)
            except Exception as e:                              # 예상 밖 — 끊김으로 다루고 계속
                next_at = time.monotonic() + self._failed(HttpError(None, type(e).__name__))

    def send_once(self) -> bool:
        """POST 한 번. 보관함에 더 보낼 것이 남았으면 True(바로 이어서)."""
        hello = self._hello() if self.need_hello else None
        with_schema = hello is not None and "webSchema" in hello["data"]
        state = self.latest_state
        head = [_dumps(hello)] if hello else []
        tail = [_dumps(state)] if state else []
        room = UPLINK_BATCH_BYTES - sum(len(x) for x in head + tail)
        items = self.spool.batch(self.batch_n, max(1, room))
        msgs = head + [ln for _, ln in items] + tail
        body = (b'{"equipment":' + _dumps(self.cfg["EQUIPMENT_ID"]) + b',"instance":' + _dumps(self.hub.instance)
                + b',"schemaHash":' + _dumps(self.hub.hello_data()["schemaHash"]) + b',"messages":['
                + b",".join(msgs) + b"]}")
        try:
            out = self.post_ingest(body)
        except HttpError as e:
            self._last_post_end = time.monotonic()
            if e.status == 413:
                return self._too_large(items, with_schema)
            raise
        self.posts += 1
        self._last_post_end = time.monotonic()
        acked = out.get("acked") or []
        if isinstance(acked, list):
            self.spool.ack(acked)
        if hello is not None:
            self.need_hello = False
            if with_schema:
                self.want_schema = False
        if out.get("needWebSchema") and not with_schema:
            self.want_schema = self.need_hello = True
        self._ok()
        for cmd in out.get("commands") or []:
            if isinstance(cmd, dict):
                self.handle_command(cmd, "post")
        if (len(items) == self.batch_n and self.batch_n < UPLINK_BATCH_MAX
                and time.monotonic() - self._last_413 > GROW_AFTER_413_S):   # 413 뒤 한동안은 줄인 크기로
            self.batch_n = min(UPLINK_BATCH_MAX, self.batch_n * 2)
        return bool(items) and self.spool.count > 0 and (len(items) >= self.batch_n or self.need_hello)

    def _too_large(self, items, with_schema: bool) -> bool:
        self._last_413 = time.monotonic()
        if len(items) > 1:
            self.batch_n = max(1, len(items) // 2)              # 반으로 나눠 바로 다시
            return True
        if len(items) == 1:
            eid = items[0][0]
            self.spool.drop(eid)
            self._note("warn", "UPLINK_TOO_LARGE", f"웹이 받지 않는 큰 메시지 하나를 버림({len(items[0][1])} B)", id=eid)
            return True
        if with_schema:
            self.want_schema = False
            self._note("warn", "UPLINK_TOO_LARGE", "webSchema 를 실은 hello 가 너무 큼 — 묶음 없이 보냄")
            return True
        raise HttpError(413, "HTTP 413")

    def _ok(self) -> None:
        self.last_ok_wall = self.sys.clock.wall()
        self.last_error = None
        if self.down_reported:
            self._note("info", "UPLINK_UP", f"웹 연결 다시 됨({self.where})")
        self.fails, self.fail_since, self.down_reported = 0, None, False
        self._warned = {k for k in self._warned if not k.startswith("post:")}

    def _failed(self, e: HttpError) -> float:
        now = time.monotonic()
        self.fails += 1
        self.last_error = e.what
        self.need_hello = True                                  # 다시 연결되면 맨 앞에 hello
        if self.fail_since is None:
            self.fail_since = now
        if e.status == 401:
            self._warn_once("post:401", "UPLINK_AUTH_FAILED", f"웹이 장비 토큰을 거부(401, {self.where}) — 60 s 마다 다시")
        elif e.status == 409:
            self._warn_once("post:409", "UPLINK_CONFLICT",
                            f"웹이 이 실행을 받지 않음(409 — 같은 장비 ID 의 다른 실행이 연결 중, {self.where}) — 10 s 마다 다시")
        self._check_down(now)
        return retry_delay(e.status, self.fails)

    def _check_down(self, now: float) -> None:
        if (self.fail_since is not None and not self.down_reported
                and now - self.fail_since >= float(self.cfg["UPLINK_DOWN_SEC"])):
            self.down_reported = True
            self._note("warn", "UPLINK_DOWN", f"웹 연결 끊김 {now - self.fail_since:.0f} s({self.where}, {self.last_error}) — "
                                              "공정·안전은 그대로, 메시지는 보관함에", lastError=self.last_error)

    # ── 명령(uplink-sse·POST 응답) ──
    def handle_command(self, cmd: dict, via: str = "sse") -> Optional[dict]:
        """같은 id 는 한 번만 실행 — 다시 오면 같은 cmd_result 를 다시 보낸다. 만료 → COMMAND_EXPIRED. 돌려주는 값 = cmd_result data."""
        cid = cmd.get("id")
        if not isinstance(cid, str) or not cid:
            self._note("warn", "UPLINK_BAD_COMMAND", f"id 없는 웹 명령을 버림({via})")
            return None
        with self._cmd_lock:
            prev = self.done_cmds.get(cid)
            if prev is not None:
                self.dup_cmds += 1
                self.hub.publish("cmd_result", dict(prev))
                return prev
            kind, args = cmd.get("kind"), cmd.get("args", {})
            by = str(cmd.get("requestedBy") or "web")
            exp = parse_iso(cmd.get("expiresAt"))
            if not isinstance(kind, str) or not isinstance(args, dict) or exp is None:
                res = CmdResult(False, "BAD_ARGS", "kind 는 문자열, args 는 객체, expiresAt 은 ISO 시각")
                self.hub.cmd_result(cid, str(kind), res, Source.WEB, by)
            elif self.sys.clock.wall() > exp + float(self.cfg["UPLINK_CLOCK_SKEW_SEC"]):
                res = CmdResult(False, "COMMAND_EXPIRED")
                self.hub.cmd_result(cid, kind, res, Source.WEB, by)
            else:
                res = self.hub.run_command(kind, args, Source.WEB, cid=cid, by=by)          # cmd_result 는 허브가
            data = self.hub.cmd_result_data(cid, str(kind), res, Source.WEB, by)
            self.done_cmds[cid] = data
            while len(self.done_cmds) > CMD_KEEP:
                self.done_cmds.popitem(last=False)
            return data

    # ── 명령 id 기억(5단계 저널) ──
    def to_state(self) -> dict:
        """처리한 웹 명령 id 와 결과(오래된 것부터, 최근 CMD_KEEP 개) — 저널이 저장한다."""
        with self._cmd_lock:
            return {"v": 1, "cmds": [[k, dict(v)] for k, v in self.done_cmds.items()]}

    def from_state(self, d: Optional[dict]) -> int:
        """재시작 뒤 저널 값으로 명령 기억을 되살린다(지금 기억 앞에 — 지금 것이 이긴다). 넣은 수."""
        items = (d or {}).get("cmds") if isinstance(d, dict) else None
        if not isinstance(items, list):
            return 0
        with self._cmd_lock:
            cur = self.done_cmds
            merged: "collections.OrderedDict[str, dict]" = collections.OrderedDict()
            n = 0
            for it in items:
                if (isinstance(it, (list, tuple)) and len(it) == 2 and isinstance(it[0], str) and it[0]
                        and isinstance(it[1], dict) and it[0] not in cur):
                    merged[it[0]] = dict(it[1])
                    n += 1
            merged.update(cur)
            while len(merged) > CMD_KEEP:
                merged.popitem(last=False)
            self.done_cmds = merged
            return n

    def _sse_loop(self) -> None:
        while not self._stop.is_set():
            code = None
            try:
                code = self._sse_once()
            except Exception:                                   # 끊김·형식 오류 — 다시 붙는다
                pass
            finally:
                self.sse_connected = False
                self._sse_sock = None
            if self._stop.is_set():
                return
            self.sse_fails += 1
            if code == 401:
                self._warn_once("sse:401", "UPLINK_AUTH_FAILED", f"웹이 장비 토큰을 거부(401, 명령 받기, {self.where}) — 60 s 마다 다시")
            elif code == 409:
                self._warn_once("sse:409", "UPLINK_CONFLICT", f"웹이 이 실행을 받지 않음(409, 명령 받기, {self.where}) — 10 s 마다 다시")
            self._stop.wait(retry_delay(code, self.sse_fails))

    def _sse_once(self) -> Optional[int]:
        c = self._conn(float(self.cfg["UPLINK_TIMEOUT_SEC"]))
        q = urlencode({"equipment": self.cfg["EQUIPMENT_ID"], "instance": self.hub.instance})
        c.request("GET", f"{self.base}/stream?{q}", headers=self._headers(Accept="text/event-stream"))
        sock = self._sse_sock = c.sock                         # getresponse 뒤에는 c.sock 이 비기도 한다(Connection: close)
        if self._stop.is_set():
            c.close()
            return None
        r = c.getresponse()
        if r.status != 200:
            r.read(65536)
            c.close()
            return r.status
        sock.settimeout(SSE_IDLE_S)
        self.sse_connected = True
        self.sse_fails = 0
        self._warned = {k for k in self._warned if not k.startswith("sse:")}
        ev, data = None, []
        try:
            while not self._stop.is_set():
                raw = r.fp.readline(1_000_000)
                if not raw:
                    return None                                  # 웹이 끊음
                line = raw.decode("utf-8", "replace").rstrip("\r\n")
                if line == "":
                    if ev == "command" and data:
                        try:
                            cmd = json.loads("\n".join(data))
                        except ValueError:
                            cmd = None
                        if isinstance(cmd, dict):
                            self.handle_command(cmd, "sse")
                        else:
                            self._note("warn", "UPLINK_BAD_COMMAND", "웹 명령 JSON 을 읽을 수 없음(sse)")
                    ev, data = None, []
                elif line.startswith(":"):
                    continue
                elif line.startswith("event:"):
                    ev = line[6:].strip()
                elif line.startswith("data:"):
                    data.append(line[6:] if line.startswith("data: ") else line[5:])
        finally:
            r.close()
            c.close()
        return None


def _dumps(obj: Any) -> bytes:
    return dumps_json(obj)
