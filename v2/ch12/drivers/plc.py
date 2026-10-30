"""챔버12 PLC 드라이버 — LS XBL-EMTA, Modbus TCP 192.168.1.2:502 직결(MOXA 미경유). v1 device/plc.py(AsyncPLC) 전부.

공유: PLC 하나를 CH1·CH2·플라즈마 클리닝·로봇·히터가 같이 쓴다(v1 main.py:282-283·330-333). v2 는 worker 하나가 포트를
  독점하고 요청마다 출처 태그(source: "CH1"/"CH2"/"PC"/"ROBOT"/"HEATER"/…)를 요청 이름·로그 이벤트에 싣는다(v1 _PLCProxy).
  proxy(source) 는 출처를 미리 넣은 얇은 래퍼.
연결·재시도·간격: plc_io.py(원시 연산 정책 — 무응답 1회로 소켓을 닫지 않음, 연속 3회째 재생성, 재연결 백오프 5 s, 명령 간격 150 ms).
  make_transport(·)·make_worker(·) 가 v1 정책대로 전송·worker 를 만든다(조립은 system 몫).

1초 묶음 읽기(source_read, v1 '저우선' 정책 — 재시도·재연결 없음) — 한 tick:
  ① FC3 D0..D11(DCV READ/WRITE 12개 — DC·RF 전원 측정) ② CH2 히터(CHn_SUPPORTS_HEATER): FC3 D110..D160, FC1 640..658
  ③ 코일 표(COILS 124개)를 블록 계획(PLC_COIL_LOG_BLOCK_PLAN, v1 json 1 = (256, 512))대로 묶은 FC1 블록 중 '매 tick 블록'
  ④ 번갈아 읽는 블록(코일 800 이상 = P 미러 800..879·부저·게이지 인터락) 매 tick ROTATE_PER_TICK 개 — 필드는 자기 품질.
  코일 로그 몫(③④)은 v1 코일 로거처럼 high 연속 타임아웃 > 0 이거나 low 연속 실패 ≥ PLC_COIL_LOG_BACKOFF_AFTER 면
  이번 tick 건너뜀(값 None). 블록 E403 이면 계획 한 단계 강등, 연속 완전 성공 PLC_COIL_LOG_PROMOTE_AFTER 회면 한 단계 승격.
  블록 짧은 응답은 v1 처럼 False/0 으로 채운다. 해독기(register_decoder)·DC/RF 제어는 이 읽기 값을 받는다.
하트비트: on_tick 에서 max(1, PLC_WATCHDOG_INTERVAL_S×0.75) = 6 s 마다 FC1(0,1) 1회(high). pause_watchdog 중(로봇 PLC 명령)·
  worker 가 다른 요청 중이면 건너뜀(연결돼 있으면 정상으로 셈). 결과로 링크 끊김 감지·복구 로그와 conn_change 이벤트(v1 문구).
쓰기: 모두 worker 요청(write, 정지는 safety). 허용 목록 밖 주소는 CommandBlocked(보내지 않음), P 미러 코일은 E403 거절.
  bench(writes_allowed=False)는 worker 가 BENCH_READ_ONLY 로 거부하고 Modbus 클라이언트도 읽기 전용(이중 보호).

다른 드라이버가 쓰는 API(heater·dc·rf·공정 엔진)
- 요청(결과는 on_done(Result) — value 에 읽은 값): write_coil(addr|이름, on), write_reg(addr|이름, value), read_coil/read_bit,
  read_reg/read_reg_name, read_coils_block(start, n), read_regs_block(start, n), pulse/press_switch, write_switch,
  gas/main_shutter/gate_valve/door/vent/turbo/lift_pin/lr_valve/rf_select/power_select/buzzer_stop/set,
  power_enable/power_write/power_apply/power_read, rf_enable/rf_write_w/rf_apply/rf_read_fwd_ref. 모두 kind·source·on_done 인자.
- worker 스레드 안(요청 fn·Sequence 단계)에서: io_read_coil(s, addr)·io_write_coil(s, addr, on)·io_read_reg·io_write_reg·
  io_read_coils(s, start, n, priority)·io_read_regs(·).
- allow_writes(coils=(), regs=()) — 허용 목록 추가(히터 WRITE_REGS·WRITE_COILS). register_decoder(blocks, decode, on_fail) —
  1초 읽기(PlcPoll: coils·regs 원시값)에서 자기 블록 값을 만든다. last_poll — 마지막 성공 읽기.
"""
from __future__ import annotations

import functools
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence as Seq, Tuple

from v2.common import modbus_tcp as MT
from v2.common.modbus_rtu import READ_COILS, READ_REGS, WRITE_COIL, WRITE_REG
from v2.common.snapshot import DeviceBlock, FieldDef, FType
from v2.common.transport import CommandBlocked, TcpSettings, TcpTransport
from v2.common.worker import Request, Result, Sequence, Status, Worker, WorkerEvent
from v2.ch12.drivers import plc_map as PM
from v2.ch12.drivers.plc_cmds import PlcCommands
from v2.ch12.drivers.plc_conv import cmd_grace_s, dac_code, dc_scale, linear_to_dac, rf_regs, rf_scale  # noqa: F401 (다시 내보냄)
from v2.ch12.drivers.plc_io import LinkState, PlcIo, PlcModbusError, RetryOpenTransport, code_of

DCV_REGS = (0, 12)                 # D0..D11
HEATER_REGS = (110, 51)            # D110..D160 (controller/heater_service.py:37-38)
HEATER_COILS = (640, 19)           # 640..658  (controller/heater_service.py:47-48)
ROTATE_MIN_ADDR = 800              # 이 주소 이상 코일은 번갈아 읽기(P 미러·부저·게이지 인터락)
ROTATE_PER_TICK = 1
READ_MARGIN_S = 0.12               # 블록 하나 더 읽을지: 마감까지 남은 시간 ≥ 명령 간격 + 이 값(응답 여유)
_ROT_COILS = tuple(n for n, a in PM.COIL_MAP.items() if a >= ROTATE_MIN_ADDR)

BUDGET = dict(
    queries_per_tick=5, bytes_tx=60, bytes_rx=253, min_gap_ms=150, fits_1s=True,
    alternate="번갈아 읽는 블록 4개(P 미러 800..879·M01000·M01500..M01800·M01900) 매 tick 1개 → 각 4 s 마다",
    note="DCV(33 B)·히터 레지스터(111 B)·히터 코일(12 B)·코일 1..481(70 B)·번갈아 1개(평균 27 B). 시작 간격 150 ms × 4 = 600 ms "
         "+ 응답 5회 — 한 tick 에 쓰기·하트비트 1~2개가 끼어도 900 ms 마감 안. 히터 미사용이면 3개. 실측은 bench(tools/bench_probe)",
)


@dataclass(frozen=True)
class PlcEvent:
    kind: str           # log / link_down / link_up / conn_change / connect_failed / coil_plan / heartbeat_fail …
    detail: str
    reason: Optional[str] = None
    code: Optional[str] = None          # v1 오류 코드(E401·E402·E403)
    data: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PlcPoll:
    t_wall: float
    t_mono: float
    coils: Dict[int, bool]
    regs: Dict[int, int]
    values: Dict[str, Any]


def make_transport(clock, cfg: Dict[str, Any], inner=None) -> RetryOpenTransport:
    """v1 연결: Modbus TCP(PLC_TCP_HOST:PORT, 접속 제한 PLC_TIMEOUT_S, SO_KEEPALIVE·TCP_NODELAY). inner 를 주면 그것(SimTransport)."""
    if inner is None:
        inner = TcpTransport(clock, TcpSettings(str(cfg["PLC_TCP_HOST"]), int(cfg["PLC_TCP_PORT"]),
                                                float(cfg["PLC_TIMEOUT_S"])))
    return RetryOpenTransport(inner, attempts=int(cfg["PLC_RECONNECT_RETRY"]) + 1,
                              delay_s=float(cfg["PLC_RECONNECT_DELAY_S"]), fg_attempts=int(cfg["PLC_FG_CONNECT_ATTEMPTS"]))


def make_worker(transport, cfg: Dict[str, Any], writes_allowed: bool, on_event=None) -> Worker:
    """무응답 판단·재생성은 드라이버(plc_io)가 하므로 worker 의 연속 무응답 끊기는 쓰지 않는다. 재연결 백오프는 v1 고정 5 s."""
    bo = float(cfg["PLC_RECONNECT_BACKOFF_S"]) or 0.0
    return Worker("plc", transport, writes_allowed=writes_allowed, fail_streak=1 << 30,
                  reconnect_start_s=bo, reconnect_max_s=bo, long_outage_s=1e12, probe_s=bo or 1.0,
                  log_every_s=600.0, on_event=on_event)


def coil_fields() -> tuple:
    out = []
    for n, a, m, ln in PM.COILS:
        out.append(FieldDef(PM.field_name(n), FType.BOOL, "", n, source=f"plc:FC1 {a} ({m})",
                            own_quality=a >= ROTATE_MIN_ADDR, summary="any", detail=True,
                            desc=f"코일 {a} = {m} (device/plc.py:{ln})"))
    for n, a in PM.P_MIRROR_MAP.items():
        out.append(FieldDef(PM.field_name(n), FType.BOOL, "", n, source=f"plc:FC1 {a} (P 미러)", own_quality=True,
                            summary="any", detail=True, desc=f"P 미러 코일 {a} (device/plc.py:225-231, 읽기 전용)"))
    for n, a, d, ln in PM.REGS:
        out.append(FieldDef(PM.field_name(n), FType.INT, "count", n, source=f"plc:FC3 {a} ({d})", detail=True,
                            desc=f"레지스터 {d} (device/plc.py:{ln})"))
    out.append(FieldDef("coil_plan", FType.INT, "", "코일 블록 계획", source="driver", detail=True,
                        desc="v1 PLC_COIL_LOG_BLOCK_PLAN 적응 계획 번호(0~3)"))
    return tuple(out)


def blocks() -> List[DeviceBlock]:
    return [DeviceBlock("plc", "PLC", coil_fields(), desc="LS XBL-EMTA Modbus TCP 192.168.1.2:502 — 코일 124·P 미러 5·D0~D11")]


_TAGGED = {"write_coil", "write_reg", "read_coil", "read_bit", "read_reg", "read_reg_name", "read_coils_block",
           "read_regs_block", "pulse", "press_switch", "write_switch", "gas", "main_shutter", "gate_valve", "door",
           "vent", "turbo", "lift_pin", "lr_valve", "rf_select", "power_select", "buzzer_stop", "set",
           "power_enable", "power_write", "power_apply", "power_read", "rf_enable", "rf_write_w", "rf_apply",
           "rf_read_fwd_ref"}


class PlcProxy:
    """출처를 미리 넣은 래퍼(v1 _PLCProxy 'CH1'/'CH2'/'PC')."""

    def __init__(self, drv: "PlcDriver", source: str) -> None:
        self._drv, self.source = drv, source

    def __getattr__(self, name):
        attr = getattr(self._drv, name)
        if name in _TAGGED:
            return functools.partial(attr, source=self.source)
        return attr


class PlcDriver(PlcCommands):
    def __init__(self, clock, worker: Worker, cfg: Dict[str, Any], writes_allowed: bool,
                 on_event: Optional[Callable[[PlcEvent], None]] = None, chamber: str = "shared",
                 heater: Optional[bool] = None, diag_probe: Optional[Callable[[str, int, float], str]] = None) -> None:
        self.clock, self.worker, self.cfg = clock, worker, cfg
        self.writes_allowed = writes_allowed
        self.on_event = on_event
        self.endpoint = f"{cfg['PLC_TCP_HOST']}:{cfg['PLC_TCP_PORT']}"
        self.link = LinkState(clock, lambda k, d, data: self.on_event and self.on_event(PlcEvent(k, d, None, None, data)),
                              self.endpoint)
        self.io = PlcIo(clock, cfg, self.link, read_only=not writes_allowed, log=lambda m: self._emit("log", m))
        self.heater = bool(cfg.get("CH2_SUPPORTS_HEATER", False)) if heater is None else bool(heater)
        self.diag_probe = diag_probe
        self.diag = ""
        self._diag_ts = None
        self.allowed_coils = set(PM.COIL_MAP.values())
        self.allowed_regs = {PM.REG_MAP[f"DCV_WRITE_{i}"] for i in range(4)}
        self.plan_idx = PM.default_plan_idx(cfg["PLC_COIL_LOG_BLOCK_PLAN"])
        self.plan_ok_streak = 0
        self._rot_i = 0
        self.decoders: List[tuple] = []          # (blocks, decode(poll)->dict, on_fail(err))
        self.last_poll: Optional[PlcPoll] = None
        self.hb_paused = False
        self._hb_last = clock.monotonic()
        self._disc_since = 0.0
        self._disc_alerted = False
        self._connect_fail_streak = 0
        self._disconnected_at = 0.0
        self._prev_worker_event = worker.on_event
        worker.on_event = self._on_worker_event

    # ───────── 공통 ─────────
    def _emit(self, kind: str, detail: str = "", reason: Optional[str] = None, code: Optional[str] = None, **data) -> None:
        if self.on_event:
            self.on_event(PlcEvent(kind, detail, reason, code, data))

    def _log(self, source: str, msg: str) -> None:
        self._emit("log", f"[{source}] {msg}" if source else msg, source=source)

    def proxy(self, source: str) -> PlcProxy:
        return PlcProxy(self, source)

    def allow_writes(self, coils: Seq[int] = (), regs: Seq[int] = ()) -> None:
        self.allowed_coils.update(int(a) for a in coils)
        self.allowed_regs.update(int(a) for a in regs)

    def register_decoder(self, blocks: Seq[str], decode: Callable[[PlcPoll], Dict[str, Any]],
                         on_fail: Optional[Callable[[str], None]] = None) -> None:
        self.decoders.append((tuple(blocks), decode, on_fail))

    def source_blocks(self) -> tuple:
        out = ["plc"]
        for b, _d, _f in self.decoders:
            out += [x for x in b if x not in out]
        return tuple(out)

    @property
    def connected(self) -> bool:
        return self.worker.connected

    # ───────── worker 이벤트 → 링크 상태(device/plc.py:907-977) ─────────
    def _on_worker_event(self, ev: WorkerEvent) -> None:
        now = self.clock.monotonic()
        if ev.kind == "connected":
            self.io.on_connected()
            self.link.note_up(f"[CH1&2] PLC 재연결 성공 ({self.endpoint})", self.diag_text())
            if self._connect_fail_streak > 0:
                down = (now - self._disconnected_at) if self._disconnected_at else 0.0
                self._emit("log", f"WARN PLC 재접속 성공 (실패 {self._connect_fail_streak}회, 끊긴 뒤 {down:.0f}초)")
            self._connect_fail_streak, self._disconnected_at = 0, 0.0
            self._emit("log", f"TCP 연결 성공: {self.endpoint} (unit={self.cfg['PLC_UNIT']})")
        elif ev.kind == "connect_failed":
            if not self._disconnected_at:
                self._disconnected_at = now
            self._connect_fail_streak += 1
            self._run_diag()
            n = getattr(self.worker.transport, "last_attempts", 1) or 1
            msg = f"Modbus TCP 연결 실패 ({self.endpoint}) - {ev.detail} (시도 {n}회, 분류={self.diag_text()})"
            self.link.note_down(f"접속 실패(시도 {n}회, 분류={self.diag_text()})", "connect")
            self._emit("connect_failed", msg, reason="LINK_DOWN", code="E401")
        elif ev.kind == "disconnected":
            if not self._disconnected_at:
                self._disconnected_at = now
            self.link.note_down("E401 연결 오류", "disconnect")
        if self._prev_worker_event:
            self._prev_worker_event(ev)

    def diag_text(self) -> str:
        """E401 문구 분류 — 값 없음·묵음(> PLC_DIAG_MIN_INTERVAL_S)은 '미확인'(device/plc.py:1000-1014)."""
        if not self.diag or self._diag_ts is None:
            return "미확인"
        if self.clock.monotonic() - self._diag_ts > float(self.cfg["PLC_DIAG_MIN_INTERVAL_S"]):
            return "미확인"
        return self.diag

    def _run_diag(self) -> None:
        """접속 최종 실패 원인 진단(PLC_DIAG_PROBE, 최소 간격 PLC_DIAG_MIN_INTERVAL_S). 진단 함수는 조립이 넣는다
        (sim·시험은 없음 → '미확인'). 실제 함수는 별도 스레드에서 connect_ex·ping(device/plc.py:1016-1094)."""
        if not (self.cfg["PLC_DIAG_PROBE"] and self.diag_probe):
            return
        now = self.clock.monotonic()
        if self._diag_ts is not None and now - self._diag_ts < float(self.cfg["PLC_DIAG_MIN_INTERVAL_S"]):
            return
        host, port = str(self.cfg["PLC_TCP_HOST"]), int(self.cfg["PLC_TCP_PORT"])
        try:
            self.diag = str(self.diag_probe(host, port, float(self.cfg["PLC_TIMEOUT_S"])))
        except Exception as e:
            self.diag = f"probe 실패: {type(e).__name__}"
        self._diag_ts = now
        self._emit("log", f"WARN PLC 진단: connect={self.diag}")

    # ───────── 하트비트(device/plc.py:1122-1214) ─────────
    @property
    def hb_period_s(self) -> float:
        return max(1.0, float(self.cfg["PLC_WATCHDOG_INTERVAL_S"]) * 0.75)

    def pause_watchdog(self) -> None:
        self.hb_paused = True

    def resume_watchdog(self) -> None:
        self.hb_paused = False

    def on_tick(self, tick=None) -> None:
        """TickLoop 의 tick 훅 — 하트비트 주기가 됐으면 ping 1회."""
        now = self.clock.monotonic()
        if now - self._hb_last < self.hb_period_s - 1e-9:
            return
        self._hb_last = now
        if self.hb_paused:
            return
        if self.worker.busy:
            if self.worker.connected:
                self._mark_conn_ok()
            return
        g = cmd_grace_s(self.cfg)
        self.worker.submit(Request("read", self._hb_fn, deadline=now + g, exec_timeout_s=g, name="plc:-:heartbeat",
                                   on_done=lambda r: self._mark_conn_ok() if r.status == Status.OK else self._mark_conn_fail()))

    def _hb_fn(self, s) -> bool:
        self.io.op(s, "heartbeat", 0, MT.pdu_read_coils(0, 1), READ_COILS, 1, "high")
        return True

    def _mark_conn_ok(self) -> None:
        self.link.note_up(f"[CH1&2] PLC 재연결 성공 ({self.endpoint})", self.diag_text())
        if self._disc_since:
            tail = ""
            if not self._disc_alerted:
                from v2.ch12.drivers.plc_io import format_suppressed
                tot, by = self.link.suppressed_snapshot()
                if tot:
                    tail = f" | 억제된 알림 {format_suppressed(tot, by)}"
            self._emit("log", f"WARN PLC 링크 복구 (끊김 {self.clock.monotonic() - self._disc_since:.1f}초, "
                              f"채팅알림={'발송' if self._disc_alerted else '미발송'})" + tail)
        if self._disc_alerted:
            self._emit("conn_change", f"[CH1&2] PLC 재연결 성공 ({self.endpoint})", connected=True)
        self._disc_since, self._disc_alerted = 0.0, False

    def _mark_conn_fail(self) -> None:
        now = self.clock.monotonic()
        if not self._disc_since:
            self._disc_since = now
            self.link.note_down("하트비트 실패", "heartbeat")
            self._emit("log", f"WARN PLC 링크 끊김 감지 (채팅 알림 임계 {float(self.cfg['PLC_DISCONNECT_ALERT_AFTER_S']):.0f}초)")
        if not self._disc_alerted and now - self._disc_since >= float(self.cfg["PLC_DISCONNECT_ALERT_AFTER_S"]):
            self._disc_alerted = True
            self._emit("conn_change", f"[CH1&2] PLC 연결 끊김 {int(now - self._disc_since)}초 경과, 재연결 실패 "
                                      f"({self.endpoint})", connected=False)

    # ───────── 1초 묶음 읽기 ─────────
    def _coil_ranges(self) -> Tuple[list, list]:
        idx, ranges = PM.coil_ranges_for_plan(PM.COIL_MAP.values(), self.plan_idx)
        self.plan_idx = idx
        always = [r for r in ranges if any(r[0] <= a < r[0] + r[1] for a in PM.COIL_MAP.values() if a < ROTATE_MIN_ADDR)]
        rot = [r for r in ranges if r not in always]
        rot.insert(0, (PM.P_MIRROR_COIL_MIN, PM.P_MIRROR_COIL_MAX - PM.P_MIRROR_COIL_MIN + 1))
        return always, rot

    def coil_log_backoff(self) -> bool:
        n = max(0, int(self.cfg["PLC_COIL_LOG_BACKOFF_AFTER"]))
        return self.io.consec > 0 or (n > 0 and self.io.consec_low >= n)

    def _read_block(self, s, kind: str, start: int, count: int, out: dict) -> None:
        if kind == "regs":
            vals = self.io.op(s, "read_regs_block", start, MT.pdu_read_regs(start, count), READ_REGS, count, "low", pad=True)
        else:
            vals = self.io.op(s, "read_coils_block", start, MT.pdu_read_coils(start, count), READ_COILS, count, "low",
                              pad=True)
        for i, v in enumerate(vals):
            out[start + i] = v

    def source_read(self, session, seq: int = 0) -> Dict[str, Any]:
        t_wall, t_mono = self.clock.wall(), self.clock.monotonic()
        skip_coils = self.coil_log_backoff()          # v1 코일 로거처럼 이번 tick 시작 때 판단(device/plc.py:1976-1985)
        regs: Dict[int, int] = {}
        coils: Dict[int, bool] = {}
        try:
            self._read_block(session, "regs", *DCV_REGS, regs)
            if self.heater:
                self._read_block(session, "regs", *HEATER_REGS, regs)
                self._read_block(session, "coils", *HEATER_COILS, coils)
        except Exception as e:
            self._fail_decoders(f"{code_of(e)} {e}")
            raise
        vals: Dict[str, Any] = {f"plc.{PM.field_name(n)}": regs.get(a) for n, a in PM.REG_MAP.items()}
        err: Optional[Exception] = None
        if skip_coils:
            self.plan_ok_streak = 0
            vals.update({f"plc.{PM.field_name(n)}": None for n, a in PM.COIL_MAP.items() if a < ROTATE_MIN_ADDR})
        else:
            always, rot = self._coil_ranges()
            pick = [rot[(self._rot_i + k) % len(rot)] for k in range(min(ROTATE_PER_TICK, len(rot)))]
            self._rot_i = (self._rot_i + len(pick)) % max(1, len(rot))
            try:
                for st, n in always + pick:
                    if session.deadline - self.clock.monotonic() < self.io.gap_s + READ_MARGIN_S:
                        break                 # 이번 tick 마감 안에 못 든다(계획 강등 뒤 블록이 많을 때) — 남은 블록은 다음 tick
                    self._read_block(session, "coils", st, n, coils)
                self._note_plan(True)
            except PlcModbusError as e:
                self._demote(str(e))
                err = e
            except Exception as e:
                self.plan_ok_streak = 0
                err = e
            for n, a in list(PM.COIL_MAP.items()) + list(PM.P_MIRROR_MAP.items()):
                if a in coils:
                    vals[f"plc.{PM.field_name(n)}"] = bool(coils[a])
                elif a < ROTATE_MIN_ADDR:
                    vals[f"plc.{PM.field_name(n)}"] = None          # 매 tick 필드는 못 읽었으면 빈 칸(묵은 값 아님)
        vals["plc.coil_plan"] = self.plan_idx
        poll = PlcPoll(t_wall, t_mono, coils, regs, vals)
        self.last_poll = poll
        for blocks_, decode, on_fail in self.decoders:
            try:
                vals.update(decode(poll) or {})
            except Exception as e:                     # 해독 오류가 읽기를 깨지 않게
                self._emit("log", f"WARN 해독 오류 {blocks_}: {e!r}")
        if err is not None:
            raise err
        return vals

    def _fail_decoders(self, why: str) -> None:
        for _b, _d, on_fail in self.decoders:
            if on_fail:
                try:
                    on_fail(why)
                except Exception:
                    pass

    def _note_plan(self, ok: bool) -> None:
        """연속 성공 PLC_COIL_LOG_PROMOTE_AFTER 회마다 기본 계획 쪽으로 한 단계 승격(device/plc.py:1559-1600)."""
        base = PM.default_plan_idx(self.cfg["PLC_COIL_LOG_BLOCK_PLAN"])
        need = int(self.cfg["PLC_COIL_LOG_PROMOTE_AFTER"])
        if not ok or self.plan_idx <= base:
            self.plan_ok_streak = 0
            return
        if need <= 0:
            return
        self.plan_ok_streak += 1
        if self.plan_ok_streak < need:
            return
        self.plan_ok_streak = 0
        new = max(base, self.plan_idx - 1)
        chk, _ = PM.coil_ranges_for_plan(PM.COIL_MAP.values(), new)
        if chk == new and new != self.plan_idx:
            self._emit("coil_plan", f"WARN PLC COIL LOG: block plan {self.plan_idx} -> {new} [promote after {need} ok]",
                       old=self.plan_idx, new=new)
            self.plan_idx = new

    def _demote(self, why: str) -> None:
        last = len(PM.COIL_BLOCK_PLANS) - 1
        if self.plan_idx >= last:
            return
        new, _ = PM.coil_ranges_for_plan(PM.COIL_MAP.values(), self.plan_idx + 1)
        if new != self.plan_idx:
            self._emit("coil_plan", f"WARN PLC COIL LOG: block plan {self.plan_idx} -> {new} [E403] {why}",
                       old=self.plan_idx, new=new, code="E403")
            self.plan_idx, self.plan_ok_streak = new, 0

    # ───────── worker 스레드 안 원시 연산(다른 드라이버도 씀) ─────────
    def _check_coil_write(self, addr: int) -> None:
        if PM.is_p_mirror(addr):
            raise PlcModbusError(f"P 미러 코일({addr}, M0050~M0054)은 읽기 전용 — 쓰기 거절")
        if addr not in self.allowed_coils:
            raise CommandBlocked(f"코일 {addr} 은 쓰기 허용 목록 밖 — 보내지 않음")

    def io_read_coil(self, s, addr: int, op: str = "read_coil") -> bool:
        return bool(self.io.op(s, op, addr, MT.pdu_read_coils(addr, 1), READ_COILS, 1, "high")[0])

    def io_write_coil(self, s, addr: int, on: bool, op: str = "write_coil") -> None:
        self._check_coil_write(int(addr))
        self.io.op(s, op, addr, MT.pdu_write_coil(int(addr), bool(on)), WRITE_COIL, 0, "high")

    def io_read_reg(self, s, addr: int, op: str = "read_reg") -> int:
        return int(self.io.op(s, op, addr, MT.pdu_read_regs(addr, 1), READ_REGS, 1, "high")[0])

    def io_write_reg(self, s, addr: int, value: int, op: str = "write_reg") -> None:
        if int(addr) not in self.allowed_regs:
            raise CommandBlocked(f"레지스터 D{int(addr)} 은 쓰기 허용 목록 밖 — 보내지 않음")
        self.io.op(s, op, addr, MT.pdu_write_reg(int(addr), int(value)), WRITE_REG, 0, "high")

    def io_read_coils(self, s, start: int, count: int, priority: str = "high") -> List[bool]:
        if count <= 0:
            return []
        return [bool(x) for x in self.io.op(s, "read_coils_block", start, MT.pdu_read_coils(start, count),
                                            READ_COILS, count, priority, pad=True)]

    def io_read_regs(self, s, start: int, count: int, priority: str = "high") -> List[int]:
        if count <= 0:
            return []
        return [int(x) for x in self.io.op(s, "read_regs_block", start, MT.pdu_read_regs(start, count),
                                           READ_REGS, count, priority, pad=True)]

    # ───────── 요청 ─────────
    def submit(self, name: str, fn: Callable, *, kind: str = "write", source: str = "",
               on_done: Optional[Callable[[Result], None]] = None, purge: bool = False,
               not_before: Optional[float] = None) -> Request:
        """요청 하나. 큐 대기 ≥ PLC_LOCK_WARN_MS·실행 ≥ PLC_IO_WARN_MS 면 v1 처럼 WARN 로그(device/plc.py:2286-2301).
        링크 down 중 연결계 실패(E401·E402)는 억제 건수로 센다(재연결 카드에 실림)."""
        g = cmd_grace_s(self.cfg)
        t_sub = self.clock.monotonic() if not_before is None else max(self.clock.monotonic(), not_before)
        rname = f"plc:{source or '-'}:{name}"

        def done(res: Result) -> None:
            if res.started is not None:
                waited = (res.started - t_sub) * 1000.0
                if waited >= float(self.cfg["PLC_LOCK_WARN_MS"]):
                    self._emit("log", f"WARN lock-wait {waited:.0f} ms (op={rname})")
                if res.finished is not None and (res.finished - res.started) * 1000.0 >= float(self.cfg["PLC_IO_WARN_MS"]):
                    self._emit("log", f"WARN in-lock IO {(res.finished - res.started) * 1000.0:.0f} ms (op={rname})")
            if res.status != Status.OK and self.link.down and res.reason in ("DEVICE_NO_RESPONSE", "LINK_DOWN"):
                self.link.note_suppressed("E402" if res.reason == "DEVICE_NO_RESPONSE" else "E401")
            if on_done:
                on_done(res)
        req = Request(kind, fn, deadline=t_sub + g, exec_timeout_s=g, on_done=done, name=rname, purge=purge,
                      not_before=not_before)
        self.worker.submit(req)
        return req

    def write_coil(self, addr, on: bool, *, source: str = "", on_done=None, kind: str = "write", name: str = "") -> Request:
        a = PM.resolve(addr)
        label = name or (addr if isinstance(addr, str) else str(a))

        def fn(s):
            self.io_write_coil(s, a, on)
            self._log(source, f"set {label} <- {bool(on)} (addr={a})")
        return self.submit(f"{label}={int(bool(on))}", fn, kind=kind, source=source, on_done=on_done)

    def write_reg(self, addr, value: int, *, source: str = "", on_done=None, kind: str = "write") -> Request:
        a = PM.resolve(addr)

        def fn(s):
            self.io_write_reg(s, a, int(value))
            self._log(source, f"write reg {addr} (addr={a}) <- {int(value)}")
        return self.submit(f"D{a}={int(value)}", fn, kind=kind, source=source, on_done=on_done)

    def read_coil(self, addr, *, source: str = "", on_done=None, kind: str = "read") -> Request:
        a = PM.resolve(addr)

        def fn(s):
            v = self.io_read_coil(s, a)
            self._log(source, f"read {addr} (addr={a}) -> {v}")
            return v
        return self.submit(f"read {addr}", fn, kind=kind, source=source, on_done=on_done)

    read_bit = read_coil

    def read_reg(self, addr, *, source: str = "", on_done=None, kind: str = "read") -> Request:
        a = PM.resolve(addr)
        return self.submit(f"read D{a}", lambda s: self.io_read_reg(s, a), kind=kind, source=source, on_done=on_done)

    read_reg_name = read_reg

    def read_coils_block(self, start: int, count: int, *, priority: str = "high", source: str = "", on_done=None) -> Request:
        return self.submit(f"FC1 {start}+{count}", lambda s: self.io_read_coils(s, start, count, priority),
                           kind="read", source=source, on_done=on_done)

    def read_regs_block(self, start: int, count: int, *, priority: str = "high", source: str = "", on_done=None) -> Request:
        return self.submit(f"FC3 {start}+{count}", lambda s: self.io_read_regs(s, start, count, priority),
                           kind="read", source=source, on_done=on_done)

    def pulse(self, addr, ms: Optional[int] = None, *, source: str = "", on_done=None, name: str = "") -> Sequence:
        """순간 스위치: write(True) → max(0.03, ms/1000) s → write(False)(device/plc.py:1633-1637). 사이에 다른 I/O 가 낄 수 있다."""
        a = PM.resolve(addr)
        ms = int(self.cfg["PLC_CMD_PULSE_MS"]) if ms is None else int(ms)
        label = name or (addr if isinstance(addr, str) else str(a))
        self._log(source, f"pulse {label} (addr={a}, {ms}ms)")

        def done(nm, reason, results):
            if on_done:
                on_done(results[-1] if results else Result(Request("write", lambda s: None), Status.REJECTED,
                                                           reason=reason))
        seq = Sequence(f"plc:{source or '-'}:pulse {label}", [("on", lambda s: self.io_write_coil(s, a, True)),
                                                              ("off", lambda s: self.io_write_coil(s, a, False))],
                       gap_s=max(0.03, ms / 1000.0), step_timeout_s=cmd_grace_s(self.cfg), on_done=done)
        self.worker.submit_sequence(seq)
        return seq

    def write_switch(self, name, on: bool, *, momentary: bool = False, pulse_ms: Optional[int] = None,
                     source: str = "", on_done=None, kind: str = "write"):
        if momentary:
            return self.pulse(name, pulse_ms, source=source, on_done=on_done)
        return self.write_coil(name, on, source=source, on_done=on_done, kind=kind)

    def press_switch(self, name, pulse_ms: Optional[int] = None, *, source: str = "", on_done=None):
        return self.write_switch(name, True, momentary=True, pulse_ms=pulse_ms, source=source, on_done=on_done)
