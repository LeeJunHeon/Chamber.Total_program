"""챔버12 명령 처리 한 곳 — 웹·로컬 화면·로봇 명령이 모두 Ch12Core.command 를 지난다(사용자 지시 3장 (4), 챔버K core.command).

command(kind, args, source, cid, by) → CmdResult(.ok·.reason·.detail·.value)
  출처 검사(commands.allowed_sources — COMMAND_SOURCES_LOCAL_ALL) → 인자 검사(ArgDef 형식·범위 → BAD_ARGS + detail) →
  상태 검사(대상 엔진 상태 이름 — process_states) → bench 쓰기 거부(BENCH_READ_ONLY, benchOk 아닌 명령) →
  인터락 판단(여기에만: v1 시작 가드는 엔진 start_guard·RunState 로, 게이트·busy 는 엔진·RobotOps 가 v1 그대로) → 실행(엔진 API) → 결과.
- 엔진이 문구와 함께 거부하면(reject_detail·StartResult.message·TSP message) 그 문구를 detail 로(300자) — 챔버K _why 와 같다.
- cmd_result 메시지는 core 가 내지 않는다(결정 D81 — hub.run_command). system.command = core.command.
- 여러 스레드(로컬 서버·웹 연결·로봇 API·시험)가 동시에 불러도 잠금 하나로 하나씩(검사와 실행 사이에 다른 명령이 끼지 않게).
  처리는 큐에 넣기·상태 바꾸기·스레드 띄우기뿐이라 짧다. 정지 명령(PROCESS_STOP·PC_STOP·TSP_STOP·ALL_STOP·POWER_OFF_ONLY)은 잠금을
  기다리지 않는다(엔진 stop 이 자기 잠금으로 안전). 로봇 명령(ROBOT_*)도 잠금 밖 — RobotOps 가 v1 잠금(lock_ch·PLC 구간)을 가지고
  초 단위로 기다리므로(게이트·척·진공 시퀀스) 그동안 STOP 이 막히지 않게. 로봇 START_SPUTTER 의 시작 가드는 어댑터가
  start_chamber 를 불러(잠금 안) 웹 시작과 같은 판단을 한다.
- 결과 value: 로봇 명령은 RobotOps.handle 의 (reply_cmd, body, log) — robot_api.dispatch_robot 이 그대로 돌려준다(v1 응답 그대로).
- by(요청자): 웹은 requestedBy, 로컬 화면 "local", 로봇 "robot". 엔진 소유자(owner): web·local·robot(출처에서).
"""
from __future__ import annotations

import os
import threading
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from v2.common.commands import Source
from v2.common.worker import Status
from v2.ch12 import commands as C
from v2.ch12 import process_states as PS

LOCK_FREE = frozenset({"PROCESS_STOP", "PC_STOP", "TSP_STOP", "ALL_STOP", "POWER_OFF_ONLY"})
SRC_WORD = {Source.WEB: "web", Source.HMI: "local", Source.ROBOT: "robot", Source.INTERNAL: "internal"}
DETAIL_MAX = 300
UI_RECIPE_LOAD_TIMEOUT_S = 30.0          # chamber_runtime.py:3428-3433 asyncio.wait_for(…, 30.0)


@dataclass
class CmdResult:
    ok: bool
    reason: Optional[str] = None
    detail: str = ""
    value: Any = None


class Rejected(Exception):
    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(reason)
        self.reason, self.detail = reason, str(detail or "")[:DETAIL_MAX]


def as_source(src: Any) -> Source:
    if isinstance(src, Source):
        return src
    s = str(getattr(src, "value", src) or "").lower()
    return {"local": Source.HMI}.get(s) or Source(s)


class Ch12Core:
    def __init__(self, system, proc: Dict[str, Any], *, pc=None, tsp=None, heater=None, thickness=None,
                 presputter=None, safety=None, run_state=None, robot_ops=None) -> None:
        self.sys, self.cfg, self.clock = system, system.cfg, system.clock
        self.proc = dict(proc)
        self.pc, self.tsp, self.heater = pc, tsp, heater
        self.thickness = dict(thickness or {})
        self.presputter = dict(presputter or {})
        self.safety, self.run_state, self.robot_ops = safety, run_state, robot_ops
        self._lock = threading.RLock()
        self.last_ui_params: Dict[str, dict] = {}       # 마지막 화면 값(프리스퍼터 'UI 현재값' — v1 화면 칸 자리)
        self.counts: Dict[str, int] = {}
        self.handlers: Dict[str, Callable[[dict, Source, str], Any]] = self._handlers()

    @property
    def writes_allowed(self) -> bool:
        return bool(self.sys.policy.device_writes)

    @staticmethod
    def default_by(source: Source, args: dict) -> str:
        if source == Source.HMI:
            return "local"
        if source == Source.ROBOT:
            return "robot"
        return str(args.get("requestedBy") or args.get("by") or getattr(source, "value", source))

    # ═══════════════════════ 처리 한 곳 ═══════════════════════
    def command(self, kind: str, args: Optional[dict] = None, source: Any = Source.WEB, cid: Any = None,
                by: Optional[str] = None) -> CmdResult:
        a = dict(args or {})
        try:
            src = as_source(source)
        except ValueError:
            return CmdResult(False, "SOURCE_NOT_ALLOWED", f"출처 {source!r}")
        by = str(by) if by else self.default_by(src, a)
        self.counts[kind] = self.counts.get(kind, 0) + 1
        try:
            if kind in LOCK_FREE or kind.startswith("ROBOT_"):
                value = self._run(kind, a, src, by)
            else:
                with self._lock:
                    value = self._run(kind, a, src, by)
        except Rejected as e:
            return CmdResult(False, e.reason, e.detail)
        except Exception as e:                       # 명령 하나의 오류가 서버·웹 연결 스레드를 멈추지 않는다
            self._emit("command", "internal_error", f"{kind}: {e!r}", "error", "INTERNAL_ERROR")
            return CmdResult(False, "INTERNAL_ERROR", repr(e)[:DETAIL_MAX])
        return CmdResult(True, None, "", value)

    def _run(self, kind: str, a: dict, src: Source, by: str) -> Any:
        c = C.get(kind)
        if c is None:
            raise Rejected("UNKNOWN_COMMAND", kind)
        if src not in C.allowed_sources(c, self.cfg):
            raise Rejected("SOURCE_NOT_ALLOWED", f"{kind} — {getattr(src, 'value', src)} 에서 받지 않음")
        bad = C.check_args(c, a, self.cfg)
        if bad:
            raise Rejected("BAD_ARGS", bad)
        st = self.state_of(c, a)
        if C.ANY not in c.states and st not in c.states:
            raise Rejected("STATE_NOT_ALLOWED", f"{kind} — 지금 상태 {st}")
        if not self.writes_allowed and not c.bench_ok:
            raise Rejected("BENCH_READ_ONLY", "bench 모드 — 장비 쓰기 금지(읽기 전용 — 조작은 장비 앞면으로)")
        h = self.handlers.get(kind)
        if h is None:
            raise Rejected("UNKNOWN_COMMAND", f"{kind} — 처리기 없음")
        r = h(a, src, by)
        if isinstance(r, str):
            raise Rejected(r)
        return r

    # ═══════════════════════ 대상·상태 ═══════════════════════
    def chamber(self, a: dict, key: str = "ch") -> Any:
        ch = f"ch{int(a.get(key) or 0)}"
        e = self.proc.get(ch)
        if e is None:
            raise Rejected("NOT_SUPPORTED", f"{ch} 엔진 없음")
        return e

    def state_of(self, c, a: dict) -> str:
        if c.target in ("chamber",) and a.get("ch") is not None:
            return PS.chamber_state(self.proc.get(f"ch{int(a['ch'])}"))
        if c.target == "pc" and self.pc is not None:
            return f"pc.{getattr(self.pc, 'phase', 'idle') or 'idle'}"
        if c.target == "tsp" and self.tsp is not None:
            return f"tsp.{getattr(self.tsp, 'phase', 'idle') or 'idle'}"
        return C.ANY

    @staticmethod
    def why(engine, call: Callable[[], Any]) -> None:
        """엔진 호출 — 거부 사유 코드(+ engine.reject_detail 문구)면 Rejected."""
        if hasattr(engine, "reject_detail"):
            engine.reject_detail = ""
        r = call()
        if r:
            raise Rejected(str(r), str(getattr(engine, "reject_detail", "") or ""))

    def _emit(self, source: str, kind: str, detail: str, level: str = "info", code: Optional[str] = None) -> None:
        try:
            self.sys.emit(source, kind, detail, level=level, code=code)
        except Exception:
            pass

    def _plc_up(self) -> bool:
        try:
            return bool(self.sys.workers["plc"].connected)
        except Exception:
            return False

    # ═══════════════════════ 챔버 시작(인터락 판단 — 웹·로컬·로봇·프리스퍼터 공통) ═══════════════════════
    def start_chamber(self, ch: int, *, params: Optional[dict] = None, source: str = "web", by: Optional[str] = None,
                      owner: str = "web", origin: str = "ui", origin_meta: Optional[dict] = None,
                      host_cb: Optional[Callable[[bool, Optional[str], str], None]] = None) -> None:
        """v1 _handle_start_clicked 가드 순서: 쿨다운 → 러너 바쁨 → 같은 CH PC 실행 중 → 컨트롤러 바쁨(엔진 start_guard 안) → 시작.
        host_cb(ok, code, detail) = 로봇 시작 가드 신호(v1 _host_report_start — 코드는 늘 None, 문구로 분류)."""
        with self._lock:
            e = self.proc.get(f"ch{int(ch)}")
            if e is None:
                raise Rejected("NOT_SUPPORTED", f"CH{ch} 엔진 없음")
            e.on_host_start = (lambda ok, msg: host_cb(bool(ok), None, str(msg))) if host_cb is not None else None
            e.reject_detail = ""
            why = e.start_guard()
            if why:
                if host_cb is not None:
                    host_cb(False, None, e.reject_detail)
                raise Rejected(why, e.reject_detail)
            rs = self.run_state
            if rs is not None and rs.is_running("pc", int(ch)):
                e.popup("실행 오류", f"CH{ch} Plasma Cleaning이 실행 중입니다.\n끝난 뒤 시작하십시오.")
                msg = f"CH{ch} Plasma Cleaning 실행 중"
                if host_cb is not None:
                    host_cb(False, None, msg)
                raise Rejected("PC_RUNNING", msg)
            if params is not None:
                self.last_ui_params[e.ch] = dict(params)
            try:
                self.why(e, lambda: e.start(source=source, by=by, owner=owner, origin=origin, origin_meta=origin_meta,
                                            params=params))
            except Rejected:
                raise
            except Exception as ex:                          # v1 _handle_start_clicked except(4483-4488)
                import traceback
                tb = "".join(traceback.format_exception(type(ex), ex, ex.__traceback__)).rstrip()
                e.log("MAIN", f"_handle_start_clicked 예외:\n{tb}")
                if host_cb is not None:
                    host_cb(False, None, f"exception: {ex!r}")
                e.popup("실행 오류", "공정 시작 준비 중 내부 오류가 발생했습니다.\n로그를 확인하세요.", level="error")
                raise Rejected("INTERNAL_ERROR", f"exception: {ex!r}")

    def _load_recipe_ui(self, e, path: str):
        """화면 Process List 파일 열기(_handle_process_list_clicked_async 3395-3438) — NAS 지연 대비 30 s 제한(넘으면 로그
        'CSV 로드 30초 timeout (NAS 응답 지연): …' 후 적재 안 함). 그 밖 예외 문구는 load_recipe_file('파일 처리 오류: …')."""
        from v2.ch12.recipe import RecipeLoad, load_recipe_file
        clk = self.clock
        box: Dict[str, Any] = {}

        def work() -> None:
            try:
                box["v"] = load_recipe_file(path, origin="ui")
            except Exception as ex:                         # noqa: BLE001
                box["v"] = RecipeLoad(path, logs=[("File", f"파일 처리 오류: {ex}")], code="E209", message=f"파일 처리 오류: {ex}")
            clk.notify()
        clk.start_thread(work, "recipe-load-ui")
        clk.wait_until(clk.monotonic() + UI_RECIPE_LOAD_TIMEOUT_S, lambda: "v" in box)
        if "v" not in box:
            msg = f"CSV 로드 30초 timeout (NAS 응답 지연): {path}"
            e.log("File", msg)
            return RecipeLoad(path, code="E227", message=msg)
        return box["v"]

    def presputter_start_ui(self, ch: str) -> bool:
        """프리스퍼터 'UI 현재값' 시작(v1 start_presputter_from_ui) — 공정 중이면 False, 아니면 시작 가드 결과와 무관하게 True(UI-143)."""
        e = self.proc.get(ch)
        if e is None or e.is_running:
            if e is not None:
                e.log("MAIN", f"[CH{e.n}] PreSputter: 이미 공정 중입니다.")
            return False
        try:
            self.start_chamber(e.n, params=dict(self.last_ui_params.get(ch) or {}), source="local", owner="local",
                               origin="presputter")
        except Rejected:
            pass
        e.log("MAIN", f"[CH{e.n}] PreSputter 자동 시작 (UI 현재값)")
        return True

    def presputter_start_recipe(self, ch: str, rows, name: str) -> Optional[str]:
        """프리스퍼터 전용 레시피 시작 — v1 start_with_recipe_string(…, origin='presputter') 이 UI Start 와 같은 _handle_start_clicked
        를 지나듯(chamber_runtime.py:7996-8006, pre_sputter_runtime.py:338) 적재 → start_chamber(쿨다운·러너 바쁨·같은 CH PC 실행 중).
        거부 사유 코드, 시작했으면 None."""
        e = self.proc.get(ch)
        if e is None:
            return "NOT_SUPPORTED"
        why = e.load(rows, source="local", by="local", name=name)
        if why:
            return why
        try:
            self.start_chamber(e.n, source="local", by="local", owner="local", origin="presputter")
        except Rejected as r:
            return r.reason
        return None

    # ═══════════════════════ 처리기 ═══════════════════════
    def _handlers(self) -> Dict[str, Callable]:
        h: Dict[str, Callable] = {}
        w = SRC_WORD

        def owner_of(src: Source) -> str:
            return {"web": "web", "local": "local", "robot": "robot"}.get(w.get(src, "web"), "local")

        # ── 정지 ──
        def process_stop(a, src, by):
            self.chamber(a).stop(source=w[src], by=by, user=True)
        h["PROCESS_STOP"] = process_stop

        def all_stop(a, src, by):
            if self.safety is None:
                raise Rejected("CORE_NOT_READY", "안전 엔진 없음")
            return self.safety.all_stop(source=w[src], by=by)
        h["ALL_STOP"] = all_stop

        def pc_stop(a, src, by):
            self._need(self.pc, "플라즈마 클리닝").stop(source=w[src])
        h["PC_STOP"] = pc_stop

        def tsp_stop(a, src, by):
            self._need(self.tsp, "TSP").stop(source=w[src])
        h["TSP_STOP"] = tsp_stop

        # ── 챔버 공정 ──
        def process_start(a, src, by):
            e = self.chamber(a)
            self.start_chamber(e.n, params=a.get("params"), source=w[src], by=by, owner=owner_of(src))
        h["PROCESS_START"] = process_start

        def recipe_load(a, src, by):
            e = self.chamber(a)
            name = str(a.get("name") or "")
            if isinstance(a.get("blocks"), list):
                return self.why(e, lambda: e.load({"blocks": a["blocks"], "name": name}, source=w[src], by=by, name=name))
            if isinstance(a.get("rows"), list):
                return self.why(e, lambda: e.load(a["rows"], source=w[src], by=by, name=name))
            if a.get("path"):
                r = self._load_recipe_ui(e, str(a["path"]))
                for s, m in r.logs:
                    e.log(s, m)
                if not r.ok:
                    if r.code == "E230":                       # v1 UI: 로그 + 경고 창 '레시피 오류'(3443-3450)
                        e.popup("레시피 오류", r.message)
                    raise Rejected(r.code or "RECIPE_INVALID", r.message)
                return self.why(e, lambda: e.load(r.rows, source=w[src], by=by,
                                                   name=name or os.path.basename(str(a["path"]))))
            raise Rejected("BAD_ARGS", "rows·blocks·path 중 하나 필요")
        h["RECIPE_LOAD"] = recipe_load

        def recipe_clear(a, src, by):
            e = self.chamber(a)
            if e.busy:
                raise Rejected("BUSY", "공정 중 — 레시피를 해제하지 않음")
            e.clear()
        h["RECIPE_CLEAR"] = recipe_clear

        def adjust(a, src, by):
            e = self.chamber(a)
            self.why(e, lambda: e.adjust(str(a["device"]), str(a["item"]), a.get("value"), source=w[src], by=by))
        h["PROCESS_ADJUST"] = adjust
        h["BLOCK_SKIP"] = lambda a, src, by: self.why(self.chamber(a), lambda: self.chamber(a).skip_block(w[src], by))
        h["PROCESS_PAUSE"] = lambda a, src, by: self.why(self.chamber(a), lambda: self.chamber(a).pause(w[src], by))
        h["PROCESS_RESUME"] = lambda a, src, by: self.why(self.chamber(a),
                                                          lambda: self.chamber(a).resume_pause(w[src], by))
        h["SEQUENCE_EDIT"] = lambda a, src, by: self.why(self.chamber(a),
                                                         lambda: self.chamber(a).edit_sequence(dict(a["op"]), w[src], by))

        def error_clear(a, src, by):
            e = self.chamber(a)
            if self.run_state is not None:
                self.run_state.clear_error("chamber", e.n)
            e.error = None
        h["CHAMBER_ERROR_CLEAR"] = error_clear

        def ps_of(a):
            p = self.presputter.get(f"ch{int(a['ch'])}")
            if p is None:
                raise Rejected("NOT_SUPPORTED", f"CH{a['ch']} 프리스퍼터 없음")
            return p

        def ps_reserve(a, src, by):
            if not ps_of(a).schedule(str(a["time"])):
                raise Rejected("TIME_FORMAT", "시간 형식 오류 (HH:MM)")
        h["PRESPUTTER_RESERVE"] = ps_reserve
        h["PRESPUTTER_CANCEL"] = lambda a, src, by: ps_of(a).stop()
        h["PRESPUTTER_RECIPE"] = lambda a, src, by: ps_of(a).set_recipe_path(str(a.get("path") or "") or None)

        # ── 플라즈마 클리닝 ──
        def pc_rows(a):
            if isinstance(a.get("rows"), list):
                return a["rows"], str(a.get("path") or "")
            if a.get("path"):
                from v2.ch12 import pc_recipe as PR
                p = str(a["path"])
                row = PR.read_first_row(p, self.pc.log)
                return ([row] if row else []), p
            return None, ""

        def pc_start(a, src, by):
            pc = self._need(self.pc, "플라즈마 클리닝")
            rows, _p = pc_rows(a)
            if rows is not None and not rows:
                raise Rejected("RECIPE_EMPTY", "데이터 행이 없습니다.")
            r = pc.start(params=a.get("params"), rows=rows, ch=a.get("ch"), source=w[src], by=by, origin="ui")
            if not r.accepted:
                msg = str(r.message or "")
                raise Rejected(r.code or ("COOLDOWN" if "대기" in msg else "PROCESS_RUNNING"), msg)
        h["PC_START"] = pc_start

        def pc_select(a, src, by):
            ok, msg = self._need(self.pc, "플라즈마 클리닝").select_channel(int(a["ch"]))
            if not ok:
                raise Rejected("CHANNEL_SWITCH_RUNNING", msg)
        h["PC_SELECT_CH"] = pc_select

        def pc_load(a, src, by):
            pc = self._need(self.pc, "플라즈마 클리닝")
            rows, p = pc_rows(a)
            if rows is None:
                raise Rejected("BAD_ARGS", "rows·path 중 하나 필요")
            ok, msg = pc.load_recipe_rows(rows, p)
            if not ok:
                raise Rejected("RECIPE_EMPTY", msg)
        h["PC_RECIPE_LOAD"] = pc_load
        h["PC_ERROR_CLEAR"] = lambda a, src, by: self._need(self.pc, "플라즈마 클리닝").clear_error(a.get("ch"))

        def power_select(a, src, by):
            if not self._plc_up():
                raise Rejected("LINK_DOWN", "PLC 연결 없음")
            on = bool(a["on"])
            self._emit("pc", "log", f"POWER_SELECT(SW) <- {on}")

            def done(res):
                if getattr(res, "status", Status.OK) != Status.OK:
                    self._emit("pc", "log", f"Power Select 실패(PLC 연결 확인): {res.error or res.reason!r}", "warn")
            self.sys.shared.plc.proxy(w[src].upper()).power_select(on=on, on_done=done)
        h["POWER_SELECT"] = power_select

        # ── TSP ──
        def tsp_start(a, src, by):
            p = {k: a[k] for k in ("target", "cycles") if a.get(k) is not None}
            r = self._need(self.tsp, "TSP").start(p, source=owner_of(src), by=by)
            if not r.get("ok"):
                raise Rejected(str(r.get("reason") or "PROCESS_RUNNING"), str(r.get("message") or ""))
        h["TSP_START"] = tsp_start

        def tsp_schedule(a, src, by):
            sch = getattr(self._need(self.tsp, "TSP"), "schedule", None)
            if sch is None:
                raise Rejected("NOT_SUPPORTED", "TSP 예약 없음")
            if a.get("time"):
                from v2.ch12.pre_sputter import parse_hhmm
                if parse_hhmm(a["time"])[0] is None:
                    raise Rejected("TIME_FORMAT", "예약 시각 형식 오류(예: 05:00)")
                sch.set_time_text(str(a["time"]))
            sch.toggle(bool(a["on"]))
        h["TSP_SCHEDULE"] = tsp_schedule

        # ── 히터 패널(CH2) ──
        def heater():
            if self.heater is None:
                raise Rejected("NOT_SUPPORTED", "CH2 히터 없음(CH2_SUPPORTS_HEATER)")
            return self.heater

        def later(src_name: str, title: str):
            def done(ok, why=""):
                if not ok or why:
                    self._emit(src_name, "panel_result", f"{title}: {why}", "info" if ok else "warn")
            return done

        def heater_onoff(a, src, by):
            hh = heater()
            if not hh.status().get("run"):
                v, err = hh.panel_target(a.get("target"))
                if err:
                    raise Rejected("BAD_ARGS", err)
            hh.panel_onoff(a.get("target"), on_done=later("heater", "히터 ON/OFF"))
        h["HEATER_ONOFF"] = heater_onoff

        def heater_apply(a, src, by):
            hh = heater()
            v, err = hh.panel_target(a.get("target"))
            if err:
                raise Rejected("BAD_ARGS", err)
            hh.panel_apply(a.get("target"), on_done=later("heater", "히터 적용"))
        h["HEATER_APPLY"] = heater_apply

        def heater_reset(a, src, by):
            hh = heater()
            if hh.status().get("run"):
                raise Rejected("HEATER_REJECTED", "히터 RUN 이 켜져 있습니다.\n먼저 히터를 OFF 한 뒤 리셋하십시오.")
            hh.panel_reset(on_done=later("heater", "히터 리셋"))
        h["HEATER_RESET"] = heater_reset

        # ── 두께 모니터 ──
        def th_of(a):
            ch = f"ch{int(a.get('ch') or 2)}"
            t = self.thickness.get(ch)
            if t is None or not getattr(t, "supported", True):
                raise Rejected("NOT_SUPPORTED", f"{ch} 두께 모니터 없음")
            return t, self.proc.get(ch)

        def th_zero(a, src, by):
            t, e = th_of(a)
            box: list = []
            t.cmd_zero(bool(e is not None and e.busy), on_done=lambda ok, msg="": box.append((ok, msg)) or (
                None if ok else self._emit("thickness", "panel_result", f"영점 실패: {msg}", "warn")))
            if box and not box[0][0]:
                raise Rejected("THICKNESS_BLOCKED", box[0][1])
        h["THICKNESS_ZERO"] = th_zero

        def th_factor_save(a, src, by):
            t, _e = th_of(a)
            ok, msg, _bad = t.factor_save(a["tooling"], a["materials"])
            if not ok:
                raise Rejected("BAD_ARGS", msg)
        h["THICKNESS_FACTOR_SAVE"] = th_factor_save

        def th_factor_write(a, src, by):
            t, e = th_of(a)
            box: list = []

            def done(d):
                box.append(d)
                if not d.get("ok"):
                    self._emit("thickness", "panel_result", str(d.get("text") or ""), "warn")
            t.factor_write(str(a["chuck"]), str(a["gun"]), a["tooling"], a["material"], bool(e is not None and e.busy),
                           on_done=done)
            if box and not box[0].get("ok"):
                raise Rejected("THICKNESS_BLOCKED", str(box[0].get("text") or ""))
        h["THICKNESS_FACTOR_WRITE"] = th_factor_write

        def th_materials(a, src, by):
            t, _e = th_of(a)
            r = t.materials_save(a["rows"], a.get("used_names") or (), confirmed=bool(a.get("confirmed")))
            if not r.get("ok"):
                raise Rejected("BAD_ARGS", r.get("confirm") or r.get("text") or "")
        h["THICKNESS_MATERIALS_SAVE"] = th_materials

        def th_reload(a, src, by):
            th_of(a)[0].reload_factors()
        h["THICKNESS_RELOAD"] = th_reload

        # ── 공유 ──
        def plc_button(a, src, by):
            if not self._plc_up():
                raise Rejected("LINK_DOWN", "PLC 연결 없음")
            b = {x.target: x for x in C.PLC_BUTTONS}[a["target"]]
            on = bool(a["on"])
            busy = [ch for ch, e in self.proc.items() if e is not None and e.busy]
            if busy:
                self._emit("command", "plc_manual", f"공정 중 PLC 버튼 {b.name} → {on}({w[src]} {by}, 공정 {','.join(busy)})",
                           "info", "PLC_MANUAL_IN_PROCESS")
            self.sys.shared.plc.proxy(w[src].upper()).write_switch(b.name, on)
        h["PLC_BUTTON"] = plc_button

        def buzz_stop(a, src, by):
            if not self._plc_up():
                raise Rejected("LINK_DOWN", "PLC 연결 없음")
            self.sys.shared.plc.proxy(w[src].upper()).buzzer_stop()
        h["BUZZ_STOP"] = buzz_stop

        def runtime_dump(a, src, by):
            from v2.ch12.runtime_dump_hook import dump_runtime        # v1 _request_runtime_dump_internal 문구·상태 그대로
            path = dump_runtime(self.sys, str(a.get("reason") or "manual_button"))
            if path is None:
                raise Rejected("DUMP_BUSY", "Runtime dump skipped/failed — 로그(DUMP) 확인")
            return {"path": path}
        h["RUNTIME_DUMP"] = runtime_dump
        h["LOG_CLEAR"] = lambda a, src, by: self._emit("ui", "log_clear", str(a.get("target") or "all"), "info", "LOG_CLEAR")

        def config(kind):
            def run(a, src, by):
                cfg = getattr(self.sys, "config", None)
                if cfg is None:
                    raise Rejected("UNKNOWN_COMMAND", "설정 적용(config_apply)이 붙지 않음")
                return (cfg.apply if kind == "apply" else cfg.rollback)(a, by) or None
            return run
        h["CONFIG_APPLY"] = config("apply")
        h["CONFIG_ROLLBACK"] = config("rollback")

        # ── 내부(안전 엔진) ──
        h["PROCESS_ABORT"] = lambda a, src, by: self.chamber(a).abort(str(a["reason"]), source="SAFETY") and None
        h["POWER_OFF_ONLY"] = lambda a, src, by: self._need(self.safety, "안전 엔진").power_off_only(
            str(a.get("reason") or "PROGRAM_EXIT"))
        h["RESIDUAL_POWER_OFF"] = lambda a, src, by: None
        h["NOTIFY"] = lambda a, src, by: self._emit("safety", "notify", str(a.get("detail") or a["code"]), str(a["level"]),
                                                     str(a["code"]))

        # ── 로봇(v1 handlers 그대로 — RobotOps) ──
        def robot(cmd: str):
            def run(a, src, by):
                ops = self.robot_ops
                if ops is None:
                    raise Rejected("ROBOT_NOT_READY", "RobotOps 없음")
                data = a.get("data") if isinstance(a.get("data"), dict) else {}
                return ops.handle(cmd, data, a.get("raw"), str(a.get("peer") or ""), a.get("received_at"),
                                  a.get("request_id"))
            return run
        for cmd in C.ROBOT_COMMANDS:
            h[f"ROBOT_{cmd}"] = robot(cmd)
        return h

    @staticmethod
    def _need(obj, what: str):
        if obj is None:
            raise Rejected("NOT_SUPPORTED", f"{what} 엔진 없음")
        return obj
