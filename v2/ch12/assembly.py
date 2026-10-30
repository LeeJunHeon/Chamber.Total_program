"""챔버12 엔진 조립 — 실행 프로그램(app)과 시험대(sim.rig.Ch12FullRig)가 같은 build(system, …) 하나를 쓴다(배선이 두 벌이 되지 않게).

Ch12System(장비·1초 행·기록) 위에 붙이는 것(순서)
  1 RunState(LinkedRunState — 챔버 실행 표시는 엔진 상태에서·종료 시각은 run_end 이벤트에서, v1 runtime_state 의 mark_started·
    mark_finished 자리) → system.run_state
  2 CH2 히터(CH2_SUPPORTS_HEATER — HeaterDriver(PLC 해독기)·HeaterEngine, 엔진 이름 heater)·두께(CHn_SUPPORTS_THICKNESS 이고
    STM-100 이 켜진 챔버 — ThicknessEngine, 공정 엔진이 poll 한다)
  3 챔버 공정 엔진 proc.ch1·proc.ch2(ProcessEngine — CH2 에 히터·두께) → 프리스퍼터 2개(PreSputter — 'UI 현재값'·전용 레시피 시작 모두 core 를 지남)
  4 플라즈마 클리닝(pc — chamber_busy = 그 CH 공정 엔진 busy, Robot CSV)·TSP(tsp — 매일 예약 복원)
  5 안전(Ch12Safety → system.safety)·명령 처리(Ch12Core → system.command)
  6 로봇: Robot CSV(RobotCsv — startup_recover) · RobotOps(v1 handlers — engines = EngineAdapter) · ops.core = core(로봇 명령도
    core.command 를 지난다 — robot_api.dispatch_robot) · RobotApi(127.0.0.1:ROBOT_API_PORT)
  7 허브(hub.attach)·설정 적용(config_apply.attach) — 이미 붙어 있으면 그대로
  닫기: 'assembly' 엔진의 close(system.stop 이 엔진 close 를 부른다) — 로봇 API·Robot CSV·프리스퍼터·히터 종료(RUN=0 — S24).
EngineAdapter = robot_engines.RobotEngines Protocol 의 실제 엔진 쪽(v1 handlers 가 runtime_state·ChamberRuntime·PlasmaCleaningRuntime 에서
  읽던 사실만). START_SPUTTER 는 v1 start_with_recipe_string 그대로(파일 30 s — E227 · E207·E208·E209 · E230 은 '레시피 오류 N건'
  RuntimeError) → 적재 → core.start_chamber(시작 가드 신호 on_preflight) → Robot CSV(수락 mark_owned · 행 시작 mark_started · 리스트 끝 finalize).
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from v2.ch12 import process_states as PS
from v2.ch12.run_state import RunState, norm_channel, norm_kind

CHAMBERS = ("ch1", "ch2")
RUNNING_STATES = ("PREFLIGHT", "RUNNING", "STOPPING")
CSV_LOAD_TIMEOUT_S = 30.0                       # v1 start_with_recipe_string asyncio.wait_for(…, 30.0)


class LinkedRunState(RunState):
    """챔버 실행 표시는 엔진 상태에서(v1 PREFLIGHT 진입 mark_started ~ 끝 mark_finished), 나머지는 RunState 그대로."""

    def __init__(self, clock) -> None:
        super().__init__(clock)
        self.chambers: Dict[int, Any] = {}

    def is_running(self, kind: str, channel: Optional[int] = None) -> bool:
        k = norm_kind(kind)
        if k == "chamber":
            e = self.chambers.get(norm_channel(k, channel))
            if e is not None and PS.chamber_state(e) in RUNNING_STATES:
                return True
        return super().is_running(kind, channel)

    def any_running(self) -> bool:
        return super().any_running() or any(PS.chamber_state(e) in RUNNING_STATES for e in self.chambers.values())

    def on_logged(self, e) -> None:
        """system.event_listeners — 챔버 엔진 run_start(시작 표시·오류 해제)·run_end(종료 시각·실패면 오류 래치)."""
        src = str(getattr(e, "source", "") or "")
        if not src.startswith("engine:ch"):
            return
        n = 1 if src.endswith("1") else 2
        kind = getattr(e, "kind", "")
        if kind == "run_start":
            self.mark_started("chamber", n)
        elif kind == "run_end":
            data = getattr(getattr(e, "event", None), "data", None) or {}
            if data.get("result") == "error":
                self.set_error("chamber", n, str(getattr(e, "detail", "") or getattr(e, "code", "") or "error"))
            self.mark_finished("chamber", n)


class HeaterHost:
    """HeaterEngine 을 조립 엔진(heater)으로 — tick·행·1초 열·busy·웹 상태·닫기(종료 RUN=0, S24)."""

    def __init__(self, eng) -> None:
        self.eng = eng

    def on_tick_start(self, tick=None) -> None:
        self.eng.on_tick_start(tick)

    def on_row(self, view=None) -> None:
        self.eng.on_row(view)

    def snapshot_fields(self) -> Dict[str, Any]:
        return self.eng.snapshot_fields()

    @property
    def busy(self) -> bool:
        return bool(self.eng.busy)

    def web_state(self) -> dict:
        return self.eng.status()

    def close(self) -> None:
        self.eng.shutdown()


class ThicknessHost:
    """두께 엔진의 1초 열(ch2.thk.* — thickness_record.blocks). poll·on_row 는 공정 엔진이 부른다(여기서 다시 부르지 않음)."""

    def __init__(self, eng, ch: str) -> None:
        self.eng, self.ch = eng, ch

    def snapshot_fields(self) -> Dict[str, Any]:
        from v2.ch12.thickness_record import BLOCK_FIELDS
        got = self.eng.snapshot_fields()
        return {f"{self.ch}.thk.{k}": got.get(k) for k in BLOCK_FIELDS}

    def web_state(self) -> dict:
        return dict(self.eng.status())


def chamber_web_state(e) -> dict:
    """챔버 엔진 화면 상태(허브 state.engines) — state_dict + 블록 목록·시작한 블록 수·일시정지 종류·명령 표 상태 이름."""
    d = dict(e.state_dict()) if hasattr(e, "state_dict") else {}
    d.pop("last_result", None)
    blocks = [{"i": i, "type": b.get("type"), "label": b.get("label") or b.get("type"), "params": dict(b.get("params") or {})}
              for i, b in enumerate(list(e.blocks or []))]
    try:
        upto = int(e.started_upto())
    except Exception:
        upto = 0
    d.update(state=PS.chamber_state(e), engineState=e.state_name, blocks=blocks, started_upto=upto,
             cur_block=e.cur_block, pause_kind=e.pause_kind, paused=bool(e.paused), recipe_name=e.recipe_name,
             stopping=PS.chamber_state(e) == "STOPPING")
    return d


class RobotStatus:
    """로봇 연결 상태(허브 state.engines["robot"]) — 내부 명령 API 의 마지막 요청 시각·peer·명령·처리 중 명령. 판단은 하지 않는다."""

    def __init__(self, clock) -> None:
        import threading
        self.clock = clock
        self.port: Optional[int] = None
        self.count = 0
        self.last: Optional[dict] = None
        self.inflight: Dict[int, dict] = {}
        self._lk = threading.Lock()
        self._n = 0

    def dispatch(self, ops, cmd, data, raw=None, peer: str = "", received_at=None, request_id=None):
        from v2.ch12.robot_api import dispatch_robot
        with self._lk:
            self._n += 1
            k = self._n
            self.inflight[k] = {"cmd": str(cmd), "peer": str(peer), "since": self.clock.wall()}
        out = None
        try:
            out = dispatch_robot(ops, cmd, data, raw, peer, received_at, request_id)
            return out
        finally:
            body = out[1] if isinstance(out, tuple) and len(out) > 1 else None
            with self._lk:
                self.inflight.pop(k, None)
                self.count += 1
                self.last = {"cmd": str(cmd), "peer": str(peer), "at": self.clock.wall(),
                             "result": body.get("result") if isinstance(body, dict) else None,
                             "errorCode": body.get("error_code") if isinstance(body, dict) else None}

    def web_state(self) -> dict:
        from v2.ch12.hub import utc_iso
        with self._lk:
            last = dict(self.last) if self.last else None
            infl = [dict(v, since=utc_iso(v["since"])) for v in self.inflight.values()]
        if last:
            last["at"] = utc_iso(last["at"])
        return {"apiPort": self.port, "listening": self.port is not None, "count": self.count, "last": last,
                "inflight": infl}


class EngineAdapter:
    """robot_engines.RobotEngines — 실제 엔진 쪽 사실·시작."""

    def __init__(self, asm: "Assembly") -> None:
        self.a = asm
        self.host_runs: Dict[int, dict] = {}        # 챔버 → {key, started, last}(v1 _host_run)

    # ── 사실 ──
    def _eng(self, ch: int):
        return self.a.proc.get(f"ch{int(ch)}")

    def chamber_state(self, ch: int) -> Dict[str, Any]:
        e, rs = self._eng(ch), self.a.run_state
        st = PS.chamber_state(e) if e is not None else None
        kind = {"PREFLIGHT": "PREFLIGHT", "RUNNING": "PROCESS", "STOPPING": "PROCESS", "COOLDOWN": "ADVANCE_QUEUE",
                "CLEANUP": "CLEANUP"}.get(st or "")
        alive = bool(e is not None and (e._cmd is not None or e.runner.is_running))
        return {"ready": e is not None, "chamber_running": rs.is_running("chamber", ch), "pc_running": rs.is_running("pc", ch),
                "tsp_running": rs.is_running("tsp", 0), "runner_state": st, "stage_alive": alive,
                "stage_kind": kind if alive else None, "has_error": rs.has_error("chamber", ch)}

    def eta(self, ch: int) -> Optional[int]:
        e = self._eng(ch)
        return None if e is None else e.eta_remaining_s()

    def min_total_s(self, ch: int) -> Optional[int]:
        e = self._eng(ch)
        return None if e is None else e.min_total_s()

    def pc_state(self) -> Dict[str, Any]:
        pc = self.a.pc
        if pc is None:
            return {"ready": False, "running": {1: False, 2: False}, "ch1_ig_wait": False, "cleaning": None, "selected_ch": 1}
        d = dict(pc.pc_state_for_robot())
        e1 = self._eng(1)
        step = None
        if e1 is not None and e1.runner.is_running and 0 <= e1.runner.idx < len(e1.runner.steps):
            step = e1.runner.steps[e1.runner.idx].action
        d["ch1_ig_wait"] = bool(d["running"].get(1) and step in (None, "IG_CMD"))
        return d

    def last_pc_result(self) -> Dict[str, Any]:
        pc = self.a.pc
        return pc.last_pc_result() if pc is not None else {"finished": {1: None, 2: None}, "error": {1: False, 2: False}}

    def recipe_folder_scan(self, folder: str, timeout_s: float) -> List[str]:
        from v2.ch12.robot_engines import scan_recipe_folder
        return scan_recipe_folder(folder, timeout_s)

    # ── 시작 ──
    def _load_file(self, path: str):
        from v2.ch12.recipe import load_recipe_file
        clk = self.a.system.clock
        box: Dict[str, Any] = {}

        def work() -> None:
            try:
                box["v"] = load_recipe_file(path, origin="host")
            except Exception as ex:                  # noqa: BLE001 — 부른 쪽에서 E209 로
                box["e"] = ex
            clk.notify()
        clk.start_thread(work, "recipe-load")
        clk.wait_until(clk.monotonic() + CSV_LOAD_TIMEOUT_S, lambda: "v" in box or "e" in box)
        if "v" not in box and "e" not in box:
            from v2.ch12.errors import CodedRuntimeError
            raise CodedRuntimeError(f"CSV 로드 30초 timeout (NAS 응답 지연): {path}", "E227")
        if "e" in box:
            raise box["e"]
        return box["v"]

    def start_sputter(self, ch: int, recipe: str, request_meta: Dict[str, Any], on_preflight) -> None:
        from v2.ch12.errors import CodedRuntimeError
        e = self._eng(ch)
        s = (recipe or "").strip()
        r = self._load_file(s)
        for src, msg in r.logs:
            e.log(src, msg)
        if r.code == "E230":
            raise RuntimeError(r.message)               # v1: '레시피 오류 N건' RuntimeError(코드 없음 — 핸들러가 E230)
        if not r.ok:
            raise CodedRuntimeError(r.message, r.code or "E209")
        key = str((request_meta or {}).get("key") or "")
        hl = self.a.robot_csv
        if hl is not None and key:
            hl.update(key, recipe_name=os.path.basename(s), row_count=len(r.rows),
                      process_names=" / ".join(str(x.get("Process_name", "") or "").strip() for x in r.rows))
        why = e.load(r.rows, source="robot", by="robot", name=os.path.basename(s))
        if why:
            raise RuntimeError(f"runner busy: {e.state_name}" if why == "BUSY" else f"레시피 적재 실패: {why}")
        from v2.ch12.core import Rejected

        def pre(ok: bool, code, detail: str) -> None:
            if ok and hl is not None and key:
                prev = self.host_runs.get(int(ch))        # v1 _host_run_begin: 확정 없이 남은 직전 런은 먼저 닫는다(7655-7664)
                if prev is not None and prev.get("key") and prev.get("key") != key:
                    try:
                        hl.finalize(prev["key"], "미확인", "다음 요청 수락 전 종료 미확인",
                                    datetime.fromtimestamp(self.a.system.clock.wall()))
                    except Exception:
                        pass
                self.host_runs[int(ch)] = {"key": key, "started": False, "last": None, "log_files": []}
                hl.mark_owned(key)
            on_preflight(ok, code, detail)
        try:
            self.a.core.start_chamber(int(ch), source="robot", by="robot", owner="robot", origin="host",
                                      origin_meta=dict(request_meta or {}), host_cb=pre)
        except Rejected:
            pass                                        # 시작 가드 거절 — 신호(on_preflight)는 이미 보냄

    def start_pc(self, ch: int, recipe: str, request_meta: Dict[str, Any], on_preflight) -> None:
        self.a.pc.start_for_robot(recipe, request_meta, on_preflight)

    # ── Robot CSV(v1 _host_run_mark_started·_host_run_finalize) ──
    def on_logged(self, ev) -> None:
        src = str(getattr(ev, "source", "") or "")
        if not src.startswith("engine:ch"):
            return
        n = 1 if src.endswith("1") else 2
        hr = self.host_runs.get(n)
        hl = self.a.robot_csv
        if hr is None or hl is None:
            return
        kind = getattr(ev, "kind", "")
        clk = self.a.system.clock
        if kind == "run_start":
            e = self._eng(n)
            p = getattr(getattr(e, "txt", None), "path", None)
            lf = os.path.basename(str(p)) if p else ""
            if not hr["started"]:
                hr["started"] = True
                hr["log_files"] = [lf] if lf else []
                hl.mark_started(hr["key"], datetime.fromtimestamp(clk.wall()), log_file=lf or None)
            elif lf and lf not in hr.setdefault("log_files", []):     # 둘째 행부터는 로그 파일만(v1 7678-7685)
                hr["log_files"].append(lf)
                hl.update(hr["key"], log_files=list(hr["log_files"]))
        elif kind == "run_end":
            hr["last"] = dict(getattr(getattr(ev, "event", None), "data", None) or {}, code=getattr(ev, "code", None),
                              detail=str(getattr(ev, "detail", "") or ""))

    def on_row(self, row=None) -> None:
        """리스트(큐) 끝 — 엔진이 다시 idle 이면 그 요청 한 줄 확정(v1 _clear_queue_and_reset_ui)."""
        from v2.ch12.outputs.robot_csv import RESULT_FAIL, RESULT_STOP, RESULT_SUCCESS, RESULT_UNKNOWN
        hl = self.a.robot_csv
        for n, hr in list(self.host_runs.items()):
            e = self._eng(n)
            if e is None or e.busy or hl is None:
                continue
            last = hr.get("last")
            if isinstance(last, dict):
                # v1 last_row(1829-1839) — 사유 = detail.reason | error_detail | errors[0]; 명시 결과(7693-7700)는 run_end 코드로
                res = last.get("result")
                rd = last.get("result_detail") if isinstance(last.get("result_detail"), dict) else {}
                errs = rd.get("errors") or []
                row_reason = str(rd.get("reason") or rd.get("error_detail") or (errs[0] if errs else "") or "")
                if res == "done":
                    result, reason = RESULT_SUCCESS, ""
                elif res == "aborted":
                    explicit = str(last.get("detail") or "") if not rd else ""      # 시작 전 STOP — '사용자 STOP (시작 전)'
                    result, reason = RESULT_STOP, (row_reason or explicit or "사용자 STOP")
                elif last.get("code") == "RECIPE_PARAM_ERROR":
                    result, reason = RESULT_FAIL, "레시피 파라미터 오류: " + str(last.get("detail") or "")
                else:
                    result, reason = RESULT_FAIL, (row_reason or str(last.get("detail") or last.get("code") or ""))
            elif not hr["started"]:
                result, reason = RESULT_SUCCESS, "공정 행 없음(대기만)"
            else:
                result, reason = RESULT_UNKNOWN, "종료 경로 미확인"
            self.host_runs.pop(n, None)
            hl.finalize(hr["key"], result, reason, datetime.fromtimestamp(self.a.system.clock.wall()))


@dataclass
class Assembly:
    system: Any
    proc: Dict[str, Any] = field(default_factory=dict)
    pc: Any = None
    tsp: Any = None
    heater: Any = None
    heater_driver: Any = None
    thickness: Dict[str, Any] = field(default_factory=dict)
    presputter: Dict[str, Any] = field(default_factory=dict)
    run_state: Any = None
    core: Any = None
    safety: Any = None
    robot_csv: Any = None
    robot_ops: Any = None
    robot_api: Any = None
    adapter: Any = None
    hub: Any = None
    config: Any = None
    inventory: Any = None
    robot_status: Any = None

    # 조립 엔진(system.stop 이 close 를 부른다) — 로봇 API·Robot CSV·프리스퍼터
    def close(self) -> None:
        csv = self.robot_csv
        self.robot_csv = None                         # 닫은 뒤 늦은 기록(마감·거절)이 보내기 스레드를 다시 띄우지 않게 먼저 뗀다
        for obj, attr in ((self.robot_ops, "robot_csv"), (self.pc, "host_log")):
            if obj is not None and hasattr(obj, attr):
                setattr(obj, attr, None)
        jobs = [getattr(self.robot_api, "stop", None), getattr(csv, "close", None), getattr(csv, "close", None),
                getattr(self.inventory, "close", None)]
        jobs += [p.close for p in self.presputter.values()]
        for fn in jobs:
            if fn is None:
                continue
            try:
                fn()
            except Exception:
                pass
        self.robot_api = None

    def on_row(self, view=None) -> None:
        if self.adapter is not None:
            self.adapter.on_row(view)


def build(system, cfg: Optional[Dict[str, Any]] = None, *, settings_dir: Optional[Path] = None,
          excel_main_dir: Optional[Path] = None, robot_api: bool = True, robot_port: Optional[int] = None,
          hub: bool = True, config_apply: bool = True, robot_csv: bool = True, schedules: bool = True) -> Assembly:
    """Ch12System 에 엔진 전부·안전·명령 처리·로봇·허브·설정 적용을 붙인다(한 번). 반환 Assembly(시험·실행 프로그램이 본다)."""
    from v2.ch12.core import Ch12Core
    from v2.ch12.heater_engine import HeaterEngine
    from v2.ch12.drivers import heater as HD
    from v2.ch12.pre_sputter import PreSputter
    from v2.ch12.process_engine import ProcessEngine
    from v2.ch12.safety import Ch12Safety
    from v2.ch12.thickness import ThicknessEngine
    from v2.ch12 import plasma_cleaning as PCM, tsp_engine as TE
    c = system.cfg if cfg is None else cfg
    clock = system.clock
    wr = bool(system.policy.device_writes)
    store = getattr(system, "store", None)
    sdir = Path(settings_dir) if settings_dir is not None else (Path(store.dir) if store is not None
                                                                else Path(c["LOCAL_DATA_DIR"]) / "settings")
    sdir.mkdir(parents=True, exist_ok=True)
    asm = Assembly(system)
    system.assembly = asm

    # 1 실행 상태
    rs = asm.run_state = LinkedRunState(clock)
    system.run_state = rs
    system.event_listeners.append(rs.on_logged)

    # 2 히터·두께(CH2)
    from v2.ch12.heater_setup import init_ch2_heater

    def make_heater():
        drv = getattr(system, "heater_drv", None) or HD.HeaterDriver(clock, system.plc, c, wr,
                                                                      on_event=system._ev_for("ch2.heater"), chamber="ch2")
        asm.heater_driver = drv
        to_sys = system._ev_for("heater")

        def on_heater(ev) -> None:                    # 히터 로그는 CH2 공정 txt 에도(공정 로그가 열려 있을 때 — HEAT-090 상태 줄 포함)
            to_sys(ev)
            e2 = asm.proc.get("ch2")
            if getattr(ev, "kind", "") == "log" and e2 is not None and getattr(e2.txt, "path", None) is not None:
                e2.log("HEATER", str(getattr(ev, "detail", "") or ""))
        return HeaterEngine(clock, drv, c, on_event=on_heater, csv=system.outputs.heater, chamber="ch2")
    if bool(c.get("CH2_SUPPORTS_HEATER", False)):
        asm.heater = init_ch2_heater(c, make_heater, lambda src, msg: system.emit("heater", "log", msg, level="warn"))
        if asm.heater is not None:
            system.register_engine("heater", HeaterHost(asm.heater))
    for ch in CHAMBERS:
        drv = system.stm.get(ch)
        if drv is not None and drv.enabled:
            asm.thickness[ch] = ThicknessEngine(clock, c, ch, driver=drv, settings_dir=sdir)
            system.register_engine(f"thk.{ch}", ThicknessHost(asm.thickness[ch], ch))
            system.emit(f"{ch}.stm100", "log", f"{ch.upper()} 두께 모니터 서비스 시작 ({drv.p.host}:{drv.p.port})")   # main.py:1340

    # 3 챔버 공정 엔진(+ 공정 모니터·재고 DB)·프리스퍼터
    from v2.ch12 import inventory_db as INV, process_monitor as PM
    asm.inventory = INV.make_inventory(c, system.mode)
    for ch in CHAMBERS:
        e = ProcessEngine(system, ch, heater=asm.heater if ch == "ch2" else None, thickness=asm.thickness.get(ch),
                          excel_main_dir=excel_main_dir)
        system.register_engine(f"proc.{ch}", e)
        asm.proc[ch] = e
        rs.chambers[e.n] = e
        e.web_state = (lambda e=e: chamber_web_state(e))          # 허브 state.engines["proc.chN"](포털 화면)
        PM.attach(system, e)                                     # 공정 모니터(가스·압력 편차·Arc·REFP — v1 process_monitor)
        if asm.inventory is not None:
            INV.attach(e, asm.inventory)                         # 큐 행마다 Gun Target 재고 DB(최대 3 s — v1 5080-5098)

    # 4 플라즈마 클리닝·TSP(Robot CSV 는 로봇 쪽과 같이 쓴다)
    if robot_csv:
        from v2.ch12.outputs.robot_csv import RobotCsv
        from v2.ch12.robot_api import client_process_to_emit
        asm.robot_csv = RobotCsv(c, writer=system.outputs.writer, start_worker=True)
        asm.robot_csv.set_event_sink(client_process_to_emit(system.emit))     # v1 _emit_client_process_card 자리(포털 카드)
        try:
            asm.robot_csv.startup_recover()
        except Exception:
            pass
    asm.pc = PCM.attach(system, run_state=rs, chamber_busy=lambda n: bool(asm.proc[f"ch{int(n)}"].busy),
                        host_log=asm.robot_csv)
    asm.tsp = TE.attach(system, rs, settings_dir=sdir, schedule=schedules)

    # 5 안전·명령 처리
    asm.safety = Ch12Safety(system, asm.proc, pc=asm.pc, tsp=asm.tsp, heater=asm.heater)
    system.safety = asm.safety
    asm.core = Ch12Core(system, asm.proc, pc=asm.pc, tsp=asm.tsp, heater=asm.heater, thickness=asm.thickness,
                        safety=asm.safety, run_state=rs)
    system.core = asm.core
    system.command = asm.core.command
    for ch, e in asm.proc.items():
        p = PreSputter(clock, e, settings_dir=sdir, start_ui=lambda ch=ch: asm.core.presputter_start_ui(ch),
                       start_recipe=lambda rows, name, ch=ch: asm.core.presputter_start_recipe(ch, rows, name),
                       log_sink=lambda line: system.emit("pc", "log", line))
        asm.presputter[ch] = p
        e.web_state = (lambda e=e, p=p: dict(chamber_web_state(e), presputter=p.state()))   # 예약 표시(PC-115)
        if schedules:
            msg = p.restore()
            if msg:
                system.emit("presputter", "restore", msg)
    asm.core.presputter = asm.presputter

    # 6 로봇
    from v2.ch12.robot_api import RobotApi, report_to_emit
    from v2.ch12.robot_ops import RobotOps
    asm.adapter = EngineAdapter(asm)
    system.event_listeners.append(asm.adapter.on_logged)
    asm.robot_ops = RobotOps(clock, system.shared.plc, c, engines=asm.adapter, robot_csv=asm.robot_csv,
                             on_report=report_to_emit(system.emit))
    asm.robot_ops.core = asm.core
    asm.core.robot_ops = asm.robot_ops
    asm.robot_status = RobotStatus(clock)
    system.register_engine("robot", asm.robot_status)
    if robot_api:
        try:
            asm.robot_api = RobotApi(asm.robot_ops, clock, c, emit=system.emit, port=robot_port,
                                     dispatch=asm.robot_status.dispatch).start()
            asm.robot_status.port = asm.robot_api.port
        except OSError as ex:                       # 포트를 못 잡아도 공정·안전은 그대로(로봇 서버는 '응답 없음'으로 다룬다)
            system.emit("robot_api", "bind_failed", f"내부 명령 API 포트 열기 실패: {ex}", level="error", code="ROBOT_NOT_READY")

    # 7 허브·설정 적용
    from v2.ch12 import config_apply as CA, hub as H
    if hub:
        asm.hub = getattr(system, "hub", None) or H.attach(system)
    if config_apply:
        asm.config = getattr(system, "config", None) or CA.attach(system)
    system.register_engine("assembly", asm)
    return asm
