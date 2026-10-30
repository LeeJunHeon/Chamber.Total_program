"""로봇 명령 시험대 — PLC 시뮬레이터(상태 주입)·가짜 공정 어댑터·RobotOps 를 가상 시계로 묶는다(시험·골든 공용).

- RobotPlcSim(PlcSim): 코일 강제값(forced — 래더 출력 뒤에 덮음: 램프 둘 다 TRUE 같은 이상 상태), 읽기·쓰기 무응답 주소(mute·mute_write),
  진공이 안 잡히는 로드락(no_vacuum — 러핑해도 게이지가 안 올라 T0052 600 s 뒤 L_VAC_NOT_READY).
- apply_state(sim, state): 사례 상태(게이트·로딩 센서·척·핀·로드락·코일·강제·무응답)를 시뮬레이터에 넣는다. v1 골든 도구
  (v2/tools/v1golden_robot_handlers.py)도 같은 함수로 같은 상태를 만든다 — 두 쪽이 같은 래더에서 돈다.
- FakeEngines(eng, clock, robot_csv): robot_engines.RobotEngines 가짜 — 사례의 eng 사전(챔버·PC 사실, eta, min_total_s,
  시작 결과(예외·시작 가드 신호 시각·결과·수락), 레시피 스캔 지연)대로 답한다.
- RobotRig: VirtualClock + RobotPlcSim + SimTransport + PLC worker·드라이버(v1 정책) + PlcLink + RobotOps.
  run(cmd, data, state, eng) → {reply_cmd, body, log, writes(코일 쓰기 (주소, 값) 순서), elapsed(가상 초)}.
"""
from __future__ import annotations

import json
import re
import struct
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from v2.common.clock import VirtualClock
from v2.common.modbus_rtu import READ_COILS, WRITE_COIL
from v2.common.transport import SimTransport
from v2.ch12.drivers import plc as P
from v2.ch12.drivers import plc_map as PM
from v2.ch12.errors import CodedRuntimeError
from v2.ch12.robot_engines import scan_recipe_folder_sync
from v2.ch12.sim.plc_sim import CHUCK_LAMP_HOLD_S, PlcSim

START_T = 1.0                 # 사례 시작 시각(시뮬레이터·시계)
PEER = "127.0.0.1:50123"      # 로봇 서버가 넘기는 peer(v1 str(peername) = "('127.0.0.1', 50123)")
RECIPE_NAME = "recipe_a.csv"
PLC_ERR = "<PLC_ERR>"         # PLC 통신 오류 문구 자리표(v1 pymodbus 문구 ↔ v2 드라이버 문구)
TS_RE = re.compile(r"^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d ")
KEY_RE = re.compile(r"^sputter-\d{17}-")


class RobotPlcSim(PlcSim):
    def __init__(self, *a, **k) -> None:
        self.forced: Dict[int, bool] = {}
        self.mute: set = set()
        self.mute_write: set = set()
        self.no_vacuum = False
        super().__init__(*a, **k)

    def _outputs(self) -> None:
        super()._outputs()
        for a, v in getattr(self, "forced", {}).items():
            self.coils[a] = bool(v)

    def _loadlock_step(self) -> None:
        if getattr(self, "no_vacuum", False) and self.ll.get("rough_t") is not None:
            self.ll["rough_t"] = self.t
        super()._loadlock_step()

    def _pdu(self, pdu: bytes, now: float) -> Optional[bytes]:
        if pdu and pdu[0] == READ_COILS and self.mute:
            addr, n = struct.unpack(">HH", pdu[1:5])
            if any(addr <= a < addr + n for a in self.mute):
                return None
        if pdu and pdu[0] == WRITE_COIL and self.mute_write:
            if struct.unpack(">H", pdu[1:3])[0] in self.mute_write:
                return None
        return super()._pdu(pdu, now)

    def coil_writes_since(self, n_frames: int) -> List[list]:
        out = []
        for _t, f in list(self.frames)[n_frames:]:
            p = f[7:]
            if p and p[0] == WRITE_COIL:
                addr, v = struct.unpack(">HH", p[1:5])
                out.append([addr, v == 0xFF00])
        return out


_GATE = {"closed": 0.0, "open": 1.0, "moving": 0.5}
_CHUCK = {"down": 0.0, "mid": 0.5, "up": 1.0, "between": 0.3}


def apply_state(sim: RobotPlcSim, state: Optional[Dict[str, Any]]) -> None:
    """사례 상태를 시뮬레이터에 넣는다(키는 모두 선택). 채널 키는 '1'/'2' 문자열도 받는다(JSON)."""
    st = state or {}
    for ch, g in (st.get("gate") or {}).items():
        ch = int(ch)
        if g == "both":
            sim.forced[PM.resolve(f"G_V_{ch}_OPEN_LAMP")] = True
            sim.forced[PM.resolve(f"G_V_{ch}_CLOSE_LAMP")] = True
        else:
            sim.gate[ch] = _GATE[g]
    for ch, v in (st.get("loading") or {}).items():
        sim.loading[int(ch)] = bool(v)
    for ch, pos in (st.get("chuck") or {}).items():
        ch = int(ch)
        sim.chuck[ch] = _CHUCK[pos]
        sim.chuck_at[ch] = (sim.chuck[ch], sim.t - (0.0 if pos == "between" else CHUCK_LAMP_HOLD_S))
    if "pin" in st:
        sim.pin = 1.0 if st["pin"] == "up" else 0.0
    ll = st.get("ll")
    if ll == "vacuum_ready":
        sim.ll["atm"] = False
        sim.coils[PM.resolve("L_VAC_READY_SW")] = True
    elif ll == "vacuum":
        sim.ll["atm"] = False
    if st.get("no_vacuum"):
        sim.no_vacuum = True
    for name, v in (st.get("coils") or {}).items():
        sim.coils[PM.resolve(name)] = bool(v)
    for name, v in (st.get("forced") or {}).items():
        sim.forced[PM.resolve(name)] = bool(v)
    for name in (st.get("mute") or ()):
        sim.mute.add(PM.resolve(name))
    for name in (st.get("mute_write") or ()):
        sim.mute_write.add(PM.resolve(name))
    for name, v in (st.get("coils") or {}).items():          # 스위치 코일을 상승 에지로 보지 않게
        sim._edge_prev[PM.resolve(name)] = bool(v)
    sim._outputs()


def prepare_files(root: Path, case: Dict[str, Any]) -> str:
    """GET_RECIPE 폴더(files: {폴더: [이름 | '하위/'], 'ALD_file': 이름 — 폴더 자리에 파일})와 레시피 CSV(recipe_rows 행,
    Process_name P1..Pn)를 root 아래에 만든다. 레시피 경로(없으면 '')."""
    import shutil
    root = Path(root)
    marker = root / ".robot_rig"
    if root.exists():
        if not marker.exists():
            raise RuntimeError(f"시험대가 만든 폴더가 아님 — 지우지 않음: {root}")
        shutil.rmtree(root)
    root.mkdir(parents=True)
    marker.write_text("robot_rig", encoding="utf-8")
    for folder, names in (case.get("files") or {}).items():
        if folder == "ALD_file":
            (root / names).write_text("x", encoding="utf-8")
            continue
        d = root / folder
        d.mkdir(parents=True, exist_ok=True)
        for n in names:
            if n.endswith("/"):
                (d / n.rstrip("/")).mkdir()
            else:
                (d / n).write_text("x", encoding="utf-8")
    n = int(case.get("recipe_rows") or 0)
    if not n:
        return ""
    src = root / "_in"
    src.mkdir()
    p = src / RECIPE_NAME
    lines = ["Process_name,process_time"] + [f"P{i + 1},1" for i in range(n)]
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return str(p)


def subst(v: Any, recipe: str) -> Any:
    """'{RECIPE}' 를 레시피 경로로(사전·목록 안까지)."""
    if isinstance(v, str):
        return v.replace("{RECIPE}", recipe)
    if isinstance(v, dict):
        return {k: subst(x, recipe) for k, x in v.items()}
    if isinstance(v, list):
        return [subst(x, recipe) for x in v]
    return v


def norm_text(s: str, roots: List[tuple]) -> str:
    for r, ph in roots:
        if r:
            s = s.replace(r, ph).replace(json.dumps(r, ensure_ascii=False)[1:-1], ph)
    return s


def normalize(out: Dict[str, Any], roots: List[tuple]) -> Dict[str, Any]:
    """비교 모양 — body 는 JSON 직렬화 문자열(로봇 서버가 그대로 보낸다), 로그 시각·핸들러 예외 traceback·Robot CSV 시각 칸·
    기록키 시각은 가림, 경로는 자리표(roots = [(문자열, 자리표)]), 응답까지 가상 초는 소수 3자리."""
    res = []
    for r in out["results"]:
        log = []
        for tag, text in r["log"]:
            text = TS_RE.sub("<TS> ", text)
            if tag == "NET" and text.startswith("Handler error for "):
                text = text.split("\n", 1)[0]
            log.append([tag, norm_text(text, roots)])
        res.append({"reply_cmd": r["reply_cmd"],
                    "body": norm_text(json.dumps(r["body"], ensure_ascii=False, default=str), roots),
                    "log": log, "elapsed": round(float(r["elapsed"]), 3)})
    rows = []
    for row in out["robot_csv"]:
        row = list(row)
        for i in (1, 2, 3, 4):
            row[i] = "<T>" if row[i] else ""
        row[14] = KEY_RE.sub("sputter-<TS>-", row[14])
        rows.append([norm_text(x, roots) for x in row])
    return {"results": res, "writes": [list(w) for w in out["writes"]], "robot_csv": rows}


# ───────────────────────── 가짜 공정 어댑터 ─────────────────────────
class FakeEngines:
    """eng 사전(v1 말로 — v2/tools/robot_golden_cases.py 독스트링)을 RobotEngines 사실로 옮긴다. v1 골든 도구는 같은 사전으로
    v1 runtime_state·ChamberRuntime·PlasmaCleaningRuntime 가짜를 만든다(같은 사실 → 같은 응답이어야 한다).
    raise_state: 이 이름의 메서드는 예외."""

    def __init__(self, eng: Dict[str, Any], clock, robot_csv=None) -> None:
        self.eng, self.clock, self.robot_csv = eng, clock, robot_csv
        rs = eng.get("rs") or {}
        self.running = {(k, int(c)) for k, c in (rs.get("running") or [])}
        self.error = {(k, int(c)) for k, c in (rs.get("error") or [])}
        self.finished = {int(c): float(t) for k, c, t in (rs.get("finished") or []) if k == "pc"}
        self.rt = {int(k): dict(v) for k, v in (eng.get("rt") or {}).items()}
        self.calls: List[tuple] = []

    def _maybe_raise(self, name: str) -> None:
        if name in (self.eng.get("raise_state") or ()):
            raise RuntimeError(f"fake {name} 실패")

    def chamber_state(self, ch: int) -> Dict[str, Any]:
        self._maybe_raise("chamber_state")
        rt = self.rt.get(ch)
        return {"ready": rt is not None, "chamber_running": ("chamber", ch) in self.running,
                "pc_running": ("pc", ch) in self.running, "tsp_running": ("tsp", 0) in self.running,
                "runner_state": (rt.get("runner_state", "IDLE") if rt is not None else None),
                "stage_alive": rt is not None and "stage" in rt, "stage_kind": (rt.get("stage") or None) if rt else None,
                "has_error": ("chamber", ch) in self.error}

    def eta(self, ch: int):
        rt = self.rt.get(ch)
        if rt is None:
            return None
        n, idx = (list(rt.get("queue") or []) + [0, -1])[:2]
        if n and idx < n - 1:
            return None
        return rt.get("eta")

    def min_total_s(self, ch: int):
        rt = self.rt.get(ch)
        if rt is None:
            return None
        n = (list(rt.get("queue") or []) + [0])[0]
        return rt.get("min_total") if n else None

    def pc_state(self) -> Dict[str, Any]:
        self._maybe_raise("pc_state")
        pc = self.eng.get("pc")
        rt1 = self.rt.get(1) or {}
        ig = bool(1 in self.rt and rt1.get("pc_running") and rt1.get("step") in (None, "IG_CMD"))
        return {"ready": pc is not None, "running": {c: ("pc", c) in self.running for c in (1, 2)},
                "ch1_ig_wait": ig, "cleaning": (bool(pc.get("is_running")) if pc is not None else None),
                "selected_ch": int((pc or {}).get("selected_ch", 1) or 1)}

    def last_pc_result(self) -> Dict[str, Any]:
        self._maybe_raise("last_pc_result")
        return {"finished": dict(self.finished), "error": {c: ("pc", c) in self.error for c in (1, 2)}}

    def recipe_folder_scan(self, folder: str, timeout_s: float) -> List[str]:
        delay = float(self.eng.get("scan_delay_s", 0.0))
        if delay > timeout_s:
            self.clock.wait_until(self.clock.monotonic() + timeout_s)
            raise TimeoutError("scan")
        if delay > 0:
            self.clock.wait_until(self.clock.monotonic() + delay)
        return scan_recipe_folder_sync(folder)

    def _start(self, kind: str, ch: int, recipe: str, meta: Dict[str, Any], on_preflight) -> None:
        import os
        self.calls.append((kind, ch, recipe, dict(meta)))
        sc = dict(self.eng.get(kind) or {})
        rz = sc.get("raise")
        if rz:
            code, msg, typ = (list(rz) + [None, None, None])[:3]
            if typ == "ValueError":
                raise ValueError(msg)
            raise CodedRuntimeError(msg, code) if code else RuntimeError(msg)
        key = str(meta.get("key") or "")
        rows = int(sc.get("rows") or 0)
        if kind == "start" and rows:
            if self.robot_csv is not None and key:          # v1 start_with_recipe_string 의 검증 통과 직후 update
                self.robot_csv.update(key, recipe_name=os.path.basename(recipe), row_count=rows,
                                      process_names=" / ".join(f"P{i + 1}" for i in range(rows)))
            self.rt.setdefault(ch, {})["queue"] = [rows, -1]
        pf = sc.get("preflight")
        if pf is None:
            return                                   # 시작 가드 신호가 오지 않음(시간 초과 사례)

        def fire() -> None:
            if self.robot_csv is not None and key and pf.get("owned"):
                self.robot_csv.mark_owned(key)
            on_preflight(bool(pf.get("ok")), pf.get("code"), str(pf.get("detail") or ""))
        after = float(pf.get("after_s", 0.0))
        if after <= 0:
            fire()
            return

        def later() -> None:
            self.clock.wait_until(self.clock.monotonic() + after)
            fire()
        self.clock.start_thread(later, f"fake-{kind}")

    def start_sputter(self, ch, recipe, request_meta, on_preflight) -> None:
        self._start("start", ch, recipe, request_meta, on_preflight)

    def start_pc(self, ch, recipe, request_meta, on_preflight) -> None:
        self._start("pc_start", ch, recipe, request_meta, on_preflight)


# ───────────────────────── 시험대 ─────────────────────────
class RobotRig:
    def __init__(self, cfg: Optional[dict] = None, out_dir: Optional[Path] = None, robot_csv: bool = False,
                 clock=None) -> None:
        """clock 을 주면 그 시계(실제 시계 — HTTP·로봇 서버 끝-끝 시험), 없으면 가상 시계."""
        from v2.ch12.params import PARAMS
        from v2.common.paramdef import defaults
        from v2.ch12.robot_ops import RobotOps
        from v2.ch12.shared import PlcLink
        self.c = clock if clock is not None else VirtualClock(start_wall=1_793_318_400.0)
        self.c.register()
        self.out = Path(out_dir) if out_dir else Path(tempfile.mkdtemp(prefix="robot_rig_"))
        self.cfg = dict(defaults(PARAMS), **(cfg or {}))
        if "ROBOT_RECIPE_ROOT_DIR" not in (cfg or {}):
            self.cfg["ROBOT_RECIPE_ROOT_DIR"] = str(self.out / "recipes")
        self.sim = RobotPlcSim(start=0.0 if clock is None else self.c.monotonic())
        self.tr = P.make_transport(self.c, self.cfg, SimTransport(self.c, self.sim))
        self.w = P.make_worker(self.tr, self.cfg, True)
        self.plc = P.PlcDriver(self.c, self.w, self.cfg, True)
        self.link = PlcLink(self.plc)
        self.csv = None
        if robot_csv:
            from v2.ch12.outputs.robot_csv import RobotCsv
            self.cfg.update(HOST_LOG_LOCAL_DIR=str(self.out / "robot"), HOST_LOG_NAS_DIR=str(self.out / "robot_nas"))
            self.csv = RobotCsv(self.cfg, start_worker=False)
        self.reports: List[tuple] = []
        self.ops = RobotOps(self.c, self.link, self.cfg, robot_csv=self.csv,
                            on_report=lambda p, src: self.reports.append((src, p)))
        self.w.start()
        if clock is None:
            self.c.wait_until(START_T)
        else:
            self.c.wait_until(self.c.monotonic() + 5.0, lambda: self.w.ready)
        self.sim.advance_to(self.c.monotonic())

    def set_engines(self, eng: Optional[Dict[str, Any]]) -> Optional[FakeEngines]:
        self.ops.engines = None if eng is None else FakeEngines(eng, self.c, self.csv)
        return self.ops.engines

    def run(self, cmd: str, data: Optional[dict] = None, raw: Optional[dict] = None, peer: str = PEER,
            received_at: Optional[str] = None) -> Dict[str, Any]:
        """명령 하나(이 스레드에서) — {reply_cmd, body, log, elapsed}."""
        data = {} if data is None else data
        raw = dict(raw) if raw is not None else {}
        raw.setdefault("request_id", "r1")
        raw.setdefault("command", cmd)
        raw.setdefault("data", data)
        t0 = self.c.monotonic()
        rx = received_at or datetime.fromtimestamp(self.c.wall()).isoformat(timespec="milliseconds")
        rc, body, log = self.ops.handle(cmd, data, raw, peer, rx)
        return {"reply_cmd": rc, "body": body, "log": log, "elapsed": self.c.monotonic() - t0}

    def run_case(self, case: Dict[str, Any]) -> Dict[str, Any]:
        """사례 하나 — 파일·상태·어댑터·시각 사건을 준비하고 명령(또는 동시 jobs)을 돈다.
        {results: [명령마다 {reply_cmd, body, log, elapsed}], writes: 코일 쓰기 [(주소, 값)] 순서, robot_csv: 내 줄 사본 행}."""
        root = Path(self.cfg["ROBOT_RECIPE_ROOT_DIR"])
        recipe = prepare_files(root, case)
        self.sim.advance_to(self.c.monotonic())
        apply_state(self.sim, case.get("state"))
        self.set_engines(case.get("eng"))
        nw = len(self.sim.frames)
        csv_before = self.csv_rows()
        t0 = self.c.monotonic()
        evs: List[bool] = []
        for i, (dt, st) in enumerate(case.get("events") or []):
            evs.append(False)

            def ev(i=i, dt=dt, st=st):
                self.c.wait_until(t0 + float(dt))
                self.sim.advance_to(self.c.monotonic())
                apply_state(self.sim, st)
                evs[i] = True
            self.c.start_thread(ev, "robot-event")
        jobs = case.get("jobs")
        if jobs:
            results = self.run_threads([(float(d), c, subst(x, recipe)) for d, c, x in jobs])
        else:
            results = [self.run(case["cmd"], subst(case.get("data") or {}, recipe), raw=case.get("raw"))]
        self.c.wait_until(self.c.monotonic() + 1e6, lambda: all(evs))
        writes = self.sim.coil_writes_since(nw)
        if self.csv is not None:
            self.csv.writer.flush(5.0)
        return {"results": results, "writes": writes, "robot_csv": self.csv_rows()[len(csv_before):],
                "recipe": recipe}

    def csv_rows(self) -> List[List[str]]:
        if self.csv is None:
            return []
        import csv as _csv
        d = Path(self.cfg["HOST_LOG_LOCAL_DIR"])
        rows: List[List[str]] = []
        for f in sorted(d.glob("Robot_*_sputter.csv")):
            with open(f, encoding="utf-8-sig", newline="") as fp:
                rows += [r for r in _csv.reader(fp)][1:]
        return rows

    def run_threads(self, jobs: List[tuple]) -> List[Dict[str, Any]]:
        """여러 명령을 동시에(각 스레드, 시작 지연 가상 초) — jobs = [(지연, cmd, data)]."""
        out: List[Optional[dict]] = [None] * len(jobs)
        t0 = self.c.monotonic()

        def mk(i, delay, cmd, data):
            def run():
                if delay:
                    self.c.wait_until(t0 + delay)
                out[i] = self.run(cmd, data, raw={"request_id": f"r{i + 1}"})
                self.c.notify()
            return run
        with self.c.hold():
            for i, j in enumerate(jobs):
                self.c.start_thread(mk(i, *j), f"robot-job{i}")
        self.c.wait_until(self.c.monotonic() + 1e6, lambda: all(o is not None for o in out))
        return out                                   # type: ignore[return-value]

    def done(self) -> None:
        from v2.ch12.shared import stop_worker
        stop_worker(self.w)
        if self.csv is not None:
            self.csv.close()

