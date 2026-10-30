"""v1 로봇 명령 핸들러 골든 — v1 host/handlers.py 를 가짜 장비·가짜 런타임으로 실제 실행해 명령 × 상태 응답을 적는다.

실행: cd Chamber.Total_program && python v2/tools/v1golden_robot_handlers.py  →  v2/tests/golden/robot_handlers.json

무엇을 실제로 돌리나(v1 코드 그대로)
- host/setup.install_host(라우터 등록 18개·HostContext·잠금) → host/router.Router.dispatch → host/handlers.HostHandlers 전부.
  HostServer 만 가짜(소켓 없이 라우터를 꺼냄). 핸들러 예외 경로는 host/server.py:343-361 과 같은 몇 줄(로그 'Handler error for ..'
  + errors.error_reporter.notify_unknown_error, 원문 명령 + '_RESULT')을 여기서 부른다.
- PLC: v1 device/plc.AsyncPLC 실물(read_bit·write_switch·press_switch·pulse·main_shutter·_addr·log_route·_io_lock·_run_io·
  _to_plc_error·간격 150 ms·무응답 재시도 정책). 바꾼 것은 맨 아래 두 곳뿐 — _locked_thread(동기 pymodbus 호출을 스레드로 보내던
  자리)와 _client(pymodbus 클라이언트): 가짜 클라이언트가 v2 PLC 시뮬레이터(sim.robot_rig.RobotPlcSim — v2 시험과 같은 래더)에
  Modbus PDU 를 넣고 응답 지연(latency 3 ms)만큼, 무응답 주소면 PLC_TIMEOUT_S(2 s)만큼 기다린 뒤 pymodbus ModbusIOException 을 낸다.
- 요청 공정 로그(Robot_YYYYMMDD.csv): v1 util/host_process_log 실물(임시 폴더). 내 줄 사본 행을 적는다.
- 공정 시작 대기: START_SPUTTER·START_PLASMA_CLEANING 의 시작 가드 대기는 v1 ChamberRuntime·PlasmaCleaningRuntime 의
  start_with_recipe_string 실물을 가짜 객체에 묶어 돌린다(레시피 CSV 적재 — 실제 임시 파일 — · 대기 시간 한도·문구·E206).
  시작 가드 신호(_handle_start_clicked·_on_click_start)·적재 오류(raise 사례)·runtime_state·Runner 속성은 사례 표의 eng 대로 가짜.
가상 시간(근거 — v1 대기는 실제로 수십~수백 초): asyncio 이벤트 루프를 가상 시각 루프(VLoop)로 돌린다. loop.time() 이 가상 시각이고
  셀렉터가 기다려야 할 때(I/O 없음) 실제로 자지 않고 가상 시각을 다음 타이머까지 옮긴다 — asyncio.sleep·wait_for·잠금 대기가
  v1 코드 그대로 가상 시각에서 돈다. host/handlers·device/plc 의 time.monotonic 도 이 시각으로 바꾼다. 스레드 풀은 즉시 실행
  (GET_RECIPE 스캔 지연 사례만 그 시간 뒤에 결과). datetime.now()(로그 시각·기록키)는 실제 시각 — 비교에서 가린다.
적는 것(사례마다): 응답 명령·body(JSON 직렬화 문자열 — 로봇 서버가 그대로 보낸다)·로그 줄(tag, text — PLC_REMOTE 시각과 핸들러 예외
  traceback 은 가림)·PLC 에 쓴 코일 (주소, 값) 순서·응답까지 가상 초·Robot CSV 사본 행(시각 칸·기록키 시각 가림).
사례 표는 v2/tools/robot_golden_cases.cases()(v2 시험도 같은 표를 골든에서 읽는다).
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import csv
import importlib
import json
import os
import shutil
import struct
import sys
import tempfile
import traceback
import types
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "v2" / "tests" / "golden" / "robot_handlers.json"
sys.path.insert(0, str(ROOT))
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from v2.tools.robot_golden_cases import cases           # noqa: E402
from v2.ch12.sim import robot_rig as RR                  # noqa: E402

PEER_S = str(("127.0.0.1", 50123))


# ───────────────────────── 가상 시각 이벤트 루프 ─────────────────────────
class VLoop(asyncio.SelectorEventLoop):
    def __init__(self) -> None:
        super().__init__()
        self._vt = 0.0
        real = self._selector.select

        def select(timeout=None):
            ev = real(0)
            if ev or timeout == 0:
                return ev
            if timeout is None:
                ev = real(2.0)
                if ev:
                    return ev
                raise RuntimeError("가상 루프: 기다릴 것이 없음(교착)")
            self._vt += timeout
            return []
        self._selector.select = select

    def time(self) -> float:
        return self._vt


class ImmediateExecutor(concurrent.futures.ThreadPoolExecutor):
    """run_in_executor·to_thread 를 그 자리에서 실행(delay_s 가 있으면 그 가상 시간 뒤에 결과)."""

    def __init__(self, loop) -> None:
        super().__init__(max_workers=1)
        self.loop = loop
        self.delay_s = 0.0

    def submit(self, fn, /, *a, **k):
        f: concurrent.futures.Future = concurrent.futures.Future()
        try:
            v, e = fn(*a, **k), None
        except BaseException as ex:        # noqa: BLE001
            v, e = None, ex

        def done():
            if e is None:
                f.set_result(v)
            else:
                f.set_exception(e)
        if self.delay_s > 0:
            self.loop.call_later(self.delay_s, done)
        else:
            done()
        return f


class TimeProxy:
    def __init__(self, real, loop) -> None:
        self._real, self._loop = real, loop

    def monotonic(self) -> float:
        return self._loop.time()

    def __getattr__(self, k):
        return getattr(self._real, k)


# ───────────────────────── v1 모듈 ─────────────────────────
# 실행할 때만 불러온다(load_v1 — main 첫머리). import 만으로는 v1(device.plc → pymodbus 등)이 필요 없게 — 다른 v1golden_* 도구와
# 같게. 아래 이름들은 load_v1 이 채우는 모듈 전역(사례 실행 함수들이 부를 때 읽는다).
TMP: Path = None                                          # type: ignore[assignment]  # 임시 폴더(Robot CSV·레시피)
cfgc = H = S = RQ = P = CR = PR = HL = EREP = AE = None
V1_PLC_ERR = ""                                           # v1 PLC 무응답 문구(load_v1 이 pymodbus 예외 문구로 채움)


def load_v1() -> None:
    """v1 모듈을 불러오고 설정(임시 폴더)·가짜 서버를 넣는다. 두 번 불러도 한 번만."""
    global TMP, cfgc, H, S, RQ, P, CR, PR, HL, EREP, AE, V1_PLC_ERR
    if H is not None:
        return
    TMP = Path(tempfile.mkdtemp(prefix="v1golden_robot_"))
    cfgc = importlib.import_module("lib.config_common")
    for k, v in dict(HOST_LOG_ENABLED=True, HOST_LOG_LOCAL_DIR=str(TMP / "robot"), HOST_LOG_NAS_DIR=str(TMP / "robot_nas"),
                     LOG_ROOT_DIR=str(TMP / "logs"), LOCAL_FALLBACK_ROOT=str(TMP / "fallback"),
                     ROBOT_RECIPE_ROOT_DIR=str(TMP / "recipes"), RECIPE_SCAN_TIMEOUT_S=20.0,
                     CHUCK_POSITION_RESOLVE_TIMEOUT_S=90.0).items():
        setattr(cfgc, k, v)
    H = importlib.import_module("host.handlers")
    S = importlib.import_module("host.setup")
    RQ = importlib.import_module("host.request_ctx")
    P = importlib.import_module("device.plc")
    CR = importlib.import_module("runtime.chamber_runtime")
    PR = importlib.import_module("runtime.plasma_cleaning_runtime")
    HL = importlib.import_module("util.host_process_log")
    EREP = importlib.import_module("errors.error_reporter")
    AE = importlib.import_module("errors.app_error")
    S.HostServer = _DummyServer
    # v1 PLC 무응답 문구(pymodbus 예외 문구 — v2 드라이버 문구와 같을 수 없어 자리표로, differences)
    V1_PLC_ERR = f"PLC 응답 없음/프레임 오류: ModbusIOException: {P.ModbusIOException('No response received after 0 retries')}"


class _DummyServer:
    def __init__(self, host, port, router, log, chat=None, popup=None) -> None:
        self.router = router

    async def start(self) -> None:
        pass

    async def aclose(self) -> None:
        pass


# ───────────────────────── 가짜 pymodbus 클라이언트 ─────────────────────────
class _Resp:
    def __init__(self, bits=None) -> None:
        self.bits = bits or []

    def isError(self) -> bool:
        return False


class FakeClient:
    def __init__(self, loop, sim, timeout_s: float) -> None:
        self.loop, self.sim, self.timeout_s = loop, sim, timeout_s
        self.writes: list = []

    def read_coils(self, address, count=1, slave=1):       # 서명만(_uid_kwargs 가 slave 를 찾는다)
        raise RuntimeError("acall 로만")

    def write_coil(self, address, value, slave=1):
        raise RuntimeError("acall 로만")

    async def acall(self, name, address, *rest, **kw):
        now = self.loop.time()
        self.sim.advance_to(now)
        if name == "read_coils":
            count = int(kw.get("count", rest[0] if rest else 1))
            resp = self.sim._pdu(struct.pack(">BHH", 1, int(address), count), now)
        else:
            value = bool(rest[0] if rest else kw.get("value"))
            self.writes.append([int(address), value])
            resp = self.sim._pdu(struct.pack(">BHH", 5, int(address), 0xFF00 if value else 0), now)
        if resp is None:
            await asyncio.sleep(self.timeout_s)
            raise P.ModbusIOException("No response received after 0 retries")
        await asyncio.sleep(self.sim.latency_s)
        if name == "read_coils":
            nb = resp[1]
            bits = [bool(resp[2 + i // 8] >> (i % 8) & 1) for i in range(nb * 8)]
            return _Resp(bits)
        return _Resp()


def make_plc(loop, sim):
    plc = P.AsyncPLC(logger=None)
    plc._closed = True                          # resume_watchdog 가 하트비트 태스크를 만들지 않게
    cli = FakeClient(loop, sim, float(plc.cfg.timeout_s))
    plc._client = cli

    async def locked_thread(fn, *args, **kwargs):
        if getattr(fn, "__self__", None) is cli:
            return await cli.acall(fn.__name__, *args, **kwargs)
        return 0 if getattr(fn, "__name__", "") == "_drain_socket" else None
    plc._locked_thread = locked_thread
    return plc, cli


# ───────────────────────── 가짜 런타임(eng 사전) ─────────────────────────
class FakeRS:
    def __init__(self, rs: dict) -> None:
        self.running = {(k, int(c)) for k, c in (rs.get("running") or [])}
        self.error = {(k, int(c)) for k, c in (rs.get("error") or [])}
        self.finished = {(k, int(c)): float(t) for k, c, t in (rs.get("finished") or [])}

    def is_running(self, kind, ch):
        return (kind, int(ch)) in self.running

    def has_error(self, kind, ch):
        return (kind, int(ch)) in self.error

    def last_finished(self, kind, ch):
        return self.finished.get((kind, int(ch)))


class FakeCtl:
    def __init__(self, rt: dict) -> None:
        self.is_running = bool(rt.get("pc_running", False))
        st = rt.get("step")
        self.current_step = None if st is None else types.SimpleNamespace(action=types.SimpleNamespace(value=st))
        self._eta, self._min = rt.get("eta"), rt.get("min_total")

    def eta_remaining_s(self):
        return self._eta

    def estimate_min_total_s(self, rows):
        return self._min


def _noop(*a, **k):
    return None


def _raiser(rz):
    code, msg, typ = (list(rz) + [None, None, None])[:3]

    async def start(recipe, *, origin="host", origin_meta=None):
        if typ == "ValueError":
            raise ValueError(msg)
        raise AE.CodedRuntimeError(msg, code) if code else RuntimeError(msg)
    return start


def make_chamber(loop, ch: int, rt: dict, sc: dict, tasks: list):
    o = types.SimpleNamespace()
    o._runner_state = rt.get("runner_state", "IDLE")
    if "stage" in rt:
        t = asyncio.ensure_future(asyncio.Event().wait())
        tasks.append(t)
        o._runner_stage_task, o._runner_stage_kind = t, rt.get("stage")
    else:
        o._runner_stage_task, o._runner_stage_kind = None, None
    o.process_controller = FakeCtl(rt)
    n, idx = (list(rt.get("queue") or []) + [0, -1])[:2]
    o.process_queue = [{} for _ in range(int(n))]
    o.current_process_index = int(idx)
    if sc.get("raise"):
        o.start_with_recipe_string = _raiser(sc["raise"])
        return o
    o._host_start_future = None
    o.cfg = types.SimpleNamespace(_get=lambda k, d=None: {"CHAMBER_HOST_START_WAIT_TIMEOUT_S": 20.0}.get(k, d))
    o._validate_raw_rows = lambda rows, row_numbers=None: []
    o._hostlog = HL.get_host_log
    o._update_ui_from_params = _noop
    o.append_log = _noop
    pf = sc.get("preflight")

    def handle_start(clicked=False, *, origin="host", origin_meta=None):
        if pf is None:
            return
        key = str((origin_meta or {}).get("key") or "")
        fut = o._host_start_future

        def resolve():
            if pf.get("owned") and key:
                HL.get_host_log().mark_owned(key)
            if fut is not None and not fut.done():
                fut.set_result((bool(pf.get("ok")), str(pf.get("detail") or "")))
        if float(pf.get("after_s", 0.0)) <= 0:
            resolve()
        else:
            loop.call_later(float(pf["after_s"]), resolve)
    o._handle_start_clicked = handle_start
    o.start_with_recipe_string = types.MethodType(CR.ChamberRuntime.start_with_recipe_string, o)
    return o


def make_pc(loop, pcd: dict, sc: dict):
    o = types.SimpleNamespace()
    o._selected_ch = int(pcd.get("selected_ch", 1) or 1)
    o.is_running = lambda: bool(pcd.get("is_running", False))
    if sc.get("raise"):
        o.start_with_recipe_string = _raiser(sc["raise"])
        return o
    o._host_start_future = None
    o._host_start_code = None
    o._cfg_mod = types.SimpleNamespace(PC_HOST_START_WAIT_TIMEOUT_S=20.0)
    o._reject_host_start_if_running = _noop
    o.append_log = _noop
    o._apply_recipe_row_to_ui = _noop
    o._read_first_row_from_csv = types.MethodType(PR.PlasmaCleaningRuntime._read_first_row_from_csv, o)
    pf = sc.get("preflight")

    async def on_click_start(*, origin="host", origin_meta=None):
        if pf is None:
            return
        fut = o._host_start_future
        key = str((origin_meta or {}).get("key") or "")
        await asyncio.sleep(float(pf.get("after_s", 0.0)))
        if pf.get("owned") and key:
            HL.get_host_log().mark_owned(key)
        if not pf.get("ok") and pf.get("code"):
            o._host_start_code = pf["code"]
        if fut is not None and not fut.done():
            fut.set_result((bool(pf.get("ok")), str(pf.get("detail") or "")))
    o._on_click_start = on_click_start
    o.start_with_recipe_string = types.MethodType(PR.PlasmaCleaningRuntime.start_with_recipe_string, o)
    return o


# ───────────────────────── 사례 실행 ─────────────────────────
def csv_rows() -> list:
    rows = []
    for f in sorted((TMP / "robot").glob("Robot_*_sputter.csv")):
        with open(f, encoding="utf-8-sig", newline="") as fp:
            rows += [r for r in csv.reader(fp)][1:]
    return rows


async def run_case(loop, ex, case: dict) -> dict:
    root = Path(cfgc.ROBOT_RECIPE_ROOT_DIR)
    recipe = RR.prepare_files(root, case)
    sim = RR.RobotPlcSim(start=0.0)
    sim.advance_to(loop.time())
    RR.apply_state(sim, case.get("state"))
    plc, cli = make_plc(loop, sim)
    eng = case.get("eng")
    tasks: list = []
    if eng is None:
        rs = ch1 = ch2 = pc = None
    else:
        rs = FakeRS(eng.get("rs") or {})
        rts = {int(k): v for k, v in (eng.get("rt") or {}).items()}
        chs = {c: (make_chamber(loop, c, rts[c], dict(eng.get("start") or {}), tasks) if c in rts else None)
               for c in (1, 2)}
        ch1, ch2 = chs[1], chs[2]
        pc = make_pc(loop, eng["pc"], dict(eng.get("pc_start") or {})) if eng.get("pc") is not None else None
    ex.delay_s = float((eng or {}).get("scan_delay_s", 0.0))
    logs: dict = {}

    def log_fn(tag, text):
        t = asyncio.current_task()
        logs.setdefault(t, []).append([str(tag), str(text)])
    handle = await S.install_host(host="127.0.0.1", port=0, log=log_fn, plc=plc, ch1=ch1, ch2=ch2, pc=pc,
                                  runtime_state=rs, chat=None, popup=None)
    router = handle._server.router
    t0 = loop.time()
    for dt, st in case.get("events") or []:
        def ev(st=st):
            sim.advance_to(loop.time())
            RR.apply_state(sim, st)
        loop.call_at(t0 + float(dt), ev)

    async def job(i, delay, cmd, data, raw):
        if delay:
            await asyncio.sleep(t0 + delay - loop.time())
        me = asyncio.current_task()
        logs[me] = []
        raw = dict(raw or {})
        rid = str(raw.get("request_id", f"r{i + 1}"))
        cmd_raw = str(raw.get("command", cmd))
        tok = RQ.set_request_ctx({"request_id": rid, "peer": PEER_S, "received_at": datetime.now()})
        ts = loop.time()
        try:
            res_cmd, res_data = await router.dispatch(cmd_raw, data)
        except Exception as e:                       # host/server.py:343-361
            tb = traceback.format_exc()
            log_fn("NET", f"Handler error for {cmd_raw}: {e}\n{tb}")
            res_data = EREP.notify_unknown_error(e, log=log_fn, chat=None, popup=None, src="HOST",
                                                 meta={"peer": PEER_S, "request_id": rid, "cmd": cmd_raw})
            res_cmd = f"{cmd_raw}_RESULT"
        finally:
            RQ.reset_request_ctx(tok)
        return {"reply_cmd": res_cmd, "body": res_data, "log": logs[me], "elapsed": loop.time() - ts}

    before = csv_rows()
    jobs = case.get("jobs")
    if jobs:
        coros = [job(i, float(d), c, RR.subst(x, recipe), {"request_id": f"r{i + 1}"}) for i, (d, c, x) in enumerate(jobs)]
    else:
        raw = dict(case.get("raw") or {})
        raw.setdefault("request_id", "r1")
        coros = [job(0, 0.0, case["cmd"], RR.subst(case.get("data") or {}, recipe), raw)]
    results = await asyncio.gather(*coros)
    for t in tasks:
        t.cancel()
    await asyncio.sleep(0)
    return {"results": list(results), "writes": cli.writes, "robot_csv": csv_rows()[len(before):], "recipe": recipe}


def run_one(case: dict) -> dict:
    """사례 하나를 새 가상 루프에서(시각 START_T 부터 — v2 시험대와 같다)."""
    loop = VLoop()
    loop._vt = RR.START_T
    asyncio.set_event_loop(loop)
    ex = ImmediateExecutor(loop)
    loop.set_default_executor(ex)
    H.time = TimeProxy(_REAL_TIME, loop)
    P.time = TimeProxy(_REAL_TIME, loop)
    try:
        o = loop.run_until_complete(run_case(loop, ex, case))
    finally:
        loop.close()
        asyncio.set_event_loop(None)
    roots = [(o["recipe"], "<RECIPE>"), (str(Path(cfgc.ROBOT_RECIPE_ROOT_DIR)), "<ROOT>"), (V1_PLC_ERR, RR.PLC_ERR)]
    return RR.normalize(o, roots)


_REAL_TIME = importlib.import_module("time")


def main() -> None:
    load_v1()
    only = sys.argv[1:]
    out = []
    for case in cases():
        if only and case["name"] not in only:
            continue
        out.append(dict(case, golden=run_one(case)))
    meta = {"tool": "v2/tools/v1golden_robot_handlers.py", "v1": "host/handlers.py·host/router.py·host/setup.py·"
            "device/plc.py AsyncPLC·util/host_process_log.py·runtime start_with_recipe_string",
            "peer": PEER_S, "start_t": RR.START_T, "cases": len(out),
            "results": sum(len(c["golden"]["results"]) for c in out)}
    if only:
        print(json.dumps(out, ensure_ascii=False, indent=1)[:20000])
        return
    OUT.write_text(json.dumps({"meta": meta, "cases": out}, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"{OUT}: 사례 {len(out)} · 응답 {meta['results']}")
    shutil.rmtree(TMP, ignore_errors=True)


if __name__ == "__main__":
    main()
