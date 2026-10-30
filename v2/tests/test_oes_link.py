"""OES 링크(v2/ch12/drivers/oes_link.py) ↔ 실제 하위 프로세스 워커(v2/ch12/workers/oes_worker --fake).

장비 대신 가짜 DLL(oes_fake) — 워커의 나머지 코드(초기화 순서·측정 루프·CSV·stop 플래그·JSONL·os._exit)는 실제와 같다.
확인: 시작·측정·CSV·tail(on_data)·하트비트·1초 행, STOP 플래그, DLL 호출 순서·인자(v1 그대로), 워커 죽음 → 재기동 → one-shot 폴백,
읽기 걸림(워커 hard_abort 125)·메인 시간 초과 kill, 초기화 실패, bench 거부, 프로세스 재시작, v1 골든(보낸 줄·명령줄·문구).
"""
from __future__ import annotations

import json
import os
import re
import signal
import threading
import time
from pathlib import Path

import pytest

from v2.common.clock import RealClock, VirtualClock
from v2.common.snapshot_bus import Source, SnapshotBus, TickLoop
from v2.ch12.drivers import oes_link as ol
from v2.ch12.drivers.worker_proc import status_worker

GOLDEN = json.loads((Path(__file__).parent / "golden" / "dev_oes.json").read_text(encoding="utf-8"))
V2_ONLY_PAIRS = {"--dll_path", "--home", "--hb_s", "--id"}


def wait_for(pred, timeout=20.0, step=0.02) -> bool:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if pred():
            return True
        time.sleep(step)
    return pred()


class Rig:
    def __init__(self, tmp: Path, monkeypatch, fake: str = "", writes_allowed: bool = True, chamber: str = "ch1", **cfg):
        monkeypatch.delenv("OES_LOCAL_BASE", raising=False)
        monkeypatch.delenv("OES_DAEMON", raising=False)
        monkeypatch.setenv("OES_FAKE", fake)
        self.calls = tmp / f"calls_{chamber}.jsonl"
        monkeypatch.setenv("OES_FAKE_LOG", str(self.calls))
        self.tmp = tmp
        self.cfg = {"OES_LOCAL_DIR": str(tmp / "oes"), "OES_WORKER_FAKE": True, "OES_WORKER_HOME": str(tmp / "home"),
                    "CHAMBER_OES_INIT_TIMEOUT_S": 20.0, **cfg}
        self.events, self.data = [], []
        self.clock = RealClock()
        self.link = ol.OesLink(self.clock, None, self.cfg, writes_allowed, on_event=self.events.append, chamber=chamber,
                               on_data=lambda x, y: self.data.append((len(x), len(y))))

    def init(self, **kw):
        r, ev = {}, threading.Event()
        self.link.initialize(on_done=lambda ok, err: (r.update(ok=ok, err=err), ev.set()), **kw)
        assert ev.wait(30)
        return r["ok"], r["err"]

    def measure_async(self, dur, integ):
        r, ev = {}, threading.Event()
        self.link.measure(dur, integ, on_done=lambda res: (r.update(res=res), ev.set()))
        return r, ev

    def measure(self, dur, integ, wait=60):
        r, ev = self.measure_async(dur, integ)
        assert ev.wait(wait), "측정 결과가 오지 않음"
        return r["res"]

    def statuses(self):
        return [e.detail for e in self.events if e.kind == "status"]

    def dll_calls(self):
        if not self.calls.exists():
            return []
        return [json.loads(x) for x in self.calls.read_text(encoding="utf-8").splitlines() if x.strip()]

    def close(self):
        self.link.close()
        assert not self.link.status()["daemon_alive"]


@pytest.fixture
def rigs(tmp_path, monkeypatch):
    made = []

    def make(**kw):
        r = Rig(tmp_path, monkeypatch, **kw)
        made.append(r)
        return r
    yield make
    for r in made:
        r.link.close()


# ───────── 시작·측정·1초 행 ─────────
def test_daemon_start_measure_csv_tail_heartbeat(rigs):
    r = rigs()
    ok, err = r.init()
    assert ok and err is None
    st = r.link.status()
    assert st["daemon_alive"] and st["daemon_ready"]
    assert wait_for(lambda: r.link.status()["hb_age_s"] is not None, 5)
    res_box, ev = r.measure_async(2.5, 50)
    assert wait_for(lambda: r.link.source_read(None, 0)["ch1.oes.measuring"], 10)
    assert wait_for(lambda: r.link.source_read(None, 0)["ch1.oes.sample_age_s"] is not None, 10)
    assert ev.wait(30)
    res = res_box["res"]
    assert res.emitted and res.success and res.message == "OES 측정 완료(로컬 저장)"
    csv_path = Path(res.out_csv)
    assert csv_path.parent == r.tmp / "oes" / "CH1" and re.fullmatch(r"OES_Data_\d{8}_\d{6}\.csv", csv_path.name)
    lines = csv_path.read_text(encoding="utf-8").splitlines()
    head = lines[0].split(",")
    assert head[0] == "Time" and len(head) == 1 + 1024          # ROI [10, 1034) — 파장 축(spGetWLTable)
    assert float(head[1]) == pytest.approx(200.0 + 0.6 * 10)
    assert len(lines) - 1 == res.rows >= 3 and re.fullmatch(r"\d\d:\d\d:\d\d", lines[1].split(",")[0])
    assert r.data and r.data[0] == (1024, 1024)
    row = r.link.source_read(None, 0)
    assert row["ch1.oes.running"] is True and row["ch1.oes.measuring"] is False
    assert any(e.kind == "finished" for e in r.events)
    h = r.link._daemon
    r.close()
    assert r.link.sent[-1]["cmd"] == "close"                    # 정상 종료는 close 명령으로
    assert h.exited and h.returncode == 0                        # close → 정리 → os._exit(0)(kill 아님)


def test_daemon_exits_by_itself_when_main_stdin_closes(rigs):
    """메인이 죽어 stdin 이 닫히면 워커는 EOF 를 보고 정리·종료(v1 oes_api.py:2285-2290)."""
    r = rigs()
    assert r.init()[0]
    h = r.link._daemon
    h.close_stdin()
    assert wait_for(lambda: h.exited, 10) and h.returncode == 0
    assert any("stdin closed (EOF)" in s for s in r.statuses())
    assert r.dll_calls()[-1]["fn"] == "spCloseGivenChannel"


def test_dll_call_order_and_args_are_v1(rigs):
    r = rigs(fake="model=0")                                       # SONY — probe 가 PDA→G9212→SONY 순으로 시도
    assert r.init()[0]
    res = r.measure(2.0, 70)
    assert res.success
    calls = r.dll_calls()
    names = [c["fn"] for c in calls]
    first = names.index("spSetTrgEx")
    init_seq = [(c["fn"], c["args"]) for c in calls[:first]]
    compact = []
    for fn, args in init_seq:
        if not compact or compact[-1][0] != fn or fn == "spInitGivenChannel":
            compact.append((fn, args))
    assert compact == [
        ("spTestAllChannels", [0]), ("spDevInfo", ["<buf 512>", "<buf 512>", 0]), ("spSetupGivenChannel", [0]),
        ("spInitGivenChannel", [2, 0]), ("spInitGivenChannel", [3, 0]), ("spInitGivenChannel", [0, 0]),
        ("spReadDataEx", ["<buf 2080>", 0]), ("spGetWLTable", ["<buf 2080>", 0]),
    ]
    assert [(c["fn"], c["args"]) for c in calls[first:first + 3]] == [
        ("spSetTrgEx", [11, 0]), ("spSetTEC", [1, 0]), ("spSetDblIntEx", [70.0, 0])]
    assert "spAutoDark" not in names                                # v1 에서 주석 처리 — 보내지 않음
    reads = [c for c in calls[first + 3:] if c["fn"] == "spReadDataEx"]
    assert len(reads) % 3 == 0 and len(reads) >= 6                   # 표본마다 avg_count 3 번
    r.close()
    assert r.dll_calls()[-1]["fn"] == "spCloseGivenChannel"


def test_stop_writes_two_local_flags_and_ends_early(rigs, monkeypatch):
    r = rigs()
    written = []
    orig = ol.OesLink._write_flags

    def rec(usb_flag, csv_flag):
        orig(usb_flag, csv_flag)
        written.append({p.name: p.read_text(encoding="utf-8") for p in (usb_flag, csv_flag) if p is not None})
    monkeypatch.setattr(ol.OesLink, "_write_flags", staticmethod(rec))
    assert r.init()[0]
    t0 = time.monotonic()
    box, ev = r.measure_async(30.0, 50)
    assert wait_for(lambda: r.link.is_running and r.link.status()["rows"] >= 1, 15)
    r.link.stop(wait=False)
    assert ev.wait(20) and time.monotonic() - t0 < 15
    assert box["res"].success
    norm = [{re.sub(r"OES_Data_\d{8}_\d{6}", "OES_Data_<TS>", k): v for k, v in w.items()} for w in written]
    assert norm == GOLDEN["stop_flags"]
    assert "[OES] daemon stop flag set(detach)" in r.statuses()
    assert not list((r.tmp / "oes" / "CH1").glob("*.stop"))           # 워커가 끝에 지움


# ───────── 죽음·시간 초과·실패 ─────────
@pytest.mark.slow
def test_daemon_death_restart_then_oneshot_fallback(rigs):
    r = rigs(fake="die_after=8")                     # 표본 2개째 읽기에서 워커 프로세스가 죽음
    assert r.init()[0]
    res = r.measure(3.0, 50)
    assert res.emitted and not res.success and res.message.startswith("OES 측정 실패(rc=")
    st = r.statuses()
    assert any(s.startswith("[OES] daemon 실패 → 재기동 후 재시도: RuntimeError: daemon stdout closed") for s in st)
    assert any(s.startswith("[OES] daemon 재시도 실패 → one-shot fallback") for s in st)
    assert [s["tag"] for s in r.link.spawned] == ["daemon", "daemon", "measure"]
    assert any(e.kind == "worker_exit" for e in r.events)


def test_worker_read_hang_hard_abort_125(rigs, monkeypatch):
    monkeypatch.setenv("OES_READ_TIMEOUT_S", "0.5")
    r = rigs(fake="hang_read_after=5", OES_DAEMON=False)
    res = r.measure(5.0, 50)
    assert not res.success and "read timeout" in (res.error or "")
    assert res.message == "OES 측정 실패(rc=125)"


@pytest.mark.slow
def test_main_timeout_kills_hung_daemon(rigs, monkeypatch):
    monkeypatch.setenv("OES_READ_TIMEOUT_S", "100")              # 워커는 스스로 포기하지 않음 → 메인 시간 초과
    r = rigs(fake="hang_read_after=5", OES_MEASURE_TIMEOUT_MIN_S=1.0, OES_MEASURE_TIMEOUT_MARGIN_S=1.0, OES_CLOSE_STEP_S=0.5)
    assert r.init()[0]
    t0 = time.monotonic()
    res = r.measure(1.0, 50)
    assert not res.success and time.monotonic() - t0 < 30
    assert any("TimeoutError: daemon measure timeout" in s for s in r.statuses())
    assert any(s == "[OES] 워커 timeout → 강제 종료" for s in r.statuses())
    assert not r.link.status()["oneshot_alive"]


def test_init_fail_reports_error(rigs):
    r = rigs(fake="devices=0")
    ok, err = r.init()
    assert not ok and "장치 스캔 실패: detected=0" in err
    assert any(s.startswith("[OES] daemon READY 실패") for s in r.statuses())
    assert not r.link.status()["daemon_alive"]


def test_bench_rejects_without_spawning(rigs):
    r = rigs(writes_allowed=False)
    ok, err = r.init()
    assert not ok and err == "BENCH_READ_ONLY"
    res = r.measure(1.0, 50)
    assert not res.emitted and res.reason == "BENCH_READ_ONLY"
    assert r.link.spawned == []


def test_restart_after_external_kill(rigs):
    r = rigs()
    assert r.init()[0]
    pid = r.link.status()["daemon_pid"]
    os.kill(pid, getattr(signal, "SIGKILL", signal.SIGTERM))      # 바깥에서 죽임(윈도우는 TerminateProcess)
    assert wait_for(lambda: any(e.kind == "worker_exit" for e in r.events), 10)
    assert r.link.source_read(None, 0)["ch1.oes.running"] is False
    res = r.measure(2.0, 50)                                         # 측정 전에 daemon 을 다시 띄운다
    assert res.success
    assert [s["tag"] for s in r.link.spawned] == ["daemon", "daemon"]
    assert r.link.status()["daemon_pid"] != pid


def test_oneshot_mode_init_and_measure(rigs):
    r = rigs(OES_DAEMON=False, chamber="ch2")
    assert r.init()[0]
    res = r.measure(2.0, 60)
    assert res.success and res.message == "OES 측정 완료(로컬 저장)"
    assert Path(res.out_csv).parent == r.tmp / "oes" / "CH2"
    assert [s["tag"] for s in r.link.spawned] == ["init", "measure"]
    assert r.dll_calls()[0]["args"] == [0] and any(c["fn"] == "spSetupGivenChannel" and c["args"] == [1] for c in r.dll_calls())


# ───────── v1 골든: 메인 → 워커 메시지 ─────────
def _norm(v, base: str):
    if isinstance(v, str):
        return re.sub(r"OES_Data_\d{8}_\d{6}\.csv", "OES_Data_<TS>.csv", v.replace(base, "<BASE>"))
    if isinstance(v, dict):
        return {k: _norm(x, base) for k, x in v.items()}
    return v


def _v1_args(argv: list) -> list:
    args = argv[3:] if argv[1:3] == ["-m", ol.MODULE] else argv[1:]
    out, i = [], 0
    while i < len(args):
        if args[i] in V2_ONLY_PAIRS:
            i += 2
            continue
        if args[i] == "--fake":
            i += 1
            continue
        out.append(args[i])
        i += 1
    return out


@pytest.mark.slow
def test_golden_v1_messages_and_argv(rigs, monkeypatch):
    d = rigs()
    base = str(d.tmp)
    assert d.init()[0]
    assert d.measure(2.0, 50).message == GOLDEN["finished"][0]["message"]
    assert d.measure(2.0, 0).message == GOLDEN["finished"][1]["message"]
    box, ev = d.measure_async(30.0, 50)
    assert wait_for(lambda: d.link.is_running and d.link.status()["rows"] >= 1, 15)
    d.link.stop(wait=False)
    assert ev.wait(20) and box["res"].message == GOLDEN["finished"][2]["message"]
    d.close()
    sent = [_norm({k: v for k, v in m.items() if k not in ("v", "id")}, base) for m in d.link.sent]
    assert sent == GOLDEN["stdin"]
    o = Rig(d.tmp, monkeypatch, OES_DAEMON=False, chamber="ch2")
    try:
        assert o.init()[0]
        assert o.measure(2.0, 60).message == GOLDEN["finished"][3]["message"]
    finally:
        o.link.close()
    spawns = [{"argv": [_norm(a, base) for a in _v1_args(s["argv"])], "env": {"OES_INIT_TIMEOUT_S": s["env"]["OES_INIT_TIMEOUT_S"]}}
              for s in d.link.spawned + o.link.spawned]
    want = [{"argv": g["argv"], "env": {"OES_INIT_TIMEOUT_S": g["env"]["OES_INIT_TIMEOUT_S"]}} for g in GOLDEN["spawns"]]
    assert spawns == want
    # v2 가 더한 인자는 --dll_path(v1 워커가 스스로 찾던 같은 파일)·--home·--hb_s·--id·--fake 뿐
    extra = {a for s in d.link.spawned for a in s["argv"] if a.startswith("--")} - {a for g in GOLDEN["spawns"] for a in g["argv"] if a.startswith("--")}
    assert extra == {"--dll_path", "--home", "--hb_s", "--fake"}


# ───────── 1초 행 ─────────
def test_one_second_row_status_only():
    clock = VirtualClock()
    link = ol.OesLink(clock, None, {}, True, chamber="ch2")
    blk = ol.blocks("ch2")
    assert [f.name for f in blk[0].fields] == ["running", "measuring", "sample_age_s"]
    w = status_worker(clock, "oes2")
    rows = []
    bus = SnapshotBus(clock, blk, [Source("oes2", w, link.source_read, ("ch2.oes",))])
    loop = TickLoop(clock, bus, sinks=[rows.append])
    clock.register()                        # 시험 스레드가 tick 을 돌린다(챔버K test_ig 와 같은 방식)
    w.start()
    loop.run(2)
    w.stop()
    clock.unregister()
    assert len(rows) == 2
    assert rows[-1]["ch2.oes.q"] == "ok" and rows[-1]["ch2.oes.running"] is False
    assert rows[-1]["ch2.oes.measuring"] is False and rows[-1]["ch2.oes.sample_age_s"] is None
    assert ol.BUDGET["fits_1s"] and ol.BUDGET["queries_per_tick"] == 0
