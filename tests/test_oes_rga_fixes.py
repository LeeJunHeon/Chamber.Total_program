# tests/test_oes_rga_fixes.py
# -*- coding: utf-8 -*-
"""OES/RGA 안정화 수정 검증. pytest / 직접 실행 모두 가능."""
from __future__ import annotations

import asyncio
import csv
import json
import os
import subprocess
import sys
import tempfile
import textwrap
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

from lib import config_common as cfgc                       # noqa: E402


# ═══════════════ 1) OES: 데몬에도 init timeout 전달 ═══════════════
def test_1_oes_daemon_passes_init_timeout():
    import device.oes as OES

    captured = {}

    class _FakeProc:
        returncode = None
        def __init__(self):
            self.stdin = self.stdout = self.stderr = _FakeStream()
        def kill(self): pass
        async def wait(self): return 0

    class _FakeStream:
        def write(self, *_a): pass
        async def drain(self): pass
        async def readline(self): return b""
        def close(self): pass

    async def _fake_exec(*cmd, **kw):
        captured["env"] = dict(kw.get("env") or {})
        captured["cmd"] = list(cmd)
        raise RuntimeError("stop-here")     # 스폰 이후는 볼 필요 없다

    async def _run(timeout_s: float):
        o = OES.OESAsync.__new__(OES.OESAsync)
        o._daemon_enabled = True
        o._daemon_lock = asyncio.Lock()
        o._daemon_proc = None
        o._daemon_ready_ev = asyncio.Event()
        o._daemon_info = None
        o._daemon_stderr_tail = []
        o._ch, o._usb = 1, 0
        o._local_dir = Path(tempfile.mkdtemp())
        o._dll_path = None
        o._status = lambda m: asyncio.sleep(0)
        o._shutdown_daemon = lambda **kw: asyncio.sleep(0)
        # 워커 exe 가 있는 것처럼
        exe = o._local_dir / "oes_worker.exe"
        exe.write_bytes(b"x")
        OES._resolve_worker_command = lambda: [str(exe)]
        try:
            await o._ensure_daemon_started(timeout_s=timeout_s)
        except Exception:
            pass

    orig = asyncio.create_subprocess_exec
    asyncio.create_subprocess_exec = _fake_exec           # type: ignore[assignment]
    try:
        asyncio.run(_run(20.0))
        assert captured["env"]["OES_INIT_TIMEOUT_S"] == "18.0", captured["env"].get("OES_INIT_TIMEOUT_S")
        asyncio.run(_run(5.0))
        assert captured["env"]["OES_INIT_TIMEOUT_S"] == "5.0", "하한 5초"
    finally:
        asyncio.create_subprocess_exec = orig             # type: ignore[assignment]


# ═══════════════ 2) rga_step_timeout_ms ═══════════════
def test_2_rga_step_timeout_ms():
    old = (cfgc.RGA_MAX_ATTEMPTS, cfgc.RGA_WORKER_TIMEOUT_S, cfgc.RGA_RETRY_DELAY_S)
    try:
        cfgc.RGA_MAX_ATTEMPTS, cfgc.RGA_WORKER_TIMEOUT_S, cfgc.RGA_RETRY_DELAY_S = 3, 60.0, 1.0
        assert cfgc.rga_step_timeout_ms() == 187000
        cfgc.RGA_MAX_ATTEMPTS, cfgc.RGA_WORKER_TIMEOUT_S, cfgc.RGA_RETRY_DELAY_S = 2, 30.0, 0.5
        assert cfgc.rga_step_timeout_ms() == 65500, "설정 변경이 즉시 반영"
    finally:
        cfgc.RGA_MAX_ATTEMPTS, cfgc.RGA_WORKER_TIMEOUT_S, cfgc.RGA_RETRY_DELAY_S = old


# ═══════════════ 3) device/rga.py — 가짜 워커 ═══════════════
_FAKE_WORKER = textwrap.dedent('''
    import json, sys, time, argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--ch", type=int); ap.add_argument("--timeout", type=float, default=30.0)
    ap.add_argument("--csv", default=""); ap.add_argument("--csv_fallback", default="")
    ap.add_argument("--mode", default="ok")
    a, _ = ap.parse_known_args()
    if a.mode == "sleep":
        time.sleep(60)
    elif a.mode == "fail":
        print(json.dumps({"ok": False, "worker_version": 2, "error": "boom"}), flush=True)
        raise SystemExit(30)
    elif a.mode == "csvfail":
        print(json.dumps({"ok": True, "worker_version": 2, "csv_ok": False,
                          "csv_path": "", "csv_error": "primary(Q:/x): OSError",
                          "csv_fallback_used": False,
                          "mass_axis": [1, 2], "pressures": [0.1, 0.2],
                          "argv_csv": a.csv, "argv_fb": a.csv_fallback}), flush=True)
    else:
        print(json.dumps({"ok": True, "worker_version": 2, "csv_ok": True,
                          "csv_path": a.csv, "csv_error": "", "csv_fallback_used": False,
                          "mass_axis": [1, 2], "pressures": [0.1, 0.2],
                          "argv_csv": a.csv, "argv_fb": a.csv_fallback}), flush=True)
''')


def _mk_rga(tmp: Path, mode: str):
    from device.rga import RGAWorkerClient
    wpy = tmp / "fake_worker.py"
    wpy.write_text(_FAKE_WORKER, encoding="utf-8")
    o = RGAWorkerClient.__new__(RGAWorkerClient)
    o.ch = 1
    o.logger = None
    o._proc = None
    o._q = asyncio.Queue()
    o.default_timeout_s = 10.0
    o._resolve_worker_cmd = lambda: [sys.executable, str(wpy), "--mode", mode]
    return o


async def _drain(o):
    evs = []
    while not o._q.empty():
        evs.append(o._q.get_nowait())
    return evs


def _kinds(evs):
    out = {}
    for e in evs:
        out[e.kind] = out.get(e.kind, 0) + 1
    return out


def test_3a_ok_csv_ok():
    tmp = Path(tempfile.mkdtemp())
    o = _mk_rga(tmp, "ok")
    asyncio.run(o.scan_histogram_to_csv(timeout_s=10.0))
    evs = asyncio.run(_drain(o))
    k = _kinds(evs)
    assert k.get("data") == 1 and k.get("finished") == 1 and k.get("failed", 0) == 0, k


def test_3b_ok_csv_fail_no_retry():
    tmp = Path(tempfile.mkdtemp())
    o = _mk_rga(tmp, "csvfail")
    asyncio.run(o.scan_histogram_to_csv(timeout_s=10.0))
    evs = asyncio.run(_drain(o))
    k = _kinds(evs)
    assert k.get("data") == 1 and k.get("finished") == 1 and k.get("failed", 0) == 0, k
    msgs = [str(e.payload.get("message", "")) for e in evs if e.kind == "status"]
    assert any("CSV 저장 실패" in m for m in msgs), msgs
    assert sum(1 for m in msgs if "attempt 2/" in m) == 0, "재시도 없음"


def test_3c_fail_retries_then_failed():
    old = cfgc.RGA_MAX_ATTEMPTS
    try:
        cfgc.RGA_MAX_ATTEMPTS = 3
        cfgc.RGA_RETRY_DELAY_S = 0.01
        tmp = Path(tempfile.mkdtemp())
        o = _mk_rga(tmp, "fail")
        asyncio.run(o.scan_histogram_to_csv(timeout_s=10.0))
        evs = asyncio.run(_drain(o))
        k = _kinds(evs)
        assert k.get("failed") == 1 and k.get("finished") == 1, k
        msgs = [str(e.payload.get("message", "")) for e in evs if e.kind == "status"]
        assert sum(1 for m in msgs if "재시도" in m) == 2, msgs

        cfgc.RGA_MAX_ATTEMPTS = 1
        o = _mk_rga(tmp, "fail")
        asyncio.run(o.scan_histogram_to_csv(timeout_s=10.0))
        evs = asyncio.run(_drain(o))
        msgs = [str(e.payload.get("message", "")) for e in evs if e.kind == "status"]
        assert sum(1 for m in msgs if "재시도" in m) == 0, "1회면 즉시 failed"
        assert _kinds(evs).get("failed") == 1
    finally:
        cfgc.RGA_MAX_ATTEMPTS = old
        cfgc.RGA_RETRY_DELAY_S = 1.0


def test_3d_timeout_kills_worker():
    old = cfgc.RGA_MAX_ATTEMPTS
    try:
        cfgc.RGA_MAX_ATTEMPTS = 1
        cfgc.RGA_RETRY_DELAY_S = 0.01
        tmp = Path(tempfile.mkdtemp())
        o = _mk_rga(tmp, "sleep")
        asyncio.run(o.scan_histogram_to_csv(timeout_s=1.0))
        evs = asyncio.run(_drain(o))
        assert _kinds(evs).get("failed") == 1, _kinds(evs)
        msgs = [str(e.payload.get("message", "")) for e in evs
                if e.kind in ("failed", "status")]
        assert any("timeout" in m.lower() for m in msgs), msgs
        if o._proc is not None:
            assert o._proc.returncode is not None, "프로세스가 종료돼야 한다"
    finally:
        cfgc.RGA_MAX_ATTEMPTS = old
        cfgc.RGA_RETRY_DELAY_S = 1.0


def test_3e_cmd_has_csv_args():
    tmp = Path(tempfile.mkdtemp())
    o = _mk_rga(tmp, "ok")
    asyncio.run(o.scan_histogram_to_csv(timeout_s=10.0))
    evs = asyncio.run(_drain(o))
    start = [str(e.payload.get("message", "")) for e in evs
             if e.kind == "status" and "start:" in str(e.payload.get("message", ""))]
    assert start, "시작 status 없음"
    pri, fb = o._resolve_csv_paths()
    assert "--csv" in start[0] and pri in start[0], start[0]
    assert "--csv_fallback" in start[0] and fb in start[0], start[0]


# ═══════════════ 4) rga_api.py — CSV 폴백 ═══════════════
def test_4_worker_csv_fallback():
    tmp = Path(tempfile.mkdtemp())
    runner = tmp / "run_worker.py"
    # 정본은 '일반 파일 아래' 경로 → root/관리자 권한에서도 반드시 실패한다
    afile = tmp / "afile"
    afile.write_text("not a dir", encoding="utf-8")
    bad = str(afile / "x.csv")
    fb = tmp / "fb" / "RGA_spectrums.csv"
    runner.write_text(textwrap.dedent(f'''
        import sys, json
        sys.path.insert(0, {str(_ROOT)!r})
        sys.path.insert(0, {str(_ROOT / "apps" / "rga_service")!r})
        import rga_api
        rga_api.rga_measure_once = lambda ip, u, p: ([1.0, 2.0], [0.5, 0.6])
        sys.argv = ["w", "--ch", "1", "--timeout", "5",
                    "--csv", {bad!r}, "--csv_fallback", {str(fb)!r}]
        raise SystemExit(rga_api.main())
    '''), encoding="utf-8")
    r = subprocess.run([sys.executable, str(runner)], capture_output=True, text=True, timeout=60)
    payload = json.loads(r.stdout.strip().splitlines()[-1])
    assert payload["ok"] is True, payload
    assert payload["csv_ok"] is True, payload
    assert payload["csv_fallback_used"] is True, payload
    assert payload["worker_version"] == 2
    assert fb.exists(), "폴백 파일 생성"
    with open(fb, "r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.reader(f))
    assert rows[0][0] == "Time" and len(rows[0]) == 3, rows[0]

    # 둘 다 실패 → ok:true, csv_ok:false
    bad2 = str(afile / "y.csv")      # 폴백도 같은 방식으로 반드시 실패
    runner2 = tmp / "run_worker2.py"
    runner2.write_text(textwrap.dedent(f'''
        import sys
        sys.path.insert(0, {str(_ROOT)!r})
        sys.path.insert(0, {str(_ROOT / "apps" / "rga_service")!r})
        import rga_api
        rga_api.rga_measure_once = lambda ip, u, p: ([1.0], [0.5])
        sys.argv = ["w", "--ch", "1", "--timeout", "5",
                    "--csv", {bad!r}, "--csv_fallback", {bad2!r}]
        raise SystemExit(rga_api.main())
    '''), encoding="utf-8")
    r2 = subprocess.run([sys.executable, str(runner2)], capture_output=True, text=True, timeout=60)
    p2 = json.loads(r2.stdout.strip().splitlines()[-1])
    assert p2["ok"] is True and p2["csv_ok"] is False, p2
    assert p2["csv_error"] and "primary" in p2["csv_error"] and "fallback" in p2["csv_error"], p2
    assert p2["pressures"] == [0.5], "측정 결과는 보존"


# ═══════════════ 5) process_controller: 모듈 상수 참조 0건 ═══════════════
def test_5_pc_uses_function_not_constant():
    src = (_ROOT / "controller" / "process_controller.py").read_text(encoding="utf-8")
    assert "RGA_STEP_TIMEOUT_MS" not in src, "모듈 상수 참조가 남아 있다"
    assert "_cfgc.rga_step_timeout_ms()" in src
    # 저장소 전체에서도 정의부(config_common) 외에는 참조 0건
    hits = []
    for p in _ROOT.rglob("*.py"):
        if {".git", "__pycache__", "dist", "build", "tests"} & set(p.parts):
            continue
        if p.name == "config_common.py":
            continue
        if "RGA_STEP_TIMEOUT_MS" in p.read_text(encoding="utf-8", errors="ignore"):
            hits.append(str(p.relative_to(_ROOT)))
    assert hits == [], hits


# ═══════════════ 6) stale RGA 이벤트 무시 ═══════════════
def test_6_stale_rga_events_ignored():
    from controller.process_controller import ProcessController
    pc = ProcessController.__new__(ProcessController)
    pc._rga_step_abandoned = True
    logs = []
    pc._emit_log = lambda src, m: logs.append(m)
    failed = []
    pc._step_failed = lambda *a, **k: failed.append(a)
    matched = []
    pc._match_token = lambda t: matched.append(t)

    pc.on_rga_finished()
    pc.on_rga_failed("RGA", "boom")
    assert matched == [] and failed == [], "stale 이벤트는 무시"
    assert len(logs) == 2 and all("stale" in m for m in logs), logs

    # 새 스텝이 시작되면 다시 받는다
    pc._rga_step_abandoned = False
    pc.on_rga_finished()
    assert len(matched) == 1


def _run_all():
    fns = [(n, f) for n, f in sorted(globals().items())
           if n.startswith("test_") and callable(f)]
    fails = []
    for n, f in fns:
        try:
            f()
            print(f"  OK   {n}")
        except Exception as e:
            import traceback
            fails.append(n)
            print(f"  FAIL {n}: {type(e).__name__}: {e}")
            traceback.print_exc()
    print("=" * 56)
    print(f"실패 {len(fails)}건" + (": " + ", ".join(fails) if fails else " — 전부 통과"))
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(_run_all())
