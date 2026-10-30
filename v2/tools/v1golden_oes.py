r"""v1 OES 메인↔워커 메시지 골든 — v1 device/oes.py 의 OESAsync 를 실제로 돌려, 가짜 워커 프로세스가 받은 것을 잡는다.

하는 일
1) 임시 폴더 T 에 'T\apps\oes_service\oes_worker.exe'(실행 가능한 파이썬 스크립트 — v1 이 찾는 이름) 를 만든다.
   가짜 워커는 v1 JSONL 계약대로 답하면서 받은 명령줄(argv)·환경(OES_INIT_TIMEOUT_S)·stdin 줄·stop 플래그 파일을 기록한다.
2) sys.argv[0] = T\main.py(v1 _main_exe_dir), OES_LOCAL_BASE = T\oes 로 두고 v1 OESAsync 를 importlib 로 불러 시나리오를 돈다:
   daemon: initialize_device → run_measurement(2 s, 50 ms) → run_measurement(2 s, 0 ms → 기본 50) → 측정 중 stop_measurement(wait=False)
           → _shutdown_daemon(graceful=True)(close 줄),  one-shot(OES_DAEMON=0): initialize_device → run_measurement(2 s, 60 ms)
3) 경로는 <BASE>, 파일 시각은 <TS> 로 바꿔 v2/tests/golden/dev_oes.json 에 쓴다(시험은 v1 을 import 하지 않고 이 파일만 읽는다).

실행: cd <저장소> && QT_QPA_PLATFORM=offscreen python3 -m v2.tools.v1golden_oes   (리눅스·윈도우 모두 — 윈도우는 .exe 가 스크립트가 아니라 안 됨)
"""
from __future__ import annotations

import asyncio
import importlib
import json
import os
import re
import stat
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "v2" / "tests" / "golden" / "dev_oes.json"

FAKE_V1_WORKER = r'''#!{python}
# v1 JSONL 계약을 흉내 내는 가짜 OES 워커(골든 생성용) — 받은 것을 V1G_CAPTURE 에 기록
import json, os, sys, time
from pathlib import Path
cap = os.environ["V1G_CAPTURE"]
def rec(**k):
    with open(cap, "a", encoding="utf-8") as f:
        f.write(json.dumps(k, ensure_ascii=False) + "\n")
def out(o):
    sys.stdout.write(json.dumps(o, ensure_ascii=False) + "\n"); sys.stdout.flush()
a = sys.argv[1:]
def arg(n, d=None):
    return a[a.index(n) + 1] if n in a else d
rec(what="spawn", argv=a, env={{k: os.environ.get(k) for k in ("OES_INIT_TIMEOUT_S", "PYTHONUNBUFFERED")}})
cmd, ch, usb = arg("--cmd"), int(arg("--ch")), int(arg("--usb"))
def measure(out_csv, dur, out_dir):
    p = Path(out_csv); p.parent.mkdir(parents=True, exist_ok=True)
    flags = [Path(out_dir) / f".stop_usb{{usb}}.flag", Path(out_csv + ".stop")]
    with open(p, "w", encoding="utf-8", newline="") as f:
        f.write("Time,200.0,200.6,201.2\n"); f.write("10:00:00,1,2,3\n"); f.flush()
        out({{"kind": "started", "ok": True, "ch": ch, "usb": usb, "resolved_usb": usb, "out_csv": out_csv, "cols": 3}})
        rows, stopped, t0 = 1, False, time.time()
        while time.time() - t0 < dur:
            seen = {{x.name: x.read_text(encoding="utf-8") for x in flags if x.exists()}}
            if seen:
                rec(what="stop_flags", files=seen); stopped = True; break
            time.sleep(0.05)
        f.write("10:00:01,1,2,3\n"); rows += 1
    for x in flags:
        try: x.unlink()
        except OSError: pass
    out({{"kind": "finished", "ok": True, "stopped": stopped, "stop_reason": "stop_flag" if stopped else None, "ch": ch,
          "usb": usb, "out_csv": out_csv, "nas_ok": False, "nas_csv": None, "nas_error": None, "local_deleted": False,
          "rows": rows, "elapsed_s": round(time.time() - t0, 3)}})
if cmd == "init":
    out({{"kind": "init", "ok": True, "ch": ch, "usb": usb, "resolved_usb": usb, "pixels": 1056, "model": "PDA"}})
elif cmd == "measure":
    measure(arg("--out_csv"), float(arg("--duration")), str(Path(arg("--out_csv")).parent))
else:
    out({{"kind": "daemon", "ok": True, "ch": ch, "usb": usb, "resolved_usb": usb, "pixels": 1056, "model": "PDA"}})
    for line in sys.stdin:
        s = line.strip()
        if not s:
            continue
        o = json.loads(s); rec(what="stdin", obj=o)
        if o.get("cmd") == "close":
            break
        if o.get("cmd") == "measure":
            measure(o["out_csv"], float(o["duration_s"]), o["out_dir"])
'''


def _norm(v, base: str):
    if isinstance(v, str):
        v = v.replace(base, "<BASE>")
        return re.sub(r"OES_Data_\d{8}_\d{6}\.csv", "OES_Data_<TS>.csv", v)
    if isinstance(v, list):
        return [_norm(x, base) for x in v]
    if isinstance(v, dict):
        return {_norm(k, base): _norm(x, base) for k, x in v.items()}
    return v


async def _scenarios(oes_mod, events: list) -> None:
    async def pump(o, tag):
        async for ev in o.events():
            if ev.kind == "status" and os.environ.get("V1G_DEBUG"):
                print(tag, ev.message[:200])
            if ev.kind == "finished":
                events.append({"scenario": tag, "success": bool(ev.success), "message": ev.message, "rows": ev.rows})

    os.environ.pop("OES_DAEMON", None)
    d = oes_mod.OESAsync(chamber=1, usb_index=0)
    t = asyncio.create_task(pump(d, "daemon"))
    assert await d.initialize_device(timeout_s=20.0)
    await d.run_measurement(2.0, 50)
    await d.run_measurement(2.0, 0)
    m = asyncio.create_task(d.run_measurement(30.0, 50))
    for _ in range(200):
        await asyncio.sleep(0.05)
        if d._out_csv_local and d._out_csv_local.exists():
            break
    await asyncio.sleep(0.3)
    await d.stop_measurement(wait=False)
    await asyncio.wait_for(m, 20)
    if os.environ.get("V1G_DEBUG"):
        print("after stop", m.result(), d._ev_q.qsize(), d._stop_requested)
    await d._shutdown_daemon(graceful=True)
    await asyncio.sleep(0.2)
    t.cancel()

    os.environ["OES_DAEMON"] = "0"
    o = oes_mod.OESAsync(chamber=2, usb_index=1)
    t = asyncio.create_task(pump(o, "oneshot"))
    assert await o.initialize_device(timeout_s=20.0)
    await o.run_measurement(2.0, 60)
    await asyncio.sleep(0.2)
    t.cancel()
    os.environ.pop("OES_DAEMON", None)


def main() -> int:
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    tmp = Path(tempfile.mkdtemp(prefix="v1g_oes_"))
    svc = tmp / "apps" / "oes_service"
    svc.mkdir(parents=True)
    exe = svc / "oes_worker.exe"
    exe.write_text(FAKE_V1_WORKER.format(python=sys.executable), encoding="utf-8")
    exe.chmod(exe.stat().st_mode | stat.S_IEXEC)
    cap = tmp / "capture.jsonl"
    os.environ["V1G_CAPTURE"] = str(cap)
    os.environ["OES_LOCAL_BASE"] = str(tmp / "oes")
    sys.path.insert(0, str(ROOT))
    argv0 = sys.argv[0]
    sys.argv[0] = str(tmp / "main.py")
    try:
        oes_mod = importlib.import_module("device.oes")
        events: list = []
        asyncio.run(_scenarios(oes_mod, events))
    finally:
        sys.argv[0] = argv0
    recs = [json.loads(x) for x in cap.read_text(encoding="utf-8").splitlines() if x.strip()]
    base = str(tmp)
    golden = {
        "generated_by": "v2/tools/v1golden_oes.py — v1 device/oes.py OESAsync 를 가짜 워커 프로세스에 붙여 실제로 보낸 것",
        "v1_commit": "66efc7b1d079dfb137dd487c4b444160744fc9cb",
        "worker_exe": "<BASE>/apps/oes_service/oes_worker.exe",
        "local_dir": {"ch1": "<BASE>/oes/CH1", "ch2": "<BASE>/oes/CH2"},
        "spawns": [_norm({"argv": r["argv"], "env": r["env"]}, base) for r in recs if r["what"] == "spawn"],
        "stdin": [_norm(r["obj"], base) for r in recs if r["what"] == "stdin"],
        "stop_flags": [_norm(r["files"], base) for r in recs if r["what"] == "stop_flags"],
        "finished": events,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(golden, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    print(f"wrote {OUT} — spawns {len(golden['spawns'])}, stdin {len(golden['stdin'])}, "
          f"stop_flags {len(golden['stop_flags'])}, finished {len(events)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
