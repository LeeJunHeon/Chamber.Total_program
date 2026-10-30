r"""v1 RGA 메인↔워커 골든 — v1 device/rga.py 의 RGAWorkerClient 를 실제로 돌려, 가짜 rga_worker.exe 가 받은 명령줄과
v1 이 낸 이벤트(status·data·failed·finished 문구)를 잡는다.

가짜 워커('T\apps\rga_service\rga_worker.exe' — 실행 가능한 파이썬 스크립트)는 V1G_RGA_MODE 에 따라 v1 응답 JSON 1줄을 낸다:
  ok · fail(ok:false, error 'TimeoutError: fake scan timeout') · fallback(csv_fallback_used) · csvfail(csv_ok false) · nojson(JSON 없이 종료 30)
경로는 <BASE>(임시 폴더)·<ROOT>(저장소) 로 바꿔 v2/tests/golden/dev_rga.json 에 쓴다.
실행: cd <저장소> && QT_QPA_PLATFORM=offscreen python3 -m v2.tools.v1golden_rga
"""
from __future__ import annotations

import asyncio
import importlib
import json
import os
import stat
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "v2" / "tests" / "golden" / "dev_rga.json"
MODES = ("ok", "fail", "fallback", "csvfail", "nojson")

FAKE_V1_WORKER = r'''#!{python}
import json, os, sys
a = sys.argv[1:]
with open(os.environ["V1G_CAPTURE"], "a", encoding="utf-8") as f:
    f.write(json.dumps({{"mode": os.environ["V1G_RGA_MODE"], "argv": a}}, ensure_ascii=False) + "\n")
mode = os.environ["V1G_RGA_MODE"]
ch = int(a[a.index("--ch") + 1])
csv = a[a.index("--csv") + 1] if "--csv" in a else ""
fb = a[a.index("--csv_fallback") + 1] if "--csv_fallback" in a else ""
ok = {{"ok": True, "worker_version": 2, "ch": ch, "ip": "192.168.1.2" + str(ch - 1), "timestamp": "2026-10-30 10:00:00",
      "duration_ms": 5, "csv_ok": True, "csv_path": csv, "csv_error": "", "csv_fallback_used": False,
      "mass_axis": [1.0, 2.0, 3.0], "pressures": [1e-9, 2e-9, 3e-9]}}
if mode == "nojson":
    print("garbage line"); sys.exit(30)
if mode == "fail":
    print(json.dumps({{"ok": False, "worker_version": 2, "stage": "worker", "ch": ch, "duration_ms": 5,
                      "error": "TimeoutError: fake scan timeout", "traceback": "tb"}})); sys.exit(30)
if mode == "fallback":
    ok.update(csv_path=fb, csv_error="primary(" + csv + "): OSError: x", csv_fallback_used=True)
if mode == "csvfail":
    ok.update(csv_ok=False, csv_path="", csv_error="primary(x): OSError: x | fallback(y): OSError: y")
print(json.dumps(ok, ensure_ascii=True))
'''


def _norm(v, base: str, root: str):
    if isinstance(v, str):
        return v.replace(base, "<BASE>").replace(root, "<ROOT>")
    if isinstance(v, list):
        return [_norm(x, base, root) for x in v]
    if isinstance(v, dict):
        return {k: _norm(x, base, root) for k, x in v.items()}
    return v


async def _run(rga_mod, mode: str, ch: int) -> list:
    os.environ["V1G_RGA_MODE"] = mode
    c = rga_mod.RGAWorkerClient(ch=ch, default_timeout_s=60.0)
    await c.scan_histogram_to_csv(timeout_s=60.0)
    out = []
    while not c._q.empty():
        ev = c._q.get_nowait()
        d = {"kind": ev.kind, "message": ev.message}
        if ev.kind == "data":
            d["mass_axis"], d["pressures"] = ev.payload.get("mass_axis"), ev.payload.get("pressures")
        if ev.kind == "failed":
            d["keys"] = sorted(ev.payload)
        out.append(d)
    return out


def main() -> int:
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    tmp = Path(tempfile.mkdtemp(prefix="v1g_rga_"))
    svc = tmp / "apps" / "rga_service"
    svc.mkdir(parents=True)
    exe = svc / "rga_worker.exe"
    exe.write_text(FAKE_V1_WORKER.format(python=sys.executable), encoding="utf-8")
    exe.chmod(exe.stat().st_mode | stat.S_IEXEC)
    cap = tmp / "capture.jsonl"
    os.environ["V1G_CAPTURE"] = str(cap)
    sys.path.insert(0, str(ROOT))
    argv0 = sys.argv[0]
    sys.argv[0] = str(tmp / "main.py")
    try:
        rga_mod = importlib.import_module("device.rga")
        cc = importlib.import_module("lib.config_common")
        fallback_dir = str(cc.LOCAL_FALLBACK_RGA_DIR)
        scen = {}
        for mode in MODES:
            for ch in ((1, 2) if mode == "ok" else (1,)):
                scen[f"{mode}.ch{ch}"] = asyncio.run(_run(rga_mod, mode, ch))
    finally:
        sys.argv[0] = argv0
    recs = [json.loads(x) for x in cap.read_text(encoding="utf-8").splitlines() if x.strip()]
    base, root = str(tmp), str(ROOT)
    golden = {
        "generated_by": "v2/tools/v1golden_rga.py — v1 device/rga.py RGAWorkerClient 를 가짜 rga_worker.exe 에 붙여 실제로 돌린 것",
        "v1_commit": "66efc7b1d079dfb137dd487c4b444160744fc9cb",
        "worker_exe": "<BASE>/apps/rga_service/rga_worker.exe",
        "fallback_dir": _norm(fallback_dir, base, root),
        "settings": {"RGA_WORKER_TIMEOUT_S": cc.RGA_WORKER_TIMEOUT_S, "RGA_MAX_ATTEMPTS": cc.RGA_MAX_ATTEMPTS,
                     "RGA_RETRY_DELAY_S": cc.RGA_RETRY_DELAY_S, "RGA_CSV_PATH": cc.RGA_CSV_PATH},
        "step_timeout_ms": cc.rga_step_timeout_ms(),
        "spawns": [_norm(r, base, root) for r in recs],
        "events": _norm(scen, base, root),
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(golden, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    print(f"wrote {OUT} — spawns {len(recs)}, scenarios {len(scen)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
