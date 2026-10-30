"""독립 도구 골든 — v1 apps/rf_service/rf_worker.py·apps/log_sync_service/log_sync.py 를 v2 출력에 실제로 돌린 결과.

실행: cd Chamber.Total_program && python3 -m v2.tools.v1golden_independent  →  v2/tests/golden/independent_tools.json

방법(v2 패키지는 v1 을 import 하지 않는다 — 이 도구가 v1 도구를 별도 프로세스로 실행한다)
1. 임시 뿌리(한글·'&'·공백 포함) 아래 v2 출력을 만든다(independent_scen.make_all — 카메라 사진·LOG_ROOT_DIR 기록·OES CSV).
2. v2 설정 값에서 두 도구용 v1 형식 settings.json 을 v2/tools/export_worker_settings 로 내보낸다(결정 D13).
3. v1 rf_worker 를 그 settings.json(env RF_SETTINGS)으로 한 번 실행 — 모델 models/cnn.onnx 로 v2 사진을 실제로 판독하고
   manifest.json·판독 CSV·NAS 사본을 만든다. v1 log_sync 를 env LOG_SYNC_SETTINGS 로 --once 실행 — LOG_SYNC_PAIRS 의 src(v2
   LOG_ROOT_DIR·OES_LOCAL_DIR)를 dst(임시 NAS)로 복사한다.
4. 적는 것: v2 가 만든 파일 목록, 내보낸 settings.json(경로는 '@'), 두 도구의 SETTINGS_KEYS(v1 소스에서), rf_worker 종료 코드·
   매니페스트(세션별 status·frames)·CSV 머리글·시각 열, NAS 사본, log_sync 종료 코드·복사된 파일·복사 안 된 파일·state.json.
시험(v2/tests/test_inv_app.py)은 v1 을 부르지 않고 이 골든과 지금 v2 출력·내보내기를 비교한다.
"""
from __future__ import annotations

import ast
import csv
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
OUT = REPO / "v2" / "tests" / "golden" / "independent_tools.json"
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from v2.tools import export_worker_settings as EXP  # noqa: E402
from v2.tools import independent_scen as S  # noqa: E402

RF = REPO / "apps" / "rf_service" / "rf_worker.py"
LS = REPO / "apps" / "log_sync_service" / "log_sync.py"


def v1_settings_keys(path: Path) -> list:
    for node in ast.parse(path.read_text(encoding="utf-8")).body:
        if isinstance(node, ast.Assign) and any(getattr(t, "id", "") == "SETTINGS_KEYS" for t in node.targets):
            return list(ast.literal_eval(node.value))
    raise RuntimeError(f"SETTINGS_KEYS 없음: {path}")


def run(script: Path, env_key: str, settings: Path, *args: str) -> subprocess.CompletedProcess:
    env = dict(os.environ, **{env_key: str(settings), "PYTHONIOENCODING": "utf-8"})
    return subprocess.run([sys.executable, str(script), *args], cwd=str(REPO), env=env, capture_output=True, text=True,
                          encoding="utf-8", timeout=600)


def main() -> int:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td) / "기록 뿌리 & 시험"
        made = S.make_all(root)
        cfg = made["cfg"]
        settings = root / "app" / "config" / "settings.json"
        obj = EXP.build(EXP.load_values(None) | {k: cfg[k] for k in EXP.ALL_KEYS if k in cfg})
        EXP.write(obj, settings)
        rf = run(RF, "RF_SETTINGS", settings)
        man_p = Path(cfg["RF_READER_STATE_DIR"]) / "manifest.json"
        man = json.loads(man_p.read_text(encoding="utf-8")) if man_p.exists() else {}
        csvs = {}
        for p in sorted(Path(cfg["RF_READER_CSV_DIR"]).rglob("*.csv")):
            with open(p, encoding="utf-8-sig", newline="") as f:
                rows = list(csv.reader(f))
            csvs[p.relative_to(cfg["RF_READER_CSV_DIR"]).as_posix()] = {"header": rows[0], "ts": [r[0] for r in rows[1:]]}
        nas_rf = S.tree(Path(cfg["RF_READER_NAS_DIR"])) if Path(cfg["RF_READER_NAS_DIR"]).exists() else []
        ls = run(LS, "LOG_SYNC_SETTINGS", settings, "--once")
        pairs = json.loads(cfg["LOG_SYNC_PAIRS"])
        copied = {Path(d).relative_to(root).as_posix(): (S.tree(Path(d)) if Path(d).exists() else []) for _s, d in pairs}
        st_p = Path(cfg["LOG_SYNC_STATE_DIR"]) / "state.json"
        state = json.loads(st_p.read_text(encoding="utf-8")) if st_p.exists() else {}
        g = {
            "meta": {"tool": "v2/tools/v1golden_independent.py", "python": sys.version.split()[0],
                     "v1": ["apps/rf_service/rf_worker.py", "apps/log_sync_service/log_sync.py", "lib/_config_loader.py"],
                     "note": "v1 도구를 별도 프로세스로 실제 실행(settings.json = v2 export_worker_settings 출력)"},
            "v2": {"camera": made["camera"], "log_root": made["log_root"], "oes": made["oes"]},
            "settings_json": S.placeholder(obj, root),
            "v1_settings_keys": {"rf_worker": v1_settings_keys(RF), "log_sync": v1_settings_keys(LS)},
            "rf_worker": {"exit": rf.returncode,
                          "manifest": {k: {"status": v.get("status"), "frames": v.get("frames")} for k, v in sorted(man.items())},
                          "csv": csvs, "nas": nas_rf,
                          "settings_line": any(ln.startswith("[rf-worker] settings=") and "settings.json" in ln
                                               for ln in rf.stdout.splitlines()),
                          "problems": [ln for ln in rf.stdout.splitlines() if ln.strip().startswith("!")]},
            "log_sync": {"exit": ls.returncode, "copied": copied,
                         "state": {k: state.get(k) for k in ("consecutive_fails", "alerted")},
                         "problems": [ln for ln in ls.stdout.splitlines() if ln.strip().startswith("!")]},
        }
    OUT.write_text(json.dumps(g, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    print(f"{OUT}: rf_worker exit={g['rf_worker']['exit']} manifest={g['rf_worker']['manifest']} "
          f"log_sync exit={g['log_sync']['exit']} copied={sum(len(v) for v in copied.values())}")
    if rf.returncode or ls.returncode:
        print(rf.stdout[-3000:], rf.stderr[-3000:], ls.stdout[-3000:], ls.stderr[-3000:], sep="\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
