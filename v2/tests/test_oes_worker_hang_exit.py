"""OES 워커 — v1 main 8603d5b 반영: 첫 프레임·reset 타임아웃이면 DLL cleanup 을 기다리지 않고 finished(ok=False) 출력 후 os._exit(125),
진단 줄(run 로그) 추가. 실제 워커 프로세스(--fake)에 DLL 호출 하나를 멈추게 해(OES_FAKE_HANG) 시험한다."""
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def _env(tmp_path, **extra):
    from v2.ch12 import sim_guard as G
    e = dict(os.environ, CH12_SIM_BASE=str(tmp_path), PYTHONPATH=str(ROOT), OES_FIRST_FRAME_CALL_TIMEOUT_S="1.0", OES_READ_TIMEOUT_S="1.0")
    e.update(extra)
    return e


def test_first_frame_timeout_helper_raises_timeout_error_for_callers_to_hard_exit():
    """_acquire_first_frame 이 TimeoutError 를 그대로 올리고, 호출부가 잡는 구조(cmd_measure·_daemon_measure_once)임을 소스로 확인."""
    from v2.ch12.workers import oes_daemon, oes_guard, oes_measure
    import inspect
    assert "raise TimeoutError" in inspect.getsource(oes_guard._acquire_first_frame)
    for fn in (oes_measure.cmd_measure, oes_daemon._daemon_measure_once):
        src = inspect.getsource(fn)
        assert "except TimeoutError" in src and "first_frame_timeout" in src and "os._exit(125)" in src
        assert "_runlog" in src and "slow read" in src
    assert "_exit_if_reset_timeout" in inspect.getsource(oes_daemon.cmd_daemon)
    assert "_reset_timeout = True" in inspect.getsource(oes_guard._daemon_reset_device)


SCRIPT = r"""
import asyncio, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from v2.ch12.workers import oes_common as C, oes_fake, oes_measure, oes_guard, oes_daemon
home, mode = Path(sys.argv[2]), sys.argv[3]
C.set_home(str(home)); oes_fake.install()
async def boom(oes, retries=20, delay_s=0.2, guard=None):
    raise TimeoutError("first frame timeout after 1.0s")
oes_measure._acquire_first_frame = boom
oes_daemon._acquire_first_frame = boom
out = home / "out"; out.mkdir(parents=True, exist_ok=True)
rc = asyncio.run(oes_measure.cmd_measure(ch=1, usb=0, duration_s=5.0, integration_ms=70, sample_interval_s=1.0, avg_count=3,
                                         out_dir=out, out_csv=out / "m.csv", dll_path=None))
print("RETURNED", rc, flush=True)
"""


def test_measure_first_frame_timeout_prints_finished_and_exits_125_without_waiting_for_cleanup(tmp_path):
    p = tmp_path / "run.py"
    p.write_text(SCRIPT, encoding="utf-8")
    t0 = time.time()
    r = subprocess.run([sys.executable, str(p), str(ROOT), str(tmp_path / "home"), "measure"], capture_output=True, text=True,
                       timeout=60, env=_env(tmp_path), cwd=str(ROOT))
    assert r.returncode == 125, (r.returncode, r.stdout[-600:], r.stderr[-600:])
    assert "RETURNED" not in r.stdout                                                # 정상 반환 없이 하드 종료
    fin = [json.loads(x) for x in r.stdout.splitlines() if x.startswith("{") and '"finished"' in x]
    assert fin and fin[-1]["ok"] is False and fin[-1]["stop_reason"] == "first_frame_timeout" and "first frame timeout" in fin[-1]["error"]
    assert not (tmp_path / "home" / "out" / "m.csv").exists()                         # 0 바이트 CSV 는 지운다
    assert time.time() - t0 < 30
