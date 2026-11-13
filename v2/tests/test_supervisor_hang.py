"""멈춤 감지 재시작(2026-11-13 D408) — 건강 확인 파일·스레드 덤프·감시 프로그램의 강제 종료·재시작."""
import json
import sys
import threading
import time
from pathlib import Path

import pytest

from v2.ch12.health_beacon import HealthBeacon
from v2.supervisor.supervisor import Supervisor

# 가짜 제어 프로그램 — 감시가 준 환경 변수의 확인 파일에 쓴다. MODE: ok(계속 움직임) · hang(한 번 쓰고 멈춤) · silent(준비 표시 없음) ·
# late(1.2 s 뒤 준비) · cmdhang(tick 은 움직이고 cmd 만 멈춤) · off(멈췄지만 설정이 enabled=false)
CHILD = r"""
import faulthandler, json, os, sys, time
mode = sys.argv[1]
hb, dump = os.environ["CH12_HEARTBEAT_FILE"], os.environ["CH12_HANG_DUMP_FILE"]
n = int(os.environ.get("CH12_RESTART_COUNT", "0"))
if n >= 1 and mode in ("hang", "silent", "cmdhang"):          # 다시 켜진 두 번째부터는 정상으로 끝(시험이 끝나게)
    sys.exit(0)
def w(tick, cmd, ready=True, **kw):
    d = dict(pid=os.getpid(), ready=ready, tick=tick, cmd=cmd, **kw)
    open(hb + ".tmp", "w").write(json.dumps(d)); os.replace(hb + ".tmp", hb)
f = open(dump, "a")
faulthandler.dump_traceback_later(1.0, file=f)
if mode == "ok":
    for i in range(1, 16):
        w(i, i); time.sleep(0.2)
    sys.exit(0)
if mode == "late":
    time.sleep(1.2)
    for i in range(1, 6):
        w(i, i); time.sleep(0.2)
    sys.exit(0)
if mode == "hang":
    w(1, 1); time.sleep(60)
if mode == "silent":
    time.sleep(60)
if mode == "cmdhang":
    for i in range(1, 60):
        w(i, 1); time.sleep(0.2)
if mode == "off":
    w(1, 1, enabled=False); time.sleep(4); sys.exit(0)
"""


def run_sup(tmp_path, mode, **kw):
    script = tmp_path / "child.py"
    script.write_text(CHILD, encoding="utf-8")
    out = []
    sup = Supervisor([sys.executable, str(script), mode], tmp_path / "sup", restart_delay_s=0.2, say=out.append, **kw)
    res = {}
    th = threading.Thread(target=lambda: res.update(code=sup.run()), daemon=True)
    th.start()
    th.join(40)
    if th.is_alive():                                    # 안전망 — 시험이 걸리면 정리
        sup.stop_requested = True
        try:
            sup.child.kill()
        except Exception:
            pass
        th.join(5)
        pytest.fail(f"감시 프로그램이 끝나지 않음: {out[-6:]}")
    return sup, res["code"], out


def test_hung_child_is_dumped_killed_and_restarted(tmp_path):
    sup, code, out = run_sup(tmp_path, "hang", hang_timeout_s=1.5, health_interval_s=0.2, start_grace_s=30.0)
    text = "\n".join(out)
    assert code == 0 and sup.hangs == 1 and sup.starts == 2, text
    assert "[멈춤]" in text and "tick 번호가 1.5 s 동안 움직이지 않음" in text
    assert "Thread" in text or "File" in text                                       # 자식이 남긴 스레드 덤프를 로그에 옮김
    assert len(sup.crashes) == 1                                                    # 재시작 한도 계산에 포함
    assert "멈춤 강제 종료" in text
    log = (tmp_path / "sup" / "supervisor.log").read_text(encoding="utf-8")
    assert "[멈춤]" in log
    assert json.loads((tmp_path / "sup" / "supervisor_state.json").read_text(encoding="utf-8"))["hangs"] == 1


def test_healthy_child_is_not_restarted(tmp_path):
    sup, code, out = run_sup(tmp_path, "ok", hang_timeout_s=1.5, health_interval_s=0.2, start_grace_s=30.0)
    assert code == 0 and sup.hangs == 0 and sup.starts == 1, out


def test_startup_grace_does_not_count_and_missing_ready_after_grace_is_a_hang(tmp_path):
    sup, code, out = run_sup(tmp_path, "late", hang_timeout_s=1.0, health_interval_s=0.2, start_grace_s=5.0)
    assert code == 0 and sup.hangs == 0 and sup.starts == 1, out                    # 준비 전 1.2 s 는 판단 제외
    (tmp_path / "b").mkdir()
    sup, code, out = run_sup(tmp_path / "b", "silent", hang_timeout_s=1.0, health_interval_s=0.2, start_grace_s=1.0)
    assert sup.hangs == 1 and any("준비 표시가 없음" in x for x in out)


def test_command_core_stall_alone_is_a_hang_even_when_ticks_move(tmp_path):
    sup, code, out = run_sup(tmp_path, "cmdhang", hang_timeout_s=1.5, health_interval_s=0.2, start_grace_s=30.0)
    assert sup.hangs == 1 and any("명령 처리(코어 잠금)" in x for x in out), out


def test_hang_check_off_by_option_and_by_setting_in_the_file(tmp_path):
    sup, code, out = run_sup(tmp_path, "off", hang_timeout_s=1.0, health_interval_s=0.2, start_grace_s=30.0)   # 파일 설정 enabled=false
    assert code == 0 and sup.hangs == 0 and sup.starts == 1, out
    (tmp_path / "c").mkdir()
    sup2, code2, out2 = run_sup(tmp_path / "c", "off", hang_timeout_s=1.0, health_interval_s=0.2, hang_enabled=False)  # 명령줄 끔
    assert sup2.hangs == 0 and code2 == 0


def test_check_health_unit_with_fake_clock(tmp_path):
    t = [0.0]

    class C:
        pid = 77
    s = Supervisor(["x"], tmp_path, hang_timeout_s=10.0, start_grace_s=100.0, health_interval_s=1.0, hb_now=lambda: t[0], say=lambda m: None)
    s.child = C()
    s._hb_reset()

    def beat(**kw):
        (tmp_path / "heartbeat.json").write_text(json.dumps(dict(pid=77, **kw)), encoding="utf-8")
    assert s.check_health() is None                                                  # 파일 없음 — 준비 시간 안
    t[0] = 50.0
    beat(ready=False, tick=0, cmd=0)
    assert s.check_health() is None
    t[0] = 101.0
    assert "준비 표시가 없음" in s.check_health()
    beat(ready=True, tick=5, cmd=5)
    assert s.check_health() is None
    t[0] = 105.0
    beat(ready=True, tick=9, cmd=9)
    assert s.check_health() is None
    t[0] = 114.0
    beat(ready=True, tick=9, cmd=12)                                                 # tick 만 9 초째 정지 — 한도(10) 전
    assert s.check_health() is None
    t[0] = 115.5
    assert "tick 번호" in s.check_health()
    beat(ready=True, tick=10, cmd=12)
    assert s.check_health() is None or "명령 처리" in s.check_health()
    other = dict(pid=999, ready=True, tick=1, cmd=1)                                 # 다른 프로세스(이전 자식)의 파일은 무시
    (tmp_path / "heartbeat.json").write_text(json.dumps(other), encoding="utf-8")
    assert s._read_hb() is None


# ───────── 자식 쪽 비콘 ─────────
class FakeLock:
    def __init__(self):
        self.held = False

    def acquire(self, timeout=None):
        return not self.held

    def release(self):
        pass


def test_beacon_writes_progress_numbers_and_settings(tmp_path):
    rows = [0]
    cfg = {"SUPERVISOR_HANG_TIMEOUT_S": 77.0, "SUPERVISOR_HEALTH_INTERVAL_S": 5.0}
    lk = FakeLock()
    b = HealthBeacon(cfg, lambda: rows[0], lk, tmp_path / "hb.json")
    d = b.beat()
    assert d["ready"] is False and d["tick"] == 0 and d["cmd"] == 1 and d["timeout"] == 77.0 and d["interval"] == 5.0 and d["enabled"] is True
    rows[0] = 3
    d = b.beat()
    assert d["ready"] is True and d["tick"] == 3 and d["cmd"] == 2
    lk.held = True                                                                   # 명령 코어 잠금을 못 잡음 — cmd 가 움직이지 않는다
    d = b.beat()
    assert d["cmd"] == 2 and d["cmd_ok"] is False
    cfg["SUPERVISOR_HANG_ENABLED"] = False                                           # LIVE — 바로 반영
    assert b.beat()["enabled"] is False
    assert json.loads((tmp_path / "hb.json").read_text(encoding="utf-8"))["pid"] > 0


def test_beacon_dumps_thread_stacks_when_progress_stops(tmp_path):
    cfg = {"SUPERVISOR_HANG_TIMEOUT_S": 1.0}
    rows = [1]
    dump = tmp_path / "dump.log"
    b = HealthBeacon(cfg, lambda: rows[0], None, tmp_path / "hb.json", dump)
    try:
        for i in range(3):                                                           # 진행 있음 — 덤프 없음
            rows[0] += 1
            b.beat()
            time.sleep(0.4)
        assert not dump.exists() or dump.read_text(encoding="utf-8") == ""
        for _ in range(4):                                                           # 진행 멈춤(tick 고정) — 1 s 뒤 덤프
            b.beat()
            time.sleep(0.5)
        txt = dump.read_text(encoding="utf-8")
        assert "Thread" in txt and "most recent call first" in txt
    finally:
        b.stop()
