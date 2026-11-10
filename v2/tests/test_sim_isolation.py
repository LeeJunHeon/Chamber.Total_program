"""sim 격리(v2/ch12/sim_guard.py) — v1 이 켜진 장비 PC 에서 sim 을 함께 띄워도 운영 파일·NAS·포트에 닿지 않는다(2026-11-09 B).

- enforce: 설정값과 무관하게 모든 쓰는 곳이 sim_out 아래, NAS·UNC 없음, 포트·주소.
- check: 밖·UNC·v1 포트·0.0.0.0·설정 폴더 밖이면 문제를 낸다. 프로그램은 시작 거부(종료 코드 2).
- 실제 하위 프로세스로 sim 을 돌리며(+OES·RGA 가짜 워커) 파이썬 감사 훅(sitecustomize — 모든 하위 프로세스에 적용)으로 쓰기 열기·폴더 만들기·
  이름 바꾸기·SQLite 연결·소켓 주소를 전부 기록해, 전부 기준 폴더 아래·127.0.0.1 인지 본다.
- 기존 v1 open_sputter.json·Robot CSV(NAS)를 둔 채로 sim 을 켜도 그 파일·폴더가 바뀌지 않는다.
"""
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from v2.ch12 import app as A
from v2.ch12 import sim_guard as G
from v2.tests.test_app import Proc, ROOT, free_port

HOOK = r'''
import os, sys, json
_p = os.environ.get("AUDIT_LOG")
if _p:
    _fd = os.open(_p, os.O_WRONLY | os.O_CREAT | os.O_APPEND)
    _W = os.O_WRONLY | os.O_RDWR | os.O_APPEND | os.O_CREAT | os.O_TRUNC
    def _hook(ev, a):
        try:
            rec = None
            if ev == "open":
                path, mode, flags = a
                w = bool(flags & _W) if isinstance(flags, int) else False
                if isinstance(mode, str) and any(c in mode for c in "wax+"):
                    w = True
                if w and isinstance(path, (str, bytes)):
                    rec = ["write", os.fsdecode(path)]
            elif ev in ("os.mkdir", "os.rmdir", "os.remove", "os.truncate"):
                rec = [ev, os.fsdecode(a[0])]
            elif ev in ("os.rename", "shutil.copyfile", "shutil.copy", "shutil.move"):
                rec = [ev, os.fsdecode(a[-1])]
            elif ev == "sqlite3.connect":
                rec = ["sqlite", os.fsdecode(a[0])]
            elif ev in ("socket.bind", "socket.connect"):
                rec = [ev, repr(a[1])]
            if rec:
                os.write(_fd, (json.dumps(rec, ensure_ascii=False) + "\n").encode())
        except Exception:
            pass
    sys.addaudithook(_hook)
'''
IGNORE = ("/dev/", "/proc/", "/sys/")


def _env(tmp_path: Path, base: Path, extra_hook: bool = True) -> dict:
    env = dict(os.environ, CH12_SIM_BASE=str(base), PYTHONDONTWRITEBYTECODE="1")
    if extra_hook:
        hook = tmp_path / "hook"
        hook.mkdir(exist_ok=True)
        (hook / "sitecustomize.py").write_text(HOOK, encoding="utf-8")
        env["AUDIT_LOG"] = str(tmp_path / "audit.log")
        env["PYTHONPATH"] = os.pathsep.join([str(hook), str(ROOT)])
    return env


def _audit(tmp_path: Path) -> list:
    p = tmp_path / "audit.log"
    return [json.loads(x) for x in p.read_text(encoding="utf-8").splitlines()] if p.exists() else []


def _outside(events: list, base: Path, audit_log: Path) -> list:
    bad = []
    b = os.path.realpath(str(base))
    for kind, val in events:
        if kind in ("socket.bind", "socket.connect"):
            if not val.startswith("('127.0.0.1'") and not val.startswith("('::1'") and not val.startswith("'/") and "127.0.0.1" not in val:
                bad.append((kind, val))
            continue
        rp = os.path.realpath(val)
        if val.startswith(IGNORE) or rp == os.path.realpath(str(audit_log)):
            continue
        if rp != b and not rp.startswith(b + os.sep):
            bad.append((kind, val))
    return bad


# ───────────── 단위 ─────────────
def test_enforce_forces_every_path_key_under_sim_out_whatever_the_setting(tmp_path):
    out = G.sim_out()
    assert Path(out) == tmp_path / "sim_out"
    cfg = {k: r"\\VanaM_NAS\VanaM_Sputter\x" for k in G.FORCED}
    cfg.update(HOST_SERVER_HOST="0.0.0.0", HOST_SERVER_PORT=50070, LOG_SYNC_PAIRS='[["C:\\\\a", "\\\\\\\\NAS\\\\b"]]')
    G.enforce(cfg)
    for k, sub in G.FORCED.items():
        v = cfg[k]
        if sub is None or k == "LOG_SYNC_PAIRS":
            assert v in ("", "[]")
        else:
            assert Path(v).is_relative_to(out) and not v.startswith("\\\\"), (k, v)
    assert (cfg["HOST_SERVER_HOST"], cfg["HOST_SERVER_PORT"]) == ("127.0.0.1", 50170)
    assert cfg["OES_WORKER_FAKE"] is True and cfg["RGA_WORKER_FAKE"] is True
    assert G.check(cfg, tmp_path / "config") == []


def test_forced_keys_cover_every_path_param():
    """설정 표의 경로 키(드라이브·UNC 기본값이거나 *_DIR·*_PATH 이름)는 FORCED 에 있거나 읽기 전용 표에 있다 — 새 경로 키를 잊지 않게."""
    from v2.ch12.params import table
    import re
    missing = []
    for d in table().params:
        v = d.default
        looks = isinstance(v, str) and (re.match(r"^[A-Za-z]:[\\/]", v) or v.startswith("\\\\"))
        named = bool(re.search(r"(_DIR|_PATH|_ROOT|_FILE)$", d.key))
        if (looks or named) and d.key not in G.FORCED and d.key not in G.READ_ONLY_KEYS:
            missing.append(d.key)
    assert not missing, f"sim_guard.FORCED 에 없는 경로 키: {missing}"


def test_check_finds_problems(tmp_path):
    cfg = {"RUN_MODE": "sim"}
    G.enforce(cfg)
    assert G.check(cfg, tmp_path / "config") == []
    bad = dict(cfg, LOG_ROOT_DIR=r"C:\VanaM_Logs\CH1&2", HOST_LOG_NAS_DIR=r"\\VanaM_NAS\VanaM_toShare\JH_Lee\Logs\Robot",
               HOST_SERVER_HOST="0.0.0.0", HOST_SERVER_PORT=50070)
    probs = "\n".join(G.check(bad, tmp_path.parent / "elsewhere"))
    assert "LOG_ROOT_DIR" in probs and "HOST_LOG_NAS_DIR" in probs and "NAS·UNC" in probs
    assert "0.0.0.0" in probs and "50070" in probs and "--config-dir" in probs
    assert G.mutex_name("USB0") == "Local\\VanaM_OES_USB0"
    os.environ["OES_MUTEX_PREFIX"] = G.OES_MUTEX_PREFIX
    try:
        assert G.mutex_name("USB0") == "Local\\CH12SIM_VanaM_OES_USB0" and G.mutex_name("REINIT") == "Local\\CH12SIM_VanaM_OES_REINIT"
    finally:
        os.environ.pop("OES_MUTEX_PREFIX")


def test_apply_env_overrides_v1_env_vars(tmp_path, monkeypatch):
    monkeypatch.setenv("OES_LOCAL_BASE", r"C:\VanaM_Logs\OES")
    monkeypatch.setenv("OES_STOP_DIR", r"C:\VanaM_Logs\OES\stop")
    monkeypatch.setenv("OES_MUTEX_PREFIX", "")
    env = G.apply_env()
    assert Path(os.environ["OES_LOCAL_BASE"]).is_relative_to(tmp_path / "sim_out")
    assert Path(os.environ["OES_STOP_DIR"]).is_relative_to(tmp_path / "sim_out") and env["OES_MUTEX_PREFIX"] == "CH12SIM_"


def test_program_refuses_when_config_dir_outside_sim_base(tmp_path):
    base = tmp_path / "base"
    other = tmp_path / "other" / "config"
    other.mkdir(parents=True)
    (other / "local_override.json").write_text(json.dumps({"values": {"LOCAL_HMI_PORT": free_port()}}), encoding="utf-8")
    pr = Proc(other, env=_env(tmp_path, base, extra_hook=False))
    assert pr.wait(60.0) == A.EXIT_CONFIG
    assert "sim 격리 검사 실패" in pr.text and "--config-dir" in pr.text and not (base / "sim_out").exists()


def test_program_refuses_sim_flag_with_prod_config(tmp_path):
    base = tmp_path / "base"
    cfg = base / "config"
    cfg.mkdir(parents=True)
    (cfg / "local_override.json").write_text(json.dumps({"values": {"RUN_MODE": "prod", "UPLINK_URL": "https://example.invalid/ch12", "UPLINK_TOKEN": "t", "LOCAL_HMI_PORT": free_port()}}), encoding="utf-8")
    p = subprocess.run([sys.executable, "-m", "v2.ch12", "--sim", "--config-dir", str(cfg)], cwd=str(ROOT), text=True,
                       capture_output=True, env=_env(tmp_path, base, extra_hook=False), timeout=60)
    assert p.returncode == A.EXIT_CONFIG and "--sim" in p.stdout and not (base / "sim_out").exists()


def test_robot_server_sim_refuses_v1_port_and_wildcard(tmp_path):
    env = _env(tmp_path, tmp_path / "base", extra_hook=False)
    for extra in (["--port", "50070"], ["--host", "0.0.0.0"]):
        p = subprocess.run([sys.executable, "-m", "v2.robot_server", "--sim"] + extra, cwd=str(ROOT), text=True,
                           capture_output=True, env=env, timeout=60)
        assert p.returncode == 2 and "sim 격리 검사 실패" in p.stdout, (extra, p.stdout, p.stderr)
    assert not (tmp_path / "base" / "sim_out").exists()


# ───────────── 실제 실행 + 파일 쓰기 감시 ─────────────
def _v1_area(tmp_path: Path) -> dict:
    """v1 이 쓰던 곳을 흉내 — sim 설정이 여기를 가리켜도 sim 은 건드리지 않는다."""
    v1 = tmp_path / "v1_machine"
    robot_local, robot_nas = v1 / "VanaM_Logs" / "Robot", v1 / "nas" / "Robot"
    for d in (robot_local, robot_nas, v1 / "VanaM_Logs" / "OES", v1 / "nas" / "RGA"):
        d.mkdir(parents=True)
    (robot_local / "open_sputter.json").write_text('{"run": "v1-live"}', encoding="utf-8")
    (robot_local / "pending_sputter.csv").write_text("v1,pending\n", encoding="utf-8")
    (robot_nas / "Robot_20261109.csv").write_text("v1,nas,csv\n", encoding="utf-8")
    return dict(v1=v1, robot_local=robot_local, robot_nas=robot_nas)


def _snapshot(root: Path) -> dict:
    return {str(p.relative_to(root)): (p.stat().st_mtime_ns, p.stat().st_size if p.is_file() else -1) for p in sorted(root.rglob("*"))}


def test_sim_with_v1_files_present_touches_nothing_outside_and_closes_ports(tmp_path):
    base = tmp_path / "base"
    area = _v1_area(tmp_path)
    cfg = base / "config"
    cfg.mkdir(parents=True)
    vals = {"LOCAL_HMI_PORT": free_port(), "ROBOT_API_PORT": free_port(),
            "LOG_ROOT_DIR": str(area["v1"] / "VanaM_Logs" / "CH1&2"), "LOCAL_DATA_DIR": str(area["v1"] / "data"),
            "HOST_LOG_LOCAL_DIR": str(area["robot_local"]), "HOST_LOG_NAS_DIR": str(area["robot_nas"]),
            "OES_LOCAL_DIR": str(area["v1"] / "VanaM_Logs" / "OES"), "PLC_COIL_LOG_NAS_DIR": str(area["v1"] / "nas" / "plc"),
            "SPUTTER_CALIB_DB_DIR": str(area["v1"] / "nas" / "calib"), "CH1_RGA_CSV_PATH": str(area["v1"] / "nas" / "RGA" / "r.csv"),
            "HOST_SERVER_HOST": "0.0.0.0", "HOST_SERVER_PORT": 50070}
    (cfg / "local_override.json").write_text(json.dumps({"values": vals}), encoding="utf-8")
    before = _snapshot(area["v1"])
    env = _env(tmp_path, base)
    pr = Proc(cfg, env=env)
    try:
        assert pr.running.wait(60.0), pr.text
        time.sleep(3.0)
        pr.p.send_signal(signal.SIGTERM)
        assert pr.wait(30.0) == A.EXIT_OK, pr.text
    finally:
        if pr.p.poll() is None:
            pr.p.kill()
    assert "sim 격리" in pr.text and str(base / "sim_out") in pr.text
    assert _snapshot(area["v1"]) == before                                  # v1 파일·폴더 그대로(mtime·크기·새 파일 없음)
    assert (area["robot_local"] / "open_sputter.json").read_text(encoding="utf-8") == '{"run": "v1-live"}'
    assert (area["robot_nas"] / "Robot_20261109.csv").read_text(encoding="utf-8") == "v1,nas,csv\n"
    ev = _audit(tmp_path)
    assert ev, "감사 기록이 비었다(훅이 안 걸림)"
    assert _outside(ev, base, tmp_path / "audit.log") == []                 # 열린 쓰기 경로 전부 기준 폴더 아래, 소켓은 127.0.0.1 만
    assert not any("50070" in v for k, v in ev if k.startswith("socket"))   # v1 로봇 서버 포트는 열지 않는다
    assert (base / "sim_out" / "data").is_dir() and (base / "sim_out" / "robot_local").exists()


def test_fake_workers_write_only_under_sim_out_and_use_other_mutex(tmp_path, monkeypatch):
    base = tmp_path / "base"
    monkeypatch.setenv("CH12_SIM_BASE", str(base))
    monkeypatch.setenv("OES_LOCAL_BASE", str(tmp_path / "v1_oes"))          # PC 에 v1 용 환경변수가 있어도
    monkeypatch.setenv("OES_STOP_DIR", str(tmp_path / "v1_stop"))
    G.apply_env()
    env = _env(tmp_path, base)
    env.update({k: os.environ[k] for k in ("OES_LOCAL_BASE", "OES_STOP_DIR", "OES_MUTEX_PREFIX")})
    home = base / "sim_out" / "oes_worker"
    p = subprocess.Popen([sys.executable, "-m", "v2.ch12.workers.oes_worker", "--cmd", "daemon", "--ch", "1", "--usb", "0",
                          "--home", str(home), "--fake"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                         env=env, text=True, cwd=str(ROOT))
    time.sleep(3.5)
    p.stdin.write('{"v":2,"id":1,"cmd":"close"}\n')
    p.stdin.flush()
    out = p.communicate(timeout=20)[0]
    assert "daemon READY" in out and f"mutex name=Local\\CH12SIM_VanaM_OES_USB0" in out.replace("\\\\", "\\")
    r = subprocess.Popen([sys.executable, "-m", "v2.ch12.workers.rga_worker", "--fake"], stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env, text=True, cwd=str(ROOT))
    csvp = base / "sim_out" / "rga" / "Ch.1" / "RGA_spectrums.csv"
    r.stdin.write(json.dumps({"v": 2, "id": 1, "cmd": "scan", "ch": 1, "timeout": 5, "csv": str(csvp),
                              "csv_fallback": str(base / "sim_out" / "fallback" / "RGA" / "Ch.1" / "x.csv"),
                              "user": "u", "password": "p"}) + "\n")
    r.stdin.write('{"v":2,"id":2,"cmd":"close"}\n')
    r.stdin.flush()
    rout = r.communicate(timeout=30)[0]
    assert '"kind": "result"' in rout and csvp.is_file(), rout[-400:]
    assert _outside(_audit(tmp_path), base, tmp_path / "audit.log") == []
    assert not (tmp_path / "v1_oes").exists() and not (tmp_path / "v1_stop").exists()


def test_robot_server_sim_listens_only_on_loopback_test_port_and_writes_under_sim_out(tmp_path):
    base = tmp_path / "base"
    port, api = free_port(), free_port()
    p = subprocess.Popen([sys.executable, "-m", "v2.robot_server", "--sim", "--port", str(port), "--api-port", str(api), "--fake-api"],
                         cwd=str(ROOT), text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=_env(tmp_path, base))
    try:
        import socket as so
        t0 = time.time()
        ok = False
        while time.time() - t0 < 30 and not ok:
            try:
                so.create_connection(("127.0.0.1", port), timeout=1).close()
                ok = True
            except OSError:
                time.sleep(0.3)
        assert ok
        with pytest.raises(OSError):                                        # 같은 포트를 외부 주소(0.0.0.0 이 아닌 다른 인터페이스)로 열 수 없음 — 루프백만
            s = so.socket()
            s.settimeout(1)
            s.connect(("192.0.2.1", port))
        p.send_signal(signal.SIGINT)
        p.communicate(timeout=20)
    finally:
        if p.poll() is None:
            p.kill()
    ev = _audit(tmp_path)
    binds = [v for k, v in ev if k == "socket.bind"]
    assert binds and all("127.0.0.1" in v for v in binds) and not any("50070" in v for v in binds), binds
    assert _outside([e for e in ev if e[0] != "socket.connect"], base, tmp_path / "audit.log") == []
    assert (base / "sim_out").is_dir()
