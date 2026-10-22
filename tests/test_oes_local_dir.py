# -*- coding: utf-8 -*-
"""OES 측정 CSV 저장 위치 — 로컬(OES_LOCAL_DIR, 기본 C:\\VanaM_Logs\\OES)에 남기고 NAS 복사는 log_sync 쌍으로.

예전: 메인은 로그인 사용자 Desktop\\OES 에 쓰고, 워커가 측정 뒤 G:(구글 공유 드라이브)로 robocopy 한 뒤 로컬을 지웠다
(공유 드라이브 항목 한도에 걸리면 G: 새 파일이 조용히 사라짐). 메인 finished 문구는 'NAS 미확인/실패' 였다.
장치·네트워크 접근 없음(가짜 데몬 응답).
"""
import os
import sys
import asyncio
import importlib.util
from pathlib import Path

import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

import device.oes as O                                          # noqa: E402
from lib import config_common, config_ch1, config_ch2           # noqa: E402
from lib._config_loader import load_settings                    # noqa: E402

WORKER = Path(_ROOT) / "apps" / "oes_service" / "oes_api.py"


@pytest.fixture
def restore_cfg():
    mods = (config_common, config_ch1, config_ch2)
    snap = [dict(vars(md)) for md in mods]
    yield
    for md, d in zip(mods, snap):
        for k in [k for k in vars(md) if k not in d]:
            delattr(md, k)
        for k, v in d.items():
            setattr(md, k, v)


def test_local_base_env_then_setting_then_default(monkeypatch, restore_cfg):
    monkeypatch.delenv("OES_LOCAL_BASE", raising=False)
    assert config_common.OES_LOCAL_DIR == r"C:\VanaM_Logs\OES"
    assert O._local_base() == Path(r"C:\VanaM_Logs\OES")
    monkeypatch.setattr(config_common, "OES_LOCAL_DIR", r"D:\OES_TEST")
    assert O._local_base() == Path(r"D:\OES_TEST")                 # 실행 시점(OESAsync 생성 때) 값
    monkeypatch.setattr(config_common, "OES_LOCAL_DIR", "  ")
    assert O._local_base() == Path(r"C:\VanaM_Logs\OES")           # 비어 있으면 기본값
    monkeypatch.setenv("OES_LOCAL_BASE", "/tmp/oes_env")
    assert O._local_base() == Path("/tmp/oes_env")                 # 환경변수가 우선


def test_settings_json_oes_dir_and_sync_pair(restore_cfg):
    load_settings()
    assert config_common.OES_LOCAL_DIR == r"C:\VanaM_Logs\OES"
    pairs = [tuple(p) for p in config_common.LOG_SYNC_PAIRS]
    assert (r"C:\VanaM_Logs\CH1&2", r"\\VanaM_NAS\VanaM_Sputter\Sputter\Logs\CH1&2") in pairs   # 기존 쌍 유지
    assert (r"C:\VanaM_Logs\OES", r"\\VanaM_NAS\VanaM_Sputter\OES") in pairs


def test_oesasync_creates_ch_folder_under_setting(tmp_path, monkeypatch, restore_cfg):
    monkeypatch.delenv("OES_LOCAL_BASE", raising=False)
    monkeypatch.setattr(config_common, "OES_LOCAL_DIR", str(tmp_path / "OES"))
    o = O.OESAsync(chamber=2, usb_index=1)
    assert o._local_dir == tmp_path / "OES" / "CH2" and o._local_dir.is_dir()


def _worker():
    spec = importlib.util.spec_from_file_location("oes_api_local_dir_test", str(WORKER))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_worker_no_g_drive_copy_and_keeps_local(tmp_path):
    src = WORKER.read_text(encoding="utf-8")
    assert 'Path(r"G:' not in src and '"robocopy",' not in src          # G: 경로·robocopy 복사 명령 없음
    W = _worker()
    f = tmp_path / "OES_Data_x.csv"
    f.write_text("t,a\n1,2\n", encoding="utf-8")
    assert asyncio.run(W._copy_csv_to_nas(f, 1)) == (False, None, None, False)
    assert f.exists()                                                  # 로컬 그대로
    os.environ.pop("OES_LOCAL_BASE", None)
    assert W._default_out_dir(2) == Path(r"C:\VanaM_Logs\OES") / "CH2"


def _daemon_oes(tmp_path, finished_obj):
    os.environ["OES_LOCAL_BASE"] = str(tmp_path / "OES")
    try:
        o = O.OESAsync(chamber=1, usb_index=0)
    finally:
        os.environ.pop("OES_LOCAL_BASE", None)

    async def _ready(**_k):
        return True

    async def _no_tail(path):
        return None

    async def _send(cmd):
        if cmd.get("cmd") == "measure":
            Path(cmd["out_csv"]).write_text("t,a\n1,2\n", encoding="utf-8")      # 워커가 쓴 CSV 흉내
            obj = dict(finished_obj, out_csv=cmd["out_csv"])
            asyncio.get_running_loop().call_soon(o._daemon_waiter.set_result, obj)
        return True
    o._ensure_daemon_started = _ready
    o._tail_csv = _no_tail
    o._daemon_send_cmd = _send
    return o


def _finished(o):
    evs = []
    while not o._ev_q.empty():
        evs.append(o._ev_q.get_nowait())
    fs = [e for e in evs if e.kind == "finished"]
    assert len(fs) == 1
    return fs[0]


def test_daemon_finish_keeps_local_csv_and_plain_message(tmp_path):
    o = _daemon_oes(tmp_path, {"ok": True, "rows": 2, "elapsed_s": 1.0,
                               "nas_ok": False, "nas_csv": None, "nas_error": None, "local_deleted": False})
    asyncio.run(o._run_measurement_daemon(1.0, 50))
    ev = _finished(o)
    assert ev.success and ev.message == "OES 측정 완료(로컬 저장)"     # 예전: '…NAS 미확인)'
    p = Path(ev.out_csv)
    assert p.parent == tmp_path / "OES" / "CH1" and p.exists()


def test_daemon_finish_old_worker_compat(tmp_path):
    """재빌드 전 워커(G: 복사 성공·로컬 못 지움)면 예전처럼 NAS 경로로 알리고 로컬을 지운다."""
    o = _daemon_oes(tmp_path, {"ok": True, "rows": 2, "elapsed_s": 1.0, "nas_ok": True,
                               "nas_csv": r"G:\x\OES_Data.csv", "nas_error": None, "local_deleted": False})
    asyncio.run(o._run_measurement_daemon(1.0, 50))
    ev = _finished(o)
    assert ev.message == "OES 측정 완료(NAS 저장)" and ev.out_csv == r"G:\x\OES_Data.csv"
    assert not list((tmp_path / "OES" / "CH1").glob("OES_*.csv"))
    o2 = _daemon_oes(tmp_path, {"ok": True, "rows": 2, "elapsed_s": 1.0, "nas_ok": False,
                                "nas_csv": None, "nas_error": "robocopy failed rc=8", "local_deleted": False})
    asyncio.run(o2._run_measurement_daemon(1.0, 50))
    assert _finished(o2).message == "OES 측정 완료(로컬 저장, NAS 실패: robocopy failed rc=8)"
