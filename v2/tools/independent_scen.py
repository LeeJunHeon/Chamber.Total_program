"""독립 도구(apps/rf_service rf_worker·apps/log_sync_service log_sync) 확인용 v2 출력 만들기 — v1 을 import 하지 않는다.

v1golden_independent.py(골든 도구)와 v2/tests/test_inv_app.py(시험)가 같은 함수로 같은 v2 출력을 만든다:
- 카메라 사진: v2 CameraBackend(cv2 회전·JPG) + outputs.camera_photos.CameraPhotos — 1920x1080 프레임을 90° 시계 방향으로 돌려
  CAMERA_LOG_ROOT/CH1/<세션>/<HHMMSS>_<NNNN>.jpg. 세션 하나는 자정을 넘긴다(rf_worker 시각 +1일 규칙).
- LOG_ROOT_DIR 아래 v1 형식 기록: 공정 txt(CH1)·SYSTEM·PLC 코일 CSV·두께 CSV·히터 CSV·selfwatch·런타임 덤프, OES_LOCAL_DIR 아래
  OES CSV 와 정지 플래그(.stop_usb{U}.flag — log_sync 가 복사하지 않아야 하는 제어 파일).
- 독립 도구용 settings.json(v2/tools/export_worker_settings — 결정 D13)의 값 사전(경로는 '@/…' = 뿌리 기준).
cv2·numpy 는 importlib 로 늦게 부른다(v2 표준 라이브러리 규칙 — test_isolation).
"""
from __future__ import annotations

import importlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

T0 = datetime(2026, 10, 30, 9, 15, 20)
CAM_SESSIONS = [("2026-10-30T10:00:00", 3), ("2026-10-30T23:59:59", 3)]     # (세션 시작, 장 수) — 둘째는 자정 넘김
REL = {
    "LOG_ROOT_DIR": "VanaM_Logs/CH1&2",
    "OES_LOCAL_DIR": "VanaM_Logs/OES",
    "CAMERA_LOG_ROOT": "VanaM_Logs/Camera_Logs",
    "RF_READER_STATE_DIR": "VanaM_Logs/Camera_Logs/_state",
    "RF_READER_CSV_DIR": "VanaM_Logs/Camera_Logs/_csv",
    "RF_READER_NAS_DIR": "NAS/Sputter/Logs/CH1&2/Camera_Logs",
    "LOG_SYNC_STATE_DIR": "VanaM_Logs/_sync_state",
    "LOCAL_FALLBACK_ROOT": "exe/Logs_LocalFallback",
    "LOCAL_DATA_DIR": "CH12_v2/data",
}
PAIRS_REL = [("VanaM_Logs/CH1&2", "NAS/Sputter/Logs/CH1&2"), ("VanaM_Logs/OES", "NAS/Sputter/OES")]


def cfg_for(root: Path) -> Dict[str, Any]:
    """v2 cfg(경로는 root 아래) — 기록 출력과 export_worker_settings 가 같이 본다."""
    import json
    c: Dict[str, Any] = {k: str(root / v) for k, v in REL.items()}
    c["LOG_SYNC_PAIRS"] = json.dumps([[str(root / s), str(root / d)] for s, d in PAIRS_REL], ensure_ascii=False)
    c["RF_READER_STALE_MIN"] = 0.0          # 막 만든 세션도 '끝난 세션'으로 보게(rf_worker is_in_progress)
    c["CAMERA_MIN_FREE_GB"] = 0.0
    c["SELFWATCH_DIR"] = ""
    c["CH1_THICKNESS_LOG_DIR"] = str(root / "VanaM_Logs/CH1&2/Thickness")
    c["PLC_COIL_LOG_NAS_DIR"] = str(root / "VanaM_Logs/CH1&2/CH1&2_PLC")     # v1 settings.json 값(LOG_ROOT_DIR 아래)
    c["PLC_COIL_LOG_LOCAL_DIR"] = ""
    return c


class _Clock:
    def __init__(self, t: datetime) -> None:
        self.t = t
        self.mono = 1000.0

    def now(self) -> datetime:
        return self.t

    def monotonic(self) -> float:
        return self.mono

    def advance(self, s: float) -> None:
        self.t += timedelta(seconds=s)
        self.mono += s


def camera_frame():
    """카메라가 주는 1920x1080(가로) BGR 프레임 — 패널 자리에 밝은 사각형 몇 개(판독 결과는 상관없다)."""
    np = importlib.import_module("numpy")
    f = np.zeros((1080, 1920, 3), dtype=np.uint8)
    f[100:300, 200:900] = (40, 40, 220)
    f[500:700, 1000:1700] = (60, 200, 60)
    return f


def make_camera(root: Path, cfg: Dict[str, Any]) -> List[str]:
    from v2.ch12.drivers.camera import CameraBackend
    from v2.ch12.outputs.base import QueueWriter
    from v2.ch12.outputs.camera_photos import CameraPhotos
    be = CameraBackend()
    w = QueueWriter("cam")
    out = []
    for t0, n in CAM_SESSIONS:
        clk = _Clock(datetime.fromisoformat(t0))
        cp = CameraPhotos(cfg, writer=w, now=clk.now)
        cp.begin("CH1")
        for _ in range(n):
            p = cp.save(be.encode_jpg(be.rotate(camera_frame())))
            out.append(p)
            clk.advance(1.0)
        cp.end()
    w.stop()
    cam_root = Path(cfg["CAMERA_LOG_ROOT"])
    return sorted(p.relative_to(cam_root).as_posix() for p in out)


def make_logs(root: Path, cfg: Dict[str, Any]) -> None:
    """LOG_ROOT_DIR·OES_LOCAL_DIR 아래 v2 기록(v1 형식·v1 경로)."""
    from v2.ch12.outputs import runtime_dump
    from v2.ch12.outputs.base import QueueWriter
    from v2.ch12.outputs.heater_csv import HeaterCsv
    from v2.ch12.outputs.paths import Paths
    from v2.ch12.outputs.plc_coil_csv import PlcCoilCsv
    from v2.ch12.outputs.process_txt import ProcessTxtLog
    from v2.ch12.outputs.selfwatch import SelfWatchLog
    from v2.ch12.outputs.system_log import SystemLog
    from v2.ch12.outputs.thickness_csv import ThicknessCsv
    clk = _Clock(T0)
    w = QueueWriter("logs")
    sl = SystemLog(cfg, writer=w, now=clk.now)
    t = ProcessTxtLog(1, cfg, system_log=sl, writer=w, now=clk.now)
    t.open_run({"process_note": "ITO 100nm", "process_time": 1.0}, origin="host",
               origin_meta={"request_id": "R-1", "peer": "192.168.1.30:5000"})
    t.log("Process", "공정 시작")
    clk.advance(1.0)
    t.log("Process", "공정 종료")
    t.close_run()
    sl.append("HOSTLOG", "기록 줄")
    pc = PlcCoilCsv(cfg, writer=w, monotonic=clk.monotonic)
    for _ in range(2):
        pc.on_tick(clk.now(), connected=True, coils={"R_P_1_OFF_TIMER": True}, regs={"DCV_READ_0": 1}, mono=clk.monotonic())
        clk.advance(5.0)
    th = ThicknessCsv(1, cfg, writer=w, monotonic=clk.monotonic)
    th.set_recording(True, "ITO 100nm", {"connected": True, "thickness_nm": 1.0, "rate_nm_s": 0.1, "freq_hz": 5.9e6,
                                         "life_pct": 90.0, "crystal_ok": True, "stale": False}, clk.now())
    th.set_recording(False)
    hc = HeaterCsv(cfg, writer=w, monotonic=clk.monotonic, now=clk.now)
    hc.start()
    hc.write_row({"pv": 25.0, "sv": 100.0, "run": True}, owner="공정")
    hc.stop()
    sw = SelfWatchLog(cfg, writer=w, now=clk.now, monotonic=clk.monotonic, wall_time=lambda: T0.timestamp())
    sw.tick({"working_set_mb": 100.0, "private_mb": 120.0, "threads": 10})
    w.flush()
    for o in (t, pc, th):
        o.stop()
    w.stop()
    runtime_dump.request_dump(cfg, state={"ch1": {"state": "idle"}}, reason="manual_button", wait=True,
                              strftime=lambda fmt: T0.strftime(fmt))
    p = Paths(cfg)
    od = p.oes_dir(1)
    od.mkdir(parents=True, exist_ok=True)
    (od / Paths.oes_filename(T0)).write_text("Time,200.0,200.5\n09:15:20,1.0,2.0\n", encoding="utf-8")
    (od / ".stop_usb0.flag").write_text("stop\n", encoding="utf-8")       # v2 oes_link 정지 플래그(복사 대상 아님)
    (od / (Paths.oes_filename(T0) + ".stop")).write_text("stop\n", encoding="utf-8")


def tree(base: Path) -> List[str]:
    import os
    import re
    pid = f"pid{os.getpid()}"
    return sorted(re.sub(r"_pid\d+\.txt$", "_pid<pid>.txt", p.relative_to(base).as_posix()) if pid in p.name
                  else p.relative_to(base).as_posix() for p in Path(base).rglob("*") if p.is_file())


def make_all(root: Path) -> Dict[str, Any]:
    cfg = cfg_for(root)
    cam = make_camera(root, cfg)
    make_logs(root, cfg)
    return {"cfg": cfg, "camera": cam,
            "log_root": tree(Path(cfg["LOG_ROOT_DIR"])), "oes": tree(Path(cfg["OES_LOCAL_DIR"]))}


def placeholder(obj: Any, root: Path) -> Any:
    """경로 글자의 root 를 '@' 로(골든 비교용 — 윈도우 '\\' 도 '/' 로)."""
    import json
    s = json.dumps(obj, ensure_ascii=False)
    for r in {str(root), str(root).replace("\\", "\\\\")}:
        s = s.replace(r, "@")
    return json.loads(s.replace("\\\\", "/"))
