"""v1 기록 출력 — 카메라 사진 경로·파일명·저장(골든 = v1 util/camera_recorder._record_loop 를 가짜 카메라로 실제 실행).

JPG 바이트는 drivers/camera.py 가 imencode 한 것을 받는다 — 여기서는 골든 JPG(v1 이 만든 그대로)를 넣어 같은 경로·이름·바이트가
되는지 본다(한글 경로 포함 — v1 이 imwrite 대신 imencode+write_bytes 를 쓴 이유).
"""
from __future__ import annotations

from pathlib import Path

from v2.ch12.outputs.base import QueueWriter
from v2.ch12.outputs.camera_photos import CameraPhotos, mode_for_owners
from v2.tests.outputs_golden import GOLDEN, Clock, assert_same_tree, files_under, load, make_root, norm_logs, v2_cfg
from v2.tools import v1golden_outputs_scen as SC


def test_camera_golden_korean_path(tmp_path):
    scen = load("camera")
    root = make_root(tmp_path)
    cfg = v2_cfg(root, SC.BASE_CFG, scen["cfg"])
    clock = Clock(scen["times"]["open"])
    w = QueueWriter("t")
    logs = []
    cp = CameraPhotos(cfg, writer=w, log=logs.append, now=clock.now)
    jpgs = [p.read_bytes() for _, p in sorted(files_under(GOLDEN / "camera" / "files").items())]
    assert len(jpgs) == scen["frames"]
    assert cp.disk_ok()
    cp.begin(mode_for_owners(["chamber2"]))
    for t, data in zip(scen["times"]["frames"], jpgs):
        clock.set(t)
        cp.save(data)
    cp.end()
    w.stop()
    assert_same_tree(root, GOLDEN / "camera" / "files", lambda rel: rel.endswith(".jpg"))
    assert norm_logs(root, logs) == scen["logs"]
    assert "Camera_Logs/CH2/" in next(iter(files_under(root, lambda r: r.endswith(".jpg"))))


def test_camera_low_disk_zero_frames_and_modes(tmp_path):
    root = make_root(tmp_path)
    cfg = v2_cfg(root, SC.BASE_CFG, {"CAMERA_MIN_FREE_GB": 20.0})
    logs, notes = [], []
    w = QueueWriter("t")
    cp = CameraPhotos(cfg, writer=w, log=logs.append, notify=notes.append, free_fn=lambda p: 3.25)
    assert not cp.disk_ok() and not cp.disk_ok(during=True)
    assert logs == ["디스크 여유 부족(3.2GB < 20.0GB) — 이번 공정 영상 미기록",
                    "디스크 여유 부족(3.2GB < 20.0GB) — 촬영 중단"]
    assert notes == [logs[0]]                          # 알림은 공정마다 1회
    cp.new_process()
    cp.begin("CLEANING")
    cp.end()                                           # 0장 → 빈 폴더 삭제
    w.stop()
    assert any(x.startswith("촬영 0장 → 빈 폴더 삭제: ") for x in logs)
    assert not any(Path(root, "로컬", "VanaM_Logs", "Camera_Logs", "CLEANING").iterdir())
    assert mode_for_owners(["chamber1"]) == "CH1" and mode_for_owners(["pc2"]) == "CLEANING"
    assert mode_for_owners(["chamber1", "pc1"]) == "ALL" and mode_for_owners(["x"]) == "ALL"


def test_camera_root_fallback(tmp_path, monkeypatch):
    from v2.ch12.outputs import paths as P
    monkeypatch.setattr(P, "exe_base_dir", lambda: tmp_path / "exe")
    root = make_root(tmp_path)
    (root / "막힘").write_text("x", encoding="utf-8")
    cfg = v2_cfg(root, SC.BASE_CFG, {"CAMERA_LOG_ROOT": "@/막힘/Camera_Logs"})
    logs = []
    w = QueueWriter("t")
    cp = CameraPhotos(cfg, writer=w, log=logs.append)
    r = cp.root()
    w.stop()
    assert r == tmp_path / "exe" / "Logs_LocalFallback" / "Camera_Logs" and "저장 루트 생성 실패" in logs[0]
