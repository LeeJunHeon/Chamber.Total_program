"""카메라 드라이버 시험(가상 시계 + 가짜 캡처 장치 — cv2 없이). v1 util/camera_recorder.py 의 장치 부분과 같은 동작인지.

v1 근거: 저장 경로·파일명(:871-877·:910-917), 90° 회전(:909), imencode + write_bytes(:913-919), 디스크 여유(:846-857·:906-913),
읽기 실패(:895-902), 0장 빈 폴더 삭제(:937-943), 공유 소유권(:252-255·:708-718·:771-832), 알림 1회(:735-746).
"""
import re

import pytest

from v2.common.clock import VirtualClock
from v2.common.paramdef import check_value, defaults
from v2.common.snapshot_bus import SnapshotBus, TickLoop
from v2.ch12.drivers import camera as C
from v2.ch12.params import camera as P

# CAMERA_LOG_ROOT·CAMERA_MIN_FREE_GB 는 outputs 조각 키(카메라 기록 경로) — 값은 v1 settings.json:304-305
CFG = dict(defaults(P.PARAMS), CAMERA_LOG_ROOT=r"C:\VanaM_Logs\Camera_Logs", CAMERA_MIN_FREE_GB=20.0)


class FakeBackend:
    def __init__(self, open_ok=True, fail_reads=0, encode_ok=True, always_fail=False):
        self.open_ok, self.fail_reads, self.encode_ok, self.always_fail = open_ok, fail_reads, encode_ok, always_fail
        self.opened, self.released, self.rotated, self.reads = [], 0, 0, 0

    def open(self, index):
        self.opened.append(index)
        return {"idx": index} if self.open_ok else None

    def read(self, cap):
        self.reads += 1
        if self.always_fail or self.reads <= self.fail_reads:
            return False, None
        return True, f"frame{self.reads}"

    def rotate(self, frame):
        self.rotated += 1
        return "rot:" + frame

    def encode_jpg(self, frame):
        return (b"\xff\xd8" + frame.encode() + b"\xff\xd9") if self.encode_ok else None

    def release(self, cap):
        self.released += 1


class Rig:
    def __init__(self, tmp_path, backend=None, free=lambda p: 500.0, cfg=None, notify=None):
        self.c = VirtualClock(start_wall=1_760_000_000.0)
        self.c.register()
        self.root = tmp_path / "카메라_로그"                               # 한글 경로(v1: cv2.imwrite 조용한 실패 회피)
        self.cfg = dict(CFG, CAMERA_LOG_ROOT=str(self.root))
        self.cfg.update(cfg or {})
        self.be = backend or FakeBackend()
        self.events, self.notes, self.logs = [], [], []
        self.d = C.CameraDriver(self.c, self.cfg, on_event=self.events.append, backend=self.be,
                                notify_cb=(notify or self.notes.append), free_gb_fn=free)
        self.d.set_log_callback(self.logs.append)

    def wait(self, dt):
        self.c.wait_until(self.c.monotonic() + dt)

    def kinds(self):
        return [e.kind for e in self.events]

    def files(self):
        return sorted(p for p in self.root.rglob("*.jpg"))

    def finish(self):
        self.d.stop()
        self.wait(0.1)
        self.d.join()


def test_params_defaults():
    for p in P.PARAMS:
        assert check_value(p, CFG[p.key]) is None and p.group == "camera" and p.v1_source
    assert CFG["CAMERA_INDEX"] == 1 and CFG["CAMERA_INTERVAL_S"] == 1.0


def test_output_keys_from_outputs_fragment():
    from v2.ch12.params import outputs as PO
    d = defaults(PO.PARAMS)
    assert d["CAMERA_MIN_FREE_GB"] == CFG["CAMERA_MIN_FREE_GB"] and d["CAMERA_LOG_ROOT"] == CFG["CAMERA_LOG_ROOT"]


def test_single_owner_records_rotated_jpgs_to_mode_folder(tmp_path):
    r = Rig(tmp_path)
    assert r.d.start(owner="chamber1")
    r.wait(3.5)
    assert r.d.is_running and r.d.current_mode == "CH1" and r.be.opened == [1]
    r.finish()
    files = r.files()
    assert len(files) == 4                                              # t=0,1,2,3 (1 s 간격)
    sess = files[0].parent
    assert sess.parent == r.root / "CH1" and re.fullmatch(r"\d{8}_\d{6}", sess.name)
    assert all(re.fullmatch(r"\d{6}_\d{4}\.jpg", f.name) for f in files)
    assert [f.name[-8:] for f in files] == ["0001.jpg", "0002.jpg", "0003.jpg", "0004.jpg"]
    assert files[0].read_bytes() == b"\xff\xd8rot:frame1\xff\xd9"      # 회전 뒤 인코딩한 바이트 그대로
    assert r.be.released == 1 and not r.d.is_running
    assert "started" in r.kinds() and "finished" in r.kinds()
    assert any("완료 — 촬영 4장" in m for m in r.logs)


def test_shared_ownership_promotes_to_all_never_demotes(tmp_path):
    r = Rig(tmp_path)
    r.d.start(owner="chamber1")
    r.wait(1.5)
    r.d.start(owner="chamber1")                                         # 같은 owner·같은 모드 — 재시작 없음
    assert len(r.be.opened) == 1
    r.d.start(owner="chamber2")                                         # 둘째 참여자 → ALL 승격(1회 재시작)
    r.wait(1.5)
    assert r.d.current_mode == "ALL" and len(r.be.opened) == 2
    assert any(m.startswith("카메라 모드 승격 CH1→ALL") for m in r.logs)
    assert r.d.stop(owner="chamber1") is False                          # 남은 참여자 → 계속(강등 없음)
    r.wait(1.0)
    assert r.d.is_running and r.d.current_mode == "ALL" and len(r.be.opened) == 2
    r.d.start(owner="chamber1")                                         # ALL 진행 중 — 다시 와도 ALL 유지, 재시작 없음
    assert len(r.be.opened) == 2
    assert r.d.stop(owner="chamber2") is False and r.d.stop(owner="chamber1") is True
    r.wait(0.1)
    r.d.join()
    assert not r.d.is_running
    folders = sorted(p.name for p in r.root.iterdir())
    assert folders == ["ALL", "CH1"]


@pytest.mark.parametrize("owner,mode", [("chamber2", "CH2"), ("pc1", "CLEANING"), ("pc2", "CLEANING"), ("robot", "ALL")])
def test_owner_modes(tmp_path, owner, mode):
    r = Rig(tmp_path)
    r.d.start(owner=owner)
    r.wait(0.5)
    assert r.d.current_mode == mode
    r.finish()
    assert (r.root / mode).is_dir()


def test_no_owner_start_always_restarts_like_v1(tmp_path):
    r = Rig(tmp_path)
    r.d.start(mode="CH2")
    r.wait(0.5)
    r.d.start(mode="CH2")
    r.wait(0.5)
    assert len(r.be.opened) == 2 and r.d.current_mode == "CH2"
    r.finish()


def test_disk_low_before_open_and_notify_once_per_process(tmp_path):
    r = Rig(tmp_path, free=lambda p: 5.0)
    r.d.start(owner="chamber1")
    r.wait(0.5)
    assert r.be.opened == [] and not r.d.is_running                    # 카메라를 열지도 않는다
    assert r.notes == ["디스크 여유 부족(5.0GB < 20.0GB) — 이번 공정 영상 미기록"]
    r.d.stop(owner="chamber1")
    r.d.start(owner="chamber1")                                         # 새 공정 → 알림 다시 1회
    r.wait(0.5)
    assert len(r.notes) == 2 and "disk_low" in r.kinds()
    assert not list(r.root.rglob("*")) or all(p.is_dir() for p in r.root.rglob("*"))


def test_disk_recheck_every_300_frames_stops(tmp_path):
    calls = []

    def free(p):
        calls.append(1)
        return 500.0 if len(calls) == 1 else 1.0
    r = Rig(tmp_path, free=free, cfg={"CAMERA_INTERVAL_S": 0.2})
    r.d.start(owner="chamber2")
    r.wait(0.2 * 305)
    assert not r.d.is_running and len(calls) == 2
    assert r.notes == ["디스크 여유 부족(1.0GB < 20.0GB) — 촬영 중단"]
    assert len(r.files()) == 299                                        # 300번째는 검사 뒤 저장하지 않음(v1 순서)


def test_open_failure_no_folder(tmp_path):
    r = Rig(tmp_path, backend=FakeBackend(open_ok=False))
    r.d.start(owner="chamber1")
    r.wait(0.5)
    assert "open_failed" in r.kinds() and any("카메라 열기 실패 (index=1)" in m for m in r.logs)
    assert not (r.root / "CH1").exists()


def test_read_failures_end_session_and_remove_empty_folder(tmp_path):
    r = Rig(tmp_path, backend=FakeBackend(always_fail=True))
    r.d.start(owner="chamber1")
    r.wait(5.0)
    assert not r.d.is_running and r.be.reads == 11                      # 연속 10번 넘으면 끝
    assert "read_failed" in r.kinds() and any("빈 폴더 삭제" in m for m in r.logs)
    assert list((r.root / "CH1").iterdir()) == []


def test_transient_read_failure_retries_after_pause(tmp_path):
    r = Rig(tmp_path, backend=FakeBackend(fail_reads=3))
    r.d.start(owner="chamber1")
    r.wait(0.95)
    assert r.be.reads == 4 and len(r.files()) == 1                      # 0.3 s 씩 3번 쉬고 넷째에 성공
    r.finish()


def test_encode_failure_logged(tmp_path):
    r = Rig(tmp_path, backend=FakeBackend(encode_ok=False))
    r.d.start(owner="pc1")
    r.wait(1.5)
    r.finish()
    assert "이미지 인코딩 실패" in r.logs and r.files() == []


def test_root_fallback_when_mkdir_fails(tmp_path, monkeypatch):
    bad = tmp_path / "file"
    bad.write_text("x")
    fb = tmp_path / "fb" / "Camera_Logs"
    monkeypatch.setattr(C, "local_fallback_root", lambda: fb)
    r = Rig(tmp_path, cfg={"CAMERA_LOG_ROOT": str(bad / "sub")})
    r.d.start(owner="chamber1")
    r.wait(0.5)
    r.finish()
    assert list(fb.rglob("*.jpg"))


def test_per_owner_log_callbacks_and_tick_rows(tmp_path):
    r = Rig(tmp_path)
    got1, got2 = [], []
    r.d.set_log_callback(got1.append, owner="chamber1")
    r.d.set_log_callback(got2.append, owner="chamber2")
    r.d.start(owner="chamber1")
    rows = []
    bus = SnapshotBus(r.c, C.blocks(), [r.d.source()], 900)
    TickLoop(r.c, bus, [rows.append]).run(3)
    assert all(x["camera.q"] == "ok" for x in rows)
    assert rows[-1]["camera.running"] is True and rows[-1]["camera.mode"] == "CH1"
    assert rows[-1]["camera.users"] == "chamber1" and rows[-1]["camera.frames"] >= 2
    assert got1 and got2 and got1 == got2                               # 동시 사용 중인 공정 로그 모두에
    r.finish()


def test_real_cv2_backend_encode_and_rotate_if_available(tmp_path):
    cv2 = pytest.importorskip("cv2")
    np = pytest.importorskip("numpy")
    be = C.CameraBackend()
    frame = np.zeros((4, 6, 3), dtype=np.uint8)
    rot = be.rotate(frame)
    assert rot.shape == (6, 4, 3)
    data = be.encode_jpg(rot)
    assert data[:2] == b"\xff\xd8"
    p = tmp_path / "한글" / "a.jpg"
    p.parent.mkdir()
    p.write_bytes(data)
    assert cv2.imdecode(np.frombuffer(p.read_bytes(), np.uint8), cv2.IMREAD_COLOR).shape == (6, 4, 3)
