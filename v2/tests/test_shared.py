"""공유 장비 소유권(v2/ch12/shared.py) — CH1·CH2 가 동시에 쓰고 놓기. 실제 조립(Ch12Rig — 시뮬레이터) 위에서.

- PLC 링크: 출처 태그 proxy(요청 이름 plc:<출처>:…), 로봇 명령 구간 동안 하트비트 쉼(두 챔버 겹침 = 참여 수).
- 카메라: 참여자 chamber1·chamber2·pc1·pc2 — 둘이면 ALL, 하나 남으면 계속 촬영.
- 펄스 포트: CH1(4007)·CH2(4005) 동시 점유, 같은 주소 다른 종류는 v1 문구로 거부, 놓으면 1초 행 off.
- OES·RGA 단독 소유·MFC 폴링 사용자·TSP 게이지(잡는 동안 CH1 IG worker 공유 — D67).
- 두 스레드가 자기 챔버 자원을 동시에 잡고 놓아도 서로 막지 않는다.
"""
import threading
from pathlib import Path

import pytest

from v2.common.clock import VirtualClock
from v2.ch12.shared import CAMERA_OWNERS, Exclusive, PlcLink
from v2.ch12.sim.rig import Ch12Rig


@pytest.fixture
def rig(tmp_path):
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    r = Ch12Rig(c, tmp_path, cfg={"CAMERA_MIN_FREE_GB": 0.0})            # 디스크 여유 가드와 무관한 시험
    r.rows = []
    r.row_listeners.append(r.rows.append)
    assert r.start()
    yield r
    r.stop()


def test_exclusive_same_owner_reacquire_other_rejected():
    x = Exclusive("ch1.oes")
    assert x.acquire("chamber1") == (True, "chamber1") and x.acquire("chamber1") == (True, "chamber1")
    assert x.acquire("pc1") == (False, "chamber1")
    assert not x.release("pc1") and x.release("chamber1") and x.holder is None
    assert x.acquire("pc1") == (True, "pc1")


def test_plc_proxy_source_tag_in_request_name(rig):
    seen = []
    w = rig.workers["plc"]
    orig = w.submit

    def spy(req):
        seen.append(req.name)
        return orig(req)
    w.submit = spy
    box = []
    rig.shared.plc.proxy("ch1").read_coil("M_V_1_SW", on_done=lambda r: (box.append(r), rig.clock.notify()))
    rig.shared.plc.proxy("ROBOT").read_coil("M_V_2_SW", on_done=lambda r: (box.append(r), rig.clock.notify()))
    rig.clock.wait_until(None, lambda: len(box) == 2)
    w.submit = orig
    assert [n.split(":")[1] for n in seen if not n.startswith("read:")] == ["CH1", "ROBOT"]
    assert all(r.value is True for r in box)                                  # 시뮬레이터: 메인 밸브 열림
    with pytest.raises(ValueError):
        rig.shared.plc.proxy("NOBODY")


def test_robot_sections_pause_heartbeat_until_last_ends(rig):
    link: PlcLink = rig.shared.plc
    assert not rig.plc.hb_paused
    assert link.begin_robot() == 1 and rig.plc.hb_paused                       # CH1 로봇 명령
    assert link.begin_robot() == 2                                            # CH2 로봇 명령 겹침
    assert link.end_robot() == 1 and rig.plc.hb_paused                         # 하나 남음 — 계속 쉼
    assert link.end_robot() == 0 and not rig.plc.hb_paused
    assert link.end_robot() == 0
    with pytest.raises(RuntimeError):
        with link.robot() as p:
            assert rig.plc.hb_paused and p.source == "ROBOT"
            raise RuntimeError("명령 실패")
    assert not rig.plc.hb_paused and link.robot_active == 0                    # 예외로 끝나도 다시 켬


def test_camera_participants_all_mode_and_leave(rig):
    cam = rig.shared.camera
    with pytest.raises(ValueError):
        cam.join("chamber3")
    assert CAMERA_OWNERS == ("chamber1", "chamber2", "pc1", "pc2")
    cam.join("chamber1")
    rig.loop.run(2)
    assert rig.camera.current_mode == "CH1" and rig.rows[-1]["camera.running"] is True
    cam.join("chamber2")
    rig.loop.run(2)
    assert rig.camera.current_mode == "ALL" and cam.users == {"chamber1", "chamber2"}
    assert cam.leave("chamber1") is False                                     # 남은 참여자 — 촬영 계속
    rig.loop.run(2)
    assert rig.rows[-1]["camera.running"] is True and rig.rows[-1]["camera.users"] == "chamber2"
    assert cam.leave("chamber2") is True
    rig.loop.run(2)
    assert rig.rows[-1]["camera.running"] is False
    jpgs = list(Path(rig.cfg["CAMERA_LOG_ROOT"]).rglob("*.jpg"))
    assert jpgs and {p.parts[-3] for p in jpgs} <= {"CH1", "ALL"}


def test_pulse_ports_ch1_ch2_concurrent_and_rejection(rig):
    p = rig.shared.pulse
    assert p.claim("dcpulse", "ch1") == (True, "", None)
    assert p.claim("rfpulse", "ch2") == (True, "", None)                       # 다른 주소(4005) — 동시에
    ok, msg, who = p.claim("rfpulse", "ch1")
    assert not ok and who == {"ch": "ch1", "kind": "dcpulse"}
    assert msg == "RF-Pulse 시작 불가: 동일 주소(192.168.1.50:4007) 장비를 CH1(DC-Pulse) 공정이 사용 중입니다."
    rig.loop.run(3)
    r = rig.rows[-1]
    assert r["ch1.dcpulse.q"] == "ok" and r["ch2.rfpulse.q"] == "ok"
    assert r["ch1.rfpulse.q"] == "off" and r["ch2.dcpulse.q"] == "off"
    snap = rig.shared.snapshot()
    assert snap["pulse 192.168.1.50:4007"] == "ch1.dcpulse" and snap["pulse 192.168.1.50:4005"] == "ch2.rfpulse"
    assert p.release_chamber("ch1") == 1
    rig.loop.run(2)
    assert rig.rows[-1]["ch1.dcpulse.q"] == "off" and rig.rows[-1]["ch2.rfpulse.q"] == "ok"
    assert p.claim("rfpulse", "ch1")[0]                                        # 놓은 뒤에는 다른 종류도 잡음
    assert p.release_chamber("ch1") == 1 and p.release_chamber("ch2") == 1


def test_oes_rga_exclusive_mfc_poll_and_tsp_gauge(rig):
    sh = rig.shared
    assert sh.oes["ch1"].acquire("chamber1")[0] and sh.oes["ch2"].acquire("chamber2")[0]
    assert sh.oes["ch1"].acquire("pc1") == (False, "chamber1")
    assert sh.rga["ch1"].acquire("pc1")[0]
    sh.mfc["ch1"].acquire("chamber1")
    sh.mfc["ch1"].acquire("pc1")
    assert rig.mfc["ch1"].polling and sh.mfc["ch1"].release("chamber1") == 1 and rig.mfc["ch1"].polling
    assert sh.mfc["ch1"].release("pc1") == 0 and not rig.mfc["ch1"].polling     # 마지막 사용자가 끌 때만
    assert rig.rows == [] or rig.rows[-1]["tsp.ig.q"] == "off"
    assert sh.tsp_ig.acquire("tsp") == (True, "tsp") and sh.tsp_ig.acquire("pc1") == (False, "tsp")
    rig.loop.run(3)
    assert rig.rows[-1]["tsp.ig.q"] == "ok" and rig.rows[-1]["tsp.ig.state"] == "off"
    assert sh.release_all("pc1") == 1 and sh.rga["ch1"].holder is None          # pc1 몫(RGA)만
    assert sh.release_all("tsp") == 1
    rig.loop.run(2)
    assert rig.rows[-1]["tsp.ig.q"] == "off"
    snap = sh.snapshot()
    assert snap["ch1.oes"] == "chamber1" and snap["ch2.oes"] == "chamber2" and snap["tsp.ig"] is None
    assert sh.tsp_ig.acquire("tsp")[0] and sh.tsp_ig.release()                  # 다시 잡기·놓기(D67 — CH1 IG worker 공유, 새 worker 없음)


def test_two_chambers_claim_release_concurrently_without_blocking(rig):
    sh = rig.shared
    errors, counts = [], {"ch1": 0, "ch2": 0}

    def run(ch, owner):
        try:
            for _ in range(200):
                assert sh.oes[ch].acquire(owner)[0] and sh.rga[ch].acquire(owner)[0]
                sh.mfc[ch].acquire(owner)
                assert sh.plc.begin_robot() >= 1
                sh.plc.proxy(ch.upper())
                assert sh.plc.end_robot() >= 0
                sh.mfc[ch].release(owner)
                assert sh.oes[ch].release(owner) and sh.rga[ch].release(owner)
                counts[ch] += 1
        except BaseException as e:
            errors.append(e)
    ts = [threading.Thread(target=run, args=(ch, f"chamber{ch[-1]}")) for ch in ("ch1", "ch2")]
    for t in ts:
        t.start()
    for t in ts:
        t.join(10.0)
    assert not errors and counts == {"ch1": 200, "ch2": 200}
    assert sh.plc.robot_active == 0 and not rig.plc.hb_paused
    assert all(x.holder is None for x in list(sh.oes.values()) + list(sh.rga.values()))
