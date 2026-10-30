"""전수 목록 thick 영역(v2/ch12/spec/inv_thick.py) 중 다른 시험이 직접 확인하지 않던 동작 — 실제 실행 시험.

두께 모니터 드라이버·상시 읽기 문구(연결·영점·재료값·리스너·처리 예외)는 v2/tests/test_inv_dev.py 의 test_stm_* 에 있다.
"""
from types import SimpleNamespace as NS

from v2.common.clock import VirtualClock
from v2.ch12.process_events import ProcessEventsMixin
from v2.ch12.sim.rig import Ch12Rig


def test_thickness_service_logs_stay_out_of_process_log():
    """THK-024 — 두께 서비스(드라이버) 로그는 시스템 일자 로그(이벤트)로만 — 공정 엔진의 장비 이벤트 펌프는 두께 모니터 이벤트를
    공정 로그(.txt)에 쓰지 않는다(공정 로그에는 런타임 판정 로그만 — ThicknessEngine 이 직접 씀)."""
    class H(ProcessEventsMixin):
        pass
    h = H()
    logs = []
    h.n, h.sources = 2, {"ch2.mfc", "ch2.ig", "ch2.stm100"}
    h.log = lambda s, m: logs.append((s, m))
    for kind in ("zero", "material_applied", "unresponsive", "link", "config_warning"):
        h.on_event(NS(source="ch2.stm100", kind=kind, event=NS(kind=kind, detail="x"), detail="x"))
    assert logs == []


def test_thickness_config_warning_prefix_and_stop_with_system(tmp_path):
    """THK-034·027 — 연결 설정 경고는 '⚠ 두께 모니터 설정: …'(main.py:1307-1309), 프로그램 종료 때 상시 읽기를 멈추고
    두께 CSV 를 닫는다(system.stop → stm.stop·outputs.stop)."""
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    r = Ch12Rig(c, tmp_path, cfg={"CH2_THICKNESS_STALE_S": 0.5})
    warns = [e.detail for e in r.events if e.source == "ch2.stm100" and e.kind == "config_warning"]
    assert warns and all(w.startswith("⚠ 두께 모니터 설정: THICKNESS_STALE_S=0.5") for w in warns)
    assert r.start()
    r.loop.run(2)
    stm = r.stm["ch2"]
    assert stm._running and stm.polls > 0
    r.stop()
    n = stm.polls
    c.wait_until(c.monotonic() + 3.0)
    assert not stm._running and stm.polls == n
