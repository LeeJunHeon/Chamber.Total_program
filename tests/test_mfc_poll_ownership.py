# -*- coding: utf-8 -*-
"""MFC 폴링 소유권(장치 인스턴스 기준) 검증 — 실제 시리얼/네트워크 없음.

AsyncMFC 는 호출 기록 스텁으로 대체하고, ChamberRuntime._apply_polling_targets 와
PlasmaCleaningRuntime._acquire_mfcs/_release_mfcs_and_finalize 를 __init__ 없이 실행한다.

1) chamber1 만 사용 → 종료 시 set_process_status(False) 정확히 1회 (회귀)
2) chamber1 + pc2 가 MFC1 공유 → pc2 종료 시 MFC1 은 정리되지 않는다
3) 이어서 chamber1 종료 → 그때 비로소 정리된다
4) pc2 가 MFC2(주인 없음) 를 쓰다 종료 → MFC2 는 즉시 정리 (과잉 차단 제거)
5) pc1 + chamber1 이 MFC1 공유 → pc1 종료 시 MFC1 폴링이 꺼지지 않는다 (차단 누락 제거)
6) set_poll_mask(gas=True, pressure=True) 원복은 left==0 일 때만
7) set_mfcs 교체 시 이전 소유권 해제 → 새 owner 로 획득
"""
import os
import sys
import contextlib

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                   # noqa: E402
from controller.runtime_state import runtime_state              # noqa: E402
from device.mfc import mfc_resource_key                         # noqa: E402
from runtime.chamber_runtime import ChamberRuntime              # noqa: E402
from runtime.plasma_cleaning_runtime import PlasmaCleaningRuntime   # noqa: E402


class StubMFC:
    def __init__(self, key):
        self.resource_key = key
        self.calls = []

    def set_process_status(self, on):
        self.calls.append(("set_process_status", bool(on)))

    def on_process_finished(self, ok):
        self.calls.append(("on_process_finished", bool(ok)))

    def set_poll_mask(self, *, gas=True, pressure=True):
        self.calls.append(("set_poll_mask", bool(gas), bool(pressure)))

    def n(self, name, *args):
        return sum(1 for c in self.calls if c[0] == name and (not args or c[1:] == args))

    def finalized(self):
        return self.n("set_process_status", False) + self.n("on_process_finished", False)


class _Dummy:
    is_running = False


def _chamber(ch, mfc):
    c = ChamberRuntime.__new__(ChamberRuntime)
    c.ch = ch
    c.mfc = mfc
    c._auto_connect_enabled = False
    c.process_controller = _Dummy()
    c.dc_pulse = c.rf_pulse = c.dc_power = c.dc_power2 = c.rf_power = None
    c.logs = []
    c.append_log = lambda src, msg: c.logs.append(f"[{src}] {msg}")
    return c


def _pc(ch, gas, pressure):
    p = PlasmaCleaningRuntime.__new__(PlasmaCleaningRuntime)
    p._selected_ch = ch
    p.mfc_gas = gas
    p.mfc_pressure = pressure
    p.logs = []
    p.append_log = lambda src, msg: p.logs.append(f"[{src}] {msg}")
    return p


@pytest.fixture(autouse=True)
def _clean_state():
    def _clr():
        for k in ("MFC1", "MFC2"):
            for o in ("chamber1", "chamber2", "pc1", "pc2"):
                runtime_state.release_shared(k, o)
    _clr()
    yield
    _clr()


def test_0_resource_key_helper():
    assert mfc_resource_key(StubMFC("MFC1")) == "MFC1"

    class _Bare:
        _override_host = "192.168.1.50"
        _override_port = 4003
    assert mfc_resource_key(_Bare()) == "MFC@192.168.1.50:4003"


def test_1_chamber_only_regression():
    m1 = StubMFC("MFC1")
    c = _chamber(1, m1)
    c._apply_polling_targets({"mfc": True})
    assert m1.calls == [("set_poll_mask", True, True), ("set_process_status", True)]
    assert runtime_state.shared_users("MFC1") == {"chamber1"}
    c._apply_polling_targets({"mfc": False})
    assert m1.n("set_process_status", False) == 1
    assert runtime_state.shared_users("MFC1") == set()
    assert not any("폴링 유지" in s for s in c.logs)
    # 획득 없이 False 를 한 번 더 받아도(초기화 경로) 기존처럼 끈다
    c._apply_polling_targets({"mfc": False})
    assert m1.n("set_process_status", False) == 2


def test_2_3_chamber1_pc2_share_mfc1():
    m1, m2 = StubMFC("MFC1"), StubMFC("MFC2")
    c = _chamber(1, m1)
    c._apply_polling_targets({"mfc": True})
    p = _pc(2, m1, m2)                      # PC(CH2): gas=mfc1, pressure=mfc2
    p._acquire_mfcs()
    assert runtime_state.shared_users("MFC1") == {"chamber1", "pc2"}
    assert runtime_state.shared_users("MFC2") == {"pc2"}

    # (2) pc2 종료 → MFC1 은 손대지 않는다, MFC2 는 즉시 정리
    p._release_mfcs_and_finalize()
    assert m1.finalized() == 0
    assert m1.n("set_poll_mask") == 1        # 챔버 기동 시 1회뿐, 해제 경로에서 원복 안 함
    assert any("MFC1 폴링 유지 — 사용 중: chamber1" in s for s in p.logs), p.logs
    assert m2.finalized() == 1 and m2.n("set_poll_mask", True, True) == 1
    assert runtime_state.shared_users("MFC1") == {"chamber1"}

    # (3) 이어서 chamber1 종료 → 그때 비로소 정리
    c._apply_polling_targets({"mfc": False})
    assert m1.n("set_process_status", False) == 1
    assert runtime_state.shared_users("MFC1") == set()


def test_4_pc2_mfc2_cleaned_immediately_even_if_chamber1_running():
    """옛 가드: PC=CH2 & chamber1 실행 중이면 MFC2 까지 방치했다. 이제 장치 기준."""
    m1, m2 = StubMFC("MFC1"), StubMFC("MFC2")
    c = _chamber(1, m1)
    c._apply_polling_targets({"mfc": True})
    p = _pc(2, m1, m2)
    p._acquire_mfcs()
    p._release_mfcs_and_finalize()
    assert m2.finalized() == 1, m2.calls
    assert m1.finalized() == 0
    c._apply_polling_targets({"mfc": False})


def test_5_pc1_chamber1_share_single_mfc1():
    """옛 가드: PC=CH1 이면 항상 False → chamber1 실행 중에도 mfc1 폴링을 껐다."""
    m1 = StubMFC("MFC1")
    c = _chamber(1, m1)
    c._apply_polling_targets({"mfc": True})
    p = _pc(1, m1, m1)                       # PC(CH1): gas=pressure=mfc1
    p._acquire_mfcs()
    assert runtime_state.shared_users("MFC1") == {"chamber1", "pc1"}
    p._release_mfcs_and_finalize()
    assert m1.finalized() == 0
    assert any("MFC1 폴링 유지" in s for s in p.logs)
    # 두 번째 해제 호출(_shutdown_rest_devices 경로)도 안전: 여전히 chamber1 이 있으므로 손대지 않음
    p._release_mfcs_and_finalize()
    assert m1.finalized() == 0
    c._apply_polling_targets({"mfc": False})
    assert m1.finalized() == 1


def test_6_poll_mask_restore_only_when_last_user():
    m1 = StubMFC("MFC1")
    c = _chamber(1, m1)
    c._apply_polling_targets({"mfc": True})
    p = _pc(2, m1, StubMFC("MFC2"))
    p._acquire_mfcs()
    before = m1.n("set_poll_mask")
    p._release_mfcs_and_finalize()
    assert m1.n("set_poll_mask") == before               # 남은 사용자 있음 → 원복 없음
    c._apply_polling_targets({"mfc": False})
    # pc 단독 사용 → 마지막 사용자 → 원복 1회
    m = StubMFC("MFC2")
    p2 = _pc(2, StubMFC("MFC1"), m)
    p2._acquire_mfcs()
    p2._release_mfcs_and_finalize()
    assert m.n("set_poll_mask", True, True) == 1 and m.finalized() == 1


def test_7_set_mfcs_swaps_claims():
    m1, m2 = StubMFC("MFC1"), StubMFC("MFC2")
    p = _pc(1, m1, m1)
    p._acquire_mfcs()
    assert runtime_state.shared_users("MFC1") == {"pc1"}
    p._selected_ch = 2
    p.set_mfcs(mfc_gas=m1, mfc_pressure=m2)   # 교체: 이전 소유권 해제 후 새 owner 로 획득
    assert runtime_state.shared_users("MFC1") == {"pc2"}
    assert runtime_state.shared_users("MFC2") == {"pc2"}
    p._release_mfcs_and_finalize()
    assert runtime_state.shared_snapshot() == {}
    # 미획득 상태에서 set_mfcs 는 아무 소유권도 만들지 않는다
    p.set_mfcs(mfc_gas=m1, mfc_pressure=m1)
    assert runtime_state.shared_snapshot() == {}


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
