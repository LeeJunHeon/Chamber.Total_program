# -*- coding: utf-8 -*-
"""runtime_state 공유 자원 사용자 집합 API (acquire/release_shared) 검증."""
import os
import sys
import contextlib

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                          # noqa: E402
from controller.runtime_state import RuntimeState      # noqa: E402


@pytest.fixture
def rs():
    return RuntimeState()


def test_1_two_owners_then_release_one(rs):
    assert rs.acquire_shared("MFC1", "chamber1") == 1
    assert rs.acquire_shared("MFC1", "pc2") == 2
    assert rs.shared_users("MFC1") == {"chamber1", "pc2"}
    assert rs.release_shared("MFC1", "pc2") == 1
    assert rs.shared_users("MFC1") == {"chamber1"}
    assert rs.shared_snapshot() == {"MFC1": ["chamber1"]}
    assert rs.snapshot()["shared"] == {"MFC1": ["chamber1"]}


def test_2_same_owner_acquire_is_idempotent(rs):
    assert rs.acquire_shared("MFC1", "chamber1") == 1
    assert rs.acquire_shared("MFC1", "chamber1") == 1
    assert rs.shared_users("MFC1") == {"chamber1"}


def test_3_release_unknown_owner_keeps_set(rs):
    rs.acquire_shared("MFC2", "chamber2")
    assert rs.release_shared("MFC2", "pc1") == 1
    assert rs.shared_users("MFC2") == {"chamber2"}
    assert rs.release_shared("CAMERA", "nobody") == 0     # 아무도 없는 자원


def test_4_last_owner_release_returns_zero(rs):
    rs.acquire_shared("MFC1", "chamber1")
    rs.acquire_shared("MFC1", "pc1")
    assert rs.release_shared("MFC1", "chamber1") == 1
    assert rs.release_shared("MFC1", "pc1") == 0
    assert rs.shared_users("MFC1") == set()
    assert rs.shared_snapshot() == {}


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
