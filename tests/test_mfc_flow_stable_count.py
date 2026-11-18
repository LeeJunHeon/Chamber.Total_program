# -*- coding: utf-8 -*-
"""가스 도달 판정 3회 연속(MFC_FLOW_STABLE_COUNT) — device/mfc.py _stabilization_loop.

예전: R60 이 목표 ±FLOW_ERROR_TOLERANCE 안에 한 번만 들어와도 FLOW_ON 확정.
지금: 압력 도달 판정처럼 연속 N회(기본 3) 들어와야 확정. 범위 밖·읽기 실패면 연속이 0 으로 끊긴다.
확정되지 않은 회차(연속 횟수 미달 포함)가 180 이면 'GAS 안정화 시간 초과'(채널당 약 3분 그대로).
장치·네트워크 없음 — R60 읽기를 각본으로 바꾸거나 가짜 장비를 붙인다.
"""
import os
import re
import sys
import asyncio
import contextlib
from types import SimpleNamespace

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

from device.mfc import AsyncMFC, MFCEvent                            # noqa: E402

_BASE = dict(
    MFC_GAP_MS=1, MFC_DELAY_MS=1, MFC_DELAY_MS_VALVE=1, MFC_STABILIZATION_INTERVAL_MS=0,
    MFC_TIMEOUT=300, MFC_ALLOW_NO_REPLY_DRAIN_MS=1, MFC_POST_OPEN_QUIET_MS=0, MFC_ZEROING_GAP_MS=1,
    MFC_POLLING_INTERVAL_MS=50, MFC_SCALE_FACTORS={1: 1.0, 2: 1.0, 3: 1.0}, FLOW_ERROR_TOLERANCE=0.05,
)


def _cfg(**kw):
    d = dict(_BASE, MFC_FLOW_STABLE_COUNT=3)
    d.update(kw)
    return SimpleNamespace(**d)


def _results(evs, cmd="FLOW_ON", *, channel=None):
    return [e for e in evs if e.kind in ("command_confirmed", "command_failed") and e.cmd == cmd
            and (channel is None or e.channel == channel)]


def _gas_msgs(evs, ch):
    return [e.message for e in evs if e.kind == "status" and e.message and e.message.startswith(f"GAS 확인... ch{ch} ")]


async def _collector(m):
    evs: list[MFCEvent] = []

    async def _collect():
        async for ev in m.events():
            evs.append(ev)
    task = asyncio.get_running_loop().create_task(_collect())
    await asyncio.sleep(0)
    return evs, task


async def _wait(pred, timeout=10.0):
    loop = asyncio.get_running_loop()
    t_end = loop.time() + timeout
    while loop.time() < t_end:
        if pred():
            return True
        await asyncio.sleep(0.005)
    return pred()


async def _stop(*tasks):
    for t in tasks:
        t.cancel()
    for t in tasks:
        with contextlib.suppress(BaseException):
            await t


def _scripted(m, script):
    """R60 읽기를 각본으로 바꾼다. 항목: [ch1, ch2, ch3] 값 목록 또는 None(읽기 실패). 각본이 끝나면 마지막 항목 반복."""
    reads = {"n": 0}

    async def _fake(tag=""):
        i = reads["n"]
        reads["n"] += 1
        await asyncio.sleep(0)
        return script[min(i, len(script) - 1)]
    m._read_r60_values = _fake
    return reads


IN, OUT = 20.0, 15.0          # 목표 20(±5% = 19~21) 안 / 밖


# ───────────────────────── 기본 규칙: 연속 3회, 밖이면 끊김 ─────────────────────────
def test_in_in_out_in_in_in_confirms_on_6th_read():
    async def go():
        m = AsyncMFC(enable_verify=False, enable_stabilization=True, cfg=_cfg())
        evs, col = await _collector(m)
        reads = _scripted(m, [[v, 0, 0] for v in (IN, IN, OUT, IN, IN, IN, OUT)])
        m._start_stab_job(1, 20.0, "chamber1")
        assert await _wait(lambda: _results(evs))
        res = _results(evs)
        assert [e.kind for e in res] == ["command_confirmed"] and res[0].channel == 1 and res[0].owner == "chamber1"
        assert reads["n"] == 6                                      # 6번째 읽기에서 확정(7번째는 읽지 않음)
        msgs = _gas_msgs(evs, 1)
        assert [re.search(r"\[연속 (\d)/3\]$", s).group(1) for s in msgs] == ["1", "2", "0", "1", "2", "3"]
        # 앞부분 문구는 예전 그대로 + 뒤에 [연속 n/3]
        assert msgs[0] == "GAS 확인... ch1 (목표: 20.00sccm, 현재: 20.00sccm) [연속 1/3]"
        assert msgs[2] == "GAS 확인... ch1 (목표: 20.00sccm, 현재: 15.00sccm) [연속 0/3]"
        assert 1 not in m._stab_jobs
        await _stop(col)
    asyncio.run(go())


def test_three_in_a_row_needed_not_one():
    async def go():
        m = AsyncMFC(enable_verify=False, enable_stabilization=True, cfg=_cfg())
        evs, col = await _collector(m)
        reads = _scripted(m, [[IN, 0, 0], [IN, 0, 0], [OUT, 0, 0]])  # 두 번 들어오고 계속 밖
        m._start_stab_job(1, 20.0, "chamber1")
        assert await _wait(lambda: _results(evs), timeout=30)
        # 예전처럼 한 번에 확정하지 않는다 → 확정 없이 180회째 시간 초과
        assert [(e.kind, e.reason) for e in _results(evs)] == [("command_failed", "GAS 안정화 시간 초과")]
        assert reads["n"] == 180
        await _stop(col)
    asyncio.run(go())


def test_read_failure_breaks_streak():
    async def go():
        m = AsyncMFC(enable_verify=False, enable_stabilization=True, cfg=_cfg())
        evs, col = await _collector(m)
        # 안·안·읽기 실패(None)·안·안·채널 값 없음([])·안·안·안 → 9번째에서 확정
        reads = _scripted(m, [[IN, 0, 0], [IN, 0, 0], None, [IN, 0, 0], [IN, 0, 0], [],
                              [IN, 0, 0], [IN, 0, 0], [IN, 0, 0]])
        m._start_stab_job(1, 20.0, "chamber1")
        assert await _wait(lambda: _results(evs))
        assert [e.kind for e in _results(evs)] == ["command_confirmed"]
        assert reads["n"] == 9
        msgs = _gas_msgs(evs, 1)
        assert [re.search(r"\[연속 (\d)/3\]$", s).group(1) for s in msgs] == ["1", "2", "0", "1", "2", "0", "1", "2", "3"]
        assert "현재: -1.00sccm" in msgs[2]                         # 읽기 실패 표기는 예전 그대로
        await _stop(col)
    asyncio.run(go())


def test_always_out_times_out_at_180():
    async def go():
        m = AsyncMFC(enable_verify=False, enable_stabilization=True, cfg=_cfg())
        evs, col = await _collector(m)
        reads = _scripted(m, [[OUT, 0, 0]])
        m._start_stab_job(1, 20.0, "chamber1")
        assert await _wait(lambda: _results(evs), timeout=30)
        res = _results(evs)
        assert [e.kind for e in res] == ["command_failed"] and res[0].reason == "GAS 안정화 시간 초과"
        assert reads["n"] == 180
        await _stop(col)
    asyncio.run(go())


def test_in_but_short_of_streak_counts_as_attempt():
    """범위 안이지만 연속 횟수 미달인 회차도 attempts 에 든다 → 안·안·밖 반복이면 180회째 시간 초과(무한 대기 없음)."""
    async def go():
        m = AsyncMFC(enable_verify=False, enable_stabilization=True, cfg=_cfg())
        evs, col = await _collector(m)
        pat = [[IN, 0, 0], [IN, 0, 0], [OUT, 0, 0]] * 70
        reads = _scripted(m, pat)
        m._start_stab_job(1, 20.0, "chamber1")
        assert await _wait(lambda: _results(evs), timeout=30)
        assert [(e.kind, e.reason) for e in _results(evs)] == [("command_failed", "GAS 안정화 시간 초과")]
        assert reads["n"] == 180
        await _stop(col)
    asyncio.run(go())


def test_two_channels_count_separately():
    async def go():
        m = AsyncMFC(enable_verify=False, enable_stabilization=True, cfg=_cfg())
        evs, col = await _collector(m)
        # ch1(목표 20): 안·안·안 → 3번째 확정 / ch3(목표 30, ±1.5): 안·밖·안·안·안 → 5번째 확정
        reads = _scripted(m, [[IN, 0, 30.0], [IN, 0, 10.0], [IN, 0, 30.0], [OUT, 0, 30.0], [OUT, 0, 30.0], [OUT, 0, 10.0]])
        m._start_stab_job(1, 20.0, "chamber1")
        m._start_stab_job(3, 30.0, "pc2")
        assert await _wait(lambda: _results(evs, channel=1) and _results(evs, channel=3))
        r1, r3 = _results(evs, channel=1), _results(evs, channel=3)
        assert [(e.kind, e.owner) for e in r1] == [("command_confirmed", "chamber1")]
        assert [(e.kind, e.owner) for e in r3] == [("command_confirmed", "pc2")]
        assert len(_gas_msgs(evs, 1)) == 3 and len(_gas_msgs(evs, 3)) == 5
        assert reads["n"] == 5                                      # 한 번 읽기로 두 채널을 함께 판정
        assert [re.search(r"\[연속 (\d)/3\]$", s).group(1) for s in _gas_msgs(evs, 3)] == ["1", "0", "1", "2", "3"]
        await _stop(col)
    asyncio.run(go())


def test_job_created_during_read_is_not_judged_that_round():
    """읽는 동안 새로 생긴 작업(created_mono > t_read)은 그 회차에 판정하지 않는 기존 규칙 유지."""
    async def go():
        m = AsyncMFC(enable_verify=False, enable_stabilization=True, cfg=_cfg())
        evs, col = await _collector(m)
        n = {"i": 0}

        async def _fake(tag=""):
            n["i"] += 1
            if n["i"] == 1:
                m._start_stab_job(3, 30.0, "pc2")                  # 읽기 도중 등록
            await asyncio.sleep(0)
            return [IN, 0, 30.0]
        m._read_r60_values = _fake
        m._start_stab_job(1, 20.0, "chamber1")
        assert await _wait(lambda: _results(evs, channel=1) and _results(evs, channel=3))
        assert len(_gas_msgs(evs, 1)) == 3
        assert len(_gas_msgs(evs, 3)) == 3                          # 1회차는 건너뜀 → 2~4회차로 3회
        assert n["i"] == 4
        await _stop(col)
    asyncio.run(go())


# ───────────────────────── 설정 오류: 고쳐 쓰지 않고 FLOW_ON 실패 ─────────────────────────
def test_bad_stable_count_fails_flow_on():
    for bad in (0, -1, 2.5, 3.0, "3", True, None):
        async def go():
            m = AsyncMFC(enable_verify=False, enable_stabilization=True, cfg=_cfg(MFC_FLOW_STABLE_COUNT=bad))
            evs, col = await _collector(m)
            reads = _scripted(m, [[IN, 0, 0]])
            m._start_stab_job(1, 20.0, "chamber1")
            assert await _wait(lambda: _results(evs))
            res = _results(evs)
            assert [e.kind for e in res] == ["command_failed"], bad
            assert "설정 오류(MFC_FLOW_STABLE_COUNT" in res[0].reason and res[0].owner == "chamber1"
            assert reads["n"] == 0 and m._stab_jobs == {}            # 읽지도 않고 바로 실패
            await _stop(col)
        asyncio.run(go())


def test_stable_count_from_cfg_is_used():
    async def go():
        m = AsyncMFC(enable_verify=False, enable_stabilization=True, cfg=_cfg(MFC_FLOW_STABLE_COUNT=1))
        evs, col = await _collector(m)
        reads = _scripted(m, [[IN, 0, 0]])
        m._start_stab_job(1, 20.0, "chamber1")
        assert await _wait(lambda: _results(evs))
        assert reads["n"] == 1 and _gas_msgs(evs, 1)[-1].endswith("[연속 1/1]")
        await _stop(col)
    asyncio.run(go())


# ───────────────────────── 실제 FLOW_ON 경로(가짜 장비): CH 공정 flow_on · 플라즈마 클리닝 flow_set_on ─────────────────────────
class FakeDevice:
    """L{ch}1/0 → ON/OFF, Q{ch} v → 설정값, R60 → reach[ch] 각본(True=설정값, False=0, 끝나면 마지막 반복)."""

    def __init__(self, m: AsyncMFC):
        self.m = m
        self.sent: list[str] = []
        self.setp = {1: 0.0, 2: 0.0, 3: 0.0}
        self.on = {1: False, 2: False, 3: False}
        self.pattern = {1: [True], 2: [True], 3: [True]}
        self.n_r60 = 0

    def write(self, payload: bytes):
        line = payload.decode("ascii", "ignore").strip()
        self.sent.append(line)
        mm = re.match(r"^L([123])([01])$", line)
        if mm:
            self.on[int(mm.group(1))] = (mm.group(2) == "1")
        mm = re.match(r"^Q([123]) ([0-9.+-]+)$", line)
        if mm:
            self.setp[int(mm.group(1))] = float(mm.group(2))
        if line == "R60":
            i = self.n_r60
            self.n_r60 += 1
            vals = []
            for c in (1, 2, 3):
                pat = self.pattern[c]
                ok = pat[min(i, len(pat) - 1)]
                vals.append(self.setp[c] if (self.on[c] and ok) else 0.0)
            self.m._on_line_from_tcp("Q0 " + " ".join(f"+{v:.2f}" for v in vals))

    async def drain(self):
        return None

    def is_closing(self):
        return False

    def close(self):
        pass

    async def wait_closed(self):
        return None


async def _live(cfg):
    m = AsyncMFC(enable_verify=False, enable_stabilization=True, cfg=cfg)
    m.resource_key = "MFC1"
    dev = FakeDevice(m)
    m._want_connected = True
    m._connected = True
    m._writer = dev
    worker = asyncio.get_running_loop().create_task(m._cmd_worker_loop())
    evs, col = await _collector(m)
    return m, dev, evs, (worker, col)


def test_plasma_cleaning_flow_set_on_needs_three():
    async def go():
        m, dev, evs, tasks = await _live(_cfg())
        dev.pattern[3] = [True, True, False, True, True, True]
        await m.gas_select(3, owner="pc2")
        await m.flow_set_on(30.0, owner="pc2")
        assert await _wait(lambda: _results(evs))
        assert [(e.kind, e.owner, e.channel) for e in _results(evs)] == [("command_confirmed", "pc2", 3)]
        assert dev.n_r60 == 6
        assert [re.search(r"\[연속 (\d)/3\]$", s).group(1) for s in _gas_msgs(evs, 3)] == ["1", "2", "0", "1", "2", "3"]
        await _stop(*tasks)
    asyncio.run(go())


def test_chamber_flow_on_needs_three_and_reflow_on_restarts():
    """CH 공정 FLOW_ON: 3회 연속 뒤 확정. 같은 채널에 새 목표 FLOW_SET → FLOW_ON(L21 재전송)이면 새 목표로 처음부터 다시 센다."""
    async def go():
        m, dev, evs, tasks = await _live(_cfg())
        assert await m.set_flow(2, 9.0, owner="chamber2")
        await m.flow_on(2, owner="chamber2")
        assert await _wait(lambda: _results(evs, channel=2))
        assert dev.n_r60 == 3
        assert await m.set_flow(2, 9.5, owner="chamber2")
        await m.flow_on(2, owner="chamber2")
        assert await _wait(lambda: len(_results(evs, channel=2)) == 2)
        assert [e.kind for e in _results(evs, channel=2)] == ["command_confirmed"] * 2
        assert dev.n_r60 == 6
        assert [s for s in dev.sent if s.startswith(("L2", "Q2"))] == ["Q2 9.0", "L21", "Q2 9.5", "L21"]
        assert _gas_msgs(evs, 2)[-1] == "GAS 확인... ch2 (목표: 9.50sccm, 현재: 9.50sccm) [연속 3/3]"
        await _stop(*tasks)
    asyncio.run(go())


def test_flow_on_bad_count_fails():
    async def go():
        m, dev, evs, tasks = await _live(_cfg(MFC_FLOW_STABLE_COUNT=0))
        assert await m.set_flow(1, 20.0, owner="chamber1")
        await m.flow_on(1, owner="chamber1")
        assert await _wait(lambda: _results(evs, channel=1))
        res = _results(evs, channel=1)
        assert [e.kind for e in res] == ["command_failed"]
        assert "설정 오류(MFC_FLOW_STABLE_COUNT=0)" in res[0].reason
        assert dev.n_r60 == 0
        await _stop(*tasks)
    asyncio.run(go())


def test_default_stable_count_is_three():
    """cfg 에 키가 없으면 config_common 값(3)."""
    from lib import config_common, config_ch1, config_ch2
    assert config_common.MFC_FLOW_STABLE_COUNT == 3
    assert config_ch1.MFC_FLOW_STABLE_COUNT == 3 and config_ch2.MFC_FLOW_STABLE_COUNT == 3
    m = AsyncMFC(enable_verify=False, enable_stabilization=True, cfg=SimpleNamespace(**_BASE))
    assert m._flow_stable_need() == 3
