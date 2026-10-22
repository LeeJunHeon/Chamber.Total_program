# -*- coding: utf-8 -*-
"""히터 출력 무응답 감시(controller/heater_response.py) [EL-092] — 판정 단위 시험 + 래더 에뮬레이터 시나리오
(HeaterService 가 끄고 카드·종료 사유를 남기는지) + ChamberRuntime 이 공정을 E832 로 한 번만 멈추는지.
장치·네트워크 접근 없음(가짜 시계·에뮬레이터)."""
import os
import sys
import asyncio
import contextlib
from types import SimpleNamespace

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
sys.path.insert(0, _ROOT)
sys.path.insert(0, _HERE)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                    # noqa: E402
from controller.heater_response import HeaterResponseGuard       # noqa: E402
from lib.heater_config import HeaterConfig, load_heater_config   # noqa: E402
import heater_ladder_sim as sim                                  # noqa: E402


# ───────────────────────── 판정 단위 시험(가짜 시계) ─────────────────────────
class _Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def _g(**kw):
    clk = _Clock()
    return HeaterResponseGuard(clock=clk, **kw), clk


def _st(pv, mv, **kw):
    d = {"run": True, "ok": True, "fault": False, "stale": False, "pv": pv, "mv": mv, "mv_limit": 1200}
    d.update(kw)
    return d


def _drive(g, clk, secs, pv_fn, mv_fn, target=200.0, holding=False, **kw):
    """1초마다 tick. 판정되면 (경과초, info) — 없으면 None."""
    t0 = clk.t
    for i in range(int(secs)):
        r = g.tick(_st(pv_fn(i), mv_fn(i), **kw), target, holding=holding)
        clk.t += 1.0
        if r is not None:
            return i, r
    return None


def test_a_dead_cold_start_trips_once():
    g, clk = _g()
    r = _drive(g, clk, 400, lambda i: 25.0, lambda i: 700)
    assert r is not None
    i, info = r
    assert i == 300 and info["kind"] == "noresp" and info["label"] == "출력 무응답"
    assert info["text"] == "MV 700(≥600) 5분 동안 PV 25.0→25.0°C (운전 시작 PV 25.0°C, 목표 200.0°C)"
    assert "VARITAP" in info["hint"] and g.tripped
    assert _drive(g, clk, 600, lambda i: 25.0, lambda i: 700) is None       # 같은 운전에서 다시 내지 않음


def test_a_threshold_follows_actual_limit():
    g, clk = _g()
    assert _drive(g, clk, 600, lambda i: 25.0, lambda i: 550) is None       # 600 미만
    g2, clk2 = _g()
    r = _drive(g2, clk2, 600, lambda i: 25.0, lambda i: 550, mv_limit=560)  # 실제 상한 560 → 548.8 이상이면 '높음'
    assert r is not None and r[0] == 300 and "MV 550(≥549)" in r[1]["text"]


def test_a_working_heater_no_trip():
    g, clk = _g()
    assert _drive(g, clk, 3000, lambda i: 25.0 + 0.1 * i, lambda i: 700) is None


def test_a_slow_but_rising_counts_as_heated():
    g, clk = _g()
    # 5분에 +6°C(1.2°C/min) — 데워짐(최저 PV 보다 5°C 이상) → A 아님
    assert _drive(g, clk, 3000, lambda i: 25.0 + 0.02 * i, lambda i: 900) is None


def test_a_barely_rising_trips():
    g, clk = _g()
    # 5분에 +3°C — 데워진 적 없음·평탄(시작 대비 5°C 미만) → 무응답
    r = _drive(g, clk, 400, lambda i: 25.0 + 0.01 * i, lambda i: 900)
    assert r is not None and r[0] == 300


def test_a_hot_restart_falling_then_recovering_no_trip():
    g, clk = _g()
    pv = lambda i: (300.0 - 0.2 * i) if i < 120 else (276.0 + 0.05 * (i - 120))   # 24°C 식은 뒤 회복
    assert _drive(g, clk, 3000, pv, lambda i: 800, target=320.0) is None


def test_a_no_heating_demand_no_trip():
    g, clk = _g()
    assert _drive(g, clk, 3000, lambda i: 300.0, lambda i: 800, target=302.0) is None   # 목표 근처(5°C 안)


def test_b_loss_trips():
    g, clk = _g()
    # 200°C 유지 중 출력 끊김: 6°C/min 으로 식고 MV 는 상한. 목표보다 10°C 낮아진 뒤(100초) 180초 창에서 18°C 하강
    r = _drive(g, clk, 600, lambda i: 200.0 - 0.1 * i, lambda i: 1200)
    assert r is not None
    i, info = r
    assert i == 280 and info["kind"] == "loss" and info["label"] == "출력 끊김"
    assert info["text"].startswith("출력 상한(MV 1200/1200)인데 3분 동안 PV ")
    assert info["text"].endswith("하강(목표 200.0°C)")
    assert "출력이 모자란 채 부하" in info["hint"]


def test_b_uses_readback_limit():
    g, clk = _g()
    r = _drive(g, clk, 600, lambda i: 200.0 - 0.1 * i, lambda i: 980, mv_limit=980)   # 포화 클램프로 D118 = 980
    assert r is not None and r[1]["kind"] == "loss" and "MV 980/980" in r[1]["text"]


def test_b_slow_fall_or_not_at_limit_no_trip():
    g, clk = _g()
    assert _drive(g, clk, 3000, lambda i: 200.0 - 0.03 * i, lambda i: 1200) is None   # 3분에 5.4°C
    g, clk = _g()
    assert _drive(g, clk, 3000, lambda i: 200.0 - 0.1 * i, lambda i: 1100) is None    # 상한 아님


def test_b_skipped_while_holding():
    g, clk = _g()
    assert _drive(g, clk, 600, lambda i: 200.0 - 0.1 * i, lambda i: 1200, holding=True) is None


def test_run_off_resets_and_rearms():
    g, clk = _g()
    assert _drive(g, clk, 400, lambda i: 25.0, lambda i: 700) is not None
    assert g.tick(_st(25.0, 0, run=False), 200.0) is None and not g.tripped
    r = _drive(g, clk, 400, lambda i: 25.0, lambda i: 700)
    assert r is not None and r[0] == 300


@pytest.mark.parametrize("bad", [{"stale": True}, {"fault": True}, {"ok": False}, {"pv": None}])
def test_bad_sample_restarts_streak(bad):
    g, clk = _g()
    assert _drive(g, clk, 200, lambda i: 25.0, lambda i: 700) is None
    st = _st(25.0, 700)
    st.update(bad)
    assert g.tick(st, 200.0) is None
    clk.t += 1.0
    r = _drive(g, clk, 400, lambda i: 25.0, lambda i: 700)
    assert r is not None and r[0] == 300                                      # 처음부터 다시 300초


def test_gap_restarts_streak():
    g, clk = _g()
    assert _drive(g, clk, 200, lambda i: 25.0, lambda i: 700) is None
    clk.t += 30.0                                                             # 응답 공백
    r = _drive(g, clk, 400, lambda i: 25.0, lambda i: 700)
    assert r is not None and r[0] == 300


def test_disabled_never_trips():
    g, clk = _g(enabled=False)
    assert _drive(g, clk, 1000, lambda i: 25.0, lambda i: 1200) is None
    assert _drive(g, clk, 1000, lambda i: 200.0 - 0.1 * i, lambda i: 1200) is None


# ───────────────────────── 설정 ─────────────────────────
def test_config_defaults_and_clamps():
    cfg, warns = load_heater_config(SimpleNamespace())
    assert warns == [] and cfg == HeaterConfig()
    assert (cfg.noresp_enable, cfg.noresp_mv, cfg.noresp_sec, cfg.noresp_rise_c) == (True, 600, 300.0, 5.0)
    assert (cfg.noresp_loss_sec, cfg.noresp_loss_below_c, cfg.noresp_loss_fall_c) == (180.0, 10.0, 10.0)
    cfg, warns = load_heater_config(SimpleNamespace(
        HEATER_NORESP_ENABLE="false", HEATER_NORESP_MV=300, HEATER_NORESP_SEC=5, HEATER_NORESP_RISE_C=0,
        HEATER_NORESP_LOSS_SEC=9999, HEATER_NORESP_LOSS_BELOW_C=1, HEATER_NORESP_LOSS_FALL_C=0))
    assert cfg.noresp_enable is False
    assert (cfg.noresp_mv, cfg.noresp_sec, cfg.noresp_rise_c) == (450, 60.0, 1.0)
    assert (cfg.noresp_loss_sec, cfg.noresp_loss_below_c, cfg.noresp_loss_fall_c) == (1800.0, 2.0, 0.5)
    assert len([w for w in warns if "HEATER_NORESP_" in w]) == 6


@pytest.fixture
def restore_cfg():
    """load_settings 가 바꾼 config 모듈 값을 원래대로 돌린다."""
    from lib import config_common, config_ch1, config_ch2
    mods = (config_common, config_ch1, config_ch2)
    snap = [dict(vars(md)) for md in mods]
    yield
    for md, d in zip(mods, snap):
        for k in [k for k in vars(md) if k not in d]:
            delattr(md, k)
        for k, v in d.items():
            setattr(md, k, v)


def test_repo_settings_values(restore_cfg):
    from lib import _config_loader, config_ch2
    _config_loader.load_settings()
    cfg, warns = load_heater_config(config_ch2)
    assert not [w for w in warns if "HEATER_NORESP_" in w]
    assert (cfg.noresp_enable, cfg.noresp_mv, cfg.noresp_sec, cfg.noresp_rise_c) == (True, 600, 300.0, 5.0)
    assert (cfg.noresp_loss_sec, cfg.noresp_loss_below_c, cfg.noresp_loss_fall_c) == (180.0, 10.0, 10.0)


# ───────────────────────── 에뮬레이터 + HeaterService ─────────────────────────
def _mk(gain, **cfg):
    for k, v in (("max_temp", 900.0), ("sv_limit_c", 900.0), ("ot_limit_c", 950.0)):
        cfg.setdefault(k, v)
    return sim.make(gain=gain, mv_limit=1200, mv_abs_max=1300, ramp_rate=6.0, hold_mode="off", **cfg)


def _cards(chat, word):
    return [c for c in chat.cards if word in c[1]]


async def _start(svc, plc, clk, target, pub=None):
    if pub is not None:
        svc.add_listener(lambda st: pub.append(st.get("noresp")))
    await sim.run(svc, plc, clk, 5)
    ok, why = await svc.turn_on(target)
    assert ok, why
    return clk.t


def test_sim_dead_output_cold_start_turns_off():
    async def go():
        svc, plc, clk, logs, chat = _mk(0.0)                         # VARITAP 입력 0 V 흉내(열 없음)
        pub = []
        t0 = await _start(svc, plc, clk, 200.0, pub)
        await sim.run(svc, plc, clk, 900)
        assert not plc.M[640] and not svc.status()["run"]           # 히터 꺼짐
        trip = [t for t, m in logs if "히터 출력 무응답:" in m]
        assert len(trip) == 1 and 600 <= trip[0] - t0 <= 720         # MV 600 도달(약 350초) + 300초
        assert len(_cards(chat, "출력 무응답")) == 1 and _cards(chat, "출력 무응답")[0][3] == "FAIL"
        end = _cards(chat, "히터 종료")
        assert len(end) == 1 and end[0][2].endswith("· 출력 무응답") and end[0][3] == "FAIL"
        assert [x for x in pub if x] and len([x for x in pub if x]) == 1   # 판정한 폴링에만 실림
        assert [x for x in pub if x][0].startswith("출력 무응답 — MV ")
        assert svc.owner is None
    asyncio.run(go())


@pytest.mark.parametrize("target,secs", [(200.0, 3600), (450.0, 7200)])
def test_sim_normal_heating_no_trip(target, secs):
    async def go():
        svc, plc, clk, logs, chat = _mk(700.0)                       # 승온 MV 600~1200 을 지나는 모델
        pub = []
        await _start(svc, plc, clk, target, pub)
        await sim.run(svc, plc, clk, secs)
        assert plc.M[640] and abs(plc.D[110] / 10.0 - target) <= 2.0
        assert not [x for x in pub if x] and not _cards(chat, "출력")
    asyncio.run(go())


def test_sim_unreachable_plateau_stall_only():
    async def go():
        svc, plc, clk, logs, chat = _mk(700.0)                       # 최대 약 492°C — 600 은 도달 불가
        pub = []
        await _start(svc, plc, clk, 600.0, pub)
        await sim.run(svc, plc, clk, 9000)
        assert plc.M[640] and not [x for x in pub if x]
        assert len(_cards(chat, "승온 정체")) == 1 and not _cards(chat, "출력")
    asyncio.run(go())


@pytest.mark.parametrize("target", [250.0, 300.0, 350.0])
def test_sim_hot_restart_working_no_trip(target):
    async def go():
        svc, plc, clk, logs, chat = _mk(700.0)
        pub = []
        await _start(svc, plc, clk, 300.0, pub)
        await sim.run(svc, plc, clk, 4000)
        await svc.turn_off("정지(패널)", owner="manual")
        await sim.run(svc, plc, clk, 300)                            # 약 222°C 까지 식은 뒤 다시 켬
        ok, why = await svc.turn_on(target)
        assert ok, why
        await sim.run(svc, plc, clk, 2400)
        assert plc.M[640] and not [x for x in pub if x] and not _cards(chat, "출력")
    asyncio.run(go())


@pytest.mark.parametrize("target,loss_at", [(200.0, 3000), (450.0, 2000)])
def test_sim_output_loss_turns_off(target, loss_at):
    """유지 중(200°C)·승온 중(450°C 로 가는 길) 출력이 끊기면 '출력 끊김'으로 끈다."""
    async def go():
        svc, plc, clk, logs, chat = _mk(700.0)
        pub = []
        t0 = await _start(svc, plc, clk, target, pub)
        await sim.run(svc, plc, clk, loss_at)
        assert plc.M[640]
        plc.gain = 0.0                                               # 차단기·배선 끊김 흉내
        t1 = clk.t
        await sim.run(svc, plc, clk, 900)
        trip = [t for t, m in logs if "히터 출력 끊김:" in m]
        assert len(trip) == 1 and trip[0] - t1 <= 600 and not plc.M[640]
        assert len(_cards(chat, "출력 끊김")) == 1
        assert _cards(chat, "히터 종료")[0][2].endswith("· 출력 끊김")
        assert [x for x in pub if x][0].startswith("출력 끊김 — 출력 상한(MV ")
    asyncio.run(go())


# ───────────────────────── ChamberRuntime: 공정 중단 E832 한 번 ─────────────────────────
class _PCRec:
    """공정 컨트롤러 대신 on_heater_* 호출만 기록(test_runtime_heater 와 같은 방식)."""

    def __init__(self):
        self.calls = []

    def __getattr__(self, name):
        if name.startswith("on_heater"):
            return lambda *a, **k: self.calls.append((name, a, k))
        raise AttributeError(name)

    def names(self):
        return [c[0] for c in self.calls]

    def fails(self):
        return [(c[1][0], c[2].get("code")) for c in self.calls if c[0] == "on_heater_failed"]


def _rt():
    from runtime.chamber_runtime import ChamberRuntime
    rt = ChamberRuntime.__new__(ChamberRuntime)
    rt.ch = 2
    rt.logs = []
    rt.append_log = lambda s, m: rt.logs.append((s, m))
    rt._get_text = lambda name: ""
    rt.supports_rf_pulse = True
    rt.supports_dc_pulse = True
    rt.supports_dc_cont = True
    rt.supports_rf_cont = True
    rt.supports_dc_cont2 = False
    rt._loop = asyncio.get_running_loop()
    rt._ensure_task_alive = lambda *a, **k: None
    rt._heater = None
    rt._heater_gen = 0
    rt._heater_job = None
    rt._heater_watch = False
    rt._heater_rate = None
    rt._runner_queue_mode = True
    rt.process_queue = []
    rt.current_process_index = -1
    rt._bind_process_controller()
    pc = rt.process_controller
    rec = _PCRec()
    rt.process_controller = rec
    return rt, pc, rec


async def _tick(n=5):
    for _ in range(n):
        await asyncio.sleep(0)


def test_runtime_noresp_during_wait_fails_once_with_cause():
    async def go():
        rt, pc, rec = _rt()
        svc, plc, clk, logs, chat = _mk(0.0)
        rt.attach_heater(svc)
        await sim.run(svc, plc, clk, 5)
        pc._heater_set(200.0, None)
        await _tick()
        await sim.run(svc, plc, clk, 2)
        assert rec.names() == ["on_heater_set_ok"] and rt._heater_watch
        pc._heater_wait(200.0)
        assert rt._heater_job is not None
        await sim.run(svc, plc, clk, 900)
        await _tick()
        fails = rec.fails()
        assert len(fails) == 1 and fails[0][1] == "E832"            # 'RUN 꺼짐'(E830) 이 아니라 원인 문구로 한 번
        assert fails[0][0].startswith("공정 중 히터 출력 무응답 — MV ") and fails[0][0].endswith(" — 공정을 중단합니다")
        assert rt._heater_job is None and not rt._heater_watch
        assert not plc.M[640]
    asyncio.run(go())


def test_runtime_loss_while_watching_fails_once():
    async def go():
        rt, pc, rec = _rt()
        svc, plc, clk, logs, chat = _mk(700.0)
        rt.attach_heater(svc)
        await sim.run(svc, plc, clk, 5)
        pc._heater_set(200.0, None)
        await _tick()
        await sim.run(svc, plc, clk, 3000)
        assert rt._heater_watch and rt._heater_job is None and plc.M[640]
        plc.gain = 0.0
        await sim.run(svc, plc, clk, 900)
        await _tick()
        fails = rec.fails()
        assert len(fails) == 1 and fails[0][1] == "E832" and fails[0][0].startswith("공정 중 히터 출력 끊김 — ")
        assert not rt._heater_watch and not plc.M[640]
    asyncio.run(go())


def test_runtime_manual_heater_trip_does_not_touch_process():
    async def go():
        rt, pc, rec = _rt()
        svc, plc, clk, logs, chat = _mk(0.0)
        rt.attach_heater(svc)
        await sim.run(svc, plc, clk, 5)
        ok, why = await svc.turn_on(200.0, owner="manual")
        assert ok, why
        await sim.run(svc, plc, clk, 900)
        assert not plc.M[640] and rec.calls == []                     # 수동: 히터만 끔
    asyncio.run(go())


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
