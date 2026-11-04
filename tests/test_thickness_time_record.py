# -*- coding: utf-8 -*-
"""두께 모니터 — Time 모드에서도 Factor 적용·영점 후 기록.
  · 시작 전 Factor 적용 시도(_thickness_time_prepare) — 실패해도 시작을 막지 않고 사유를 Fallback Reason 에
  · 성공 런만 Shutter Delay 뒤·Main Shutter 앞에 THICKNESS_ZERO(THICKNESS_WAIT 없음, 레시피 시간으로 끝)
  · 기록: 기준점이 있으면 마지막 − 기준점, 없으면 마지막 − 처음 · Factor 열 채움 · End Mode 'Time' · Target 공란"""
import asyncio
import contextlib
import csv
import os
import sys
import tempfile
import types
from pathlib import Path

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))
sys.path.insert(0, _HERE)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                    # noqa: E402

import test_thickness_process_steps as ps                        # noqa: E402
import test_thickness_runtime as tr                              # noqa: E402
from controller.data_logger import DataLogger                    # noqa: E402
from controller.process_controller import ProcessController      # noqa: E402


def _time_params(**kw):
    p = tr._params(thickness=None)                               # Time 모드(thickness 비움), 건 G1 하나, chuck mid
    p.update(kw)
    return p


# ═══════════════ 시퀀스·ETA ═══════════════
def _names(seq):
    return [s.action.name for s in seq]


def test_time_record_sequence_has_zero_between_shutter_delay_and_main_shutter():
    async def go():
        base_p = dict(ps.TIME_CASES["ch2_thk_dcp"][1])
        base = ps._pc_of(ProcessController, 2)._create_process_sequence(dict(base_p))
        seq = ps._pc_of(ProcessController, 2)._create_process_sequence(dict(base_p, thickness_time_record=True))
        n = _names(seq)
        z = n.index("THICKNESS_ZERO")
        assert seq[z - 1].action.name == "DELAY" and seq[z - 1].message.startswith("Shutter Delay")
        assert seq[z + 1].action.name == "PLC_CMD" and seq[z + 1].params[:2] == ("MS", True)
        assert "THICKNESS_WAIT" not in n and "THICKNESS_ZERO" not in _names(base)
        main = [s for s in seq if s.action.name == "DELAY" and s.message.startswith("메인 공정")]
        assert len(main) == 1 and main[0].duration == int(round(base_p["process_time"] * 60 * 1000))   # 레시피 시간 그대로
        rows, brows = [ps.hs._rows([s]) for s in seq], [ps.hs._rows([s]) for s in base]
        assert rows[:z] + rows[z + 1:] == brows                    # 영점 한 줄 말고는 같다(OES·셔터·종료 절차)

        pc = ps._pc_of(ProcessController, 2)                       # ETA — 영점 확인 최대 초를 더한다(일찍 알리지 않게)
        pc.process_sequence = seq
        sd = n.index("DELAY")
        pc2 = ps._pc_of(ProcessController, 2)
        pc2.process_sequence = base
        assert pc._eta_sum_delay_from(sd) == pytest.approx(pc2._eta_sum_delay_from(sd) + 5.0)
        # Time 모드 골든(플래그 없음 — 시퀀스·ETA 그대로)은 test_thickness_process_steps 가 본다
    asyncio.run(go())


# ═══════════════ 시작 전 Factor 적용(차단 없음) ═══════════════
def _prep(svc, params, chuck_ok=True):
    async def go():
        rt, pc, rec = tr._rt(svc)
        rt._run_warnings = []
        await rt._thickness_time_prepare(params, chuck_ok)
        return rt
    return asyncio.run(go())


def test_time_prepare_factor_ok_sets_flag_and_pre():
    svc = tr.FakeSvc()
    p = _time_params()
    rt = _prep(svc, p)
    assert p.get("thickness_time_record") is True and "thickness_record_note" not in p and "thickness_mode" not in p
    assert svc.mat_calls == [(2.73, 1.08, 110.0)] and svc.zero_calls == []          # 영점은 THICKNESS_ZERO 단계에서
    assert rt._thk_pre == {"chuck": "mid", "gun": "G1", "tooling": 110.0, "density": 2.73, "z_factor": 1.08,
                           "material": "Aluminum", "factor_source": "T:factor D:factor Z:factor"}
    line = [m for s, m in rt.logs if s == "THICKNESS"][-1]
    assert line.startswith("Time 모드(두께 기록) — G1 · chuck mid · tooling 110 %") and "목표" not in line
    assert rt._run_warnings == []


@pytest.mark.parametrize("mut,code", [
    (lambda s: s.factors.tooling["mid"].__setitem__("G1", None), "E845"),          # Factor 미설정
    (lambda s: s.st.update(connected=False), "E840"),                               # 응답 없음
    (lambda s: s.st.update(crystal_ok=False), "E841"),                              # XTAL FAIL
    (lambda s: setattr(s, "mat_error", RuntimeError("되읽기 불일치")), "E846"),       # 쓰기·되읽기 실패
])
def test_time_prepare_failure_does_not_block(mut, code):
    svc = tr.FakeSvc()
    mut(svc)
    p = _time_params()
    rt = _prep(svc, p)                                                              # 예외 없음 = 시작 차단 없음
    assert "thickness_time_record" not in p and rt._thk_pre is None
    note = p["thickness_record_note"]
    assert note.startswith(f"[{code}]") and note.endswith("Factor 미적용")
    assert rt._run_warnings == [f"두께 기록: {note}"]
    assert any(s == "THICKNESS" and "Time 모드 두께 기록" in m and note in m for s, m in rt.logs)
    assert not any("두께 모니터 점검 실패" in m for s, m in rt.logs)                 # Thickness 모드 거부 흐름 아님
    seq = asyncio.run(_seq(p))
    assert "THICKNESS_ZERO" not in _names(seq) and "THICKNESS_WAIT" not in _names(seq)


async def _seq(p):
    q = dict(ps.TIME_CASES["ch2_thk_dcp"][1])
    q.update({k: v for k, v in p.items() if k.startswith("thickness_")})
    return ps._pc_of(ProcessController, 2)._create_process_sequence(q)


def test_time_prepare_chuck_unknown_does_not_block():
    svc = tr.FakeSvc()
    p = _time_params(chuck_position="")
    rt = _prep(svc, p)
    assert p["thickness_record_note"].startswith("[E845] Chuck 위치를 알 수 없어") and svc.mat_calls == []
    assert rt._thk_pre is None


@pytest.mark.parametrize("guns,word", [(("g1", "g2"), "co-sputter(G1+G2) — Factor 미적용"), ((), "사용 건 없음 — Factor 미적용")])
def test_time_prepare_cosputter_or_no_gun_sends_nothing(guns, word):
    svc = tr.FakeSvc()
    flags = {"use_g1": False, "use_g2": False, "use_g3": False}
    flags.update({f"use_{g}": True for g in guns})
    p = _time_params(**flags)
    sent = []
    svc.status = lambda: (sent.append("status"), dict(svc.st))[1]
    rt = _prep(svc, p)
    assert svc.mat_calls == [] and svc.zero_calls == [] and sent == []          # 장비에 아무 명령도(상태 조회도) 하지 않음
    assert p["thickness_record_note"] == word and "thickness_time_record" not in p
    rt._thk_run_started(p)
    assert rt._thk_rec["fallback_reason"] == word and "time_record" not in rt._thk_rec


def test_thickness_mode_unchanged_by_time_branch():
    """Thickness 모드 — 시작 전 점검·재료값 쓰기 로그 문구가 그대로(목표 포함), 플래그는 thickness_mode 만."""
    async def go():
        svc = tr.FakeSvc()
        rt, pc, rec = tr._rt(svc)
        rt._run_warnings = []
        p = tr._params()
        assert rt._thickness_check_basic(p) == ("", "")
        assert await rt._thickness_apply_material(p, True) == ("", "")
        assert [m for s, m in rt.logs if s == "THICKNESS"][-1].startswith("Thickness 모드 — 목표 100 nm · G1 · chuck mid")
        assert "thickness_time_record" not in p
        p2 = tr._params(dep_rate=0)
        assert rt._thickness_check_basic(p2)[0] == "E842"
    asyncio.run(go())


# ═══════════════ 영점·기록 ═══════════════
def _start_time_run(svc, **kw):
    rt, pc, rec = tr._rt(svc)
    rt._run_warnings = []
    p = _time_params(**kw)
    return rt, pc, rec, p


async def _zero(rt, pc, svc, new_vals, crystal_ok=True):
    views = []
    rt.set_thickness_view(lambda *a: views.append(a))
    pc._thickness_zero(5.0)
    await asyncio.sleep(0.05)
    if not crystal_ok:
        svc.st.update(crystal_ok=False, link_ok=True)
    for seq, s in new_vals:
        svc.st.update(seq=seq, thickness_nm=s)
        await asyncio.sleep(0.15)
    for _ in range(30):
        if rt._thk_zero is not None:
            break
        await asyncio.sleep(0.05)
    svc.st.update(crystal_ok=True)
    return views


def _main_window(rt, svc, vals, dt=10.0):
    rt._proc_polling_active = True
    tr._drive(rt, svc, vals, dt=dt)
    rt._proc_polling_active = False


def test_time_zero_success_final_is_last_minus_base_and_factor_columns():
    async def go():
        svc = tr.FakeSvc()
        rt, pc, rec, p = _start_time_run(svc)
        await rt._thickness_time_prepare(p, True)
        rt._thk_run_started(p)
        assert rt._thk_rec["time_record"] and rt._thk_rec["target"] is None
        svc.zero_ok = False

        async def zero(mode="B", why=""):
            svc.zero_calls.append(mode)
            return False, "영점 실패 주입"                                        # 첫 값 1.5 nm 를 기준점으로
        svc.zero = zero
        views = await _zero(rt, pc, svc, [(8, 1.5)])
        assert svc.zero_calls == ["B"] and "on_thickness_zero_done" in rec.names()
        assert rt._thk_rec["zero_base"] == 1.5 and views and views[0][0] == "Time"
        _main_window(rt, svc, [(9, 2.0), (10, 6.0), (11, 11.5)])                    # 처음 2.0 · 마지막 11.5 (20초)
        v = rt._thk_values()
        assert v["end"] == "Time" and v["target"] is None
        assert v["final_nm"] == pytest.approx(11.5 - 1.5) and v["avg_rate"] == pytest.approx(10.0 / 20.0)
        assert (v["tooling"], v["density"], v["z_factor"], v["material"], v["chuck"], v["gun"]) == \
            (110.0, 2.73, 1.08, "Aluminum", "mid", "G1")
        assert v["factor_source"] == "T:factor D:factor Z:factor"
        recs = []
        rt.data_logger = types.SimpleNamespace(set_thickness_record=lambda r: recs.append(dict(r)))
        rt._thk_finalize_record()
        assert any("두께 요약 — 종료 방식 Time · 최종 10.00 nm" in m for s, m in rt.logs)
        return recs[-1]
    tr_rec = asyncio.run(go())
    with tempfile.TemporaryDirectory() as tmp:                                      # Calib CSV 열(같은 기록값)
        dl = DataLogger(ch=2, csv_dir=Path(tmp), thickness_columns=True)
        dl.start_new_log_session({"process_note": "t", "use_ms": False})
        dl.set_thickness_record(tr_rec)
        dl.finalize_and_write_log(True)
        r = list(csv.DictReader(open(Path(tmp) / "Ch2_log.csv", encoding="utf-8-sig")))[-1]
        assert (r["Thickness: End Mode"], r["Thickness: Target nm"], r["Thickness: Final nm"]) == ("Time", "", "10.00")
        assert (r["Thickness: Tooling %"], r["Thickness: Material"], r["Thickness: Factor Source"]) == \
            ("110", "Aluminum", "T:factor D:factor Z:factor")
        assert (r["Thickness: Chuck"], r["Thickness: Gun"], r["Thickness: Fallback Reason"]) == ("mid", "G1", "")


def test_time_zero_without_base_falls_back_to_last_minus_first():
    async def go():
        svc = tr.FakeSvc()
        rt, pc, rec, p = _start_time_run(svc)
        await rt._thickness_time_prepare(p, True)
        rt._thk_run_started(p)
        await _zero(rt, pc, svc, [], crystal_ok=False)                              # XTAL FAIL → 기준점 없음
        assert rt._thk_zero["base"] is None and rt._thk_rec["zero_base"] is None
        _main_window(rt, svc, [(9, 2.0), (10, 7.0)])
        v = rt._thk_values()
        assert v["final_nm"] == pytest.approx(5.0) and v["tooling"] == 110.0
    asyncio.run(go())


def test_time_failed_prepare_records_difference_and_reason():
    async def go():
        svc = tr.FakeSvc()
        svc.st.update(connected=False)
        rt, pc, rec, p = _start_time_run(svc)
        await rt._thickness_time_prepare(p, True)
        rt._thk_run_started(p)
        _main_window(rt, svc, [(9, 40.0), (10, 43.0)])
        v = rt._thk_values()
        assert v["final_nm"] == pytest.approx(3.0) and v.get("tooling") is None
        assert v["fallback_reason"].startswith("[E840] 두께 모니터 응답 없음")
        rt2, _pc2, _ = tr._rt(svc)                                                  # 값이 하나도 없어도 사유는 남긴다
        rt2._run_warnings = []
        p2 = _time_params()
        await rt2._thickness_time_prepare(p2, True)
        rt2._thk_run_started(p2)
        assert rt2._thk_values()["fallback_reason"] == p2["thickness_record_note"]
    asyncio.run(go())


def test_time_zero_stop_leaves_no_hanging_task():
    async def go():
        svc = tr.FakeSvc()
        rt, pc, rec, p = _start_time_run(svc)
        await rt._thickness_time_prepare(p, True)
        rt._thk_run_started(p)
        pc._thickness_zero(5.0)
        await asyncio.sleep(0.2)                                                    # 새 값 없음 — 확인 대기 중
        pc._cancel_thickness_cb()                                                   # Stop
        await asyncio.sleep(0.3)
        pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        assert pending == [] and "on_thickness_zero_done" not in rec.names()
    asyncio.run(go())


def test_ch_without_service_does_nothing():
    async def go():
        rt, pc, rec = tr._rt(None)
        assert not rt._thickness_supported()
        rt._thk_run_started(_time_params())
        assert rt._thk_rec == {} and rt._thk_values() == {}
    asyncio.run(go())
