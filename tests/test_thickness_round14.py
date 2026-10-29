# -*- coding: utf-8 -*-
"""두께 모니터 14차 — Thickness 모드 실사용 전 정리.
1) OES 를 메인 공정 구간과 맞춤(OES_RUN = 상한, THICKNESS_WAIT 끝에 stop — 워커는 stop flag 뒤 CSV 저장·finished ok)
2) 시간 대체 증착률 상식 범위(실측이 레시피의 0.5~2배 밖이면 레시피)
3) 시간 대체 남은 시간에서 마지막 값 뒤 경과를 뺌
4) 패널 [영점] — 확인창 [예] 뒤 다시 검사
5) 메인 공정 전에 끝난 Thickness 모드 공정 기록(종료 방식 '중단')"""
import asyncio
import contextlib
import os
import sys
import tempfile
import types
from pathlib import Path

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
sys.path.insert(0, _ROOT)
sys.path.insert(0, _HERE)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                    # noqa: E402

import test_thickness_process_steps as ps                        # noqa: E402
import test_thickness_runtime as tr                              # noqa: E402
from controller.process_controller import ProcessController      # noqa: E402
from controller.thickness_process import RATE_SANE_MAX, RATE_SANE_MIN, ThicknessEndJudge  # noqa: E402
from lib.thickness_config import ThicknessJudgeParams            # noqa: E402

P = ThicknessJudgeParams()


# ═══════════════════════ 1) OES ═══════════════════════
def test_oes_run_duration_thickness_limit_time_process_time():
    async def go():
        seq = ps._pc_of(ProcessController, 2)._create_process_sequence(ps._thk_params())
        oes = [s for s in seq if s.action.name == "OES_RUN"]
        wait = [s for s in seq if s.action.name == "THICKNESS_WAIT"][0]
        assert len(oes) == 1 and oes[0].params[0] == pytest.approx(wait.params[1]) == pytest.approx(4000.0)
        assert oes[0].message == "OES 측정 시작 (메인 공정 끝까지, 최대 66.67분)"
        for name, (ch, p) in ps.TIME_CASES.items():                 # Time 모드 — process_time 그대로
            seq = ps._pc_of(ProcessController, ch)._create_process_sequence(dict(p))
            o = [s for s in seq if s.action.name == "OES_RUN"]
            assert len(o) == 1 and o[0].params[0] == pytest.approx(float(p["process_time"]) * 60.0), name
    asyncio.run(go())


class FakeOES:
    def __init__(self, events=()):
        self.stops = []
        self._events = list(events)

    async def stop_measurement(self, *, wait=True, timeout_s=60.0):
        self.stops.append(wait)

    async def events(self):
        for ev in self._events:
            yield ev


@pytest.mark.parametrize("kind", ["Thickness", "시간대체", "상한", "중단"])
@pytest.mark.parametrize("active", [True, False])
def test_thickness_wait_end_stops_oes_once_when_active(kind, active):
    async def go():
        svc = tr.FakeSvc()
        rt, pc, _rec = tr._rt(svc)
        rt.oes = FakeOES()
        rt._oes_active = active
        rt._thk_run_started({"thickness_mode": True, "thickness": 100.0})
        pc._thickness_end(kind)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert rt.oes.stops == ([False] if active else [])          # 정리 코드와 같이 wait=False
        msgs = [m for s, m in rt.logs if "OES 측정 종료 요청" in m]
        assert msgs == (["OES 측정 종료 요청 — 메인 공정 끝(Thickness 모드)"] if active else [])
    asyncio.run(go())


def test_stopped_worker_finished_ok_is_normal_end():
    """stop flag 로 끝난 워커의 finished(ok=True) — _pump_oes_events 가 정상 종료(on_oes_ok)로 처리."""
    async def go():
        svc = tr.FakeSvc()
        rt, _pc, _rec = tr._rt(svc)
        ev = types.SimpleNamespace(kind="finished", success=True, out_csv="C:/x.csv", message="OES 측정 완료(로컬 저장)")
        rt.oes = FakeOES([ev])
        calls = []
        rt.process_controller = types.SimpleNamespace(on_oes_ok=lambda: calls.append("ok"),
                                                      on_oes_failed=lambda *a, **k: calls.append("failed"))
        rt._oes_active = True
        await rt._pump_oes_events()
        assert calls == ["ok"] and rt._oes_active is False
    asyncio.run(go())


def test_device_stop_twice_is_harmless(tmp_path, monkeypatch):
    """메인 공정 끝 stop(wait=False) 뒤 정리 코드가 stop 을 또 불러도 문제없다 — 워커 살아 있음/끝남/없음 모두."""
    import device.oes as O
    monkeypatch.setenv("OES_LOCAL_BASE", str(tmp_path / "OES"))
    o = O.OESAsync(chamber=2, usb_index=1)
    o._daemon_enabled = False
    o._out_csv_local = o._local_dir / "run.csv"

    class Proc:
        returncode = None
        pid = 1
    o._proc = Proc()

    async def go():
        await o.stop_measurement(wait=False)                      # 메인 공정 끝
        flag = Path(str(o._out_csv_local) + ".stop")
        assert flag.exists() and o._stop_requested
        await o.stop_measurement(wait=False)                      # 정리 코드(워커가 아직 저장 중)
        assert flag.exists()
        o._proc.returncode = 0                                    # 워커 끝남
        o.is_running = True                                       # run_measurement 가 아직 정리 중
        await o.stop_measurement(wait=False)
        assert o._stop_requested is False
        o._proc = None                                            # run_measurement 정리 뒤
        await o.stop_measurement(wait=False)
    asyncio.run(go())


def test_worker_stop_flag_saves_csv_and_finishes_ok():
    """워커 — stop flag 를 보면 측정을 멈추고 CSV 를 닫은 뒤 finished ok=True(stopped, stop_flag)."""
    from test_oes_worker_cyusb import LIST_A, FakeDLL, W, _csv_rows, _Env, patched
    from test_oes_worker_cyusb_safety import _fake_init_for
    with _Env() as env:
        out_csv = env.tmp / "f" / "stop.csv"

        def _on_read(n):
            if n == 5:
                Path(str(out_csv) + ".stop").write_text("stop\n", encoding="utf-8")
        dll = FakeDLL(on_read=_on_read)
        with patched(W.OESAsync, initialize_device=_fake_init_for(dll)), \
             patched(W, _list_cyusb_interfaces=lambda: list(LIST_A)):
            rc = asyncio.run(W.cmd_measure(ch=1, usb=0, duration_s=60.0, integration_ms=50, sample_interval_s=0.02,
                                           avg_count=1, out_dir=None, out_csv=out_csv, dll_path=None))
        assert rc == 0
        fin = env.finished()
        assert fin["ok"] is True and fin["stopped"] is True and fin["stop_reason"] == "stop_flag"
        assert fin["elapsed_s"] < 30 and fin["rows"] == len(_csv_rows(out_csv)) - 1 >= 1


# ═══════════════════════ 2) 증착률 상식 범위 ═══════════════════════
def _judge_with_rate(k, dep=0.1, span=40.0):
    j = ThicknessEndJudge(100.0, dep, P, start_t=0.0, base=0.0, seq0=0)
    j._set_eff(dep * k * span, span)
    return j


@pytest.mark.parametrize("k,src", [(0.3, "레시피"), (1.0, "실측"), (3.0, "레시피")])
def test_use_rate_sane_range(k, src):
    j = _judge_with_rate(k)
    rate, s = j.use_rate()
    assert s == src and rate == pytest.approx(0.1 * (k if src == "실측" else 1.0))
    assert (RATE_SANE_MIN, RATE_SANE_MAX) == (0.5, 2.0)


def test_out_of_range_rate_is_logged_on_fallback():
    j = _judge_with_rate(3.0)
    ev = j._to_fallback(40.0, "새 두께 값 없음", permanent=False)
    msg = j.events[-1]
    assert ev.kind == "fallback" and "레시피 0.1000 nm/s" in msg and "실측 0.3000 nm/s" in msg and "0.5~2배 밖" in msg


# ═══════════════════════ 3) 남은 시간 기준점 ═══════════════════════
def test_fallback_remaining_subtracts_time_since_last_value():
    j = _judge_with_rate(1.0)                              # 유효 4 nm @40초, 실측 0.1 nm/s
    ev = j._to_fallback(52.0, "새 두께 값 없음 12초", permanent=False)
    assert ev.remaining_s == pytest.approx((100.0 - 4.0) / 0.1 - 12.0)
    assert "마지막 값 뒤 12초 반영" in j.events[-1]
    j2 = ThicknessEndJudge(5.0, 0.1, P, start_t=0.0, base=0.0, seq0=0)
    j2._set_eff(4.5, 10.0)                                 # 남은 0.5 nm ÷ 레시피 0.1 = 5초 < 경과 12초 → 0
    assert j2._to_fallback(22.0, "x", permanent=False).remaining_s == 0.0
    j3 = ThicknessEndJudge(10.0, 0.1, P, start_t=0.0, base=None, seq0=0)   # 기준점 없음(eff_t 없음) → 지금처럼
    ev3 = j3.start_event()
    assert ev3.remaining_s == pytest.approx(100.0) and "마지막 값 뒤" not in j3.events[-1]


# ═══════════════════════ 4) 패널 [영점] ═══════════════════════
def test_zero_rechecks_after_confirm(monkeypatch):
    from PySide6.QtWidgets import QApplication, QMessageBox
    QApplication.instance() or QApplication([])
    import test_thickness_panel as tp
    from ui.thickness_panel import ThicknessPanel
    from lib.thickness_config import ThicknessConfig

    async def go():
        with tempfile.TemporaryDirectory() as tmp:
            svc = tp.FakeSvc()
            zeros = []

            async def zero(mode="B", why=""):
                zeros.append(mode)
                return True, ""
            svc.zero = zero
            p = ThicknessPanel(svc, ThicknessConfig(enabled=True, ch=2, port=4008, factor_file=str(Path(tmp) / "f.json")))
            p.update_status(dict(tp._OK))

            def answer_yes_while_robot_starts(*a, **k):
                p.update_status(dict(tp._OK, process_running=True))     # 확인창이 떠 있는 동안 공정 시작
                return QMessageBox.StandardButton.Yes
            monkeypatch.setattr(QMessageBox, "question", answer_yes_while_robot_starts)
            p._on_zero_clicked()
            for _ in range(5):
                await asyncio.sleep(0)
            assert zeros == [] and not p._busy
            assert any("공정 중" in b.text() and "보내지 않았습니다" in b.text() for b in p._msg_boxes)
            p.update_status(dict(tp._OK))                                # 막힘 없으면 그대로 보낸다
            monkeypatch.setattr(QMessageBox, "question", lambda *a, **k: QMessageBox.StandardButton.Yes)
            p._on_zero_clicked()
            for _ in range(5):
                await asyncio.sleep(0)
            assert zeros == ["B"]
            for b in list(p._msg_boxes):
                b.close()
    asyncio.run(go())


# ═══════════════════════ 5) 메인 공정 전 종료 기록 ═══════════════════════
def test_stop_during_shutter_delay_records_abort_before_main():
    async def go():
        svc = tr.FakeSvc()
        rt, _pc, _rec = tr._rt(svc)
        rt._thk_pre = {"chuck": "mid", "gun": "G1", "tooling": 110.0, "density": 2.7, "z_factor": 1.08}
        rt._thk_run_started({"thickness_mode": True, "thickness": 100.0})   # Shutter Delay 중 정지 — THICKNESS_WAIT 없음
        views, recs = [], []
        rt.set_thickness_view(lambda *a: views.append(a))
        rt.data_logger = types.SimpleNamespace(set_thickness_record=lambda v: recs.append(dict(v)))
        v = rt._thk_values()
        assert v["end"] == "중단" and v["final_nm"] is None and v["tooling"] == 110.0
        card = {}
        rt._thk_card_fields(card)
        assert card == {"thickness_end": "중단", "thickness_final": "-- / 100 nm"}
        rt._thk_finalize_record()
        assert recs[-1]["end"] == card["thickness_end"] and recs[-1]["final_nm"] is None    # Calib CSV = 카드
        assert views[-1] == ("중단", "100.0 nm", None, "종료(중단) — 메인 공정 전")
        # 메인 공정까지 간 공정은 그대로(_thk_on_end 가 넣은 종료 방식)
        rt._thk_run_started({"thickness_mode": True, "thickness": 100.0})
        rt._thk_last_judge = None
        rt._thk_on_end("Thickness")
        assert rt._thk_values()["end"] == "Thickness"
    asyncio.run(go())
