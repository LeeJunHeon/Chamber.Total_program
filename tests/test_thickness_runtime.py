# -*- coding: utf-8 -*-
"""ChamberRuntime ↔ 두께 모니터(2단계): 시작 전 점검(거부 코드), 재료값 쓰기·되읽기 재시도(실제 서비스 + 가짜 STM-100),
THICKNESS_WAIT 콜백(영점 → 판정기), 리스너 판정·예외, 공정 기록(Time/Thickness)·DataLogger 열·종료 카드."""
import asyncio
import contextlib
import csv
import os
import sys
import tempfile
import types
from collections import deque
from pathlib import Path

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
sys.path.insert(0, _ROOT)
sys.path.insert(0, _HERE)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                    # noqa: E402

from controller.data_logger import THICKNESS_COLUMNS, DataLogger  # noqa: E402
from controller.thickness_process import ThicknessEndJudge       # noqa: E402
from controller.runtime_state import runtime_state               # noqa: E402
from errors.app_error import CodedRuntimeError                   # noqa: E402
from lib.thickness_config import ThicknessJudgeParams            # noqa: E402
from lib.thickness_factors import ThicknessFactors               # noqa: E402
from runtime.chamber_runtime import ChamberRuntime, _CfgAdapter  # noqa: E402


class _PCRec:
    """공정 컨트롤러 대신 on_thickness_*·set_thickness_state 호출만 기록(중간 변경 확인은 진짜 컨트롤러에 맡긴다)."""

    def __init__(self, real):
        self.calls = []
        self.real = real
        self.current_params = {}
        self.is_running = True

    def thickness_mid_change_conflict(self, p):
        return self.real.thickness_mid_change_conflict(p)

    def __getattr__(self, name):
        if name.startswith("on_thickness") or name == "set_thickness_state":
            return lambda *a, **k: self.calls.append((name, a))
        raise AttributeError(name)

    def names(self):
        return [c[0] for c in self.calls]


class FakeSvc:
    def __init__(self):
        self.t = 100.0
        self.st = {"connected": True, "ok_ts": 99.0, "crystal_ok": True, "seq": 7, "reset_seen": 0,
                   "life_pct": 50.0, "thickness_nm": 3.0, "stale": False}
        self.factors = ThicknessFactors()
        self.factors.tooling["mid"]["G1"] = 110.0
        self.factors.material["G1"] = "Aluminum"
        from lib.thickness_materials import Material
        self.materials = [Material("Aluminum", "Al", 2.73, 1.08), Material("Titanium", "Ti", 4.5, 0.628)]
        self.cfg = types.SimpleNamespace(factor_file="config/thickness_factors_ch2.json")
        self.mat_calls = []
        self.zero_calls = []
        self.recording = []                 # set_recording 호출 기록
        self.mat_error = None
        self.listeners = []

    def status(self):
        return dict(self.st)

    def find_material(self, name):
        from lib.thickness_materials import find_material
        return find_material(self.materials, name)

    def clock(self):
        return self.t

    def set_recording(self, on, process_name=""):
        self.recording.append((bool(on), process_name))

    def add_listener(self, cb):
        self.listeners.append(cb)

    async def set_material(self, d, z, t, material=""):
        self.mat_calls.append((d, z, t))
        self.mat_names = getattr(self, "mat_names", []) + [material]
        if self.mat_error:
            raise self.mat_error
        return {"density": d, "z_factor": z, "tooling": t}

    async def zero(self, mode="B", why=""):
        self.zero_calls.append(mode)
        return True, ""


def _rt(svc=None, supports=True):
    rt = ChamberRuntime.__new__(ChamberRuntime)
    rt.ch = 2
    rt.logs = []
    rt.append_log = lambda s, m: rt.logs.append((s, m))
    rt.append_side_log = lambda s, m: rt.logs.append((s, m))
    rt._get_text = lambda name: ""
    rt.supports_rf_pulse = rt.supports_dc_pulse = rt.supports_dc_cont = rt.supports_rf_cont = True
    rt.supports_dc_cont2 = False
    rt._loop = asyncio.get_running_loop()
    rt._ensure_task_alive = lambda *a, **k: None
    rt._spawn_detached = lambda coro, **k: asyncio.ensure_future(coro)
    rt._heater = None
    rt._runner_queue_mode = True
    rt.process_queue = []
    rt.current_process_index = -1
    rt.cfg = _CfgAdapter(types.SimpleNamespace(SUPPORTS_THICKNESS=supports, PROCESS_GUN_SHUTTERS=["G1", "G2", "G3"]), 2)
    rt.chat = None
    rt.plc = None
    rt._run_warnings = []
    rt._bind_process_controller()
    pc = rt.process_controller
    rec = _PCRec(pc)
    rt.process_controller = rec
    if svc is not None:
        rt.attach_thickness(svc)
    return rt, pc, rec


def _params(**kw):
    p = dict(thickness=100.0, dep_rate=0.05, use_g1=True, use_g2=False, use_g3=False, shutter_delay=5.0,
             chuck_position="mid", process_note="t1", use_dc_pulse=True, dc_pulse_power=80.0)
    p.update(kw)
    return p


# ═══════════════════════ 모드 판정 ═══════════════════════
def test_supported_requires_service_and_flag():
    async def go():
        rt, _pc, _ = _rt(None)
        assert not rt._thickness_supported()
        rt, _pc, _ = _rt(FakeSvc(), supports=False)
        assert not rt._thickness_supported()
        rt, pc, _ = _rt(FakeSvc())
        assert rt._thickness_supported()
        assert pc._thickness_supported() is True                  # ETA 하한도 같은 판정
    asyncio.run(go())


# ═══════════════════════ 시작 전 점검 ═══════════════════════
@pytest.mark.parametrize("mut,code", [
    (lambda s, p: s.st.update(connected=False), "E840"),
    (lambda s, p: s.st.update(ok_ts=90.0), "E840"),                # 최근 5초 안 정상 S 없음
    (lambda s, p: s.st.update(ok_ts=None), "E840"),
    (lambda s, p: s.st.update(crystal_ok=False), "E841"),
    (lambda s, p: p.update(dep_rate=None), "E842"),
    (lambda s, p: p.update(thickness=0), "E842"),
    (lambda s, p: p.update(use_g2=True), "E843"),                  # co-sputter
    (lambda s, p: p.update(use_g1=False), "E843"),                 # 건 없음
    (lambda s, p: p.update(power_change_time="6m", change_power_value="100"), "E844"),
])
def test_basic_check_rejects(mut, code):
    async def go():
        svc = FakeSvc()
        rt, _pc, _ = _rt(svc)
        p = _params()
        mut(svc, p)
        c, msg = rt._thickness_check_basic(p)
        assert c == code and msg
    asyncio.run(go())


def test_basic_check_passes():
    async def go():
        rt, _pc, _ = _rt(FakeSvc())
        assert rt._thickness_check_basic(_params()) == ("", "")
        assert rt._thickness_check_basic(_params(power_change_time="2m", change_power_value="100")) == ("", "")
    asyncio.run(go())


def test_apply_material_factor_and_errors():
    async def go():
        svc = FakeSvc()
        rt, _pc, _ = _rt(svc)
        c, msg = await rt._thickness_apply_material(_params(chuck_position="up"), True)
        assert c == "E845" and "tooling" in msg and svc.mat_calls == []           # up × G1 tooling 없음
        svc.factors.material["G1"] = None
        c, msg = await rt._thickness_apply_material(_params(), True)
        assert c == "E845" and "G1 재료 미선택" in msg and svc.mat_calls == []
        svc.factors.material["G1"] = "Unobtainium"
        c, msg = await rt._thickness_apply_material(_params(), True)
        assert c == "E845" and "G1 재료 'Unobtainium' 가 재료 목록에 없음" in msg and svc.mat_calls == []
        svc.factors.material["G1"] = "aluminum"                                      # 이름은 대소문자 무시
        c, _m = await rt._thickness_apply_material(_params(chuck_position=""), True)
        assert c == "E845"                                                          # chuck 위치 모름
        svc.mat_error = RuntimeError("되읽기 불일치")
        c, msg = await rt._thickness_apply_material(_params(), True)
        assert c == "E846" and "되읽기" in msg
        svc.mat_error = None
        svc.st["life_pct"] = 4.0
        assert await rt._thickness_apply_material(_params(), True) == ("", "")
        assert svc.mat_calls[-1] == (2.73, 1.08, 110.0) and svc.mat_names[-1] == "Aluminum"   # 재료 목록 값
        assert rt._thk_pre["tooling"] == 110.0 and rt._thk_pre["chuck"] == "mid" and rt._thk_pre["gun"] == "G1"
        assert rt._thk_pre["material"] == "Aluminum"
        assert any("재료 Al — Aluminum" in m for _s, m in rt.logs)                      # 시작 전 점검 로그
        assert any("크리스탈 수명" in w for w in rt._run_warnings)
    asyncio.run(go())


def test_reject_flow_like_gate_check():
    async def go():
        rt, _pc, _ = _rt(FakeSvc())
        ev = []
        rt.chat = types.SimpleNamespace(notify_error_event=lambda *a: ev.append(a), flush=lambda: None)

        async def stopw(light=False):
            ev.append("stop")
        rt._stop_device_watchdogs = stopw
        rt._on_process_status_changed = lambda on: ev.append(("status", on))
        rt._auto_connect_enabled = True
        with pytest.raises(CodedRuntimeError) as ei:
            await rt._thickness_reject(_params(), "E843", "co-sputter")
        assert ei.value.code == "E843"
        assert ev[0][1] == "E843" and "stop" in ev and ("status", False) in ev and not rt._auto_connect_enabled
        assert runtime_state.has_error("chamber", 2)
        runtime_state.clear_error("chamber", 2)
    asyncio.run(go())


def test_set_material_readback_retry_with_real_service():
    """실제 ThicknessService + 드라이버 + 가짜 STM-100: 첫 되읽기가 다르면 1회 다시 쓰고 통과."""
    from test_thickness_monitor import FakeSTM
    from controller.thickness_service import ThicknessService
    from device.thickness_monitor import AsyncThicknessMonitor
    from lib.thickness_config import ThicknessConfig

    async def go():
        srv = FakeSTM()
        await srv.start()
        with tempfile.TemporaryDirectory() as tmp:
            fac = Path(tmp) / "f.json"
            from lib.thickness_factors import save_factors
            f = ThicknessFactors()
            f.tooling["mid"]["G1"] = 110.0
            f.material["G1"] = "Aluminum"
            save_factors(fac, f)
            from lib.thickness_materials import Material, save_materials
            mfile = Path(tmp) / "materials.json"
            save_materials(mfile, [Material("Aluminum", "Al", 2.7, 1.08)])
            dev = AsyncThicknessMonitor("127.0.0.1", srv.port, cmd_timeout_s=0.2, backoff_start_s=0.05, settle_s=0.0)
            cfg = ThicknessConfig(enabled=True, ch=2, port=srv.port, poll_interval_s=0.25, slow_poll_s=0.25,
                                  log_dir=str(Path(tmp) / "logs"), factor_file=str(fac))
            svc = ThicknessService(cfg, log=lambda s, m: None, device=dev, materials_file=str(mfile))
            svc.start()
            try:
                for _ in range(100):
                    if svc.status()["seq"] >= 1 and svc.status()["crystal_ok"] is not None:
                        break
                    await asyncio.sleep(0.05)
                rt, _pc, _ = _rt(svc)
                assert rt._thickness_check_basic(_params()) == ("", "")
                srv.script["J?"] = deque([("raw", "A105.0")])                       # 첫 되읽기만 틀림
                assert await rt._thickness_apply_material(_params(), True) == ("", "")
                assert srv.rx.count("J=110") == 2 and srv.rx.count("E=2.7") == 2
                assert svc.status()["tooling"] == 110.0 and svc.status()["material"] == "Aluminum"
                srv.script["J?"] = deque([("raw", "A105.0"), ("raw", "A105.0")])   # 두 번 다 틀림 → E846
                c, _m = await rt._thickness_apply_material(_params(), True)
                assert c == "E846"
            finally:
                await svc.shutdown(timeout=1.0)
                await srv.close()
    asyncio.run(asyncio.wait_for(go(), 20))


# ═══════════════════════ THICKNESS_WAIT 콜백·리스너 ═══════════════════════
def test_start_callback_creates_judge_with_zero_step_base():
    """THICKNESS_WAIT 시작(셔터 연 뒤) — 영점은 보내지 않고 THICKNESS_ZERO 에서 정한 기준점으로 판정기를 만든다."""
    async def go():
        svc = FakeSvc()
        rt, pc, rec = _rt(svc)
        rec.current_params = {"dep_rate": 0.05}
        views = []
        rt.set_thickness_view(lambda *a: views.append(a))
        rt._thk_zero = {"base": 0.0, "reset0": 0}
        pc._thickness_start(100.0, 2000.0, 4000.0)
        j = rt._thk_judge
        assert svc.zero_calls == [] and isinstance(j, ThicknessEndJudge) and j.base == 0.0
        assert j.target == 100.0 and j.dep_rate == 0.05 and j._seq_last == 7
        assert "on_thickness_fallback" not in rec.names()
        assert views and views[-1][0] == "Thickness"
        pc._cancel_thickness_cb()                       # 취소 → 판정기 해제
        assert rt._thk_judge is None
    asyncio.run(go())


def _drive(rt, svc, seq_vals, dt=0.5):
    for seq, s in seq_vals:
        svc.t += dt
        svc.st.update(seq=seq, thickness_nm=s)
        rt._on_thickness_status(svc.status())


def test_listener_done_fallback_resume():
    async def go():
        svc = FakeSvc()
        rt, _pc, rec = _rt(svc)
        rt._thk_rec = {"mode": "Thickness", "end": "Thickness", "target": 1.0}
        j = ThicknessEndJudge(1.0, 0.1, ThicknessJudgeParams(), start_t=svc.t, base=3.0, seq0=7)
        rt._thk_judge = rt._thk_last_judge = j
        _drive(rt, svc, [(8, 3.0)] + [(9 + i, 3.0 + 0.05 * (i + 1)) for i in range(10)])
        assert "set_thickness_state" in rec.names() and "on_thickness_done" not in rec.names()
        for _ in range(12):                              # 새 값 없음 12초
            svc.t += 1.0
            rt._on_thickness_status(svc.status())
        assert rec.names().count("on_thickness_fallback") == 1
        _drive(rt, svc, [(30, 3.95)])
        assert "on_thickness_resume" in rec.names()
        _drive(rt, svc, [(31, 4.0)])                                   # 목표 도달 즉시
        assert rec.names()[-1] == "on_thickness_done" and rt._thk_judge is None
        n = len(rec.calls)
        _drive(rt, svc, [(34, 5.0)])                     # 끝난 뒤에는 판정하지 않음
        assert len(rec.calls) == n
    asyncio.run(go())


def test_listener_exception_falls_back_once_with_traceback():
    async def go():
        svc = FakeSvc()
        rt, _pc, rec = _rt(svc)
        j = ThicknessEndJudge(10.0, 0.1, ThicknessJudgeParams(), start_t=svc.t, base=0.0, seq0=7)

        def boom(*a):
            raise RuntimeError("주입 예외")
        j.tick = boom
        rt._thk_judge = j
        rt._on_thickness_status(svc.status())
        rt._on_thickness_status(svc.status())
        assert rec.names() == ["on_thickness_fallback"]
        assert rec.calls[0][1][0] == pytest.approx(100.0)   # (10 − 0) ÷ 레시피 0.1
        tb = [m for s, m in rt.logs if "Traceback" in m]
        assert len(tb) == 1 and rt._thk_judge is None
    asyncio.run(go())


def test_judge_internal_exception_event_logs_traceback():
    async def go():
        svc = FakeSvc()
        rt, _pc, rec = _rt(svc)
        rt._thk_judge = ThicknessEndJudge(10.0, 0.1, ThicknessJudgeParams(), start_t=svc.t, base=0.0, seq0=7)
        svc.st.update(seq=8, thickness_nm="bad")
        rt._on_thickness_status(svc.status())
        assert rec.names()[0] == "on_thickness_fallback"
        assert any("traceback" in m for s, m in rt.logs)
    asyncio.run(go())


# ═══════════════════════ 기록 ═══════════════════════
def test_time_mode_record_uses_main_window_values():
    async def go():
        svc = FakeSvc()
        rt, _pc, _ = _rt(svc)
        rt._thk_run_started({"process_note": "x"})
        assert rt._thk_rec["mode"] == "Time"
        rt._proc_polling_active = False
        _drive(rt, svc, [(8, 1.0)])                      # 메인 공정 전 — 버림
        rt._proc_polling_active = True
        _drive(rt, svc, [(9, 2.0), (9, 2.5), (10, 3.0), (11, 4.0)], dt=10.0)
        v = rt._thk_values()
        assert v["end"] == "Time" and v["final_nm"] == pytest.approx(2.0) and v["avg_rate"] == pytest.approx(2.0 / 30.0)
        p = {}
        rt._thk_card_fields(p)
        assert p == {"thickness_end": "Time", "thickness_final": "2.0 nm"}
        rt2, _pc2, _ = _rt(None)                          # 두께 모니터 없음 → 카드·기록 그대로
        p = {}
        rt2._thk_card_fields(p)
        assert p == {}
    asyncio.run(go())


def test_thickness_end_limit_returns_text_and_records():
    """상한 — 경고 목록·구글챗에 넣지 않고(E848 실패 문구가 된다) '유효 x / 목표 y nm' 를 돌려준다. 기록은 종료 방식 '상한'."""
    async def go():
        svc = FakeSvc()
        rt, pc, _ = _rt(svc)
        sent = []
        rt.chat = types.SimpleNamespace(notify_text=lambda t: sent.append(t), flush=lambda: None)
        rt._thk_pre = {"chuck": "mid", "gun": "G1", "tooling": 110.0, "density": 2.7, "z_factor": 1.08}
        rt._thk_run_started({"thickness_mode": True, "thickness": 100.0})
        j = ThicknessEndJudge(100.0, 0.05, ThicknessJudgeParams(), start_t=0, base=0.0, seq0=0)
        j.eff, j.eff_t, j.jump_count = 80.0, 1600.0, 2
        rt._thk_last_judge = j
        assert pc._thickness_end("상한") == "유효 80.0 / 목표 100.0 nm"
        v = rt._thk_values()
        assert v["end"] == "상한" and v["final_nm"] == 80.0 and v["avg_rate"] == pytest.approx(0.05)
        assert v["tooling"] == 110.0 and v["jumps"] == 2
        assert rt._run_warnings == [] and sent == []                     # 공정 중 구글챗 없음
        p = {}
        rt._thk_card_fields(p)
        assert p == {"thickness_end": "상한", "thickness_final": "80.0 / 100 nm", "thickness_jumps": "2회(기록만)"}
    asyncio.run(go())


def test_data_logger_columns_and_header_migration():
    with tempfile.TemporaryDirectory() as tmp:
        f = Path(tmp) / "Ch2_log.csv"
        old = DataLogger(ch=2, csv_dir=Path(tmp))
        assert not any(c in old.header for c in THICKNESS_COLUMNS)               # 기본(두께 모니터 없음)은 그대로
        with open(f, "w", encoding="utf-8-sig", newline="") as fp:
            w = csv.writer(fp)
            w.writerow(old.header)
            w.writerow(["2026-10-01 10:00:00", "old"] + [""] * (len(old.header) - 2))
        dl = DataLogger(ch=2, csv_dir=Path(tmp), thickness_columns=True)
        assert dl.header[-len(THICKNESS_COLUMNS):] == THICKNESS_COLUMNS
        rows = list(csv.DictReader(open(f, encoding="utf-8-sig")))
        assert rows[0]["Process Name"] == "old" and rows[0]["Thickness: End Mode"] == ""   # 기존 행 보존
        dl.start_new_log_session({"process_note": "run", "use_ms": False})
        dl.set_thickness_record({"end": "Thickness", "target": 100.0, "final_nm": 100.04, "avg_rate": 0.0512,
                                 "tooling": 110.0, "density": 2.7, "z_factor": 1.08, "chuck": "mid", "gun": "G1",
                                 "jumps": 1, "fallback_reason": ""})
        dl.finalize_and_write_log(True)
        rows = list(csv.DictReader(open(f, encoding="utf-8-sig")))
        r = rows[-1]
        assert (r["Thickness: End Mode"], r["Thickness: Final nm"], r["Thickness: Avg Rate nm/s"]) == ("Thickness", "100.04", "0.0512")
        assert (r["Thickness: Tooling %"], r["Thickness: Chuck"], r["Thickness: Gun"], r["Thickness: Jump Count"]) == ("110", "mid", "G1", "1")
        dl.start_new_log_session({"process_note": "run2", "use_ms": False})        # 새 세션은 비움
        dl.finalize_and_write_log(True)
        r = list(csv.DictReader(open(f, encoding="utf-8-sig")))[-1]
        assert r["Thickness: End Mode"] == "" and r["Thickness: Final nm"] == ""


def test_end_card_shows_mode_and_final():
    from controller.chat_notifier import ChatNotifier

    class C(ChatNotifier):
        def __init__(self):
            super().__init__(webhook_url="")
            self.cards = []

        def _post_card(self, title, subtitle="", status="INFO", fields=None, urgent=False, **k):
            self.cards.append(dict(fields or {}))

        def _upsert_error_card(self):
            pass

        def flush(self):
            pass
    c = C()
    c.notify_process_finished_detail(True, {"process_name": "a", "ch": 2, "thickness_end": "Thickness",
                                            "thickness_final": "100.0 / 100 nm"})
    assert c.cards[-1]["종료 방식"] == "Thickness" and c.cards[-1]["최종 두께"] == "100.0 / 100 nm"
    c.notify_process_finished_detail(True, {"process_name": "b", "ch": 1})
    assert "종료 방식" not in c.cards[-1] and "최종 두께" not in c.cards[-1]


@pytest.mark.parametrize("thk_mode", [False, True])
def test_recording_follows_process_start_and_end(thk_mode):
    """연속 기록 CSV — 런타임이 공정 시작 이벤트에 켜고(공정명) 종료 처리에서 끈다. Time·Thickness 모드 모두."""
    async def go():
        svc = FakeSvc()
        rt, _pc, _rec = _rt(svc)
        p = {"process_name": "Run A", "thickness_mode": thk_mode, "thickness": 100.0}
        rt._thk_run_started(p)
        assert svc.recording == [(True, "Run A")]
        rt._thk_stop_recording()
        assert svc.recording[-1] == (False, "")
        rt2, _pc2, _ = _rt(None)                 # 두께 모니터 없음 — 아무것도 하지 않음
        rt2._thk_run_started(p)
        rt2._thk_stop_recording()
    asyncio.run(go())

if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
