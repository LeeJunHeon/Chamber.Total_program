"""ThicknessEngine + 실제 Stm100Driver + Stm100Sim(챔버 모델 thickness_A·rate_A_s·xtal_fail) + VirtualClock + Worker.

목표 도달 2번 연속 종료(CH2 운전값), 늦은 응답(주파수 값이 S 로 읽힘 — 15차 88455cf 현장 사례)이 튀어도 종료 안 함,
XTAL FAIL → 영구 시간 대체, 통신 끊김 → 시간 대체 → 복귀 뒤 두께로 종료, 상한 도달 = E848.
엔진 순서는 v1 runtime/chamber_runtime.py:1559-1632 + controller/process_controller.py:1488-1613 (THICKNESS_ZERO → 셔터 → WAIT).
"""
from v2.common.clock import VirtualClock
from v2.common.paramdef import defaults
from v2.common.transport import SimTransport
from v2.common.worker import Worker
from v2.ch12.drivers import stm100 as D
from v2.ch12.params import PARAMS
from v2.ch12.sim.chamber import Chamber
from v2.ch12.sim.stm100_sim import Stm100Sim, frame
from v2.ch12.thickness import ThicknessEngine

CFG = defaults(PARAMS)


class DepChamber(Chamber):
    """시험용 — 메인 셔터 열림 대신 rate_A(Å/s)로 곧장 쌓는다(공용 챔버 모델의 thickness_A·rate_A_s 를 그대로 씀)."""
    dep_A_s = 0.0

    def _step(self, dt):
        self.rate_A_s = self.dep_A_s
        self.thickness_A += self.dep_A_s * dt


class SpikeSim(Stm100Sim):
    """늦은 응답 흉내 — spike_at 시각 뒤 첫 S 응답 하나를 주파수 값(5922079 Å)으로 돌려준다(15차 88455cf 의 현장 사례)."""
    spike_at = None

    def handle(self, data, now):
        if self.spike_at is not None and now >= self.spike_at and self._decode(data) == "S":
            self.spike_at = None
            self.cmds.append("S")
            return frame("A5922079")
        return super().handle(data, now)


class SimRig:
    def __init__(self, tmp, sim_cls=Stm100Sim, cfg=None):
        self.c = VirtualClock(start_wall=1_760_000_000.0)
        self.c.register()
        self.cfg = dict(CFG, **(cfg or {}))
        self.ch = DepChamber("ch2")
        self.sim = sim_cls(self.ch)
        self.w = Worker("stm100", SimTransport(self.c, self.sim), writes_allowed=True, **D.worker_kwargs())
        self.d = D.Stm100Driver(self.c, self.w, self.cfg, True, chamber="ch2")
        self.logs, self.ends, self.zeros = [], [], []
        self.e = ThicknessEngine(self.c, self.cfg, "ch2", self.d, settings_dir=tmp,
                                 on_log=lambda s, m: self.logs.append((self.c.monotonic(), s, m)))
        self.w.start()
        self.d.start()
        self.c.wait_until(2.0)

    def msgs(self, src=None):
        return [m for _t, s, m in self.logs if src is None or s == src]

    def run_until(self, pred, limit_s):
        end = self.c.monotonic() + limit_s
        while self.c.monotonic() < end and not pred():
            self.c.wait_until(min(end, self.c.monotonic() + 0.25))
            self.e.on_row()
        return pred()

    def main(self, target_nm, rate_nm_s, expected, limit):
        """THICKNESS_ZERO → (셔터 열림 = 증착 시작) → THICKNESS_WAIT."""
        self.ch.thickness_A = 3500.0                              # 이전 공정 두께 — 영점으로 지운다
        self.e.zero_start(5.0, self.zeros.append)
        assert self.run_until(lambda: bool(self.zeros), 10.0) and self.zeros[0].base == 0.0
        self.ch.dep_A_s = rate_nm_s * 10.0
        self.e.wait_start(target_nm, expected, limit, self.ends.append)

    def done(self):
        self.d.stop()
        self.w.stop()


def test_sim_target_reached_twice_in_a_row_ends(tmp_path):
    r = SimRig(tmp_path)
    try:
        r.e.run_started({"thickness_mode": True, "thickness": 20.0, "dep_rate": 1.0})
        r.main(20.0, 1.0, 20.0, 320.0)
        assert r.run_until(lambda: bool(r.ends), 60.0)
        end = r.ends[0]
        assert end.end == "Thickness" and end.ok and 20.0 <= end.elapsed_s <= 21.5
        dev_nm = r.ch.thickness_A / 10.0 - r.sim.zero_A / 10.0
        assert 20.0 <= dev_nm <= 21.5
        done = [m for m in r.msgs("THICKNESS") if m.startswith("목표 도달")]
        assert len(done) == 1 and done[0].endswith("(연속 2회)")
        assert "B" in r.sim.cmds and r.e.record_values()["final_nm"] >= 20.0
    finally:
        r.done()


def test_sim_late_reply_spike_does_not_end(tmp_path):
    r = SimRig(tmp_path, sim_cls=SpikeSim)
    try:
        r.main(50.0, 1.0, 50.0, 400.0)
        t_spike = r.c.monotonic() + 10.0
        r.sim.spike_at = t_spike                                # 10 s 뒤 S 하나가 592207.9 nm 로 읽힘
        assert r.run_until(lambda: bool(r.ends), 120.0)
        end = r.ends[0]
        assert end.end == "Thickness" and end.elapsed_s >= 50.0      # 튄 값에서 끝나지 않고 실제 목표에서
        assert any("큰 튐" in m for m in r.msgs("THICKNESS")) and r.sim.spike_at is None
    finally:
        r.done()


def test_sim_xtal_fail_goes_to_permanent_time_fallback(tmp_path):
    r = SimRig(tmp_path)
    try:
        r.e.run_started({"thickness_mode": True, "thickness": 60.0, "dep_rate": 1.0})
        r.main(60.0, 1.0, 60.0, 360.0)
        r.c.wait_until(r.c.monotonic() + 20.0)
        r.ch.xtal_fail = True                                    # S·T 값 없음, M '!' — 드라이버가 M 을 바로 읽는다
        assert r.run_until(lambda: bool(r.ends), 120.0)
        end = r.ends[0]
        assert end.end == "시간대체" and end.ok and 55.0 <= end.elapsed_s <= 66.0
        assert any("XTAL FAIL(크리스탈 이상)" in m and "시간 대체로 전환" in m for m in r.msgs("THICKNESS"))
        assert r.e.run_warnings and r.e.run_warnings[-1].startswith("시간 대체로 종료(XTAL FAIL")
        assert r.e.snapshot_fields()["thickness_nm"] is not None             # 값 없음이어도 마지막 두께 유지
    finally:
        r.done()


def test_sim_link_lost_falls_back_then_recovers_and_ends(tmp_path):
    r = SimRig(tmp_path)
    try:
        r.main(80.0, 1.0, 80.0, 400.0)
        t0 = r.c.monotonic()
        r.sim.add_fault("disconnect", t0 + 15.0, t0 + 45.0)      # 30 s 끊김 — 10 s 뒤 시간 대체, 복귀하면 두께 판정
        assert r.run_until(lambda: any("두께 판정 복귀 — 시간 대체 타이머 해제" in m for m in r.msgs("Process")), 120.0)
        assert any(m.startswith("두께 → 시간 대체:") for m in r.msgs("Process"))
        assert r.run_until(lambda: bool(r.ends), 120.0)
        end = r.ends[0]
        assert end.end == "Thickness" and 80.0 <= end.elapsed_s <= 90.0
    finally:
        r.done()


def test_sim_limit_reached_is_e848(tmp_path):
    r = SimRig(tmp_path)
    try:
        r.main(100.0, 0.2, 20.0, 40.0)                            # 레시피 5 nm/s 로 적었지만 실제 0.2 nm/s
        assert r.run_until(lambda: bool(r.ends), 80.0)
        end = r.ends[0]
        assert (end.end, end.code) == ("상한", "E848") and 40.0 <= end.elapsed_s <= 41.0
        assert end.detail.startswith("두께 목표 미도달 — 최대 시간 초과: 유효 ")
    finally:
        r.done()
