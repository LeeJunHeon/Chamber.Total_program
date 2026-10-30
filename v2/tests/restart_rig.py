"""재시작 시험 공통 — '제어 프로그램을 죽였다 살리기'를 가상 시계에서(장비 시뮬레이터 상태는 그대로, 저널만 남김).

- boot(tmp, sims=None, t0=…): 실행 프로그램과 같은 조립(Ch12System + assembly.build — sim.rig.Ch12FullRig 와 같음)에
  저널·재시작 판단(resume.attach)을 붙여 시작한다. sims 를 주면 그 시뮬레이터(챔버 모델·PLC 래더 상태 그대로)에 새 전송으로 붙는다.
  새 가상 시계는 t0(= 죽인 시각 + 공백)에서 시작 — 벽시계·단조 시계가 이어진다(같은 부팅). wall0 를 바꾸면 '재부팅'.
- kill(rig): 저널을 지금 상태로 한 번 쓰고(죽기 직전의 1초 저널) 쓰기 스레드를 버린 뒤 시계를 닫는다 — 모든 worker·엔진 스레드가
  그 자리에서 끝난다(장비로 아무것도 보내지 않음). 남은 파일 쓰기 스레드만 정리.
- drive(rig, until, max_ticks, hooks): tick 을 돌린다(죽인 시계면 ClockClosed 를 삼키고 끝).
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional

from v2.common.clock import ClockClosed, VirtualClock
from v2.ch12 import resume as RS
from v2.ch12.sim.camera_sim import SimCamera
from v2.ch12.sim.rig import base_cfg, local_paths, make_sims, sim_transports
from v2.ch12.system import TEST_RETENTION, Ch12System, new_store

W0 = 1_760_000_000.0
CFG = {"CH1_SUPPORTS_RFPULSE": True, "CH2_SUPPORTS_DCPULSE": True, "CH2_SUPPORTS_RF_CONT": True,
       "CAMERA_MIN_FREE_GB": 0.0}


class RestartRig(Ch12System):
    """Ch12FullRig 와 같은 조립 — 시뮬레이터를 바깥에서 받을 수 있다(재시작)."""

    def __init__(self, clock, out_dir: Path, cfg: Optional[Dict[str, Any]] = None, sims=None, chambers=None,
                 robot_api: bool = False) -> None:
        out = Path(out_dir)
        full = dict(base_cfg(), **local_paths(out), **(cfg or {}))
        if sims is None:
            chambers, sims = make_sims(clock.monotonic(), full, log_keep=TEST_RETENTION.sim_log)
            sims["ch1.mfc"].sp[2] = 3.0 * 0.1            # SP3(CH1) — 작업 압력 3 mTorr(proc_rig 와 같게)
            sims["ch2.mfc"].sp[3] = 5.0 * 0.1            # SP4(CH2)
        self.chambers, self.sims = chambers, sims
        self.sim_camera = SimCamera()
        super().__init__(clock, full, Path(full["LOCAL_DATA_DIR"]), sim_transports(clock, sims), mode="sim",
                         store=new_store(out / "config"), retention=TEST_RETENTION, links=False,
                         camera_backend=self.sim_camera)
        from v2.ch12.assembly import build
        self.asm = build(self, settings_dir=out / "settings", excel_main_dir=out / "xl_main", robot_api=robot_api,
                         robot_port=0, schedules=False)
        self.eng = {ch: self.engine(f"proc.{ch}") for ch in ("ch1", "ch2")}


def boot(tmp, sims=None, chambers=None, t0: float = 0.0, wall0: float = W0, cfg: Optional[dict] = None,
         robot_api: bool = False, start: bool = True, before_start: Optional[Callable[[Any], None]] = None) -> RestartRig:
    c = VirtualClock(start_wall=wall0)
    if t0:
        c.advance(t0)
    c.register()
    rig = RestartRig(c, Path(tmp), cfg=dict(CFG, **(cfg or {})), sims=sims, chambers=chambers, robot_api=robot_api)
    RS.attach(rig)
    if before_start is not None:
        before_start(rig)
    if start:
        assert rig.start(ready_timeout_s=15.0)
        rig.resumer.start()
    return rig


def drive(rig, until: Callable[[], bool], max_ticks: int = 600, hooks: Iterable[Callable] = ()) -> int:
    for h in hooks:
        rig.loop.tick_hooks.append(h)
    n = 0
    try:
        while n < max_ticks:
            rig.loop.run(1)
            n += 1
            if until():
                break
    except ClockClosed:
        pass
    return n


def kill(rig) -> float:
    """제어 프로그램이 죽음 — 마지막 1초 저널을 쓰고 시계를 닫는다. 죽은 시각(단조)을 돌려준다."""
    t = rig.clock.monotonic()
    rig.journal.flush()
    rig.journal.abandon()
    rig.clock.close()
    return t


def cleanup(rig) -> None:
    """죽인(또는 끝난) 조립의 남은 스레드(파일 쓰기·로봇 API·재고 DB 가짜 서버 등) 정리 — 시계가 닫혀 worker 는 이미 끝났으므로
    엔진 close 가 보내는 요청은 장비에 닿지 않는다(죽은 프로그램처럼)."""
    try:
        rig.journal.abandon()
    except BaseException:
        pass
    for e in list(rig.engines.values()):
        f = getattr(e, "close", None)
        if f is not None:
            try:
                f()
            except BaseException:
                pass
    for f in (rig.recorder.stop, rig.outputs.stop, rig.camera.stop):
        try:
            f()
        except BaseException:
            pass


def finish(rig) -> None:
    """정상 끝(시험 마지막) — 조립 정지·시계 닫기."""
    try:
        rig.journal.stop()
        rig.stop()
    finally:
        rig.clock.close()


def reboot(tmp, old, gap_s: float, **kw) -> RestartRig:
    """죽인 조립의 시뮬레이터에 새 조립을 붙여 gap_s 뒤 시작."""
    t = old.clock.monotonic() + gap_s
    cleanup(old)
    return boot(tmp, sims=old.sims, chambers=old.chambers, t0=t, **kw)


def ends(e) -> List[tuple]:
    return [(x.data.get("result"), x.code) for x in e.events if x.kind == "run_end"]


def resumed(e) -> List[dict]:
    return [dict(x.data, code=x.code, detail=x.detail) for x in e.events if x.kind == "resumed"]


def block_hook(e, btype: str, fn: Callable[[], None], after_s: float = 0.0) -> Callable:
    """그 블록에 들어간 뒤 after_s 가 지난 첫 tick 에 fn() 한 번."""
    done = [False]

    def hook(t):
        inst = e.cur_inst
        bt = inst.type if inst else None
        if done[0] or bt != btype or e.block_t0 is None:
            return
        if e.clock.monotonic() - e.block_t0 >= after_s:
            done[0] = True
            fn()
    return hook
