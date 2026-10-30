"""챔버12 시뮬레이터 시험대 — 시뮬레이터 전부 + 공용 챔버 모델 둘 + SimTransport → 실행 프로그램과 같은 조립(Ch12System).

여기는 시뮬레이터와 전송만 만든다. 배선은 v2.ch12.system.Ch12System 하나(실행 프로그램 app 의 sim 모드도 make_sims·
sim_transports 를 같이 쓴다 — 배선이 두 벌이 되지 않게).
- 챔버 모델 CH1·CH2(sim.chamber.Chamber) — PLC 시뮬레이터 하나(PlcSim(ch1, ch2))가 두 모델에 밸브·게이트·셔터·전원(PLC D/A)을
  넣고, 챔버마다 MFC·IG·DC Pulse·RF Pulse·STM-100 시뮬레이터가 자기 챔버 모델을 읽고 쓴다. TSP 게이지(4001 — CH1 게이지와
  같은 포트)는 CH1 모델을 읽는 IG 시뮬레이터 하나 더. 카메라는 sim.camera_sim.SimCamera.
- MFC 시뮬레이터의 스케일은 설정(CHn_MFC_SCALE_FACTORS_1..3)과 같게.
- 기록 경로(local_paths): v1 기록 출력·1초 기록·폴백이 모두 out_dir 아래로(시험이 C:\\·G:\\·NAS 에 쓰지 않게).
- OES·RGA 링크: 시험 하니스는 기본으로 만들지 않는다(links=False → 1초 행 off — 실제 하위 프로세스는 실행 프로그램만).
- 시뮬레이터가 받은 요청 기록은 길이 제한(retention.sim_log, 없으면 simdev.SIM_LOG_KEEP) — 오래 돌려도 늘지 않게.
"""
from __future__ import annotations

import collections
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from v2.common.paramdef import defaults
from v2.common.simdev import SIM_LOG_KEEP
from v2.common.transport import SimTransport
from v2.ch12.drivers import tsp as T
from v2.ch12.sim.camera_sim import SimCamera
from v2.ch12.sim.chamber import Chamber
from v2.ch12.sim.dcpulse_sim import DcPulseSim
from v2.ch12.sim.ig_sim import IgSim
from v2.ch12.sim.mfc_sim import MfcSim
from v2.ch12.sim.plc_sim import PlcSim
from v2.ch12.sim.rfpulse_sim import RfPulseSim
from v2.ch12.sim.stm100_sim import Stm100Sim
from v2.ch12.sim.tsp_sim import TspSim
from v2.ch12.system import CHAMBERS, TEST_RETENTION, Ch12System, Retention, TransportSet, new_store

SIM_LOGS = ("received", "frames", "cmds", "rx_log")


def base_cfg() -> Dict[str, Any]:
    from v2.ch12.params import PARAMS
    return defaults(PARAMS)


def local_paths(root: Path) -> Dict[str, str]:
    """기록·폴백 경로를 root 아래로(install 키 — 실행 프로그램 시험은 local_override.json 에 같은 값)."""
    r = Path(root)
    logs = r / "logs"
    return {"LOCAL_DATA_DIR": str(r / "data"), "LOG_ROOT_DIR": str(logs), "LOCAL_FALLBACK_ROOT": str(r / "fallback"),
            "OUTPUT_LOCAL_FIRST_DIR": str(logs / "LocalFirst"), "PLC_COIL_LOG_NAS_DIR": str(logs / "CH1&2_PLC"),
            "PLC_COIL_LOG_LOCAL_DIR": str(r / "fallback" / "PLC_Coil"),
            "CH1_THICKNESS_LOG_DIR": str(logs / "Thickness"), "CH2_THICKNESS_LOG_DIR": str(logs / "Thickness"),
            "CAMERA_LOG_ROOT": str(r / "camera"), "OES_LOCAL_DIR": str(r / "oes"),
            "HOST_LOG_LOCAL_DIR": str(r / "robot"), "HOST_LOG_NAS_DIR": str(r / "robot_nas"),
            "SPUTTER_CALIB_DB_DIR": str(r / "calib"), "GDRIVE_LOG_DIR": str(r / "gdrive"),
            "CH1_RGA_CSV_PATH": str(r / "rga" / "Ch.1" / "RGA_spectrums.csv"),
            "CH2_RGA_CSV_PATH": str(r / "rga" / "Ch.2" / "RGA_spectrums.csv"),
            "RF_READER_STATE_DIR": str(r / "camera" / "_state"), "RF_READER_CSV_DIR": str(r / "camera" / "_csv"),
            "LOG_SYNC_STATE_DIR": str(r / "_sync_state"), "ROBOT_CACHE_PATH": str(r / "state" / "robot_last_replies.json")}


def make_sims(start: float = 0.0, cfg: Optional[Dict[str, Any]] = None,
              log_keep: Optional[int] = None) -> Tuple[Dict[str, Chamber], Dict[str, Any]]:
    """(챔버 모델 {ch1, ch2}, {장비 이름: 시뮬레이터}). 장비 이름은 1초 행 블록 이름과 같다(plc·ch1.mfc·…·tsp·tsp.ig).
    start = 시계의 지금(실제 시계는 부팅 뒤 초 — 0 에서 시작하면 첫 응답 때 그만큼을 0.1 s 씩 따라잡느라 멈춘 것처럼 된다)."""
    c = cfg or base_cfg()
    ch = {n: Chamber(n, start) for n in CHAMBERS}
    sims: Dict[str, Any] = {"plc": PlcSim(ch["ch1"], ch["ch2"], start=start), "tsp": TspSim(),
                            "tsp.ig": IgSim(ch["ch1"], name="tsp.ig")}
    for n in CHAMBERS:
        scale = tuple(float(c.get(f"{n.upper()}_MFC_SCALE_FACTORS_{i}", 1.0)) for i in (1, 2, 3))
        sims[f"{n}.mfc"] = MfcSim(ch[n], scale=scale, name=f"{n}.mfc")
        sims[f"{n}.ig"] = IgSim(ch[n], name=f"{n}.ig")
        sims[f"{n}.dcpulse"] = DcPulseSim(ch[n])
        sims[f"{n}.rfpulse"] = RfPulseSim(ch[n], control=2)
        sims[f"{n}.stm100"] = Stm100Sim(ch[n])
    keep = SIM_LOG_KEEP if log_keep is None else log_keep
    for x in sims.values():
        for name in SIM_LOGS:
            q = getattr(x, name, None)
            if q is not None and (not isinstance(q, collections.deque) or q.maxlen != keep):
                setattr(x, name, collections.deque(q, maxlen=keep))
    return ch, sims


def sim_transports(clock, sims: Dict[str, Any]) -> TransportSet:
    fixed = {k: SimTransport(clock, sims[k]) for k in ("plc", "ch1.mfc", "ch2.mfc", "ch1.ig", "ch2.ig",
                                                     "ch1.stm100", "ch2.stm100")}
    fixed["tsp"] = T.make_sim_transport(clock, sims["tsp"])
    return TransportSet(fixed, lambda kind, ch: SimTransport(clock, sims[f"{ch}.{kind}"]),
                        lambda: SimTransport(clock, sims["tsp.ig"]))


class Ch12Rig(Ch12System):
    """시험 하니스 — 시뮬레이터를 만들어 실행 프로그램과 같은 조립(Ch12System)에 넣는다.
    cfg = 기본값 위에 덮을 값(기록 경로는 out_dir 아래가 먼저 깔린다). 설정 저장소는 out_dir/config(파일 없으면 기본값)."""

    def __init__(self, clock, out_dir: Path, cfg: Optional[Dict[str, Any]] = None, mode: str = "sim",
                 retention: Retention = TEST_RETENTION, links: bool = False, scenario_hook=None,
                 camera: Optional[SimCamera] = None) -> None:
        out = Path(out_dir)
        full = dict(base_cfg(), **local_paths(out), **(cfg or {}))
        self.chambers, self.sims = make_sims(clock.monotonic(), full, log_keep=retention.sim_log)
        self.sim_camera = camera or SimCamera()
        super().__init__(clock, full, Path(full["LOCAL_DATA_DIR"]), sim_transports(clock, self.sims), mode=mode,
                         store=new_store(out / "config"), retention=retention, links=links,
                         camera_backend=self.sim_camera, scenario_hook=scenario_hook)


class Ch12FullRig(Ch12Rig):
    """실행 프로그램과 같은 엔진 조립(assembly.build)까지 붙인 시험대 — 엔진 전부·안전·명령 처리(core)·로봇(RobotOps·RobotApi
    127.0.0.1 임의 포트)·허브·설정 적용. 기록·설정 폴더는 out_dir 아래(엑셀 메인 사본도 — v1 G: 경로에 쓰지 않게).
    robot_api=False 면 내부 명령 API 서버를 띄우지 않는다(로봇 명령은 robot_ops·dispatch_robot 으로)."""

    def __init__(self, clock, out_dir: Path, cfg: Optional[Dict[str, Any]] = None, mode: str = "sim",
                 retention: Retention = TEST_RETENTION, links: bool = False, scenario_hook=None,
                 camera: Optional[SimCamera] = None, robot_api: bool = True, schedules: bool = True) -> None:
        super().__init__(clock, out_dir, cfg=cfg, mode=mode, retention=retention, links=links,
                         scenario_hook=scenario_hook, camera=camera)
        from v2.ch12.assembly import build
        out = Path(out_dir)
        self.asm = build(self, settings_dir=out / "settings", excel_main_dir=out / "xl_main", robot_api=robot_api,
                         robot_port=0, schedules=schedules)
