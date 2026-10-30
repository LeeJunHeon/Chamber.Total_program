"""챔버12 공정 스텝 — v1 controller/process_controller.py 의 스텝(ProcessStep)과 스텝 순서 함수를 다시 쓴 것.

v1 근거(기준 커밋 66efc7b)
- ProcessStep·ActionType·값 검사: process_controller.py:303-395
- 공통 정보(_get_common_process_info): 2125-2151
- 공정 순서(_create_process_sequence): 2153-2735
- 정상 종료 절차(_create_shutdown_sequence): 2737-2832 — 끝날 때와 STOP(_start_normal_shutdown 1926-1977) 이 같은 함수
- 긴급 종료 절차(_create_emergency_shutdown_sequence): 2834-2930
- 순서 검사(validate_process_sequence): 2987-3018, 기간 해석(_parse_duration_seconds): 3178-3198
- Thickness 모드 중간 변경 충돌(E844, thickness_mid_change_conflict): 1083-1108
- 두께 판정 상수(load_judge_params): lib/thickness_config.py:85-117, expected_seconds·max_time_s: controller/thickness_process.py:70-78

구성
- 스텝 조각 함수(gas_prep_steps·power_on_steps·shutdown_steps …)를 blocks.py 의 블록 펼치기가 그대로 쓴다.
- build_sequence(params, chamber, cfg)(본체 process_sequence.py)는 v1 _create_process_sequence 와 같은 순서로 조각을 잇는다
  (블록 없이 — 비교 기준).
  blocks.template_blocks → blocks.expand 가 같은 스텝을 내는지 골든 시험이 확인한다.
- to_golden(): tests/data/process_sequences_golden.json 과 같은 8개 필드 [action, message, value, repr(params),
  duration, parallel, polling, no_wait] — v1 tests/test_process_heater_steps.py:_rows 와 같다.
- 스텝을 만들 때 v1 이 남기던 로그(_emit_log("Process", …))는 log 목록에 (src, msg) 로 같은 순서로 넣는다.

설정 키(cfg) — v1 은 채널 모듈(config_ch1/ch2)에서 읽는다 → v2 는 채널 키 "CH1_"/"CH2_" + v1 이름, 없으면 같은 이름의 공통 키
(정의는 params/core.py·core_heater.py·core_thickness.py·rfpulse.py·stm100.py — 다른 담당). PROCESS_GAS_INFO 는 v2 에서 코드 상수
(params/core.py DROPPED) — 키가 없으면 GAS_INFO_DEFAULT.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional

# ───────────── 스텝 동작(엔진이 알아야 할 것) ─────────────
# action → (장비 호출 — v1 _send_and_collect_tokens 1336-1487 이 부르던 콜백, 완료 판정)
ACTIONS: Dict[str, str] = {
    "IG_CMD": "IG 베이스 압력 대기 ig_wait(value=목표 Torr) — IG_OK 토큰(on_ig_ok)",
    "RGA_SCAN": "RGA 스캔 rga_scan() — 스캔·그래프 완료(on_rga_finished)",
    "MFC_CMD": "MFC 명령 send_mfc(params[0], params[1]) — FLOW_OFF·VALVE_OPEN·PS_ZEROING·MFC_ZEROING·FLOW_SET·FLOW_ON·"
               "SP1_SET·SP1_ON·SP2_SET·SP2_ON·SP3_ON·SP4_ON·WAIT_PRESSURE, MFC 확인(on_mfc_confirmed(cmd))",
    "PLC_CMD": "PLC 코일 send_plc(name, on, ch) — params=(name, on[, ch]), 이름 정규화(norm_plc_name), PLC 확인(on_plc_confirmed)",
    "DELAY": "대기 duration ms(카운트다운, polling=True 면 장비 폴링 켬)",
    "DC_POWER_SET": "DC 연속 send_dc_power(value W) — 목표 도달(on_dc_target_reached)",
    "DC2_POWER_SET": "DC2 연속 send_dc_power2(value W) — 목표 도달(on_dc2_target_reached)",
    "RF_POWER_SET": "RF 연속 send_rf_power(value W) — 목표 도달(on_rf_target_reached)",
    "DC_POWER_STOP": "DC 연속 끄기 stop_dc_power() — OFF 확인(on_dc_off_finished)",
    "DC2_POWER_STOP": "DC2 끄기 stop_dc_power2() — OFF 확인(on_dc2_off_finished)",
    "RF_POWER_STOP": "RF 연속 끄기 stop_rf_power() — 램프다운·0 W(on_rf_off_finished, 한도 rf_power_stop_timeout_ms)",
    "OES_RUN": "OES 측정 oes_run(params[0] 초, params[1] ms) — no_wait(기다리지 않음)",
    "DC_PULSE_START": "DC Pulse 설정·ON start_dc_pulse(value W, freq kHz|None, off µs|'DC'|None) — 목표 도달",
    "DC_PULSE_SET": "DC Pulse 출력 유지 + 목표 변경 set_dc_pulse_power(value W) — REF_POWER 확인(on_dc_pulse_set_confirmed)",
    "DC_PULSE_STOP": "DC Pulse 끄기 stop_dc_pulse() — OFF 확인",
    "RF_PULSE_START": "RF Pulse 설정·ON start_rf_pulse(value W, freq Hz|None, duty %|None) — 목표 도달",
    "RF_PULSE_SET": "RF Pulse 출력 유지 + 목표 변경 set_rf_pulse_power(value W) — no_wait",
    "RF_PULSE_STOP": "RF Pulse 끄기 stop_rf_pulse() — OFF 확인",
    "HEATER_SET": "CH2 히터 목표·램프 heater_set(value °C, params[0] °C/min|None) — on_heater_set_ok",
    "HEATER_WAIT": "히터 도달·안정 대기 heater_wait(value[, params[0] SOAK 초]) — on_heater_ready",
    "HEATER_HOLD": "히터 유지 모드 측정 대기 heater_hold() — on_heater_hold_ok",
    "THICKNESS_ZERO": "두께 영점(B)·확인 thickness_zero(params[0] 확인 최대 초) — on_thickness_zero_done",
    "THICKNESS_WAIT": "두께 도달 대기 thickness_start(value nm, params[0] 예상 초, params[1] 상한 초) — 상한·시간 대체 타이머",
    "ROW_DELAY": "v2: v1 레시피 delay 행(러너 대기, 컨트롤러 없음 — chamber_runtime.py:5127-5145). duration ms = int(초)×1000, "
                 "params=(label, 표시 이름) — 1초마다 '지연 대기 중: {label} · 남은 시간 HH:MM:SS'",
}

GAS_INFO_DEFAULT = {"AR": {"channel": 1}, "O2": {"channel": 2}, "N2": {"channel": 3}}   # config_common.py:770-774
ETA_CONFIRM_PREFIXES = ("Shutter Delay", "메인 공정 진행")                              # process_controller.py:1717


class StepError(Exception):
    """v1 AppError(code, detail) 자리 — 스텝 형식 오류(E214~E222·E847)·Thickness 충돌(E844)."""

    def __init__(self, code: str, detail: str):
        super().__init__(f"{code}: {detail}")
        self.code = code
        self.detail = detail


_VALUE_REQUIRED = ("DC_POWER_SET", "DC2_POWER_SET", "RF_POWER_SET", "IG_CMD", "DC_PULSE_SET", "RF_PULSE_SET",
                   "HEATER_SET", "HEATER_WAIT", "THICKNESS_WAIT")


@dataclass
class ProcessStep:
    """v1 ProcessStep 8개 필드 + 블록 정보(block·ref·holding — 골든 비교 단위 아님)."""
    action: str
    message: str
    value: Optional[float] = None
    params: Optional[tuple] = None
    duration: Optional[int] = None          # ms
    parallel: bool = False
    polling: bool = False
    no_wait: bool = False
    block: int = -1                         # blocks.BlockInst 순번
    ref: str = ""                           # 공정 중 수정(PROCESS_ADJUST) 항목 키
    holding: bool = False                   # 상태를 유지하며 기다리는 스텝(재시작 뒤 이어가기 후보)

    def __post_init__(self):                # process_controller.py:343-395 그대로
        a = self.action
        if a not in ACTIONS:
            raise StepError("E216", f"모르는 action {a!r}")
        if a == "DELAY":
            if self.duration is None:
                raise StepError("E214", "DELAY 액션은 duration이 필요합니다.")
            if self.parallel:
                raise StepError("E215", "DELAY는 병렬 블록에 포함할 수 없습니다.")
        if a in _VALUE_REQUIRED and self.value is None:
            raise ValueError(f"{a} 액션은 value가 필요합니다.")
        if a == "PLC_CMD" and (not self.params or len(self.params) not in (2, 3)):
            raise StepError("E216", "PLC_CMD params는 (name:str, on:any[, ch:int]) 형태여야 합니다.")
        if a == "MFC_CMD" and (not self.params or len(self.params) != 2 or not isinstance(self.params[1], dict)):
            raise StepError("E217", "MFC_CMD params는 (cmd:str, args:dict) 형태여야 합니다.")
        if a == "THICKNESS_ZERO" and (not self.params or len(self.params) != 1):
            raise StepError("E847", "THICKNESS_ZERO params는 (확인 최대 초,) 형태여야 합니다.")
        if a == "THICKNESS_WAIT" and (not self.params or len(self.params) != 2):
            raise StepError("E847", "THICKNESS_WAIT params는 (예상 초, 상한 초) 형태여야 합니다.")
        if a == "OES_RUN" and (not self.params or len(self.params) != 2):
            raise StepError("E218", "OES_RUN params는 (process_time_sec:float, integration_ms:int) 형태여야 합니다.")
        if a == "DC_PULSE_START":
            if self.value is None:
                raise StepError("E219", "DC_PULSE_START에는 value(타깃 파워)가 필요합니다.")
            if not self.params or len(self.params) != 2:
                raise StepError("E221", "DC_PULSE_START params는 (freq_kHz|None, off_time_us|'DC'|None) 형태여야 합니다.")
        if a == "RF_PULSE_START":
            if self.value is None:
                raise StepError("E220", "RF_PULSE_START에는 value(타깃 파워)가 필요합니다.")
            if not self.params or len(self.params) != 2:
                raise StepError("E222", "RF_PULSE_START params는 (freqkHz|None, duty%|None) 형태로 받고 내부에서 Hz로 변환합니다.")

    def golden(self) -> list:
        """v1 tests/test_process_heater_steps.py:_rows 와 같은 8개 필드."""
        return [self.action, self.message, self.value, repr(self.params), self.duration, self.parallel, self.polling,
                self.no_wait]


S = ProcessStep


def to_golden(steps: List[ProcessStep]) -> List[list]:
    return [s.golden() for s in steps]


# ───────────── 설정·지원 ─────────────
def _ck(chamber: int, key: str) -> str:
    return f"CH{int(chamber)}_{key}"


def _cget(cfg: Mapping[str, Any], chamber: int, key: str, default: Any) -> Any:
    """채널 키(CHn_KEY) → 공통 키(KEY) → v1 코드 기본값(default) — v2 파라미터 규칙(params/core.py 머리: 채널 키가 없는
    채널은 같은 이름의 공통 키). v1 getattr(self._cfg, KEY, default) 자리. 목록·사전 값은 JSON 글자로 와도 된다(JSON_LIST)."""
    v = cfg.get(_ck(chamber, key))
    if v is None:
        v = cfg.get(key)
    if v is None:
        return default
    if isinstance(default, (list, dict)) and isinstance(v, str):
        v = json.loads(v)
    return v


@dataclass(frozen=True)
class StepCfg:
    chamber: int
    gas_info: Dict[str, Dict[str, int]]
    gun_shutters: tuple
    sp_on_cmd: str
    sp_label: str
    sp_index: int
    base_pressure: float
    integration_ms: int
    pressure_wait_timeout_s: float
    rf_pulse_post_on_delay_ms: int
    boost_target: float
    power_select_settle_ms: int
    zero_confirm_s: float
    zero_confirm_nm: float
    max_time_ratio: float
    max_time_margin_s: float


def _judge(cfg: Mapping[str, Any], chamber: int, key: str, default: float, lo: float, hi: float, warns: list) -> float:
    """lib/thickness_config.py:load_judge_params 의 _num(형식 오류 → 기본값, 범위 밖 → 클램프)."""
    raw = _cget(cfg, chamber, key, default)
    try:
        if isinstance(raw, bool):
            raise TypeError
        v = float(raw)
    except Exception:
        warns.append(f"{key}={raw!r} 형식 오류 → 기본값 {default!r}")
        return float(default)
    if not (lo <= v <= hi):
        nv = min(max(v, lo), hi)
        warns.append(f"{key}={v:g} 범위({lo:g}~{hi:g}) 밖 → {nv:g}")
        return nv
    return v


def step_cfg(cfg: Mapping[str, Any], chamber: int) -> StepCfg:
    ch = int(chamber)
    w: list = []
    return StepCfg(
        chamber=ch,
        gas_info=dict(_cget(cfg, ch, "PROCESS_GAS_INFO", GAS_INFO_DEFAULT)),
        gun_shutters=tuple(_cget(cfg, ch, "PROCESS_GUN_SHUTTERS", [] if ch == 1 else ["G1", "G2", "G3"])),
        sp_on_cmd=_cget(cfg, ch, "PROCESS_PRESSURE_CONTROL_SP_ON_CMD", "SP3_ON" if ch == 1 else "SP4_ON"),
        sp_label=_cget(cfg, ch, "PROCESS_PRESSURE_CONTROL_SP_LABEL", "SP3" if ch == 1 else "SP4"),
        sp_index=int(_cget(cfg, ch, "PROCESS_PRESSURE_CONTROL_SP_INDEX", 3 if ch == 1 else 4)),
        base_pressure=float(_cget(cfg, ch, "PROCESS_DEFAULT_BASE_PRESSURE", 1e-5)),
        integration_ms=int(_cget(cfg, ch, "PROCESS_DEFAULT_OES_INTEGRATION_MS", 60)),
        pressure_wait_timeout_s=float(_cget(cfg, ch, "PC_PRESSURE_WAIT_TIMEOUT_S", 180.0)),
        rf_pulse_post_on_delay_ms=int(_cget(cfg, ch, "PC_RF_PULSE_POST_ON_DELAY_MS", 20_000)),
        boost_target=float(_cget(cfg, ch, "PC_WORKING_PRESSURE_BOOST_TARGET", 10.0)),
        power_select_settle_ms=int(_cget(cfg, ch, "PC_POWER_SELECT_SETTLE_MS", 7_000)),
        zero_confirm_s=_judge(cfg, ch, "THICKNESS_ZERO_CONFIRM_S", 5.0, 1.0, 60.0, w),
        zero_confirm_nm=_judge(cfg, ch, "THICKNESS_ZERO_CONFIRM_NM", 2.0, 0.1, 100.0, w),
        max_time_ratio=_judge(cfg, ch, "THICKNESS_MAX_TIME_RATIO", 2.0, 1.0, 10.0, w),
        max_time_margin_s=_judge(cfg, ch, "THICKNESS_MAX_TIME_MARGIN_S", 300.0, 0.0, 7200.0, w),
    )


@dataclass(frozen=True)
class Caps:
    """챔버 지원 플래그 — v1 ChamberRuntime.__init__ 512-536 + main.py:479(CH2 RF 연속 강제) 결과.
    set_dcp·set_rfp: 중간 변경 콜백 주입 여부(런타임은 늘 주입 — chamber_runtime.py:1650·1654)."""
    dc: bool
    dc2: bool
    rf: bool
    dcp: bool
    rfp: bool
    heater: bool = False
    thickness: bool = False
    set_dcp: bool = True
    set_rfp: bool = True


def _b(v: Any) -> Optional[bool]:
    return None if v is None else bool(v)


def caps_from_cfg(cfg: Mapping[str, Any], chamber: int) -> Caps:
    """v1 규칙: SUPPORTS_DC_CONT→SUPPORTS_DC(없으면 ch==2), SUPPORTS_DC_CONT2→SUPPORTS_DC2(없으면 False, CH2 전용·히터와 배타
    — _dc2_allowed 389-392), SUPPORTS_RF_CONT(없으면 False, CH2 는 main.py:479 가 True 로 강제),
    SUPPORTS_DC_PULSE→SUPPORTS_DCPULSE(없으면 ch==1), SUPPORTS_RFPULSE→SUPPORTS_RF_PULSE(없으면 ch==2)."""
    ch = int(chamber)

    def first(*names):                    # v1 _cfg_bool: 이름 순서대로, 채널 값(없으면 공통 값)이 있는 첫 이름
        for n in names:
            v = _b(_cget(cfg, ch, n, None))
            if v is not None:
                return v
        return None

    def pick(v, d):
        return d if v is None else v

    heater = bool(first("SUPPORTS_HEATER")) and ch == 2
    return Caps(dc=pick(first("SUPPORTS_DC_CONT", "SUPPORTS_DC"), ch == 2),
                dc2=bool(pick(first("SUPPORTS_DC_CONT2", "SUPPORTS_DC2"), False)) and ch == 2 and not bool(first("SUPPORTS_HEATER")),
                rf=True if ch == 2 else pick(first("SUPPORTS_RF_CONT"), False),
                dcp=pick(first("SUPPORTS_DC_PULSE", "SUPPORTS_DCPULSE"), ch == 1),
                rfp=pick(first("SUPPORTS_RFPULSE", "SUPPORTS_RF_PULSE"), ch == 2),
                heater=heater, thickness=bool(first("SUPPORTS_THICKNESS")))


# ───────────── 기간 해석(컨트롤러 규칙) ─────────────
_NUM_RE = re.compile(r"\d+(?:\.\d+)?")
_HMS_RE = re.compile(r"(?:(\d+(?:\.\d+)?)h)?(?:(\d+(?:\.\d+)?)m)?(?:(\d+(?:\.\d+)?)s)?")


def parse_duration_pc(s: Any) -> float:
    """process_controller._parse_duration_seconds(3178-3198): 숫자만 = 초, h/m/s 조합(소수, 전체 일치), 그 밖 0."""
    s = (s or "").strip().lower()
    if not s:
        return 0.0
    if _NUM_RE.fullmatch(s):
        return float(s)
    m = _HMS_RE.fullmatch(s)
    if not m:
        return 0.0
    return float(float(m.group(1) or 0.0) * 3600.0 + float(m.group(2) or 0.0) * 60.0 + float(m.group(3) or 0.0))


def norm_plc_name(name: str) -> str:
    """process_controller._norm_plc_name(3163-3176)."""
    nm = (name or "").strip().upper().replace(" ", "")
    aliases = {"ARGON": "AR", "AR2": "AR", "AR_2": "AR", "MAINVALVE": "MV", "MAIN_VALVE": "MV",
               "MAINSHUTTER": "MS", "MAIN_SHUTTER": "MS", "AR": "AR", "O2": "O2", "N2": "N2", "MAIN": "MAIN",
               "MV": "MV", "MS": "MS", "G1": "G1", "G2": "G2", "G3": "G3"}
    return aliases.get(nm, nm)


# ───────────── 공통 정보·쓰는 장비 ─────────────
def common_info(params: Mapping[str, Any], caps: Caps) -> Dict[str, Any]:
    """_get_common_process_info(2125-2151). float() 는 v1 처럼 값이 이상하면 예외."""
    req_dc = bool(params.get("use_dc_power", False)) and float(params.get("dc_power", 0)) > 0
    req_dc2 = bool(params.get("use_dc_power2", False)) and float(params.get("dc_power2", 0)) > 0
    req_rf = bool(params.get("use_rf_power", False)) and float(params.get("rf_power", 0)) > 0
    req_dcp = bool(params.get("use_dc_pulse", False)) and float(params.get("dc_pulse_power", 0)) > 0
    req_rfp = bool(params.get("use_rf_pulse", False)) and float(params.get("rf_pulse_power", 0)) > 0
    return {"use_ms": bool(params.get("use_ms", False)), "use_dc": req_dc and caps.dc, "use_dc2": req_dc2 and caps.dc2,
            "use_rf": req_rf and caps.rf, "use_dc_pulse": req_dcp and caps.dcp, "use_rf_pulse": req_rfp and caps.rfp,
            "req_dc": req_dc, "req_dc2": req_dc2, "req_rf": req_rf, "req_dcp": req_dcp, "req_rfp": req_rfp}


@dataclass
class Uses:
    """종료 절차가 끄는 것 — v1 은 행 params 로, 블록 레시피는 그 구간 블록으로 정한다(blocks.segment_uses)."""
    dc: bool = False
    dc2: bool = False
    rf: bool = False
    dcp: bool = False
    rfp: bool = False
    gases: Dict[str, bool] = field(default_factory=dict)      # 가스 이름(AR·O2·N2) → 사용
    guns: Dict[str, bool] = field(default_factory=dict)       # G1~G3 → 사용
    power_select: bool = False


def uses_from_params(params: Mapping[str, Any], caps: Caps, sc: StepCfg) -> Uses:
    info = common_info(params, caps)
    return Uses(dc=info["use_dc"], dc2=info["use_dc2"], rf=info["use_rf"], dcp=info["use_dc_pulse"],
                rfp=info["use_rf_pulse"],
                gases={g: bool(params.get(f"use_{g.lower()}", False)) for g in list(sc.gas_info) + ["AR", "O2", "N2"]},
                guns={g: bool(params.get(f"use_{g.lower()}", False)) for g in ("G1", "G2", "G3")},
                power_select=bool(params.get("use_power_select", False)))


# ───────────── 스텝 조각(v1 순서 그대로) ─────────────
def row_test_steps(sec: float) -> List[ProcessStep]:
    """TEST MODE(2154-2175): DELAY int(초×1000) ms, 문구 int(초)."""
    return [S("DELAY", f"TEST MODE ({int(sec)}s)", duration=int(sec * 1000), polling=False, holding=True)]


def heater_text(temp: float, ramp: Optional[float], soak: Optional[float]) -> str:
    r_txt = f"{ramp:g}°C/min" if ramp else "기본 램프 속도"
    s_txt = f", SOAK {soak:g}분" if soak is not None else ""
    return f"히터 사용 — 목표 {temp:g}°C ({r_txt}{s_txt})"


def heater_set_steps(temp: float, ramp: Optional[float]) -> List[ProcessStep]:
    r_txt = f"{ramp:g}°C/min" if ramp else "기본 램프 속도"
    return [S("HEATER_SET", f'히터 {temp:g}°C 설정 ({r_txt})', value=temp, params=(ramp,), ref="heater.target")]


def heater_wait_steps(temp: float, soak: Optional[float]) -> List[ProcessStep]:
    return [S("HEATER_WAIT", f'히터 {temp:g}°C 도달·안정 대기' + (f' (SOAK {soak:g}분)' if soak is not None else ''),
              value=temp, params=(soak * 60.0,) if soak is not None else None, ref="heater.target", holding=True)]


def base_wait_steps(bp: float) -> List[ProcessStep]:
    return [S("IG_CMD", f'베이스 압력({bp:.1e}) 도달 대기', value=bp)]


def rga_steps() -> List[ProcessStep]:
    return [S("RGA_SCAN", 'RGA 스캔 및 그래프 출력 대기')]


def _gas_sel(sc: StepCfg, used: Mapping[str, bool]) -> List[tuple]:
    """v1 use_any 규칙: 하나라도 쓰면 쓰는 가스만, 하나도 안 쓰면 전부(2326-2350·2794-2803)."""
    use_any = any(bool(used.get(k, False)) for k in ("AR", "O2", "N2"))
    return [(g, info) for g, info in sc.gas_info.items() if not (use_any and not used.get(g.upper(), False))]


def gas_prep_steps(sc: StepCfg, used: Mapping[str, bool]) -> List[ProcessStep]:
    sel = _gas_sel(sc, used)
    out = [S("MFC_CMD", f'Ch{i["channel"]}({g}) Flow Off', params=('FLOW_OFF', {'channel': i["channel"]})) for g, i in sel]
    out += [S("MFC_CMD", 'MFC Valve Open', params=('VALVE_OPEN', {})), S("MFC_CMD", '압력 센서 Zeroing', params=('PS_ZEROING', {}))]
    out += [S("MFC_CMD", f'Ch{i["channel"]}({g}) Zeroing', params=('MFC_ZEROING', {'channel': i["channel"]})) for g, i in sel]
    return out


def gas_intro_steps(sc: StepCfg, used: Mapping[str, bool], flows: Mapping[str, float]) -> List[ProcessStep]:
    ch = sc.chamber
    out = [S("PLC_CMD", '메인 밸브 열기', params=('MV', True, ch))]
    for gas, info in sc.gas_info.items():
        if used.get(gas.upper(), False):
            fv = float(flows.get(gas.upper(), 0))
            c = info["channel"]
            out += [S("PLC_CMD", f'{gas} 밸브 열기', params=(gas, True, ch)),
                    S("MFC_CMD", f'Ch{c}({gas}) GAS {fv}sccm 설정', params=('FLOW_SET', {'channel': c, 'value': fv}),
                      ref=f"mfc.{gas.lower()}_flow"),
                    S("MFC_CMD", f'Ch{c}({gas}) GAS 공급 시작', params=('FLOW_ON', {'channel': c}))]
    return out


def pressure_ctrl_steps(sc: StepCfg, target: float, timeout_s: float) -> List[ProcessStep]:
    """채널 SP(CH1 SP3·CH2 SP4) ON + 그 setpoint 기준 도달 대기(2375-2412)."""
    return [S("MFC_CMD", f'압력 제어({sc.sp_label}) 시작', params=(sc.sp_on_cmd, {})),
            S("MFC_CMD", f'압력 도달 대기 (SP{sc.sp_index} setpoint 기준, timeout={timeout_s:.0f}s)',
              params=("WAIT_PRESSURE", {"target": target, "timeout_sec": timeout_s, "source": "ps",
                                        "use_sp_target": True, "sp_index": sc.sp_index}), holding=True)]


def shutter_steps(sc: StepCfg, action: str, targets: List[str]) -> List[ProcessStep]:
    """건 셔터(설정 PROCESS_GUN_SHUTTERS 순서)·메인 셔터 열기/닫기(2420-2429·2657-2663)."""
    on = action == "open"
    word = "열기" if on else "닫기"
    out = [S("PLC_CMD", f'Gun Shutter {g} {word}', params=(g, on, sc.chamber)) for g in sc.gun_shutters
           if g.lower() in targets]
    if "ms" in targets:
        out.append(S("PLC_CMD", f'Main Shutter {word}', params=('MS', on, sc.chamber)))
    return out


def power_select_steps() -> List[ProcessStep]:
    """CH2 SW_RF_SELECT ON — ch 인자 없음(2431-2436). 안정화 대기는 별도 DELAY(wait 블록)."""
    return [S("PLC_CMD", "Power_select: Power Select ON (SW_RF_SELECT)", params=("SW_RF_SELECT", True))]


def delay_steps(sec: float, message: str, polling: bool = False) -> List[ProcessStep]:
    """DELAY int(round(초×1000)) ms — Shutter Delay·안정화·Power Delay(2437-2445·2552-2557·2609-2645)."""
    return [S("DELAY", message, duration=int(round(float(sec) * 1000.0)), polling=polling, holding=True)]


def unsupported_logs(info: Mapping[str, Any]) -> List[str]:
    out = []
    for req, use, txt in (("req_dc", "use_dc", "DC 연속 파워 미지원 → DC 단계 스킵"), ("req_dc2", "use_dc2", "DC2 연속 파워 미지원 → DC2 단계 스킵"),
                          ("req_rf", "use_rf", "RF 연속 파워 미지원 → RF 단계 스킵"), ("req_dcp", "use_dc_pulse", "DC Pulse 미지원 → Pulse 단계 스킵"),
                          ("req_rfp", "use_rf_pulse", "RF Pulse 미지원 → Pulse 단계 스킵")):
        if info.get(req) and not info[use]:
            out.append(f"주의: 이 챔버는 {txt}")
    return out


def _num(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def power_on_steps(sc: StepCfg, *, dc: Optional[float] = None, dc2: Optional[float] = None, dcp: Optional[float] = None,
                   dcp_freq: Any = None, dcp_off: Any = None, rfp: Optional[float] = None, rfp_freq_khz: Any = None,
                   rfp_duty: Any = None, rf: Optional[float] = None) -> List[ProcessStep]:
    """파워 켜기(2447-2550). 값이 있고 > 0 인 전원만(= v1 use_*). 병렬 = (DC∨DC2)∧(RF∨RF Pulse)."""
    use_dc, use_dc2, use_rf = (_num(x) and x > 0 for x in (dc, dc2, rf))
    use_dcp, use_rfp = (_num(x) and x > 0 for x in (dcp, rfp))
    par = (use_dc or use_dc2) and (use_rf or use_rfp)
    out: List[ProcessStep] = []
    if use_dc:
        out.append(S("DC_POWER_SET", f'DC1 Power {float(dc)}W 설정', value=float(dc), parallel=par, ref="dc.power"))
    if use_dc2:
        out.append(S("DC2_POWER_SET", f'DC2 Power {float(dc2)}W 설정', value=float(dc2), ref="dc2.power"))
    f, off = dcp_freq, dcp_off
    if f is not None:
        f = int(f)
    if off is not None and not isinstance(off, str):
        off = round(float(off), 1)
    if use_dcp:
        f_txt = f"{f}kHz" if f is not None else "keep"
        if off is None:
            o_txt = "keep"
        elif isinstance(off, str):
            o_txt = str(off).upper()
        else:
            o_txt = f"{float(off):.1f}us"
            if f:
                _p = 1000.0 / max(1e-6, float(f))
                o_txt += f"(듀티 {round((_p - float(off)) / _p * 100.0, 1):g}%)"
        out.append(S("DC_PULSE_START", f'DC Pulse 설정 및 ON (P={float(dcp)}W, f={f_txt}, off={o_txt})', value=float(dcp),
                     params=(f, off), ref="dcp.power"))
    hz = int(round(float(rfp_freq_khz) * 1000.0)) if rfp_freq_khz is not None else None
    duty = int(rfp_duty) if rfp_duty is not None else None
    if use_rfp:
        if sc.chamber == 2:
            out.append(S("PLC_CMD", "Power Select ON (SW_POWER_SELECT)", params=("SW_POWER_SELECT", True)))
        f_txt = f"{float(rfp_freq_khz):.3f}kHz" if rfp_freq_khz is not None else "keep"
        d_txt = f"{duty}%" if duty is not None else "keep"
        out.append(S("RF_PULSE_START", f'RF Pulse 설정 및 ON (P={float(rfp)}W, f={f_txt}, duty={d_txt})', value=float(rfp),
                     params=(hz, duty), parallel=par, ref="rfp.power"))
    if use_rf:
        if not use_rfp:
            out.append(S("PLC_CMD", "Power Select OFF (SW_POWER_SELECT)", params=("SW_POWER_SELECT", False)))
        out.append(S("RF_POWER_SET", f'RF Power {float(rf)}W 설정', value=float(rf), parallel=par, ref="rf.power"))
    return out


def boost_steps(value: float, timeout_s: float) -> List[ProcessStep]:
    """working_pressure < 부스트 목표면 SP2 로 먼저(2559-2585)."""
    return [S("MFC_CMD", f'목표 압력(SP2) {value:.2f} 설정', params=('SP2_SET', {'value': value})),
            S("MFC_CMD", '압력 제어(SP2) 시작', params=('SP2_ON', {})),
            S("MFC_CMD", f'압력 도달 대기 (SP2, target={value:.2f}, timeout={timeout_s:.0f}s)',
              params=("WAIT_PRESSURE", {"target": value, "timeout_sec": timeout_s, "source": "ps"}), holding=True)]


def sp1_steps(wp: float) -> List[ProcessStep]:
    return [S("MFC_CMD", f'목표 압력(SP1) {wp:.2f} 설정', params=('SP1_SET', {'value': wp}), ref="mfc.wp"),
            S("MFC_CMD", '압력 제어(SP1) 시작', params=('SP1_ON', {}), polling=False)]


def heater_hold_steps() -> List[ProcessStep]:
    return [S("HEATER_HOLD", '히터 유지 모드 측정 대기 (작업 압력·플라즈마 조건)', polling=False, holding=True)]


def pulse_set_steps(kind: str, power: float, message: str, polling: bool) -> List[ProcessStep]:
    """중간 펄스 전력 변경 — DC 는 REF_POWER 확인까지 기다림, RF 는 no_wait(2618-2624·2709-2715)."""
    act = "DC_PULSE_SET" if kind == "dc" else "RF_PULSE_SET"
    return [S(act, message, value=float(power), polling=polling, no_wait=(act != "DC_PULSE_SET"),
              ref="dcp.power" if kind == "dc" else "rfp.power")]


def pulse_set_message(kind: str, power: float, t_sec: Optional[float] = None, phase: str = "") -> str:
    lab = "DC Pulse" if kind == "dc" else "RF Pulse"
    tail = f" (t={t_sec:.1f}s, {phase})" if t_sec is not None else ""
    return f'{lab} Power 변경 → {float(power)}W{tail}'


def thickness_zero_steps(sc: StepCfg) -> List[ProcessStep]:
    return [S("THICKNESS_ZERO", f'두께 영점·확인 (≤ {sc.zero_confirm_nm:g} nm, 최대 {sc.zero_confirm_s:g}초)',
              params=(float(sc.zero_confirm_s),))]


def thickness_main_steps(sc: StepCfg, target_nm: float, dep_rate: float, integration_ms: int) -> List[ProcessStep]:
    """Thickness 모드 메인(2665-2686): OES 는 상한까지(no_wait), THICKNESS_WAIT(예상, 상한)."""
    exp_s = float(target_nm) / float(dep_rate)
    lim_s = max(exp_s * float(sc.max_time_ratio), exp_s + float(sc.max_time_margin_s))
    return [S("OES_RUN", f'OES 측정 시작 (메인 공정 끝까지, 최대 {lim_s / 60.0:.2f}분)', params=(lim_s, integration_ms), no_wait=True),
            S("THICKNESS_WAIT", f'메인 공정 진행 (두께 {target_nm:g}nm, 예상 {exp_s / 60.0:.2f}분)', value=target_nm,
              params=(exp_s, lim_s), polling=True, holding=True)]


def main_steps(minutes: float, integration_ms: int, change: Optional[Mapping[str, Any]] = None) -> List[ProcessStep]:
    """Time 모드 메인(2687-2731). change = {after_sec(메인 시작부터), kind, power, message} 면 DELAY 를 둘로 나눈다."""
    pt_sec = float(minutes) * 60.0
    out = [S("OES_RUN", f'OES 측정 시작 ({minutes}분, {integration_ms}ms)', params=(pt_sec, integration_ms), no_wait=True)]
    if change:
        part1 = max(0.0, float(change["after_sec"]))
        part2 = max(0.0, float(pt_sec - part1))
        if part1 > 0:
            out.append(S("DELAY", f'메인 공정 진행 (변경 전) {part1 / 60.0:.2f}분', duration=int(round(part1 * 1000.0)),
                         polling=True, ref="process.main_remaining", holding=True))
        out += pulse_set_steps(change["kind"], change["power"], change["message"], True)
        if part2 > 0:
            out.append(S("DELAY", f'메인 공정 진행 (변경 후) {part2 / 60.0:.2f}분', duration=int(round(part2 * 1000.0)),
                         polling=True, ref="process.main_remaining", holding=True))
    else:
        out.append(S("DELAY", f'메인 공정 진행 ({minutes}분)', duration=int(round(pt_sec * 1000.0)), polling=True,
                     ref="process.main_remaining", holding=True))
    return out


# 정상 종료 절차 부분(끝 정지 블록 skip 단위) — 파워 끄기(power)·Power Select 릴레이(relay)는 뺄 수 없다
SHUTDOWN_PARTS: tuple = (
    ("ms_close", "Main Shutter 닫기", True),
    ("power", "파워 끄기(DC1·DC2·RF·DC Pulse·RF Pulse, CH2 RF Pulse 뒤 SW_POWER_SELECT OFF)", False),
    ("flow_off", "MFC Flow Off(쓴 가스, 없으면 전부)", True),
    ("valve_open", "전체 MFC Valve Open", True),
    ("gun_close", "Gun Shutter 닫기", True),
    ("relay", "Power Select OFF (SW_RF_SELECT)", False),
    ("gas_close", "PLC 가스 밸브 닫기(전부)", True),
    ("mv_close", "메인 밸브 닫기", True),
)
SHUTDOWN_SKIPPABLE = tuple(p[0] for p in SHUTDOWN_PARTS if p[2])
SHUTDOWN_MUST_KEEP = tuple(p[0] for p in SHUTDOWN_PARTS if not p[2])


def shutdown_steps(sc: StepCfg, u: Uses, *, force_all: bool = False, skip: tuple = ()) -> List[ProcessStep]:
    """_create_shutdown_sequence(2737-2832) 그대로. skip 은 SHUTDOWN_SKIPPABLE 만(블록 검사가 확인)."""
    ch = sc.chamber
    sk = set(skip) - set(SHUTDOWN_MUST_KEEP)
    out: List[ProcessStep] = []
    if "ms_close" not in sk:
        out.append(S("PLC_CMD", 'Main Shutter 닫기 (항상)', params=('MS', False, ch)))
    if force_all or u.dc:
        out.append(S("DC_POWER_STOP", 'DC1 Power Off'))
    if force_all or u.dc2:
        out.append(S("DC2_POWER_STOP", 'DC2 Power Off'))
    if force_all or u.rf:
        out.append(S("RF_POWER_STOP", 'RF Power Off'))
    if force_all or u.dcp:
        out.append(S("DC_PULSE_STOP", 'DC Pulse Off'))
    if force_all or u.rfp:
        out.append(S("RF_PULSE_STOP", 'RF Pulse Off'))
        if ch == 2:
            out.append(S("PLC_CMD", "Power Select OFF (SW_POWER_SELECT)", params=("SW_POWER_SELECT", False)))
    if "flow_off" not in sk:
        out += [S("MFC_CMD", f'Ch{i["channel"]}({g}) Flow Off', params=('FLOW_OFF', {'channel': i["channel"]}))
                for g, i in _gas_sel(sc, u.gases)]
    if "valve_open" not in sk:
        out.append(S("MFC_CMD", '전체 MFC Valve Open', params=('VALVE_OPEN', {})))
    if "gun_close" not in sk:
        out += [S("PLC_CMD", f'Gun Shutter {g} 닫기', params=(g, False, ch)) for g in sc.gun_shutters
                if u.guns.get(g.upper(), False) or force_all]
    if (u.power_select or force_all) and ch == 2:
        out.append(S("PLC_CMD", "Power_select 종료: Power Select OFF (SW_RF_SELECT)", params=("SW_RF_SELECT", False)))
    if "gas_close" not in sk:
        out += [S("PLC_CMD", f'PLC {g} 밸브 닫기', params=(g, False, ch)) for g in sc.gas_info]
    if "mv_close" not in sk:
        out.append(S("PLC_CMD", '메인 밸브 닫기', params=('MV', False, ch)))
    return out


SHUTDOWN_LOG = "종료 절차가 생성되었습니다."


def emergency_steps(sc: StepCfg, u: Uses) -> List[ProcessStep]:
    """_create_emergency_shutdown_sequence(2834-2930) — 모두 no_wait, 가스는 쓴 것만(AR·O2·N2 고정 목록)."""
    ch = sc.chamber
    both = (u.dc or u.dc2 or u.dcp) and (u.rf or u.rfp)
    out = [S("PLC_CMD", '[긴급] Main Shutter 즉시 닫기', params=('MS', False, ch), no_wait=True)]
    if u.dc:
        out.append(S("DC_POWER_STOP", '[긴급] DC1 Power 즉시 차단', parallel=both, no_wait=True))
    if u.dc2:
        out.append(S("DC2_POWER_STOP", '[긴급] DC2 Power 즉시 차단', parallel=both, no_wait=True))
    if u.rf:
        out.append(S("RF_POWER_STOP", '[긴급] RF Power 즉시 차단', parallel=both, no_wait=True))
    if u.rfp:
        out.append(S("RF_PULSE_STOP", '[긴급] RF Pulse 즉시 차단', parallel=both, no_wait=True))
        if ch == 2:
            out.append(S("PLC_CMD", '[긴급] Power Select 즉시 OFF', params=("SW_POWER_SELECT", False), no_wait=True))
    if u.dcp:
        out.append(S("DC_PULSE_STOP", '[긴급] DC Pulse 즉시 차단', parallel=both, no_wait=True))
    if u.power_select and ch == 2:
        out.append(S("PLC_CMD", '[긴급] Power Select 즉시 OFF', params=("SW_RF_SELECT", False), no_wait=True))
    for g, i in sc.gas_info.items():
        if u.gases.get(g.upper(), False):
            out.append(S("MFC_CMD", f'[긴급] Ch{i["channel"]}({g}) FLOW OFF', params=('FLOW_OFF', {'channel': i["channel"]}),
                         no_wait=True))
    for g in ("AR", "O2", "N2"):
        if u.gases.get(g, False):
            out.append(S("PLC_CMD", f'[긴급] {g} 가스 즉시 차단', params=(g, False, ch), no_wait=True))
    out.append(S("PLC_CMD", '[긴급] 메인 밸브 즉시 닫기', params=('MV', False, ch), no_wait=True))
    return out


EMERGENCY_LOG = "긴급 종료 절차가 생성되었습니다."


def build_sequence(params, chamber, cfg, *, caps=None, log=None):
    """v1 _create_process_sequence 와 같은 스텝 목록 — 본체는 process_sequence.build_sequence."""
    from v2.ch12.process_sequence import build_sequence as _f
    return _f(params, chamber, cfg, caps=caps, log=log)
