"""챔버12 블록 공정 — 레시피 = 블록 목록(JSON). 블록 = {"type", "params", "label"(선택), "row"(선택 — v1 행 순번)}.

챔버K v2/chk/blocks.py 방식(BlockDef·BParam·check_blocks·template_blocks·expand·ADJUST_ITEMS)을 따른다.
- 블록 순서·조합은 자유 — 검사는 블록 값(v1 과 같은 범위)·장비 지원(챔버·SUPPORTS_*)·같은 블록 안 값의 짝뿐.
  v1 의 행 단위 조합 검사(가스 1개 이상·CH1 펄스 필수·CH2 건 1~2개 등)는 v1 행에만(recipe_check.check_norm_params).
- 기본 템플릿(template_blocks — blocks_expand.py): v1 행 params → v1 _create_process_sequence 와 같은 순서의 블록 목록.
  펼치면(expand) v1 과 같은 스텝이 나온다(골든 시험).
- 끝 정지(end_stop)는 v1 정상 종료 절차(_create_shutdown_sequence)를 펼친다 — 끌 장비는 그 구간(앞 end_stop 뒤부터)의 블록이
  쓴 장비. skip 에는 파워 끄기(power)·Power Select 릴레이(relay) 를 넣을 수 없다(챔버K END_STOP_MUST_KEEP 과 같은 원칙).
  목록 끝이 끝 정지가 아니고 그 뒤 구간에 장비를 건드리는 블록이 있으면 자동 끝 정지(뺀 것 없음, label "자동 끝 정지")를 덧붙인다.
  ※ 장비를 건드리지 않는 블록(TEST·delay 행)만 있으면 덧붙이지 않는다 — v1 TEST 행은 종료 절차 없이 DELAY 1단계(골든 ch2_test).
- STOP·이상 중단의 정지 순서는 블록과 무관하게 고정(지금 구간 장비로 process_steps.shutdown_steps — v1 _start_normal_shutdown).

공정 중 자유 제어 정의(엔진이 쓴다 — 실행은 다음 단계)
- skippable / skip_cleanup: 지금 블록을 그만두고 다음으로 갈 수 있는지와 그때 정리할 것. 정의 없으면(False) 거부.
- pausable / pause: 일시정지의 뜻. 못 멈추는 블록은 False(거부).
- resumable / resume_checks: 프로그램 재시작 뒤 이어가기 가능 블록(상태 유지 블록만 — 대기·가스 안정화(압력 대기)·
  메인 증착·히터 유지·히터 도달 대기·TEST·delay 행). 그 블록의 holding 스텝(process_steps.ProcessStep.holding) 중일 때만.
  진공·벤트·게이트·척·핀(로봇 시퀀스 — 블록 아님)·장비 설정 쓰기 도중·정지 순서는 False.
  resume_checks = 이어가기 전에 다시 읽어 저널의 기대 상태와 맞는지 볼 것(RESUME_CHECKS 의 id).
- 시퀀스 편집(edit_sequence): 아직 시작 안 한 블록만 추가·삭제·순서 변경, 편집마다 판 번호 +1·전체 재검사.
"""
from __future__ import annotations

import copy
import math
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Tuple

from v2.ch12.process_steps import SHUTDOWN_PARTS, SHUTDOWN_SKIPPABLE, Caps, caps_from_cfg

BLOCK_KEYS: tuple = ("type", "params", "label", "row")
LABEL_MAX = 120
AUTO_END_STOP_LABEL = "자동 끝 정지"
MAX_BLOCKS = 500

# 이어가기 전 다시 읽어 확인할 장비 상태(저널 기대 상태와 비교 — 확인 방법은 5단계 resume.py)
RESUME_CHECKS: Dict[str, str] = {
    "power_output": "파워 출력 — 이 구간에서 켠 DC·DC2·RF·DC Pulse·RF Pulse 가 켜져 있고 목표 근처(끈 것은 꺼져 있음)",
    "shutter": "셔터 — Main Shutter·건 셔터 열림/닫힘이 저널과 같음(PLC 코일)",
    "gas": "가스 — 메인 밸브·가스 밸브(PLC)·MFC 유량 설정과 FLOW ON",
    "pressure": "압력 — 선택된 SP·설정값(SP1·SP2·채널 SP)과 지금 압력이 범위 안",
    "power_select": "Power Select 릴레이 — SW_RF_SELECT·SW_POWER_SELECT 가 저널과 같음",
    "heater_run": "CH2 히터 RUN·목표값이 저널과 같음",
    "watchdog": "CH2 히터 래더 워치독 이상(WD_ERR) 없음",
    "thickness": "두께 모니터 연결·크리스탈 정상·기준점(저널) 있음",
}
_ALL_RUN_CHECKS = ("power_output", "shutter", "gas", "pressure", "power_select", "heater_run", "watchdog")


@dataclass(frozen=True)
class BParam:
    name: str
    type: str                          # float / int / bool / enum / str / list / off(µs 숫자 | "DC") / parts
    unit: str = ""
    default: Any = None                # None = 비움, "cfg:키" = 펼칠 때 설정(채널 키 CHn_키)
    required: bool = False
    min: Optional[float] = None
    max: Optional[float] = None
    min_exclusive: bool = False
    enum: Optional[tuple] = None
    label: str = ""
    desc: str = ""

    def public(self) -> dict:
        return {"name": self.name, "type": self.type, "unit": self.unit, "default": self.default,
                "required": self.required, "min": self.min, "max": self.max, "minExclusive": self.min_exclusive,
                "enum": list(self.enum) if self.enum else None, "label": self.label, "desc": self.desc}


@dataclass(frozen=True)
class BlockDef:
    type: str
    label: str
    params: tuple
    steps: str                          # 펼친 스텝(사람이 읽는 것)
    v1: str                             # v1 근거(process_controller.py 줄)
    touches: bool = True                # 장비를 건드림(자동 끝 정지 판단)
    chamber: Optional[int] = None       # 그 챔버에서만
    needs: str = ""                     # 필요한 지원(Caps 필드 이름)
    skippable: bool = False
    skip_cleanup: tuple = ()
    pausable: bool = False
    pause: str = ""
    resumable: bool = False
    resume_checks: tuple = ()
    desc: str = ""

    def public(self) -> dict:
        return {"type": self.type, "label": self.label, "params": [p.public() for p in self.params], "steps": self.steps,
                "v1": self.v1, "touches": self.touches, "chamber": self.chamber, "needs": self.needs,
                "skippable": self.skippable, "skipCleanup": list(self.skip_cleanup), "pausable": self.pausable,
                "pause": self.pause, "resumable": self.resumable, "resumeChecks": list(self.resume_checks),
                "desc": self.desc}


P = BParam
_GUNS = ("g1", "g2", "g3", "ms")
_NOT_RESUMABLE_SET = "장비 설정 쓰기 — 재시작 뒤 이어가지 않음(안전 정지)"

BLOCK_DEFS: tuple = (
    BlockDef("heater_set", "히터 설정", (
        P("target_c", "float", "°C", required=True, min=0.0, min_exclusive=True, label="목표 온도",
          desc="상한 min(HEATER_MAX_TEMP, HEATER_SV_LIMIT_C)"),
        P("ramp_c_per_min", "float", "°C/min", None, min=0.0, max=60.0, min_exclusive=True, label="램프",
          desc="비우면 히터 기본 램프 속도")),
        "HEATER_SET", "pc:2289-2306", chamber=2, needs="heater", desc=_NOT_RESUMABLE_SET),
    BlockDef("heater_wait", "히터 도달·안정 대기", (
        P("target_c", "float", "°C", None, min=0.0, min_exclusive=True, label="목표 온도", desc="비우면 앞 히터 설정 블록의 목표"),
        P("soak_min", "float", "분", None, min=0.0, max=480.0, label="SOAK", desc="도달 뒤 시간만 셈. 비우면 SOAK 없음")),
        "HEATER_WAIT", "pc:2307-2311", chamber=2, needs="heater",
        skippable=True, skip_cleanup=("히터 도달 대기 취소(cancel_heater) — 히터는 목표대로 계속 운전",),
        pausable=True, pause="대기 타이머(SOAK 경과) 정지 — 히터는 목표 유지", resumable=True,
        resume_checks=("heater_run", "watchdog")),
    BlockDef("base_wait", "베이스 압력 대기(IG)", (
        P("target_torr", "float", "Torr", "cfg:PROCESS_DEFAULT_BASE_PRESSURE", label="목표 압력"),),
        "IG_CMD", "pc:2314-2318",
        skippable=True, skip_cleanup=("IG 대기·재점등 취소(cancel_ig)",),
        desc="IG 점등·재점등·판정은 IG 드라이버가 돌린다 — 일시정지 없음, 재시작 뒤 이어가지 않음"),
    BlockDef("rga_scan", "RGA 스캔", (), "RGA_SCAN", "pc:2320-2324",
             skippable=True, skip_cleanup=("RGA 스캔 결과를 기다리지 않음(늦게 온 완료는 무시 — _rga_is_stale)",)),
    BlockDef("gas_prep", "가스 준비(Flow Off·밸브·영점)", (
        P("ar", "bool", default=False, label="Ar"), P("o2", "bool", default=False, label="O2"),
        P("n2", "bool", default=False, label="N2", desc="CH1 은 N2 라인 없음")),
        "FLOW_OFF · VALVE_OPEN · PS_ZEROING · MFC_ZEROING — 고른 가스(하나도 안 고르면 전부, v1 use_any)", "pc:2326-2350",
        desc=_NOT_RESUMABLE_SET),
    BlockDef("gas_intro", "가스 주입", (
        P("ar", "bool", default=False, label="Ar"), P("ar_flow", "float", "sccm", None, min=0.0, label="Ar 유량"),
        P("o2", "bool", default=False, label="O2"), P("o2_flow", "float", "sccm", None, min=0.0, label="O2 유량"),
        P("n2", "bool", default=False, label="N2"), P("n2_flow", "float", "sccm", None, min=0.0, label="N2 유량")),
        "PLC MV ON · 가스마다 PLC 밸브 ON · FLOW_SET · FLOW_ON", "pc:2352-2373", desc=_NOT_RESUMABLE_SET),
    BlockDef("pressure_ctrl", "압력 제어(채널 SP)", (
        P("target", "float", "mTorr", required=True, label="작업 압력", desc="SP 읽기 실패 때 쓰는 목표(v1 working_pressure)"),
        P("timeout_sec", "float", "s", "cfg:PC_PRESSURE_WAIT_TIMEOUT_S", min=0.0, min_exclusive=True, label="최대 대기")),
        "MFC SP3_ON(CH1)·SP4_ON(CH2) · WAIT_PRESSURE(그 SP setpoint 기준)", "pc:2375-2412",
        skippable=True, skip_cleanup=("압력 도달 대기만 끝냄 — SP 제어·가스는 그대로",),
        resumable=True, resume_checks=("gas", "pressure"), desc="이어가기는 WAIT_PRESSURE(가스 안정화) 중일 때만"),
    BlockDef("pressure_boost", "압력 부스트(SP2)", (
        P("value", "float", "mTorr", "cfg:PC_WORKING_PRESSURE_BOOST_TARGET", label="SP2 목표"),
        P("timeout_sec", "float", "s", "cfg:PC_PRESSURE_WAIT_TIMEOUT_S", min=0.0, min_exclusive=True, label="최대 대기")),
        "SP2_SET · SP2_ON · WAIT_PRESSURE", "pc:2559-2585",
        skippable=True, skip_cleanup=("압력 도달 대기만 끝냄 — SP2 제어는 그대로",),
        resumable=True, resume_checks=("power_output", "gas", "pressure", "power_select", "heater_run", "watchdog"),
        desc="이어가기는 WAIT_PRESSURE 중일 때만"),
    BlockDef("pressure_set", "작업 압력(SP1)", (
        P("value", "float", "mTorr", required=True, label="SP1"),),
        "SP1_SET · SP1_ON", "pc:2586-2594", desc=_NOT_RESUMABLE_SET),
    BlockDef("shutter", "셔터", (
        P("action", "enum", "", required=True, enum=("open", "close"), label="동작"),
        P("targets", "list", "", required=True, enum=_GUNS, label="셔터", desc="g1~g3(CH2 건 셔터)·ms(Main Shutter)")),
        "PLC G1~G3·MS ON/OFF", "pc:2420-2429·2657-2663", desc=_NOT_RESUMABLE_SET),
    BlockDef("power_select", "Power Select(SW_RF_SELECT)", (), "PLC SW_RF_SELECT ON(ch 인자 없음)", "pc:2431-2436",
             chamber=2, desc="안정화 대기는 뒤 대기 블록(기본 템플릿: PC_POWER_SELECT_SETTLE_MS)"),
    BlockDef("power_on", "파워 켜기", (
        P("dc_power", "float", "W", None, min=0.0, min_exclusive=True, label="DC1"),
        P("dc2_power", "float", "W", None, min=0.0, min_exclusive=True, label="DC2"),
        P("dcp_power", "float", "W", None, min=0.0, min_exclusive=True, label="DC Pulse"),
        P("dcp_freq_khz", "int", "kHz", None, min=20, max=150, label="DC Pulse 주파수", desc="비우면 장비값 유지"),
        P("dcp_off_us", "off", "µs", None, label="DC Pulse Off Time", desc="숫자(1.0~min(10, 400/f)) 또는 DC, 비우면 유지"),
        P("rfp_power", "float", "W", None, min=0.0, min_exclusive=True, label="RF Pulse"),
        P("rfp_freq_khz", "float", "kHz", None, label="RF Pulse 주파수", desc="0.001~RFPULSE_FREQ_MAX_HZ/1000. 장비에는 Hz"),
        P("rfp_duty", "int", "%", None, min=1, max=99, label="RF Pulse 듀티"),
        P("rf_power", "float", "W", None, min=0.0, min_exclusive=True, label="RF 연속")),
        "DC_POWER_SET · DC2_POWER_SET · DC_PULSE_START · [CH2 SW_POWER_SELECT ON] RF_PULSE_START · "
        "[RF Pulse 없으면 SW_POWER_SELECT OFF] RF_POWER_SET — 병렬 = (DC∨DC2)∧(RF∨RF Pulse)", "pc:2447-2550",
        pausable=True, pause="파워 램프 정지 — 지금 출력값 유지(목표 쓰기 멈춤), 재개하면 목표까지 램프 계속",
        desc="램프 도중 재시작은 이어가지 않음. RF Pulse 뒤 Power Delay 는 뒤 대기 블록(PC_RF_PULSE_POST_ON_DELAY_MS)"),
    BlockDef("wait", "대기", (
        P("sec", "float", "s", required=True, min=0.0, label="시간"),),
        "DELAY int(round(초×1000)) ms(문구 = label, polling 없음)", "pc:2437-2445·2552-2557·2609-2645",
        touches=False, skippable=True, skip_cleanup=("타이머만 끝냄",), pausable=True, pause="타이머 정지 — 장비 상태 그대로",
        resumable=True, resume_checks=_ALL_RUN_CHECKS, desc="Shutter Delay·Power Select 안정화·Power Delay 도 이 블록"),
    BlockDef("pulse_set", "펄스 파워 변경", (
        P("kind", "enum", "", required=True, enum=("dc", "rf"), label="장비"),
        P("power", "float", "W", required=True, min=0.0, min_exclusive=True, label="파워"),
        P("polling", "bool", default=False, label="폴링", desc="메인 공정 중이면 true")),
        "DC_PULSE_SET(REF_POWER 확인) · RF_PULSE_SET(no_wait)", "pc:2609-2638", desc=_NOT_RESUMABLE_SET),
    BlockDef("thickness_zero", "두께 영점·확인", (), "THICKNESS_ZERO(THICKNESS_ZERO_CONFIRM_S)", "pc:2647-2655",
             needs="thickness", desc="Main Shutter 열기 전(use_ms=F 여도 같은 자리)"),
    BlockDef("main", "메인 공정(시간)", (
        P("min", "float", "분", required=True, min=0.0, min_exclusive=True, label="시간"),
        P("integration_ms", "int", "ms", "cfg:PROCESS_DEFAULT_OES_INTEGRATION_MS", label="OES 적분"),
        P("change_after_sec", "float", "s", None, min=0.0, label="중간 변경 시점", desc="메인 시작부터. 비우면 변경 없음"),
        P("change_kind", "enum", "", None, enum=("dc", "rf"), label="변경 장비"),
        P("change_power", "float", "W", None, min=0.0, min_exclusive=True, label="변경 파워"),
        P("change_label", "str", "", None, label="변경 문구")),
        "OES_RUN(no_wait) · DELAY(polling) [· DC/RF_PULSE_SET · DELAY]", "pc:2687-2731",
        skippable=True, skip_cleanup=("메인 타이머 끝냄 → 다음 블록(끝 정지가 셔터·파워를 끔)", "OES 측정 정지"),
        pausable=True, pause="메인 타이머 정지 — 파워·가스·셔터 그대로(증착은 계속됨). OES 측정 시간은 늘지 않음(미확인)",
        resumable=True, resume_checks=_ALL_RUN_CHECKS),
    BlockDef("thickness_main", "메인 공정(두께)", (
        P("target_nm", "float", "nm", required=True, min=0.0, min_exclusive=True, label="목표 두께"),
        P("dep_rate", "float", "nm/s", required=True, min=0.0, min_exclusive=True, label="증착률"),
        P("integration_ms", "int", "ms", "cfg:PROCESS_DEFAULT_OES_INTEGRATION_MS", label="OES 적분")),
        "OES_RUN(상한 초, no_wait) · THICKNESS_WAIT(예상·상한)", "pc:2665-2686", needs="thickness",
        skippable=True, skip_cleanup=("두께 판정 중단(cancel_thickness)", "OES 측정 정지"),
        desc="두께 도달은 장비 값으로 판정 — 일시정지 없음", resumable=True, resume_checks=_ALL_RUN_CHECKS + ("thickness",)),
    BlockDef("heater_hold", "히터 유지 측정 대기", (), "HEATER_HOLD", "pc:2596-2602", chamber=2, needs="heater",
             skippable=True, skip_cleanup=("유지 모드 측정 대기 취소(cancel_heater)",),
             resumable=True, resume_checks=_ALL_RUN_CHECKS),
    BlockDef("test", "TEST MODE", (P("sec", "float", "s", required=True, min=1.0, label="시간"),),
             "DELAY int(초×1000) ms 'TEST MODE (N s)'", "pc:2154-2175", touches=False,
             skippable=True, skip_cleanup=("대기만 끊음 — 종료 절차 없음(v1 TEST STOP)",), pausable=True, pause="타이머 정지",
             resumable=True),
    BlockDef("row_delay", "대기 행(delay)", (
        P("sec", "float", "s", required=True, min=0.0, min_exclusive=True, label="시간", desc="1초씩 int(초) 번 센다"),
        P("label", "str", "", required=True, label="표시"), P("name", "str", "", required=True, label="이름")),
        "ROW_DELAY(러너 대기 — 장비 명령·새 로그 없음)", "rt:5127-5145", touches=False,
        skippable=True, skip_cleanup=("대기만 끝냄",), pausable=True, pause="카운트다운 정지", resumable=True),
    BlockDef("end_stop", "정상 끝 정지", (
        P("skip", "parts", "", None, enum=SHUTDOWN_SKIPPABLE, label="뺄 단계",
          desc="ms_close·flow_off·valve_open·gun_close·gas_close·mv_close 만. 파워 끄기(power)·릴레이(relay)는 뺄 수 없음"),),
        "v1 정상 종료 절차(구간 장비) — MS 닫기 · 파워 OFF · Flow Off · MFC Valve Open · 건 셔터 닫기 · SW_RF_SELECT OFF · "
        "PLC 가스 닫기 · MV 닫기", "pc:2737-2832", desc="정지 순서 — 건너뛰기·일시정지·이어가기 없음"),
)


def by_type() -> Dict[str, BlockDef]:
    return {b.type: b for b in BLOCK_DEFS}


def definition_table() -> List[dict]:
    """블록 정의 표(웹 묶음·보고용) — 종류·건너뛰기·일시정지 뜻·이어가기."""
    return [{"type": d.type, "label": d.label, "skippable": d.skippable, "skip_cleanup": list(d.skip_cleanup),
             "pausable": d.pausable, "pause": d.pause, "resumable": d.resumable, "resume_checks": list(d.resume_checks)}
            for d in BLOCK_DEFS]


# ───────────── 검사 ─────────────
def _empty(v: Any) -> bool:
    return v is None or (isinstance(v, str) and not v.strip())


def _num(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool) and not (isinstance(v, float) and math.isnan(v))


def _as_type(p: BParam, v: Any) -> Any:
    if p.type == "float" and _num(v):
        return float(v)
    if p.type == "int" and _num(v) and float(v).is_integer():
        return int(v)
    return v


def _check_param(p: BParam, v: Any) -> Optional[str]:
    t = p.type
    if t == "bool":
        return None if isinstance(v, bool) else "true/false 여야 함"
    if t == "str":
        return None if isinstance(v, str) else "문자열이어야 함"
    if t == "enum":
        return None if v in (p.enum or ()) else f"{list(p.enum or ())} 중 하나"
    if t in ("list", "parts"):
        if not isinstance(v, list) or any(x not in (p.enum or ()) for x in v) or len(set(v)) != len(v):
            if t == "parts" and isinstance(v, list):
                keep = sorted({x for x in v if isinstance(x, str)} - set(p.enum or ()))
                return f"뺄 수 없는 단계 {keep} — 뺄 수 있는 것 {list(p.enum or ())}"
            return f"{list(p.enum or ())} 중 하나 이상(중복 없음)"
        if t == "list" and not v:
            return "하나 이상"
        return None
    if t == "off":
        if isinstance(v, str) and v.strip().upper() == "DC":
            return None
        if not _num(v):
            return "숫자(µs) 또는 DC"
        return None
    if not _num(v):
        return "숫자여야 함"
    if t == "int" and not float(v).is_integer():
        return "정수여야 함"
    if p.min is not None and (v <= p.min if p.min_exclusive else v < p.min):
        return f"{v} {'≤' if p.min_exclusive else '<'} {p.min:g}"
    if p.max is not None and v > p.max:
        return f"{v} > {p.max:g}"
    return None


def _check_one(b: Any, path: str, chamber: int, caps: Caps, cfg: Mapping[str, Any], errs: list, warns: list) -> Optional[dict]:
    from v2.ch12.recipe_check import check_dc_pulse, rfp_freq_max_khz, heater_limit_c
    defs = by_type()
    if not isinstance(b, dict):
        errs.append(f"블록 {path}: 객체여야 함")
        return None
    t = b.get("type")
    d = defs.get(t) if isinstance(t, str) else None
    if d is None:
        errs.append(f"블록 {path}: type 필요" if _empty(t) else f"블록 {path}: 모르는 종류 {t!r}")
        return None
    pre = f"블록 {path}({d.type})"
    extra = set(b) - set(BLOCK_KEYS)
    if extra:
        errs.append(f"{pre}: 모르는 키 {sorted(extra)}")
    params = b.get("params") or {}
    if not isinstance(params, dict):
        errs.append(f"{pre}: params 는 객체")
        return None
    for k in params:
        if k not in {p.name for p in d.params}:
            errs.append(f"{pre}: {k}: 모르는 값")
    out: Dict[str, Any] = {}
    bad: set = set()
    for p in d.params:
        v = params.get(p.name)
        if _empty(v) or (p.type == "parts" and v == []):
            if p.required:
                errs.append(f"{pre}: {p.name}: 값 필요")
                bad.add(p.name)
            out[p.name] = None if isinstance(p.default, str) and p.default.startswith("cfg:") else p.default
            continue                                   # "cfg:키" 는 비운 채로 둔다 — 펼칠 때(공정 시작 때) 설정에서
        v = _as_type(p, v)
        e = _check_param(p, v)
        if e:
            errs.append(f"{pre}: {p.name}: {e}")
            bad.add(p.name)
        out[p.name] = v
    label = None if _empty(b.get("label")) else b.get("label")
    if label is not None and (not isinstance(label, str) or len(label) > LABEL_MAX):
        errs.append(f"{pre}: label 은 {LABEL_MAX}자 이하 문자열")
    row = b.get("row")
    if row is not None and (not isinstance(row, int) or isinstance(row, bool) or row < 1):
        errs.append(f"{pre}: row 는 1 이상 정수")
    # 장비·챔버 — v1 장비 사실(CH1 은 건 셔터·N2·히터 없음 등)
    if d.chamber is not None and d.chamber != int(chamber):
        errs.append(f"{pre}: CH{int(chamber)} 에서는 쓸 수 없음(CH{d.chamber} 전용)")
    if d.needs and not getattr(caps, d.needs):
        errs.append(f"{pre}: 이 챔버 설정에서 꺼져 있음(SUPPORTS_{d.needs.upper()})")
    o = out
    if d.type == "gas_intro":
        for g in ("ar", "o2", "n2"):
            if o[g] is True and g + "_flow" not in bad and _empty(o[g + "_flow"]):
                warns.append(f"{pre}: {g}_flow 비움 → 0 sccm(v1 공란 = 0)")
    if d.type in ("gas_prep", "gas_intro") and int(chamber) == 1 and o.get("n2") is True:
        errs.append(f"{pre}: n2: CH1 은 N2 라인 없음")
    if d.type == "shutter" and "targets" not in bad:
        from v2.ch12.process_steps import step_cfg
        guns = {g.lower() for g in step_cfg(cfg, chamber).gun_shutters}
        miss = [x for x in o["targets"] if x != "ms" and x not in guns]
        if miss:
            errs.append(f"{pre}: targets: 이 챔버에 없는 건 셔터 {miss}(PROCESS_GUN_SHUTTERS)")
    if d.type == "power_on":
        for k, cap, lab in (("dc_power", "dc", "DC 연속"), ("dc2_power", "dc2", "DC2(폐지 — 히터와 배타)"),
                            ("rf_power", "rf", "RF 연속"), ("dcp_power", "dcp", "DC Pulse"), ("rfp_power", "rfp", "RF Pulse")):
            if _num(o[k]) and not getattr(caps, cap):
                errs.append(f"{pre}: {k}: 이 챔버는 {lab} 미지원")
        if _num(o["dcp_power"]) and _num(o["rfp_power"]):
            errs.append(f"{pre}: RF Pulse와 DC Pulse는 동시에 사용할 수 없습니다 (같은 시리얼 포트)")
        if _num(o["rfp_freq_khz"]):
            fmax = rfp_freq_max_khz(cfg, chamber)
            if not (0.001 <= float(o["rfp_freq_khz"]) <= fmax):
                errs.append(f"{pre}: rfp_freq_khz: 허용 범위 0.001~{fmax:g}kHz 밖")
        if "dcp_off_us" not in bad and o["dcp_off_us"] is not None:
            f = o["dcp_freq_khz"] if _num(o["dcp_freq_khz"]) else None
            off = o["dcp_off_us"] if isinstance(o["dcp_off_us"], str) else round(float(o["dcp_off_us"]), 1)
            errs += [f"{pre}: {e}" for e in check_dc_pulse({"dc_pulse_freq": f, "dc_pulse_off_time": off})
                     if not e.startswith("DC Pulse Freq")]
    if d.type == "heater_set" and _num(o["target_c"]):
        lim = heater_limit_c(cfg)
        if o["target_c"] > lim:
            errs.append(f"{pre}: target_c: {o['target_c']:g} > {lim:g}°C")
    if d.type == "main":
        has = [not _empty(o[k]) for k in ("change_after_sec", "change_kind", "change_power")]
        if any(has) and not all(has):
            errs.append(f"{pre}: change_after_sec·change_kind·change_power 는 함께 넣어야 함")
    return {"type": d.type, "params": out, "label": label or "", **({"row": row} if row is not None else {})}


def check_blocks(blocks: Any, cfg: Mapping[str, Any], chamber: int, *, caps: Optional[Caps] = None) -> tuple:
    """(정리된 블록 목록 또는 None, 오류, 경고). 값·장비 지원만 본다 — 순서·조합 규칙은 없다."""
    caps = caps or caps_from_cfg(cfg, chamber)
    errs: list = []
    warns: list = []
    if not isinstance(blocks, list) or not blocks:
        return None, ["블록 목록이 비었다"], warns
    if len(blocks) > MAX_BLOCKS:
        return None, [f"블록 {len(blocks)}개 > {MAX_BLOCKS}"], warns
    out = [x for x in (_check_one(b, str(i), chamber, caps, cfg, errs, warns) for i, b in enumerate(blocks, 1))
           if x is not None]
    if not errs:
        try:
            expand(out, cfg, chamber, caps=caps)
        except (ValueError, KeyError, ZeroDivisionError) as e:
            errs.append(f"펼치기 실패: {e}")
    return (None if errs else out), errs, warns


def expand(blocks: List[dict], cfg: Mapping[str, Any], chamber: int, *, caps: Optional[Caps] = None) -> tuple:
    """블록 → (스텝 목록, 블록 실행 목록). 본체는 blocks_expand.expand."""
    from v2.ch12.blocks_expand import expand as _f
    return _f(blocks, cfg, chamber, caps=caps)


def template_blocks(p: Mapping[str, Any], chamber: int, cfg: Mapping[str, Any], *, caps: Optional[Caps] = None) -> tuple:
    """v1 행 params → (기본 템플릿 블록, v1 로그). 본체는 blocks_expand.template_blocks."""
    from v2.ch12.blocks_expand import template_blocks as _f
    return _f(p, chamber, cfg, caps=caps)


# ───────────── 공정 중 수정(PROCESS_ADJUST) ─────────────
@dataclass(frozen=True)
class AdjustItem:
    device: str
    item: str
    key: str                    # 이 값을 쓰는 스텝의 ref
    param: BParam
    apply: str                  # 지금 블록이면 바로 쓰는 경로(장비 호출 — v1 근거)
    when_live: str              # '지금 쓰는 중' 판단
    label: str = ""

    def public(self) -> dict:
        return {"device": self.device, "item": self.item, "key": self.key, "param": self.param.public(),
                "apply": self.apply, "whenLive": self.when_live, "label": self.label}


ADJUST_ITEMS: tuple = (
    AdjustItem("mfc", "ar_flow", "mfc.ar_flow", P("value", "float", "sccm", required=True, min=0.0),
               "MFC FLOW_SET(ch 1) → 되읽기(v1 device/mfc.py FLOW_SET 확인)", "Ar FLOW_SET 이 끝났으면", "Ar 유량"),
    AdjustItem("mfc", "o2_flow", "mfc.o2_flow", P("value", "float", "sccm", required=True, min=0.0),
               "MFC FLOW_SET(ch 2) → 되읽기", "O2 FLOW_SET 이 끝났으면", "O2 유량"),
    AdjustItem("mfc", "n2_flow", "mfc.n2_flow", P("value", "float", "sccm", required=True, min=0.0),
               "MFC FLOW_SET(ch 3) → 되읽기(CH2 만)", "N2 FLOW_SET 이 끝났으면", "N2 유량"),
    AdjustItem("mfc", "wp", "mfc.wp", P("value", "float", "mTorr", required=True, min=0.0),
               "MFC SP1_SET → 되읽기", "SP1_SET 이 끝났으면", "작업 압력(SP1)"),
    AdjustItem("dc", "power", "dc.power", P("value", "float", "W", required=True, min=0.0, min_exclusive=True),
               "DC 연속 새 목표(send_dc_power — 램프)", "DC1 을 켰으면", "DC1 파워"),
    AdjustItem("dc2", "power", "dc2.power", P("value", "float", "W", required=True, min=0.0, min_exclusive=True),
               "DC2 새 목표(send_dc_power2)", "DC2 를 켰으면", "DC2 파워"),
    AdjustItem("rf", "power", "rf.power", P("value", "float", "W", required=True, min=0.0, min_exclusive=True),
               "RF 연속 새 목표(send_rf_power — 램프)", "RF 를 켰으면", "RF 파워"),
    AdjustItem("dcp", "power", "dcp.power", P("value", "float", "W", required=True, min=0.0, min_exclusive=True),
               "DC Pulse set_dc_pulse_power(출력 유지 + REF_POWER 확인 — v1 DC_PULSE_SET)", "DC Pulse 를 켰으면", "DC Pulse 파워"),
    AdjustItem("rfp", "power", "rfp.power", P("value", "float", "W", required=True, min=0.0, min_exclusive=True),
               "RF Pulse set_rf_pulse_power(출력 유지 — v1 RF_PULSE_SET)", "RF Pulse 를 켰으면", "RF Pulse 파워"),
    AdjustItem("heater", "target", "heater.target", P("value", "float", "°C", required=True, min=0.0, min_exclusive=True),
               "CH2 히터 목표 변경(heater_set 목표만 — 운전 유지)", "히터 설정이 끝났으면", "히터 목표"),
    AdjustItem("process", "main_remaining", "process.main_remaining", P("value", "float", "s", required=True, min=0.0),
               "지금 메인 블록 남은 시간 = 값(아직이면 다음 메인 블록 길이)", "메인 블록이 돌고 있으면", "메인 남은 시간"),
    AdjustItem("process", "skip_wait", "process.skip_wait", P("value", "bool", "", required=True),
               "지금 대기 블록을 바로 끝냄(value=true) — 건너뛰기와 같은 정리", "대기 블록이 돌고 있을 때만", "지금 대기 끝내기"),
)


def adjust_item(device: str, item: str) -> Optional[AdjustItem]:
    return next((a for a in ADJUST_ITEMS if a.device == device and a.item == item), None)


# ───────────── 시퀀스 편집·판 번호 ─────────────
EDIT_OPS = ("insert", "delete", "move")


@dataclass
class EditResult:
    ok: bool
    blocks: Optional[List[dict]] = None
    rev: int = 0
    reason: str = ""                       # EDIT_STARTED_BLOCK · EDIT_BAD_OP · EDIT_INVALID
    errors: List[str] = field(default_factory=list)
    warns: List[str] = field(default_factory=list)


def edit_sequence(blocks: List[dict], started_upto: int, op: Mapping[str, Any], cfg: Mapping[str, Any], chamber: int, *,
                  rev: int = 0, caps: Optional[Caps] = None) -> EditResult:
    """아직 시작 안 한 블록만 추가·삭제·순서 변경. started_upto = 시작한(지금 포함) 맨 위 블록 수 — blocks[:started_upto] 는
    바꿀 수 없다. op = {"op": "insert", "at", "block"} · {"op": "delete", "at"} · {"op": "move", "from", "to"}(0 부터).
    편집 뒤 전체 재검사(check_blocks) — 통과하면 판 번호 +1. 끝 정지·파워 끄기는 expand 가 늘 보장(자동 끝 정지)."""
    cur = copy.deepcopy(list(blocks))
    n = len(cur)
    k = int(started_upto)
    kind = op.get("op") if isinstance(op, Mapping) else None
    if kind not in EDIT_OPS or not (0 <= k <= n):
        return EditResult(False, rev=rev, reason="EDIT_BAD_OP", errors=[f"op 는 {list(EDIT_OPS)} 중 하나, started_upto 0~{n}"])

    def idx(name, hi):
        v = op.get(name)
        return v if isinstance(v, int) and not isinstance(v, bool) and 0 <= v <= hi else None
    if kind == "insert":
        at = idx("at", n)
        if at is None or not isinstance(op.get("block"), dict):
            return EditResult(False, rev=rev, reason="EDIT_BAD_OP", errors=["insert: at(0~길이)·block 필요"])
        if at < k:
            return EditResult(False, rev=rev, reason="EDIT_STARTED_BLOCK", errors=[f"시작한 블록 앞({at} < {k})에는 넣을 수 없음"])
        cur.insert(at, copy.deepcopy(dict(op["block"])))
    elif kind == "delete":
        at = idx("at", n - 1)
        if at is None:
            return EditResult(False, rev=rev, reason="EDIT_BAD_OP", errors=["delete: at(0~길이-1) 필요"])
        if at < k:
            return EditResult(False, rev=rev, reason="EDIT_STARTED_BLOCK", errors=[f"시작한 블록({at})은 지울 수 없음"])
        del cur[at]
    else:
        a, b = idx("from", n - 1), idx("to", n - 1)
        if a is None or b is None:
            return EditResult(False, rev=rev, reason="EDIT_BAD_OP", errors=["move: from·to(0~길이-1) 필요"])
        if a < k or b < k:
            return EditResult(False, rev=rev, reason="EDIT_STARTED_BLOCK", errors=[f"시작한 블록 범위(0~{k - 1})는 옮길 수 없음"])
        cur.insert(b, cur.pop(a))
    if cur[:k] != list(blocks)[:k]:
        return EditResult(False, rev=rev, reason="EDIT_STARTED_BLOCK", errors=["시작한 블록이 바뀜"])
    clean, errs, warns = check_blocks(cur, cfg, chamber, caps=caps)
    if errs:
        return EditResult(False, rev=rev, reason="EDIT_INVALID", errors=errs, warns=warns)
    return EditResult(True, clean, rev + 1, warns=warns)


@dataclass
class SequenceState:
    """레시피 판(공정 중 편집) — 원래 레시피·지금 블록·판 번호·편집 기록. 저널·기록이 그대로 싣는다."""
    original: List[dict]
    blocks: List[dict]
    rev: int = 0
    history: List[dict] = field(default_factory=list)

    @classmethod
    def start(cls, blocks: List[dict]) -> "SequenceState":
        return cls(copy.deepcopy(blocks), copy.deepcopy(blocks), 0, [])

    def edit(self, op: Mapping[str, Any], started_upto: int, cfg: Mapping[str, Any], chamber: int, *,
             caps: Optional[Caps] = None, origin: str = "") -> EditResult:
        r = edit_sequence(self.blocks, started_upto, op, cfg, chamber, rev=self.rev, caps=caps)
        self.history.append({"rev": r.rev, "op": dict(op), "started_upto": int(started_upto), "ok": r.ok,
                             "reason": r.reason, "origin": origin})
        if r.ok:
            self.blocks, self.rev = r.blocks, r.rev
        return r

    def record(self) -> dict:
        return {"original": copy.deepcopy(self.original), "blocks": copy.deepcopy(self.blocks), "rev": self.rev,
                "history": copy.deepcopy(self.history)}


def shutdown_part_ids() -> Tuple[str, ...]:
    return tuple(p[0] for p in SHUTDOWN_PARTS)
