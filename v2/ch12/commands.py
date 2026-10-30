"""챔버12 명령 표 — 명령마다 종류·인자(ArgDef)·받는 출처(web/local(hmi)/robot)·허용 상태·대상·benchOk, 사유 코드, 시작·명령 조건,
PLC 버튼(사용자 지시 3장 (4)). 챔버K v2/chk/commands.py 와 같은 모양(CommandDef·check_command·check_args) — core 가 이 표만 본다.

출처(sources)
- 표의 sources 는 전환 기간 기본(COMMAND_SOURCES_LOCAL_ALL=true) 그대로의 출처 — 로컬 화면에서도 전부 가능.
- portal_only=True 명령(공정 시작·레시피·시퀀스 편집·두께 Factor/재료 저장 = '공정 시작·레시피·설정')은
  COMMAND_SOURCES_LOCAL_ALL=false 면 로컬 화면(hmi)에서 빠진다(설정만 바꿔서 '포털에서만' — allowed_sources).
- web_only=True(CONFIG_APPLY·CONFIG_ROLLBACK — 설정 판·스키마 해시가 웹 묶음 기준)는 설정과 상관없이 웹만.
- ROBOT_<명령>(로봇 서버 18개, v1 host/setup.py) 은 robot 만. 공정 중 자유 제어 4종(PROCESS_ADJUST·BLOCK_SKIP·PAUSE/RESUME·
  SEQUENCE_EDIT)은 로봇에서 받지 않는다(지시 3장 (3)).
- internal(안전 엔진)은 ALL_STOP·PROCESS_ABORT·POWER_OFF_ONLY·RESIDUAL_POWER_OFF·NOTIFY(safety_rules 의 조치).
대상(target): chamber(인자 ch 1|2 — CH1·CH2 엔진) · pc · tsp · heater(CH2) · thickness(인자 ch) · shared(PLC 버튼·덤프·ALL STOP) ·
  system(설정) · robot(로봇 명령 — RobotOps).
허용 상태(states): 그 대상 엔진의 상태 이름(process_states.STATES). "*" 는 전부. 엔진이 더 자세히 거부하는 것은 엔진 사유 그대로.
benchOk: bench(장비 쓰기 금지)에서도 받는 명령 — 장비에 쓰지 않는다(_BENCH_OK 한 곳). 나머지는 core 가 BENCH_READ_ONLY.
v1 근거: ui/main_window.py 버튼(ch1/ch2 Start·Stop·processList, PC Start·Stop·processList·채널 라디오·Power_Select·
RuntimeDump, TSP Start·Stop·예약, preSputter Reserve·Cancel·Recipe), ui/heater_panel.py(ON/OFF·적용·리셋),
ui/thickness_panel.py·thickness_factor_dialog.py·thickness_material_dialog.py(영점·Factor·재료), host/setup.py(로봇 18개).
"""
from __future__ import annotations

import math
from dataclasses import dataclass, replace
from typing import Any, Dict, Mapping, Optional, Tuple

from v2.common.commands import ArgDef, COMMON_REASONS, Reason, Source
from v2.ch12 import process_states as PS
from v2.ch12.drivers import plc_map as PM

WEB, HMI, ROBOT, INT = Source.WEB, Source.HMI, Source.ROBOT, Source.INTERNAL
BOTH = (WEB, HMI)
ANY = "*"
LOCAL_ALL_KEY = "COMMAND_SOURCES_LOCAL_ALL"

# ───────────── 사유 코드(챔버12) — 공통(COMMON_REASONS) 위에. v1 오류 코드 E### 는 errors.py 그대로(여기 없음) ─────────────
CH12_REASONS: tuple = (
    Reason("CORE_NOT_READY", "명령 처리(core)가 아직 없음(조립 전)", "reject"),
    Reason("COOLDOWN", "이전 공정 종료 후 대기(쿨다운) 중", "reject"),
    Reason("RUNNER_BUSY", "이전 공정·정리가 아직 진행 중", "reject"),
    Reason("BUSY", "공정 진행 중 — 레시피를 바꿀 수 없음", "reject"),
    Reason("RESTART_PENDING", "재시작 뒤 이어가기 판단 중 — 새 시작 거부(5단계)", "reject"),
    Reason("NO_RECIPE", "적재된 레시피 없음", "reject"),
    Reason("RECIPE_EMPTY", "레시피에 공정 행이 없음", "reject"),
    Reason("RECIPE_INVALID", "블록 레시피 검사 실패", "reject"),
    Reason("E230", "레시피 행 값 오류(v1 E230)", "reject"),
    Reason("PROCESS_RUNNING", "공정 진행 중", "reject"),
    Reason("PROCESS_NOT_RUNNING", "진행 중인 공정 없음", "reject"),
    Reason("CH1_PROCESS_RUNNING", "CH1 에서 다른 공정(챔버·PC) 실행 중(TSP 시작 불가)", "reject"),
    Reason("PC_RUNNING", "같은 채널 플라즈마 클리닝 실행 중", "reject"),
    Reason("CHAMBER_RUNNING", "같은 채널 챔버 공정 실행 중", "reject"),
    Reason("PULSE_PORT_BUSY", "펄스 전원 포트를 다른 챔버가 쓰는 중", "reject"),
    Reason("NOT_SUPPORTED", "설정에서 이 챔버에 없는 장비(히터·두께 모니터)", "reject"),
    Reason("TIME_FORMAT", "시간 형식 오류(HH:MM)", "reject"),
    Reason("SKIP_NOT_ALLOWED", "건너뛸 수 없는 블록", "reject"),
    Reason("PAUSE_NOT_ALLOWED", "일시정지할 수 없는 블록", "reject"),
    Reason("PAUSE_NOT_AVAILABLE", "이 블록의 일시정지는 아직 없음(히터 SOAK)", "reject"),
    Reason("NOT_PAUSED", "일시정지 상태가 아님", "reject"),
    Reason("ADJUST_NOT_USED", "공정 중 수정: 이번 공정에서 쓰지 않는 장비·항목", "reject"),
    Reason("ADJUST_OUT_OF_RANGE", "공정 중 수정: 범위 밖", "reject"),
    Reason("ADJUST_NOT_APPLICABLE", "공정 중 수정: 지금 블록에 해당 없음", "reject"),
    Reason("EDIT_BAD_OP", "시퀀스 편집 형식 오류", "reject"),
    Reason("EDIT_STARTED_BLOCK", "이미 시작한 블록은 편집 불가", "reject"),
    Reason("EDIT_INVALID", "편집 뒤 레시피 검사 실패", "reject"),
    Reason("CHANNEL_SWITCH_RUNNING", "플라즈마 클리닝 실행 중 — 챔버 전환 불가(v1 PC 라디오)", "reject"),
    Reason("THICKNESS_BLOCKED", "두께 모니터 쓰기 금지(공정 중·미연결·응답 없음)", "reject"),
    Reason("HEATER_REJECTED", "히터 패널 명령 거부(v1 문구는 detail)", "reject"),
    Reason("DUMP_BUSY", "런타임 덤프 진행 중(v1 already in progress)", "reject"),
    Reason("ROBOT_NOT_READY", "로봇 명령 처리(RobotOps)가 붙지 않음", "reject"),
    # 공정 끝·중단(run_end 사유 — 엔진이 낸다)
    Reason("PROCESS_FAILED", "공정 실패(v1 대표 오류 코드 없음)", "abort"),
    Reason("PREFLIGHT_FAILED", "시작 전 점검 실패", "abort"),
    Reason("GATE_CHECK_FAILED", "게이트 밸브 CLOSED 아님(v1 E301)", "abort"),
    Reason("MAIN_VALVE_CHECK_FAILED", "Main Valve 인터락·열림 확인 실패(v1 E301)", "abort"),
    Reason("RECIPE_PARAM_ERROR", "레시피 값 검사 실패(시작 직전)", "abort"),
    Reason("PROGRAM_EXIT", "프로그램 의도적 종료(콘솔 닫기·Ctrl+C·로그오프) — 파워 출력만 끔(챔버K 결정 4)", "abort"),
    Reason("PROGRAM_RESTART", "재시작 뒤 이어가기 조건 불충족 — 정지 순서(5단계)", "abort"),
    # 안전 엔진 알림
    Reason("RESIDUAL_POWER_OFF", "시작 때 파워 출력이 남아 있음 — 공정이 없어 파워만 끔(S51)", "info"),
    Reason("RESIDUAL_POWER_KEPT", "시작 때 파워가 남아 있으나 공정·운전 중이라 끄지 않음", "info"),
    Reason("POWER_OFF_ONLY", "의도적 종료 — 파워 출력만 끔(PLC 릴레이·MFC 그대로)", "info"),
    Reason("PLC_MANUAL_IN_PROCESS", "공정 중 PLC 버튼 실행(기록)", "info"),
    Reason("LOG_CLEAR", "화면 로그 지우기(장비 동작 없음)", "info"),
)

ALL_REASONS: tuple = COMMON_REASONS + CH12_REASONS


def reason_codes() -> set:
    return {r.code for r in ALL_REASONS}


# ───────────── 시작·명령 조건(precondition) — core 가 평가 ─────────────
@dataclass(frozen=True)
class Pre:
    id: str
    desc: str
    reason: str
    v1: str = ""

    def public(self) -> dict:
        return {"id": self.id, "desc": self.desc, "reason": self.reason, "v1": self.v1}


PRECONDITIONS: tuple = (
    Pre("CHAMBER_IDLE", "그 챔버 공정 없음(시작 가드 — 쿨다운 60 s·러너 IDLE·컨트롤러 멈춤)", "RUNNER_BUSY",
        "runtime/chamber_runtime.py:4375-4416 _handle_start_clicked"),
    Pre("NO_COOLDOWN", "이전 공정 종료 뒤 60 s", "COOLDOWN", "runtime/chamber_runtime.py:4375-4380"),
    Pre("PC_SAME_CH_IDLE", "같은 채널 플라즈마 클리닝이 실행 중이 아님(다른 채널은 허용)", "PC_RUNNING",
        "runtime/chamber_runtime.py:4404-4411"),
    Pre("RECIPE_LOADED", "레시피 적재됨(또는 시작 인자 params)", "NO_RECIPE", "chamber_runtime START_QUEUE/START"),
    Pre("NOT_BUSY", "공정 중 레시피 적재 금지", "BUSY", "결정 D85"),
    Pre("RUNNING", "공정 진행 중(정지 순서 아님)", "PROCESS_NOT_RUNNING", "v2 자유 제어"),
    Pre("PLC_LINK_UP", "PLC 통신 정상(PLC 버튼·Power Select)", "LINK_DOWN", "main.py:1244-1255"),
    Pre("PC_IDLE", "플라즈마 클리닝 실행 중이 아님(채널 전환)", "CHANNEL_SWITCH_RUNNING", "main.py:825-834"),
    Pre("PC_START_GUARD", "PC 시작 가드(실행 중 E205·쿨다운·같은 CH 챔버 바쁨)", "PROCESS_RUNNING",
        "runtime/plasma_cleaning_runtime.py:1235-1275"),
    Pre("TSP_START_GUARD", "TSP 시작 가드(실행 중·CH1 챔버/PC 실행 중·쿨다운·입력 파싱)", "PROCESS_RUNNING",
        "runtime/tsp_runtime.py:394-454"),
    Pre("HEATER_SUPPORTED", "CH2_SUPPORTS_HEATER", "NOT_SUPPORTED", "main.py _init_ch2_heater"),
    Pre("THICKNESS_SUPPORTED", "CHn_SUPPORTS_THICKNESS(두께 모니터 연결)", "NOT_SUPPORTED", "ui/thickness_panel.py"),
    Pre("THICKNESS_WRITABLE", "두께 모니터 쓰기 가능(공정 중 아님·연결·응답)", "THICKNESS_BLOCKED", "ui/thickness_panel.py:37-47"),
)


# ───────────── PLC 버튼 — v1 PLC 스위치 코일(device/plc.py PLC_COIL_MAP 의 *_SW·SW_*) 전부 ─────────────
@dataclass(frozen=True)
class PlcButton:
    target: str            # 명령 인자 값(소문자)
    name: str              # plc_map.COIL_MAP 이름(write_switch)
    label: str
    chamber: str           # ch1 · ch2 · loadlock · shared

    def public(self) -> dict:
        return {"target": self.target, "name": self.name, "label": self.label, "chamber": self.chamber,
                "coil": PM.COIL_MAP[self.name]}


_WORDS = (("R_P_", "로터리 펌프"), ("R_V_", "러핑 밸브"), ("F_V_", "포어라인 밸브"), ("M_V_", "메인 밸브"),
          ("VENT_", "벤트"), ("TURBO_", "터보 펌프"), ("DOOR_", "도어"), ("G_V_", "게이트 밸브"),
          ("MAIN_SHUTTER_", "메인 셔터"), ("Ar_", "Ar 가스"), ("O2_", "O2 가스"), ("N2_", "N2 가스"), ("MAIN_", "메인 가스"),
          ("L_R_P_", "로드락 로터리 펌프"), ("L_R_V_", "로드락 러핑 밸브"), ("L_PIN_UP", "로드락 4핀 올림"),
          ("L_PIN_DOWN", "로드락 4핀 내림"), ("L_VENT", "로드락 벤트"), ("L_VAC_READY", "로드락 진공 준비"),
          ("SW_RF_SELECT", "Power Select 릴레이(SW_RF_SELECT)"), ("SW_POWER_SELECT", "Power Select(SW_POWER_SELECT)"),
          ("SHUTTER_", "건 셔터"), ("Z_M_P_", "척 모터"), ("BUZZER_STOP", "부저 정지"))


def _button_label(name: str) -> str:
    base = next((w for p, w in _WORDS if name.startswith(p)), name)
    tail = [ko for word, ko in (("OPEN", "열기"), ("CLOSE", "닫기"), ("_CCW", "역회전"), ("_CW", "정회전"), ("_STOP", "정지"),
                                 ("_MID", "중간")) if word in name and not (word == "_CW" and "_CCW" in name)]
    if name.startswith("BUZZER"):
        tail = []
    return " ".join([base] + tail) + f" ({name})"


def _button_chamber(name: str) -> str:
    if name.startswith("L_"):
        return "loadlock"
    if name.startswith("SHUTTER_") or name.startswith("SW_") or name.startswith("BUZZER"):
        return "shared"
    for n in ("1", "2"):
        if f"_{n}_" in name or name.endswith(f"_{n}_SW"):
            return f"ch{n}"
    return "shared"


PLC_BUTTONS: tuple = tuple(PlcButton(n.lower(), n, _button_label(n), _button_chamber(n))
                           for n in PM.COIL_MAP if n.endswith("_SW") or n.startswith("SW_"))
POWER_SELECT_COIL = "SW_POWER_SELECT"


# ───────────── 명령 정의 ─────────────
@dataclass(frozen=True)
class CommandDef:
    kind: str
    label: str
    target: str
    sources: tuple
    states: tuple = (ANY,)
    requires: tuple = ()
    args: tuple = ()
    desc: str = ""
    v1: str = ""
    role: str = "operator"
    portal_only: bool = False
    web_only: bool = False
    bench_ok: bool = False

    def public(self) -> dict:
        return {"kind": self.kind, "label": self.label, "target": self.target,
                "sources": [s.value for s in self.sources], "portalOnly": self.portal_only, "webOnly": self.web_only,
                "role": self.role, "states": list(self.states), "requires": list(self.requires),
                "args": [{"name": a.name, "type": a.type, "required": a.required, "min": a.min, "max": a.max,
                          "paramMin": a.param_min, "paramMax": a.param_max, "enum": list(a.enum) if a.enum else None,
                          "desc": a.desc} for a in self.args],
                "desc": self.desc, "v1": self.v1, "benchOk": self.bench_ok}


def _a(name, type_, required=True, **kw) -> ArgDef:
    return ArgDef(name=name, type=type_, required=required, **kw)


CH = _a("ch", "int", min=1, max=2, desc="챔버 1|2")
CH_OPT = _a("ch", "int", False, min=1, max=2, desc="채널 1|2(비우면 지금 선택)")
CH2_ONLY = _a("ch", "int", False, min=2, max=2, desc="2(CH2 — 비우면 2)")
ADJ_DEVICES = ("mfc", "dc", "dc2", "rf", "dcp", "rfp", "heater", "process")
C = CommandDef
IDLE = PS.IDLE_STATES
ACT = PS.ACTIVE_STATES

# 로봇 명령(v1 host/setup.py:49-66 등록 순서) — RobotOps 가 v1 handlers 그대로 처리한다
ROBOT_COMMANDS: Tuple[str, ...] = (
    "GET_SPUTTER_STATUS", "GET_LOADING_1_SENSOR", "GET_LOADING_2_SENSOR", "GET_RECIPE", "START_SPUTTER",
    "START_PLASMA_CLEANING", "VACUUM_ON", "VACUUM_OFF", "4PIN_UP", "4PIN_DOWN", "CH1_GATE_OPEN", "CH2_GATE_OPEN",
    "CH1_GATE_CLOSE", "CH2_GATE_CLOSE", "CH1_CHUCK_UP", "CH2_CHUCK_UP", "CH1_CHUCK_DOWN", "CH2_CHUCK_DOWN",
)
ROBOT_READS = ("GET_SPUTTER_STATUS", "GET_LOADING_1_SENSOR", "GET_LOADING_2_SENSOR", "GET_RECIPE")
_ROBOT_ARGS = (_a("data", "obj", False, desc="요청 data(정규화된 명령의 인자)"), _a("raw", "obj", False, desc="요청 JSON 전체"),
               _a("peer", "str", False), _a("received_at", "any", False), _a("request_id", "str", False))

COMMANDS: tuple = (
    # ── 정지(로컬 화면에서도 늘) ──
    C("PROCESS_STOP", "챔버 STOP", "chamber", BOTH, (ANY,), (), (CH,),
      "어떤 상태든 그 챔버 Runner STOP(대기 행·쿨다운·큐 자동 진행까지 멈춤 — v1 request_stop_all). 공정이 없으면 정리만",
      "ch1_Stop_button·ch2_Stop_button → runtime/chamber_runtime.py:4497-4498"),
    C("ALL_STOP", "ALL STOP", "shared", (WEB, HMI, INT), (ANY,), (), (),
      "CH1 STOP + CH2 STOP + 플라즈마 클리닝 STOP + TSP STOP 을 한꺼번에(결정 D05 — 각각 v1 정지 순서 그대로, 새 판정 없음)",
      "v2(D05) — v1 은 챔버별 Stop 만"),
    C("PC_STOP", "플라즈마 클리닝 STOP", "pc", BOTH, (ANY,), (), (), "실행 중이 아니면 무시(v1 '[STOP] 실행 중이 아님')",
      "PC_Stop_button → plasma_cleaning_runtime.py _on_click_stop"),
    C("TSP_STOP", "TSP STOP", "tsp", BOTH, (ANY,), (), (), "예약은 건드리지 않음", "TSP_Stop_button → tsp_runtime.py on_stop_clicked"),
    # ── 챔버 공정 ──
    C("PROCESS_START", "공정 시작", "chamber", BOTH, IDLE, ("NO_COOLDOWN", "CHAMBER_IDLE", "PC_SAME_CH_IDLE", "RECIPE_LOADED"),
      (CH, _a("params", "obj", False, desc="단일 공정 화면 값(v1 칸 → 정규화) — 레시피가 적재돼 있으면 무시(v1 START_QUEUE)")),
      "적재된 레시피(큐) 또는 params 단일 공정", "ch1_Start_button·ch2_Start_button → _handle_start_clicked",
      portal_only=True),
    C("RECIPE_LOAD", "레시피 적재", "chamber", BOTH, IDLE, ("NOT_BUSY",),
      (CH, _a("rows", "rows", False, desc="v1 행 목록(recipe.RECIPE_COLUMNS)"), _a("blocks", "rows", False, desc="블록 목록"),
       _a("path", "str", False, desc="CSV/XLSX 파일 경로(v1 Select File)"), _a("name", "str", False)),
      "rows·blocks·path 중 하나(E230 행 값 검사 — v1 같게)", "ch1/ch2_processList_button → chamber_runtime.py:3359-3467",
      portal_only=True),
    C("RECIPE_CLEAR", "레시피 적재 해제", "chamber", BOTH, IDLE, (), (CH,), "공정 중이면 아무것도 하지 않음", "v2", portal_only=True),
    C("PROCESS_ADJUST", "공정 중 값 수정", "chamber", BOTH, ACT, ("RUNNING",),
      (CH, _a("device", "enum", enum=ADJ_DEVICES), _a("item", "str"), _a("value", "any", desc="blocks.ADJUST_ITEMS 의 형식·범위")),
      "지금 블록이면 바로 쓰고 되읽어 확인, 뒤 블록은 예약(챔버K 같게) — 로봇 제외", "v2(지시 3장 (3))"),
    C("BLOCK_SKIP", "블록 건너뛰기", "chamber", BOTH, ACT, ("RUNNING",), (CH,), "블록 정의 skippable 일 때만", "v2(지시 3장 (3))"),
    C("PROCESS_PAUSE", "일시정지", "chamber", BOTH, ACT, ("RUNNING",), (CH,), "블록 정의 pausable 일 때만", "v2(지시 3장 (3))"),
    C("PROCESS_RESUME", "일시정지 재개", "chamber", BOTH, ACT, ("RUNNING",), (CH,), "", "v2(지시 3장 (3))"),
    C("SEQUENCE_EDIT", "시퀀스 편집", "chamber", BOTH, (ANY,), (),
      (CH, _a("op", "obj", desc="{op: insert|delete|move, at·block / at / from·to} — 아직 시작 안 한 블록만")),
      "편집마다 레시피 판 +1·전체 재검사·원래 레시피와 실행 순서 기록", "v2(지시 3장 (3))", portal_only=True),
    C("CHAMBER_ERROR_CLEAR", "챔버 오류 표시 해제", "chamber", BOTH, (ANY,), (), (CH,),
      "오류 팝업을 닫음(확인/X/ESC 무관) → runtime_state 오류 해제(Idle 표시)", "chamber_runtime.py:8487-8525 · TEST-088"),
    C("PRESPUTTER_RESERVE", "프리스퍼터 예약", "chamber", BOTH, (ANY,), (), (CH, _a("time", "str", desc="HH:MM('8시30분' 허용)")),
      "그 챔버 매일 예약 등록/갱신", "preSputter Reserve → main.py:1812-1818"),
    C("PRESPUTTER_CANCEL", "프리스퍼터 예약 취소", "chamber", BOTH, (ANY,), (), (CH,), "진행 중 공정은 그대로",
      "preSputter Cancel → main.py:1820-1826"),
    C("PRESPUTTER_RECIPE", "프리스퍼터 레시피", "chamber", BOTH, (ANY,), (), (CH, _a("path", "str", False, desc="비우면 해제")),
      "예약 공정에 쓸 레시피 파일", "preSputter Recipe → main.py:1710-1758", portal_only=True),
    # ── 플라즈마 클리닝 ──
    C("PC_START", "플라즈마 클리닝 시작", "pc", BOTH, ("pc.idle",), ("PC_START_GUARD",),
      (_a("params", "obj", False, desc="화면 값(pc_recipe.UI_KEYS)"), _a("rows", "rows", False, desc="CSV 행"),
       _a("path", "str", False, desc="CSV 경로"), CH_OPT),
      "v1 Start 와 같은 가드(E205·쿨다운·같은 CH 챔버 바쁨)", "PC_Start_button → plasma_cleaning_runtime.py:1235-1275",
      portal_only=True),
    C("PC_SELECT_CH", "플라즈마 클리닝 채널", "pc", BOTH, (ANY,), ("PC_IDLE",), (CH,),
      "실행 중이면 거부·이전 선택 유지", "PC 라디오 → main.py:825-834·856-901"),
    C("PC_RECIPE_LOAD", "플라즈마 클리닝 레시피 불러오기", "pc", BOTH, (ANY,), (),
      (_a("rows", "rows", False), _a("path", "str", False)), "첫 비어 있지 않은 행을 화면 값으로(시작하지 않음)",
      "PC_processList_button → PC-087", portal_only=True),
    C("PC_ERROR_CLEAR", "플라즈마 클리닝 오류 표시 해제", "pc", BOTH, (ANY,), (), (CH_OPT,), "치명 팝업 닫기", "PC-086·TEST-088"),
    C("POWER_SELECT", "Power Select", "shared", BOTH, (ANY,), ("PLC_LINK_UP",), (_a("on", "bool"),),
      "SW_POWER_SELECT(코일 215) 쓰기 — 실패면 v1 문구로 LINK_DOWN", "Power_Select_button → main.py:1241-1255"),
    # ── TSP ──
    C("TSP_START", "TSP 시작", "tsp", BOTH, ("tsp.idle",), ("TSP_START_GUARD",),
      (_a("target", "any", False, desc="목표 압력 Torr(글자 또는 수)"), _a("cycles", "any", False, desc="사이클 수")),
      "", "TSP_Start_button → tsp_runtime.py:394-454", portal_only=True),
    C("TSP_SCHEDULE", "TSP 매일 예약", "tsp", BOTH, (ANY,), (),
      (_a("on", "bool"), _a("time", "str", False, desc="HH:MM")), "켜기/끄기·시각 — 설정 폴더 tsp_schedule.json(D90)",
      "TSP 예약 체크박스·시각 → tsp_runtime.py:111-132"),
    # ── CH2 히터 패널 ──
    C("HEATER_ONOFF", "히터 ON/OFF", "heater", BOTH, (ANY,), ("HEATER_SUPPORTED",), (_a("target", "any", False, desc="목표 °C"),),
      "RUN 이면 끄기('정지(패널)'), 아니면 목표 검사 후 켜기", "ui/heater_panel.py:370-380"),
    C("HEATER_APPLY", "히터 목표 적용", "heater", BOTH, (ANY,), ("HEATER_SUPPORTED",), (_a("target", "any", desc="목표 °C"),),
      "공정 잠금 없이 목표만(v1 그대로 — HEAT-114)", "ui/heater_panel.py:382-387"),
    C("HEATER_RESET", "히터 이상 리셋", "heater", BOTH, (ANY,), ("HEATER_SUPPORTED",), (),
      "RUN 이면 거부(확인창은 화면)", "ui/heater_panel.py:389-401"),
    # ── 두께 모니터 ──
    C("THICKNESS_ZERO", "두께 영점", "thickness", BOTH, (ANY,), ("THICKNESS_SUPPORTED", "THICKNESS_WRITABLE"), (CH2_ONLY,),
      "공정 중이면 거부(v1 패널)", "ui/thickness_panel.py:318-357"),
    C("THICKNESS_FACTOR_SAVE", "두께 Factor 저장", "thickness", BOTH, (ANY,), ("THICKNESS_SUPPORTED",),
      (CH2_ONLY, _a("tooling", "any", desc="tooling 칸(9칸)"), _a("materials", "any", desc="건별 재료 이름")),
      "검사 통과 시 원자적 저장(장비에 쓰지 않음)", "ui/thickness_factor_dialog.py:306-344", portal_only=True),
    C("THICKNESS_FACTOR_WRITE", "두께 Factor 장비에 쓰기", "thickness", BOTH, (ANY,), ("THICKNESS_SUPPORTED",
                                                                                   "THICKNESS_WRITABLE"),
      (CH2_ONLY, _a("chuck", "str"), _a("gun", "str"), _a("tooling", "any"), _a("material", "any")),
      "고른 chuck×건 tooling + 그 건 재료 density·Z 를 set_material(why 'Factor 창 수동') 뒤 되읽기", "thickness_factor_dialog"),
    C("THICKNESS_MATERIALS_SAVE", "두께 재료 목록 저장", "thickness", BOTH, (ANY,), ("THICKNESS_SUPPORTED",),
      (CH2_ONLY, _a("rows", "rows"), _a("used_names", "rows", False)),
      "density 0.500~99.99·Z 0.100~9.999·이름 중복 검사 뒤 원자적 저장", "ui/thickness_material_dialog.py:117-163", portal_only=True),
    C("THICKNESS_RELOAD", "두께 Factor 다시 읽기", "thickness", BOTH, (ANY,), ("THICKNESS_SUPPORTED",), (CH2_ONLY,),
      "", "thickness_service.reload_factors"),
    # ── 공유·프로그램 ──
    C("PLC_BUTTON", "PLC 버튼", "shared", BOTH, (ANY,), ("PLC_LINK_UP",),
      (_a("target", "enum", enum=tuple(b.target for b in PLC_BUTTONS)), _a("on", "bool")),
      "v1 PLC 스위치 코일 쓰기(write_switch). 인터락은 PLC 래더. 공정 중 실행은 기록(PLC_MANUAL_IN_PROCESS)",
      "device/plc.py:1747-1830 PLC 명령(door·gate·shutter·vent·turbo·pin·gas·select …)"),
    C("BUZZ_STOP", "부저 정지", "shared", BOTH, (ANY,), ("PLC_LINK_UP",), (),
      "BUZZER_STOP_SW(M01000) 켬 — v1 PlcDriver.buzzer_stop 과 같은 동작(PLC_BUTTON buzzer_stop_sw 와 같은 코일)",
      "device/plc.py:1810-1811 buzzer_stop(v1 화면 버튼 없음 — PLC API)"),
    C("RUNTIME_DUMP", "런타임 덤프", "shared", BOTH, (ANY,), (), (_a("reason", "str", False),),
      "LOG_ROOT_DIR\\RUNTIME_DUMP 에 상태·스레드 덤프(동시에 1개)", "RuntimeDump_button → main.py:841-844·1170-1207"),
    C("LOG_CLEAR", "로그 지우기(화면)", "shared", BOTH, (ANY,), (), (_a("target", "str", False),),
      "화면 로그만 비움 — 장비·기록 동작 없음(이벤트로 화면에 알림)", "server_page 'Clear'(화면만)"),
    C("CONFIG_APPLY", "설정 적용", "system", (WEB,), (ANY,), (),
      (_a("version", "int", min=1), _a("values", "obj"), _a("schemaHash", "str")),
      "config_apply.ConfigApply.apply — 결과 config_ack", "v2(챔버K)", role="admin", web_only=True),
    C("CONFIG_ROLLBACK", "이전 설정으로 되돌리기", "system", (WEB,), (ANY,), (), (_a("version", "int", False, min=1),),
      "ConfigStore.rollback", "v2(챔버K)", role="admin", web_only=True),
    # ── 내부(안전 엔진 조치) ──
    C("PROCESS_ABORT", "공정 중단", "chamber", (INT,), (ANY,), (), (CH, _a("reason", "str")), "엔진 abort(정지 순서 고정)",
      role="system"),
    C("POWER_OFF_ONLY", "파워 출력만 끄기", "shared", (INT,), (ANY,), (), (_a("reason", "str", False),),
      "의도적 종료 — DC·RF·DC Pulse·RF Pulse OFF, 히터 RUN=0, PLC 릴레이·MFC 그대로(챔버K 결정 4)", role="system"),
    C("RESIDUAL_POWER_OFF", "남은 파워 끄기", "shared", (INT,), (ANY,), (), (),
      "프로그램 시작 때 공정이 없는데 파워가 남아 있으면 파워만(S51)", role="system"),
    C("NOTIFY", "이벤트·알람", "shared", (INT,), (ANY,), (), (_a("level", "str"), _a("code", "str")), role="system"),
) + tuple(C(f"ROBOT_{c}", f"로봇 {c}", "robot", (ROBOT,), (ANY,), (), _ROBOT_ARGS,
            "RobotOps(v1 host/handlers.py 그대로) — 응답 body 는 v1 형식 그대로(CmdResult.value)",
            f"host/setup.py {c}", role="system") for c in ROBOT_COMMANDS)

# bench 에서도 받는 명령(장비에 쓰지 않음) — 이 목록 한 곳만(core·묶음 benchOk 가 같은 값을 본다)
_BENCH_OK = frozenset({"RECIPE_LOAD", "RECIPE_CLEAR", "SEQUENCE_EDIT", "CHAMBER_ERROR_CLEAR", "PRESPUTTER_RECIPE",
                       "PC_RECIPE_LOAD", "PC_ERROR_CLEAR", "THICKNESS_FACTOR_SAVE", "THICKNESS_MATERIALS_SAVE",
                       "THICKNESS_RELOAD", "RUNTIME_DUMP", "LOG_CLEAR", "CONFIG_APPLY", "CONFIG_ROLLBACK", "NOTIFY"}
                      | {f"ROBOT_{c}" for c in ROBOT_READS})
assert _BENCH_OK <= {c.kind for c in COMMANDS}
COMMANDS = tuple(replace(c, bench_ok=c.kind in _BENCH_OK) for c in COMMANDS)
_BY_KIND: Dict[str, CommandDef] = {c.kind: c for c in COMMANDS}
assert len(_BY_KIND) == len(COMMANDS)


def by_kind() -> Dict[str, CommandDef]:
    return dict(_BY_KIND)


def get(kind: str) -> Optional[CommandDef]:
    return _BY_KIND.get(kind)


def allowed_sources(c: CommandDef, cfg: Optional[Mapping[str, Any]] = None) -> tuple:
    """설정에 따른 지금 출처 — COMMAND_SOURCES_LOCAL_ALL=false 면 portal_only 명령에서 로컬 화면(hmi)을 뺀다."""
    local_all = True if cfg is None else bool(cfg.get(LOCAL_ALL_KEY, True))
    if c.portal_only and not local_all:
        return tuple(s for s in c.sources if s is not HMI)
    return c.sources


def kinds_for(source: Source, cfg: Optional[Mapping[str, Any]] = None) -> list:
    return [c.kind for c in COMMANDS if source in allowed_sources(c, cfg)]


def check_command(kind: str, source: Source, state: str, cfg: Optional[Mapping[str, Any]] = None) -> Optional[str]:
    """표만으로 판정하는 거부 사유(모름·출처·상태). 통과면 None."""
    c = _BY_KIND.get(kind)
    if c is None:
        return "UNKNOWN_COMMAND"
    if source not in allowed_sources(c, cfg):
        return "SOURCE_NOT_ALLOWED"
    if ANY not in c.states and state not in c.states:
        return "STATE_NOT_ALLOWED"
    return None


def _num(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


def check_args(c: CommandDef, args: Mapping[str, Any], cfg: Mapping[str, Any]) -> Optional[str]:
    """인자 검사(챔버K check_args 와 같은 규칙) — 틀리면 사람이 읽는 문구(core 가 BAD_ARGS + detail), 통과면 None.
    required(None·빠짐은 없는 것) · float/int(bool 아닌 유한 숫자, int 는 정수값) · bool · str · enum · obj(dict) · rows(list) ·
    any(통과) · min/max · paramMin/paramMax(그 설정의 지금 값). 모르는 인자는 무시."""
    for a in c.args:
        v = args.get(a.name)
        if v is None:
            if a.required:
                return f"{a.name} — 값 필요"
            continue
        t = a.type
        if t in ("float", "int"):
            if not _num(v):
                return f"{a.name} {v!r} — 숫자여야 함"
            if t == "int" and not float(v).is_integer():
                return f"{a.name} {v!r} — 정수여야 함"
        elif t == "bool" and not isinstance(v, bool):
            return f"{a.name} {v!r} — true/false 여야 함"
        elif t == "str" and not isinstance(v, str):
            return f"{a.name} {v!r} — 문자열이어야 함"
        elif t == "enum" and not (isinstance(v, str) and v in (a.enum or ())):
            return f"{a.name} {v!r} — 허용 값 중 하나여야 함({len(a.enum or ())}개)"
        elif t == "obj" and not isinstance(v, dict):
            return f"{a.name} — 객체(dict)여야 함"
        elif t == "rows" and not isinstance(v, list):
            return f"{a.name} — 목록이어야 함"
        if t not in ("float", "int"):
            continue
        lo = float(cfg[a.param_min]) if a.param_min else a.min
        hi = float(cfg[a.param_max]) if a.param_max else a.max
        if (lo is None or v >= lo) and (hi is None or v <= hi):
            continue
        if hi is None:
            return f"{a.name} {v:g} — {lo:g} 이상"
        if lo is None:
            return f"{a.name} {v:g} — {hi:g} 이하"
        return f"{a.name} {v:g} — 범위 {lo:g} ~ {hi:g}"
    return None


def public() -> Dict[str, Any]:
    """웹 묶음 commands 부분(web_schema.commands_part) — {commands, preconditions, reasons, plcButtons}."""
    return {"commands": [c.public() for c in COMMANDS],
            "preconditions": [p.public() for p in PRECONDITIONS],
            "reasons": [{"code": r.code, "label": r.label, "category": r.category} for r in ALL_REASONS],
            "plcButtons": [b.public() for b in PLC_BUTTONS],
            "localAllKey": LOCAL_ALL_KEY}
