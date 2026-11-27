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
from v2.ch12 import plc_labels as PL
from v2.ch12 import process_states as PS
from v2.ch12.drivers import plc_map as PM

WEB, HMI, ROBOT, INT = Source.WEB, Source.HMI, Source.ROBOT, Source.INTERNAL
BOTH = (WEB, HMI)
ANY = "*"
LOCAL_ALL_KEY = "COMMAND_SOURCES_LOCAL_ALL"

# ───────────── 사유 코드(챔버12) — 공통(COMMON_REASONS) 위에. v1 오류 코드 E### 는 errors.py 그대로(여기 없음) ─────────────
CH12_REASONS: tuple = (
    Reason("CORE_NOT_READY", "명령 처리 준비 전", "reject"),
    Reason("COOLDOWN", "공정 사이 대기 중", "reject"),
    Reason("RUNNER_BUSY", "이전 공정 정리 중", "reject"),
    Reason("BUSY", "공정 중이라 레시피를 바꿀 수 없음", "reject"),
    Reason("RESTART_PENDING", "재시작 뒤 이어가기 판단 중", "reject"),
    Reason("NO_RECIPE", "불러온 레시피 없음", "reject"),
    Reason("RECIPE_EMPTY", "레시피에 공정이 없음", "reject"),
    Reason("RECIPE_INVALID", "레시피 검사 실패", "reject"),
    Reason("E230", "레시피 값 오류", "reject"),
    Reason("PROCESS_RUNNING", "공정 진행 중", "reject"),
    Reason("PROCESS_NOT_RUNNING", "진행 중인 공정 없음", "reject"),
    Reason("CH1_PROCESS_RUNNING", "CH1 공정 중이라 TSP 시작 불가", "reject"),
    Reason("PC_RUNNING", "같은 채널 Plasma Cleaning 중", "reject"),
    Reason("CHAMBER_RUNNING", "같은 채널 공정 중", "reject"),
    Reason("PULSE_PORT_BUSY", "펄스 장비를 다른 챔버가 사용 중", "reject"),
    Reason("NOT_SUPPORTED", "이 챔버에 없는 장비", "reject"),
    Reason("TIME_FORMAT", "시간 형식 오류", "reject"),
    Reason("SKIP_NOT_ALLOWED", "건너뛸 수 없는 단계", "reject"),
    Reason("PAUSE_NOT_ALLOWED", "멈출 수 없는 단계", "reject"),
    Reason("PAUSE_NOT_AVAILABLE", "이 단계는 일시정지 준비 전", "reject"),
    Reason("NOT_PAUSED", "일시정지 상태 아님", "reject"),
    Reason("ADJUST_NOT_USED", "이번 공정에서 쓰지 않는 항목", "reject"),
    Reason("ADJUST_OUT_OF_RANGE", "바꿀 값이 범위 밖", "reject"),
    Reason("ADJUST_NOT_APPLICABLE", "지금 단계에 해당 없음", "reject"),
    Reason("ADJUST_GAS_STEP", "가스 단계 진행 중", "reject"),
    Reason("EDIT_BAD_OP", "순서 편집 형식 오류", "reject"),
    Reason("EDIT_STARTED_BLOCK", "이미 시작한 단계는 편집 불가", "reject"),
    Reason("EDIT_INVALID", "편집 뒤 레시피 검사 실패", "reject"),
    Reason("CHANNEL_SWITCH_RUNNING", "진행 중이라 채널 전환 불가", "reject"),
    Reason("THICKNESS_BLOCKED", "두께 모니터에 쓸 수 없음", "reject"),
    Reason("HEATER_REJECTED", "히터 명령 거부", "reject"),
    Reason("DUMP_BUSY", "런타임 덤프 진행 중", "reject"),
    Reason("ROBOT_NOT_READY", "로봇 명령 처리 준비 전", "reject"),
    # 공정 끝·중단(run_end 사유 — 엔진이 낸다)
    Reason("PROCESS_FAILED", "공정 실패", "abort"),
    Reason("PREFLIGHT_FAILED", "시작 전 점검 실패", "abort"),
    Reason("GATE_CHECK_FAILED", "Gate 닫힘 확인 실패", "abort"),
    Reason("MAIN_VALVE_CHECK_FAILED", "MV 확인 실패", "abort"),
    Reason("RECIPE_PARAM_ERROR", "레시피 값 검사 실패", "abort"),
    Reason("PROGRAM_EXIT", "프로그램 종료", "abort"),
    Reason("PROGRAM_RESTART", "재시작 뒤 이어가기 불가", "abort"),
    Reason("PLC_COMM_LOST", "PLC 통신 끊김", "abort"),
    Reason("MFC_COMM_LOSS", "MFC 통신 끊김", "abort"),
    Reason("RFP_COMM_LOST", "RF Pulse 통신 끊김", "abort"),
    Reason("DCP_COMM_LOST", "DC Pulse 통신 끊김", "abort"),
    # 안전 엔진 알림
    Reason("RESIDUAL_POWER_OFF", "남은 파워 끄기", "info"),
    Reason("RESIDUAL_POWER_KEPT", "남은 파워 그대로 둠", "info"),
    Reason("POWER_OFF_ONLY", "파워 출력만 끄기", "info"),
    Reason("PLC_MANUAL_IN_PROCESS", "공정 중 PLC 버튼", "info"),
    Reason("PLC_BUTTON", "PLC 버튼", "info"),
    Reason("PLC_BUTTON_FAILED", "PLC 버튼 쓰기 실패", "fail"),
    Reason("LOG_CLEAR", "화면 로그 지우기", "info"),
)

# 사유 운전자용 설명(묶음 16 ② — 왜 생겼는지·무엇을 하면 되는지 한 문장, 70자 안). Reason.help 가 비면 이 표. 사유 code·category·
# 사건 message 는 그대로. 모든 사유가 help 를 가진다(시험 test_web_labels).
REASON_HELP: Dict[str, str] = {
    "UNKNOWN_COMMAND": "장비 프로그램이 모르는 명령입니다. 화면과 프로그램 버전을 확인하세요.",
    "BAD_ARGS": "보낸 값의 형식이나 범위가 맞지 않습니다. 값을 확인하고 다시 보내세요.",
    "SOURCE_NOT_ALLOWED": "이 명령은 웹이나 노트북 중 한쪽에서만 할 수 있습니다.",
    "PERMISSION": "이 조작은 권한이 있는 계정만 할 수 있습니다.",
    "STATE_NOT_ALLOWED": "장비가 지금 상태에서는 이 명령을 받지 않습니다.",
    "EXPIRED": "너무 늦게 도착한 명령이라 실행하지 않았습니다. 다시 보내세요.",
    "COMMAND_EXPIRED": "장비 시계로 유효 시간이 지난 웹 명령이라 실행하지 않았습니다. 다시 보내세요.",
    "DUPLICATE": "같은 명령이 두 번 도착해 한 번만 처리했습니다.",
    "LINK_DOWN": "장비와 연결이 끊겨 명령을 보낼 수 없습니다. 케이블과 장비 전원을 확인하세요.",
    "DEVICE_DISABLED": "장비 설정에서 사용하지 않는 장비라 명령을 보내지 않았습니다.",
    "CONFIG_INVALID": "설정 값이 규칙에 맞지 않아 적용하지 않았습니다. 표시된 값을 고치세요.",
    "CONFIG_HASH_MISMATCH": "웹이 가진 설정 항목이 장비 프로그램과 다릅니다. 화면을 새로 고치세요.",
    "CONFIG_VERSION_STALE": "이미 적용한 버전보다 오래된 설정이라 받지 않았습니다.",
    "CONFIG_SECRET_FROM_WEB": "토큰 같은 비밀 값은 노트북 설정 파일에서만 바꿀 수 있습니다.",
    "CONFIG_LOCKED_KEY": "실행 모드와 장비 ID 는 노트북 설정 파일에서만 바꿀 수 있습니다.",
    "CONFIG_NO_PREVIOUS": "되돌릴 이전 설정 버전이 없습니다.",
    "DC_LAST_STATE_FULLY": "DC Power 가 전원이 돌아오면 출력까지 켜지게 되어 있습니다. 장비 앞면에서 바꾸세요.",
    "DC_PROTECTION_TRIPPED": "DC Power 과전압·과전류 보호가 걸려 출력이 나오지 않습니다. 장비 앞면을 확인하세요.",
    "MFC_SCALE_MISMATCH": "MFC 의 압력·유량 범위가 장비 설정과 달라 공정을 시작하지 않습니다.",
    "MFC_SCALE_UNREAD": "MFC 의 압력·유량 범위를 읽을 수 없어 장비 설정 값을 씁니다.",
    "COMMAND_BLOCKED": "장비 설정을 바꾸는 명령 등 허용 목록 밖이라 장비에 보내지 않았습니다.",
    "BENCH_READ_ONLY": "읽기 전용 모드라 장비에 쓰지 않습니다. 조작은 장비 앞면에서 하세요.",
    "QUEUE_FULL": "장비에 보낼 요청이 너무 많이 쌓였습니다. 잠시 뒤 다시 하세요.",
    "CANCELLED": "안전을 위한 요청이 먼저 실행되어 이 요청은 취소되었습니다.",
    "TIMEOUT": "정해진 시간 안에 끝나지 않았습니다.",
    "DEVICE_NO_RESPONSE": "장비가 응답하지 않습니다. 케이블과 장비 전원을 확인하세요.",
    "DEVICE_REJECTED": "장비가 명령을 받지 않았습니다. 장비 앞면의 상태를 확인하세요.",
    "VERIFY_MISMATCH": "장비에 쓴 값을 다시 읽었더니 달랐습니다. 장비 상태를 확인하세요.",
    "INTERNAL_ERROR": "프로그램 오류입니다. 로그 파일을 담당자에게 보내 주세요.",
    "USER_STOP": "사용자가 STOP 을 눌러 멈췄습니다.",
    "ALL_STOP": "사용자가 ALL STOP 을 눌러 모두 멈췄습니다.",
    "CORE_NOT_READY": "장비 프로그램이 아직 시작 중입니다. 잠시 뒤 다시 하세요.",
    "COOLDOWN": "이전 공정이 끝난 뒤 정해진 시간 동안 기다리는 중입니다.",
    "RUNNER_BUSY": "이전 공정의 정리가 아직 끝나지 않았습니다. 끝난 뒤 다시 하세요.",
    "BUSY": "공정이 진행 중이라 레시피를 바꿀 수 없습니다. 끝난 뒤 하세요.",
    "RESTART_PENDING": "프로그램을 다시 켠 뒤 이어가기를 판단하는 중이라 새로 시작할 수 없습니다.",
    "NO_RECIPE": "먼저 레시피를 불러오세요.",
    "RECIPE_EMPTY": "레시피에 실행할 공정 줄이 없습니다. 레시피를 확인하세요.",
    "RECIPE_INVALID": "값이 규칙에 맞지 않습니다. 레시피 화면에서 오류를 고치세요.",
    "E230": "레시피 줄의 값이 맞지 않습니다. 표시된 줄과 칸을 고치세요.",
    "PROCESS_RUNNING": "공정이 진행 중이라 할 수 없습니다.",
    "PROCESS_NOT_RUNNING": "진행 중인 공정이 없어 할 수 없습니다.",
    "CH1_PROCESS_RUNNING": "CH1 에서 공정이나 Plasma Cleaning 이 진행 중이라 TSP 를 시작할 수 없습니다.",
    "PC_RUNNING": "같은 채널에서 Plasma Cleaning 이 진행 중입니다. 끝난 뒤 하세요.",
    "CHAMBER_RUNNING": "같은 채널에서 챔버 공정이 진행 중입니다. 끝난 뒤 하세요.",
    "PULSE_PORT_BUSY": "다른 챔버가 펄스 파워 장비를 쓰는 중입니다. 그 공정이 끝난 뒤 하세요.",
    "NOT_SUPPORTED": "장비 설정에서 이 챔버에 없는 장비입니다.",
    "TIME_FORMAT": "시간은 08:30 처럼 시:분으로 넣으세요.",
    "SKIP_NOT_ALLOWED": "끄기·닫기·정상 끝 정지 단계는 건너뛸 수 없습니다.",
    "PAUSE_NOT_ALLOWED": "바로 끝나는 단계는 일시정지할 수 없습니다.",
    "PAUSE_NOT_AVAILABLE": "히터 유지 단계의 일시정지는 아직 쓸 수 없습니다.",
    "NOT_PAUSED": "일시정지한 공정이 없어 재개할 것이 없습니다.",
    "ADJUST_NOT_USED": "이번 공정에서 쓰지 않는 장비나 항목은 바꿀 수 없습니다.",
    "ADJUST_OUT_OF_RANGE": "레시피와 같은 범위 안의 값만 넣을 수 있습니다.",
    "ADJUST_NOT_APPLICABLE": "지금 단계에서는 이 값을 바꿀 수 없습니다.",
    "ADJUST_GAS_STEP": "같은 가스를 단계로 올리거나 넣는 중입니다. 끝난 뒤 다시 바꾸세요.",
    "EDIT_BAD_OP": "공정 순서 편집 요청의 형식이 맞지 않습니다.",
    "EDIT_STARTED_BLOCK": "이미 시작했거나 끝난 단계는 바꿀 수 없습니다.",
    "EDIT_INVALID": "바꾼 순서가 레시피 규칙에 맞지 않습니다. 표시된 오류를 고치세요.",
    "CHANNEL_SWITCH_RUNNING": "Plasma Cleaning 이 진행 중이라 챔버를 바꿀 수 없습니다.",
    "THICKNESS_BLOCKED": "공정 중이거나 두께 모니터가 응답하지 않아 쓸 수 없습니다.",
    "HEATER_REJECTED": "히터가 지금 상태에서 이 명령을 받지 않습니다. 히터 상태를 확인하세요.",
    "DUMP_BUSY": "런타임 덤프를 만드는 중입니다. 끝난 뒤 다시 하세요.",
    "ROBOT_NOT_READY": "로봇 명령을 받을 준비가 되지 않았습니다. 장비 프로그램을 확인하세요.",
    "PROCESS_FAILED": "공정이 오류로 멈췄습니다. 공정 기록에서 원인을 확인하세요.",
    "PREFLIGHT_FAILED": "시작 전 점검에서 장비 연결이나 상태가 맞지 않았습니다. 표시된 내용을 확인하세요.",
    "GATE_CHECK_FAILED": "Gate 가 닫혀 있지 않아 공정을 시작하지 않았습니다.",
    "MAIN_VALVE_CHECK_FAILED": "MV 인터락이나 열림을 확인하지 못해 공정을 시작하지 않았습니다.",
    "RECIPE_PARAM_ERROR": "시작 직전에 레시피 값이 규칙에 맞지 않았습니다. 레시피를 고치세요.",
    "PROGRAM_EXIT": "프로그램이 닫혀 파워 출력을 껐습니다. 펌프·밸브는 그대로입니다.",
    "PLC_COMM_LOST": "PLC 와 통신이 끊긴 채 이어져 공정을 멈췄습니다. 랜 케이블과 전원을 확인하세요.",
    "MFC_COMM_LOSS": "MFC 와 통신이 끊긴 채 이어져 공정을 멈췄습니다. 케이블과 전원을 확인하세요.",
    "RFP_COMM_LOST": "RF Pulse 와 통신이 끊겨 공정을 멈췄습니다. 케이블과 전원을 확인하세요.",
    "DCP_COMM_LOST": "DC Pulse 와 통신이 끊겨 공정을 멈췄습니다. 케이블과 전원을 확인하세요.",
    "PROGRAM_RESTART": "프로그램을 다시 켰지만 이어갈 조건이 맞지 않아 정지 순서를 실행했습니다.",
    "RESIDUAL_POWER_OFF": "시작할 때 파워 출력이 남아 있고 공정이 없어 파워만 껐습니다.",
    "RESIDUAL_POWER_KEPT": "시작할 때 파워가 남아 있지만 공정이 진행 중이라 끄지 않았습니다.",
    "POWER_OFF_ONLY": "프로그램을 닫을 때 파워 출력만 껐습니다. 펌프·밸브·MFC 는 그대로입니다.",
    "PLC_MANUAL_IN_PROCESS": "공정 중에 PLC 버튼을 눌렀습니다.",
    "PLC_BUTTON": "PLC 버튼을 눌렀습니다.",
    "PLC_BUTTON_FAILED": "PLC 가 응답하지 않거나 오류를 내 버튼을 쓰지 못했습니다.",
    "LOG_CLEAR": "화면의 로그만 지웠습니다. 장비 동작은 없습니다.",
}


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
    short: str = ""        # 로그·확인창 이름(코일 이름 없이 — 'CH1 벤트'·'건 셔터 G1'·'로드락 4핀 올림')
    proc_chamber: Optional[str] = None   # 이 버튼이 닿는 공정 챔버(ch1·ch2 — 공유 버튼은 그 장비를 쓰는 챔버, 로드락·부저는 없음)
    hazard_on: Optional[str] = None      # 켤 때 위험 분류 — person(사람)·equipment(장비)·None
    hazard_off: Optional[str] = None     # 끌 때 위험 분류
    desc: str = ""                       # 상세 보기 — 래더 이름·코일 번호(이름표에는 넣지 않음, 묶음 16)

    def hazard(self, on: bool) -> Optional[str]:
        return self.hazard_on if on else self.hazard_off

    def public(self) -> dict:
        return {"target": self.target, "name": self.name, "label": self.label, "chamber": self.chamber,
                "coil": PM.COIL_MAP[self.name], "short": self.short, "procChamber": self.proc_chamber,
                "hazard": {"on": self.hazard_on, "off": self.hazard_off}, "desc": self.desc}


def _button_label(name: str) -> str:
    """버튼 이름 = 그 코일의 스냅샷 필드 이름(plc_labels — 묶음 16 용어표). 래더 이름은 desc 로."""
    return PL.label(name)


def _button_desc(name: str) -> str:
    return f"{name} · 코일 {PM.COIL_MAP[name]}"


def _button_chamber(name: str) -> str:
    if name.startswith("L_"):
        return "loadlock"
    if name.startswith("SHUTTER_") or name.startswith("SW_") or name.startswith("BUZZER"):
        return "shared"
    for n in ("1", "2"):
        if f"_{n}_" in name or name.endswith(f"_{n}_SW"):
            return f"ch{n}"
    return "shared"


def _button_short(name: str, chamber: str) -> str:
    """로그·확인창 이름 = 버튼 이름 = 필드 이름(같은 대상 같은 이름 — 묶음 16). 예 'CH1 Vent'·'Gun1 셔터'·'Loadlock 4Pin 올림'."""
    return _button_label(name)


def _button_proc_chamber(name: str, chamber: str) -> Optional[str]:
    """공정 챔버 — 챔버 버튼은 그 챔버, 공유 버튼 중 공정이 쓰는 건 셔터(G1~G3)·Power Select(SW_RF_SELECT·SW_POWER_SELECT)는
    그 장비를 쓰는 CH2(블록 정의 — 건 셔터는 CH2 건, Power Select 블록·파워 켜기 SW_POWER_SELECT 는 CH2), 로드락·부저는 없음."""
    if chamber in ("ch1", "ch2"):
        return chamber
    if name.startswith("SHUTTER_") or name in ("SW_RF_SELECT", "SW_POWER_SELECT"):
        return "ch2"
    return None


def _button_hazard(name: str) -> Tuple[Optional[str], Optional[str]]:
    """(켤 때, 끌 때) 위험 분류(2026-11-20 D425). 사람 위험 = 벤트·도어·게이트(여닫기 모두 — 손 끼임)·로드락 핀·척 모터(정지 빼고),
    장비 위험 = 터보·로터리 끄기, 포어라인·러핑·메인 밸브(여닫기 모두), Power Select(SW_RF_SELECT·SW_POWER_SELECT 켜고 끄기 모두)."""
    if (name.startswith(("VENT_", "L_VENT", "DOOR_", "G_V_", "L_PIN_")) or
            (name.startswith("Z_M_P_") and "_STOP" not in name)):
        return "person", "person"
    if name.startswith(("TURBO_", "R_P_", "L_R_P_")):
        return None, "equipment"
    if name.startswith(("F_V_", "R_V_", "M_V_", "L_R_V_")) or name in ("SW_RF_SELECT", "SW_POWER_SELECT"):
        return "equipment", "equipment"
    return None, None


HAZARD_TEXT = {
    "person": "사람 위험: Vent·Door·Gate·Loadlock 4Pin·Chuck 은 사람이 다칠 수 있습니다. 장비 주변과 챔버·Loadlock 안에 "
              "사람이 없는지 반드시 확인하세요.",
    "equipment": "장비 위험: Turbo·Rotary 끄기, FV·RV·MV, Power Select·RF Select 를 잘못 바꾸면 펌프·밸브·파워 장비가 손상될 수 "
                 "있습니다. 지금 진공과 파워 상태를 확인하세요.",
    "rf_select_dcp": "지금 DC Pulse 사용 중입니다. RF Select 는 RF 전용 릴레이라 DC Pulse 를 쓰는 중에 켜면 특히 위험합니다. "
                     "DC Pulse 를 먼저 끄세요.",
}

PLC_BUTTONS: tuple = tuple(PlcButton(n.lower(), n, _button_label(n), _button_chamber(n),
                                     _button_short(n, _button_chamber(n)), _button_proc_chamber(n, _button_chamber(n)),
                                     *_button_hazard(n), desc=_button_desc(n))
                           for n in PM.COIL_MAP if n.endswith("_SW") or n.startswith("SW_"))
PLC_BY_TARGET: Dict[str, "PlcButton"] = {b.target: b for b in PLC_BUTTONS}
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
# 로봇 명령 화면 이름(묶음 16 — 명령 이름 GET_*·4PIN_* 는 kind·desc 에 그대로)
ROBOT_LABEL: Dict[str, str] = {
    "GET_SPUTTER_STATUS": "공정 상태 묻기", "GET_LOADING_1_SENSOR": "CH1 로딩 센서 읽기",
    "GET_LOADING_2_SENSOR": "CH2 로딩 센서 읽기", "GET_RECIPE": "레시피 목록 읽기", "START_SPUTTER": "공정 시작",
    "START_PLASMA_CLEANING": f"{PL.PC} 시작", "VACUUM_ON": "Loadlock 진공", "VACUUM_OFF": "Loadlock 대기압",
    "4PIN_UP": "4Pin 올림", "4PIN_DOWN": "4Pin 내림", "CH1_GATE_OPEN": "CH1 Gate 열기", "CH2_GATE_OPEN": "CH2 Gate 열기",
    "CH1_GATE_CLOSE": "CH1 Gate 닫기", "CH2_GATE_CLOSE": "CH2 Gate 닫기", "CH1_CHUCK_UP": "CH1 Chuck 올림",
    "CH2_CHUCK_UP": "CH2 Chuck 올림", "CH1_CHUCK_DOWN": "CH1 Chuck 내림", "CH2_CHUCK_DOWN": "CH2 Chuck 내림",
}
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
    C("PC_STOP", "Plasma Cleaning STOP", "pc", BOTH, (ANY,), (), (), "실행 중이 아니면 무시(v1 '[STOP] 실행 중이 아님')",
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
    C("BLOCK_SKIP", "지금 단계 건너뛰기", "chamber", BOTH, ACT, ("RUNNING",), (CH,), "블록 정의 skippable 일 때만", "v2(지시 3장 (3))"),
    C("PROCESS_PAUSE", "일시정지", "chamber", BOTH, ACT, ("RUNNING",), (CH,), "블록 정의 pausable 일 때만", "v2(지시 3장 (3))"),
    C("PROCESS_RESUME", "재개", "chamber", BOTH, ACT, ("RUNNING",), (CH,), "", "v2(지시 3장 (3))"),
    C("SEQUENCE_EDIT", "공정 순서 편집", "chamber", BOTH, (ANY,), (),
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
    C("PC_START", "Plasma Cleaning 시작", "pc", BOTH, ("pc.idle",), ("PC_START_GUARD",),
      (_a("params", "obj", False, desc="화면 값(pc_recipe.UI_KEYS)"), _a("rows", "rows", False, desc="CSV 행"),
       _a("path", "str", False, desc="CSV 경로"), CH_OPT),
      "v1 Start 와 같은 가드(E205·쿨다운·같은 CH 챔버 바쁨)", "PC_Start_button → plasma_cleaning_runtime.py:1235-1275",
      portal_only=True),
    C("PC_SELECT_CH", "Plasma Cleaning 채널", "pc", BOTH, (ANY,), ("PC_IDLE",), (CH,),
      "실행 중이면 거부·이전 선택 유지", "PC 라디오 → main.py:825-834·856-901"),
    C("PC_RECIPE_LOAD", "Plasma Cleaning 레시피 불러오기", "pc", BOTH, (ANY,), (),
      (_a("rows", "rows", False), _a("path", "str", False)), "첫 비어 있지 않은 행을 화면 값으로(시작하지 않음)",
      "PC_processList_button → PC-087", portal_only=True),
    C("PC_ERROR_CLEAR", "Plasma Cleaning 오류 표시 해제", "pc", BOTH, (ANY,), (), (CH_OPT,), "치명 팝업 닫기", "PC-086·TEST-088"),
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
    C("HEATER_ONOFF", "히터 켜기·끄기", "heater", BOTH, (ANY,), ("HEATER_SUPPORTED",), (_a("target", "any", False, desc="목표 °C"),),
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
    C("THICKNESS_WINDOW", "두께 Factor·재료 창 값 읽기", "thickness", BOTH, (ANY,), ("THICKNESS_SUPPORTED",),
      (CH2_ONLY, _a("req", "str", False, desc="화면 요청 표시(결과 사건에 그대로)")),
      "파일을 읽어 창에 보일 값(factor_window·materials_window)을 결과 value 와 사건 thickness/window 로(장비·파일 쓰기 없음)",
      "ui/thickness_factor_dialog.py:33-106·ui/thickness_material_dialog.py:20-90(창을 열 때 파일 읽기)"),
    # ── 공유·프로그램 ──
    C("PLC_BUTTON", "PLC 버튼", "shared", BOTH, (ANY,), ("PLC_LINK_UP",),
      (_a("target", "enum", enum=tuple(b.target for b in PLC_BUTTONS)), _a("on", "bool"),
       _a("by_name", "str", False, desc="웹 — 로그인한 사람 이름(웹 서버가 넣는다, 화면·기록용 — 계정은 requestedBy)")),
      "v1 PLC 스위치 코일 쓰기(write_switch). 인터락은 PLC 래더(공정 중에도 누를 수 있음 — 사용자 결정 2026-11-20). "
      "버튼마다 사건 plc_button 하나(버튼·켬/끔·누가·웹/노트북·결과·그 챔버 공정 상태 — D425), SYSTEM 일자 로그에 늘, 공정 중이면 그 챔버 공정 로그에도",
      "device/plc.py:1747-1830 PLC 명령(door·gate·shutter·vent·turbo·pin·gas·select …)"),
    C("BUZZ_STOP", "부저 정지", "shared", BOTH, (ANY,), ("PLC_LINK_UP",), (),
      "BUZZER_STOP_SW(M01000) 켬 — v1 PlcDriver.buzzer_stop 과 같은 동작(PLC_BUTTON buzzer_stop_sw 와 같은 코일)",
      "device/plc.py:1810-1811 buzzer_stop(v1 화면 버튼 없음 — PLC API)"),
    C("RUNTIME_DUMP", "런타임 덤프", "shared", BOTH, (ANY,), (), (_a("reason", "str", False),),
      "LOG_ROOT_DIR\\RUNTIME_DUMP 에 상태·스레드 덤프(동시에 1개)", "RuntimeDump_button → main.py:841-844·1170-1207"),
    C("LOG_CLEAR", "화면 로그 지우기", "shared", BOTH, (ANY,), (), (_a("target", "str", False),),
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
    C("NOTIFY", "사건·알람", "shared", (INT,), (ANY,), (), (_a("level", "str"), _a("code", "str")), role="system"),
) + tuple(C(f"ROBOT_{c}", f"로봇 {ROBOT_LABEL[c]}", "robot", (ROBOT,), (ANY,), (), _ROBOT_ARGS,
            "RobotOps(v1 host/handlers.py 그대로) — 응답 body 는 v1 형식 그대로(CmdResult.value)",
            f"host/setup.py {c}", role="system") for c in ROBOT_COMMANDS)

# bench 에서도 받는 명령(장비에 쓰지 않음) — 이 목록 한 곳만(core·묶음 benchOk 가 같은 값을 본다)
_BENCH_OK = frozenset({"RECIPE_LOAD", "RECIPE_CLEAR", "SEQUENCE_EDIT", "CHAMBER_ERROR_CLEAR", "PRESPUTTER_RECIPE",
                       "PC_RECIPE_LOAD", "PC_ERROR_CLEAR", "THICKNESS_FACTOR_SAVE", "THICKNESS_MATERIALS_SAVE",
                       "THICKNESS_RELOAD", "THICKNESS_WINDOW", "RUNTIME_DUMP", "LOG_CLEAR", "CONFIG_APPLY", "CONFIG_ROLLBACK", "NOTIFY"}
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
            "reasons": [{"code": r.code, "label": r.label, "category": r.category, "help": r.help or REASON_HELP.get(r.code, "")}
                        for r in ALL_REASONS],
            "plcButtons": [b.public() for b in PLC_BUTTONS],
            "plcHazardText": dict(HAZARD_TEXT),
            "localAllKey": LOCAL_ALL_KEY}
