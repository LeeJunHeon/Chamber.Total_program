"""챔버12 안전 규칙 표(S 번호) · STOP/ALL STOP 정지 순서 · 끝 정지 skip 금지 단계 · 의도적 종료(파워만) — 챔버K
v2/chk/safety_rules.py 와 같은 모양(Rule·Act·SeqStep, 평가 위치·종류).

원칙(사용자 지시 2장 3·3장 (4)(6)): 규칙은 v1 에 있는 것만(새 판정 규칙 금지). v1 의 공정 중단 조건(장비 실패·통신 실패·PLC 명령 실패·
DC Pulse AUTO_STOP·RF Pulse FORP/REFP·IG 실패·MFC 실패·두께 E848·히터 E830~E833 …)은 엔진·드라이버가 v1 처럼 이미 처리하므로
표에는 그 위치만 적는다(impl — safety.IMPLEMENTED_IN 과 같은 값). 안전 엔진(safety.Ch12Safety)이 직접 실행하는 것은 v1 의 상위
동작과 합의된 변경뿐이다: ALL STOP(D05), 프로그램 시작 때 남은 파워 정리(지시 3장 (6) — 챔버K S51), 의도적 종료 때 파워 출력만 끄기
(챔버K 결정 4). 가스 밸브는 PLC 래더 인터락이라 소프트웨어 규칙 없음(지시 4장), RF/DC 파워는 PLC 인터락이 없어 소프트웨어가 끈다.

평가 위치(where): plc250(PLC 빠른 폴링 — 챔버12 v1 에는 없음) · tick(1초 행) · event:<이름> · state:<상태> · command · config · worker
종류(kind): trip(조건 → 중단·OFF) · reject(명령·시작 거부) · notify(알림만) · recover(복구·시작 때 안전 상태) · maintain(늘 하는 동작) ·
  guard(다른 동작을 막거나 고정)
지속 시간(duration): v1 에서 실제로 걸리던 시간(파라미터 키 또는 설명). '0' = 즉시.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Tuple

from v2.ch12 import process_steps as PST


@dataclass(frozen=True)
class Act:
    kind: str
    args: dict = field(default_factory=dict)

    def public(self) -> dict:
        return {"kind": self.kind, "args": dict(self.args)}


@dataclass(frozen=True)
class Rule:
    id: str
    name: str
    kind: str
    where: str
    when: str
    engine: str                  # chamber · pc · tsp · heater · shared · robot
    impl: str                    # 실행 위치(모듈·함수)
    v1: str                      # v1 근거(파일:줄) 또는 합의(지시·결정)
    params: tuple = ()
    duration: str = "0"
    states: tuple = ("*",)
    actions: tuple = ()
    release: str = ""
    reason: str = ""             # 거부·중단 사유 코드(대표)

    def public(self) -> dict:
        return {"id": self.id, "name": self.name, "kind": self.kind, "where": self.where, "when": self.when,
                "engine": self.engine, "impl": self.impl, "v1": self.v1, "params": list(self.params),
                "duration": self.duration, "states": list(self.states), "actions": [a.public() for a in self.actions],
                "release": self.release, "reason": self.reason}


def _abort(code: str) -> Act:
    return Act("PROCESS_ABORT", {"reason": code})


def _rej(code: str) -> Act:
    return Act("START_REJECT", {"reason": code})


def _note(level: str, code: str) -> Act:
    return Act("NOTIFY", {"level": level, "code": code})


R = Rule
RUN = ("RUNNING",)
GATE_P = ("CHAMBER_GATE_RECHECK_COUNT", "CHAMBER_GATE_RECHECK_INTERVAL_S", "CHAMBER_GATE_READ_TIMEOUT_S")

RULES: Tuple[Rule, ...] = (
    # ── 챔버 시작 ──
    R("S1", "챔버 공정 시작 가드", "reject", "command",
      "쿨다운(이전 종료 뒤 60 s) → 러너 IDLE 아님 → 같은 CH 플라즈마 클리닝 실행 중 → 컨트롤러 실행 중이면 시작 거부(v1 순서·문구)",
      "chamber", "core.Ch12Core.start_chamber → ProcessEngine.start_guard + RunState(pc)",
      "runtime/chamber_runtime.py:4375-4416 _handle_start_clicked (PROC-061~066)", duration="60 s(COOLDOWN_S)",
      states=("IDLE",), actions=(_rej("COOLDOWN"), _rej("RUNNER_BUSY"), _rej("PC_RUNNING")), reason="COOLDOWN"),
    R("S2", "게이트 밸브 CLOSED 확인", "reject", "state:PREFLIGHT",
      "G_V_n_OPEN_LAMP·CLOSE_LAMP 가 CLOSED 일 때만 시작 — 둘 다 F·읽기 오류면 0.2 s 뒤 재확인 최대 5회", "chamber",
      "process_preflight.gate_check", "runtime/chamber_runtime.py:3621-3692·3990-4012 (PROC-099·100, 챗 E301)", GATE_P,
      "5회 × 0.2 s", ("PREFLIGHT",), (_rej("GATE_CHECK_FAILED"), _note("error", "E301")), reason="GATE_CHECK_FAILED"),
    R("S3", "Main Valve 인터락·열림 확인", "reject", "state:PREFLIGHT",
      "M_V_n_SW ∧ M_V_n_인터락 둘 다 참일 때만 시작(재확인 5회·0.2 s)", "chamber", "process_preflight.main_valve_check",
      "runtime/chamber_runtime.py:3700-3760·4013-4040 (PROC-101·102, 챗 E301)", GATE_P, "5회 × 0.2 s", ("PREFLIGHT",),
      (_rej("MAIN_VALVE_CHECK_FAILED"), _note("error", "E301")), reason="MAIN_VALVE_CHECK_FAILED"),
    R("S4", "시작 전 장비 연결 확인", "reject", "state:PREFLIGHT",
      "이번 런 장비(MFC·IG·고른 펄스 전원)가 한도 안에 연결 안 되면 '장비 연결 실패' — 시작 안 함", "chamber",
      "process_preflight(PreflightMixin)", "runtime/chamber_runtime.py:3929-3977 (UI-090)",
      ("CHAMBER_PREFLIGHT_TIMEOUT_S", "CHAMBER_PREFLIGHT_TIMEOUT_WITH_PULSE_S"), "CHAMBER_PREFLIGHT_TIMEOUT_S(8 s·펄스 10 s)",
      ("PREFLIGHT",), (_rej("PREFLIGHT_FAILED"),), reason="PREFLIGHT_FAILED"),
    R("S5", "펄스 전원 포트 점유", "reject", "state:PREFLIGHT",
      "고른 DC Pulse·RF Pulse 포트를 다른 쪽이 잡고 있으면 시작 거부(같은 시리얼 포트 — v1 은 화면에서 동시 선택 금지)", "chamber",
      "process_preflight → drivers/pulse_port.PulsePorts.claim", "runtime/chamber_runtime.py:5817-5962 (UI-071 같은 시리얼 포트)",
      states=("PREFLIGHT",), actions=(_rej("PULSE_PORT_BUSY"),), reason="PULSE_PORT_BUSY"),
    # ── 챔버 공정 중 중단(엔진이 v1 처럼) ──
    R("S6", "PLC 명령 실패 → 공정 중단", "trip", "event:plc_command_failed",
      "공정 스텝의 PLC 쓰기가 실패하면 지금 단계와 무관하게 중단(v1 대표 오류 코드)", "chamber",
      "process_actions._a_plc_cmd → StepRunner.step_failed", "controller/process_controller.py:865-877 (PROC-163)",
      states=RUN, actions=(_abort("PROCESS_FAILED"),), reason="PROCESS_FAILED"),
    R("S7", "MFC 명령 실패 → 공정 중단", "trip", "event:mfc_command_failed",
      "지금 기다리는 MFC 명령의 실패만 중단(다른 것은 '경고(무시)')", "chamber", "process_events(MFC) → StepRunner.mfc_failed",
      "controller/process_controller.py:838-860 (PROC-164)", states=RUN, actions=(_abort("PROCESS_FAILED"),)),
    R("S8", "IG 베이스 압력 실패 → 공정 중단", "trip", "event:ig_base_failed", "IG base_failed(도달 실패·두절)", "chamber",
      "process_events(IG) → StepRunner.step_failed", "runtime/chamber_runtime.py:2274-2284 (PROC-165)", states=RUN,
      actions=(_abort("PROCESS_FAILED"),)),
    R("S9", "DC 연속 과전류·출력 이상 → 공정 중단", "trip", "event:dc_target_failed",
      "측정 I > 한계(목표 W 기준) 연속 DC_OVERCURRENT_STREAK_N, 저전력 P ≤ 1 W 3회, 도달 뒤 저전류 3회 → target_failed", "chamber",
      "drivers/dc.py(판정) → process_events(DC) → step_failed", "device/dc_power.py:280-364 · runtime/chamber_runtime.py:2366-2390 "
      "(PROC-166·DEV-123)", ("DC_OVERCURRENT_STREAK_N", "DC_OVERCURRENT_I1_A", "DC_OVERCURRENT_P1_W"),
      "측정 주기 DC_INTERVAL_MS(5 s) × 연속 횟수", RUN, (_abort("PROCESS_FAILED"),)),
    R("S10", "RF 연속 출력 이상 → 공정 중단", "trip", "event:rf_target_failed",
      "blind 도달 시간·Ref.p 대기·저출력 3회 → target_failed", "chamber", "drivers/rf.py → process_events(RF) → step_failed",
      "device/rf_power.py · runtime/chamber_runtime.py:2483-2493 (PROC-167)", states=RUN, actions=(_abort("PROCESS_FAILED"),)),
    R("S11", "RF Pulse FORP·REFP·명령 실패 → 공정 중단", "trip", "event:rfpulse_command_failed",
      "순방향 이탈·반사파 초과 연속, START/SET/STOP 명령 실패(RF_OFF 제외 — OFF 실패 경로)", "chamber",
      "drivers/rfpulse(_poll) → process_events(RF Pulse) → step_failed",
      "device/rf_pulse.py · runtime/chamber_runtime.py:2588-2593 (PROC-168)",
      ("RFPULSE_FORP_TOLERANCE_PERCENT", "RFPULSE_FORP_CONSECUTIVE_LIMIT", "RFPULSE_REFP_LIMIT_WATTS",
       "RFPULSE_REFP_CONSECUTIVE_LIMIT"), "폴링 주기 × 연속 횟수(v1)", RUN, (_abort("PROCESS_FAILED"),)),
    R("S12", "DC Pulse AUTO_STOP·과전류·시작 실패 → 공정 중단", "trip", "event:dcpulse_command_failed",
      "AUTO_STOP/target_failed, 시작 시퀀스 실패, set_reference_power·output_off 예외(READ_*·FAULT_RESET·prepare 단계는 무시)",
      "chamber", "drivers/dcpulse(_monitor) → process_events(DC Pulse) → step_failed",
      "runtime/chamber_runtime.py:1160-1285·2694-2772 (PROC-169·170)", states=RUN, actions=(_abort("PROCESS_FAILED"),)),
    R("S13", "히터 이상 → 공정 중단(E830~E833)", "trip", "event:heater_failed",
      "히터 설정·대기 E830, 공정 중 E832, 유지 E833(히터 이상·TC·워치독·출력 없음)", "chamber",
      "heater_link(on_process) → process_actions._heater_result → step_failed",
      "controller/process_controller.py:1027-1035 (PROC-171) · controller/heater_service.py", states=RUN,
      actions=(_abort("E832"),)),
    R("S14", "두께 상한 → 공정 중단(E848)", "trip", "state:RUNNING",
      "두께 대기(THICKNESS_WAIT)가 상한 max(예상 × 비율, 예상 + 여유) 안에 목표에 못 닿음", "chamber",
      "thickness.wait_start(thickness_judge) → StepRunner", "controller/process_controller.py:1488-1613 (PROC-171)",
      ("CH2_THICKNESS_MAX_TIME_RATIO", "CH2_THICKNESS_MAX_TIME_MARGIN_S"), "max(예상 × RATIO, 예상 + MARGIN)", RUN,
      (_abort("E848"),)),
    R("S15", "파워 OFF 미확인 경고", "notify", "event:power_off_failed",
      "정지 순서의 DC·DC2·RF·DC Pulse·RF Pulse OFF 가 확인 안 됨 — '⚠️ … Power OFF 실패 - 출력 상태 미확인' 경고(정지 순서는 계속)",
      "chamber", "process_events(power_off_failed)", "runtime/chamber_runtime.py (PROC-157·158)",
      ("POWER_OFF_ZERO_DEADLINE_S", "PC_POWER_OFF_TIMEOUT_MS"), "POWER_OFF_ZERO_DEADLINE_S(10 s)",
      actions=(_note("error", "POWER_OFF_UNCONFIRMED"),)),
    # ── 통신 ──
    R("S16", "PLC 링크 끊김 알림", "notify", "event:link_down",
      "PLC 링크 down 전이 1회 카드·복구 카드 1회(끊김당 2장), 끊긴 동안 E401·E402 개별 카드 억제. 공정 중단은 하지 않는다 — "
      "공정은 다음 PLC 명령 실패(S6)에서 v1 처럼 중단", "shared", "drivers/plc.py(link_state) → system 이벤트(link_down·link_up)",
      "lib/link_state.py:8-140 · device/plc.py:1170-1214 · main.py:285-300 (DEV-001·003·032)",
      ("PLC_RECONNECT_BACKOFF_S",), "PLC_DISCONNECT_ALERT_AFTER_S(0 s)", actions=(_note("error", "PLC_LINK_DOWN"),),
      release="링크 복구 때 카드 1회(억제 건수 포함)"),
    # ── 플라즈마 클리닝·TSP ──
    R("S17", "플라즈마 클리닝 시작 가드", "reject", "command",
      "이미 실행 중 E205 → 쿨다운(PC 전역·같은 CH 챔버·CH1 이면 TSP 60 s) → 같은 CH PC 실행 중 E205 → 같은 CH 챔버 실행·바쁨 E205",
      "pc", "plasma_cleaning._click_start(+ chamber_busy = ProcessEngine.busy)",
      "runtime/plasma_cleaning_runtime.py:618-631·1235-1275 (PC-006·008~012)", duration="60 s",
      states=("pc.idle",), actions=(_rej("E205"),), reason="E205"),
    R("S18", "플라즈마 클리닝 GV 인터락", "reject", "state:pc.preflight",
      "G_V_n_인터락 읽기(0.6 s) FALSE·실패면 시작 안 함(E302·E511) — 켠 것이 없어도 정리 순서를 탄다(v1 그대로)", "pc",
      "pc_preflight", "runtime/plasma_cleaning_runtime.py (PC-025·026)", states=("pc.preflight",),
      actions=(_rej("E302"),), reason="E302"),
    R("S19", "플라즈마 클리닝 채널 전환 금지", "reject", "command", "실행 중 CH1/CH2 라디오 전환 거부·이전 선택 유지", "pc",
      "core(PC_SELECT_CH) → plasma_cleaning.select_channel", "main.py:825-834·856-901 (UI-034)", states=("pc.run",),
      actions=(_rej("CHANNEL_SWITCH_RUNNING"),), reason="CHANNEL_SWITCH_RUNNING"),
    R("S20", "TSP 시작 가드", "reject", "command",
      "실행 중 → CH1 챔버·CH1 PC 실행 중(CH2 는 막지 않음) → TSP 실행 중 → 쿨다운 TSP_COOLDOWN_S → 입력 파싱", "tsp",
      "tsp_engine.start", "runtime/tsp_runtime.py:394-454 (PC-123·124)", ("TSP_COOLDOWN_S",), "TSP_COOLDOWN_S(60 s)",
      ("tsp.idle",), (_rej("CH1_PROCESS_RUNNING"), _rej("COOLDOWN")), reason="CH1_PROCESS_RUNNING"),
    # ── CH2 히터 ──
    R("S21", "시작 때 남은 히터 RUN 정리", "recover", "event:plc_first_poll",
      "첫 PLC 읽기에서 RUN 이 켜져 있으면 RUN=0·SV=PV(이전 세션 흔적)", "heater", "heater_engine(첫 폴링 _clear_residual_run)",
      "controller/heater_service.py (HEAT — 첫 폴링 잔류 RUN)", actions=(Act("HEATER_RESIDUAL_CLEAR"),)),
    R("S22", "히터 워치독 하트비트", "maintain", "worker",
      "엔진이 D114 를 주기적으로 바꾼다 — 래더가 30 s 변화 없으면 WD_ERR 로 히터 차단(PLC 쪽 보호)", "heater",
      "heater_engine.on_tick_start(_wd)", "controller/heater_service.py · 지시 4장(D114 30 s)", ("CH2_HEATER_WD_PERIOD_S",),
      "30 s(래더)"),
    R("S23", "히터 리셋 보호", "reject", "command", "RUN 이 켜져 있으면 이상 리셋 거부('먼저 OFF')", "heater",
      "heater_commands.reset_fault", "controller/heater_service.py:417-431 (HEAT-031)", actions=(_rej("HEATER_REJECTED"),)),
    R("S24", "종료 때 히터 RUN=0", "maintain", "event:shutdown", "프로그램 종료 때 RUN 이고 링크 있으면 RUN=0 1회(안전 요청)", "heater",
      "assembly.HeaterHost.close → HeaterEngine.shutdown", "controller/heater_service.py:433-466",
      actions=(Act("HEATER_FORCE_OFF"),)),
    # ── 두께 ──
    R("S25", "두께 모니터 쓰기 금지", "reject", "command", "공정 중 > 미연결 > 응답 없음이면 영점·장비에 쓰기 거부", "chamber",
      "core(THICKNESS_*) → thickness_ops.cmd_zero·factor_write", "ui/thickness_panel.py:37-47 (THK-083·UI-115)",
      actions=(_rej("THICKNESS_BLOCKED"),), reason="THICKNESS_BLOCKED"),
    # ── PLC 래더 ──
    R("S26", "가스 밸브 인터락", "maintain", "worker", "가스 차단밸브 인터락은 PLC 래더가 처리 — 소프트웨어 규칙 없음", "shared",
      "PLC 래더(소프트웨어 없음)", "지시 4장 · device/plc.py *_GAS_인터락 코일(읽기만)"),
    R("S27", "Power Select 릴레이(SW_RF_SELECT)", "guard", "state:RUNNING",
      "RF 전용 릴레이 — 레시피 use_power_select 일 때만 켜고 끝 정지에서 끈다(DC Pulse 경로에는 켜지 않음, 따로 판정 없음)", "chamber",
      "process_sequence(_create_process_sequence)·process_steps.shutdown_steps(relay)",
      "controller/process_controller.py:2431-2435 · 지시 4장"),
    # ── 정지 순서 ──
    R("S28", "STOP·이상 중단 정지 순서 고정", "guard", "command",
      "STOP·이상 중단은 지금 구간 장비의 v1 정상 종료 절차 전체(STOP_SEQUENCE — 끝 정지 블록의 skip 은 적용하지 않음). "
      "공정 중 편집으로도 빠지지 않는다", "chamber", "ProcessEngine.shutdown_steps → process_steps.shutdown_steps(skip 없음)",
      "controller/process_controller.py:2737-2832 _create_shutdown_sequence · runtime/chamber_runtime.py:4775-4839 (PROC-141·148~157)",
      ("SHUTDOWN_STEP_TIMEOUT_MS", "SHUTDOWN_STEP_GAP_MS", "PC_POWER_OFF_TIMEOUT_MS"), "단계 2.5 s·사이 0.5 s·파워 OFF 240 s"),
    R("S29", "끝 정지 skip 금지 단계", "guard", "config",
      "정상 끝 정지(end_stop)의 skip 에 파워 끄기(power)·Power Select 릴레이(relay)는 넣을 수 없다(넣어도 무시)", "chamber",
      "blocks(end_stop skip enum) · process_steps.SHUTDOWN_MUST_KEEP", "결정 D32 · 챔버K END_STOP_MUST_KEEP",
      actions=(_rej("RECIPE_INVALID"),)),
    R("S30", "ALL STOP", "trip", "command",
      "CH1 STOP + CH2 STOP + 플라즈마 클리닝 STOP + TSP STOP 을 한꺼번에(각각 v1 정지 순서 그대로)", "shared", "safety.Ch12Safety.all_stop",
      "결정 D05 · v1 챔버별 request_stop_all(chamber_runtime.py:4497)", actions=(Act("ALL_STOP"),), reason="ALL_STOP"),
    # ── 프로그램 시작·종료 ──
    R("S31", "시작 때 남은 파워 정리", "recover", "event:first_read",
      "프로그램이 켜진 뒤 첫 정상 PLC 행에서 공정·운전(챔버·PC·TSP)이 없는데 PLC D/A 전원(CH2 DC1·DC2·CH2 RF·PC RF)의 SET 래치가 "
      "켜져 있거나 WRITE 값이 0 이 아니면 그 전원만 끈다(0 W 확인 → SET OFF). 펌프·밸브·셔터·가스는 건드리지 않는다. 공정 중이면 "
      "끄지 않고 알림만. bench 는 아무것도 쓰지 않는다. DC Pulse·RF Pulse 는 포트를 공정이 고를 때만 열어 시작 때 읽지 않는다",
      "shared", "safety.Ch12Safety.first_read", "지시 3장 (6) · 챔버K S51(사용자 확정 2026-10-20) — v1 에 없음(합의된 변경)",
      actions=(Act("RESIDUAL_POWER_OFF"), _note("warn", "RESIDUAL_POWER_OFF")), reason="RESIDUAL_POWER_OFF"),
    R("S32", "의도적 종료 — 파워 출력만 끄기", "maintain", "event:shutdown_forced",
      "콘솔 닫기·Ctrl+C·로그오프 때 공정을 멈추고(새 장비 명령 없음, run_end PROGRAM_EXIT) DC1·DC2·RF(CH2·PC)·DC Pulse·RF Pulse OFF·"
      "히터 RUN=0. PLC 릴레이(펌프·밸브·가스 차단밸브·셔터)·MFC 는 그대로", "shared", "safety.Ch12Safety.power_off_only",
      "지시 3장 (6) · 챔버K 결정 4(2026-10-20)", actions=(Act("POWER_OFF_ONLY"),), reason="PROGRAM_EXIT"),
    R("S33", "종료 차단", "reject", "event:shutdown_request",
      "공정·정지 순서·PC·TSP·히터 레시피가 busy 면 Ctrl+C 정상 종료 거부(콘솔에 이유)", "shared", "app.App.exit_check(system.busy)",
      "main.py:1120-1168·1487-1526 closeEvent (UI-042)", actions=(_rej("PROCESS_RUNNING"),)),
    # ── 명령 ──
    R("S34", "명령 출처·상태·인자 검사", "reject", "command",
      "명령 표의 출처(web/local/robot — COMMAND_SOURCES_LOCAL_ALL)·허용 상태·인자 형식·범위", "shared",
      "commands.check_command·check_args(core)", "지시 3장 (4)", ("COMMAND_SOURCES_LOCAL_ALL",),
      actions=(_rej("SOURCE_NOT_ALLOWED"), _rej("STATE_NOT_ALLOWED"), _rej("BAD_ARGS"))),
    R("S35", "bench 장비 쓰기 거부", "reject", "command", "bench(읽기 전용)에서 장비에 쓰는 명령은 BENCH_READ_ONLY", "shared",
      "core(benchOk) + 드라이버·worker 이중 보호", "v2(챔버K 5-14)", actions=(_rej("BENCH_READ_ONLY"),)),
    R("S36", "로봇 명령 인터락", "reject", "command",
      "게이트 상태·busy·챔버 잠금·Main Valve·로드락 전환 중 판정 — v1 handlers 그대로(E301·E302·E205·E70x …)", "robot",
      "robot_ops(robot_base·robot_status·robot_vacuum·robot_motion)", "host/handlers.py (ROB-*)"),
)

# 엔진·드라이버가 v1 처럼 이미 처리하는 규칙 — 실행 위치(시험이 표와 같은지 본다)
IMPLEMENTED_IN: Dict[str, str] = {r.id: r.impl for r in RULES}
ENGINE_RULES: Tuple[str, ...] = ("S30", "S31", "S32")     # 안전 엔진(safety.py)이 직접 실행


# ───────────── 정지 순서 ─────────────
@dataclass(frozen=True)
class SeqStep:
    no: int
    part: str                   # process_steps.SHUTDOWN_PARTS 이름(챔버) 또는 단계 이름
    label: str
    when: str = ""
    skippable: bool = False     # 정상 끝 정지(end_stop)에서 뺄 수 있나(STOP·이상 중단은 늘 전체)
    act: str = ""

    def public(self) -> dict:
        return {"no": self.no, "part": self.part, "label": self.label, "when": self.when, "skippable": self.skippable,
                "act": self.act}


_PART_SKIP = {p[0]: p[2] for p in PST.SHUTDOWN_PARTS}
# 챔버 STOP·이상 중단(v1 _create_shutdown_sequence 순서 — process_steps.shutdown_steps 와 같다). 단계마다 '지금 구간에서 쓴 장비'일 때만.
STOP_SEQUENCE: Tuple[SeqStep, ...] = (
    SeqStep(1, "ms_close", "Main Shutter 닫기(항상)", "", _PART_SKIP["ms_close"], "PLC MS OFF"),
    SeqStep(2, "power", "DC1 Power Off", "DC1 썼으면", False, "DC_POWER_STOP"),
    SeqStep(3, "power", "DC2 Power Off", "DC2 썼으면", False, "DC2_POWER_STOP"),
    SeqStep(4, "power", "RF Power Off(램프다운)", "RF 연속 썼으면", False, "RF_POWER_STOP"),
    SeqStep(5, "power", "DC Pulse Off", "DC Pulse 썼으면", False, "DC_PULSE_STOP"),
    SeqStep(6, "power", "RF Pulse Off(+CH2 SW_POWER_SELECT OFF)", "RF Pulse 썼으면", False, "RF_PULSE_STOP"),
    SeqStep(7, "flow_off", "MFC Flow Off(쓴 가스, 없으면 전부)", "", _PART_SKIP["flow_off"], "MFC FLOW_OFF"),
    SeqStep(8, "valve_open", "전체 MFC Valve Open", "", _PART_SKIP["valve_open"], "MFC VALVE_OPEN"),
    SeqStep(9, "gun_close", "Gun Shutter 닫기", "쓴 건", _PART_SKIP["gun_close"], "PLC G1~G3 OFF"),
    SeqStep(10, "relay", "Power Select OFF(SW_RF_SELECT)", "CH2 use_power_select", False, "PLC SW_RF_SELECT OFF"),
    SeqStep(11, "gas_close", "PLC 가스 밸브 닫기(전부)", "", _PART_SKIP["gas_close"], "PLC Ar·O2·N2 OFF"),
    SeqStep(12, "mv_close", "메인 밸브 닫기", "", _PART_SKIP["mv_close"], "PLC MV OFF"),
)
END_STOP_MUST_KEEP: Tuple[str, ...] = PST.SHUTDOWN_MUST_KEEP          # ("power", "relay") — 결정 D32

# 플라즈마 클리닝 정지(v1 _safe_rf_stop·_shutdown_rest_devices·_final_cleanup — pc_stop.py)
PC_STOP_SEQUENCE: Tuple[SeqStep, ...] = (
    SeqStep(1, "rf", "RF 안전 정지(cleanup 5 s → 0 W 확인 15 s → 강제 0 W·SET OFF)", act="RfPower.stop"),
    SeqStep(2, "flow_off", "선택 가스 FLOW OFF", act="MFC FLOW_OFF"),
    SeqStep(3, "valve_open", "SP4 쪽 MFC VALVE OPEN", act="MFC VALVE_OPEN"),
    SeqStep(4, "mfc_release", "MFC 소유권 해제(게이트 밸브는 닫지 않음)", act="release"),
)
# TSP 정지(v1 tsp_controller finally·tsp_runtime finally — tsp_engine.py)
TSP_STOP_SEQUENCE: Tuple[SeqStep, ...] = (
    SeqStep(1, "tsp_off", "TSP OFF", act="TspDriver off"),
    SeqStep(2, "ig_off", "IG OFF(SIG 0)", act="IgDriver"),
    SeqStep(3, "release", "연결 닫기·게이지 놓기", act="end_use"),
)
# ALL STOP(D05) — 넷을 한꺼번에(각자 자기 스레드에서 v1 정지 순서). 순서는 보내는 순서일 뿐 서로 기다리지 않는다
ALL_STOP_SEQUENCE: Tuple[SeqStep, ...] = (
    SeqStep(1, "ch1", "CH1 STOP(request_stop_all)", act="ProcessEngine.stop(ch1)"),
    SeqStep(2, "ch2", "CH2 STOP(request_stop_all)", act="ProcessEngine.stop(ch2)"),
    SeqStep(3, "pc", "플라즈마 클리닝 STOP", act="PlasmaCleaning.stop"),
    SeqStep(4, "tsp", "TSP STOP", act="TspEngine.stop"),
)
# 의도적 종료 — 끄는 것과 남기는 것(챔버K 결정 4)
POWER_OFF_ONLY: Tuple[SeqStep, ...] = (
    SeqStep(1, "hold", "공정 멈춤(새 장비 명령 없음 — run_end PROGRAM_EXIT)", act="engine hold"),
    SeqStep(2, "dc", "CH2 DC1·DC2 OFF(0 W → SET OFF)", act="DcPower.stop"),
    SeqStep(3, "rf", "CH2 RF·PC RF OFF(0 W → SET OFF, 램프다운 없음)", act="RfPower 0 W·SET OFF"),
    SeqStep(4, "dcpulse", "DC Pulse OUTPUT OFF(연결된 것)", act="DcPulseDriver.output_off"),
    SeqStep(5, "rfpulse", "RF Pulse RF OFF(연결된 것)", act="RfPulseDriver.stop_process"),
    SeqStep(6, "heater", "히터 RUN=0", act="HeaterEngine.shutdown"),
    SeqStep(7, "tsp", "TSP 운전 중이면 이상 중단(TSP OFF)", act="TspEngine.abort"),
)
KEEP_ON_EXIT = ("PLC 릴레이(펌프·밸브·가스 차단밸브·셔터·게이트)", "MFC 유량·밸브", "IG 필라멘트")


def by_id() -> Dict[str, Rule]:
    return {r.id: r for r in RULES}


def public() -> Dict[str, Any]:
    """웹 묶음 stopSequence(web_schema.stop_part) — 정지 순서는 고정(fixed)."""
    return {"fixed": True, "steps": [s.public() for s in STOP_SEQUENCE], "endStopMustKeep": list(END_STOP_MUST_KEEP),
            "pcSteps": [s.public() for s in PC_STOP_SEQUENCE], "tspSteps": [s.public() for s in TSP_STOP_SEQUENCE],
            "allStopSequence": [s.public() for s in ALL_STOP_SEQUENCE],
            "powerOffOnly": [s.public() for s in POWER_OFF_ONLY], "keepOnExit": list(KEEP_ON_EXIT),
            "rules": [r.public() for r in RULES]}
