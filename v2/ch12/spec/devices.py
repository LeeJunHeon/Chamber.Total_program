r"""챔버12 장비 사실 표(사용자 지시 0단계 'spec 표: 장비 사실 — 연결·프로토콜·명령·간격·타임아웃·1초 예산').

근거: devices_v1facts.py(v1 device/*.py 를 끝까지 읽고 적은 사실, 파일:줄)와 v1 원본. 드라이버를 쓰면서 바로잡힌 사실은 그대로
반영했다(CORRECTIONS — 예 TSP SO_KEEPALIVE 는 실효값 false(D20), CH1 RF Pulse·DC Pulse 는 4007 공유(D192)).
장비 매뉴얼은 이 저장소에 없다 — 매뉴얼로만 알 수 있는 것(응답 시간·기종·통신 두절 때 장비 동작)은 '미확인'(UNVERIFIED)으로
두고 v1 과 같게 한다. 1초 예산(TICK_PLAN)은 각 드라이버 모듈의 BUDGET 을 import 해서 모은 것(실측은 bench — v2/tools/bench_probe).
명령 바이트 허용 목록은 v2/ch12/cmd_guard.py, 주소 기본값은 v2/ch12/params(시험 v2/tests/test_devices.py 가 같은지 본다).

구역
- ENDPOINTS      : 연결(장비·챔버·프로토콜·주소·포트·설정 키·NPort 60 s 무통신 대책·SO_KEEPALIVE·근거)
- LAN            : 랜 공유(PLC·RGA 2대·NPort 가 노트북 유선 랜 하나)
- DEVICES        : 장비마다 명령(이름·바이트 형식·응답 형식·v1 근거 파일:줄)·간격·타임아웃·메모
- TICK_PLAN      : 1초 예산(드라이버 BUDGET) + 근거 · ALTERNATE: 1초에 못 들어가는 장비의 번갈아 읽기
- PC_DEAD        : PC(제어 프로그램)가 죽었을 때 출력이 어떻게 되는지 — RF/DC 파워는 PLC 인터락이 없어 소프트웨어가 꺼야 함,
                   가스 밸브 인터락은 PLC
- UNVERIFIED     : 미확인 목록(무엇을·어떻게 확인 — bench 항목)
이 파일은 import 만으로 아무 일도 하지 않는다(데이터 + 드라이버 BUDGET import).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

from v2.ch12.drivers import (camera, dc, dcpulse, ig, mfc, oes_link, plc, rf, rfpulse, rga_link, stm100, tsp)

_NP = "192.168.1.50"     # MOXA NPort(장비 시리얼 서버) — v1 lib/config_common.py:255·343·414·574, settings.json


# ═════════════ 연결 ═════════════
@dataclass(frozen=True)
class Endpoint:
    device: str          # 드라이버·가드 이름
    chamber: str         # ch1 / ch2 / shared / tsp
    protocol: str
    host: str
    port: int
    host_key: str        # v2 설정 키(params 기본값과 같아야 함 — test_devices)
    port_key: str
    via_nport: bool
    idle_60s: str        # NPort 60 s 무통신 끊김 대책(v1·v2)
    keepalive: str       # SO_KEEPALIVE(v1 실효값 → v2)
    source: str          # v1 근거


_IDLE_1S = "v2 는 1초 읽기가 늘 질의 — 무통신 없음(v1 은 대책 없음, D190)"
ENDPOINTS: tuple = (
    Endpoint("plc", "shared", "Modbus TCP(MBAP, 유닛 1) — pymodbus 대신 v2.common.modbus_tcp", "192.168.1.2", 502,
             "PLC_TCP_HOST", "PLC_TCP_PORT", False,
             "해당 없음(NPort 미경유). PLC FEnet 수신 타임아웃 15 s — v1 하트비트 6 s(FC1 0,1)·v2 1초 묶음 읽기",
             "1(v1 device/plc.py:967-972) → v2 TcpTransport 1", "lib/config_common.py:612-616;device/plc.py:313-318"),
    Endpoint("mfc", "ch1", "ASCII + CR(NPort TCP 서버)", _NP, 4003, "MFC_TCP_HOST", "CH1_MFC_TCP_PORT", True,
             _IDLE_1S, "v1 0 → v2 1(공용 TcpTransport, D39)", "lib/config_ch1.py:14;main.py:419-426"),
    Endpoint("mfc", "ch2", "ASCII + CR(NPort TCP 서버)", _NP, 4006, "MFC_TCP_HOST", "CH2_MFC_TCP_PORT", True,
             _IDLE_1S, "v1 0 → v2 1(D39)", "lib/config_ch2.py:14;main.py:427-436"),
    Endpoint("ig", "ch1", "ASCII + CR(NPort TCP 서버)", _NP, 4001, "IG_TCP_HOST", "CH1_IG_TCP_PORT", True,
             _IDLE_1S, "v1 0 → v2 1(D39)", "lib/config_ch1.py:13;main.py:439-443"),
    Endpoint("ig", "ch2", "ASCII + CR(NPort TCP 서버)", _NP, 4002, "IG_TCP_HOST", "CH2_IG_TCP_PORT", True,
             _IDLE_1S, "v1 0 → v2 1(D39)", "lib/config_ch2.py:13;main.py:444-449"),
    Endpoint("ig", "tsp", "ASCII + CR — TSP 운전이 쓰는 IG = CH1 IG 와 같은 포트(4001), 그 밖 설정은 CH2 값(D189)", _NP, 4001,
             "IG_TCP_HOST", "TSP_IG_TCP_PORT", True, "TSP 운전 중에만 연결", "v1 0 → v2 1(D39)",
             "lib/config_common.py:589;runtime/tsp_runtime.py:522-523;device/ig.py:23"),
    Endpoint("dcpulse", "ch1", "EnerPulse EP5 RS-232 Type 4(02 CMD DATA 03 CHK) — RF Pulse 와 같은 포트 4007(D192)", _NP, 4007,
             "DCPULSE_TCP_HOST", "DCPULSE_TCP_PORT", True,
             "공정이 DC Pulse 를 고를 때만 연결·출력 ON 중에만 읽음(v1 대책 없음 — 쓰지 않을 때는 연결 안 함)",
             "DCP_TCP_KEEPALIVE false → 0(device/dc_pulse.py:2161-2169)", "lib/config_common.py:343-344;device/dc_pulse.py:3021-3024"),
    Endpoint("dcpulse", "ch2", "EP5 RS-232 — MOXA Port 5 를 RF Pulse 와 바꿔 씀(Parity None + Null Modem)", _NP, 4005,
             "DCPULSE_TCP_HOST", "CH2_DCPULSE_TCP_PORT", True, "위와 같음", "0",
             "config/settings.json:563-567"),
    Endpoint("rfpulse", "ch1", "AE Bus(CESAR) — DC Pulse 와 같은 포트 4007(D192). 채널 모듈 config_ch1.py 에만 있는 값(D140)", _NP, 4007,
             "RFPULSE_TCP_HOST", "CH1_RFPULSE_TCP_PORT", True,
             "공정이 RF Pulse 를 고를 때만 연결·RF ON 뒤에만 읽음", "설정과 무관하게 1(device/rf_pulse.py:1057-1063, D185)",
             "lib/config_ch1.py:16-17;device/rf_pulse.py:995-1002"),
    Endpoint("rfpulse", "ch2", "AE Bus(CESAR)", _NP, 4005, "RFPULSE_TCP_HOST", "CH2_RFPULSE_TCP_PORT", True,
             "위와 같음", "1(D185)", "config/settings.json:566"),
    Endpoint("tsp", "shared", "RS-232 Window 프로토콜(02 ADDR WIN COM DATA 03 CRC2), 주소 0x80", _NP, 4004,
             "TSP_TCP_HOST", "TSP_TCP_PORT", True,
             "필요할 때만 연결(상주 연결 없음) — 끊긴 소켓은 다음 호출 실패 뒤 재연결(v1 그대로)",
             "실효값 false(settings.json common 이 .py True 를 덮음 — CFG-019, D20 정정)", "device/tsp.py:118-225;lib/config_common.py:574-587"),
    Endpoint("stm100", "ch2", "Sycon STM-100/MF(02 LEN DATA CHK), RS-232 9600-8-N-1 — 포트 4008 은 임시값(D152·D191)", _NP, 4008,
             "CH2_THICKNESS_TCP_HOST", "CH2_THICKNESS_TCP_PORT", True,
             "상시 0.5 s 폴링이 막음(v1 그대로)", "설정 안 함(asyncio 기본) → v2 도 건드리지 않음",
             "config/settings.json:644-646;device/thickness_monitor.py:241-366"),
)

LAN: dict = dict(
    shared="PLC(192.168.1.2)·RGA 2대(192.168.1.20 CH1·192.168.1.21 CH2)·MOXA NPort(192.168.1.50)가 노트북 유선 랜 하나를 공유한다",
    nport_ports={4001: "CH1 IG(= TSP 운전 IG)", 4002: "CH2 IG", 4003: "CH1 MFC", 4004: "TSP", 4005: "CH2 DC Pulse/RF Pulse(바꿔 씀)",
                 4006: "CH2 MFC", 4007: "CH1 DC Pulse/RF Pulse(공유)", 4008: "CH2 두께 모니터(임시값)"},
    nport_idle="MOXA NPort 는 60 s 무통신이면 연결을 끊는다(사용자 지시 4장) — v1 은 대책 없음(D190), v2 는 1초 읽기",
    one_connection="NPort 포트 하나에 TCP 연결 하나로 쓴다(v1 과 같음) — v1 이 켜져 있으면 v2·bench 가 같은 포트를 열지 말 것",
    source="lib/config_common.py:139-142(RGA IP);plan.py 4장;devices_v1facts.NPORT_IDLE_V1",
)


# ═════════════ 명령 ═════════════
@dataclass(frozen=True)
class Cmd:
    name: str
    wire: str            # 보내는 바이트 형식
    reply: str           # 응답 형식
    source: str          # v1 파일:줄
    kind: str = "read"   # read / write


@dataclass(frozen=True)
class Device:
    key: str
    name: str
    model: str
    commands: tuple
    gaps: str
    timeouts: str
    notes: tuple = ()


def _plc_cmds() -> tuple:
    return (
        Cmd("FC1 read_coils", "MBAP(tid,00 00,len,01) 01 {addr:2B} {n:2B}", "01 {바이트수} {비트}", "device/plc.py:1369-1441"),
        Cmd("FC3 read_regs", "MBAP 03 {addr:2B} {n:2B}", "03 {2n} {u16…}", "device/plc.py:1395-1404·1688-1712"),
        Cmd("FC5 write_coil", "MBAP 05 {addr:2B} FF 00 / 00 00", "에코", "device/plc.py:1380-1393", "write"),
        Cmd("FC6 write_reg", "MBAP 06 {addr:2B} {u16}", "에코", "device/plc.py:1406-1414", "write"),
        Cmd("하트비트", "FC1 0,1(6 s 마다 — max(1, 8×0.75))", "성공 = 링크 정상", "device/plc.py:1125·1146-1153"),
        Cmd("DC·RF DAC", "FC6 D4~D7 = round(W/1000×4000), 래치 FC5 DCV_SET_0~3(80~83)", "에코", "device/plc.py:2074-2121", "write"),
        Cmd("DC·RF ADC", "FC3 D0~D3·D8~D11(v2 는 1초 묶음 D0..D11 한 번)", "V=raw×0.50316·I=raw×0.00097405, RF a·raw+b",
            "device/plc.py:2136-2218"),
        Cmd("CH2 히터", "FC3 D110..D160·FC1 640..658 읽기, 쓰기 WRITE_REGS 13개·RUN/RST·마커 D160·WD D114",
            "controller/heater_service 해석", "controller/heater_service.py:37-58·476-486·929-933", "write"),
    )


DEVICES: tuple = (
    Device("plc", "PLC", "LS XBL-EMTA(v1 주석) — Modbus TCP 직결", _plc_cmds(),
           gaps="명령 간격 PLC_CMD_GAP_MS 150 ms(device/plc.py:838-844), 순간 스위치 180 ms(lib/config_common.py:637)",
           timeouts="응답 2.0 s·pymodbus 재시도 0, 같은 소켓 3회째 재생성, 재연결 3회(0.5 s)·백오프 5 s(device/plc.py:846-977)",
           notes=("PLC 는 소켓을 닫으면 1.5~3분 새 SYN 에 응답 안 함 — 타임아웃 1회로 닫지 않는다(device/plc.py:410-412)",
                  "P 미러 코일 800~879 읽기 전용(E403), 타이머 표는 정의만(device/plc.py:252-278)",
                  "로봇 명령 구간 하트비트 pause/resume(host/handlers.py:270-292)")),
    Device("dc", "DC 연속 전원(CH2 DC1·DC2)", "PLC D/A·A/D 경유(아날로그) — 자체 포트 없음", (
        Cmd("SET_ON/OFF", "plc.power_enable(DCV_SET_0|3)", "-", "device/dc_power.py:181-188·455-469", "write"),
        Cmd("WRITE_W", "plc.power_write → D4|D7 = round(W×4)", "-", "device/dc_power.py:199-203", "write"),
        Cmd("READ_PVI", "D0/D1 · D10/D11", "(P,V,I)", "runtime/chamber_runtime.py:704-708"),
    ), gaps="램프 없음 — 즉시 목표 W, 측정(5 s)마다 ±1 W", timeouts="0 W 쓰기 0.5 s 간격 최대 10 s(device/dc_power.py:435-453)",
        notes=("SUPPORTS_HEATER 면 DC2 금지(D/A CH3 = 히터 출력)",)),
    Device("rf", "RF 연속 전원(CH2·플라즈마 클리닝)", "PLC D/A·A/D 경유(아날로그) — 자체 포트 없음", (
        Cmd("SET_ON/OFF", "plc.rf_enable → DCV_SET_2(CH2)·DCV_SET_1(PC)", "-", "device/rf_power.py:297-307·899-913", "write"),
        Cmd("WRITE", "W'=1.6546×W+2.6323 → D6|D5 = round(W'×4)", "-", "device/rf_power.py:846-861", "write"),
        Cmd("READ", "D8/D9(CH2) · D2/D3(PC)", "forward·reflected W", "device/plc.py:2186-2218"),
    ), gaps="램프 1 W/측정(1 s), 정지 1 W/s(D187)", timeouts="blind 150 s·Ref 60 s·0 W 10 s(lib/config_common.py:184·190)"),
    Device("mfc", "MFC ×2(CH1·CH2)", "미확인(v1 코드에 기종 없음 — 명령 글자는 챔버K GPC3000 과 같음)", (
        Cmd("READ_FLOW_ALL", "'R60\\r'", "'Q0+…' %FS", "lib/config_common.py:306;device/mfc.py:1849-1873"),
        Cmd("READ_PRESSURE", "'R5\\r'", "'P…' hw, UI = hw/0.1", "lib/config_common.py:309;device/mfc.py:1907-1941"),
        Cmd("READ_SP1~4", "'R1\\r'~'R4\\r'", "'S{n}…'", "lib/config_common.py:310-313"),
        Cmd("FLOW_SET", "'Q{ch} {str(float)}\\r'", "응답 없음", "lib/config_common.py:305;device/mfc.py:419-448", "write"),
        Cmd("FLOW_ON/OFF", "'L{ch}1\\r' / 'L{ch}0\\r'", "응답 없음", "lib/config_common.py:302-303", "write"),
        Cmd("ZEROING", "'L{4+ch}1\\r' · 'Z1\\r'", "응답 없음", "lib/config_common.py:304·319", "write"),
        Cmd("VALVE", "'O\\r' / 'C\\r'", "응답 없음", "lib/config_common.py:317-318", "write"),
        Cmd("SP_ON/SET", "'D{n}\\r' · 'S{n} {round(UI×0.1,3)}\\r'", "응답 없음", "lib/config_common.py:322-329", "write"),
    ), gaps="응답 뒤 MFC_GAP_MS 1000, no-reply 뒤 + 80 ms, 연결 직후 800 ms(lib/config_common.py:266-281)",
        timeouts="응답 2000 ms, 무응답 1회면 재연결(D182), 백오프 1→20 s(device/mfc.py:1419-1437)"),
    Device("ig", "이온 게이지 ×2(+ TSP 운전 IG)", "미확인(v1 코드에 기종 없음)", (
        Cmd("READ_PRESSURE", "'RDI\\r'", "'1.2x10e-5' → Torr / 'IG OFF'", "device/ig.py:834-906"),
        Cmd("IG_ON", "'SIG 1\\r'", "'OK…' 이어야 성공", "device/ig.py:929-939", "write"),
        Cmd("IG_OFF", "'SIG 0\\r'", "응답 안 기다림", "device/ig.py:974-1030", "write"),
    ), gaps="응답 뒤 IG_GAP_MS 1000 + 보내기 전 흡수 30 ms(device/ig.py:687-760)",
        timeouts="응답 3000 ms·재시도 0(무응답으로 끊지 않음), 베이스 대기 900 s(D137)"),
    Device("dcpulse", "DC Pulse(CH1·CH2)", "EnerPulse EP5(v1 device/dc_pulse.py:4)", (
        Cmd("MASTER ONOFF/REFER/MODE=HOST", "02 7b|7c|7d 00 03 03 {chk}", "06 ACK", "device/dc_pulse.py:1153-1172", "write"),
        Cmd("REG_POWER · REF_POWER", "02 81 00 03 03 89 · 02 83 {round(W/10):2B} 03 {chk}", "06", "device/dc_pulse.py:1180-1217", "write"),
        Cmd("OUTPUT ON/OFF", "02 80 00 01 03 86 / 02 80 00 02 03 87", "06(아니면 0x90 HV 확인)", "device/dc_pulse.py:1219-1338", "write"),
        Cmd("PULSE SYNC/FREQ/OFF", "02 65|66|67 {2B} 03 {chk}", "06", "device/dc_pulse.py:1373-1394", "write"),
        Cmd("FAULT/ARC RESET", "02 6f 00 01 03 75 · 02 8c 00 02 03 93", "06", "device/dc_pulse.py:1396-1400·1632-1683", "write"),
        Cmd("READ_*", "02 {90 91 96 99 9a 9c 9e a5 a6 a7 ae af bb bc bd} 03 {chk}", "02 CMD {2B·9A 6B} 03 CHK",
            "device/dc_pulse.py:9·1051-1124·1460-1630"),
    ), gaps="DCP_GAP_MS 150(보낸 시각 기준), 연결 직후 0.8 s, 주파수 뒤 0.5 s(lib/config_common.py:383-391)",
        timeouts="응답 2500 + 2000 ms, 무응답 1회 세션 재시작, 백오프 1→10 s(device/dc_pulse.py:2286-2338)"),
    Device("rfpulse", "RF Pulse(CH1·CH2)", "AE CESAR 1310(v1 device/rf_pulse.py:69)", (
        Cmd("RF_OFF/ON", "08 01 09 / 08 02 0a", "ACK + CSR", "device/rf_pulse.py:36-37", "write"),
        Cmd("SET_CTRL_MODE FWD · SET_ACTIVE_CTRL HOST", "09 03 06 0c · 09 0e 02 05", "CSR", "device/rf_pulse.py:487-510", "write"),
        Cmd("SETPOINT · PULSING · FREQ · DUTY", "0a 08 {u16} · 09 1b {0~5} · 0b 5d {u24} · 0a 60 {u16}", "CSR",
            "device/rf_pulse.py:512-546·612-625", "write"),
        Cmd("REPORT 155·162·165·166·193·196", "08 {cmd} {xor}", "데이터 프레임", "device/rf_pulse.py:479·647-665·1503-1509·1837-1852"),
    ), gaps="CMD_GAP_MS 1500(lib/config_common.py:84), 재시도 쉼 max(150, gap)",
        timeouts="exec 2000·질의 4500·폴링 질의 9000·RF ON/OFF 2500 ms, 재시도 3, 무응답으로 끊지 않음"),
    Device("tsp", "TSP(티타늄 승화 펌프)", "미확인", (
        Cmd("TSP_ON/OFF", "02 80 '011' '1' '1'|'0' 03 CRC2 (= 02 80 30 31 31 31 31 03 42 33)", "ACK", "device/tsp.py:391-409", "write"),
        Cmd("READ_STATUS", "02 80 '205' '0' 03 CRC2 (= 02 80 32 30 35 30 03 38 34)", "DATA 0~5", "device/tsp.py:380-388"),
    ), gaps="보낸 뒤 20 ms, 상태 확인 0.05 s × 1.5 s(device/tsp.py:257-264·412-431)",
        timeouts="바이트마다 1.2 s(device/tsp.py:266-294)"),
    Device("stm100", "두께 모니터(CH2)", "Sycon STM-100/MF(v1 device/thickness_monitor.py:3)", (
        Cmd("S·T·U·V·M", "02 01 {53|54|55|56|4d} {chk}", "'A'+값", "device/thickness_monitor.py:165-207"),
        Cmd("E?·F?·J?", "02 02 {45|46|4a} 3f {chk}", "'A'+숫자", "device/thickness_monitor.py:535-542"),
        Cmd("a·@", "02 01 61 61 · 02 01 40 40", "리셋 상태·판", "device/thickness_monitor.py:529-533"),
        Cmd("L", "02 01 4c 4c", "A/B — 리셋 플래그 확인", "device/thickness_monitor.py:488-504", "write"),
        Cmd("B·C 영점 · E=/F=/J=", "02 01 42 42 · 02 {len} 'E=8.9' {sum}", "A", "device/thickness_monitor.py:545-571", "write"),
    ), gaps="poll 0.5 s(S·T)·5 s(U·V·M)·60 s(E?·F?·J?), 재시도 쉼 0.1 s", timeouts="명령 1.0 s × 3회, 6회 연속 실패면 재연결"),
)


# ═════════════ 1초 예산(드라이버 BUDGET 모음) ═════════════
@dataclass(frozen=True)
class TickPlan:
    device: str
    module: str
    budget: dict
    basis: str           # 예산 계산의 v1 근거(간격·바이트)


TICK_PLAN: tuple = (
    TickPlan("plc", "v2.ch12.drivers.plc", plc.BUDGET, "PLC_CMD_GAP_MS 150(device/plc.py:838-844) — 블록 5개(D0..D11·히터 2·코일·번갈아 1)"),
    TickPlan("dc", "v2.ch12.drivers.dc", dc.BUDGET, "PLC 1초 묶음에 포함(D54)"),
    TickPlan("rf", "v2.ch12.drivers.rf", rf.BUDGET, "PLC 1초 묶음에 포함(D54)"),
    TickPlan("mfc", "v2.ch12.drivers.mfc", mfc.BUDGET, "MFC_GAP_MS 1000 = 응답 뒤 1 s(lib/config_common.py:266) → 1초에 질의 하나(D36)"),
    TickPlan("ig", "v2.ch12.drivers.ig", ig.BUDGET, "IG_GAP_MS 1000 + 흡수 30 ms(device/ig.py:687-760, D36)"),
    TickPlan("dcpulse", "v2.ch12.drivers.dcpulse", dcpulse.BUDGET, "DCP_GAP_MS 150(lib/config_common.py:383, 지시 4장) — 출력 ON 중만(D48)"),
    TickPlan("rfpulse", "v2.ch12.drivers.rfpulse", rfpulse.BUDGET, "CMD_GAP_MS 1500(lib/config_common.py:84) — v1 폴링 일정 그대로(D50)"),
    TickPlan("tsp", "v2.ch12.drivers.tsp", tsp.BUDGET, "보낸 뒤 20 ms(lib/config_common.py:579) — 쓰는 동안만 205(D15)"),
    TickPlan("stm100", "v2.ch12.drivers.stm100", stm100.BUDGET, "상시 0.5 s 폴링(controller/thickness_service.py:262-300) — 1초 행은 마지막 값"),
    TickPlan("camera", "v2.ch12.drivers.camera", camera.BUDGET, "통신 없음(USB 카메라, 촬영 1 s)"),
    TickPlan("oes", "v2.ch12.drivers.oes_link", oes_link.BUDGET, "통신 없음(별도 프로세스 워커)"),
    TickPlan("rga", "v2.ch12.drivers.rga_link", rga_link.BUDGET, "통신 없음(별도 프로세스 워커)"),
)
BUDGET_KEYS = ("queries_per_tick", "bytes_tx", "bytes_rx", "min_gap_ms", "fits_1s", "alternate", "note")

# 1초에 못 들어가는 장비(fits_1s False)와 번갈아 읽는 장비 — 드라이버 BUDGET.alternate 그대로
# (alternate 가 비면 note — IG 는 번갈아 읽을 다른 질의가 없어 간격을 못 맞춘 tick 이 stale)
ALTERNATE: Dict[str, str] = {t.device: str(t.budget.get("alternate") or t.budget["note"]) for t in TICK_PLAN
                             if not t.budget["fits_1s"] or t.budget.get("alternate")}


# ═════════════ PC(제어 프로그램)가 죽었을 때 ═════════════
@dataclass(frozen=True)
class PcDead:
    target: str
    outputs: str          # 무엇이 켜져 있을 수 있나
    on_pc_dead: str       # PC 가 죽으면 어떻게 되나
    who_turns_off: str    # software / plc_ladder / device / none
    source: str
    confirmed: bool       # v1 코드로 확인된 것인가(False = 미확인 — 매뉴얼·래더 없음)


PC_DEAD: tuple = (
    PcDead("RF 연속(CH2·플라즈마 클리닝)", "PLC D/A D6·D5 + DCV_SET_2·1 래치",
           "PLC 는 마지막 D/A 값을 유지 → RF 계속 출력. RF/DC 파워에는 PLC 인터락이 없다 — 소프트웨어가 꺼야 한다(0 W → SET OFF)",
           "software", "plan.py 4장(사용자 지시);device/rf_power.py:899-913", False),
    PcDead("DC 연속(CH2 DC1·DC2)", "PLC D/A D4·D7 + DCV_SET_0·3 래치",
           "마지막 D/A 값 유지 → DC 계속 출력(PLC 인터락 없음 — 소프트웨어가 끔)", "software",
           "plan.py 4장;device/dc_power.py:435-469", False),
    PcDead("DC Pulse(EP5)", "OUTPUT ON(0x80=1)",
           "RS-232 호스트 두절 때 장비 동작 근거 없음 — 계속 출력으로 본다(소프트웨어가 OUTPUT_OFF)", "software",
           "device/dc_pulse.py:1327-1338(정지는 소프트웨어 명령뿐)", False),
    PcDead("RF Pulse(CESAR)", "RF ON(명령 2)",
           "RS-232 워치독 없음(챔버K CESAR 매뉴얼 p.87: PROFIBUS 오류 때만 OFF) — 계속 출력(소프트웨어가 RF_OFF)", "software",
           "device/rf_pulse.py:772-886;챔버K devices.RFPULSE.comm_loss", False),
    PcDead("가스(Ar·O2·N2 밸브·MFC 유량)", "PLC 가스 밸브 코일 + MFC Q·L",
           "가스 밸브 인터락은 PLC 래더(Ar_n_GAS_인터락 등 M0002B~M0004E) — 인터락 조건이 깨지면 PLC 가 닫는다. "
           "PC 두절 자체로 닫히지는 않음(래더에 PC 워치독 근거 없음), MFC 는 마지막 유량 유지로 본다",
           "plc_ladder", "device/plc.py:66-83(인터락 코일);plan.py 4장", False),
    PcDead("CH2 히터(VSCD-30 + PLC PID)", "M00400 RUN",
           "래더 워치독: D114 가 30 s 바뀌지 않으면 WD_ERR → 히터 OFF(v1 은 RUN 중 HEATER_WD_PERIOD_S 5 s 마다 D114+1)",
           "plc_ladder", "lib/config_common.py:471;controller/heater_service.py:17·460·729", True),
    PcDead("PLC 밸브·펌프·게이트·척", "PLC 출력 코일",
           "래더가 마지막 상태 유지(인터락 조건만 래더가 처리) — 진공 유지가 정상", "plc_ladder", "device/plc.py:55-200(코일 표)", False),
    PcDead("TSP", "011 ON(승화)", "TSP 컨트롤러 자체 사이클 — 두절 때 동작 근거 없음", "device", "device/tsp.py:391-409", False),
    PcDead("IG 필라멘트", "SIG 1", "그대로 켜짐(장비 자체 보호만 — 근거 없음)", "device", "device/ig.py:929-1030", False),
)
SOFTWARE_MUST_OFF = tuple(p.target for p in PC_DEAD if p.who_turns_off == "software")   # RF·DC·DC Pulse·RF Pulse


# ═════════════ 드라이버 작성 중 바로잡힌 사실 ═════════════
CORRECTIONS: tuple = (
    ("TSP SO_KEEPALIVE", "devices_v1facts 는 1", "v1 실효값 false(settings.json common 이 .py True 를 덮음, CFG-019)", "D20"),
    ("CH1 RF Pulse·DC Pulse 포트", "따로 적힘", "둘 다 4007 공유 — pulse_port 가 점유를 가린다(같은 챔버 다른 종류는 거부)", "D192·D140"),
    ("RF Pulse SO_KEEPALIVE", "설정 RFP_TCP_KEEPALIVE", "설정과 무관하게 늘 1", "D185"),
    ("MFC·IG SO_KEEPALIVE", "v1 0", "v2 공용 TcpTransport 는 늘 1(사용자 확인 필요)", "D39"),
    ("MFC·IG 연결", "v1 cleanup 이 TCP 를 닫음", "v2 는 1초 읽기 때문에 유지(60 s 무통신 끊김도 없어짐)", "D37"),
    ("DC Pulse Regulation", "v1 독스트링 0x82", "실제 전송 0x81 data 3", "D184"),
    ("두께 모니터 포트", "4008", "settings 주석상 임시값 — 실제 MOXA 포트 미확인", "D152·D191"),
)


# ═════════════ 미확인 목록 ═════════════
@dataclass(frozen=True)
class Unverified:
    device: str
    item: str
    v1: str               # v1 근거·지금 쓰는 값
    how: str              # 확인 방법(bench_probe 항목 등)


UNVERIFIED: tuple = (
    Unverified("plc", "트랜잭션 왕복 시간·1초 묶음 5블록 소요", "명령 간격 150 ms 로 계산 600 ms + 응답", "bench_probe 응답 p50/p99·놓친 tick"),
    Unverified("plc", "PC 워치독 래더 유무·PLC STOP 때 D/A 출력", "v1 코드에 근거 없음(히터 D114 워치독만)", "XG5000 프로젝트·모듈 매뉴얼"),
    Unverified("plc", "히터 D 영역이 래치(정전 유지)인지", "세션 마커 D160 으로 재기동 감지(v1)", "PLC 프로젝트"),
    Unverified("mfc", "기종·응답 시간·최소 명령 간격", "간격 1000 ms(v1), 1초에 질의 하나", "bench_probe R 질의 응답 분포"),
    Unverified("mfc", "R61~R63·R65~R67·R69·R6·R7 응답 형식", "v1 운영 미사용(검증 경로 — DEV-096)", "bench_probe 설정 읽기 원문"),
    Unverified("ig", "기종·RDI 응답 시간", "응답 3000 ms 한도, 간격 1 s", "bench_probe RDI 응답 분포"),
    Unverified("dcpulse", "NPort 시리얼 설정(속도·패리티)", "코드 근거 없음(DCPULSE_BAUD 9600 레거시)", "NPort 웹 설정"),
    Unverified("dcpulse", "1초 PIV·SAT·ANT 읽기 때 NAK 증가·응답 지연", "v1 은 5 s(점화 1 s) 주기(D48)", "bench_probe --ch1-pulse dcpulse"),
    Unverified("rfpulse", "NPort 시리얼 설정·응답 지연", "간격 1500 ms(v1)", "bench_probe --ch1-pulse rfpulse"),
    Unverified("rfpulse", "128·130·152·198 질의 응답(기종·용량·자동 차단·판)", "v1 미사용 — 챔버K CESAR 매뉴얼 근거", "bench_probe 설정 읽기"),
    Unverified("pulse", "지금 4007·4005 에 어느 펄스 장비가 꽂혀 있는가", "공정이 고른 쪽만 연결(v1)", "현장 확인 후 bench_probe 선택"),
    Unverified("tsp", "205 응답 데이터 길이·형식·주소 0x80 외 응답", "v1 해석이 받는 모양(sim 가정)", "bench_probe 205 원문"),
    Unverified("stm100", "실제 MOXA 포트(4008 임시값)", "settings.json:646 주석", "현장 배선·bench_probe 연결"),
    Unverified("stm100", "E=/F=/J= 비휘발 저장 여부·L 이 지우는 것", "v1 은 공정마다 재료값을 씀", "매뉴얼 5장"),
    Unverified("all", "NPort 가 포트당 TCP 연결을 몇 개 받는지", "v1 은 하나씩", "NPort 설정(Max connection)"),
    Unverified("all", "PC 가 죽었을 때 RF·DC·펄스 전원이 실제로 계속 출력하는지", "PC_DEAD confirmed=False", "PLC 래더·장비 매뉴얼 — 사람이 확인"),
)


def endpoint(device: str, chamber: str) -> Optional[Endpoint]:
    for e in ENDPOINTS:
        if e.device == device and e.chamber == chamber:
            return e
    return None


def budget_table() -> tuple:
    """(장비, 질의/tick, 보냄 B, 받음 B, 최소 간격 ms, 1초 안, 번갈아) — 보고·bench 요약용."""
    return tuple((t.device, t.budget["queries_per_tick"], t.budget["bytes_tx"], t.budget["bytes_rx"], t.budget["min_gap_ms"],
                  bool(t.budget["fits_1s"]), str(t.budget.get("alternate") or "")) for t in TICK_PLAN)
