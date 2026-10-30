"""통신층 명령 가드(챔버12) — 포트 바로 위에서 보내려는 바이트를 장비별 허용 목록과 정확히 맞춰 보고, 아니면 포트에 닿기 전에 막는다.

챔버K v2/chk/cmd_guard.py(de8e946)와 같은 원칙(사용자 지시 2026-10-16: 실제 장비는 절대 제어하지 않는다).
- bench 목록 = *_BENCH(질의·읽기뿐). PLC 는 Modbus TCP FC1·FC3 요청 프레임만(MBAP·유닛 검사).
- prod·sim 목록 = 드라이버가 실제로 보내는 명령(*_PROD — 각 드라이버 코드와 골든 tests/golden/dev_*.json 에서 뽑았다.
  시험 test_cmd_guard 가 '모든 골든 송신 바이트가 prod 목록을 통과'를 확인). bench 질의는 prod 에서도 허용(읽기).
- 장비 설정을 바꾸거나 장비 메모리에 저장하는 명령은 어느 모드에서도 막는다(*_FORBIDDEN — 이름 붙은 사유로 막고, 그 밖은
  '허용 목록 밖'으로 막는다). 단 v1 이 실제로 보내는 명령은 막지 않는다 — 그중 설정·메모리 저장성이 있는 것은 MEMORY_NOTES.
막으면 CommandBlocked(사유 COMMAND_BLOCKED — worker 가 결과 reason 으로 싣고 연결은 유지) + GuardEvent.
드라이버 안의 bench 거부(worker BENCH_READ_ONLY + 드라이버 이중 보호)와 별개인 마지막 관문이다.

장비(가드 이름): plc(Modbus TCP 192.168.1.2:502) · mfc · ig · dcpulse(EnerPulse EP5) · rfpulse(CESAR AE Bus) · tsp · stm100.
DC·RF 연속 전원은 PLC D/A·A/D 경유라 따로 포트가 없다(PLC 가드가 DCV_WRITE·DCV_SET 를 본다).
매뉴얼 근거는 챔버K 매뉴얼 표(같은 기종일 때만 — CESAR 1310 은 v1 device/rf_pulse.py:69 에 기종이 적혀 있음). 챔버12 MFC·IG 의
기종은 v1 코드에 없다(미확인) — MFC_FORBIDDEN 은 명령 글자가 같은 챔버K GPC3000 매뉴얼 표를 '추가로 막는 쪽'으로만 쓴다.
"""
from __future__ import annotations

import re
import struct
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from v2.common import aebus
from v2.common.modbus_rtu import READ_COILS, READ_REGS, WRITE_COIL, WRITE_COILS, WRITE_REG, WRITE_REGS
from v2.common.transport import CommandBlocked, Transport
from v2.ch12.drivers import plc_map as PM

_NUM = r"[+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?"        # 파이썬 str(float) 모양(예 '0.7000000000000001', '1e-05')

# ═════════════ PLC(LS XBL-EMTA, Modbus TCP 직결) ═════════════
# 히터(CH2) 주소 — v1 controller/heater_service.py:37-58 의 쓰기 허용 상수 그대로(v1 이 '이 밖으로는 절대 쓰지 않는다')
HEATER_WRITE_REGS = frozenset({112, 113, 114, 118, 120, 121, 126, 127, 128, 142, 143, 144, 145})   # SV·SV 한계·WD·MV 한계·램프·홀드백·OT·저속·KP·TI
HEATER_WRITE_COILS = frozenset({640, 643})                                                        # RUN(M00400)·RST(M00403)
PLC_DCV_WRITE_REGS = frozenset(PM.REG_MAP[f"DCV_WRITE_{i}"] for i in range(4))                     # D4~D7 (device/plc.py:245-248)
PLC_READ_MAX = {READ_COILS: 2000, READ_REGS: 125}                                                  # Modbus 한도(plc_map COIL/REG_BLOCK_HARD_MAX)
PLC_FCS_PROD = (READ_COILS, READ_REGS, WRITE_COIL, WRITE_REG)   # 드라이버가 쓰는 함수(골든 FC1·3·5·6) — FC15·16 은 쓰지 않음


def _plc_forbidden_reg(addr: int) -> Optional[str]:
    if addr in (0, 1, 2, 3, 8, 9, 10, 11):
        return "DCV_READ(A/D 입력 — 읽기 전용, device/plc.py:237-244)"
    if 110 <= addr <= 160:
        return "히터 영역 D110~D160 중 쓰기 허용 밖(측정·상태 읽기 전용, 또는 CH2_SUPPORTS_HEATER 꺼짐)"
    return None


def _plc_forbidden_coil(addr: int) -> Optional[str]:
    if PM.is_p_mirror(addr):
        return "P 미러 코일 800~879(래더가 매 스캔 덮어씀 — 읽기 전용, v1 E403 device/plc.py:1380-1385)"
    if 640 <= addr <= 658:
        return "히터 코일 640~658 중 쓰기 허용 밖(상태 읽기 전용 — 쓰기는 RUN·RST, CH2_SUPPORTS_HEATER 일 때만)"
    return None


def plc_writable(cfg: Dict[str, Any]) -> tuple:
    """(코일, 레지스터) — 코일 표 124개 + DCV_WRITE 4개 + (CH2 히터면) 히터 WRITE_COILS·WRITE_REGS·세션 마커."""
    coils = set(PM.COIL_MAP.values())
    regs = set(PLC_DCV_WRITE_REGS)
    if bool(cfg.get("CH2_SUPPORTS_HEATER", False)):
        coils |= HEATER_WRITE_COILS
        regs |= HEATER_WRITE_REGS
        mark = int(cfg.get("CH2_HEATER_SESSION_MARK_REG", 0) or 0)
        if mark:
            regs.add(mark)                          # v1 heater_service.py:929-933 — 세션 마커 직접 write_reg(D146)
    return frozenset(coils), frozenset(regs)


def plc_check(data: bytes, mode: str, unit: int, coils=frozenset(), regs=frozenset()) -> Optional[str]:
    if len(data) < 12:
        return f"Modbus TCP 요청 프레임이 짧음: {data.hex(' ')}"
    _tid, proto, ln, u = struct.unpack(">HHHB", data[:7])
    if proto != 0:
        return f"MBAP 프로토콜 id {proto} ≠ 0"
    if ln != len(data) - 6:
        return f"MBAP 길이 {ln} ≠ {len(data) - 6}"
    if u != unit:
        return f"유닛 {u} ≠ {unit}"
    pdu = data[7:]
    fc = pdu[0]
    addr = (pdu[1] << 8) | pdu[2]
    if fc in PLC_READ_MAX:
        if len(pdu) != 5:
            return f"읽기 프레임 길이 다름: {data.hex(' ')}"
        n = (pdu[3] << 8) | pdu[4]
        return None if 1 <= n <= PLC_READ_MAX[fc] else f"FC{fc} 개수 {n} 범위 밖"
    if mode == "bench":
        return f"bench 는 FC1·FC3 만 — FC{fc}"
    if fc == WRITE_COIL:
        val = (pdu[3] << 8) | pdu[4] if len(pdu) == 5 else -1
        if val not in (0xFF00, 0x0000):
            return f"FC5 값 형식 아님: {data.hex(' ')}"
        if addr in coils:
            return None
        why = _plc_forbidden_coil(addr)
        return f"금지 명령 — {why}: 코일 {addr}" if why else f"쓰기 허용 목록 밖 코일: {addr}"
    if fc == WRITE_REG:
        if len(pdu) != 5:
            return f"FC6 프레임 길이 다름: {data.hex(' ')}"
        if addr in regs:
            return None
        why = _plc_forbidden_reg(addr)
        return f"금지 명령 — {why}: D{addr}" if why else f"쓰기 허용 목록 밖 레지스터: D{addr}"
    if fc in (WRITE_COILS, WRITE_REGS):
        return f"드라이버가 쓰지 않는 함수 FC{fc}(여러 개 쓰기) — 허용 목록 밖"
    return f"모르는 함수 FC{fc}"


# ═════════════ MFC(MOXA NPort, ASCII + CR — v1 lib/config_common.py:300-330 MFC_COMMANDS) ═════════════
MFC_BENCH: tuple = ("R60", "R61", "R62", "R63", "R65", "R66", "R67", "R69",      # 유량 전체·채널·설정·ON/OFF
                    "R1", "R2", "R3", "R4", "R5", "R6", "R7")                    # SP1~4·압력·밸브·상태
# 15개 — v1 MFC_COMMANDS 의 READ_* 전부(READ_FLOW R6{ch}·READ_FLOW_SET R6{4+ch} 는 채널 1~3, lib/config_common.py:306-316)
MFC_PROD_QUERIES = frozenset({"R60", "R5", "R1", "R2", "R3", "R4"})          # 드라이버가 보내는 것(mfc.py·mfc_ops.py)
MFC_PROD_WRITES = (
    re.compile(rf"Q[1-3] {_NUM}"),       # FLOW_SET 'Q{ch} {str(float)}'(device/mfc.py:419-448 — D183)
    re.compile(r"L[1-3][01]"),           # FLOW_ON 'L{ch}1' / FLOW_OFF 'L{ch}0'
    re.compile(r"L[5-7]1"),              # MFC_ZEROING 'L{4+ch}1'
    re.compile(r"Z1"),                   # PS_ZEROING
    re.compile(r"[OC]"),                 # VALVE_OPEN / VALVE_CLOSE
    re.compile(r"D[1-4]"),               # SP1~4_ON
    re.compile(rf"S[1-4] {_NUM}"),       # SP1~4_SET 'S{n} {round(UI×0.1, 3)}'
)
MFC_FORBIDDEN = (   # 챔버K GPC3000 매뉴얼 표(같은 명령 글자) — 챔버12 MFC 기종 미확인이라 '더 막는 쪽'으로만
    (re.compile(r"L0"), "L0 전체 ON/OFF 마스크(v1 SET_ONOFF_MASK — 운영 미사용, v2 드라이버도 보내지 않음)"),
    (re.compile(r"[EF]"), "E·F(압력 레인지·단위 설정) [챔버K MFC p.74]"),
    (re.compile(r"[GJATBNUXMVWPY]"), "센서·밸브·제어 설정(G·J·A·T·B·N·U·X·M·V·W·P·Y) [챔버K MFC p.74-77]"),
    (re.compile(r"Z[2-4]"), "Z2~Z4(특수 영점·영점 해제·A/D 교정) [챔버K MFC p.75]"),
    (re.compile(r"Q[4-8]"), "Q4~Q8(없는 채널·유량 설정 + 플래시 기록) [챔버K MFC p.78]"),
    (re.compile(r"D[5-9]"), "D5 이상(설정점 선택 — v1 은 D1~D4 만)"),
)


def _line(data: bytes, term: bytes) -> Optional[str]:
    if not data.endswith(term):
        return None
    try:
        return data[: -len(term)].decode("ascii")
    except UnicodeDecodeError:
        return None


def mfc_check(data: bytes, mode: str) -> Optional[str]:
    s = _line(data, b"\r")
    if s is None:
        return f"형식(CR 로 끝나는 ASCII 한 줄) 아님: {data!r}"
    if mode == "bench":
        return None if s in MFC_BENCH else f"bench 허용 질의 아님: {s}"
    if re.fullmatch(r"R\d+", s):
        return None if s in MFC_BENCH else f"허용 목록 밖 질의: {s}"
    if any(rx.fullmatch(s) for rx in MFC_PROD_WRITES):
        return None
    for rx, why in MFC_FORBIDDEN:
        if rx.match(s):
            return f"금지 명령 — {why}: {s}"
    return f"허용 목록 밖: {s}"


# ═════════════ IG(MOXA NPort, ASCII + CR — device/ig.py:834-1030) ═════════════
IG_BENCH: tuple = ("RDI",)
IG_PROD: tuple = ("RDI", "SIG 1", "SIG 0")       # 압력 읽기·점등·소등 — v1 이 보내는 명령은 이 셋뿐
IG_FORBIDDEN: dict = {}                          # 기종·매뉴얼 미확인 — 이름 붙일 근거 없음(목록 밖은 모두 막힘)


def ig_check(data: bytes, mode: str) -> Optional[str]:
    s = _line(data, b"\r")
    if s is None:
        return f"형식(CR 로 끝나는 ASCII 한 줄) 아님: {data!r}"
    if s in IG_FORBIDDEN:
        return f"금지 명령 — {IG_FORBIDDEN[s]}"
    allowed = IG_BENCH if mode == "bench" else IG_PROD
    return None if s in allowed else f"{'bench ' if mode == 'bench' else ''}허용 명령 아님: {s!r}"


# ═════════════ DC Pulse(EnerPulse EP5, 02 CMD DATA 03 CHK — v2.ch12.drivers.dcpulse_proto) ═════════════
DCP_BENCH = frozenset({0x90, 0x91, 0x96, 0x99, 0x9A, 0x9C, 0x9E, 0xA5, 0xA6, 0xA7, 0xAE, 0xAF, 0xBB, 0xBC, 0xBD})
# 15개 — v1 이 읽는 코드 전부(device/dc_pulse.py:9 독스트링·각 _read_raw)
DCP_PROD_WRITES: Dict[int, tuple] = {            # 코드 → (허용 데이터 값, 이름, v1 근거)
    0x7B: (frozenset({3}), "MASTER_ONOFF=HOST", "device/dc_pulse.py:1153-1172"),
    0x7C: (frozenset({3}), "MASTER_REFER=HOST", "device/dc_pulse.py:1153-1172"),
    0x7D: (frozenset({3}), "MASTER_MODE=HOST", "device/dc_pulse.py:1153-1172"),
    0x81: (frozenset({3}), "REG_POWER", "device/dc_pulse.py:1180-1182"),
    0x83: (frozenset(range(0, 101)), "REF_POWER raw=round(W/10) 0~100", "device/dc_pulse.py:1196-1217"),
    0x80: (frozenset({1, 2}), "OUTPUT ON/OFF", "device/dc_pulse.py:1219-1338"),
    0x65: (frozenset({0, 1}), "PULSE_SYNC Int/Ext", "device/dc_pulse.py:1373-1376"),
    0x66: (frozenset(range(20, 151)), "PULSE_FREQ kHz 20~150", "device/dc_pulse.py:1378-1383"),
    0x67: (frozenset({9} | set(range(10, 101))), "OFF_TIME raw 9(DC)·10~100", "device/dc_pulse.py:1385-1394"),
    0x6F: (frozenset({1}), "FAULT_RESET", "device/dc_pulse.py:1632-1683"),
    0x8C: (frozenset({2}), "ARC_COUNT_RESET", "device/dc_pulse.py:1396-1400"),
}
DCP_FORBIDDEN: Dict[int, str] = {                # v1 이 쓰는 코드라도 v1 과 다른 값 = 장비 설정 변경
    0x7B: "마스터를 HOST(3) 가 아닌 값으로 — 제어권 이동(v1 은 HOST 만)",
    0x7C: "마스터를 HOST(3) 가 아닌 값으로 — 제어권 이동(v1 은 HOST 만)",
    0x7D: "마스터를 HOST(3) 가 아닌 값으로 — 제어권 이동(v1 은 HOST 만)",
    0x81: "Regulation 을 Power(3) 가 아닌 V·I 모드로(v1 은 REG_POWER 만, device/dc_pulse.py:1175-1182)",
}


def _dcp_chk(items: bytes) -> int:
    hi = lo = 0
    for b in items:
        hi += (b >> 4) & 0x0F
        lo += b & 0x0F
    hi += lo >> 4
    return ((hi & 0x0F) << 4) | (lo & 0x0F)


def dcp_check(data: bytes, mode: str) -> Optional[str]:
    if len(data) not in (4, 6) or data[0] != 0x02 or data[-2] != 0x03:
        return f"EP5 프레임 형식(02 CMD [DATA 2] 03 CHK) 아님: {data.hex(' ')}"
    if _dcp_chk(data[:-1]) != data[-1]:
        return f"CHK 다름: {data.hex(' ')}"
    cmd, payload = data[1], data[2:-2]
    if not payload:
        if cmd in DCP_BENCH:
            return None
        return f"{'bench ' if mode == 'bench' else ''}허용 읽기 코드 아님: 0x{cmd:02X}"
    if mode == "bench":
        return f"bench 는 읽기(데이터 없는 프레임)만 — 0x{cmd:02X}"
    val = (payload[0] << 8) | payload[1]
    ent = DCP_PROD_WRITES.get(cmd)
    if ent is not None and val in ent[0]:
        return None
    if cmd in DCP_FORBIDDEN:
        return f"금지 명령 — {DCP_FORBIDDEN[cmd]}: 0x{cmd:02X}={val}"
    if ent is not None:
        return f"허용 값 밖: 0x{cmd:02X}({ent[1]}) = {val}"
    return f"허용 목록 밖: 0x{cmd:02X}"


# ═════════════ RF Pulse(AE CESAR 1310, AE Bus — device/rf_pulse.py) ═════════════
RFP_BENCH = frozenset({155, 162, 165, 166, 193, 196,    # v1 이 읽는 것(제어 모드·상태·순방향·반사·주파수·듀티)
                       128, 130, 152, 198})             # 챔버K RFP_BENCH(CESAR 매뉴얼: 기종·용량·반사 자동 차단 시간·SW 판) — v1 미사용
RFP_PROD_WRITES: Dict[int, tuple] = {            # 명령 → (데이터 검사, 이름, v1 근거)
    1: (lambda d: d == b"", "RF_OFF", "device/rf_pulse.py:36"),
    2: (lambda d: d == b"", "RF_ON", "device/rf_pulse.py:37"),
    3: (lambda d: d == b"\x06", "SET_CTRL_MODE FWD(6)", "device/rf_pulse.py:38·505-510"),
    8: (lambda d: len(d) == 2, "SET_SETPOINT W(u16)", "device/rf_pulse.py:39·512-522"),
    14: (lambda d: d == b"\x02", "SET_ACTIVE_CTRL HOST(2)", "device/rf_pulse.py:40·487-494"),
    27: (lambda d: len(d) == 1 and d[0] <= 5, "SET_PULSING 0~5", "device/rf_pulse.py:50-59·612-625"),
    93: (lambda d: len(d) == 3, "SET_PULSE_FREQ Hz(u24)", "device/rf_pulse.py:61·536-540"),
    96: (lambda d: len(d) == 2, "SET_PULSE_DUTY %(u16)", "device/rf_pulse.py:62·542-546"),
}
RFP_FORBIDDEN: Dict[int, str] = {
    3: "조절 모드를 FWD(6) 가 아닌 값으로(LOAD·EXT — v1 은 FWD 만)",
    14: "제어 모드를 HOST(2) 가 아닌 값으로(v1 은 HOST 만)",
    33: "반사 전력 자동 차단 시간 설정(장비 설정) [챔버K CESAR p.107] — v1 미사용",
}


def rfp_check(data: bytes, mode: str, addr: int) -> Optional[str]:
    try:
        a, cmd, payload = aebus.parse(data)
    except Exception as e:
        return f"AE Bus 프레임 아님: {data.hex(' ')} ({e})"
    if a != addr:
        return f"주소 {a} ≠ {addr}"
    if cmd >= 128:
        if payload:
            return f"읽기 명령에 데이터: 명령 {cmd}"
        return None if cmd in RFP_BENCH else f"{'bench ' if mode == 'bench' else ''}허용 질의 아님: 명령 {cmd}"
    if mode == "bench":
        return f"bench 는 질의(128~255)만 — 명령 {cmd}"
    ent = RFP_PROD_WRITES.get(cmd)
    if ent is not None and ent[0](payload):
        return None
    if cmd in RFP_FORBIDDEN:
        return f"금지 명령 — {RFP_FORBIDDEN[cmd]}: 명령 {cmd} {payload.hex(' ')}"
    if ent is not None:
        return f"허용 데이터 밖: 명령 {cmd}({ent[1]}) {payload.hex(' ')}"
    return f"허용 목록 밖: 명령 {cmd}"


# ═════════════ TSP(RS-232 Window 프로토콜 — device/tsp.py:72-87) ═════════════
TSP_BENCH_WINS = frozenset({"205"})                       # 상태 읽기 — v1 이 읽는 창은 이것뿐
TSP_PROD_WRITES = frozenset({("011", "1"), ("011", "0")})  # TSP ON/OFF(device/tsp.py:391-409)
TSP_FORBIDDEN = "011 외 창 쓰기(장비 설정 변경 — v1 은 011 ON/OFF 만 쓴다)"


def tsp_check(data: bytes, mode: str, addr: int) -> Optional[str]:
    if len(data) < 9 or data[0] != 0x02 or data[-3] != 0x03:
        return f"Window 프레임 형식(02 ADDR WIN COM [DATA] 03 CRC2) 아님: {data.hex(' ')}"
    body = data[1:-2]
    x = 0
    for b in body:
        x ^= b
    if data[-2:] != f"{x:02X}".encode("ascii"):
        return f"CRC 다름: {data.hex(' ')}"
    if data[1] != (addr & 0xFF):
        return f"주소 0x{data[1]:02X} ≠ 0x{addr & 0xFF:02X}"
    try:
        win, com, val = data[2:5].decode("ascii"), chr(data[5]), data[6:-3].decode("ascii")
    except UnicodeDecodeError:
        return f"WIN·DATA 가 ASCII 아님: {data.hex(' ')}"
    if com == "0":
        if val:
            return f"읽기에 데이터: WIN {win}"
        return None if win in TSP_BENCH_WINS else f"{'bench ' if mode == 'bench' else ''}허용 읽기 창 아님: WIN {win}"
    if com != "1":
        return f"COM 은 '0'/'1': {com!r}"
    if mode == "bench":
        return f"bench 는 읽기(COM '0')만 — WIN {win} ← {val!r}"
    if (win, val) in TSP_PROD_WRITES:
        return None
    if win != "011":
        return f"금지 명령 — {TSP_FORBIDDEN}: WIN {win} ← {val!r}"
    return f"허용 값 밖: WIN 011 ← {val!r}"


# ═════════════ 두께 모니터 STM-100/MF(02 LEN DATA CHK — device/thickness_monitor.py) ═════════════
STM_BENCH: tuple = ("S", "T", "U", "V", "M", "E?", "F?", "J?", "a", "@")
# 10개 — 두께·증착률·주파수·수명·크리스탈·재료 3값 되읽기·리셋 상태·판(device/thickness_monitor.py:165-207·507-542)
STM_PROD_FIXED = frozenset(set(STM_BENCH) | {"L", "B", "C"})     # L 리셋 플래그 확인(플래그를 지움 — bench 금지), B·C 영점
STM_PROD_WRITES = (re.compile(r"[EFJ]=\d+(?:\.\d+)?"),)          # 재료값 E=/F=/J=(check_material 문자열, :552-571)
STM_BENCH_EXCLUDED = (   # v1 이 보내므로 금지는 아니지만 질의가 아니라 bench 에서는 막는 것
    (re.compile(r"L"), "L(전원 리셋 플래그 확인 — 장비 상태를 지움, 질의 아님)"),
    (re.compile(r"[BC]"), "B·C(두께 영점 — 장비 누적값을 지움)"),
    (re.compile(r"[EFJ]=.*"), "E=·F=·J=(재료값 쓰기)"),
)
STM_FORBIDDEN: tuple = ()   # 매뉴얼 없음 — 이름 붙일 근거 없음(목록 밖은 모두 막힘)


def stm_check(data: bytes, mode: str) -> Optional[str]:
    if len(data) < 4 or data[0] != 0x02 or data[1] != len(data) - 3:
        return f"STM 프레임 형식(02 LEN DATA CHK) 아님: {data.hex(' ')}"
    body = data[2:-1]
    if (sum(body) & 0xFF) != data[-1]:
        return f"체크섬 다름: {data.hex(' ')}"
    try:
        s = body.decode("ascii")
    except UnicodeDecodeError:
        return f"ASCII 아님: {data.hex(' ')}"
    if mode == "bench":
        if s in STM_BENCH:
            return None
        for rx, why in STM_BENCH_EXCLUDED:
            if rx.fullmatch(s):
                return f"bench 금지 — {why}"
        return f"bench 허용 질의 아님: {s!r}"
    if s in STM_PROD_FIXED or any(rx.fullmatch(s) for rx in STM_PROD_WRITES):
        return None
    return f"허용 목록 밖: {s!r}"


# ═════════════ v1 이 보내는 명령 중 설정·메모리 저장성이 있는 것(막지 않음 — 보고용) ═════════════
MEMORY_NOTES: tuple = (
    ("stm100", "E=·F=·J=", "재료값(밀도·Z·tooling)을 장비에 써 둔다 — 전원을 꺼도 남는지(비휘발) 미확인",
     "device/thickness_monitor.py:552-571"),
    ("stm100", "B·C", "두께(·타이머) 영점 — 장비 누적값을 지움", "device/thickness_monitor.py:545-550"),
    ("dcpulse", "0x7B·0x7C·0x7D=3", "마스터 ONOFF·REFER·MODE 를 HOST 로 — 장비 제어권 설정(패널 조작이 막힘), 저장 여부 미확인",
     "device/dc_pulse.py:1153-1172"),
    ("dcpulse", "0x81·0x83·0x65·0x66·0x67", "Regulation·출력 기준·펄스 동기·주파수·Off Time — 장비 운전 설정, 저장 여부 미확인",
     "device/dc_pulse.py:1175-1394"),
    ("dcpulse", "0x8C=2", "아크 카운터 리셋(장비 누적값을 지움 — 2026-10-12 실측상 안 먹힘)", "device/dc_pulse.py:1396-1400"),
    ("rfpulse", "3·14·27·93·96·8", "조절 모드 FWD·제어 모드 HOST·펄싱·주파수·듀티·셋포인트 — CESAR 운전 설정, 비휘발 저장 여부 미확인",
     "device/rf_pulse.py:487-625"),
    ("mfc", "Q{ch}·S{n}", "유량·설정점 값 — 장비가 값을 들고 있음(챔버K GPC3000 매뉴얼상 Q1~Q4 는 플래시 기록 없음, S 는 미확인)",
     "lib/config_common.py:305·326-329"),
    ("plc", "FC6 히터 D112~D145", "히터 SV·한계·PID 값을 PLC D 영역에 — 래치(정전 유지) 영역인지 미확인(세션 마커 D160 이 재기동을 감지)",
     "controller/heater_service.py:37-58·922-940"),
)


# ═════════════ 판정기·전송 ═════════════
@dataclass(frozen=True)
class GuardEvent:
    kind: str            # COMMAND_BLOCKED
    device: str
    detail: str
    reason: str = "COMMAND_BLOCKED"


DEVICES = ("plc", "mfc", "ig", "dcpulse", "rfpulse", "tsp", "stm100")


def device_kind(name: str) -> str:
    """worker 이름('ch1.mfc'·'tsp.ig'·'plc') → 가드 장비 이름."""
    k = str(name).rsplit(".", 1)[-1]
    if k not in DEVICES:
        raise ValueError(f"가드 장비 아님: {name}")
    return k


def checker(device: str, mode: str, cfg: Dict[str, Any]) -> Callable[[bytes], Optional[str]]:
    """장비별 판정 함수 — 바이트 → None(통과) 또는 막는 사유. mode 는 sim·bench·prod(bench 만 질의 목록)."""
    k = device_kind(device)
    if k == "plc":
        unit = int(cfg.get("PLC_UNIT", 1))
        coils, regs = plc_writable(cfg)
        return lambda b: plc_check(b, mode, unit, coils, regs)
    if k == "mfc":
        return lambda b: mfc_check(b, mode)
    if k == "ig":
        return lambda b: ig_check(b, mode)
    if k == "dcpulse":
        return lambda b: dcp_check(b, mode)
    if k == "rfpulse":
        addr = int(cfg.get("RFPULSE_ADDR", 1))
        return lambda b: rfp_check(b, mode, addr)
    if k == "tsp":
        a = cfg.get("TSP_RS232_ADDR")
        addr = int(cfg.get("TSP_ADDR", 0x80) if a is None else a)
        return lambda b: tsp_check(b, mode, addr)
    return lambda b: stm_check(b, mode)


def allowed_counts() -> Dict[str, tuple]:
    """장비별 (bench 허용 수, prod 허용 수(값 범위가 있는 명령은 명령 하나로 셈), 이름 붙은 금지 수) — 보고·bench 첫 줄."""
    return {
        "plc": (2, len(PLC_FCS_PROD), 2),                                   # FC1·FC3 / FC1·3·5·6 / P 미러·읽기 전용 주소
        "mfc": (len(MFC_BENCH), len(MFC_BENCH) + len(MFC_PROD_WRITES), len(MFC_FORBIDDEN)),
        "ig": (len(IG_BENCH), len(IG_PROD), len(IG_FORBIDDEN)),
        "dcpulse": (len(DCP_BENCH), len(DCP_BENCH) + len(DCP_PROD_WRITES), len(DCP_FORBIDDEN)),
        "rfpulse": (len(RFP_BENCH), len(RFP_BENCH) + len(RFP_PROD_WRITES), len(RFP_FORBIDDEN)),
        "tsp": (len(TSP_BENCH_WINS), len(TSP_BENCH_WINS) + len(TSP_PROD_WRITES), 1),
        "stm100": (len(STM_BENCH), len(STM_PROD_FIXED) + 3, len(STM_FORBIDDEN)),   # + E=·F=·J=
    }


class GuardedTransport(Transport):
    """포트 바로 위 — write 만 검사하고 나머지는 그대로 넘긴다."""

    def __init__(self, inner: Transport, device: str, mode: str, cfg: Dict[str, Any],
                 on_event: Optional[Callable[[GuardEvent], None]] = None) -> None:
        super().__init__(inner.clock)
        self.inner, self.device, self.mode = inner, device, mode
        self.check = checker(device, mode, cfg)
        self.on_event = on_event
        self.blocked: list = []

    @property
    def is_open(self) -> bool:
        return self.inner.is_open

    def open(self) -> None:
        self.inner.open()

    def close(self) -> None:
        self.inner.close()

    def reset_input(self) -> None:
        self.inner.reset_input()

    def write(self, data: bytes) -> None:
        why = self.check(bytes(data))
        if why:
            self.blocked.append((bytes(data), why))
            if self.on_event:
                try:
                    self.on_event(GuardEvent("COMMAND_BLOCKED", self.device, why))
                except Exception:
                    pass
            raise CommandBlocked(f"{self.device}: {why}")
        self.inner.write(data)

    def read_until(self, deadline, terminator=None, size=None, cancel=None) -> bytes:
        return self.inner.read_until(deadline, terminator=terminator, size=size, cancel=cancel)

    def __getattr__(self, name):           # 시뮬레이터 전송의 device·소켓 등
        return getattr(self.inner, name)


def guard(inner: Transport, device: str, mode: str, cfg: Dict[str, Any], on_event=None) -> GuardedTransport:
    return GuardedTransport(inner, device, mode, cfg, on_event)


def guard_pulse_factory(factory: Callable[[str, str], Transport], mode: str, cfg: Dict[str, Any],
                        on_event=None) -> Callable[[str, str], Transport]:
    """pulse_port 의 transport_factory(kind, chamber) 를 감싼다 — 같은 포트라도 붙는 종류의 목록으로 검사."""
    def make(kind: str, chamber: str) -> Transport:
        return GuardedTransport(factory(kind, chamber), f"{chamber}.{kind}", mode, cfg, on_event)
    return make
