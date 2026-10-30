"""PLC 주소 표(챔버12, LS XBL-EMTA Modbus TCP) — v1 device/plc.py 의 주소 맵을 정의 순서 그대로 옮긴 것.

- COILS  : v1 PLC_COIL_MAP 124개 (이름, 코일 번호, v1 주석 M 주소, v1 device/plc.py 줄). 코일 번호 = M 워드(10진)×16 + 비트(16진).
           정의 순서 = v1 PLC 코일 CSV 열 순서(device/plc.py:2027-2049) — 바꾸지 않는다.
- REGS   : v1 PLC_REG_MAP 12개 (이름, D 번호, 'D0000n', 줄). WRITE_n = D(4+n), READ_0~3 = D0~D3, READ_4~7 = D8~D11.
- TIMERS : v1 PLC_TIMER_MAP 25개 (이름, 번호, 줄) — v1 에서 정의만 있고 읽거나 쓰는 곳 없음(device/plc.py:252-278, 기록만).
- P 미러 : P0000~P0004 → M0050~M0054 = 코일 800~879 (device/plc.py:201-233). 래더가 매 스캔 덮어써서 읽기 전용 —
           쓰기는 E403 으로 거절(device/plc.py:1380-1385). 코일 로그 열에는 넣지 않는다(COILS 와 분리).
- 별칭·논리명: device/plc.py:449-462 (AR_{ch}_GAS_* → Ar_{ch}_GAS_*, MV@n/MS@n/G1~G3).
- 히터(CH2) 주소(D110~D160·코일 640~658)는 여기에 없다 — v1 도 controller/heater_service.py 상수로만 둠(heater 드라이버 몫).
이 파일은 import 만으로 아무 일도 하지 않는다(데이터·순수 함수).
"""
from __future__ import annotations

from typing import Dict

COILS = (
    ('R_P_1_OFF_TIMER', 1, 'M00001', 55),
    ('R_P_2_OFF_TIMER', 2, 'M00002', 56),
    ('L_R_P_OFF_TIMER', 3, 'M00003', 57),
    ('R_V_1_인터락', 33, 'M00021', 58),
    ('F_V_1_인터락', 34, 'M00022', 59),
    ('M_V_1_인터락', 35, 'M00023', 60),
    ('VENT_1_인터락', 36, 'M00024', 61),
    ('TURBO_1_인터락', 37, 'M00025', 62),
    ('DOOR_1_인터락', 38, 'M00026', 63),
    ('G_V_1_인터락', 40, 'M00028', 64),
    ('MAIN_SHUTTER_1_인터락', 42, 'M0002A', 65),
    ('Ar_1_GAS_인터락', 43, 'M0002B', 66),
    ('O2_1_GAS_인터락', 44, 'M0002C', 67),
    ('MAIN_1_GAS_인터락', 45, 'M0002D', 68),
    ('L_R_V_인터락', 49, 'M00031', 69),
    ('L_PIN_인터락', 50, 'M00032', 70),
    ('L_VENT_인터락', 52, 'M00034', 71),
    ('R_V_2_인터락', 65, 'M00041', 72),
    ('F_V_2_인터락', 66, 'M00042', 73),
    ('M_V_2_인터락', 67, 'M00043', 74),
    ('VENT_2_인터락', 68, 'M00044', 75),
    ('TURBO_2_인터락', 69, 'M00045', 76),
    ('DOOR_2_인터락', 70, 'M00046', 77),
    ('G_V_2_인터락', 72, 'M00048', 78),
    ('MAIN_SHUTTER_2_인터락', 74, 'M0004A', 79),
    ('Ar_2_GAS_인터락', 75, 'M0004B', 80),
    ('O2_2_GAS_인터락', 76, 'M0004C', 81),
    ('N2_2_GAS_인터락', 77, 'M0004D', 82),
    ('MAIN_2_GAS_인터락', 78, 'M0004E', 83),
    ('DCV_SET_0', 80, 'M00050', 86),
    ('DCV_SET_1', 81, 'M00051', 87),
    ('DCV_SET_2', 82, 'M00052', 88),
    ('DCV_SET_3', 83, 'M00053', 89),
    ('LP_STEP1', 96, 'M00060', 91),
    ('LP_STEP2', 97, 'M00061', 92),
    ('L_VAC_READY_SW', 98, 'M00062', 93),
    ('L_ATM', 99, 'M00063', 94),
    ('L_VAC_NOT_READY', 100, 'M00064', 95),
    ('Z1_UP', 112, 'M00070', 97),
    ('Z1_MID', 113, 'M00071', 98),
    ('Z1_DOWN', 114, 'M00072', 99),
    ('Z2_UP', 115, 'M00073', 100),
    ('Z2_MID', 116, 'M00074', 101),
    ('Z2_DOWN', 117, 'M00075', 102),
    ('Z1_UP_LOCATION', 128, 'M00080', 105),
    ('Z1_MID_LOCATION', 129, 'M00081', 106),
    ('Z1_DOWN_LOCATION', 130, 'M00082', 107),
    ('Z2_UP_LOCATION', 131, 'M00083', 108),
    ('Z2_MID_LOCATION', 132, 'M00084', 109),
    ('Z2_DOWN_LOCATION', 133, 'M00085', 110),
    ('Z_M_P_1_STOP_SW', 160, 'M00100', 113),
    ('Z_M_P_1_MID_SW', 176, 'M00110', 114),
    ('Z_M_P_1_MID_CW', 177, 'M00111', 115),
    ('Z_M_P_1_MID_CCW', 178, 'M00112', 116),
    ('Z_M_P_2_MID_SW', 179, 'M00113', 117),
    ('Z_M_P_2_MID_CW', 180, 'M00114', 118),
    ('Z_M_P_2_MID_CCW', 181, 'M00115', 119),
    ('R_P_1_SW', 192, 'M00120', 123),
    ('R_V_1_SW', 193, 'M00121', 124),
    ('F_V_1_SW', 194, 'M00122', 125),
    ('M_V_1_SW', 195, 'M00123', 126),
    ('VENT_1_SW', 196, 'M00124', 127),
    ('TURBO_1_SW', 197, 'M00125', 128),
    ('DOOR_1_OPEN_SW', 198, 'M00126', 129),
    ('DOOR_1_CLOSE_SW', 199, 'M00127', 130),
    ('G_V_1_OPEN_SW', 200, 'M00128', 131),
    ('G_V_1_CLOSE_SW', 201, 'M00129', 132),
    ('MAIN_SHUTTER_1_SW', 202, 'M0012A', 133),
    ('Ar_1_GAS_SW', 203, 'M0012B', 134),
    ('O2_1_GAS_SW', 204, 'M0012C', 135),
    ('MAIN_1_GAS_SW', 205, 'M0012D', 136),
    ('L_R_P_SW', 208, 'M00130', 139),
    ('L_R_V_SW', 209, 'M00131', 140),
    ('L_PIN_UP_SW', 210, 'M00132', 141),
    ('L_PIN_DOWN_SW', 211, 'M00133', 142),
    ('L_VENT_SW', 212, 'M00134', 143),
    ('SW_RF_SELECT', 214, 'M00136', 144),
    ('SW_POWER_SELECT', 215, 'M00137', 145),
    ('SHUTTER_2_SW', 216, 'M00138', 146),
    ('SHUTTER_3_SW', 217, 'M00139', 147),
    ('Z_M_P_1_SW', 218, 'M0013A', 148),
    ('Z_M_P_1_CW_SW', 219, 'M0013B', 149),
    ('Z_M_P_1_CCW_SW', 220, 'M0013C', 150),
    ('Z_M_P_2_SW', 221, 'M0013D', 151),
    ('Z_M_P_2_CW_SW', 222, 'M0013E', 152),
    ('Z_M_P_2_CCW_SW', 223, 'M0013F', 153),
    ('R_P_2_SW', 224, 'M00140', 156),
    ('R_V_2_SW', 225, 'M00141', 157),
    ('F_V_2_SW', 226, 'M00142', 158),
    ('M_V_2_SW', 227, 'M00143', 159),
    ('VENT_2_SW', 228, 'M00144', 160),
    ('TURBO_2_SW', 229, 'M00145', 161),
    ('DOOR_2_OPEN_SW', 230, 'M00146', 162),
    ('DOOR_2_CLOSE_SW', 231, 'M00147', 163),
    ('G_V_2_OPEN_SW', 232, 'M00148', 164),
    ('G_V_2_CLOSE_SW', 233, 'M00149', 165),
    ('MAIN_SHUTTER_2_SW', 234, 'M0014A', 166),
    ('Ar_2_GAS_SW', 235, 'M0014B', 167),
    ('O2_2_GAS_SW', 236, 'M0014C', 168),
    ('N2_2_GAS_SW', 237, 'M0014D', 169),
    ('MAIN_2_GAS_SW', 238, 'M0014E', 170),
    ('SHUTTER_1_SW', 239, 'M0014F', 171),
    ('Z_M_P_2_STOP_SW', 320, 'M00200', 174),
    ('VENT_1_LAMP', 356, 'M00224', 175),
    ('DOOR_1_OPEN_LAMP', 358, 'M00226', 176),
    ('DOOR_1_CLOSE_LAMP', 359, 'M00227', 177),
    ('G_V_1_OPEN_LAMP', 360, 'M00228', 178),
    ('G_V_1_CLOSE_LAMP', 361, 'M00229', 179),
    ('L_PIN_UP_LAMP', 370, 'M00232', 180),
    ('L_PIN_DOWN_LAMP', 371, 'M00233', 181),
    ('L_VENT_LAMP', 372, 'M00234', 182),
    ('VENT_2_LAMP', 388, 'M00244', 183),
    ('DOOR_2_OPEN_LAMP', 390, 'M00246', 184),
    ('DOOR_2_CLOSE_LAMP', 391, 'M00247', 185),
    ('G_V_2_OPEN_LAMP', 392, 'M00248', 186),
    ('G_V_2_CLOSE_LAMP', 393, 'M00249', 187),
    ('LOADING_1_SENSOR_LAMP', 480, 'M00300', 190),
    ('LOADING_2_SENSOR_LAMP', 481, 'M00301', 191),
    ('BUZZER_STOP_SW', 1600, 'M01000', 193),
    ('GAUGE_1_A_INTERLOCK', 2400, 'M01500', 194),
    ('GAUGE_1_B_INTERLOCK', 2560, 'M01600', 195),
    ('L_GAUGE_A_INTERLOCK', 2720, 'M01700', 196),
    ('GAUGE_2_A_INTERLOCK', 2880, 'M01800', 197),
    ('GAUGE_2_B_INTERLOCK', 3040, 'M01900', 198),
)
REGS = (
    ('DCV_READ_0', 0, 'D00000', 237),
    ('DCV_READ_1', 1, 'D00001', 238),
    ('DCV_READ_2', 2, 'D00002', 239),
    ('DCV_READ_3', 3, 'D00003', 240),
    ('DCV_READ_4', 8, 'D00008', 241),
    ('DCV_READ_5', 9, 'D00009', 242),
    ('DCV_READ_6', 10, 'D00010', 243),
    ('DCV_READ_7', 11, 'D00011', 244),
    ('DCV_WRITE_0', 4, 'D00004', 245),
    ('DCV_WRITE_1', 5, 'D00005', 246),
    ('DCV_WRITE_2', 6, 'D00006', 247),
    ('DCV_WRITE_3', 7, 'D00007', 248),
)
TIMERS = (
    ('AIR_ALARM_TIMER', 0, 253),
    ('Z_M_P_1_UP_DELAY', 1, 254),
    ('Z_M_P_1_DOWN_DELAY', 2, 255),
    ('Z_M_P_1_MID_DELAY', 3, 256),
    ('Z_M_P_2_UP_DELAY', 4, 257),
    ('Z_M_P_2_DOWN_DELAY', 5, 258),
    ('Z_M_P_2_MID_DELAY', 6, 259),
    ('VENT_1_EXT_TIME', 7, 260),
    ('VENT_2_EXT_TIME', 8, 261),
    ('L_VENT_EXT_TIME', 9, 262),
    ('GAUGE_1_SP1_ON_DELAY', 16, 263),
    ('GAUGE_1_SP1_OFF_DELAY', 17, 264),
    ('GAUGE_1_SP2_ON_DELAY', 18, 265),
    ('GAUGE_1_SP2_OFF_DELAY', 19, 266),
    ('GAUGE_1_SP3_ON_DELAY', 20, 267),
    ('GAUGE_1_SP3_OFF_DELAY', 21, 268),
    ('GAUGE_2_SP1_ON_DELAY', 22, 269),
    ('GAUGE_2_SP1_OFF_DELAY', 23, 270),
    ('GAUGE_2_SP2_ON_DELAY', 24, 271),
    ('GAUGE_2_SP2_OFF_DELAY', 25, 272),
    ('GAUGE_2_SP3_ON_DELAY', 26, 273),
    ('GAUGE_2_SP3_OFF_DELAY', 27, 274),
    ('TURBO_1_TIMER_인터락', 32, 275),
    ('TURBO_2_TIMER_인터락', 33, 276),
    ('ATM_ALARM_TIMER', 34, 277),
)

COIL_MAP: Dict[str, int] = {n: a for n, a, _m, _l in COILS}
REG_MAP: Dict[str, int] = {n: a for n, a, _m, _l in REGS}
TIMER_MAP: Dict[str, int] = {n: a for n, a, _l in TIMERS}
COIL_M_ADDR: Dict[str, str] = {n: m for n, _a, m, _l in COILS}

# ───────── P 영역 미러(읽기 전용) ─────────
P_MIRROR_WORDS = range(0, 5)            # P0000~P0004
P_MIRROR_M_BASE_WORD = 50               # → M0050~M0054
P_MIRROR_COIL_MIN = P_MIRROR_M_BASE_WORD * 16                              # 800
P_MIRROR_COIL_MAX = (P_MIRROR_M_BASE_WORD + len(P_MIRROR_WORDS)) * 16 - 1  # 879


def p_mirror_coil(p_addr: str) -> int:
    """P 주소 → 미러 코일 번호. 예 "P00003" → 803, "P00030" → 848. 범위(P0000~P0004) 밖이면 KeyError(v1 과 같은 문구)."""
    s = str(p_addr).strip().upper()
    if not s.startswith("P") or len(s) < 3:
        raise KeyError(f"P 주소 형식 아님: {p_addr}")
    try:
        word, bit = int(s[1:-1], 10), int(s[-1], 16)
    except ValueError:
        raise KeyError(f"P 주소 형식 아님: {p_addr}") from None
    if word not in P_MIRROR_WORDS:
        raise KeyError(f"미러 범위(P0000~P0004) 밖: {p_addr}")
    return (P_MIRROR_M_BASE_WORD + word) * 16 + bit


P_MIRROR_MAP: Dict[str, int] = {
    "L_GAUGE_A": p_mirror_coil("P00003"),      # 803 (M00503)
    "L_ATM_SENSOR": p_mirror_coil("P00009"),   # 809 (M00509)
    "L_R_P_OUT": p_mirror_coil("P00030"),      # 848 (M00530)
    "L_R_V_OUT": p_mirror_coil("P00031"),      # 849 (M00531)
    "L_VENT_OUT": p_mirror_coil("P00034"),     # 852 (M00534)
}


def is_p_mirror(addr: int) -> bool:
    return P_MIRROR_COIL_MIN <= int(addr) <= P_MIRROR_COIL_MAX


# ───────── 별칭·논리명(device/plc.py:449-462) ─────────
ALIASES: Dict[str, str] = {}
for _ch in (1, 2):
    ALIASES[f"AR_{_ch}_GAS_SW"] = f"Ar_{_ch}_GAS_SW"
    ALIASES[f"AR_{_ch}_GAS_인터락"] = f"Ar_{_ch}_GAS_인터락"
LOGICAL: Dict[str, str] = {
    "MV@1": "M_V_1_SW", "MV@2": "M_V_2_SW",
    "MS@1": "MAIN_SHUTTER_1_SW", "MS@2": "MAIN_SHUTTER_2_SW",
    "G1": "SHUTTER_1_SW", "G2": "SHUTTER_2_SW", "G3": "SHUTTER_3_SW",
}

# 코일 CSV 레지스터 열 순서(device/plc.py:1883-1887)
REG_LOG_KEYS = ("DCV_READ_0", "DCV_READ_1", "DCV_READ_2", "DCV_READ_3",
                "DCV_WRITE_0", "DCV_WRITE_1", "DCV_WRITE_2", "DCV_WRITE_3",
                "DCV_READ_4", "DCV_READ_5", "DCV_READ_6", "DCV_READ_7")

# 코일 블록 계획 (max_gap, max_span) — 앞쪽일수록 블록 수가 적다(device/plc.py:594-597). FC1 한 블록 최대 2000
COIL_BLOCK_PLANS = ((2000, 2000), (256, 512), (64, 256), (8, 64))
COIL_BLOCK_HARD_MAX = 2000
REG_BLOCK_HARD_MAX = 125            # FC3 한도(controller/heater_service.py:7-8)


def resolve(name_or_addr) -> int:
    """이름 → 주소(device/plc.py:1640-1655): int 는 그대로, 별칭 보정 → COIL_MAP → REG_MAP → P_MIRROR_MAP, 없으면 KeyError."""
    if isinstance(name_or_addr, int) and not isinstance(name_or_addr, bool):
        return name_or_addr
    key = str(name_or_addr)
    key = ALIASES.get(key, key)
    if key in COIL_MAP:
        return COIL_MAP[key]
    if key in REG_MAP:
        return REG_MAP[key]
    if key in P_MIRROR_MAP:
        return P_MIRROR_MAP[key]
    raise KeyError(f"알 수 없는 주소/이름: {name_or_addr}")


def build_sparse_ranges(addrs, max_gap: int = 8, max_span: int = 64) -> list:
    """가까운 주소끼리만 (start, count) 블록으로 묶는다(device/plc.py:1443-1476 와 같은 규칙)."""
    xs = sorted({int(a) for a in addrs})
    if not xs:
        return []
    ranges = []
    start = prev = xs[0]
    for a in xs[1:]:
        if a - prev <= max_gap and a - start + 1 <= max_span:
            prev = a
            continue
        ranges.append((start, prev - start + 1))
        start = prev = a
    ranges.append((start, prev - start + 1))
    return ranges


def coil_ranges_for_plan(addrs, idx: int):
    """계획 idx 부터 블록 길이가 FC1 한도를 넘지 않는 첫 계획의 (인덱스, ranges)(device/plc.py:609-625)."""
    last = len(COIL_BLOCK_PLANS) - 1
    i = max(0, min(int(idx), last))
    while True:
        gap, span = COIL_BLOCK_PLANS[i]
        ranges = build_sparse_ranges(addrs, max_gap=max(0, int(gap)), max_span=max(1, int(span)))
        if not ranges or max(c for _, c in ranges) <= COIL_BLOCK_HARD_MAX or i >= last:
            return i, ranges
        i += 1


def default_plan_idx(v) -> int:
    """설정값 → 시작 계획(범위 밖·형식 오류면 마지막 = 가장 안전, device/plc.py:599-607)."""
    last = len(COIL_BLOCK_PLANS) - 1
    try:
        i = int(v)
    except Exception:
        return last
    return last if (i < 0 or i > last) else i


def field_name(v1_name: str) -> str:
    """v1 이름 → 1초 행 필드 이름(소문자). 예 'Ar_1_GAS_인터락' → 'ar_1_gas_인터락'."""
    return v1_name.lower()
