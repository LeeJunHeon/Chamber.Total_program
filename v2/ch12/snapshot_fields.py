"""챔버12 1초 스냅샷 필드 — 블록 순서·공정 맥락·목표값·프로그램 상태(사용자 지시 3장 (2)).

한 행 = tick 정보(ts·seq·jitter_ms·skipped) → 장비가 아닌 칸(EXTRA_FIELDS) → 장비 블록(BLOCK_ORDER 순서).
- 장비 블록: 드라이버 모듈의 blocks() 를 모은 것(이 파일은 이름과 순서만 정한다 — 필드 정의는 드라이버가 주인).
  열 이름 <블록>.<필드>, 장비 품질 <블록>.q·<블록>.age_s, 번갈아 읽는 필드는 <블록>.<필드>.q·.age_s(snapshot_bus).
  설정에서 꺼진 장비·쓰지 않는 펄스 전원·TSP 를 안 쓰는 동안도 블록(열)은 그대로 있고 품질 off — 열이 설정에 따라
  바뀌지 않아 기록 파일(SQLite·CSV)의 열이 같은 날 안에서 흔들리지 않는다.
- 공정 맥락(CONTEXTS — ch1·ch2·pc 플라즈마 클리닝·tsp): <c>.proc.<이름>. 엔진(3단계)이 snapshot_fields() 로 채운다.
  확정 전(ETA 등)은 null — 추정값을 넣지 않는다.
- 목표값(TARGETS): <c>.set.<이름> — 그 시각 엔진이 장비에 요청한 값(설정 파워·유량·압력·온도·두께). 엔진이 채운다.
  장비 블록 안의 '드라이버가 확인한 값'(예 ch1.dcpulse.set_w = 0x83 확인값)과 다르다 — 이것은 공정이 원하는 값.
- 프로그램 상태(PROGRAM_FIELDS): prog.<이름> — v1 util/self_watch.py 샘플(working_set·private·handles·gdi·user·threads·gc)을
  1초마다(program_state). 윈도우는 GetProcessMemoryInfo(PROCESS_MEMORY_COUNTERS_EX — PrivateUsage)·GetProcessHandleCount·
  GetGuiResources, 리눅스는 /proc/self/status·/proc/self/fd. 구할 수 없는 값은 None(채우지 않음 — v1 CSV 의 -1 은
  outputs.selfwatch 가 만든다). 기록 큐·tick 통계도 같이(오래 켜 두기 확인용).
품질 규칙(ok/late/stale/fail/off/suppressed, 나이)은 v2.common.snapshot_bus 그대로.
"""
from __future__ import annotations

import gc
import os
import re
import sys
import threading
from typing import Any, Dict, Iterable, List, Mapping, Optional

import dataclasses

from v2.common.snapshot import DeviceBlock, FieldDef, FType, Quality, row_columns
from v2.ch12.drivers import camera, dc, dcpulse, heater, ig, mfc, oes_link, plc, rf, rfpulse, rga_link, stm100, tsp
from v2.ch12 import plc_labels as PL
from v2.ch12 import thickness_record

F, I, B, S = FType.FLOAT, FType.INT, FType.BOOL, FType.STR
CHAMBERS = ("ch1", "ch2")

# ───────── 장비 블록 ─────────
BLOCK_ORDER: tuple = (
    "plc",
    "ch1.mfc", "ch1.ig", "ch1.dcpulse", "ch1.rfpulse", "ch1.stm100", "ch1.oes", "ch1.rga",
    "ch2.mfc", "ch2.ig", "ch2.dcpulse", "ch2.rfpulse", "ch2.dc", "ch2.rf", "ch2.heater", "ch2.stm100", "ch2.thk", "ch2.oes",
    "ch2.rga", "pc.rf", "tsp", "tsp.ig", "camera",
)

# 블록 → 그 블록을 채우는 1초 읽기 소스(worker) 이름. PLC 하나가 plc·ch2.dc·ch2.rf·pc.rf 를 한 번 읽기로 채운다(품질 공유).
BLOCK_SOURCE: Dict[str, str] = {b: b for b in BLOCK_ORDER}
BLOCK_SOURCE.update({"ch2.dc": "plc", "ch2.rf": "plc", "pc.rf": "plc", "ch2.heater": "plc", "ch2.thk": "engine"})
# ch2.heater: PLC 1초 묶음의 히터 해독(drivers/heater.py — CH2_SUPPORTS_HEATER 일 때) + 엔진 열(owner·target·hold …).
# ch2.thk: 두께 공정 판정(thickness_record.blocks — ThicknessEngine.snapshot_fields, 조립 assembly.ThicknessHost) — 장비 worker 없음.


def device_blocks() -> List[DeviceBlock]:
    """드라이버 blocks() 를 BLOCK_ORDER 순서로."""
    found: Dict[str, DeviceBlock] = {}
    lists = [plc.blocks(), camera.blocks(), tsp.blocks(), ig.blocks("tsp"), rf.blocks("ch2"), rf.blocks("pc"),
             dc.blocks("ch2"), heater.blocks("ch2"), thickness_record.blocks("ch2")]
    for ch in CHAMBERS:
        lists += [mfc.blocks(ch), ig.blocks(ch), dcpulse.blocks(ch), rfpulse.blocks(ch), stm100.blocks(ch),
                  oes_link.blocks(ch), rga_link.blocks(ch)]
    for lst in lists:
        for b in lst:
            found[b.name] = b
    missing = [n for n in BLOCK_ORDER if n not in found]
    if missing:
        raise ValueError(f"드라이버 blocks() 에 없는 블록: {missing}")
    return [_screen_names(found[n]) for n in BLOCK_ORDER]


# ───────── 화면 이름(2026-11-24 묶음 16 — 챔버K 묶음 21 용어표) ─────────
# 드라이버가 정한 필드 이름(label)은 장비 쪽 표기(래더 이름·레지스터·되읽기 등)가 섞여 있다 — 화면에는 이 표의 이름을 싣는다.
# 열 이름·형식·desc 는 그대로(래더 이름·주소처럼 이름에서 뺀 장비 표기는 desc 앞에 붙인다). 키 = 블록 종류.필드
# (ch1.·ch2.·pc. 를 뗀 블록 이름 — 두 챔버 같은 이름) 또는 블록 이름 전체.필드(그쪽이 먼저).
BLOCK_LABEL: Dict[str, str] = {
    "ch1.ig": "CH1 Ion Gauge", "ch2.ig": "CH2 Ion Gauge", "tsp.ig": "TSP Ion Gauge",
    "ch2.dc": "CH2 DC Power", "ch2.rf": "CH2 RF Power", "ch2.heater": "CH2 히터", "pc.rf": f"{PL.PC} RF Power",
}
FIELD_LABEL: Dict[str, str] = {
    # MFC
    "mfc.o2_sccm": "O₂ 유량", "mfc.n2_sccm": "N₂ 유량", "mfc.pressure_mtorr": "챔버 압력",
    "mfc.ar_set_sccm": "Ar 목표", "mfc.o2_set_sccm": "O₂ 목표", "mfc.n2_set_sccm": "N₂ 목표",
    "mfc.ar_on": "Ar 흐름", "mfc.o2_on": "O₂ 흐름", "mfc.n2_on": "N₂ 흐름", "mfc.valve_pct": "MFC 밸브 위치",
    "mfc.sp1_mtorr": "SP1 장비 값", "mfc.sp2_mtorr": "SP2 장비 값", "mfc.sp3_mtorr": "SP3 장비 값",
    "mfc.sp4_mtorr": "SP4 장비 값", "mfc.sp_active": "사용 중 SP",
    "mfc.valve_age_s": "MFC 밸브 위치 나이", "mfc.on_age_s": "가스 흐름 나이", "mfc.sp1_age_s": "SP1 값 나이",
    "mfc.sp2_age_s": "SP2 값 나이", "mfc.sp3_age_s": "SP3 값 나이", "mfc.sp4_age_s": "SP4 값 나이",
    "mfc.sp_active_age_s": "사용 중 SP 나이", "mfc.polling": "공정 중 읽기",
    # Ion Gauge(CH1·CH2·TSP)
    "ig.pressure_torr": "Ion Gauge 압력", "ig.state": "압력 상태", "ig.target_torr": "베이스 압력 목표",
    "tsp.ig.pressure_torr": "Ion Gauge 압력", "tsp.ig.state": "압력 상태", "tsp.ig.target_torr": "베이스 압력 목표",
    # DC Pulse
    "dcpulse.p_w": "파워", "dcpulse.v_v": "전압", "dcpulse.i_a": "전류", "dcpulse.arc_soft": "Soft 아크 누적",
    "dcpulse.arc_hard": "Hard 아크 누적", "dcpulse.arc_soft_rate": "Soft 아크 표시값", "dcpulse.arc_hard_rate": "Hard 아크 표시값",
    "dcpulse.arc_run_soft": "공정 Soft 아크", "dcpulse.arc_run_hard": "공정 Hard 아크", "dcpulse.set_w": "목표",
    "dcpulse.out_on": "출력", "dcpulse.freq_khz": "펄스 주파수", "dcpulse.off_raw": "Off Time 장비 값",
    # RF Pulse
    "rfpulse.fwd_w": "Forward", "rfpulse.ref_w": "Reflected", "rfpulse.itl_open": "RF Pulse 인터락",
    "rfpulse.status_raw": "상태 번호", "rfpulse.freq_hz": "펄스 주파수", "rfpulse.duty_pct": "듀티",
    # 두께 모니터
    "stm100.reset_seen": "전원 리셋 횟수", "stm100.density": "적용 밀도", "stm100.z_factor": "적용 Z-factor",
    "stm100.tooling": "적용 Tooling",
    # OES·RGA
    "oes.running": "OES 프로그램 실행", "rga.running": "RGA 프로그램 실행",
    # RF Power(CH2·Plasma Cleaning — PLC D/A)
    "rf.fwd_w": "Forward", "rf.ref_w": "Reflected", "rf.set_w": "보정 목표", "rf.write_w": "D/A 쓰기 값", "rf.target_w": "목표",
    "rf.state": "상태", "rf.running": "동작 중", "rf.enable": "출력 허용",
    # DC Power(CH2 DC1·DC2 — PLC D/A)
    "dc.dc1_p": "DC1 파워", "dc.dc1_set_w": "DC1 마지막 쓰기 값", "dc.dc1_enable": "DC1 출력 허용",
    "dc.dc2_p": "DC2 파워", "dc.dc2_set_w": "DC2 마지막 쓰기 값", "dc.dc2_enable": "DC2 출력 허용",
    # 히터(CH2 PLC PID)
    "heater.pv": "현재 온도", "heater.sv": "목표 온도", "heater.sv_ramp": "승온 중간 목표", "heater.cur_sv": "승온 적용 목표",
    "heater.sv_limit": "목표 상한", "heater.ot_limit": "과온 한계", "heater.mv": "출력", "heater.mv_pct": "출력 비율",
    "heater.mv_limit": "출력 상한", "heater.pid_err": "PID 오류 번호", "heater.ramp_rate": "PLC 승온 속도",
    "heater.holdback": "홀드백", "heater.slow_zone": "감속 구간", "heater.slow_rate": "감속 승온 속도", "heater.kp_act": "적용 Kp",
    "heater.ti_act": "적용 Ti", "heater.marker": "세션 표시", "heater.wd": "워치독 카운터", "heater.run": "운전",
    "heater.itl": "히터 인터락", "heater.dac_en": "출력 허용", "heater.water_ok": "Water", "heater.owner": "히터 제어 주체",
    "heater.target": "최종 목표", "heater.ramp_active": "프로그램 승온 중", "heater.ramp_set_rate": "프로그램 승온 속도",
    "heater.hold": "유지 모드", "heater.hold_value": "유지 출력 상한", "heater.sat_clamp": "포화 출력 상한",
    "heater.phase": "공정 연동 단계", "heater.ready": "켤 수 있음", "heater.stale": "상태 오래됨",
    # 두께 판정
    "thk.text": "판정 문구",
    # TSP·카메라
    "tsp.status": "상태 번호", "tsp.link": "연결", "camera.frames": "촬영 장 수",
}
ENUM_LABEL: Dict[str, tuple] = {
    "ig.state": (("ok", "정상"), ("off", "꺼짐"), ("invalid", "값 오류"), ("none", "응답 없음")),
    "tsp.ig.state": (("ok", "정상"), ("off", "꺼짐"), ("invalid", "값 오류"), ("none", "응답 없음")),
    "tsp.state": (("STOP", "정지"), ("FAIL", "이상"), ("WAIT_INTERLOCK", "인터락 대기"), ("RAMP", "승온"),
                  ("WAIT_SUBLIM", "승화 대기"), ("SUBLIM", "승화")),
    "camera.mode": (("CH1", "CH1"), ("CH2", "CH2"), ("CLEANING", PL.PC), ("ALL", "전체")),
}
_DEV_IN_LABEL = re.compile(r"\(|\bD\d{3}|[A-Z]{2,}_|\bR\d+\b|v1|되읽기|raw|hex|is_ready|파이썬")


def _kind(block: str) -> str:
    return block.split(".", 1)[1] if block.startswith(("ch1.", "ch2.", "pc.")) else block


def _screen_names(b: DeviceBlock) -> DeviceBlock:
    """드라이버 블록 → 화면 이름을 바꾼 블록(열·형식은 그대로)."""
    out = []
    for f in b.fields:
        if b.name == "plc":
            ladder = f.label
            new = PL.COIL_PLAN_LABEL if f.name == "coil_plan" else PL.label(ladder)
            desc = f.desc if f.name == "coil_plan" else f"{ladder} · {f.desc}"     # 래더 이름은 desc(상세 보기)로
            out.append(dataclasses.replace(f, label=new, desc=desc))
            continue
        k = _kind(b.name)
        new = FIELD_LABEL.get(f"{b.name}.{f.name}") or FIELD_LABEL.get(f"{k}.{f.name}") or f.label
        enum = ENUM_LABEL.get(f"{b.name}.{f.name}") or ENUM_LABEL.get(f"{k}.{f.name}") or f.enum_labels
        desc = f.desc
        if new != f.label and _DEV_IN_LABEL.search(f.label):      # 옛 이름의 장비 표기(D112·되읽기 등)는 desc 로
            desc = f"{f.label} · {desc}" if desc else f.label
        out.append(dataclasses.replace(f, label=new, enum_labels=enum, desc=desc))
    return dataclasses.replace(b, label=BLOCK_LABEL.get(b.name, b.label), fields=tuple(out))


# ───────── 공정 맥락(엔진이 채움) ─────────
CONTEXTS = ("ch1", "ch2", "pc", "tsp")
CONTEXT_LABEL = {"ch1": "CH1", "ch2": "CH2", "pc": PL.PC, "tsp": "TSP"}
OWNERS = ("robot", "web", "local")


def _ctx_fields(c: str) -> List[FieldDef]:
    p = f"{c}.proc."
    lb = CONTEXT_LABEL[c]
    return [
        FieldDef(p + "state", S, "", f"{lb} 공정 상태", source="engine", detail=True, desc="엔진 상태 이름(대기·진행·정지 순서 …)"),
        FieldDef(p + "run_id", S, "", f"{lb} 공정 ID", source="engine", detail=True),
        FieldDef(p + "owner", S, "", f"{lb} 조작 주체", source="engine", detail=True, desc="robot / web / local",
                 enum_labels=(("robot", "로봇"), ("web", "웹"), ("local", "노트북"))),
        FieldDef(p + "recipe_rev", I, "", f"{lb} 순서 판", source="engine", detail=True,
                 desc="공정 중 시퀀스 편집마다 +1(3장 (3))"),
        FieldDef(p + "block_no", I, "", f"{lb} 단계 번호", source="engine", detail=True, desc="1 부터"),
        FieldDef(p + "block_count", I, "", f"{lb} 단계 수", source="engine", detail=True),
        FieldDef(p + "block_type", S, "", f"{lb} 단계 종류", source="engine", detail=True, desc="blocks.BLOCK_DEFS 의 type"),
        FieldDef(p + "block_label", S, "", f"{lb} 단계 이름", source="engine", detail=True),
        FieldDef(p + "block_elapsed_s", F, "s", f"{lb} 단계 경과", ".0f", source="engine", detail=True),
        FieldDef(p + "remain_s", F, "s", f"{lb} 단계 남은 시간", ".0f", source="engine", detail=True,
                 desc="지금 블록(대기·증착 등)의 남은 초 — 끝이 정해지지 않은 블록은 null"),
        FieldDef(p + "eta_remaining_s", F, "s", f"{lb} 공정 남은 시간", ".0f", source="engine(eta)",
                 desc="ETA — eta.CHn.remaining_s, 확정 전 null(추정값을 보내지 않음)"),
        FieldDef(p + "min_total_s", F, "s", f"{lb} 최소 총 시간", ".0f", source="engine(eta)", detail=True,
                 desc="min_total_s — 확정 전 null"),
        FieldDef(p + "paused", B, "", f"{lb} 일시정지", source="engine", summary="any", detail=True),
        FieldDef(p + "alarms", S, "", f"{lb} 활성 알람", source="safety", detail=True, desc="쉼표로 묶은 규칙 id(4단계)"),
    ]


# ───────── 목표값(엔진이 채움) ─────────
_T = {  # 이름 → (형식, 단위, 이름표)
    "dcpulse_w": (F, "W", "DC Pulse 목표"), "dcpulse_freq_khz": (I, "kHz", "DC Pulse 주파수"),
    "dcpulse_duty_pct": (F, "%", "DC Pulse 듀티"),
    "rfpulse_w": (F, "W", "RF Pulse 목표"), "rfpulse_freq_hz": (I, "Hz", "RF Pulse 주파수"),
    "rfpulse_duty_pct": (F, "%", "RF Pulse 듀티"),
    "dc_w": (F, "W", "DC1 목표"), "dc2_w": (F, "W", "DC2 목표"), "rf_w": (F, "W", "RF Power 목표"),
    "ar_sccm": (F, "sccm", "Ar 목표"), "o2_sccm": (F, "sccm", "O₂ 목표"), "n2_sccm": (F, "sccm", "N₂ 목표"),
    "pressure_mtorr": (F, "mTorr", "압력 목표"), "base_torr": (F, "Torr", "베이스 압력 목표"),
    "heater_c": (F, "°C", "히터 목표 온도"), "thickness_nm": (F, "nm", "목표 두께"), "time_s": (F, "s", "공정 시간 목표"),
}
TARGETS: Dict[str, tuple] = {
    # CH1 가스는 Ar·O2 만(4장), 전원은 DC Pulse·RF Pulse(PLC 의 DC·RF 연속은 CH2 결선 — dc.py·rf.py)
    "ch1": ("dcpulse_w", "dcpulse_freq_khz", "dcpulse_duty_pct", "rfpulse_w", "rfpulse_freq_hz", "rfpulse_duty_pct",
            "ar_sccm", "o2_sccm", "pressure_mtorr", "base_torr", "thickness_nm", "time_s"),
    "ch2": ("dcpulse_w", "dcpulse_freq_khz", "dcpulse_duty_pct", "rfpulse_w", "rfpulse_freq_hz", "rfpulse_duty_pct",
            "dc_w", "dc2_w", "rf_w", "ar_sccm", "o2_sccm", "n2_sccm", "pressure_mtorr", "base_torr", "heater_c",
            "thickness_nm", "time_s"),
    "pc": ("rf_w", "rfpulse_w", "ar_sccm", "o2_sccm", "n2_sccm", "pressure_mtorr", "time_s"),
    "tsp": ("base_torr",),
}


def _target_fields(c: str) -> List[FieldDef]:
    out = []
    for n in TARGETS[c]:
        t, unit, label = _T[n]
        out.append(FieldDef(f"{c}.set.{n}", t, unit, f"{CONTEXT_LABEL[c]} {label}", source="engine", detail=True,
                            desc="그 시각 공정이 요청한 값(장비 확인값은 장비 블록)"))
    return out


# ───────── 프로그램 상태(self_watch 대체) ─────────
PROGRAM_FIELDS: tuple = (
    FieldDef("prog.working_set_mb", F, "MB", "작업 집합", ".1f", "GetProcessMemoryInfo / VmRSS", summary="max", detail=True),
    FieldDef("prog.private_mb", F, "MB", "전용 메모리", ".1f", "PrivateUsage / RssAnon", summary="max", detail=True),
    FieldDef("prog.handles", I, "", "핸들 수", source="GetProcessHandleCount / /proc/self/fd", summary="max", detail=True),
    FieldDef("prog.gdi", I, "", "GDI 개체", source="GetGuiResources(0) — 윈도우만", summary="max", detail=True),
    FieldDef("prog.user", I, "", "USER 개체", source="GetGuiResources(1) — 윈도우만", summary="max", detail=True),
    FieldDef("prog.threads", I, "", "스레드 수", source="threading.active_count()", summary="max", detail=True),
    FieldDef("prog.os_threads", I, "", "OS 스레드 수", source="/proc/self/status Threads — 리눅스만", summary="max",
             detail=True),
    FieldDef("prog.gc0", I, "", "GC 0세대", source="gc.get_count()", detail=True),
    FieldDef("prog.gc1", I, "", "GC 1세대", source="gc.get_count()", detail=True),
    FieldDef("prog.gc2", I, "", "GC 2세대", source="gc.get_count()", detail=True),
    FieldDef("prog.uptime_s", F, "s", "실행 시간", ".0f", "clock", detail=True),
    FieldDef("prog.rec_pending", I, "", "1초 기록 대기", source="Recorder", summary="max", detail=True),
    FieldDef("prog.rec_dropped", I, "", "1초 기록 버린 수", source="Recorder", summary="max", detail=True),
    FieldDef("prog.out_pending", I, "", "기록 출력 대기", source="outputs QueueWriter", summary="max", detail=True),
    FieldDef("prog.events", I, "", "내부 사건 수", source="system.events", detail=True),
    FieldDef("prog.engines", S, "", "실행 엔진", source="system.register_engine", detail=True),
)

EXTRA_FIELDS: tuple = tuple(
    [f for c in CONTEXTS for f in _ctx_fields(c)] + [f for c in ("ch1", "ch2", "pc", "tsp") for f in _target_fields(c)]
    + list(PROGRAM_FIELDS))
ENGINE_COLUMNS = frozenset([f.name for f in EXTRA_FIELDS if not f.name.startswith("prog.")]
                           + heater.engine_columns("ch2")
                           + [f"ch2.thk.{n}" for n in thickness_record.BLOCK_FIELDS])   # 엔진이 채우는 장비 블록 열


def columns() -> list:
    return row_columns(device_blocks(), EXTRA_FIELDS)


# ───────── 프로그램 상태 읽기 ─────────
def _win_counters() -> Dict[str, Optional[float]]:
    out: Dict[str, Optional[float]] = {"working_set_mb": None, "private_mb": None, "handles": None, "gdi": None,
                                       "user": None}
    try:
        import ctypes
        from ctypes import wintypes

        class PMC_EX(ctypes.Structure):
            _fields_ = [("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD),
                        ("PeakWorkingSetSize", ctypes.c_size_t), ("WorkingSetSize", ctypes.c_size_t),
                        ("QuotaPeakPagedPoolUsage", ctypes.c_size_t), ("QuotaPagedPoolUsage", ctypes.c_size_t),
                        ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t), ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                        ("PagefileUsage", ctypes.c_size_t), ("PeakPagefileUsage", ctypes.c_size_t),
                        ("PrivateUsage", ctypes.c_size_t)]
        k = ctypes.WinDLL("kernel32")
        k.GetCurrentProcess.restype = wintypes.HANDLE
        h = k.GetCurrentProcess()
        k.K32GetProcessMemoryInfo.argtypes = [wintypes.HANDLE, ctypes.POINTER(PMC_EX), wintypes.DWORD]
        c = PMC_EX()
        c.cb = ctypes.sizeof(PMC_EX)
        if k.K32GetProcessMemoryInfo(h, ctypes.byref(c), c.cb):
            out["working_set_mb"] = round(c.WorkingSetSize / 1048576, 1)
            out["private_mb"] = round(c.PrivateUsage / 1048576, 1)
        n = wintypes.DWORD(0)
        k.GetProcessHandleCount.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
        if k.GetProcessHandleCount(h, ctypes.byref(n)):
            out["handles"] = int(n.value)
        u = ctypes.WinDLL("user32")
        u.GetGuiResources.argtypes = [wintypes.HANDLE, wintypes.DWORD]
        out["gdi"] = int(u.GetGuiResources(h, 0))      # GR_GDIOBJECTS
        out["user"] = int(u.GetGuiResources(h, 1))     # GR_USEROBJECTS
    except Exception:
        pass
    return out


def _proc_counters() -> Dict[str, Optional[float]]:
    out: Dict[str, Optional[float]] = {"working_set_mb": None, "private_mb": None, "handles": None, "gdi": None,
                                       "user": None, "os_threads": None}
    try:
        with open("/proc/self/status", encoding="ascii", errors="replace") as f:
            for line in f:
                k, _, v = line.partition(":")
                if k == "VmRSS":
                    out["working_set_mb"] = round(int(v.split()[0]) / 1024, 1)
                elif k == "RssAnon":
                    out["private_mb"] = round(int(v.split()[0]) / 1024, 1)
                elif k == "Threads":
                    out["os_threads"] = int(v.split()[0])
    except (OSError, ValueError, IndexError):
        pass
    try:
        out["handles"] = len(os.listdir("/proc/self/fd"))
    except OSError:
        pass
    return out


def program_state() -> Dict[str, Any]:
    """prog.* 값(구할 수 없으면 None). 예외를 내지 않는다."""
    c = _win_counters() if sys.platform == "win32" else _proc_counters()
    out = {f"prog.{k}": v for k, v in c.items()}
    try:
        out["prog.threads"] = threading.active_count()
        g = gc.get_count()
        out["prog.gc0"], out["prog.gc1"], out["prog.gc2"] = int(g[0]), int(g[1]), int(g[2])
    except Exception:
        pass
    return out


def selfwatch_state(row: Mapping[str, Any]) -> Dict[str, Any]:
    """1초 행의 prog.* → outputs.selfwatch 샘플 키(v1 CSV 열). tasks 는 v2 에 asyncio 가 없어 비움(-1 은 selfwatch 가)."""
    keys = ("working_set_mb", "private_mb", "handles", "gdi", "user", "threads", "gc0", "gc1", "gc2")
    return {k: row.get(f"prog.{k}") for k in keys}


# ───────── 엔진이 받는 1초 행 보기 ─────────
USABLE = (Quality.OK.value, Quality.LATE.value)


class RowView(Mapping):
    """닫힌 1초 행(읽기 전용). view["ch1.mfc.ar_sccm"] 처럼 열 이름으로. 품질을 함께 보려면 fresh/value.

    - q(block)            : 블록(장비) 품질 문자열
    - fresh(col)          : 그 값이 이번 행에서 새로 받은 값인가(필드 자기 품질이 있으면 그것, 없으면 블록 품질 ok/late)
    - value(col)          : fresh 일 때만 값, 아니면 None(묵은 값을 판정에 쓰지 않으려는 쪽)
    - last(col)           : 품질과 무관하게 행에 실린 값(stale 이면 마지막 값 — 나이는 age(col))
    """

    def __init__(self, row: Mapping[str, Any]) -> None:
        self.row = row

    def __getitem__(self, k: str) -> Any:
        return self.row[k]

    def __iter__(self):
        return iter(self.row)

    def __len__(self) -> int:
        return len(self.row)

    @property
    def seq(self) -> int:
        return self.row.get("seq")

    @property
    def ts(self) -> str:
        return self.row.get("ts")

    def q(self, block: str) -> Optional[str]:
        return self.row.get(f"{block}.q")

    def _block_of(self, col: str) -> str:
        for b in sorted(BLOCK_ORDER, key=len, reverse=True):
            if col.startswith(b + "."):
                return b
        return col.rsplit(".", 1)[0]

    def fresh(self, col: str) -> bool:
        own = self.row.get(f"{col}.q")
        if own is not None:
            return own in USABLE
        return self.q(self._block_of(col)) in USABLE

    def value(self, col: str) -> Any:
        return self.row.get(col) if self.fresh(col) else None

    def last(self, col: str) -> Any:
        return self.row.get(col)

    def age(self, col: str) -> Optional[float]:
        own = self.row.get(f"{col}.age_s")
        if own is not None or f"{col}.q" in self.row:
            return own
        return self.row.get(f"{self._block_of(col)}.age_s")


def block_qualities(row: Mapping[str, Any], blocks: Iterable[str] = BLOCK_ORDER) -> Dict[str, str]:
    return {b: row.get(f"{b}.q") for b in blocks}
