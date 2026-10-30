"""v1 기록 출력 골든의 입력 정의(v1golden_outputs.py 가 v1 에, v2 시험이 v2 에 같은 입력을 넣는다 — scenario.json 으로 저장).

경로 값 '@/…' 는 시험마다 새로 만든 임시 뿌리(한글 포함 '기록 시험') 기준. 키는 v1 lib.config_common 이름(v2 cfg 와 같음 —
채널 구역 키 THICKNESS_LOG_DIR 만 v2 에서 CHn_ 접두).
"""
from __future__ import annotations

T0 = "2026-10-30T09:15:20.123000"

BASE_CFG = {
    "LOG_ROOT_DIR": "@/로컬/VanaM_Logs/CH1&2",
    "LOCAL_FALLBACK_ROOT": "@/exe 폴더/Logs_LocalFallback",
    "LOCAL_FALLBACK_SYSTEM_DIR": "@/exe 폴더/Logs_LocalFallback/SYSTEM",
    "SPUTTER_CALIB_DB_DIR": "@/G/공유 드라이브/VanaM_Sputter/Sputter/Calib/Database",
    "GDRIVE_LOG_DIR": "@/G/공유 드라이브/VanaM_Sputter/Process_log",
    "GDRIVE_ARC_ALERT_THRESH": 5000,
    "PLC_COIL_LOG_NAS_DIR": "@/로컬/VanaM_Logs/CH1&2/CH1&2_PLC",
    "PLC_COIL_LOG_LOCAL_DIR": "@/exe 폴더/Logs_LocalFallback/PLC_Coil",
    "PLC_COIL_LOG_INTERVAL_S": 5.0,
    "PLC_COIL_LOG_SUMMARY_S": 60.0,
    "CAMERA_LOG_ROOT": "@/로컬/VanaM_Logs/Camera_Logs",
    "CAMERA_MIN_FREE_GB": 0.0,
    "SELFWATCH_DIR": "",
    "HOST_LOG_ENABLED": True,
    "HOST_LOG_NAS_DIR": "@/NAS/VanaM_toShare/JH_Lee/Logs/Robot",
    "HOST_LOG_LOCAL_DIR": "@/로컬/VanaM_Logs/Robot",
    "HOST_LOG_RETRY_S": 30,
    "HOST_LOG_LOCK_STALE_S": 120,
}

# ───────── 공정 txt ─────────
PROCESS_TXT = {
    "process_txt_host": dict(ch=2, t0=T0, events=[
        ["log", "Process", "시작 버튼 — 레시피 확인"],
        ["log", "PLC", "게이트 닫힘 확인"],
        ["t", "2026-10-30T09:15:21.456000"],
        ["open", {"process_note": "ITO 100nm / test:1*", "Process_name": "무시됨", "process_time": 12.5},
         "host", {"request_id": "R-7", "peer": "192.168.1.9:5000"}],
        ["log", "Process", "가스 안정화 대기 60 s"],
        ["drain"],
        ["t", "2026-10-30T09:16:22.000000"],
        ["log", "DCPulse2", "출력 ON 150 W"],
        ["side", "THK", "두께 모니터 연결됨(공정 무관)"],
        ["log", "HEATER", "히터 상태 — PV 200.5°C · SV 210.0°C · 목표 250.0°C · MV 1200/1500 · 램프 · 공정"],
        ["t", "2026-10-30T09:28:59.999000"],
        ["log", "Process", "공정 종료"],
        ["close"],
        ["log", "Process", "종료 뒤 줄(SYSTEM 으로)"],
        ["t", "2026-10-30T09:30:00.000000"],
        ["open", {"Process_name": "  ..  "}, "ui", None],
        ["log", "Process", "이름 없는 공정"],
        ["close"],
    ]),
    "process_txt_names": dict(ch=1, t0="2026-10-31T00:00:01.007000", events=[
        ["open", {"Process_name": "산화막   증착 시험\t(2차) " + "가" * 70, "process_time": "3"}, "presputter", None],
        ["log", "Process", "긴 이름"],
        ["close"],
        ["t", "2026-10-31T00:00:02.000000"],
        ["open", {"process_note": "a<b>c:d\"e|f?g*h\\i/j"}, "ui", None],
        ["log", "Process", "특수문자"],
        ["close"],
    ]),
}

SYSTEM_LOG = dict(lines=[
    ["2026-10-30T23:59:59.900000", "CH1:Process", "자정 전 줄"],
    ["2026-10-31T00:00:00.100000", "HOSTLOG", "자정 뒤 줄 — 새 파일"],
    ["2026-10-31T00:00:01.000000", "WARN/DUMP", "Runtime dump skipped(manual_button): runtime dump already in progress"],
])

# ───────── Calib CSV ─────────
P_CH2 = {
    "t0_wall": "2026-10-30T09:10:05", "process_note": "ITO 100nm", "Process_name": "ITO 100nm",
    "use_ms": True, "shutter_delay": 1.0, "integration_time": 60, "use_power_select": False,
    "G1 Target": "ITO (T-12)", "G2 Target": "", "G3 Target": "Ti (T-3)", "base_pressure": 5e-6,
    "use_dc_pulse": True, "dc_pulse_power": 150.0, "dc_pulse_freq": 100, "dc_pulse_duty": 80,
    "dc_pulse_off_time": 2.5, "use_rf_pulse": True, "rf_pulse_power": 100, "rf_pulse_freq": 50,
    "rf_pulse_duty": 40, "use_dc_power": True, "dc_power": 200.0, "use_rf_power": True, "rf_power": 300.0,
    "chuck_position": "mid", "process_time": 10.0, "Ar_flow": 20.0, "O2_flow": 2.0, "working_pressure": 3.0,
    "operator": "홍길동", "substrate": "Si wafer", "note": "비고, 쉼표", "dep_rate": 0.5, "thickness": 100.0,
}
EV_CH2 = [
    ["start", P_CH2],
    ["ig", 4.2e-6], ["ig", "1.5E-05 Torr"], ["ig", 3.3e-6],
    ["dc", 1.0, 1.0, 1.0],                  # 셔터 닫힘 → 버림
    ["shutter_open"],
    ["dc", 200.1, 400.2, 0.5], ["dc", 199.9, 401.0, 0.49],
    ["dcpulse", 150.3, 310.0, 0.48], ["dcpulse", 149.1, 309.0, 0.47],
    ["rf", 300.0, 2.0], ["rf", 301.5, 1.5],
    ["rfpulse", 100.2, 0.3], ["rfpulse", 99.8, 0.5],
    ["flow", "Ar", 20.1], ["flow", "AR", 19.9], ["flow", "o2", 2.05], ["flow", "He", 9.0],
    ["pressure", "V+3.01"], ["pressure", 2.99], ["pressure", "P=3.05"],
    ["heater", 200.5, 250.0, 1200, True], ["heater", None, 250.0, 1250, True], ["heater", 201.25, 250.0, None, False],
    ["shutter_close"],
    ["dc", 9.0, 9.0, 9.0],
    ["thickness", {"end": "thickness", "target": 100.0, "final_nm": 100.456, "avg_rate": 0.16742,
                   "tooling": 102.5, "density": 7.14, "z_factor": 1.0, "chuck": "mid", "gun": "G1",
                   "fallback_reason": "", "jumps": 2, "material": "ITO", "factor_source": "재료 표"}],
    ["end", {"process_name": "ITO 100nm", "result": "ok", "stopped": False, "actual_shutter_delay_min": 1.0,
             "actual_process_time_min": 10.016666666666667, "soft_arc_count": 3, "hard_arc_count": 1}],
]
P_CH1 = {"t0_wall": "2026-10-30T11:00:00", "Process_name": "Cu seed", "use_ms": False, "use_rf_power": True,
         "rf_power": 120.0, "G1 Target": "Cu", "G2 Target": "무시", "base_pressure": 7e-6, "process_time": 5,
         "shutter_delay": "", "chuck_position": None, "use_dc_pulse": True, "dc_pulse_power": 80,
         "dc_pulse_off_time": "DC", "dc_pulse_freq": "", "Ar_flow": 15.0}


def _ch1(t0, extra_end, ok):
    p = dict(P_CH1, t0_wall=t0)
    return dict(ok=ok, events=[["start", p], ["rf", 120.0, 3.0], ["flow", "Ar", 15.2], ["pressure", "bad"],
                               ["end", dict({"process_name": "", "result": "x"}, **extra_end)]])


CALIB = {
    "calib_ch2_full": dict(ch=2, thickness=True, records=[dict(ok=True, events=EV_CH2)]),
    "calib_ch1_results": dict(ch=1, records=[
        _ch1("2026-10-30T11:00:00", {"stopped": False}, False),
        _ch1("2026-10-30T11:30:00", {"stopped": True, "actual_process_time_min": 2.5}, False),
        _ch1("2026-10-30T12:00:00", {"actual_process_time_min": 5.0}, True),
    ]),
    "calib_upgrade": dict(ch=1, pre_files={
        "G/공유 드라이브/VanaM_Sputter/Sputter/Calib/Database/Ch1_log.csv":
            "\ufeffTimestamp,Process Note,Base Pressure\r\n2026-01-01 10:00:00,old run,1.00e-06\r\n"},
        records=[_ch1("2026-10-30T11:00:00", {}, True)]),
    "calib_upgrade_fail": dict(ch=1, pre_files={
        "G/공유 드라이브/VanaM_Sputter/Sputter/Calib/Database/Ch1_log.csv":
            "\ufeffTimestamp,Process Note,Base Pressure,Result\r\n2026-01-01 10:00:00,old run,1.00e-06,\r\n",
        "G/공유 드라이브/VanaM_Sputter/Sputter/Calib/Database/Ch1_log.tmp": None},
        records=[_ch1("2026-10-30T11:00:00", {}, True)]),
    "calib_pending": dict(ch=1, cfg={"SPUTTER_CALIB_DB_DIR": "@/G/막힘/Database"}, pre_files={"G/막힘": "x"},
                          records=[_ch1("2026-10-30T11:00:00", {}, True),
                                   dict(_ch1("2026-10-30T11:10:00", {"stopped": True}, False), stage="blocked"),
                                   dict(_ch1("2026-10-30T11:20:00", {}, True), unblock=["G/막힘"])]),
}

# ───────── 엑셀 ─────────
_MAIN = "G/공유 드라이브/VanaM_Sputter/Process_log"
P_X1 = {"t0_wall": "2026-10-30T13:00:00", "Process_name": "RF only", "use_ms": True, "use_rf_power": True,
        "rf_power": 100.0, "G1 Target": "Al", "G2 Target": "버림", "G3 Target": "버림", "process_time": 7.5,
        "working_pressure": 2.0, "Ar_flow": 10.0, "chuck_position": "up"}
EV_X1 = [["start", P_X1], ["ig", 2e-6], ["shutter_open"], ["rf", 100.0, 1.0], ["rf", 120.0, 3.0],
         ["flow", "Ar", 12.0], ["pressure", 2.5]]
EXCEL = {
    "excel_ch2_multi": dict(ch=2, thickness=True, main_dir=_MAIN, records=[
        dict(events=EV_CH2, operator="", substrate="Si", note="첫 공정"),
        dict(events=[["start", {"t0_wall": "2026-10-30T14:00:00", "Process_name": "대기만", "base_pressure": 1e-5}]],
             operator="김", substrate="", note=""),
        dict(events=[["start", dict(P_CH2, t0_wall="2026-10-30T15:00:00", use_dc_power=False, use_rf_power=False,
                                    use_rf_pulse=False, dc_pulse_off_time="DC")],
                     ["dcpulse", 150.0, 300.0, 0.5],
                     ["end", {"process_name": "Arc 많음", "soft_arc_count": 3000, "hard_arc_count": 2500}]]),
    ]),
    "excel_ch1_phantom": dict(ch=1, main_dir=_MAIN, pre="phantom", records=[dict(events=EV_X1)]),
    "excel_ch2_extend": dict(ch=2, main_dir=_MAIN, pre="base_only", records=[dict(events=EV_CH2)]),
    "excel_broken": dict(ch=1, main_dir=_MAIN, pre="broken", records=[dict(events=EV_X1)]),
    "excel_blocked": dict(ch=1, main_dir="G/막힘/Process_log", pre="blocked", records=[dict(events=EV_X1)]),
    "excel_pc": dict(ch=2, main_dir=_MAIN, records=[
        dict(t="2026-10-30T16:00:00", pc={"process_name": "Plasma Cleaning", "time": 5.0, "base_pressure": 4.5e-6,
                                          "sp_ar": 20.0, "avg_ar": 25.0, "sp_pressure": 10.0, "avg_pressure": 10.2,
                                          "sp_power": 100.0, "avg_forp": 99.5, "avg_refp": 1.2, "avg_load": None,
                                          "avg_tune": None}),
        dict(t="2026-10-30T16:30:00", pc={"time": 3.0, "sp_ar": 20.0}),
    ]),
}

# ───────── PLC 코일 ─────────
_REG = {"DCV_READ_0": 100, "DCV_READ_1": 0, "DCV_READ_2": 4095, "DCV_READ_3": 7, "DCV_WRITE_0": 1234,
        "DCV_WRITE_1": 0, "DCV_WRITE_2": 55, "DCV_WRITE_3": 0, "DCV_READ_4": 1, "DCV_READ_5": 2, "DCV_READ_6": 3}
PLC_COIL = dict(t0="2026-10-30T23:59:20.500000", cfg={},
                reg_keys=["DCV_READ_0", "DCV_READ_1", "DCV_READ_2", "DCV_READ_3", "DCV_WRITE_0", "DCV_WRITE_1",
                          "DCV_WRITE_2", "DCV_WRITE_3", "DCV_READ_4", "DCV_READ_5", "DCV_READ_6", "DCV_READ_7"],
                steps=[
                    dict(kind="ok", on=["R_P_1_OFF_TIMER"], regs=_REG),
                    dict(kind="ok", on=[], regs=_REG),
                    dict(kind="disconnected"),
                    dict(kind="backoff"),
                    dict(kind="empty"),
                    dict(kind="ok", on=["L_R_P_OFF_TIMER"], reg_fail=True),
                    dict(kind="ok", on=["R_P_2_OFF_TIMER"], regs=_REG),
                    dict(kind="ok", on=[], regs={}),
                    dict(kind="ok", on=[], regs=_REG),
                    dict(kind="ok", on=[], regs=_REG),
                    dict(kind="disconnected"),
                    dict(kind="ok", on=[], regs=_REG),
                    dict(kind="ok", on=["R_P_1_OFF_TIMER", "L_R_P_OFF_TIMER"], regs=_REG),
                    dict(kind="ok", on=[], regs=_REG),
                    dict(kind="ok", on=[], regs=_REG),
                ])

# ───────── 두께·히터 ─────────
_ST = {"connected": True, "thickness_nm": 12.3456, "rate_nm_s": 0.12345, "freq_hz": 5987654.7, "life_pct": 87.25,
       "crystal_ok": True, "stale": False}
THICKNESS = dict(ch=2, t0="2026-10-30T23:59:58", process_name="ITO 100nm",
                 cfg={"THICKNESS_LOG_DIR": "@/로컬/VanaM_Logs/CH1&2/Thickness"},
                 events=[["row", _ST], ["t", "2026-10-30T23:59:59"],
                         ["row", {"connected": False, "thickness_nm": None, "rate_nm_s": None, "freq_hz": None,
                                  "life_pct": None, "crystal_ok": None, "stale": True}],
                         ["t", "2026-10-31T00:00:00"], ["row", _ST], ["close"],
                         ["t", "2026-10-31T00:05:00"], ["row", dict(_ST, thickness_nm="bad", freq_hz=0)]])
_HST = {"pv": 25.04, "sv": 250.0, "sv_ramp": 30.0, "cur_sv": 29.95, "mv": 1200.4, "mv_limit": 1500, "mv_pct": 80.03,
        "pid_err": -12.6, "ramp_rate": 5, "holdback": 10.0, "ot_limit": 300.0, "run": True, "itl": False,
        "fault": False, "ot": False, "tc_err": False, "wd_err": False, "tc_ok": True, "da_ok": True,
        "water_ok": True, "vac_ok": True, "pid_run": True, "output_dead": False}
HEATER = dict(t0="2026-10-30T10:00:00", cfg={}, events=[
    ["t", "2026-10-30T10:00:00", 100.0], ["start"],
    ["row", _HST, {"owner": "공정", "hold": [False, None], "sat_clamp": None, "ramp_target": 250.0}],
    ["t", "2026-10-30T10:00:03", 103.04], ["row", dict(_HST, pv=None, mv="x"),
                                          {"owner": "공정", "hold": [True, 1450], "sat_clamp": 1400}],
    ["t", "2026-10-30T10:00:06", 106.0], ["row", dict(_HST, run=False), {"owner": "공정", "note": "운전 종료"}],
    ["stop"], ["start"], ["row", _HST, {"owner": "외부"}], ["stop"],
])

# ───────── 카메라·selfwatch·ERROR·덤프 ─────────
CAMERA = dict(t0="2026-10-30T09:20:00.000000", mode="CH2", frames=3, cfg={"CAMERA_MIN_FREE_GB": 0.0})
SELFWATCH = dict(t0="2026-10-30T09:00:00", cfg={}, samples=[
    ["2026-10-30T09:00:30", {"working_set_mb": 512.3, "private_mb": 640.0, "handles": 900, "threads": 31,
                             "gc0": 10, "gc1": 2, "gc2": 1}],
    ["2026-10-30T09:01:00", {"working_set_mb": 2100.1, "private_mb": 2500.5, "handles": 6000, "gdi": 100,
                             "user": 120, "threads": 33, "gc0": 1, "gc1": 0, "gc2": 3}],
    ["2026-10-31T00:00:10", {"working_set_mb": 500.0}],
])
APP_LOG = dict(t0="2026-10-30T09:00:00", logger="CH_1_2_program", cfg={}, records=[
    dict(t="2026-10-30T09:00:00.001", level="INFO", msg="system logging ready. root=%s", args=["C:\\x"]),
    dict(t="2026-10-30T09:00:01.999", level="WARNING", msg="[%s] %s", args=["WARN/DUMP", "덤프 건너뜀"]),
    dict(t="2026-10-30T09:00:02.500", level="ERROR", msg="TASK CRASHED", args=[], exc="boom 한글"),
    dict(t="2026-10-30T09:00:03.000", level="CRITICAL", msg="PROCESS SIGNAL RECEIVED: %s (%s)", args=["SIGINT", 2]),
    dict(t="2026-10-30T09:00:04.000", level="DEBUG", msg="버려짐", args=[]),
], classify=[["ERROR/DUMP", "x"], ["CH1:Process", "❌ 실패"], ["PLC", "2026-10-30 09:00:00 [CMD] WARN 늦음"],
             ["WARN", "a"], ["CH2", "⚠ 경고"], ["CH2", "정상"], ["ERR", "x"], ["x", "error: lower"]])
RUNTIME_DUMP = dict(reason="manual_button", state={
    "runtime_state": {"ch1": "running", "ch2": "idle", "pc": None}, "pc_use_ch": 1, "plc_owner": None,
    "closing_started": False, "close_ready": True, "plc": {"connected": True, "host": "127.0.0.1", "port": 502},
    "ch1": {"state": "running", "step": 3, "name": "ITO 공정 " + "x" * 500}, "server_page": None})

# ───────── Robot CSV ─────────
_R = "2026-10-30T09:15:20.123000"
ROBOT = {
    "robot_basic": dict(t0=_R, events=[
        ["request", "a", {"target": "CH1", "request_id": "REQ 1/ä", "peer": "192.168.1.30:51000",
                          "recipe_name": "r.csv"}],
        ["update", "a", {"row_count": 3, "process_names": "A / B / C", "recipe_name": None}],
        ["t", "2026-10-30T09:15:25.000000"], ["started", "a", "CH1_A_20261030_091525_000.txt"],
        ["t", "2026-10-30T09:20:00.000000"], ["started", "a", "CH1_B_20261030_092000_000.txt"],
        ["t", "2026-10-30T09:27:31.000000"], ["finalize", "a", "성공", "무시되는 사유"],
        ["request", "b", {"target": "PC(CH2)", "request_id": "R2", "peer": "p", "recipe_name": "pc.csv"}],
        ["reject", "b", "E205 busy\r\n두 번째 줄\r\r셋째"],
        ["request", "d1", {"target": "CH2", "request_id": "DUP", "peer": "q", "received_at": _R}],
        ["request", "d2", {"target": "CH2", "request_id": "DUP", "peer": "q", "received_at": _R}],
        ["request", "d3", {"target": "CH2", "request_id": "DUP", "peer": "q", "received_at": _R}],
        ["started", "d1", ""], ["t", "2026-10-30T09:40:00.000000"],
        ["finalize", "d1", "STOP", "사용자 STOP"], ["finalize", "d1", "STOP", "두 번째(무시)"],
        ["flush"],
        ["request", "e", {"target": "CH1", "request_id": "E", "peer": "r"}],
        ["started", "d3", "d3.txt"],
        ["restart"], ["recover"], ["flush"],
    ]),
    "robot_nas_blocked": dict(t0=_R, cfg={"HOST_LOG_NAS_DIR": "@/NAS막힘/blocker/Robot"},
                              pre_files={"NAS막힘/blocker": {"text": "x"}}, events=[
        ["request", "a", {"target": "CH1", "request_id": "P1", "peer": "x", "received_at": _R}],
        ["finalize", "a", "실패", "시작 실패: E301"], ["flush"],
        ["request", "b", {"target": "CH1", "request_id": "P1", "peer": "x", "received_at": _R}],
        ["finalize", "b", "성공", ""], ["flush"], ["stage", "blocked"],
        ["unblock", "NAS막힘/blocker"], ["flush"],
    ]),
    "robot_nas_dup_truncated": dict(t0=_R, pre_files={
        "NAS/VanaM_toShare/JH_Lee/Logs/Robot/Robot_20261030.csv": {
            "text": "\ufeff대상,요청시각,시작시각,종료시각,소요(분),결과,사유,공정명,레시피,행개수,request_id,peer,로그파일,프로그램,기록키\r\n"
                    "CH1,09:00:00,,,,성공,,,,,X,ald,,ald,sputter-20261030091520123-R9\r\n"
                    "ALD1,09:01:00,09:01:05,,,진행"}}, events=[
        ["request", "a", {"target": "CH1", "request_id": "R9", "peer": "x"}],
        ["finalize", "a", "성공", ""],
        ["request", "b", {"target": "CH2", "request_id": "R10", "peer": "y"}],
        ["finalize", "b", "미확인", "종료 경로 미확인"], ["flush"],
    ]),
    "robot_stale_lock": dict(t0=_R, pre_files={
        "NAS/VanaM_toShare/JH_Lee/Logs/Robot/_lock/Robot.lock": {"text": "ald 99 old", "age_s": 500}}, events=[
        ["request", "a", {"target": "CH1", "request_id": "S", "peer": "x"}], ["finalize", "a", "성공", ""],
        ["flush"]]),
    "robot_fresh_lock": dict(t0=_R, pre_files={
        "NAS/VanaM_toShare/JH_Lee/Logs/Robot/_lock/Robot.lock": {"text": "ald 99 now", "age_s": 0}}, events=[
        ["request", "a", {"target": "CH1", "request_id": "F", "peer": "x"}], ["finalize", "a", "성공", ""],
        ["flush"]]),
    "robot_stale_cfg_exc": dict(t0=_R, del_cfg=["HOST_LOG_LOCK_STALE_S"], pre_files={
        "NAS/VanaM_toShare/JH_Lee/Logs/Robot/_lock/Robot.lock": {"text": "ald 99 60s", "age_s": 60}}, events=[
        ["request", "a", {"target": "CH1", "request_id": "C", "peer": "x"}], ["finalize", "a", "성공", ""],
        ["flush"]]),
}
