"""로봇 명령 × 상태 사례 표 — v1golden_robot_handlers.py(v1 실행)와 tests/test_robot_ops.py(v2 실행)가 같은 사례를 돈다.

사례 = dict(name, cmd, data, state, eng, events, jobs, raw, recipe_rows, files)
- state: sim.robot_rig.apply_state 의 상태(게이트·로딩 센서·척·핀·로드락·코일·강제·무응답).
- eng: 공정 쪽 사실(v1 말로) — rs{running:[[kind, ch]], error:[[kind, ch]], finished:[[kind, ch, t]]}, rt{ch: {runner_state, stage,
  pc_running, step, eta, queue:[행 수, 지금 행], min_total}}, pc{selected_ch, is_running}, start/pc_start{raise:[code, msg, type] |
  preflight{after_s, ok, code, detail, owned}, rows}, scan_delay_s. eng=None 이면 런타임 없음(v1 ctx.ch1/ch2/pc/runtime_state None).
- events: [[가상 초, state]] — 명령 시작 뒤 그 시각에 시뮬레이터 상태를 바꾼다(러핑 중 게이트 열림 등).
- jobs: [[지연 초, cmd, data]] — 여러 명령을 동시에(로드락 전환 중 E321, PC 시작 중복 E206, 잠금 대기).
- data 의 '{RECIPE}' 는 recipe_rows 행짜리 레시피 CSV 경로로, GET_RECIPE 폴더는 files 로 만든다(경로는 비교 때 '<ROOT>').
"""
from __future__ import annotations

from typing import Any, Dict, List

IDLE_ENG: Dict[str, Any] = {"rs": {}, "rt": {"1": {}, "2": {}}, "pc": {"selected_ch": 1, "is_running": False}}
VAC = {"ll": "vacuum"}                    # 로드락 진공(L_ATM FALSE) — 게이트 열기 조건


def eng(**kw) -> Dict[str, Any]:
    import copy
    e = copy.deepcopy(IDLE_ENG)
    for k, v in kw.items():
        if k == "rt":
            for ch, d in v.items():
                e["rt"].setdefault(str(ch), {}).update(d)
        elif k == "no_rt":
            for ch in v:
                e["rt"].pop(str(ch), None)
        else:
            e[k] = v
    return e


def _c(out: List[dict], name: str, cmd: str, data=None, **kw) -> None:
    out.append(dict(name=name, cmd=cmd, data={} if data is None else data, **kw))


def _status(out: List[dict]) -> None:
    _c(out, "status_no_runtime", "GET_SPUTTER_STATUS", eng=None)
    _c(out, "status_idle_vacuum", "GET_SPUTTER_STATUS", state=VAC, eng=eng())
    _c(out, "status_idle_atm", "GET_SPUTTER_STATUS", eng=eng())
    _c(out, "status_ch1_running", "GET_SPUTTER_STATUS", eng=eng(rs={"running": [["chamber", 1]]}))
    _c(out, "status_ch2_pc_running", "GET_SPUTTER_STATUS", eng=eng(rs={"running": [["pc", 2]]}))
    _c(out, "status_ch2_error", "GET_SPUTTER_STATUS", eng=eng(rs={"error": [["chamber", 2]]}))
    _c(out, "status_ch2_error_but_cooldown", "GET_SPUTTER_STATUS",
       eng=eng(rs={"error": [["chamber", 2]]}, rt={2: {"runner_state": "COOLDOWN"}}))
    _c(out, "status_ch1_stage_alive", "GET_SPUTTER_STATUS", eng=eng(rt={1: {"stage": "ADVANCE_QUEUE"}}))
    _c(out, "status_ch1_runner_idle_lower", "GET_SPUTTER_STATUS", eng=eng(rt={1: {"runner_state": "idle"}}))
    _c(out, "status_ll_pc1_running", "GET_SPUTTER_STATUS", eng=eng(rs={"running": [["pc", 1]]}))
    _c(out, "status_ll_last_pc_error", "GET_SPUTTER_STATUS",
       eng=eng(rs={"error": [["pc", 1]], "finished": [["pc", 1, 200.0], ["pc", 2, 100.0]]}))
    _c(out, "status_ll_error_then_ok", "GET_SPUTTER_STATUS",
       eng=eng(rs={"error": [["pc", 1]], "finished": [["pc", 1, 100.0], ["pc", 2, 200.0]]}))
    _c(out, "status_ll_error_no_finish", "GET_SPUTTER_STATUS", eng=eng(rs={"error": [["pc", 2]]}))
    _c(out, "status_ll_ig_wait", "GET_SPUTTER_STATUS", eng=eng(rt={1: {"pc_running": True, "step": "IG_CMD"}}))
    _c(out, "status_ll_ig_wait_no_step", "GET_SPUTTER_STATUS", eng=eng(rt={1: {"pc_running": True, "step": None}}))
    _c(out, "status_ll_other_step", "GET_SPUTTER_STATUS", eng=eng(rt={1: {"pc_running": True, "step": "DELAY"}}))
    _c(out, "status_ll_pc_cleaning", "GET_SPUTTER_STATUS", eng=eng(pc={"selected_ch": 2, "is_running": True}))
    _c(out, "status_eta", "GET_SPUTTER_STATUS",
       eng=eng(rt={1: {"pc_running": True, "step": "DELAY", "eta": 300}, 2: {"eta": 0}}))
    _c(out, "status_eta_queue_not_last", "GET_SPUTTER_STATUS",
       eng=eng(rt={1: {"eta": 300, "queue": [3, 1]}, 2: {"eta": 50, "queue": [3, 2]}}))
    _c(out, "status_plc_no_response", "GET_SPUTTER_STATUS", state={"mute": ["L_ATM"]}, eng=eng())
    _c(out, "status_lowercase", "get_sputter_status", eng=eng(), raw={"command": " get_sputter_status "})
    for n in (1, 2):
        _c(out, f"loading{n}_false", f"GET_LOADING_{n}_SENSOR", eng=eng())
        _c(out, f"loading{n}_true", f"GET_LOADING_{n}_SENSOR", state={"loading": {str(n): True}}, eng=eng())
        _c(out, f"loading{n}_no_response", f"GET_LOADING_{n}_SENSOR",
           state={"mute": [f"LOADING_{n}_SENSOR_LAMP"]}, eng=eng())


def _recipe(out: List[dict]) -> None:
    files = {"CH1": ["b.csv", "A.CSV", "c.xlsx", "note.txt", "sub/", "Z.Xlsx"], "CH2": [], "ALD_file": "ALD"}
    _c(out, "recipe_ch1", "GET_RECIPE", {"folder": " ch1 "}, eng=eng(), files=files)
    _c(out, "recipe_ch2_empty", "GET_RECIPE", {"folder": "CH2"}, eng=eng(), files=files)
    _c(out, "recipe_ald_not_dir", "GET_RECIPE", {"folder": "ALD"}, eng=eng(), files=files)
    _c(out, "recipe_ald_missing", "GET_RECIPE", {"folder": "ald"}, eng=eng(), files={"CH1": []})
    _c(out, "recipe_bad_folder", "GET_RECIPE", {"folder": "XYZ"}, eng=eng(), files=files)
    _c(out, "recipe_empty_folder", "GET_RECIPE", {}, eng=eng(), files=files)
    _c(out, "recipe_timeout", "GET_RECIPE", {"folder": "CH1"}, eng=eng(scan_delay_s=25.0), files=files)


def _start(out: List[dict]) -> None:
    R = "{RECIPE}"
    ok = {"preflight": {"after_s": 1.5, "ok": True, "owned": True}, "rows": 2}
    _c(out, "start_ch_missing", "START_SPUTTER", {"recipe": R}, eng=eng(), recipe_rows=2)
    _c(out, "start_ch3", "START_SPUTTER", {"ch": 3, "recipe": R}, eng=eng(), recipe_rows=2)
    _c(out, "start_ch_abc", "START_SPUTTER", {"ch": "abc", "recipe": R}, eng=eng(), recipe_rows=2)
    _c(out, "start_ch_abc_lower", "start_sputter", {"ch": "abc", "recipe": R}, eng=eng(), recipe_rows=2,
       raw={"command": "start_sputter"})
    _c(out, "start_recipe_empty", "START_SPUTTER", {"ch": 1, "recipe": "  "}, eng=eng())
    _c(out, "start_no_runtime", "START_SPUTTER", {"ch": 1, "recipe": R}, eng=None, recipe_rows=2)
    _c(out, "start_ch2_no_rt", "START_SPUTTER", {"ch": 2, "recipe": R}, eng=eng(no_rt=[2]), recipe_rows=2)
    busy = [("rs_chamber", dict(rs={"running": [["chamber", 1]]})), ("rs_pc", dict(rs={"running": [["pc", 1]]})),
            ("tsp", dict(rs={"running": [["tsp", 0]]})),
            ("multi", dict(rs={"running": [["chamber", 1], ["pc", 1], ["tsp", 0]]},
                           rt={1: {"runner_state": "RUNNING", "stage": "PROCESS"}}))]
    for st in ("PREFLIGHT", "RUNNING", "CLEANUP", "STOPPING", "COOLDOWN", "DELAY", "WEIRD"):
        busy.append((f"runner_{st.lower()}", dict(rt={1: {"runner_state": st}})))
    for k in ("ADVANCE_QUEUE", "PROCESS", "PREFLIGHT", "CLEANUP", "OTHER", ""):
        busy.append((f"stage_{k.lower() or 'none'}", dict(rt={1: {"stage": k}})))
    for nm, kw in busy:
        _c(out, f"start_busy_{nm}", "START_SPUTTER", {"ch": 1, "recipe": R}, eng=eng(**kw), recipe_rows=2)
    _c(out, "start_ch2_tsp_not_busy", "START_SPUTTER", {"ch": 2, "recipe": R},
       eng=eng(rs={"running": [["tsp", 0]]}, start=ok), recipe_rows=2)
    for g in ("open", "moving", "both"):
        _c(out, f"start_gate_{g}", "START_SPUTTER", {"ch": 1, "recipe": R}, state={"gate": {"1": g}}, eng=eng(),
           recipe_rows=2)
    _c(out, "start_gate_no_response", "START_SPUTTER", {"ch": 2, "recipe": R}, state={"mute": ["G_V_2_CLOSE_LAMP"]},
       eng=eng(), recipe_rows=2)
    _c(out, "start_mv_interlock_off", "START_SPUTTER", {"ch": 1, "recipe": R}, state={"coils": {"M_V_1_인터락": False}},
       eng=eng(), recipe_rows=2)
    _c(out, "start_mv_closed", "START_SPUTTER", {"ch": 2, "recipe": R}, state={"coils": {"M_V_2_SW": False}},
       eng=eng(), recipe_rows=2)
    raises = [("e207", ["E207", "레시피 파일을 찾을 수 없습니다: X.csv", None]),
              ("recipe_error", [None, "레시피 오류 2건:\n - 2행 gas: 값 없음\n - 3행 power: 범위 밖", None]),
              ("cooldown", [None, "cooldown 42s remaining", None]),
              ("one_minute", [None, "공정 종료 후 1분 대기 중", None]),
              ("already", [None, "this chamber already running", None]),
              ("value_error", [None, "bad value", "ValueError"])]
    for nm, rz in raises:
        _c(out, f"start_raise_{nm}", "START_SPUTTER", {"ch": 1, "recipe": R}, eng=eng(start={"raise": rz}), recipe_rows=2)
    _c(out, "start_ok", "START_SPUTTER", {"ch": 1, "recipe": R}, eng=eng(start=ok, rt={1: {"queue": [2, -1],
                                                                                         "min_total": 1290}}),
       recipe_rows=2)
    _c(out, "start_ok_ch2_str", "START_SPUTTER", {"ch": "2", "recipe": R}, eng=eng(start=ok), recipe_rows=2)
    _c(out, "start_ok_no_queue", "START_SPUTTER", {"ch": 1.9, "recipe": R},
       eng=eng(start={"preflight": {"after_s": 0.0, "ok": True, "owned": True}, "rows": 2}), recipe_rows=2)
    _c(out, "start_preflight_reject", "START_SPUTTER", {"ch": 1, "recipe": R},
       eng=eng(start={"preflight": {"after_s": 3.0, "ok": False, "detail": "runner busy: CLEANUP"}, "rows": 2}),
       recipe_rows=2)
    _c(out, "start_preflight_reject_cooldown", "START_SPUTTER", {"ch": 2, "recipe": R},
       eng=eng(start={"preflight": {"after_s": 0.5, "ok": False, "detail": "cooldown 12s remaining"}, "rows": 2}),
       recipe_rows=2)
    _c(out, "start_preflight_timeout", "START_SPUTTER", {"ch": 1, "recipe": R},
       eng=eng(start={"preflight": None, "rows": 2}), recipe_rows=2)


def _pc(out: List[dict]) -> None:
    R = "{RECIPE}"
    _c(out, "pc_recipe_empty", "START_PLASMA_CLEANING", {}, eng=eng())
    _c(out, "pc_no_runtime", "START_PLASMA_CLEANING", {"recipe": R}, eng=None, recipe_rows=1)
    for nm, rz in [("e207", ["E207", "CSV 파일을 찾을 수 없습니다: X.csv", None]),
                   ("e205", ["E205", "CH1 Plasma Cleaning 실행 중", None]),
                   ("recipe_error", [None, "레시피 오류 1건:\n - x", None]),
                   ("cooldown", [None, "Plasma Cleaning cooldown 10s", None]),
                   ("value_error", [None, "boom", "ValueError"])]:
        _c(out, f"pc_raise_{nm}", "START_PLASMA_CLEANING", {"recipe": R}, eng=eng(pc_start={"raise": rz}), recipe_rows=1)
    _c(out, "pc_ok_ch2", "START_PLASMA_CLEANING", {"recipe": R},
       eng=eng(pc={"selected_ch": 2, "is_running": False},
               pc_start={"preflight": {"after_s": 2.0, "ok": True, "owned": True}}), recipe_rows=1)
    _c(out, "pc_fail_code", "START_PLASMA_CLEANING", {"recipe": R},
       eng=eng(pc_start={"preflight": {"after_s": 1.0, "ok": False, "code": "E302", "detail": "PC 게이트 인터락"}}),
       recipe_rows=1)
    _c(out, "pc_fail_empty_reason", "START_PLASMA_CLEANING", {"recipe": R},
       eng=eng(pc_start={"preflight": {"after_s": 1.0, "ok": False, "detail": ""}}), recipe_rows=1)
    _c(out, "pc_timeout", "START_PLASMA_CLEANING", {"recipe": R}, eng=eng(pc_start={"preflight": None}),
       recipe_rows=1)
    _c(out, "pc_double_e206", "START_PLASMA_CLEANING", eng=eng(pc_start={"preflight": {"after_s": 5.0, "ok": True,
                                                                                        "owned": True}}),
       recipe_rows=1, jobs=[[0.0, "START_PLASMA_CLEANING", {"recipe": R}], [1.0, "START_PLASMA_CLEANING", {"recipe": R}]])


def _vacuum(out: List[dict]) -> None:
    E = eng()
    _c(out, "von_gate1_open", "VACUUM_ON", state={"gate": {"1": "open"}}, eng=E)
    _c(out, "von_gate2_moving", "VACUUM_ON", state={"gate": {"2": "moving"}}, eng=E)
    _c(out, "von_gate_no_response", "VACUUM_ON", state={"mute": ["G_V_1_OPEN_LAMP"]}, eng=E)
    _c(out, "von_already_ready", "VACUUM_ON", state={"ll": "vacuum_ready"}, eng=E)
    _c(out, "von_ready_roughing_on", "VACUUM_ON",
       state={"ll": "vacuum_ready", "forced": {"L_R_P_SW": True}}, events=[[3.0, {"forced": {"L_R_P_SW": False}}]],
       eng=E)
    _c(out, "von_ready_roughing_stuck", "VACUUM_ON", state={"ll": "vacuum_ready", "forced": {"L_R_V_SW": True}}, eng=E)
    _c(out, "von_off_timer", "VACUUM_ON", state={"coils": {"L_R_P_OFF_TIMER": True}}, eng=E)
    _c(out, "von_rv_interlock_off", "VACUUM_ON", state={"coils": {"L_R_V_인터락": False}}, eng=E)
    _c(out, "von_full", "VACUUM_ON", eng=E)
    _c(out, "von_lp_step_active", "VACUUM_ON", {"timeout_s": 120},
       state={"coils": {"L_R_P_SW": True, "L_R_V_SW": True}}, eng=E)
    _c(out, "von_timeout_30", "VACUUM_ON", {"timeout_s": 30}, state={"no_vacuum": True}, eng=E)
    _c(out, "von_not_ready", "VACUUM_ON", state={"no_vacuum": True}, eng=E)
    _c(out, "von_gate_opens", "VACUUM_ON", eng=E, events=[[20.0, {"gate": {"2": "open"}}]])
    _c(out, "von_valve_drops", "VACUUM_ON", eng=E, events=[[20.0, {"forced": {"L_R_V_SW": False}}]])
    _c(out, "von_valve_drops_lp1", "VACUUM_ON", eng=E, events=[[40.0, {"forced": {"L_R_V_SW": False}}]])
    _c(out, "von_valve_drops_air", "VACUUM_ON", eng=E,
       events=[[40.0, {"forced": {"L_R_V_SW": False}, "coils": {"L_R_P_OFF_TIMER": True}}]])
    _c(out, "von_pump_drops", "VACUUM_ON", eng=E, events=[[40.0, {"forced": {"L_R_P_SW": False}}]])
    _c(out, "von_both_drop", "VACUUM_ON", eng=E, events=[[20.0, {"forced": {"L_R_P_SW": False, "L_R_V_SW": False}}]])
    _c(out, "von_both_drop_ready_late", "VACUUM_ON", eng=E,
       events=[[20.0, {"forced": {"L_R_P_SW": False, "L_R_V_SW": False}}], [27.0, {"forced": {"L_VAC_READY_SW": True}}]])
    _c(out, "von_both_drop_lp", "VACUUM_ON", eng=E,
       events=[[40.0, {"forced": {"L_R_P_SW": False, "L_R_V_SW": False, "LP_STEP1": True}}]])
    _c(out, "von_timeout_abc", "VACUUM_ON", {"timeout_s": "abc"}, eng=E)
    _c(out, "von_vent_write_fail", "VACUUM_ON", state={"mute_write": ["L_VENT_SW"]}, eng=E)
    _c(out, "von_valve_write_fail", "VACUUM_ON", state={"mute_write": ["L_R_V_SW"]}, eng=E)
    _c(out, "voff_write_fail", "VACUUM_OFF", state={"ll": "vacuum", "mute_write": ["L_R_P_SW"]}, eng=E)
    _c(out, "voff_already_atm", "VACUUM_OFF", eng=E)
    _c(out, "voff_vent", "VACUUM_OFF", state=VAC, eng=E)
    _c(out, "voff_vent_interlock_off", "VACUUM_OFF", state={"ll": "vacuum", "coils": {"L_VENT_인터락": False}}, eng=E)
    _c(out, "voff_timeout", "VACUUM_OFF", {"timeout_s": 10}, state=VAC, eng=E)
    _c(out, "voff_gate_open", "VACUUM_OFF", state={"ll": "vacuum", "gate": {"2": "open"}}, eng=E)
    _c(out, "voff_gate_both", "VACUUM_OFF", state={"gate": {"1": "both"}}, eng=E)
    _c(out, "v_on_then_off", "VACUUM_ON", eng=E, jobs=[[0.0, "VACUUM_ON", {"timeout_s": 20}], [2.0, "VACUUM_OFF", {}],
                                                       [3.0, "CH1_GATE_OPEN", {}], [4.0, "CH2_GATE_CLOSE", {}]])
    _c(out, "v_off_then_on", "VACUUM_OFF", state=VAC, eng=E, jobs=[[0.0, "VACUUM_OFF", {}], [1.0, "VACUUM_ON", {}]])


def _pins(out: List[dict]) -> None:
    E = eng()
    for up, nm in ((True, "4PIN_UP"), (False, "4PIN_DOWN")):
        k = nm.lower()
        _c(out, f"{k}_interlock_off", nm, state={"coils": {"L_PIN_인터락": False}}, eng=E)
        _c(out, f"{k}_ok", nm, state={"pin": "down" if up else "up"}, eng=E)
        _c(out, f"{k}_lamp_stuck", nm, {"wait_s": 3, "poll_s": 0.7, "settle_s": 1},
           state={"forced": {"L_PIN_UP_LAMP" if up else "L_PIN_DOWN_LAMP": False}}, eng=E)
        _c(out, f"{k}_lamp_no_response", nm, state={"mute": ["L_PIN_UP_LAMP" if up else "L_PIN_DOWN_LAMP"]}, eng=E)
        _c(out, f"{k}_bad_arg", nm, {"wait_s": "x"}, eng=E)
        _c(out, f"{k}_pulse_fail", nm, state={"mute_write": ["L_PIN_UP_SW" if up else "L_PIN_DOWN_SW"]}, eng=E)
    _c(out, "4pin_up_busy_ignored", "4PIN_UP", eng=eng(rs={"running": [["chamber", 1]]}))


def _gates(out: List[dict]) -> None:
    for ch in (1, 2):
        o = 2 if ch == 1 else 1
        G = f"CH{ch}_GATE_OPEN"
        C = f"CH{ch}_GATE_CLOSE"
        _c(out, f"g{ch}open_busy", G, eng=eng(rt={ch: {"runner_state": "RUNNING"}}))
        _c(out, f"g{ch}open_both", G, state={"gate": {str(ch): "both"}}, eng=eng())
        _c(out, f"g{ch}open_already", G, state={"gate": {str(ch): "open"}}, eng=eng())
        _c(out, f"g{ch}open_atm", G, eng=eng())
        _c(out, f"g{ch}open_vent_rough", G, state={"ll": "vacuum", "coils": {"L_VENT_SW": True, "L_R_V_SW": True}},
           eng=eng())
        _c(out, f"g{ch}open_other_open", G, state={"ll": "vacuum", "gate": {str(o): "open"}}, eng=eng())
        _c(out, f"g{ch}open_interlock_off", G, state={"ll": "vacuum", "coils": {f"G_V_{ch}_인터락": False}}, eng=eng())
        _c(out, f"g{ch}open_ok", G, state=VAC, eng=eng())
        _c(out, f"g{ch}open_short_wait", G, {"wait_s": 1}, state=VAC, eng=eng())
        _c(out, f"g{ch}open_no_response", G, state={"ll": "vacuum", "mute": [f"G_V_{ch}_OPEN_LAMP"]}, eng=eng())
        _c(out, f"g{ch}open_ch_in_data_ignored", G, {"ch": o}, state=VAC, eng=eng())
        _c(out, f"g{ch}close_busy", C, eng=eng(rs={"running": [["pc", ch]]}))
        _c(out, f"g{ch}close_already", C, eng=eng())
        _c(out, f"g{ch}close_both", C, state={"gate": {str(ch): "both"}}, eng=eng())
        _c(out, f"g{ch}close_no_loading", C, state={"gate": {str(ch): "open"}}, eng=eng())
        _c(out, f"g{ch}close_ok", C, state={"gate": {str(ch): "open"}, "loading": {str(ch): True}}, eng=eng())
        _c(out, f"g{ch}close_short_wait", C, {"wait_s": 1}, state={"gate": {str(ch): "open"}, "loading": {str(ch): True}},
           eng=eng())
        _c(out, f"g{ch}close_loading_no_response", C,
           state={"gate": {str(ch): "open"}, "mute": [f"LOADING_{ch}_SENSOR_LAMP"]}, eng=eng())
        _c(out, f"g{ch}close_bad_arg", C, {"wait_s": "x"}, eng=eng())
    _c(out, "g2open_ms_interlock_off_already", "CH2_GATE_OPEN",
       state={"gate": {"2": "open"}, "coils": {"MAIN_SHUTTER_2_인터락": False}}, eng=eng())
    _c(out, "g2open_ms_interlock_off_after", "CH2_GATE_OPEN", state={"ll": "vacuum"}, eng=eng(),
       events=[[1.0, {"coils": {"MAIN_SHUTTER_2_인터락": False}}]])
    _c(out, "g1open_no_engine", "CH1_GATE_OPEN", state=VAC, eng=None)
    O2L = {"gate": {"2": "open"}, "loading": {"2": True}}
    _c(out, "g2close_shutter_fail", "CH2_GATE_CLOSE", state=dict(O2L, mute_write=["MAIN_SHUTTER_2_SW"]), eng=eng())
    _c(out, "g2close_shutter_fail_lamp", "CH2_GATE_CLOSE", {"wait_s": 1},
       state=dict(O2L, mute_write=["MAIN_SHUTTER_2_SW"]), eng=eng())
    _c(out, "g2close_already_shutter_fail", "CH2_GATE_CLOSE", state={"mute_write": ["MAIN_SHUTTER_2_SW"]}, eng=eng())
    _c(out, "g2open_already_shutter_fail", "CH2_GATE_OPEN", state={"gate": {"2": "open"},
                                                                   "mute_write": ["MAIN_SHUTTER_2_SW"]}, eng=eng())
    _c(out, "g1open_pulse_fail", "CH1_GATE_OPEN", state={"ll": "vacuum", "mute_write": ["G_V_1_OPEN_SW"]}, eng=eng())
    _c(out, "g_open_both_channels", "CH1_GATE_OPEN", state=VAC, eng=eng(),
       jobs=[[0.0, "CH1_GATE_OPEN", {}], [0.5, "CH2_GATE_OPEN", {}]])
    _c(out, "g_open_while_close", "CH1_GATE_CLOSE", state={"ll": "vacuum", "gate": {"1": "open"}, "loading": {"1": True}},
       eng=eng(), jobs=[[0.0, "CH1_GATE_CLOSE", {}], [0.2, "CH1_GATE_OPEN", {}]])


def _chuck(out: List[dict]) -> None:
    for ch in (1, 2):
        U, D = f"CH{ch}_CHUCK_UP", f"CH{ch}_CHUCK_DOWN"
        s = str(ch)
        _c(out, f"c{ch}up_busy", U, eng=eng(rt={ch: {"runner_state": "DELAY"}}))
        _c(out, f"c{ch}up_from_down", U, eng=eng())
        _c(out, f"c{ch}up_from_up", U, state={"chuck": {s: "up"}}, eng=eng())
        _c(out, f"c{ch}up_already_mid", U, state={"chuck": {s: "mid"}}, eng=eng())
        _c(out, f"c{ch}up_unknown", U, state={"chuck": {s: "between"}}, eng=eng())
        _c(out, f"c{ch}up_unknown_resolves", U, state={"chuck": {s: "between"}}, eng=eng(),
           events=[[5.5, {"chuck": {s: "down"}}]])
        _c(out, f"c{ch}up_timeout", U, {"wait_s": 3}, eng=eng())
        _c(out, f"c{ch}up_no_response", U, state={"mute": [f"Z{ch}_MID_LOCATION"]}, eng=eng())
        _c(out, f"c{ch}down_from_mid", D, state={"chuck": {s: "mid"}}, eng=eng())
        _c(out, f"c{ch}down_already", D, eng=eng())
        _c(out, f"c{ch}down_unknown", D, state={"chuck": {s: "between"}}, eng=eng())
        _c(out, f"c{ch}down_busy_stage", D, eng=eng(rt={ch: {"stage": "CLEANUP"}}))
        _c(out, f"c{ch}down_lamp_no_response", D, state={"chuck": {s: "up"}}, eng=eng(),
           events=[[1.0, {"mute": [f"Z{ch}_DOWN_LOCATION"]}]])
        _c(out, f"c{ch}down_bad_arg", D, {"wait_s": None}, eng=eng())
        _c(out, f"c{ch}down_power_write_fail", D, state={"chuck": {s: "mid"}, "mute_write": [f"Z_M_P_{ch}_SW"]},
           eng=eng())
    _c(out, "chuck_two_on_same_ch", "CH1_CHUCK_UP", eng=eng(),
       jobs=[[0.0, "CH1_CHUCK_UP", {}], [1.0, "CH1_CHUCK_DOWN", {}]])


def cases() -> List[dict]:
    out: List[dict] = []
    for f in (_status, _recipe, _start, _pc, _vacuum, _pins, _gates, _chuck):
        f(out)
    names = [c["name"] for c in out]
    assert len(names) == len(set(names)), "사례 이름 중복"
    return out
