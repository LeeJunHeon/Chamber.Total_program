"""v1 전수 목록 — 기준 커밋 66efc7b 이후 main 에 들어온 커밋(2026-11-09 반영, V1_BASE = 2f968be).

a6e8d46 터보 꺼짐 지속 알림 / 984556e CH1 TSP 실행 로그의 사용 중 필라멘트·에러 코드 / 2f968be 두께 Time 모드 Factor·영점·기록.
작업 중 main 에 커밋이 더 생기면 마지막에 다시 확인해 여기에 더한다.
"""
from __future__ import annotations

ITEMS = [
    dict(id="SYNC-001", area="sync",
         desc="터보 꺼짐 지속 판정(TurboOffWatch): CH1/CH2 켜짐 = TURBO_n_SW and TURBO_n_인터락, 읽기 성공 행에서만, 꺼짐이 TURBO_OFF_ALERT_S(3600 s) 이상 "
              "계속이면 off_long 한 번, 그 뒤 켜지면 back_on 한 번(켜짐 전 짧은 꺼짐은 조용), 0 이하면 끔",
         v1="lib/turbo_watch.py:70-108; device/plc.py:1919-1938", v2="v2/ch12/turbo_watch.py; v2/ch12/outputs/row_feed.py",
         test="v2/tests/test_turbo_watch.py::test_off_long_once_then_back_on", status="done", note=""),
    dict(id="SYNC-002", area="sync",
         desc="터보 알림 카드: '터보 꺼짐 지속'(FAIL, '[CHn] 터보 펌프 꺼짐 X 경과 | 꺼진 시각 HH:MM:SS (PLC 기동 출력 기준)')·'터보 다시 켜짐'(SUCCESS, "
              "'꺼져 있던 시간 X'), PLC 끊김 전용 방(urgent), PLC 로그 한 줄 'TURBO WATCH: 제목 — 부제'. v2: 이벤트 plc·turbo_off_long|turbo_back_on(data.card) → 포털이 PLC 웹훅으로",
         v1="lib/turbo_watch.py:54-67; main.py:331-350", v2="v2/ch12/system.py; v2/ch12/turbo_watch.py; vanam-process lib/ops2/ch12/chatCards.ts",
         test="v2/tests/test_turbo_watch.py::test_row_feed_emits_from_one_second_rows", status="done", note=""),
    dict(id="SYNC-003", area="sync",
         desc="설정 TURBO_OFF_ALERT_S(3600.0, 0 이하 = 끔)", v1="config/settings.json:272; lib/config_common.py:671",
         v2="v2/ch12/params/outputs.py", test="v2/tests/test_inv_cfg.py::test_key[common.TURBO_OFF_ALERT_S]", status="done", note=""),
    dict(id="SYNC-010", area="sync",
         desc="TSP 671(Active filament)·206(Error code) 읽기 전용 창 + 표시 문구(filament_label·error_label·ERROR_LABELS 0~5, 엄격한 숫자 해석)",
         v1="device/tsp.py:32-127; device/tsp.py:426-433", v2="v2/ch12/drivers/tsp.py",
         test="v2/tests/test_tsp_filament_log.py::test_labels_and_strict_decode", status="done", note=""),
    dict(id="SYNC-011", area="sync",
         desc="TSP 실행 로그 [FILAMENT]: 시작 1번('시작 시 사용 중 필라멘트: N번'·읽기 실패 사유, 시작을 막지 않음), 종료 1번(정상·중단·오류 모두, 시작을 읽은 런만 — "
              "'종료 시 …(에러 코드 N = 라벨)'·변경 없음/변경 줄·에러 4 이면 카트리지 교체 경고), 3 s 한도·늦은 응답은 연결을 끊음",
         v1="runtime/tsp_runtime.py:506-575; runtime/tsp_runtime.py:645-651; runtime/tsp_runtime.py:745-757",
         v2="v2/ch12/tsp_engine.py; v2/ch12/drivers/tsp.py",
         test="v2/tests/test_tsp_filament_log.py::test_filament_changed_during_run_and_exhausted; v2/tests/test_tsp_filament_log.py::test_normal_end_same_filament; "
              "v2/tests/test_tsp_filament_log.py::test_user_stop_still_logs_end; v2/tests/test_tsp_filament_log.py::test_read_failure_does_not_block_start",
         status="done", note="종료 로그 유실 수정(_drain_log_queue)은 v2 가 기록을 큐로 먼저 넣고 닫아 해당 없음"),
    dict(id="SYNC-020", area="sync",
         desc="Time 모드(두께 모니터 채널) 시작 전 Factor 적용 시도(_thickness_time_prepare): 건 1개·통신·크리스탈 점검 → 재료값 쓰기, 성공이면 thickness_time_record, "
              "실패·co-sputter·건 없음은 시작을 막지 않고 THICKNESS 로그·경고·Fallback Reason(thickness_record_note)",
         v1="runtime/chamber_runtime.py:4048-4090; runtime/chamber_runtime.py:6631-6700", v2="v2/ch12/thickness.py; v2/ch12/process_preflight.py",
         test="v2/tests/test_thickness_time_record.py::test_prepare_ok_sets_flag_and_pre; v2/tests/test_thickness_time_record.py::test_prepare_failure_does_not_block; "
              "v2/tests/test_thickness_time_record.py::test_prepare_cosputter_or_no_gun_sends_nothing", status="done", note=""),
    dict(id="SYNC-021", area="sync",
         desc="성공 런만 Shutter Delay 뒤·Main Shutter 앞(없으면 메인·종료 절차 앞)에 THICKNESS_ZERO, THICKNESS_WAIT 없음(레시피 시간으로 끝), ETA 합이 영점 확인 최대 초를 더함",
         v1="controller/process_controller.py:1717-1730; controller/process_controller.py:2650-2665", v2="v2/ch12/blocks_expand.py; v2/ch12/process_sequence.py; v2/ch12/eta.py; v2/ch12/process_engine.py",
         test="v2/tests/test_process_steps_golden.py::test_combo; v2/tests/test_thickness_time_record.py::test_zero_step_between_shutter_delay_and_main_shutter_and_eta", status="done", note=""),
    dict(id="SYNC-022", area="sync",
         desc="Time 모드 영점 결과(기준점)를 기록에만 쓰고(zero_base), 최종 두께 = 마지막 − 기준점(없으면 마지막 − 처음), Factor 열·Fallback Reason·영점 확인 화면 문구('Time')",
         v1="runtime/chamber_runtime.py:1566-1600; runtime/chamber_runtime.py:6796-6960", v2="v2/ch12/thickness.py; v2/ch12/thickness_record.py",
         test="v2/tests/test_thickness_time_record.py::test_time_zero_success_final_is_last_minus_base; v2/tests/test_thickness_time_record.py::test_failed_prepare_records_difference_and_reason",
         status="done", note=""),
    dict(id="SYNC-100", area="sync",
         desc="sim 격리(2026-11-09 B): sim 에서는 모든 출력 경로를 C:\\CH12_v2\\sim_out 아래로 강제(설정값과 무관) — LOG_ROOT_DIR·HOST_LOG_*(open_sputter.json·pending 포함)·Robot CSV·OES_LOCAL_DIR·"
              "OES 정지 플래그·CHn_RGA_CSV_PATH·SPUTTER_CALIB_DB_DIR·GDRIVE_LOG_DIR·CAMERA_LOG_ROOT·OUTPUT_LOCAL_FIRST_DIR·PLC_COIL_LOG_NAS_DIR·두께·히터·selfwatch·SYSTEM·ERROR·RUNTIME_DUMP·Logs_LocalFallback 등. NAS·UNC 는 쓰지 않음",
         v1="(v2 안전 규칙 — v1 과 같은 PC 에서 함께 켜는 경우)", v2="v2/ch12/sim_guard.py; v2/ch12/app.py; v2/ch12/outputs/paths.py",
         test="v2/tests/test_sim_isolation.py::test_enforce_forces_every_path_key_under_sim_out_whatever_the_setting; v2/tests/test_sim_isolation.py::test_forced_keys_cover_every_path_param",
         status="done", note=""),
    dict(id="SYNC-101", area="sync",
         desc="sim 의 OES 가짜 데몬: 뮤텍스 이름 접두어(Local\\CH12SIM_VanaM_OES_USB{n}·REINIT — v1 은 Local\\VanaM_OES_USB{n})·정지 플래그 폴더(OES_STOP_DIR)·OES_LOCAL_BASE 를 환경변수로 sim_out 아래로(PC 에 v1 환경변수가 있어도 이김), RGA 가짜 워커 출력도 sim_out",
         v1="apps/oes_service/oes_api.py:375-419", v2="v2/ch12/sim_guard.py; v2/ch12/workers/oes_common.py",
         test="v2/tests/test_sim_isolation.py::test_fake_workers_write_only_under_sim_out_and_use_other_mutex; v2/tests/test_sim_isolation.py::test_apply_env_overrides_v1_env_vars", status="done", note=""),
    dict(id="SYNC-102", area="sync",
         desc="시작 자기 검사: sim 인데 쓰기 대상이 C:\\CH12_v2 밖·UNC·설정 폴더 밖·HOST_SERVER_HOST 0.0.0.0·포트 50070 이면 시작 거부(사유 출력, 종료 코드 2), --sim 인데 RUN_MODE 가 sim 이 아니면 거부, 로봇 서버 --sim 도 같은 검사",
         v1="(v2)", v2="v2/ch12/sim_guard.py; v2/ch12/app.py; v2/robot_server/app.py",
         test="v2/tests/test_sim_isolation.py::test_check_finds_problems; v2/tests/test_sim_isolation.py::test_program_refuses_when_config_dir_outside_sim_base; "
              "v2/tests/test_sim_isolation.py::test_program_refuses_sim_flag_with_prod_config; v2/tests/test_sim_isolation.py::test_robot_server_sim_refuses_v1_port_and_wildcard", status="done", note=""),
    dict(id="SYNC-103", area="sync",
         desc="포트는 127.0.0.1 만, 50070 은 sim 에서 열지 않음 — 시험용 로봇 서버 포트 HOST_SERVER_SIM_PORT(50170)·run_ch12_sim.cmd·run_ch12_sim_robot_server.cmd(--port 옵션)",
         v1="host/server.py", v2="v2/robot_server/app.py; v2/run_ch12_sim.cmd; v2/run_ch12_sim_robot_server.cmd; v2/ch12/params/robot.py",
         test="v2/tests/test_sim_isolation.py::test_robot_server_sim_listens_only_on_loopback_test_port_and_writes_under_sim_out", status="done", note=""),
    dict(id="SYNC-104", area="sync",
         desc="v1 open_sputter.json·Robot CSV(NAS)·기록 폴더를 둔 채 sim 을 켜도 그 파일·폴더가 바뀌지 않고, sim 실행 중 열린 쓰기 경로 전부가 기준 폴더 아래(감사 훅으로 확인)",
         v1="(v2)", v2="v2/ch12/sim_guard.py", test="v2/tests/test_sim_isolation.py::test_sim_with_v1_files_present_touches_nothing_outside_and_closes_ports", status="done", note=""),
    dict(id="SYNC-200", area="sync",
         desc="정상 끝 정지(end_stop 블록) 도중 재시작: 설정값을 다시 쓰지 않고 끄는 순서를 처음부터 끝까지(끄기만 하므로 안전), 결과는 실패가 아니라 '정상 종료(재시작 후 정지 이어감)'(run_end done·이벤트 RESUMED_STOP), "
              "STOP 정지 순서 도중은 사용자 중단 그대로, 이상 중단 정지는 PROGRAM_RESTART. BLOCK_WRITES 에서 end_stop 항목 정리",
         v1="(v2 — 지시 3장 (6))", v2="v2/ch12/process_view.py; v2/ch12/resume.py",
         test="v2/tests/test_resume.py::test_every_block_kind_random_time; v2/tests/test_resume.py::test_end_stop_crash_at_every_step_never_turns_anything_back_on", status="done", note=""),
    dict(id="SYNC-201", area="sync",
         desc="공정 중 값 수정 상한: 파워(dc·dc2·rf·dcp·rfp)는 장비 최대(DC_MAX_POWER·RF_MAX_POWER·DCP_MAX_POWER_W)를 넘으면 거부(D11), 히터 목표는 상한, inf·NaN·음수 유량 거부, "
              "지금 블록에 바로 쓸 때도 driver.retarget(start() 와 같은 제한·카운터·과전류 한계 — 제한된 목표 기준)",
         v1="(v2)", v2="v2/ch12/process_adjust.py; v2/ch12/drivers/dc.py; v2/ch12/drivers/rf.py",
         test="v2/tests/test_process_safety_1109.py::test_adjust_power_above_equipment_max_is_rejected_not_clamped; v2/tests/test_process_safety_1109.py::test_adjust_dc_live_goes_through_start_path_limit_and_overcurrent_basis; "
              "v2/tests/test_process_safety_1109.py::test_adjust_pulse_and_rf_and_heater_limits", status="done", note=""),
    dict(id="SYNC-202", area="sync",
         desc="파워 램프 블록 일시정지: 그 순간 측정 출력으로 목표 고정(DC·RF·DC Pulse·RF Pulse 드라이버 hold), 재개하면 원래 목표로 램프 계속, 고정 중에도 드라이버 감시·안전 규칙은 계속",
         v1="(v2 — 지시 3장 (3))", v2="v2/ch12/process_adjust.py; v2/ch12/process_actions.py; v2/ch12/drivers/dc.py; v2/ch12/drivers/rf.py; v2/ch12/drivers/dcpulse.py; v2/ch12/drivers/rfpulse.py",
         test="v2/tests/test_process_safety_1109.py::test_pause_power_on_holds_target_at_measured_and_resume_restores", status="done", note=""),
    dict(id="SYNC-203", area="sync",
         desc="시퀀스 편집과 정지 경쟁: 잠금 안에서 shutdown·stop_requested·failed·_stopping 을 다시 확인하고 정지가 시작됐으면 편집 거부",
         v1="(v2)", v2="v2/ch12/process_adjust.py; v2/ch12/process_engine.py",
         test="v2/tests/test_process_safety_1109.py::test_sequence_edit_rejected_once_stop_started_even_before_runner_flags", status="done", note=""),
    dict(id="SYNC-204", area="sync",
         desc="공정이 없을 때 남은 파워 정리를 하나로 — 안전 규칙 S31 이 있으면 이어가기 모듈은 하지 않는다(두 번 도는 것 정리)",
         v1="(v2)", v2="v2/ch12/resume.py; v2/ch12/safety.py", test="v2/tests/test_resume.py::test_residual_dc_power_without_process_is_turned_off", status="done", note=""),
]
