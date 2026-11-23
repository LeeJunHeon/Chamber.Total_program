"""v1 전수 목록 — 기준 커밋 66efc7b 이후 main 에 들어온 커밋(2026-11-09 반영, V1_BASE = 2f968be).

a6e8d46 터보 꺼짐 지속 알림 / 984556e CH1 TSP 실행 로그의 사용 중 필라멘트·에러 코드 / 2f968be 두께 Time 모드 Factor·영점·기록.
작업 중 main 에 커밋이 더 생기면 마지막에 다시 확인해 여기에 더한다.
그 뒤 반영: 3112ea5·3992a58·9cab3bf·f02f20a(11-10), 2fc00c4·8603d5b(11-13), acf3d96·27decba(11-19 — 가스 도달 판정 3회 연속·O2 단계 설정, V1_BASE 27decba).
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
    dict(id="SYNC-300", area="sync",
         desc="로봇 서버: 응답 시간 한도로 E205 를 내지 않고 v1 처럼 실제 결과를 끝까지 기다린다. E205 는 내부 API 에 연결 자체가 안 될 때(연결 거부·접속 시간 초과)와 진행 중 연결이 끊겼을 때만",
         v1="host/server.py:262-300", v2="v2/robot_server/forward.py; v2/robot_server/server.py",
         test="v2/tests/test_robot_server.py::test_slow_control_program_is_waited_for_not_e205; v2/tests/test_robot_server.py::test_connection_dropped_mid_request_is_e205_and_loading_sensor_never_cached; "
              "v2/tests/test_robot_server.py::test_two_connections_slow_command_and_status_run_in_parallel_without_e205", status="done", note=""),
    dict(id="SYNC-301", area="sync",
         desc="GET_LOADING_n_SENSOR 는 캐시 값을 절대 보내지 않는다(실제 읽기·v1 오류·연결 불가면 E205, 디스크 캐시에 저장·읽기 없음). GET_SPUTTER_STATUS 는 연결 불가일 때만 마지막 실제 상태를 v1 형식 그대로(running 으로 바꾸지 않음), 나이는 로그에만",
         v1="host/handlers.py:543-665", v2="v2/robot_server/cache.py; v2/robot_server/server.py",
         test="v2/tests/test_robot_server.py::test_down_keeps_connection_and_uses_cache; v2/tests/test_robot_server.py::test_old_disk_cache_sensor_values_are_ignored; v2/tests/test_robot_server.py::test_disk_cache_survives_restart", status="done", note=""),
    dict(id="SYNC-302", area="sync",
         desc="request_id 는 v1(host/server.py:270)처럼 str() 로 받는다(내부 처리 경로 dispatch_robot 포함)",
         v1="host/server.py:270", v2="v2/ch12/robot_api.py", test="v2/tests/test_robot_ops.py::test_request_id_is_str_like_v1", status="done", note=""),
    dict(id="SYNC-303", area="sync",
         desc="VACUUM_ON/OFF 로드락 전환 잠금 확인·획득을 원자적으로(비차단 획득 실패 = v1 busy 오류 E321), PC 시작 중복(E206) 확인·표시도 같게",
         v1="host/handlers.py:1205-1230; plasma_cleaning_runtime.py:3207-3214", v2="v2/ch12/robot_base.py; v2/ch12/robot_vacuum.py; v2/ch12/robot_status.py",
         test="v2/tests/test_robot_ops.py::test_try_lock_is_atomic_two_requests_only_one_passes_other_is_busy_e321; v2/tests/test_robot_ops.py::test_pc_start_duplicate_e206_is_atomic", status="done", note=""),
    dict(id="SYNC-304", area="sync",
         desc="E208(CSV 공정 데이터 없음) 거절 줄의 레시피 이름·행 수 칸을 v1 과 같게(거절 전에 호스트 로그 갱신 — 이름·행 수 0·공정 이름 빈 칸). PLC 무응답 detail 을 v1 pymodbus 3.11.2 문구와 같게(나머지는 differences X05)",
         v1="runtime/chamber_runtime.py:8085-8101; device/plc.py:1215-1250", v2="v2/ch12/assembly.py; v2/ch12/robot_base.py",
         test="v2/tests/test_robot_ops.py::test_e208_refusal_updates_host_log_recipe_name_and_zero_rows_like_v1; v2/tests/test_robot_ops.py::test_plc_timeout_detail_is_v1_pymodbus_3_11_text", status="done", note=""),
    dict(id="SYNC-400", area="sync",
         desc="GPC3000 밸브 위치 R6 를 매 tick 읽어 1초 기록·화면·웹에(CH1·CH2, 단위 %, 매뉴얼 p.73 Valve Position Value), R69(ON/OFF)·R1~R4(SP 값)·R7(선택 SP)도 매 tick 읽어 기억값 대신 실제값으로, 1초 예산 9개 질의·50 ms 간격",
         v1="device/mfc.py:1694-1847; lib/config_common.py:309-317", v2="v2/ch12/drivers/mfc.py; v2/ch12/spec/devices.py",
         test="v2/tests/test_mfc.py::test_valve_position_onoff_sp_values_and_active_sp_are_read_every_tick_as_actual_values; v2/tests/test_mfc.py::test_parse_valve_and_active_sp; v2/tests/test_mfc.py::test_params_and_budget", status="done", note=""),
    dict(id="SYNC-401", area="sync",
         desc="RF Pulse SET_SETPOINT(8)·SET_PULSING(27) 쓰기 뒤 되읽기(164·177 — CESAR 1310 매뉴얼 Table 4-16, 154 와 같은 표) → 다시 쓰기 → VERIFY_MISMATCH(시작 시퀀스·공정 중 셋포인트 변경), SET_MODE(3)는 번호 설정 RFPULSE_Q_MODE(155 는 제어 모드라 불가 — 기본 154, SYNC-501)",
         v1="device/rf_pulse.py", v2="v2/ch12/drivers/rfpulse_verify.py; v2/ch12/drivers/rfpulse.py; v2/ch12/params/rfpulse.py",
         test="v2/tests/test_rfpulse.py::test_setpoint_and_pulsing_are_read_back_after_write_164_177; v2/tests/test_rfpulse.py::test_setpoint_readback_mismatch_rewrites_then_verify_mismatch_and_no_rf_on; "
              "v2/tests/test_rfpulse.py::test_run_setpoint_change_is_verified_too_and_readback_can_be_turned_off; v2/tests/test_rfpulse.py::test_mode_readback_154_by_default_and_off_with_zero_155_is_not_regulation_mode", status="done", note="2026-11-16 D413: 164·177 번호를 CESAR 1310 매뉴얼 Table 4-16 으로 확인(매뉴얼 쪽수 미확인 문구 정리, 장비 사실 표 UNVERIFIED 에서 뺌 — 코드 동작 변경 없음)"),
    dict(id="SYNC-402", area="sync",
         desc="DC Pulse REF_LEVEL(0x83)·REGULATION(0x81)·PLC 스위치 코일의 되읽기 없음(READBACK_NONE)과 그 근거를 장비 사실 표에 분명히",
         v1="device/dc_pulse.py; device/plc.py", v2="v2/ch12/spec/devices.py", test="v2/tests/test_devices.py::test_readback_table_matches_params_and_lists_commands_without_readback", status="done", note=""),
    dict(id="SYNC-030", area="sync",
         desc="CH2 히터 CSV(main 9cde7c4): 하루 1개 파일 CH2_HEATER_YYYYMMDD.csv 에 프로그램이 도는 동안 항상 기록(운전 중 HEATER_LOG_PERIOD_S 1초·정지 중 HEATER_POLL_IDLE_S 3초, 폴링 간격 흔들림 보정 slack), "
              "운전 시작·종료 행(note, 종료 행은 정리 전 owner·경과·유지/클램프), 프로그램 종료 행, PLC 미연결 행(timestamp·note 만, 값 빈칸 — 마지막 RUN 기준 주기), 날짜는 행 시각 기준(자정 새 파일), 같은 날 이어 쓰기(헤더 한 번), 운전별 파일 없음, 기본 주기 3.0 → 1.0",
         v1="lib/heater_logger.py; controller/heater_service.py:757-880; config/settings.json:587-623",
         v2="v2/ch12/outputs/heater_csv.py; v2/ch12/heater_engine.py; v2/ch12/heater_edges.py; v2/ch12/heater_commands.py; v2/ch12/params/outputs.py",
         test="v2/tests/test_heater_engine.py::test_csv_daily_idle_3s_run_1s_blank_rows_when_disconnected_and_edges; v2/tests/test_outputs_csv.py::test_heater_golden; "
              "v2/tests/test_outputs_csv.py::test_heater_daily_file_header_once_midnight_split_and_restart_append; v2/tests/test_heater_engine.py::test_run_edges_csv_and_chat", status="done", note=""),
    dict(id="SYNC-031", area="sync",
         desc="GPC3000 압력·밸브 위치 상시 CSV(main 3112ea5·3992a58): {CH1|CH2}_YYYYMMDD.csv(MFC_STATE_LOG_DIR), 헤더 Timestamp,Pressure_mTorr,Valve_pct, Timestamp 밀리초, 빈 칸·둘 다 없으면 행 없음, 설정 MFC_STATE_LOG_ENABLED·INTERVAL_S·DIR(GAP_MS·TIMEOUT_MS 는 v2 가 따로 읽지 않아 버림). v2 는 1초 행의 mfc 값에서 뽑는다",
         v1="controller/mfc_state_logger.py; device/mfc.py; config/settings.json", v2="v2/ch12/outputs/mfc_state_csv.py; v2/ch12/outputs/row_feed.py; v2/ch12/params/outputs.py",
         test="v2/tests/test_mfc_state_csv.py::test_rows_ms_timestamp_blank_values_and_skip; v2/tests/test_mfc_state_csv.py::test_disabled_writes_nothing_and_sim_guard_forces_dir", status="done", note=""),
    dict(id="SYNC-032", area="sync",
         desc="히터 CSV '프로그램 종료' 행(f02f20a): 종료 순간 PLC 를 못 읽고 있었으면 값 열 빈칸, 읽고 있었으면 owner·경과·유지/클램프 포함",
         v1="controller/heater_service.py", v2="v2/ch12/heater_commands.py", test="v2/tests/test_heater_engine.py::test_csv_daily_idle_3s_run_1s_blank_rows_when_disconnected_and_edges", status="done", note=""),
    dict(id="SYNC-500", area="sync",
         desc="MFC 나눠 읽기(D407): MFC_READ_MODE split/all·MFC_TICK_EVERY·MFC_TICK_ROTATE(허용 목록·R60/R5 필수), 돌아가며 읽는 값 유효 (개수+1) s·읽은 뒤 나이 칸·기억값 없음, 쓰기 되읽기는 즉시",
         v1="(v2 신설 — v1 은 R60·R5 만 폴링)", v2="v2/ch12/drivers/mfc.py; v2/ch12/params/mfc.py",
         test="v2/tests/test_mfc.py::test_split_default_reads_three_every_tick_and_rotates_the_other_six_one_per_tick; "
              "v2/tests/test_mfc.py::test_rotated_value_expires_after_rotate_count_plus_one_seconds_and_is_never_filled_with_remembered_value; "
              "v2/tests/test_mfc.py::test_write_readback_reads_the_register_immediately_regardless_of_rotation; v2/tests/test_mfc.py::test_tick_lists_reject_out_of_allowlist_and_require_r60_r5; "
              "v2/tests/test_mfc.py::test_all_mode_reads_nine_every_tick", status="done", note=""),
    dict(id="SYNC-501", area="sync",
         desc="RF Pulse 조절 모드 되읽기 RFPULSE_Q_MODE 기본 154(CESAR 1310 Table 4-16): SET_MODE(3) → 154 되읽기 → 다시 쓰기 → VERIFY_MISMATCH, 통신 실패는 검증 생략, 0 이면 끔",
         v1="device/rf_pulse.py(v1 은 되읽기 없음)", v2="v2/ch12/drivers/rfpulse.py; v2/ch12/drivers/rfpulse_verify.py; v2/ch12/params/rfpulse.py",
         test="v2/tests/test_rfpulse.py::test_mode_readback_154_by_default_and_off_with_zero_155_is_not_regulation_mode; v2/tests/test_rfpulse.py::test_mode_readback_mismatch_retries_then_verify_mismatch; "
              "v2/tests/test_rfpulse.py::test_mode_readback_comm_failure_skips_verification", status="done", note=""),
    dict(id="SYNC-502", area="sync",
         desc="멈춤 감지 재시작(D408): 건강 확인 파일(tick·cmd·준비·설정)·faulthandler 스레드 덤프·감시 프로그램 강제 종료·재시작 한도 포함·시작 준비 시간·끄기",
         v1="apps/recovery_service(메인이 쓰지 않아 동작 안 함)", v2="v2/supervisor/supervisor.py; v2/ch12/health_beacon.py; v2/ch12/app.py",
         test="v2/tests/test_supervisor_hang.py::test_hung_child_is_dumped_killed_and_restarted; v2/tests/test_supervisor_hang.py::test_healthy_child_is_not_restarted; "
              "v2/tests/test_supervisor_hang.py::test_command_core_stall_alone_is_a_hang_even_when_ticks_move; v2/tests/test_supervisor_hang.py::test_hang_check_off_by_option_and_by_setting_in_the_file; "
              "v2/tests/test_supervisor_hang.py::test_real_app_writes_heartbeat_under_supervision_env", status="done", note=""),
    dict(id="SYNC-503", area="sync",
         desc="장비 응답 시간 1분 집계 CSV(D409): 모든 직렬 장비·PLC 의 명령별 응답 시간, 개수·최소·평균·최대·p95·시간 초과, data/device_timing(sim_out), 통신 경로 무영향, 웹 상태 deviceTiming",
         v1="(v2 신설)", v2="v2/common/timing_stats.py; v2/ch12/outputs/device_timing_csv.py; v2/ch12/outputs/row_feed.py; v2/ch12/hub.py",
         test="v2/tests/test_device_timing.py::test_summarize_count_min_avg_max_p95_and_timeouts; v2/tests/test_device_timing.py::test_csv_every_interval_header_once_and_retention; "
              "v2/tests/test_device_timing.py::test_full_rig_records_every_device_into_sim_data_dir; v2/tests/test_device_timing.py::test_mfc_driver_records_latency_and_timeout", status="done", note=""),
    dict(id="SYNC-504", area="sync",
         desc="간격·읽기 설정이 재시작 없이(공정 중이 아니면 바로) 적용(MFC_GAP_MS 0·20·50 비교)과 MFC·IG 1초 읽기 상수 설정화(D410)",
         v1="(v2 신설)", v2="v2/ch12/drivers/mfc.py; v2/ch12/drivers/ig.py; v2/ch12/params/mfc.py; v2/ch12/params/ig.py",
         test="v2/tests/test_mfc.py::test_mfc_gap_ms_0_20_50_changes_real_spacing_and_applies_live; v2/tests/test_config_apply.py::test_device_gap_and_mfc_read_settings_apply_without_restart_when_idle", status="done", note=""),
    dict(id="SYNC-505", area="sync",
         desc="통신 속도 사실 표 BAUD(D411): 장비별 범위·매뉴얼 근거·바꾸는 곳, 공유 포트 메모", v1="(문서)", v2="v2/ch12/spec/devices.py",
         test="v2/tests/test_devices.py::test_baud_facts_cover_every_serial_device_with_basis", status="done", note=""),
    dict(id="SYNC-510", area="sync",
         desc="DC Power 저전류 감시 기준(main 2fc00c4): 설정 파워 ≤ DC_LOW_CURRENT_LOWP_MAX_W(50 W)이면 DC_LOW_CURRENT_LOWP_THRESH_A(0.01 A), 판정마다 적용, 런 시작 기준 1줄, DC_OVERCURRENT_I1_A 1.5",
         v1="device/dc_power.py; lib/power_limits.py; lib/config_common.py:215-216; config/settings.json", v2="v2/ch12/drivers/dc.py; v2/ch12/params/dc.py",
         test="v2/tests/test_dc.py::test_low_current_threshold_pure_function_cases; v2/tests/test_dc.py::test_low_power_target_does_not_trip_at_0_023a_but_60w_target_trips_at_0_040a; "
              "v2/tests/test_dc.py::test_low_current_lowp_can_be_turned_off_and_uses_live_cfg; v2/tests/test_dc.py::test_overcurrent_limit_and_dac", status="done", note=""),
    dict(id="SYNC-511", area="sync",
         desc="OES 워커(main 8603d5b): 읽기 버퍼 후보 최댓값 이상, 첫 프레임·reset 타임아웃 → finished(ok=False) 후 os._exit(125), 설정 rc·측정 진단 기록",
         v1="apps/oes_service/oes_api.py", v2="v2/ch12/workers/oes_device.py; v2/ch12/workers/oes_guard.py; v2/ch12/workers/oes_measure.py; v2/ch12/workers/oes_daemon.py",
         test="v2/tests/test_oes_link.py::test_read_buffer_is_always_at_least_the_largest_candidate_but_returns_first_npix; "
              "v2/tests/test_oes_worker_hang_exit.py::test_measure_first_frame_timeout_prints_finished_and_exits_125_without_waiting_for_cleanup", status="done", note=""),
    dict(id="SYNC-520", area="sync",
         desc="멈춤 감지 설정 범위(D413 — D408 보완): SUPERVISOR_HANG_TIMEOUT_S 60~3600 s(코어 잠금 최대 30 s — 화면 레시피·PC CSV 읽기 — 의 2배), "
              "SUPERVISOR_HEALTH_INTERVAL_S 1~30 s(한도 하한의 절반), 키 사이 규칙 SUP01(한도 ≥ 2 × PLC 명령 실행 상한 — 히터 패널 대기), "
              "범위 밖 저장값은 고쳐 쓰지 않고 거부(D11 — 직전 버전, 둘 다 밖이면 시작 거부), 감시 프로그램은 범위를 따로 검사하지 않음",
         v1="(v2 신설)", v2="v2/ch12/params/supervision.py; v2/ch12/core.py; v2/supervisor/supervisor.py",
         test="v2/tests/test_config_apply.py::test_supervisor_ranges_d413_boundaries_and_sup01; "
              "v2/tests/test_config_apply.py::test_saved_values_out_of_new_supervisor_range_are_rejected_not_clamped_d11; "
              "v2/tests/test_supervisor_hang.py::test_longest_core_lock_hold_is_not_a_hang_at_the_range_limits_d413", status="done", note=""),
    dict(id="SYNC-521", area="sync",
         desc="코어 잠금 안 NAS 파일 접근 시간 제한(D413): PC CSV 읽기(PC_RECIPE_LOAD·PC_START path) 30 s('CSV 로드 30초 timeout (NAS 응답 지연): 경로' → E227 — v1 executor 30 s, v2 에서 빠져 있던 것), "
              "프리스퍼터 레시피 지정의 파일 확인 3 s('(파일 없음)' 뒤 늦은 답 반영)",
         v1="runtime/plasma_cleaning_runtime.py:3141-3150; main.py:1639-1654", v2="v2/ch12/core.py; v2/ch12/pre_sputter.py",
         test="v2/tests/test_core.py::test_pc_csv_path_is_read_and_nas_stall_is_limited_to_30s_d413; "
              "v2/tests/test_pre_sputter.py::test_recipe_path_exists_check_waits_at_most_3s_and_late_answer_is_applied_d413", status="done", note=""),
    dict(id="SYNC-530", area="sync",
         desc="웹 로그 평소 보기(D414): 공정 진행 줄(process block·skip·pause·resume·edit·adjust·recipe_loaded·popup·chat·state·result)이 묶음 eventKinds main, "
              "block 사건 data total·elapsed_s, snapshot fmt format spec 통일, 화면 연 직후 최근 줄(연결 때 첫 묶음)",
         v1="(v2 신설 — v1 로그 창은 모든 줄)", v2="v2/ch12/web_schema.py; v2/ch12/process_view.py",
         test="v2/tests/test_web_log_main.py::test_process_progress_kinds_are_main_and_chat_cards_are_detail; "
              "v2/tests/test_web_log_main.py::test_block_events_carry_total_and_elapsed_and_are_replayed_to_a_new_viewer; "
              "vanam-process/scripts/ops2-ch12-ui-check.mjs::ui:log-main", status="done", note=""),
    dict(id="SYNC-531", area="sync",
         desc="sim 가상 MFC 시작 설정점(D415→D421): SIM_MFC_START_SP_MTORR 기본 0,0,30,30,0 mTorr — sim 압력 대기가 setpoint 읽기 실패로 멈추지 않음, 실장비 경로 영향 없음",
         v1="(v1 sim 없음 — 실장비는 패널 값)", v2="v2/ch12/params/mfc.py; v2/ch12/sim/rig.py",
         test="v2/tests/test_sim_mfc_start_sp.py::test_full_rig_reads_sp3_sp4_from_the_virtual_mfc; v2/tests/test_sim_mfc_start_sp.py::test_only_the_sim_reads_the_setting; "
              "v2/tests/test_sim_mfc_start_sp.py::test_bad_format_is_rejected_by_config_check_not_fixed_up", status="done", note=""),
    dict(id="SYNC-532", area="sync",
         desc="챔버12 웹 화면 챔버K 틀(D416): 상태 띠·탭·운전(CH1·CH2 나란히)·HMI 계통도·기록·입력칸(이름·단위·범위, 범위 밖 막음)·잠금 순서·노트북 PLC 버튼·로봇 연결 실제 상태·개발용 글 숨김",
         v1="(v2 신설)", v2="vanam-process components/ops2/ch12/Ch12Page.tsx",
         test="vanam-process/scripts/ops2-ch12-ui-check.mjs::ui:status-band; vanam-process/scripts/ops2-ch12-ui-check.mjs::ui:tabs-locks; "
              "vanam-process/scripts/ops2-ch12-ui-check.mjs::ui:hmi-map; vanam-process/scripts/ops2-ch12-ui-check.mjs::ui:inputs-range; "
              "vanam-process/scripts/ops2-ch12-ui-check.mjs::ui:no-dev-text; vanam-process/scripts/ops2-ch12-ui-check.mjs::ui:pc-recipe", status="done", note=""),
    dict(id="SYNC-540", area="sync",
         desc="가스 도달 판정 연속 N 회(main 27decba): 목표 ±FLOW_ERROR_TOLERANCE 안 연속 MFC_FLOW_STABLE_COUNT(기본 3) 회 → FLOW_ON 확정, 범위 밖·읽기 실패면 0, "
              "확정 안 된 회차 180 → 시간 초과, 상태 문구 '[연속 n/N]', 1 이상 정수 아님 → 'FLOW_ON 설정 오류'",
         v1="device/mfc.py _flow_stable_need·_stabilization_loop; lib/config_common.py; config/settings.json(ch1·ch2)",
         v2="v2/ch12/drivers/mfc_ops.py; v2/ch12/params/mfc.py",
         test="v2/tests/test_mfc_flow_stable_count.py::test_in_in_out_in_in_in_confirms_on_6th_read; "
              "v2/tests/test_mfc_flow_stable_count.py::test_in_but_short_of_streak_counts_as_attempt; "
              "v2/tests/test_mfc_flow_stable_count.py::test_bad_stable_count_fails_flow_on; "
              "v2/tests/test_mfc_flow_stable_count.py::test_chamber_flow_on_needs_three_and_reflow_on_restarts", status="done", note=""),
    dict(id="SYNC-541", area="sync",
         desc="가스 단계 설정(main 27decba): 목표보다 작은 중간값만 작은 순서대로 FLOW_SET→FLOW_ON('(단계 i/n)'·'(… 도달 대기)'), 중간값 없으면 예전과 같음, "
              "도달 실패면 다음 단계 없이 중단",
         v1="controller/process_controller.py _create_process_sequence; lib/config_ch2.py; config/settings.json(ch2)",
         v2="v2/ch12/process_steps.py; v2/ch12/params/core.py",
         test="v2/tests/test_gas_step_levels.py::test_ch2_o2_steps_follow_table; v2/tests/test_gas_step_levels.py::test_no_mid_levels_identical_to_before; "
              "v2/tests/test_gas_step_levels.py::test_runner_sends_steps_in_order; v2/tests/test_gas_step_levels.py::test_mid_step_failure_aborts_before_next_step; "
              "v2/tests/test_process_steps_golden.py::test_combo", status="done", note=""),
    dict(id="SYNC-542", area="sync",
         desc="가스 단계 설정 검사(main 27decba): 모르는 가스·0 이하·숫자 아님·오름차순 아님·중복·dict 아님 → 고쳐 쓰지 않고 E211 시작 거부(Chuck 이동 전, "
              "로그·챗 장비 오류), v2 는 저장 때도 거부(D11 — C05·C06)",
         v1="controller/process_controller.py _gas_step_levels·gas_step_levels_error; runtime/chamber_runtime.py _gas_step_reject",
         v2="v2/ch12/process_steps.py; v2/ch12/process_preflight.py; v2/ch12/params/core.py",
         test="v2/tests/test_gas_step_levels.py::test_levels_check_matches_v1; v2/tests/test_gas_step_levels.py::test_bad_levels_block_start_before_chuck_e211; "
              "v2/tests/test_gas_step_levels.py::test_bad_levels_rejected_by_param_table_and_ignored_by_expand", status="done", note=""),
    dict(id="SYNC-543", area="sync",
         desc="설정 키 ch1·ch2.MFC_FLOW_STABLE_COUNT·ch2.PROCESS_GAS_STEP_LEVELS(+ 코드 기본값 {}) — v2 CH1_·CH2_ 키, 변환 도구·담당 표",
         v1="config/settings.json; lib/config_common.py; lib/config_ch2.py",
         v2="v2/ch12/params/mfc.py; v2/ch12/params/core.py; v2/ch12/spec/settings_v1facts.py; v2/ch12/params/ownership.py",
         test="v2/tests/test_params.py::test_ownership_covers_all_542_v1_keys; v2/tests/test_params.py::test_my_defaults_equal_v1_effective; "
              "v2/tests/test_mfc_flow_stable_count.py::test_default_stable_count_is_three_and_channel_keys", status="done", note=""),
    dict(id="SYNC-544", area="sync",
         desc="v1 requirements 정리(main acf3d96 — pytesseract·matplotlib·scipy 1.14.1 추가, python-docx 삭제, requirements.dev.txt)",
         v1="requirements.build.txt; requirements.dev.txt", v2="v2/requirements.txt",
         test="", status="na", note="v1 빌드 venv 전용 — v2 코드는 그 패키지를 쓰지 않음(검색 0). RGA 워커 환경 메모만 v2/requirements.txt 주석에(D417 ④)"),
    dict(id="SYNC-550", area="sync",
         desc="화면을 다시 열어도 앞선 공정 진행 줄(D418): 허브 기본 보기 줄 챔버별 보관(HUB_KEEP_MAIN)·연결 때 먼저 다시 보냄, 웹 첫 묶음 챔버별 main·경고·오류, "
              "화면 로그 목록 기본 보기 줄 따로 남김",
         v1="(v2 신설 — v1 은 한 화면 로그 창)", v2="v2/ch12/hub.py; v2/ch12/params/core.py",
         test="v2/tests/test_hub_keep_main.py::test_main_lines_survive_thousands_of_detail_lines_and_come_first_in_order; "
              "v2/tests/test_hub_keep_main.py::test_full_rig_new_viewer_sees_earlier_block_lines_after_a_detail_flood; "
              "v2/tests/test_hub_keep_main.py::test_keep_count_setting_range_rejected_not_fixed; "
              "vanam-process/scripts/ops2-ch12-ui-check.mjs::ui:first-events", status="done", note=""),
    dict(id="SYNC-551", area="sync",
         desc="추세 라벨 겹침 없음(D419 — 공용 Chk2Trend: 간격 = 글자 줄 높이 + 여유, 같은 값 한 줄·띠 늘림)·HMI 사각 버튼 상태 글 켜짐/꺼짐",
         v1="(v2 신설)", v2="v2/hmi",
         test="vanam-process/scripts/ops2-ch12-ui-check.mjs::ui:trend-labels; vanam-process/scripts/ops2-ch12-ui-check.mjs::ui:hmi-buttons",
         status="done", note=""),
    dict(id="SYNC-560", area="sync",
         desc="가스 단계 규칙 C(D420): 공정 중 값 바꾸기도 같은 규칙 — 아직 안 한 가스 주입은 새 목표로 다시 펼침, 흐르는 중 올리면 중간값을 작은 순서대로"
              "(각 도달 판정) 뒤 새 값, 내리면 바로, 도달 실패 → 공정 중단, 단계 진행 중 재요청 거부, 시퀀스 편집·재시작 뒤에도 같은 모양",
         v1="(v2 신설 — v1 은 공정 중 수정 없음, 단계 규칙은 main 27decba)", v2="v2/ch12/process_gas_steps.py; v2/ch12/process_steps.py; v2/ch12/process_adjust.py",
         test="v2/tests/test_gas_step_rule_c.py::test_future_10_to_9p2_restep_9_then_9p2; v2/tests/test_gas_step_rule_c.py::test_future_10_to_12_restep_9_9p5_12; "
              "v2/tests/test_gas_step_rule_c.py::test_future_10_to_8_no_mid_levels; v2/tests/test_gas_step_rule_c.py::test_live_8_to_10_goes_9_9p5_10_each_reached; "
              "v2/tests/test_gas_step_rule_c.py::test_live_9p2_to_10_goes_9p5_then_10; v2/tests/test_gas_step_rule_c.py::test_live_10_to_12_direct; "
              "v2/tests/test_gas_step_rule_c.py::test_live_lowering_10_to_8_is_direct; v2/tests/test_gas_step_rule_c.py::test_live_mid_step_not_reached_aborts_like_gas_injection; "
              "v2/tests/test_gas_step_rule_c.py::test_rerequest_during_stepping_is_rejected_with_reason; v2/tests/test_gas_step_rule_c.py::test_ar_without_levels_is_unchanged_direct; "
              "v2/tests/test_gas_step_rule_c.py::test_edit_sequence_after_future_restep_keeps_shape_and_steps_match; "
              "v2/tests/test_gas_step_rule_c.py::test_journal_restores_restepped_shape_and_stepping_blocks_resume; "
              "v2/tests/test_gas_step_rule_c.py::test_pause_does_not_stop_stepping_and_gas_intro_cannot_be_skipped; "
              "vanam-process/scripts/ops2-ch12-ui-check.mjs::ui:gas-stepping", status="done", note=""),
    dict(id="SYNC-561", area="sync",
         desc="sim 시작 설정점 SP3·SP4 30 mTorr(D421) — 일반 레시피가 압력 제어 30 → SP2 부스트 10 → SP1 → 메인 공정",
         v1="(v1 sim 없음)", v2="v2/ch12/params/mfc.py; v2/ch12/sim/rig.py",
         test="v2/tests/test_sim_mfc_start_sp.py::test_default_is_sp3_sp4_30_mtorr_as_device_values; "
              "v2/tests/test_sim_mfc_start_sp.py::test_default_sp30_is_reached_then_sp2_boost_and_sp1_then_main", status="done", note=""),
    dict(id="SYNC-562", area="sync",
         desc="추세 라벨 위·아래 양쪽 쌍 겹침 없음(D422 — 공용 Chk2Trend 아래→위 맞춤)",
         v1="(v2 신설)", v2="v2/hmi",
         test="vanam-process/scripts/ops2-ch12-ui-check.mjs::ui:trend-labels", status="done", note=""),    dict(id="SYNC-563", area="sync",
         desc="공정 모니터 기준 = 지금 장비에 확정된 목표(D423): 공정 중 값 바꾸기(바로·가스 단계)·뒤 블록 예약·시퀀스 편집·재시작 이어가기 뒤 거짓 편차 카드 없음, "
              "바꾸는 동안 판정 쉼·새 기준에 처음 들어올 때부터 판정·실패면 기준 그대로·열린 사건은 로그 한 줄로 닫음, 새 값 도달 뒤 진짜 편차는 카드",
         v1="(v1 은 공정 중 값 바꾸기 없음 — 기준은 레시피 값, process_monitor.activate)", v2="v2/ch12/process_monitor.py; v2/ch12/monitor_targets.py",
         test="v2/tests/test_monitor_targets.py::test_hold_skips_and_set_target_closes_open_excursion_with_one_log_line; v2/tests/test_monitor_targets.py::test_arm_waits_for_first_in_range_then_judges_or_judges_after_deadline; v2/tests/test_monitor_targets.py::test_release_keeps_baseline_rearm_and_several_holders; v2/tests/test_monitor_targets.py::test_only_active_hold_and_arm_before_shutter_open_is_v1_immediate; v2/tests/test_monitor_targets.py::test_live_lowering_o2_10_to_8_no_false_card_baseline_follows; v2/tests/test_monitor_targets.py::test_live_raising_o2_8_to_10_with_steps_no_false_card; v2/tests/test_monitor_targets.py::test_live_working_pressure_3_to_4_no_false_card; v2/tests/test_monitor_targets.py::test_real_deviation_after_change_one_card_with_new_setpoint; v2/tests/test_monitor_targets.py::test_live_change_failure_keeps_baseline; v2/tests/test_monitor_targets.py::test_mid_step_failure_keeps_baseline_then_aborts; v2/tests/test_monitor_targets.py::test_scheduled_value_for_later_block_is_baseline_there; v2/tests/test_monitor_targets.py::test_sequence_edit_inserted_pressure_block_is_baseline; v2/tests/test_monitor_targets.py::test_resume_keeps_changed_baseline_and_watch", status="done", note=""),
    dict(id="SYNC-564", area="sync",
         desc="sim 압력 모델 보정(D424): 총 유량 5~60 sccm 에서 SP3·SP4 30 mTorr·작업 압력 1~5 mTorr 를 잡음, 압력 대기 수십 초, 30 → SP2 10 → SP1 2 mTorr 레시피 "
              "3 종 메인 공정 압력 ±0.3 안·편차 카드 0, 실장비 경로 영향 없음",
         v1="(v1 sim 없음)", v2="v2/ch12/sim/chamber.py",
         test="v2/tests/test_sim_pressure_model.py::test_constants_cover_5_to_60_sccm_for_30_and_1_to_5_mtorr; v2/tests/test_sim_pressure_model.py::test_every_flow_reaches_every_target_and_wait_is_tens_of_seconds; v2/tests/test_sim_pressure_model.py::test_old_constants_could_not_reach_2_mtorr_at_30_sccm; v2/tests/test_sim_pressure_model.py::test_only_the_sim_reads_the_model_constants; v2/tests/test_sim_pressure_model.py::test_recipe_30_boost_10_sp1_2_main_within_tolerance_no_monitor_card", status="done", note=""),
    dict(id="SYNC-565", area="sync",
         desc="PLC 버튼 — 잠그지 않고 확인창·기록 강화(D425): 공정 중이면 확인창 맨 위 빨간 줄, 위험 두 갈래(사람·장비, RF 전용 릴레이), 누가(웹 이름·"
              "노트북), 버튼마다 사건 하나(결과·그 챔버 공정 상태), 장비 로그 늘·공정 중이면 그 챔버 공정 로그, 화면 로그 줄·탭 분류",
         v1="(v1 은 PLC 버튼 기록 없음 — 화면 버튼은 확인 없이 바로)", v2="v2/ch12/plc_button_log.py; v2/ch12/commands.py; v2/ch12/core.py",
         test="v2/tests/test_plc_button_log.py::test_hazard_classes_by_direction; v2/tests/test_plc_button_log.py::test_proc_chamber_short_name_and_bundle; v2/tests/test_plc_button_log.py::test_who_and_line_text; v2/tests/test_plc_button_log.py::test_web_press_idle_one_event_system_log_no_process_line; v2/tests/test_plc_button_log.py::test_notebook_press_and_tabs_for_shared_and_loadlock; v2/tests/test_plc_button_log.py::test_in_process_press_goes_to_that_chambers_process_log; v2/tests/test_plc_button_log.py::test_rejections_are_one_event_each_with_reason; v2/tests/test_plc_button_log.py::test_write_failure_is_failed_event; v2/tests/test_plc_button_log.py::test_one_event_per_press_and_name_fallback; vanam-process/scripts/ops2-ch12-ui-check.mjs::ui:plc-confirm; vanam-process/scripts/ops2-ch12-ui-check.mjs::ui:plc-confirm-text; vanam-process/scripts/ops2-ch12-ui-check.mjs::ui:plc-log-line; vanam-process/scripts/ops2-ch12-ui-check.mjs::ui:plc-who", status="done", note=""),
    dict(id="SYNC-566", area="sync",
         desc="공정 txt 파일 이름(D426): 공정명에 점이 있어도 시각이 남음(점 하나·여러 개·끝 점·숫자만·한글), 같은 레시피를 두 번 돌리면 파일 둘",
         v1="runtime/chamber_runtime.py:560,7323-7343(v1 은 with_suffix — 점이 든 이름에서 시각이 빠짐, 이번에 고치지 않음)",
         v2="v2/ch12/outputs/process_txt.py",
         test="v2/tests/test_outputs_text.py::test_process_txt_name_keeps_time_with_dots; v2/tests/test_outputs_text.py::test_process_txt_same_recipe_twice_two_files_and_similar_names_differ; v2/tests/test_outputs_text.py::test_process_txt_golden",
         status="done", note=""),
]
