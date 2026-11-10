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
]
