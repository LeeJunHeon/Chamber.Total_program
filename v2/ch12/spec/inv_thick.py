"""v1 전수 목록 — 영역 thick(두께 모니터 Sycon STM-100/MF, QCM).

덮는 범위
  STM-100 응답 해석(결과 코드·리셋 플래그·값 없음·되읽기)과 재료값 쓰기·영점, 상시 읽기 서비스(S·T 0.5초, U·V·M 5초, E?/F?/J? 60초),
  실패·재연결 정책과 상태(stale·link_ok·seq), Thickness 모드 판정(레시피 thickness>0 + SUPPORTS_THICKNESS),
  시작 전 점검(E840~E846), 영점 B·영점 확인(기준점), 종료 판정(유효두께 ≥ 목표 새 값 THICKNESS_DONE_CONSECUTIVE 번 연속 — CH2 = 2,
  15차 커밋 88455cf), 시간 대체(실측/레시피 증착률)·복귀·영구 대체, 최대 시간 상한(E848), 기록만 하는 경고,
  Time 모드(process_time 역산·두께 기록만)와 그 골든(tests/data/thickness_time_mode_golden.json), ETA 하한·확정 보류,
  Factor 파일(tooling chuck×건, 건별 재료 이름)·재료 목록 파일, 패널·Factor 창·재료 창의 명령 동작(영점·장비에 쓰기·저장 검사),
  연속 기록 CSV(Thickness_YYYYMMDD.csv), Calib CSV 두께 열, 공정 시작·종료 카드 두께 필드, 설정 키.
  패널 그리기 세부(색·위치)는 ui 영역이 덮는다. 바이트 단위 프레임은 dev 영역과 겹칠 수 있다(여기서도 판정에 필요한 만큼 적음).

읽은 v1 파일
  controller/thickness_process.py, controller/thickness_service.py, lib/thickness_config.py, lib/thickness_factors.py,
  lib/thickness_materials.py, lib/thickness_logger.py, device/thickness_monitor.py, ui/thickness_panel.py,
  ui/thickness_factor_dialog.py, ui/thickness_material_dialog.py, config/thickness_factors_ch2.json, config/thickness_materials.json,
  tests/data/thickness_time_mode_golden.json(+ tests/test_thickness_process_steps.py 머리 — 골든 만드는 법),
  runtime/chamber_runtime.py(thick/_thk 검색 206곳 전부), controller/process_controller.py(THICKNESS_* 스텝·ETA),
  controller/data_logger.py(두께 열), controller/chat_notifier.py(두께 카드 필드), main.py(_init_thickness·배치·종료),
  errors/error_registry.py(E840~E848), config/settings.json(ch1·ch2 THICKNESS_*), lib/config_common.py(THICKNESS_* 기본값),
  git show 88455cf(15차 — 2번 연속 확인).
"""
from __future__ import annotations

ITEMS = [
    # ───────────────────────── 장비 응답 해석(STM-100) ─────────────────────────
    dict(id="THK-001", area="thick",
         desc="프레임 = STX 0x02 + LEN(데이터 길이 1바이트) + ASCII 데이터 + CHK(데이터 바이트 합 & 0xFF). 송신 데이터 1~10바이트(밖이면 ValueError). 장비는 질의-응답만",
         v1="device/thickness_monitor.py:5-8;device/thickness_monitor.py:37-38;device/thickness_monitor.py:106-114",
         v2='v2/ch12/drivers/stm100_proto.py', test='v2/tests/test_stm100.py::test_pure_functions_same_as_v1; v2/tests/test_stm100.py::test_fact_table_bytes_and_params', status='done', note=''),
    dict(id="THK-002", area="thick",
         desc="수신 프레임 꺼내기: STX 앞 쓰레기 버림, LEN 1~32 밖이면 1바이트 버리고 체크섬 오류, 체크섬 불일치 오류, 덜 오면 대기. 수신 버퍼 최대 4096바이트(앞을 버림)",
         v1="device/thickness_monitor.py:39-40;device/thickness_monitor.py:117-140;device/thickness_monitor.py:406-421",
         v2='v2/ch12/drivers/stm100_proto.py', test='v2/tests/test_stm100.py::test_pure_functions_same_as_v1; v2/tests/test_stm100.py::test_garbage_is_frame_error_retry', status='done', note=''),
    dict(id="THK-003", area="thick",
         desc="응답 첫 글자 = 결과 코드: A 정상, B 정상+전원리셋, F/G 잘못된 명령, H/I 잘못된 값, J/K 잘못된 수식자(뒤쪽이 리셋 플래그). 모르는 코드는 프레임 오류(재시도). "
              "A/B 가 아니면 query 는 ThicknessCommandRejected('의미(code) — 보낸 명령 …')",
         v1="device/thickness_monitor.py:9-10;device/thickness_monitor.py:42-49;device/thickness_monitor.py:88-94;device/thickness_monitor.py:459-463;device/thickness_monitor.py:506-511",
         v2='v2/ch12/drivers/stm100_proto.py', test='v2/tests/test_stm100.py::test_pure_functions_same_as_v1; v2/tests/test_stm100.py::test_device_commands_same_bytes_as_v1', status='done', note=''),
    dict(id="THK-004", area="thick",
         desc="전원 리셋 플래그(B/G/I/K): 새로 보이면 reset_seen +1 과 '⚠ 두께 모니터 전원 리셋 플래그(code) — L 로 확인 (누적 n)' 로그, 'L' 로 확인(최소 2초 간격, L 자체에는 안 보냄), "
              "실패면 '⚠ 리셋 플래그 확인(L) 실패'. 리셋 없는 응답이 오면 활성 해제",
         v1="device/thickness_monitor.py:43;device/thickness_monitor.py:57;device/thickness_monitor.py:488-504",
         v2='v2/ch12/drivers/stm100.py', test='v2/tests/test_stm100.py::test_reset_flag_counts_and_acks; v2/tests/test_stm100.py::test_device_commands_same_bytes_as_v1', status='done', note=''),
    dict(id="THK-005", area="thick",
         desc="명령 주고받기: 잠금 하나로 한 번에 하나(FIFO), 송신 전 수신 버퍼 비움, 응답 대기 THICKNESS_CMD_TIMEOUT_S(1.0초), 응답 없음·체크섬 오류면 0.1초 뒤 재송신 2번(총 3회), "
              "송신 실패면 연결 끊김 처리 후 통신 실패. 미연결이면 '두께 모니터 미연결'",
         v1="device/thickness_monitor.py:56;device/thickness_monitor.py:242-253;device/thickness_monitor.py:424-486",
         v2='v2/ch12/drivers/stm100.py', test='v2/tests/test_stm100.py::test_device_commands_same_bytes_as_v1; v2/tests/test_stm100.py::test_six_failures_reconnect_with_backoff_and_recover', status='done', note='잠금 = worker 하나(포트 독점)'),
    dict(id="THK-006", area="thick",
         desc="값 해석: 앞에 명령 글자 반향('U 5319234','M !')은 뗌. 본문이 공백·'-'·'.' 만이거나 N/A 면 '값 없음'(ThicknessValueUnavailable, 통신 실패와 구분). "
              "S 두께 Å(부호 정수), T 증착률 Å/s(숫자), U 주파수 Hz(양의 정수), V 수명 %(0~100), M '@'=정상 '!'=XTAL FAIL, E?/F?/J? 숫자. 형식이 틀리면 형식 오류",
         v1="device/thickness_monitor.py:11-13;device/thickness_monitor.py:59-60;device/thickness_monitor.py:149-215;device/thickness_monitor.py:513-542",
         v2='v2/ch12/drivers/stm100_proto.py', test='v2/tests/test_stm100.py::test_pure_functions_same_as_v1; v2/tests/test_stm100.py::test_service_sequences_like_v1', status='done', note=''),
    dict(id="THK-007", area="thick",
         desc="재료값 범위·문자열: density 0.500~99.99(소수 3자리 압축 '1.23'), Z-factor 0.100~9.999(3자리), tooling 10.0~399(1자리). 범위 밖이면 보내지 않고 ValueError",
         v1="device/thickness_monitor.py:51-53;device/thickness_monitor.py:143-146;device/thickness_monitor.py:218-226",
         v2='v2/ch12/drivers/stm100_proto.py', test='v2/tests/test_stm100.py::test_pure_functions_same_as_v1; v2/tests/test_stm100.py::test_set_material_with_model_and_verify_failure', status='done', note=''),
    dict(id="THK-008", area="thick",
         desc="재료값 쓰기: 'E=d' → 'F=z' → 'J=t' → 0.1초 → 'E?','F?','J?' 되읽어 허용오차(density 0.01, Z 0.002, tooling 0.2) 비교, 다르면 1회 다시 쓰고 그래도 다르면 "
              "ThicknessVerifyError('되읽기 불일치 — density 보냄 a/읽음 b, …'). 성공이면 되읽은 값",
         v1="device/thickness_monitor.py:54;device/thickness_monitor.py:248;device/thickness_monitor.py:552-571",
         v2='v2/ch12/drivers/stm100.py', test='v2/tests/test_stm100.py::test_set_material_with_model_and_verify_failure; v2/tests/test_stm100.py::test_device_commands_same_bytes_as_v1', status='done', note=''),
    dict(id="THK-009", area="thick",
         desc="영점 명령 'B'(두께·타이머) / 'C'(두께만), 그 밖은 ValueError. v1 은 패널·공정 모두 'B' 만 보냄",
         v1="device/thickness_monitor.py:545-550;controller/thickness_service.py:195-206;runtime/chamber_runtime.py:1574;ui/thickness_panel.py:339",
         v2='v2/ch12/drivers/stm100.py', test='v2/tests/test_stm100.py::test_thickness_follows_chamber_and_seq_listener; v2/tests/test_stm100.py::test_device_commands_same_bytes_as_v1', status='done', note=''),
    dict(id="THK-010", area="thick",
         desc="연결 유지: 워치독 루프 0.2초마다 미연결이고 재시도 시각이 지났으면 접속(3초), 실패 시 백오프 1→2→…→30초, 실패 로그는 연결될 때까지 1번. EOF 면 끊김 '두께 모니터 연결 끊김 — 재연결 대기'. "
              "quiet(무응답 동안)면 연결·재연결·끊김 로그 생략. 링크 유지(MOXA 60초 무통신)는 상시 폴링이 맡음",
         v1="device/thickness_monitor.py:16-18;device/thickness_monitor.py:241-257;device/thickness_monitor.py:304-404",
         v2='v2/ch12/drivers/stm100.py', test='v2/tests/test_stm100.py::test_reconnect_backoff_1_2_4_8_16_30; v2/tests/test_inv_dev.py::test_stm_link_log_texts_once_and_quiet_while_unresponsive', status='done', note='재연결은 worker 백오프(D18), 연결 로그 줄 이번에 추가'),
    dict(id="THK-011", area="thick",
         desc="오류 문구 분류: 응답 없음/통신 실패/체크섬 오류/프레임 오류/명령 거부/값 없음/되읽기 불일치/응답 형식 오류 — '종류 — 내용'",
         v1="controller/thickness_service.py:54-63", v2='v2/ch12/drivers/stm100_proto.py', test='v2/tests/test_stm100.py::test_error_text_labels_same_as_v1', status='done', note=''),

    # ───────────────────────── 상시 읽기 서비스 ─────────────────────────
    dict(id="THK-012", area="thick",
         desc="상시 읽기(공정·레시피와 무관, 프로그램이 켜져 있는 동안): S·T 는 THICKNESS_POLL_INTERVAL_S(0.5초), U·V·M 은 THICKNESS_SLOW_POLL_S(5초), "
              "적용값 E?·F?·J? 는 연결 직후와 60초마다. 연결 직후 실패 카운터 초기화·느린 읽기 즉시",
         v1="controller/thickness_service.py:5-7;controller/thickness_service.py:47;controller/thickness_service.py:281-298;controller/thickness_service.py:337-371",
         v2='v2/ch12/drivers/stm100.py', test='v2/tests/test_stm100.py::test_service_cadence_like_v1', status='done', note='v2 1초 기록과 별개로 0.5초 유지(plan 3-(2))'),
    dict(id="THK-013", area="thick",
         desc="루프: 폴링 뒤 대기 = max(0.05, 주기 − 걸린 시간), 요청이 오면 바로 깸. 처리 예외 '⚠ 두께 폴링 처리 예외' 60초 1회. 시작 실패 '⚠ 두께 모니터 시작 실패'",
         v1="controller/thickness_service.py:50;controller/thickness_service.py:257-279", v2='v2/ch12/drivers/stm100.py',
         test='v2/tests/test_stm100.py::test_service_cadence_like_v1; v2/tests/test_inv_dev.py::test_stm_status_keys_last_error_listener_and_loop_errors', status='done', note="'⚠ 두께 폴링 처리 예외' 이번에 추가. '⚠ 두께 모니터 시작 실패' 는 v2 에 별도 start 접속이 없어(worker 가 접속) 생기지 않음"),
    dict(id="THK-014", area="thick",
         desc="명령 요청 큐(영점·재료값 쓰기): 폴링과 같은 루프에서 다음 폴링 전에 순서대로, 1건 15초 상한('… 시간 초과(15초)'), 요청자는 20초까지 기다림, "
              "서비스가 안 돌면 '두께 서비스가 동작 중이 아님', 종료 때 남은 요청은 '프로그램 종료 — 요청 취소'",
         v1="controller/thickness_service.py:51;controller/thickness_service.py:122-141;controller/thickness_service.py:221-255",
         v2='v2/ch12/drivers/stm100.py', test='v2/tests/test_stm100.py::test_thickness_follows_chamber_and_seq_listener; v2/tests/test_stm100.py::test_disabled_channel_is_off_and_rejects_jobs; v2/tests/test_stm100.py::test_bench_rejects_writes_allows_reads', status='done', note="요청 = worker write 요청(실행 상한 15 s '… 시간 초과(15초)', 끝나면 바로 poll). 서비스 꺼짐은 '두께 모니터가 꺼져 있음'(DEVICE_DISABLED), 종료 때 남은 요청은 worker 정지가 CANCELLED — 문구 다름(결정 후보)"),
    dict(id="THK-015", area="thick",
         desc="단위 변환 Å → nm ×0.1 은 서비스 한 곳. 정상 S 마다 seq +1 과 ok_ts 갱신(같은 값이어도 새 값으로 셈)",
         v1="controller/thickness_service.py:44;controller/thickness_service.py:317-323", v2='v2/ch12/drivers/stm100.py',
         test='v2/tests/test_stm100.py::test_service_cadence_like_v1; v2/tests/test_stm100.py::test_thickness_follows_chamber_and_seq_listener', status='done', note=''),
    dict(id="THK-016", area="thick",
         desc="S 값 없음: 통신 정상으로 보고 연속 횟수 +1, last_error '값 없음 n회 연속', 처음 없어진 폴링이면(크리스탈이 이미 FAIL 이 아니면) 그 폴링에서 M 을 바로 읽음. "
              "thickness_nm 는 지우지 않고 마지막 값 유지",
         v1="controller/thickness_service.py:310-313;controller/thickness_service.py:380-384", v2='v2/ch12/drivers/stm100.py',
         test='v2/tests/test_stm100.py::test_service_sequences_like_v1; v2/tests/test_inv_dev.py::test_stm_status_keys_last_error_listener_and_loop_errors', status='done', note='마지막 값 유지(패널·CSV 에 stale 과 함께 남음) — v1 그대로 / D148'),
    dict(id="THK-017", area="thick",
         desc="T 값 없음 → rate_nm_s=None(통신 정상). S 가 통신 실패면 그 폴링의 T 는 건너뜀(형식 오류면 T 는 읽음). U·V·M·E?·F?·J? 값 없음 → None(실패로 세지 않음)",
         v1="controller/thickness_service.py:314-335;controller/thickness_service.py:347-349;controller/thickness_service.py:365-367",
         v2='v2/ch12/drivers/stm100.py', test='v2/tests/test_stm100.py::test_service_sequences_like_v1', status='done', note=''),
    dict(id="THK-018", area="thick",
         desc="실패 정책: 통신 실패·프레임 오류만 실패로 세고 6회 연속이면 재연결(카운터 초기화). 형식 오류·명령 거부는 기록만(같은 문구 로그 60초 1번), 재연결 사유 아님. "
              "정상 프레임을 받으면 실패 카운터 0",
         v1="controller/thickness_service.py:11-14;controller/thickness_service.py:45-46;controller/thickness_service.py:373-414",
         v2='v2/ch12/drivers/stm100.py', test='v2/tests/test_stm100.py::test_service_sequences_like_v1; v2/tests/test_stm100.py::test_six_failures_reconnect_with_backoff_and_recover', status='done', note=''),
    dict(id="THK-019", area="thick",
         desc="무응답 전환 로그: 처음 재연결하게 되면 '⚠ 두께 모니터 응답 없음 — 전원·케이블·MOXA 포트 확인 (재연결 계속 시도) [오류]' 1줄 + 드라이버 quiet, 정상 프레임이 다시 오면 '두께 모니터 응답 복귀'",
         v1="controller/thickness_service.py:18-19;controller/thickness_service.py:405-425", v2='v2/ch12/drivers/stm100.py',
         test='v2/tests/test_stm100.py::test_six_failures_reconnect_with_backoff_and_recover; v2/tests/test_inv_dev.py::test_stm_link_log_texts_once_and_quiet_while_unresponsive', status='done', note=''),
    dict(id="THK-020", area="thick",
         desc="last_error = 'HH:MM:SS 문구'(벽시계, 복구 뒤에도 남음)",
         v1="controller/thickness_service.py:398-400", v2='v2/ch12/drivers/stm100.py', test='v2/tests/test_inv_dev.py::test_stm_status_keys_last_error_listener_and_loop_errors', status='done', note=''),
    dict(id="THK-021", area="thick",
         desc="상태 키: connected, ok_ts, age_s, stale(마지막 정상 S 뒤 THICKNESS_STALE_S 초과 또는 없음), link_ok(연결 & 마지막 정상 프레임 ≤ STALE_S — 값 유무 무관), link_age_s, "
              "thickness_nm, rate_nm_s, freq_hz, life_pct, crystal_ok, reset_seen(드라이버 누적), seq, density, z_factor, tooling(장비 되읽기), material(마지막 적용 이름), "
              "last_error, process_running, process_name(공정 중일 때만), t",
         v1="controller/thickness_service.py:22-25;controller/thickness_service.py:427-458", v2='v2/ch12/drivers/stm100.py',
         test='v2/tests/test_inv_dev.py::test_stm_status_keys_last_error_listener_and_loop_errors; v2/tests/test_stm100.py::test_tick_rows_carry_last_value_and_age; v2/tests/test_stm100.py::test_xtal_fail_model_and_recovery', status='done', note='process_running·process_name 은 두께 CSV 쪽(thickness_csv)이 채움'),
    dict(id="THK-022", area="thick",
         desc="공정 중 판단(process_info) = 런타임 is_running 또는 is_busy, 이름 = current_params 의 process_name/Process_name",
         v1="main.py:1315-1322", v2='v2/ch12/outputs/thickness_csv.py; v2/ch12/outputs/row_feed.py', test='v2/tests/test_outputs_csv.py::test_thickness_golden', status='done', note=''),
    dict(id="THK-023", area="thick",
         desc="상태 리스너 발행은 폴링마다(dict 복사), 리스너 예외는 같은 문구 60초 1번 '두께 상태 리스너 예외' 후 계속",
         v1="controller/thickness_service.py:49;controller/thickness_service.py:460-466", v2='v2/ch12/drivers/stm100.py',
         test='v2/tests/test_inv_dev.py::test_stm_status_keys_last_error_listener_and_loop_errors', status='done', note=''),
    dict(id="THK-024", area="thick",
         desc="서비스 로그는 화면·SYSTEM 일자 로그(append_side_log, 출처 THICKNESS)로만 — 공정 로그(.txt)에는 런타임 판정 로그만",
         v1="main.py:1324-1325;controller/thickness_service.py:468-472", v2='v2/ch12/drivers/stm100.py; v2/ch12/process_events.py', test='v2/tests/test_inv_thick.py::test_thickness_service_logs_stay_out_of_process_log', status='done', note='시스템 일자 로그 = system 이벤트(record)'),
    dict(id="THK-025", area="thick",
         desc="영점 요청(서비스): 성공 '두께 영점(B) — 사유', 실패 '⚠ 두께 영점(B) 실패 — 사유: 오류' 후 (False, '영점 실패: …')",
         v1="controller/thickness_service.py:195-206", v2='v2/ch12/drivers/stm100.py', test='v2/tests/test_inv_dev.py::test_stm_zero_and_material_service_texts; v2/tests/test_thickness_ops.py::test_cmd_zero_rechecks_block', status='done', note='실패 문구에 사유 꼬리 이번에 맞춤'),
    dict(id="THK-026", area="thick",
         desc="재료값 적용(서비스): 되읽은 density·Z·tooling 과 재료 이름을 상태에 넣고 '두께 모니터 재료값 적용(사유) 이름 density a · Z b · tooling c%' 로그, 즉시 발행. 실패는 예외로 전달",
         v1="controller/thickness_service.py:208-219", v2='v2/ch12/drivers/stm100.py', test='v2/tests/test_inv_dev.py::test_stm_zero_and_material_service_texts', status='done', note=''),
    dict(id="THK-027", area="thick",
         desc="종료: 루프 취소(2초) → 남은 요청 실패 → 드라이버 stop(2초) → CSV 닫기. main 은 4초 상한, 그 뒤 패널 정리",
         v1="controller/thickness_service.py:122-141;main.py:1423-1430", v2='v2/ch12/system.py; v2/ch12/drivers/stm100.py', test='v2/tests/test_inv_thick.py::test_thickness_config_warning_prefix_and_stop_with_system', status='done', note=''),

    # ───────────────────────── 연속 기록 CSV ─────────────────────────
    dict(id="THK-028", area="thick",
         desc="연속 기록은 그 채널 공정 중에만(set_recording): 공정 시작에 켜고(공정명, 즉시 1행) 끝에 끄며 파일 닫음. 켜져 있으면 1초 1행",
         v1="controller/thickness_service.py:48;controller/thickness_service.py:158-168;controller/thickness_service.py:300-303;runtime/chamber_runtime.py:6756-6759;runtime/chamber_runtime.py:6925-6930;runtime/chamber_runtime.py:1874",
         v2='v2/ch12/outputs/thickness_csv.py', test='v2/tests/test_outputs_csv.py::test_thickness_golden', status='done', note='Time·Thickness 모드 모두'),
    dict(id="THK-029", area="thick",
         desc="CSV 경로 THICKNESS_LOG_DIR\\CH{n}\\Thickness_YYYYMMDD.csv(기본 C:\\VanaM_Logs\\CH1&2\\Thickness), 같은 날이면 이어 씀(머리글·utf-8-sig 는 새 파일·빈 파일만, 이어 쓸 땐 utf-8), "
              "날짜가 바뀌면 새 파일, 열 수 없으면 LOCAL_FALLBACK_ROOT\\Thickness\\CH{n}(경고 1회), 실패는 경고 1회 후 60초 뒤 다시 엶, 매 행 flush",
         v1="lib/thickness_logger.py:1-8;lib/thickness_logger.py:20-27;lib/thickness_logger.py:73-132", v2='v2/ch12/outputs/thickness_csv.py; v2/ch12/outputs/paths.py',
         test='v2/tests/test_outputs_csv.py::test_thickness_golden; v2/tests/test_outputs_csv.py::test_thickness_gdrive_dir_replaced_and_fallback; v2/tests/test_inv_gap_a.py::test_thickness_csv_open_and_write_failures_retry_after_60s', status='done', note=''),
    dict(id="THK-030", area="thick",
         desc="CSV 열: timestamp(YYYY-MM-DD HH:MM:SS), connected(1/0), thickness_nm(.2f), rate_nm_s(.3f), freq_hz(정수), life_pct(.1f), crystal_ok, stale, process_running(1/0/빈칸), process_name",
         v1="lib/thickness_logger.py:18-19;lib/thickness_logger.py:29-39;lib/thickness_logger.py:108-119", v2='v2/ch12/outputs/thickness_csv.py',
         test='v2/tests/test_outputs_csv.py::test_thickness_golden', status='done', note=''),

    # ───────────────────────── 설정 ─────────────────────────
    dict(id="THK-031", area="thick",
         desc="연결 설정 읽기(호출 시점, 경고 후 안전 쪽): SUPPORTS_THICKNESS, HOST/PORT 가 올바르지 않으면(빈 host, port 1~65535 밖) 끔, POLL 0.25~5, SLOW_POLL POLL~60, "
              "CMD_TIMEOUT 0.3~5, STALE ≥ max(2, 4×POLL), LOG_DIR 비면 기본·'G:' 면 거부하고 기본(구글드라이브 사라짐 문제), FACTOR_FILE 상대 경로는 exe 옆/프로젝트 루트 기준(비면 config/thickness_factors_ch{N}.json)",
         v1="lib/thickness_config.py:15-26;lib/thickness_config.py:103-161", v2='v2/ch12/drivers/stm100.py; v2/ch12/params/stm100.py; v2/ch12/thickness_factors.py', test='v2/tests/test_stm100.py::test_settings_clamps_like_v1; v2/tests/test_outputs_csv.py::test_thickness_gdrive_dir_replaced_and_fallback; v2/tests/test_thickness_files.py::test_factor_file_location_in_settings_dir', status='done', note='FACTOR_FILE 상대 경로 = 설정 폴더 기준(v2 설정 폴더)'),
    dict(id="THK-032", area="thick",
         desc="판정 상수 읽기(범위 클램프·형식 오류 경고, bool 은 형식 오류): STALE 2~120, READY_FRESH 1~60, LIFE_WARN 0~100, MAX_TIME_RATIO 1~10, MARGIN 0~7200, ZERO_CONFIRM_NM 0.1~100, "
              "ZERO_CONFIRM_S 1~60, NEG_WARN 0.1~100, DONE_CONSECUTIVE 1~10(정수), JUMP_MIN 0.1~100, JUMP_RATE_K 1~1000, MEASURED_RATE_MIN_S 5~3600, RATE_ZERO_WARN_S 5~3600, RATE_SPIKE_K 1.5~100",
         v1="lib/thickness_config.py:44-100", v2='v2/ch12/thickness_judge.py; v2/ch12/params/core_thickness.py', test='v2/tests/test_thickness_judge.py::test_params_clamp_and_format_like_v1; v2/tests/test_thickness_judge.py::test_defaults', status='done', note=''),
    dict(id="THK-033", area="thick",
         desc="운전값: ch2 SUPPORTS_THICKNESS true, HOST 192.168.1.50, PORT 4008(임시값), POLL 0.5, SLOW 5.0, STALE 10, CMD_TIMEOUT 1.0, LOG_DIR C:\\VanaM_Logs\\CH1&2\\Thickness, "
              "FACTOR_FILE config/thickness_factors_ch2.json, READY_FRESH 5, LIFE_WARN 10, RATIO 2.0, MARGIN 300, ZERO 2.0nm/5.0s, NEG 2.0, DONE_CONSECUTIVE 2, JUMP 1.0/10, "
              "MEASURED_RATE_MIN 30, RATE_ZERO_WARN 60, SPIKE 5. ch1 SUPPORTS_THICKNESS false(PORT 0, DONE_CONSECUTIVE 키 없음 → 공통 1). 공통 기본 DONE_CONSECUTIVE 1",
         v1="config/settings.json:513-526;config/settings.json:643-667;lib/config_common.py:462;lib/config_common.py:520-541",
         v2='v2/ch12/params/stm100.py; v2/ch12/params/core_thickness.py', test='v2/tests/test_stm100.py::test_fact_table_bytes_and_params; v2/tests/test_thickness_judge.py::test_params_from_cfg_ch2_done_consecutive_is_2_and_ch1_is_1', status='done', note='v1→v2 설정 변환 대상, PORT 4008 은 settings 주석상 임시값 / D152·D149'),
    dict(id="THK-034", area="thick",
         desc="판정 설정 경고는 서비스 붙일 때 '⚠ 두께 판정 설정: …', 연결 설정 경고는 '⚠ 두께 모니터 설정: …'(side log)",
         v1="runtime/chamber_runtime.py:6591-6597;main.py:1307-1309", v2='v2/ch12/thickness.py; v2/ch12/drivers/stm100.py', test='v2/tests/test_thickness_ops.py::test_load_warnings_logged_at_start; v2/tests/test_inv_thick.py::test_thickness_config_warning_prefix_and_stop_with_system', status='done', note='연결 설정 경고 머리말 이번에 맞춤'),
    dict(id="THK-035", area="thick",
         desc="초기화: SUPPORTS_THICKNESS 꺼진 채널은 아무것도 안 함. 서비스 시작, 패널은 CH2 만(히터 패널 밑, 히터 없으면 (1086,10)), 런타임에 붙이고 패널 진행 영역 콜백 연결. "
              "로그 'CHn 두께 모니터 서비스 시작 (host:port)', 연동 실패 '⚠ 두께 모니터 공정 연동 실패 — Thickness 모드 끔(Time 모드만)', 초기화 실패 'CHn 두께 모니터 초기화 실패'",
         v1="main.py:1294-1366", v2='v2/ch12/assembly.py; vanam-process components/ops2/ch12/Ch12Aux.tsx', test='v2/tests/test_inv_dev.py::test_thickness_service_start_log_on_assembly; v2/tests/test_assembly.py::test_build_wires_everything_and_all_paths_pass_core', status='done', note='패널은 포털 Ch12Aux(두께 블록 카드). 시작 로그 이번에 추가'),

    # ───────────────────────── Factor·재료 파일 ─────────────────────────
    dict(id="THK-036", area="thick",
         desc="Factor 파일(version 2): tooling[up|mid|down][G1|G2|G3](%, 10~399, null=미설정), material[G1..G3] = {'material': 재료 이름}(빈 문자열=미선택). "
              "파일 없으면 빈 표(경고 없음), 읽기 실패·형식 오류는 빈 표+경고, 틀린 칸은 그 칸만 미설정+경고, 이전 형식(density·z_factor 숫자)은 경고 후 무시",
         v1="lib/thickness_factors.py:1-12;lib/thickness_factors.py:24-115;config/thickness_factors_ch2.json:1-31", v2='v2/ch12/thickness_factors.py',
         test='v2/tests/test_thickness_files.py::test_load_factors_same_as_v1; v2/tests/test_thickness_files.py::test_invalid_values_only_that_cell_unset; v2/tests/test_thickness_files.py::test_missing_file_is_blank_table; v2/tests/test_thickness_files.py::test_shipped_v1_ch2_file_is_blank_and_valid', status='done', note='D12'),
    dict(id="THK-037", area="thick",
         desc="Factor·재료 파일 저장: 같은 폴더 임시 파일(utf-8, LF, indent 2, ensure_ascii=False)에 쓰고 fsync 후 os.replace(원자적), 실패면 임시 파일 지우고 예외(원본 그대로)",
         v1="lib/thickness_factors.py:118-136;lib/thickness_materials.py:131-153", v2='v2/ch12/thickness_factors.py; v2/ch12/thickness_materials.py', test='v2/tests/test_thickness_files.py::test_atomic_save_roundtrip_korean_path; v2/tests/test_thickness_files.py::test_atomic_save_failure_keeps_original; v2/tests/test_thickness_files.py::test_save_rejects_duplicates_and_is_atomic', status='done', note=''),
    dict(id="THK-038", area="thick",
         desc="재료 목록 파일 config/thickness_materials.json(version 1, materials[name,symbol,density,z_factor,note]): 없으면 STM-100 매뉴얼 4.3 표 99종으로 새로 만듦(Z '-' 인 재료는 1.000, "
              "note '매뉴얼 Z 미기재 — 1.000 사용'), 읽기 실패면 기본 목록+경고(파일 안 건드림), 잘못된 항목·이름 중복(대소문자 무시, 뒤쪽)은 경고 후 제외. 이름 찾기는 대소문자 무시. 표시 '기호 — 이름'",
         v1="lib/thickness_materials.py:1-10;lib/thickness_materials.py:23-128;lib/thickness_materials.py:156-203;config/thickness_materials.json:1-3",
         v2='v2/ch12/thickness_materials.py', test='v2/tests/test_thickness_files.py::test_default_list_same_as_v1_manual_table; v2/tests/test_thickness_files.py::test_default_file_same_as_v1_shipped_file; v2/tests/test_thickness_files.py::test_load_materials_same_as_v1', status='done', note='배포 파일 내용 = 기본 목록과 같음(99종 일치 확인)'),
    dict(id="THK-039", area="thick",
         desc="서비스 시작 때 Factor 경고 '⚠ Factor 파일: …', 재료 경고 '⚠ 재료 목록: …'. 창에서 저장하면 reload_factors — 'Factor 파일·재료 목록 다시 읽음[ — 경고 n건]'(진행 중 공정엔 영향 없음, 다음 공정부터)",
         v1="controller/thickness_service.py:102-106;controller/thickness_service.py:174-193", v2='v2/ch12/thickness.py; v2/ch12/thickness_ops.py',
         test='v2/tests/test_thickness_ops.py::test_load_warnings_logged_at_start; v2/tests/test_thickness_ops.py::test_factor_and_material_save_reload', status='done', note=''),

    # ───────────────────────── 모드 판정·시작 전 점검 ─────────────────────────
    dict(id="THK-040", area="thick",
         desc="Thickness 모드 = 레시피 thickness > 0 이고 그 채널이 두께 모니터를 씀(SUPPORTS_THICKNESS=true + 서비스 붙음). 아니면 Time 모드(process_time 칸이 비었는지는 보지 않음). ETA 도 같은 규칙",
         v1="controller/thickness_process.py:63-67;runtime/chamber_runtime.py:6612-6619;runtime/chamber_runtime.py:4044", v2='v2/ch12/thickness.py',
         test='v2/tests/test_thickness.py::test_supported_requires_driver_and_flag; v2/tests/test_eta.py::test_thickness_rows_count_shutter_only_when_supported', status='done', note=''),
    dict(id="THK-041", area="thick",
         desc="Time 모드 process_time 역산: process_time 이 비었거나 ≤0 이고 dep_rate(헤더 dep_rate 또는 dep.rate, >0)·thickness 가 있으면 thickness ÷ dep_rate ÷ 60(분) "
              "+ 로그 'process_time 자동 계산: Tnm ÷ Rnm/s = x.xxx분'. 화면에서 Dep.Rate·Thickness 를 바꾸면 processTime_edit 에 소수 3자리로 자동 입력",
         v1="runtime/chamber_runtime.py:6297-6319;runtime/chamber_runtime.py:3334-3357;controller/process_controller.py:115-138",
         v2='v2/ch12/recipe.py; v2/ch12/eta.py', test='v2/tests/test_recipe.py::test_normalize_misc; v2/tests/test_eta.py::test_1_thickness_recipe_matches_runtime_inversion; v2/tests/test_eta.py::test_2_dep_rate_key_variants_and_invalid', status='done', note='화면 자동 입력은 포털 레시피 편집'),
    dict(id="THK-042", area="thick",
         desc="레시피 열 tooling·density·z_factor(선택 숫자, 비면 Factor). 원시 행 검사(E230): thickness·dep_rate·dep.rate·tooling·density·z_factor 숫자, "
              "범위 tooling 10~399 %, density 0.5~99.99 g/cm3, z_factor 0.1~9.999('… → 범위 a~b 단위 밖입니다')",
         v1="runtime/chamber_runtime.py:74-75;runtime/chamber_runtime.py:6370-6373;runtime/chamber_runtime.py:8590-8608;runtime/chamber_runtime.py:8683-8695",
         v2='v2/ch12/recipe.py; v2/ch12/recipe_check.py', test='v2/tests/test_process_steps_golden.py::test_raw_rows_e230; v2/tests/test_recipe.py::test_raw_check_messages_and_overflow', status='done', note=''),
    dict(id="THK-043", area="thick",
         desc="Chuck 이동 전 점검(Thickness 모드만): 서비스 없음·미연결·link 없음 → E840 '두께 모니터 응답 없음 — 연결·최근 5초 안 정상 두께 값이 필요합니다 (마지막 정상 값 N초 전)', "
              "XTAL FAIL → E841, 최근 READY_FRESH_S 안 정상 두께 없음 → E840 '두께 값 없음(통신은 정상) …', dep_rate·thickness ≤0 → E842, 사용 건 ≠ 1개 → E843('co-sputter(G1+G2)'/'사용 건 없음'), "
              "중간 펄스 전력 변경 충돌 → E844",
         v1="runtime/chamber_runtime.py:4042-4048;runtime/chamber_runtime.py:6621-6650", v2='v2/ch12/thickness.py', test='v2/tests/test_thickness.py::test_basic_check_rejects; v2/tests/test_thickness.py::test_basic_check_messages_and_pass', status='done', note=''),
    dict(id="THK-044", area="thick",
         desc="중간 펄스 전력 변경 충돌 규칙: DC/RF 펄스 사용 & 콜백 있음 & power_change_time·change_power_value 둘 다 값 & 전력 > 0 & 변경 시점 ≥ shutter_delay 이면 "
              "'중간 펄스 전력 변경(power_change_time=…, nW)이 메인 공정 구간에 있습니다 — Thickness 모드는 … 함께 쓸 수 없습니다'(Shutter Delay 안 변경은 허용). 시퀀스 만들 때도 E844",
         v1="controller/process_controller.py:1083-1108;controller/process_controller.py:2207-2212", v2='v2/ch12/thickness.py; v2/ch12/blocks.py',
         test='v2/tests/test_thickness.py::test_basic_check_messages_and_pass; v2/tests/test_blocks.py::test_template_e844_and_refs', status='done', note=''),
    dict(id="THK-045", area="thick",
         desc="Chuck 확정 뒤 재료값: Chuck 이동 실패면 실제 위치로('Chuck 이동 실패 — Factor 는 실제 위치 …'), up/mid/down 아니면 E845. 값별로 레시피 우선, 비면 Factor — tooling = chuck×건, "
              "density·Z = 그 건 재료 이름을 재료 목록에서 찾은 값(레시피에 둘 다 있으면 재료는 안 봄). 빠지면 E845 'Factor 미설정 — … (화면 [Factor 설정] 또는 파일)'",
         v1="runtime/chamber_runtime.py:4068-4072;runtime/chamber_runtime.py:6652-6694", v2='v2/ch12/thickness.py', test='v2/tests/test_thickness.py::test_apply_material_factor_and_errors; v2/tests/test_thickness.py::test_apply_material_uses_actual_chuck_when_move_failed; v2/tests/test_thickness.py::test_apply_recipe_values_take_priority_per_value; v2/tests/test_thickness.py::test_apply_e845_only_when_value_missing_in_both', status='done', note=''),
    dict(id="THK-046", area="thick",
         desc="장비에 재료값 쓰기 실패(되읽기 포함) → E846. 성공이면 factor_source 'T:recipe|factor D:… Z:…', 레시피 값을 쓴 항목이 있으면 thickness_factor_note '레시피 지정(…)', "
              "시작 전 값(chuck·건·tooling·density·Z·재료·출처) 보관, 로그 'Thickness 모드 — 목표 T nm · 건 · chuck c · tooling …(출처) · density …(출처) · Z …(출처)'",
         v1="runtime/chamber_runtime.py:6695-6725", v2='v2/ch12/thickness.py', test='v2/tests/test_thickness.py::test_apply_material_factor_and_errors; v2/tests/test_thickness.py::test_apply_recipe_values_take_priority_per_value', status='done', note=''),
    dict(id="THK-047", area="thick",
         desc="크리스탈 수명 < THICKNESS_LIFE_WARN_PCT(10%) 이면 경고만 '크리스탈 수명 x% (경고 기준 10% 미만) — 교체 준비'(로그 + 이번 런 경고 → 종료 카드)",
         v1="runtime/chamber_runtime.py:6714-6720", v2='v2/ch12/thickness.py', test='v2/tests/test_thickness.py::test_apply_material_factor_and_errors', status='done', note=''),
    dict(id="THK-048", area="thick",
         desc="점검 실패 흐름: 로그 '[CHn] 두께 모니터 점검 실패 [code] msg → 공정 시작 차단', 챗 장비 오류 카드(notify_error_event + flush, '… (공정: 이름)'), 자동연결 끔, 장비 감시 정지, "
              "공정 상태 꺼짐, runtime_state 오류·종료 기록, CodedRuntimeError(code)",
         v1="runtime/chamber_runtime.py:6727-6742", v2='v2/ch12/thickness.py; v2/ch12/process_preflight.py', test='v2/tests/test_thickness.py::test_time_mode_ignores_recipe_factor_columns_and_reject_texts', status='done', note='카드는 포털(error_event)'),
    dict(id="THK-049", area="thick",
         desc="통과하면 params.thickness_mode=True, thickness_expected_s = thickness ÷ dep_rate, thickness_limit_s = max(예상×RATIO, 예상+MARGIN) — 시작 카드용",
         v1="runtime/chamber_runtime.py:4073-4078;controller/thickness_process.py:70-78", v2='v2/ch12/thickness.py',
         test='v2/tests/test_thickness.py::test_apply_material_factor_and_errors', status='done', note=''),

    # ───────────────────────── 공정 스텝 ─────────────────────────
    dict(id="THK-050", area="thick",
         desc="Thickness 모드 스텝: Shutter Delay 뒤·Main Shutter 열기 전에 THICKNESS_ZERO('두께 영점·확인 (≤ 2 nm, 최대 5초)', params=(ZERO_CONFIRM_S,)) — use_ms=F 여도 같은 자리",
         v1="controller/process_controller.py:2647-2655", v2='v2/ch12/process_steps.py; v2/ch12/blocks_expand.py', test='v2/tests/test_process_engine.py::test_thickness_mode_run_ends_on_target; v2/tests/test_process_steps_golden.py::test_combo', status='done', note=''),
    dict(id="THK-051", area="thick",
         desc="메인 공정 DELAY 대신 OES_RUN(상한 초, integration_ms, no_wait, 'OES 측정 시작 (메인 공정 끝까지, 최대 x.xx분)') + THICKNESS_WAIT(value=목표 nm, params=(예상 초, 상한 초), "
              "polling=True, '메인 공정 진행 (두께 Tnm, 예상 x.xx분)'). 중간 전력 변경 분할 없음",
         v1="controller/process_controller.py:2665-2686", v2='v2/ch12/process_steps.py; v2/ch12/blocks_expand.py', test='v2/tests/test_process_engine.py::test_thickness_mode_run_ends_on_target; v2/tests/test_process_steps_golden.py::test_combo', status='done', note=''),
    dict(id="THK-052", area="thick",
         desc="THICKNESS_ZERO params 는 (확인 최대 초,), THICKNESS_WAIT params 는 (예상 초, 상한 초) — 아니면 E847, THICKNESS_WAIT 는 value 필수. 토큰 THICKNESS_DONE/THICKNESS_ZERO",
         v1="controller/process_controller.py:327-329;controller/process_controller.py:351-382;controller/process_controller.py:3046",
         v2='v2/ch12/process_steps.py; v2/ch12/process_tokens.py', test='v2/tests/test_blocks.py::test_process_step_validation_codes', status='done', note=''),
    dict(id="THK-053", area="thick",
         desc="Time 모드 스텝은 두께 기능 전과 같음 — 골든 4개(ch2_thk_dcp, ch2_thk_change_main, ch1_thk_rfp, ch2_thk_heater_rf, 커밋 6cd7d1a 코드로 생성)와 ETA eta_rows 4790초",
         v1="tests/data/thickness_time_mode_golden.json:2-1300;tests/test_thickness_process_steps.py:1-60", v2='v2/ch12/process_steps.py; v2/ch12/eta.py',
         test='v2/tests/test_process_steps_golden.py::test_v1_repo_golden', status='done', note='tests/data/thickness_time_mode_golden.json 4 시나리오'),
    dict(id="THK-054", area="thick",
         desc="THICKNESS_ZERO 단계: 콜백 없으면 '⚠ 두께 영점 콜백 없음 → 영점 없이 진행', 시작 예외면 '⚠ 두께 영점 시작 실패(…) → 영점 없이 진행', 끝 토큰을 확인 초 + 30초까지 기다림 "
              "(넘으면 '⚠ 두께 영점·확인 N초 초과 → 기다림을 끊고 진행'), 중단이면 '두께 영점·확인 중단됨'. 어떤 경우에도 공정을 멈추지 않음, Shutter Delay·메인 시간에 넣지 않음",
         v1="controller/process_controller.py:141;controller/process_controller.py:1488-1517", v2='v2/ch12/process_runner.py; v2/ch12/thickness.py',
         test='v2/tests/test_thickness.py::test_zero_step_hard_budget_never_hangs_and_no_driver; v2/tests/test_thickness.py::test_zero_step_abort; v2/tests/test_thickness.py::test_zero_step_comm_lost_then_permanent_time_fallback', status='done', note=''),
    dict(id="THK-055", area="thick",
         desc="영점(런타임): 세대 +1, 서비스 없으면 기준점 없음으로 통과. 영점 전 reset_seen 기록 → zero('B', '메인 셔터 열기 전 영점') → 실패면 '⚠ 영점 명령 실패(…)' → 패널 'Thickness / — / 0% / 영점 확인 중' → "
              "0.1초마다 ZeroConfirm → 결과 로그·경고(이번 런 경고 목록) → 기준점·reset0 보관 → 영점 끝 토큰",
         v1="runtime/chamber_runtime.py:1559-1595", v2='v2/ch12/thickness.py', test='v2/tests/test_thickness.py::test_zero_step_confirms_and_sets_base_zero; v2/tests/test_thickness.py::test_zero_step_not_confirmed_or_cmd_failed', status='done',
         note='v1 은 영점 확인에만 time.monotonic(), 판정기는 서비스 시계 — 실기에선 같음'),
    dict(id="THK-056", area="thick",
         desc="영점 확인 규칙: XTAL FAIL 이면 바로 기준점 없음 '기준점 없음 — XTAL FAIL(크리스탈 이상), 시간으로 진행'. 새 값(seq 변화)마다: 영점 실패면 첫 값을 기준점(경고), "
              "값 ≤ ZERO_CONFIRM_NM(음수 포함)이면 기준점 0 '영점 확인 — 영점 뒤 t초 값 x nm (≤ 2) → 기준점 0 nm'. ZERO_CONFIRM_S 안에 확인 안 되면 마지막 새 값을 기준점(경고), 새 값이 없으면 기준점 없음(경고)",
         v1="controller/thickness_process.py:9-14;controller/thickness_process.py:88-129", v2='v2/ch12/thickness_judge.py', test='v2/tests/test_thickness_judge.py::test_zero_confirm_same_as_v1; v2/tests/test_thickness_judge.py::test_zero_confirm_xtal_fail_is_immediate; v2/tests/test_thickness_judge.py::test_not_confirmed_in_5s_uses_last_value_with_warning; v2/tests/test_thickness_judge.py::test_zero_command_failed_takes_first_value_with_warning; v2/tests/test_thickness_judge.py::test_no_values_means_no_base', status='done', note='D60'),
    dict(id="THK-057", area="thick",
         desc="두께 판정 시작(메인 셔터 연 뒤): 서비스 없으면 예상시간으로 시간 대체. 레시피 dep_rate(없으면 목표÷max(1,예상)), 판정기(시작 = 서비스 시계, 기준점·reset0 = 영점 결과), "
              "로그 '두께 판정 시작 — 목표 T nm · 기준점 없음|x nm · 레시피 증착률 r · 예상 n초 · 상한 m초', 패널 '메인 공정 진행'. 기준점이 없으면 처음부터 영구 시간 대체",
         v1="runtime/chamber_runtime.py:1597-1621;controller/thickness_process.py:174-178", v2='v2/ch12/thickness.py', test='v2/tests/test_thickness.py::test_listener_done_fallback_resume; v2/tests/test_thickness.py::test_start_failure_and_no_driver_use_expected_time; v2/tests/test_thickness_judge.py::test_no_base_is_permanent_fallback_from_start', status='done', note=''),
    dict(id="THK-058", area="thick",
         desc="THICKNESS_WAIT 단계: 폴링 켬, 카운트다운 총량 = 상한, 끝 = THICKNESS_DONE 토큰 | 시간 대체 마감 | 상한 | abort(STOP·종료 절차) 중 먼저. 대기 간격 ≤1초. "
              "판정 시작 콜백이 없거나 예외면 예상시간 뒤 시간 대체. 로그 '… 시작 — 목표 T nm, 예상 n초, 최대 m초 (목표 도달 시 종료, 이상 시 시간 대체, 상한 도달 시 정상 종료 절차)', 끝 '… 완료 — 종료 방식 X (n초)'|'… 중단됨'",
         v1="controller/process_controller.py:1519-1600", v2='v2/ch12/thickness.py; v2/ch12/process_runner.py', test='v2/tests/test_thickness.py::test_limit_timer_ends_without_listener_e848; v2/tests/test_thickness.py::test_done_ends_thickness_and_stops_oes_once; v2/tests/test_thickness.py::test_abort_ends_wait_and_records', status='done',
         note="시작 로그의 '상한 도달 시 정상 종료 절차' 문구와 실제(E848 실패) 다름 — 알려진 이상 동작 / D147(시작 로그 문구 v1 그대로)"),
    dict(id="THK-059", area="thick",
         desc="상한 도달 → 공정 실패 E848 '두께 목표 미도달 — 최대 시간 초과: 유효 x / 목표 y nm, 경과 a분(상한 b분)'(공정 중 장비 이상과 같은 _step_failed, 종료 절차 첫 단계 Main Shutter 닫기). "
              "시간 대체 마감은 정상 종료(다음 단계로)",
         v1="controller/process_controller.py:1520-1523;controller/process_controller.py:1574-1579;controller/process_controller.py:1607-1613",
         v2='v2/ch12/thickness.py', test='v2/tests/test_thickness.py::test_limit_timer_ends_without_listener_e848; v2/tests/test_thickness_sim.py::test_sim_limit_reached_is_e848', status='done', note='D61'),
    dict(id="THK-060", area="thick",
         desc="시간 대체 알림: 대기 중일 때만 마감 = 지금 + 남은 초, 로그 '두께 → 시간 대체: n초 뒤 메인 공정 종료 (사유)'. 복귀 알림은 시간 대체 타이머만 지움(상한 유지) '두께 판정 복귀 — 시간 대체 타이머 해제'. "
              "상태창 문구는 대기 중·종료 절차 아님일 때만",
         v1="controller/process_controller.py:1054-1071", v2='v2/ch12/thickness.py', test='v2/tests/test_thickness.py::test_fallback_timer_ends_and_resume_cancels; v2/tests/test_thickness_sim.py::test_sim_link_lost_falls_back_then_recovers_and_ends', status='done', note=''),
    dict(id="THK-061", area="thick",
         desc="두께 판정 취소(대기 해제·타이머 지움·런타임 세대 +1·판정기 없앰)는 STOP·실패·종료 절차 때",
         v1="controller/process_controller.py:1073-1081;controller/process_controller.py:783;controller/process_controller.py:793;controller/process_controller.py:1959;controller/process_controller.py:2086;runtime/chamber_runtime.py:1630-1632",
         v2='v2/ch12/thickness.py', test='v2/tests/test_thickness.py::test_abort_ends_wait_and_records; v2/tests/test_thickness.py::test_stop_before_main_records_abort', status='done', note=''),

    # ───────────────────────── 종료 판정 ─────────────────────────
    dict(id="THK-062", area="thick",
         desc="판정 tick(서비스 폴링마다, 0.5초): 영구 대체면 아무것도 안 함. reset_seen 이 영점 전보다 늘면 영구 시간 대체 '공정 중 두께 모니터 전원 리셋(n회)', XTAL FAIL 이면 영구 시간 대체. "
              "새 값(seq 변화·값 있음)이 없고 두께 판정 중이며 마지막 새 값 뒤 THICKNESS_STALE_S(10초) 초과면 시간 대체(복귀 가능) '새 두께 값 없음 N초'",
         v1="controller/thickness_process.py:272-286", v2='v2/ch12/thickness_judge.py', test='v2/tests/test_thickness_judge.py::test_judge_same_events_and_logs_as_v1; v2/tests/test_thickness_judge.py::test_xtal_fail_or_reset_is_permanent; v2/tests/test_thickness_judge.py::test_stale_fallback_then_resume_and_done_on_resume', status='done', note='v1 판정기를 같은 입력으로 돌린 골든(reset_flag·xtal_fail·stale_resume)'),
    dict(id="THK-063", area="thick",
         desc="종료: 유효두께 = 장비 두께 − 기준점, ≥ 목표 − 1e-6 인 새 값이 THICKNESS_DONE_CONSECUTIVE 번 연속이면 done '목표 도달 — 유효 x / T nm (연속 n회)'. 하나라도 미만이면 다시 셈. "
              "CH2 운전값 2(늦은 응답이 두께로 읽혀 한 값이 튀어도 종료하지 않게, 약 0.5초 늦음)",
         v1="controller/thickness_process.py:44;controller/thickness_process.py:287-322;config/settings.json:662",
         v2='v2/ch12/thickness_judge.py', test='v2/tests/test_thickness_judge.py::test_consecutive2_single_spike_then_two_in_a_row; v2/tests/test_thickness_judge.py::test_judge_same_events_and_logs_as_v1; v2/tests/test_thickness_sim.py::test_sim_target_reached_twice_in_a_row_ends', status='done', note='15차 커밋 88455cf(1 → 2)'),
    dict(id="THK-064", area="thick",
         desc="통신 복귀(시간 대체 중 새 값): 두께 판정으로, 연속 횟수 초기화. 첫 값이 이미 목표 이상이면 연속 1로 세고 — 필요 횟수가 1이면 done('복귀 시 이미 목표 이상'), "
              "아니면 resume('통신 복귀(목표 이상 — 다음 값으로 확인)'). 미만이면 resume '두께 판정 복귀 — 유효 x / T nm'",
         v1="controller/thickness_process.py:292-305", v2='v2/ch12/thickness_judge.py', test='v2/tests/test_thickness_judge.py::test_consecutive2_resume_first_value_over_target_needs_next; v2/tests/test_thickness_judge.py::test_consecutive1_resume_over_target_ends_at_once; v2/tests/test_thickness_judge.py::test_judge_same_events_and_logs_as_v1', status='done', note='88455cf 에서 바뀐 부분'),
    dict(id="THK-065", area="thick",
         desc="시간 대체 전환: 이미 대체 중이면 다시 내지 않음(영구 사유가 오면 영구로 올리고 '시간 대체 유지 — 두께 판정으로 돌아가지 않음: 사유'). 전환 횟수·사유 기록, "
              "로그 '⚠ 시간 대체로 전환 — 사유. 남은 시간 n초 = (목표 − 유효두께) ÷ 실측|레시피 r nm/s [(실측 … 밖 → 레시피 사용)] − 마지막 값 뒤 t초 반영[ · 통신이 돌아오면 두께 판정으로 복귀]'",
         v1="controller/thickness_process.py:244-270", v2='v2/ch12/thickness_judge.py', test='v2/tests/test_thickness_judge.py::test_stale_then_xtal_fail_becomes_permanent_without_new_event; v2/tests/test_thickness_judge.py::test_judge_same_events_and_logs_as_v1', status='done', note=''),
    dict(id="THK-066", area="thick",
         desc="시간 대체 증착률: 셔터 연 뒤 측정 ≥ THICKNESS_MEASURED_RATE_MIN_S(30초)·유효두께 > 0 이면 실측 평균(유효두께 ÷ 경과), 실측이 레시피 dep_rate 의 0.5~2.0배 안이면 실측, 아니면 레시피. "
              "남은 시간 = max(0, 목표 − 마지막 유효두께) ÷ 증착률 − (지금 − 마지막 값 시각)",
         v1="controller/thickness_process.py:45-46;controller/thickness_process.py:187-209", v2='v2/ch12/thickness_judge.py',
         test='v2/tests/test_thickness_judge.py::test_fallback_rate_measured_after_30s; v2/tests/test_thickness_judge.py::test_use_rate_sane_range; v2/tests/test_thickness_judge.py::test_out_of_range_rate_is_logged_on_fallback; v2/tests/test_thickness_judge.py::test_fallback_remaining_subtracts_time_since_last_value', status='done', note=''),
    dict(id="THK-067", area="thick",
         desc="기록만 하는 경고(판정 무관, 셔터 연 뒤 두 번째 값부터): 큰 튐 증가량 > max(JUMP_MIN 1 nm, dep_rate×10×Δt)(횟수 셈), 두께가 60초 늘지 않음(1회, 늘면 다시 무장), "
              "장비 증착률 > dep_rate×5(켜짐, ×2.5 미만이면 해제), 유효두께 < −2 nm(1회)",
         v1="controller/thickness_process.py:307-311;controller/thickness_process.py:324-348", v2='v2/ch12/thickness_judge.py',
         test='v2/tests/test_thickness_judge.py::test_warnings_only_do_not_change_judgement; v2/tests/test_thickness_judge.py::test_negative_warning_uses_neg_warn_nm; v2/tests/test_thickness_judge.py::test_judge_same_events_and_logs_as_v1', status='done', note=''),
    dict(id="THK-068", area="thick",
         desc="판정 코드 예외는 영구 시간 대체(traceback 은 이벤트 detail → '⚠ 두께 판정 예외 traceback:' 로그). 호출부 예외도 판정기를 내리고 영구 시간 대체 '판정 예외(…)'(traceback 1번)",
         v1="controller/thickness_process.py:180-185;runtime/chamber_runtime.py:6806-6830", v2='v2/ch12/thickness_judge.py; v2/ch12/thickness.py',
         test='v2/tests/test_thickness_judge.py::test_judge_exception_falls_back; v2/tests/test_thickness.py::test_listener_exception_falls_back_once_with_traceback; v2/tests/test_thickness.py::test_judge_internal_exception_event_logs_traceback', status='done', note=''),
    dict(id="THK-069", area="thick",
         desc="XTAL FAIL 판정 = crystal_ok is False 이고 통신 정상(link_ok, 없으면 not stale) — 값 없음(stale)이어도 봄",
         v1="controller/thickness_process.py:56-60", v2='v2/ch12/thickness_judge.py', test='v2/tests/test_thickness_judge.py::test_xtal_fail_seen_while_values_missing_if_link_ok', status='done', note=''),
    dict(id="THK-070", area="thick",
         desc="두께 상태 리스너(런타임): 기록 → 판정 tick(서비스 시계) → 이벤트 적용(done → 판정기 내리고 THICKNESS_DONE, fallback → 시간 대체, resume → 복귀) → "
              "판정 중(또는 방금 대체)이면 상태 문구를 공정 상태창과 패널(종료 방식 'Thickness'|'시간 대체', 목표, 진행률)에",
         v1="runtime/chamber_runtime.py:6766-6785;runtime/chamber_runtime.py:6806-6816", v2='v2/ch12/thickness.py', test='v2/tests/test_thickness.py::test_listener_done_fallback_resume; v2/tests/test_thickness.py::test_snapshot_fields_keep_thickness_when_stale', status='done', note=''),
    dict(id="THK-071", area="thick",
         desc="상태 문구 '[시간 대체] 두께 x.x / T.T nm (p%) · 증착률 r.rrr nm/s · 예상 남은 시간 mm:ss' — 두께 판정 중 증착률 = 장비 값(>0) 아니면 평균 아니면 레시피, 대체 중엔 사용 증착률",
         v1="controller/thickness_process.py:81-85;controller/thickness_process.py:211-229", v2='v2/ch12/thickness_judge.py', test='v2/tests/test_thickness_judge.py::test_view_text; v2/tests/test_thickness.py::test_listener_done_fallback_resume', status='done', note=''),
    dict(id="THK-072", area="thick",
         desc="THICKNESS_WAIT 끝(어떻게 끝나든): 세대 +1, 판정기 내림, 이번 런 OES 측정 중이면 멈춤 요청(기다리지 않음, 'OES 측정 종료 요청 — 메인 공정 끝(Thickness 모드)')",
         v1="runtime/chamber_runtime.py:1623-1628;runtime/chamber_runtime.py:6832-6843", v2='v2/ch12/thickness.py', test='v2/tests/test_thickness.py::test_done_ends_thickness_and_stops_oes_once', status='done', note=''),
    dict(id="THK-073", area="thick",
         desc="종료 기록: 종료 방식(Thickness/시간대체/상한/중단), 최종 유효두께·평균 증착률·큰 튐 횟수·대체 사유. 상한 '⚠ 최대 시간 상한 도달 — 유효 x / 목표 y nm → 공정 실패(E848)', "
              "시간대체 경고 '시간 대체로 종료(사유) — 마지막 확인 …', 그 밖 '메인 공정 종료 — 종료 방식 X, …'(Thickness 인데 대체가 있었으면 경고 '공정 중 시간 대체 n회(사유) — 복귀 뒤 두께로 종료'). 패널 '종료(X) — x nm'",
         v1="controller/thickness_process.py:39-43;runtime/chamber_runtime.py:6845-6879", v2='v2/ch12/thickness.py; v2/ch12/thickness_record.py', test='v2/tests/test_thickness.py::test_limit_record_and_card; v2/tests/test_thickness.py::test_fallback_end_adds_run_warning; v2/tests/test_thickness.py::test_done_ends_thickness_and_stops_oes_once', status='done', note=''),

    # ───────────────────────── 공정 기록·카드·ETA ─────────────────────────
    dict(id="THK-074", area="thick",
         desc="공정 시작 때 이번 런 두께 기록 초기화(두께 모니터 붙은 채널만): 모드 Thickness/Time, 목표, 시작 전 값. 패널 Thickness '공정 시작 — 메인 공정 대기' / Time '—' 'Time 모드 공정(두께는 기록만)'",
         v1="runtime/chamber_runtime.py:1762;runtime/chamber_runtime.py:6744-6764", v2='v2/ch12/thickness_record.py', test='v2/tests/test_thickness.py::test_time_mode_ignores_recipe_factor_columns_and_reject_texts; v2/tests/test_thickness.py::test_stop_before_main_records_abort', status='done', note=''),
    dict(id="THK-075", area="thick",
         desc="Time 모드 두께 기록: 메인 공정 폴링 중·공정 실행 중·stale 아님일 때 새 두께 값의 처음·마지막(값, 시각) → 최종 = 마지막−처음, 평균 = 최종 ÷ 시간. 값이 없으면 종료 방식 Time 만",
         v1="runtime/chamber_runtime.py:6787-6804;runtime/chamber_runtime.py:6893-6901", v2='v2/ch12/thickness_record.py', test='v2/tests/test_thickness.py::test_time_mode_record_uses_main_window_values', status='done', note=''),
    dict(id="THK-076", area="thick",
         desc="Thickness 모드인데 메인 공정(THICKNESS_WAIT) 전에 끝나면(Shutter Delay·영점 중 정지, IG 타임아웃 등) 종료 방식 '중단', 최종·평균 없음 — 카드·Calib CSV 같은 값, 패널 '종료(중단) — 메인 공정 전'",
         v1="runtime/chamber_runtime.py:6886-6892;runtime/chamber_runtime.py:6912-6915", v2='v2/ch12/thickness_record.py', test='v2/tests/test_thickness.py::test_stop_before_main_records_abort', status='done', note=''),
    dict(id="THK-077", area="thick",
         desc="공정 끝 두께 요약 로그 '두께 요약 — 종료 방식 X · 최종 x.xx nm[ / 목표 T nm][ · 평균 r.rrrr nm/s][ · 큰 튐 n회(기록만)]'(최종 있을 때), 실패는 '⚠ 두께 기록 실패(무시)'",
         v1="runtime/chamber_runtime.py:1873;runtime/chamber_runtime.py:6903-6923", v2='v2/ch12/thickness_record.py', test='v2/tests/test_thickness.py::test_time_mode_record_uses_main_window_values', status='done', note=''),
    dict(id="THK-078", area="thick",
         desc="Calib CSV·엑셀 두께 열(SUPPORTS_THICKNESS=true 채널 파일만, 맨 뒤 13열): End Mode, Target nm(g), Final nm(.2f), Avg Rate nm/s(.4f), Tooling %(g), Density(g), Z(g), Chuck, Gun, "
              "Fallback Reason, Jump Count, Material, Factor Source — 기록 없으면 공란",
         v1="controller/data_logger.py:18-23;controller/data_logger.py:55;controller/data_logger.py:122-123;controller/data_logger.py:148-149;controller/data_logger.py:437-439;controller/data_logger.py:605-630;runtime/chamber_runtime.py:608",
         v2='v2/ch12/outputs/calib_csv.py; v2/ch12/outputs/excel_style.py', test='v2/tests/test_outputs_csv.py::test_calib_golden', status='done', note='골든 calib_ch2_full(두께 13열)'),
    dict(id="THK-079", area="thick",
         desc="공정 종료 카드 두께 필드(최종 있거나 Thickness 모드일 때): 종료 방식, 최종 두께 'x.x / T nm'|'x.x nm'(없으면 '--'), 큰 튐 'n회(기록만)'. 이번 런 경고 목록(영점·수명·대체)은 warnings",
         v1="runtime/chamber_runtime.py:1993-1998;runtime/chamber_runtime.py:6932-6943;controller/chat_notifier.py:594-599",
         v2='v2/ch12/thickness_record.py; vanam-process lib/ops2/ch12/chatCards.ts', test='v2/tests/test_thickness.py::test_limit_record_and_card; v2/tests/test_thickness.py::test_stop_before_main_records_abort; vanam-process/lib/ops2/ch12/chat-golden/web_ch12_chat.json::ch2_thickness_full_detail', status='done', note='카드는 v2 에서 포털이 보냄 / 카드는 포털'),
    dict(id="THK-080", area="thick",
         desc="공정 시작 카드(Thickness 모드): Process Time 칸 '목표 두께 T nm (예상 x.x분, 상한 y.y분)', '종료 방식' Thickness, 레시피 Factor 값을 썼으면 'Factor' 레시피 지정(…)",
         v1="controller/chat_notifier.py:496-505", v2='vanam-process lib/ops2/ch12/chatCards.ts; v2/ch12/thickness.py', test='vanam-process/lib/ops2/ch12/chat-golden/web_ch12_chat.json::start_thickness_mode; v2/tests/test_thickness.py::test_apply_material_factor_and_errors', status='done', note='장비는 params(thickness_mode·expected·limit·factor_note), 카드는 포털'),
    dict(id="THK-081", area="thick",
         desc="ETA: Thickness 모드 행의 최소 소요 = shutter_delay 만(메인은 더 빨리 끝날 수 있음), Time 행 = (shutter_delay + process_time(역산 포함))×60, delay 행 = 대기 초, 합 + ETA_TAIL_MIN_S. "
              "시퀀스에 THICKNESS_WAIT 가 있으면 남은 시간 확정 안 함('[ETA] Thickness 모드 — … remaining_s=null')",
         v1="controller/process_controller.py:144-154;controller/process_controller.py:193-198;controller/process_controller.py:1545;controller/process_controller.py:1736-1738;controller/process_controller.py:1789-1807",
         v2='v2/ch12/eta.py; v2/ch12/thickness.py', test='v2/tests/test_eta.py::test_1_thickness_recipe_matches_runtime_inversion; v2/tests/test_eta.py::test_no_confirm_with_thickness_wait_or_disabled; v2/tests/test_eta.py::test_thickness_rows_count_shutter_only_when_supported; v2/tests/test_thickness.py::test_limit_timer_ends_without_listener_e848', status='done', note=''),
    dict(id="THK-082", area="thick",
         desc="오류 코드: E840 두께 모니터 응답 없음, E841 XTAL FAIL, E842 Thickness 모드 레시피 값 오류, E843 건 선택 오류, E844 중간 펄스 전력 변경 충돌, E845 Factor 미설정, "
              "E846 재료값 적용 실패, E847 두께 대기 단계 구성 오류, E848 두께 목표 미도달 — 최대 시간 초과(category THICKNESS, SEV_ERROR)",
         v1="errors/error_registry.py:1641-1730", v2='v2/ch12/error_codes.py; v2/ch12/errors.py', test='v2/tests/test_errors.py::test_registry_same_as_v1_golden', status='done', note=''),

    # ───────────────────────── 화면 명령(패널·Factor 창·재료 창) ─────────────────────────
    dict(id="THK-083", area="thick",
         desc="장비 쓰기 금지 사유(영점 버튼·Factor 창 [장비에 쓰기] 공용): 공정 중 → '공정 중', 미연결 → '두께 모니터 미연결', stale → '두께 모니터 응답 없음'",
         v1="ui/thickness_panel.py:37-47", v2='v2/ch12/thickness_factors.py', test='v2/tests/test_thickness_files.py::test_device_write_block_shared_by_zero_and_write', status='done', note=''),
    dict(id="THK-084", area="thick",
         desc="패널 [영점]: 금지 사유 있거나 실행 중이면 비활성. 공정 중이면 경고 '공정 중에는 영점을 할 수 없습니다.', 확인창 '두께 모니터의 두께·타이머를 0 으로 맞춥니다. 계속하시겠습니까?' → "
              "다시 금지 사유 확인('… — 영점을 보내지 않았습니다.') → zero('B', why='패널'), 실패면 경고",
         v1="ui/thickness_panel.py:259-262;ui/thickness_panel.py:318-357", v2='v2/ch12/thickness_ops.py; v2/ch12/commands.py; vanam-process components/ops2/ch12/Ch12Aux.tsx', test='v2/tests/test_thickness_ops.py::test_cmd_zero_rechecks_block; v2/tests/test_inv_dev.py::test_stm_zero_and_material_service_texts', status='done', note='확인창은 포털 명령 확인(Ch12Aux 두께 영점) — 문구는 포털'),
    dict(id="THK-085", area="thick",
         desc="패널 [Factor 설정]: 항상 활성, 창이 열려 있으면 앞으로, 아니면 Factor 창을 연다(저장 시 서비스 reload_factors, 서비스·공정 중 여부 전달)",
         v1="ui/thickness_panel.py:359-376", v2='v2/ch12/thickness_ops.py; v2/ch12/commands.py', test='v2/tests/test_thickness_ops.py::test_factor_and_material_save_reload; v2/tests/test_thickness_files.py::test_factor_window_dropdown_values_and_save', status='done', note='창 열기는 포털'),
    dict(id="THK-086", area="thick",
         desc="패널 상태 의미: 램프 연결(소켓)/XTAL(통신 안 되면 회색, '!' 면 XTAL FAIL)/응답(link_ok), 통신 정상+XTAL FAIL 이면 LCD 'XTAL FAIL' 표, 리셋 플래그 'n회', "
              "적용값 줄(재료 기호 · Density · Z · Tooling %), 빨간 오류 줄은 연결된 채 응답 없을 때만('응답 없음 N초 · 마지막 오류'), stale 이면 값 회색",
         v1="ui/thickness_panel.py:30-34;ui/thickness_panel.py:188-274", v2='vanam-process components/ops2/ch12/Ch12Aux.tsx; v2/ch12/drivers/stm100.py', test='v2/tests/test_stm100.py::test_tick_rows_carry_last_value_and_age; v2/tests/test_stm100.py::test_xtal_fail_model_and_recovery; v2/tests/test_web_schema.py::test_snapshot_definitions_match_hello_columns', status='done', note='그리기는 ui 영역 / 그리기는 포털(ch2.stm100 블록 카드 — connected·link_ok·crystal_ok·reset_seen·적용값·last_error)'),
    dict(id="THK-087", area="thick",
         desc="패널 진행 영역: 공정 없으면 '공정 없음'(막대 비움), 공정 중에는 '종료 방식 X'·'목표 …'·진행률(0~100, 없으면 비움)·상태 문구, 공정이 끝나면 다음 공정 전까지 마지막 결과 유지",
         v1="ui/thickness_panel.py:6-8;ui/thickness_panel.py:276-303;runtime/chamber_runtime.py:6599-6610", v2='vanam-process components/ops2/ch12/Ch12Aux.tsx; v2/ch12/thickness.py',
         test='v2/tests/test_thickness.py::test_snapshot_block_fields_match_engine; v2/tests/test_thickness.py::test_snapshot_fields_keep_thickness_when_stale', status='done', note='진행 영역 = ch2.thk 블록(mode·phase·진행률·문구), 그리기는 포털'),
    dict(id="THK-088", area="thick",
         desc="Factor 창 저장: tooling 9칸(빈 칸 = 미설정, 10.0~399) 과 건별 재료(목록에 있어야 함) 검사, 틀린 칸 빨갛게·'고칠 칸이 있어 저장하지 않았습니다', 맞으면 원자적 저장 → on_saved → 닫힘. "
              "저장은 장비에 쓰지 않음(공정 시작 때 씀)",
         v1="ui/thickness_factor_dialog.py:1-10;ui/thickness_factor_dialog.py:307-344", v2='v2/ch12/thickness_factors.py', test='v2/tests/test_thickness_files.py::test_factor_window_dropdown_values_and_save; v2/tests/test_thickness_files.py::test_write_values_rules', status='done', note=''),
    dict(id="THK-089", area="thick",
         desc="Factor 창 [장비에 쓰기](서비스 있을 때만 보임): 고른 chuck×건의 tooling 칸 + 그 건 재료 density·Z(창의 현재 값, 저장 무관) 검사 → 금지 사유(공정 중 포함) 확인 → "
              "set_material(why 'Factor 창 수동') → '장비에 적용됨 — 이름 · density · Z · tooling%' 또는 실패 알림. 창 닫으면 진행 중 요청 취소. 저장 파일은 안 건드림",
         v1="ui/thickness_factor_dialog.py:108-131;ui/thickness_factor_dialog.py:221-304", v2='v2/ch12/thickness_ops.py', test='v2/tests/test_thickness_ops.py::test_factor_write_uses_window_values_without_saving; v2/tests/test_thickness_files.py::test_write_values_rules', status='done', note=''),
    dict(id="THK-090", area="thick",
         desc="Factor 창 재료 고르기: 맨 위 '(선택 안 함)', 항목 '기호 — 이름', 입력 찾기(포함·대소문자 무시), 목록에 없는 저장 이름은 입력 칸에 그대로·'목록에 없음', 고른 재료 'ρ d · Z z' 표시. "
              "읽기 경고(최대 8줄 + '… 외 n건', 목록에 없는 재료 포함)",
         v1="ui/thickness_factor_dialog.py:57-106;ui/thickness_factor_dialog.py:146-204", v2='v2/ch12/thickness_factors.py', test='v2/tests/test_thickness_files.py::test_factor_window_dropdown_values_and_save; v2/tests/test_thickness_files.py::test_factor_window_warns_material_not_in_list', status='done', note='그리기는 포털'),
    dict(id="THK-091", area="thick",
         desc="재료 목록 창: 열 이름·기호·density·Z-factor·note, 행 추가/삭제, 저장 때 범위(density 0.500~99.99, Z 0.100~9.999)·이름 빈칸·이름 중복(대소문자 무시) 검사(빨간 칸, 완전히 빈 행 무시), "
              "Factor 에서 쓰는 재료를 지우면 확인창('… 지우면 그 건은 '재료 목록에 없음'이 되어 Thickness 모드 시작이 거부됩니다(E845)'), 저장 뒤 Factor 창 드롭다운 갱신 + 서비스 다시 읽기",
         v1="ui/thickness_material_dialog.py:1-163;ui/thickness_factor_dialog.py:206-219", v2='v2/ch12/thickness_materials.py', test='v2/tests/test_thickness_files.py::test_material_window_validate_delete_confirm_and_save; v2/tests/test_thickness_ops.py::test_factor_and_material_save_reload', status='done', note='D62'),

    # ───────────────────────── v1 안에서 쓰이지 않는 것 ─────────────────────────
    dict(id="THK-092", area="thick",
         desc="드라이버 read_reset_status('a')·read_version('@')",
         v1="device/thickness_monitor.py:529-533", v2="v2/ch12/drivers/stm100.py", test="", status="na",
         note="v1 안에서 호출하는 곳 없음(grep — 정의만)"),
    dict(id="THK-093", area="thick",
         desc="materials_by_name(이름→재료 사전), ThicknessService.remove_listener",
         v1="lib/thickness_materials.py:206-207;controller/thickness_service.py:150-152", v2="v2/ch12/thickness.py", test="", status="na",
         note="v1 안에서 호출하는 곳 없음(grep — 정의만)"),
]
