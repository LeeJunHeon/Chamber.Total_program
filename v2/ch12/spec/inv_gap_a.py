"""v1 전수 목록 — 빠진 항목 찾기 A(gap).

방법: 전수 목록(inv_*.py)을 보지 않고 v1 코드만 읽어 관찰 가능한 동작 872개를 독립적으로 적은 뒤
(device/ 전부, lib/ 전부, config/*.json, errors/ 전부 — 기준 커밋 66efc7b),
all_items() 2,359개와 대조해 같은 v1 줄 근처·같은 의미의 항목이 없는 동작만 여기에 둔다.
장비 명령·주기·타임아웃·재시도·판정·오류 코드·설정 키는 이미 DEV·CFG·ERR·HEAT·THK·LOG 영역에 빠짐없이 있었다.
여기 남은 것은 주로 장비 드라이버가 내는 상태·실패 문구(공정 로그에 그대로 찍힘)와 잘 안 타는 분기다.
"""
from __future__ import annotations

ITEMS = [
    dict(id="GAPA-001", area="gap",
         desc="MFC 채널 안정화(FLOW_ON 확인 대기) 중단 시 그 작업 주체에게 보내는 command_failed(cmd FLOW_ON, owner·channel) 문구: "
              "다른 주체가 같은 채널을 다시 켜면 'ch{n} 안정화 중단 — 다른 주체({새 owner 또는 -})가 같은 채널을 다시 켬', "
              "그 밖의 취소(notify=True)는 'ch{n} 안정화 중단 ({사유})'. 사유 문자열: '같은 채널 FLOW_ON 재요청'(이전 주체가 다를 때만 통지), "
              "'FLOW_OFF 요청'(이전 주체가 다를 때만 통지), 'MFC 연결 종료'(cleanup), '정리({label})로 안정화 중단'(on_process_finished/cleanup), "
              "'{owner} 사용 종료'(release_owner — 통지 없음). 마지막 작업이 빠지면 안정화 감시 태스크도 취소",
         v1="device/mfc.py:1594-1626; device/mfc.py:453-455; device/mfc.py:491-494; device/mfc.py:318; device/mfc.py:1176; device/mfc.py:1199-1202",
         v2='v2/ch12/drivers/mfc_ops.py', test='v2/tests/test_inv_dev.py::test_mfc_stab_cancel_messages_by_owner; v2/tests/test_mfc.py::test_process_finished_purges_and_cancels_stab', status='done', note=''),
    dict(id="GAPA-002", area="gap",
         desc="MFC flow_set_on(선택 채널 FLOW_SET → FLOW_ON, 플라즈마 클리닝이 사용): FLOW_SET 이 실패(False)하면 FLOW_ON 명령을 보내지 않고 "
              "command_failed(cmd 'FLOW_ON', reason 'ch{n} FLOW_SET 실패로 FLOW_ON 생략', owner·channel)를 낸다 — 기다리는 쪽은 FLOW_ON 실패로 끝남",
         v1="device/mfc.py:541-547",
         v2='v2/ch12/drivers/mfc_ops.py; v2/ch12/drivers/mfc.py', test='v2/tests/test_inv_dev.py::test_mfc_gas_select_flow_set_on_and_release_owner_texts', status='done', note=''),
    dict(id="GAPA-003", area="gap",
         desc="MFC 가스 선택: gas_select(idx) 는 1~3(1 Ar·2 O2·3 N2) 밖이면 ValueError '지원하지 않는 가스 채널: {idx}', "
              "선택 시 status '가스 선택: ch{n} ({Ar|O2|N2})' + 선택 채널·소유자 기록. 선택 없이 flow_set_on/flow_off_selected 를 부르면 "
              "RuntimeError '선택된 가스가 없습니다. gas_select()를 먼저 호출하세요.'",
         v1="device/mfc.py:526-552",
         v2='v2/ch12/drivers/mfc.py', test='v2/tests/test_inv_dev.py::test_mfc_gas_select_flow_set_on_and_release_owner_texts', status='done', note=''),
    dict(id="GAPA-004", area="gap",
         desc="공유 MFC release_owner(owner, reason) 의 status 이벤트(owner 지정) 문구: "
              "'[{owner}] 사용 종료 — 다른 사용자가 있어 대기열 유지, 이 주체 몫만 정리 (채널 {[채널목록] 또는 -}, 안정화 취소 {n}건{, 사유})'. "
              "owner 가 비면 아무것도 안 함, 선택 채널은 그 owner 가 고른 경우에만 해제",
         v1="device/mfc.py:1191-1216",
         v2='v2/ch12/drivers/mfc_ops.py', test='v2/tests/test_inv_dev.py::test_mfc_gas_select_flow_set_on_and_release_owner_texts; v2/tests/test_mfc.py::test_release_owner_keeps_queue_and_other_channels', status='done', note=''),
    dict(id="GAPA-005", area="gap",
         desc="MFC WAIT_PRESSURE 진행 status 문구(공정 로그에 찍힘): 시작 '[PRESSURE] 목표압 {t:.3g} (tol_abs={abs}, tol_rel={rel×100:.1f}%) 도달까지 대기 시작', "
              "매 읽기 '[PRESSURE] OK ({k}/{N}) target=, current=' / '[PRESSURE] 아직 목표 미달: target=, current=', 도달 '[PRESSURE] 목표압 도달 및 안정: …', "
              "읽기 연속 실패 '[PRESSURE] READ_PRESSURE 연속 {n}회 실패 → 통신불가로 중단', 시간 초과 '[PRESSURE] 타임아웃({s:.1f}s) - target=, last=', "
              "target ≤ 0 '[PRESSURE] target <= 0 이라서 압력 대기를 건너뜁니다. target=…'(결과는 실패). use_sp_target 경로: "
              "'[WAIT_PRESSURE] SP{n} setpoint 읽기 실패 (재시도 i/5)', '[WAIT_PRESSURE] SP{n} setpoint {v:.3g} 기준으로 압력 도달 대기', "
              "sp_index 가 1~4 아니면 '[WAIT_PRESSURE] 잘못된 sp_index=… → UI target 사용'. SP 되읽기 '[READ_SP{n}] 현재 setpoint (UI) = x.xxx', "
              "'[READ_SP{n}] 응답 없음', '[READ_SP{n}] 파싱 실패: …', 명령 미정의 '[READ_SP] '…' 명령이 MFC_COMMANDS에 정의되어 있지 않음', "
              "잘못된 index '[READ_SP] 지원하지 않는 SP index: n'",
         v1="device/mfc.py:750-802; device/mfc.py:894-982; device/mfc.py:1076-1118",
         v2='v2/ch12/drivers/mfc_ops.py', test='v2/tests/test_inv_dev.py::test_mfc_wait_pressure_texts_sp_target_and_bad_index; v2/tests/test_mfc.py::test_garbage_reply_wait_pressure_fail_streak', status='done', note='SP 명령 미정의·잘못된 SP index 문구는 v2 에서 SP 1~4 만 받는 경로라 생기지 않음(MFC_COMMANDS 고정)'),
    dict(id="GAPA-006", area="gap",
         desc="MFC 명령 READ_FLOW_ALL(별칭 READ_FLOW) 단발 읽기: R60 응답을 못 읽거나 'Q0' 파싱이 실패하면 command_failed(cmd 'READ_FLOW', reason 'R60 파싱 실패'); "
              "성공이면 채널별 flow 이벤트(sccm) + 유량 이탈 감시(폴링과 같은 판정)",
         v1="device/mfc.py:864-876; device/mfc.py:1136-1137",
         v2='v2/ch12/drivers/mfc_ops.py', test='v2/tests/test_inv_dev.py::test_mfc_read_pressure_no_reply_and_read_flow_parse_fail', status='done', note=''),
    dict(id="GAPA-007", area="gap",
         desc="IG 명령 워커·수신 status 문구: 전송마다 '[SEND] {cmd} (tag={tag})', 전송 실패 '[SEND-ERROR] {tag} {cmd} 전송 오류: {e!r}'(재시도 남으면 재큐, 아니면 실패 콜백 → TCP 끊기), "
              "줄 큐(1024) 포화 시 가장 오래된 줄을 버리고 '라인 큐 포화 → 가장 오래된 라인 폐기', 수신 루프 예외 '리더 루프 예외: {e!r}', 끊김 'IG TCP 연결 끊김' — "
              "앞의 두 문구는 연결을 원하는 중(want_connected)이고 cleanup 중이 아닐 때만 낸다",
         v1="device/ig.py:700-724; device/ig.py:611-614; device/ig.py:660-661; device/ig.py:673-682",
         v2='v2/ch12/drivers/ig.py', test='v2/tests/test_inv_dev.py::test_ig_endpoint_backoff_and_link_texts; v2/tests/test_inv_dev.py::test_ig_io_texts_echo_timeout_and_single_shot_api', status='done', note="'[SEND]'·'[SEND-ERROR]'·'IG TCP 연결 끊김' 이번에 맞춤(io 줄도 공정 로그로 — process_events·pc_events). 줄 큐 포화·리더 루프 예외는 v2 동기 읽기에 없음"),
    dict(id="GAPA-008", area="gap",
         desc="IG 문자열 명령 라우터 command()/apply_command() 의 오류 처리: 모르는 명령은 status '[command] 지원하지 않는 IG 명령: {cmd!r}', "
              "실행 중 예외(인자 누락·ensure_on 실패 등)는 status '[command] 예외: {e!r}' 로만 알리고 예외를 올리지 않음(base_failed 아님). "
              "READ_PRESSURE 는 결과를 pressure 이벤트로, WAIT_FOR_BASE 의 interval_ms 기본은 IG_POLLING_INTERVAL_MS, SET_ENDPOINT 의 reconnect 기본 True",
         v1="device/ig.py:1329-1371",
         v2='v2/ch12/drivers/ig_ops.py', test='v2/tests/test_inv_dev.py::test_ig_process_status_off_purges_and_route_errors', status='done', note='v1 라우터 자체는 호출처 없음(DEV-118)'),
    dict(id="GAPA-009", area="gap",
         desc="RF 연속 전원(RFPowerAsync) 시작 문구·콜백 미주입 분기: toggle_enable 이 없으면 'RF SET ON 생략(toggle_enable 미주입)' 후 그대로 진행(SET 래치 없이), "
              "request_status_read 가 없으면 '상태읽기 콜백 없음 → 측정 없이 진행'(direct·일반 모두, 폴링 태스크 없음). direct 모드 시작 문구 "
              "'Kick+Ramp 시작: {kick}W kick → {목표}W까지 ↑{step}W/s, ↓{down}W/s' → 'Kick {kick}W 전송 완료 — ramp 시작 대기', 또는 "
              "'Direct set: {목표} W' → 'Direct set {목표}W 전송 — 도달 판정 대기'; 전송 예외면 'Kick 전송 실패: …'/'Direct set 실패: …' 후 시작 중단. "
              "direct 정지 문구 'Kick+Ramp shutdown: {step}W/s 하강 → {cut}W 도달 시 즉시 OFF', 직접 런 즉시 OFF 오류 'RF 즉시 OFF 오류: …', 일반 정지 첫 줄 '정지 신호 수신됨.'",
         v1="device/rf_power.py:298-356; device/rf_power.py:378-379; device/rf_power.py:403-450",
         v2='v2/ch12/drivers/rf.py', test='v2/tests/test_inv_dev.py::test_rf_pc_direct_set_and_kick_texts; v2/tests/test_rf.py::test_pc_direct_mode_kick_and_shutdown', status='done', note="v2 는 PLC 결선이 늘 있어 'toggle_enable 미주입'·'상태읽기 콜백 없음' 분기가 없음(v1 런타임도 늘 주입 — runtime/chamber_runtime.py:776-780·runtime/plasma_cleaning_runtime.py:1075-1079). kick 전송 실패는 v1 _send_rf_power 가 삼켜 이어감 — 같게"),
    dict(id="GAPA-010", area="gap",
         desc="DC Pulse 폴링 루프의 예외 처리: 한 주기 안에서 예외가 나면 status '[poll] 예외: {e!r}' 를 내고 폴링을 계속한다(중단·AUTO_STOP 없음). "
              "다음 PIV 까지 대기 = max(0.05 s, 마지막 PIV + 유효 주기 − 지금)",
         v1="device/dc_pulse.py:2995-3004",
         v2='v2/ch12/drivers/dcpulse.py; v2/common/worker.py', test='v2/tests/test_dcpulse.py::test_disconnect_and_reconnect_during_polling; v2/tests/test_dcpulse.py::test_garbage_reply_logged_as_chkfail', status='done', note="v2 는 tick 읽기 — 한 tick 이 실패해도 다음 tick 계속(중단·AUTO_STOP 없음), 문구는 worker 이벤트·'[CHKFAIL]'(‘[poll] 예외’ 문구 없음 — 결정 후보)"),
    dict(id="GAPA-011", area="gap",
         desc="OES one-shot 측정 중 예상 못 한 예외: finished 이벤트(success False, message 'OES 예외', out_csv=로컬 CSV 경로(없으면 None), "
              "rows=그때까지 받은 행 수, elapsed_s, error '{Type}: {msg}') + status '[OES] 예외: {Type}: {msg}\\n{traceback}', 이후 tail 정지·프로세스 정리·is_running False",
         v1="device/oes.py:1085-1094",
         v2='v2/ch12/drivers/oes_oneshot.py', test='v2/tests/test_inv_dev.py::test_oes_oneshot_unexpected_exception_result_and_traceback', status='done', note='traceback 줄 이번에 맞춤'),
    dict(id="GAPA-012", area="gap",
         desc="OES daemon 측정 폴백 status 문구: STOP 요청 때문에 실패했으면 '[OES] stop 요청으로 종료됨 → fallback 재기동 skip ({Type})', "
              "아니면 '[OES] daemon 실패 → 재기동 후 재시도: {Type}: {e}', 재시도도 실패면 '[OES] daemon 재시도 실패 → one-shot fallback: {Type}: {e}'. "
              "daemon 측정 시작 '[OES] daemon measure start: {분:.1f}분, out={csv}', 시작 알림 '[OES] daemon started out_csv= cols= usb=', "
              "daemon 기동 '[OES] daemon spawn ch= usb= parent_timeout= worker_timeout= cmd= cwd=', READY '[OES] daemon READY: {info}', exe 없음 '[OES] daemon 워커 exe 없음: {경로}'",
         v1="device/oes.py:758-782; device/oes.py:460-462; device/oes.py:486-490; device/oes.py:523; device/oes.py:626-628; device/oes.py:705",
         v2='v2/ch12/drivers/oes_link.py', test='v2/tests/test_inv_dev.py::test_oes_daemon_status_texts; v2/tests/test_oes_link.py::test_daemon_death_restart_then_oneshot_fallback', status='done', note=''),
    dict(id="GAPA-013", area="gap",
         desc="CH2 히터 CSV 경고 문구(로거 인스턴스당 1회만, 공정을 막지 않음): 지정 폴더를 못 만들어 폴백에 쓰면 '히터 CSV 폴더({기본})를 만들 수 없어 {폴백} 에 저장합니다', "
              "둘 다 실패 '히터 CSV 폴더를 만들 수 없어 기록하지 않습니다', 파일 열기 실패 '히터 CSV 열기 실패: {e!r}', 행 쓰기 실패 '히터 CSV 기록 실패(이후 경고 생략): {e!r}'",
         v1="lib/heater_logger.py:57-104; lib/heater_logger.py:139-140",
         v2='v2/ch12/outputs/heater_csv.py', test='v2/tests/test_inv_gap_a.py::test_heater_csv_folder_fallback_and_none_warn_once; v2/tests/test_inv_gap_a.py::test_heater_csv_open_and_write_failures', status='done', note=''),
    dict(id="GAPA-014", area="gap",
         desc="두께 연속 CSV 경고 문구(로거당 1회만): 지정·폴백 폴더 모두 실패 '두께 기록 CSV 를 열 수 없어 기록하지 않습니다(60초 뒤 재시도): {e!r}', "
              "폴백 사용 '두께 기록 폴더({기본})를 쓸 수 없어 {폴백} 에 저장합니다', 행 쓰기 실패 '두께 기록 CSV 쓰기 실패(60초 뒤 다시 엶): {e!r}'(파일 닫고 60 s 뒤 재오픈)",
         v1="lib/thickness_logger.py:63-124",
         v2='v2/ch12/outputs/thickness_csv.py', test='v2/tests/test_inv_gap_a.py::test_thickness_csv_open_and_write_failures_retry_after_60s; v2/tests/test_outputs_csv.py::test_thickness_gdrive_dir_replaced_and_fallback', status='done', note=''),
    dict(id="GAPA-015", area="gap",
         desc="두께 재료 목록 경고·오류 문구: 파일 생성 실패 '재료 목록 파일을 만들 수 없음({경로}) — 기본 목록 사용: {e!r}', 읽기 실패 '재료 목록 파일을 읽을 수 없음({파일명}) — 기본 목록 사용: {e!r}'"
              "('materials 목록이 없음' 포함), 항목 '{i}번째 항목이 표(객체)가 아님 — 제외', \"{i}번째 항목 '{name}': {오류, …} — 제외\"(오류: '이름이 비어 있음', "
              "'{density|z_factor} {v!r} 숫자가 아님', '{key} {f:g} 범위(lo~hi) 밖'), \"{i}번째 항목 '{name}': 이름 중복 — 제외\". 저장 시 이름 중복이면 ValueError '재료 이름 중복: a, b'",
         v1="lib/thickness_materials.py:95-116; lib/thickness_materials.py:131-135; lib/thickness_materials.py:156-193",
         v2='v2/ch12/thickness_materials.py', test='v2/tests/test_inv_gap_a.py::test_material_list_warnings_and_duplicate_save; v2/tests/test_thickness_files.py::test_load_materials_same_as_v1', status='done', note=''),
    dict(id="GAPA-016", area="gap",
         desc="두께 Factor 파일 경고 문구: 'Factor 파일을 읽을 수 없음({파일명}) — 빈 표로 시작: {e!r}', 'Factor 파일 형식 오류({파일명}) — 빈 표로 시작', "
              "'tooling 이 표(객체)가 아님 — 전부 미설정', 'tooling.{chuck} 가 표(객체)가 아님 — 그 줄 미설정', "
              "'tooling[{chuck}][{gun}] {v!r} 숫자가 아님 — 미설정' / 'tooling[{chuck}][{gun}] {f:g} 범위(10~399%) 밖 — 미설정', "
              "'material 이 표(객체)가 아님 — 전부 미선택', 'material.{gun} 가 표(객체)가 아님 — 미선택'",
         v1="lib/thickness_factors.py:56-115",
         v2='v2/ch12/thickness_factors.py', test='v2/tests/test_inv_gap_a.py::test_factor_file_warnings; v2/tests/test_thickness_files.py::test_load_factors_same_as_v1', status='done', note=''),
]
