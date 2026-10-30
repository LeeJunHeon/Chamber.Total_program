"""v1 전수 목록 — 영역 heat(CH2 히터: VARITAP VSCD-30 + PLC 내장 PID).

덮는 범위
  CH2 히터의 PLC 주소·디코드·쓰기 허용 목록, 폴링·워치독(D114) 하트비트·RUN/RST 코일, 설정 push(D113~D145)와 되읽기,
  세션 마커(D160)·잔존 RUN 정리·FAULT/RST/출력 죽음, 파이썬 램프(감속 접근), 유지 모드(dac), DAC 포화 감시·승온 정체,
  출력 무응답·끊김 감시(EL-092), 공정 연동(HEATER_SET/WAIT/HOLD, E830~E833, 승온 대기 한도·SOAK), 레시피 열·값 검사,
  공정 종료 후 히터 처리(큐 유지·끄기·종료 사유), 구글챗 카드(시작·종료·도달·이상·포화·정체·무응답·재기동 등),
  기록(CH2_HEATER_*.csv, 공정 로그 상태 줄, Calib CSV·엑셀 히터 열·요약 줄), 설정 키 검증·클램프,
  패널 명령의 서비스 쪽 의미(owner·사유), 래더 동작 모델(tests/heater_ladder_sim.py — 워치독 30초 등).
  화면 그리기(ui/heater_panel.py 의 표시)는 ui 영역이 덮고, 여기서는 패널이 서비스에 보내는 명령만 적는다.

읽은 v1 파일(기준 커밋 66efc7b 이후 rewrite/v2 작업 트리의 v1 파일)
  controller/heater_service.py, controller/heater_ramp.py, controller/heater_hold.py, controller/heater_saturation.py,
  controller/heater_response.py, controller/heater_process.py,
  lib/heater_config.py, lib/heater_profile.py, lib/heater_logger.py,
  runtime/chamber_runtime.py(heater 검색 185곳 전부), device/plc.py(heater 가 쓰는 read_regs_block·read_coils_block·write_reg·write_coil),
  controller/process_controller.py(HEATER_* 스텝·토큰), controller/data_logger.py(히터 열), main.py(_init_ch2_heater·종료·닫기 확인),
  ui/heater_panel.py(명령 부분 330~406), errors/error_registry.py(E830~E833), config/settings.json(ch2 HEATER_*),
  lib/config_common.py(HEATER_* 기본값), tests/heater_ladder_sim.py, tests/data/thickness_time_mode_golden.json(ch2_thk_heater_rf).
"""
from __future__ import annotations

ITEMS = [
    # ───────────────────────── 주소·디코드·PLC I/O ─────────────────────────
    dict(id="HEAT-001", area="heat",
         desc="상태 읽기 = 레지스터 블록 D110~D160(51워드, FC3) + 코일 블록 M640~M658(19비트, FC1), 둘 다 priority='low'. "
              "응답 길이가 모자라면 ValueError('응답 길이 부족') 로 실패 처리",
         v1="controller/heater_service.py:37-38;controller/heater_service.py:47-48;controller/heater_service.py:761-767",
         v2="v2/ch12/drivers/heater.py; v2/ch12/drivers/plc.py", test="v2/tests/test_heater_engine.py::test_write_addresses_whitelist_and_low_priority_reads; v2/tests/test_heater_driver.py::test_decode_block_v1_rules; v2/tests/test_plc.py::test_short_block_response_padded_like_v1", status="done", note="읽기는 PLC 1초 묶음(FC3 D110..D160·FC1 640..658 low) — 값이 없으면 해독하지 않음('응답 길이 부족' 자리)"),
    dict(id="HEAT-002", area="heat",
         desc="레지스터 주소: PV D110(s16), SV D112, SV상한 D113, 워치독 D114, CUR_SV D116(s16), PID_ERR D117, DAC상한 D118, "
              "램프중간목표 D119(s16), 램프속도 D120(카운트/초), 홀드백 D121, OT D126, 감속구간 D127, 감속속도 D128, MV D141, "
              "Kp D142(하위)/D143(상위), Ti D144/D145, 적용 Kp D146/D147, 적용 Ti D148/D149",
         v1="controller/heater_service.py:39-45", v2="v2/ch12/drivers/heater.py", test="v2/tests/test_heater_driver.py::test_decode_block_v1_rules", status="done", note=""),
    dict(id="HEAT-003", area="heat",
         desc="코일 주소: RUN 640, ITL 641, FAULT 642, RST 643, OT 644, TC_ERR 645, WD_ERR 646, PID_RUN 649, DAC_EN 652, "
              "TC_OK 653, DA_OK 656, WATER_OK 657(M00411 ← WATER_2_SGN), VAC_OK 658(M00412 ← M_V_2_인터락)",
         v1="controller/heater_service.py:47-52", v2="v2/ch12/drivers/heater.py", test="v2/tests/test_heater_driver.py::test_decode_block_v1_rules; v2/tests/test_heater_sim.py::test_interlock_loss_stops_run_with_cause", status="done", note=""),
    dict(id="HEAT-004", area="heat",
         desc="쓰기 허용 목록 — 레지스터 {D112,D113,D114,D118,D120,D121,D126,D127,D128,D142~D145}, 코일 {640 RUN, 643 RST}. "
              "밖이면 ValueError('히터 쓰기 허용 밖 …') 로 쓰지 않음",
         v1="controller/heater_service.py:54-58;controller/heater_service.py:475-486", v2="v2/ch12/drivers/heater.py",
         test="v2/tests/test_heater_engine.py::test_driver_rejects_outside_whitelist; v2/tests/test_heater_engine.py::test_write_addresses_whitelist_and_low_priority_reads; v2/tests/test_heater_driver.py::test_write_seq_excludes_watchdog_and_marker", status="done", note=""),
    dict(id="HEAT-005", area="heat",
         desc="모든 히터 PLC 호출(읽기·쓰기)은 asyncio.wait_for 6.0초(IO_TIMEOUT_S) 상한. 쓰기는 write_reg(FC6 write_register)·"
              "write_coil(FC5) 기본 priority(high) 경로",
         v1="controller/heater_service.py:60;controller/heater_service.py:480;controller/heater_service.py:486;device/plc.py:1380-1414",
         v2="v2/ch12/drivers/heater.py; v2/ch12/drivers/plc_io.py", test="v2/tests/test_plc.py::test_timeout_policy_matches_v1", status="done", note="D72 — v1 wait_for 6 s 상한 대신 PLC 원시 연산 정책(응답 2 s·재시도 1회)과 요청 실행 상한"),
    dict(id="HEAT-006", area="heat",
         desc="히터 주소는 PLC_REG_MAP·PLC_COIL_MAP 에 넣지 않는다(넣으면 snapshot_regs_fast 범위가 FC3 한도 125 를 넘고 PLC 코일 CSV 열이 바뀜) "
              "— 코일 CSV·스냅샷 레지스터 범위에 히터 주소가 없어야 한다",
         v1="controller/heater_service.py:5-8", v2="v2/ch12/drivers/heater.py; v2/ch12/drivers/plc_map.py", test="v2/tests/test_inv_heat.py::test_heat006_heater_addresses_not_in_plc_maps", status="done",
         note="v2 1초 스냅샷에서 히터 값은 별도 필드로(코일 CSV 열 불변)"),
    dict(id="HEAT-007", area="heat",
         desc="저우선(low) 블록 읽기는 끊긴 링크를 되살리지 않고 즉시 실패(접속·close·재시도 없음, 공통 타임아웃 카운터 불변) — "
              "단 프로세스 시작 뒤 한 번도 접속·끊김이 없던 상태의 최초 접속만 허용",
         v1="device/plc.py:1252-1275;device/plc.py:1416-1438;device/plc.py:1688-1709", v2="v2/ch12/drivers/plc.py; v2/ch12/drivers/plc_io.py",
         test="v2/tests/test_plc.py::test_timeout_policy_matches_v1", status="done", note=""),
    dict(id="HEAT-008", area="heat",
         desc="디코드: 온도 = raw × HEATER_TEMP_SCALE(0.1). PV raw = -1(0xFFFF) 이면 ok=False·pv=None(TC 이상). ramp_rate = D120×6, "
              "slow_rate = D128×6(°C/min), Kp/Ti 적용값은 REAL(하위 워드 = 앞 주소), 마커는 session_mark_reg 가 블록 안이면 그 값",
         v1="controller/heater_service.py:656-700;lib/heater_config.py:93-102", v2="v2/ch12/drivers/heater.py",
         test="v2/tests/test_heater_driver.py::test_decode_block_v1_rules", status="done", note=""),
    dict(id="HEAT-009", area="heat",
         desc="출력% mv_pct = (MV − MV_MIN)/(MV_ABS_MAX − MV_MIN)×100, 0~100 클램프, MV < MV_MIN 이면 0",
         v1="controller/heater_service.py:668-670", v2="v2/ch12/drivers/heater.py", test="v2/tests/test_heater_driver.py::test_decode_block_v1_rules", status="done", note="계산은 드라이버 decode_block"),
    dict(id="HEAT-010", area="heat",
         desc="PLC 응답이 요청 개수보다 짧으면 레지스터는 0, 코일은 False 로 채워 돌려준다(그래서 히터의 '응답 길이 부족' 검사는 실제 PLC 에서 걸리지 않음)",
         v1="device/plc.py:1436-1437;device/plc.py:1707-1708;controller/heater_service.py:766-767", v2="v2/ch12/drivers/plc.py; v2/ch12/drivers/plc_io.py",
         test="v2/tests/test_plc.py::test_short_block_response_padded_like_v1", status="done", note="알려진 이상 동작 — v1 과 같게 두고 결정 목록에 — v1 그대로(짧은 응답 0/False 채움)"),

    # ───────────────────────── 폴링·워치독·상태 ─────────────────────────
    dict(id="HEAT-011", area="heat",
         desc="폴링 주기: RUN·램프 활성·보류 OFF·(push 필요 & stale 아님 & 백오프 끝) 중 하나면 HEATER_POLL_INTERVAL_S(기본 1.0초), "
              "아니면 HEATER_POLL_IDLE_S(기본 3.0초). 처리 예외는 '⚠ 히터 폴링 처리 예외: …' 로그 후 계속",
         v1="controller/heater_service.py:710-725", v2="v2/ch12/heater_engine.py", test="v2/tests/test_heater_engine.py::test_poll_intervals; v2/tests/test_heater_engine.py::test_poll_cadence_on_rows", status="done",
         note="v2 1초 기록과 별개로 제어 주기 유지(plan 3-(2)) — D72 — 1초 묶음 값을 v1 주기(RUN 1 s·정지 3 s)대로 처리"),
    dict(id="HEAT-012", area="heat",
         desc="성공 폴링 처리 순서: (1)첫 폴링 잔존 RUN 정리 → (2)보류 OFF → (3)되읽기 확인·세션 마커/설정 push → HEATER 프로그램 유무 → "
              "(4)FAULT 엣지 → RST 고착 → 리셋 결과 → (5)출력 죽음 → 상태 확정 → (6)RUN 엣지 → (7)램프 tick → 포화 감시 → 유지 tick → "
              "D118 요청 실행 → 출력 무응답 감시 → (8)CSV 행 → 발행",
         v1="controller/heater_service.py:10-14;controller/heater_service.py:789-857", v2="v2/ch12/heater_engine.py; v2/ch12/heater_push.py; v2/ch12/heater_edges.py",
         test="v2/tests/test_heater_engine.py::test_residual_run_cleared_before_first_publish; v2/tests/test_heater_engine.py::test_off_deferred_while_link_down_on_rejected; v2/tests/test_heater_engine.py::test_fault_rise_sends_run_off_once; v2/tests/test_heater_engine.py::test_run_edges_csv_and_chat", status="done", note=""),
    dict(id="HEAT-013", area="heat",
         desc="워치독 하트비트: HEATER_WD_PERIOD_S(기본 5초)마다 RUN 이고 링크가 살아 있을 때만 D114 = (이전+1) & 0x7FFF. "
              "turn_on 성공 직후 즉시 1회. 쓰기 실패 로그는 60초에 1줄('워치독(D114) 쓰기 실패 — 다음 주기에 재시도')",
         v1="controller/heater_service.py:66;controller/heater_service.py:296;controller/heater_service.py:509-521;controller/heater_service.py:727-736",
         v2="v2/ch12/heater_engine.py; v2/ch12/drivers/heater.py", test="v2/tests/test_heater_engine.py::test_watchdog_only_while_run; v2/tests/test_heater_engine.py::test_turn_on_kicks_watchdog_once_and_sets_owner; v2/tests/test_heater_driver.py::test_write_seq_excludes_watchdog_and_marker", status="done", note="워치독 쓰기는 write_seq 를 올리지 않음(478)"),
    dict(id="HEAT-014", area="heat",
         desc="쓰기 경합 방지: 상태를 바꾸는 쓰기마다 write_seq+1(워치독 제외). 읽기 시작 뒤 쓰기가 끼었으면 그 폴링 값을 확정·발행하지 않음. "
              "읽기 실패 때 마지막 확정 상태가 그 뒤 쓰기 전 값이면 stale 전(age ≤ STALE)에는 다시 발행하지 않음(옛 RUN=0 으로 E830/E832 오판 방지)",
         v1="controller/heater_service.py:125-126;controller/heater_service.py:738-754;controller/heater_service.py:760-787",
         v2="v2/ch12/heater_engine.py; v2/ch12/drivers/heater.py", test="v2/tests/test_heater_engine.py::test_process_turn_on_during_poll_read_no_false_e832; v2/tests/test_heater_engine.py::test_panel_turn_off_during_poll_read_no_stale_run; v2/tests/test_heater_engine.py::test_watchdog_write_during_read_does_not_drop_poll; v2/tests/test_heater_engine.py::test_process_turn_on_then_one_read_fail_no_false_stop; v2/tests/test_heater_engine.py::test_panel_turn_off_then_read_fail_no_old_run", status="done", note=""),
    dict(id="HEAT-015", area="heat",
         desc="읽기 실패: 첫 실패에 '⚠ 히터 상태 읽기 실패: why' 1줄(첫 성공 전 'PLC 미연결'은 로그 없이 stale 만), 복구 시 "
              "'히터 상태 읽기 복구 (끊긴 N초)'. stale = 마지막 성공 뒤 HEATER_STALE_SEC(기본 10초) 초과 또는 성공 이력 없음, age_s 함께 발행",
         v1="controller/heater_service.py:738-758;controller/heater_service.py:773-776", v2="v2/ch12/heater_engine.py",
         test="v2/tests/test_heater_engine.py::test_quiet_before_first_success_then_logged; v2/tests/test_heater_engine.py::test_not_connected_no_io_and_stale; v2/tests/test_heater_engine.py::test_read_fail_without_write_still_republishes", status="done", note=""),
    dict(id="HEAT-016", area="heat",
         desc="상태 발행 키: ok,pv,sv,sv_limit,cur_sv,pid_err,mv,mv_pct,mv_limit,sv_ramp,ramp_rate,holdback,ot_limit,slow_zone,slow_rate,"
              "kp_act,ti_act,marker,run,itl,fault,ot,tc_err,wd_err,pid_run,dac_en,tc_ok,da_ok,water_ok,vac_ok,rst,output_dead,noresp,stale,age_s,t "
              "(+residual_run). '_' 로 시작하는 내부 키 제외, 리스너 예외는 '상태 리스너 예외' 로그",
         v1="controller/heater_service.py:178-183;controller/heater_service.py:646-654;controller/heater_service.py:702-708",
         v2="v2/ch12/heater_engine.py", test="v2/tests/test_inv_heat.py::test_heat016_published_keys", status="done", note=""),
    dict(id="HEAT-017", area="heat",
         desc="잔존 RUN 정리(첫 성공 폴링에서 RUN=1): RUN=0 → TC 정상이면 SV(D112)=PV raw. 실패하면 발행하지 않고 재시도(로그 10초 1회). "
              "성공 시 로그 '이전 세션 히터 RUN 잔존 → OFF, SV a°C → 현재 온도 b°C' + 카드 '잔존 RUN 정리' WARN(PV·이전 SV)",
         v1="controller/heater_service.py:67;controller/heater_service.py:792-794;controller/heater_service.py:859-885",
         v2="v2/ch12/heater_edges.py", test="v2/tests/test_heater_engine.py::test_residual_run_cleared_before_first_publish; v2/tests/test_heater_engine.py::test_residual_failure_not_published_then_retried", status="done", note="v2 재시작 이어가기(5단계)와 충돌 여부 결정 필요 — 이어가기(adopt_journal)일 때는 잔존 RUN 정리를 하지 않음(heater_link)"),
    dict(id="HEAT-018", area="heat",
         desc="세션 마커: 설정 push 성공 뒤 random 0x1000~0xFFFE 를 HEATER_SESSION_MARK_REG(D160)에 씀(허용 목록·write_seq 를 거치지 않는 직접 write_reg). "
              "폴링마다 비교해 2회 연속 불일치면 'PLC 재기동 감지': 로그·카드 'PLC 재기동 감지' WARN, 램프 정지, owner=None, 운전 중이면 종료 사유 'PLC 재기동', "
              "백오프 무시하고 즉시 재 push",
         v1="controller/heater_service.py:63;controller/heater_service.py:887-945", v2="v2/ch12/heater_push.py",
         test="v2/tests/test_heater_engine.py::test_marker_single_mismatch_ignored_double_repushes; v2/tests/test_heater_engine.py::test_marker_mismatch_run_fall_reason_restart; v2/tests/test_heater_sim.py::test_plc_restart_end_reason", status="done", note="마커 쓰기가 WRITE_REGS 밖 경로 — v1 과 같게 — D146 마커는 허용 목록 밖 직접 쓰기(v1 그대로)"),
    dict(id="HEAT-019", area="heat",
         desc="설정 push 순서: D113(SV상한) → D126(OT) → D118(DAC상한) → D120(램프 카운트) → D121(홀드백) → D127(감속구간) → D128(감속속도) → "
              "Kp(D143 상위 먼저 → D142) → Ti(D145 → D144). Kp/Ti 는 범위 밖(invalid)이면 생략, 적용값이 이미 같으면(상대 1e-5) 다시 쓰지 않음. "
              "성공 로그 '히터 설정 적용: SV상한 … · OT … · DAC상한 … · 램프 …°C/min · 홀드백 … · 감속 … · Kp … · Ti …s'",
         v1="controller/heater_service.py:956-1005", v2="v2/ch12/heater_push.py", test="v2/tests/test_heater_engine.py::test_push_order_values_high_word_first_and_marker; v2/tests/test_heater_engine.py::test_pid_not_rewritten_when_act_matches_and_invalid_not_written", status="done", note=""),
    dict(id="HEAT-020", area="heat",
         desc="push/마커 쓰기 실패: 첫 실패에서 멈추고 30초 백오프(PUSH_RETRY_SEC), 로그 60초에 1줄 '⚠ 히터 설정 쓰기 실패(n회째) — 30초 뒤 재시도: 항목(Dxxx)'. "
              "복구 시 '히터 설정 쓰기 복구 (n회 실패 뒤)'. HEATER_PUSH_CONFIG=false 면 설정 쓰기 없이 마커만",
         v1="controller/heater_service.py:68-69;controller/heater_service.py:892-896;controller/heater_service.py:922-954",
         v2="v2/ch12/heater_push.py", test="v2/tests/test_heater_engine.py::test_push_stops_on_first_failure_backoff_and_recovery; v2/tests/test_inv_heat.py::test_heat020_push_config_false_writes_marker_only", status="done", note=""),
    dict(id="HEAT-021", area="heat",
         desc="push 직후 다음 블록에서 되읽기 확인 — 레지스터 불일치면 '설정값이 되읽기와 다릅니다 (래더가 강제 중일 수 있음): Dxxx 요청 a → 실제 b' 로그·"
              "카드 '설정 되읽기 불일치' WARN 1회, PID 불일치(상대 1e-3)면 'PID 값이 적용되지 않음(…래더 기본값 사용 중)' 로그·카드. 운전 중이면 특이사항",
         v1="controller/heater_service.py:889-891;controller/heater_service.py:997-999;controller/heater_service.py:1007-1037",
         v2="v2/ch12/heater_push.py", test="v2/tests/test_heater_engine.py::test_readback_mismatch_warns_once", status="done", note=""),
    dict(id="HEAT-022", area="heat",
         desc="HEATER 프로그램 없음 감지: TC_OK·DA_OK 모두 꺼짐이고 KP_ACT 가 0/없음이면 '⚠ PLC 에 HEATER 프로그램이 없는 것 같음' 1회",
         v1="controller/heater_service.py:1039-1045", v2="v2/ch12/heater_push.py", test="v2/tests/test_inv_heat.py::test_heat022_no_heater_program_warned_once", status="done", note=""),
    dict(id="HEAT-023", area="heat",
         desc="히터 사용 가능(is_ready) 조건과 문구: PLC 링크 없음 / 'PLC 응답 없음 (N초 전 값)' / '히터 이상: …' / '인터락 미충족: 빠진 조건' / "
              "'히터 설정·세션 마커 적용 전(PLC 쓰기 대기/실패) — 잠시 후 다시 시도하십시오'",
         v1="controller/heater_service.py:233-245", v2="v2/ch12/heater_engine.py", test="v2/tests/test_heater_engine.py::test_turn_on_rejections; v2/tests/test_heater_engine.py::test_push_stops_on_first_failure_backoff_and_recovery; v2/tests/test_heater_sim.py::test_interlock_loss_stops_run_with_cause; v2/tests/test_heater_engine.py::test_off_deferred_while_link_down_on_rejected", status="done", note=""),
    dict(id="HEAT-024", area="heat",
         desc="인터락 빠진 조건 문구: TC · D/A · 냉각수 · 진공(메인밸브 인터락) 중 꺼진 것 + FAULT, 없으면 '원인 미상'(래더 H4 ITL = TC1_OK·DA_OK·WATER_OK·VAC_OK·¬FAULT)",
         v1="controller/heater_process.py:36-46", v2="v2/ch12/heater_process.py; v2/ch12/heater_engine.py", test="v2/tests/test_heater_sim.py::test_interlock_off_before_start_blocks_turn_on; v2/tests/test_heater_sim.py::test_interlock_loss_stops_run_with_cause", status="done", note=""),

    # ───────────────────────── 명령 ─────────────────────────
    dict(id="HEAT-025", area="heat",
         desc="turn_on(목표, 속도, owner): 잠금 → is_ready → 이미 RUN 이면 '이미 운전 중입니다 — 목표를 바꾸려면 [적용]을 쓰십시오' → 목표 범위 → "
              "owner 충돌 '제어 주체 충돌 — 현재 …' → 속도 기본 HEATER_RAMP_RATE_C_PER_MIN → 램프 시작(안 되면 SV 직접) → RUN=1. "
              "예외면 램프 정지(D120 원복)·'PLC 쓰기 실패: …'. 성공 로그 '운전 ON 요청 — 목표 x°C (주체)'",
         v1="controller/heater_service.py:266-297", v2="v2/ch12/heater_commands.py", test="v2/tests/test_heater_engine.py::test_turn_on_rejections; v2/tests/test_heater_engine.py::test_turn_on_kicks_watchdog_once_and_sets_owner; v2/tests/test_inv_heat.py::test_heat025_turn_on_while_running_rejected", status="done", note=""),
    dict(id="HEAT-026", area="heat",
         desc="목표 범위: 0 < 목표 ≤ min(HEATER_MAX_TEMP, D113 되읽기 SV상한(>0일 때)), 아니면 '목표 범위 밖 — 0 초과 x°C 이하' / '목표 온도가 숫자가 아닙니다'",
         v1="controller/heater_service.py:632-643", v2="v2/ch12/heater_commands.py", test="v2/tests/test_heater_engine.py::test_turn_on_rejections; v2/tests/test_heater_process.py::test_panel_apply_changes_process_target_v1_anomaly", status="done", note=""),
    dict(id="HEAT-027", area="heat",
         desc="공정 잠금: owner 가 주어지고 'process' 가 아니며 현재 주체가 공정이고 RUN 이면 turn_off·apply_target 거부 '공정이 히터를 제어 중입니다'. "
              "수동↔외부 사이는 막지 않음",
         v1="controller/heater_service.py:299-308;controller/heater_service.py:389-393", v2="v2/ch12/heater_commands.py",
         test="v2/tests/test_heater_process.py::test_disarm_releases_hold_and_run_off_restores; v2/tests/test_heater_process.py::test_panel_apply_changes_process_target_v1_anomaly", status="done", note=""),
    dict(id="HEAT-028", area="heat",
         desc="turn_off(사유): 종료 사유 기록 → 램프 활성이면 정지(D120 원복), 아니면 D120 이 설정값과 다르면 원복 → 링크 다운이면 OFF 보류 "
              "(True, '링크 다운 — OFF 보류(복구 시 적용)') → RUN=0 로그 '운전 OFF (사유)'. 쓰기 실패면 보류 (True, '쓰기 실패 — OFF 보류(다음 폴링에 재시도)')",
         v1="controller/heater_service.py:304-308;controller/heater_service.py:361-387", v2="v2/ch12/heater_commands.py",
         test="v2/tests/test_heater_engine.py::test_off_deferred_while_link_down_on_rejected; v2/tests/test_heater_units.py::test_ramp_stop_restores_ramp_rate", status="done", note=""),
    dict(id="HEAT-029", area="heat",
         desc="보류 OFF: 다음 성공 폴링에서 RUN=0 쓰기 '보류했던 히터 OFF 적용', 실패면 '⚠ 보류 OFF 적용 실패 — 다음 폴링에 재시도'",
         v1="controller/heater_service.py:795-804", v2="v2/ch12/heater_engine.py", test="v2/tests/test_heater_engine.py::test_off_deferred_while_link_down_on_rejected", status="done", note=""),
    dict(id="HEAT-030", area="heat",
         desc="apply_target(목표, 속도, owner): 링크·stale·범위 확인 → RUN 중이면 속도 기본(진행 중 램프 속도 또는 설정값), 램프 정지(D120 원복 없이) 후 "
              "새 램프(라벨 '공정'/'적용'), 램프가 안 되면 SV 직접. RUN 이 아니면 SV(D112)만 씀",
         v1="controller/heater_service.py:389-415", v2="v2/ch12/heater_commands.py", test="v2/tests/test_heater_process.py::test_take_over_manual_run_uses_apply_target; v2/tests/test_heater_engine.py::test_write_addresses_whitelist_and_low_priority_reads", status="done", note=""),
    dict(id="HEAT-031", area="heat",
         desc="reset_fault: RUN 이면 거부 '히터 RUN 이 켜져 있습니다 — 먼저 OFF 한 뒤 리셋하십시오', 링크 없으면 거부, RST(M643)=1 쓰기, "
              "로그 '사용자가 히터 이상 리셋을 요청했습니다(RST)'. 다음 폴링에도 FAULT 면 '⚠ 리셋 후에도 히터 이상 유지 — …: 원인이 남아 있습니다'",
         v1="controller/heater_service.py:417-431;controller/heater_service.py:811-814", v2="v2/ch12/heater_commands.py",
         test="v2/tests/test_heater_engine.py::test_reset_refused_while_run; v2/tests/test_heater_engine.py::test_reset_with_cause_remaining_logged", status="done", note=""),
    dict(id="HEAT-032", area="heat",
         desc="RST 코일 고착: RST 가 2회 연속 폴링에서 1 이면 0 으로 되돌리고 '⚠ 히터 리셋 코일(M00403)이 1 로 남아 있어 0 으로 되돌림' 로그(실패면 재시도 로그)",
         v1="controller/heater_service.py:70;controller/heater_service.py:1078-1094", v2="v2/ch12/heater_edges.py",
         test="v2/tests/test_heater_engine.py::test_rst_stuck_cleared_after_two_polls", status="done", note=""),
    dict(id="HEAT-033", area="heat",
         desc="take_over(owner): 다른 주체가 운전 중이면 '수동 운전 중인 히터를 공정이(가) 넘겨받음' 로그 후 주체 변경",
         v1="controller/heater_service.py:310-314", v2="v2/ch12/heater_commands.py", test="v2/tests/test_heater_process.py::test_take_over_manual_run_uses_apply_target; v2/tests/test_heater_process.py::test_disarm_releases_hold_and_run_off_restores", status="done", note=""),
    dict(id="HEAT-034", area="heat",
         desc="set_hold_armable(on): 끌 때 holding 이면 해제(D118 원복), 포화 클램프 허용/금지, 유지 무장 허용/금지 후 쌓인 D118 요청 실행(실패 로그 'DAC 상한(D118) 원복 실패')",
         v1="controller/heater_service.py:316-332", v2="v2/ch12/heater_commands.py", test="v2/tests/test_heater_process.py::test_disarm_releases_hold_and_run_off_restores; v2/tests/test_heater_process.py::test_set_wait_hold_then_fault_in_process", status="done", note=""),
    dict(id="HEAT-035", area="heat",
         desc="force_hold(사유): RUN 아니거나 stale 이면 실패, 이미 유지면 성공, 최근 60초 MV 평균(없으면 현재 MV)으로 dac 강제 진입 → D118 쓰기 → "
              "특이사항 '유지 모드 강제 진입' + 카드 '유지 모드 강제 진입' FAIL(부제 '맥락 · 사유 — DAC 상한 v 고정(출처)', PV·MV)",
         v1="controller/heater_service.py:334-359", v2="v2/ch12/heater_commands.py", test="v2/tests/test_heater_process.py::test_hold_timeout_then_force_uses_recent_average; v2/tests/test_heater_process.py::test_hold_failure_handling", status="done", note=""),
    dict(id="HEAT-036", area="heat",
         desc="프로그램 종료 shutdown: 폴링·워치독 취소(각 1초 대기) → RUN 이고 링크 있으면 RUN=0(2초) '프로그램 종료 — 히터 RUN OFF', "
              "못 보내면 '⚠ 종료 시 히터 OFF 를 보내지 못함 — PLC 워치독이 30초 뒤 차단' → CSV 마지막 행(note '프로그램 종료') 후 닫기. main 은 4초 상한으로 기다림",
         v1="controller/heater_service.py:433-466;main.py:1413-1421", v2="v2/ch12/heater_commands.py; v2/ch12/safety.py", test="v2/tests/test_inv_heat.py::test_heat036_shutdown_turns_run_off_and_closes_csv", status="done",
         note="v2 의도적 종료 규칙(파워만 끔)과의 관계는 decisions 에 — D73 — 종료 RUN=0 은 safety 요청. 의도적 종료(plan 3장 (6))는 S32 가 heater.shutdown 을 부름"),
    dict(id="HEAT-037", area="heat",
         desc="SV 쓰기: raw = round(°C/0.1) 0~32767 클램프 → D112. 로그 '목표(SV) x°C 기록 (D112=raw)' 는 강제이거나 마지막 기록과 1.0°C 이상 차이 또는 30초 경과 때만",
         v1="controller/heater_service.py:64-65;controller/heater_service.py:488-496;lib/heater_config.py:83-85",
         v2="v2/ch12/heater_engine.py", test="v2/tests/test_heater_guards.py::test_helpers; v2/tests/test_heater_engine.py::test_residual_run_cleared_before_first_publish", status="done", note=""),
    dict(id="HEAT-038", area="heat",
         desc="D120 쓰기 1~100 클램프, D118 쓰기 [MV_MIN+20, HEATER_MV_LIMIT] 클램프, 램프 카운트 = max(1, round(°C/min ÷ 6)), 설정 램프 °C/min = 카운트×6",
         v1="controller/heater_service.py:498-507;lib/heater_config.py:87-90", v2="v2/ch12/heater_engine.py; v2/ch12/heater_profile.py",
         test="v2/tests/test_heater_guards.py::test_helpers; v2/tests/test_heater_engine.py::test_turn_on_kicks_watchdog_once_and_sets_owner", status="done", note=""),

    # ───────────────────────── 이상·엣지·카드 ─────────────────────────
    dict(id="HEAT-039", area="heat",
         desc="FAULT 상승 엣지: 사유 문구(과온 트립/온도센서 이상(단선/모듈)/통신 워치독 타임아웃/히터 이상 (PID err=n), 첫 폴링이면 ' — 기존 래치(프로그램 시작 시 확인)'), "
              "RUN 이면 RUN=0 1회(실패 시 보류), 램프 정지, 라벨(이상 — 과온/온도센서/통신 워치독/이상) 특이사항·종료 사유, owner=None, 로그, "
              "카드 '이상' FAIL(부제 '맥락 · 라벨', 상세·PV·PID err). 해제 시 '히터 이상 해제됨 (정상 복귀)'",
         v1="controller/heater_service.py:589-597;controller/heater_service.py:617-625;controller/heater_service.py:1047-1076",
         v2="v2/ch12/heater_edges.py", test="v2/tests/test_heater_engine.py::test_fault_rise_sends_run_off_once; v2/tests/test_heater_engine.py::test_first_poll_fault_marked_existing_latch; v2/tests/test_heater_cards.py::test_overtemp_during_run_fault_card_and_end_fail", status="done", note=""),
    dict(id="HEAT-040", area="heat",
         desc="출력 죽음: RUN·ITL·¬FAULT 이고 MV < MV_MIN 이 3회 연속이며 RUN 상승 뒤 5초 지났으면 1회 — 특이사항 '출력 죽음', 로그, "
              "카드 '출력 죽음' FAIL('운전 중인데 DAC 출력이 최소치 미만입니다 (MV a < 400, PID 에러 D117=n)'). 조건이 풀리면 다시 무장",
         v1="controller/heater_service.py:61-62;controller/heater_service.py:1096-1111", v2="v2/ch12/heater_edges.py",
         test="v2/tests/test_heater_engine.py::test_output_dead_grace_and_three_in_a_row", status="done", note=""),
    dict(id="HEAT-041", area="heat",
         desc="RUN 상승 엣지: owner 없으면 'external'(외부), CSV 새 파일, 로그 '운전 시작 (주체) — 목표 … · 램프 … · 래더 램프 … · 홀드백 … · SV상한 … · OT … · DAC상한 … · "
              "Kp/Ti 적용 a/b · 유지 모드 … · CSV 경로|없음', 이번 운전 특이사항·맥락 초기화, 카드 '시작' INFO(목표·램프·현재 PV, 유지 모드 dac 면 '유지 모드')",
         v1="controller/heater_service.py:1113-1135", v2="v2/ch12/heater_edges.py", test="v2/tests/test_heater_engine.py::test_run_edges_csv_and_chat; v2/tests/test_heater_cards.py::test_manual_ok_start_and_end_success; v2/tests/test_heater_cards.py::test_start_card_shows_hold_mode_when_dac", status="done", note=""),
    dict(id="HEAT-042", area="heat",
         desc="RUN 하강 엣지: CSV 마지막 행(note '운전 종료') 후 닫기 → 램프 정지(D120 원복) 또는 D120 원복(실패 로그) → 종료 사유 우선순위: PLC 이상 플래그 라벨 > "
              "기록된 종료 사유 > 'PLC 재기동 의심(세션 마커 불일치)' > '인터락 해제(빠진 조건) — PLC 가 RUN 을 끔' > '외부 OFF 또는 순간 인터락 해제(이 프로그램이 끄지 않음)'",
         v1="controller/heater_service.py:1141-1171", v2="v2/ch12/heater_edges.py", test="v2/tests/test_heater_engine.py::test_marker_mismatch_run_fall_reason_restart; v2/tests/test_heater_sim.py::test_interlock_loss_stops_run_with_cause; v2/tests/test_heater_sim.py::test_brief_interlock_drop_between_polls; v2/tests/test_heater_cards.py::test_overtemp_during_run_fault_card_and_end_fail", status="done", note=""),
    dict(id="HEAT-043", area="heat",
         desc="종료 카드: '종료' — 정상 사유 {'정지(패널)','공정 종료','공정 중단 — 사용자 STOP','공정 중단 — 원격 STOP'} 이고 이상 사건·PLC 이상 없음이면 SUCCESS, "
              "아니면 FAIL(비정상 사유는 특이사항에도). 필드 마지막 PV·운전 시간('h시간 m분'|'m분')·사유·특이사항. 로그 '운전 종료 — 사유 … · 마지막 PV … · 운전 x.x분 · 특이사항 …'. "
              "그 뒤 owner·사유·맥락 초기화, 유지 무장·포화 클램프 다시 허용",
         v1="controller/heater_service.py:73-74;controller/heater_service.py:612-615;controller/heater_service.py:1172-1188",
         v2="v2/ch12/heater_edges.py; v2/ch12/heater_notify.py", test="v2/tests/test_heater_cards.py::test_manual_ok_start_and_end_success; v2/tests/test_heater_cards.py::test_process_end_cards; v2/tests/test_heater_cards.py::test_duration_format", status="done", note="카드는 v2 에서 포털이 보냄(같은 계기·내용) — 카드는 이벤트 — 포털이 보냄"),
    dict(id="HEAT-044", area="heat",
         desc="이번 운전 특이사항: 같은 라벨은 1번, 최대 4개 표시 후 ' 외 N건', 없으면 '없음'",
         v1="controller/heater_service.py:75;controller/heater_service.py:599-610", v2="v2/ch12/heater_notify.py",
         test="v2/tests/test_heater_cards.py::test_many_events_capped_at_four", status="done", note=""),
    dict(id="HEAT-045", area="heat",
         desc="히터 카드 공통: 제목 '[CH2] 히터 {제목}', urgent=True, 챗 객체 없으면 생략·예외 삼킴. 온도 표기 '--.-' / 'x.x°C'",
         v1="controller/heater_service.py:530-536;controller/heater_service.py:584-586", v2="v2/ch12/heater_notify.py; vanam-process lib/ops2/ch12/chatCards.ts",
         test="v2/tests/test_heater_cards.py::test_card_events_carry_v1_shape; vanam-process/lib/ops2/ch12/chat-golden/web_ch12_chat.json::heater_cards", status="done", note="v2 는 챗 유무와 상관없이 카드 이벤트를 늘 냄(보낼지는 포털)"),
    dict(id="HEAT-046", area="heat",
         desc="카드 부제 맥락: 운전 중이면 이번 운전 맥락, 공정 소유면 '공정 \"이름\" (현재/전체)', 아니면 주체 이름(수동/공정/외부). 운전 중 공정이 맥락을 바꾸면 따라감",
         v1="controller/heater_service.py:72;controller/heater_service.py:195-214;runtime/chamber_runtime.py:7123-7131",
         v2="v2/ch12/heater_notify.py; v2/ch12/heater_link.py", test="v2/tests/test_heater_cards.py::test_queue_two_processes_one_start_two_reached_one_end; v2/tests/test_heater_cards.py::test_manual_ok_start_and_end_success", status="done", note=""),
    dict(id="HEAT-047", area="heat",
         desc="도달 카드(공정 소유 히터의 승온 대기 통과 때만): '도달' — 목표·도달 PV·승온 소요(SOAK 뺀 'm분 s초'), 레시피 SOAK 이 있으면 SOAK·SOAK 중 PV 범위, 다음 단계, "
              "특이사항(있으면 FAIL 아니면 SUCCESS). 실패는 '⚠ 히터 도달 카드 실패' 로그",
         v1="controller/heater_service.py:216-231;runtime/chamber_runtime.py:7133-7149", v2="v2/ch12/heater_link.py",
         test="v2/tests/test_heater_cards.py::test_process_end_cards; v2/tests/test_heater_process.py::test_400c_soak_60min_counts_time_after_arrival_with_heater_on; v2/tests/test_heater_process.py::test_blank_soak_uses_setting_and_card_unchanged", status="done", note=""),
    dict(id="HEAT-048", area="heat",
         desc="카드 '유지 모드 포기'(FAIL, PV), 'DAC 포화'(FAIL, '도달 후 N초째 상한(L)에 물림 → D118 v', PV·MV·클램프), '승온 정체'(FAIL, 목표·상한·상승 속도, "
              "현재 속도로 '약 N분'|'도달 안 됨', 확인 'HEATER_MV_LIMIT(출력 상한)') — 각각 특이사항 라벨도 추가",
         v1="controller/heater_service.py:545-556;controller/heater_service.py:574-582", v2="v2/ch12/heater_engine.py; v2/ch12/heater_notify.py",
         test="v2/tests/test_heater_cards.py::test_manual_stall_fail_card_and_end_fail; v2/tests/test_heater_process.py::test_hold_give_up_is_reported_once_after_start; v2/tests/test_heater_driver.py::test_stack_saturation_clamp_after_arrival", status="done", note=""),
    dict(id="HEAT-049", area="heat",
         desc="출력 무응답·끊김 판정 시: 이번 발행 상태 noresp='라벨 — 판정 문구', 특이사항, 로그 '⚠ 히터 라벨: … → 히터를 끕니다 (확인)', "
              "카드 라벨 FAIL(부제 '맥락 · 라벨 — 배선·VARITAP·차단기 확인', 판정·PV·MV a/limit·확인) → turn_off(사유=라벨)",
         v1="controller/heater_service.py:558-572;controller/heater_service.py:844-849", v2="v2/ch12/heater_edges.py",
         test="v2/tests/test_heater_sim.py::test_sim_dead_output_cold_start_turns_off; v2/tests/test_heater_process.py::test_runtime_noresp_during_wait_fails_once_with_cause; v2/tests/test_heater_driver.py::test_stack_noresp_a_turns_off", status="done", note=""),
    dict(id="HEAT-050", area="heat",
         desc="유지·포화 모듈 메시지는 HEATER 로그로(경고 수준이면 '⚠ ' 접두). 유지 진입은 'DAC 상한 v 유지 진입' 정상 로그(특이사항 아님)",
         v1="controller/heater_service.py:538-543", v2="v2/ch12/heater_engine.py; v2/ch12/heater_notify.py", test="v2/tests/test_heater_process.py::test_gas_load_rise_not_clamped_and_guard_rearms; v2/tests/test_heater_units.py::test_hold_capture_after_enter_window", status="done", note=""),

    # ───────────────────────── 램프(파이썬 감속 접근) ─────────────────────────
    dict(id="HEAT-051", area="heat",
         desc="램프 시작: 출발점 = (RUN 이고 D119 있으면) D119, 아니면 PV, 아니면 D119; 없으면 '램프: 현재 온도를 읽을 수 없어 …' 후 직접 목표. "
              "목표 ≤ 출발점+0.05 또는 속도 ≤ 0 이면 램프 없음(호출부가 SV 직접). D120 = max(2, ceil(속도/6)+1) 먼저, 그다음 SV = round(출발점,1)(RUN 켜기 전). "
              "로그 '램프[ 라벨]: a→b°C · r°C/min · 접근 Z°C→m°C/min · 예상 x분 (래더 D120 은 n°C/min 로 열어 둠)'",
         v1="controller/heater_ramp.py:76-122", v2="v2/ch12/heater_ramp.py", test="v2/tests/test_heater_units.py::test_ramp_start_writes_ramp_then_sv_base; v2/tests/test_heater_units.py::test_ramp_start_from_sv_ramp_when_running; v2/tests/test_heater_units.py::test_ramp_target_not_above_base_returns_false", status="done", note=""),
    dict(id="HEAT-052", area="heat",
         desc="램프 tick(성공 폴링마다): dt = 지난 tick 뒤 시간(0~5초 클램프), 남은 거리 = 목표 − D119, 속도 = approach_rate, "
              "새 SV = min(목표, max(이전 SV, D119−0.05) + 속도×dt/60, D119 + HEATER_APPROACH_LEAD_C), 뒤로 가지 않음, 0.1°C 눈금이 바뀔 때만 D112 쓰기(실패는 다음 폴링 재시도). "
              "SV ≥ 목표 이고 D119 ≥ 목표−0.15 면 '램프 완료 x°C'",
         v1="controller/heater_ramp.py:124-157", v2="v2/ch12/heater_ramp.py", test="v2/tests/test_heater_units.py::test_ramp_tick_rate_and_lead_limit; v2/tests/test_heater_units.py::test_ramp_dt_capped_at_5s; v2/tests/test_heater_units.py::test_ramp_approach_decelerates_near_target; v2/tests/test_heater_units.py::test_ramp_completion; v2/tests/test_heater_units.py::test_ramp_completion_waits_for_ladder; v2/tests/test_heater_units.py::test_ramp_write_failure_falls_back", status="done", note=""),
    dict(id="HEAT-053", area="heat",
         desc="램프 정지: 활성 해제·목표 지움, SV 는 건드리지 않음, restore_rate 이고 활성이었으면 D120 을 설정값 카운트로 원복(실패 로그 '래더 램프 속도 원복 실패')",
         v1="controller/heater_ramp.py:159-168", v2="v2/ch12/heater_ramp.py", test="v2/tests/test_heater_units.py::test_ramp_stop_restores_ramp_rate", status="done", note=""),
    dict(id="HEAT-054", area="heat",
         desc="감속 접근 프로파일: d < zone 이면 r(d) = rmin + (rate−rmin)·d/zone, 아니면 rate(rate ≤ rmin 또는 zone ≤ 0 이면 rate). "
              "소요 시간 = (span−d1)/rate + ln((rmin+b·d1)/rmin)/b, b=(rate−rmin)/zone, d1=min(span,zone)",
         v1="lib/heater_profile.py:28-49", v2="v2/ch12/heater_profile.py", test="v2/tests/test_heater_units.py::test_approach_rate_linear_in_zone; v2/tests/test_heater_units.py::test_ramp_minutes_approach_integral", status="done", note=""),
    dict(id="HEAT-055", area="heat",
         desc="램프 상태 조회: 활성·최종 목표·속도·남은 분(D119 기준 ramp_minutes) — 패널 표시용 ramp_info, final_target = 램프 목표 없으면 SV",
         v1="controller/heater_ramp.py:53-73;controller/heater_service.py:247-257", v2="v2/ch12/heater_engine.py; v2/ch12/heater_ramp.py",
         test="v2/tests/test_heater_process.py::test_take_over_manual_run_uses_apply_target; v2/tests/test_heater_process.py::test_panel_apply_changes_process_target_v1_anomaly", status="done", note="ramp_info()·final_target() — 화면은 포털 히터 칸"),

    # ───────────────────────── 유지 모드(dac) ─────────────────────────
    dict(id="HEAT-056", area="heat",
         desc="유지 도달 래치: 램프 끝(|D119−SV| ≤ 0.05, |SV−최종| ≤ 0.05)·ok·PV/SV/D119/MV 있음 상태에서 |PV−최종| ≤ ARRIVE_TOL(1.0) 한 번 → "
              "'목표 도달 확인 — 유지 모드 측정 시작'. 최종 목표가 0.05 넘게 바뀌면 래치 초기화",
         v1="controller/heater_hold.py:222-232", v2="v2/ch12/heater_hold.py", test="v2/tests/test_heater_units.py::test_hold_needs_arrival_latch", status="done", note=""),
    dict(id="HEAT-057", area="heat",
         desc="유지 측정 창: |PV−SV| ≤ ENTER_TOL(3.0) 이 ENTER_SEC(60초) 연속·표본 4개 이상 → 창 전·후반 평균 드리프트가 "
              "PV max(DRIFT_PV, 0.5×ENTER_TOL)·MV max(DRIFT_MV, 0.05×MV_LIMIT) 안이면 캡처, 아니면 처음부터 다시(재측정 로그 30초 1회)",
         v1="controller/heater_hold.py:21-25;controller/heater_hold.py:100-103;controller/heater_hold.py:234-260",
         v2="v2/ch12/heater_hold.py", test="v2/tests/test_heater_units.py::test_hold_capture_after_enter_window; v2/tests/test_heater_units.py::test_hold_drift_remeasures", status="done", note=""),
    dict(id="HEAT-058", area="heat",
         desc="유지 고정 거부: 창 평균 MV ≥ 0.98×MV_LIMIT 이면 고정하지 않고 '유지 모드 포기'(경고 1회·give_up 콜백), ≤ MV_MIN+20 이면 고정하지 않음(경고 1회)",
         v1="controller/heater_hold.py:261-283", v2="v2/ch12/heater_hold.py", test="v2/tests/test_heater_units.py::test_hold_give_up_near_limit; v2/tests/test_heater_units.py::test_hold_floor_not_captured", status="done", note=""),
    dict(id="HEAT-059", area="heat",
         desc="유지 캡처: D118 ← 창 평균 MV([MV_MIN+20, MV_LIMIT] 클램프, 클램프 시 로그) → holding, '유지 모드 — DAC 상한 v 고정 (PV/SV)'",
         v1="controller/heater_hold.py:286-298", v2="v2/ch12/heater_hold.py", test="v2/tests/test_heater_units.py::test_hold_capture_after_enter_window", status="done", note=""),
    dict(id="HEAT-060", area="heat",
         desc="holding 중: SV 가 0.05 넘게 바뀌면 해제(D118 = MV_LIMIT 원복, '목표 변경 a → b'), D118 되읽기가 고정값과 다르면 5초 뒤 재적용 'DAC 상한 재적용'",
         v1="controller/heater_hold.py:300-312", v2="v2/ch12/heater_hold.py", test="v2/tests/test_heater_units.py::test_hold_release_restores_on_run_off_fault_target_change; v2/tests/test_heater_units.py::test_hold_reapply_when_reverted", status="done", note=""),
    dict(id="HEAT-061", area="heat",
         desc="유지 해제·초기화: RUN OFF('운전 OFF')·FAULT('히터 이상')·모드 off('기능 꺼짐') → D118 원복 요청, armable=False 면 측정·도달 초기화. "
              "set_armable 은 측정 상태와 마지막 창 평균을 버린다(holding 이면 호출부가 먼저 해제)",
         v1="controller/heater_hold.py:92-98;controller/heater_hold.py:170-220", v2="v2/ch12/heater_hold.py",
         test="v2/tests/test_heater_units.py::test_hold_release_restores_on_run_off_fault_target_change; v2/tests/test_heater_process.py::test_disarm_releases_hold_and_run_off_restores", status="done", note=""),
    dict(id="HEAT-062", area="heat",
         desc="강제 진입 값 출처: 마지막 측정 창 평균 → (없으면) 인자 MV(최근 60초 평균 또는 현재 MV). 모드 off 면 실패 '유지 모드 꺼짐(HEATER_HOLD_MODE=off)', "
              "값 없으면 '측정 창 평균도 현재 MV 도 없음'. 고정값 클램프 후 '유지 모드 강제 진입 — DAC 상한 v 고정 (출처)'",
         v1="controller/heater_hold.py:134-167;controller/heater_saturation.py:92-96", v2="v2/ch12/heater_hold.py",
         test="v2/tests/test_heater_units.py::test_hold_force_dac_hold; v2/tests/test_heater_process.py::test_hold_timeout_then_force_uses_recent_average", status="done", note=""),
    dict(id="HEAT-063", area="heat",
         desc="현재 운전 설정 HEATER_HOLD_MODE='off' — HEATER_HOLD 단계는 포화 감시만 다시 켜고 바로 통과('유지 모드 꺼짐(HEATER_HOLD_MODE=off) — 포화 감시만 다시 켜고 진행')",
         v1="config/settings.json:610;lib/config_common.py:491;runtime/chamber_runtime.py:1537-1544", v2="v2/ch12/heater_link.py; v2/ch12/params/core_heater.py",
         test="v2/tests/test_heater_process.py::test_hold_mode_off_passes_and_rearms_guard; v2/tests/test_heater_guards.py::test_cfg_v2_param_defaults_are_v1_runtime_values", status="done", note="HOLD_MV_*·HOLD_WAIT·HOLD_FAIL_ACTION 은 dac 일 때만 쓰임"),

    # ───────────────────────── DAC 포화 감시·승온 정체 ─────────────────────────
    dict(id="HEAT-064", area="heat",
         desc="포화 감시 a) RUN OFF/FAULT: 클램프 중이면 해제(D118 원복 '운전 OFF'/'히터 이상') + 이력·도달·무장·정체·유지 정보 전부 초기화",
         v1="controller/heater_saturation.py:159-167;controller/heater_saturation.py:139-145", v2="v2/ch12/heater_saturation.py",
         test="v2/tests/test_heater_units.py::test_sat_release_restores_limit; v2/tests/test_heater_units.py::test_sat_post_arrival_clamp_to_pre_average_once", status="done", note=""),
    dict(id="HEAT-065", area="heat",
         desc="포화 감시 b) 도달 래치(목표당 1회): 파이썬 램프 끝·ok·|D119−SV| ≤ 0.05·|SV−최종| ≤ 0.05·|PV−최종| ≤ ARRIVE_TOL. 최종 목표가 0.05 넘게 바뀌면 도달·무장·정체 초기화",
         v1="controller/heater_saturation.py:174-183", v2="v2/ch12/heater_saturation.py", test="v2/tests/test_heater_units.py::test_sat_not_arrived_while_ramp_or_ladder_ramp_behind; v2/tests/test_heater_units.py::test_sat_target_change_resets_arrival; v2/tests/test_heater_units.py::test_sat_arrival_at_limit_is_not_saturation", status="done", note=""),
    dict(id="HEAT-066", area="heat",
         desc="포화 감시 c) 유지 중에는 경고·카드·클램프 없음(dac 유지 진입 시 클램프는 D118 을 유지 모드에 넘김). dac 유지 중 SV−PV ≥ 10°C 가 HEATER_MV_SAT_SEC 이어지면 정보 로그 1회",
         v1="controller/heater_saturation.py:44;controller/heater_saturation.py:185-206", v2="v2/ch12/heater_saturation.py",
         test="v2/tests/test_heater_units.py::test_sat_dac_hold_no_warning_no_clamp; v2/tests/test_heater_units.py::test_sat_dac_hold_low_pv_info_once; v2/tests/test_heater_sim.py::test_dac_hold_shutter_offset_info_only", status="done", note=""),
    dict(id="HEAT-067", area="heat",
         desc="포화 감시 d) MV 이력: 유지 중이 아니면 매 tick 적재, (포화 시작 또는 지금) − 60초보다 오래된 것 버림. '상한' = D118 되읽기(>0), 없으면 설정값. 상한에 물림 = MV ≥ 0.98×상한",
         v1="controller/heater_saturation.py:37-38;controller/heater_saturation.py:217-224;controller/heater_saturation.py:290-295",
         v2="v2/ch12/heater_saturation.py", test="v2/tests/test_heater_units.py::test_sat_judged_on_readback_limit; v2/tests/test_heater_units.py::test_sat_saturation_right_after_ramp_uses_ramp_history", status="done", note=""),
    dict(id="HEAT-068", area="heat",
         desc="승온 정체(도달 전, 클램프 없음): 상한에 물리고 PV < 최종−ARRIVE_TOL 인 창 300초(앞·뒤 30초 평균)에서 상승 < 0.5°C/min 이고 현재 속도로 남은 시간 > 60분이면 "
              "목표당 1회 경고 '승온 정체 — 출력 상한(L)에 붙은 채 … 클램프하지 않음' + 정체 카드",
         v1="controller/heater_saturation.py:40-43;controller/heater_saturation.py:226-230;controller/heater_saturation.py:258-288",
         v2="v2/ch12/heater_saturation.py", test="v2/tests/test_heater_units.py::test_sat_stall_once_no_clamp; v2/tests/test_heater_units.py::test_sat_no_stall_below_limit_or_short_eta; v2/tests/test_heater_units.py::test_sat_stall_rejudged_after_target_change; v2/tests/test_heater_sim.py::test_unreachable_300_no_clamp_one_stall_card", status="done", note=""),
    dict(id="HEAT-069", area="heat",
         desc="포화 감시 f) 도달 후 MV 가 상한 아래로 한 번 내려와야 무장. 무장 뒤 상한에 HEATER_MV_SAT_SEC(120초) 연속이면 1회: 직전 60초 이력이 30초 이상이고 평균 < 0.98×상한이면 "
              "D118 ← 평균([MV_MIN+20, MV_LIMIT]) 클램프 + 포화 카드, 아니면 경고만('직전 이력 부족'/'직전 평균 n 도 상한'). 클램프 금지 상태(공정이 유지 재측정 전)면 경고만",
         v1="controller/heater_saturation.py:39;controller/heater_saturation.py:233-256;controller/heater_saturation.py:297-329",
         v2="v2/ch12/heater_saturation.py", test="v2/tests/test_heater_units.py::test_sat_post_arrival_clamp_to_pre_average_once; v2/tests/test_heater_units.py::test_sat_pre_average_also_at_limit_no_clamp_warn_once; v2/tests/test_heater_units.py::test_sat_short_pre_history_no_clamp; v2/tests/test_heater_units.py::test_sat_before_arrival_never_clamps", status="done", note=""),
    dict(id="HEAT-070", area="heat",
         desc="포화 클램프 미룸: 출력 끊김 판정 B 구간이 막 시작돼(2×30초 미만) PV 하강 속도를 못 쟀으면(loss_wait) 클램프를 미루고 정보 로그 1회",
         v1="controller/heater_saturation.py:248-255;controller/heater_response.py:96-98;controller/heater_service.py:833-834",
         v2="v2/ch12/heater_saturation.py; v2/ch12/heater_response.py", test="v2/tests/test_heater_guards.py::test_unit_loss_wait_only_while_window_short; v2/tests/test_heater_guards.py::test_unit_saturation_defers_clamp_then_uses_same_pre_average; v2/tests/test_heater_guards.py::test_unit_defer_ignored_when_clamp_disabled", status="done", note=""),
    dict(id="HEAT-071", area="heat",
         desc="포화 감시 g) SV 가 0.05 넘게 바뀌면 클램프 해제(D118 원복, 이력 유지). set_clamp_enabled(False) 는 클램프 중이면 원복, (True) 로 켤 때 꺼져 있던 동안의 포화 진행·이력·무장 버림",
         v1="controller/heater_saturation.py:104-137;controller/heater_saturation.py:208-212", v2="v2/ch12/heater_saturation.py",
         test="v2/tests/test_heater_units.py::test_sat_release_restores_limit; v2/tests/test_heater_guards.py::test_unit_defer_ignored_when_clamp_disabled; v2/tests/test_heater_process.py::test_set_wait_hold_then_fault_in_process", status="done", note=""),

    # ───────────────────────── 출력 무응답 감시(EL-092) ─────────────────────────
    dict(id="HEAT-072", area="heat",
         desc="출력 무응답 A: 이번 운전에서 최저 PV 보다 HEATER_NORESP_RISE_C(5) 이상 오른 적이 없고, 목표보다 5°C 이상 낮으며, MV ≥ min(HEATER_NORESP_MV(600), 0.98×상한), "
              "PV 가 구간 시작 PV 와 RISE_C 안(평탄)인 채 HEATER_NORESP_SEC(300초) 연속 → '출력 무응답'",
         v1="controller/heater_response.py:37-38;controller/heater_response.py:138-160", v2="v2/ch12/heater_response.py",
         test="v2/tests/test_heater_guards.py::test_resp_a_dead_cold_start_trips_once; v2/tests/test_heater_guards.py::test_resp_a_threshold_follows_actual_limit; v2/tests/test_heater_guards.py::test_resp_a_working_heater_no_trip; v2/tests/test_heater_guards.py::test_resp_a_slow_but_rising_counts_as_heated; v2/tests/test_heater_guards.py::test_resp_a_barely_rising_trips; v2/tests/test_heater_guards.py::test_resp_a_hot_restart_falling_then_recovering_no_trip; v2/tests/test_heater_guards.py::test_resp_a_no_heating_demand_no_trip", status="done", note=""),
    dict(id="HEAT-073", area="heat",
         desc="출력 끊김 B: MV ≥ 0.98×상한(D118 되읽기)이고 PV 가 목표보다 LOSS_BELOW_C(10) 이상 낮은 상태가 LOSS_SEC(180초) 연속이며 창 앞·뒤 30초 평균 하강 ≥ LOSS_FALL_C(10) → '출력 끊김'. "
              "dac 유지 중 제외",
         v1="controller/heater_response.py:39;controller/heater_response.py:162-188", v2="v2/ch12/heater_response.py",
         test="v2/tests/test_heater_guards.py::test_resp_b_loss_trips; v2/tests/test_heater_guards.py::test_resp_b_uses_readback_limit; v2/tests/test_heater_guards.py::test_resp_b_slow_fall_or_not_at_limit_no_trip; v2/tests/test_heater_guards.py::test_resp_b_skipped_while_holding", status="done", note=""),
    dict(id="HEAT-074", area="heat",
         desc="포화 클램프 중의 B: 클램프가 걸린 순간 B 구간 PV 추세가 B 기준 속도(fall_c × (span−30)/(loss_sec−30)) 이상 하강이면 클램프 중에도 B 를 보고, 아니면 그 클램프 동안 보지 않음",
         v1="controller/heater_response.py:82-94;controller/heater_response.py:122-125;controller/heater_response.py:164-166",
         v2="v2/ch12/heater_response.py", test="v2/tests/test_heater_guards.py::test_unit_clamp_during_fast_fall_keeps_judging; v2/tests/test_heater_guards.py::test_unit_clamp_without_fall_rests_until_released", status="done", note="남는 한계(클램프 뒤 새 끊김 미검출)도 v1 그대로 — 남는 한계도 v1 그대로"),
    dict(id="HEAT-075", area="heat",
         desc="무응답 감시 초기화·1회: RUN OFF 면 운전 기준 전부 초기화, stale·FAULT·TC 이상·값 없음이면 연속 판정 초기화, 표본 간격 > 10초면 초기화. 운전당 1번만 판정. "
              "확인 문구 'D/A 출력 배선(DV04A CH3 → VARITAP)·VARITAP 전원/차단기·히터 단선·TC 위치 확인'(끊김이면 '— 출력이 모자란 채 부하(가스 등)가 늘어도 같은 증상' 추가). "
              "HEATER_NORESP_ENABLE=false 면 끔",
         v1="controller/heater_response.py:40-41;controller/heater_response.py:61-76;controller/heater_response.py:116-136;controller/heater_response.py:190-193",
         v2="v2/ch12/heater_response.py", test="v2/tests/test_heater_guards.py::test_resp_run_off_resets_and_rearms; v2/tests/test_heater_guards.py::test_resp_bad_sample_restarts_streak; v2/tests/test_heater_guards.py::test_resp_gap_restarts_streak; v2/tests/test_heater_guards.py::test_resp_disabled_never_trips", status="done", note=""),

    # ───────────────────────── 설정 키 ─────────────────────────
    dict(id="HEAT-076", area="heat",
         desc="설정 읽기(호출 시점, 예외 없이 경고 후 안전 쪽): SUPPORTS_HEATER(bool 문자열 1/true/yes/on), HEATER_TEMP_SCALE ≤0 → 0.1, 형식 오류 → 기본값 + '키=값 형식 오류 → 기본값'",
         v1="lib/heater_config.py:105-133", v2="v2/ch12/heater_profile.py; v2/ch12/params/core_heater.py", test="v2/tests/test_heater_guards.py::test_cfg_defaults_without_keys; v2/tests/test_heater_guards.py::test_cfg_enabled_flag; v2/tests/test_heater_guards.py::test_cfg_roundtrip_dict", status="done", note="D11 — v2 파라미터 표가 범위 밖 값을 먼저 거부, 호출 시점 규칙은 그대로 둠"),
    dict(id="HEAT-077", area="heat",
         desc="온도 한계 규칙(래더와 같게): SV_LIMIT ≤0 → 500, > 900 → 900; OT ≤0 → 550, > 950 → 950; OT ≤ SV상한이면 SV상한+30; MAX_TEMP ≤0 또는 > SV상한이면 SV상한",
         v1="lib/heater_config.py:16-20;lib/heater_config.py:135-152", v2="v2/ch12/heater_profile.py", test="v2/tests/test_heater_guards.py::test_cfg_temp_limits; v2/tests/test_heater_guards.py::test_cfg_temp_limits_at_ladder_max_no_warning; v2/tests/test_heater_guards.py::test_cfg_max_temp_and_approach", status="done", note="D11"),
    dict(id="HEAT-078", area="heat",
         desc="출력 규칙: MV_MIN ≠ 400 이면 경고만(값은 그대로), MV_ABS_MAX > 1600 → 1600, < MV_MIN+20 → 1600, MV_LIMIT 는 [MV_MIN+20, MV_ABS_MAX] 클램프",
         v1="lib/heater_config.py:154-163", v2="v2/ch12/heater_profile.py", test="v2/tests/test_heater_guards.py::test_cfg_mv_rules", status="done", note="D11"),
    dict(id="HEAT-079", area="heat",
         desc="램프 규칙: RAMP_RATE < 6 → 6, SLOW_RATE < 6 → 6, SLOW_ZONE < 0 → 0, HOLDBACK ≤0 → 10. PID: KP 0.1~50, TI 10~3000 밖이면 '쓰지 않음(래더 기본값 3.0/300)'",
         v1="lib/heater_config.py:165-187", v2="v2/ch12/heater_profile.py", test="v2/tests/test_heater_guards.py::test_cfg_ramp_holdback_rules; v2/tests/test_heater_guards.py::test_cfg_pid_out_of_range_marked_unused", status="done", note="D11"),
    dict(id="HEAT-080", area="heat",
         desc="접근·유지 규칙: APPROACH_ZONE <0 → 0, MIN_RATE ≤0 → 1.0 / > 6 → 6.0, LEAD < 0.2 → 0.2, SOAK_TOLERANCE ≤0 → 2.0, HOLD_MODE off|dac 외 → dac, "
              "ENTER_TOL 0.5~20, ENTER_SEC 10~600, ARRIVE_TOL 0.2~10, DRIFT_PV 0.1~10, DRIFT_MV 3~200, MV_SAT_SEC < 10 → 10",
         v1="lib/heater_config.py:189-216", v2="v2/ch12/heater_profile.py", test="v2/tests/test_heater_guards.py::test_cfg_max_temp_and_approach; v2/tests/test_heater_guards.py::test_cfg_hold_mode_tc2_becomes_dac; v2/tests/test_heater_guards.py::test_cfg_hold_and_period_clamps", status="done", note="D11"),
    dict(id="HEAT-081", area="heat",
         desc="공정·주기 규칙: SOAK_TIME 0~3600, WAIT_TIMEOUT 600~43200, HOLD_WAIT 60~1800, HOLD_FAIL_ACTION force|abort 외 → force, POLL 0.5~5, POLL_IDLE POLL~10, "
              "WD_PERIOD 1~9, STALE < 2×POLL_IDLE 이면 2×POLL_IDLE, LOG_PERIOD 1~60, RUN_LOG_PERIOD <0 → 0(끔), >0 이면 2~600",
         v1="lib/heater_config.py:218-238", v2="v2/ch12/heater_profile.py", test="v2/tests/test_heater_guards.py::test_cfg_hold_and_period_clamps; v2/tests/test_heater_guards.py::test_cfg_run_log_period", status="done", note="D11"),
    dict(id="HEAT-082", area="heat",
         desc="무응답·마커 규칙: NORESP_MV 450~1600, NORESP_SEC 60~1800, RISE 1~50, LOSS_SEC 60~1800, LOSS_BELOW 2~200, LOSS_FALL 0.5~100. "
              "SESSION_MARK_REG 가 D110~D149 와 겹치면 0(끔), D150~D160 밖이면 0(끔), 음수면 0",
         v1="lib/heater_config.py:240-259", v2="v2/ch12/heater_profile.py", test="v2/tests/test_heater_guards.py::test_cfg_noresp_defaults_and_clamps; v2/tests/test_heater_guards.py::test_cfg_marker_register_overlap", status="done", note="D11"),
    dict(id="HEAT-083", area="heat",
         desc="ch2 운전값(settings.json): SUPPORTS_HEATER true, TEMP_SCALE 0.1, POLL 1.0/IDLE 3.0, WD 5.0, STALE 10, MAX_TEMP 900, SV_LIMIT 900, OT 950, RAMP 6, HOLDBACK 10, "
              "SLOW_ZONE 0, SLOW_RATE 60, MV_MIN 400, MV_ABS_MAX 1300, MV_LIMIT 1200, KP 3.0, TI 300, PUSH_CONFIG true, MARK_REG 160, APPROACH 20/1.0/1.0, SOAK_TOL 2.0, HOLD_MODE off, "
              "HOLD_MV 3.0/60/1.0/0.5/15, SAT 120, SOAK_TIME 60, WAIT_TIMEOUT 5400, HOLD_WAIT 300, FAIL force, LOG true/3.0/'', RUN_LOG 10, NORESP true/600/300/5/180/10/10. "
              "공통 기본값은 config_common(SUPPORTS_HEATER=False)",
         v1="config/settings.json:584-632;lib/config_common.py:461;lib/config_common.py:468-513", v2='v2/ch12/params/core_heater.py; v2/tools/convert_v1_config.py',
         test='v2/tests/test_heater_records.py::test_v1_ch2_heater_values_convert_and_load', status='done', note="v1→v2 설정 변환 대상"),
    dict(id="HEAT-084", area="heat",
         desc="히터 초기화: CH2 설정에서 읽고 SUPPORTS_HEATER=false 면 아무것도 만들지 않음(태스크·PLC I/O·창 크기 그대로), 읽기 실패면 '히터 설정 읽기 실패 — 히터 기능 끔', "
              "설정 경고는 CH2 로그 '⚠ 히터 설정: …', 성공 로그 'CH2 히터 서비스 시작', 실패 'CH2 히터 초기화 실패'. CH2 PLC 연결(_plc_ch2)과 CH2 챗을 씀",
         v1="main.py:1257-1292", v2='v2/ch12/heater_setup.py', test='v2/tests/test_heater_records.py::test_init_ch2_heater_rules', status='done', note='조립(assembly)이 init_ch2_heater(cfg, build, log) 를 불러 CH2 엔진에 넘긴다'),
    dict(id="HEAT-085", area="heat",
         desc="DC2 와 히터 배타: DC2 는 CH2 전용이고 SUPPORTS_HEATER=true 면 쓸 수 없음(D/A CH3 = 히터 출력) — 레시피 use_dc_power2 는 'DC2 Power 는 사용할 수 없습니다(DC2 폐지)'",
         v1="runtime/chamber_runtime.py:389-392;runtime/chamber_runtime.py:533;runtime/chamber_runtime.py:902;runtime/chamber_runtime.py:8896-8898;config/settings.json:583",
         v2='v2/ch12/recipe_check.py; v2/ch12/system.py; v2/ch12/process_steps.py', test='v2/tests/test_heater_records.py::test_dc2_only_without_heater', status='done', note=""),
    dict(id="HEAT-086", area="heat",
         desc="프로그램 닫기 확인창: 히터 RUN 이면 문구에 'CH2 히터가 운전 중입니다(PV x.x°C). 종료하면 히터가 꺼집니다.' 추가",
         v1="main.py:1533-1541", v2="v2/ch12/safety.py; v2/ch12/heater_commands.py; v2/ch12/resume.py", test="v2/tests/test_shutdown.py::test_intentional_exit_powers_off_only_and_no_resume; v2/tests/test_inv_heat.py::test_heat036_shutdown_turns_run_off_and_closes_csv", status="done", note="v2 는 웹·로컬 화면 종료 확인에 같은 정보 — 합의된 변경 plan 3장 (6)·D70 — v2 는 종료 확인창이 없음(콘솔 닫기·Ctrl+C = 의도적 종료). 종료 때 히터 RUN=0(S32 → heater.shutdown). 확인창 문구는 UI-043 과 함께 결정 후보"),

    # ───────────────────────── 기록 ─────────────────────────
    dict(id="HEAT-087", area="heat",
         desc="히터 운전 CSV: 위치 HEATER_LOG_DIR(비면 LOG_ROOT_DIR(C:\\VanaM_Logs\\CH1&2)\\CH2_Heater), 만들 수 없으면 LOCAL_FALLBACK_ROOT\\CH2_Heater(경고 1회), "
              "파일명 CH2_HEATER_YYYYMMDD_HHMMSS.csv(있으면 _2, _3 …), utf-8-sig, RUN 상승 엣지에 새 파일·하강 때 마지막 행 후 닫기, 매 행 flush, 실패 경고 1회",
         v1="lib/heater_logger.py:1-8;lib/heater_logger.py:27-35;lib/heater_logger.py:57-104;lib/heater_logger.py:142-151;lib/config_common.py:645",
         v2="v2/ch12/outputs/heater_csv.py", test="v2/tests/test_outputs_csv.py::test_heater_golden; v2/tests/test_outputs_csv.py::test_heater_on_tick_edges_and_suffix; v2/tests/test_heater_engine.py::test_run_edges_csv_and_chat", status="done", note=""),
    dict(id="HEAT-088", area="heat",
         desc="히터 CSV 열: timestamp(YYYY-MM-DD HH:MM:SS), elapsed_sec(.1f), owner(수동/공정/외부), pv_c, sv_c, ramp_sv_c(D119), cur_sv_c(.1f), mv, mv_limit(.0f), mv_pct(.1f), "
              "pid_err(.0f), ramp_rate_c_per_min(.0f), holdback_c, ot_limit_c, run, itl, fault, ot, tc_err, wd_err, tc_ok, da_ok, water_ok, vac_ok, pid_run, output_dead(0/1), "
              "hold(0/1), hold_mv, sat_clamp, ramp_target_c(.1f), note. 값 없으면 빈칸",
         v1="lib/heater_logger.py:17-24;lib/heater_logger.py:106-140", v2="v2/ch12/outputs/heater_csv.py", test="v2/tests/test_outputs_csv.py::test_heater_golden; v2/tests/test_heater_sim.py::test_interlock_loss_recorded_in_csv", status="done", note=""),
    dict(id="HEAT-089", area="heat",
         desc="히터 CSV 행 주기: RUN 이고 HEATER_LOG_ENABLED 일 때 HEATER_LOG_PERIOD_S(3초)마다 1행, RUN 상승 직후 첫 행은 바로. 하강 행 note '운전 종료', 종료 행 note '프로그램 종료'",
         v1="controller/heater_service.py:850-855;controller/heater_service.py:1117-1118;controller/heater_service.py:1142-1146;controller/heater_service.py:461-464",
         v2="v2/ch12/outputs/heater_csv.py; v2/ch12/heater_edges.py", test="v2/tests/test_outputs_csv.py::test_heater_on_tick_edges_and_suffix; v2/tests/test_heater_engine.py::test_run_edges_csv_and_chat; v2/tests/test_inv_heat.py::test_heat036_shutdown_turns_run_off_and_closes_csv", status="done", note="1초 스냅샷에서 만들어도 됨(같은 주기·열)"),
    dict(id="HEAT-090", area="heat",
         desc="공정 로그(.txt) 히터 상태 줄: 공정 진행 중·로그 파일 열림·RUN 일 때 HEATER_RUN_LOG_PERIOD_S(10초)마다 "
              "'히터 상태 — PV x°C · SV(D119) x°C · 목표 x°C · MV a/b · 유지(DAC 상한 v)|램프|제어 · 주체'(TC 이상이면 PV --.-). 기록 실패는 1회 경고",
         v1="runtime/chamber_runtime.py:7024-7071", v2="v2/ch12/heater_link.py; v2/ch12/assembly.py", test="v2/tests/test_heater_process.py::test_runtime_status_lines_and_samples; v2/tests/test_heater_process.py::test_runtime_status_lines_off_when_period_zero", status="done", note="히터 로그는 조립이 CH2 공정 txt 에도(열려 있을 때)"),
    dict(id="HEAT-091", area="heat",
         desc="Calib CSV·엑셀 히터 열(CH2 파일만, 맨 뒤): Heater: SV(.1f), PV Avg/Min/Max(.1f), MV Avg(.0f) — 메인 공정 폴링 중·셔터 열림 표본, PV 는 TC 정상일 때만, "
              "SV(최종 목표)·MV 는 RUN 일 때만, 세션 시작·셔터 열림 때 비움, 표본 없으면 공란",
         v1="controller/data_logger.py:16-17;controller/data_logger.py:146-147;controller/data_logger.py:415-435;controller/data_logger.py:441-453;controller/data_logger.py:589-603;runtime/chamber_runtime.py:7033-7037",
         v2='v2/ch12/process_records.py; v2/ch12/outputs/calib_csv.py; v2/ch12/outputs/run_stats.py', test='v2/tests/test_heater_records.py::test_heater_columns_and_summary_from_main_polling', status='done', note=""),
    dict(id="HEAT-092", area="heat",
         desc="공정 끝 히터 요약 한 줄(히터 붙은 채널·표본 있을 때): '히터 요약(메인 공정 구간) — 목표 x°C|히터 꺼짐 · PV 평균 a°C (최소 b · 최대 c) · MV 평균 n · 표본 k개'",
         v1="controller/data_logger.py:455-466;runtime/chamber_runtime.py:1876-1877;runtime/chamber_runtime.py:7073-7080",
         v2='v2/ch12/process_records.py; v2/ch12/outputs/run_stats.py', test='v2/tests/test_heater_records.py::test_heater_columns_and_summary_from_main_polling; v2/tests/test_heater_records.py::test_no_heater_no_summary', status='done', note=""),

    # ───────────────────────── 레시피·공정 스텝 ─────────────────────────
    dict(id="HEAT-093", area="heat",
         desc="레시피 열 use_heater(T/F)·heater_temp(°C)·heater_ramp(°C/min, 비면 기본)·heater_soak(분, 비면 SOAK 없음) 정규화. use_heater=F 인데 값이 있으면 "
              "'use_heater=F 이므로 히터를 쓰지 않습니다 (… 은 무시)' 로그 후 None",
         v1="runtime/chamber_runtime.py:114-117;runtime/chamber_runtime.py:154-157;runtime/chamber_runtime.py:6365-6382",
         v2='v2/ch12/recipe.py', test='v2/tests/test_heater_records.py::test_recipe_heater_columns_and_checks', status='done', note=""),
    dict(id="HEAT-094", area="heat",
         desc="레시피 값 검사(use_heater=T): CH1 이면 'CH1 에는 히터가 없습니다', 서비스 없으면 '히터 기능이 꺼져 있어 …', heater_temp 0 초과 min(MAX_TEMP, SV_LIMIT) 이하, "
              "heater_ramp 0 초과 60 이하, heater_soak 0~480분. 원시 행 검사(E230)에서 heater_* 는 숫자, use_heater 는 T/F 토큰",
         v1="runtime/chamber_runtime.py:385-386;runtime/chamber_runtime.py:8598-8599;runtime/chamber_runtime.py:8622;runtime/chamber_runtime.py:8900-8919",
         v2='v2/ch12/recipe_check.py', test='v2/tests/test_heater_records.py::test_recipe_heater_columns_and_checks', status='done', note=""),
    dict(id="HEAT-095", area="heat",
         desc="스텝: use_heater 면 공정 시작 직후(베이스 압력 대기 전) HEATER_SET('히터 T°C 설정 (r°C/min|기본 램프 속도)', params=(ramp,)) → "
              "HEATER_WAIT('히터 T°C 도달·안정 대기[ (SOAK s분)]', params=(soak×60,)|None), 작업 압력 SP1_ON 뒤·Shutter Delay 전 HEATER_HOLD('히터 유지 모드 측정 대기 (작업 압력·플라즈마 조건)', polling=False). "
              "로그 '히터 사용 — 목표 T°C (…)'",
         v1="controller/process_controller.py:2292-2312;controller/process_controller.py:2596-2602;tests/data/thickness_time_mode_golden.json:948-1299",
         v2='v2/ch12/process_steps.py', test='v2/tests/test_process_steps_golden.py; v2/tests/test_blocks.py', status='done', note="골든 ch2_thk_heater_rf 시나리오"),
    dict(id="HEAT-096", area="heat",
         desc="HEATER_SET/WAIT 는 value 필수(없으면 ValueError), 콜백이 없으면 E830 AppError. 기대 토큰 HEATER_SET/HEATER_READY/HEATER_HOLD, on_heater_failed → _step_failed('HEATER')(종료 절차)",
         v1="controller/process_controller.py:323-326;controller/process_controller.py:351-365;controller/process_controller.py:1018-1035;controller/process_controller.py:1448-1472;controller/process_controller.py:3045",
         v2='v2/ch12/process_steps.py; v2/ch12/process_actions.py', test='v2/tests/test_heater_records.py::test_heater_steps_value_required_and_message; v2/tests/test_process_list.py::test_heater_set_failure_aborts_before_ig', status='done', note=""),
    dict(id="HEAT-097", area="heat",
         desc="HEATER_SET 처리: 서비스 없으면 E830('히터 기능이 꺼져 있습니다…'), is_ready 실패 E830('히터 사용 불가 — …'), 공정이 넘겨받고 맥락 설정, 유지·포화 클램프 금지 → "
              "운전 중이면 apply_target(owner process), 아니면 turn_on. 실패면(RUN 아니면 release) E830 '히터 설정 실패 — …'. 성공이면 감시 시작, 로그 '공정 히터 설정 — 목표 … ({운전 중 목표 변경|운전 시작})'. "
              "취소 세대가 바뀌었으면 아무것도 안 함",
         v1="runtime/chamber_runtime.py:1464-1503", v2="v2/ch12/heater_link.py", test="v2/tests/test_heater_process.py::test_set_rejected_when_interlock_off; v2/tests/test_heater_process.py::test_set_failures_report_e830_and_release_owner; v2/tests/test_heater_process.py::test_take_over_manual_run_uses_apply_target; v2/tests/test_heater_process.py::test_cancel_during_set_does_not_touch_process; v2/tests/test_heater_process.py::test_set_wait_hold_then_fault_in_process", status="done", note=""),
    dict(id="HEAT-098", area="heat",
         desc="HEATER_WAIT 처리: 한도 = heater_wait_timeout_s(현재 PV, 목표, 이번 램프 속도, 기본 HEATER_WAIT_TIMEOUT_SEC, SOAK_TIME) — 올릴 때 감속 접근 포함 예상 + SOAK_TIME, 내릴 때 1°C/min 냉각 가정; "
              "max(기본, 예상+1800초), 상한 43200초. 로그 '히터 승온 대기 — 목표 T°C ±tol°C 를 s초 유지하면 진행|도달, 그 뒤 SOAK …, 한도 m분(근거)'",
         v1="runtime/chamber_runtime.py:1505-1526;controller/heater_process.py:22-24;controller/heater_process.py:68-86",
         v2="v2/ch12/heater_link.py; v2/ch12/heater_process.py", test="v2/tests/test_heater_guards.py::test_soak_waiter_failures_and_timeout_estimate; v2/tests/test_heater_process.py::test_400c_soak_60min_counts_time_after_arrival_with_heater_on; v2/tests/test_heater_process.py::test_blank_soak_uses_setting_and_card_unchanged", status="done", note="한도에 레시피 heater_soak 은 넣지 않음(도달 전까지만 시간 초과를 봄)"),
    dict(id="HEAT-099", area="heat",
         desc="승온 대기 판정: 램프 끝(¬램프, |SV−목표| ≤ 0.05, |D119−목표| ≤ 0.15) & |PV−목표| ≤ HEATER_SOAK_TOLERANCE 가 HEATER_SOAK_TIME_SEC 연속이면 도달. 벗어나면 다시 셈. "
              "진행 로그 60초마다, 도달 전 한도 초과면 E831 '히터 승온 시간 초과(m분) — PV … / 목표 …'",
         v1="controller/heater_process.py:25;controller/heater_process.py:27;controller/heater_process.py:167-197", v2="v2/ch12/heater_process.py",
         test="v2/tests/test_heater_guards.py::test_soak_waiter_failures_and_timeout_estimate; v2/tests/test_heater_process.py::test_400c_soak_60min_counts_time_after_arrival_with_heater_on", status="done", note=""),
    dict(id="HEAT-100", area="heat",
         desc="레시피 SOAK(heater_soak>0): 도달 뒤 시간만 셈(범위 밖·복귀는 로그만, 다시 세지 않음), 끝나면 '히터 준비 완료 — SOAK … 끝, PV … (SOAK 중 a~b°C[, ±tol°C 밖 t]), 대기 m분'. "
              "진행 로그 60초마다. SOAK 중에도 이상·꺼짐·무응답은 실패",
         v1="controller/heater_process.py:115-165;controller/heater_process.py:56-65", v2="v2/ch12/heater_process.py",
         test="v2/tests/test_heater_guards.py::test_waiter_hold_counts_time_and_ignores_arrival_timeout; v2/tests/test_heater_process.py::test_wobble_during_soak_is_recorded_not_restarted; v2/tests/test_heater_process.py::test_heater_trouble_during_soak_still_stops", status="done", note=""),
    dict(id="HEAT-101", area="heat",
         desc="대기 공통 실패: FAULT → E832 '히터 이상 발생', stale 이 60초 이어지면 E830 '히터 PLC 응답 없음 N초'(그 전에는 판정 쉼), RUN 꺼짐 → E830 "
              "('히터 인터락 해제(…) — PLC 가 히터를 끔' 또는 '히터가 꺼짐(공정 밖에서 OFF 또는 PLC 가 RUN 해제)')",
         v1="controller/heater_process.py:26;controller/heater_process.py:29-32;controller/heater_process.py:49-53;controller/heater_process.py:98-112",
         v2="v2/ch12/heater_process.py", test="v2/tests/test_heater_guards.py::test_soak_waiter_failures_and_timeout_estimate; v2/tests/test_heater_process.py::test_interlock_loss_during_wait_e830_names_cause; v2/tests/test_heater_engine.py::test_long_read_fail_after_turn_on_publishes_stale_then_waiter_fails_as_before", status="done", note=""),
    dict(id="HEAT-102", area="heat",
         desc="HEATER_HOLD 처리: 유지 무장·포화 클램프 허용 → 모드 off 면 바로 통과, 아니면 '히터 유지 모드 측정 시작(작업 압력·플라즈마 조건) — 최대 HOLD_WAIT초' 후 대기: "
              "holding 이면 통과 '유지 모드 진입 — DAC 상한 v 고정(n초)', 시작 뒤 포기 횟수 증가면 E833 give_up, 한도 초과면 E833 timeout",
         v1="runtime/chamber_runtime.py:1528-1551;controller/heater_process.py:200-222", v2="v2/ch12/heater_link.py; v2/ch12/heater_process.py",
         test="v2/tests/test_heater_process.py::test_set_wait_hold_then_fault_in_process; v2/tests/test_heater_process.py::test_hold_mode_off_passes_and_rearms_guard; v2/tests/test_heater_process.py::test_hold_give_up_is_reported_once_after_start", status="done", note=""),
    dict(id="HEAT-103", area="heat",
         desc="유지 실패 처리: HOLD_FAIL_ACTION=abort → E833 '… (HEATER_HOLD_FAIL_ACTION=abort)', 포기(출력 상한)면 경고 후 계속('… 상한이 곧 출력 제한 역할을 합니다. 공정을 계속합니다'), "
              "시간 초과면 force_hold → 성공 시 계속, 실패 시 E833 '… — 강제 진입 실패: …'",
         v1="runtime/chamber_runtime.py:6992-6994;runtime/chamber_runtime.py:7082-7107", v2="v2/ch12/heater_link.py",
         test="v2/tests/test_heater_process.py::test_hold_failure_handling", status="done", note=""),
    dict(id="HEAT-104", area="heat",
         desc="공정 중 감시(HEATER_SET 성공 뒤~종료 절차): 무응답·끊김 noresp 가 오면 대기·감시 끝내고 E832 '공정 중 히터 {문구} — 공정을 중단합니다'(일반 RUN 꺼짐보다 먼저). "
              "대기가 없을 때 FAULT 또는 (stale 아님 & RUN 꺼짐) → E832 '공정 중 히터 이상(FAULT)|히터 인터락 해제(…)|히터가 꺼짐(외부 OFF 또는 순간 인터락 해제) — 공정을 중단합니다'",
         v1="runtime/chamber_runtime.py:6955-7006", v2="v2/ch12/heater_link.py", test="v2/tests/test_heater_process.py::test_interlock_loss_in_process_e832_names_cause; v2/tests/test_heater_process.py::test_set_wait_hold_then_fault_in_process; v2/tests/test_heater_process.py::test_runtime_loss_while_watching_fails_once; v2/tests/test_heater_process.py::test_runtime_noresp_during_wait_fails_once_with_cause; v2/tests/test_heater_process.py::test_runtime_manual_heater_trip_does_not_touch_process", status="done", note=""),
    dict(id="HEAT-105", area="heat",
         desc="판정 코드 예외: 대기·감시를 끝내고 traceback 을 HEATER 로그에, 대기 중이었으면 E830 '히터 도달|유지 대기 판정 중 프로그램 오류(…)', 감시 중이었으면 E832",
         v1="runtime/chamber_runtime.py:7008-7022", v2="v2/ch12/heater_link.py", test="v2/tests/test_heater_process.py::test_exception_during_soak_fails_once_e830; v2/tests/test_heater_process.py::test_exception_before_reach_fails_once_e830; v2/tests/test_heater_process.py::test_exception_in_hold_wait_fails_once_e830; v2/tests/test_heater_process.py::test_exception_in_watch_fails_once_e832", status="done", note=""),
    dict(id="HEAT-106", area="heat",
         desc="히터 대기·감시 취소(세대 +1, 대기·감시 해제)는 STOP·실패·종료 절차 시작 때 — 히터 OFF 여부는 공정 종료 후 런타임이 정함",
         v1="runtime/chamber_runtime.py:1553-1556;controller/process_controller.py:782;controller/process_controller.py:792;controller/process_controller.py:1958;controller/process_controller.py:2085",
         v2="v2/ch12/heater_link.py; v2/ch12/process_actions.py", test="v2/tests/test_heater_process.py::test_cancel_during_set_does_not_touch_process; v2/tests/test_process_list.py::test_heater_set_failure_aborts_before_ig", status="done", note=""),
    dict(id="HEAT-107", area="heat",
         desc="공정 1행 끝: 공정 소유 히터만 — 큐 연속 실행·성공·정지 아님이고 다음 '공정' 행(delay 행 건너뜀, TEST 행은 안 씀)이 use_heater=T 면 유지('다음 공정도 히터를 씀 → 히터 유지…'), "
              "아니면 끄기(RUN 이면 turn_off(사유, owner process) '공정이 히터를 끕니다 — 사유', 아니면 release)",
         v1="runtime/chamber_runtime.py:4988-4994;runtime/chamber_runtime.py:7109-7121;runtime/chamber_runtime.py:7166-7184",
         v2="v2/ch12/heater_link.py", test="v2/tests/test_heater_process.py::test_next_row_lookahead; v2/tests/test_heater_process.py::test_after_row_keep_or_off; v2/tests/test_heater_process.py::test_after_row_ignores_manual_heater; v2/tests/test_process_list.py::test_queue_keeps_heater_between_heater_rows", status="done", note=""),
    dict(id="HEAT-108", area="heat",
         desc="공정이 끌 때 종료 사유: SUCCESS → '공정 종료', STOP → '공정 중단 — 사용자 STOP'(사용자) / '공정 중단 — 원격 STOP', 오류 코드 있으면 '공정 중단 — 오류 (Exxx 제목)', "
              "없으면 '공정 중단 — 오류 (상세 40자)'",
         v1="runtime/chamber_runtime.py:4789;runtime/chamber_runtime.py:7151-7164", v2="v2/ch12/heater_link.py",
         test="v2/tests/test_heater_cards.py::test_process_end_cards; v2/tests/test_heater_cards.py::test_process_fail_without_code_uses_detail_40", status="done", note=""),
    dict(id="HEAT-109", area="heat",
         desc="큐 종료 안전망: 큐가 끝났는데 공정이 히터를 쥐고 있으면 끈다(사유 '공정 큐 종료') — 실패·정지·검증 실패·시작 실패 경로 공통",
         v1="runtime/chamber_runtime.py:7745-7749", v2="v2/ch12/heater_link.py", test="v2/tests/test_heater_process.py::test_queue_end_safety_net_turns_off", status="done", note=""),
    dict(id="HEAT-110", area="heat",
         desc="히터를 쓰지 않는 공정 시작 때 히터가 운전 중이면 1줄 '히터가 운전 중이지만 이 공정은 히터를 쓰지 않습니다(use_heater=F) — 히터는 그대로 둡니다'",
         v1="runtime/chamber_runtime.py:4086-4090", v2="v2/ch12/heater_link.py", test="v2/tests/test_heater_process.py::test_process_started_without_heater_logs_once", status="done", note=""),
    dict(id="HEAT-111", area="heat",
         desc="히터 공정 연동은 CH2 만(attach_heater: CH1 이면 'CH1 에는 히터를 붙이지 않습니다'), 붙으면 'CH2 히터 공정 연동 준비(레시피 use_heater)'. 대기 판정은 서비스 시계(svc.clock) 기준",
         v1="runtime/chamber_runtime.py:6946-6953;controller/heater_service.py:173-176", v2="v2/ch12/heater_engine.py; v2/ch12/heater_setup.py; v2/ch12/assembly.py",
         test="v2/tests/test_inv_heat.py::test_heat111_start_logs; v2/tests/test_heater_records.py::test_init_ch2_heater_rules", status="done", note="조립이 CH2 공정 엔진에만 heater 를 넘긴다. 대기 판정 시계 = 엔진 clock"),
    dict(id="HEAT-112", area="heat",
         desc="오류 코드: E830 히터 사용 불가, E831 히터 승온 시간 초과, E832 공정 중 히터 이상, E833 히터 유지 모드 진입 실패(category HEATER, SEV_ERROR)",
         v1="errors/error_registry.py:1594-1640", v2="v2/ch12/error_codes.py", test="v2/tests/test_errors.py::test_registry_same_as_v1_golden; v2/tests/test_heater_guards.py::test_soak_waiter_failures_and_timeout_estimate", status="done", note=""),

    # ───────────────────────── 패널 명령(서비스 쪽 의미) ─────────────────────────
    dict(id="HEAT-113", area="heat",
         desc="패널 ON/OFF: RUN 이면 turn_off('정지(패널)', owner='manual'), 아니면 목표 검사 후 turn_on(목표, owner='manual')(속도 = 설정 기본). 버튼 표시는 RUN 만 따름",
         v1="ui/heater_panel.py:370-380", v2="v2/ch12/heater_commands.py; v2/ch12/core.py", test="v2/tests/test_heater_process.py::test_panel_can_still_stop_external_run; v2/tests/test_core.py::test_args_rejected_with_detail", status="done", note="명령 HEATER_ONOFF — 화면은 포털 Ch12Aux 히터 칸"),
    dict(id="HEAT-114", area="heat",
         desc="패널 [적용]: apply_target(목표) — owner 를 넘기지 않아 공정 잠금이 걸리지 않음(공정 소유 운전 중에도 목표가 바뀜)",
         v1="ui/heater_panel.py:382-387;controller/heater_service.py:299-302;controller/heater_service.py:392-393", v2="v2/ch12/heater_commands.py; v2/ch12/core.py",
         test="v2/tests/test_heater_process.py::test_panel_apply_changes_process_target_v1_anomaly", status="done", note="알려진 이상 동작 — v1 과 같게 두고 결정 목록에(사용자 확인 필요) — D144 v1 그대로(사용자 확인 필요)"),
    dict(id="HEAT-115", area="heat",
         desc="패널 [리셋]: RUN 이면 '리셋 불가' 경고, 확인창('히터 이상을 리셋합니다. 원인(배선·TC 모듈·과온)을 확인했습니까? …') 예 → reset_fault. "
              "명령 결과가 (True, 문구)여도 문구를 경고로 띄움(보류 OFF 알림)",
         v1="ui/heater_panel.py:340-368;ui/heater_panel.py:389-401", v2="v2/ch12/heater_commands.py; v2/ch12/core.py", test="v2/tests/test_inv_heat.py::test_heat115_panel_reset_refused_while_run; v2/tests/test_heater_engine.py::test_reset_refused_while_run", status="done", note="확인창은 포털 명령 확인(onAsk). 결과 문구는 명령 결과로"),

    # ───────────────────────── 래더 동작 모델(시험 하니스) ─────────────────────────
    dict(id="HEAT-116", area="heat",
         desc="래더 워치독(H2): RUN 중 D114 가 D115(이전 값)와 같으면 타이머 누적, 30초 이상이면 WD_ERR(M646) → FAULT → RUN 리셋·출력 0. 값이 바뀌면 D115=D114, 타이머 0",
         v1="tests/heater_ladder_sim.py:27;tests/heater_ladder_sim.py:110-115;tests/heater_ladder_sim.py:179-188",
         v2="v2/ch12/sim/plc_ladder.py", test="v2/tests/test_inv_heat.py::test_heat116_ladder_watchdog_30s; v2/tests/test_plc.py::test_sim_heater_ladder_watchdog_30s", status="done", note=""),
    dict(id="HEAT-117", area="heat",
         desc="래더 H0 기본값·클램프: 0 이면 D120=2, D121=100, D113=5000, D126=5500, D128=10, D118=520; D113 ≤ 9000, D126 ≤ 9500, D126 ≤ D113 이면 D113+300, D118 ≤ 1600",
         v1="tests/heater_ladder_sim.py:91-101", v2="v2/ch12/sim/plc_ladder.py", test="v2/tests/test_inv_heat.py::test_heat117_ladder_h0_defaults_and_clamps; v2/tests/test_heater_sim.py::test_ladder_h0_accepts_900_950_limits", status="done", note=""),
    dict(id="HEAT-118", area="heat",
         desc="래더 H1~H5: RUN & ¬TC_OK → TC_ERR, TC_OK & PV > D126 → OT, FAULT = OT|TC_ERR|WD_ERR, ITL = TC_OK·DA_OK·WATER_OK·VAC_OK·¬FAULT, RST → OT/TC/WD 래치 해제(자기 리셋)",
         v1="tests/heater_ladder_sim.py:103-120", v2="v2/ch12/sim/plc_ladder.py", test="v2/tests/test_inv_heat.py::test_heat118_ladder_h1_h5", status="done", note=""),
    dict(id="HEAT-119", area="heat",
         desc="래더 H6 램프(D119 가 min(D112,D113) 로 1초마다 D120(감속 구간 D127 안이면 D128) 카운트씩, 홀드백 D121 넘게 PV 보다 앞서지 않음, RUN 아니면 D119=PV), "
              "H7 PID(정동작, MV 400~D118, 포화 방향 적분 정지, Kp/Ti 범위 밖이면 3.0/300), H9(¬RUN·FAULT·¬ITL → 출력 0, ¬ITL·FAULT → RUN 리셋)",
         v1="tests/heater_ladder_sim.py:80-89;tests/heater_ladder_sim.py:122-191", v2="v2/ch12/sim/plc_ladder.py",
         test="v2/tests/test_inv_heat.py::test_heat119_ladder_ramp_pid_and_cutoff", status="done", note=""),
    dict(id="HEAT-120", area="heat",
         desc="열 모델(시험용 값): dT/dt = (2250·f + plasma − (T−25)(1+gas_loss))/900, f = (min(D141,D118) − 400)/1200. 쓰기마다 dt=0 스캔, 하니스는 1초 스캔·5초 워치독",
         v1="tests/heater_ladder_sim.py:7-10;tests/heater_ladder_sim.py:22-26;tests/heater_ladder_sim.py:189-224;tests/heater_ladder_sim.py:251-261",
         v2="v2/ch12/sim/plc_ladder.py", test="v2/tests/test_inv_heat.py::test_heat120_thermal_model", status="done", note="실제 CH2 히터 값이 아님(시험 재현용) — 시험 재현용 값(실제 CH2 히터 값 아님)"),

    # ───────────────────────── v1 안에서 쓰이지 않는 것 ─────────────────────────
    dict(id="HEAT-121", area="heat",
         desc="HeaterService.claim(owner) — 주체가 비었거나 같으면 차지",
         v1="controller/heater_service.py:189-193", v2="v2/ch12/heater_engine.py", test="", status="na",
         note="v1 안에서 호출하는 곳 없음(grep '.claim(' — tests 외 없음)"),
    dict(id="HEAT-122", area="heat",
         desc="solve_rate_for_minutes(span, minutes) — 정해진 시간에 올리는 본 구간 속도 역산",
         v1="lib/heater_profile.py:52-73", v2="v2/ch12/heater_engine.py", test="", status="na",
         note="v1 안에서 호출하는 곳 없음(grep — 정의만)"),
    dict(id="HEAT-123", area="heat",
         desc="HeaterHold.engaged_relaxed 플래그(강제 진입이면 True)",
         v1="controller/heater_hold.py:56;controller/heater_hold.py:160;controller/heater_hold.py:292", v2="v2/ch12/heater_engine.py",
         test="", status="na", note="값을 읽는 곳 없음(grep — 대입만)"),
]
