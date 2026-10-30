"""v1 전수 목록 — 빠진 항목 찾기 B(gap).

방법: 전수 목록(inv_*.py)과 v2/ 를 보지 않고 v1 코드만 읽어 관찰 가능한 동작 752개를 독립적으로 적은 뒤
(runtime/ 전부(chamber_runtime.py 포함), controller/ 전부, main.py, ui/ 전부 — 기준 커밋 66efc7b),
all_items()(대조 시점 2,375개)와 줄 근접·낱말 겹침·정확한 문구 탐색으로 맞춰 보고, 어디에도 없는 동작만 여기에 둔다.
대부분은 이미 PROC·PC·HEAT·THK·LOG·CHAT·UI 영역에 있었다. 여기 남은 것은 주로 chamber_runtime 의
장치 기동·설정 재적용·기록 파라미터 갱신·태스크 수명·로그 문구다.
죽은 경로(비상 정지·리셋 로그, 병렬 Pre-Sputter 결과 문구)와 문구만 다르고 뜻이 같은 것은 넣지 않았다.
"""
from __future__ import annotations

ITEMS = [
    dict(id="GAPB-001", area="gap",
         desc="챔버 장치 자동 기동(_start_devices_task): 순서 PLC → MFC → IG → (이번 런에 고른 경우만) DC Pulse → RF Pulse. "
              "장치마다 이미 연결이면 '{라벨} already connected → skip', start(없으면 connect) 메서드가 없으면 "
              "'start/connect 메서드 없음 → skip'. 호출은 asyncio.to_thread 로 하고 DEVICE_START_TIMEOUT_S(기본 20초) 제한 — "
              "넘으면 '{메서드} timeout(20.0s) → skip', 결과가 awaitable 이면 같은 제한으로 await('… await timeout(..s) → skip'), "
              "예외는 '{메서드} 호출 실패: {e!r}', 성공은 '{메서드} 호출 완료'. 어느 경우든 공정을 막지 않고 다음 장치로 간다. "
              "PLC 는 두 챔버가 공유하므로 PLC 소유 챔버(_owns_plc)만 로그를 남기고 다른 챔버는 조용히 시도한다",
         v1="runtime/chamber_runtime.py:2972-3048",
         v2="v2/ch12/process_engine.py", test="", status="todo",
         note="DEV_MODE 생략은 CFG-185, 펄스 선택은 PROC-093 에 있음 — 시간 제한·skip 규칙·순서·PLC 로그 소유가 빠져 있었다"),
    dict(id="GAPB-002", area="gap",
         desc="ChamberRuntime 설정 재적용(reload_runtime_cfg, 설정 Apply): 공정 실행 중이거나 러너가 IDLE 이 아니면 "
              "'Apply(Runtime): 공정/정리 진행 중이므로 ChamberRuntime 고정 캐시는 다음 공정부터 반영됩니다.' 로그 후 지원 플래그는 손대지 않음. "
              "IDLE 이면 SUPPORTS_DC_CONT(별칭 SUPPORTS_DC)·SUPPORTS_DC_CONT2(SUPPORTS_DC2, CH/히터 조건 _dc2_allowed 적용)·"
              "SUPPORTS_RF_CONT·SUPPORTS_DC_PULSE(SUPPORTS_DCPULSE)·SUPPORTS_RFPULSE(SUPPORTS_RF_PULSE) 를 다시 읽되, "
              "장치 객체가 없는데 ON 으로 바꾼 항목은 반영하지 않고(실효 False) "
              "'{DC 연속파|DC 연속파 2호기|RF 연속파|DC-Pulse|RF-Pulse} 지원 ON 요청은 현재 runtime에 {객체} 객체가 없어 즉시 반영하지 않습니다. 프로그램 재시작 후 반영됩니다.' "
              "로그. OFF 는 즉시 반영. 결과를 ProcessController 의 supports_* 에도 같이 넣고 "
              "'ChamberRuntime 설정 캐시 갱신 완료 (…)' 로그, 예외면 'reload_runtime_cfg 실패: {e!r}'",
         v1="runtime/chamber_runtime.py:866-962",
         v2="v2/ch12/params.py", test="", status="todo", note=""),
    dict(id="GAPB-003", area="gap",
         desc="펄스 시작 성공 뒤 장비 실측 펄스 값 되읽기(read_actual_pulse_params)를 기록 파라미터(data_logger.process_params)에 덮어써 "
              "Calib CSV·GDrive·채팅이 실제 적용값을 쓰게 한다. DC Pulse: dc_pulse_freq=freq_khz, dc_pulse_duty·dc_pulse_duty_cycle=duty_pct, "
              "dc_pulse_off_time_us=off_time_us, dc_pulse_off_time=off_time_us(None 이면 'DC') + 로그 "
              "'[실측] f={kHz}kHz, off={x.x}us|DC, duty={%}%'. RF Pulse: rf_pulse_freq·rf_pulse_duty_cycle·rf_pulse_off_time_us 만 덮어쓰고 로그 없음. "
              "되읽기 실패·빈 결과는 조용히 무시(공정 계속)",
         v1="runtime/chamber_runtime.py:1173-1195; runtime/chamber_runtime.py:1294-1302",
         v2="v2/ch12/process_engine.py", test="", status="todo",
         note="LOG-145 는 '실측 dc_pulse_off_time_us 우선' 만 적음 — 덮어쓰는 시점·키·RF 쪽·로그가 없었다"),
    dict(id="GAPB-004", area="gap",
         desc="공정 PLC 명령 콜백마다 요청·완료 로그와 소요시간: 시작 '[CHn] 요청: {NAME} -> {True|False} (raw='{원문}', ch=n)', "
              "확인 뒤 '[CHn] 완료: {NAME} -> {True|False} ({ms:.0f} ms)'(루프 시간 기준). 예외면 소요시간을 잰 뒤 on_plc_failed 로 넘김",
         v1="runtime/chamber_runtime.py:970-1105",
         v2="v2/ch12/process_engine.py", test="", status="todo",
         note="명령 라우팅(MV·가스·MS·CH1 무시)은 PROC-208 등에 있음 — 요청/완료 로그와 ms 계측만 빠져 있었다"),
    dict(id="GAPB-005", area="gap",
         desc="장치 이벤트 펌프(Pump.*) 태스크는 상주(keep-alive): 공정 종료 정리(_bg_tasks 일괄 취소) 대상에 넣지 않는다. "
              "같은 이름이 살아 있으면 만들지 않고(다른 스레드에서 부르면 call_soon_threadsafe 로 예약 후 그 시점에 다시 중복 확인), "
              "끝나면 목록에서 빠져 다음 ensure 때 다시 만든다. 예외로 죽으면 'Task{ch}' 로그 '[{name}] crashed:\\n{traceback}', "
              "생성 실패는 '[{name}] create_task failed:\\n{traceback}'. 취소로 끝난 건 로그 없음",
         v1="runtime/chamber_runtime.py:2855-2910",
         v2="v2/ch12/core.py", test="", status="todo",
         note="v1 주석: 펌프를 정리 때 취소하면 다음 공정 StartAfterPreflight 가 안 잡히는 문제가 있어 상주로 바꿨다"),
    dict(id="GAPB-006", area="gap",
         desc="장치 폴링 값 로그 문구(공정 로그에 매번): MFC '[poll] {Ar|O2|N2}: {x.xx} sccm'(출처 MFC{n}), "
              "DC 연속 '측정: {P:.1f} W, {V:.1f} V, {I:.3f} A'(출처 DC1), RF 연속 '[poll] fwd={x.x}W, ref={x.x}W'(출처 RF{n}), "
              "DC Pulse '[telemetry] P={x.x} W, V={x.xx} V, I={x.xxx} A' + 원시값이 있으면 ' (raw P/I/V={P}/{I}/{V})'(출처 DCPulse{n}; "
              "앞부분 P=/V=/I= 형식은 기존 분석 스크립트 호환용으로 유지)",
         v1="runtime/chamber_runtime.py:2225-2226; runtime/chamber_runtime.py:2362-2363; runtime/chamber_runtime.py:2479-2480; runtime/chamber_runtime.py:2626-2633",
         v2="v2/ch12/outputs/", test="", status="todo",
         note="'[poll] ChamberP'(PROC-215)·IG '[poll] … Torr'(PC-284)는 있었다"),
    dict(id="GAPB-007", area="gap",
         desc="RF Pulse 접속 주소 폴백: RFPULSE_TCP_HOST·RFPULSE_TCP_PORT 가 둘 다 있으면 그것, 하나라도 없으면 레거시 RFPULSE_PORT "
              "('ip:port' 문자열 또는 (host, port) 튜플/리스트)를 써서 프리플라이트 전에 rf_pulse.set_endpoint(host, port) 로 덮는다. "
              "둘 다 없거나 해석 실패면 None(덮지 않음)",
         v1="runtime/chamber_runtime.py:350-380; runtime/chamber_runtime.py:676-688",
         v2="v2/ch12/params.py", test="", status="todo",
         note="DEV-179 는 '포트·호스트 미정의면 RuntimeError' 만 적음"),
]
