"""v1 전수 목록 — 영역 ui(화면 동작).

이 영역이 덮는 것
- v1 노트북 화면(PySide6) 의 관찰 가능한 동작 전부: 버튼·입력칸·라디오·체크박스마다 하는 일, 확인창·알림창(제목·문구·자동 닫힘),
  화면 갱신 주기(로그창 100 ms, 상태 문구 1 s 카운트다운, 그래프 쓰로틀 0.5 s, 서버 로그 파일 1 s 등),
  프리스퍼터 예약(라디오·Reserve/Cancel·전용 레시피·QSettings 저장/복원·상태 문구), Power Select 버튼,
  런타임 덤프(State Dump 버튼·종료 차단 시 자동 덤프·파일 이름·내용), PC(플라즈마 클리닝) 챔버 선택 라디오,
  CH2 히터 패널, 두께 모니터 패널, Factor 설정창·재료 목록창, 레시피 파일 선택(공용 파일 열기 창),
  RGA·OES 그래프, 로그창(형식·줄 수·자동 스크롤·출처별 배달), 종료(closeEvent) 조건과 정리 순서,
  시작 시 동작(settings.json 검사·로깅·자동 PLC 연결·호스트 서버 자동 시작·자기 감시),
  서버 페이지(메인 runtime/server_page.py, 별도 로봇 서버 앱 apps/robot_server/runtime/server_page.py).
- 수동(UI) 단일 공정 시작 때 화면 칸에서 공정 인자를 만드는 규칙과 입력 검사 알림창 문구도 여기 둔다
  (공정 순서·인터락 자체는 proc 영역, 레시피 행 검사 E230 은 recipe/proc 영역이 덮는다).
- 다른 영역과 겹치는 main.py 조립부(구글챗 알림기 생성·PLC 링크 카드·카메라 레코더·장비 생성)는
  빠지지 않도록 항목만 두고 note 에 겹침을 적었다.
- PC 페이지의 START/STOP·File Path 버튼과 TSP 페이지 버튼 동작은 runtime/plasma_cleaning_runtime.py·runtime/tsp_runtime.py
  안에 있어 pc/tsp 영역 목록이 덮는다(여기서는 main.py 가 넣는 기본값·챔버 선택만).

v2 담당
- 명령: v2/ch12/core.py·v2/ch12/commands.py(화면 버튼 = 명령, 출처 local/web).
- 화면: 포털 vanam-process components/ops2/ch12 (v2/hmi 는 같은 화면을 local 모드로 빌드한 것).
- 그 밖에 MODULES 의 담당 모듈(pre_sputter.py·heater_engine.py·thickness.py·robot_server/·app.py 등).

읽은 v1 파일(전부 끝까지)
- main.py (1~2145)
- ui/main_window.py, ui/heater_panel.py, ui/thickness_panel.py, ui/thickness_factor_dialog.py, ui/thickness_material_dialog.py
- runtime/server_page.py, apps/robot_server/runtime/server_page.py (+ apps/robot_server/main_server.py 의 페이지 생성부)
- util/file_pick.py, util/runtime_dump.py, util/timed_popup.py, util/settings_error_box.py
- runtime/pre_sputter_runtime.py (프리스퍼터 예약 동작)
- controller/graph_controller.py (그래프)
- runtime/chamber_runtime.py 의 화면 연결부(grep: connect·QMessageBox·QTimer·_post_warning·_post_critical·_u(·setPlainText·_set_state_text)
  — 543~598, 1690~1700, 2075~2095, 3045~3620, 3875~3950, 4090~4105, 4355~4525, 5810~6160, 6455~6515, 7180~7320, 7740~7990,
  8370~8530, 8920~8951
- 확인용으로 일부만: controller/process_controller.py(상태 문구), device/plc.py(Power Select 코일), lib/config_common.py(기본값),
  lib/heater_config.py·lib/thickness_config.py(폴링 주기), lib/thickness_factors.py·device/thickness_monitor.py(범위)
"""
from __future__ import annotations

_P = "vanam-process components/ops2/ch12"
_CMD = "v2/ch12/core.py; v2/ch12/commands.py"
_CP = f"{_CMD}; {_P}"

ITEMS = [
    # ───────────────────────────── 시작 시 동작 ─────────────────────────────
    dict(id="UI-001", area="ui",
         desc="시작 맨 앞에서 settings.json 을 검사·적용(check_and_load_settings). 고칠 곳이 1건이라도 있으면 값을 하나도 적용하지 않고 "
              "알림창(Critical, 제목 '설정 파일 오류 — 프로그램을 시작하지 않습니다', 본문 'settings.json 에 고칠 곳이 N건 있습니다…(장비에는 아직 "
              "연결하지 않았습니다)', 상세에 '파일: 경로'+처음 15건, 15건 초과분은 '자세히 보기')을 exec 로 띄우고 종료 코드 1(장비 연결 전). "
              "검사 중 예외도 고칠 곳 1건으로 취급. 성공 시 'settings.json 적용: 경로 (N개 값)' INFO 로그. MainWindow 생성 때 load_settings() 를 한 번 더 부른다",
         v1="main.py:2048-2078; main.py:150-158; util/settings_error_box.py:10-27",
         v2="v2/ch12/app.py; v2/ch12/params.py", test="", status="todo",
         note="v2 는 설정 저장소 검사로 대체 — 시작 거부 동작은 같게"),
    dict(id="UI-002", area="ui",
         desc="앱 로깅 설정: app_name 'CH_1_2_program', 파일 INFO 까지, 콘솔 끔. 전역 예외 훅·warnings·시그널·Qt 메시지·asyncio 예외 로깅 설치, "
              "aboutToQuit 에 'QApplication.aboutToQuit emitted' WARNING·Qt 로그 핸들러 원복·loop.stop(call_soon_threadsafe) 연결",
         v1="main.py:2059-2070; main.py:2080-2109; main.py:2129-2134",
         v2="v2/ch12/app.py", test="", status="todo", note=""),
    dict(id="UI-003", area="ui",
         desc="Windows 에서는 import 전에 WindowsSelectorEventLoopPolicy 고정, __main__ 에서 multiprocessing.freeze_support() 호출. "
              "개발 실행 시 프로그램 폴더를 sys.path 맨 앞에 넣는다",
         v1="main.py:17-24; main.py:2139-2144",
         v2="v2/ch12/app.py; v2/ch12/winshell.py", test="", status="todo", note="윈도우 확인 필요"),
    dict(id="UI-004", area="ui",
         desc="asyncio 기본 executor 를 ThreadPoolExecutor(max_workers=32, thread_name_prefix='AppIO') 로 교체(qasync 기본 10개 대신). 실패는 무시",
         v1="main.py:2093-2099",
         v2="v2/ch12/system.py", test="", status="todo", note=""),
    dict(id="UI-005", area="ui",
         desc="창 show 뒤 자기 감시(start_self_watch) 시작 — 알림기는 chat_plc, 없으면 chat_ch1, 로그는 WARNING. aboutToQuit 에 stop_self_watch. 실패는 무시",
         v1="main.py:2114-2122",
         v2="v2/ch12/snapshot_fields.py; v2/supervisor/", test="", status="todo",
         note="지시 3장(2): selfwatch 는 1초 행의 프로그램 상태로 대체(기록 출력 selfwatch 는 log 영역)"),
    dict(id="UI-006", area="ui",
         desc="단일 실행(중복 실행 방지) 검사 없음 — 두 번 켜면 두 번 뜬다(호스트 포트 50070 바인드는 두 번째가 실패 → 'Host start failed' 로그)",
         v1="main.py:1-2145(검사 코드 없음); main.py:1890-1894",
         v2="v2/ch12/app.py", test="", status="na",
         note="v1 에 기능 없음(grep CreateMutex/QLockFile/QSharedMemory 결과 없음) — 기록용. v2 에서 추가하면 decisions 에"),
    dict(id="UI-007", area="ui",
         desc="부팅 직후 공용 PLC 자동 연결 → '부팅 시 자동 연결 성공' / '부팅 시 자동 연결 실패: {e}' 방송 로그. 종료가 시작되지 않았으면 "
              "PLC 코일 CSV 로거 시작(주기 PLC_COIL_LOG_INTERVAL_S 기본 5.0 s, NAS/로컬 경로는 설정값) → 'PLC COIL CSV ... ({iv:g}s)' / "
              "'PLC COIL CSV 로깅 시작 실패: {e!r}'",
         v1="main.py:335-365; lib/config_common.py:664",
         v2="v2/ch12/system.py; v2/ch12/drivers/plc.py; v2/ch12/outputs/", test="", status="todo",
         note="코일 CSV 기록 자체는 log 영역. 지시: 1초 스냅샷으로 통합"),
    dict(id="UI-008", area="ui",
         desc="PLC 연결 상태 변화 콜백 → 'PLC' 출처로 방송 로그만(detail 문구). 링크 끊김/재연결 챗 카드는 link_state 발송기가 단독으로 보낸다",
         v1="main.py:285-289",
         v2="v2/ch12/drivers/plc.py; v2/ch12/hub.py", test="", status="todo", note=""),
    dict(id="UI-009", area="ui",
         desc="PLC 링크 카드: 끊김 '[CH1&2] PLC 연결 끊김 (ip:port) | 감지 HH:MM:SS | 사유=… [| op=…]'(FAIL·urgent), 재연결 '…PLC 재연결 성공 (ip:port) "
              "| 끊김 N.N초 [| 끊긴 동안 억제된 연결 알림 N건 (…)] [| 진단=…]'(SUCCESS·urgent). 전용 웹훅(CHAT_WEBHOOK_PLC_DISCONNECT_URL, 즉시 전송), "
              "워커 스레드에서 와도 루프 스레드로 넘겨 보냄",
         v1="main.py:234-243; main.py:291-328",
         v2=_P, test="", status="todo", note="chat 영역과 겹침 — 구글챗은 포털이 보낸다"),
    dict(id="UI-010", area="ui",
         desc="구글챗 알림기를 용도별로 만든다: HOST·CH1·CH2·PC·TSP(CHAT_WEBHOOK_URL 이 있을 때만), PLC 끊김 전용, 클라이언트(로봇) 요청 공정 전용"
              "(CHAT_WEBHOOK_CLIENT_PROCESS_URL, host_process_log 이벤트 싱크로 시작/종료/에러 카드, 루프 스레드로 넘김). 종료 때 shutdown",
         v1="main.py:213-276; main.py:1439-1450",
         v2=_P, test="", status="todo", note="chat 영역과 겹침"),
    dict(id="UI-011", area="ui",
         desc="공유 장비 생성: MFC1(기본 192.168.1.50:4003)·MFC2(:4006)·IG1(:4001)·IG2(:4002), 채널 설정값 우선. MFC enable_verify=False, "
              "enable_stabilization=True, 스케일 기본 CH1 {1:1.0,2:1.0,3:10.0}·CH2 {1:1.0,2:10.0,3:2.0}, resource_key 'MFC1'/'MFC2'. "
              "CH2 런타임은 supports_rf_cont=True 강제",
         v1="main.py:395-449; main.py:456-484",
         v2="v2/ch12/system.py; v2/ch12/drivers/mfc.py; v2/ch12/drivers/ig.py", test="", status="todo", note="dev 영역과 겹침"),
    dict(id="UI-012", area="ui",
         desc="카메라 레코더 1개(camera_index=1, interval=1.0 s)를 CH1·CH2·PC 런타임이 같이 씀. 알림 콜백은 'CAM' 방송 로그 + chat_host.notify_error_with_src('CAM')+flush"
              "(데몬 스레드 → 루프 스레드). 'CameraRecorder 초기화 성공' / '…실패: {e!r}' 로그",
         v1="main.py:570-597",
         v2="v2/ch12/drivers/camera.py; v2/ch12/shared.py", test="", status="todo", note="dev 영역과 겹침"),
    dict(id="UI-013", area="ui",
         desc="PC 런타임 조립: gas MFC 는 항상 MFC1, 압력·IG 는 처음엔 CH1. GDRIVE_LOG_DIR(기본 'G:/공유 드라이브/VanaM_Sputter/Process_log') 주입, "
              "PC 완료 콜백, '같은 CH 챔버가 바쁜지(is_busy)' 확인 함수 주입(실패 로그 '챔버 상태 확인 함수 주입 실패'). 초기화 실패 시 pc=None·"
              "'Failed to initialize PlasmaCleaningRuntime' 로그",
         v1="main.py:556-622; lib/config_common.py:831",
         v2="v2/ch12/plasma_cleaning.py; v2/ch12/system.py", test="", status="todo", note=""),
    dict(id="UI-014", area="ui",
         desc="TSP 컨트롤러 생성: host/port=TSP_TCP_HOST/PORT, 주소 TSP_RS232_ADDR → TSP_ADDR → 기본 0x80, TSP 전용 알림기, 로그 루트 전달",
         v1="main.py:624-634",
         v2="v2/ch12/tsp_engine.py; v2/ch12/drivers/tsp.py", test="", status="todo", note=""),
    dict(id="UI-015", area="ui",
         desc="호스트(로봇) 서버를 부팅 때 자동 시작(HOST_SERVER_HOST 0.0.0.0 : HOST_SERVER_PORT 50070). 성공 → 서버 페이지 RUNNING·주소 표시, "
              "NET 'Host started on host:port', 호스트 요청 공정 CSV 미마무리 복구(startup_recover)를 데몬 스레드 'HostLogStartupRecover' 로. "
              "실패 → STOPPED·'Host start failed: {e!r}'. 이미 떠 있으면 'Host already running'",
         v1="main.py:642-682; main.py:1841-1894; lib/config_common.py:35-36",
         v2="v2/robot_server/; v2/ch12/robot_api.py; v2/ch12/app.py", test="", status="todo", note=""),

    # ───────────────────────────── 로그 배달·로그창 ─────────────────────────────
    dict(id="UI-016", area="ui",
         desc="호스트 서버 로그(netlog): 태그를 대문자로 시스템 로그(WARNING+)·서버 페이지에 쓰고, 태그가 PLC_HOST·PLC_REMOTE·NET·HOST 이거나 '/HOST' 로 "
              "끝나면 서버 페이지에만 남긴다. 그 밖의 태그는 CH1·CH2·PC 로그창에도 방송",
         v1="main.py:651-681",
         v2=f"v2/robot_server/; {_P}", test="", status="todo", note=""),
    dict(id="UI-017", area="ui",
         desc="PLC 로그 배달 순서: 본문이 '[x]' 로 시작하면 출처 x. ① ContextVar 출처(CH1/CH2/PC 프록시)면 그 창으로만 'PLC' ② 본문에 [CHn]·CH=n 힌트"
              "(정규식 \\[CH\\s*(\\d)\\]|\\bCH(?:=|:)?\\s*(\\d)\\b)면 그 챔버 ③ PLC 소유 챔버(공정 중인 챔버) ④ PC 실행 중이면 PC 창에 'PLC(Global)' "
              "⑤ 아니면 SYSTEM 일자 로그 + CH1·CH2 창에 'PLC(Global)'",
         v1="main.py:91-132; main.py:330-333; main.py:696-765",
         v2="v2/ch12/hub.py; v2/ch12/shared.py", test="", status="todo", note=""),
    dict(id="UI-018", area="ui",
         desc="PLC 로그 소유 챔버: 챔버 런타임의 running 값이 실제로 바뀔 때만 on_plc_owner(ch 또는 None) 호출 → 각 런타임 set_plc_log_owner",
         v1="main.py:685-688; runtime/chamber_runtime.py:3258-3267",
         v2="v2/ch12/shared.py", test="", status="todo", note=""),
    dict(id="UI-019", area="ui",
         desc="방송 로그(_broadcast_log): SYSTEM 일자 로그 + 시스템 로그(레벨 판정 classify_log_level(출처, 본문), WARNING 이상만 '[src] msg') + "
              "CH1·CH2·PC 로그창 모두",
         v1="main.py:767-805",
         v2="v2/ch12/hub.py; v2/ch12/outputs/", test="", status="todo", note=""),
    dict(id="UI-020", area="ui",
         desc="챔버 로그창 줄 형식 화면 '[HH:MM:SS] [CHn:src] msg', 파일 '[YYYY-MM-DD HH:MM:SS] [CHn:src] msg'. 공정 로그 파일이 없으면 프리스타트 버퍼"
              "(최대 1000줄)와 SYSTEM 일자 로그에 남김",
         v1="runtime/chamber_runtime.py:7186-7202; runtime/chamber_runtime.py:562",
         v2=f"v2/ch12/hub.py; {_P}", test="", status="todo", note="log 영역과 겹침(파일 쪽)"),
    dict(id="UI-021", area="ui",
         desc="챔버 로그창 갱신: QTimer 100 ms 마다 버퍼(deque 최대 5000, 넘치면 오래된 것 버림)에서 최대 200줄씩 한 번에 붙인다. 창 최대 2000줄, Undo 끔. "
              "사용자가 바닥(최대값-2 이내)을 보고 있을 때만 바닥으로 자동 스크롤, 아니면 스크롤 위치 유지",
         v1="runtime/chamber_runtime.py:581-598; runtime/chamber_runtime.py:3331-3332; runtime/chamber_runtime.py:7226-7308",
         v2=_P, test="", status="todo", note=""),
    dict(id="UI-022", area="ui",
         desc="화면 전용 로그(append_side_log): 화면과 SYSTEM 일자 로그에만, 공정 로그 파일·프리스타트 버퍼에는 넣지 않음(두께 모니터 연결·오류 문구용)",
         v1="runtime/chamber_runtime.py:7204-7210",
         v2="v2/ch12/hub.py", test="", status="todo", note=""),
    dict(id="UI-023", area="ui",
         desc="CH 로그 폴더(NAS) mkdir 실패 시 로그창에 '[Logger] NAS 폴더 접근 실패 → 실제 기록 시 CHn 로컬 폴백 예정: {경로}' 한 줄",
         v1="runtime/chamber_runtime.py:7310-7321",
         v2="v2/ch12/outputs/", test="", status="todo", note=""),
    dict(id="UI-024", area="ui",
         desc="PC 페이지 로그창(pc_logMessage_edit) 붙이기: 다른 스레드/루프에서 오면 루프 스레드로 넘김, 바닥을 보고 있을 때만 자동 스크롤(0 ms 뒤 재보정, 중복 예약 방지)",
         v1="main.py:1979-2045",
         v2=_P, test="", status="todo", note=""),

    # ───────────────────────────── 창·페이지·공통 위젯 ─────────────────────────────
    dict(id="UI-025", area="ui",
         desc="메인 창 1086×1010, 제목 'Form'. 스택 페이지: page_3=PC/TSP/Pre-Sputter(시작 페이지, index 0), page=CH1, page_2=CH2, server=서버 페이지(코드로 추가, "
              "실패 시 'ServerPage init failed' NET 로그·서버 페이지 없음)",
         v1="ui/main_window.py:22-31; ui/main_window.py:894; ui/main_window.py:901; main.py:205-211; main.py:373-388",
         v2=_P, test="", status="todo", note=""),
    dict(id="UI-026", area="ui",
         desc="페이지 이동 버튼: PC 페이지 'CH.1'·'CH.2'·'Server', CH1 페이지 'CH.2'·'Plasma\\nCleaning', CH2 페이지 'CH.1'·'Plasma\\nCleaning', 서버 페이지 우상단 "
              "'Plasma\\nCleaning'·'CH.1'·'CH.2'(110×48). 연결은 1회만",
         v1="main.py:807-817; main.py:836-839; main.py:846-854; main.py:1056-1059; runtime/server_page.py:120-128",
         v2=_P, test="", status="todo", note=""),
    dict(id="UI-027", area="ui",
         desc="모든 QPlainTextEdit/QTextEdit 에서 Tab/Shift+Tab 이 다음/이전 칸 이동(setTabChangesFocus, StrongFocus). 탭 순서는 .ui 의 setTabOrder 목록",
         v1="main.py:200-201; main.py:1828-1839; ui/main_window.py:786-890",
         v2=_P, test="", status="todo", note="창 생성 초기에 한 번만 — 뒤에 만든 위젯(Pre-Sputter Recipe 칸·서버 페이지)은 대상 아님"),
    dict(id="UI-028", area="ui",
         desc="CH1/CH2 'Integration Time [ms]' 칸을 'Process Name' 칸으로 재활용(라벨 변경, 툴팁 '공정명(로그/구글챗/CSV Process Note에 기록). Integration Time은 "
              "기본 60ms로 고정됩니다.'). CH1 위젯 이름은 오타 ch1_intergrationTime_edit 그대로",
         v1="main.py:178-189; runtime/chamber_runtime.py:8381",
         v2=_P, test="", status="todo", note=""),
    dict(id="UI-029", area="ui",
         desc="시작 시 화면 기본값(비어 있는 칸만, overwrite=False): TSP 목표 '2.5e-07'·사이클 '10'·예약 시각 'HH:MM'(TSP_DAILY_HH/MM 기본 05:00), "
              "PC Base Pressure 5e-06·Gas Flow 0.0·Working Pressure 2.0·RF Power 100.0·Process Time 1.0, CH1/CH2 Base Pressure 1e-05",
         v1="main.py:203; main.py:1061-1118; lib/config_common.py:601-606; lib/config_common.py:776; lib/config_common.py:788-794",
         v2=_P, test="", status="todo",
         note="CH1/CH2 Base Pressure 는 바로 뒤 ChamberRuntime 기본값(5e-6/9e-6)이 덮어씀 — UI-146"),
    dict(id="UI-030", area="ui",
         desc="PC 페이지 'Config' 버튼(30,415,211×101)은 만들어만 두고 아무 동작에도 연결되지 않음",
         v1="ui/main_window.py:279-283; ui/main_window.py:943",
         v2=_P, test="", status="na",
         note="연결 코드 없음(grep Config_button: main_window.py 외 0건) — 죽은 버튼"),
    dict(id="UI-031", area="ui",
         desc="라디오 묶음: Pre-Sputter(buttonGroup)·PC 챔버(buttonGroup_2) 라디오는 autoExclusive=False 이지만 그룹을 setExclusive(True) 로 고정. "
              "펄스 라디오(RF Pulse/DC Pulse)는 그룹 없음(둘 다 해제 가능)",
         v1="ui/main_window.py:229-262; ui/main_window.py:338-345; main.py:819-823; runtime/chamber_runtime.py:3088-3093",
         v2=_P, test="", status="todo", note=""),

    # ───────────────────────────── Power Select ─────────────────────────────
    dict(id="UI-032", area="ui",
         desc="PC 페이지 'Power Select' 토글 버튼(기본 OFF, 글자 'Power\\nSelect: ON/OFF'). 토글마다 PLC 스위치 SW_POWER_SELECT(코일 215 = M00137) 쓰기"
              "(momentary=False), 로그 'POWER_SELECT(SW) <- on'",
         v1="ui/main_window.py:267-274; main.py:195-198; main.py:1241-1243; device/plc.py:145; device/plc.py:1787-1789",
         v2=f"{_CP}; v2/ch12/drivers/plc.py", test="", status="todo",
         note="SW_RF_SELECT(214) 가 아니라 SW_POWER_SELECT(215) 를 쓴다. 시작 때 PLC 상태를 읽어 버튼에 반영하지 않음(항상 OFF 표시로 시작)"),
    dict(id="UI-033", area="ui",
         desc="Power Select 쓰기 실패 → 'Power Select 실패(PLC 연결 확인): {e!r}' 방송 로그, 버튼 눌림을 blockSignals 로 원복(재호출 방지)",
         v1="main.py:1244-1255",
         v2=f"{_CP}", test="", status="todo",
         note="원복 때 toggled 가 막혀 글자는 바뀌지 않음(알려진 이상 동작 — UI-142)"),

    # ───────────────────────────── PC 챔버 선택 ─────────────────────────────
    dict(id="UI-034", area="ui",
         desc="PC 챔버 선택 라디오 CH1/CH2: 기본 CH1. 바꿀 때 PC 실행 중이면 거부 — PC 로그 '플라즈마 클리닝 실행 중에는 챔버 전환이 불가합니다.' 후 이전 선택으로 되돌림. "
              "같은 선택이면 무시",
         v1="main.py:825-834; main.py:856-901",
         v2=f"{_CP}; v2/ch12/plasma_cleaning.py", test="", status="todo", note=""),
    dict(id="UI-035", area="ui",
         desc="PC 챔버 적용(시작 때도 1회): 선택 챔버 IG 장치 교체 + IG 콜백(ensure_on/turn_on, read_mTorr·read_pressure_mTorr·get_mTorr 없으면 Torr×1000), "
              "Gas Flow MFC=항상 MFC1, SP4(압력) MFC=선택 챔버 MFC, set_selected_ch. PC 로그 '[Plasma Cleaning] Use CH{n} IG, SP4 → MFC{n}, GasFlow → MFC1 ch3'. "
              "각 단계 실패는 'IG 디바이스 주입 실패'·'IG 콜백 바인딩 실패'·'MFC 주입 실패' 로그",
         v1="main.py:639-640; main.py:903-968",
         v2="v2/ch12/plasma_cleaning.py; v2/ch12/shared.py", test="", status="todo", note=""),

    # ───────────────────────────── 런타임 덤프 ─────────────────────────────
    dict(id="UI-036", area="ui",
         desc="PC 페이지 'State Dump' 버튼(툴팁 '현재 프로그램 상태(쓰레드/asyncio task/UI/런타임)를 덤프로 저장'): 누르면 버튼 끔 → 덤프(reason 'manual_button') → "
              "500 ms 뒤 다시 켬. 덤프 모듈 import 실패면 경고창 'Dump' / 'util/runtime_dump.py(request_dump) 로드 실패', 예외면 'Runtime dump failed: {e!r}'",
         v1="ui/main_window.py:285-292; main.py:72-76; main.py:841-844; main.py:1209-1239",
         v2=_CP, test="", status="todo", note=""),
    dict(id="UI-037", area="ui",
         desc="덤프 결과 로그: 성공 'Runtime dump saved({reason}): {path}'(WARN/DUMP), 이미 진행 중(RuntimeError) 'Runtime dump skipped({reason}): …'(WARN/DUMP), "
              "그 밖 'Runtime dump failed({reason}): {e!r}'(ERROR/DUMP), 모듈 없음 'util.runtime_dump import 실패(request_dump=None)'",
         v1="main.py:1187-1207",
         v2=_CMD, test="", status="todo", note=""),
    dict(id="UI-038", area="ui",
         desc="덤프에 넣는 객체: runtime_state 스냅샷, pc_use_ch, plc_owner, closing_started, close_ready, 그리고 plc·mfc1·mfc2·ig1·ig2·ch1·ch2·pc·tsp_ctrl·server_page "
              "요약(type, is_connected/connected/_running/running, vars 중 ui·_loop·loop·app·qapp·chat·plc·_plc·mfc·mfc_gas·mfc_pressure·ig 제외, repr 400자 자름)",
         v1="main.py:1170-1185; util/runtime_dump.py:41-44; util/runtime_dump.py:61-66; util/runtime_dump.py:146-177",
         v2=_CMD, test="", status="todo", note=""),
    dict(id="UI-039", area="ui",
         desc="덤프 파일: <LOG_ROOT>/RUNTIME_DUMP/runtime_dump_{YYYYmmdd_HHMMSS}_{reason}_pid{pid}.txt (mkdir 실패 시 LOCAL_FALLBACK_ROOT/RUNTIME_DUMP). "
              "내용 '=== SNAPSHOT(JSON) ===' + JSON(meta: timestamp·pid·python·executable·argv·platform·cwd / threads / ui 위젯 값(로그창은 끝 300줄) / "
              "asyncio 태스크(이름·done·cancelled·coro·스택 limit 50) / objects) + '=== THREAD TRACEBACK (faulthandler) ===' 전 스레드 + '[end]'. "
              "스냅샷은 UI 스레드에서 모으고 파일 쓰기는 데몬 스레드 'runtime-dump-writer'",
         v1="util/runtime_dump.py:20-59; util/runtime_dump.py:74-144; util/runtime_dump.py:179-243",
         v2="v2/ch12/outputs/", test="", status="todo", note=""),
    dict(id="UI-040", area="ui",
         desc="덤프 중복 방지: 진행 중이면 RuntimeError('runtime dump already in progress')",
         v1="util/runtime_dump.py:38-39; util/runtime_dump.py:212-217; util/runtime_dump.py:239-241",
         v2=_CMD, test="", status="todo",
         note="오류 카탈로그에 E910 '런타임 덤프 중복 실행'(errors/error_registry.py:1812-1819)이 있으나 main 은 코드 없이 로그만 남김"),

    # ───────────────────────────── 종료(closeEvent) ─────────────────────────────
    dict(id="UI-041", area="ui",
         desc="창 닫기: 정리 끝난 뒤 다시 온 close 는 수락, 종료 절차 진행 중이면 무시",
         v1="main.py:1476-1485",
         v2="v2/ch12/app.py; v2/ch12/winshell.py", test="", status="todo", note=""),
    dict(id="UI-042", area="ui",
         desc="공정 실행 중 종료 차단: 실행 중 목록 'CH1 Sputter'·'CH2 Sputter'(is_running)·'Plasma Cleaning (CHn)'·'TSP'(runtime_state). 하나라도 있으면 "
              "'프로그램 종료 차단: 실행 중 공정 = …' WARN/EXIT 로그, 덤프 1회(reason 'close_blocked_while_running'), 경고창(제목 '프로그램 종료 차단', "
              "'현재 공정이 진행 중이므로 프로그램을 종료할 수 없습니다.\\n\\n실행 중: …\\n\\n공정을 먼저 정상 종료 또는 정지한 뒤 프로그램을 닫아주세요.', "
              "5000 ms 자동 닫힘, 떠 있으면 다시 안 띄움) 후 무시",
         v1="main.py:1120-1168; main.py:1487-1526",
         v2="v2/ch12/app.py; v2/ch12/winshell.py; v2/ch12/core.py", test="", status="todo",
         note="CH 는 is_running(프로세스 컨트롤러) 기준 — 프리플라이트·척 이동·큐 행 사이(is_busy 만 True)에는 막지 않음. 덤프 1회 플래그는 종료 확인(예) 때만 초기화"),
    dict(id="UI-043", area="ui",
         desc="공정이 없어도 종료 확인창(question, 제목 '프로그램 종료 확인', '프로그램을 종료하시겠습니까?', 기본 No). CH2 히터 RUN 이면 "
              "'\\n\\nCH2 히터가 운전 중입니다(PV xx.x°C). 종료하면 히터가 꺼집니다.'(PV 없으면 '--.-') 추가. 아니오 → '사용자가 프로그램 종료를 취소했습니다.' 로그·무시",
         v1="main.py:1528-1558",
         v2="v2/ch12/app.py; v2/ch12/winshell.py", test="", status="todo",
         note="v2 는 콘솔 닫기·Ctrl+C·로그오프 = 의도적 종료(지시 3장(6)) — 확인창 대응은 decisions 에"),
    dict(id="UI-044", area="ui",
         desc="종료 수락: 'closeEvent accepted' WARNING, '프로그램 종료 시퀀스 시작' 로그, 창 전체 비활성, _shutdown_app_async 끝나면 close_ready 후 0 ms 뒤 다시 close",
         v1="main.py:1560-1594",
         v2="v2/ch12/app.py", test="", status="todo", note=""),
    dict(id="UI-045", area="ui",
         desc="종료 정리 순서: '앱 종료 정리 시작' → 부팅 태스크(PLC·호스트) 취소 → 호스트 서버 종료(3 s) → PLC 코일 CSV 로거 정지(3 s) → PC·CH1·CH2 런타임 "
              "shutdown_fast_async(각 8 s) → CH2 히터 shutdown(timeout 2 s, 대기 4 s)·패널 정리 → 두께 서비스 shutdown(2 s/4 s)·패널 정리 → PLC close(3 s) → "
              "알림기 6개 shutdown → Qt 로그 핸들러 원복 → 호스트 요청 공정 로그 워커 close(2 s) → '앱 종료 정리 완료' → 기본 executor 종료",
         v1="main.py:1368-1474",
         v2="v2/ch12/app.py; v2/ch12/system.py; v2/ch12/safety_rules.py", test="", status="todo",
         note="v2 의도적 종료는 파워만 끄기(지시 3장(6)) — 순서 차이는 differences 에"),

    # ───────────────────────────── 프리스퍼터 예약 ─────────────────────────────
    dict(id="UI-046", area="ui",
         desc="프리스퍼터 예약 런타임을 챔버마다 1개(CH1 전용·CH2 전용) 만든다: 기본 06:00, parallel=False(순차), 구글챗 없음(chat=None), 로그는 PC 로그창, "
              "표시는 main 이 그림(ui sink). 초기화 실패 시 'PreSputter 예약 초기화 실패' 로그",
         v1="main.py:500-517; main.py:553-554; runtime/pre_sputter_runtime.py:40-101",
         v2="v2/ch12/pre_sputter.py", test="", status="todo", note=""),
    dict(id="UI-047", area="ui",
         desc="Pre-Sputter 챔버 라디오(기본 CH1)로 고른 챔버에만 Reserve/Cancel 이 작동. 라디오를 바꾸면 그 챔버 상태·남은 시간·레시피 표시를 다시 그리고, "
              "그 챔버에 살아 있는 예약이 있으면 HH:MM 칸을 그 예약 시각으로 바꾼다(예약 없으면 칸을 건드리지 않음)",
         v1="main.py:520-539; main.py:1596-1602; main.py:1793-1826",
         v2=f"v2/ch12/pre_sputter.py; {_CP}", test="", status="todo", note=""),
    dict(id="UI-048", area="ui",
         desc="'Reserve' → HH:MM 칸을 읽어 그 챔버 매일 예약 등록/갱신. 형식 정규식 ^\\s*(\\d{1,2})\\s*(?::|시)\\s*(\\d{1,2}) ('8시30분' 허용), 0~23/0~59. "
              "틀리면 상태 '시간 형식 오류 (HH:MM)'·로그 '[설정] 잘못된 시간 형식(HH:MM)'. 성공 시 QSettings 에 예약 저장",
         v1="main.py:1812-1818; runtime/pre_sputter_runtime.py:227-244; runtime/pre_sputter_runtime.py:492-503",
         v2=f"v2/ch12/pre_sputter.py; {_CMD}", test="", status="todo", note=""),
    dict(id="UI-049", area="ui",
         desc="예약 등록(start_daily): 기존 예약을 조용히 취소, 다음 시각 = 오늘 HH:MM(지났으면 내일, 초 0), HH:MM 칸에 표시, 상태 '예약됨 · 매일 HH:MM', "
              "로그 '[PreSputter] 예약 등록: YYYY-MM-DD HH:MM:SS (매일 반복)'",
         v1="runtime/pre_sputter_runtime.py:11-19; runtime/pre_sputter_runtime.py:166-185",
         v2="v2/ch12/pre_sputter.py", test="", status="todo", note=""),
    dict(id="UI-050", area="ui",
         desc="'Cancel' → 그 챔버 예약 취소(진행 중 공정은 그대로), 상태 '예약 없음'·남은 시간 '--:--:--', 실제로 취소했을 때만 로그 '예약 취소됨', QSettings 예약 해제",
         v1="main.py:1820-1826; runtime/pre_sputter_runtime.py:197-210",
         v2=f"v2/ch12/pre_sputter.py; {_CMD}", test="", status="todo", note=""),
    dict(id="UI-051", area="ui",
         desc="예약 표시: 라벨 'Remaining Time' 을 'Reservation' 으로 바꿔 preSputter_remainigTime_edit=상태 문구, preSputter_LeftTime_edit=남은 시간 HH:MM:SS. "
              "1 s 마다 갱신, 같은 글자면 다시 쓰지 않음, 갱신 경로에서 파일 stat 금지",
         v1="main.py:541-543; main.py:1604-1628; runtime/pre_sputter_runtime.py:256-273; ui/main_window.py:142-148",
         v2=f"v2/ch12/pre_sputter.py; {_P}", test="", status="todo", note=""),
    dict(id="UI-052", area="ui",
         desc="예약 시각 도달: 남은 시간 '00:00:00'. 그 챔버가 바쁘면(is_busy) 이번 회 PASS — 로그 '[PreSputter] 해당 챔버가 이미 공정 중 → 이번 예약 PASS', "
              "상태 'PASS(공정 중) · 다음 HH:MM', 다음날 재예약 로그 '[PreSputter] 다음 반복 예약: …'",
         v1="runtime/pre_sputter_runtime.py:274-291; runtime/pre_sputter_runtime.py:385-394",
         v2="v2/ch12/pre_sputter.py", test="", status="todo", note=""),
    dict(id="UI-053", area="ui",
         desc="예약 실행(순차 _run_one): 상태 'Pre-Sputter 실행 중' → 챔버 레시피 큐 임시 보관 → 전용 레시피가 있으면 start_with_recipe_string(경로, origin "
              "'presputter')·로그 '레시피 실행: 파일명', 없으면 start_presputter_from_ui·로그 'UI 현재값으로 실행' → 60 s 안에 is_busy 가 되는지 0.2 s 간격 확인"
              "(아니면 '시작 신호 미확인 (timeout)') → 끝날 때까지 1 s 간격 대기 → 큐 복구 → 상태 '완료 · 다음 HH:MM' 또는 '실행 실패 · 다음 HH:MM', 로그 '완료'",
         v1="runtime/pre_sputter_runtime.py:396-404; runtime/pre_sputter_runtime.py:428-489",
         v2="v2/ch12/pre_sputter.py", test="", status="todo", note=""),
    dict(id="UI-054", area="ui",
         desc="예약 회차 예외 → 로그 '[PreSputter] 실행 중 예외: … — 이번 회차 실패, 다음 예약 유지', 상태 '실행 오류 · 다음 HH:MM'. 취소(Cancel)로 끝나면 표시를 덮어쓰지 않음",
         v1="runtime/pre_sputter_runtime.py:295-311; runtime/pre_sputter_runtime.py:471-486",
         v2="v2/ch12/pre_sputter.py", test="", status="todo", note=""),
    dict(id="UI-055", area="ui",
         desc="레시피 큐 격리: 실행 직전 snapshot_recipe_queue(큐 비움, 있으면 '레시피 큐 N건 임시 보관 (Pre-Sputter)'), 끝나면 restore_recipe_queue — 챔버가 아직 "
              "바쁘면 복구 생략·'공정 진행 중 → 레시피 큐 복구 생략(실행 큐 보호). 보관분 N건은 파기됨 — 필요 시 파일을 다시 여십시오.', 복구 시 첫 행으로 화면 갱신·"
              "'레시피 큐 N건 복구 완료'",
         v1="runtime/chamber_runtime.py:7953-7989",
         v2="v2/ch12/pre_sputter.py; v2/ch12/process_engine.py", test="", status="todo", note=""),
    dict(id="UI-056", area="ui",
         desc="UI 현재값 예약 시작(start_presputter_from_ui): 이미 공정 중이면 'PreSputter: 이미 공정 중입니다.' False, 아니면 Start 버튼과 같은 경로"
              "(origin 'presputter')로 시작하고 'PreSputter 자동 시작 (UI 현재값)' 로그·True",
         v1="runtime/chamber_runtime.py:4500-4515",
         v2="v2/ch12/pre_sputter.py; v2/ch12/core.py", test="", status="todo",
         note="시작 가드가 알림창으로 거절해도 True 반환 — UI-143"),
    dict(id="UI-057", area="ui",
         desc="프리스퍼터 로그 줄 '[HH:MM:SS] [PreSputter] CH1|CH2 msg' 를 PC 로그창에만(구글챗 없음)",
         v1="runtime/pre_sputter_runtime.py:505-517; main.py:502-512",
         v2="v2/ch12/pre_sputter.py; v2/ch12/hub.py", test="", status="todo", note=""),
    dict(id="UI-058", area="ui",
         desc="Pre-Sputter 전용 레시피 위젯을 코드로 추가: 'Recipe' 버튼(560,350,91×31), 읽기 전용 표시칸(560,385,201×31) 기본 'UI 현재값 사용'",
         v1="main.py:545-546; main.py:1668-1685",
         v2=_P, test="", status="todo", note=""),
    dict(id="UI-059", area="ui",
         desc="'Recipe' → 선택 챔버의 레시피 파일 선택(공용 파일 열기, 제목 'CH{n} Pre-Sputter 레시피 선택', 시작 폴더 = 현재 경로의 폴더, 필터 "
              "'Recipe Files (*.csv *.xlsx);;CSV Files (*.csv);;Excel Files (*.xlsx);;All Files (*.*)'). 취소 = 변경 없음. 고르면 런타임에 경로 지정, "
              "존재 확인 1회, QSettings 저장, 'PreSputter CH{n} 레시피 지정: 경로' 로그, 표시 갱신",
         v1="main.py:1710-1758",
         v2=f"v2/ch12/pre_sputter.py; {_CP}", test="", status="todo", note="레시피 경로를 비워 'UI 현재값 사용' 으로 되돌리는 화면 수단 없음 — UI-144"),
    dict(id="UI-060", area="ui",
         desc="레시피 표시: 경로 없음 'UI 현재값 사용', 있으면 파일명, 파일이 없으면 '파일명 (파일 없음)'. 존재 확인(os.path.exists)은 부팅 1회와 새로 고른 직후만(NAS stat 이 UI 를 막지 않게)",
         v1="main.py:1639-1666",
         v2=_P, test="", status="todo", note=""),
    dict(id="UI-061", area="ui",
         desc="QSettings(조직 'VanaM', 앱 'CH1_2_Program') 키: presputter/recipe_ch1·_ch2(경로), presputter/reserved_ch1·_ch2(bool), presputter/reserve_hhmm_ch1·_ch2('HH:MM'). "
              "부팅 때 레시피 경로 복원(없는 파일도 경로 유지, 'PreSputter 레시피 파일 없음: 경로' 로그)",
         v1="main.py:1630-1637; main.py:1687-1708; main.py:1760-1768",
         v2="v2/ch12/pre_sputter.py; v2/ch12/params.py", test="", status="todo",
         note="v1 설정 변환 도구가 QSettings(윈도우 레지스트리) 값도 옮길지 decisions 에"),
    dict(id="UI-062", area="ui",
         desc="부팅 때 예약 복원: reserved=True 인 챔버만, 'HH:MM' 정규식 ^\\s*(\\d{1,2})\\s*:\\s*(\\d{1,2})\\s*$·범위 검사, 실패 로그 'PreSputter CHn 예약 복원 실패: 시각 형식 "
              "오류(…)/범위 초과(…)', 성공 'PreSputter CHn 예약 복원: 매일 HH:MM'. 복원 뒤 HH:MM 칸을 선택 챔버 값으로 맞춤. 지난 시각은 내일로(따라잡기 없음)",
         v1="main.py:549-552; main.py:1770-1791",
         v2="v2/ch12/pre_sputter.py; v2/ch12/resume.py", test="", status="todo", note=""),
    dict(id="UI-063", area="ui",
         desc="병렬 실행(_run_parallel)·inter_ch_delay 5 s·bind_ui·_on_start_clicked·start_once_now 는 main 구성(parallel=False, 챔버당 런타임 1개)에서 쓰이지 않음",
         v1="main.py:504-511; runtime/pre_sputter_runtime.py:187-195; runtime/pre_sputter_runtime.py:212-225; runtime/pre_sputter_runtime.py:246-251; runtime/pre_sputter_runtime.py:319-383",
         v2="v2/ch12/pre_sputter.py", test="", status="na",
         note="호출 없음(main.py 는 parallel=False·schedule_from_ui 만 사용)"),

    # ───────────────────────────── CH 페이지: 버튼·입력칸 ─────────────────────────────
    dict(id="UI-064", area="ui",
         desc="CH 페이지 버튼 연결: START→시작 처리, STOP→정지, 'Select File'(processList)→레시피 파일 선택(분리 태스크). Dep.Rate/Thickness 칸 글자 바뀜→Process Time 자동 계산",
         v1="runtime/chamber_runtime.py:3316-3342; runtime/chamber_runtime.py:814",
         v2=_CP, test="", status="todo", note=""),
    dict(id="UI-065", area="ui",
         desc="Process Time 자동 계산: dep>0 이고 thickness>0 이면 Process Time[min] = thickness[nm] / dep[nm/s] / 60 을 소수 3자리로(같은 값이면 안 씀). START 때도 한 번 강제 계산",
         v1="runtime/chamber_runtime.py:3344-3357; runtime/chamber_runtime.py:4441",
         v2=f"v2/ch12/blocks.py; {_P}", test="", status="todo", note=""),
    dict(id="UI-066", area="ui",
         desc="START/STOP 버튼은 상태와 무관하게 항상 활성(공정 상태 바뀔 때마다 다시 setEnabled(True))",
         v1="runtime/chamber_runtime.py:3249-3256",
         v2=_P, test="", status="todo", note=""),
    dict(id="UI-067", area="ui",
         desc="START 가드 알림창(경고, 5 s 자동 닫힘): 쿨다운 60 s '대기 필요'/'이전 공정 종료 후 1분 대기 필요합니다.\\n{초}초 후에 시작하십시오.'(올림), 정리 미완료 "
              "'정리 미완료'/'이전 공정의 장치 정리가 완전히 끝나지 않았습니다(타임아웃).\\nSTOP을 한 번 더 눌러 강제 복구를 재시도하거나, 필요하면 프로그램을 재시작하세요.\\n"
              "(정리 로그에 남아있는 장치 이름이 원인입니다.)', 러너 바쁨 '대기 중'/'이전 공정/정리가 아직 진행 중입니다. 잠시 후 다시 시도하세요.', 챔버 실행 중 "
              "'실행 오류'/'CH{n}는 이미 다른 공정이 실행 중입니다.', 같은 CH PC 실행 중 '실행 오류'/'CH{n} Plasma Cleaning이 실행 중입니다.\\n끝난 뒤 시작하십시오.', "
              "컨트롤러 바쁨 '실행 오류'/'다른 공정이 실행 중입니다.'",
         v1="runtime/chamber_runtime.py:4359-4416",
         v2=_CP, test="", status="todo", note="판정 자체는 proc 영역 — 여기는 문구"),
    dict(id="UI-068", area="ui",
         desc="START 때 레시피 큐가 있으면 파일 공정(START_QUEUE) — 로그 '[CHn] 파일 기반 자동 공정 시작'. 없으면 화면 칸으로 단일 공정: base_pressure(빈칸 1e-5)·"
              "working_pressure(빈칸 0)·shutter_delay(0)·dep_rate/thickness(빈칸 None)·process_time·integration_time 60 고정·process_note/Process_name = Process Name "
              "칸(빈칸이면 'Single CH{n}')·t0_pressed_wall/ns, 'G1 Target' 등 키 동기화, 로그 '입력 검증 통과 → Runner START'",
         v1="runtime/chamber_runtime.py:4417-4489",
         v2=_CP, test="", status="todo", note=""),
    dict(id="UI-069", area="ui",
         desc="단일 공정 정규화 검사 오류 → 경고창 '입력값 확인' 본문 '- 오류' 줄 목록. 시작 처리 중 예외 → 'MAIN' 로그에 traceback, 오류창(Critical) '실행 오류'/"
              "'공정 시작 준비 중 내부 오류가 발생했습니다.\\n로그를 확인하세요.'",
         v1="runtime/chamber_runtime.py:4472-4495",
         v2=_CP, test="", status="todo", note=""),
    dict(id="UI-070", area="ui",
         desc="STOP → 어떤 상태든 러너에 STOP(user_initiated=True) 명령(공정 중이 아니어도 막지 않음)",
         v1="runtime/chamber_runtime.py:4497-4498; runtime/chamber_runtime.py:4517-4523",
         v2=_CP, test="", status="todo", note=""),
    dict(id="UI-071", area="ui",
         desc="CH1 단일 공정 화면 검사: 가스(Ar/O2) 하나 이상 '선택 오류'/'가스를 하나 이상 선택해야 합니다.', 유량 숫자·0 이상 '입력값 확인'/'가스 유량 입력을 확인하세요.', "
              "RF·DC Pulse 동시 선택 '선택 오류'/'RF Pulse와 DC Pulse는 동시에 사용할 수 없습니다 (같은 시리얼 포트, 입력칸 공유). 하나만 선택하세요.', 미선택 "
              "'CH1은 RF-Pulse(또는 DC-Pulse) 중 하나를 선택해야 합니다.'(DC 만 지원 'CH1은 DC-Pulse를 선택해야 합니다.', 없음 'CH1에서 사용 가능한 파워 장치가 없습니다.'). "
              "DC Pulse: 파워>0 'DC-Pulse Target Power(W)를 확인하세요.', 주파수(있으면) int 20..150 'DC-Pulse Freq(kHz)는 20..150 범위입니다.', Off Time UI-074. "
              "RF Pulse: 파워>0 'RF Pulse Target Power(W)를 확인하세요.', 주파수 float 0.001..상한 'RF Pulse Freq(kHz)는 0.001..{max} 범위입니다.\\n(CESAR 사양 3-5: "
              "1Hz~30kHz — 초과 시 W40으로 출력 차단)', 듀티 int 1..99 'RF Pulse Duty(%) 1..99'. 결과: use_g1~3=False, 연속 파워 미사용, use_power_select=P.W. Select 체크",
         v1="runtime/chamber_runtime.py:5817-5962",
         v2=f"{_CP}; v2/ch12/recipe.py", test="", status="todo", note="CH1 은 N2 체크박스가 없어 use_n2 항상 False"),
    dict(id="UI-072", area="ui",
         desc="CH2 단일 공정 화면 검사: 가스(Ar/O2/N2)·유량 검사(CH1 과 같은 문구), DC1/DC2/RF 연속 파워 체크 시 >0 'DC Power(W)를 확인하세요.'/'DC2 Power(W)를 확인하세요.'/"
              "'RF Power(W)를 확인하세요.', 펄스 동시 선택 거부, DC Pulse(입력칸 rfPulse* 공유) 파워·20..150 kHz·Off Time, RF Pulse 'RF-Pulse Target Power(W)를 확인하세요.'·"
              "0.001..상한 kHz·'RF-Pulse Duty(%)는 1..99 범위입니다.', 하나도 없으면 '선택 오류'/'DC1/DC2/RF Power 또는 DC/RF Pulse 중 하나 이상 선택해야 합니다.'. "
              "G1~G3 체크·타겟명·P.W. Select(시작을 막지 않음) 전달",
         v1="runtime/chamber_runtime.py:5963-6155",
         v2=f"{_CP}; v2/ch12/recipe.py", test="", status="todo", note=""),
    dict(id="UI-073", area="ui",
         desc="RF Pulse 주파수 상한 = RFPULSE_FREQ_MAX_HZ(기본 30000)/1000 kHz, 0 이하 설정이면 30 kHz",
         v1="runtime/chamber_runtime.py:3182-3188; lib/config_common.py:379",
         v2="v2/ch12/params.py; v2/ch12/recipe.py", test="", status="todo", note=""),
    dict(id="UI-074", area="ui",
         desc="DC Pulse Off Time 칸 해석: 공란=장비 현재값 유지(None), 'DC'(대소문자 무시)=DC 모드, 숫자는 소수 1자리 반올림 후 1.0~min(10.0, 400/f_kHz) µs(주파수 공란이면 10.0). "
              "숫자 아님 '입력값 확인'/'DC-Pulse Off Time 은 숫자(µs) 또는 'DC' 여야 합니다.\\n공란이면 장비 현재값을 유지합니다.', 범위 밖 '…{v:.1f}µs 는 허용 범위 밖입니다.\\n"
              "{f}kHz 기준 1.0 ~ {max:.1f}µs (매뉴얼 p.13 그림3: 상한 = min(10.0, 400/f))'. dc_pulse_duty 는 'DC' 면 100.0, 아니면 Off Time·주파수로 환산",
         v1="runtime/chamber_runtime.py:3190-3221; runtime/chamber_runtime.py:5944-5950",
         v2=f"v2/ch12/recipe.py; {_P}", test="", status="todo", note=""),
    dict(id="UI-075", area="ui",
         desc="펄스 라디오 배타: 한쪽이 켜지면 다른 쪽을 끔(재진입 방지), 켜진 것을 다시 누르면 꺼져 둘 다 해제 가능. 공유 칸 라벨 DC 선택 'DC Pulse Off Time [µs]', "
              "RF 'RF Pulse Duty Cycle [%]', 없음 'Duty [%] / Off Time [µs]'. 연결은 1회",
         v1="runtime/chamber_runtime.py:3088-3158",
         v2=_P, test="", status="todo", note=""),
    dict(id="UI-076", area="ui",
         desc="CH1 위젯 이름 대응: integrationTime_edit→intergrationTime_edit, g1/g2/g3Target_name→gunTarget_name(하나), rfPulsePower/Freq/DutyCycle_edit→dcPulse* "
              "(RF/DC 공유), rfPulseDutyCycle_label→dcPulseDutyCycle_label. 없는 위젯은 None 으로 조용히 통과",
         v1="runtime/chamber_runtime.py:8373-8410",
         v2=_P, test="", status="todo", note=""),
    dict(id="UI-077", area="ui",
         desc="CH2 잠금: RF Pulse 미지원이면 RF 라디오 비활성(DC Pulse 도 미지원일 때만 rfPulse* 칸까지), DC Pulse 미지원이면 DC 라디오 비활성, DC2 미지원(SUPPORTS_HEATER 와 배타)이면 "
              "DC2 체크·칸 비활성·툴팁 'DC2 미사용 — D/A CH3 는 CH2 히터 출력'",
         v1="runtime/chamber_runtime.py:7864-7891; runtime/chamber_runtime.py:533",
         v2=_P, test="", status="todo", note=""),
    dict(id="UI-078", area="ui",
         desc="CH 화면 기본값(시작·공정 후 리셋): 공통 Working 2·Ar 20·O2 0·N2 0·DC 130·DC2 0·DC Pulse 200(라디오 끔)·RF Pulse 100(라디오 끔)·RF 끔 0. "
              "CH1: Process Name 'RF pulse_pre_30min'·Base 5e-6·Working 5·Ar 20·Ar 체크·RF Pulse 선택·DC 0·펄스 250 W·20 kHz·80 %·G1·M.S. Open 체크·Shutter Delay 5·"
              "Process Time 25. CH2: Base 9e-6·G2·Ar·DC1 체크·DC2 끔 0·Shutter Delay 5",
         v1="runtime/chamber_runtime.py:7792-7862",
         v2=f"v2/ch12/blocks.py; {_P}", test="", status="todo",
         note="Shutter Delay 단위는 분(라벨 [min], ETA 계산 ×60) — 코드 주석 '5 초' 는 틀린 주석"),
    dict(id="UI-079", area="ui",
         desc="공정(큐) 끝 정리: 레시피 큐 비움(다음 START 는 화면 값 단일 공정), 기본값 복원, 타겟명 비움, 체크 모두 해제 후 챔버 기본 체크 복원, 상태 '대기 중', "
              "Power/Voltage/Current/For.P/Ref.P 칸 비움, DC 표시 버퍼 초기화, 그래프 초기화, 타겟명 재고 DB 재로드",
         v1="runtime/chamber_runtime.py:7740-7768; runtime/chamber_runtime.py:7894-7940",
         v2=f"v2/ch12/process_engine.py; {_P}", test="", status="todo", note=""),

    # ───────────────────────────── 레시피 파일 선택 ─────────────────────────────
    dict(id="UI-080", area="ui",
         desc="'Select File': 시작 폴더 = 마지막으로 연 폴더, 없으면 PROCESS_LIST_START_DIR('\\\\VanaM_NAS\\VanaM_toShare'). 제목 'CH{n} 프로세스 리스트 파일 선택', 필터 "
              "'Recipe Files (*.csv *.xlsx);;CSV Files (*.csv);;Excel Files (*.xlsx);;All Files (*)'. 취소 '파일 선택 취소', 고르면 '선택된 파일: 경로' 로그",
         v1="runtime/chamber_runtime.py:3359-3380; lib/config_common.py:725",
         v2=_CP, test="", status="todo", note="v2 는 포털 레시피 적재로 대체"),
    dict(id="UI-081", area="ui",
         desc="레시피 파일 읽기(executor, 30 s 제한 — 넘으면 'CSV 로드 30초 timeout (NAS 응답 지연): 경로'): xlsx 는 첫 시트만·1행 헤더·빈 행 제외, csv 는 utf-8-sig·"
              "모든 칸 빈 행 건너뜀('빈 행 N개 건너뜀 (모든 칸이 빈 행)'). Process_name 없으면 '#' 열, 그것도 없으면 '공정 N'. 예외 '파일 처리 오류: …'",
         v1="runtime/chamber_runtime.py:3381-3438",
         v2="v2/ch12/recipe.py", test="", status="todo", note="recipe 영역과 겹침"),
    dict(id="UI-082", area="ui",
         desc="행 검사 오류가 있으면 로그 '레시피 오류 N건:\\n - …' + 경고창 '레시피 오류'(본문 '- …' 목록), 큐에 넣지 않음. 성공 시 큐 교체·인덱스 -1, "
              "'총 N개 공정 읽음.', 첫 행으로 화면 갱신. 행이 없으면 '파일에 공정이 없습니다.'",
         v1="runtime/chamber_runtime.py:3440-3459; runtime/chamber_runtime.py:8653",
         v2=f"v2/ch12/recipe.py; {_P}", test="", status="todo", note="E230 검사 내용은 recipe 영역"),
    dict(id="UI-083", area="ui",
         desc="레시피 행 → 화면 반영: 로그 '[CHn] 자동 공정 (i/N) 준비: 이름' 또는 '단일 공정 UI 업데이트'. 펄스 라디오는 use_rf_pulse/use_dc_pulse(T/TRUE/1/Y/YES)만 따름, "
              "둘 다면 '[CHn] RF/DC Pulse 동시 지정 레시피 → Start 시 거부됨'. 공유 펄스 칸에 선택된 쪽 파워·주파수·듀티(DC 는 Off Time, 레거시 duty 환산), "
              "'0'/''/'nan' 은 빈칸. DC/DC2/RF 체크·값, Process Name, 유량, 압력, Shutter Delay, Dep.Rate('dep_rate' 또는 'dep.rate'), Thickness, process_time>0 일 때만 "
              "Process Time, G1~G3·Ar/O2/N2·M.S.·P.W. Select 체크('T' 정확히 같을 때만), 타겟명(비면 재고 DB 로드)",
         v1="runtime/chamber_runtime.py:3461-3593; runtime/chamber_runtime.py:3160-3180",
         v2=_P, test="", status="todo", note=""),
    dict(id="UI-084", area="ui",
         desc="위젯 값 쓰기 규칙(_set): setChecked 있으면 bool, setValue 있으면 숫자, 아니면 setPlainText/setText(str). 실패는 '_set(leaf) 실패' UI 로그",
         v1="runtime/chamber_runtime.py:3595-3619",
         v2=_P, test="", status="todo", note=""),
    dict(id="UI-085", area="ui",
         desc="공용 파일 열기 창(pick_open_file): Qt 자체 창(네이티브 아님), ExistingFile, ReadOnly(이름 바꾸기·삭제·새 폴더 막음), 모든 뷰 드래그 앤 드롭 끔(열기 전·후 2회), "
              "WindowModal, 시작 폴더는 50 ms 뒤 지정, 취소면 ''. 닫힌 뒤 숨기고 마우스 버튼이 다 떨어지면 deleteLater 1회(200 ms 간격, 최대 10분)",
         v1="util/file_pick.py:39-129; runtime/chamber_runtime.py:8423-8430",
         v2=_P, test="", status="todo", note="2026-10-12 크래시 대책 — v2 는 브라우저 파일 선택으로 대체(differences)"),

    # ───────────────────────────── 알림창 공통 ─────────────────────────────
    dict(id="UI-086", area="ui",
         desc="챔버 경고창(_post_warning): Warning 아이콘, OK, WindowModal, 기본 5000 ms 자동 닫힘, 같은 (제목, 본문)이 떠 있으면 'UI' 로그 '(중복 억제) 제목: 본문'만, "
              "동시에 5개 넘으면 '(팝업 상한 초과) …' 로그만. UI 없으면 'WARN' 로그",
         v1="runtime/chamber_runtime.py:8432-8485; util/timed_popup.py:9-56",
         v2=_P, test="", status="todo", note=""),
    dict(id="UI-087", area="ui",
         desc="챔버 오류창(_post_critical): Critical, 자동 닫힘 없음, 중복·상한 규칙 같음. clear_status_to_idle=True 면 상한 무시, 창이 닫히면(확인/X/ESC) runtime_state 오류 해제(idle). "
              "UI 없으면 'ERROR' 로그",
         v1="runtime/chamber_runtime.py:8487-8525",
         v2=f"{_P}; v2/ch12/core.py", test="", status="todo", note=""),
    dict(id="UI-088", area="ui",
         desc="공정 실패 알림창: 제목 'CH{n} 공정 실패', 본문 '{공정명|(process)}\\n\\n사유: {사유|process failed}\\n\\n확인을 누르면 상태 표시가 Idle로 변경됩니다.'(닫으면 idle)",
         v1="runtime/chamber_runtime.py:2071-2093",
         v2=f"{_P}; v2/ch12/core.py", test="", status="todo", note=""),
    dict(id="UI-089", area="ui",
         desc="펄스 공유 장비 점유 충돌: 경고창 '공유 장비 사용 중'/'{DC-Pulse|RF-Pulse} 시작 불가: 동일 주소(ip:port) 장비를 CH{m}({종류}) 공정이 사용 중입니다.'",
         v1="runtime/chamber_runtime.py:3870-3893",
         v2=f"{_P}; v2/ch12/shared.py", test="", status="todo", note=""),
    dict(id="UI-090", area="ui",
         desc="프리플라이트 장비 연결 실패: 오류창 '장비 연결 실패'/'다음 장비 연결을 확인하지 못했습니다:\\n - {목록}\\n\\n케이블/전원/포트 설정 확인 후 재시도'(제한 펄스 없음 8 s·있음 10 s)",
         v1="runtime/chamber_runtime.py:3935-3950",
         v2=f"{_P}; v2/ch12/core.py", test="", status="todo", note="판정·타임아웃은 proc 영역"),
    dict(id="UI-091", area="ui",
         desc="공정 시작 예외: 오류창 '오류'/'오류: '{process_note}' 시작 실패. ({e})'. 백그라운드 태스크(로그 writer) 생성 실패: 상태 '내부 오류: 태스크 생성 실패(로그 확인)'·"
              "오류창 '내부 오류'/'백그라운드 작업 생성에 실패했습니다.\\n프로그램이 멈춘 것처럼 보일 수 있습니다.\\n\\n자세한 내용은 로그 파일(또는 터미널)을 확인해주세요.'",
         v1="runtime/chamber_runtime.py:4097-4102; runtime/chamber_runtime.py:8256-8267",
         v2=f"{_P}; v2/ch12/core.py", test="", status="todo", note=""),
    dict(id="UI-092", area="ui",
         desc="호스트(로봇) 명령 알림창: Critical·OK·비모달 open, 같은 (제목, 본문) 떠 있으면 'HOST' 로그 '(중복 억제) …', 동시 5개 상한 '(팝업 상한 초과) …', 15000 ms 자동 닫힘",
         v1="main.py:1910-1955",
         v2=f"{_P}; v2/ch12/robot_api.py", test="", status="todo", note=""),

    # ───────────────────────────── 상태·값 표시 ─────────────────────────────
    dict(id="UI-093", area="ui",
         desc="공정 상태칸(processState_edit): 컨트롤러 'state' 이벤트 글자를 그대로(같은 글자면 생략). 카운트다운은 1 s 마다 '{기본 문구} (남은 시간: m분 s초)'"
              "(1분 미만 's초', 올림). 그 밖 '대기 중', '공정 완료'/'공정 중단됨'",
         v1="runtime/chamber_runtime.py:1695-1696; runtime/chamber_runtime.py:3292-3311; controller/process_controller.py:829; controller/process_controller.py:1663-1675; controller/process_controller.py:2107",
         v2=f"v2/ch12/snapshot_fields.py; {_P}", test="", status="todo", note=""),
    dict(id="UI-094", area="ui",
         desc="런타임이 직접 쓰는 상태 문구: 'Chuck {UP|MID|DOWN} 이동 중…', '정리 지연(취소 타임아웃). STOP을 한 번 더 눌러 복구를 시도하세요.', "
              "'정리 지연(취소 미완료). STOP을 한 번 더 눌러 복구를 시도하세요.', 'STOP 요청 중... (공정 종료 대기)', '지연 대기 중: {label} · 남은 시간 HH:MM:SS', "
              "'다음 공정 대기중 ({reason}) · 남은 시간 HH:MM:SS'",
         v1="runtime/chamber_runtime.py:4296; runtime/chamber_runtime.py:4721; runtime/chamber_runtime.py:4807; runtime/chamber_runtime.py:4814; runtime/chamber_runtime.py:5139; runtime/chamber_runtime.py:5200",
         v2=f"v2/ch12/snapshot_fields.py; {_P}", test="", status="todo", note=""),
    dict(id="UI-095", area="ui",
         desc="파워 값 표시: RF For.P/Ref.P 소수 2자리, DC Power/Voltage 1자리·Current 3자리. DC2 가 있으면 'DC1|DC2' 병기(P·V 0자리, I 2자리, 값 없으면 '-'). 값이 비면 "
              "'for.p/ref.p 비어있음'·'P/V/I 비어있음' 로그",
         v1="runtime/chamber_runtime.py:3050-3086",
         v2=f"v2/ch12/snapshot_fields.py; {_P}", test="", status="todo", note=""),
    dict(id="UI-096", area="ui",
         desc="타겟명 자동 채움(재고 DB fetch_gun_targets): 생성 때·레시피 행에 타겟명이 없을 때·공정 후 리셋 때. 비어 있는 칸만 채우고 툴팁도 같은 값. CH1 은 G1 하나, "
              "CH2 는 g1~g3. 실패는 'TARGET' 로그 '[CHn] 타겟 정보 불러오기 실패'. 파일 공정 시작 때 타겟이 모두 비면 최대 3 s 기다림('Gun Target DB 3초 초과 — 빈 값으로 진행')",
         v1="runtime/chamber_runtime.py:788-789; runtime/chamber_runtime.py:3578-3593; runtime/chamber_runtime.py:5080-5095; runtime/chamber_runtime.py:7940; runtime/chamber_runtime.py:8920-8951",
         v2=f"v2/ch12/system.py; {_P}", test="", status="todo", note="재고 API 연동은 app/dev 영역과 겹침"),

    # ───────────────────────────── 그래프 ─────────────────────────────
    dict(id="UI-097", area="ui",
         desc="RGA 그래프(CHn rgaGraph): x 'Atomic Mass' 0~65(눈금 14, '%d'), y 'Partial Pressure' 로그축 1e-11~1e-5 Torr('%.0e', 보조 눈금 없음), 빨간 점(크기 6)+"
              "빨간 세로선(ymin→값). 비유한값·x 범위 밖·y≤0 점 버림, 점 없으면 갱신 안 함. 선 시리즈는 풀로 재사용",
         v1="controller/graph_controller.py:50-102; controller/graph_controller.py:170-258",
         v2=_P, test="", status="todo", note=""),
    dict(id="UI-098", area="ui",
         desc="OES 그래프: x 'Wavelength (nm)' 100~1200(100 간격 고정), y 'Intensity' 0~16000(눈금 9, '%d'), 빨간 선 굵기 1. OES_PLOT_MIN_INTERVAL_S(0.5 s) 안 연속 호출은 건너뜀, "
              "유효점 2개 미만이면 생략, x 정렬. 종료 중이면 그리지 않음, 실패는 'OES' 로그 '그래프 업데이트 실패(무시)'",
         v1="controller/graph_controller.py:57-58; controller/graph_controller.py:104-137; controller/graph_controller.py:260-346; runtime/chamber_runtime.py:6491-6515; lib/config_common.py:248",
         v2=_P, test="", status="todo", note=""),
    dict(id="UI-099", area="ui",
         desc="그래프 초기화: 생성 직후·공정 후 리셋·OES/RGA 단계에서 clear/reset(축 범위 기본값으로). 헤드리스면 'Graph' 로그 'reset skipped (headless)' 등",
         v1="controller/graph_controller.py:348-395; runtime/chamber_runtime.py:542-547; runtime/chamber_runtime.py:6466-6489; runtime/chamber_runtime.py:7936-7937",
         v2=_P, test="", status="todo", note=""),
    dict(id="UI-100", area="ui",
         desc="그래프 모양: 축 글꼴 9 pt(제목·눈금 같음), 범례 숨김, 차트 여백 6, 플롯 영역 여백 좌 75·위 30·우 28·아래 60, 축선 검정·격자 회색, 컨테이너 최소 120×100",
         v1="controller/graph_controller.py:21-48; controller/graph_controller.py:101; controller/graph_controller.py:136; controller/graph_controller.py:409-445",
         v2=_P, test="", status="todo", note=""),

    # ───────────────────────────── CH2 히터 패널 ─────────────────────────────
    dict(id="UI-101", area="ui",
         desc="CH2 히터: 설정(load_heater_config)이 켜져 있을 때만 서비스·패널 생성(꺼지면 창 크기·태스크·PLC I/O 그대로). 패널 (1086,10) 240×340, 스택 밖이라 모든 페이지에서 보임, "
              "창 1338×1010. 설정 경고는 CH2 로그 '⚠ 히터 설정: …', 'CH2 히터 서비스 시작', CH2 런타임에 attach_heater. 실패 'HEATER' 로그 '히터 설정 읽기 실패 — 히터 기능 끔'/'CH2 히터 초기화 실패'",
         v1="main.py:486-487; main.py:1257-1292; ui/heater_panel.py:21-22",
         v2=f"v2/ch12/heater_engine.py; {_P}", test="", status="todo", note=""),
    dict(id="UI-102", area="ui",
         desc="히터 패널 구성: 제목 'CH2 Heater [°C]', 소유자 표시(manual '수동'·process '공정'·external '외부'·없음 '—'), LCD PV(20pt)·°C·SV(15pt)·Δ, 상태 줄, 배지, MV 막대, "
              "램프 8개(4열×2줄: TC·D/A·냉각수·진공·인터락 / 과온·TC 이상·워치독, 툴팁에 PLC 비트 M0040D·M00410·M00411·M00412·M00401·M00404·M00405·M00406), "
              "목표 칸(placeholder '°C'), '적용', 'ON'(체크형), 상태 문구(시작 'PLC 응답 대기'), '리셋'(숨김)",
         v1="ui/heater_panel.py:24-148",
         v2=_P, test="", status="todo", note=""),
    dict(id="UI-103", area="ui",
         desc="히터 패널 그리기: PV '---'/소수 1자리, SV 는 RUN 이면 cur_sv 아니면 sv, RUN 이고 PV·SV 있으면 'Δ+x.x'(|Δ|≤soak_tol 기본 2.0 이면 초록). 응답 없음(stale)이면 값 회색(마지막 값). "
              "배지 우선순위 FAULT(빨강)>ITL(주황, itl 꺼짐)>RUN>STOP, stale 이면 색만 회색. MV 막대 = (mv - mv_min 400)/(mv_limit - 400)×100 %, 글자 'MV mv/lim', 없으면 'MV -/-'. "
              "램프: stale 또는 ok 없음 → 회색, ok 종류 켜짐 초록, err 종류 켜짐 빨강·꺼짐 초록. 스타일시트는 바뀔 때만",
         v1="ui/heater_panel.py:150-255",
         v2=_P, test="", status="todo", note=""),
    dict(id="UI-104", area="ui",
         desc="히터 상태 줄/문구: 램프 중 '램프 {rate:g}°C/min → {target:.1f}°C[ · 약 N분]', 유지 '유지 · DAC 상한 {value} 고정', '운전 중', '정지'. 상태 문구 우선순위: "
              "이상 '과온 트립'/'센서 이상'/'워치독 트립'/'이상 발생'(빨강) > '인터락 — {빠진 조건} 꺼짐'(주황) > 'PLC 응답 없음 · N초 전 값'(빨강) > '운전 중 · 램프'/'운전 중 · 유지'/"
              "'운전 중'(초록) > '정지'",
         v1="ui/heater_panel.py:166-174; ui/heater_panel.py:268-309",
         v2=_P, test="", status="todo", note=""),
    dict(id="UI-105", area="ui",
         desc="히터 버튼 활성: ON/OFF·적용은 stale 아님·소유자가 공정 아님·명령 진행 중 아님일 때만(공정 소유면 툴팁 '공정이 히터를 제어 중'). 리셋은 이상일 때만 보이고 "
              "stale·진행 중이면 비활성. ON/OFF 글자·눌림은 RUN 상태만 따름(클릭 토글은 되돌림)",
         v1="ui/heater_panel.py:156-164; ui/heater_panel.py:257-266; ui/heater_panel.py:370-372",
         v2=_P, test="", status="todo", note=""),
    dict(id="UI-106", area="ui",
         desc="히터 ON/OFF·적용: RUN 이면 turn_off('정지(패널)', owner='manual'), 아니면 목표 검사 후 turn_on(t, owner='manual'). '적용' → apply_target(t). 목표 검사 문구: 빈칸 "
              "'목표 온도를 입력하십시오', 숫자 아님 '목표 온도가 숫자가 아닙니다', 범위 '목표 범위 밖 — 0 초과 {lim:.1f}°C 이하'(lim = min(max_temp 기본 500, PLC sv_limit))",
         v1="ui/heater_panel.py:324-338; ui/heater_panel.py:370-387",
         v2=f"{_CMD}; v2/ch12/heater_engine.py; {_P}", test="", status="todo", note=""),
    dict(id="UI-107", area="ui",
         desc="히터 리셋: RUN 이면 경고 '리셋 불가'/'히터 RUN 이 켜져 있습니다.\\n먼저 히터를 OFF 한 뒤 리셋하십시오.', 아니면 확인창 '히터 이상 리셋'/'히터 이상을 리셋합니다.\\n"
              "원인(배선·TC 모듈·과온)을 확인했습니까?\\n\\n원인이 남아 있으면 리셋 직후 다시 이상이 걸립니다.'(기본 No) → reset_fault()",
         v1="ui/heater_panel.py:389-401",
         v2=f"{_CMD}; v2/ch12/heater_engine.py; {_P}", test="", status="todo", note=""),
    dict(id="UI-108", area="ui",
         desc="히터 명령 실행: 진행 중이면 무시, 진행 중 버튼 잠금, 실패 사유 또는 성공이지만 사유가 있으면(보류 OFF 등) 비모달 경고창(제목=명령명), 태스크 생성 실패 "
              "'명령을 실행할 수 없습니다: …'. 앱 종료 때 남은 태스크 취소",
         v1="ui/heater_panel.py:311-368; ui/heater_panel.py:403-406",
         v2=f"{_CMD}; {_P}", test="", status="todo", note=""),
    dict(id="UI-109", area="ui",
         desc="히터 패널 갱신 주기 = 서비스 폴링마다 리스너 호출(HEATER_POLL_INTERVAL_S 기본 1.0 s 운전 중, HEATER_POLL_IDLE_S 3.0 s 정지, HEATER_STALE_SEC 10 s 이상 무응답이면 stale)",
         v1="main.py:1282; ui/heater_panel.py:176-182; lib/heater_config.py:33-36; lib/heater_config.py:227-232; controller/heater_service.py:710-725",
         v2="v2/ch12/heater_engine.py; v2/ch12/snapshot_fields.py", test="", status="todo",
         note="지시 3장(2): 기록은 1초, 제어 주기는 유지 — heat 영역과 겹침"),

    # ───────────────────────────── 두께 모니터 패널 ─────────────────────────────
    dict(id="UI-110", area="ui",
         desc="두께 모니터: 채널마다 설정 읽기(경고는 화면 전용 로그 '⚠ 두께 모니터 설정: …'), 켜진 채널만 서비스. 패널은 CH2 만. 서비스에 공정 정보(is_running|is_busy, 공정명) 제공, "
              "런타임에 attach_thickness·set_thickness_view(실패 '⚠ 두께 모니터 공정 연동 실패 — Thickness 모드 끔(Time 모드만)'), 'CH{n} 두께 모니터 서비스 시작 (host:port)'. "
              "실패 'CH{n} 두께 모니터 설정 읽기 실패 — 기능 끔'/'초기화 실패'",
         v1="main.py:489-494; main.py:1294-1344",
         v2=f"v2/ch12/thickness.py; v2/ch12/drivers/stm100.py; {_P}", test="", status="todo", note=""),
    dict(id="UI-111", area="ui",
         desc="오른쪽 패널 배치: 히터 패널이 보이면 두께 패널은 그 밑(y = 히터 아래끝+1+10), 없으면 (1086,10). 두께 패널 240×344. 두께 패널이 있으면 창 = (1086+폭+12) × max(1010, y+344+10). "
              "실패 'THICKNESS' 로그 '오른쪽 패널 배치 실패'",
         v1="main.py:1346-1366; ui/thickness_panel.py:20-21",
         v2=_P, test="", status="todo", note=""),
    dict(id="UI-112", area="ui",
         desc="두께 패널 구성: 제목 'CH{n} Thickness Monitor', 우상단 '리셋 N회'(장비 전원 리셋 플래그 본 횟수, 0 이면 빈칸), 램프 3개 '연결'/'끊김'·'XTAL'/'XTAL FAIL'·'응답'/'응답 없음', "
              "LCD 두께 nm(소수 1)·증착률 nm/s(소수 3)·'주파수 {int:,} Hz'·'수명 x.x %', 'XTAL FAIL' 태그, 적용값 줄 'Density {:.3g} · Z {:.3f} · Tooling {:g} %', "
              "진행 영역, 오류 줄, '영점'·'Factor 설정' 버튼",
         v1="ui/thickness_panel.py:22-172",
         v2=_P, test="", status="todo", note=""),
    dict(id="UI-113", area="ui",
         desc="두께 패널 그리기: stale 이면 값 회색. 통신(link_ok, 없으면 connected∧¬stale)이 되고 crystal_ok=False 면 XTAL FAIL 태그·빨간 램프, 통신 없으면 XTAL·응답 램프 회색"
              "(응답 툴팁 '최근 장비 정상 응답 없음 · N초 전'). 연결된 채 응답 없을 때만 빨간 오류 줄 '응답 없음 N초 · 마지막 오류'(218 px 말줄임)",
         v1="ui/thickness_panel.py:188-257",
         v2=_P, test="", status="todo", note=""),
    dict(id="UI-114", area="ui",
         desc="적용값 줄: 재료가 목록에 있으면 기호(예 Al2O3), 없으면 이름을 앞에 붙임. 216 px 를 넘으면 재료/값 사이(재료 없으면 Tooling 앞)에서 두 줄. 툴팁에 '장비에서 되읽은 "
              "density(g/cm3)·Z-factor·tooling(%)' 와 '재료는 마지막으로 적용한 재료'",
         v1="ui/thickness_panel.py:236-247; ui/thickness_panel.py:264-274",
         v2=_P, test="", status="todo", note=""),
    dict(id="UI-115", area="ui",
         desc="장비 쓰기 차단 사유(device_write_block): '공정 중' > '두께 모니터 미연결' > '두께 모니터 응답 없음', 없으면 ''. 영점 버튼 활성 = 사유 없음∧진행 중 아님, "
              "툴팁 공정 중 '공정 중에는 쓸 수 없음' 아니면 '두께·타이머 영점(B). 공정 중에는 쓸 수 없음'",
         v1="ui/thickness_panel.py:37-47; ui/thickness_panel.py:259-262",
         v2="v2/ch12/thickness.py; v2/ch12/commands.py", test="", status="todo", note=""),
    dict(id="UI-116", area="ui",
         desc="'영점': 공정 중이면 경고 '두께 영점'/'공정 중에는 영점을 할 수 없습니다.', 확인창 '두께 영점'/'두께 모니터의 두께·타이머를 0 으로 맞춥니다.\\n계속하시겠습니까?'(기본 No), "
              "확인 뒤 사유 재검사('{사유} — 영점을 보내지 않았습니다.'), zero('B', why='패널'), 실패 경고 '영점 실패'/사유",
         v1="ui/thickness_panel.py:318-357",
         v2=f"{_CMD}; v2/ch12/thickness.py; {_P}", test="", status="todo", note=""),
    dict(id="UI-117", area="ui",
         desc="두께 진행 영역: 공정 없음 '공정 없음'(막대 비움). 공정 중·끝난 뒤(다음 공정 전까지) '종료 방식 {mode}'·'목표 {target}'(—면 빈칸)·진행률 막대 'NN%'(없으면 숨김)·상태 문구(204 px 말줄임, 툴팁 전체)",
         v1="ui/thickness_panel.py:276-303; main.py:1336-1337",
         v2=f"v2/ch12/thickness.py; {_P}", test="", status="todo", note="mode·문구 내용은 thick 영역"),
    dict(id="UI-118", area="ui",
         desc="'Factor 설정': 창이 이미 떠 있으면 앞으로, 아니면 Factor 창 1개(파일=factor_file, ch, 저장 후 reload_factors, 재료 목록 파일, 서비스, 공정 중 확인 함수) 열기. 실패 "
              "'Factor 설정'/'설정창을 열 수 없습니다: …'. 앱 종료 때 패널 태스크 취소·창 닫기, 서비스 shutdown(2 s/대기 4 s)",
         v1="ui/thickness_panel.py:359-384; main.py:1423-1433",
         v2=f"{_CMD}; {_P}", test="", status="todo", note=""),

    # ───────────────────────────── Factor 창·재료 창 ─────────────────────────────
    dict(id="UI-119", area="ui",
         desc="Factor 창: 제목 'CH{n} Thickness Monitor — Factor 설정', 모달 500×440. tooling 표 3×3(행 Up/Mid/Down × 열 G1/G2/G3, 높이 126, 값 :g, 빈칸=미설정), "
              "안내 'Tooling (%, 10.0~399) — chuck 위치 × 건, 빈 칸 = 미설정'",
         v1="ui/thickness_factor_dialog.py:27-69; device/thickness_monitor.py:53; lib/thickness_factors.py:24-25",
         v2=_P, test="", status="todo", note=""),
    dict(id="UI-120", area="ui",
         desc="건별 재료 드롭다운(G1~G3): 편집 가능(삽입 안 함), 맨 위 '(선택 안 함)', 항목 '기호 — 이름'(label), 입력 찾기(대소문자 무시·포함). 옆에 'ρ {density:g} · Z {z:.3f}'"
              "(툴팁 note), 목록에 없으면 '목록에 없음', 미선택 '—'. 목록에 없는 저장값은 입력칸에 그대로",
         v1="ui/thickness_factor_dialog.py:71-91; ui/thickness_factor_dialog.py:146-198",
         v2=_P, test="", status="todo", note=""),
    dict(id="UI-121", area="ui",
         desc="Factor 창 안내·경고: '파일: … \\n재료 목록: …\\n저장하면 다음 공정 시작 때 적용됩니다(진행 중인 공정에는 영향 없음).', 읽기 경고(+재료 목록 경고+'G 재료 'x' 가 "
              "재료 목록에 없음 — 다시 고르십시오') 처음 8건과 '… 외 N건'",
         v1="ui/thickness_factor_dialog.py:93-106; ui/thickness_material_dialog.py:22",
         v2=_P, test="", status="todo", note=""),
    dict(id="UI-122", area="ui",
         desc="Factor '저장': tooling 칸마다 10.0~399 검사('… 숫자가 아님'/'… 범위(10~399%) 밖', 틀린 칸 빨강 #ffcdd2), 재료는 목록에 있어야 함(틀린 콤보 빨강). 오류면 경고 "
              "'Factor 설정'/'고칠 칸이 있어 저장하지 않았습니다:\\n\\n…', 저장(원자적 교체) 실패 '저장 실패(파일은 바뀌지 않음):\\n…', 성공 → 서비스 다시 읽기 → 창 닫음. "
              "저장은 장비에 쓰지 않음(공정 시작 때 씀). '취소' = 닫기",
         v1="ui/thickness_factor_dialog.py:306-344; ui/thickness_factor_dialog.py:137-144; lib/thickness_factors.py:56-68",
         v2=f"{_CMD}; v2/ch12/thickness.py; {_P}", test="", status="todo", note=""),
    dict(id="UI-123", area="ui",
         desc="Factor 창 '장비에 쓰기'(서비스 있을 때만 보임): chuck·건을 골라 그 tooling 칸 + 그 건 재료 density·Z 를 지금 창 값(저장 무관)으로 set_material(why='Factor 창 수동'), "
              "되읽어 확인. 칸 오류 '고칠 칸이 있어 장비에 쓰지 않았습니다:\\n\\n…', 차단 사유(공정 중 포함) '{사유} — 장비에 쓰지 않았습니다.', 실패 '장비에 쓰지 못했습니다(되읽기 확인 포함): …', "
              "성공 '장비에 적용됨 — {재료} · density {d:g} · Z {z:.3f} · tooling {t:g}%'(정보). 진행 중 버튼 잠금, 창 닫으면 취소. 안내 문구 WRITE_NOTE",
         v1="ui/thickness_factor_dialog.py:30; ui/thickness_factor_dialog.py:108-131; ui/thickness_factor_dialog.py:221-304",
         v2=f"{_CMD}; v2/ch12/thickness.py; {_P}", test="", status="todo", note=""),
    dict(id="UI-124", area="ui",
         desc="Factor 창 '재료 목록 편집' → 재료 창(지금 고른 재료 이름을 '사용 중'으로 넘김). 재료 저장 뒤 드롭다운·표시값 다시 읽기(고른 이름 유지) + 서비스 다시 읽기",
         v1="ui/thickness_factor_dialog.py:200-219",
         v2=_P, test="", status="todo", note=""),
    dict(id="UI-125", area="ui",
         desc="재료 창: 제목 'Thickness Monitor — 재료 목록', 모달 640×560, 열 '이름'·'기호'·'density (g/cm3)'·'Z-factor'·'note'(열 0·4 폭 170), 안내 '재료 목록 — 이름(재료를 구별하는 키, "
              "중복 불가)·기호·density 0.500~99.99·Z-factor 0.100~9.999', 읽기 경고 처음 6건, '파일: …' 와 다음 공정 적용 안내",
         v1="ui/thickness_material_dialog.py:20-61",
         v2=_P, test="", status="todo", note=""),
    dict(id="UI-126", area="ui",
         desc="재료 창 '행 추가'(새 행 이름 칸 편집 시작)·'행 삭제'(선택 행들, 없으면 현재 행). Factor 에서 쓰는 재료를 지우면 확인창 '재료 삭제'/'Factor 설정에서 쓰는 재료입니다: …\\n"
              "지우면 그 건은 '재료 목록에 없음'이 되어 Thickness 모드 시작이 거부됩니다(E845).\\n지울까요?'(기본 No)",
         v1="ui/thickness_material_dialog.py:92-115",
         v2=_P, test="", status="todo", note=""),
    dict(id="UI-127", area="ui",
         desc="재료 창 '저장': 완전히 빈 행 무시, 칸마다 이름 빈칸·density 0.500~99.99·Z 0.100~9.999 검사(틀린 칸 빨강), 이름 중복(대소문자 무시) '이름 중복(n행)'. 오류 처음 15건 "
              "경고 '재료 목록'/'고칠 칸이 있어 저장하지 않았습니다', 원자적 저장 실패 '저장 실패(파일은 바뀌지 않음)', 성공 → on_saved → 닫음",
         v1="ui/thickness_material_dialog.py:117-163; device/thickness_monitor.py:51-52",
         v2=f"{_CMD}; v2/ch12/thickness.py; {_P}", test="", status="todo", note=""),

    # ───────────────────────────── 서버 페이지(메인) ─────────────────────────────
    dict(id="UI-128", area="ui",
         desc="서버 페이지: 제목 'Server / Communication'(굵게, 기본+6pt 이상 12), 'Host Server' 묶음 Bind Host·Bind Port(읽기 전용), Running('UNKNOWN' → 'RUNNING'/'STOPPED'), "
              "Start/Stop/Restart 버튼 → 메인의 호스트 시작/정지/재시작",
         v1="runtime/server_page.py:104-163; runtime/server_page.py:229-235; main.py:380-393",
         v2=f"v2/robot_server/; {_P}", test="", status="todo", note=""),
    dict(id="UI-129", area="ui",
         desc="'Connected Clients' 목록(높이 140 고정, 안내 '※ 'Client connected/disconnected' 로그를 기반으로 표시합니다.'): NET 로그 정규식 Client (connected|disconnected|closed): (…)"
              "로 갱신 — 본문에 'connected' 가 있으면 추가, 아니면 제거. Pause 중에도 갱신",
         v1="runtime/server_page.py:58; runtime/server_page.py:165-180; runtime/server_page.py:302-304; runtime/server_page.py:340-353; host/server.py:173; host/server.py:497; host/server.py:505",
         v2=f"v2/robot_server/; {_P}", test="", status="todo", note="'disconnected' 에도 'connected' 가 들어 있어 제거되지 않음 — UI-145"),
    dict(id="UI-130", area="ui",
         desc="'Communication Log': 최대 5000줄(오래된 줄 자동 삭제)·Undo 끔·읽기 전용, 'Auto-scroll'(기본 켬, 스크롤바 바닥으로), 'Pause'(화면 표시만 멈춤), "
              "'Hide GET_SPUTTER_STATUS'(기본 켬: cmd=GET_SPUTTER_STATUS(_RESULT) 줄과 '[GET_SPUTTER_STATUS]' 붙은 PLC_REMOTE 줄을 화면에서만 숨김), 'Clear'(화면만 비움)",
         v1="runtime/server_page.py:59-63; runtime/server_page.py:182-227; runtime/server_page.py:306-338",
         v2=f"v2/robot_server/; {_P}", test="", status="todo", note=""),
    dict(id="UI-131", area="ui",
         desc="서버 로그 줄 형식 'YYYY-MM-DD HH:MM:SS [TAG] 본문'(본문 앞에 같은 형식 시각이 있으면 그 시각을 앞으로). 워커 스레드에서 오면 루프 스레드로 넘김, 루프 없으면 logging 으로만",
         v1="runtime/server_page.py:65-68; runtime/server_page.py:237-292",
         v2="v2/robot_server/", test="", status="todo", note=""),
    dict(id="UI-132", area="ui",
         desc="서버 로그 자동 저장(Pause/Hide 무관 전부): <LOG_ROOT>/CH1&2_Server/server_page_YYYYMMDD.log(mkdir 실패 시 LOCAL_FALLBACK_SERVER_DIR = <폴백>/Server), utf-8 추가 쓰기, "
              "1 s 타이머로 백그라운드 1스레드('ServerLogIO')에서 flush, 2000줄 쌓이면 즉시, 직전 flush 진행 중이면 다음 tick, 버퍼 50000줄 넘으면 앞 절반 버리고 "
              "'server_page 일별 로그 버퍼 상한 초과 — N줄 폐기' 1회",
         v1="runtime/server_page.py:80-99; runtime/server_page.py:355-463; lib/config_common.py:20-24",
         v2="v2/robot_server/; v2/ch12/outputs/", test="", status="todo", note="log 영역과 겹침"),
    dict(id="UI-133", area="ui",
         desc="'Save to file': 화면 로그 전체를 <LOG_ROOT>/CH1&2_Server/server_comm_YYYYmmdd_HHMMSS.log 로 저장, 아래 'Saved: 경로' 또는 'Save failed: …'",
         v1="runtime/server_page.py:465-485",
         v2=f"v2/robot_server/; {_P}", test="", status="todo", note=""),
    dict(id="UI-134", area="ui",
         desc="서버 페이지 closeEvent: 남은 버퍼 동기 기록, IO 풀 종료(대기), 파일 닫기",
         v1="runtime/server_page.py:487-510",
         v2="v2/robot_server/", test="", status="todo",
         note="미확인: 스택 안 자식 위젯이라 메인 창을 닫을 때 이 closeEvent 가 불리는 경로를 v1 에서 찾지 못함(마지막 1 s 분량 유실 가능)"),

    # ───────────────────────────── 로봇 서버 앱 페이지 ─────────────────────────────
    dict(id="UI-135", area="ui",
         desc="별도 로봇 서버 앱(RAW TCP 프록시)의 페이지: 제목 'Robot Server / Communication', 네비 버튼 없음, Host Server 묶음·Start/Stop/Restart·클라이언트 목록·로그 필터는 메인과 같음. "
              "앱 시작 때 페이지 표시, 'HOST_PROXY' 'Upstream endpoint = host:port', 프록시 자동 시작 후 RUNNING",
         v1="apps/robot_server/runtime/server_page.py:29-173; apps/robot_server/main_server.py:209-243",
         v2="v2/robot_server/", test="", status="todo",
         note="빌드·실행 스크립트에서 참조를 찾지 못함(grep main_server/robot_server) — 사용 여부 사용자 확인 필요"),
    dict(id="UI-136", area="ui",
         desc="로봇 서버 페이지 기록: <log_root>/CH1&2_Server/RobotServer/robot_server_page_YYYYMMDD.log(log_root 고정 'G:\\공유 드라이브\\VanaM_Sputter\\Sputter\\Logs', 실패 시 "
              "cwd/Logs/CH1&2_Server/RobotServer), 1 s 타이머로 UI 스레드에서 동기 쓰기, 2000줄이면 즉시, 실패 'Auto-save failed: …' 표시. 숨김은 cmd=GET_SPUTTER_STATUS 줄만. "
              "5000줄 넘으면 끝 4000줄만 남김, 자동 스크롤은 커서 끝 이동. 'Save to file' → robot_server_comm_YYYYmmdd_HHMMSS.log",
         v1="apps/robot_server/runtime/server_page.py:47-66; apps/robot_server/runtime/server_page.py:186-308; apps/robot_server/main_server.py:218",
         v2="v2/robot_server/", test="", status="todo", note=""),

    # ───────────────────────────── 죽은 코드(main.py) ─────────────────────────────
    dict(id="UI-137", area="ui",
         desc="PC↔Main 공정 로그 병합(_pending_log): 메인 완료 콜백은 _pending_log[ch][공정명]['pc'] 가 있으면 PC 값을 넘기지만, 'pc' 를 쓰는 곳이 없어 항상 None(메인 단독 행). "
              "PC 완료 콜백은 그 칸을 지우기만 함",
         v1="main.py:496-498; main.py:599-605; main.py:970-1014",
         v2="v2/ch12/outputs/", test="", status="na",
         note="_pending_log 에 'pc' 를 쓰는 코드 없음(grep main.py·runtime/) — 병합 경로는 죽은 코드, 관찰 동작은 '항상 메인 단독'"),
    dict(id="UI-138", area="ui",
         desc="_save_pc_only_row(save_pc_only)·_save_merged_row(save_process_log, CHAT_WEBHOOK_MONITOR_URL) 메서드",
         v1="main.py:1016-1054",
         v2="v2/ch12/outputs/", test="", status="na", note="호출하는 곳 없음(grep 결과 정의만)"),

    # ───────────────────────────── 화면 위젯 목록(.ui) ─────────────────────────────
    dict(id="UI-139", area="ui",
         desc="PC 페이지 위젯: 상태칸(pc_processState_edit), TSP 묶음('TSP', #·# Cycle·Base Pressure·U. Base Pressure·START/STOP·Auto Schedule HH:MM·Reserve 체크·예약 상태칸), "
              "Pre-Sputter 묶음(CH1/CH2 라디오·HH:MM·Waiting...·Remaining Time·Reserve·Cancel), Plasma Cleaning 묶음(CH1/CH2·Process Name·Base Pressure·Gas Flow·Working Pressure·"
              "RF Power·Process Time·For.P/Ref.P/Bias V/Temp/Hum 읽기 전용·START/STOP·'File\\nPath'), 로그창(10,650,1061×301)",
         v1="ui/main_window.py:30-293; ui/main_window.py:901-944",
         v2=_P, test="", status="todo", note="PC·TSP 버튼 동작은 pc/tsp 영역"),
    dict(id="UI-140", area="ui",
         desc="CH1 페이지 위젯: 'CH.1 Sputtering', Base Pressure, Process Name, M.S. Open, P.W. Select, Gun Target Name(1칸), Ar/O2 체크·유량, Working Pressure, RF Pulse/DC Pulse 라디오, "
              "펄스 파워·'RF/DC Pulse Freq [kHz]'·'Duty [%] / Off Time [µs]', Shutter Delay [min], Dep.Rate[nm/s], Thickness[nm], Process Time [min], For.P/Ref.P/Power/Voltage/Current "
              "읽기 전용, Process List 'Select File', START/STOP, 상태칸(230,0,841×61), RGA(230,90,841×311)·OES(230,420,841×351) 그래프, 로그창(230,800,841×200)",
         v1="ui/main_window.py:295-538; ui/main_window.py:945-978",
         v2=_P, test="", status="todo", note=""),
    dict(id="UI-141", area="ui",
         desc="CH2 페이지 위젯: 'CH.2 Sputtering', Base Pressure, Process Name, M.S. Open, P.W. Select, G1/G2/G3 체크·타겟명 3칸, Ar/O2/N2 체크·유량, Working Pressure, RF/DC Pulse 라디오·"
              "공유 펄스 칸 3개, DC1/DC2/RF 체크·파워 칸, Shutter Delay, Dep.Rate, Thickness, Process Time, For.P/Ref.P/Power/Voltage/Current 읽기 전용, Select File, START/STOP, 상태칸, "
              "그래프 2개, 로그창",
         v1="ui/main_window.py:540-785; ui/main_window.py:979-1019",
         v2=_P, test="", status="todo", note=""),

    # ───────────────────────────── 알려진 이상 동작(v1 그대로) ─────────────────────────────
    dict(id="UI-142", area="ui",
         desc="[이상 동작] Power Select 쓰기가 실패하면 버튼 눌림만 되돌리고(blockSignals) 글자 갱신 신호도 막혀 'Power\\nSelect: ON' 처럼 실제와 다른 글자가 남는다",
         v1="ui/main_window.py:271-274; main.py:1246-1255",
         v2=_P, test="", status="todo", note="v1 동작 그대로 둘지 decisions 에(사용자 확인 필요)"),
    dict(id="UI-143", area="ui",
         desc="[이상 동작] UI 현재값 프리스퍼터는 시작 가드(쿨다운·입력 검사 등)가 알림창으로 거절해도 True 를 돌려줘, 예약은 60 s 동안 시작 신호를 기다린 뒤 "
              "'시작 신호 미확인 (timeout)' → '실행 실패'. 새벽 무인 시각에 경고창이 뜬다",
         v1="runtime/chamber_runtime.py:4500-4515; runtime/pre_sputter_runtime.py:396-404; runtime/pre_sputter_runtime.py:456-467",
         v2="v2/ch12/pre_sputter.py", test="", status="todo", note="decisions 에(사용자 확인 필요)"),
    dict(id="UI-144", area="ui",
         desc="[이상 동작] Pre-Sputter 전용 레시피를 한 번 고르면 화면에서 비울 방법이 없다('UI 현재값 사용' 으로 되돌리려면 QSettings 를 지워야 함)",
         v1="main.py:1741-1758; main.py:1704-1708",
         v2=f"v2/ch12/pre_sputter.py; {_P}", test="", status="todo", note="decisions 에(사용자 확인 필요)"),
    dict(id="UI-145", area="ui",
         desc="[이상 동작] 서버 페이지 클라이언트 목록: 'Client disconnected: …' 에도 'connected' 가 들어 있어 추가로 처리(제거 안 됨). 'Client closed: …' 때에야 제거",
         v1="runtime/server_page.py:340-353; apps/robot_server/runtime/server_page.py:227-240; host/server.py:497; host/server.py:505",
         v2=f"v2/robot_server/; {_P}", test="", status="todo", note=""),
    dict(id="UI-146", area="ui",
         desc="[이상 동작] main 이 넣는 CH1/CH2 Base Pressure 기본값(PROCESS_DEFAULT_BASE_PRESSURE 1e-05)은 ChamberRuntime 생성 때 기본값(CH1 5e-6, CH2 9e-6)이 바로 덮어써 화면에 나타나지 않는다",
         v1="main.py:203; main.py:1108-1118; runtime/chamber_runtime.py:814; runtime/chamber_runtime.py:3342; runtime/chamber_runtime.py:7823; runtime/chamber_runtime.py:7839",
         v2=f"v2/ch12/blocks.py; {_P}", test="", status="todo", note=""),
    dict(id="UI-147", area="ui",
         desc="[이상 동작] 레시피 행 → 화면 반영에서 체크박스(gun1~3·Ar/O2/N2·main_shutter·power_select·use_dc_power 등)는 값이 정확히 'T' 일 때만 체크 — 't'·'TRUE'·'1' 은 화면에 해제로 "
              "보이지만 실제 공정 인자(정규화 tf)는 참으로 쓴다(펄스 라디오만 T/TRUE/1/Y/YES)",
         v1="runtime/chamber_runtime.py:3538-3577; runtime/chamber_runtime.py:3471-3473; runtime/chamber_runtime.py:6159-6160",
         v2=_P, test="", status="todo", note=""),
    dict(id="UI-148", area="ui",
         desc="[이상 동작] 종료 차단은 CH 의 is_running(프로세스 컨트롤러)만 보므로 프리플라이트·척 이동·큐 행 사이·delay 대기처럼 is_busy 만 참인 구간에는 종료 확인창이 바로 뜬다",
         v1="main.py:1120-1150; runtime/chamber_runtime.py:3270-3290",
         v2="v2/ch12/app.py", test="", status="todo", note="decisions 에(사용자 확인 필요)"),
    dict(id="UI-149", area="ui",
         desc="[이상 동작] 런타임 덤프: _DUMP_IN_PROGRESS 를 True 로 둔 뒤 스냅샷 수집·폴더 만들기에서 예외가 나면 False 로 되돌리지 않아 이후 모든 덤프가 'already in progress' 로 건너뛰어진다",
         v1="util/runtime_dump.py:214-225; util/runtime_dump.py:239-243",
         v2=_CMD, test="", status="todo", note=""),
    dict(id="UI-150", area="ui",
         desc="[이상 동작] 'PLC COIL CSV ... ({iv:g}s)' 시작 로그 문구가 말줄임표 그대로(경로·성공 여부 표시 없음)",
         v1="main.py:357-359",
         v2="v2/ch12/system.py", test="", status="todo", note="문구만"),
]
