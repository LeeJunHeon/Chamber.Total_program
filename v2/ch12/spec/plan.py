r"""CH1&2(챔버12) 장비 프로그램 v2 재개발 — 작업 지시 원문과 진행표.

이 독스트링의 '지시 원문'은 사용자 지시(2026-10-30)를 요약하지 않고 그대로 옮긴 것이다.
새 턴을 시작하거나 대화 요약이 일어난 뒤에는 먼저 이 파일·decisions.py·git log --oneline -30 을 읽고 이어 간다.
진행표(PROGRESS)는 단계마다 갱신한다.

기준 커밋(시작 시각 기록)
- Chamber.Total_program main = 66efc7b1d079dfb137dd487c4b444160744fc9cb (rewrite/v2 의 출발점)
- vanam-process chk2-web     = db72813b814208b38a273e08035d4cd197f3d428 (ch12-web 의 출발점)
- Chamber.K_program rewrite/v2 = de8e946b14fa5fe4bff61e671df2b72ac637f072 (참고 구현, 읽기만)

───────────────────────────────── 지시 원문 ─────────────────────────────────
CH1&2(챔버12) 장비 프로그램 v2 재개발 — 프로토타입 완성까지 사람에게 묻지 않고 끝까지 진행한다. 이 세션에는 Chamber.Total_program, vanam-process, Chamber.K_program 세 저장소가 있다. 답변·주석·독스트링·커밋 메시지는 한국어로 쓴다.

■ 0. 배경과 목표
- 운영 중인 CH1&2 스퍼터 장비 프로그램(Chamber.Total_program main, 이하 v1)은 동작에 문제가 없다. 문제는 구조다: 파일이 수천 줄씩 커짐(runtime/chamber_runtime.py 8,924줄 등), 장비마다 방식이 제각각, PySide6/qasync 에 묶임, 가끔 프로그램이 죽음(2026-09-20 04:41 0xc0000409, 직전 '가상 메모리 부족' 팝업 — util/self_watch.py 독스트링).
- 기능은 하나도 빼지 않고 코드는 처음부터 새로 쓴다. 정답은 v1 코드다.
- 참고 구현: 챔버K v2(Chamber.K_program rewrite/v2 의 v2/)와 그 포털 화면(vanam-process chk2-web 의 components/ops2/chk, lib/ops2, app/api/ops2, hmi/, scripts/hmi-*.mjs). 같은 사용자가 같은 목표로 만든 것이므로 구조·이름·계약·시험 방식을 최대한 따른다. 시작 전에 챔버K v2 의 v2/common/*.py, v2/chk/app.py·system.py·core.py·commands.py·params.py·blocks.py·process_engine.py·safety_rules.py·safety.py·web_schema.py·uplink.py·local_server.py·hub.py·cmd_guard.py, v2/tools/bench_probe.py, v2/tests(골든·v1 비교 시험)의 독스트링을 읽는다.
- 결과물: 시뮬레이터(sim)로 끝까지 도는 프로토타입. 실제 장비 검증(bench·운전)은 이 작업 뒤에 사람이 한다.

■ 1. 저장소·브랜치
- Chamber.Total_program: origin/main 최신에서 rewrite/v2 를 만들어 여기서만 작업·push. 시작 시 main 커밋 해시를 기준 커밋으로 기록한다. 기존 v1 파일(v2/ 밖)은 수정하지 않는다(.gitignore 에 v2 항목 추가만 허용).
- vanam-process: origin/chk2-web 에서 ch12-web 을 만들어 여기서만 작업·push. 챔버K 화면과 기존 기능을 깨뜨리지 않는다. 배포하지 않는다. DB 변경은 추가만(기존 표 변경·삭제 금지). AGENTS.md 규칙(Next.js 문서 먼저 읽기)을 따른다.
- Chamber.K_program: rewrite/v2 를 읽기만 한다(push 금지). 참고한 커밋 해시를 기록한다.
- 시작하자마자 각 저장소 .git/hooks/pre-push 에 보호 훅을 둔다: Chamber.Total_program 은 main, vanam-process 는 main·chk2-web 으로의 push 를 거부, Chamber.K_program 은 모든 push 를 거부.
- 커밋은 의미 단위로 작게, 앞에 단계 번호(예 "2-3: 1초 스냅샷 기록"). 최소 1시간마다, 그리고 단계가 끝날 때마다 push 한다(세션이 멈춰도 잃지 않게). PR 은 만들지 않는다.

■ 2. 절대 규칙
1) 기능 누락 금지 — v1 의 모든 기능·화면 동작·설정 키·오류 코드·로봇 명령·기록 출력·알림이 v2 어딘가에 있어야 한다. 빼거나 바꾸고 싶으면 v1 과 같게 구현하고 결정 목록에 '사용자 확인 필요'로 남긴다.
2) 동작 보존 — 공정 순서·타이밍·판정·장비로 보내는 명령 바이트·로봇 응답은 v1 과 같게. 예외는 3장의 '합의된 변경'뿐이다. 알려진 이상 동작(예: use_*_pulse=F 인데 freq·duty 칸에 값이 남아 있으면 펄스 전원이 켜짐 — _run_select 의 dc_requested·rf_requested)도 v1 과 같게 두고 결정 목록에 올린다.
3) 제어 로직은 가장 단순하게 — 합의된 것 외에 새 판정 규칙·자동 보정·알림을 붙이지 않는다(사용자 원칙: 규칙을 많이 붙이면 늘 문제가 됐다).
4) 실제 장비·외부 서비스에 접속하지 않는다 — 시험은 시뮬레이터와 127.0.0.1 만. NAS·장비망(192.168.1.x)·구글챗·운영 포털 접속 금지. 토큰·웹훅 같은 비밀값은 커밋하지 않는다.
5) md 파일을 새로 만들지 않는다(README·계획서·작업 기록 모두). 명세·결정·진행표는 챔버K처럼 파이썬 모듈(독스트링·데이터 표)로 v2/ch12/spec/ 에 둔다.
6) 추측 금지 — 장비 사실(주소·명령·응답·타이밍)은 v1 코드(파일:줄)에 근거한다. 근거가 없으면 '미확인'으로 적고 v1 과 같게 한다.
7) 검증은 py_compile 로 끝내지 않는다 — 실제 import 와 실행으로 확인한다. 코드를 끼워 넣을 때 앞뒤 줄이 데코레이터인지 확인한다(과거 e38d695: 헬퍼가 @dataclass 와 class 사이에 들어가 import 단계에서 즉사).
8) 장비 노트북은 Windows 다. Python 3.11 문법까지만 쓴다(3.12 전용 문법 금지). 윈도우 전용 부분(콘솔 닫기·로그오프 처리, 자동 시작)은 분리하고, 리눅스에서 못 돌린 시험은 최종 보고의 '윈도우 확인 필요'에 적는다.

■ 3. 목표 구조와 합의된 변경
(1) 폴더·원칙
- v2/common: 챔버K v2/common 을 복사해 시작한다(출처 커밋을 독스트링에). 추가할 것: Modbus TCP(PLC 192.168.1.2:502 직결), MOXA NPort TCP 시리얼 전송, CH1·CH2 동시 운전. 웹 계약(messages.py)은 챔버K와 호환을 유지한다.
- v2/ch12: 장비 쪽 전부(드라이버·조립·공정 엔진·블록·안전 규칙·명령·파라미터·설정·기록·웹 스키마·로컬 서버·웹 연결·저널·spec).
- v2/robot_server: 로봇 서버(별도 프로그램). v2/supervisor: 감시·재시작 프로그램(별도, 표준 라이브러리만). v2/hmi: 포털에서 빌드해 넣는 노트북 화면(직접 편집 금지). v2/tests, v2/tools.
- 파일 하나는 대략 600줄 이내·책임 하나, 이름과 방식을 통일한다. 제어 프로그램·로봇 서버에 Qt/qasync 를 쓰지 않는다. 의존성은 최소로.
- OES·RGA 는 업체 DLL·라이브러리 때문에 지금처럼 별도 프로세스 워커로 둔다(2026-09-30 OES 드라이버 BSOD 이력). 워커 안의 장비 호출 순서·인자는 바꾸지 않고 메인과의 연결부만 v2 방식으로 바꾼다. apps/rf_service·apps/log_sync_service 는 독립 도구라 그대로 쓰고 v2 기록 경로·형식과 맞는지만 확인한다.
- 실행·배포는 챔버K(run_chk_v2.cmd, 소스 실행)처럼 하고, v1 처럼 PyInstaller onedir 로 묶는 도구도 둔다. 어느 쪽을 쓸지는 결정 목록에.
(2) 1초 동기 읽기·기록
- 장비마다 다르던 주기(IG 10 s, DC·DC Pulse·RF Pulse 5 s, MFC 3 s, PLC 코일 로그 5 s, TSP 10 s 등) 대신, 정각 1초마다 모든 장비를 한 번 읽어 한 행으로 묶는다(챔버K clock·snapshot_bus·snapshot·recorder 방식 — 품질 ok/late/stale/fail/off/suppressed 와 나이, 빈 값은 채우지 않음).
- 한 행 = CH1·CH2·공유 장비 값 + 그 시각 목표값(설정 파워·유량·압력·온도 등) + 공정 맥락(run id·챔버·블록 번호·종류·레시피 판·소유자 robot/web/local) + 프로그램 상태(메모리·핸들·스레드 — self_watch 대체).
- 기록은 1초지만, v1 이 1초보다 빨리 읽던 제어·판정(인터락 확인, 램프, 게이트·척 재확인, 두께 종료 판정 0.5 s, TSP 확인 등)은 그 속도를 유지한다(worker 안에서).
- 장비별 1초 예산(질의 수·바이트·간격)을 장비 사실 표에 적는다. 1초에 못 들어가면 번갈아 읽는다(자기 품질·나이). 실측은 나중에 bench 에서 하므로 읽기 전용 bench 도구를 만든다(챔버K tools/bench_probe 처럼).
- 일별 SQLite(WAL)+CSV, NAS 사본은 설정이 있을 때만. 기록이 tick·제어를 막지 않는다.
- v1 기록 출력(공정 txt, Calib CSV, CH1/CH2 엑셀, PLC 코일 CSV, OES·RGA CSV, 카메라 사진, selfwatch, Robot_YYYYMMDD.csv 등)은 같은 위치·형식으로 계속 낸다(1초 스냅샷에서 만들어도 됨). Robot_YYYYMMDD.csv 는 ALD 프로그램과 같이 쓰는 파일이다 — util/host_process_log.py 의 공통 규약(잠금 파일·기록키 중복 검사·로컬 pending/open 복구)을 그대로 지킨다.
(3) 블록 레시피와 공정 중 자유 제어
- 챔버K blocks.py·process_engine.py 방식: 레시피 = 블록 목록(JSON), 순서 자유. v1 레시피 열 전부(recipes/Recipe_Template.csv·xlsx — power_change_time·change_power_value, 두께 dep_rate·thickness·tooling·density·z_factor, 히터, delay 행, # 열 test 행 포함)는 기본 템플릿 블록으로 바뀌고, 펼친 스텝이 v1 과 같아야 한다 — tests/data/process_sequences_golden.json 7개 시나리오와 v1 코드로 만든 열 조합 골든으로 확인. 레시피 값 검사(E230)도 같게.
- 공정 중 명령(웹·로컬 화면에서, 로봇은 제외):
  · 값 수정 PROCESS_ADJUST(챔버K와 같게) — 지금 블록은 바로 쓰고 되읽어 확인, 뒤 블록은 예약.
  · 블록 건너뛰기 — 블록 정의에 건너뛸 수 있는지와 건너뛸 때 정리할 것을 둔다(정의 없으면 거부).
  · 일시정지·재개 — 블록마다 의미를 정의한다(대기 = 타이머 정지, 파워 램프 = 현재 값 유지 등). 못 멈추는 블록은 거부.
  · 시퀀스 편집 — 아직 시작 안 한 블록만 추가·삭제·순서 변경. 편집마다 레시피 판 번호 +1, 전체 재검사, 원래 레시피와 실제 실행 순서를 모두 기록.
- 어떤 편집으로도 안전 규칙·인터락·두절 감시·끝날 때 파워 끄기는 빠지지 않는다(챔버K: STOP·이상 중단 정지 순서 고정, 끝 정지에서 파워 끄기 단계는 못 뺌).
- 플라즈마 클리닝, 프리스퍼터 예약, TSP, CH2 히터, 두께 모니터(nm·nm/s, 레시피 값 우선·비면 설정값, 장비가 알려 주는 두께가 목표에 닿으면 종료 — 단순하게), ETA(min_total_s·eta.CHn.remaining_s, 확정 전에는 null — 추정값을 보내지 않음)도 v1 과 같게.
- CH1·CH2 는 실제로 동시에 돈다: 엔진은 챔버마다, 공유 장비(PLC 링크·카메라 등)는 명시적 소유권으로. 동시 운전 시험은 필수.
(4) 명령 경로 하나·출처 표·파라미터
- 웹·로컬 화면·로봇 명령이 모두 같은 명령 처리 한 곳을 지난다(챔버K core.command) — 상태·인자 검사 → 실행 → 결과. 인터락 판단은 여기에만 둔다.
- 명령 표에 명령마다 받는 출처(web/local/robot)를 적는다. 기본값은 전환 기간용으로 로컬 화면에서도 전부 가능. 나중에 '공정 시작·레시피·설정은 포털에서만'으로 바꾸는 것은 설정만 바꿔서 되게 한다.
- 파라미터마다 정의(형식·단위·범위·기본값 = v1 값·적용 시점 live/idle/restart·쓰는 곳). 설정은 판 번호·되돌리기(챔버K ConfigStore). v1 config/settings.json(모든 키)과 thickness json 을 v2 설정으로 옮기는 변환 도구와 시험을 만든다. 모르는 키는 조용히 버리지 않고 분명히 알린다(v1 의 '선언 안 된 키 무시'·'채널 설정 import 시 복사' 문제를 구조로 없앤다).
- 오류 코드는 v1 errors/ 를 그대로 잇는다 — 로봇에 나가는 코드·문구·필드 동일.
(5) 로봇 서버 분리
- v2/robot_server = 별도 프로그램, 0.0.0.0:50070(HOST_SERVER_PORT), host/protocol.py 의 16바이트 헤더+JSON 그대로. host/setup.py 의 18개 명령(GET_SPUTTER_STATUS, GET_LOADING_1_SENSOR, GET_LOADING_2_SENSOR, GET_RECIPE, START_SPUTTER, START_PLASMA_CLEANING, VACUUM_ON, VACUUM_OFF, 4PIN_UP, 4PIN_DOWN, CH1/CH2_GATE_OPEN, CH1/CH2_GATE_CLOSE, CH1/CH2_CHUCK_UP, CH1/CH2_CHUCK_DOWN)의 이름·응답 필드·오류 코드·ETA·응답 시점을 v1 과 바이트 단위로 같게. host/server.py 의 연결·프레이밍·로그 동작도 같게.
- 로봇 서버는 판단하지 않는다 — 요청을 제어 프로그램의 내부 명령 API(127.0.0.1 전용, 출처 robot)로 넘기고 결과를 v1 형식으로 돌려줄 뿐이다. 진공·벤트·게이트·척·핀 시퀀스와 인터락(게이트 상태·busy·잠금)은 제어 프로그램 쪽에서 v1 host/handlers.py 동작 그대로.
- 제어 프로그램이 꺼져 있거나 재시작 중이면 로봇 연결은 유지하고, 상태 질의에는 마지막 상태를 기존 형식대로('공정 중'), 시작·이동 명령은 v1 기존 오류 코드 중 맞는 것으로 거부한다(무엇을 썼는지 결정 목록에).
- 시험: v1 handlers 를 가짜 장비로 실제 실행한 응답(어려운 부분은 v1 코드에서 뽑은 골든)과 v2 응답을 명령 × 상태 전 조합으로 비교한다.
(6) 자동 재시작·공정 이어가기 (사용자와 합의한 규칙)
- v2/supervisor: 제어 프로그램이 비정상 종료하면 몇 초 안에 다시 켠다. 10분에 3번 넘게 죽으면 재시작을 멈추고 기록한다(장비 끊김 알림은 포털 몫). 정상 종료면 다시 켜지 않는다. 로그온 자동 시작은 등록 도구만 만들고 실제 등록은 사람이 한다(v1 처럼 schtasks /it). v1 apps/recovery_service 는 메인이 쓰지 않아 동작하지 않던 것이다 — 참고만.
- 저널: 챔버마다 1초마다 원자적으로 디스크에 저장한다 — run id, 소유자, 레시피 전체·판 번호(공정 중 편집 포함), 현재 블록·블록 경과, 메인 경과, 공정 중 수정값, 히터·두께 상태, 처리한 명령 id 와 결과(웹·로봇 — 재시작 뒤 같은 명령이 다시 와도 실행하지 않게).
- 다시 켜졌을 때 저널이 '공정 중'이면 아래가 모두 맞을 때만 이어간다: ① 꺼져 있던 시간 ≤ RESUME_MAX_GAP_S(기본 30 s) ② 이 공정에서 첫 재시작 ③ 다시 읽은 장비 상태가 저널의 기대 상태와 일치(파워 출력·셔터·가스·압력·히터 RUN·워치독 이상 없음 등 — 블록 종류별 확인 목록을 정의) ④ 지금 블록이 상태를 유지하는 블록(대기·가스 안정화·프리스퍼터·메인 증착·히터 유지) — 진공·벤트·게이트·척·핀 이동·장비 설정 쓰기 도중·정지 순서 중은 안 됨 ⑤ RESUME_ENABLED 켜짐.
- 이어갈 때: 꺼져 있던 시간도 경과에 넣는다(장비는 계속 켜져 있었음). 설정값을 저널대로 다시 쓰고 되읽어 확인한다(실패하면 안전 정지). 공정 기록·웹에 '복구됨(공백 N s)' 이벤트를 남긴다. 1초 기록의 빈 구간은 채우지 않는다.
- 하나라도 아니면: STOP 과 같은 정지 순서로 끄고 run_end error, 사유 PROGRAM_RESTART. PC 재부팅(BSOD·정전)처럼 공백이 긴 경우는 항상 이쪽이다.
- 의도적 종료(콘솔 닫기·Ctrl+C·로그오프)는 저널에 먼저 '의도적 종료'를 적어 이어가지 않게 하고, 챔버K 결정 4 처럼 파워 출력만 끈다. 공정이 없을 때 켜졌는데 파워가 남아 있으면 파워만 끈다(챔버K S51).
- RESUME_ENABLED·RESUME_MAX_GAP_S 는 파라미터 표에 둔다(포털에서 변경 가능).
- 시험: sim 에서 모든 블록 종류·임의 시각에 제어 프로그램을 죽였다 살려 이어가기/안전 정지 판단, 명령 중복 실행 없음, 반복 크래시 제한을 확인한다.
(7) 웹 연결·화면 (포털 vanam-process ch12-web)
- 노트북: 로컬 서버(127.0.0.1, 챔버K local_server 와 같은 /api/v1 hello·schema·stream·command, v2/hmi 정적 파일) + 웹 상시 연결(챔버K uplink·messages 계약 그대로). 장비 ID 는 CH12, 시뮬레이터는 CH12-SIM. 웹이 끊겨도 공정·안전은 노트북에서 계속된다.
- 포털: 챔버K 화면 부품·kit 를 재사용해 components/ops2/ch12 를 만든다 — CH1·CH2 나란히(장비 그림: 게이트·척·밸브·셔터, 1초 값, 현재 블록·남은 시간·ETA, 알람·로그), CH2 히터·두께 패널, 플라즈마 클리닝, TSP, 로봇 연결 상태, 조작(STOP·ALL STOP, 공정 중 수정·건너뛰기·일시정지·시퀀스 편집, 레시피 적재·시작, 설정, PLC 버튼), 레시피 블록 편집기, 이력. 레시피 검사는 lib/ops2/ch12(TS)로 만들고 파이썬과 같은 결과인지 골든으로 확인한다(챔버K W-4b 방식). 포털 서버 쪽(app/api/ops2, lib/ops2)은 CH12 를 추가하는 방식으로 하고 챔버K 동작은 바꾸지 않는다.
- 같은 화면을 local 모드로 빌드해(hmi/ 진입점 추가, hmi-build·hmi-copy 가 CH12 대상도 지원) Chamber.Total_program v2/hmi 에 넣는다(build.json 에 출처).
- 구글챗은 챔버K처럼 포털이 보낸다. v1 이 보내던 카드(시작·종료·종료 사유·장비 오류 등)를 같은 계기·같은 내용으로 보내고, 새 알림은 늘리지 않는다(사용자 원칙: 최소). 재시작·이어가기 결과만 카드 1장을 더한다.
- npm run lint·build·기존 시험(ops2:golden 등)이 모두 통과해야 한다.

■ 4. 알려진 장비 사실 (v1 근거와 함께 장비 사실 표에)
- PLC 는 MOXA 를 거치지 않고 192.168.1.2:502 직결. IG·MFC·DC Pulse·RF Pulse·TSP·두께 모니터는 MOXA NPort TCP — 60초 무통신이면 끊긴다.
- 장비 전체(PLC·RGA 2대·NPort)가 노트북 유선 랜 하나를 공유한다.
- 가스 밸브 인터락은 PLC 가 처리한다. RF/DC 파워에는 PLC 인터락이 없어 소프트웨어가 꺼야 한다.
- Power Select(SW_RF_SELECT)는 RF 전용 릴레이 — DC Pulse 중에는 켜지 않는다.
- CH1 가스는 Ar·O2 만. CH2 히터는 VARITAP VSCD-30 + PLC 내장 PID, 래더 워치독(D114 가 30초 변화 없으면 WD_ERR).
- DC Pulse 명령 간격 DCP_GAP_MS 150.
- 구글 드라이브(G:) 스트리밍에 새 파일을 쓰면 성공처럼 보이고 사라질 수 있다 — 항상 로컬에 먼저 쓰고 사본만 보낸다.
- cv2.imwrite 는 한글 경로에서 조용히 실패한다(imencode + write_bytes). openpyxl max_row 는 서식만 있는 행도 센다. 워커 exe 는 onedir 로(onefile 은 kill 이 부트로더만 죽인다).

■ 5. 진행 순서 (단계마다: 완료 조건 확인 → 별도 서브에이전트 검증 → 커밋·push → 다음)
0단계 준비·전수 조사
 - 브랜치·훅·기준 커밋 기록, v2/common 복사.
 - v1 전수 목록 v2/ch12/spec/inventory.py: 기능, 화면 동작(main.py·ui/·runtime/server_page.py 등 — 프리스퍼터 예약, Power Select, 런타임 덤프, PC 채널 선택, 히터·두께 패널, Factor·재료 창 등), 로봇 명령, 설정 키, 오류 코드, 기록 출력(경로·파일명·열), 구글챗 카드, 타이머·타임아웃·주기, 인터락·중단 조건, 장비 명령, 워커 앱(apps/*), 기존 시험(tests/ 92개 파일·889개 항목)이 확인하는 동작. 항목마다 id·설명·v1 위치(파일:줄)·v2 담당·v2 시험·상태(todo/done/na+이유).
 - 영역별로 서브에이전트를 병렬로 써서 만들고, 다른 서브에이전트에게 목록 없이 v1 코드만 주어 빠진 항목을 찾게 한다.
 - spec 표: 장비 사실(연결·프로토콜·명령·간격·타임아웃·1초 예산), 안전 규칙(S 번호), 파라미터 정의, 명령 표(인자·출처·상태), 블록 정의(건너뛰기·일시정지·이어가기 가능 여부 포함), 결정 목록 decisions.py(id·내용·이유·사용자 확인 필요), 의도된 차이 differences.py, 진행표 plan.py.
 - test_inventory_complete: 모든 항목이 done(담당·시험 있음) 또는 na(이유)일 때만 통과 — 마지막에 반드시 통과.
1단계 전송·드라이버·시뮬레이터 — PLC(Modbus TCP), MFC×2, IG×2, DC, DC Pulse, RF(PLC DAC/ADC), RF Pulse(CESAR AE Bus), TSP, 두께 모니터 STM-100, CH2 히터(PLC), OES·RGA 워커 연결, 카메라. 장비로 보내는 바이트가 v1 과 같은지 시험. 통신층 명령 가드(챔버K cmd_guard).
2단계 1초 스냅샷·기록·v1 기록 출력.
3단계 공정 엔진·블록·템플릿 골든·자유 제어 4종·CH1/CH2 동시·플라즈마 클리닝·프리스퍼터·TSP·히터·두께·ETA.
4단계 안전 규칙·STOP/ALL STOP 순서·시작 시 정리.
5단계 저널·감시 프로그램·이어가기.
6단계 로봇 서버·v1 응답 비교.
7단계 로컬 서버·웹 연결·설정 저장소·v1 설정 변환.
8단계 포털 화면·레시피 검사 골든·hmi 빌드·v2/hmi 넣기.
9단계 마무리 — 아래 '7. 최종 확인' 전부.

■ 6. 막혔을 때·맥락 유지
- 사람에게 묻지 않는다. 정보가 없거나 의견이 갈리면 v1 과 같은 쪽·더 안전한 쪽으로 정하고 decisions.py 에 남긴 뒤 계속한다.
- 같은 실패가 3번 반복되면 원인과 시도를 decisions.py 에 적고 범위를 줄여 다음으로 넘어갔다가 마지막에 돌아온다.
- 패키지는 PyPI·npm 만 된다. v1 import 가 필요한 비교 시험에서 PySide6 가 안 되면 v1 의 순수 로직만 import 하거나 v1 코드에서 골든을 뽑는다.
- 대화가 길어지면 앞부분이 요약된다. 0단계 첫 커밋에서 이 지시 전체(규칙·합의된 변경·장비 사실·진행 순서·최종 확인)를 요약하지 말고 v2/ch12/spec/plan.py 독스트링으로 옮긴다.
- 새 턴을 시작하거나 요약이 일어난 뒤에는 먼저 plan.py·decisions.py·git log --oneline -30 을 읽고 이어 간다. plan.py 진행표(단계·상태·마지막 커밋·다음 할 일)는 단계마다 갱신한다.
- 검증은 작업한 맥락과 분리한다 — 단계마다 별도 서브에이전트가 새로 clone → 의존성 설치 → 모든 모듈 실제 import → 전체 시험을 돌리고 결과만 보고한다.

■ 7. 최종 확인 (전부 실제 명령 출력으로 보여 준다)
1) Chamber.Total_program rewrite/v2: v2 전체 pytest 통과(개수 포함), 모든 모듈 import 성공.
2) test_inventory_complete 통과(todo 0).
3) sim 종합 시나리오 통과: CH1·CH2 동시 공정, 로봇 명령 전체 흐름, 플라즈마 클리닝, CH2 히터 공정, 두께 종료 공정, 공정 중 수정·건너뛰기·일시정지·시퀀스 편집, 재시작 후 이어가기와 안전 정지, 웹 끊김·재연결, 가상 시계 7일 연속 운전(메모리·큐가 늘지 않음).
4) vanam-process ch12-web: lint·build·기존 시험·CH12 골든 통과(챔버K 시험 포함).
5) 기준 커밋 이후 main 에 들어온 커밋을 모두 v2 에 반영(목록 출력, 남은 것 0 — 사용자가 작업 중에도 main 에 커밋할 수 있다).
6) 두 저장소 git status 깨끗, push 완료.
7) 최종 보고를 채팅에 출력(md 파일 아님): 만든 것 요약·시험 수, 결정 목록 중 '사용자 확인 필요' 전체, 의도된 차이 전체, 윈도우·실제 장비에서 확인할 것, 개발 노트북(윈도우)에서 v2 를 sim 으로 실행하고 노트북 화면을 여는 방법.

지금 0단계부터 시작한다.
───────────────────────────────── 지시 원문 끝 ─────────────────────────────────

추가 지시(/goal, 2026-10-30): 위 지시를 끝까지 완료한다. 완료는 '7. 최종 확인' 7개가 모두 실제 명령 출력으로 대화에 보일 때.
사람에게 묻지 않고 결정은 decisions.py 에 남긴다. 300턴이 지나면 그때까지의 진행과 남은 일을 보고하고 멈춘다.
"""
from __future__ import annotations

BASE_COMMITS = {
    "Chamber.Total_program/main": "66efc7b1d079dfb137dd487c4b444160744fc9cb",
    "vanam-process/chk2-web": "db72813b814208b38a273e08035d4cd197f3d428",
    "Chamber.K_program/rewrite/v2": "de8e946b14fa5fe4bff61e671df2b72ac637f072",
}

# 진행표 — (단계, 상태 todo/doing/done, 마지막 커밋 요약, 다음 할 일)
PROGRESS = [
    ("0", "doing", "0-1: 지시 원문·기준 커밋·v2/common 복사", "inventory.py 전수 목록·spec 표"),
    ("1", "todo", "", "전송(Modbus TCP·NPort)·드라이버·시뮬레이터·명령 바이트 시험"),
    ("2", "todo", "", "1초 스냅샷·기록·v1 기록 출력"),
    ("3", "todo", "", "공정 엔진·블록·템플릿 골든·자유 제어·동시 운전"),
    ("4", "todo", "", "안전 규칙·STOP/ALL STOP·시작 정리"),
    ("5", "todo", "", "저널·감시 프로그램·이어가기"),
    ("6", "todo", "", "로봇 서버·v1 응답 비교"),
    ("7", "todo", "", "로컬 서버·웹 연결·설정 저장소·v1 설정 변환"),
    ("8", "todo", "", "포털 화면·레시피 검사 골든·hmi 빌드"),
    ("9", "todo", "", "최종 확인 7개"),
]
