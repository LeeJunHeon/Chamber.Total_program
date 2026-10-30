"""로봇 서버(별도 프로그램, python -m v2.robot_server) — 사용자 지시 3장 (5).

0.0.0.0:HOST_SERVER_PORT(50070)에서 v1 host/protocol.py 의 16바이트 헤더 + JSON 프레임을 받아, 연결·프레이밍·로그·라우터
(미등록 E106·공백 E105)는 v1 host/server.py·router.py 와 같게 처리하고, 등록된 18개 명령은 판단 없이 제어 프로그램의
내부 명령 API(127.0.0.1:ROBOT_API_PORT, POST /robot/v1/command — forward.py)로 넘긴다. 제어 프로그램이 꺼져 있으면
cache.py 규칙대로 답하고 로봇 연결은 유지한다. 표준 라이브러리만 쓴다(Qt/qasync 없음).

모듈: protocol(프레임·서버가 만드는 실패 data) · server(연결·프레이밍) · forward(내부 API 클라이언트·명령별 응답 한도) ·
cache(마지막 응답·꺼짐 응답) · log(server_page_YYYYMMDD.log·remote_cmd_YYYYMMDD.csv) · fake_api(시험용 가짜 내부 API) ·
app(조립·실행). 파라미터: v2/ch12/params/robot.py.
"""
