"""Chamber 제어 프로그램 v2 — CH1&2(챔버12) 스퍼터.

v2\\common       : 장비 공통(챔버K v2 에서 복사해 시작 — 출처는 common/__init__.py)
v2\\ch12         : 챔버12 장비 쪽 전부(드라이버·조립·공정·안전·명령·파라미터·기록·웹·저널·spec)
v2\\robot_server : 로봇 서버(별도 프로그램, 0.0.0.0:50070)
v2\\supervisor   : 감시·재시작 프로그램(별도, 표준 라이브러리만)
v2\\hmi          : 포털에서 빌드해 넣는 노트북 화면(직접 편집 금지)
v2\\tests        : python -m pytest v2\\tests
v2\\tools        : bench·변환·빌드 도구

v1 모듈(device·controller·runtime·host·lib·util·ui …)을 import 하지 않는다 — v2\\tests\\test_isolation.py 가 지킨다.
"""
