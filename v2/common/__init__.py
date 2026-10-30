"""장비 공통 부분 — 챔버K 와 챔버1·2 가 함께 쓴다. 장비 이름·주소·공정 규칙은 여기 두지 않는다.

출처: Chamber.K_program rewrite/v2 커밋 de8e946b14fa5fe4bff61e671df2b72ac637f072 의 v2/common 을 복사해 시작(2026-10-30).
챔버12 에서 더한 것: Modbus TCP(modbus_tcp.py — PLC 직결), MOXA NPort TCP 시리얼 전송(transport.TcpTransport),
챔버 여러 개 동시 운전에 필요한 일반화. 웹 계약(messages.py)은 챔버K 와 호환을 유지한다.
"""
