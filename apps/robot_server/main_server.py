# apps/robot_server/main_server.py
from __future__ import annotations

import threading
import time

from robot_server.state_api import run_state_api

API_TOKEN = "CHANGE_ME"
STATE_HOST = "127.0.0.1"
STATE_PORT = 9010


def run_robot_server_blocking():
    """
    ✅ 여기를 너의 '기존 로봇 서버 시작 코드'로 교체하면 됨.

    예)
      from server.robot_server import RobotServer
      RobotServer(...).serve_forever()

    지금은 샘플로 무한 대기만 함.
    """
    print("[ROBOT_SERVER] placeholder running... (replace with your real robot server)")
    while True:
        time.sleep(1)


def main():
    # 1) 상태 API는 별도 스레드로 띄우고,
    th = threading.Thread(
        target=run_state_api,
        kwargs={"host": STATE_HOST, "port": STATE_PORT, "api_token": API_TOKEN},
        daemon=True
    )
    th.start()

    # 2) 로봇 서버는 메인에서 블로킹 실행(보통 serve_forever 형태)
    run_robot_server_blocking()


if __name__ == "__main__":
    main()
