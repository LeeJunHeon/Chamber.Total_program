# apps/process_app/main_process.py
from __future__ import annotations

import threading
import time

from common.reporter import RobotReporter
from devices.rga_http_client import RGAHttpClient

API_TOKEN = "CHANGE_ME"
ROBOT_STATE_URL = "http://127.0.0.1:9010"
RGA_SERVICE_URL = "http://127.0.0.1:9020"
HEARTBEAT_INTERVAL_S = 2.0


def start_heartbeat_loop(reporter: RobotReporter, ch_id: int, get_run_id_callable):
    """
    RUNNING일 때만 heartbeat를 보내고 싶으면,
    get_run_id_callable()이 None이면 heartbeat skip하도록 구현하면 된다.
    지금은 단순화를 위해 run_id가 있으면 보냄.
    """
    stop_evt = threading.Event()

    def loop():
        while not stop_evt.is_set():
            run_id = get_run_id_callable()
            if run_id:
                reporter.send_heartbeat({"ch_id": ch_id, "run_id": run_id})
            time.sleep(HEARTBEAT_INTERVAL_S)

    th = threading.Thread(target=loop, daemon=True)
    th.start()
    return stop_evt


def create_runtimes(reporter: RobotReporter, rga: RGAHttpClient):
    """
    ✅ 너의 실제 런타임 생성 코드로 교체해야 하는 부분.

    예)
      from runtime.chamber_runtime import ChamberRuntime
      ch1 = ChamberRuntime(ch=1, reporter=reporter, rga=rga)
      ch2 = ChamberRuntime(ch=2, reporter=reporter, rga=rga)
      plasma = PlasmaCleaningRuntime(..., reporter=reporter, rga=rga)
      return ch1, ch2, plasma
    """
    ch1 = {"ch": 1, "run_id": None}
    ch2 = {"ch": 2, "run_id": None}
    plasma = {"ch": "plasma", "run_id": None}
    return ch1, ch2, plasma


def run_ui(ch1, ch2, plasma):
    """
    ✅ 기존 main.py에서 UI 실행하던 코드를 여기로 옮기면 됨.
    지금은 샘플로 대기만 함.
    """
    print("[PROCESS_APP] UI placeholder running... (replace with your real UI loop)")
    while True:
        time.sleep(1)


def main():
    reporter = RobotReporter(base_url=ROBOT_STATE_URL, token=API_TOKEN)
    rga = RGAHttpClient(base_url=RGA_SERVICE_URL)

    ch1, ch2, plasma = create_runtimes(reporter, rga)

    # (선택) heartbeat 스레드 시작 - 실제론 "RUNNING일 때만" 보내도록 개선 권장
    hb1 = start_heartbeat_loop(reporter, 1, lambda: getattr(ch1, "run_id", None) if hasattr(ch1, "run_id") else ch1.get("run_id"))
    hb2 = start_heartbeat_loop(reporter, 2, lambda: getattr(ch2, "run_id", None) if hasattr(ch2, "run_id") else ch2.get("run_id"))

    try:
        run_ui(ch1, ch2, plasma)
    finally:
        hb1.set()
        hb2.set()
        reporter.close()


if __name__ == "__main__":
    main()
