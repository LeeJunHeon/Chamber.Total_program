# common/reporter.py
from __future__ import annotations

import json
import queue
import threading
import time
import uuid
from typing import Any

try:
    import requests  # 있으면 사용
except Exception:
    requests = None

import urllib.request


class RobotReporter:
    def __init__(self, base_url: str, token: str, timeout_s: float = 2.0, max_retry: int = 2, logger=None):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout_s = timeout_s
        self.max_retry = max_retry
        self.logger = logger

        self._q: "queue.Queue[tuple[str, dict[str, Any]]]" = queue.Queue()
        self._stop = threading.Event()
        self._th = threading.Thread(target=self._worker, daemon=True)
        self._th.start()

    def new_run_id(self) -> str:
        return str(uuid.uuid4())

    def close(self) -> None:
        self._stop.set()
        try:
            self._q.put_nowait(("", {}))
        except Exception:
            pass

    def send_state(self, payload: dict) -> None:
        self._q.put(("/api/state", payload))

    def send_heartbeat(self, payload: dict) -> None:
        self._q.put(("/api/heartbeat", payload))

    # ---------------- internal ----------------
    def _log(self, level: str, msg: str) -> None:
        if self.logger:
            fn = getattr(self.logger, level, None)
            if fn:
                try:
                    fn(msg)
                    return
                except Exception:
                    pass
        print(f"[{level.upper()}] {msg}")

    def _post(self, path: str, payload: dict) -> bool:
        url = self.base_url + path
        headers = {"X-Api-Token": self.token, "Content-Type": "application/json"}
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")

        for attempt in range(self.max_retry + 1):
            try:
                if requests:
                    r = requests.post(url, headers=headers, data=data, timeout=self.timeout_s)
                    if 200 <= r.status_code < 300:
                        return True
                    self._log("warning", f"report failed status={r.status_code} body={r.text[:200]}")
                else:
                    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
                    with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
                        if 200 <= resp.status < 300:
                            return True
                        self._log("warning", f"report failed status={resp.status}")
            except Exception as e:
                if attempt < self.max_retry:
                    time.sleep(0.2 * (attempt + 1))
                    continue
                self._log("warning", f"report exception: {e}")
                return False
        return False

    def _worker(self) -> None:
        while not self._stop.is_set():
            path, payload = self._q.get()
            if not path:
                continue
            # 실패해도 공정 죽이면 안 됨 → warn만
            self._post(path, payload)
