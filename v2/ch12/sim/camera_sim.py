"""카메라 시뮬레이터 — drivers.camera.CameraBackend 와 같은 메서드(cv2 없이). sim 실행·시험 하니스가 쓴다.

프레임은 번호 문자열, JPG 는 SOI/EOI 사이에 그 번호(실제 그림 아님 — 파일 경로·장 수·디스크 가드 동작만 본다).
고장 주입: open_ok=False(열기 실패 — v1 '이번 공정 미기록'), fail_reads=n(처음 n 번 읽기 실패).
"""
from __future__ import annotations

from typing import Any, Optional, Tuple


class SimCamera:
    def __init__(self, open_ok: bool = True, fail_reads: int = 0) -> None:
        self.open_ok, self.fail_reads = open_ok, fail_reads
        self.opened = 0
        self.reads = 0
        self.released = 0

    def open(self, index: int) -> Optional[Any]:
        self.opened += 1
        return {"index": index} if self.open_ok else None

    def read(self, cap) -> Tuple[bool, Any]:
        self.reads += 1
        if self.reads <= self.fail_reads:
            return False, None
        return True, f"frame{self.reads}"

    def rotate(self, frame):
        return frame

    def encode_jpg(self, frame) -> Optional[bytes]:
        return b"\xff\xd8" + str(frame).encode("ascii") + b"\xff\xd9"

    def release(self, cap) -> None:
        self.released += 1
