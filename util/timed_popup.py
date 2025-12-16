# util/timed_popup.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from PySide6.QtCore import QTimer
from PySide6.QtWidgets import QMessageBox


def attach_autoclose(box: QMessageBox, ms: int = 5000) -> None:
    """
    QMessageBox를 ms 후 자동으로 닫는다.
    - ms <= 0 이면 자동닫힘 비활성
    - box를 open() (비모달)로 띄워도 정상 닫힘
    """
    try:
        ms = int(ms)
    except Exception:
        ms = 5000

    if ms <= 0:
        return

    # 타이머가 GC로 사라지지 않도록 부모를 box로 둠
    timer = QTimer(box)
    timer.setSingleShot(True)

    def _close():
        try:
            box.close()
        except Exception:
            # 혹시 close가 실패하면 done으로 마무리
            try:
                box.done(0)
            except Exception:
                pass

    timer.timeout.connect(_close)

    # box가 먼저 닫히면 타이머도 정리
    def _cleanup(*_):
        try:
            timer.stop()
        except Exception:
            pass
        try:
            timer.deleteLater()
        except Exception:
            pass

    try:
        box.finished.connect(_cleanup)
    except Exception:
        # finished가 없으면 부모(box) 삭제 시 timer도 함께 정리됨
        pass

    timer.start(ms)
