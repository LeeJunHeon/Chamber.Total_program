# -*- coding: utf-8 -*-
from __future__ import annotations

from PySide6.QtCore import QTimer
from PySide6.QtWidgets import QMessageBox


def attach_autoclose(box: QMessageBox, timeout_ms: int = 5000) -> None:
    """QMessageBox를 timeout_ms(ms) 후 자동으로 닫는다."""
    try:
        QTimer.singleShot(int(timeout_ms), box.close)
    except Exception:
        pass
