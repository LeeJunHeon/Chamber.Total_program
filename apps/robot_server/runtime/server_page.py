# robot_server/runtime/server_page.py
# -*- coding: utf-8 -*-

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Optional

from PySide6.QtCore import Qt, Signal, QTimer
from PySide6.QtGui import QTextCursor
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QGroupBox,
    QFormLayout,
    QLabel,
    QPushButton,
    QPlainTextEdit,
    QCheckBox,
    QLineEdit,
    QListWidget,
    QSizePolicy,
)


class ServerPage(QWidget):
    """
    Robot Server(통신) 전용 페이지 (UI는 메인 ServerPage와 동일)
    - Host 서버 상태(호스트/포트/Running)
    - 연결 클라이언트 목록(로그 기반)
    - 통신 로그(REQ/RES, NET 등) + 필터(특히 GET_SPUTTER_STATUS 숨김)
    """
    sigHostStart = Signal()
    sigHostStop = Signal()
    sigHostRestart = Signal()

    _PEER_RE = re.compile(r"Client (?:connected|disconnected|closed):\s*(\(.+?\))")
    _CMD_STATUS_RE = re.compile(r"cmd=GET_SPUTTER_STATUS(?:_RESULT)?\b", re.IGNORECASE)

    _DT_PREFIX_RE = re.compile(
        r"^(?P<ts>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+(?P<rest>.*)$"
    )

    def __init__(self, *, log_root: Optional[Path] = None) -> None:
        super().__init__()
        self._log_root = Path(log_root) if log_root else None

        self._clients: set[str] = set()
        self._max_lines = 5000

        # ✅ 서버 프로그램용: 자동 저장 파일명을 메인과 분리
        self._daily_buf: list[str] = []
        self._daily_ext = ".log"
        self._daily_prefix = "robot_server_page"  # ← 핵심: 메인과 파일명 분리

        self._daily_flush_timer = QTimer(self)
        self._daily_flush_timer.setInterval(1000)
        self._daily_flush_timer.timeout.connect(self._flush_daily_log)
        self._daily_flush_timer.start()

        self._build_ui()
        self._wire_ui()

    def _build_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(10)

        # 헤더 + 네비(메인과 동일하게 유지)
        header_row = QHBoxLayout()
        header_row.setSpacing(10)

        self.lblTitle = QLabel("Robot Server / Communication")
        f = self.lblTitle.font()
        f.setPointSize(max(12, f.pointSize() + 6))
        f.setBold(True)
        self.lblTitle.setFont(f)
        header_row.addWidget(self.lblTitle, 1)

        root.addLayout(header_row)

        # Host 서버 상태/제어
        grpServer = QGroupBox("Host Server")
        form = QFormLayout(grpServer)
        form.setLabelAlignment(Qt.AlignRight)

        self.edHost = QLineEdit()
        self.edHost.setReadOnly(True)
        self.edPort = QLineEdit()
        self.edPort.setReadOnly(True)
        self.lblRunning = QLabel("UNKNOWN")

        self.btnHostStart = QPushButton("Start")
        self.btnHostStop = QPushButton("Stop")
        self.btnHostRestart = QPushButton("Restart")

        self.btnHostStart.clicked.connect(self.sigHostStart.emit)
        self.btnHostStop.clicked.connect(self.sigHostStop.emit)
        self.btnHostRestart.clicked.connect(self.sigHostRestart.emit)

        btn_row = QHBoxLayout()
        btn_row.addWidget(self.btnHostStart)
        btn_row.addWidget(self.btnHostStop)
        btn_row.addWidget(self.btnHostRestart)
        btn_row.addStretch(1)

        form.addRow("Bind Host", self.edHost)
        form.addRow("Bind Port", self.edPort)
        form.addRow("Running", self.lblRunning)
        form.addRow("", btn_row)

        root.addWidget(grpServer)

        # 클라이언트 목록
        grpClients = QGroupBox("Connected Clients")
        v_clients = QVBoxLayout(grpClients)
        self.lstClients = QListWidget()

        self.lstClients.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.lstClients.setFixedHeight(140)

        self.lblClientsHint = QLabel("※ 'Client connected/disconnected' 로그를 기반으로 표시합니다.")
        self.lblClientsHint.setStyleSheet("color: gray;")
        v_clients.addWidget(self.lstClients)
        v_clients.addWidget(self.lblClientsHint)

        root.addWidget(grpClients, 0)

        # 로그 + 필터
        grpLog = QGroupBox("Communication Log")
        v_log = QVBoxLayout(grpLog)

        filter_row = QHBoxLayout()
        self.chkAutoScroll = QCheckBox("Auto-scroll")
        self.chkAutoScroll.setChecked(True)
        self.chkPause = QCheckBox("Pause")
        self.chkHideStatus = QCheckBox("Hide GET_SPUTTER_STATUS")
        self.chkHideStatus.setChecked(True)

        self.btnClear = QPushButton("Clear")
        self.btnSave = QPushButton("Save to file")

        filter_row.addWidget(self.chkAutoScroll)
        filter_row.addWidget(self.chkPause)
        filter_row.addWidget(self.chkHideStatus)
        filter_row.addStretch(1)
        filter_row.addWidget(self.btnClear)
        filter_row.addWidget(self.btnSave)

        self.logEdit = QPlainTextEdit()
        self.logEdit.setReadOnly(True)
        self.lblSaved = QLabel("")
        self.lblSaved.setStyleSheet("color: gray;")

        v_log.addLayout(filter_row)
        v_log.addWidget(self.logEdit, 3)
        v_log.addWidget(self.lblSaved)

        root.addWidget(grpLog, 1)

    def _wire_ui(self) -> None:
        self.btnClear.clicked.connect(self.logEdit.clear)
        self.btnSave.clicked.connect(self._save_log_to_file)

    def set_host_info(self, host: str, port: int) -> None:
        self.edHost.setText(str(host))
        self.edPort.setText(str(port))

    def set_running(self, running: bool) -> None:
        self.lblRunning.setText("RUNNING" if running else "STOPPED")

    def _format_with_ts(self, tag: str, msg: str) -> str:
        msg = str(msg)
        m = self._DT_PREFIX_RE.match(msg)
        if m:
            ts = m.group("ts")
            rest = m.group("rest")
        else:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            rest = msg
        return f"{ts} [{tag}] {rest}"

    def append_log(self, tag: str, text: str) -> None:
        msg = str(text)

        # 1) 파일 저장은 전부 저장 (Pause/HideStatus 무관)
        line = self._format_with_ts(tag, msg)
        try:
            self._queue_daily_line(line)
        except Exception:
            pass

        # 2) Pause여도 클라이언트 목록 갱신
        if tag == "NET":
            self._update_clients_from_net_log(msg)

        # 3) 화면 표시만 제어
        if self.chkPause.isChecked():
            return
        if self.chkHideStatus.isChecked() and self._CMD_STATUS_RE.search(msg):
            return

        self._append_line(line)

    def _append_line(self, line: str) -> None:
        self.logEdit.appendPlainText(line)

        try:
            doc = self.logEdit.document()
            if doc.blockCount() > self._max_lines:
                lines = self.logEdit.toPlainText().splitlines()[-4000:]
                self.logEdit.setPlainText("\n".join(lines))
        except Exception:
            pass

        if self.chkAutoScroll.isChecked():
            try:
                c = self.logEdit.textCursor()
                c.movePosition(QTextCursor.End)
                self.logEdit.setTextCursor(c)
            except Exception:
                pass

    def _update_clients_from_net_log(self, msg: str) -> None:
        m = self._PEER_RE.search(msg)
        if not m:
            return
        peer = m.group(1)

        if "connected" in msg:
            self._clients.add(peer)
        else:
            self._clients.discard(peer)

        self.lstClients.clear()
        for p in sorted(self._clients):
            self.lstClients.addItem(p)

    def _daily_log_dir(self) -> Path:
        """
        NAS 우선: <log_root>/CH1&2_Server
        실패 시 로컬: ./Logs/CH1&2_Server
        """
        if self._log_root:
            try:
                d = self._log_root / "CH1&2_Server"
                d.mkdir(parents=True, exist_ok=True)
                return d
            except Exception:
                pass

        d = Path.cwd() / "Logs" / "CH1&2_Server"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _daily_log_path(self) -> Path:
        # 하루 1파일 + 메인과 파일명 분리
        return self._daily_log_dir() / f"{self._daily_prefix}_{datetime.now():%Y%m%d}{self._daily_ext}"

    def _queue_daily_line(self, line: str) -> None:
        self._daily_buf.append(line)
        if len(self._daily_buf) >= 2000:
            self._flush_daily_log()

    def _flush_daily_log(self) -> None:
        if not self._daily_buf:
            return

        lines = self._daily_buf
        self._daily_buf = []

        try:
            fp = self._daily_log_path()
            fp.parent.mkdir(parents=True, exist_ok=True)
            with open(fp, "a", encoding="utf-8") as f:
                f.write("\n".join(lines) + "\n")
        except Exception as e:
            try:
                self.lblSaved.setText(f"Auto-save failed: {e!r}")
            except Exception:
                pass

    def _save_log_to_file(self) -> None:
        try:
            base = (self._log_root / "Server") if self._log_root else (Path.cwd() / "ServerLogs")
            base.mkdir(parents=True, exist_ok=True)

            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            fp = base / f"robot_server_comm_{ts}.log"
            fp.write_text(self.logEdit.toPlainText(), encoding="utf-8")
            self.lblSaved.setText(f"Saved: {fp}")
        except Exception as e:
            self.lblSaved.setText(f"Save failed: {e!r}")

    def closeEvent(self, event) -> None:  # noqa: N802
        try:
            self._flush_daily_log()
        except Exception:
            pass
        try:
            super().closeEvent(event)
        except Exception:
            pass
