# runtime/server_page.py
# -*- coding: utf-8 -*-

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Optional

from PySide6.QtCore import Qt, Signal
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
    Server(통신) 전용 페이지
    - Host 서버 상태(호스트/포트/Running)
    - 연결 클라이언트 목록(로그 기반)
    - 통신 로그(REQ/RES, NET 등) + 필터(특히 GET_SPUTTER_STATUS 숨김)
    """
    sigHostStart = Signal()
    sigHostStop = Signal()
    sigHostRestart = Signal()

    _PEER_RE = re.compile(r"Client (?:connected|disconnected|closed):\s*(\(.+?\))")
    _CMD_STATUS_RE = re.compile(r"cmd=GET_SPUTTER_STATUS(?:_RESULT)?\b", re.IGNORECASE)

    def __init__(self, *, log_root: Optional[Path] = None) -> None:
        super().__init__()
        self._log_root = Path(log_root) if log_root else None

        self._clients: set[str] = set()
        self._max_lines = 5000

        self._build_ui()
        self._wire_ui()

    def _build_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(10)

        # 헤더 + 네비
        header_row = QHBoxLayout()
        header_row.setSpacing(10)

        self.lblTitle = QLabel("Server / Communication")
        f = self.lblTitle.font()
        f.setPointSize(max(12, f.pointSize() + 6))
        f.setBold(True)
        self.lblTitle.setFont(f)
        header_row.addWidget(self.lblTitle, 1)

        self.btnGoPC = QPushButton("Plasma\nCleaning")
        self.btnGoCh1 = QPushButton("CH.1")
        self.btnGoCh2 = QPushButton("CH.2")
        for b in (self.btnGoPC, self.btnGoCh1, self.btnGoCh2):
            b.setFixedWidth(110)
            b.setFixedHeight(48)
        header_row.addWidget(self.btnGoPC)
        header_row.addWidget(self.btnGoCh1)
        header_row.addWidget(self.btnGoCh2)

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
        
        # ✅ 버튼 동작 연결
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
        self.lstClients.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.MinimumExpanding)
        self.lblClientsHint = QLabel("※ 'Client connected/disconnected' 로그를 기반으로 표시합니다.")
        self.lblClientsHint.setStyleSheet("color: gray;")
        v_clients.addWidget(self.lstClients)
        v_clients.addWidget(self.lblClientsHint)
        root.addWidget(grpClients, 1)

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

        root.addWidget(grpLog, 3)

    def _wire_ui(self) -> None:
        self.btnClear.clicked.connect(self.logEdit.clear)
        self.btnSave.clicked.connect(self._save_log_to_file)

    # MainWindow에서 호출
    def set_host_info(self, host: str, port: int) -> None:
        self.edHost.setText(str(host))
        self.edPort.setText(str(port))

    def set_running(self, running: bool) -> None:
        self.lblRunning.setText("RUNNING" if running else "STOPPED")

    def append_log(self, tag: str, text: str) -> None:
        msg = str(text)

        # ✅ Pause여도 클라이언트 목록은 갱신(연결상태는 계속 반영)
        if tag == "NET":
            self._update_clients_from_net_log(msg)

        if self.chkPause.isChecked():
            return

        # GET_SPUTTER_STATUS 폴링 로그 숨김
        if self.chkHideStatus.isChecked() and self._CMD_STATUS_RE.search(msg):
            return

        self._append_line(f"[{tag}] {msg}")

    # 내부 유틸
    def _append_line(self, line: str) -> None:
        self.logEdit.appendPlainText(line)

        # 너무 커지면 최근 일부만 유지(렉 방지)
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

    def _save_log_to_file(self) -> None:
        try:
            base = (self._log_root / "Server") if self._log_root else (Path.cwd() / "ServerLogs")
            base.mkdir(parents=True, exist_ok=True)

            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            fp = base / f"server_comm_{ts}.log"
            fp.write_text(self.logEdit.toPlainText(), encoding="utf-8")
            self.lblSaved.setText(f"Saved: {fp}")
        except Exception as e:
            self.lblSaved.setText(f"Save failed: {e!r}")
