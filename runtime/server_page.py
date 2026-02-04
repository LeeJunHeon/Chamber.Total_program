# runtime/server_page.py
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

    # ✅ handlers._plc_file_logger()가 붙이는 컨텍스트 태그
    # 예: "[GET_SPUTTER_STATUS] read L_ATM ..."
    _STATUS_CTX_RE = re.compile(r"\[GET_SPUTTER_STATUS\]", re.IGNORECASE)

    # "2026-01-15 15:12:38 ..." 형태면 ts/rest로 분리해서 ts를 앞으로 통일
    _DT_PREFIX_RE = re.compile(
        r"^(?P<ts>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+(?P<rest>.*)$"
    )

    def __init__(self, *, log_root: Optional[Path] = None) -> None:
        super().__init__()
        self._log_root = Path(log_root) if log_root else None

        self._clients: set[str] = set()
        self._max_lines = 5000

        # ✅ ServerPage 로그를 하루 1파일(.log)로 자동 저장
        # - ServerPage로 들어오는 로그는 "전부" 저장(Pause/HideStatus 여부 무관)
        # - NAS(log_root) 실패 시 로컬 ./Logs/ServerPage 로 폴백
        self._daily_buf: list[str] = []
        self._daily_ext = ".log"   # 요구사항: .log 고정

        self._daily_flush_timer = QTimer(self)
        self._daily_flush_timer.setInterval(1000)  # 1초마다 파일로 flush
        self._daily_flush_timer.timeout.connect(self._flush_daily_log)
        self._daily_flush_timer.start()

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

        # ✅ 클라이언트 목록은 너무 커지지 않게 고정(원하는 높이로 조절)
        self.lstClients.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.lstClients.setFixedHeight(140)  # ← 여기 숫자만 취향대로(120~200 추천)

        self.lblClientsHint = QLabel("※ 'Client connected/disconnected' 로그를 기반으로 표시합니다.")
        self.lblClientsHint.setStyleSheet("color: gray;")
        v_clients.addWidget(self.lstClients)
        v_clients.addWidget(self.lblClientsHint)

        # ✅ stretch를 0으로(또는 아예 생략) → Connected Clients는 최소/고정 크기 유지
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

        # ✅ 로그가 남는 공간을 더 크게 가져가게 stretch 증가
        root.addWidget(grpLog, 1)   # (위에서 grpClients를 0으로 바꿨으면 1만 줘도 충분)

    def _wire_ui(self) -> None:
        self.btnClear.clicked.connect(self.logEdit.clear)
        self.btnSave.clicked.connect(self._save_log_to_file)

    # MainWindow에서 호출
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

        # ✅ 1) "전부 저장" (Pause/HideStatus 무관)
        line = self._format_with_ts(tag, msg)
        try:
            self._queue_daily_line(line)
        except Exception:
            pass

        # ✅ 2) Pause여도 클라이언트 목록은 계속 갱신(연결상태 반영)
        if tag == "NET":
            self._update_clients_from_net_log(msg)

        # ✅ 3) 아래부터는 "화면 표시"만 제어
        if self.chkPause.isChecked():
            return

        # 화면에서만 숨김(파일에는 이미 저장됨)
        if self.chkHideStatus.isChecked():
            # 1) REQ/RES 라인(예: cmd=GET_SPUTTER_STATUS)은 기존처럼 숨김
            if self._CMD_STATUS_RE.search(msg):
                return

            # 2) GET_SPUTTER_STATUS 처리 중 발생한 PLC_REMOTE(read L_ATM 등)도 숨김
            #    (handlers.py에서 "[GET_SPUTTER_STATUS]" 태그를 붙여준다는 전제)
            if tag == "PLC_REMOTE" and self._STATUS_CTX_RE.search(msg):
                return

        self._append_line(line)

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
                
    def _daily_log_dir(self) -> Path:
        """
        NAS 우선: <log_root>/CH1&2_Server
        실패 시 로컬: ./Logs/CH1&2_Server
        """
        # 1) NAS(log_root) 우선
        if self._log_root:
            try:
                d = self._log_root / "CH1&2_Server"
                d.mkdir(parents=True, exist_ok=True)
                return d
            except Exception:
                pass

        # 2) 로컬 폴백
        d = Path.cwd() / "Logs" / "CH1&2_Server"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _daily_log_path(self) -> Path:
        # 하루 1파일
        return self._daily_log_dir() / f"server_page_{datetime.now():%Y%m%d}{self._daily_ext}"

    def _queue_daily_line(self, line: str) -> None:
        self._daily_buf.append(line)

        # 메모리 보호: 너무 많이 쌓이면 즉시 flush
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
            # 자동 저장 실패는 UI/통신을 방해하면 안 됨 → 표시만 하고 무시
            try:
                self.lblSaved.setText(f"Auto-save failed: {e!r}")
            except Exception:
                pass

    def _save_log_to_file(self) -> None:
        try:
            base = (self._log_root / "CH1&2_Server") if self._log_root else (Path.cwd() / "Logs" / "CH1&2_Server")
            base.mkdir(parents=True, exist_ok=True)

            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            fp = base / f"server_comm_{ts}.log"
            fp.write_text(self.logEdit.toPlainText(), encoding="utf-8")
            self.lblSaved.setText(f"Saved: {fp}")
        except Exception as e:
            self.lblSaved.setText(f"Save failed: {e!r}")

    def closeEvent(self, event) -> None:  # noqa: N802 (Qt naming)
        try:
            self._flush_daily_log()
        except Exception:
            pass
        try:
            super().closeEvent(event)
        except Exception:
            pass

