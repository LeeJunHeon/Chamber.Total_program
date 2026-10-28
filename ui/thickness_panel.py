# ui/thickness_panel.py
# -*- coding: utf-8 -*-
"""두께 모니터(STM-100) 상태 패널 — 코드로 만든 QFrame(HeaterPanel 과 같은 방식). 히터 패널 밑에 둔다.

리스너로 받은 상태 dict(st)로만 화면을 그린다. 스타일시트는 값이 바뀔 때만 다시 넣는다.
버튼: [영점](공정 중·미연결·응답 없음이면 비활성 — 회색) / [Factor 설정](항상 활성).
진행 영역: 공정이 없으면 '공정 없음' 한 줄(막대는 비움), 공정 중에는 종료 방식·목표·진행률·상태 문구(런타임이 set_process_view),
공정이 끝나면 다음 공정 전까지 마지막 결과를 그대로 둔다.
아래 빨간 줄은 연결된 채 응답이 없을 때만(응답 없음 n초 + 마지막 오류) — 연결 상태는 위 램프로만 보여 준다.
"""
from __future__ import annotations

import asyncio
import contextlib
from typing import Any, Optional

from PySide6.QtCore import QRect, Qt
from PySide6.QtWidgets import QFrame, QLabel, QMessageBox, QProgressBar, QPushButton, QWidget

PANEL_W = 240
PANEL_H = 344
IDLE_TEXT = "공정 없음"
_BTN_CSS = ("QPushButton {background: #ebebe9; color: black; border-radius: 4px; border: 1px solid #cccccc;}"
            "QPushButton:disabled {background: #f3f4f6; color: #b0b7be; border: 1px solid #e0e3e7;}")

_OK_BG, _OK_FG = "#e8f5e9", "#2e7d32"
_OFF_BG, _OFF_FG = "#eef1f4", "#9aa3ab"
_ERR_BG, _ERR_FG = "#ffebee", "#c62828"

_LAMPS = [
    ("conn", "연결", "MOXA TCP 연결"),
    ("xtal", "XTAL", "크리스탈 상태(M) — 빨강 = XTAL FAIL"),
    ("resp", "응답", "최근 THICKNESS_STALE_S 안에 장비 정상 응답(통신) — 값 유무와 무관"),
]


def _fmt(v, fmt: str, none: str = "---") -> str:
    if v is None:
        return none
    try:
        return fmt.format(float(v))
    except Exception:
        return none


class ThicknessPanel(QFrame):
    def __init__(self, service: Any, cfg: Any, parent: Optional[QWidget] = None, *, title: str = ""):
        super().__init__(parent)
        self._svc = service
        self._cfg = cfg
        self._st: dict = {}
        self._busy = False
        self._styles: dict = {}
        self._tasks: set = set()
        self._msg_boxes: list = []
        self._dialog = None
        ch = int(getattr(cfg, "ch", 2) or 2)

        self.setObjectName("thickness_panel")
        self.resize(PANEL_W, PANEL_H)
        self.setFrameShape(QFrame.Shape.StyledPanel)
        self.setStyleSheet("QFrame#thickness_panel {background: #ffffff; border: 2px solid #cccccc; border-radius: 8px;}")

        self.title = QLabel(title or f"CH{ch} Thickness Monitor", self)
        self.title.setGeometry(QRect(10, 6, 172, 20))
        self.title.setStyleSheet("QLabel {border: none; color: #333333; font-weight: bold;}")
        self.reset_label = QLabel("", self)
        self.reset_label.setGeometry(QRect(176, 6, 54, 20))
        self.reset_label.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        self.reset_label.setToolTip("장비 전원 리셋 플래그(B 응답)를 본 횟수 — 프로그램 시작 뒤 누적")

        # ── 램프 3개 ──
        self.lamps = {}
        for i, (key, text, tip) in enumerate(_LAMPS):
            w = QLabel(text, self)
            w.setGeometry(QRect(10 + i * 74, 30, 70, 20))
            w.setAlignment(Qt.AlignmentFlag.AlignCenter)
            w.setToolTip(tip)
            self.lamps[key] = w

        # ── LCD ──
        self.lcd = QFrame(self)
        self.lcd.setObjectName("thickness_lcd")
        self.lcd.setGeometry(QRect(10, 56, 220, 116))
        self.lcd.setStyleSheet("QFrame#thickness_lcd {background: #f7f8fa; border: 1px solid #dfe3e8; border-radius: 6px;}")
        _small = "QLabel {border: none; background: transparent; color: #6b7280; font-size: 8pt;}"
        lab = QLabel("두께", self.lcd); lab.setGeometry(QRect(8, 8, 30, 14)); lab.setStyleSheet(_small)
        self.xtal_tag = QLabel("XTAL FAIL", self.lcd)      # 통신 정상 + 크리스탈 FAIL 일 때만(두께·증착률 값 대신 빈칸 응답)
        self.xtal_tag.setGeometry(QRect(8, 24, 62, 15))
        self.xtal_tag.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.xtal_tag.setToolTip("크리스탈 이상(M='!') — 장비가 두께·증착률 대신 빈칸을 보냄. 크리스탈을 확인하십시오")
        self.xtal_tag.setStyleSheet(f"QLabel {{border: 1px solid {_ERR_FG}; background: {_ERR_BG}; color: {_ERR_FG};"
                                    " font-size: 7.5pt; font-weight: bold; border-radius: 3px;}")
        self.xtal_tag.hide()
        self.thk_label = QLabel("---", self.lcd)
        self.thk_label.setGeometry(QRect(36, 2, 146, 34))
        self.thk_label.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        unit = QLabel("nm", self.lcd); unit.setGeometry(QRect(186, 12, 26, 18)); unit.setStyleSheet(_small)
        lab = QLabel("증착률", self.lcd); lab.setGeometry(QRect(8, 42, 40, 14)); lab.setStyleSheet(_small)
        self.rate_label = QLabel("---", self.lcd)
        self.rate_label.setGeometry(QRect(48, 38, 164, 22))
        self.rate_label.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        sep = QFrame(self.lcd); sep.setGeometry(QRect(8, 64, 204, 1))
        sep.setStyleSheet("background: #e5e7eb; border: none;")
        self.freq_label = QLabel("주파수 --- Hz", self.lcd)
        self.freq_label.setGeometry(QRect(8, 70, 204, 18))
        self.life_label = QLabel("수명 --- %", self.lcd)
        self.life_label.setGeometry(QRect(8, 90, 204, 18))

        # ── 적용값(장비에서 되읽은 값) — 폭이 모자라면 두 줄 ──
        self.material_label = QLabel("Density --- · Z --- · Tooling --- %", self)
        self.material_label.setGeometry(QRect(10, 176, 220, 30))
        self.material_label.setWordWrap(True)
        self.material_label.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        self.material_label.setToolTip("현재 장비에 적용된 density(g/cm3) · Z-factor · tooling(%) — 장비에서 되읽은 값")
        self.material_label.setStyleSheet("QLabel {border: none; color: #546e7a; font-size: 8pt;}")

        # ── 진행 영역(공정 연동) ──
        self.proc_box = QFrame(self)
        self.proc_box.setObjectName("thickness_proc")
        self.proc_box.setGeometry(QRect(10, 210, 220, 66))
        self.proc_box.setStyleSheet("QFrame#thickness_proc {background: #fafafa; border: 1px dashed #d5d9de; border-radius: 6px;}")
        _proc = "QLabel {border: none; background: transparent; color: #6b7280; font-size: 8pt;}"
        self.mode_label = QLabel("", self.proc_box)
        self.mode_label.setGeometry(QRect(8, 4, 100, 16)); self.mode_label.setStyleSheet(_proc)
        self.target_label = QLabel("", self.proc_box)
        self.target_label.setGeometry(QRect(108, 4, 104, 16)); self.target_label.setStyleSheet(_proc)
        self.target_label.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        self.progress = QProgressBar(self.proc_box)
        self.progress.setGeometry(QRect(8, 24, 204, 16))
        self.progress.setRange(0, 100)
        self.progress.setValue(0)
        self.progress.setTextVisible(False)
        self.progress.setFormat("")
        self.progress.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.progress.setStyleSheet(
            "QProgressBar {background: #eceff1; border: 1px solid #dfe3e8; border-radius: 3px; color: #37474f; font-size: 8pt;}"
            "QProgressBar::chunk {background: #90a4ae; border-radius: 2px;}")
        self.proc_text = QLabel("", self.proc_box)
        self.proc_text.setGeometry(QRect(8, 44, 204, 16)); self.proc_text.setStyleSheet(_proc)

        # ── 오류 줄(연결된 채 응답 없음일 때만) ──
        self.error_label = QLabel("", self)
        self.error_label.setGeometry(QRect(10, 282, 220, 18))
        self.error_label.setStyleSheet("QLabel {border: none; color: #c62828; font-size: 8pt;}")

        # ── 버튼 ──
        self.zero_button = QPushButton("영점", self)
        self.zero_button.setGeometry(QRect(10, 306, 100, 26))
        self.zero_button.setToolTip("두께·타이머 영점(B). 공정 중에는 쓸 수 없음")
        self.zero_button.setStyleSheet(_BTN_CSS)
        self.factor_button = QPushButton("Factor 설정", self)
        self.factor_button.setGeometry(QRect(116, 306, 114, 26))
        self.factor_button.setStyleSheet(_BTN_CSS)
        self.zero_button.clicked.connect(self._on_zero_clicked)
        self.factor_button.clicked.connect(self._on_factor_clicked)

        self.set_idle()
        self.update_status({"stale": True, "connected": False})

    # ═══════════════ 표시 ═══════════════
    def _style(self, w: QWidget, css: str) -> None:
        if self._styles.get(id(w)) != css:
            self._styles[id(w)] = css
            w.setStyleSheet(css)

    def update_status(self, st: dict) -> None:
        """리스너 진입점 — 받은 st 로만 그린다."""
        try:
            self._st = dict(st or {})
            self._render()
        except Exception:
            pass

    def _render(self) -> None:
        st = self._st
        stale = bool(st.get("stale"))
        conn = bool(st.get("connected"))
        link = bool(st.get("link_ok")) if "link_ok" in st else (conn and not stale)   # 통신 정상(값 유무와 무관)
        xo = st.get("crystal_ok")
        xfail = link and xo is False

        val_col = "#9aa3ab" if stale else "#1f2937"
        self.thk_label.setText(_fmt(st.get("thickness_nm"), "{:.1f}"))
        self._style(self.thk_label, f"QLabel {{border: none; background: transparent; color: {val_col}; font-size: 20pt; font-weight: bold;}}")
        r_nm = st.get("rate_nm_s")
        self.rate_label.setText(f"{_fmt(r_nm, '{:.3f}')} nm/s")
        self._style(self.rate_label, f"QLabel {{border: none; background: transparent; color: {val_col}; font-size: 10pt; font-weight: bold;}}")
        sub_col = "#9aa3ab" if stale else "#546e7a"
        fq = st.get("freq_hz")
        self.freq_label.setText("주파수 " + ("---" if fq is None else f"{int(fq):,}") + " Hz")
        self.life_label.setText("수명 " + _fmt(st.get("life_pct"), "{:.1f}") + " %")
        for w in (self.freq_label, self.life_label):
            self._style(w, f"QLabel {{border: none; background: transparent; color: {sub_col}; font-size: 8.5pt;}}")

        self.xtal_tag.setVisible(xfail)

        # 램프 — 연결은 소켓 상태, 응답은 통신(link_ok), XTAL 은 통신이 안 되면 회색(마지막으로 읽은 값)
        def _lamp(key: str, state: Optional[bool], text: Optional[str] = None) -> None:
            w = self.lamps[key]
            if text is not None:
                w.setText(text)
            bg, fg = (_OFF_BG, _OFF_FG) if state is None else ((_OK_BG, _OK_FG) if state else (_ERR_BG, _ERR_FG))
            self._style(w, f"QLabel {{border: 1px solid {fg}; background: {bg}; color: {fg}; font-size: 8pt; border-radius: 3px;}}")

        _lamp("conn", conn, "연결" if conn else "끊김")
        if not link or xo is None:
            _lamp("xtal", None, "XTAL")
        else:
            _lamp("xtal", bool(xo), "XTAL" if xo else "XTAL FAIL")
        if not link:
            age = st.get("link_age_s", st.get("age_s"))
            _lamp("resp", None, "응답 없음")
            self.lamps["resp"].setToolTip("최근 장비 정상 응답 없음" + (f" · {float(age):.0f}초 전" if age is not None else ""))
        else:
            _lamp("resp", True, "응답")
            self.lamps["resp"].setToolTip(_LAMPS[2][2])

        rs = int(st.get("reset_seen") or 0)
        self.reset_label.setText(f"리셋 {rs}회" if rs else "")
        self._style(self.reset_label, "QLabel {border: none; color: #ef6c00; font-size: 8pt;}")

        vals = (f"Density {_fmt(st.get('density'), '{:.3g}')} · Z {_fmt(st.get('z_factor'), '{:.3f}')} · "
                f"Tooling {_fmt(st.get('tooling'), '{:g}')} %")
        name = str(st.get("material") or "")
        tag = self._material_tag(name)
        text = f"{tag} · {vals}" if tag else vals
        self.material_label.ensurePolished()            # 스타일시트 글꼴(8pt)로 폭을 잰다
        if self.material_label.fontMetrics().horizontalAdvance(text) > 216:
            # 한 줄에 안 들어가면 재료 / 값 사이에서 두 줄로(값 중간에서 끊기지 않게)
            text = f"{tag}\n{vals}" if tag else vals.replace(" · Tooling", "\nTooling")
        self.material_label.setText(text)
        self.material_label.setToolTip(f"{name + ' · ' if name else ''}{vals} — 장비에서 되읽은 density(g/cm3)·Z-factor·tooling(%)"
                                       + (", 재료는 마지막으로 적용한 재료" if name else ""))

        err = ""
        if conn and not link:                           # 연결 상태는 램프로 — 이 줄은 연결된 채 응답이 없을 때만
            age = st.get("link_age_s", st.get("age_s"))
            err = "응답 없음" + (f" {float(age):.0f}초" if age is not None else "")
            last = str(st.get("last_error") or "")
            if last:
                err += f" · {last}"
        self.error_label.setText(self.error_label.fontMetrics().elidedText(err, Qt.TextElideMode.ElideRight, 218))
        self.error_label.setToolTip(err)

        process = bool(st.get("process_running"))
        can_zero = conn and (not stale) and (not process) and not self._busy
        self.zero_button.setEnabled(can_zero)
        self.zero_button.setToolTip("공정 중에는 쓸 수 없음" if process else "두께·타이머 영점(B). 공정 중에는 쓸 수 없음")

    def _material_tag(self, name: str) -> str:
        """적용값 줄 앞에 붙일 재료 표시 — 재료 목록에 있으면 기호(예: Al2O3), 없으면 이름."""
        if not name:
            return ""
        try:
            m = self._svc.find_material(name)
            if m is not None and getattr(m, "symbol", ""):
                return str(m.symbol)
        except Exception:
            pass
        return name

    def set_idle(self) -> None:
        """공정 없음 — '공정 없음' 한 줄, 막대는 비우고 글자 없음."""
        self.mode_label.setText(IDLE_TEXT)
        self.target_label.setText("")
        self.progress.setValue(0)
        self.progress.setTextVisible(False)
        self.proc_text.setText("")
        self.proc_text.setToolTip("")

    def set_process_view(self, mode: str = "", target: str = "—", pct: Optional[float] = None,
                         text: str = "") -> None:
        """공정 중·종료 뒤(다음 공정 전까지) — 종료 방식 / 목표 / 진행률 / 상태 문구. mode 가 비면 공정 없음."""
        if not mode:
            self.set_idle()
            return
        self.mode_label.setText(f"종료 방식 {mode}")
        self.target_label.setText(f"목표 {target}" if target and target != "—" else "")
        if pct is None:
            self.progress.setValue(0)
            self.progress.setTextVisible(False)
        else:
            p = max(0.0, min(100.0, float(pct)))
            self.progress.setValue(int(round(p)))
            self.progress.setFormat(f"{p:.0f}%")
            self.progress.setTextVisible(True)
        self.proc_text.ensurePolished()
        self.proc_text.setText(self.proc_text.fontMetrics().elidedText(text, Qt.TextElideMode.ElideRight, 204))
        self.proc_text.setToolTip(text)

    # ═══════════════ 조작 ═══════════════
    def _warn(self, title: str, text: str) -> None:
        """비모달 경고."""
        try:
            box = QMessageBox(QMessageBox.Icon.Warning, title, text, QMessageBox.StandardButton.Ok, self)
            box.setWindowModality(Qt.WindowModality.NonModal)
            box.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, True)
            self._msg_boxes.append(box)
            box.finished.connect(lambda *_a, b=box: self._msg_boxes.remove(b) if b in self._msg_boxes else None)
            box.show()
        except Exception:
            pass

    def _on_zero_clicked(self, *_a) -> None:
        if self._busy:
            return
        if self._st.get("process_running"):
            self._warn("두께 영점", "공정 중에는 영점을 할 수 없습니다.")
            return
        reply = QMessageBox.question(
            self, "두께 영점", "두께 모니터의 두께·타이머를 0 으로 맞춥니다.\n계속하시겠습니까?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, QMessageBox.StandardButton.No)
        if reply != QMessageBox.StandardButton.Yes:
            return
        self._busy = True
        self._render()

        async def _go():
            try:
                ok, why = await self._svc.zero("B", why="패널")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                ok, why = False, f"{e!r}"
            finally:
                self._busy = False
            if not ok:
                self._warn("두께 영점", why or "영점 실패")
            self._render()

        try:
            t = asyncio.get_event_loop().create_task(_go())
            self._tasks.add(t)
            t.add_done_callback(self._tasks.discard)
        except Exception as e:
            self._busy = False
            self._render()
            self._warn("두께 영점", f"명령을 실행할 수 없습니다: {e!r}")

    def _on_factor_clicked(self, *_a) -> None:
        try:
            if self._dialog is not None and self._dialog.isVisible():
                self._dialog.raise_()
                self._dialog.activateWindow()
                return
            from ui.thickness_factor_dialog import ThicknessFactorDialog
            dlg = ThicknessFactorDialog(self._cfg.factor_file, ch=int(getattr(self._cfg, "ch", 2) or 2),
                                        on_saved=self._svc.reload_factors,
                                        materials_path=getattr(self._svc, "materials_file", None), parent=self.window())
            dlg.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, True)
            dlg.destroyed.connect(lambda *_a: setattr(self, "_dialog", None))
            self._dialog = dlg
            dlg.open()
        except Exception as e:
            self._warn("Factor 설정", f"설정창을 열 수 없습니다: {e!r}")

    def shutdown(self) -> None:
        for t in list(self._tasks):
            with contextlib.suppress(Exception):
                t.cancel()
        with contextlib.suppress(Exception):
            if self._dialog is not None:
                self._dialog.close()
