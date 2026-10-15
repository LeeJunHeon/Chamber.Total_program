# ui/heater_panel.py
# -*- coding: utf-8 -*-
"""CH2 히터 상태 패널 — 코드로 만든 QFrame(Qt Designer 파일을 쓰지 않음). 챔버K heater_group 스타일.

리스너로 받은 상태 dict(st)로만 화면을 그린다. 버튼은 RUN 의 '표시'일 뿐이고,
누르면 HeaterService 명령을 태스크로 실행해 결과 사유를 비모달 경고로 보여 준다.
스타일시트는 값이 바뀔 때만 다시 넣는다.
"""
from __future__ import annotations

import asyncio
import contextlib
from typing import Any, Optional

from PySide6.QtCore import QRect, Qt
from PySide6.QtWidgets import (QFrame, QLabel, QLineEdit, QMessageBox, QProgressBar,
                               QPushButton, QWidget)

from controller.heater_process import itl_missing_text

PANEL_W = 240
PANEL_H = 340

_OK_BG, _OK_FG = "#e8f5e9", "#2e7d32"
_OFF_BG, _OFF_FG = "#eef1f4", "#9aa3ab"
_ERR_BG, _ERR_FG = "#ffebee", "#c62828"

# (키, 글자, 종류 'ok'|'err', 툴팁) — 4열 × 2줄: 윗줄 인터락 조건, 아랫줄 결과·이상
_LAMPS = [
    ("tc_ok", "TC", "ok", "TC1_OK (M0040D) — 열전대 입력 정상"),
    ("da_ok", "D/A", "ok", "DA_OK (M00410) — D/A 출력 모듈 정상"),
    ("water_ok", "냉각수", "ok", "WATER_OK (M00411) — CH2 냉각수 신호(WATER_2_SGN)"),
    ("vac_ok", "진공", "ok", "VAC_OK (M00412) — CH2 메인밸브 인터락(M_V_2_인터락): "
                             "공압 · 게이지A 진공 · 러핑 · 포라인 · 터보 · 벤트 닫힘"),
    ("itl", "인터락", "ok", "ITL (M00401) — TC1_OK · DA_OK · 냉각수 · 진공 · ¬FAULT. "
                         "운전 중 꺼지면 PLC 가 RUN 을 끈다(복귀해도 다시 켜지 않음)"),
    ("ot", "과온", "err", "OT (M00404) — 과온 트립(래치, 리셋 필요)"),
    ("tc_err", "TC 이상", "err", "TC_ERR (M00405) — 운전 중 센서 이상(래치, 리셋 필요)"),
    ("wd_err", "워치독", "err", "WD_ERR (M00406) — 파이썬 워치독 30초 무변화(래치, 리셋 필요)"),
]
LAMP_COLS = 4          # 한 줄 램프 수
LAMP_W, LAMP_H = 52, 20
LAMP_STEP_X, LAMP_STEP_Y = 55, 24

_OWNER_TEXT = {"manual": "수동", "process": "공정", "external": "외부"}


class HeaterPanel(QFrame):
    def __init__(self, service: Any, cfg: Any, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self._svc = service
        self._cfg = cfg
        self._st: dict = {}
        self._busy = False
        self._styles: dict = {}
        self._tasks: set = set()
        self._msg_boxes: list = []

        self.setObjectName("heater_panel")
        self.resize(PANEL_W, PANEL_H)
        self.setFrameShape(QFrame.Shape.StyledPanel)
        self.setStyleSheet("QFrame#heater_panel {background: #ffffff; border: 2px solid #cccccc; border-radius: 8px;}")

        self.title = QLabel("CH2 Heater [°C]", self)
        self.title.setGeometry(QRect(10, 6, 140, 20))
        self.title.setStyleSheet("QLabel {border: none; color: #333333; font-weight: bold;}")
        self.owner_label = QLabel("—", self)
        self.owner_label.setGeometry(QRect(150, 6, 80, 20))
        self.owner_label.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        self.owner_label.setStyleSheet("QLabel {border: none; color: #6b7280; font-size: 8pt;}")

        # ── LCD ──
        self.lcd = QFrame(self)
        self.lcd.setObjectName("heater_lcd")
        self.lcd.setGeometry(QRect(10, 30, 220, 132))
        self.lcd.setStyleSheet("QFrame#heater_lcd {background: #f7f8fa; border: 1px solid #dfe3e8; border-radius: 6px;}")
        _small = "QLabel {border: none; background: transparent; color: #6b7280; font-size: 8pt;}"
        lab = QLabel("PV", self.lcd); lab.setGeometry(QRect(8, 8, 22, 14)); lab.setStyleSheet(_small)
        self.pv_label = QLabel("---", self.lcd)
        self.pv_label.setGeometry(QRect(30, 2, 150, 34))
        self.pv_label.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        unit = QLabel("°C", self.lcd); unit.setGeometry(QRect(186, 12, 26, 18)); unit.setStyleSheet(_small)
        lab = QLabel("SV", self.lcd); lab.setGeometry(QRect(8, 44, 22, 14)); lab.setStyleSheet(_small)
        self.sv_label = QLabel("---", self.lcd)
        self.sv_label.setGeometry(QRect(30, 38, 104, 28))
        self.sv_label.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        self.dev_label = QLabel("", self.lcd)
        self.dev_label.setGeometry(QRect(138, 44, 74, 18))
        self.dev_label.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        sep = QFrame(self.lcd); sep.setGeometry(QRect(8, 72, 204, 1))
        sep.setStyleSheet("background: #e5e7eb; border: none;")
        self.state_label = QLabel("정지", self.lcd)
        self.state_label.setGeometry(QRect(8, 78, 204, 18))
        self.state_label.setStyleSheet("QLabel {border: none; background: transparent; color: #546e7a; font-size: 8.5pt;}")
        self.badge = QLabel("STOP", self.lcd)
        self.badge.setGeometry(QRect(8, 104, 46, 18))
        self.badge.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.mv_bar = QProgressBar(self.lcd)
        self.mv_bar.setGeometry(QRect(60, 104, 152, 18))
        self.mv_bar.setRange(0, 100)
        self.mv_bar.setValue(0)
        self.mv_bar.setTextVisible(True)
        self.mv_bar.setFormat("MV -/-")
        self.mv_bar.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.mv_bar.setStyleSheet(
            "QProgressBar {background: #eceff1; border: 1px solid #dfe3e8; border-radius: 3px; color: #37474f; font-size: 8pt;}"
            "QProgressBar::chunk {background: #90a4ae; border-radius: 2px;}")

        # ── 램프 8개(4열 × 2줄) ──
        self.lamps = {}
        for i, (key, text, kind, tip) in enumerate(_LAMPS):
            w = QLabel(text, self)
            w.setGeometry(QRect(10 + (i % LAMP_COLS) * LAMP_STEP_X, 170 + (i // LAMP_COLS) * LAMP_STEP_Y,
                                LAMP_W, LAMP_H))
            w.setAlignment(Qt.AlignmentFlag.AlignCenter)
            w.setToolTip(tip)
            self.lamps[key] = (w, kind)

        # ── 목표 입력 + 버튼 ──
        self.target_edit = QLineEdit(self)
        self.target_edit.setGeometry(QRect(10, 226, 70, 26))
        self.target_edit.setPlaceholderText("°C")
        self.target_edit.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        self.apply_button = QPushButton("적용", self)
        self.apply_button.setGeometry(QRect(86, 226, 62, 26))
        self.onoff_button = QPushButton("ON", self)
        self.onoff_button.setGeometry(QRect(154, 226, 76, 26))
        self.onoff_button.setCheckable(True)
        self.onoff_button.setStyleSheet(
            "QPushButton {background: #ebebe9; color: black; font-weight: bold; border-radius: 4px; border: 1px solid #cccccc;}"
            "QPushButton:checked {background: #32FF32;}"
            "QPushButton:disabled {background: #f3f4f6; color: #9aa3ab; border: 1px solid #e0e3e7;}"
            "QPushButton:checked:disabled {background: #c8e6c9; color: #6b7280;}")

        # ── 상태 문구 + 리셋 ──
        self.status_label = QLabel("PLC 응답 대기", self)
        self.status_label.setGeometry(QRect(10, 262, 156, 66))
        self.status_label.setWordWrap(True)
        self.status_label.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
        self.reset_button = QPushButton("리셋", self)
        self.reset_button.setGeometry(QRect(172, 262, 58, 26))
        self.reset_button.setVisible(False)

        self.onoff_button.clicked.connect(self._on_onoff_clicked)
        self.apply_button.clicked.connect(self._on_apply_clicked)
        self.reset_button.clicked.connect(self._on_reset_clicked)

        self.update_status({"stale": True, "age_s": None})

    # ═══════════════ 표시 ═══════════════
    def _style(self, w: QWidget, css: str) -> None:
        if self._styles.get(id(w)) != css:
            self._styles[id(w)] = css
            w.setStyleSheet(css)

    def _set_button_view(self, checked: bool, text: str) -> None:
        """ON/OFF 버튼 표시를 바꾸는 유일한 경로(blockSignals 로 감싸 재진입 차단)."""
        btn = self.onoff_button
        try:
            btn.blockSignals(True)
            btn.setChecked(bool(checked))
            btn.setText(text)
        finally:
            btn.blockSignals(False)

    @staticmethod
    def _fault_text(st: dict) -> str:
        if st.get("ot"):
            return "과온 트립"
        if st.get("tc_err"):
            return "센서 이상"
        if st.get("wd_err"):
            return "워치독 트립"
        return "이상 발생"

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
        run = bool(st.get("run"))
        fault = bool(st.get("fault"))
        pv = st.get("pv")
        owner = getattr(self._svc, "owner", None) if self._svc is not None else None
        self.owner_label.setText(_OWNER_TEXT.get(owner, "—"))

        # PV / SV / Δ
        val_col = "#9aa3ab" if stale else "#1f2937"
        self.pv_label.setText("---" if pv is None else f"{float(pv):.1f}")
        self._style(self.pv_label, f"QLabel {{border: none; background: transparent; color: {val_col}; font-size: 20pt; font-weight: bold;}}")
        sv_show = st.get("cur_sv") if run else st.get("sv")
        self.sv_label.setText("---" if sv_show is None else f"{float(sv_show):.1f}")
        sv_col = "#9aa3ab" if stale else "#6b7280"
        self._style(self.sv_label, f"QLabel {{border: none; background: transparent; color: {sv_col}; font-size: 15pt; font-weight: bold;}}")
        if run and pv is not None and sv_show is not None:
            dev = float(pv) - float(sv_show)
            tol = float(getattr(self._cfg, "soak_tol", 2.0))
            self.dev_label.setText(f"Δ{dev:+.1f}")
            dcol = "#2e7d32" if abs(dev) <= tol else "#6b7280"
            if stale:
                dcol = "#9aa3ab"                      # 마지막으로 읽은 값
        else:
            self.dev_label.setText("")
            dcol = "#6b7280"
        self._style(self.dev_label, f"QLabel {{border: none; background: transparent; color: {dcol}; font-size: 9pt;}}")

        # 상태 줄
        self.state_label.setText(self._state_line(st))

        # 배지
        if fault:
            badge, bd, bg, fg = "FAULT", "#c62828", "#c62828", "#ffffff"
        elif not st.get("itl"):
            badge, bd, bg, fg = "ITL", "#ef6c00", "#fff3e0", "#ef6c00"
        elif run:
            badge, bd, bg, fg = "RUN", "#2e7d32", "#e8f5e9", "#2e7d32"
        else:
            badge, bd, bg, fg = "STOP", "#c8cdd3", "#eef1f4", "#5f6b76"
        if stale:
            bd, bg, fg = "#c8cdd3", "#eef1f4", "#9aa3ab"   # 글자는 그대로, 색만 회색(마지막으로 읽은 값)
        self.badge.setText(badge)
        self._style(self.badge, f"QLabel {{border: 1px solid {bd}; background: {bg}; color: {fg}; font-size: 8pt; font-weight: bold; border-radius: 3px;}}")

        # 출력 막대(현재 상한 기준)
        mv, lim = st.get("mv"), st.get("mv_limit")
        mv_min = int(getattr(self._cfg, "mv_min", 400))
        if mv is not None and lim is not None and int(lim) > mv_min:
            pct = max(0.0, min(100.0, (int(mv) - mv_min) / (int(lim) - mv_min) * 100.0))
            self.mv_bar.setValue(int(round(pct)))
            self.mv_bar.setFormat(f"MV {int(mv)}/{int(lim)}")
        else:
            self.mv_bar.setValue(0)
            self.mv_bar.setFormat("MV -/-")

        # 램프
        for key, (w, kind) in self.lamps.items():
            on = bool(st.get(key))
            if stale or st.get("ok") is None:
                bg, fg = _OFF_BG, _OFF_FG
            elif kind == "ok":
                bg, fg = (_OK_BG, _OK_FG) if on else (_OFF_BG, _OFF_FG)
            else:
                bg, fg = (_ERR_BG, _ERR_FG) if on else (_OK_BG, _OK_FG)
            self._style(w, f"QLabel {{border: 1px solid {fg}; background: {bg}; color: {fg}; font-size: 8pt; border-radius: 3px;}}")

        # 상태 문구
        text, col = self._status_text(st)
        self.status_label.setText(text)
        self._style(self.status_label, f"QLabel {{border: none; color: {col}; font-size: 9pt; font-weight: bold;}}")

        # 버튼
        self._set_button_view(run, "OFF" if run else "ON")
        process = owner == "process"
        self.onoff_button.setEnabled((not stale) and (not process) and not self._busy)
        self.apply_button.setEnabled((not stale) and (not process) and not self._busy)
        tip = "공정이 히터를 제어 중" if process else ""
        self.onoff_button.setToolTip(tip)
        self.apply_button.setToolTip(tip)
        self.reset_button.setVisible(fault)
        self.reset_button.setEnabled((not stale) and not self._busy)

    def _state_line(self, st: dict) -> str:
        svc = self._svc
        try:
            active, target, rate, remain = svc.ramp_info()
        except Exception:
            active, target, rate, remain = False, None, 0.0, None
        if active and target is not None:
            tail = f" · 약 {remain:.0f}분" if remain is not None else ""
            return f"램프 {rate:g}°C/min → {float(target):.1f}°C{tail}"
        try:
            hs = svc.hold_snapshot()
        except Exception:
            hs = {}
        if st.get("run") and hs.get("holding"):
            return f"유지 · DAC 상한 {hs.get('value')} 고정"
        if st.get("run"):
            return "운전 중"
        return "정지"

    def _status_text(self, st: dict):
        if st.get("fault"):
            return self._fault_text(st), "#c62828"
        if st.get("ok") is not None and not st.get("itl") and not st.get("stale"):
            return f"인터락 — {itl_missing_text(st)} 꺼짐", "#ef6c00"
        if st.get("stale"):
            age = st.get("age_s")
            return ("PLC 응답 없음" + (f" · {float(age):.0f}초 전 값" if age is not None else "")), "#c62828"
        if st.get("run"):
            try:
                hs = self._svc.hold_snapshot()
            except Exception:
                hs = {}
            try:
                ramp = self._svc.ramp_info()[0]
            except Exception:
                ramp = False
            if ramp:
                return "운전 중 · 램프", "#2e7d32"
            if hs.get("holding"):
                return "운전 중 · 유지", "#2e7d32"
            return "운전 중", "#2e7d32"
        return "정지", "#616161"

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

    def _target_value(self):
        txt = (self.target_edit.text() or "").strip()
        if not txt:
            return None, "목표 온도를 입력하십시오"
        try:
            t = float(txt)
        except Exception:
            return None, "목표 온도가 숫자가 아닙니다"
        lim = float(getattr(self._cfg, "max_temp", 500.0))
        svl = self._st.get("sv_limit")
        if svl:
            lim = min(lim, float(svl))
        if not (0.0 < t <= lim):
            return None, f"목표 범위 밖 — 0 초과 {lim:.1f}°C 이하"
        return t, ""

    def _run_cmd(self, title: str, coro_factory) -> None:
        if self._busy:
            return
        self._busy = True
        self._render()

        async def _go():
            try:
                ok, why = await coro_factory()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                ok, why = False, f"{e!r}"
            finally:
                self._busy = False
            if not ok and why:
                self._warn(title, why)
            elif ok and why:
                self._warn(title, why)          # 보류 OFF 등 알림
            self._render()

        try:
            t = asyncio.get_event_loop().create_task(_go())
            self._tasks.add(t)
            t.add_done_callback(self._tasks.discard)
        except Exception as e:
            self._busy = False
            self._render()
            self._warn(title, f"명령을 실행할 수 없습니다: {e!r}")

    def _on_onoff_clicked(self, *_a) -> None:
        run = bool(self._st.get("run"))
        self._set_button_view(run, "OFF" if run else "ON")   # 클릭 토글을 되돌린다 — 표시는 RUN 만 따른다
        if run:
            self._run_cmd("히터 OFF", lambda: self._svc.turn_off("정지(패널)", owner="manual"))
            return
        t, why = self._target_value()
        if t is None:
            self._warn("히터 ON", why)
            return
        self._run_cmd("히터 ON", lambda: self._svc.turn_on(t, owner="manual"))

    def _on_apply_clicked(self, *_a) -> None:
        t, why = self._target_value()
        if t is None:
            self._warn("목표 적용", why)
            return
        self._run_cmd("목표 적용", lambda: self._svc.apply_target(t))

    def _on_reset_clicked(self, *_a) -> None:
        if self._st.get("run"):
            self._warn("리셋 불가", "히터 RUN 이 켜져 있습니다.\n먼저 히터를 OFF 한 뒤 리셋하십시오.")
            return
        reply = QMessageBox.question(
            self, "히터 이상 리셋",
            "히터 이상을 리셋합니다.\n원인(배선·TC 모듈·과온)을 확인했습니까?\n\n"
            "원인이 남아 있으면 리셋 직후 다시 이상이 걸립니다.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No)
        if reply != QMessageBox.StandardButton.Yes:
            return
        self._run_cmd("히터 리셋", lambda: self._svc.reset_fault())

    def shutdown(self) -> None:
        for t in list(self._tasks):
            with contextlib.suppress(Exception):
                t.cancel()
