# controller/graph_controller.py
from __future__ import annotations

from typing import Sequence
import numpy as np
import pyqtgraph as pg
from PySide6.QtCore import Qt
from PySide6.QtGui import QFont
from PySide6.QtWidgets import QVBoxLayout, QWidget


class GraphController:
    """
    RGA: 스템(Stem) + 마커 구성 (로그 값은 수동 변환 후 y축은 선형 스케일 유지)
    OES: 라인 그래프
    """
    # 기본 축 범위
    RGA_X_RANGE = (0, 65)
    RGA_Y_RANGE = (-11, -5)  # log10(Torr) 값(예: 1e-11 ~ 1e-5)
    OES_X_RANGE = (100, 1200)
    OES_Y_RANGE = (0, 16000)

    def __init__(self, rga_widget: QWidget, oes_widget: QWidget):
        font = QFont()
        font.setPointSize(12)

        # ───── RGA Plot ─────────────────────────────────────────────
        self.rga_plot = pg.PlotWidget()
        self.rga_plot.setBackground("w")
        self.rga_plot.setLabel("left", "Partial Pressure", color="k")
        self.rga_plot.setLabel("bottom", "Atomic Mass", color="k")
        self.rga_plot.showGrid(x=False, y=True)
        self.rga_plot.setMouseEnabled(x=False, y=False)
        # 로그 변환은 직접(log10) 하므로 PlotWidget 자체 로그 모드는 사용하지 않음
        self.rga_plot.setLogMode(y=False)

        # 올바른 기본 범위 적용 (기존 코드의 x/y 스왑 버그 수정)
        self._rga_default_xrange = self.RGA_X_RANGE
        self._rga_default_yrange = self.RGA_Y_RANGE
        self.rga_plot.setXRange(*self._rga_default_xrange)
        self.rga_plot.setYRange(*self._rga_default_yrange)

        # 축 스타일
        for axis in ("left", "bottom"):
            ax = self.rga_plot.getAxis(axis)
            ax.setStyle(tickFont=font)
            ax.setTextPen("k")
            ax.setPen(pg.mkPen("k"))

        # y축 눈금(1E-5 ~ 1E-11)
        y_axis_rga = self.rga_plot.getAxis("left")
        log_ticks = [[(-i, f"1E{-i}") for i in range(5, 12)]]
        y_axis_rga.setTicks(log_ticks)
        y_axis_rga.enableAutoSIPrefix(False)

        # x축 눈금(0 ~ 65 step 5)
        x_axis_rga = self.rga_plot.getAxis("bottom")
        x_ticks = [[(i, str(i)) for i in range(0, 66, 5)]]
        x_axis_rga.setTicks(x_ticks)

        # 스템(점선) + 마커
        pen_style = pg.mkPen(color="r", width=1, style=Qt.PenStyle.DotLine)
        self.rga_stem_item = pg.ErrorBarItem(
            x=np.array([]), y=np.array([]), height=np.array([]), beam=0, pen=pen_style
        )
        self.rga_marker_item = pg.PlotDataItem(
            pen=None, symbol="s", symbolSize=6, symbolBrush="r", symbolPen=None
        )
        self.rga_plot.addItem(self.rga_stem_item)
        self.rga_plot.addItem(self.rga_marker_item)

        rga_layout = QVBoxLayout()
        rga_layout.setContentsMargins(0, 0, 0, 0)
        rga_layout.addWidget(self.rga_plot)
        rga_widget.setLayout(rga_layout)

        # ───── OES Plot ─────────────────────────────────────────────
        self.oes_plot = pg.PlotWidget()
        self.oes_plot.setBackground("w")
        self.oes_plot.setLabel("left", "Intensity", color="k")
        self.oes_plot.setLabel("bottom", "Wavelength (nm)", color="k")
        self.oes_plot.showGrid(x=False, y=True)
        self.oes_plot.setMouseEnabled(x=False, y=False)

        self._oes_default_xrange = self.OES_X_RANGE
        self._oes_default_yrange = self.OES_Y_RANGE
        self.oes_plot.setXRange(*self._oes_default_xrange)
        self.oes_plot.setYRange(*self._oes_default_yrange)

        # 눈금
        x_axis_oes = self.oes_plot.getAxis("bottom")
        x_axis_oes.setTicks([[(i, str(i)) for i in range(100, 1300, 100)]])
        y_axis_oes = self.oes_plot.getAxis("left")
        y_axis_oes.setTicks([[(i, str(i)) for i in range(0, 18000, 2000)]])
        y_axis_oes.enableAutoSIPrefix(False)
        for axis in ("left", "bottom"):
            ax = self.oes_plot.getAxis(axis)
            ax.setStyle(tickFont=font)
            ax.setTextPen("k")
            ax.setPen(pg.mkPen("k"))

        self.oes_curve = self.oes_plot.plot(pen=pg.mkPen("r", width=1))

        oes_layout = QVBoxLayout()
        oes_layout.setContentsMargins(0, 0, 0, 0)
        oes_layout.addWidget(self.oes_plot)
        oes_widget.setLayout(oes_layout)

    # ──────────────────────────────────────────────────────────────
    # Update APIs
    # ──────────────────────────────────────────────────────────────
    def update_rga_plot(self, x_data: Sequence[float], y_data: Sequence[float]) -> None:
        """RGA 값을 스템+마커로 갱신. y는 Torr 기준 입력(양수), 내부에서 log10 변환."""
        if x_data is None or y_data is None:
            self._clear_rga_items()
            return

        x = np.asarray(x_data, dtype=float)
        y = np.asarray(y_data, dtype=float)

        # 길이 보정
        n = min(len(x), len(y))
        if n == 0:
            self._clear_rga_items()
            return
        x = x[:n]
        y = y[:n]

        # 음수/NaN/inf 제거
        valid = np.isfinite(y) & (y > 0)
        if not np.any(valid):
            self._clear_rga_items()
            return
        xv = x[valid]
        yv = y[valid]

        # log10 변환 (아주 작은 값은 하한으로 클리핑)
        logy = np.log10(np.clip(yv, 1e-12, None))

        # 현재 뷰의 y 최소값(하단)에서 막대를 위로 그린다.
        y_min = self.rga_plot.getViewBox().viewRange()[1][0]
        # 막대 상단이 화면 하단보다 낮으면(=더 작은 값) 화면 하단으로 클리핑
        top = np.maximum(logy, y_min)
        bottom = np.full_like(top, y_min)
        centers = (top + bottom) / 2.0
        heights = top - bottom  # 항상 >= 0

        # 업데이트
        self.rga_stem_item.setData(x=xv, y=centers, height=heights)
        self.rga_marker_item.setData(x=xv, y=top)

    def update_oes_plot(self, x_data: Sequence[float], y_data: Sequence[float]) -> None:
        self.oes_curve.setData(x_data, y_data)

    # ──────────────────────────────────────────────────────────────
    # Clear/Reset
    # ──────────────────────────────────────────────────────────────
    def clear_rga_plot(self) -> None:
        self._clear_rga_items()
        self.rga_plot.setXRange(*self._rga_default_xrange)
        self.rga_plot.setYRange(*self._rga_default_yrange)

    def clear_oes_plot(self) -> None:
        self.oes_curve.setData([], [])
        self.oes_plot.setXRange(*self._oes_default_xrange)
        self.oes_plot.setYRange(*self._oes_default_yrange)

    def reset(self) -> None:
        self.clear_rga_plot()
        self.clear_oes_plot()

    # ──────────────────────────────────────────────────────────────
    # Internal
    # ──────────────────────────────────────────────────────────────
    def _clear_rga_items(self) -> None:
        self.rga_stem_item.setData(x=np.array([]), y=np.array([]), height=np.array([]))
        self.rga_marker_item.setData(x=np.array([]), y=np.array([]))
