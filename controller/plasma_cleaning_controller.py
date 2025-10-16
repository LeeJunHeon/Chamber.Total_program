"""
plasma_cleaning_controller.py
=================================

This controller binds UI events to the plasma cleaning runtime.  It
connects the Start/Stop buttons on the Plasma Cleaning page to the
runtime's start and stop methods and can optionally send notifications
via a chat notifier.  The goal of this controller is to encapsulate
UI‑specific logic separately from the runtime implementation.
"""

from __future__ import annotations

from typing import Any, Optional
from PySide6.QtCore import QObject  # for type hints only

from controller.chat_notifier import ChatNotifier  # type: ignore
from runtime.plasma_cleaning_runtime import PlasmaCleaningRuntime


class PlasmaCleaningController:
    """UI controller for the plasma cleaning runtime."""

    def __init__(self, *, ui: Any, runtime: PlasmaCleaningRuntime, chat: Optional[ChatNotifier] = None) -> None:
        self.ui = ui
        self.runtime = runtime
        self.chat = chat
        # Bind UI buttons to runtime actions
        try:
            start_btn = getattr(self.ui, "PC_Start_button", None)
            stop_btn = getattr(self.ui, "PC_Stop_button", None)
            if start_btn:
                start_btn.clicked.connect(self._on_start_clicked)  # type: ignore[attr-defined]
            if stop_btn:
                stop_btn.clicked.connect(self._on_stop_clicked)  # type: ignore[attr-defined]
        except Exception:
            # In headless or test environments the UI may not be available
            pass

    # --------------------------------------------------------------
    def _on_start_clicked(self) -> None:
        """Handle Start button click: begin the plasma cleaning process."""
        try:
            self.runtime.start_process_from_ui()
        except Exception as e:
            # Log locally and via chat notifier
            msg = f"Failed to start plasma cleaning: {e!r}"
            try:
                self.runtime.append_log("PC", msg)
            except Exception:
                pass
            if self.chat:
                with self.chat:
                    try:
                        self.chat.notify_error_with_src("PC", msg)
                    except Exception:
                        pass

    def _on_stop_clicked(self) -> None:
        """Handle Stop button click: cancel the running process."""
        try:
            self.runtime.stop()
        except Exception as e:
            msg = f"Failed to stop plasma cleaning: {e!r}"
            try:
                self.runtime.append_log("PC", msg)
            except Exception:
                pass
            if self.chat:
                with self.chat:
                    try:
                        self.chat.notify_error_with_src("PC", msg)
                    except Exception:
                        pass
