"""
plasma_cleaning_runtime.py
=================================

This module defines a minimal asynchronous runtime for executing a plasma
cleaning process.  The runtime is designed to mirror the structure of the
existing ChamberRuntime: it accepts pre‑created device instances (PLC,
MFC, IG) and exposes a simple API to start and stop the plasma cleaning
sequence.  The process sequence roughly follows these steps:

1. Wait for the vacuum chamber to reach a specified base pressure using
   the IG (ion gauge).
2. Enable the MFC channel used for sputter gun SP4 and set the gas flow
   to a desired value.
3. Wait until the chamber pressure rises to a target working pressure.
4. Apply RF power via the shared PLC's DAC channel for a specified
   duration.
5. Ramp down RF power and close the gas flow.

The runtime does not depend on Qt; it communicates status through
asyncio logs and optionally a chat notifier.
"""

from __future__ import annotations

import asyncio
from typing import Optional, Callable, Any

from device.mfc import AsyncMFC
from device.ig import AsyncIG
from device.rf_power import RFPowerAsync
from device.plc import AsyncPLC  # type: ignore
from controller.chat_notifier import ChatNotifier  # type: ignore


class PlasmaCleaningRuntime:
    """Asynchronous runtime for executing a plasma cleaning process.

    Parameters
    ----------
    ui: Any
        Reference to the main UI.  The runtime will read user inputs
        (target pressure, gas flow, working pressure, RF power, process
        time) from the appropriate widgets.  It also provides an
        ``append_log`` method to write log messages to the UI.
    prefix: str
        Prefix for locating UI widgets (e.g. ``PC_Start_button``).  An
        empty string means that widgets are named exactly as in the UI.
    loop: asyncio.AbstractEventLoop
        Event loop used for creating background tasks.
    plc: AsyncPLC
        Shared PLC instance used for controlling valves and RF power.
    mfc: Optional[AsyncMFC]
        Initial MFC instance.  This controls the SP4 channel on the
        selected chamber; it can be replaced later via :meth:`set_devices`.
    ig: Optional[AsyncIG]
        Initial IG (ion gauge) instance for reading vacuum pressure.  It
        can be replaced later via :meth:`set_devices`.
    chat: Optional[ChatNotifier]
        Optional chat notifier for reporting errors.
    """

    def __init__(
        self,
        *,
        ui: Any,
        prefix: str,
        loop: asyncio.AbstractEventLoop,
        plc: AsyncPLC,
        mfc: Optional[AsyncMFC] = None,
        ig: Optional[AsyncIG] = None,
        chat: Optional[ChatNotifier] = None,
    ) -> None:
        self.ui = ui
        self.prefix = prefix
        self.loop = loop
        self.plc = plc
        self.mfc_sp4: Optional[AsyncMFC] = mfc
        self.gas_mfc: Optional[AsyncMFC] = mfc
        self.gas_channel: Optional[int] = None
        self.ig: Optional[AsyncIG] = ig
        self.chat = chat
        # RF power controller uses PLC's DCV channel 1 (index 1) by default.
        # The PLC wrapper exposes power_apply/power_write to control the
        # forward power; we wrap these in callbacks for RFPowerAsync.
        async def _rf_send(power: float) -> None:
            # ensure the RF enable latch is set before applying power
            try:
                await self.plc.power_apply(power, family="DCV", ensure_set=True, channel=1)
            except Exception as e:
                self.append_log("RF", f"power_apply failed: {e!r}")

        async def _rf_send_unverified(power: float) -> None:
            try:
                await self.plc.power_write(power, family="DCV", write_idx=1)
            except Exception as e:
                self.append_log("RF", f"power_write failed: {e!r}")

        async def _rf_read_status() -> dict[str, float] | None:
            try:
                fwd = await self.plc.read_reg_name("DCV_READ_2")
                ref = await self.plc.read_reg_name("DCV_READ_3")
                return {"forward": float(fwd), "reflected": float(ref)}
            except Exception as e:
                self.append_log("RF", f"read RF status failed: {e!r}")
                return None

        self.rf_power = RFPowerAsync(
            send_rf_power=_rf_send,
            send_rf_power_unverified=_rf_send_unverified,
            request_status_read=_rf_read_status,
        )
        # Background task handle
        self._task: Optional[asyncio.Task[Any]] = None

    # ------------------------------------------------------------------
    # Device binding
    def set_devices(
        self,
        *,
        ig: Optional[AsyncIG] = None,
        mfc_sp4: Optional[AsyncMFC] = None,
        gas_mfc: Optional[AsyncMFC] = None,
        gas_channel: Optional[int] = None,
    ) -> None:
        """Bind IG/MFC instances for the plasma cleaning process.

        Parameters are optional; only provided values will be updated.  If
        ``gas_mfc`` or ``gas_channel`` are given, these control which MFC
        channel supplies the gas during plasma cleaning (e.g. always MFC1
        channel 3 in the current design).  The ``mfc_sp4`` controls the
        SP4_SET/SP4_ON commands for the selected chamber.
        """
        if ig is not None:
            self.ig = ig
        if mfc_sp4 is not None:
            self.mfc_sp4 = mfc_sp4
        if gas_mfc is not None:
            self.gas_mfc = gas_mfc
        if gas_channel is not None:
            self.gas_channel = int(gas_channel)

    # ------------------------------------------------------------------
    # Logging helper
    def append_log(self, src: str, msg: str) -> None:
        """Send a log message to the UI or chat notifier."""
        try:
            # UI log: use the same interface as ChamberRuntime
            log_fn = getattr(self.ui, "append_log", None)
            if callable(log_fn):
                log_fn(src, msg)
        except Exception:
            pass
        # Chat notifier
        if self.chat:
            with asyncio.get_running_loop().call_exception_handler:
                try:
                    self.chat.notify_text(f"[{src}] {msg}")
                except Exception:
                    pass

    # ------------------------------------------------------------------
    async def _wait_base_pressure(self, target_pressure: float) -> bool:
        """Wait until the IG reports a pressure lower than ``target_pressure``."""
        if not self.ig:
            self.append_log("IG", "IG device not set; skipping base pressure wait")
            return True
        try:
            await self.ig.start()
        except Exception:
            pass
        try:
            ok = await self.ig.wait_for_base_pressure(float(target_pressure))
            return ok
        except Exception as e:
            self.append_log("IG", f"wait_for_base_pressure failed: {e!r}")
            return False

    async def _set_gas_flow(self, flow: float) -> None:
        """Set the SP4 flow on the selected chamber and open the valve."""
        if not self.mfc_sp4:
            self.append_log("MFC", "MFC (SP4) device not set; skipping gas flow")
            return
        try:
            await self.mfc_sp4.handle_command("SP4_SET", {"flow": float(flow)})
            await self.mfc_sp4.handle_command("SP4_ON", {})
        except Exception as e:
            self.append_log("MFC", f"SP4_SET/ON failed: {e!r}")

        if self.gas_mfc and self.gas_channel is not None:
            try:
                await self.gas_mfc.handle_command("FLOW_SET", {"ch": self.gas_channel, "flow": float(flow)})
                await self.gas_mfc.handle_command("FLOW_ON", {"ch": self.gas_channel})
            except Exception as e:
                self.append_log("MFC", f"FLOW_SET/ON failed: {e!r}")

    async def _close_gas_flow(self) -> None:
        """Turn off SP4 and the gas flow channel."""
        if self.mfc_sp4:
            try:
                await self.mfc_sp4.handle_command("SP4_OFF", {})
            except Exception as e:
                self.append_log("MFC", f"SP4_OFF failed: {e!r}")
        if self.gas_mfc and self.gas_channel is not None:
            try:
                await self.gas_mfc.handle_command("FLOW_OFF", {"ch": self.gas_channel})
            except Exception as e:
                self.append_log("MFC", f"FLOW_OFF failed: {e!r}")

    async def _wait_working_pressure(self, working_pressure: float) -> None:
        """Wait until chamber pressure rises to at least ``working_pressure``."""
        if not self.ig:
            return
        try:
            if hasattr(self.ig, "events"):
                async for ev in self.ig.events():
                    if getattr(ev, "kind", None) == "pressure":
                        value = getattr(ev, "pressure", None)
                        try:
                            if value is not None and float(value) >= float(working_pressure):
                                return
                        except Exception:
                            pass
            else:
                await asyncio.sleep(5.0)
        except Exception:
            pass

    async def _apply_rf_power(self, power: float, duration_sec: float) -> None:
        """Apply RF power for ``duration_sec`` seconds and then ramp down."""
        try:
            await self.rf_power.start_process(float(power))
        except Exception as e:
            self.append_log("RF", f"start_process failed: {e!r}")
            return
        try:
            await asyncio.sleep(max(0.0, float(duration_sec)))
        finally:
            try:
                await self.rf_power.cleanup()
            except Exception as e:
                self.append_log("RF", f"cleanup failed: {e!r}")

    async def _run_sequence(
        self,
        *,
        target_pressure: float,
        gas_flow: float,
        working_pressure: float,
        rf_power: float,
        process_time_min: float,
    ) -> None:
        """Execute the plasma cleaning sequence with the given parameters."""
        self.append_log("PC", f"Waiting for base pressure {target_pressure} Torr…")
        ok = await self._wait_base_pressure(target_pressure)
        if not ok:
            self.append_log("PC", "Base pressure wait failed; aborting")
            return
        self.append_log("PC", "Base pressure reached")

        self.append_log("PC", f"Setting gas flow to {gas_flow} sccm and opening valves")
        await self._set_gas_flow(gas_flow)

        self.append_log("PC", f"Waiting for working pressure {working_pressure} Torr…")
        await self._wait_working_pressure(working_pressure)
        self.append_log("PC", "Working pressure reached or timeout")

        duration_sec = float(process_time_min) * 60.0
        self.append_log("PC", f"Applying RF power {rf_power} W for {process_time_min} min")
        await self._apply_rf_power(rf_power, duration_sec)
        self.append_log("PC", "RF power process finished")

        self.append_log("PC", "Closing gas valves")
        await self._close_gas_flow()
        self.append_log("PC", "Plasma cleaning sequence complete")

    # ------------------------------------------------------------------
    def _read_ui_params(self) -> dict[str, float] | None:
        """Read user‑specified parameters from the UI."""
        try:
            t_p = float(self.ui.PC_targetPressure_edit.toPlainText())
            g_f = float(self.ui.PC_gasFlow_edit.toPlainText())
            w_p = float(self.ui.PC_workingPressure_edit.toPlainText())
            rf = float(self.ui.PC_rfPower_edit.toPlainText())
            p_t = float(self.ui.PC_ProcessTime_edit.toPlainText())
        except Exception as e:
            self.append_log("PC", f"Failed to parse UI parameters: {e!r}")
            return None
        return {
            "target_pressure": t_p,
            "gas_flow": g_f,
            "working_pressure": w_p,
            "rf_power": rf,
            "process_time_min": p_t,
        }

    # ------------------------------------------------------------------
    def start_process_from_ui(self) -> None:
        """Read parameters from UI and begin the plasma cleaning process."""
        params = self._read_ui_params()
        if not params:
            return
        if self._task and not self._task.done():
            self.append_log("PC", "Cancelling existing plasma cleaning task")
            self._task.cancel()
        self._task = self.loop.create_task(self._run_sequence(**params))

    def stop(self) -> None:
        """Stop the running plasma cleaning process."""
        if self._task and not self._task.done():
            self._task.cancel()
            self.append_log("PC", "Plasma cleaning process cancelled by user")
        self.loop.create_task(self.rf_power.cleanup())
        self.loop.create_task(self._close_gas_flow())

    def shutdown_fast(self) -> None:
        """Quickly shut down without awaiting device cleanup."""
        if self._task and not self._task.done():
            self._task.cancel()
        try:
            self.loop.create_task(self.rf_power.cleanup())
        except Exception:
            pass
        try:
            self.loop.create_task(self._close_gas_flow())
        except Exception:
            pass
