from __future__ import annotations

import copy
import contextlib
import inspect
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Tuple

from lib import user_config
from lib import config_common as cfgc
from lib import config_ch1
from lib import config_ch2


# ============================================================
# Data Models
# ============================================================

@dataclass
class ConfigChange:
    section: str
    key: str
    old: Any
    new: Any

    @property
    def path(self) -> Tuple[str, str]:
        return (self.section, self.key)

    @property
    def dotted(self) -> str:
        return f"{self.section}.{self.key}"


@dataclass
class ApplyPlan:
    immediate: List[ConfigChange] = field(default_factory=list)
    blocked_now: List[ConfigChange] = field(default_factory=list)


# ============================================================
# Controller
# ============================================================

class ConfigApplyController:
    """
    정책
    - changed-only
    - 지금 적용 가능한 값만 즉시 적용
    - 지금 적용 불가능하면 blocked_now 로만 반환
    - pending / deferred / 자동 flush 없음

    주의
    - main.py와의 호환을 위해 deferred / pending 키는 빈 리스트로 반환
    - has_pending() / flush_pending_if_safe() 도 남겨두되, 항상 빈 동작
    """

    SECTION_ORDER = (
        "communication",
        "tsp",
        "plasma_cleaning",
        "ch1",
        "ch2",
    )

    def __init__(self) -> None:
        self._last_seen_snapshot: Dict[str, Dict[str, Any]] = self._load_snapshot()

    # --------------------------------------------------------
    # Public API
    # --------------------------------------------------------

    def reset_baseline(self) -> None:
        self._last_seen_snapshot = self._load_snapshot()

    def get_pending_items(self) -> List[ConfigChange]:
        # 단순 정책: pending 사용 안 함
        return []

    def has_pending(self) -> bool:
        # 단순 정책: pending 사용 안 함
        return False

    async def apply_runtime(self, main_window) -> Dict[str, Any]:
        """
        Apply(Runtime) 시 호출
        - 현재 저장된 user_config를 읽음
        - 이전 기준선과 비교해서 changed-only 추출
        - 지금 적용 가능한 항목만 immediate
        - 나머지는 blocked_now
        """
        current = self._load_snapshot()

        # config module(cfgc/config_ch1/config_ch2)에 최신값 반영
        with contextlib.suppress(Exception):
            user_config.apply_overrides(current)

        new_changes = self._diff_snapshots(self._last_seen_snapshot, current)
        self._last_seen_snapshot = copy.deepcopy(current)

        state = self._runtime_state(main_window)
        plan = self._classify_changes(new_changes, state)

        if plan.immediate:
            await self._apply_immediate(main_window, plan.immediate, state)

        return {
            "state": state,
            "new_changes": new_changes,
            "applied_now": plan.immediate,
            "deferred": [],      # main.py 호환용
            "blocked_now": plan.blocked_now,
            "pending": [],       # main.py 호환용
        }

    async def flush_pending_if_safe(self, main_window) -> Dict[str, Any]:
        """
        단순 정책: 자동 flush 없음.
        main.py가 아직 이 함수를 호출하더라도 안전하게 빈 결과만 반환.
        """
        state = self._runtime_state(main_window)
        return {
            "state": state,
            "applied_now": [],
            "deferred": [],
            "blocked_now": [],
            "pending": [],
        }

    # --------------------------------------------------------
    # Snapshot / Diff
    # --------------------------------------------------------

    def _load_snapshot(self) -> Dict[str, Dict[str, Any]]:
        raw = user_config.load() or {}
        if not isinstance(raw, dict):
            raw = {}

        out: Dict[str, Dict[str, Any]] = {}
        for sec in self.SECTION_ORDER:
            v = raw.get(sec, {})
            out[sec] = copy.deepcopy(v if isinstance(v, dict) else {})
        return out

    def _diff_snapshots(
        self,
        old_snap: Dict[str, Dict[str, Any]],
        new_snap: Dict[str, Dict[str, Any]],
    ) -> List[ConfigChange]:
        changes: List[ConfigChange] = []

        all_sections = set(old_snap.keys()) | set(new_snap.keys())
        for sec in all_sections:
            old_map = old_snap.get(sec, {}) or {}
            new_map = new_snap.get(sec, {}) or {}

            all_keys = set(old_map.keys()) | set(new_map.keys())
            for key in all_keys:
                old_v = old_map.get(key, None)
                new_v = new_map.get(key, None)
                if old_v != new_v:
                    changes.append(ConfigChange(sec, key, old_v, new_v))

        return changes

    # --------------------------------------------------------
    # Runtime State
    # --------------------------------------------------------

    def _rt_running(self, rt) -> bool:
        if not rt:
            return False

        ir = getattr(rt, "is_running", None)
        if isinstance(ir, bool):
            return ir
        if callable(ir):
            with contextlib.suppress(Exception):
                return bool(ir())

        return bool(getattr(rt, "_running", False))

    def _runtime_state(self, main_window) -> Dict[str, bool]:
        ch1_running = self._rt_running(getattr(main_window, "ch1", None))
        ch2_running = self._rt_running(getattr(main_window, "ch2", None))
        pc_running = self._rt_running(getattr(main_window, "pc", None))
        any_running = ch1_running or ch2_running or pc_running

        return {
            "any_running": any_running,
            "ch1_running": ch1_running,
            "ch2_running": ch2_running,
            "pc_running": pc_running,
        }

    # --------------------------------------------------------
    # Classification Policy
    # --------------------------------------------------------

    def _is_endpoint_key(self, key: str) -> bool:
        key = str(key or "").upper()

        endpoint_tokens = (
            "_HOST",
            "_PORT",
            "_ADDR",
            "USB_INDEX",
            "SERIAL_PORT",
            "COM_PORT",
            "TCP_HOST",
            "TCP_PORT",
            "RS232",
        )
        return any(tok in key for tok in endpoint_tokens)

    def _can_apply_now(self, chg: ConfigChange, state: Dict[str, bool]) -> bool:
        """
        True  -> 지금 적용 가능
        False -> 지금 적용 금지(blocked_now)

        정책
        - communication:
            - 공용 endpoint 계열은 공정 중 금지
            - 공용 non-endpoint도 공정 중 금지
        - ch1:
            - CH1 실행 중이면 CH1 섹션 변경 금지
        - ch2:
            - CH2 실행 중이면 CH2 섹션 변경 금지
        - plasma_cleaning:
            - PC 실행 중이면 금지
        - tsp:
            - 보수적으로 any_running이면 금지
        """
        sec = chg.section

        if sec == "communication":
            return not state["any_running"]

        if sec == "ch1":
            return not state["ch1_running"]

        if sec == "ch2":
            return not state["ch2_running"]

        if sec == "plasma_cleaning":
            return not state["pc_running"]

        if sec == "tsp":
            return not state["any_running"]

        return True

    def _classify_changes(
        self,
        changes: Iterable[ConfigChange],
        state: Dict[str, bool],
    ) -> ApplyPlan:
        plan = ApplyPlan()

        for chg in changes:
            if self._can_apply_now(chg, state):
                plan.immediate.append(chg)
            else:
                plan.blocked_now.append(chg)

        return plan

    # --------------------------------------------------------
    # Immediate Apply
    # --------------------------------------------------------

    async def _apply_immediate(
        self,
        main_window,
        changes: List[ConfigChange],
        state: Dict[str, bool],
    ) -> None:
        if not changes:
            return

        touched_sections = {c.section for c in changes}
        comm_keys = {c.key for c in changes if c.section == "communication"}
        ch1_keys = {c.key for c in changes if c.section == "ch1"}
        ch2_keys = {c.key for c in changes if c.section == "ch2"}
        pc_keys = {c.key for c in changes if c.section == "plasma_cleaning"}
        tsp_keys = {c.key for c in changes if c.section == "tsp"}

        # communication
        if comm_keys:
            await self._apply_communication_scope(main_window, comm_keys)

        # chamber scopes
        if ch1_keys:
            await self._apply_chamber_scope(
                main_window=main_window,
                chamber_rt=getattr(main_window, "ch1", None),
                ig=getattr(main_window, "ig1", None),
                mfc=getattr(main_window, "mfc1", None),
                cfgm=config_ch1,
                keys=ch1_keys,
            )

        if ch2_keys:
            await self._apply_chamber_scope(
                main_window=main_window,
                chamber_rt=getattr(main_window, "ch2", None),
                ig=getattr(main_window, "ig2", None),
                mfc=getattr(main_window, "mfc2", None),
                cfgm=config_ch2,
                keys=ch2_keys,
            )

        # plasma cleaning
        if pc_keys:
            await self._apply_pc_scope(main_window, pc_keys)

        # tsp
        if tsp_keys:
            await self._apply_tsp_scope(main_window, tsp_keys)

        # UI 기본값 동기화
        # - running 상태가 아니어야 여기까지 왔으므로 overwrite=True 로 갱신
        if touched_sections & {"tsp", "plasma_cleaning", "ch1", "ch2"}:
            with contextlib.suppress(Exception):
                fn = getattr(main_window, "_apply_ui_defaults_from_config", None)
                if callable(fn):
                    fn(overwrite=True)

        # 로그
        with contextlib.suppress(Exception):
            log = getattr(main_window, "_broadcast_log", None)
            if callable(log):
                joined = ", ".join(sorted(c.dotted for c in changes))
                log("CFG", f"즉시 적용 완료: {joined}")

    async def _apply_communication_scope(
        self,
        main_window,
        keys: Iterable[str],
    ) -> None:
        keys = {str(k).upper() for k in keys}

        # 1) PLC
        if any(k.startswith("PLC_") for k in keys):
            await self._apply_plc(main_window, allow_reconnect=True)

        # 2) HOST SERVER
        if any(k.startswith("HOST_SERVER_") for k in keys):
            await self._apply_host_server(main_window, allow_restart=True)

        # 3) IG common
        if any(k.startswith("IG_") for k in keys):
            await self._reload_ig_pair(main_window, allow_reconnect=True)

        # 4) MFC common
        if any(k.startswith("MFC_") for k in keys):
            await self._reload_mfc_pair(main_window, allow_reconnect=True)

        # 5) common pulse params
        if any(k.startswith("DCPULSE_") or k.startswith("RFPULSE_") for k in keys):
            await self._reload_common_pulses(main_window, allow_reconnect=True)

        # 6) 기타 공용 설정이 영향을 줄 수 있는 runtime들
        if any(
            k.startswith(prefix)
            for prefix in (
                "RGA_",
                "OES_",
                "LOG_",
                "CHAT_",
                "ACS_",
            )
        ):
            with contextlib.suppress(Exception):
                ch1 = getattr(main_window, "ch1", None)
                if ch1 and hasattr(ch1, "reload_runtime_cfg"):
                    ch1.reload_runtime_cfg()

            with contextlib.suppress(Exception):
                ch2 = getattr(main_window, "ch2", None)
                if ch2 and hasattr(ch2, "reload_runtime_cfg"):
                    ch2.reload_runtime_cfg()

            with contextlib.suppress(Exception):
                pc = getattr(main_window, "pc", None)
                if pc and hasattr(pc, "reload_runtime_cfg"):
                    pc.reload_runtime_cfg()

    async def _apply_chamber_scope(
        self,
        main_window,
        chamber_rt,
        ig,
        mfc,
        cfgm,
        keys: Iterable[str],
    ) -> None:
        keys = {str(k).upper() for k in keys}

        # 1) chamber runtime reload
        with contextlib.suppress(Exception):
            if chamber_rt and hasattr(chamber_rt, "reload_runtime_cfg"):
                chamber_rt.reload_runtime_cfg()

        # 2) chamber-local device reload
        if ig is not None:
            with contextlib.suppress(Exception):
                if hasattr(ig, "reload_runtime_cfg"):
                    ig.reload_runtime_cfg()

        if mfc is not None:
            with contextlib.suppress(Exception):
                if hasattr(mfc, "reload_runtime_cfg"):
                    mfc.reload_runtime_cfg()

        for dev_name in ("dc_pulse", "rf_pulse", "dc_power", "rf_power"):
            dev = getattr(chamber_rt, dev_name, None) if chamber_rt else None
            with contextlib.suppress(Exception):
                if dev and hasattr(dev, "reload_runtime_cfg"):
                    dev.reload_runtime_cfg()

        # 3) endpoint reconnect는 이 시점엔 이미 적용 가능 상태로 분류된 경우만 들어옴
        if any(k.startswith("IG_") for k in keys):
            host = getattr(cfgm, "IG_TCP_HOST", getattr(cfgc, "IG_TCP_HOST", None))
            port = getattr(cfgm, "IG_TCP_PORT", None)
            await self._safe_set_endpoint(ig, host, port)

        if any(k.startswith("MFC_") for k in keys):
            host = getattr(cfgm, "MFC_TCP_HOST", getattr(cfgc, "MFC_TCP_HOST", None))
            port = getattr(cfgm, "MFC_TCP_PORT", None)
            await self._safe_set_endpoint(mfc, host, port)

        if any(k.startswith("DCPULSE_") for k in keys):
            dp = getattr(chamber_rt, "dc_pulse", None) if chamber_rt else None
            host = getattr(cfgm, "DCPULSE_TCP_HOST", getattr(cfgc, "DCPULSE_TCP_HOST", None))
            port = getattr(cfgm, "DCPULSE_TCP_PORT", getattr(cfgc, "DCPULSE_TCP_PORT", None))
            await self._safe_set_endpoint(dp, host, port)

        if any(k.startswith("RFPULSE_") for k in keys):
            rp = getattr(chamber_rt, "rf_pulse", None) if chamber_rt else None
            host = getattr(cfgm, "RFPULSE_TCP_HOST", getattr(cfgc, "RFPULSE_TCP_HOST", None))
            port = getattr(cfgm, "RFPULSE_TCP_PORT", getattr(cfgc, "RFPULSE_TCP_PORT", None))
            await self._safe_set_endpoint(rp, host, port)

    async def _apply_pc_scope(
        self,
        main_window,
        keys: Iterable[str],
    ) -> None:
        pc = getattr(main_window, "pc", None)
        with contextlib.suppress(Exception):
            if pc and hasattr(pc, "reload_runtime_cfg"):
                pc.reload_runtime_cfg()

    async def _apply_tsp_scope(
        self,
        main_window,
        keys: Iterable[str],
    ) -> None:
        tsp = getattr(main_window, "tsp_ctrl", None)
        if tsp is None:
            return

        with contextlib.suppress(Exception):
            if hasattr(tsp, "reload_runtime_cfg"):
                tsp.reload_runtime_cfg()

        keys = {str(k).upper() for k in keys}

        if any(k.startswith("TSP_") and self._is_endpoint_key(k) for k in keys):
            host = getattr(cfgc, "TSP_TCP_HOST", None)
            port = getattr(cfgc, "TSP_TCP_PORT", None)

            if hasattr(tsp, "set_endpoint"):
                with contextlib.suppress(Exception):
                    addr = getattr(cfgc, "TSP_RS232_ADDR", getattr(cfgc, "TSP_ADDR", 0x80))
                    ret = tsp.set_endpoint(host, int(port), int(addr))
                    if inspect.isawaitable(ret):
                        await ret

    # --------------------------------------------------------
    # Device Helpers
    # --------------------------------------------------------

    async def _apply_plc(self, main_window, allow_reconnect: bool) -> None:
        plc = getattr(main_window, "plc", None)
        if plc is None or not allow_reconnect:
            return

        with contextlib.suppress(Exception):
            if hasattr(plc, "set_endpoint_reconnect"):
                ret = plc.set_endpoint_reconnect(cfgc.PLC_TCP_HOST, int(cfgc.PLC_TCP_PORT))
                if inspect.isawaitable(ret):
                    await ret
            elif hasattr(plc, "set_endpoint"):
                ret = plc.set_endpoint(cfgc.PLC_TCP_HOST, int(cfgc.PLC_TCP_PORT), reconnect=True)
                if inspect.isawaitable(ret):
                    await ret

    async def _apply_host_server(self, main_window, allow_restart: bool) -> None:
        new_host = str(getattr(cfgc, "HOST_SERVER_HOST", "0.0.0.0"))
        new_port = int(getattr(cfgc, "HOST_SERVER_PORT", 0))
        old_host, old_port = getattr(main_window, "_host_bound", (None, None))

        # ServerPage 표시값은 항상 최신으로
        with contextlib.suppress(Exception):
            sp = getattr(main_window, "server_page", None)
            if sp and hasattr(sp, "set_host_info"):
                sp.set_host_info(new_host, new_port)

        if (new_host, new_port) == (old_host, old_port):
            return

        if not allow_restart:
            return

        with contextlib.suppress(Exception):
            if getattr(main_window, "_host_handle", None):
                ret = main_window._restart_host()
                if inspect.isawaitable(ret):
                    await ret
            else:
                main_window._host_bound = (new_host, new_port)
                log = getattr(main_window, "_broadcast_log", None)
                if callable(log):
                    log("NET", f"Host 설정 반영: {new_host}:{new_port} (현재 host 미실행 상태)")

    async def _reload_ig_pair(self, main_window, allow_reconnect: bool) -> None:
        for ig, cfgm in (
            (getattr(main_window, "ig1", None), config_ch1),
            (getattr(main_window, "ig2", None), config_ch2),
        ):
            if ig is None:
                continue

            with contextlib.suppress(Exception):
                if hasattr(ig, "reload_runtime_cfg"):
                    ig.reload_runtime_cfg()

            if allow_reconnect:
                host = getattr(cfgm, "IG_TCP_HOST", getattr(cfgc, "IG_TCP_HOST", None))
                port = getattr(cfgm, "IG_TCP_PORT", None)
                await self._safe_set_endpoint(ig, host, port)

    async def _reload_mfc_pair(self, main_window, allow_reconnect: bool) -> None:
        for mfc, cfgm in (
            (getattr(main_window, "mfc1", None), config_ch1),
            (getattr(main_window, "mfc2", None), config_ch2),
        ):
            if mfc is None:
                continue

            with contextlib.suppress(Exception):
                if hasattr(mfc, "reload_runtime_cfg"):
                    mfc.reload_runtime_cfg()

            if allow_reconnect:
                host = getattr(cfgm, "MFC_TCP_HOST", getattr(cfgc, "MFC_TCP_HOST", None))
                port = getattr(cfgm, "MFC_TCP_PORT", None)
                await self._safe_set_endpoint(mfc, host, port)

    async def _reload_common_pulses(self, main_window, allow_reconnect: bool) -> None:
        for rt, cfgm in (
            (getattr(main_window, "ch1", None), config_ch1),
            (getattr(main_window, "ch2", None), config_ch2),
        ):
            if rt is None:
                continue

            dp = getattr(rt, "dc_pulse", None)
            rp = getattr(rt, "rf_pulse", None)

            with contextlib.suppress(Exception):
                if dp and hasattr(dp, "reload_runtime_cfg"):
                    dp.reload_runtime_cfg()
            with contextlib.suppress(Exception):
                if rp and hasattr(rp, "reload_runtime_cfg"):
                    rp.reload_runtime_cfg()

            if allow_reconnect:
                if dp is not None:
                    host = getattr(cfgm, "DCPULSE_TCP_HOST", getattr(cfgc, "DCPULSE_TCP_HOST", None))
                    port = getattr(cfgm, "DCPULSE_TCP_PORT", getattr(cfgc, "DCPULSE_TCP_PORT", None))
                    await self._safe_set_endpoint(dp, host, port)

                if rp is not None:
                    host = getattr(cfgm, "RFPULSE_TCP_HOST", getattr(cfgc, "RFPULSE_TCP_HOST", None))
                    port = getattr(cfgm, "RFPULSE_TCP_PORT", getattr(cfgc, "RFPULSE_TCP_PORT", None))
                    await self._safe_set_endpoint(rp, host, port)

    async def _safe_set_endpoint(self, dev, host: Any, port: Any) -> None:
        if dev is None or host is None or port is None:
            return

        with contextlib.suppress(Exception):
            if hasattr(dev, "set_endpoint_reconnect"):
                ret = dev.set_endpoint_reconnect(str(host), int(port))
                if inspect.isawaitable(ret):
                    await ret
                return

        with contextlib.suppress(Exception):
            if hasattr(dev, "set_endpoint"):
                try:
                    ret = dev.set_endpoint(str(host), int(port), reconnect=True)
                except TypeError:
                    ret = dev.set_endpoint(str(host), int(port))
                if inspect.isawaitable(ret):
                    await ret