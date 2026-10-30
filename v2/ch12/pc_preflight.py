"""플라즈마 클리닝 프리플라이트 — v1 runtime/plasma_cleaning_runtime.py 의 _preflight_connect(447-578)·GV 인터락 확인(1456-1507).

장비 연결(PC-021): 대상 PLC·MFC(GAS)·MFC(SP4)·IG(같은 MFC 면 두 번 나온다 — v1 그대로). 이미 연결이면 '{이름} 이미 연결됨'.
  PLC 는 read_coil(0) 핸드셰이크 PC_PREFLIGHT_PLC_HANDSHAKE_TIMEOUT_S(1 s), 나머지는 connect()(v1 = 워치독 시작 — v2 는 그 장비 worker 가
  돌고 있는지를 PC_PREFLIGHT_DEVICE_CONNECT_TIMEOUT_S(3 s) 안에 보고 '호출' 로그, 넘으면 '{이름} connect timeout(3.0s)') → 전체 연결 대기 PC_PREFLIGHT_TIMEOUT_S(10 s)·PC_PREFLIGHT_CONNECT_POLL_INTERVAL_S(0.5 s) 간격, 넘으면
  '장비 연결 타임아웃: a, b'. 실패는 RuntimeError('{이름} 연결 실패: {e!r}') — 부르는 쪽이 '장치 연결에 실패했습니다: …'.
MFC 폴링 마스크 분리(PC-023, 529-551): gas MFC ≠ pressure MFC(CH2 PC)이고 gas MFC 를 다른 주체가 안 쓰면(폴링 ∪ 런 사용자) GAS→R60만·
  SP4→R5만, 쓰면 분리 생략 로그.
이벤트 펌프(PC-024): v1 은 여기서 RF·MFC·IG 이벤트 펌프를 띄워 종료 정리(_final_cleanup D)에서 내렸다. v2 는 조립의 이벤트 구독 하나로
  받고 '펌프가 떠 있는 동안'만 처리한다(pumps_on). 라벨은 띄울 때 정한다(같은 MFC 면 'MFC(SP4/GAS-CHn)', 아니면 'MFC(GAS)'·'MFC(SP4-CHn)',
  IG 는 'IG-CHn').
GV 인터락(PC-025·026): PLC read_bit('G_V_{ch}_인터락') 0.6 s — 시간 초과는 'GV 인터락 read timeout(0.6s): 키' 예외. PLC 가 없으면 통과.
"""
from __future__ import annotations

from typing import Optional, Tuple

from v2.common.worker import Status
from v2.ch12.pc_steps import Box, err_of


class PreflightIO:
    @staticmethod
    def _connected(dev) -> bool:
        try:
            fn = getattr(dev, "is_connected", None)
            if callable(fn):
                return bool(fn())
            return bool(getattr(dev, "connected", False))
        except Exception:
            return False

    def preflight_connect(self) -> None:
        to = self._cfgf("PC_PREFLIGHT_TIMEOUT_S", 10.0)
        hs = self._cfgf("PC_PREFLIGHT_PLC_HANDSHAKE_TIMEOUT_S", 1.0)
        poll = self._cfgf("PC_PREFLIGHT_CONNECT_POLL_INTERVAL_S", 0.5)
        self.log("PC", "프리플라이트: 장비 연결 확인/시작")
        need = []
        if self.dev.plc is not None:
            need.append(("PLC", self.dev.plc_drv))
        if self.dev.mfc_gas is not None:
            need.append(("MFC(GAS)", self.dev.mfc_gas))
        if self.dev.mfc_pressure is not None:
            need.append(("MFC(SP4)", self.dev.mfc_pressure))
        if self.dev.ig is not None:
            need.append(("IG", self.dev.ig))
        for name, d in need:
            if self._connected(d):
                self.log("PC", f"{name} 이미 연결됨")
                continue
            if name == "PLC":
                box = Box(self.clock)
                self.dev.plc.read_coil(0, on_done=box.set)
                self.clock.wait_until(self.clock.monotonic() + hs, lambda: box.done)
                if not box.done:
                    raise RuntimeError(f"{name} 연결 실패: {TimeoutError()!r}")
                if box.value.status != Status.OK:
                    raise RuntimeError(f"{name} 연결 실패: {RuntimeError(err_of(box.value))!r}")
                self.log("PC", "PLC 핸드셰이크(read_coil 0)")
            else:
                # v1 connect()(= start — 워치독·명령 워커가 없으면 띄움, 종료 정리 중이면 그 락을 기다림) 를
                # PC_PREFLIGHT_DEVICE_CONNECT_TIMEOUT_S 로 감쌌다(470-485). v2 대응: 그 장비 worker 가 돌고 있는가(멈추는 중·멈춤이면
                # 다시 돌 때까지) — 넘으면 v1 문구 '{이름} connect timeout(3.0s)'. 연결 성립은 아래 전체 대기(10 s)에서.
                dct = self._cfgf("PC_PREFLIGHT_DEVICE_CONNECT_TIMEOUT_S", 3.0)
                w = getattr(d, "worker", None)
                if w is not None and hasattr(w, "running") and not self.clock.wait_until(
                        self.clock.monotonic() + dct, lambda: bool(w.running) or self._connected(d)):
                    raise RuntimeError(f"{name} 연결 실패: {RuntimeError(f'{name} connect timeout({dct:.1f}s)')!r}")
                self.log("PC", f"{name} connect 호출")
        t0 = self.clock.monotonic()
        while True:
            missing = [n for n, d in need if not self._connected(d)]
            if not missing:
                break
            if self.clock.monotonic() - t0 >= to:
                raise RuntimeError(f"장비 연결 타임아웃: {', '.join(missing)}")
            self.clock.wait_until(self.clock.monotonic() + poll)
        g, pr = self.dev.mfc_gas, self.dev.mfc_pressure
        try:
            if g is not None and pr is not None and g is not pr:
                key = g.resource_key
                others = (set(g.poll_users) | set(self.run_state.run_users(key))) - {self.mfc_owner()}
                if others:
                    self.log("PC", f"{key} 다른 공정 사용 중({', '.join(sorted(others))}) → MFC 폴링 마스크 분리 생략")
                else:
                    g.set_poll_mask(gas=True, pressure=False)
                    pr.set_poll_mask(gas=False, pressure=True)
                    self.log("PC", "MFC 폴링 마스크 분리: GAS→R60만, SP4→R5만")
        except Exception:
            pass
        self.pumps_on_now()

    def preflight_gv_interlock(self) -> Tuple[Optional[bool], Optional[str]]:
        """(인터락 값, 읽기 실패 문구). PLC 없으면 (True, None)."""
        if self.dev.plc is None:
            return True, None
        key = f"G_V_{self.ch}_인터락"
        self.log("PLC", "GV 인터락 확인(프리플라이트)")
        v, err, to = self._plc_read_bit(key, 0.6)
        if to:
            return None, f"GV 인터락 read timeout(0.6s): {key}"
        if err is not None:
            return None, err
        return bool(v), None
