# -*- coding: utf-8 -*-
"""
main_tsp.py — 엔트리포인트(버튼 핸들러/CLI)
- RFC2217 TSP 드라이버 + 공정 러너(process_tsp)를 연결
- IG는 기존 네 AsyncIG를 그대로 사용(아답터로 메서드명만 맞춤)
"""
from __future__ import annotations
import asyncio
from typing import Optional, Any, Callable

from tsp_controller import TSPLetterClientRFC2217, AsyncTSP
from process_tsp import TSPBurstRunner, TSPProcessConfig, IGControllerLike

# ─── IG 어댑터: 기존 AsyncIG 메서드명 다양성 대응 ──────────────────────────
class IGAdapter(IGControllerLike):
    def __init__(self, ig_obj: Any, *, on_names=("ensure_on","on","power_on"),
                 off_names=("ensure_off","off","power_off"),
                 read_names=("read_pressure","read_pressure_torr","get_pressure","read")):
        self.ig = ig_obj
        self._on_names = on_names
        self._off_names = off_names
        self._read_names = read_names

    async def _call_best(self, names: tuple[str, ...]) -> Any:
        for nm in names:
            fn = getattr(self.ig, nm, None)
            if fn is None: 
                continue
            res = fn()
            if asyncio.iscoroutine(res):
                return await res
            return res
        raise AttributeError(f"IG object has none of {names}")

    async def ensure_on(self) -> bool:
        r = await self._call_best(self._on_names)
        return bool(r) if r is not None else True

    async def ensure_off(self) -> bool:
        r = await self._call_best(self._off_names)
        return bool(r) if r is not None else True

    async def read_pressure(self) -> float:
        r = await self._call_best(self._read_names)
        return float(r)

# ─── 앱 래퍼: UI에서 쓰기 쉽게 ────────────────────────────────────────────
class TSPApp:
    def __init__(self, tsp_host: str, tsp_port: int, tsp_baud: int, addr: int, ig_obj: Any,
                 log_cb: Optional[Callable[[str,str],None]] = None,
                 state_cb: Optional[Callable[[str],None]] = None):
        self.client = TSPLetterClientRFC2217(tsp_host, tsp_port, tsp_baud, addr)
        self.client.open()
        self.tsp = AsyncTSP(self.client)
        self.ig = IGAdapter(ig_obj)
        self.runner: Optional[TSPBurstRunner] = None
        self.cancel_event: Optional[asyncio.Event] = None
        self._log_cb = log_cb or (lambda s, m: print(f"[{s}] {m}"))
        self._state_cb = state_cb or (lambda t: print(f"[STATE] {t}"))

    def _log(self, src: str, msg: str) -> None:
        self._log_cb(src, msg)

    def _state(self, text: str) -> None:
        self._state_cb(text)

    async def start_process(self,
                            target_torr: float = 1.0e-6,
                            on_secs: float = 75.0,
                            off_wait_secs: float = 150.0,
                            max_cycles: int = 10,
                            poll_sec: float = 1.0):
        self.runner = TSPBurstRunner(self.tsp, self.ig, log_cb=self._log, state_cb=self._state)
        self.cancel_event = asyncio.Event()
        cfg = TSPProcessConfig(target_pressure_torr=target_torr,
                               tsp_on_seconds=on_secs,
                               off_wait_seconds=off_wait_secs,
                               max_cycles=max_cycles,
                               ig_poll_interval=poll_sec)
        res = await self.runner.run(cfg, cancel_event=self.cancel_event)
        self._log("RESULT", f"ok={res.ok}, cycles={res.cycles_used}, lastP={res.last_pressure_torr}, reason={res.reason}")

    def stop_process(self):
        if self.cancel_event and not self.cancel_event.is_set():
            self.cancel_event.set()

    def close(self):
        try:
            self.client.close()
        except Exception:
            pass

# ─── CLI 단독 실행 (테스트용) ─────────────────────────────────────────────
async def _cli_demo():
    # 실제 환경에 맞게 IG 객체를 전달하세요.
    # 여기서는 더미로 동작 확인만 수행합니다.
    class DummyIG:
        def __init__(self): self.p = 5e-6
        async def ensure_on(self): return True
        async def ensure_off(self): return True
        async def read_pressure(self):
            import random
            self.p *= (0.985 + random.random()*0.02)
            return self.p

    ig = DummyIG()
    app = TSPApp("192.168.1.50", 4001, 9600, 1, ig)
    try:
        await app.start_process(target_torr=1.0e-6)
    finally:
        app.close()

if __name__ == "__main__":
    asyncio.run(_cli_demo())
