# oes_test.py 사용 예시:
#   python oes_test.py 1 30 1000  -> 챔버1(USB0)에서 30초 동안 1000ms 통합시간으로 측정
#   python oes_test.py 2          -> 챔버2(USB1)에서 10초 동안 기본 통합시간(1000ms)으로 측정

import asyncio
import sys
from device.oes import OESAsync

async def _consume_events(oes: OESAsync) -> None:
    async for ev in oes.events():
        if ev.kind == "status" and ev.message:
            print(f"[status] {ev.message}")
        elif ev.kind == "data" and ev.x and ev.y:
            lam, inten = ev.x[0], ev.y[0]
            print(f"[data] λ0={lam:.2f} I0={inten:.1f}")
        elif ev.kind == "finished":
            print("[finished] 측정 완료")
            break

async def test_oes(chamber: int = 1, duration_sec: float = 10.0, integration_ms: int = 1000) -> None:
    oes = OESAsync(chamber=chamber)
    ok = await oes.initialize_device()
    if not ok:
        print(f"초기화 실패: CH{chamber}")
        return
    print(f"초기화 성공: CH{chamber} / USB{oes.sChannel}")
    await asyncio.gather(
        oes.run_measurement(duration_sec, integration_ms),
        _consume_events(oes),
    )

if __name__ == "__main__":
    ch = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    dur = float(sys.argv[2]) if len(sys.argv) > 2 else 10.0
    integ = int(sys.argv[3]) if len(sys.argv) > 3 else 1000
    asyncio.run(test_oes(ch, dur, integ))
