# main_tsp.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import asyncio, argparse, sys
from datetime import datetime

# 장비
from device.ig import AsyncIG
from device.tsp import AsyncTSP
from controller.tsp_controller import TSPProcessController, TSPRunConfig

# CH1 기본(필요시 인자에서 변경)
DEFAULT_HOST = "192.168.1.50"
DEFAULT_IG_PORT  = 4001   # CH1 IG (MOXA 1번 포트)
DEFAULT_TSP_PORT = 4004   # TSP (네가 1:1 테스트했던 포트)

def _ts():
    return datetime.now().strftime("%H:%M:%S")

def make_logger(prefix: str):
    def _log(msg: str):
        print(f"[{_ts()}] {prefix} {msg}")
    return _log

async def amain(args):
    # 장비 인스턴스
    ig  = AsyncIG(host=args.host, port=args.ig_port)
    tsp = AsyncTSP(host=args.host, port=args.tsp_port)

    # 콜백(콘솔 프린트)
    def log_cb(msg: str): print(f"[{_ts()}] {msg}")
    def state_cb(s: str): print(f"[{_ts()}] [STATE] {s}")
    def pressure_cb(p: float): print(f"[{_ts()}] [IG] P={p:.3e}")
    def cycle_cb(cur: int, total: int): print(f"[{_ts()}] [TSP] cycle {cur}/{total} 완료")

    ctrl = TSPProcessController(
        tsp=tsp, ig=ig,
        log_cb=log_cb, state_cb=state_cb,
        pressure_cb=pressure_cb, cycle_cb=cycle_cb,
        turn_off_ig_on_finish=True,
    )

    cfg = TSPRunConfig(
        target_pressure=args.target,
        cycles=args.cycles,
        dwell_sec=args.dwell_sec,
        poll_sec=args.poll_sec,
        verify_with_status=True,  # TSP on/off 시 205 상태 확인(원치 않으면 False)
    )

    print(f"=== TSP 공정 시작 === host={args.host} ig={args.ig_port} tsp={args.tsp_port} "
          f"target={args.target} cycles={args.cycles} dwell={args.dwell_sec}s poll={args.poll_sec}s")

    result = await ctrl.run(cfg)

    print("\n=== 결과 ===")
    print(f" success       : {result.success}")
    print(f" cycles_done   : {result.cycles_done}")
    print(f" final_pressure: {result.final_pressure:.3e}" if result.final_pressure==result.final_pressure else " final_pressure: NaN")
    print(f" reason        : {result.reason}")

def main(argv=None):
    p = argparse.ArgumentParser(description="CH1 IG + TSP 공정 실행기")
    p.add_argument("--host", default=DEFAULT_HOST)
    p.add_argument("--ig-port", type=int, default=DEFAULT_IG_PORT)
    p.add_argument("--tsp-port", type=int, default=DEFAULT_TSP_PORT)
    p.add_argument("--target", type=float, required=True, help="목표 압력(이하 도달 시 종료)")
    p.add_argument("--cycles", type=int, required=True, help="TSP ON/OFF 반복 횟수")
    p.add_argument("--dwell-sec", type=float, default=150.0, help="ON/OFF 사이 대기(초)")
    p.add_argument("--poll-sec", type=float, default=5.0, help="IG 압력 폴링 간격(초)")
    args = p.parse_args(argv)

    try:
        asyncio.run(amain(args))
    except KeyboardInterrupt:
        print("\n[사용자 중단] KeyboardInterrupt")
        return 2
    return 0

if __name__ == "__main__":
    sys.exit(main())
