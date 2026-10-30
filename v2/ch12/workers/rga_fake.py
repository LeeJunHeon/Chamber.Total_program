r"""가짜 RGA100(시험용, rga_worker --fake) — srsinst.rga.RGA100 과 같은 호출 모양.

호출마다 RGA_FAKE_LOG 파일(있으면)에 JSON 한 줄 {"call": 이름, "args": [...]} (비밀번호는 '***') → 시험이 순서를 확인.
동작은 환경변수 RGA_FAKE="키=값,..":
  masses=N        질량 칸 수(기본 65)
  fail=connect|scan  그 단계에서 예외
  hang=1          get_histogram_scan 에서 멈춤(메인 시간 초과 → kill)
  die=1           get_histogram_scan 에서 프로세스가 죽음(os._exit(9))
"""
from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, List


def _opts() -> Dict[str, str]:
    out: Dict[str, str] = {}
    for part in (os.environ.get("RGA_FAKE", "") or "").split(","):
        if "=" in part:
            k, v = part.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def _log(call: str, args: List[Any]) -> None:
    path = os.environ.get("RGA_FAKE_LOG")
    if path:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps({"call": call, "args": args}, ensure_ascii=False) + "\n")


class _Filament:
    def turn_on(self) -> None:
        _log("filament.turn_on", [])

    def turn_off(self) -> None:
        _log("filament.turn_off", [])


class _Scan:
    def __init__(self, n: int, opts: Dict[str, str]) -> None:
        self.n, self.opts = n, opts

    def get_histogram_scan(self):
        _log("scan.get_histogram_scan", [])
        if self.opts.get("hang") == "1":
            time.sleep(3600)
        if self.opts.get("die") == "1":
            os._exit(9)
        if self.opts.get("fail") == "scan":
            raise TimeoutError("fake scan timeout")
        return [100 + i for i in range(self.n)]

    def get_partial_pressure_corrected_spectrum(self, histogram):
        _log("scan.get_partial_pressure_corrected_spectrum", [len(histogram)])
        return [1.0e-9 * (1 + (v % 10)) for v in histogram]

    def get_mass_axis(self, for_analog_scan=True):
        _log("scan.get_mass_axis", [{"for_analog_scan": for_analog_scan}])
        return [float(i + 1) for i in range(self.n)]


class FakeRGA100:
    def __init__(self, interface: str, ip: str, user: str, password: str) -> None:
        _log("RGA100", [interface, ip, user, "***" if password else ""])
        opts = _opts()
        if opts.get("fail") == "connect":
            raise ConnectionRefusedError(f"fake connect refused {ip}")
        self.filament = _Filament()
        self.scan = _Scan(int(opts.get("masses", "65")), opts)

    def disconnect(self) -> None:
        _log("disconnect", [])
