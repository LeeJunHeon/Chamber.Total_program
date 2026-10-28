# -*- coding: utf-8 -*-
"""controller/thickness_service.py · lib/thickness_logger.py — 가짜 장치·가상 시계로 상태·stale·실패 정책·요청 순서·CSV.
pytest / 직접 실행 모두 가능."""
import asyncio
import contextlib
import csv
import datetime
import os
import sys
import tempfile
from pathlib import Path

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                    # noqa: E402

from controller.thickness_service import (MAX_FAIL_BEFORE_RECONNECT, MAX_UNAVAILABLE,  # noqa: E402
                                          ThicknessService)
from device.thickness_monitor import ThicknessTimeout, ThicknessValueUnavailable  # noqa: E402
from lib.thickness_config import ThicknessConfig                 # noqa: E402
from lib.thickness_logger import COLUMNS, ThicknessCsvLogger     # noqa: E402


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


class FakeDev:
    def __init__(self):
        self.is_connected = True
        self.reset_seen = 0
        self.calls = []
        self.s_seq = []                     # read_thickness_a 결과(값 또는 예외) 차례
        self.thk = 1234.0
        self.rate = 12.3
        self.reconnects = 0
        self.started = False

    async def start(self):
        self.started = True

    async def stop(self):
        self.calls.append("stop")

    async def reconnect(self, why=""):
        self.reconnects += 1

    async def read_thickness_a(self):
        self.calls.append("S")
        if self.s_seq:
            v = self.s_seq.pop(0)
            if isinstance(v, Exception):
                raise v
            return v
        return self.thk

    async def read_rate_a_s(self):
        self.calls.append("T")
        return self.rate

    async def read_frequency_hz(self):
        self.calls.append("U")
        return 5900000

    async def read_crystal_life(self):
        self.calls.append("V")
        return 12.4

    async def read_crystal_ok(self):
        self.calls.append("M")
        return True

    async def read_density(self):
        return 2.7

    async def read_z_factor(self):
        return 1.08

    async def read_tooling(self):
        return 100.0

    async def zero(self, mode="B"):
        self.calls.append("zero:" + mode)
        self.thk = 0.0

    async def set_material(self, d, z, t):
        self.calls.append("mat")
        return {"density": d, "z_factor": z, "tooling": t}


def _svc(tmp, dev=None, clock=None, **kw):
    cfg = ThicknessConfig(enabled=True, ch=2, port=4008, log_dir=str(Path(tmp) / "logs"),
                          factor_file=str(Path(tmp) / "f.json"), **kw)
    logs = []
    svc = ThicknessService(cfg, log=lambda s, m: logs.append(m), device=dev or FakeDev(),
                           clock=clock or Clock(), process_info=lambda: (False, ""))
    return svc, logs


def test_status_values_and_units_and_seq():
    with tempfile.TemporaryDirectory() as tmp:
        async def go():
            svc, _ = _svc(tmp)
            seen = []
            svc.add_listener(seen.append)
            await svc.poll_once()
            st = svc.status()
            assert st["connected"] and not st["stale"]
            assert st["thickness_nm"] == pytest.approx(123.4)          # Å → nm 한 곳
            assert st["rate_nm_s"] == pytest.approx(1.23) and "rate_a_s" not in st      # 밖으로는 nm 만
            assert st["freq_hz"] == 5900000 and st["life_pct"] == 12.4 and st["crystal_ok"] is True
            assert (st["density"], st["z_factor"], st["tooling"]) == (2.7, 1.08, 100.0)
            assert st["seq"] == 1 and seen and seen[-1]["seq"] == 1
            await svc.poll_once()                                       # 같은 값이어도 새 S 면 seq +1
            assert svc.status()["seq"] == 2 and svc.status()["thickness_nm"] == pytest.approx(123.4)
            svc.csv.close()
        asyncio.run(go())


def test_slow_poll_interval():
    with tempfile.TemporaryDirectory() as tmp:
        async def go():
            clk = Clock()
            dev = FakeDev()
            svc, _ = _svc(tmp, dev, clk)
            await svc.poll_once()
            assert dev.calls.count("U") == 1
            for _ in range(9):                                          # 0.5초 × 9 = 4.5초 < 5초
                clk.t += 0.5
                await svc.poll_once()
            assert dev.calls.count("U") == 1 and dev.calls.count("S") == 10
            clk.t += 0.5
            await svc.poll_once()
            assert dev.calls.count("U") == 2
            svc.csv.close()
        asyncio.run(go())


def test_stale_is_time_based_and_failures_do_not_disconnect():
    with tempfile.TemporaryDirectory() as tmp:
        async def go():
            clk = Clock()
            dev = FakeDev()
            svc, _ = _svc(tmp, dev, clk, stale_s=10.0)
            await svc.poll_once()
            assert not svc.status()["stale"]
            dev.s_seq = [ThicknessTimeout("x")] * 3
            for _ in range(3):
                clk.t += 3.0
                await svc.poll_once()
            st = svc.status()
            assert st["connected"] and dev.reconnects == 0             # 한두 번 실패로 끊지 않는다
            assert st["age_s"] == pytest.approx(9.0) and not st["stale"]
            assert st["last_error"][9:].startswith("응답 없음")
            clk.t += 1.5
            await svc.poll_once()                                       # 정상 S → 다시 신선
            assert not svc.status()["stale"]
            dev.is_connected = False                                    # 소켓 끊김 — 시간이 지나야 stale
            clk.t += 9.0
            await svc.poll_once()
            assert not svc.status()["connected"] and not svc.status()["stale"]
            clk.t += 2.0
            await svc.poll_once()
            assert svc.status()["stale"]
            svc.csv.close()
        asyncio.run(go())


def test_unavailable_counts_then_reconnect_policy():
    with tempfile.TemporaryDirectory() as tmp:
        async def go():
            dev = FakeDev()
            svc, logs = _svc(tmp, dev)
            dev.s_seq = [ThicknessValueUnavailable("--------")] * MAX_UNAVAILABLE
            for _ in range(MAX_UNAVAILABLE):
                await svc.poll_once()
            assert dev.reconnects == 0 and "값 없음" in svc.status()["last_error"]
            await svc.poll_once()                                       # 정상 → 카운터 초기화
            dev.s_seq = [ThicknessTimeout("x")] * (MAX_FAIL_BEFORE_RECONNECT - 1)
            for _ in range(MAX_FAIL_BEFORE_RECONNECT - 1):
                await svc.poll_once()
            assert dev.reconnects == 0
            dev.s_seq = [ThicknessTimeout("x")]
            await svc.poll_once()
            assert dev.reconnects == 1 and any("재연결" in m for m in logs)
            # '값 없음' 이 MAX_UNAVAILABLE 을 넘으면 실패로 센다
            dev.s_seq = [ThicknessValueUnavailable("-")] * (MAX_UNAVAILABLE + MAX_FAIL_BEFORE_RECONNECT)
            for _ in range(MAX_UNAVAILABLE + MAX_FAIL_BEFORE_RECONNECT):
                await svc.poll_once()
            assert dev.reconnects == 2
            svc.csv.close()
        asyncio.run(go())


def test_listener_exception_is_logged_and_polling_continues():
    with tempfile.TemporaryDirectory() as tmp:
        async def go():
            svc, logs = _svc(tmp)
            got = []

            def bad(_st):
                raise RuntimeError("boom")
            svc.add_listener(bad)
            svc.add_listener(got.append)
            await svc.poll_once()
            await svc.poll_once()
            assert len(got) == 2
            assert sum("리스너 예외" in m for m in logs) == 1           # 같은 문구는 60초에 1번
            svc.csv.close()
        asyncio.run(go())


def test_process_info_exception_is_safe():
    with tempfile.TemporaryDirectory() as tmp:
        async def go():
            svc, _ = _svc(tmp)

            def boom():
                raise RuntimeError("x")
            svc._process_info = boom
            await svc.poll_once()
            assert svc.status()["process_running"] is False
            svc._process_info = lambda: (True, "Run A")
            await svc.poll_once()
            assert svc.status()["process_running"] and svc.status()["process_name"] == "Run A"
            svc.csv.close()
        asyncio.run(go())


def test_requests_run_in_same_loop_before_next_poll():
    with tempfile.TemporaryDirectory() as tmp:
        async def go():
            dev = FakeDev()
            svc, logs = _svc(tmp, dev, clock=__import__("time").monotonic, poll_interval_s=0.25)
            svc.start()
            await asyncio.sleep(0.1)
            ok, why = await svc.zero("B", why="테스트")
            assert ok and why == ""
            i = dev.calls.index("zero:B")
            assert dev.calls[i + 1] == "S"                               # 요청 뒤 곧바로 폴링(섞이지 않음)
            res = await svc.set_material(2.7, 1.08, 100.0)
            assert res["tooling"] == 100.0 and svc.status()["density"] == 2.7
            await asyncio.sleep(0.6)
            assert svc.status()["thickness_nm"] == 0.0
            assert any("영점" in m for m in logs)
            await svc.shutdown(timeout=1.0)
            assert "stop" in dev.calls
            ok, why = await svc.zero("B")                                # 멈춘 뒤 요청은 실패 반환
            assert not ok
        asyncio.run(go())


def test_csv_one_row_per_second_and_day_rollover():
    with tempfile.TemporaryDirectory() as tmp:
        async def go():
            clk = Clock()
            svc, _ = _svc(tmp, clock=clk)
            for _ in range(10):                                          # 0.5초 × 10 = 5초 → 5행
                await svc.poll_once()
                clk.t += 0.5
            svc.csv.close()
            files = list((Path(tmp) / "logs" / "CH2").glob("Thickness_*.csv"))
            assert len(files) == 1
            rows = list(csv.reader(open(files[0], encoding="utf-8-sig")))
            assert rows[0] == COLUMNS and len(rows) == 1 + 5
            assert rows[1][1] == "1" and rows[1][2] == "123.40" and rows[1][7] == "0"
        asyncio.run(go())

        lg = ThicknessCsvLogger(str(Path(tmp) / "roll"), 2)
        st = {"connected": True, "thickness_nm": 1.0, "stale": False}
        lg.write_row(st, datetime.datetime(2026, 10, 27, 23, 59, 59))
        lg.write_row(st, datetime.datetime(2026, 10, 28, 0, 0, 0))
        lg.close()
        lg.write_row(st, datetime.datetime(2026, 10, 28, 0, 0, 1))      # 같은 날 다시 열면 이어 씀
        lg.close()
        d = Path(tmp) / "roll" / "CH2"
        a = list(csv.reader(open(d / "Thickness_20261027.csv", encoding="utf-8-sig")))
        b = list(csv.reader(open(d / "Thickness_20261028.csv", encoding="utf-8-sig")))
        assert len(a) == 2 and len(b) == 3 and b[0] == COLUMNS and b[2][0] == "2026-10-28 00:00:01"


def test_csv_failure_does_not_raise(monkeypatch):
    with tempfile.TemporaryDirectory() as tmp:
        blocker = Path(tmp) / "file"
        blocker.write_text("x")
        warns = []
        import lib.thickness_logger as tl
        monkeypatch.setattr(tl, "_fallback_dir", lambda ch: blocker / "fb")   # 대체 폴더도 못 만든다
        lg = ThicknessCsvLogger(str(blocker), 2, warn=warns.append)           # 파일 밑에는 폴더를 못 만든다
        lg.write_row({"connected": False})
        lg.write_row({"connected": False})
        assert not lg.is_open() and len(warns) == 1


def test_end_to_end_with_fake_stm_server():
    """실제 드라이버 + 서비스 + 가짜 STM-100 서버: 값·리셋 감지·끊김 후 재연결·영점."""
    from tests.test_thickness_monitor import FakeSTM
    from device.thickness_monitor import AsyncThicknessMonitor

    with tempfile.TemporaryDirectory() as tmp:
        async def go():
            srv = FakeSTM()
            await srv.start()
            srv.echo = True
            logs = []
            dev = AsyncThicknessMonitor("127.0.0.1", srv.port, cmd_timeout_s=0.2, backoff_start_s=0.05,
                                        backoff_max_s=0.2, settle_s=0.0, log=logs.append)
            cfg = ThicknessConfig(enabled=True, ch=2, port=srv.port, poll_interval_s=0.25, slow_poll_s=0.25,
                                  stale_s=2.0, log_dir=str(Path(tmp) / "logs"), factor_file=str(Path(tmp) / "f.json"))
            svc = ThicknessService(cfg, log=lambda s, m: logs.append(m), device=dev)
            svc.start()

            async def until(pred, t=5.0):
                for _ in range(int(t / 0.05)):
                    if pred():
                        return True
                    await asyncio.sleep(0.05)
                return False
            try:
                assert await until(lambda: svc.status()["seq"] >= 2)
                st = svc.status()
                assert st["connected"] and not st["stale"] and st["thickness_nm"] == pytest.approx(123.4)
                assert st["crystal_ok"] is True and st["freq_hz"] == 5912345 and st["tooling"] == 100.0
                srv.reset_flag = True
                assert await until(lambda: svc.status()["reset_seen"] == 1)
                assert "L" in srv.rx
                seq0 = svc.status()["seq"]
                srv.drop_all()
                assert await until(lambda: svc.status()["seq"] > seq0 + 2 and srv.conns >= 2)
                ok, _ = await svc.zero("B")
                assert ok and await until(lambda: svc.status()["thickness_nm"] == 0.0)
            finally:
                await svc.shutdown(timeout=1.0)
                await srv.close()
            assert not dev.is_connected
        asyncio.run(asyncio.wait_for(go(), 20.0))


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
