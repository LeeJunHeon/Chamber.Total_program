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

from controller.thickness_service import MAX_FAIL_BEFORE_RECONNECT, ThicknessService  # noqa: E402
from device.thickness_monitor import (ThicknessChecksumError, ThicknessProtocolError, ThicknessTimeout,  # noqa: E402
                                      ThicknessValueUnavailable)
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
        self.t_seq = []                     # read_rate_a_s 결과 차례
        self.xtal = True                    # read_crystal_ok 결과
        self.blank = False                  # True = XTAL FAIL 흉내(S·T·U·V 값 없음)
        self.thk = 1234.0
        self.rate = 12.3
        self.reconnects = 0
        self.started = False
        self.log = None                     # 드라이버 로그 흉내(quiet 이면 생략)
        self.dead = False                   # True = 모든 읽기가 응답 없음

    async def start(self):
        self.started = True

    async def stop(self):
        self.calls.append("stop")

    async def reconnect(self, why=""):
        self.reconnects += 1
        if self.log is not None and not getattr(self, "quiet", False):     # 드라이버 재연결 로그 흉내
            self.log(f"두께 모니터 재연결: {why}")

    async def read_thickness_a(self):
        if self.dead:
            raise ThicknessTimeout("응답 없음")
        self.calls.append("S")
        if self.blank:
            raise ThicknessValueUnavailable("S: 값 없음('    . ')")
        if self.s_seq:
            v = self.s_seq.pop(0)
            if isinstance(v, Exception):
                raise v
            return v
        return self.thk

    async def read_rate_a_s(self):
        if self.dead:
            raise ThicknessTimeout("응답 없음")
        self.calls.append("T")
        if self.blank:
            raise ThicknessValueUnavailable("T: 값 없음('    . ')")
        if self.t_seq:
            v = self.t_seq.pop(0)
            if isinstance(v, Exception):
                raise v
            return v
        return self.rate

    async def read_frequency_hz(self):
        if self.dead:
            raise ThicknessTimeout("응답 없음")
        self.calls.append("U")
        if self.blank:
            raise ThicknessValueUnavailable("U: 값 없음")
        return 5900000

    async def read_crystal_life(self):
        if self.dead:
            raise ThicknessTimeout("응답 없음")
        self.calls.append("V")
        if self.blank:
            raise ThicknessValueUnavailable("V: 값 없음")
        return 12.4

    async def read_crystal_ok(self):
        if self.dead:
            raise ThicknessTimeout("응답 없음")
        self.calls.append("M")
        return self.xtal

    async def read_density(self):
        if self.dead:
            raise ThicknessTimeout("응답 없음")
        return 2.7

    async def read_z_factor(self):
        if self.dead:
            raise ThicknessTimeout("응답 없음")
        return 1.08

    async def read_tooling(self):
        if self.dead:
            raise ThicknessTimeout("응답 없음")
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


def test_value_unavailable_and_format_errors_never_reconnect():
    """프레임은 정상인데 값이 없거나(XTAL FAIL 빈칸) 형식이 틀린 응답 — 재연결 사유가 아니다. link_ok 는 유지."""
    with tempfile.TemporaryDirectory() as tmp:
        async def go():
            clk = Clock()
            dev = FakeDev()
            svc, logs = _svc(tmp, dev, clk, stale_s=10.0)
            await svc.poll_once()
            dev.s_seq = [ThicknessValueUnavailable("S: 값 없음")] * 60
            dev.t_seq = [ThicknessProtocolError("T: 증착률 형식 오류 '12 .4'")] * 60
            for _ in range(60):
                clk.t += 0.5
                await svc.poll_once()
            st = svc.status()
            assert dev.reconnects == 0 and st["link_ok"] and st["stale"]         # 통신 정상 · 두께 값은 오래됨
            assert "값 없음" in st["last_error"] or "형식 오류" in st["last_error"]
            fmt_logs = [m for m in logs if "형식 오류" in m]
            assert 1 <= len(fmt_logs) <= 1                                        # 같은 문구는 60초에 1번(30초 동안 1번)
            assert not any("재연결" in m for m in logs)
            svc.csv.close()
        asyncio.run(go())


@pytest.mark.parametrize("err", [ThicknessTimeout("응답 없음 'S' (3회)"), ThicknessChecksumError("체크섬 불일치")])
def test_comm_and_frame_errors_reconnect_after_max(err):
    """진짜 통신 실패(무응답·체크섬) — 지금처럼 MAX 회 연속이면 재연결. 중간에 정상 프레임(값 없음 포함)이 오면 다시 센다."""
    with tempfile.TemporaryDirectory() as tmp:
        async def go():
            clk = Clock()
            dev = FakeDev()
            svc, logs = _svc(tmp, dev, clk, stale_s=10.0)
            dev.s_seq = [err] * (MAX_FAIL_BEFORE_RECONNECT - 1) + [ThicknessValueUnavailable("-")] + \
                [err] * (MAX_FAIL_BEFORE_RECONNECT - 1)
            for _ in range(2 * MAX_FAIL_BEFORE_RECONNECT - 1):
                await svc.poll_once()
            assert dev.reconnects == 0
            dev.is_connected = True
            dev.s_seq = [err]
            await svc.poll_once()
            assert dev.reconnects == 1 and sum("응답 없음 — 전원" in m for m in logs) == 1
            assert svc.status()["link_ok"]                                       # 조금 전까지 정상 프레임
            clk.t += 11.0                                                        # 정상 프레임 없이 stale_s 지나면 꺼진다
            dev.s_seq = [err]
            dev.is_connected = False
            await svc.poll_once()
            assert not svc.status()["link_ok"]
            svc.csv.close()
        asyncio.run(go())


def test_xtal_fail_blank_replies_keep_link_and_read_crystal_at_once():
    """XTAL FAIL 흉내(S·T·U·V 빈칸, M='!') — 재연결 0회, link_ok, crystal_ok=False 를 곧바로(느린 폴링 주기 기다리지 않고)."""
    with tempfile.TemporaryDirectory() as tmp:
        async def go():
            clk = Clock()
            dev = FakeDev()
            svc, logs = _svc(tmp, dev, clk, stale_s=10.0)
            await svc.poll_once()                                                # 정상 — M 한 번 읽음
            assert svc.status()["crystal_ok"] is True and dev.calls.count("M") == 1
            dev.blank, dev.xtal = True, False
            clk.t += 0.5
            await svc.poll_once()                                                # 값이 막 없어짐 → 이번 폴링에서 M
            st = svc.status()
            assert dev.calls.count("M") == 2 and st["crystal_ok"] is False and st["link_ok"]
            assert st["freq_hz"] is None and st["life_pct"] is None and st["rate_nm_s"] is None
            for _ in range(40):                                                  # 20초 — 느린 폴링(U·V·M)은 계속
                clk.t += 0.5
                await svc.poll_once()
            st = svc.status()
            assert dev.reconnects == 0 and st["link_ok"] and st["stale"] and st["crystal_ok"] is False
            assert dev.calls.count("M") >= 5
            dev.blank, dev.xtal = False, True                                    # 정상 복귀
            clk.t += 5.0
            await svc.poll_once()
            st = svc.status()
            assert not st["stale"] and st["crystal_ok"] is True and st["thickness_nm"] == pytest.approx(123.4)
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
            svc.set_recording(True, "Run A")                             # 공정 중에만 기록
            for _ in range(10):                                          # 0.5초 × 10 = 5초 → 5행
                await svc.poll_once()
                clk.t += 0.5
            svc.csv.close()
            files = list((Path(tmp) / "logs" / "CH2").glob("Thickness_*.csv"))
            assert len(files) == 1
            rows = list(csv.reader(open(files[0], encoding="utf-8-sig")))
            assert rows[0] == COLUMNS and len(rows) == 1 + 5
            assert rows[1][1] == "1" and rows[1][2] == "123.40" and rows[1][7] == "0"
            assert rows[1][8] == "1" and rows[1][9] == "Run A"           # 공정중·공정명
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


def test_xtal_fail_with_fake_stm_server_no_reconnect_then_recover():
    """실제 드라이버 + 가짜 STM: XTAL FAIL(S·T·U·V '    . '/'-------', M='!') → 재연결 0회, link_ok, crystal_ok=False.
    정상 복귀(M='@', 숫자) → 값 표시. 진짜 무응답은 지금처럼 재연결."""
    from tests.test_thickness_monitor import FakeSTM
    from device.thickness_monitor import AsyncThicknessMonitor

    with tempfile.TemporaryDirectory() as tmp:
        async def go():
            srv = FakeSTM()
            await srv.start()
            logs = []
            dev = AsyncThicknessMonitor("127.0.0.1", srv.port, cmd_timeout_s=0.2, backoff_start_s=0.05,
                                        backoff_max_s=0.2, settle_s=0.0, log=logs.append)
            cfg = ThicknessConfig(enabled=True, ch=2, port=srv.port, poll_interval_s=0.1, slow_poll_s=0.3,
                                  stale_s=1.0, log_dir=str(Path(tmp) / "logs"), factor_file=str(Path(tmp) / "f.json"))
            svc = ThicknessService(cfg, log=lambda s, m: logs.append(m), device=dev)
            svc.start()

            async def until(pred, t=5.0):
                for _ in range(int(t / 0.05)):
                    if pred():
                        return True
                    await asyncio.sleep(0.05)
                return False
            try:
                assert await until(lambda: svc.status()["seq"] >= 2 and svc.status()["crystal_ok"] is True)
                for blank in ("    . ", "-------"):
                    srv.blank, srv.xtal = blank, "!"
                    assert await until(lambda: svc.status()["crystal_ok"] is False and svc.status()["stale"])
                    await asyncio.sleep(1.5)                               # 실패가 쌓일 시간(예전엔 ~1초마다 재연결)
                    st = svc.status()
                    assert st["link_ok"] and st["connected"] and st["crystal_ok"] is False and srv.conns == 1
                    assert st["freq_hz"] is None and st["rate_nm_s"] is None
                    srv.blank, srv.xtal = None, "@"
                    assert await until(lambda: not svc.status()["stale"] and svc.status()["crystal_ok"] is True)
                assert srv.conns == 1 and not any("재연결" in m for m in logs)
                assert svc.status()["thickness_nm"] == pytest.approx(123.4)
                for c in ("S", "T", "U", "V", "M", "E?", "F?", "J?", "L"):            # 진짜 무응답(모든 명령) → 재연결
                    srv.script[c] = __import__("collections").deque([("silent",)] * 40)
                assert await until(lambda: srv.conns >= 2, t=10.0)
                assert sum("응답 없음 — 전원" in m for m in logs) == 1
            finally:
                await svc.shutdown(timeout=1.0)
                await srv.close()
        asyncio.run(asyncio.wait_for(go(), 30.0))


def test_unresponsive_10min_logs_once_and_recovery_once():
    """장비 무응답 10분(가상 시계) — 재연결은 지금처럼 계속(실패 6회마다), 로그는 '응답 없음' 1줄. 응답 시작 → '응답 복귀' 1줄."""
    with tempfile.TemporaryDirectory() as tmp:
        async def go():
            clk = Clock()
            dev = FakeDev()
            svc, logs = _svc(tmp, dev, clk)
            dev.log = logs.append
            await svc.poll_once()                                       # 정상
            n0 = len(logs)
            dev.dead = True
            polls = 1200                                                # 0.5초 × 1200 = 10분
            for _ in range(polls):
                clk.t += 0.5
                await svc.poll_once()
            new = logs[n0:]
            assert [m for m in new if "응답 없음" in m] == new and len(new) == 1
            assert new[0].startswith("⚠ 두께 모니터 응답 없음 — 전원·케이블·MOXA 포트 확인 (재연결 계속 시도)")
            assert dev.reconnects >= polls // MAX_FAIL_BEFORE_RECONNECT // 2 and dev.quiet   # 재연결은 계속
            assert not svc.status()["link_ok"]
            dev.dead = False
            clk.t += 0.5
            await svc.poll_once()
            clk.t += 0.5
            await svc.poll_once()
            assert logs[n0 + 1:] == ["두께 모니터 응답 복귀"] and not dev.quiet
            assert svc.status()["link_ok"] and not svc.status()["stale"]
            svc.csv.close()
        asyncio.run(go())


def test_unresponsive_with_fake_tcp_server_driver_logs_quiet():
    """TCP 는 받지만 답이 없는 가짜 서버 + 실제 드라이버 — 재연결은 반복(conns 증가), '재연결'·'연결' 줄은 반복되지 않는다.
    처음부터 무응답이면 첫 연결 1줄 + '응답 없음' 1줄. 응답 시작 → '응답 복귀' 1줄."""
    from tests.test_thickness_monitor import FakeSTM
    from device.thickness_monitor import AsyncThicknessMonitor

    with tempfile.TemporaryDirectory() as tmp:
        async def go():
            srv = FakeSTM()
            srv.mute = True
            await srv.start()
            logs = []
            dev = AsyncThicknessMonitor("127.0.0.1", srv.port, cmd_timeout_s=0.1, retries=0, backoff_start_s=0.05,
                                        backoff_max_s=0.1, settle_s=0.0, log=logs.append)
            cfg = ThicknessConfig(enabled=True, ch=2, port=srv.port, poll_interval_s=0.25, slow_poll_s=0.25,
                                  stale_s=2.0, log_dir=str(Path(tmp) / "logs"), factor_file=str(Path(tmp) / "f.json"))
            svc = ThicknessService(cfg, log=lambda s, m: logs.append(m), device=dev)
            svc.start()

            async def until(pred, t=10.0):
                for _ in range(int(t / 0.05)):
                    if pred():
                        return True
                    await asyncio.sleep(0.05)
                return False
            try:
                assert await until(lambda: srv.conns >= 4)                # 재연결은 계속된다
                assert sum("응답 없음 — 전원" in m for m in logs) == 1
                assert sum("두께 모니터 연결 " in m for m in logs) == 1   # 처음 연결 1줄만
                assert len(logs) == 2, logs                                # 연결 1줄 + 응답 없음 1줄 — 그 밖의 반복 없음
                srv.mute = False
                assert await until(lambda: svc.status()["seq"] >= 2)
                assert sum(m == "두께 모니터 응답 복귀" for m in logs) == 1
            finally:
                await svc.shutdown(timeout=1.0)
                await srv.close()
        asyncio.run(asyncio.wait_for(go(), 30.0))


def test_set_material_log_shows_source():
    """Factor 창 수동 쓰기는 로그에 출처, 공정 쪽 호출(why 없음)은 기존 문구."""
    with tempfile.TemporaryDirectory() as tmp:
        async def go():
            svc, logs = _svc(tmp, FakeDev(), clock=__import__("time").monotonic, poll_interval_s=0.25)
            svc.start()
            await asyncio.sleep(0.1)
            await svc.set_material(4.5, 0.628, 120.0, material="Titanium", why="Factor 창 수동")
            await svc.set_material(2.7, 1.08, 110.0, material="Aluminum")
            assert logs[-2] == "두께 모니터 재료값 적용(Factor 창 수동) Titanium density 4.5 · Z 0.628 · tooling 120.0%"
            assert logs[-1] == "두께 모니터 재료값 적용 Aluminum density 2.7 · Z 1.08 · tooling 110.0%"
            assert svc.status()["material"] == "Aluminum"
            await svc.shutdown(timeout=1.0)
        asyncio.run(go())


def test_csv_only_while_process_recording():
    """공정 없음 → 파일·행 없음(패널 갱신은 계속). 공정 시작 → 1초 1행, 종료 → 멈춤·파일 닫힘, 다음 공정 → 같은 날 같은 파일 이어쓰기."""
    with tempfile.TemporaryDirectory() as tmp:
        async def go():
            clk = Clock()
            svc, _ = _svc(tmp, clock=clk)
            seen = []
            svc.add_listener(seen.append)
            d = Path(tmp) / "logs" / "CH2"

            async def run(n):
                for _ in range(n):
                    await svc.poll_once()
                    clk.t += 0.5
            await run(6)
            assert not svc.recording and len(seen) == 6 and not d.exists()       # 공정 없음 — 파일 없음, 패널은 갱신
            svc.set_recording(True, "Run A")
            await run(6)                                                          # 3초 → 3행
            assert svc.csv.is_open()
            svc.set_recording(False)
            assert not svc.recording and not svc.csv.is_open()                   # 종료 — 파일 닫힘
            await run(6)
            svc.set_recording(True, "Run B (Time 모드)")
            await run(4)                                                          # 2초 → 2행
            svc.set_recording(False)
            files = list(d.glob("Thickness_*.csv"))
            assert len(files) == 1
            rows = list(csv.reader(open(files[0], encoding="utf-8-sig")))
            assert rows[0] == COLUMNS and [r[9] for r in rows[1:]] == ["Run A"] * 3 + ["Run B (Time 모드)"] * 2
            assert len(seen) == 22
        asyncio.run(go())

if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
