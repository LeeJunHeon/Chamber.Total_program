# -*- coding: utf-8 -*-
"""RGA 워커 JSON 출력은 ASCII 만 — 한글(G: 경로·Windows 오류 문구)은 \\uXXXX 로 내보낸다.

예전: 워커가 stdout 파이프 기본 인코딩(한국어 Windows = cp949)으로 쓰고 메인(device/rga.py)은 UTF-8 로 읽어
'공유 드라이브' 같은 한글이 깨졌다(측정값은 숫자라 영향 없음). 지금은 워커 쪽 인코딩이 무엇이든 메인이 읽은 값이 같다.
장치 접근 없음(측정 함수는 가짜로 바꾼다).
"""
import os
import sys
import subprocess
import textwrap
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from device.rga import RGAWorkerClient     # noqa: E402


def _run(tmp, csv, fb, enc, measure="lambda ip, u, p: ([1.0, 2.0], [0.5, 0.6])"):
    runner = tmp / "run_rga_ascii.py"
    runner.write_text(textwrap.dedent(f'''
        import sys
        sys.path.insert(0, {str(ROOT)!r})
        sys.path.insert(0, {str(ROOT / "apps" / "rga_service")!r})
        import rga_api
        rga_api.rga_measure_once = {measure}
        sys.argv = ["w", "--ch", "1", "--timeout", "5", "--csv", {csv!r}, "--csv_fallback", {fb!r}]
        raise SystemExit(rga_api.main())
    '''), encoding="utf-8")
    env = dict(os.environ, PYTHONIOENCODING=enc)
    out = subprocess.run([sys.executable, str(runner)], capture_output=True, env=env, timeout=60).stdout
    assert out and all(b < 128 for b in out), out[-300:]
    # device/rga.py 와 같은 방식으로 읽는다(UTF-8, errors=replace → 첫 JSON 줄)
    payload = RGAWorkerClient._parse_json_from_stdout(out.decode("utf-8", errors="replace"))
    assert isinstance(payload, dict), out[-300:]
    return payload


@pytest.mark.parametrize("enc", ["cp949", "utf-8"])
def test_korean_csv_path_survives(tmp_path, enc):
    pri = tmp_path / "공유 드라이브" / "RGA" / "Ch.1" / "RGA_spectrums.csv"
    fb = tmp_path / "fb" / "RGA_spectrums.csv"
    p = _run(tmp_path, str(pri), str(fb), enc)
    assert p["ok"] is True and p["csv_ok"] is True and p["csv_fallback_used"] is False
    assert p["csv_path"] == str(pri) and pri.exists()
    assert p["pressures"] == [0.5, 0.6]


def test_korean_csv_error_survives(tmp_path):
    afile = tmp_path / "파일"
    afile.write_text("not a dir", encoding="utf-8")
    bad1, bad2 = str(afile / "공유 드라이브.csv"), str(afile / "폴백.csv")      # 둘 다 반드시 실패
    p = _run(tmp_path, bad1, bad2, "cp949")
    assert p["ok"] is True and p["csv_ok"] is False
    assert f"primary({bad1})" in p["csv_error"] and f"fallback({bad2})" in p["csv_error"]


def test_korean_worker_error_survives(tmp_path):
    p = _run(tmp_path, str(tmp_path / "a.csv"), str(tmp_path / "b.csv"), "cp949",
             measure="lambda ip, u, p: (_ for _ in ()).throw(RuntimeError('RGA 연결 실패: 응답 없음'))")
    assert p["ok"] is False and p["error"] == "RuntimeError: RGA 연결 실패: 응답 없음"
