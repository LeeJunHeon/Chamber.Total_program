# lib/thickness_config.py
# -*- coding: utf-8 -*-
"""두께 모니터(STM-100) 설정 — config 모듈(config_ch1/config_ch2)에서 '호출 시점에' 읽어 검증·클램프한다.

heater_config 와 같은 원칙: 예외를 던지지 않고 경고 문구를 돌려준 뒤 안전한 쪽으로 고친다.
값은 config/settings.json 의 ch1/ch2 섹션에 둔다(common 에만 두면 채널 모듈로 넘어가지 않는다).
"""
from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, List, Tuple

DEFAULT_LOG_DIR = r"C:\VanaM_Logs\CH1&2\Thickness"


def base_dir() -> Path:
    """settings.json 과 같은 기준 — exe 옆(배포) / 프로젝트 루트(소스 실행)."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[1]


def default_factor_file(ch: int) -> Path:
    return base_dir() / "config" / f"thickness_factors_ch{int(ch)}.json"


@dataclass(frozen=True)
class ThicknessConfig:
    """실제 운전값은 config/settings.json ch1/ch2 와 lib/config_common.py 에서 온다."""
    enabled: bool = False
    ch: int = 2
    host: str = "192.168.1.50"
    port: int = 0
    poll_interval_s: float = 0.5         # S·T
    slow_poll_s: float = 5.0             # U·V·M
    stale_s: float = 10.0                # 마지막 정상 S 뒤 이 시간을 넘으면 stale
    cmd_timeout_s: float = 1.0           # 명령 1회 응답 대기
    log_dir: str = DEFAULT_LOG_DIR       # 연속 기록 CSV 뿌리(그 아래 CH{n}\Thickness_YYYYMMDD.csv)
    factor_file: str = ""                # tooling·재료값 JSON(절대 경로로 정규화)

    # ── 공정 연동(2단계) 판정 상수 자리 ──
    # Thickness 모드 종료 판정(영점 확인·계단 튐·시간 대체·최대 시간 상한)의 상수는 2단계에서
    # 증착률 배수·시간 기준의 이름 붙은 값으로 여기에 추가하고 settings 로 덮어쓸 수 있게 한다.


def load_thickness_config(cfg_module: Any, ch: int) -> Tuple[ThicknessConfig, List[str]]:
    """config 모듈에서 SUPPORTS_THICKNESS / THICKNESS_* 를 읽어 검증한다. (ThicknessConfig, 경고 목록) — 예외를 던지지 않는다."""
    warns: List[str] = []
    d = ThicknessConfig()

    def _get(key: str, default, cast):
        try:
            v = getattr(cfg_module, key, default)
            if cast is bool:
                if isinstance(v, str):
                    return v.strip().lower() in ("1", "true", "yes", "on")
                return bool(v)
            return cast(v)
        except Exception:
            warns.append(f"{key}={getattr(cfg_module, key, None)!r} 형식 오류 → 기본값 {default!r}")
            return default

    def _clamp(name: str, v: float, lo: float, hi: float) -> float:
        if v < lo or v > hi:
            nv = min(max(v, lo), hi)
            warns.append(f"{name}={v:g} 범위({lo:g}~{hi:g}) 밖 → {nv:g}")
            return nv
        return v

    enabled = _get("SUPPORTS_THICKNESS", False, bool)
    host = str(_get("THICKNESS_TCP_HOST", d.host, str) or "").strip()
    port = int(_get("THICKNESS_TCP_PORT", d.port, float))
    if enabled and (not host or not (1 <= port <= 65535)):
        warns.append(f"THICKNESS_TCP_HOST/PORT={host!r}:{port} 가 올바르지 않음 → 두께 모니터 끔")
        enabled = False

    poll = _clamp("THICKNESS_POLL_INTERVAL_S", _get("THICKNESS_POLL_INTERVAL_S", d.poll_interval_s, float), 0.25, 5.0)
    slow = _clamp("THICKNESS_SLOW_POLL_S", _get("THICKNESS_SLOW_POLL_S", d.slow_poll_s, float), poll, 60.0)
    tmo = _clamp("THICKNESS_CMD_TIMEOUT_S", _get("THICKNESS_CMD_TIMEOUT_S", d.cmd_timeout_s, float), 0.3, 5.0)
    stale = _get("THICKNESS_STALE_S", d.stale_s, float)
    stale_min = max(2.0, 4.0 * poll)
    if stale < stale_min:
        warns.append(f"THICKNESS_STALE_S={stale:g} < {stale_min:g}(폴링 4회) → {stale_min:g}")
        stale = stale_min

    log_dir = str(_get("THICKNESS_LOG_DIR", d.log_dir, str) or "").strip() or DEFAULT_LOG_DIR
    if log_dir[:2].upper() == "G:":
        # G: 구글드라이브에 새 파일을 만들면 조용히 사라지는 문제가 있었다 — 로컬에 쓰고 log_sync 가 NAS 로 복사한다
        warns.append(f"THICKNESS_LOG_DIR={log_dir!r} — G: 드라이브는 쓰지 않음 → {DEFAULT_LOG_DIR}")
        log_dir = DEFAULT_LOG_DIR

    ff = str(_get("THICKNESS_FACTOR_FILE", "", str) or "").strip()
    if not ff:
        factor = default_factor_file(ch)
    else:
        p = Path(ff)
        factor = p if p.is_absolute() else base_dir() / p

    cfg = ThicknessConfig(
        enabled=bool(enabled), ch=int(ch), host=host or d.host, port=int(port),
        poll_interval_s=float(poll), slow_poll_s=float(slow), stale_s=float(stale), cmd_timeout_s=float(tmo),
        log_dir=log_dir, factor_file=str(factor),
    )
    return cfg, warns
