"""v1 전수 목록(사용자 지시 0단계) — v1 의 기능·화면 동작·로봇 명령·설정 키·오류 코드·기록 출력·구글챗 카드·
타이머·인터락·장비 명령·워커 앱·기존 시험이 v2 어디에 있는지.

영역별 표는 inv_<영역>.py 의 ITEMS(이 모듈이 모은다). 항목 = dict
  id      : 영역 접두어-번호(예 "UI-012") — 전체에서 하나
  area    : 영역(ui·proc·pc·dev·robot·cfg·err·heat·thick·log·chat·app·test·gap)
  desc    : 무엇(한국어 한 줄~두 줄)
  v1      : v1 위치 "파일:줄"(여러 곳이면 ; 로)
  v2      : v2 담당 모듈 경로(v2/... .py) — 계획 단계에서 미리 적고, 구현하면 실제 경로로
  test    : v2 시험 "v2/tests/test_x.py::test_y"(done 이면 필수)
  status  : todo | done | na
  note    : na 의 이유, 사용자 확인 필요 등
test_inventory_complete 는 모든 항목이 done(v2 파일이 있고 시험 함수가 있음) 또는 na(이유 있음)일 때만 통과한다.
"""
from __future__ import annotations

import importlib
import pkgutil
from typing import Dict, List

from v2.ch12 import spec as _spec

FIELDS = ("id", "area", "desc", "v1", "v2", "test", "status", "note")
STATUSES = ("todo", "done", "na")


def all_items() -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for m in sorted(pkgutil.iter_modules(_spec.__path__), key=lambda m: m.name):
        if m.name.startswith("inv_"):
            mod = importlib.import_module(f"v2.ch12.spec.{m.name}")
            for it in mod.ITEMS:
                d = {k: str(it.get(k, "")) for k in FIELDS}
                out.append(d)
    return out


def summary() -> Dict[str, int]:
    c: Dict[str, int] = {s: 0 for s in STATUSES}
    for it in all_items():
        c[it["status"]] = c.get(it["status"], 0) + 1
    return c
