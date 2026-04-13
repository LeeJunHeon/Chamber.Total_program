# util/inventory_client.py
import asyncio
import contextlib
from typing import Dict

_DEFAULT_URL = "https://vanam.synology.me/api/chamber-slots"
_TIMEOUT_S = 5.0

# Location name 매핑
# "Chamber 1 - Gun 1" → ch=1, gun="G1 Target"
# "Chamber 2 - Gun 3" → ch=2, gun="G3 Target"
def _parse_slot(loc_name: str) -> tuple[int, str] | None:
    """
    "Chamber 1 - Gun 1" → (1, "G1 Target")
    파싱 실패 시 None 반환.
    """
    try:
        # "Chamber 1 - Gun 1" 분리
        left, right = loc_name.split("-")
        # "Chamber 1" → ch_num = 1
        ch_num = int(left.strip().split()[-1])
        # "Gun 1" → gun_num = 1
        gun_num = int(right.strip().split()[-1])
        gun_key = f"G{gun_num} Target"
        return ch_num, gun_key
    except Exception:
        return None


def _fetch_slots_sync(url: str) -> list:
    import urllib.request, json
    req = urllib.request.Request(
        url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=_TIMEOUT_S) as resp:
        return json.loads(resp.read().decode())


async def fetch_gun_targets(ch: int) -> Dict[str, str]:
    """
    챔버 번호(1 or 2)를 받아서 현재 장착된 타겟 이름을 반환.

    반환 예:
      {"G1 Target": "TiN 3inch", "G2 Target": "Pt 3inch", "G3 Target": ""}

    실패 시 빈 딕셔너리 반환 — 메인 공정에 영향 없음.
    """
    try:
        from lib import config_common as _cfgc
        url = getattr(_cfgc, "INVENTORY_API_URL", _DEFAULT_URL)
    except Exception:
        url = _DEFAULT_URL

    result: Dict[str, str] = {
        "G1 Target": "", "G2 Target": "", "G3 Target": ""
    }

    try:
        loop = asyncio.get_event_loop()
        slots = await loop.run_in_executor(None, _fetch_slots_sync, url)
    except Exception:
        return result   # 네트워크 실패 → 빈 딕셔너리, 메인 공정 무영향

    for slot in slots:
        loc_name: str = slot.get("locationName") or ""
        parsed = _parse_slot(loc_name)
        if parsed is None:
            continue
        ch_num, gun_key = parsed
        if ch_num != ch:
            continue
        # itemName 없으면 빈 문자열
        result[gun_key] = slot.get("itemName") or ""

    return result