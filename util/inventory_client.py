# util/inventory_client.py
import asyncio
import contextlib
from typing import Dict

_DEFAULT_URL = "https://inventory.vanam.synology.me/api/chamber-slots"
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
    import urllib.request, json, ssl

    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    req = urllib.request.Request(
        url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=_TIMEOUT_S, context=ctx) as resp:
        return json.loads(resp.read().decode())


async def fetch_gun_targets(ch: int) -> Dict[str, str]:
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
        item_name = slot.get("itemName") or ""
        unit_id = slot.get("targetUnitId")
        result[gun_key] = f"{item_name} ({unit_id})" if unit_id is not None else item_name

    return result