"""v1 TSP 드라이버(device/tsp.py)를 실제로 실행해 골든을 만든다 → v2/tests/golden/dev_tsp.json.

실행: cd <저장소 루트> && python3 v2/tools/v1golden_tsp.py      (QT_QPA_PLATFORM=offscreen 은 스크립트가 넣는다)

- v1 모듈은 importlib 로 동적으로 부른다(v2 안에서 v1 을 import 문으로 쓰지 않는다 — test_isolation).
- frames : v1 _build_frame(win, write, data, addr) 의 결과 바이트(또는 예외) — 여러 인자 조합.
- parse  : v1 AsyncTSP._parse_response(raw) 의 결과(kind, data 또는 예외).
- cases  : v1 AsyncTSP 를 127.0.0.1 가짜 TCP 서버에 붙여 on()/off()/get_status()/read_win() 을 실제로 부르고,
           서버가 받은 프레임(연결 번호와 함께)과 결과(값·예외 이름)를 적는다. 서버 응답은 경우마다 정한 대본.
v2 시험(test_tsp.py)은 v1 을 import 하지 않고 이 골든만 읽어 같은 인자·같은 대본에서 같은 바이트를 보내는지 본다.
"""
from __future__ import annotations

import asyncio
import importlib
import json
import os
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "v2" / "tests" / "golden" / "dev_tsp.json"


def _load_v1():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    return importlib.import_module("device.tsp")


def _xor(b: bytes) -> int:
    x = 0
    for c in b:
        x ^= c
    return x


def code_frame(code: int, addr: int = 0x80) -> bytes:
    body = bytes([addr, code, 0x03])
    return b"\x02" + body + f"{_xor(body):02X}".encode()


def data_frame(win: str, data: str, addr: int = 0x80, com: str = "0") -> bytes:
    body = bytes([addr]) + win.encode() + com.encode() + data.encode() + b"\x03"
    return b"\x02" + body + f"{_xor(body):02X}".encode()


def _framed(body: bytes) -> bytes:
    return b"\x02" + body + f"{_xor(body):02X}".encode()


def st(n: int) -> bytes:
    return data_frame("205", f"{n:06d}")


def _exc(e: BaseException) -> dict:
    return {"exc": type(e).__name__, "msg": str(e)}


# ───────── 순수 함수 ─────────
def frames(tsp) -> list:
    out = []
    for win in ("011", "205", "000", "999", "01", "0111", "abc", "2O5"):
        for write in (False, True):
            for data in (None, "", "0", "1", "12"):
                for addr in (0x80, 0x81, 0x00, 0xFF, 0x180):
                    args = dict(win=win, write=write, data=data, addr=addr)
                    try:
                        out.append(dict(args, hex=tsp._build_frame(win, write, data, addr=addr).hex()))
                    except Exception as e:
                        out.append(dict(args, **_exc(e)))
    return out


def parse(tsp) -> list:
    dev = tsp.AsyncTSP(host="127.0.0.1", port=1, cfg=types.SimpleNamespace())
    raws = [b"\x06", b"\x15", b"\x32", b"\x33", b"\x34", b"\x35", b"\x07",
            code_frame(0x06), code_frame(0x15), code_frame(0x32), code_frame(0x35), code_frame(0x06, 0x81),
            st(0), st(3), st(5), data_frame("205", "3", com=""), data_frame("205", "13", com=""),
            data_frame("205", "07", com=""), data_frame("011", "1"), data_frame("205", "", com=""),
            _framed(b"\x80\x03"), b"\x02\x80\x06\x03" + b"00",
            b"\x01\x02\x03", b"\x02\x80\x32\x30\x35\x03", b"", _framed(b"\x80\x41\x42\x03")]
    out = []
    for raw in raws:
        try:
            kind, data = dev._parse_response(raw)
            out.append(dict(raw=raw.hex(), kind=kind, data=data))
        except Exception as e:
            out.append(dict(raw=raw.hex(), **_exc(e)))
    return out


# ───────── 가짜 서버 위에서 실제 실행 ─────────
class FakeServer:
    """프레임(ETX + CRC 2바이트로 끝) 하나 받을 때마다 대본에서 응답 하나. None = 무응답, 'close' = 연결 닫기."""

    def __init__(self) -> None:
        self.script: list = []
        self.rx: list = []
        self.conn = 0
        self.server = None
        self.port = 0

    async def start(self) -> None:
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]

    async def _handle(self, reader, writer) -> None:
        self.conn += 1
        n = self.conn
        buf = b""
        try:
            while True:
                chunk = await reader.read(256)
                if not chunk:
                    break
                buf += chunk
                while True:
                    i = buf.find(b"\x03")
                    if i < 0 or len(buf) < i + 3:
                        break
                    frame, buf = buf[:i + 3], buf[i + 3:]
                    self.rx.append({"conn": n, "hex": frame.hex()})
                    r = self.script.pop(0) if self.script else None
                    if r == "close":
                        writer.close()
                        return
                    if r is not None:
                        writer.write(r)
                        await writer.drain()
        except Exception:
            pass
        finally:
            try:
                writer.close()
            except Exception:
                pass


CASES = [
    # (이름, 설정 덮어쓰기, 대본, 동작 목록)
    ("on_ok", {}, [code_frame(0x06), st(3)], [("on", {})]),
    ("on_after_polls", {}, [code_frame(0x06), st(0), st(2), st(5)], [("on", {})]),
    ("off_ok", {}, [code_frame(0x06), st(0)], [("off", {})]),
    ("off_after_polls", {}, [code_frame(0x06), st(5), st(3), st(0)], [("off", {})]),
    ("status_values", {}, [st(4), st(0), data_frame("205", "3", com=""), code_frame(0x06)],
     [("get_status", {}), ("get_status", {}), ("get_status", {}), ("get_status", {})]),
    ("on_short_ack", {}, [b"\x06", st(4)], [("on", {})]),
    ("on_nack_then_reconnect", {}, [code_frame(0x15), st(0)], [("on", {}), ("get_status", {})]),
    ("on_short_nack", {}, [b"\x15", st(0)], [("on", {}), ("get_status", {})]),
    ("window_error", {}, [code_frame(0x32), st(0)], [("get_status", {}), ("get_status", {})]),
    ("crc_bad_then_reconnect", {}, [st(3)[:-2] + b"00", st(3)], [("get_status", {}), ("get_status", {})]),
    ("write_gets_data", {}, [st(3), st(3)], [("off", {}), ("get_status", {})]),
    ("no_verify", {"TSP_VERIFY_WITH_STATUS": False}, [code_frame(0x06), code_frame(0x06)], [("on", {}), ("off", {})]),
    ("addr_0x81", {"TSP_RS232_ADDR": 0x81}, [code_frame(0x06, 0x81), st(3)], [("on", {})]),
    ("legacy_addr_key", {"TSP_RS232_ADDR": None, "TSP_ADDR": 0x82}, [st(0)], [("get_status", {})]),
    ("no_response_then_reconnect", {"TSP_WRITE_TIMEOUT_S": 0.3}, [None, st(0)], [("get_status", {}), ("get_status", {})]),
    ("server_closes_then_reconnect", {}, ["close", st(0)], [("get_status", {}), ("get_status", {})]),
    ("not_stx", {}, [b"\x41", st(0)], [("get_status", {}), ("get_status", {})]),
    ("short_not_tolerated", {"TSP_TOLERATE_SHORT_RESP": False}, [b"\x06", st(0)], [("get_status", {}), ("get_status", {})]),
    ("verify_timeout_short_window", {}, [code_frame(0x06)] + [st(2)] * 400, [("on", {"wait_ok": 0.2})]),
]


async def _run_case(tsp, server: FakeServer, overrides: dict, script: list, ops: list) -> dict:
    server.script = list(script)
    server.rx = []
    server.conn = 0
    cfg = dict(TSP_TCP_HOST="127.0.0.1", TSP_TCP_PORT=server.port, TSP_CONNECT_TIMEOUT_S=2.0, TSP_WRITE_TIMEOUT_S=1.2,
               TSP_POST_SEND_DELAY_MS=20, TSP_VERIFY_WITH_STATUS=True, TSP_TOLERATE_SHORT_RESP=True,
               TSP_TCP_KEEPALIVE=False, TSP_RS232_ADDR=0x80, TSP_ADDR=0x80, TSP_STATUS_POLL_INTERVAL_S=0.05)
    cfg.update(overrides)
    ns = types.SimpleNamespace(**{k: v for k, v in cfg.items() if v is not None})
    dev = tsp.AsyncTSP(host="127.0.0.1", port=server.port, cfg=ns)
    results = []
    for name, kw in ops:
        try:
            v = await getattr(dev, name)(**kw)
            results.append({"op": name, "ok": True, "value": v})
        except Exception as e:
            results.append(dict({"op": name, "ok": False}, **_exc(e)))
        await asyncio.sleep(0.05)
    await dev.aclose()
    await asyncio.sleep(0.05)
    return {"cfg": {k: v for k, v in overrides.items()}, "script": [x.hex() if isinstance(x, bytes) else x for x in script],
            "ops": [{"op": n, "kw": kw} for n, kw in ops], "tx": list(server.rx), "results": results}


async def _cases(tsp) -> list:
    server = FakeServer()
    await server.start()
    out = []
    for name, ov, script, ops in CASES:
        r = await _run_case(tsp, server, ov, script, ops)
        r["name"] = name
        out.append(r)
    server.server.close()
    return out


def main() -> None:
    tsp = _load_v1()
    data = {
        "source": "v1 device/tsp.py 를 실제로 실행(v2/tools/v1golden_tsp.py) — 127.0.0.1 가짜 TCP 서버",
        "v1_files": ["device/tsp.py"],
        "frames": frames(tsp),
        "parse": parse(tsp),
        "cases": asyncio.run(_cases(tsp)),
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(data, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    print(f"{OUT} — frames {len(data['frames'])}, parse {len(data['parse'])}, cases {len(data['cases'])}")


if __name__ == "__main__":
    main()
