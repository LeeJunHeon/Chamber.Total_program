# apps/rga_service/main_rga.py
from __future__ import annotations

from rga_service.rga_api import run_rga_api

HOST = "127.0.0.1"
PORT = 9020

def main():
    run_rga_api(host=HOST, port=PORT)

if __name__ == "__main__":
    main()
