# apps/process_app/main_process.py
import sys, asyncio

def run() -> int:
    if sys.platform.startswith("win"):
        from multiprocessing import freeze_support
        freeze_support()
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    from main import main
    return main()

if __name__ == "__main__":
    raise SystemExit(run())
