"""Run a command for N seconds, then stop it (SIGINT = graceful, SIGKILL = simulate a crash).

    uv run python scripts/run_for.py 120 -- python -m ingestion.run_local run
    uv run python scripts/run_for.py 20 --kill -- python -m ingestion.run_local poller
"""

import argparse
import signal
import subprocess
import sys
import time

ap = argparse.ArgumentParser()
ap.add_argument("seconds", type=float)
ap.add_argument("--kill", action="store_true", help="SIGKILL instead of SIGINT (crash simulation)")
ap.add_argument("cmd", nargs=argparse.REMAINDER)
a = ap.parse_args()
cmd = a.cmd[1:] if a.cmd and a.cmd[0] == "--" else a.cmd
p = subprocess.Popen(cmd)
try:
    p.wait(timeout=a.seconds)
except subprocess.TimeoutExpired:
    p.send_signal(signal.SIGKILL if a.kill else signal.SIGINT)
    print(f"[run_for] sent {'SIGKILL' if a.kill else 'SIGINT'} after {a.seconds}s", file=sys.stderr, flush=True)
    try:
        p.wait(timeout=30)
    except subprocess.TimeoutExpired:
        p.kill()
time.sleep(0.2)
sys.exit(0)
