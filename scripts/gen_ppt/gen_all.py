#!/usr/bin/env python
"""Run all PPT image generators sequentially."""
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
SCRIPTS = sorted(SCRIPT_DIR.glob("gen_s*.py"))

force = "--force" in sys.argv

print(f"Running {len(SCRIPTS)} generators...\n")

for script in SCRIPTS:
    sid = script.stem.replace("gen_", "").upper()
    args = ["python", str(script)]
    if force:
        args.append("--force")
    print(f"--- {sid} ---")
    result = subprocess.run(args, cwd=str(script.parent.parent.parent))
    if result.returncode != 0:
        print(f"  FAILED: {sid}")
    print()

print("Done.")
