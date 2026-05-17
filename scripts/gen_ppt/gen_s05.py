#!/usr/bin/env python
"""S5 — 提效对比：能力覆盖+速度碾压"""
import sys, pathlib; sys.path.insert(0, str(pathlib.Path(__file__).parent))
from ppt_base import generate
if __name__ == "__main__":
    generate("S5", force="--force" in sys.argv)
