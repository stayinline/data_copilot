#!/usr/bin/env python
"""S2 — 当前问题：时间轴 + 数据高墙"""
import sys, pathlib; sys.path.insert(0, str(pathlib.Path(__file__).parent))
from ppt_base import generate
if __name__ == "__main__":
    generate("S2", force="--force" in sys.argv)
