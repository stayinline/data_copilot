#!/usr/bin/env python
"""S4 — 项目价值：左右对比"""
import sys, pathlib; sys.path.insert(0, str(pathlib.Path(__file__).parent))
from ppt_base import generate
if __name__ == "__main__":
    generate("S4", force="--force" in sys.argv)
