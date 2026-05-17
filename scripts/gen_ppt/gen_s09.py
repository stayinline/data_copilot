#!/usr/bin/env python
"""S9 — 场景落地矩阵"""
import sys, pathlib; sys.path.insert(0, str(pathlib.Path(__file__).parent))
from ppt_base import generate
if __name__ == "__main__":
    generate("S9", force="--force" in sys.argv)
