#!/usr/bin/env python
"""S7 — 技术实现：三层引擎架构"""
import sys, pathlib; sys.path.insert(0, str(pathlib.Path(__file__).parent))
from ppt_base import generate
if __name__ == "__main__":
    generate("S7", force="--force" in sys.argv)
