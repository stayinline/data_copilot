#!/usr/bin/env python
"""S6 — 创意页：不同物种"""
import sys, pathlib; sys.path.insert(0, str(pathlib.Path(__file__).parent))
from ppt_base import generate
if __name__ == "__main__":
    generate("S6", force="--force" in sys.argv)
