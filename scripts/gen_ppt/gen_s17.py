#!/usr/bin/env python
"""S17 — 过渡页/金句页"""
import sys, pathlib; sys.path.insert(0, str(pathlib.Path(__file__).parent))
from ppt_base import generate
if __name__ == "__main__":
    generate("S17", force="--force" in sys.argv)
