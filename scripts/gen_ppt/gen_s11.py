#!/usr/bin/env python
"""S11 — 路线图"""
import sys, pathlib; sys.path.insert(0, str(pathlib.Path(__file__).parent))
from ppt_base import generate
if __name__ == "__main__":
    generate("S11", force="--force" in sys.argv)
