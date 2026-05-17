#!/usr/bin/env python
"""S10 — 扩展性：多业务赋能"""
import sys, pathlib; sys.path.insert(0, str(pathlib.Path(__file__).parent))
from ppt_base import generate
if __name__ == "__main__":
    generate("S10", force="--force" in sys.argv)
