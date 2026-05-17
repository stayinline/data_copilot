#!/usr/bin/env python
"""S8 — 技术栈总览"""
import sys, pathlib; sys.path.insert(0, str(pathlib.Path(__file__).parent))
from ppt_base import generate
if __name__ == "__main__":
    generate("S8", force="--force" in sys.argv)
