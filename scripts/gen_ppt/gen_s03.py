#!/usr/bin/env python
"""S3 — 痛点矩阵：七种绝望"""
import sys, pathlib; sys.path.insert(0, str(pathlib.Path(__file__).parent))
from ppt_base import generate
if __name__ == "__main__":
    generate("S3", force="--force" in sys.argv)
