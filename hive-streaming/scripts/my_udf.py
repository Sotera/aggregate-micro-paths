#!/usr/bin/env/python
import sys
for line in sys.stdin:
    col1, col2 = line.strip().split("\t")[:2]
    print(f"col1: {col1}, col2: {col2}")
