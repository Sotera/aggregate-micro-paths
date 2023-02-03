# Test UDF ability to import local modules.
# Solution found using pydoc.importfile.

import sys
from pathlib import Path
import logging

# Pydoc.importfile allows UDF to import local modules w/o
# having to zip & send the whole virtual env.
from pydoc import importfile

config = importfile("config.py")
import config

_logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, stream=sys.stderr)
_logger.info("Files visible to my_udf.py:")
for file in Path().glob("**/*.py"):
    _logger.info(f" - {file}")


_logger.info(f"2: {config.math.cos(30)}.")
print(f"cos(30) = {config.math.cos(30)}.")


_logger.info(":::")
for i, line in enumerate(sys.stdin):
    col1, col2 = line.split("\t")[:2]
    _logger.info(f"{i:2}: - col1: {col1}, col2: {col2}  | ")
