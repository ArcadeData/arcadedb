#!/usr/bin/env python3
"""Fails when a source file carries a raw control byte.

A NUL byte reached `ArcadeDBServer.java` as a char literal written out as the byte itself instead of
the escape `\0` (issue #6426). It compiled and behaved correctly, so nothing flagged it for years -
but one control byte makes grep classify the file as binary, and every search over that 1351-line
class silently returned NOTHING. A search for a method that *is* declared there came back empty, so
both people and tools concluded it did not exist. The cost is not the byte, it is that the file
stops answering questions.

Checked bytes are the C0 controls except tab, newline and carriage return, plus DEL. Every one of
them has an escape that reads better anyway, so there is no reason for the raw byte to be in a
source file.

Usage:
  check-source-control-chars.py [path ...]   # defaults to the whole repository

@author Luca Garulli (l.garulli@arcadedata.com)
"""

import sys
from pathlib import Path

# TAB (0x09), LF (0x0a) and CR (0x0d) are the layout of the file, everything else in C0 plus DEL is a control byte.
FORBIDDEN = bytes(b for b in range(0x00, 0x20) if b not in (0x09, 0x0A, 0x0D)) + b"\x7f"

SUFFIXES = {".java", ".py", ".sh", ".js", ".ts", ".json", ".xml", ".yml", ".yaml", ".md", ".sql", ".properties"}

# Directories that hold build output or third-party text nobody edits by hand.
SKIPPED_DIRS = {".git", "target", "node_modules", "build", "dist", ".idea", "venv", ".venv"}

NAMES = {0x00: "NUL", 0x07: "BEL", 0x08: "BS", 0x0B: "VT", 0x0C: "FF", 0x1A: "SUB", 0x1B: "ESC", 0x7F: "DEL"}


def offenders(path):
    """Yields (line, column, byte) for every control byte in the file, 1-based, in file order."""
    try:
        content = path.read_bytes()
    except OSError as e:
        print(f"{path}: cannot read ({e})", file=sys.stderr)
        return

    # The membership test is one pass over the distinct bytes; only a file that has one pays for the locating.
    if not any(b in FORBIDDEN for b in set(content)):
        return

    line = column = 1
    for byte in content:
        if byte in FORBIDDEN:
            yield line, column, byte
        if byte == 0x0A:
            line, column = line + 1, 1
        else:
            column += 1


def walk(root):
    if root.is_file():
        yield root
        return
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.suffix not in SUFFIXES:
            continue
        if SKIPPED_DIRS.intersection(path.parts):
            continue
        yield path


def main(argv):
    roots = [Path(a) for a in argv[1:]] or [Path(__file__).resolve().parents[2]]

    found = 0
    for root in roots:
        for path in walk(root):
            for line, column, byte in offenders(path):
                found += 1
                name = NAMES.get(byte, f"0x{byte:02x}")
                # Print the location the way a compiler does, so an editor can jump straight to it.
                print(f"{path}:{line}:{column}: raw {name} control byte (0x{byte:02x})")

    if found:
        print()
        print(f"{found} raw control byte(s) found in source files.")
        print("Write the escape instead: a raw control byte makes grep treat the whole file as binary, so")
        print("every search over it silently returns nothing (issue #6426).")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
