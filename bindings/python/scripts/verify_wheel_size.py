#!/usr/bin/env python3
"""Fail a wheel that is too large for an embedded database, warn when one looks wrong.

The wheels of this project stay under 100 MB each (PyPI's default limit, and the project's own policy even where the
project's limit is higher: an embedded multi-model database should not ship huge wheels). A wheel at or over the limit
means a mistake: a whole distribution copied into JAR_LIB_DIR, an optional jar bundled by accident, a stale artifact
inside the build. The wheels of 26.9.1 are 66 to 71 MB. A wheel far below that probably lacks the engine or the JRE,
which is only a warning here because deliberately minimal wheels are built for A/B tests.

Usage:
    python verify_wheel_size.py WHEEL [WHEEL ...] [--max-mb 100] [--warn-mb 90] [--min-mb 30]

The limits are decimal megabytes (1 MB = 1,000,000 bytes). ARCADEDB_WHEEL_MAX_MB overrides --max-mb for an
exceptional build (state why in the build log; the release workflow has no override).

Exit status: 0 all wheels under the limit (warnings may print), 1 a wheel is at or over the limit, 2 usage error or a
wheel that is missing or not a zip file.
"""

from __future__ import annotations

import argparse
import os
import sys
import zipfile
from pathlib import Path

MB = 1_000_000
DEFAULT_MAX_MB = 100.0  # the policy: a wheel at or over this fails
DEFAULT_WARN_MB = 90.0
DEFAULT_MIN_MB = 30.0


def _largest_members(wheel: Path, count: int = 6) -> list[tuple[str, int]]:
    with zipfile.ZipFile(wheel) as zf:
        members = sorted(zf.infolist(), key=lambda m: m.compress_size, reverse=True)
        return [(m.filename, m.compress_size) for m in members[:count]]


def check(wheel: Path, max_mb: float, warn_mb: float, min_mb: float) -> int:
    """Print one wheel's verdict and return 0 (ok) or 1 (at or over the limit)."""
    size = wheel.stat().st_size
    mb = size / MB
    label = f"{wheel.name}: {size:,} bytes ({mb:.1f} MB)"
    if size >= max_mb * MB:
        print(f"ERROR: {label} is at or over the {max_mb:g} MB limit", file=sys.stderr)
        print("   largest members (compressed):", file=sys.stderr)
        for name, compressed in _largest_members(wheel):
            print(f"     {compressed / MB:8.1f} MB  {name}", file=sys.stderr)
        print(
            "   This is probably a mistake (a whole distribution in JAR_LIB_DIR, an optional jar, "
            "a stale artifact). Do not publish it.",
            file=sys.stderr,
        )
        return 1
    if size >= warn_mb * MB:
        print(
            f"WARNING: {label} is within {max_mb - mb:.1f} MB of the {max_mb:g} MB limit"
        )
    elif size < min_mb * MB:
        print(
            f"WARNING: {label} is below {min_mb:g} MB: is the engine or the JRE missing?"
        )
    else:
        print(f"OK: {label}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("wheels", nargs="+", type=Path)
    parser.add_argument("--max-mb", type=float, default=DEFAULT_MAX_MB)
    parser.add_argument("--warn-mb", type=float, default=DEFAULT_WARN_MB)
    parser.add_argument("--min-mb", type=float, default=DEFAULT_MIN_MB)
    args = parser.parse_args(argv)
    max_mb = float(os.environ.get("ARCADEDB_WHEEL_MAX_MB") or args.max_mb)
    if max_mb != args.max_mb:
        print(f"NOTE: size limit set to {max_mb:g} MB by ARCADEDB_WHEEL_MAX_MB")
    status = 0
    for wheel in args.wheels:
        if not wheel.is_file() or not zipfile.is_zipfile(wheel):
            print(f"ERROR: {wheel} is not a wheel file", file=sys.stderr)
            return 2
        status = max(status, check(wheel, max_mb, args.warn_mb, args.min_mb))
    return status


if __name__ == "__main__":
    sys.exit(main())
