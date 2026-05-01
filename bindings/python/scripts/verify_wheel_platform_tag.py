#!/usr/bin/env python3
"""Verify the Linux wheel's manylinux platform tag matches the bundled JRE.

Regression guard for issue #4037: the wheel was tagged ``manylinux_2_35`` while
the bundled JRE only required ``GLIBC_2.34``, blocking installation on systems
running glibc 2.34 (RHEL 9, Rocky 9, AlmaLinux 9, Amazon Linux 2023, ...). The
wheel platform tag must equal the highest ``GLIBC_x.y`` symbol version actually
referenced by the JRE binaries (``java``, ``libjvm.so``, ...). Tagging higher
locks out users unnecessarily; tagging lower lets the wheel install on systems
where the JRE cannot run.

Usage:
    python verify_wheel_platform_tag.py <wheel_path> <jre_dir>
        Verify a wheel's filename platform tag against the JRE in jre_dir.
    python verify_wheel_platform_tag.py --jre <jre_dir>
        Print the recommended manylinux tag for the JRE (no wheel check).

Exits with status 0 on success, non-zero on mismatch.

@author Luca Garulli (l.garulli@arcadedata.com)
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

GLIBC_SYM_RE = re.compile(rb"GLIBC_(\d+)\.(\d+)(?:\.(\d+))?")
WHEEL_TAG_RE = re.compile(r"-(manylinux_(\d+)_(\d+)_(x86_64|aarch64))\.whl$")


def _max_glibc_in_file(path: Path) -> tuple[int, int] | None:
    """Return the highest (major, minor) GLIBC_ symbol referenced by an ELF file."""
    try:
        data = path.read_bytes()
    except OSError:
        return None
    if not data.startswith(b"\x7fELF"):
        return None
    best: tuple[int, int] | None = None
    for match in GLIBC_SYM_RE.finditer(data):
        major = int(match.group(1))
        minor = int(match.group(2))
        if best is None or (major, minor) > best:
            best = (major, minor)
    return best


def max_glibc_in_dir(jre_dir: Path) -> tuple[int, int]:
    """Walk a JRE/JDK tree and return its highest GLIBC_ symbol version."""
    best: tuple[int, int] = (0, 0)
    found_any = False
    for root, _dirs, files in os.walk(jre_dir):
        for name in files:
            p = Path(root) / name
            if p.is_symlink():
                continue
            ver = _max_glibc_in_file(p)
            if ver is None:
                continue
            found_any = True
            if ver > best:
                best = ver
    if not found_any:
        raise SystemExit(f"No ELF binaries with GLIBC_ symbols found under {jre_dir}")
    return best


def parse_wheel_tag(wheel_path: Path) -> tuple[tuple[int, int], str]:
    """Extract (glibc_version, arch) from a manylinux wheel filename."""
    match = WHEEL_TAG_RE.search(wheel_path.name)
    if not match:
        raise SystemExit(
            f"Wheel '{wheel_path.name}' does not have a recognized manylinux_X_Y_arch tag"
        )
    return (int(match.group(2)), int(match.group(3))), match.group(4)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("wheel", nargs="?", type=Path, help="Path to the .whl file")
    parser.add_argument(
        "jre", nargs="?", type=Path, help="Path to the bundled JRE/JDK directory"
    )
    parser.add_argument(
        "--jre",
        dest="jre_only",
        type=Path,
        help="Inspect a JRE only and print its required tag",
    )
    args = parser.parse_args()

    if args.jre_only is not None:
        major, minor = max_glibc_in_dir(args.jre_only)
        print(f"manylinux_{major}_{minor}")
        return 0

    if args.wheel is None or args.jre is None:
        parser.print_usage(sys.stderr)
        return 2

    wheel_glibc, _arch = parse_wheel_tag(args.wheel)
    jre_glibc = max_glibc_in_dir(args.jre)

    print(f"Wheel platform tag glibc: {wheel_glibc[0]}.{wheel_glibc[1]}")
    print(f"JRE max GLIBC_ symbol:    {jre_glibc[0]}.{jre_glibc[1]}")

    if wheel_glibc != jre_glibc:
        print(
            f"ERROR: wheel manylinux tag (manylinux_{wheel_glibc[0]}_{wheel_glibc[1]}) "
            f"does not match the JRE's actual glibc requirement "
            f"(manylinux_{jre_glibc[0]}_{jre_glibc[1]}). "
            "Update WHEEL_PLAT in scripts/Dockerfile.build (issue #4037).",
            file=sys.stderr,
        )
        return 1

    print("OK: wheel platform tag matches the JRE's glibc requirement.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
