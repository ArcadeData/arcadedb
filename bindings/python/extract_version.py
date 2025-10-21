#!/usr/bin/env python3
"""Extract ArcadeDB version from parent pom.xml

Usage:
    python extract_version.py [pom_path] [--format=pep440|docker]

    --format=pep440  : Convert to PEP 440 for Python packaging (default)
                       25.10.1-SNAPSHOT -> 25.10.1.dev0
    --format=docker  : Raw Maven version for Docker tags
                       25.10.1-SNAPSHOT -> 25.10.1-SNAPSHOT
"""
import re
import sys
from pathlib import Path

# Python-specific patch version (increment for Python-only releases)
PYTHON_PATCH = 0  # Change this to 2, 3, etc. for subsequent Python patches


def extract_raw_version_from_pom(pom_file):
    """Extract the raw version string from pom.xml (no conversion)"""
    with open(pom_file, "r", encoding="utf-8") as f:
        content = f.read()

    # Find the first <version> tag after <artifactId>arcadedb-parent</artifactId>
    pattern = r"<artifactId>arcadedb-parent</artifactId>.*?<version>(.*?)</version>"
    match = re.search(pattern, content, re.DOTALL)

    if match:
        return match.group(1).strip()

    raise ValueError("Could not find version in pom.xml")


def extract_version_from_pom(pom_file, fmt="pep440"):
    """Extract version from Maven pom.xml

    Args:
        pom_file: Path to pom.xml file
        fmt: 'pep440' for Python packaging, 'docker' for Docker tags
    """
    ver = extract_raw_version_from_pom(pom_file)

    if fmt == "docker":
        # Return raw Maven version for Docker tags
        return ver

    # Convert Maven version to PEP 440 compatible version
    # Examples:
    #   25.10.1-SNAPSHOT -> 25.10.1.dev0
    #   25.10.1-RC1 -> 25.10.1rc1
    #   25.10.1 -> 25.10.1

    if "-SNAPSHOT" in ver:
        ver = ver.replace("-SNAPSHOT", ".dev0")
    elif "-RC" in ver:
        ver = ver.replace("-RC", "rc")
    elif "-" in ver:
        # Generic replacement for other pre-release identifiers
        ver = ver.replace("-", "")

    # Add Python patch version for Python-specific releases
    # PYTHON_PATCH = 0 → 25.9.1 (matches Java version)
    # PYTHON_PATCH = 1 → 25.9.1.1 (first Python-only patch)
    # PYTHON_PATCH = 2 → 25.9.1.2 (second Python-only patch)
    # Not applied to dev versions (e.g., 25.9.1.dev0)
    if PYTHON_PATCH > 0 and ".dev" not in ver and "rc" not in ver.lower():
        ver = f"{ver}.{PYTHON_PATCH}"

    return ver


if __name__ == "__main__":
    # Default to parent pom.xml (two levels up from bindings/python)
    script_dir = Path(__file__).parent
    pom_path = script_dir / "../../pom.xml"
    output_format = "pep440"

    # Parse arguments
    for arg in sys.argv[1:]:
        if arg.startswith("--format="):
            output_format = arg.split("=", 1)[1]
            if output_format not in ["pep440", "docker"]:
                print(
                    "Error: Invalid format. Use 'pep440' or 'docker'", file=sys.stderr
                )
                sys.exit(1)
        elif not arg.startswith("--"):
            pom_path = Path(arg)

    try:
        ver = extract_version_from_pom(pom_path, output_format)
        print(ver)
    except (ValueError, FileNotFoundError, OSError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
