#!/usr/bin/env python3
"""Extract ArcadeDB version from parent pom.xml

ArcadeDB Python Bindings Versioning Strategy
============================================

This script implements the automated versioning system for ArcadeDB Python bindings.
It extracts versions from the main ArcadeDB pom.xml and converts them to PEP 440
compliant Python versions, supporting both development and release workflows.

## Key Principles

1. **Single Source of Truth**: Version is only defined in pom.xml - everything else
   extracts it
2. **PEP 440 Compliance**: All Python versions follow Python packaging standards
3. **Development/Release Distinction**: Different handling for -SNAPSHOT vs release
   versions
4. **Automated Conversion**: No manual version editing required in Python files
5. **Dev Version Tracking**: Automatic incrementing of .devN for repeated SNAPSHOT builds

## Version Conversion Rules

| Maven Version (pom.xml) | Python Version | Use Case |
|-------------------------|----------------|----------|
| 25.10.1-SNAPSHOT (1st)  | 25.10.1.dev0   | First development build |
| 25.10.1-SNAPSHOT (2nd)  | 25.10.1.dev1   | Second development build |
| 25.10.1-SNAPSHOT (3rd)  | 25.10.1.dev2   | Third development build |
| 25.9.1                  | 25.9.1         | Release builds |
| 25.9.1 (--python-patch 1) | 25.9.1.post1 | Python-specific patches |
| 25.9.1 (--python-patch 2) | 25.9.1.post2 | Additional Python patches |

## Development Mode vs Release Mode

**Development Mode** (SNAPSHOT versions):
- Triggered by: -SNAPSHOT suffix in pom.xml
- Conversion: X.Y.Z-SNAPSHOT → X.Y.Z.devN (N auto-increments)
- Purpose: Pre-release development builds
- Example: 25.10.1-SNAPSHOT → 25.10.1.dev0 (first), 25.10.1.dev1 (second), etc.
- Tracking: Dev numbers stored in .dev_version_tracker.json

**Release Mode** (clean versions):
- Triggered by: No -SNAPSHOT suffix in pom.xml
- Conversion: X.Y.Z → X.Y.Z (or X.Y.Z.postN for Python patches)
- Purpose: Official releases to PyPI
- Example: 25.9.1 → 25.9.1 or 25.9.1.post1

## Python-Specific Patches

For Python-only bug fixes that don't require a new ArcadeDB version:

```bash
# Build with Python patch number
python extract_version.py --python-patch 1
# Results in: 25.9.1.post1 (if base ArcadeDB version is 25.9.1)

# Subsequent Python patches
python extract_version.py --python-patch 2
# Results in: 25.9.1.post2
```

## Usage Examples

```bash
# Basic usage (development mode)
python extract_version.py
# Output: 25.10.1.dev0 (from 25.10.1-SNAPSHOT in pom.xml, first build)

# Increment dev version for next build
python extract_version.py --increment-dev
# Output: 25.10.1.dev1 (increments and saves to tracker)

# Basic usage (release mode)
python extract_version.py
# Output: 25.9.1 (from 25.9.1 in pom.xml)

# With Python patch for release
python extract_version.py --python-patch 1
# Output: 25.9.1.post1

# Docker format (raw Maven version)
python extract_version.py --format=docker
# Output: 25.10.1-SNAPSHOT (no conversion)

# Custom pom.xml path
python extract_version.py /path/to/pom.xml --format=pep440
```

## Integration Points

This script is used by:
- build.sh: For building Python packages
- Dockerfile.build: During Docker-based builds
- GitHub Actions: For CI/CD version extraction
- Release scripts: For PyPI publishing

## Error Handling

- Validates pom.xml file exists and is readable
- Ensures version tag is found in expected location
- Validates command-line arguments
- Provides clear error messages for debugging

Usage:
    python extract_version.py [pom_path] [--format=pep440|docker] [--python-patch=N] [--increment-dev]

    --format=pep440  : Convert to PEP 440 for Python packaging (default)
                       25.10.1-SNAPSHOT -> 25.10.1.devN (development, N from tracker)
                       25.9.1 -> 25.9.1 (first release)
    --format=docker  : Raw Maven version for Docker tags
                       25.10.1-SNAPSHOT -> 25.10.1-SNAPSHOT
    --python-patch=N : For released Java versions, add .postN suffix
                       25.9.1 + --python-patch=1 -> 25.9.1.post1
    --increment-dev  : Increment dev version number in tracker for next build
                       (only applies to SNAPSHOT versions)
"""
import json
import re
import sys
from pathlib import Path


def get_dev_version_tracker_path():
    """Get path to dev version tracker file"""
    script_dir = Path(__file__).parent
    return script_dir / ".dev_version_tracker.json"


def load_dev_version_tracker():
    """Load dev version tracker, create if doesn't exist"""
    tracker_path = get_dev_version_tracker_path()

    if not tracker_path.exists():
        # Create default tracker
        default_tracker = {
            "comment": "Tracks development release numbers for SNAPSHOT versions",
            "versions": {},
        }
        save_dev_version_tracker(default_tracker)
        return default_tracker

    with open(tracker_path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_dev_version_tracker(tracker):
    """Save dev version tracker"""
    tracker_path = get_dev_version_tracker_path()
    with open(tracker_path, "w", encoding="utf-8") as f:
        json.dump(tracker, f, indent=2)
        f.write("\n")  # Add trailing newline


def get_dev_version_number(base_version):
    """Get current dev version number for a base version"""
    tracker = load_dev_version_tracker()
    return tracker.get("versions", {}).get(base_version, 0)


def increment_dev_version_number(base_version):
    """Increment and save dev version number for a base version"""
    tracker = load_dev_version_tracker()

    if "versions" not in tracker:
        tracker["versions"] = {}

    current = tracker["versions"].get(base_version, 0)
    new_version = current + 1
    tracker["versions"][base_version] = new_version

    save_dev_version_tracker(tracker)
    return new_version


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


def extract_version_from_pom(
    pom_file, fmt="pep440", patch_version=0, increment_dev=False
):
    """Extract version from Maven pom.xml

    Args:
        pom_file: Path to pom.xml file
        fmt: 'pep440' for Python packaging, 'docker' for Docker tags
        patch_version: For released versions, add .postN suffix (0 = no suffix)
        increment_dev: If True, increment dev version number in tracker
    """
    raw_version = extract_raw_version_from_pom(pom_file)

    if fmt == "docker":
        # Return raw Maven version for Docker tags
        return raw_version

    # Convert Maven version to PEP 440 compatible version
    is_snapshot = "-SNAPSHOT" in raw_version

    if is_snapshot:
        # Development mode: 25.10.1-SNAPSHOT -> 25.10.1.devN
        base_version = raw_version.replace("-SNAPSHOT", "")

        # Get current dev version number from tracker
        if increment_dev:
            dev_number = increment_dev_version_number(base_version)
        else:
            dev_number = get_dev_version_number(base_version)

        return f"{base_version}.dev{dev_number}"
    else:
        # Release mode: 25.9.1 -> 25.9.1 (or 25.9.1.post1 if patch_version > 0)
        base_version = raw_version

        # Handle other pre-release identifiers
        if "-RC" in base_version:
            base_version = base_version.replace("-RC", "rc")
        elif "-" in base_version:
            # Generic replacement for other pre-release identifiers
            base_version = base_version.replace("-", "")

        # Add Python patch version for Python-only releases
        if patch_version > 0:
            return f"{base_version}.post{patch_version}"
        else:
            return base_version


if __name__ == "__main__":
    # Default to parent pom.xml (two levels up from bindings/python)
    script_dir = Path(__file__).parent
    pom_path = script_dir / "../../pom.xml"
    output_format = "pep440"
    python_patch = 0
    increment_dev = False
    tag_version = None

    # Parse arguments
    for arg in sys.argv[1:]:
        if arg.startswith("--format="):
            output_format = arg.split("=", 1)[1]
            if output_format not in ["pep440", "docker"]:
                print(
                    "Error: Invalid format. Use 'pep440' or 'docker'", file=sys.stderr
                )
                sys.exit(1)
        elif arg.startswith("--python-patch="):
            try:
                python_patch = int(arg.split("=", 1)[1])
                if python_patch < 0:
                    raise ValueError("Python patch version must be >= 0")
            except ValueError as e:
                print(f"Error: Invalid python-patch value: {e}", file=sys.stderr)
                sys.exit(1)
        elif arg.startswith("--tag-version="):
            tag_version = arg.split("=", 1)[1]
        elif arg == "--increment-dev":
            increment_dev = True
        elif not arg.startswith("--"):
            pom_path = Path(arg)

    # If tag version is provided, use it directly without any conversion
    if tag_version:
        print(tag_version)
        sys.exit(0)

    try:
        extracted_version = extract_version_from_pom(
            pom_path, output_format, python_patch, increment_dev
        )
        print(extracted_version)
    except (ValueError, FileNotFoundError, OSError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
