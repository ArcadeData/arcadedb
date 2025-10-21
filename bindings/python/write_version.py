#!/usr/bin/env python3
"""Write version to _version.py file during build"""
import sys
from pathlib import Path

from extract_version import extract_version_from_pom


def write_version_file(ver, out_path):
    """Write version to _version.py"""
    version_file_content = f'''"""Version information for arcadedb_embedded package.
Generated automatically during build from parent pom.xml.
"""

__version__ = "{ver}"
'''

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(version_file_content)

    print(f"✅ Version {ver} written to {out_path}")


if __name__ == "__main__":
    # Get pom.xml path from command line or use default
    script_dir = Path(__file__).parent
    pom_path = script_dir / "../../pom.xml"

    if len(sys.argv) > 1:
        pom_path = Path(sys.argv[1])

    # Extract version from pom.xml
    try:
        version = extract_version_from_pom(pom_path)

        # Write to src/arcadedb_embedded/_version.py
        output_path = script_dir / "src/arcadedb_embedded/_version.py"
        write_version_file(version, output_path)

    except (ValueError, FileNotFoundError, OSError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
