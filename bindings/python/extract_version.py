#!/usr/bin/env python3
"""Extract ArcadeDB version from parent pom.xml"""
import re
import sys
from pathlib import Path

def extract_version_from_pom(pom_path):
    """Extract version from Maven pom.xml"""
    with open(pom_path, 'r') as f:
        content = f.read()
    
    # Find the first <version> tag after <artifactId>arcadedb-parent</artifactId>
    pattern = r'<artifactId>arcadedb-parent</artifactId>.*?<version>(.*?)</version>'
    match = re.search(pattern, content, re.DOTALL)
    
    if match:
        version = match.group(1).strip()
        # Remove -SNAPSHOT suffix for release builds
        if '-SNAPSHOT' in version:
            version = version.replace('-SNAPSHOT', '')
        return version
    
    raise ValueError("Could not find version in pom.xml")

if __name__ == "__main__":
    # Default to parent pom.xml (two levels up from bindings/python)
    script_dir = Path(__file__).parent
    pom_path = script_dir / "../../pom.xml"
    
    if len(sys.argv) > 1:
        pom_path = Path(sys.argv[1])
    
    try:
        version = extract_version_from_pom(pom_path)
        print(version)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
