#!/usr/bin/env python3
"""
Setup script to copy ArcadeDB JAR files to the Python package.

This script should be run as part of the build process to ensure
all necessary JAR files are included in the wheel.

Environment Variables:
    ARCADEDB_DISTRIBUTION: Distribution type (minimal, headless, full)
                          Default: minimal
"""

import os
import shutil
from pathlib import Path


def find_jar_files():
    """Find all necessary ArcadeDB JAR files based on distribution type."""
    # Get distribution type from environment variable
    distribution = os.environ.get("ARCADEDB_DISTRIBUTION", "minimal").lower()

    if distribution not in ["minimal", "headless", "full"]:
        print(f"⚠️  Warning: Invalid distribution '{distribution}', using 'minimal'")
        distribution = "minimal"

    print(f"📦 Building for distribution: {distribution}")

    # Look for JAR files copied from the ArcadeDB Docker image
    # The Dockerfile copies JARs from the distribution-specific image to these locations
    quick_paths = [
        Path("/build/jars"),  # Docker build location
        Path("/home/arcadedb/lib"),  # Direct from ArcadeDB image
    ]

    found_jars = []

    # Check for jars from Docker image
    for quick in quick_paths:
        if quick.exists():
            jars = list(map(str, quick.glob("*.jar")))
            if jars:
                print("✅ Found {} JAR files in: {}".format(len(jars), quick))
                found_jars.extend(jars)
                break

    # Remove duplicates while preserving order
    seen = set()
    unique_jars = []
    for jar in found_jars:
        jar_name = Path(jar).name
        # Skip test and source JARs to reduce size
        if "-tests.jar" in jar_name or "-sources.jar" in jar_name:
            continue
        if jar not in seen:
            seen.add(jar)
            unique_jars.append(jar)

    return unique_jars


def copy_jars_to_package():
    """Copy JAR files to the Python package."""
    package_dir = Path(__file__).parent
    jar_dir = package_dir / "src" / "arcadedb_embedded" / "jars"

    # Create jars directory if it doesn't exist
    jar_dir.mkdir(parents=True, exist_ok=True)

    # Find all JAR files
    jar_files = find_jar_files()

    if not jar_files:
        print("⚠️  Warning: No JAR files found!")
        print("   This script expects to run inside a Docker build")
        print("   where JARs are copied from the ArcadeDB image.")
        return False

    print(f"📦 Found {len(jar_files)} JAR files")

    # Copy JAR files
    copied_count = 0
    for jar_file in jar_files:
        jar_path = Path(jar_file)
        if jar_path.exists() and jar_path.stat().st_size > 0:
            dest_path = jar_dir / jar_path.name
            shutil.copy2(jar_path, dest_path)
            print(f"   ✅ Copied: {jar_path.name}")
            copied_count += 1
        else:
            print(f"   ⚠️  Skipped (empty or missing): {jar_path.name}")

    print(f"✅ Successfully copied {copied_count} JAR files to {jar_dir}")

    # Calculate total size
    total_size = sum(f.stat().st_size for f in jar_dir.glob("*.jar"))
    print(f"📊 Total package size: {total_size / 1024 / 1024:.1f} MB")

    return copied_count > 0


def main():
    """Main function."""
    distribution = os.environ.get("ARCADEDB_DISTRIBUTION", "minimal").lower()

    print("🎮 ArcadeDB Python Package Setup")
    print("=" * 40)
    print(f"📦 Distribution: {distribution}")
    print()

    if copy_jars_to_package():
        print("\n🎉 Setup completed successfully!")
    else:
        print("\n❌ Setup failed!")
        print("💡 Make sure to run this via build-all.sh:")
        print("   cd bindings/python")
        print("   ./build-all.sh headless")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
