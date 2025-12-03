#!/usr/bin/env python3
"""
Setup script to copy ArcadeDB JAR files and bundled JRE to the Python package.

This script should be run as part of the build process to ensure
all necessary JAR files and the bundled JRE are included in the wheel.

Note: JAR filtering is done upstream:
  - Docker builds: Filtered in Dockerfile.build (jre-builder stage)
  - Native builds: Filtered in download-jars GitHub Actions job
"""

import shutil
from pathlib import Path


def find_jar_files():
    """Find all ArcadeDB JAR files (already filtered upstream)."""
    print("📦 Building arcadedb-embedded with bundled JRE")

    # Look for JAR files copied from the ArcadeDB Docker image
    # The Dockerfile copies JARs from the full distribution image to these locations
    quick_paths = [
        Path("/build/jars"),  # Docker build location
        Path("/home/arcadedb/lib"),  # Direct from ArcadeDB image
    ]

    all_jars = []

    # Check for jars from Docker image
    for quick in quick_paths:
        if quick.exists():
            jars = list(map(str, quick.glob("*.jar")))
            if jars:
                print(f"✅ Found {len(jars)} JAR files in: {quick}")
                all_jars.extend(jars)
                break

    if all_jars:
        print("\n✅ JAR files (already filtered):")
        for jar in all_jars:
            jar_name = Path(jar).name
            size_mb = Path(jar).stat().st_size / 1024 / 1024
            print(f"   - {jar_name} ({size_mb:.1f}MB)")

    return all_jars


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


def copy_jre():
    """Copy bundled JRE to the Python package."""
    print("\n🔧 Copying bundled JRE...")
    print("=" * 40)

    package_dir = Path(__file__).parent
    jre_source = Path("/build/jre")
    jre_dest = package_dir / "src" / "arcadedb_embedded" / "jre"

    # Check if JRE source exists
    if not jre_source.exists():
        print(f"❌ Error: JRE source directory not found: {jre_source}")
        print("   Expected JRE to be built by jre-builder stage in Docker")
        return False

    # Remove existing JRE directory if it exists
    if jre_dest.exists():
        print(f"🧹 Removing existing JRE directory: {jre_dest}")
        shutil.rmtree(jre_dest)

    # Copy JRE directory
    print(f"📦 Copying JRE from {jre_source} to {jre_dest}...")
    shutil.copytree(jre_source, jre_dest, symlinks=True)

    # Calculate JRE size
    total_size = sum(f.stat().st_size for f in jre_dest.rglob("*") if f.is_file())
    file_count = sum(1 for f in jre_dest.rglob("*") if f.is_file())

    print("✅ Successfully copied JRE")
    print(f"📊 JRE size: {total_size / 1024 / 1024:.1f} MB")
    print(f"📊 Files: {file_count}")

    # Verify java executable exists
    java_exe = jre_dest / "bin" / "java"
    if java_exe.exists():
        print(f"✅ Java executable found: {java_exe}")
    else:
        print(f"⚠️  Warning: Java executable not found at {java_exe}")

    return True


def main():
    """Main function."""
    print("🎮 ArcadeDB Python Package Setup")
    print("=" * 40)
    print("📦 Package: arcadedb-embedded (with bundled JRE)")
    print()

    # Copy JAR files
    if not copy_jars_to_package():
        print("\n❌ Setup failed!")
        print("💡 Make sure to run this via build.sh:")
        print("   cd bindings/python")
        print("   ./build.sh")
        return 1

    # Copy bundled JRE (always included)
    if not copy_jre():
        print("\n❌ JRE copy failed!")
        return 1

    print("\n🎉 Setup completed successfully!")
    return 0


if __name__ == "__main__":
    exit(main())
