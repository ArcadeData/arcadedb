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
import glob
from pathlib import Path


def find_jar_files():
    """Find all necessary ArcadeDB JAR files based on distribution type."""
    # Get distribution type from environment variable
    distribution = os.environ.get("ARCADEDB_DISTRIBUTION", "minimal").lower()
    
    if distribution not in ["minimal", "headless", "full"]:
        print(f"⚠️  Warning: Invalid distribution '{distribution}', using 'minimal'")
        distribution = "minimal"
    
    print(f"📦 Building for distribution: {distribution}")
    
    # Look for JAR files in the Maven target directories
    # Try different locations based on environment
    possible_roots = [
        Path(__file__).parent.parent.parent,  # Normal case: bindings/python/setup_jars.py
        Path("/arcadedb"),  # Docker case
        Path.cwd().parent.parent,  # Alternative
    ]
    
    found_jars = []
    repo_root = None
    
    for root in possible_roots:
        if not root.exists():
            continue
            
        print(f"🔍 Searching for JARs in: {root}")
        
        # First, try to find the packaged distribution with all dependencies
        # Build pattern based on distribution type
        if distribution == "full":
            # Full distribution has no suffix
            package_lib_patterns = [
                "package/target/arcadedb-*.dir/arcadedb-*/lib/*.jar",
            ]
        else:
            # Minimal and headless have suffixes
            package_lib_patterns = [
                f"package/target/arcadedb-*-{distribution}.dir/arcadedb-*/lib/*.jar",
            ]
        
        for pattern in package_lib_patterns:
            jars = glob.glob(str(root / pattern))
            if jars:
                print(f"✅ Found {len(jars)} JAR files in {distribution} distribution")
                found_jars.extend(jars)
                repo_root = root
                break
        
        if found_jars:
            break
        
        # Fallback: look for individual module JARs (without dependencies)
        print(f"   Package distribution not found, trying individual modules...")
        jar_patterns = [
            "engine/target/arcadedb-engine-*.jar",
            "network/target/arcadedb-network-*.jar",
            "server/target/arcadedb-server-*.jar",
        ]
        
        for pattern in jar_patterns:
            jars = glob.glob(str(root / pattern))
            found_jars.extend(jars)
        
        if found_jars:
            print(f"✅ Found {len(found_jars)} module JAR files (warning: dependencies not included)")
            repo_root = root
            break
    
    # Remove duplicates while preserving order
    seen = set()
    unique_jars = []
    for jar in found_jars:
        jar_name = Path(jar).name
        # Skip test and source JARs to reduce size
        if '-tests.jar' in jar_name or '-sources.jar' in jar_name:
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
        print("   Make sure you've built ArcadeDB with Maven first:")
        print("   mvn clean package -DskipTests")
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
        print("💡 You can now build and install the package:")
        print("   cd bindings/python")
        print("   pip install -e .")
    else:
        print("\n❌ Setup failed!")
        print("💡 Make sure to build ArcadeDB first:")
        print("   mvn clean package -DskipTests")
        print()
        print("💡 Or build specific distribution:")
        print("   mvn clean package -DskipTests -P minimal")
        print("   mvn clean package -DskipTests -P headless")
        print("   mvn clean package -DskipTests -P full")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())