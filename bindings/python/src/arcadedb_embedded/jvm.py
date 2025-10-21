"""
ArcadeDB Python Bindings - JVM Management

Handles JVM initialization and JAR file management.
"""

import glob
import os
from pathlib import Path

import jpype
import jpype.imports

from .exceptions import ArcadeDBError


def get_jar_path() -> str:
    """Get the path to bundled JAR files."""
    package_dir = Path(__file__).parent
    jar_dir = package_dir / "jars"
    return str(jar_dir)


def start_jvm():
    """Start the JVM with ArcadeDB JARs if not already started."""
    if jpype.isJVMStarted():
        return

    jar_path = get_jar_path()
    jar_files = glob.glob(os.path.join(jar_path, "*.jar"))

    if not jar_files:
        raise ArcadeDBError(
            f"No JAR files found in {jar_path}. "
            "The package may be corrupted or incomplete."
        )

    classpath = os.pathsep.join(jar_files)

    # Allow customization via environment variables
    max_heap = os.environ.get("ARCADEDB_JVM_MAX_HEAP", "2g")

    # Prepare JVM arguments
    jvm_args = [
        f"-Xmx{max_heap}",  # Max heap (default 2g, override with env var)
        "-Djava.awt.headless=true",  # Headless mode for server use
    ]

    # Configure JVM error log location (hs_err_pid*.log files)
    # Default: ./log/hs_err_pid%p.log (keeps crash logs with application logs)
    error_file = os.environ.get("ARCADEDB_JVM_ERROR_FILE")
    if error_file:
        jvm_args.append(f"-XX:ErrorFile={error_file}")
    else:
        # Set sensible default: put JVM crash logs in ./log/ directory
        jvm_args.append("-XX:ErrorFile=./log/hs_err_pid%p.log")

    # Allow additional custom JVM arguments
    extra_args = os.environ.get("ARCADEDB_JVM_ARGS")
    if extra_args:
        jvm_args.extend(extra_args.split())

    try:
        jpype.startJVM(*jvm_args, classpath=classpath)
    except Exception as e:
        raise ArcadeDBError(f"Failed to start JVM: {e}")


def shutdown_jvm():
    """Shutdown JVM if it was started by this module."""
    if jpype.isJVMStarted():
        try:
            jpype.shutdownJVM()
        except Exception:
            pass  # Ignore errors during shutdown
