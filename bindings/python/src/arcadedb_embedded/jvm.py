"""
ArcadeDB Python Bindings - JVM Management

Handles JVM initialization and JAR file management.
"""

import glob
import os
import platform
from pathlib import Path
from typing import Iterable, Optional, Union

import jpype
import jpype.imports

from .exceptions import ArcadeDBError

_JVM_CONFIG = None


def get_jar_path() -> str:
    """Get the path to bundled JAR files."""
    package_dir = Path(__file__).parent
    jar_dir = package_dir / "jars"
    return str(jar_dir)


def get_bundled_jre_lib_path() -> str:
    """
    Get the path to bundled JRE's JVM library.

    Returns:
        Path to the JVM library (platform-specific: jvm.dll, libjvm.dylib,
        or libjvm.so).

    Raises:
        ArcadeDBError: If the bundled JRE or JVM library is not found.
    """
    package_dir = Path(__file__).parent
    jre_dir = package_dir / "jre"

    # Check if JRE directory exists
    if not jre_dir.exists():
        raise ArcadeDBError(
            f"Bundled JRE not found at {jre_dir}. "
            "The package may be corrupted or incomplete."
        )

    # Platform-specific JVM library paths
    system = platform.system()
    if system == "Windows":
        # Windows: bin/server/jvm.dll
        jvm_lib_path = jre_dir / "bin" / "server" / "jvm.dll"
    elif system == "Darwin":
        # macOS: lib/server/libjvm.dylib
        jvm_lib_path = jre_dir / "lib" / "server" / "libjvm.dylib"
    else:
        # Linux: lib/server/libjvm.so
        jvm_lib_path = jre_dir / "lib" / "server" / "libjvm.so"

    if not jvm_lib_path.exists():
        raise ArcadeDBError(
            f"JVM library not found at {jvm_lib_path}. "
            "The package may be corrupted or incomplete."
        )

    return str(jvm_lib_path)


def start_jvm(
    heap_size: Optional[str] = "4g",
    disable_xml_limits: bool = True,
    jvm_args: Optional[Union[Iterable[str], str]] = None,
):
    """
    Start the JVM with ArcadeDB JARs if not already started.

    JVM Configuration (programmatic preferred):
    -------------------------------------------
    heap_size (optional)
        Max heap size (e.g. "8g", "4096m"). Defaults to "4g".
        Sets -Xmx and overrides any existing -Xmx from jvm_args or env.
        To honor ARCADEDB_JVM_ARGS -Xmx, pass heap_size=None.

    disable_xml_limits (optional)
        If True, relaxes JDK XML entity limits to support large XML
        dumps (adds jdk.xml.* limits).

    jvm_args (optional)
        Additional JVM flags to pass through (e.g. "-XX:MaxDirectMemorySize=8g",
        "-Dfoo=bar"). Can be a space-separated string or an iterable of strings.
        Note: -Xmx is managed by heap_size when provided.

    JVM Configuration (environment fallback):
    -----------------------------------------
    ARCADEDB_JVM_ARGS (optional)
        JVM arguments for memory and JVM-wide options (space-separated).
        Used as a fallback when no explicit args are provided to start_jvm().
        If not specified, defaults to: "-Xmx4g -Djava.awt.headless=true".

        Common options to set here (JVM-wide only):
            -Xmx<size> / -Xms<size>   Heap sizing (must be set before JVM start)
            -XX:MaxDirectMemorySize=<size>   Direct buffer cap
            -Djava.util.concurrent.ForkJoinPool.common.parallelism=<count>   Limit
                common pool threads if you want fewer threads for graph builds

        XML import may exceed default JDK entity limits.
        Configure these BEFORE import to avoid JAXP limit errors:
            -Djdk.xml.maxGeneralEntitySizeLimit=0
            -Djdk.xml.entityExpansionLimit=0
            -Djdk.xml.totalEntitySizeLimit=0

        Examples:
            # Production with 8GB heap
            start_jvm(heap_size="8g", jvm_args="-XX:MaxDirectMemorySize=8g")

            # Development/testing (smaller memory)
            start_jvm(heap_size="2g")

    ARCADEDB_JVM_ERROR_FILE (optional)
        Path for JVM crash logs (default: ./log/hs_err_pid%p.log)

    Note: JVM options must be set BEFORE the first JVM start, as the JVM
          can only be configured once per Python process.
    """
    global _JVM_CONFIG
    if jpype.isJVMStarted():
        candidate_args = tuple(
            _build_jvm_args(
                heap_size=heap_size,
                disable_xml_limits=disable_xml_limits,
                jvm_args=jvm_args,
            )
        )
        if _JVM_CONFIG is not None:
            if candidate_args != _JVM_CONFIG:
                raise ArcadeDBError(
                    "JVM is already started. Configure JVM args/heap before the "
                    "first database/importer creation."
                )
            return

        has_overrides = (
            jvm_args is not None
            or (heap_size not in (None, "4g"))
            or (disable_xml_limits is not True)
        )
        if has_overrides:
            raise ArcadeDBError(
                "JVM is already started. Configure JVM args/heap before the "
                "first database/importer creation."
            )

        _JVM_CONFIG = candidate_args
        return

    jar_path = get_jar_path()
    jar_files = glob.glob(os.path.join(jar_path, "*.jar"))

    if not jar_files:
        raise ArcadeDBError(
            f"No JAR files found in {jar_path}. "
            "The package may be corrupted or incomplete."
        )

    classpath = os.pathsep.join(jar_files)

    # Get bundled JRE's JVM library path
    jvm_path = get_bundled_jre_lib_path()

    jvm_args = _build_jvm_args(
        heap_size=heap_size, disable_xml_limits=disable_xml_limits, jvm_args=jvm_args
    )

    try:
        # Always use bundled JRE
        jpype.startJVM(jvm_path, *jvm_args, classpath=classpath)
        _JVM_CONFIG = tuple(jvm_args)
    except Exception as e:
        raise ArcadeDBError(f"Failed to start JVM: {e}") from e


def _normalize_jvm_args(jvm_args: Optional[Union[Iterable[str], str]]) -> list[str]:
    if not jvm_args:
        return []
    if isinstance(jvm_args, str):
        return jvm_args.split()
    return list(jvm_args)


def _parse_memory_size(value: str) -> Optional[int]:
    """Parse JVM memory size to bytes. Supports k, m, g (case-insensitive)."""
    if not value:
        return None
    try:
        unit = value[-1].lower()
        number = value[:-1]
        if unit in {"k", "m", "g"}:
            base = float(number)
            if unit == "k":
                return int(base * 1024)
            if unit == "m":
                return int(base * 1024 * 1024)
            if unit == "g":
                return int(base * 1024 * 1024 * 1024)
        return int(value)
    except Exception:
        return None


def _dedupe_max_heap(args: list[str]) -> list[str]:
    """Deduplicate -Xmx args keeping the maximum value when possible."""
    heap_values = []
    for arg in args:
        if arg.startswith("-Xmx"):
            heap_values.append(arg[4:])

    if len(heap_values) <= 1:
        return args

    parsed = [(val, _parse_memory_size(val)) for val in heap_values]
    parsed_valid = [p for p in parsed if p[1] is not None]

    if parsed_valid:
        max_value = max(parsed_valid, key=lambda p: p[1])[0]
    else:
        # Fallback: keep the last -Xmx if parsing fails
        max_value = heap_values[-1]

    filtered = [arg for arg in args if not arg.startswith("-Xmx")]
    filtered.append(f"-Xmx{max_value}")
    return filtered


def _build_jvm_args(
    heap_size: Optional[str],
    disable_xml_limits: bool,
    jvm_args: Optional[Union[Iterable[str], str]],
) -> list[str]:
    """Helper to construct JVM arguments from params, env vars, and defaults."""
    # JVM arguments: start from env, then merge explicit args
    jvm_args_str = os.environ.get("ARCADEDB_JVM_ARGS")
    if jvm_args_str:
        merged_args = jvm_args_str.split()
    else:
        merged_args = []

    merged_args.extend(_normalize_jvm_args(jvm_args))

    # Optional XML import limits
    if disable_xml_limits:
        xml_args = [
            "-Djdk.xml.maxGeneralEntitySizeLimit=0",
            "-Djdk.xml.entityExpansionLimit=0",
            "-Djdk.xml.totalEntitySizeLimit=0",
        ]
        for arg in xml_args:
            if arg not in merged_args:
                merged_args.append(arg)

    # Merge mandatory defaults if missing from user arguments
    if not any(
        arg.startswith("--add-modules") and "jdk.incubator.vector" in arg
        for arg in merged_args
    ):
        merged_args.append("--add-modules=jdk.incubator.vector")

    if not any(arg.startswith("-Djava.awt.headless=") for arg in merged_args):
        merged_args.append("-Djava.awt.headless=true")

    if not any(arg.startswith("--enable-native-access") for arg in merged_args):
        merged_args.append("--enable-native-access=ALL-UNNAMED")

    # Heap handling (single place):
    # - If heap_size is explicitly set to non-default, override.
    # - If heap_size is default or None, keep env/user -Xmx and dedupe (or add default).
    has_xmx = any(arg.startswith("-Xmx") for arg in merged_args)
    if heap_size is not None and heap_size != "4g":
        merged_args = [arg for arg in merged_args if not arg.startswith("-Xmx")]
        merged_args.append(f"-Xmx{heap_size}")
    else:
        if not has_xmx:
            merged_args.append("-Xmx4g")
        merged_args = _dedupe_max_heap(merged_args)

    # Configure JVM crash log location (hs_err_pid*.log files)
    error_file = os.environ.get("ARCADEDB_JVM_ERROR_FILE")
    if error_file:
        merged_args.append(f"-XX:ErrorFile={error_file}")
    else:
        merged_args.append("-XX:ErrorFile=./log/hs_err_pid%p.log")

    return merged_args


def shutdown_jvm():
    """Shutdown JVM if it was started by this module."""
    if jpype.isJVMStarted():
        try:
            jpype.shutdownJVM()
        except Exception:
            pass  # Ignore errors during shutdown
