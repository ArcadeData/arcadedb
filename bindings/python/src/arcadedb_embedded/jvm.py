"""
ArcadeDB Python Bindings - JVM Management

Handles JVM initialization and JAR file management.
"""

import glob
import os
import platform
import shlex
import sys
import zipfile
from pathlib import Path
from typing import Iterable, Optional, Union

import jpype
import jpype.imports

from .exceptions import ArcadeDBError

_JVM_CONFIG = None


def _project_dir() -> Path:
    return Path(__file__).resolve().parent.parents[1]


def _extract_runtime_resource(resource_name: str) -> Path:
    package_dir = Path(__file__).resolve().parent
    resource_dir = package_dir / resource_name
    if resource_dir.exists():
        return resource_dir

    project_dir = _project_dir()
    dist_dir = project_dir / "dist"
    wheels = sorted(dist_dir.glob("arcadedb_embedded-*.whl"), reverse=True)
    if not wheels:
        return resource_dir

    cache_root = project_dir / ".runtime-cache"
    extracted_root = cache_root / "arcadedb_embedded"
    extracted_resource_dir = extracted_root / resource_name
    if extracted_resource_dir.exists():
        return extracted_resource_dir

    cache_root.mkdir(parents=True, exist_ok=True)
    prefix = f"arcadedb_embedded/{resource_name}/"

    with zipfile.ZipFile(wheels[0]) as wheel_zip:
        members = [name for name in wheel_zip.namelist() if name.startswith(prefix)]
        if not members:
            return resource_dir
        wheel_zip.extractall(cache_root, members)

    return extracted_resource_dir


def get_jar_path() -> str:
    """Get the path to bundled JAR files."""
    jar_dir = _extract_runtime_resource("jars")
    return str(jar_dir)


_JAR_FINGERPRINT_CACHE = None

# JARs this project compiles, as opposed to the ArcadeDB engine JARs staged
# from the image. Excluded from engine_sha256 because they are built here and
# are not byte-reproducible: the PyPI 26.8.1 wheel and a local build of the
# same version differ in this JAR alone, at identical size.
_OUR_JARS = frozenset({"arcadedb-python-bridge.jar"})


def jar_fingerprint(per_jar: bool = False) -> dict:
    """Identify the engine this install actually carries.

    ``__version__`` is a *package* version. It says nothing about which JARs
    are in the wheel, and the two can disagree without any error: a wheel built
    from a locally patched Java tree still reports the released version number
    while carrying a different engine.

    That is not hypothetical. A local build of 26.8.1.dev-something shipped 59
    JARs instead of 64 and a bt-server-patched engine, and it was caught only
    because one packaging test happened to count JAR names. Benchmarks run
    against it would have recorded the released version string beside numbers
    the release cannot reproduce, which is how a fix gets credited to the wrong
    commit (see the false null on ArcadeDB #5388).

    So: hash what is actually on disk. Two installs agree iff this agrees.

        >>> import arcadedb_embedded as adb
        >>> adb.jar_fingerprint()["sha256"][:12]
        'a3f1c0d8b214'

    Benchmark harnesses should record ``sha256`` next to the engine version, so
    a results row proves which engine produced it rather than asserting it.

    Args:
        per_jar: also return the sorted (name, size, sha256) of every JAR, for
            diffing two installs that disagree.

    Two hashes, because they answer different questions.

    ``sha256`` covers every JAR: "is this the same build?" ``engine_sha256``
    excludes our own compiled shim: "is this the same ArcadeDB?"

    The distinction is measured, not defensive. Comparing the released 26.8.1
    wheel from PyPI against a local build of the same version: identical file
    size, identical JAR count, identical total JAR bytes, and a different
    ``sha256``. Diffing found 63 of 64 JARs byte-identical, with the only
    difference in ``arcadedb-python-bridge.jar`` at the *same* 10907 bytes.
    That JAR is compiled during the build, so it carries timestamps and is not
    reproducible; the 63 engine JARs come from the ArcadeDB image and are.

    So a bare ``sha256`` comparison would report "different engine" for two
    builds of the same engine, and anyone using it that way would stop
    believing it. Use ``engine_sha256`` to ask whether two installs are running
    the same ArcadeDB.

    Returns:
        ``{"count", "bytes", "sha256", "engine_sha256", "engine_count",
        "jar_dir"}``, plus ``"jars"`` when ``per_jar`` is set. Each hash covers
        the JAR names and contents it spans, so a renamed, added, removed or
        modified JAR all change it.
    """
    global _JAR_FINGERPRINT_CACHE
    if _JAR_FINGERPRINT_CACHE is not None and not per_jar:
        return dict(_JAR_FINGERPRINT_CACHE)

    import hashlib
    import os

    jar_dir = get_jar_path()
    names = sorted(n for n in os.listdir(jar_dir) if n.endswith(".jar"))
    combined = hashlib.sha256()
    engine = hashlib.sha256()
    total = 0
    engine_count = 0
    entries = []
    for name in names:
        path = os.path.join(jar_dir, name)
        h = hashlib.sha256()
        with open(path, "rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                h.update(chunk)
        size = os.path.getsize(path)
        total += size
        # Name AND digest, so swapping two JARs' contents is not a collision.
        combined.update(name.encode())
        combined.update(h.digest())
        if name not in _OUR_JARS:
            engine.update(name.encode())
            engine.update(h.digest())
            engine_count += 1
        if per_jar:
            entries.append(
                {
                    "name": name,
                    "bytes": size,
                    "sha256": h.hexdigest(),
                    "engine": name not in _OUR_JARS,
                }
            )

    out = {
        "count": len(names),
        "bytes": total,
        "sha256": combined.hexdigest(),
        "engine_sha256": engine.hexdigest(),
        "engine_count": engine_count,
        "jar_dir": jar_dir,
    }
    _JAR_FINGERPRINT_CACHE = dict(out)
    if per_jar:
        out["jars"] = entries
    return out


def get_bundled_jre_lib_path() -> str:
    """
    Get the path to bundled JRE's JVM library.

    Returns:
        Path to the JVM library (platform-specific: jvm.dll, libjvm.dylib,
        or libjvm.so).

    Raises:
        ArcadeDBError: If the bundled JRE or JVM library is not found.
    """
    jre_dir = _extract_runtime_resource("jre")

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
    common_pool_parallelism: Optional[int] = None,
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

    common_pool_parallelism (optional)
        Sets `-Djava.util.concurrent.ForkJoinPool.common.parallelism=<count>`.
        Use this to make thread-cap settings explicit and reproducible from the
        calling Python code.

        Example:
            start_jvm(heap_size="8g", common_pool_parallelism=8)

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

            # Explicit thread cap for reproducible benchmarks
            start_jvm(heap_size="8g", common_pool_parallelism=8)

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
                common_pool_parallelism=common_pool_parallelism,
            )
        )
        has_overrides = (
            jvm_args is not None
            or (heap_size not in (None, "4g"))
            or (disable_xml_limits is not True)
            or (common_pool_parallelism is not None)
        )
        if not has_overrides:
            # No explicit configuration requested: join the running JVM
            # (e.g. open_database() after create_database(jvm_kwargs=...)).
            if _JVM_CONFIG is None:
                _JVM_CONFIG = candidate_args
            return

        if _JVM_CONFIG is not None and candidate_args == _JVM_CONFIG:
            return
        raise ArcadeDBError(
            "JVM is already started with different settings. Configure JVM "
            "args/heap before the first database or server creation."
        )

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
        heap_size=heap_size,
        disable_xml_limits=disable_xml_limits,
        jvm_args=jvm_args,
        common_pool_parallelism=common_pool_parallelism,
    )

    try:
        # Always use bundled JRE
        jpype.startJVM(jvm_path, *jvm_args, classpath=classpath)
        _JVM_CONFIG = tuple(jvm_args)
    except Exception as e:
        raise ArcadeDBError(f"Failed to start JVM: {e}") from e

    # Registered AFTER JPype's own atexit hook so it runs BEFORE it (atexit
    # is LIFO): close any Database left open. Engine >= 850ce7c37 (#5418)
    # also daemonizes its background threads and installs its own JVM
    # shutdown hook, so this is defense-in-depth for older jars and for
    # deterministic teardown before JPype detaches.
    import atexit

    atexit.register(_close_active_databases)

    if sys.platform == "win32":
        # HotSpot handles SEH access violations internally (safepoint polls,
        # implicit null checks), but any enabled Python faulthandler prints
        # them as fake "Windows fatal exception" dumps. Disabling at
        # configure time is not enough: tools (e.g. pytest's faulthandler
        # plugin) may re-enable it before the JVM starts, so disable at the
        # last reliable point, right after JVM start.
        import faulthandler

        faulthandler.disable()


def _close_active_databases():
    """Close every Database still open so JVM shutdown cannot block."""
    if not jpype.isJVMStarted():
        return
    try:
        factory = jpype.JClass("com.arcadedb.database.DatabaseFactory")
        for db in list(factory.getActiveDatabaseInstances()):
            try:
                if db.isOpen():
                    db.close()
            except Exception:  # nosec B110 - best-effort cleanup at exit
                pass
    except Exception:  # nosec B110 - best-effort cleanup at exit
        pass


def _normalize_jvm_args(jvm_args: Optional[Union[Iterable[str], str]]) -> list[str]:
    if not jvm_args:
        return []
    if isinstance(jvm_args, str):
        return shlex.split(jvm_args)
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
    common_pool_parallelism: Optional[int] = None,
) -> list[str]:
    """Helper to construct JVM arguments from params, env vars, and defaults."""
    if common_pool_parallelism is not None and common_pool_parallelism < 1:
        raise ArcadeDBError("common_pool_parallelism must be >= 1")

    # JVM arguments: start from env, then merge explicit args
    jvm_args_str = os.environ.get("ARCADEDB_JVM_ARGS")
    if jvm_args_str:
        merged_args = shlex.split(jvm_args_str)
    else:
        merged_args = []

    merged_args.extend(_normalize_jvm_args(jvm_args))

    if common_pool_parallelism is not None:
        merged_args = [
            arg
            for arg in merged_args
            if not arg.startswith(
                "-Djava.util.concurrent.ForkJoinPool.common.parallelism="
            )
        ]
        merged_args.append(
            "-Djava.util.concurrent.ForkJoinPool.common.parallelism="
            f"{common_pool_parallelism}"
        )

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

    # Force UTF-8 regardless of OS locale to avoid silent encoding mismatches
    if not any(arg.startswith("-Dfile.encoding=") for arg in merged_args):
        merged_args.append("-Dfile.encoding=UTF8")

    # Reflection access required by ArcadeDB engine internals (same as server.sh)
    if not any("java.base/java.util.concurrent.atomic" in arg for arg in merged_args):
        merged_args.append(
            "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED"
        )

    if not any("java.base/java.nio.channels.spi" in arg for arg in merged_args):
        merged_args.append("--add-opens=java.base/java.nio.channels.spi=ALL-UNNAMED")

    if not any("java.base/java.lang=" in arg for arg in merged_args):
        merged_args.append("--add-opens=java.base/java.lang=ALL-UNNAMED")

    # Silence Truffle/GraalVM interpreter warning on standard HotSpot JDKs
    if not any(
        arg.startswith("-Dpolyglot.engine.WarnInterpreterOnly=") for arg in merged_args
    ):
        merged_args.append("-Dpolyglot.engine.WarnInterpreterOnly=false")

    # Enable compact object headers (JDK 25+) to reduce heap footprint
    if not any("UseCompactObjectHeaders" in arg for arg in merged_args):
        merged_args.append("-XX:+UseCompactObjectHeaders")

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
    """Shutdown JVM if it was started by this module.

    JPype can raise RuntimeError when the JVM is already mid-shutdown or
    has been detached from the calling thread; in that case there is
    nothing left for us to do.
    """
    if jpype.isJVMStarted():
        _close_active_databases()
        try:
            jpype.shutdownJVM()
        except RuntimeError:
            return
