"""
Shared pytest fixtures and configuration for ArcadeDB tests.
"""

import os
import shutil
import sys
import tempfile
import threading

import pytest

# A test file that cannot run here is not collected, so nothing is reported as skipped: a skip means
# a test that should have run, and scripts/check_test_skips.py fails the CI job on any skip.
collect_ignore = []
if sys.platform == "win32":
    # test_sigint.py sends SIGINT to a child process, which Windows cannot deliver
    collect_ignore.append("test_sigint.py")
if not os.path.isdir(os.path.join(os.path.dirname(os.path.dirname(__file__)), "docs")):
    # the upstream pull request branch has no docs/ directory
    collect_ignore.append("test_docs_examples.py")


@pytest.hookimpl(trylast=True)
def pytest_configure(config):
    # HotSpot routinely raises access violations it handles itself
    # (safepoints, implicit null checks). On Windows, pytest's faulthandler
    # prints a fatal-looking Python stack for each one even though nothing
    # crashed. Disable it there; real crashes still fail the run, and
    # faulthandler_timeout's hang dump does not depend on it.
    # trylast: pytest's own faulthandler plugin enables it in its
    # pytest_configure, and a conftest hook otherwise runs before that.
    # This hook was dead from 2026-07-25 to 2026-09-29: a second
    # pytest_configure further down replaced it (test_jvm_args pins it now).
    import sys

    if sys.platform == "win32":
        import faulthandler

        if faulthandler.is_enabled():
            faulthandler.disable()


# A test still running this long gets every Java thread's stack on stderr,
# shortly before faulthandler_timeout (600 s in pyproject.toml) dumps the
# Python threads. The Python dump alone shows a test waiting inside a Java
# call and nothing about why, which is all a Windows vector-search hang left
# behind twice (humemai/arcadedb-embedded-python#10).
JAVA_DUMP_AFTER_S = float(os.environ.get("ARCADEDB_TEST_JAVA_DUMP_AFTER_S", "540"))


def dump_java_threads(reason, out=None):
    """Write every Java thread's name, state, and stack to `out` (stderr).

    Returns False when the JVM is not running, so there is nothing to dump.
    """
    import jpype

    out = out or sys.stderr
    if not jpype.isJVMStarted():
        return False
    traces = jpype.JClass("java.lang.Thread").getAllStackTraces()
    lines = [f"=== Java threads: {reason} ==="]
    for thread in traces.keySet():
        lines.append(
            f'"{thread.getName()}" daemon={thread.isDaemon()} state={thread.getState()}'
        )
        lines.extend(f"    at {frame}" for frame in traces.get(thread))
    out.write("\n".join(lines) + "\n")
    out.flush()
    return True


def _dump_java_threads_for(nodeid, capman):
    # Captured output of a test that never finishes is lost when the job is
    # killed, so the dump goes past pytest's capture, as a debugger's would.
    try:
        if capman is not None:
            with capman.global_and_fixture_disabled():
                dump_java_threads(
                    f"{nodeid} still running after {JAVA_DUMP_AFTER_S:.0f} s"
                )
        else:
            dump_java_threads(f"{nodeid} still running after {JAVA_DUMP_AFTER_S:.0f} s")
    except Exception as exc:  # the dump is evidence; it must not fail the run
        sys.stderr.write(f"Java thread dump failed: {exc!r}\n")


@pytest.hookimpl(wrapper=True)
def pytest_runtest_call(item):
    capman = item.config.pluginmanager.getplugin("capturemanager")
    timer = threading.Timer(
        JAVA_DUMP_AFTER_S, _dump_java_threads_for, args=(item.nodeid, capman)
    )
    timer.daemon = True
    timer.start()
    try:
        return (yield)
    finally:
        timer.cancel()


# Shared test password used by server-mode tests. ArcadeDB requires >= 8 chars.
# Hardcoded test fixture, not a real credential.
TEST_PASSWORD = "test12345"  # nosec B105


@pytest.fixture
def temp_server_root():
    """Create a temporary server root directory."""
    temp_dir = tempfile.mkdtemp(prefix="arcadedb_test_server_")
    yield temp_dir
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)


@pytest.fixture
def temp_db_path():
    """Create a temporary database path."""
    temp_dir = tempfile.mkdtemp(prefix="arcadedb_test_db_")
    db_path = os.path.join(temp_dir, "test_db")
    yield db_path
    # Cleanup
    if os.path.exists(temp_dir):
        # Force garbage collection to release file handles (Windows fix)
        import gc

        gc.collect()

        try:
            shutil.rmtree(temp_dir)
        except PermissionError:
            # On Windows, files might still be locked by Java process
            # Wait a bit and try again
            import time

            time.sleep(0.5)
            try:
                shutil.rmtree(temp_dir)
            except PermissionError:
                # If still locked, ignore (OS will clean up temp eventually)
                pass


@pytest.fixture
def temp_db():
    """Create a temporary database, yield it, and clean up."""
    import arcadedb_embedded as arcadedb

    temp_dir = tempfile.mkdtemp(prefix="arcadedb_test_db_")
    db_path = os.path.join(temp_dir, "test_db")

    db = arcadedb.create_database(db_path)
    yield db

    # Cleanup
    # Database has is_open(), not is_closed(): the old call raised
    # AttributeError, the bare except swallowed it, and the directory was
    # removed under a still-open database, which the engine then failed to
    # flush at JVM shutdown ("Failed to allocate sparse segment component ...
    # No such file or directory", 2026-09-07). No fixture test ever closed.
    try:
        if db.is_open():
            db.close()
    except Exception as exc:  # noqa: BLE001
        # Never silent: a swallowed teardown is how the is_closed() bug hid for
        # ten months. Warn so it shows in the summary, but do not fail the test
        # that just passed for a close-time problem it did not cause.
        import warnings

        warnings.warn(f"temp_db teardown: close failed: {exc!r}", stacklevel=1)

    # Force garbage collection to release file handles (Windows fix)
    import gc

    gc.collect()

    if os.path.exists(temp_dir):
        try:
            shutil.rmtree(temp_dir)
        except PermissionError:
            # On Windows, files might still be locked by Java process
            import time

            time.sleep(0.5)
            try:
                shutil.rmtree(temp_dir)
            except PermissionError:
                pass


@pytest.fixture
def temp_dir_factory():
    """Factory fixture to create multiple temporary directories with cleanup."""
    temp_dirs = []

    def _create_temp_dir(prefix="arcadedb_test_"):
        """Create a temporary directory and register it for cleanup."""
        temp_dir = tempfile.mkdtemp(prefix=prefix)
        temp_dirs.append(temp_dir)
        return temp_dir

    yield _create_temp_dir

    # Cleanup all created directories
    for temp_dir in temp_dirs:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)


def pytest_unconfigure(config):
    """
    Prefer graceful JVM shutdown after pytest completes.

    The old test suite used `os._exit(0)` unconditionally because JVM shutdown
    used to hang. The suite now exits cleanly after explicit async-executor
    ownership cleanup, so graceful shutdown is the default behavior.

    `ARCADEDB_PYTEST_FORCE_EXIT=1` is retained only as an emergency override
    for debugging future shutdown regressions.
    """
    import sys

    from arcadedb_embedded.jvm import shutdown_jvm

    # Flush all output to ensure we see test results
    sys.stdout.flush()
    sys.stderr.flush()

    if os.environ.get("ARCADEDB_PYTEST_FORCE_EXIT", "0") == "1":
        os._exit(0)

    shutdown_jvm()


def pytest_sessionfinish(session, exitstatus):
    """Session finish hook."""
    pass


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    """Terminal summary hook."""
    pass
