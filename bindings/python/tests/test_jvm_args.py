import os
from unittest.mock import patch

from arcadedb_embedded.jvm import _build_jvm_args


def test_defaults_no_env_vars():
    """Test defaults when no environment variables are set."""
    with patch.dict(os.environ, {}, clear=True):
        args = _build_jvm_args(
            heap_size="4g",
            disable_xml_limits=True,
            jvm_args=None,
        )
        assert "-Xmx4g" in args
        assert "-Djava.awt.headless=true" in args
        assert "--add-modules=jdk.incubator.vector" in args
        assert "--enable-native-access=ALL-UNNAMED" in args
        assert "-Dfile.encoding=UTF8" in args
        assert any("java.base/java.util.concurrent.atomic" in a for a in args)
        assert any("java.base/java.nio.channels.spi" in a for a in args)
        assert any("java.base/java.lang=" in a for a in args)
        assert "-Dpolyglot.engine.WarnInterpreterOnly=false" in args
        assert "-XX:+UseCompactObjectHeaders" in args
        # Should have default error log
        assert any("hs_err_pid" in arg for arg in args)


def test_custom_jvm_args_merging():
    """Test merging critical flags when user provides custom JVM args."""
    with patch.dict(os.environ, {"ARCADEDB_JVM_ARGS": "-Xmx8g -Dfoo=bar"}, clear=True):
        args = _build_jvm_args(
            heap_size="4g",
            disable_xml_limits=True,
            jvm_args=None,
        )

        # User args preserved
        assert "-Xmx8g" in args
        assert "-Dfoo=bar" in args

        # Mandatory args injected
        assert "-Djava.awt.headless=true" in args
        assert "--add-modules=jdk.incubator.vector" in args
        assert "--enable-native-access=ALL-UNNAMED" in args
        assert "-Dfile.encoding=UTF8" in args
        assert any("java.base/java.util.concurrent.atomic" in a for a in args)
        assert any("java.base/java.nio.channels.spi" in a for a in args)
        assert any("java.base/java.lang=" in a for a in args)
        assert "-Dpolyglot.engine.WarnInterpreterOnly=false" in args
        assert "-XX:+UseCompactObjectHeaders" in args


def test_dedupe_heap_keeps_max():
    """Multiple -Xmx values keep the maximum."""
    with patch.dict(
        os.environ, {"ARCADEDB_JVM_ARGS": "-Xmx2g -Xmx4096m -Xmx1g"}, clear=True
    ):
        args = _build_jvm_args(
            heap_size="4g",
            disable_xml_limits=True,
            jvm_args=None,
        )
        assert "-Xmx4096m" in args
        assert sum(1 for a in args if a.startswith("-Xmx")) == 1


def test_custom_jvm_args_injects_heap_default_when_missing():
    """Ensure we add a heap default if user omits -Xmx."""
    with patch.dict(os.environ, {"ARCADEDB_JVM_ARGS": "-Dfoo=bar"}, clear=True):
        args = _build_jvm_args(
            heap_size="4g",
            disable_xml_limits=True,
            jvm_args=None,
        )
        assert "-Xmx4g" in args
        assert "-Dfoo=bar" in args


def test_custom_jvm_args_no_duplicates():
    """Test that we don't duplicate flags if user provides them."""
    custom_args = "-Xmx2g -Djava.awt.headless=false --add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED -XX:+UseCompactObjectHeaders"
    with patch.dict(os.environ, {"ARCADEDB_JVM_ARGS": custom_args}, clear=True):
        args = _build_jvm_args(
            heap_size="4g",
            disable_xml_limits=True,
            jvm_args=None,
        )

        # Should NOT add defaults if present
        # Count occurrences
        modules_count = sum(1 for a in args if "jdk.incubator.vector" in a)
        headless_count = sum(1 for a in args if "headless" in a)
        native_count = sum(1 for a in args if "enable-native-access" in a)
        encoding_count = sum(1 for a in args if a.startswith("-Dfile.encoding="))
        compact_headers_count = sum(1 for a in args if "UseCompactObjectHeaders" in a)

        assert modules_count == 1
        assert headless_count == 1
        assert native_count == 1
        assert encoding_count == 1
        assert compact_headers_count == 1

        # Verify user's explicit choice is respected (e.g., they might want headless=false for some reason)
        # Note: Our logic just checks key presence, it doesn't force overwrite if key exists with different value.
        assert "-Djava.awt.headless=false" in args


def test_error_file_env():
    """Test ARCADEDB_JVM_ERROR_FILE injection."""
    with patch.dict(
        os.environ,
        {"ARCADEDB_JVM_ERROR_FILE": "/tmp/crash.log"},  # nosec B108
        clear=True,
    ):
        args = _build_jvm_args(
            heap_size="4g",
            disable_xml_limits=True,
            jvm_args=None,
        )
        assert "-XX:ErrorFile=/tmp/crash.log" in args


def test_common_pool_parallelism_arg_injected():
    """Explicit common_pool_parallelism should inject the JVM thread cap flag."""
    with patch.dict(os.environ, {}, clear=True):
        args = _build_jvm_args(
            heap_size="4g",
            disable_xml_limits=True,
            jvm_args=None,
            common_pool_parallelism=8,
        )

        assert "-Djava.util.concurrent.ForkJoinPool.common.parallelism=8" in args


def test_common_pool_parallelism_overrides_env_jvm_args():
    """Explicit common_pool_parallelism should override any env-provided value."""
    with patch.dict(
        os.environ,
        {
            "ARCADEDB_JVM_ARGS": (
                "-Xmx4g " "-Djava.util.concurrent.ForkJoinPool.common.parallelism=2"
            )
        },
        clear=True,
    ):
        args = _build_jvm_args(
            heap_size="4g",
            disable_xml_limits=True,
            jvm_args=None,
            common_pool_parallelism=6,
        )

        assert "-Djava.util.concurrent.ForkJoinPool.common.parallelism=6" in args
        assert "-Djava.util.concurrent.ForkJoinPool.common.parallelism=2" not in args


def test_common_pool_parallelism_must_be_positive():
    """common_pool_parallelism must be >= 1 when provided."""
    with patch.dict(os.environ, {}, clear=True):
        try:
            _build_jvm_args(
                heap_size="4g",
                disable_xml_limits=True,
                jvm_args=None,
                common_pool_parallelism=0,
            )
            assert False, "Expected ArcadeDBError for common_pool_parallelism=0"
        except Exception as exc:
            assert "common_pool_parallelism must be >= 1" in str(exc)


def test_conftest_binds_each_pytest_hook_once():
    # A second `def pytest_configure` in conftest.py silently replaced the
    # first, so the Windows faulthandler hook below never ran for two months.
    import ast
    from collections import Counter
    from pathlib import Path

    tree = ast.parse((Path(__file__).parent / "conftest.py").read_text())
    names = Counter(
        node.name
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name.startswith("pytest_")
    )
    assert [n for n, c in names.items() if c > 1] == []


def test_faulthandler_is_off_on_windows_only():
    # The conftest hook is a function of sys.platform, so every platform can check it for all
    # three. It runs in a child process: calling faulthandler.enable() or disable() in the test
    # process after the JVM started replaces HotSpot's SIGSEGV handler (the JVM raises SIGSEGV
    # on purpose for safepoints and implicit null checks), and the next one kills the run with
    # exit 139 and no hs_err file. On Windows the session's real state is read as well.
    import faulthandler
    import subprocess  # nosec B404 - test-controlled child process
    import sys
    from pathlib import Path

    code = """
import faulthandler, sys
sys.path.insert(0, sys.argv[1])
from tests import conftest

for platform, expect_enabled in (("win32", False), ("linux", True), ("darwin", True)):
    faulthandler.enable()
    sys.platform = platform
    conftest.pytest_configure(None)
    assert faulthandler.is_enabled() is expect_enabled, platform
print("ok")
"""
    root = str(Path(__file__).resolve().parents[1])
    result = subprocess.run(  # nosec B603 - fixed argument list, no shell
        [sys.executable, "-c", code, root],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"

    if sys.platform == "win32":
        assert not faulthandler.is_enabled()


def test_java_thread_dump_lists_the_jvm_threads(temp_db):
    # The conftest timer calls this shortly before faulthandler_timeout, so a
    # hang inside a Java call leaves the Java side's stacks in the CI log (#10).
    import io

    from tests.conftest import dump_java_threads

    out = io.StringIO()
    assert dump_java_threads("test", out) is True
    text = out.getvalue()
    assert text.startswith("=== Java threads: test ===")
    assert '"Reference Handler"' in text
    assert "state=" in text and "    at " in text
