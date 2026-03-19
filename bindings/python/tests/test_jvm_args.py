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
    custom_args = "-Xmx2g -Djava.awt.headless=false --add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED"
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

        assert modules_count == 1
        assert headless_count == 1
        assert native_count == 1

        # Verify user's explicit choice is respected (e.g., they might want headless=false for some reason)
        # Note: Our logic just checks key presence, it doesn't force overwrite if key exists with different value.
        assert "-Djava.awt.headless=false" in args


def test_error_file_env():
    """Test ARCADEDB_JVM_ERROR_FILE injection."""
    with patch.dict(
        os.environ, {"ARCADEDB_JVM_ERROR_FILE": "/tmp/crash.log"}, clear=True
    ):
        args = _build_jvm_args(
            heap_size="4g",
            disable_xml_limits=True,
            jvm_args=None,
        )
        assert "-XX:ErrorFile=/tmp/crash.log" in args
