"""Tests for start_jvm() re-entry behavior once the JVM is running."""

import arcadedb_embedded as arcadedb
import jpype
import pytest
from arcadedb_embedded import jvm
from arcadedb_embedded.exceptions import ArcadeDBError


@pytest.fixture(autouse=True)
def _jvm_running(tmp_path):
    """All tests here assume a started JVM (any earlier DB use starts it)."""
    if not jpype.isJVMStarted():
        db = arcadedb.create_database(str(tmp_path / "jvm_boot"))
        db.close()
    assert jpype.isJVMStarted()


def test_bare_start_jvm_joins_running_jvm(monkeypatch):
    # Regression: create_database(jvm_kwargs={"heap_size": "6g"}) followed by
    # open_database() raised, because the default-built args were compared
    # against the stored custom config instead of noticing no override was
    # requested.
    monkeypatch.setattr(jvm, "_JVM_CONFIG", ("-Xmx6g", "-Xms6g"))
    jvm.start_jvm()  # must not raise


def test_identical_config_is_idempotent(monkeypatch):
    args = tuple(
        jvm._build_jvm_args(
            heap_size="6g",
            disable_xml_limits=True,
            jvm_args="-Xms6g",
            common_pool_parallelism=None,
        )
    )
    monkeypatch.setattr(jvm, "_JVM_CONFIG", args)
    jvm.start_jvm(heap_size="6g", jvm_args="-Xms6g")  # must not raise


def test_conflicting_override_raises(monkeypatch):
    monkeypatch.setattr(jvm, "_JVM_CONFIG", ("-Xmx4g",))
    with pytest.raises(ArcadeDBError, match="already started"):
        jvm.start_jvm(heap_size="99g")


def test_create_close_reopen_same_process(tmp_path):
    path = str(tmp_path / "reopen_db")
    db = arcadedb.create_database(path)
    db.command("sql", "CREATE DOCUMENT TYPE Doc")
    with db.transaction():
        db.command("sql", "INSERT INTO Doc SET k = 1")
    db.close()
    db2 = arcadedb.open_database(path)
    rows = db2.query("sql", "SELECT k FROM Doc").to_list()
    db2.close()
    assert rows == [{"k": 1}]


def test_interpreter_exits_with_unclosed_database(tmp_path):
    """A leaked (unclosed) Database must not hang interpreter exit.

    The engine's non-daemon background threads (e.g. AsyncFlush) keep the
    JVM alive; the bindings close active databases from an atexit hook so
    JPype's shutdown can complete. Without the hook this hangs forever.
    """
    import subprocess  # nosec B404 - test-controlled child process
    import sys as _sys

    script = (
        "import arcadedb_embedded as a\n"
        f"db = a.create_database({str(tmp_path / 'leak_db')!r})\n"
        "db.command('sql', 'CREATE DOCUMENT TYPE Doc')\n"
        "print('OK', flush=True)\n"
    )
    proc = subprocess.run(  # nosec B603 - fixed argv, no shell, test-owned
        [_sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert "OK" in proc.stdout
    assert proc.returncode == 0
