"""Tests for AsyncExecutor with SQL/Cypher-first usage."""

import shutil
import tempfile
import time
from pathlib import Path

import arcadedb_embedded as arcadedb


def test_async_executor_sql_command_insert():
    db_path = Path(tempfile.mkdtemp()) / "test_async_sql_insert"

    try:
        db = arcadedb.create_database(str(db_path))
        db.command("sql", "CREATE DOCUMENT TYPE Item")

        async_exec = db.async_executor().set_parallel_level(4).set_commit_every(1)

        for i in range(200):
            async_exec.command(
                "sql",
                "INSERT INTO Item SET id = ?, name = ?",
                callback=lambda _r: None,
                args=(i, f"Item{i}"),
            )

        async_exec.wait_completion()
        async_exec.close()

        count = db.query("sql", "SELECT count(*) as c FROM Item").first().get("c")
        assert count > 0
        db.close()
    finally:
        shutil.rmtree(db_path, ignore_errors=True)


def test_async_executor_query_callback_collects_rows(temp_db):
    db = temp_db
    db.command("sql", "CREATE DOCUMENT TYPE Person")

    with db.transaction():
        for i in range(20):
            db.command(
                "sql",
                "INSERT INTO Person SET id = :id, name = :name",
                {"id": i, "name": f"Person{i}"},
            )

    seen = []

    def on_row(row):
        seen.append(row.get("id"))

    async_exec = db.async_executor().set_parallel_level(2).set_commit_every(10)
    async_exec.query("sql", "SELECT id FROM Person ORDER BY id", on_row)
    async_exec.wait_completion()
    async_exec.close()

    assert seen == list(range(20))


def test_async_executor_pending_and_processing_flags(temp_db):
    db = temp_db
    db.command("sql", "CREATE DOCUMENT TYPE Msg")

    async_exec = db.async_executor().set_commit_every(100)
    assert not async_exec.is_pending()

    for i in range(1000):
        async_exec.command("sql", "INSERT INTO Msg SET id = :id", id=i)

    saw_processing = False
    deadline = time.time() + 1.0
    while time.time() < deadline:
        if async_exec.is_processing():
            saw_processing = True
            break

        try:
            if async_exec._java_async.waitCompletion(0):
                break
        except Exception:
            pass

        time.sleep(0.01)

    async_exec.wait_completion()
    assert async_exec.is_pending() is False

    async_exec.close()


def test_async_executor_getters_and_sync_modes(temp_db):
    db = temp_db
    async_exec = db.async_executor()

    async_exec.set_parallel_level(3)
    async_exec.set_commit_every(123)
    async_exec.set_back_pressure(40)
    async_exec.set_transaction_use_wal(False)
    async_exec.set_transaction_sync("yes_nometadata")

    assert async_exec.get_parallel_level() == 3
    assert async_exec.get_commit_every() == 123
    assert async_exec.get_back_pressure() >= 0
    assert async_exec.is_transaction_use_wal() is False
    assert async_exec.get_transaction_sync() == "yes_nometadata"
    assert async_exec.get_thread_count() >= 1

    async_exec.close()


def test_async_executor_command_error_callback(temp_db):
    db = temp_db
    errors = []

    def on_error(exc):
        errors.append(str(exc))

    async_exec = db.async_executor()
    async_exec.command(
        "sql",
        "INSERT INTO MissingType SET id = :id",
        error_callback=on_error,
        id=1,
    )
    async_exec.wait_completion()
    async_exec.close()

    assert errors, "Expected async error callback to be invoked"


def test_async_executor_global_callbacks(temp_db):
    db = temp_db
    db.command("sql", "CREATE DOCUMENT TYPE Log")

    ok_calls = {"count": 0}
    err_calls = {"count": 0}

    def on_ok():
        ok_calls["count"] += 1

    def on_error(_exc):
        err_calls["count"] += 1

    async_exec = db.async_executor().on_ok(on_ok).on_error(on_error)

    for i in range(5):
        async_exec.command("sql", "INSERT INTO Log SET id = :id", id=i)

    async_exec.wait_completion()
    async_exec.close()

    assert ok_calls["count"] >= 1
    assert err_calls["count"] >= 0
