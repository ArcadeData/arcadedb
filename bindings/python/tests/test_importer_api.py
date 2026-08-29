from pathlib import Path

import arcadedb_embedded as arcadedb
import pytest


def _write_people_csv(csv_path: Path) -> None:
    csv_path.write_text(
        (
            "name,age,city\n"
            "Alice,30,New York\n"
            "Bob,25,London\n"
            "Charlie,35,Paris\n"
        ),
        encoding="utf-8",
    )


def test_import_documents_imports_csv_from_path(temp_db_path, tmp_path):
    csv_path = tmp_path / "people.csv"
    _write_people_csv(csv_path)

    with arcadedb.create_database(temp_db_path) as db:
        result = db.import_documents(csv_path, document_type="Person")

        assert result.result == "OK"
        assert result.operation == "import documents"
        assert result.source_url == csv_path.resolve().as_uri()
        assert isinstance(result.statistics, dict)

        count = db.query("sql", "SELECT count(*) as c FROM Person").one().get("c")
        first = db.query("sql", "SELECT FROM Person WHERE name = 'Alice'").one()

        assert count == 3
        assert first.get("city") == "New York"


def test_import_documents_accepts_explicit_importer_settings(temp_db_path, tmp_path):
    csv_path = tmp_path / "people.csv"
    _write_people_csv(csv_path)

    with arcadedb.create_database(temp_db_path) as db:
        result = db.import_documents(
            csv_path,
            document_type="Person",
            file_type="csv",
            commit_every=2,
            parallel=1,
            wal=False,
            extra_settings={"maxPropertySize": 1048576},
        )

        assert result.result == "OK"

        count = db.query("sql", "SELECT count(*) as c FROM Person").one().get("c")
        assert count == 3


def test_import_documents_applies_and_restores_runtime_settings(temp_db_path, tmp_path):
    csv_path = tmp_path / "people.csv"
    _write_people_csv(csv_path)

    with arcadedb.create_database(temp_db_path) as db:
        db.set_read_your_writes(True)
        async_exec = db.async_executor()
        async_exec.set_parallel_level(1)
        async_exec.set_commit_every(7)
        async_exec.set_transaction_use_wal(True)

        result = db.import_documents(
            csv_path,
            document_type="Person",
            file_type="csv",
            commit_every=2,
            parallel=1,
            wal=False,
        )

        assert result.result == "OK"
        assert db.is_read_your_writes() is True
        assert async_exec.get_parallel_level() == 1
        assert async_exec.get_commit_every() == 7
        assert async_exec.is_transaction_use_wal() is True


def test_set_commit_every_rejects_below_one(temp_db_path):
    with arcadedb.create_database(temp_db_path) as db:
        async_exec = db.async_executor()
        with pytest.raises(ValueError, match="commit_every must be >= 1"):
            async_exec.set_commit_every(0)
        with pytest.raises(ValueError, match="commit_every must be >= 1"):
            async_exec.set_commit_every(-5)


def test_import_documents_missing_file_raises_arcadedb_error(temp_db_path, tmp_path):
    missing_path = tmp_path / "missing.csv"

    with arcadedb.create_database(temp_db_path) as db:
        with pytest.raises(arcadedb.ArcadeDBError):
            db.import_documents(missing_path, document_type="Person")


def test_import_documents_restores_runtime_settings_after_failure(
    temp_db_path, tmp_path
):
    missing_path = tmp_path / "missing.csv"

    with arcadedb.create_database(temp_db_path) as db:
        db.set_read_your_writes(True)
        async_exec = db.async_executor()
        async_exec.set_parallel_level(1)
        async_exec.set_commit_every(7)
        async_exec.set_transaction_use_wal(True)

        with pytest.raises(arcadedb.ArcadeDBError):
            db.import_documents(
                missing_path,
                document_type="Person",
                file_type="csv",
                commit_every=2,
                parallel=4,
                wal=False,
            )

        assert db.is_read_your_writes() is True
        assert async_exec.get_parallel_level() == 1
        assert async_exec.get_commit_every() == 7
        assert async_exec.is_transaction_use_wal() is True


def _write_csv_with_a_duplicate_key(csv_path: Path) -> None:
    """Three rows, the middle one repeating the first row's unique key."""
    csv_path.write_text(
        ("sku,name\n" "A-1,first\n" "A-1,duplicate\n" "B-2,third\n"),
        encoding="utf-8",
    )


@pytest.mark.parametrize(
    "mode,expect_all_good_rows",
    [(None, False), ("abort", False), ("skip", True)],
)
def test_import_documents_on_row_error(
    temp_db_path, tmp_path, mode, expect_all_good_rows
):
    """`on_row_error="skip"` keeps the good rows; the default aborts the job.

    The duplicate key only fails at index time, by which point the bad row
    already has a bucket write, which is exactly why the engine commits per row
    in skip mode: rolling back just that row's own transaction is what lets the
    surrounding good rows survive.

    The default and an explicit "abort" are asserted to behave the same, so a
    change to the engine's default cannot pass silently here.
    """
    csv_path = tmp_path / "products.csv"
    _write_csv_with_a_duplicate_key(csv_path)

    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE Product")
        db.command("sql", "CREATE PROPERTY Product.sku STRING")
        db.command("sql", "CREATE INDEX ON Product (sku) UNIQUE")

        kwargs = {} if mode is None else {"on_row_error": mode}
        if mode == "skip":
            # Must NOT raise: rolling back the bad row's own transaction is what
            # lets the surrounding good rows survive.
            db.import_documents(csv_path, document_type="Product", **kwargs)
        else:
            # Must raise. A try/except here passed when abort returned normally
            # with a partial result, so a regression that silently stopped
            # aborting would not have failed this test.
            with pytest.raises(arcadedb.ArcadeDBError):
                db.import_documents(csv_path, document_type="Product", **kwargs)

        rows = sorted(
            str(r.get("sku"))
            for r in db.query("sql", "SELECT sku FROM Product")
            if r.get("sku") is not None
        )
        if expect_all_good_rows:
            assert rows == ["A-1", "B-2"], rows
        else:
            # Whatever abort leaves behind, it must not be the full good set.
            assert rows != ["A-1", "B-2"], rows


def test_import_documents_rejects_unknown_on_row_error(temp_db_path, tmp_path):
    """A typo must raise rather than silently degrade to abort.

    `ImporterSettings.isSkipOnRowError()` is `"skip".equalsIgnoreCase(value)`,
    so every unrecognised value means abort. Passing "ignore" or "SKIPP" would
    otherwise run the whole import in the opposite mode from the one asked for,
    with nothing logged anywhere. Validation is Python-side for that reason:
    the engine has no failure to propagate, which is what makes this class of
    mistake invisible without it.
    """
    csv_path = tmp_path / "people.csv"
    _write_people_csv(csv_path)

    with arcadedb.create_database(temp_db_path) as db:
        for bad in ("ignore", "SKIPP", ""):
            with pytest.raises(ValueError, match="Invalid on_row_error mode"):
                db.import_documents(csv_path, document_type="Person", on_row_error=bad)

        # Case-insensitive, matching the engine's equalsIgnoreCase.
        db.import_documents(csv_path, document_type="Person", on_row_error="SKIP")
