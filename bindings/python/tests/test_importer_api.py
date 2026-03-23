from pathlib import Path

import arcadedb_embedded as arcadedb
import pytest


def _write_people_csv(csv_path: Path) -> None:
    csv_path.write_text(
        "name,age,city\n" "Alice,30,New York\n" "Bob,25,London\n" "Charlie,35,Paris\n",
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
        async_exec.set_commit_every(0)
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
        assert async_exec.get_commit_every() == 0
        assert async_exec.is_transaction_use_wal() is True


def test_import_documents_missing_file_raises_arcadedb_error(temp_db_path, tmp_path):
    missing_path = tmp_path / "missing.csv"

    with arcadedb.create_database(temp_db_path) as db:
        with pytest.raises(arcadedb.ArcadeDBError):
            db.import_documents(missing_path, document_type="Person")
