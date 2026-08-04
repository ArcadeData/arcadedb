"""Tests for ResultSet.to_arrow().

The point of this path is not only speed. `to_columns` follows pandas
conventions, and two of them are lossy: a nullable int64 column is promoted to
float64 with NaN, which loses the type and loses precision above 2**53; and a
nullable boolean column degrades to a Python list. Arrow carries a validity
bitmap next to the values, so both keep their type.

The lossiness tests below are therefore the reason this method exists, and they
assert against `to_columns` directly so the difference cannot quietly go away.
"""

import arcadedb_embedded as arcadedb
import pytest

pa = pytest.importorskip("pyarrow", reason="to_arrow() requires pyarrow")


def test_to_arrow_basic_types(temp_db_path):
    """Ints, floats, strings and bools survive the round trip with types."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE Rec")
        with db.transaction():
            db.command(
                "sql", "INSERT INTO Rec SET name = 'a', n = 1, x = 1.5, ok = true"
            )
            db.command(
                "sql", "INSERT INTO Rec SET name = 'b', n = 2, x = 2.5, ok = false"
            )

        table = db.query(
            "sql", "SELECT name, n, x, ok FROM Rec ORDER BY name"
        ).to_arrow()

        assert table is not None, "pyarrow present, so to_arrow must not return None"
        assert isinstance(table, pa.Table)
        assert table.num_rows == 2
        assert table.column("name").to_pylist() == ["a", "b"]
        assert table.column("n").to_pylist() == [1, 2]
        assert pa.types.is_integer(table.schema.field("n").type)
        assert pa.types.is_floating(table.schema.field("x").type)
        assert pa.types.is_string(table.schema.field("name").type)


def test_to_arrow_keeps_int64_with_nulls(temp_db_path):
    """A nullable integer stays an integer here; to_columns turns it into float64.

    This is the concrete reason to prefer Arrow for anything that will be
    checked against the stored value: once the column is float64, an id larger
    than 2**53 no longer round-trips.
    """
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE Rec")
        with db.transaction():
            db.command("sql", "INSERT INTO Rec SET k = 'has', n = 7")
            db.command("sql", "INSERT INTO Rec SET k = 'none'")  # n missing -> null

        arrow_tbl = db.query("sql", "SELECT k, n FROM Rec ORDER BY k").to_arrow()
        cols = db.query("sql", "SELECT k, n FROM Rec ORDER BY k").to_columns()

        assert pa.types.is_integer(
            arrow_tbl.schema.field("n").type
        ), "nullable int must stay an integer in Arrow"
        assert arrow_tbl.column("n").to_pylist() == [7, None]

        # and record what the pandas-convention path does with the same data,
        # so a future change to either is visible here
        import numpy as np

        assert cols["n"].dtype == np.float64
        assert np.isnan(cols["n"][1])


def test_to_arrow_keeps_bool_with_nulls(temp_db_path):
    """A nullable boolean stays a bool column rather than becoming a list."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE Rec")
        with db.transaction():
            db.command("sql", "INSERT INTO Rec SET k = 'a', ok = true")
            db.command("sql", "INSERT INTO Rec SET k = 'b'")  # ok missing -> null

        table = db.query("sql", "SELECT k, ok FROM Rec ORDER BY k").to_arrow()

        assert pa.types.is_boolean(table.schema.field("ok").type)
        assert table.column("ok").to_pylist() == [True, None]


def test_to_arrow_nullable_strings(temp_db_path):
    """Strings are wrapped from the offsets+blob buffer, nulls included."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE Rec")
        with db.transaction():
            db.command("sql", "INSERT INTO Rec SET k = 'a', s = 'hello'")
            db.command("sql", "INSERT INTO Rec SET k = 'b'")  # s missing -> null
            db.command("sql", "INSERT INTO Rec SET k = 'c', s = 'wörld'")  # non-ascii

        table = db.query("sql", "SELECT k, s FROM Rec ORDER BY k").to_arrow()

        assert table.column("s").to_pylist() == ["hello", None, "wörld"]


def test_to_arrow_multi_batch(temp_db_path):
    """More rows than one batch: chunks must concatenate, not truncate."""
    n = 300
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE Rec")
        with db.transaction():
            for i in range(n):
                db.command("sql", f"INSERT INTO Rec SET n = {i}")

        table = db.query("sql", "SELECT n FROM Rec").to_arrow(batch_size=64)

        assert table.num_rows == n
        assert sorted(table.column("n").to_pylist()) == list(range(n))


def test_to_arrow_empty(temp_db_path):
    """An empty result is an empty table, not None and not an error."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE Rec")
        table = db.query("sql", "SELECT FROM Rec").to_arrow()
        assert table is not None
        assert table.num_rows == 0


def test_to_arrow_matches_to_columns_when_not_null(temp_db_path):
    """With no nulls the two paths must agree; only null handling differs."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE Rec")
        with db.transaction():
            for i in range(50):
                db.command("sql", f"INSERT INTO Rec SET n = {i}, s = 'v{i}'")

        arrow_tbl = db.query("sql", "SELECT n, s FROM Rec ORDER BY n").to_arrow()
        cols = db.query("sql", "SELECT n, s FROM Rec ORDER BY n").to_columns()

        assert arrow_tbl.column("n").to_pylist() == list(cols["n"])
        assert arrow_tbl.column("s").to_pylist() == list(cols["s"])
