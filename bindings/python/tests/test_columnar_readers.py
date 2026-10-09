"""to_columns, to_dataframe, and to_arrow on schemaless and DECIMAL data.

The three readers take their rows over the bridge's columnar buffer in batches.
Each test runs at several batch sizes, because the point of these fixes is that
the answer must not depend on where the batch boundaries fall: #113 (a property
the first row lacks was dropped), #114 (an Arrow column's type followed the batch
size), #115 (a DECIMAL column came back as JSON numbers).

Like test_bridge_fixes.py, they need the bridge jar built from this tree.
"""

import math
from decimal import Decimal

import arcadedb_embedded as arcadedb
import pytest

pa = pytest.importorskip("pyarrow")
pd = pytest.importorskip("pandas")

BATCH_SIZES = [25_000, 2, 1]


@pytest.fixture
def events_db(temp_db_path):
    """Three documents; `reason` first appears on the second one."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE Event")
        with db.transaction():
            db.command("sql", "INSERT INTO Event SET n = 1, kind = 'start'")
            db.command(
                "sql", "INSERT INTO Event SET n = 2, kind = 'stop', reason = 'timeout'"
            )
            db.command(
                "sql", "INSERT INTO Event SET n = 3, kind = 'stop', reason = 'user'"
            )
        yield db


@pytest.mark.parametrize("batch_size", BATCH_SIZES)
def test_a_property_the_first_row_lacks_is_kept(events_db, batch_size):
    q = "SELECT FROM Event ORDER BY n"
    expected = {
        "n": [1, 2, 3],
        "kind": ["start", "stop", "stop"],
        "reason": [None, "timeout", "user"],
    }

    columns = events_db.query("sql", q).to_columns(batch_size=batch_size)
    assert list(columns) == ["n", "kind", "reason"]
    assert {k: list(v) for k, v in columns.items()} == expected

    assert events_db.query("sql", q).to_arrow(batch_size=batch_size).to_pydict() == (
        expected
    )


def test_to_dataframe_keeps_the_late_property(events_db):
    frame = events_db.query("sql", "SELECT FROM Event ORDER BY n").to_dataframe()
    assert list(frame.columns) == ["n", "kind", "reason"]
    assert pd.isna(frame["reason"].iloc[0])  # pandas shows a missing string as NaN
    assert frame["reason"].tolist()[1:] == ["timeout", "user"]
    # the same rows in the other order always kept it
    other = events_db.query("sql", "SELECT FROM Event ORDER BY n DESC").to_dataframe()
    assert sorted(other.columns) == sorted(frame.columns)


@pytest.mark.parametrize("batch_size", BATCH_SIZES)
def test_a_late_numeric_property_keeps_its_type(temp_db_path, batch_size):
    """`level` is absent from the first rows: to_columns follows pandas (float64
    with NaN), to_arrow keeps int64 with nulls, at every batch size."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE Reading")
        with db.transaction():
            for n in (1, 2, 3):
                db.command("sql", "INSERT INTO Reading SET n = ?", n)
            db.command("sql", "INSERT INTO Reading SET n = 4, level = 40")
            db.command("sql", "INSERT INTO Reading SET n = 5, level = 50")

        q = "SELECT FROM Reading ORDER BY n"
        level = db.query("sql", q).to_columns(batch_size=batch_size)["level"]
        assert level.dtype.kind == "f"
        assert [None if math.isnan(x) else x for x in level] == [
            None,
            None,
            None,
            40.0,
            50.0,
        ]

        table = db.query("sql", q).to_arrow(batch_size=batch_size)
        assert table.schema.field("level").type == pa.int64()
        assert table.column("level").to_pylist() == [None, None, None, 40, 50]


@pytest.fixture
def readings_db(temp_db_path):
    """`level` is an int on rows 1, 2, 5 and missing on rows 3 and 4; `tags` is a
    list that is empty on rows 2 and 4."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE Reading")
        rows = [
            (1, 10, ["a"]),
            (2, 20, []),
            (3, None, ["b", "c"]),
            (4, None, []),
            (5, 50, ["d"]),
        ]
        with db.transaction():
            for n, level, tags in rows:
                if level is None:
                    db.command(
                        "sql", "INSERT INTO Reading SET n = ?, tags = ?", n, tags
                    )
                else:
                    db.command(
                        "sql",
                        "INSERT INTO Reading SET n = ?, level = ?, tags = ?",
                        n,
                        level,
                        tags,
                    )
        yield db


@pytest.mark.parametrize("batch_size", BATCH_SIZES)
def test_arrow_column_types_do_not_depend_on_batch_size(readings_db, batch_size):
    """With batch_size=2 the middle batch has no `level` at all and with
    batch_size=1 one batch holds only an empty list: both used to turn the
    column into strings (#114)."""
    table = readings_db.query("sql", "SELECT FROM Reading ORDER BY n").to_arrow(
        batch_size=batch_size
    )
    assert table.schema.field("level").type == pa.int64()
    assert table.column("level").to_pylist() == [10, 20, None, None, 50]
    assert table.schema.field("tags").type == pa.list_(pa.string())
    assert table.column("tags").to_pylist() == [["a"], [], ["b", "c"], [], ["d"]]


@pytest.mark.parametrize("batch_size", BATCH_SIZES)
def test_to_columns_does_not_depend_on_batch_size(readings_db, batch_size):
    level = readings_db.query("sql", "SELECT FROM Reading ORDER BY n").to_columns(
        batch_size=batch_size
    )["level"]
    assert level.dtype.kind == "f"
    assert [None if math.isnan(x) else x for x in level] == [10, 20, None, None, 50]


def test_a_column_of_mixed_types_becomes_strings_in_arrow(temp_db_path):
    """An int in one row and a string in the next, inside one batch, raised a raw
    pyarrow ArrowInvalid from to_arrow while every other reader returned (#114)."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE Reading")
        with db.transaction():
            db.command("sql", "INSERT INTO Reading SET n = 6, mixed = 1")
            db.command("sql", "INSERT INTO Reading SET n = 7, mixed = 'seven'")
        q = "SELECT n, mixed FROM Reading ORDER BY n"

        assert db.query("sql", q).to_list() == [
            {"n": 6, "mixed": 1},
            {"n": 7, "mixed": "seven"},
        ]
        assert db.query("sql", q).to_columns()["mixed"] == [1, "seven"]
        table = db.query("sql", q).to_arrow()
        assert table.schema.field("mixed").type == pa.string()
        assert table.column("mixed").to_pylist() == ["1", "seven"]


DECIMAL_CASES = {
    "36 digits": [Decimal("12345678901234567890.123456789012345678")],
    "whole amounts only": [Decimal("2"), Decimal("3")],
    "whole and fractional": [Decimal("2"), Decimal("2.5")],
    "a whole amount above 2**63": [Decimal("100000000000000000000")],
    "with a null": [Decimal("1.25"), None, Decimal("-3.5")],
}


@pytest.fixture
def price_db(temp_db_path):
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE Price")
        db.command("sql", "CREATE PROPERTY Price.amount DECIMAL")
        yield db


def _load_prices(db, values):
    with db.transaction():
        db.command("sql", "DELETE FROM Price")
    with db.transaction():
        for n, value in enumerate(values):
            if value is None:
                db.command("sql", "INSERT INTO Price SET n = ?", n)
            else:
                db.command("sql", "INSERT INTO Price SET n = ?, amount = ?", n, value)


@pytest.mark.parametrize("batch_size", [25_000, 1])
@pytest.mark.parametrize("case", list(DECIMAL_CASES))
def test_a_decimal_column_keeps_every_digit(price_db, case, batch_size):
    """DECIMAL came back as JSON numbers: digits lost to a double, the dtype
    following the data (int64 for whole amounts), and an OverflowError from
    to_arrow above 2**63 (#115)."""
    values = DECIMAL_CASES[case]
    _load_prices(price_db, values)
    q = "SELECT n, amount FROM Price ORDER BY n"

    column = price_db.query("sql", q).to_columns(batch_size=batch_size)["amount"]
    assert list(column) == values
    assert all(v is None or type(v) is Decimal for v in column)

    frame = price_db.query("sql", q).to_dataframe()
    assert frame["amount"].tolist() == values

    table = price_db.query("sql", q).to_arrow(batch_size=batch_size)
    assert pa.types.is_decimal(table.schema.field("amount").type)
    assert table.column("amount").to_pylist() == values


def test_decimal_batches_of_different_scale_unify(price_db):
    """One batch of whole amounts and one of fractions are decimal128(1, 0) and
    decimal128(2, 1) on their own; the column ends up as one decimal type."""
    values = [Decimal("2"), Decimal("2.5"), Decimal("30")]
    _load_prices(price_db, values)
    table = price_db.query("sql", "SELECT amount FROM Price ORDER BY n").to_arrow(
        batch_size=1
    )
    assert pa.types.is_decimal(table.schema.field("amount").type)
    assert table.column("amount").to_pylist() == values


def test_a_decimal_wider_than_decimal128_stays_exact(price_db):
    """decimal128 holds 38 digits and decimal256 76: a 41-digit value is a
    decimal256 column, an 80-digit one an exact string, never a double."""
    wide = Decimal("1234567890123456789012345678901234567890.5")
    _load_prices(price_db, [wide])
    q = "SELECT amount FROM Price"
    table = price_db.query("sql", q).to_arrow()
    assert table.schema.field("amount").type == pa.decimal256(41, 1)
    assert table.column("amount").to_pylist() == [wide]

    wider = Decimal("1" * 80)
    _load_prices(price_db, [wider])
    table = price_db.query("sql", q).to_arrow()
    assert table.schema.field("amount").type == pa.string()
    assert table.column("amount").to_pylist() == [str(wider)]
    assert price_db.query("sql", q).to_columns()["amount"][0] == wider


def test_export_to_csv_header_is_the_union_of_the_rows(events_db, tmp_path):
    """The header came from the first row, so a property it lacked raised "dict
    contains fields not in fieldnames" after part of the file was written (#113)."""
    import csv

    from arcadedb_embedded.exporter import export_to_csv

    path = str(tmp_path / "events.csv")
    export_to_csv(events_db.query("sql", "SELECT FROM Event ORDER BY n"), path)
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert list(rows[0]) == ["n", "kind", "reason"]
    assert [r["reason"] for r in rows] == ["", "timeout", "user"]

    listed = str(tmp_path / "listed.csv")
    export_to_csv([{"a": 1}, {"a": 2, "b": 3}, {"c": 4}], listed)
    with open(listed, newline="", encoding="utf-8") as f:
        assert next(csv.reader(f)) == ["a", "b", "c"]


def test_export_to_csv_names_a_column_that_first_appears_after_the_header(
    temp_db_path, tmp_path
):
    """Across batches the header cannot grow: the export says which column
    arrived too late and what to do, not a bare writer error. The batch size of
    the export is 10,000 rows, so the last of 10,001 rows is in the second."""
    from arcadedb_embedded.exporter import export_to_csv

    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE Row")
        rows = [{"n": n} for n in range(10_000)] + [{"n": 10_000, "late": "x"}]
        db.insert_many("Row", rows)
        results = db.query("sql", "SELECT FROM Row ORDER BY n")
        with pytest.raises(arcadedb.ArcadeDBError, match="'late' first appears"):
            export_to_csv(results, str(tmp_path / "late.csv"))


@pytest.mark.parametrize("batch_size", BATCH_SIZES)
def test_pinned_columns_are_read_as_given(events_db, batch_size):
    """`columns=` reads exactly those columns, in that order: a row lacking one
    reads null and a property not listed is left out."""
    q = "SELECT FROM Event ORDER BY n"
    columns = events_db.query("sql", q).to_columns(
        batch_size=batch_size, columns=["reason", "n"]
    )
    assert list(columns) == ["reason", "n"]
    assert list(columns["reason"]) == [None, "timeout", "user"]
    assert list(columns["n"]) == [1, 2, 3]

    table = events_db.query("sql", q).to_arrow(
        batch_size=batch_size, columns=["n", "missing"]
    )
    assert table.column_names == ["n", "missing"]
    assert table.column("n").to_pylist() == [1, 2, 3]
    assert table.column("missing").to_pylist() == [None, None, None]
