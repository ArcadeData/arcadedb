"""insert_many, GraphBatch.create_vertices, and GraphBatch.new_edges send rows to the JVM as one JSON
string, and the engine reads it with its own JSON parser. That parser changes some values the
per-value entry points (Document.set, create_vertex, new_edge) store exactly or refuse: an integer
beyond 64 bits keeps its low 64 bits, NaN and the infinities become strings, a non-str dict key
becomes its text, and a lone surrogate becomes "?". A value the JSON text cannot carry unchanged
keeps the call off the bulk path, so every entry point stores the same value or raises.
"""

import json
import math

import arcadedb_embedded as arcadedb
import pytest
from arcadedb_embedded import type_conversion

ENTRY_POINTS = ["insert_many", "create_vertices", "new_edges"]


def _create_types(db):
    db.command("sql", "CREATE DOCUMENT TYPE D")
    db.command("sql", "CREATE VERTEX TYPE V")
    db.command("sql", "CREATE EDGE TYPE E")


def _write(db, entry, value):
    """Write {"v": value} through one bulk entry point and return everything stored under "v"."""
    if entry == "insert_many":
        db.insert_many("D", [{"v": value}])
        return [r.get("v") for r in db.query("sql", "SELECT v FROM D").to_list()]
    with db.transaction():
        a = db.new_vertex("V").save()
        b = db.new_vertex("V").save()
    with db.graph_batch(parallel_flush=False) as batch:
        if entry == "create_vertices":
            batch.create_vertices("V", [{"v": value}])
        else:
            batch.new_edges(
                [a.get_rid()], "E", [b.get_rid()], properties=[{"v": value}]
            )
    if entry == "create_vertices":
        rows = db.query("sql", "SELECT v FROM V WHERE v IS NOT NULL").to_list()
    else:
        rows = db.query("sql", "SELECT v FROM E").to_list()
    return [r.get("v") for r in rows]


@pytest.mark.parametrize("entry", ENTRY_POINTS)
@pytest.mark.parametrize("value", [2**63 - 1, -(2**63), 0, 2**53 + 1])
def test_integers_that_fit_64_bits_round_trip_exactly(temp_db_path, entry, value):
    with arcadedb.create_database(temp_db_path) as db:
        _create_types(db)
        assert _write(db, entry, value) == [value]


@pytest.mark.parametrize("entry", ENTRY_POINTS)
@pytest.mark.parametrize("value", [2**63, -(2**63) - 1, 2**64, 10**30])
def test_integers_beyond_64_bits_are_refused_not_wrapped(temp_db_path, entry, value):
    with arcadedb.create_database(temp_db_path) as db:
        _create_types(db)
        with pytest.raises(
            Exception
        ):  # noqa: B017 - OverflowError or ArcadeDBError, as the per-value paths raise
            _write(db, entry, value)
        stored = []
        for type_name in ("D", "V", "E"):
            query = f"SELECT v FROM {type_name} WHERE v IS NOT NULL"  # nosec B608 - fixed type names
            stored += [r.get("v") for r in db.query("sql", query).to_list()]
        assert stored == []


@pytest.mark.parametrize("entry", ENTRY_POINTS)
@pytest.mark.parametrize("value", [math.inf, -math.inf])
def test_infinities_stay_floats(temp_db_path, entry, value):
    with arcadedb.create_database(temp_db_path) as db:
        _create_types(db)
        assert _write(db, entry, value) == [value]


@pytest.mark.parametrize("entry", ENTRY_POINTS)
def test_nan_stays_a_float(temp_db_path, entry):
    with arcadedb.create_database(temp_db_path) as db:
        _create_types(db)
        (stored,) = _write(db, entry, math.nan)
        assert isinstance(stored, float) and math.isnan(stored)


@pytest.mark.parametrize("entry", ENTRY_POINTS)
def test_non_str_dict_keys_are_kept(temp_db_path, entry):
    with arcadedb.create_database(temp_db_path) as db:
        _create_types(db)
        assert _write(db, entry, {1: "a"}) == [{1: "a"}]


@pytest.mark.parametrize("entry", ENTRY_POINTS)
def test_a_lone_surrogate_raises_instead_of_becoming_a_question_mark(
    temp_db_path, entry
):
    with arcadedb.create_database(temp_db_path) as db:
        _create_types(db)
        with pytest.raises(
            Exception
        ):  # noqa: B017 - UnicodeEncodeError or ArcadeDBError
            _write(db, entry, "a\ud800b")


def test_ordinary_rows_stay_on_the_bulk_path(temp_db_path):
    rows = [
        {"id": i, "name": f"n{i}", "score": i * 0.5, "flag": i % 2 == 0, "note": None}
        for i in range(5)
    ]
    assert type_conversion.json_bulk_dumps(rows) == json.dumps(rows)
    assert all(
        type_conversion.json_bulk_scalar_ok(v) for row in rows for v in row.values()
    )
    with arcadedb.create_database(temp_db_path) as db:
        _create_types(db)
        with db.graph_batch(parallel_flush=False) as batch:
            assert len(batch._create_vertices_json_bulk("V", rows)) == len(rows)
            assert batch._create_vertices_json_bulk("V", rows + [{"id": 2**70}]) is None
            assert (
                batch._create_vertices_json_bulk("V", rows + [{"x": math.nan}]) is None
            )


@pytest.mark.parametrize(
    "value, ok",
    [
        (None, True),
        (True, True),
        (0, True),
        (2**63 - 1, True),
        (-(2**63), True),
        (2**63, False),
        (1.5, True),
        (math.nan, False),
        (math.inf, False),
        ("text", True),
        ("emoji \U0001f600", True),
        ("a\ud800b", False),
        (b"bytes", False),
    ],
)
def test_json_bulk_scalar_ok(value, ok):
    assert type_conversion.json_bulk_scalar_ok(value) is ok
