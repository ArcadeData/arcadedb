"""Engine findings about declared properties that reach Python users (known-issues.md).

Each finding has a test of its documented workaround, which must keep passing, and a test
of the engine behavior itself. While the engine bug is open that test is a strict `xfail`, a
tripwire as `test_restore_sql.py` used for #6096: when the fix reaches the wheel it starts
passing and the suite fails. Once the fix is in the engine the wheel packages, the `xfail`
comes off and the test asserts the fixed behavior.

Upstream: ArcadeData/arcadedb #9014 and #9027 (a value that cannot be converted was stored
as NULL, and '' as 0; fixed in 26.10.1 by PR #9121), #9017 (CREATE PROPERTY mandatory +
notnull was accepted over records that lack the property, then ORDER BY dropped them; fixed
in 26.10.1 by PR #9116, which refuses the declaration), #9021 (an index on INTEGER answered
for a fractional bound as if it were rounded; fixed in 26.10.1 by PR #9126).
"""

import math

import arcadedb_embedded as arcadedb
import pytest


def _five_records_one_without_v(db, name):
    db.command("sql", f"CREATE DOCUMENT TYPE {name}")
    with db.transaction():
        for i in range(1, 5):
            db.command(
                "sql", f"INSERT INTO {name} SET id = {i}, v = {i}"
            )  # nosec B608 - fixed names
        db.command("sql", f"INSERT INTO {name} SET id = 5")  # nosec B608 - fixed names


def _ids_by_v(db, name):
    query = f"SELECT id FROM {name} ORDER BY v"  # nosec B608 - fixed names
    return sorted(r.get("id") for r in db.query("sql", query))


def test_constraints_over_missing_values_are_refused(temp_db_path):
    """#9017, fixed in 26.10.1: CREATE PROPERTY (mandatory, notnull) over records that lack
    the property is refused, and no property is left behind."""
    with arcadedb.create_database(temp_db_path) as db:
        _five_records_one_without_v(db, "Declared")
        with pytest.raises(
            Exception
        ):  # noqa: B017 - the engine's CommandExecutionException
            db.command(
                "sql",
                "CREATE PROPERTY Declared.v INTEGER (mandatory true, notnull true)",
            )
        assert not db.schema.get_type("Declared").existsProperty("v")
        assert _ids_by_v(db, "Declared") == [1, 2, 3, 4, 5]


def test_give_every_record_the_property_before_declaring_constraints(temp_db_path):
    """known-issues.md: fill the missing values first, then declare the constraints."""
    with arcadedb.create_database(temp_db_path) as db:
        _five_records_one_without_v(db, "Repaired")
        with db.transaction():
            db.command("sql", "UPDATE Repaired SET v = 0 WHERE v IS NULL")
        db.command(
            "sql", "CREATE PROPERTY Repaired.v INTEGER (mandatory true, notnull true)"
        )
        db.command("sql", "CREATE INDEX ON Repaired (v) NOTUNIQUE")
        assert _ids_by_v(db, "Repaired") == [1, 2, 3, 4, 5]


def _indexed_and_plain_integers(db):
    for name in ("Indexed", "Plain"):
        db.command("sql", f"CREATE DOCUMENT TYPE {name}")
        db.command("sql", f"CREATE PROPERTY {name}.i INTEGER")
    db.command("sql", "CREATE INDEX ON Indexed (i) NOTUNIQUE")
    with db.transaction():
        for value in (11, 12, 13):
            db.command(
                "sql", f"INSERT INTO Indexed SET i = {value}"
            )  # nosec B608 - fixed names
            db.command(
                "sql", f"INSERT INTO Plain SET i = {value}"
            )  # nosec B608 - fixed names


def _matching(db, name, condition, bound):
    query = f"SELECT i FROM {name} WHERE {condition}"  # nosec B608 - fixed names
    return sorted(r.get("i") for r in db.query("sql", query, {"b": bound}))


@pytest.mark.parametrize(
    "condition, expected",
    [("i = :b", []), ("i >= :b", [13]), ("i < :b", [11, 12])],
)
def test_index_agrees_with_scan_for_a_fractional_bound(
    temp_db_path, condition, expected
):
    """#9021, fixed in 26.10.1: an index on an INTEGER answers for the exact bound, so a
    float bound of 12.5 returns the rows the unindexed scan returns. The plan check keeps
    the query on the index, so the test cannot pass by comparing a scan with a scan."""
    with arcadedb.create_database(temp_db_path) as db:
        _indexed_and_plain_integers(db)
        query = f"EXPLAIN SELECT i FROM Indexed WHERE {condition}"  # nosec B608
        plan = db.query("sql", query, {"b": 12.5}).first().get("executionPlanAsString")
        assert "FETCH FROM INDEX" in plan
        assert _matching(db, "Indexed", condition, 12.5) == expected
        assert _matching(db, "Plain", condition, 12.5) == expected


def test_rounded_bound_workaround_for_a_fractional_bound(temp_db_path):
    """known-issues.md: ceil for >= and <, floor for > and <=, and no equality query for a
    bound that is not an integer."""
    bound = 12.5
    rounded = {
        "i >= :b": math.ceil(bound),
        "i > :b": math.floor(bound),
        "i <= :b": math.floor(bound),
        "i < :b": math.ceil(bound),
    }
    with arcadedb.create_database(temp_db_path) as db:
        _indexed_and_plain_integers(db)
        for condition, integer_bound in rounded.items():
            assert _matching(db, "Indexed", condition, integer_bound) == _matching(
                db, "Plain", condition, bound
            ), condition


def _stored_integer(db, value):
    """Write `value` to a declared INTEGER with Document.set and read it back (None when stored NULL)."""
    with db.transaction():
        doc = db.new_document("N").set("tag", "x").set("i", value)
        doc.save()
    row = db.query("sql", "SELECT i FROM N WHERE tag = 'x'").first()
    with db.transaction():
        db.command("sql", "DELETE FROM N")
    return row.get("i")


@pytest.mark.parametrize(
    "value", [True, [1, 2], {"a": 1}], ids=["bool", "list", "dict"]
)
def test_an_inconvertible_value_is_refused_not_stored_as_null(temp_db_path, value):
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE N")
        db.command("sql", "CREATE PROPERTY N.i INTEGER")
        with pytest.raises(
            Exception
        ):  # noqa: B017 - the engine's Java IllegalArgumentException
            _stored_integer(db, value)


def test_an_empty_string_is_not_stored_as_zero(temp_db_path):
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE N")
        db.command("sql", "CREATE PROPERTY N.i INTEGER")
        try:
            with db.transaction():
                db.command("sql", "INSERT INTO N SET tag = 'x', i = :v", {"v": ""})
        except Exception:  # noqa: BLE001 - a refusal is what a fixed engine does
            return
        assert db.query("sql", "SELECT i FROM N WHERE tag = 'x'").first().get("i") != 0


def _to_int(value):
    """known-issues.md: convert in Python first; an empty string means no value."""
    return None if value == "" else int(value)


def test_convert_in_python_workaround_for_inconvertible_values(temp_db_path):
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE N")
        db.command("sql", "CREATE PROPERTY N.i INTEGER")
        for bad in ([1, 2], {"a": 1}):
            with pytest.raises(TypeError):
                _to_int(bad)
        assert _stored_integer(db, _to_int("7")) == 7
        assert _stored_integer(db, _to_int(True)) == 1
        assert _stored_integer(db, _to_int("")) is None
