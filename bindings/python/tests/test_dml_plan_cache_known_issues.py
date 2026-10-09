"""Positional parameters in UPDATE, DELETE, and SQL scripts that share a cached plan (known-issues.md).

ArcadeData/arcadedb#9245: since #9218, in 26.10.1 snapshots, the read side of an UPDATE or
DELETE is planned as a ``SELECT FROM <type> WHERE <where>`` and that plan is shared through
the execution plan cache under the SELECT's text, where every positional ``?`` prints the
same. ``UPDATE A SET brand = ? WHERE sku = ?`` run after ``SELECT FROM A WHERE sku = ?`` then
read its WHERE from the SET value: it changed the record whose sku is that value, not the one
asked for, and reported ``count=1``. Named parameters were not affected. 26.9.1 and snapshots
before #9218 are not affected. Fixed in 26.10.1 by PR #9252, which keys the source plan by the
text of its statement.

These tests were a strict ``xfail`` tripwire until the fix reached the engine they run on.
They assert the right answer on every engine: 26.9.1 and the snapshots before #9218 pass
because the regression did not exist yet, and the snapshots between #9218 and #9252 fail.
The cases besides the first are the other shapes the differential of the parameter binding
found: a script of two UPDATEs, an UPSERT after a SELECT, a DELETE after an UPDATE, and
``:name`` parameters bound by position.
"""

import arcadedb_embedded as arcadedb


def _products(db, name):
    db.command("sql", f"CREATE DOCUMENT TYPE {name}")
    db.command("sql", f"CREATE PROPERTY {name}.sku STRING")
    db.command("sql", f"CREATE PROPERTY {name}.brand STRING")
    db.command("sql", f"CREATE INDEX ON {name} (sku) UNIQUE")
    with db.transaction():
        for sku, brand in (("S1", "b1"), ("S2", "b2"), ("NEW", "b3")):
            db.command(
                "sql", f"INSERT INTO {name} SET sku = ?, brand = ?", sku, brand
            )  # nosec B608 - fixed names


def _brands(db, name):
    query = f"SELECT sku, brand FROM {name}"  # nosec B608 - fixed names
    return {r["sku"]: r["brand"] for r in db.query("sql", query).to_list()}


BEFORE = {"S1": "b1", "S2": "b2", "NEW": "b3"}
# The statement targets S2. NEW is the value it sets, so a WHERE that reads the SET
# parameter finds the record whose sku is NEW.
AFTER = {"S1": "b1", "S2": "NEW", "NEW": "b3"}


def test_positional_update_after_a_select_with_the_same_where(temp_db_path):
    """#9245, fixed in 26.10.1: the UPDATE changes S2, and the record whose sku is the SET
    value keeps its brand."""
    with arcadedb.create_database(temp_db_path) as db:
        _products(db, "A")
        assert _brands(db, "A") == BEFORE
        assert db.query("sql", "SELECT FROM A WHERE sku = ?", "S1").to_list()

        with db.transaction():
            count = db.command(
                "sql", "UPDATE A SET brand = ? WHERE sku = ?", "NEW", "S2"
            ).to_list()

        assert (count, _brands(db, "A")) == ([{"count": 1}], AFTER)


def test_positional_delete_after_an_update_with_the_same_where(temp_db_path):
    """#9245, fixed in 26.10.1: the DELETE removes S2. It read a parameter it did not have
    and deleted nothing, reporting a count of 0."""
    with arcadedb.create_database(temp_db_path) as db:
        _products(db, "D")
        with db.transaction():
            db.command("sql", "UPDATE D SET brand = ? WHERE sku = ?", "b1", "S1")

        with db.transaction():
            count = db.command("sql", "DELETE FROM D WHERE sku = ?", "S2").to_list()

        assert (count, _brands(db, "D")) == ([{"count": 1}], {"S1": "b1", "NEW": "b3"})


def test_script_of_two_updates_with_positional_parameters(temp_db_path):
    """#9245, fixed in 26.10.1: in one SQL script each UPDATE reads its own parameter. The
    second one read the parameter of the first, so S1 was updated twice and S2 not at all.
    """
    with arcadedb.create_database(temp_db_path) as db:
        _products(db, "A")
        script = (
            "UPDATE A SET brand = 'x1' WHERE sku = ?;"
            " UPDATE A SET brand = 'x2' WHERE sku = ?;"
        )

        with db.transaction():
            db.command("sqlscript", script, "S1", "S2")

        assert _brands(db, "A") == {"S1": "x1", "S2": "x2", "NEW": "b3"}


def test_positional_upsert_after_a_select_with_the_same_where(temp_db_path):
    """#9245, fixed in 26.10.1: the UPSERT of a sku no record has inserts S9. It read the
    SET value as its key, found NEW, and updated that record instead."""
    with arcadedb.create_database(temp_db_path) as db:
        _products(db, "B")
        assert db.query("sql", "SELECT FROM B WHERE sku = ?", "S1").to_list()

        with db.transaction():
            count = db.command(
                "sql", "UPDATE B SET brand = ? UPSERT WHERE sku = ?", "NEW", "S9"
            ).to_list()

        assert (count, _brands(db, "B")) == (
            [{"count": 1}],
            {"S1": "b1", "S2": "b2", "NEW": "b3", "S9": "NEW"},
        )


def test_named_parameters_bound_by_position(temp_db_path):
    """#9245, fixed in 26.10.1: `:name` parameters given as positional values bind by their
    order, and the UPDATE after a SELECT with the same WHERE changes S2."""
    with arcadedb.create_database(temp_db_path) as db:
        _products(db, "N")
        assert db.query("sql", "SELECT FROM N WHERE sku = :sku", "S1").to_list()

        with db.transaction():
            count = db.command(
                "sql", "UPDATE N SET brand = :brand WHERE sku = :sku", "NEW", "S2"
            ).to_list()

        assert (count, _brands(db, "N")) == ([{"count": 1}], AFTER)


def test_named_parameters_are_the_workaround(temp_db_path):
    """known-issues.md: the same SELECT and UPDATE with named parameters change S2."""
    with arcadedb.create_database(temp_db_path) as db:
        _products(db, "F")
        assert db.query(
            "sql", "SELECT FROM F WHERE sku = :sku", {"sku": "S1"}
        ).to_list()

        with db.transaction():
            count = db.command(
                "sql",
                "UPDATE F SET brand = :brand WHERE sku = :sku",
                {"brand": "NEW", "sku": "S2"},
            ).to_list()

        assert (count, _brands(db, "F")) == ([{"count": 1}], AFTER)
