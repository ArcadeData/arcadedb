"""Results and records after their Database is closed or dropped (#117)."""

import gc

import arcadedb_embedded as arcadedb
import pytest
from arcadedb_embedded.exceptions import ArcadeDBError

NAMES = ["Ann", "Bob", "Cy"]


@pytest.fixture
def people_path(temp_db_path):
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE VERTEX TYPE Person")
        with db.transaction():
            for name in NAMES:
                db.command("sql", "INSERT INTO Person SET name = ?", name)
    return temp_db_path


def test_reads_through_a_closed_database_raise(people_path):
    """Before the fix a record row came back as {}, a record property as None,
    and a plain scan raised a raw TransactionException."""
    db = arcadedb.open_database(people_path)
    rows = db.query("sql", "SELECT FROM Person ORDER BY name")
    lazy = db.query("sql", "SELECT FROM Person")
    row = db.query("sql", "SELECT FROM Person WHERE name = 'Ann'").first()
    vertex = row.get_vertex()
    assert vertex.to_dict() == {"name": "Ann"}
    db.close()

    with pytest.raises(ArcadeDBError, match="Database is closed"):
        rows.to_list()
    with pytest.raises(ArcadeDBError, match="Database is closed"):
        list(lazy)
    for read in (
        lambda: vertex.get("name"),
        lambda: vertex.to_dict(),
        lambda: vertex.get_property_names(),
        lambda: vertex.has_property("name"),
        lambda: vertex.get_out_edges(),
        lambda: row.get("name"),
        lambda: row.to_dict(),
        lambda: row.to_json(),
        lambda: row.get_vertex(),
        lambda: row.get_element(),
    ):
        with pytest.raises(ArcadeDBError, match="Database is closed"):
            read()


def test_a_projection_is_refused_too(people_path):
    """An unread projection result SET is refused too: its rows may still be read
    lazily from the engine, so none can be promised after close."""
    db = arcadedb.open_database(people_path)
    projected = db.query("sql", "SELECT name FROM Person ORDER BY name")
    db.close()
    with pytest.raises(ArcadeDBError, match="Database is closed"):
        projected.to_list()


def test_a_projection_or_command_row_stays_readable_after_close(people_path):
    """A row that holds its own values (a projection, a command result such as
    IMPORT DATABASE's) is readable after close: example 16 reads one, and
    refusing it was a regression the examples job caught."""
    db = arcadedb.open_database(people_path)
    projected = db.query("sql", "SELECT name FROM Person ORDER BY name").first()
    counted = db.command("sql", "SELECT count(*) AS n FROM Person").one()
    db.close()
    assert projected.get("name") == "Ann"
    assert projected.to_dict() == {"name": "Ann"}
    assert counted.get("n") == 3


def test_a_result_set_read_to_its_end_stays_empty_after_close(people_path):
    db = arcadedb.open_database(people_path)
    rows = db.query("sql", "SELECT FROM Person")
    assert len(rows.to_list()) == 3
    db.close()
    assert rows.to_list() == []


def _open_and_query(path):
    handle = arcadedb.open_database(path)  # dropped when the function returns
    rows = handle.query("sql", "SELECT FROM Person ORDER BY name")
    vertex = (
        handle.query("sql", "SELECT FROM Person WHERE name = 'Ann'")
        .first()
        .get_vertex()
    )
    return rows, vertex


def test_a_result_keeps_its_dropped_database_open(people_path):
    """Database.__del__ closed the database when the last reference to the
    wrapper went, so a function that returned its result returned [{}, {}, {}]."""
    rows, vertex = _open_and_query(people_path)
    gc.collect()
    assert [r["name"] for r in rows.to_list()] == NAMES
    assert vertex.to_dict() == {"name": "Ann"}


def test_the_kept_database_is_still_open_for_the_engine(people_path):
    """The consequence of the above: while a result is alive its database is
    open, and the engine refuses a second instance of the same path."""
    rows, vertex = _open_and_query(people_path)
    with pytest.raises(ArcadeDBError, match="already in use"):
        arcadedb.open_database(people_path)
    del rows, vertex
    gc.collect()
    with arcadedb.open_database(people_path) as db:
        assert db.count_type("Person") == 3


def test_the_database_is_released_once_its_results_are_gone(people_path):
    """The strong reference must not leak the database: with every result and
    record gone the wrapper's __del__ closes it, so the path can be opened
    again."""
    rows, vertex = _open_and_query(people_path)
    del rows, vertex
    gc.collect()
    with arcadedb.open_database(people_path) as db:
        assert db.count_type("Person") == 3
