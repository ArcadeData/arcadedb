"""Per-call paths of Database.query()/command(), ResultSet.first(), Result.get() (#280).

Parameters that cross as they are (a lone dict of str keys, or positional values, each an
exact scalar type or a Java array) go to the engine through ``DbCalls`` in one JPype call.
Everything here pins that the answer is the one the plain path (a ``Map`` or ``Object[]``
built in Python) gives: the same bound values with the same Java classes, the same overload
choice, the same errors, and the same result-set state after ``first()``.
"""

import arcadedb_embedded as arcadedb
import jpype
import numpy as np
import pytest
from arcadedb_embedded import core, results
from arcadedb_embedded.core import Database
from arcadedb_embedded.exceptions import ArcadeDBError


@pytest.fixture
def db(temp_db):
    temp_db.command("sql", "CREATE DOCUMENT TYPE T")
    with temp_db.transaction():
        for key in range(5):
            temp_db.command("sql", "INSERT INTO T SET k = ?, v = ?", key, key * 10)
    return temp_db


def _bound_classes(row, names):
    """The Java class name of each bound parameter as the engine stored it."""
    return {name: type(row.get_raw(name)).__name__ for name in names}


def _plain_query(db, command, args):
    """The same statement through the plain path: a Map or Object[] built in Python."""
    java_rs = db.get_java_database().query(
        "sql", command, Database._java_parameters(args)
    )
    return results.ResultSet(java_rs, db)


SCALARS = {
    "i": 7,
    "big": 2**40,
    "f": 1.5,
    "s": "text",
    "b": True,
    "n": None,
}
PROJECTION = "SELECT :i AS i, :big AS big, :f AS f, :s AS s, :b AS b, :n AS n"


def test_named_scalars_bind_as_the_plain_path_binds_them(db):
    glued = db.query("sql", PROJECTION, SCALARS).first()
    plain = _plain_query(db, PROJECTION, (SCALARS,)).first()
    assert glued.to_dict() == plain.to_dict() == SCALARS
    assert _bound_classes(glued, SCALARS) == _bound_classes(plain, SCALARS)


def test_positional_scalars_bind_as_the_plain_path_binds_them(db):
    command = "SELECT ? AS a, ? AS b, ? AS c, ? AS d, ? AS e, ? AS f"
    values = (7, 2**40, 1.5, "text", True, None)
    glued = db.query("sql", command, *values).first()
    plain = _plain_query(db, command, values).first()
    names = "abcdef"
    assert glued.to_dict() == plain.to_dict() == dict(zip(names, values))
    assert _bound_classes(glued, names) == _bound_classes(plain, names)


@pytest.mark.parametrize(
    "args",
    [
        pytest.param(([7, 8],), id="lone list is the positional list"),
        pytest.param(((7, 8),), id="lone tuple is the positional list"),
        pytest.param((7, 8), id="two args"),
    ],
)
def test_positional_spellings_bind_the_same(db, args):
    row = db.query("sql", "SELECT ? AS a, ? AS b", *args).first()
    assert row.to_dict() == {"a": 7, "b": 8}


def test_mapping_in_a_lone_list_is_still_the_named_map(db):
    assert db.query("sql", "SELECT :a AS a", [{"a": 3}]).first().get("a") == 3


def test_empty_dict_binds_nothing(db):
    assert db.query("sql", "SELECT 1 AS one", {}).first().get("one") == 1
    assert db.command("sql", "SELECT 1 AS one", {}).first().get("one") == 1


def test_command_with_named_parameters_updates(db):
    with db.transaction():
        count = db.command(
            "sql", "UPDATE T SET v = :v WHERE k = :k", {"v": 99, "k": 3}
        ).first()
    assert count.get("count") == 1
    assert db.query("sql", "SELECT v FROM T WHERE k = 3").first().get("v") == 99


def test_command_with_positional_parameters_updates(db):
    with db.transaction():
        count = db.command("sql", "UPDATE T SET v = ? WHERE k = ?", 98, 2).first()
    assert count.get("count") == 1
    assert db.query("sql", "SELECT v FROM T WHERE k = 2").first().get("v") == 98


def test_a_java_array_value_binds_in_both_spellings(db):
    vec = arcadedb.to_java_float_array(np.array([1.0, 2.0, 3.0], dtype=np.float32))
    named = db.query("sql", "SELECT :e AS e", {"e": vec}).first().to_dict()
    positional = db.query("sql", "SELECT ? AS e, ? AS n", vec, 1).first().to_dict()
    assert list(named["e"]) == [1.0, 2.0, 3.0]
    assert list(positional["e"]) == [1.0, 2.0, 3.0] and positional["n"] == 1


@pytest.mark.parametrize("lone", [None, "java-array"])
def test_a_lone_null_or_lone_java_array_keeps_the_plain_path(db, lone):
    # JPype would read either as the varargs array itself, so neither takes DbCalls.
    value = None if lone is None else arcadedb.to_java_float_array([1.0, 2.0])
    assert (
        core._through_bridge(
            "query", db.get_java_database(), "sql", "SELECT ? AS x", (value,)
        )
        is core._NO_GLUE
    )
    row = db.query("sql", "SELECT ? AS x", value).first()
    assert (
        (row.get("x") is None) if lone is None else (list(row.get("x")) == [1.0, 2.0])
    )


@pytest.mark.parametrize(
    "args",
    [
        pytest.param(({"d": __import__("decimal").Decimal("1.5")},), id="Decimal"),
        pytest.param(({1: "x"},), id="non-str key"),
        pytest.param((np.int64(3),), id="numpy scalar"),
        pytest.param(([1, 2], [3]), id="collections among args"),
    ],
)
def test_other_values_take_the_plain_path(db, args):
    assert (
        core._through_bridge("query", db.get_java_database(), "sql", "SELECT 1", args)
        is core._NO_GLUE
    )


def test_numpy_bool_still_binds_as_a_bool(db):
    row = db.query("sql", "SELECT ? AS ok, ? AS n", np.bool_(True), 1).first()
    assert row.get("ok") is True


def test_errors_keep_their_wrapper(db):
    with pytest.raises(ArcadeDBError, match="Query failed"):
        db.query("nosuchlanguage", "SELECT 1", {"a": 1})
    with pytest.raises(ArcadeDBError, match="Command failed"):
        db.command("sql", "INSERT INTO NoSuchType SET a = :a", {"a": 1})
    with pytest.raises(ArcadeDBError, match="Command failed"):
        db.command("sql", "INSERT INTO NoSuchType SET a = ?", 1)


def test_closed_database_still_raises(temp_db):
    temp_db.close()
    with pytest.raises(ArcadeDBError, match="Database is closed"):
        temp_db.query("sql", "SELECT 1", {"a": 1})
    with pytest.raises(ArcadeDBError, match="Database is closed"):
        temp_db.command("sql", "SELECT 1", 1)


def test_without_the_bridge_class_the_plain_path_answers_the_same(db, monkeypatch):
    monkeypatch.setattr(core, "_DB_CALLS", False)
    assert db.query("sql", "SELECT :a AS a", {"a": 1}).first().get("a") == 1
    assert db.query("sql", "SELECT ? AS a, ? AS b", 1, None).first().to_dict() == {
        "a": 1,
        "b": None,
    }
    with db.transaction():
        assert (
            db.command("sql", "UPDATE T SET v = ? WHERE k = ?", 5, 1)
            .first()
            .get("count")
            == 1
        )


# ---- ResultSet.first()


def test_first_returns_the_first_row_and_closes_the_rest(db):
    rs = db.query("sql", "SELECT FROM T ORDER BY k")
    assert rs.first().get("k") == 0
    assert rs._closed
    with pytest.raises(ArcadeDBError, match="closed before all its rows"):
        list(rs)


def test_first_on_an_empty_result_is_none_and_reads_as_empty(db):
    rs = db.query("sql", "SELECT FROM T WHERE k = 99")
    assert rs.first() is None
    assert rs._closed and rs._exhausted
    assert list(rs) == []
    assert rs.first() is None


def test_first_twice_on_a_partly_read_set_raises(db):
    rs = db.query("sql", "SELECT FROM T ORDER BY k")
    assert rs.first().get("k") == 0
    with pytest.raises(ArcadeDBError, match="closed before all its rows"):
        rs.first()


def test_first_after_the_set_was_read_to_its_end_is_none(db):
    rs = db.query("sql", "SELECT FROM T WHERE k = 1")
    assert [r.get("k") for r in rs] == [1]
    assert rs.first() is None


def test_first_on_a_closed_database_raises_for_an_unread_set(db):
    rs = db.query("sql", "SELECT FROM T ORDER BY k")
    db.close()
    with pytest.raises(ArcadeDBError, match="Database is closed"):
        rs.first()


def test_first_without_the_bridge_class_is_the_same(db, monkeypatch):
    monkeypatch.setitem(results._BRIDGE_CLASSES, "RowAccess", None)
    rs = db.query("sql", "SELECT FROM T ORDER BY k")
    assert rs.first().get("k") == 0 and rs._closed
    empty = db.query("sql", "SELECT FROM T WHERE k = 99")
    assert empty.first() is None and empty._exhausted


# ---- Result.get()


def test_get_returns_present_absent_and_null_properties(db):
    with db.transaction():
        db.command("sql", "INSERT INTO T SET k = 50, n = null, s = 'x'")
    row = db.query("sql", "SELECT FROM T WHERE k = 50").first()
    assert row.get("s") == "x"
    assert row.get("n") is None
    assert row.get("missing") is None
    assert row.get_raw("missing") is None
    assert row.has_property("n") and not row.has_property("missing")


def test_get_on_a_record_after_close_still_raises(db):
    row = db.query("sql", "SELECT FROM T WHERE k = 1").first()
    db.close()
    with pytest.raises(ArcadeDBError, match="Database is closed"):
        row.get("v")


def test_get_without_the_bridge_class_is_the_same(db, monkeypatch):
    monkeypatch.setitem(results._BRIDGE_CLASSES, "RowAccess", None)
    row = db.query("sql", "SELECT FROM T WHERE k = 1").first()
    assert row.get("v") == 10 and row.get("missing") is None


# ---- cached array classes


@pytest.mark.parametrize(
    "vector",
    [
        pytest.param(np.array([0.5, 1.5], dtype=np.float32), id="float32"),
        pytest.param(np.array([0.5, 1.5], dtype=np.float64), id="float64"),
        pytest.param([0.5, 1.5], id="list"),
        pytest.param((0.5, 1.5), id="tuple"),
        pytest.param(iter([0.5, 1.5]), id="iterator"),
    ],
)
def test_float_array_conversion_gives_a_float_array(vector):
    out = arcadedb.to_java_float_array(vector)
    assert str(out.getClass().getName()) == "[F"
    assert list(out) == [0.5, 1.5]
    assert type(out) is type(arcadedb.to_java_float_array([1.0]))


# ---- lookup_by_key() and Document.wrap()


@pytest.fixture
def keyed(temp_db):
    temp_db.command("sql", "CREATE DOCUMENT TYPE D")
    temp_db.command("sql", "CREATE PROPERTY D.k INTEGER")
    temp_db.command("sql", "CREATE INDEX ON D (k) UNIQUE")
    temp_db.command("sql", "CREATE VERTEX TYPE Vx")
    temp_db.command("sql", "CREATE PROPERTY Vx.k INTEGER")
    temp_db.command("sql", "CREATE INDEX ON Vx (k) UNIQUE")
    temp_db.command("sql", "CREATE EDGE TYPE Ed")
    temp_db.command("sql", "CREATE PROPERTY Ed.k INTEGER")
    temp_db.command("sql", "CREATE INDEX ON Ed (k) UNIQUE")
    with temp_db.transaction():
        temp_db.command("sql", "INSERT INTO D SET k = 1, name = 'doc'")
        a = temp_db.command(
            "sql", "CREATE VERTEX Vx SET k = 1, name = 'vertex'"
        ).first()
        b = temp_db.command("sql", "CREATE VERTEX Vx SET k = 2").first()
        temp_db.command(
            "sql",
            f"CREATE EDGE Ed FROM {a.get_rid()} TO {b.get_rid()} SET k = 1, name = 'edge'",
        )
    return temp_db


@pytest.mark.parametrize(
    "type_name, wrapper, name",
    [("D", "Document", "doc"), ("Vx", "Vertex", "vertex"), ("Ed", "Edge", "edge")],
)
def test_lookup_by_key_wraps_the_record_by_its_kind(keyed, type_name, wrapper, name):
    record = keyed.lookup_by_key(type_name, ["k"], [1])
    assert type(record).__name__ == wrapper
    assert record.get("name") == name


@pytest.mark.parametrize("type_name", ["D", "Vx", "Ed"])
def test_lookup_by_key_without_a_match_is_none(keyed, type_name):
    assert keyed.lookup_by_key(type_name, ["k"], [99]) is None


def test_lookup_by_key_without_the_bridge_class_is_the_same(keyed, monkeypatch):
    monkeypatch.setattr(core, "_DB_CALLS", False)
    assert keyed.lookup_by_key("D", ["k"], [1]).get("name") == "doc"
    assert keyed.lookup_by_key("D", ["k"], [99]) is None


def test_lookup_by_key_on_an_unknown_type_raises_arcadedb_error(keyed):
    with pytest.raises(ArcadeDBError, match="Failed to lookup by key in 'Nope'"):
        keyed.lookup_by_key("Nope", ["k"], [1])


def test_lookup_by_key_record_raises_after_close(keyed):
    record = keyed.lookup_by_key("D", ["k"], [1])
    keyed.close()
    with pytest.raises(ArcadeDBError, match="Database is closed"):
        record.get("name")
