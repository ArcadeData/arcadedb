"""Type-faithful batched row transport and the graph and vector per-call glue (#285).

``ResultSet.to_list()`` and ``iter_dicts()`` read rows through the bridge's ``TypedRows``: Java writes
JSON, tags the values JSON cannot carry exactly, and refers to everything else by index into a side
array of the engine's own objects. Every test here pins that the answer is the one the row-by-row
path (``Result.to_dict()``, which converts each value in Python) gives: the same Python type and
value for every property type.
"""

import datetime as dt
import math
from decimal import Decimal

import arcadedb_embedded as arcadedb
import jpype
import numpy as np
import pytest
from arcadedb_embedded import results
from arcadedb_embedded.exceptions import ArcadeDBError


def exact(value):
    """A string that differs when the type or the value differs (1 vs 1.0 vs True, naive vs aware)."""
    if isinstance(value, dict):
        return "{" + ",".join(f"{exact(k)}:{exact(v)}" for k, v in value.items()) + "}"
    if isinstance(value, (list, tuple)):
        return type(value).__name__ + "[" + ",".join(exact(v) for v in value) + "]"
    if isinstance(value, (set, frozenset)):
        return "set{" + ",".join(sorted(exact(v) for v in value)) + "}"
    if isinstance(value, float) and math.isnan(value):
        return "float:nan"
    if isinstance(value, dt.datetime):
        return f"datetime:{value.isoformat()}|{value.tzinfo!r}"
    return f"{type(value).__module__}.{type(value).__name__}:{value!r}"


def reference(db, query):
    """The rows through the row-by-row path: one Result.to_dict() per row."""
    return [row.to_dict() for row in db.query("sql", query)]


def assert_same(db, query):
    expected = [exact(r) for r in reference(db, query)]
    assert [exact(r) for r in db.query("sql", query).to_list()] == expected
    assert [exact(r) for r in db.query("sql", query).iter_dicts()] == expected
    return len(expected)


PROPERTIES = [
    ("bo", "BOOLEAN"),
    ("bt", "BYTE"),
    ("sh", "SHORT"),
    ("i", "INTEGER"),
    ("l", "LONG"),
    ("f", "FLOAT"),
    ("x", "DOUBLE"),
    ("n", "DECIMAL"),
    ("s", "STRING"),
    ("d", "DATE"),
    ("t", "DATETIME"),
    ("ts", "DATETIME_SECOND"),
    ("tm", "DATETIME_MICROS"),
    ("tn", "DATETIME_NANOS"),
    ("bin", "BINARY"),
    ("li", "LIST"),
    ("ma", "MAP"),
    ("lk", "LINK"),
    ("af", "ARRAY_OF_FLOATS"),
    ("ai", "ARRAY_OF_INTEGERS"),
    ("al", "ARRAY_OF_LONGS"),
    ("ash", "ARRAY_OF_SHORTS"),
    ("ad", "ARRAY_OF_DOUBLES"),
]


@pytest.fixture
def db(temp_db):
    temp_db.command("sql", "CREATE DOCUMENT TYPE Emb")
    temp_db.command("sql", "CREATE DOCUMENT TYPE Wide")
    temp_db.command("sql", "CREATE VERTEX TYPE VT")
    for name, kind in PROPERTIES:
        temp_db.command("sql", f"CREATE PROPERTY Wide.{name} {kind}")
    temp_db.command("sql", "CREATE PROPERTY Wide.em EMBEDDED OF Emb")
    with temp_db.transaction():
        vertex = temp_db.new_vertex("VT").save()
        temp_db.command(
            "sql",
            "INSERT INTO Wide SET bo = true, bt = -5, sh = 300, i = -7, l = 9007199254740993, "
            "f = 0.1, x = 1e-7, n = :n, s = :s, d = :d, t = :t, ts = :t, tm = :tm, tn = :tm, "
            "bin = :bin, li = [1, 'a', null, 2.5, {'k': [1, 2]}], "
            "ma = {'a': 1, 'b': {'c': [1, 2, 3]}, 'dd': '2020-01-01'}, "
            "em = {'@type': 'Emb', 'q': 1}, lk = :lk, af = :af, ai = [1, 2, 3], al = [4, 5], "
            "ash = [1, 2], ad = [0.5, 0.25]",
            {
                "n": Decimal("12345678901234567890.123456789"),
                "s": 'q"uote\\ \n\t\u0001 é 日本 \U0001f600',
                "d": dt.date(1999, 12, 31),
                "t": dt.datetime(2021, 2, 3, 4, 5, 6, 789000),
                "tm": dt.datetime(2021, 2, 3, 4, 5, 6, 789123),
                "bin": b"\x00\x01\xff",
                "lk": vertex.get_identity(),
                "af": arcadedb.to_java_float_array([0.1, 0.2]),
            },
        )
        temp_db.command("sql", "INSERT INTO Wide SET s = null")
        temp_db.command(
            "sql",
            "INSERT INTO Wide SET x = :x, f = :f",
            {"x": float("nan"), "f": float("inf")},
        )
        temp_db.command(
            "sql",
            "INSERT INTO Wide SET x = -0.0, i = 2147483647, l = -9223372036854775808, n = :n",
            {"n": Decimal("1E+3")},
        )
        temp_db.command("sql", "INSERT INTO Wide SET s = ''")
        temp_db.command(
            "sql",
            "INSERT INTO Wide SET d = :d, t = :t, tm = :t",
            {"d": dt.date(1, 1, 1), "t": dt.datetime(1, 1, 1, 0, 0, 0, 1)},
        )
        temp_db.command(
            "sql",
            "INSERT INTO Wide SET d = :d, t = :t",
            {
                "d": dt.date(9999, 12, 31),
                "t": dt.datetime(9999, 12, 31, 23, 59, 59, 999999),
            },
        )
    return temp_db


def test_every_property_type_comes_back_as_the_row_by_row_path_gives_it(db):
    assert assert_same(db, "SELECT FROM Wide") == 7


@pytest.mark.parametrize(
    "query",
    [
        "SELECT FROM Wide LIMIT 1",
        "SELECT FROM Wide WHERE 1 = 0",
        "SELECT FROM VT",
        "SELECT count(*) AS c, list(s) AS ls, set(s) AS ss FROM Wide",
        "SELECT [1, 2] AS a, {'x': {'y': 1}} AS m, 1 / 3.0 AS q, date('2020-01-01') AS d, 2.5 AS dec",
        "SELECT s AS `\u0001`, i FROM Wide LIMIT 3",
    ],
)
def test_other_result_shapes(db, query):
    assert_same(db, query)


@pytest.mark.parametrize("rows", [1, 999, 1000, 1001, 2000, 2500])
def test_batch_boundaries(temp_db, rows):
    temp_db.command("sql", "CREATE DOCUMENT TYPE B")
    with temp_db.transaction():
        for i in range(rows):
            temp_db.command("sql", "INSERT INTO B SET i = ?, s = ?", i, f"r{i}")
    assert assert_same(temp_db, "SELECT FROM B ORDER BY i") == rows


def _in_memory(rows):
    """A result set over rows built in Java, for values SQL cannot produce."""
    internal = jpype.JClass("com.arcadedb.query.sql.executor.InternalResultSet")()
    result_internal = jpype.JClass("com.arcadedb.query.sql.executor.ResultInternal")
    hash_map = jpype.JClass("java.util.LinkedHashMap")
    for row in rows:
        java_row = result_internal()
        for key, value in row.items():
            java_row.setProperty(key, value)
        internal.add(java_row)
    return internal


def test_java_values_sql_cannot_produce(db):
    time = jpype.JClass("java.time.Instant")
    zoned = jpype.JClass("java.time.ZonedDateTime")
    offset = jpype.JClass("java.time.OffsetDateTime")
    zone = jpype.JClass("java.time.ZoneId")
    big_integer = jpype.JClass("java.math.BigInteger")
    hash_map = jpype.JClass("java.util.HashMap")
    linked_set = jpype.JClass("java.util.LinkedHashSet")
    java_date = jpype.JClass("java.util.Date")
    character = jpype.JClass("java.lang.Character")
    instant = time.parse("2021-02-03T04:05:06.789123456Z")
    non_string_keys = hash_map()
    non_string_keys.put(jpype.JInt(1), "one")
    tag_key = hash_map()
    tag_key.put("\u0001", "tag")
    members = linked_set()
    members.add("a")
    members.add("b")
    rows = [
        {
            "instant": instant,
            "zoned": zoned.ofInstant(instant, zone.of("Asia/Seoul")),
            "offset": offset.parse("2021-02-03T04:05:06.5+09:00"),
            "big": big_integer("123456789012345678901234567890"),
            "util_date": java_date(1612325106789),
            "char": character("x"),
            "set": members,
            "non_string_keys": non_string_keys,
            "tag_key": tag_key,
            "ints": jpype.JArray(jpype.JInt)([1, 2]),
            "bools": jpype.JArray(jpype.JBoolean)([True, False]),
            "floats": jpype.JArray(jpype.JFloat)([0.5, float("nan")]),
            "emoji": "x \U0001f600 \u2028",
        },
        {"\u0001": 1, "other": 2},
    ]
    expected = [
        exact(r) for r in (r.to_dict() for r in results.ResultSet(_in_memory(rows), db))
    ]
    assert [
        exact(r) for r in results.ResultSet(_in_memory(rows), db).to_list()
    ] == expected
    assert [
        exact(r) for r in results.ResultSet(_in_memory(rows), db).iter_dicts()
    ] == expected


def test_value_python_cannot_hold_raises_as_before(db):
    far_future = jpype.JClass("java.time.Instant").parse("+10000-01-01T00:00:00Z")
    with pytest.raises(OverflowError):
        [r.to_dict() for r in results.ResultSet(_in_memory([{"t": far_future}]), db)]
    with pytest.raises(OverflowError):
        results.ResultSet(_in_memory([{"t": far_future}]), db).to_list()
    with pytest.raises(OverflowError):
        list(results.ResultSet(_in_memory([{"t": far_future}]), db).iter_dicts())


def test_object_array_raises_as_before(db):
    """The row-by-row path cannot convert an Object[]; the batched paths hand it to the same code."""
    strings = jpype.JArray(jpype.JString)(["a", "b"])
    with pytest.raises(BufferError):
        [r.to_dict() for r in results.ResultSet(_in_memory([{"a": strings}]), db)]
    with pytest.raises(BufferError):
        results.ResultSet(_in_memory([{"a": strings}]), db).to_list()
    with pytest.raises(BufferError):
        list(results.ResultSet(_in_memory([{"a": strings}]), db).iter_dicts())


def test_iter_dicts_reads_one_row_at_a_time(db):
    internal = _in_memory([{"n": i} for i in range(5)])
    rows = results.ResultSet(internal, db).iter_dicts()
    assert next(rows) == {"n": 0}
    remaining = 0
    while internal.hasNext():
        internal.next()
        remaining += 1
    assert remaining == 4


def test_iter_dicts_stopped_early_then_closed(db):
    result_set = db.query("sql", "SELECT FROM Wide")
    rows = result_set.iter_dicts()
    next(rows)
    result_set.close()
    with pytest.raises(ArcadeDBError, match="closed before all its rows"):
        next(rows)


def test_exhausted_result_set_reads_as_empty(db):
    result_set = db.query("sql", "SELECT FROM Wide")
    assert len(result_set.to_list()) == 7
    assert result_set.to_list() == []
    assert list(result_set.iter_dicts()) == []


def test_statement_error_surfaces_as_a_read_error(db):
    with pytest.raises(ArcadeDBError):
        db.query("sql", "SELECT FROM NoSuchType").to_list()
    with pytest.raises(ArcadeDBError):
        list(db.query("sql", "SELECT FROM NoSuchType").iter_dicts())


# ---- graph traversal and vector search results ----


@pytest.fixture
def graph(temp_db):
    temp_db.command("sql", "CREATE VERTEX TYPE P")
    temp_db.command("sql", "CREATE EDGE TYPE Knows")
    temp_db.command("sql", "CREATE EDGE TYPE Likes")
    with temp_db.transaction():
        people = [temp_db.new_vertex("P").set("n", i).save() for i in range(6)]
        for i in range(1, 6):
            people[0].new_edge("Knows", people[i], w=i).save()
        people[0].new_edge("Likes", people[3]).save()
        people[2].new_edge("Knows", people[0]).save()
    return temp_db, people


def _plain_edges(vertex, direction, *labels):
    java_direction = jpype.JClass("com.arcadedb.graph.Vertex$DIRECTION")
    iterable = vertex.get_java_document().getEdges(
        getattr(java_direction, direction), *labels
    )
    return [str(edge.getIdentity()) for edge in iterable]


@pytest.mark.parametrize(
    "labels", [(), ("Knows",), ("Likes",), ("Knows", "Likes"), ("Nope",)]
)
def test_vertex_edges_match_the_engine_iteration(graph, labels):
    _db, people = graph
    for method, direction in (
        ("get_out_edges", "OUT"),
        ("get_in_edges", "IN"),
        ("get_both_edges", "BOTH"),
    ):
        for vertex in people:
            try:
                expected = _plain_edges(vertex, direction, *labels)
            except Exception as exc:
                with pytest.raises(type(exc)):
                    getattr(vertex, method)(*labels)
                continue
            got = getattr(vertex, method)(*labels)
            assert [e.get_rid() for e in got] == expected
            assert all(isinstance(e, arcadedb.Edge) for e in got)


def test_vertex_edges_after_close_raise(graph):
    db, people = graph
    db.close()
    with pytest.raises(ArcadeDBError, match="closed"):
        people[0].get_out_edges()


def test_vector_search_hits_match_the_per_hit_path(temp_db):
    temp_db.command("sql", "CREATE VERTEX TYPE D")
    temp_db.command("sql", "CREATE PROPERTY D.e ARRAY_OF_FLOATS")
    rng = np.random.default_rng(3)
    with temp_db.transaction():
        for i in range(300):
            temp_db.command(
                "sql",
                "INSERT INTO D SET vid = ?, e = ?",
                i,
                arcadedb.to_java_float_array(rng.random(16, dtype=np.float32)),
            )
    index = temp_db.create_vector_index("D", "e", dimensions=16, id_property="vid")
    query = [float(x) for x in rng.random(16)]
    for k in (1, 10, 50):
        hits = index.find_nearest(query, k=k)
        assert len(hits) == k
        scores = [s for _r, s in hits]
        assert scores == sorted(scores)
        assert all(type(s) is float for s in scores)
        # the per-hit path: the same engine search, each hit wrapped one call at a time
        java_vector = arcadedb.to_java_float_array(query)
        pairs = next(iter(index._iter_lsm_indexes())).findNeighborsFromVector(
            java_vector, k
        )
        plain = [
            (temp_db._lookup_by_java_rid(p.getFirst()).get_rid(), float(p.getSecond()))
            for p in pairs
        ]
        plain.sort(key=lambda item: item[1])
        assert [(r.get_rid(), s) for r, s in hits] == plain
        assert all(isinstance(r, arcadedb.Vertex) for r, _s in hits)


def test_without_the_bridge_classes_the_plain_paths_answer_the_same(
    db, graph, monkeypatch
):
    """A jar without TypedRows or GraphCalls sends every caller down the plain path."""
    from arcadedb_embedded import graph as graph_module

    wide = [exact(r) for r in db.query("sql", "SELECT FROM Wide").to_list()]
    people_db, people = graph
    edges = [e.get_rid() for e in people[0].get_out_edges("Knows")]

    monkeypatch.setitem(results._BRIDGE_CLASSES, "TypedRows", None)
    monkeypatch.setattr(graph_module, "_GRAPH_CALLS", False)
    assert [exact(r) for r in db.query("sql", "SELECT FROM Wide").to_list()] == wide
    assert [exact(r) for r in db.query("sql", "SELECT FROM Wide").iter_dicts()] == wide
    assert [e.get_rid() for e in people[0].get_out_edges("Knows")] == edges
