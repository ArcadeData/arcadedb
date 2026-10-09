"""Engine findings about openCypher count push-downs and BYTE aggregates that reach Python users
(known-issues.md), now all fixed in ArcadeDB 26.11.1.

Each finding has a test of its documented workaround, which must keep passing, and a test of the
engine behavior itself that asserts the answer the row pipeline gives. While an engine bug is open
that test is a strict `xfail`, a tripwire as in `test_null_index_known_issues.py`: when the fix
reaches the wheel it starts passing and the suite fails, and the test is then converted to a plain
one.

#9277, #9278 and #9281 (ArcadeData/arcadedb#9288) and #9290 (ArcadeData/arcadedb#9299) are fixed in
26.11.1, which is the engine this tree builds against, so their tests are plain regression tests
now. They fail on the 26.10.1 engine, the one in the released wheel; known-issues.md says so.

Where a query is still answered by the `COUNT ANTI-JOIN CHAIN` push-down (the LSQB Q9 shape, the
only shape the engine verifies against the row pipeline since #9299) the test first checks the
plan, so a plan that stops using the push-down fails the suite instead of passing by comparing the
row pipeline with itself. Every other shape takes the row pipeline since #9299, so those tests
assert the count only.

Upstream: ArcadeData/arcadedb #9277 (the push-down counts the wrong vertices when the far end of the
chain has another label than the first hop's target; the change for #9203 let `id(a) <> id(b)` reach
it), #9278 (the push-down ignores the property map of the negated pattern's relationship), #9281
(`sum()` and `avg()` over a `BYTE` property raise `IllegalArgumentException`), all three fixed by
#9288; #9290 (the push-down counts wrong for chains with more than two hops of one type, an
inequality between other nodes, no inequality, a negated pattern away from the first node, or an
unlabelled node with the first node as the target), fixed by #9299 (the push-down now applies only
to the verified shape).
"""

import arcadedb_embedded as arcadedb
import pytest

# ---------------------------------------------------------------------------------------------
# #9277: another label at the far end of the chain (fixed in 26.11.1, #9288)
# ---------------------------------------------------------------------------------------------

CROSS_CHAIN = "MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Employee)-[:HAS_INTEREST]->(t:Tag) "


def _cross_label_graph(db):
    """One path (x0:Person)-(x1:Person)-(y0:Employee)-(t:Tag) in which x0 and y0 are not
    connected, so the path passes `NOT (p1)-[:KNOWS]-(p3)` and the count is 1."""
    for ddl in (
        "CREATE VERTEX TYPE Person",
        "CREATE VERTEX TYPE Employee",
        "CREATE VERTEX TYPE Tag",
        "CREATE EDGE TYPE KNOWS",
        "CREATE EDGE TYPE HAS_INTEREST",
    ):
        db.command("sql", ddl)
    with db.transaction():
        x0 = db.new_vertex("Person").set("id", 0).save()
        x1 = db.new_vertex("Person").set("id", 1).save()
        y0 = db.new_vertex("Employee").set("id", 0).save()
        tag = db.new_vertex("Tag").save()
        x0.new_edge("KNOWS", x1).save()
        x1.new_edge("KNOWS", y0).save()
        y0.new_edge("HAS_INTEREST", tag).save()


def _count(db, query):
    return db.query("opencypher", query).first().get("n")


def _require_anti_join_plan(db, query):
    plan = (
        db.query("opencypher", f"EXPLAIN {query}").first().get("executionPlanAsString")
    )
    if "ANTI-JOIN" not in plan:
        pytest.fail(
            f"{query!r} is no longer planned through the COUNT ANTI-JOIN CHAIN push-down; if the "
            f"engine now answers it another way, check the answer and update the test:\n{plan}"
        )


@pytest.mark.parametrize(
    "where",
    [
        pytest.param("NOT (p1)-[:KNOWS]-(p3)", id="negated-pattern"),
        pytest.param("NOT (p1)-[:KNOWS]-(p3) AND p1 <> p3", id="node-inequality"),
        pytest.param("NOT (p1)-[:KNOWS]-(p3) AND id(p1) <> id(p3)", id="id-inequality"),
        pytest.param(
            "id(p1) <> id(p3) AND NOT (p1)-[:KNOWS]-(p3)", id="id-inequality-first"
        ),
    ],
)
def test_negated_pattern_in_a_chain_with_another_label_at_the_far_end_counts_the_path(
    temp_db_path, where
):
    """#9277: the single path passes the negated pattern, so the count is 1, as the same WHERE
    after a `WITH` gives."""
    with arcadedb.create_database(temp_db_path) as db:
        _cross_label_graph(db)
        written = f"{CROSS_CHAIN}WHERE {where} RETURN count(*) AS n"  # nosec B608
        if "<>" in where:
            # Since #9299 the push-down needs the inequality between the negated pattern's nodes;
            # without it the engine answers through the row pipeline, and the count is the test.
            _require_anti_join_plan(db, written)
        assert _count(db, written) == 1


@pytest.mark.parametrize(
    "where",
    [
        pytest.param("NOT (p1)-[:KNOWS]-(p3)", id="negated-pattern"),
        pytest.param("NOT (p1)-[:KNOWS]-(p3) AND p1 <> p3", id="node-inequality"),
        pytest.param("NOT (p1)-[:KNOWS]-(p3) AND id(p1) <> id(p3)", id="id-inequality"),
        pytest.param(
            "id(p1) <> id(p3) AND NOT (p1)-[:KNOWS]-(p3)", id="id-inequality-first"
        ),
    ],
)
def test_with_before_the_where_gives_the_row_pipeline_count(temp_db_path, where):
    """known-issues.md (#9277): `WITH p1, p2, p3, t` before the WHERE keeps the query off the
    push-down and gives the right count on every engine."""
    with arcadedb.create_database(temp_db_path) as db:
        _cross_label_graph(db)
        row_pipeline = f"{CROSS_CHAIN}WITH p1, p2, p3, t WHERE {where} RETURN count(*) AS n"  # nosec B608
        assert _count(db, row_pipeline) == 1


def test_a_chain_with_the_same_label_throughout_is_not_affected(temp_db_path):
    """#9277 needs a far end with another label: with the same label on every node the
    push-down answers as the row pipeline does."""
    with arcadedb.create_database(temp_db_path) as db:
        for ddl in (
            "CREATE VERTEX TYPE Person",
            "CREATE VERTEX TYPE Tag",
            "CREATE EDGE TYPE KNOWS",
            "CREATE EDGE TYPE HAS_INTEREST",
        ):
            db.command("sql", ddl)
        with db.transaction():
            x0 = db.new_vertex("Person").set("id", 0).save()
            x1 = db.new_vertex("Person").set("id", 1).save()
            x2 = db.new_vertex("Person").set("id", 2).save()
            tag = db.new_vertex("Tag").save()
            x0.new_edge("KNOWS", x1).save()
            x1.new_edge("KNOWS", x2).save()
            x2.new_edge("HAS_INTEREST", tag).save()
        chain = (
            "MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person)"
            "-[:HAS_INTEREST]->(t:Tag) "
        )
        where = "NOT (p1)-[:KNOWS]-(p3) AND p1 <> p3"
        written = f"{chain}WHERE {where} RETURN count(*) AS n"  # nosec B608
        _require_anti_join_plan(db, written)
        row_pipeline = f"{chain}WITH p1, p2, p3, t WHERE {where} RETURN count(*) AS n"  # nosec B608
        assert _count(db, written) == _count(db, row_pipeline) == 1


# ---------------------------------------------------------------------------------------------
# #9278: the property map of the negated pattern's relationship (fixed in 26.11.1, #9288)
# ---------------------------------------------------------------------------------------------

EDGE_CHAIN = "MATCH (x:P)-[:K]->(y:P)-[:K]->(z:P)-[:L]->(t:Q) "


def _edge_property_graph(db):
    """P x -K-> P y -K-> P z -L-> Q t, and one more edge x -K {w: 0}-> z."""
    for ddl in (
        "CREATE VERTEX TYPE P",
        "CREATE VERTEX TYPE Q",
        "CREATE EDGE TYPE K",
        "CREATE EDGE TYPE L",
    ):
        db.command("sql", ddl)
    with db.transaction():
        x = db.new_vertex("P").set("id", 0).save()
        y = db.new_vertex("P").set("id", 1).save()
        z = db.new_vertex("P").set("id", 2).save()
        t = db.new_vertex("Q").set("id", 0).save()
        x.new_edge("K", y).save()
        y.new_edge("K", z).save()
        z.new_edge("L", t).save()
        x.new_edge("K", z, w=0).save()


@pytest.mark.parametrize(
    "where",
    [
        pytest.param("NOT (x)-[:K {w: 1}]->(z)", id="map"),
        pytest.param("NOT (x)-[:K {w: 1}]->(z) AND x <> z", id="map-and-inequality"),
    ],
)
def test_negated_pattern_with_a_property_map_counts_the_path(temp_db_path, where):
    """#9278: the direct edge has w = 0, so no edge with w = 1 connects x to z and the path
    passes: the count is 1, as the same WHERE after a `WITH` gives."""
    with arcadedb.create_database(temp_db_path) as db:
        _edge_property_graph(db)
        # No plan check: since #9288 the engine declines the push-down for a negated pattern with a
        # property map and answers it through the row pipeline, so the query is no longer planned
        # through `COUNT ANTI-JOIN CHAIN`; the count is what the test is about.
        written = f"{EDGE_CHAIN}WHERE {where} RETURN count(*) AS n"  # nosec B608
        assert _count(db, written) == 1


@pytest.mark.parametrize(
    "where, expected",
    [
        pytest.param("NOT (x)-[:K {w: 1}]->(z)", 1, id="map-w1"),
        pytest.param(
            "NOT (x)-[:K {w: 1}]->(z) AND x <> z", 1, id="map-w1-and-inequality"
        ),
        pytest.param("NOT (x)-[:K {w: 0}]->(z)", 0, id="map-w0"),
        pytest.param("NOT (x)-[:K]->(z)", 0, id="no-map"),
    ],
)
def test_with_before_the_where_honours_the_property_map(temp_db_path, where, expected):
    """known-issues.md (#9278): after a `WITH` the negated pattern keeps its property map."""
    with arcadedb.create_database(temp_db_path) as db:
        _edge_property_graph(db)
        row_pipeline = f"{EDGE_CHAIN}WITH x, y, z, t WHERE {where} RETURN count(*) AS n"  # nosec B608
        assert _count(db, row_pipeline) == expected


@pytest.mark.parametrize(
    "where",
    [
        pytest.param("NOT (x)-[:K {w: 0}]->(z)", id="map-w0"),
        pytest.param("NOT (x)-[:K]->(z)", id="no-map"),
    ],
)
def test_negated_pattern_the_map_does_not_change_is_counted_right(temp_db_path, where):
    """Where the property map does not change the answer (the direct edge has w = 0, or there is
    no map) the count is 0, as the row pipeline gives. Since #9299 the push-down needs an inequality
    between the negated pattern's nodes, which neither case has, so the row pipeline answers both
    and there is no plan check.
    """
    with arcadedb.create_database(temp_db_path) as db:
        _edge_property_graph(db)
        written = f"{EDGE_CHAIN}WHERE {where} RETURN count(*) AS n"  # nosec B608
        assert _count(db, written) == 0


# ---------------------------------------------------------------------------------------------
# #9281: sum() and avg() over a BYTE property (fixed in 26.11.1, #9288)
# ---------------------------------------------------------------------------------------------


def _byte_types(db):
    db.command("sql", "CREATE DOCUMENT TYPE T")
    db.command("sql", "CREATE PROPERTY T.b BYTE")
    db.command("sql", "CREATE PROPERTY T.s SHORT")
    db.command("sql", "CREATE VERTEX TYPE V")
    db.command("sql", "CREATE PROPERTY V.b BYTE")
    with db.transaction():
        for value in (100, 50):
            db.command(
                "sql", f"INSERT INTO T SET b = {value}, s = {value}"
            )  # nosec B608
            db.command("sql", f"CREATE VERTEX V SET b = {value}")  # nosec B608


def _result_or_error(db, language, query, reported_error="Cannot increment value"):
    """The value of `r`, or the text of the error the issue reports, so the tripwire's
    comparison fails with it as with a wrong value; any other exception fails the suite.
    """
    try:
        return db.query(language, query).first().get("r")
    except Exception as exc:  # noqa: BLE001 - re-raised unless it is the reported one
        if reported_error not in str(exc):
            raise
        return f"raised: {exc}"


@pytest.mark.parametrize(
    "language, query, expected",
    [
        pytest.param("sql", "SELECT sum(b) AS r FROM T", 150, id="sql-sum"),
        pytest.param("sql", "SELECT avg(b) AS r FROM T", 75.0, id="sql-avg"),
        pytest.param(
            "opencypher", "MATCH (n:V) RETURN sum(n.b) AS r", 150, id="cypher-sum"
        ),
    ],
)
def test_sum_and_avg_over_a_byte_property(temp_db_path, language, query, expected):
    """#9281: the two rows hold 100 and 50, so the sum is 150 and the average 75.0, as for a
    SHORT property."""
    with arcadedb.create_database(temp_db_path) as db:
        _byte_types(db)
        assert _result_or_error(db, language, query) == expected


def test_sum_and_avg_over_a_short_property_are_not_affected(temp_db_path):
    """The same two values in a SHORT property sum and average as expected."""
    with arcadedb.create_database(temp_db_path) as db:
        _byte_types(db)
        assert _result_or_error(db, "sql", "SELECT sum(s) AS r FROM T") == 150
        assert _result_or_error(db, "sql", "SELECT avg(s) AS r FROM T") == 75.0


@pytest.mark.parametrize(
    "language, query, expected",
    [
        pytest.param("sql", "SELECT sum(b.asInteger()) AS r FROM T", 150, id="sql-sum"),
        pytest.param(
            "sql", "SELECT avg(b.asInteger()) AS r FROM T", 75.0, id="sql-avg"
        ),
        pytest.param(
            "opencypher",
            "MATCH (n:V) RETURN sum(toInteger(n.b)) AS r",
            150,
            id="cypher-sum",
        ),
    ],
)
def test_converting_a_byte_to_an_integer_before_the_aggregate_works(
    temp_db_path, language, query, expected
):
    """known-issues.md (#9281): `b.asInteger()` in SQL and `toInteger(n.b)` in openCypher
    give the aggregate a value type it can add."""
    with arcadedb.create_database(temp_db_path) as db:
        _byte_types(db)
        assert _result_or_error(db, language, query) == expected


# ---------------------------------------------------------------------------------------------
# #9290: other shapes of the chain (fixed in 26.11.1, #9299)
# ---------------------------------------------------------------------------------------------

_P3 = "MATCH (p0:Person)-[:KNOWS]-(p1:Person)-[:KNOWS]-(p2:Person)-[:HAS_INTEREST]->(t:Tag) "
_P4 = (
    "MATCH (p0:Person)-[:KNOWS]-(p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person) "
)
_P3_UNLABELLED = (
    "MATCH (p0:Person)-[:KNOWS]-(p1)-[:KNOWS]-(p2:Person)-[:HAS_INTEREST]->(t:Tag) "
)


def _shapes_graph(db):
    """Four Person a, b, c, d and one Tag t; KNOWS a-b, b-c, c-d, a-c (stored one way, matched both
    ways); every Person has HAS_INTEREST to t. The expected counts below enumerate the walks with
    distinct relationships (Cypher semantics) over this edge list."""
    for ddl in (
        "CREATE VERTEX TYPE Person",
        "CREATE VERTEX TYPE Tag",
        "CREATE EDGE TYPE KNOWS",
        "CREATE EDGE TYPE HAS_INTEREST",
    ):
        db.command("sql", ddl)
    with db.transaction():
        people = [db.new_vertex("Person").set("id", i).save() for i in range(4)]
        tag = db.new_vertex("Tag").save()
        for a, b in ((0, 1), (1, 2), (2, 3), (0, 2)):
            people[a].new_edge("KNOWS", people[b]).save()
        for person in people:
            person.new_edge("HAS_INTEREST", tag).save()


# (match, variables kept by the WITH, where, expected count)
_SHAPES = [
    pytest.param(
        _P4,
        "p0, p1, p2, p3",
        "NOT (p0)-[:KNOWS]-(p2) AND p0 <> p2",
        2,
        id="three-hops-of-one-type",
    ),
    pytest.param(
        _P3,
        "p0, p1, p2, t",
        "NOT (p0)-[:KNOWS]-(p2) AND p1 <> p2",
        4,
        id="inequality-between-other-nodes",
    ),
    pytest.param(_P3, "p0, p1, p2, t", "NOT (p0)-[:KNOWS]-(p2)", 4, id="no-inequality"),
    pytest.param(
        _P4,
        "p0, p1, p2, p3",
        "NOT (p1)-[:KNOWS]-(p3) AND p1 <> p3",
        2,
        id="negated-pattern-away-from-the-first-node",
    ),
    pytest.param(
        _P3_UNLABELLED,
        "p0, p1, p2, t",
        "NOT (p2)-[:KNOWS]-(p0) AND p2 <> p0",
        4,
        id="unlabelled-node-and-first-node-as-target",
    ),
]


@pytest.mark.parametrize("match, variables, where, expected", _SHAPES)
def test_the_push_down_counts_the_other_shapes_like_the_row_pipeline(
    temp_db_path, match, variables, where, expected
):
    """#9290: each shape counts what the row pipeline counts. Since #9299 the engine takes the row
    pipeline for every shape but the verified one, so there is no plan check here."""
    with arcadedb.create_database(temp_db_path) as db:
        _shapes_graph(db)
        written = f"{match}WHERE {where} RETURN count(*) AS n"  # nosec B608
        assert _count(db, written) == expected


@pytest.mark.parametrize("match, variables, where, expected", _SHAPES)
def test_with_before_the_where_gives_the_row_pipeline_count_for_the_other_shapes(
    temp_db_path, match, variables, where, expected
):
    """known-issues.md (#9290): `WITH <the chain's variables>` before the WHERE keeps the query
    off the push-down and gives the right count."""
    with arcadedb.create_database(temp_db_path) as db:
        _shapes_graph(db)
        row_pipeline = (
            f"{match}WITH {variables} WHERE {where} RETURN count(*) AS n"  # nosec B608
        )
        assert _count(db, row_pipeline) == expected


def test_the_two_hop_shape_the_push_down_is_written_for_is_not_affected(temp_db_path):
    """#9290: two hops, the negated pattern between the first and third node, the inequality between
    the same two, and a label on every node (the LSQB Q9 shape) counts right through the push-down.
    """
    with arcadedb.create_database(temp_db_path) as db:
        _shapes_graph(db)
        written = f"{_P3}WHERE NOT (p0)-[:KNOWS]-(p2) AND p0 <> p2 RETURN count(*) AS n"  # nosec B608
        _require_anti_join_plan(db, written)
        assert _count(db, written) == 4
