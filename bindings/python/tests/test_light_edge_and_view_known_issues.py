"""Engine findings about openCypher counts over light edges and over Graph Analytical Views that
reach Python users (known-issues.md), both fixed in ArcadeDB 26.11.1.

Each finding has plain tests of its documented workarounds, which must keep passing, plain tests of
the cases that are not affected, and plain regression tests of the engine behavior itself. They pass
on the engine the suite builds against (upstream's 26.11.1 snapshot, which has the fix) and fail on
the 26.10.1 wheel; known-issues.md says so. While the bugs were open the regression tests were strict
`xfail` tripwires, as in `test_count_pushdown_known_issues.py`: they started passing when the fix
reached the snapshot, and the suite failed until they were converted.

Upstream: ArcadeData/arcadedb #9378 (a one-hop `count(*)` answered 0 over the edges that
`GraphBatch.withLightEdges(true)` wrote into an edge type that is not declared `LIGHTWEIGHT`: the
count push-down plans `CONSTANT COUNT` because the type holds no records) and #9377 (a pattern
predicate over an edge type that a Graph Analytical View does not list was evaluated against the
view, where that type has no edges, so `NOT (a)-[:F]->(b)` excluded nothing and `(a)-[:F]->(b)`
matched nothing in an aggregate), both fixed by ArcadeData/arcadedb#9383. For #9378 the fix is that a
batch with `withLightEdges(true)` refuses an edge type that does not declare `LIGHTWEIGHT`, with an
`IllegalArgumentException` that `new_edge` raises as `ArcadeDBError`. A database that an older version
wrote is counted right since #9409 (closing #9389): the last two tests of the #9378 section build such a
database with the engine's `newLightEdge`.
"""

import time

import arcadedb_embedded as arcadedb
import pytest
from arcadedb_embedded.exceptions import ArcadeDBError

# ---------------------------------------------------------------------------------------------
# #9378: a one-hop count over light edges in an edge type that is not declared LIGHTWEIGHT
# ---------------------------------------------------------------------------------------------

ONE_HOP = "MATCH (a:V)-[:E]->(b:V) RETURN count(*) AS n"


def _light_edge_graph(db, edge_ddl="CREATE EDGE TYPE E", **batch_options):
    """Vertices a, b, c of type V and the edges a -E-> b and b -E-> c, loaded with a
    `graph_batch`, so two edges hang between three vertices."""
    db.command("sql", "CREATE VERTEX TYPE V")
    db.command("sql", edge_ddl)
    with db.graph_batch(**batch_options) as batch:
        a, b, c = batch.create_vertices("V", 3)
        batch.new_edge(a, "E", b)
        batch.new_edge(b, "E", c)


def _graph_an_older_version_loaded(db):
    """The graph that `graph_batch(light_edges=True)` wrote into an undeclared type on 26.10.1 and
    earlier: two light edges, and an edge type that holds no records. The batch refuses to write it
    now, so the edges are made with the engine's `newLightEdge`, which stores the same thing and is
    accepted by every engine."""
    db.command("sql", "CREATE VERTEX TYPE V")
    db.command("sql", "CREATE EDGE TYPE E")
    with db.transaction():
        a = db.new_vertex("V").save()
        b = db.new_vertex("V").save()
        c = db.new_vertex("V").save()
        a._java_document.newLightEdge("E", b._java_document)
        b._java_document.newLightEdge("E", c._java_document)


def _count(db, query, language="opencypher"):
    return db.query(language, query).first().get("n")


def _plan(db, query):
    return (
        db.query("opencypher", f"EXPLAIN {query}").first().get("executionPlanAsString")
    )


def _require_the_edges_are_there(db):
    """The edges are in the graph: the rows and the SQL traversal see them."""
    rows = len(db.query("opencypher", "MATCH (a:V)-[:E]->(b:V) RETURN a, b").to_list())
    sql = _count(
        db, "SELECT count(*) AS n FROM (SELECT expand(out('E')) FROM V)", "sql"
    )
    if (rows, sql) != (2, 2):
        pytest.fail(
            f"the edges are not all there: {rows} rows, {sql} by out('E'), want 2 and 2"
        )


def test_light_edges_for_an_undeclared_edge_type_are_refused(temp_db_path):
    """#9378, fixed in 26.11.1 (#9383): a batch with `light_edges=True` refuses to write an edge
    without properties into a type that does not declare `LIGHTWEIGHT`. `new_edge` raises
    `ArcadeDBError` that names the type and the two ways out, and no edge is written."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE VERTEX TYPE V")
        db.command("sql", "CREATE EDGE TYPE E")
        with pytest.raises(ArcadeDBError) as refused:
            with db.graph_batch(light_edges=True) as batch:
                a, b = batch.create_vertices("V", 2)
                batch.new_edge(a, "E", b)
        message = str(refused.value)
        assert "java.lang.IllegalArgumentException" in message
        assert "Edge type 'E' does not declare LIGHTWEIGHT" in message
        assert "CREATE EDGE TYPE E LIGHTWEIGHT" in message
        assert _count(db, ONE_HOP) == 0
        assert (
            _count(
                db, "SELECT count(*) AS n FROM (SELECT expand(out('E')) FROM V)", "sql"
            )
            == 0
        )


@pytest.mark.parametrize(
    "edge_ddl, batch_options",
    [
        pytest.param(
            "CREATE EDGE TYPE E LIGHTWEIGHT", {"light_edges": True}, id="declared-light"
        ),
        pytest.param(
            "CREATE EDGE TYPE E LIGHTWEIGHT",
            {"light_edges": False},
            id="declared-regular",
        ),
        pytest.param("CREATE EDGE TYPE E", {"light_edges": False}, id="flag-false"),
        pytest.param("CREATE EDGE TYPE E", {}, id="flag-not-passed"),
    ],
)
def test_one_hop_count_where_the_edges_and_the_type_agree(
    temp_db_path, edge_ddl, batch_options
):
    """known-issues.md (#9378): declaring the type `LIGHTWEIGHT` before the load, or not passing
    `light_edges=True`, gives the right one-hop count on every engine."""
    with arcadedb.create_database(temp_db_path) as db:
        _light_edge_graph(db, edge_ddl, **batch_options)
        _require_the_edges_are_there(db)
        assert _count(db, ONE_HOP) == 2


def test_light_edges_with_properties_in_an_undeclared_type_are_regular_edges(
    temp_db_path,
):
    """`light_edges=True` only makes edges without properties light, so edges with properties go
    into an undeclared type as before, and the one-hop count is right."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE VERTEX TYPE V")
        db.command("sql", "CREATE EDGE TYPE E")
        with db.graph_batch(light_edges=True) as batch:
            a, b, c = batch.create_vertices("V", 3)
            batch.new_edge(a, "E", b, w=1)
            batch.new_edge(b, "E", c, w=2)
        _require_the_edges_are_there(db)
        assert _count(db, ONE_HOP) == 2
        assert _count(db, "SELECT count(*) AS n FROM E", "sql") == 2


@pytest.mark.parametrize(
    "query",
    [
        pytest.param(
            "MATCH (a:V)-[:E]->(b:V) WITH a, b RETURN count(*) AS n", id="with"
        ),
        pytest.param(
            "MATCH (a:V)-[:E]->(b:V) RETURN count(b) AS n", id="count-of-a-node"
        ),
        pytest.param(
            "MATCH (a:V)-[r:E]->(b:V) RETURN count(r) AS n",
            id="count-of-the-relationship",
        ),
    ],
)
def test_other_ways_to_count_light_edges_an_older_version_loaded(temp_db_path, query):
    """known-issues.md (#9378): on a graph that an older version loaded with `light_edges=True` into
    an undeclared type, a `WITH a, b` before the count, `count(b)`, or `count(r)` over a named
    relationship is not planned as `CONSTANT COUNT` and gives the right count, on every engine.
    """
    with arcadedb.create_database(temp_db_path) as db:
        _graph_an_older_version_loaded(db)
        _require_the_edges_are_there(db)
        assert "CONSTANT COUNT" not in _plan(db, query)
        assert _count(db, query) == 2


def test_one_hop_count_over_light_edges_an_older_version_loaded(temp_db_path):
    """The right count is 2, as the rows and `out('E')` show. Until ArcadeData/arcadedb#9409 (merge
    9b6aaedd40, closing #9389) the push-down planned this CONSTANT COUNT and answered 0 for light edges
    an older version had written; this was a strict `xfail` tripwire then. The snapshot now counts
    them through the CSR chain-count push-down, so it is a plain regression test. It fails on the
    26.10.1 wheel."""
    with arcadedb.create_database(temp_db_path) as db:
        _graph_an_older_version_loaded(db)
        _require_the_edges_are_there(db)
        assert "CONSTANT COUNT" not in _plan(db, ONE_HOP)
        assert _count(db, ONE_HOP) == 2


# ---------------------------------------------------------------------------------------------
# #9377: a pattern predicate over an edge type that a Graph Analytical View does not list
# ---------------------------------------------------------------------------------------------

VIEW_CHAIN = "MATCH (a:V)-[:E]->(b:V) "


def _view_graph(db, view_edge_types):
    """Vertices x, y, z of type V and the edges x -E-> y, x -F-> y, y -E-> z, then a Graph
    Analytical View over V and `view_edge_types` (None for no view), waited on until it is READY.
    """
    for ddl in ("CREATE VERTEX TYPE V", "CREATE EDGE TYPE E", "CREATE EDGE TYPE F"):
        db.command("sql", ddl)
    with db.transaction():
        x = db.new_vertex("V").save()
        y = db.new_vertex("V").save()
        z = db.new_vertex("V").save()
        x.new_edge("E", y).save()
        x.new_edge("F", y).save()
        y.new_edge("E", z).save()
    if view_edge_types is None:
        return
    db.command(
        "sql",
        f"CREATE GRAPH ANALYTICAL VIEW gav VERTEX TYPES (V) EDGE TYPES ({view_edge_types}) "
        "UPDATE MODE OFF",
    )
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        row = db.query(
            "sql", "SELECT FROM schema:graphAnalyticalViews WHERE name = ?", "gav"
        ).first()
        if row is not None and row.get("status") == "READY":
            return
        time.sleep(0.02)
    pytest.fail("the Graph Analytical View did not reach READY in 120 s")


def _require_view_plan(db, query):
    plan = (
        db.query("opencypher", f"EXPLAIN {query}").first().get("executionPlanAsString")
    )
    if "GAV" not in plan:
        pytest.fail(
            f"{query!r} is no longer planned through the view; if the engine now answers it "
            f"another way, check the answer and update the test:\n{plan}"
        )


@pytest.mark.parametrize(
    "where, aggregate",
    [
        pytest.param("WHERE NOT (a)-[:F]->(b)", "count(*)", id="negated-count-star"),
        pytest.param("WHERE (a)-[:F]->(b)", "count(*)", id="positive-count-star"),
        pytest.param(
            "WHERE NOT (a)-[:F]->(b)", "count(a)", id="negated-count-of-a-node"
        ),
    ],
)
def test_pattern_predicate_over_an_edge_type_the_view_does_not_list(
    temp_db_path, where, aggregate
):
    """#9377, fixed in 26.11.1 (#9383): only the pair (x, y) has an F edge, so the negated form keeps the pair (y, z) and the
    positive form keeps (x, y): the count is 1 either way, as with no view."""
    with arcadedb.create_database(temp_db_path) as db:
        _view_graph(db, "E")
        query = (
            f"{VIEW_CHAIN}{where} RETURN {aggregate} AS n"  # nosec B608 - fixed text
        )
        _require_view_plan(db, query)
        assert _count(db, query) == 1


@pytest.mark.parametrize(
    "where",
    [
        pytest.param("WHERE NOT (a)-[:F]->(b)", id="negated"),
        pytest.param("WHERE (a)-[:F]->(b)", id="positive"),
    ],
)
def test_with_before_the_where_gives_the_row_pipeline_count_over_a_view(
    temp_db_path, where
):
    """known-issues.md (#9377): `WITH a, b` before the WHERE keeps the query on the row pipeline,
    which counts the right number with the view that does not list F, on every engine.
    """
    with arcadedb.create_database(temp_db_path) as db:
        _view_graph(db, "E")
        written = f"{VIEW_CHAIN}{where} RETURN count(*) AS n"  # nosec B608 - fixed text
        _require_view_plan(db, written)
        with_form = f"{VIEW_CHAIN}WITH a, b {where} RETURN count(*) AS n"  # nosec B608
        assert _count(db, with_form) == 1


@pytest.mark.parametrize(
    "where",
    [
        pytest.param("WHERE NOT (a)-[:F]->(b)", id="negated"),
        pytest.param("WHERE (a)-[:F]->(b)", id="positive"),
    ],
)
def test_a_view_that_lists_the_predicates_edge_type_counts_right(temp_db_path, where):
    """known-issues.md (#9377): a view that lists F as well as E still serves the query, and the
    count is right, on every engine."""
    with arcadedb.create_database(temp_db_path) as db:
        _view_graph(db, "E, F")
        query = f"{VIEW_CHAIN}{where} RETURN count(*) AS n"  # nosec B608 - fixed text
        _require_view_plan(db, query)
        assert _count(db, query) == 1


@pytest.mark.parametrize(
    "where",
    [
        pytest.param("WHERE NOT (a)-[:F]->(b)", id="negated"),
        pytest.param("WHERE (a)-[:F]->(b)", id="positive"),
    ],
)
def test_the_same_counts_with_no_view_are_right(temp_db_path, where):
    """#9377 needs a view: with none, the same query counts 1 on every engine."""
    with arcadedb.create_database(temp_db_path) as db:
        _view_graph(db, None)
        query = f"{VIEW_CHAIN}{where} RETURN count(*) AS n"  # nosec B608 - fixed text
        assert _count(db, query) == 1


def test_a_query_that_returns_rows_is_not_affected_by_the_view(temp_db_path):
    """#9377 was about an aggregate: the same negated query returning rows, over the view that does
    not list F, returns the one pair (y, z) on every engine."""
    with arcadedb.create_database(temp_db_path) as db:
        _view_graph(db, "E")
        rows = db.query(
            "opencypher", f"{VIEW_CHAIN}WHERE NOT (a)-[:F]->(b) RETURN a, b"
        ).to_list()
        assert len(rows) == 1
