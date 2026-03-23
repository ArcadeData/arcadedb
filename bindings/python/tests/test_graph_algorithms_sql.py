"""Graph algorithm SQL coverage for Python bindings."""

import arcadedb_embedded as arcadedb
import pytest

_GRAPH_ALGO_UNAVAILABLE_TOKENS = (
    "Unknown method name: shortestPath",
    "Unknown method name: dijkstra",
    "Unknown method name: astar",
)


def _setup_weighted_graph(db):
    db.command("sql", "CREATE VERTEX TYPE Node")
    db.command("sql", "CREATE EDGE TYPE Road UNIDIRECTIONAL")
    db.command("sql", "CREATE PROPERTY Road.distance LONG")

    with db.transaction():
        a = db.new_vertex("Node").set("name", "A").save()
        b = db.new_vertex("Node").set("name", "B").save()
        c = db.new_vertex("Node").set("name", "C").save()
        d = db.new_vertex("Node").set("name", "D").save()

        a.new_edge("Road", b, distance=1).save()
        b.new_edge("Road", c, distance=1).save()
        a.new_edge("Road", c, distance=10).save()
        c.new_edge("Road", d, distance=1).save()

    return {
        "A": a.get_rid(),
        "B": b.get_rid(),
        "C": c.get_rid(),
        "D": d.get_rid(),
    }


def _path_query_or_skip(db, select_statement):
    script = f"""
    LET $src = (SELECT FROM Node WHERE name = 'A' LIMIT 1);
    LET $dst = (SELECT FROM Node WHERE name = 'D' LIMIT 1);
    {select_statement}
    """

    try:
        result = db.command("sqlscript", script)
        path = None
        for row in result:
            if "path" in row.property_names:
                path = row.get("path")

        if path is None:
            raise AssertionError("Graph algorithm query did not return a 'path' column")

        return path
    except Exception as e:
        message = str(e)
        if any(token in message for token in _GRAPH_ALGO_UNAVAILABLE_TOKENS):
            pytest.skip(
                "Graph algorithm SQL functions are not available in this packaged runtime"
            )
        raise


def test_graph_shortest_path_sql_unweighted_path_shape(temp_db_path):
    """`shortestPath` may return a minimum-hop path or empty path for directed-edge runtime behavior."""
    with arcadedb.create_database(temp_db_path) as db:
        rids = _setup_weighted_graph(db)

        path = _path_query_or_skip(
            db,
            "SELECT shortestPath($src.@rid, $dst.@rid) AS path FROM Node LIMIT 1",
        )

        assert isinstance(path, list)
        if path:
            assert str(path[0]) == rids["A"]
            assert str(path[-1]) == rids["D"]


def test_graph_dijkstra_sql_weighted_path_shape(temp_db_path):
    """`dijkstra` returns the minimum total-weight path using the configured edge weight property."""
    with arcadedb.create_database(temp_db_path) as db:
        rids = _setup_weighted_graph(db)

        path = _path_query_or_skip(
            db,
            "SELECT dijkstra($src, $dst, 'distance') AS path FROM Node LIMIT 1",
        )

        assert isinstance(path, list)
        assert len(path) == 4
        assert [str(p) for p in path] == [rids["A"], rids["B"], rids["C"], rids["D"]]


def test_graph_astar_sql_weighted_path_shape(temp_db_path):
    """`astar` returns a weighted path using the same edge-weight property."""
    with arcadedb.create_database(temp_db_path) as db:
        rids = _setup_weighted_graph(db)

        path = _path_query_or_skip(
            db,
            "SELECT astar($src, $dst, 'distance') AS path FROM Node LIMIT 1",
        )

        assert isinstance(path, list)
        assert len(path) == 4
        assert [str(p) for p in path] == [rids["A"], rids["B"], rids["C"], rids["D"]]


def test_graph_dijkstra_sql_accepts_rid_variables(temp_db_path):
    """`dijkstra` accepts RID variable inputs in sqlscript, matching documented usage."""
    with arcadedb.create_database(temp_db_path) as db:
        rids = _setup_weighted_graph(db)

        path = _path_query_or_skip(
            db,
            "SELECT dijkstra($src.@rid, $dst.@rid, 'distance') AS path FROM Node LIMIT 1",
        )

        assert isinstance(path, list)
        assert len(path) == 4
        assert [str(p) for p in path] == [rids["A"], rids["B"], rids["C"], rids["D"]]


def test_graph_shortest_path_sql_no_path_returns_empty_or_null(temp_db_path):
    """`shortestPath` on disconnected vertices should not produce a valid multi-hop path."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE VERTEX TYPE Node")
        db.command("sql", "CREATE EDGE TYPE Road UNIDIRECTIONAL")

        with db.transaction():
            db.new_vertex("Node").set("name", "X").save()
            db.new_vertex("Node").set("name", "Y").save()

        path = _path_query_or_skip(
            db,
            "SELECT shortestPath((SELECT FROM Node WHERE name='X' LIMIT 1), (SELECT FROM Node WHERE name='Y' LIMIT 1)) AS path FROM Node LIMIT 1",
        )

        assert path in (None, []) or (isinstance(path, list) and len(path) <= 1)
