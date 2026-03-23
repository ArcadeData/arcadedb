import arcadedb_embedded as arcadedb


def test_graph_batch_creates_vertices_and_edges(temp_db_path):
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE VERTEX TYPE Person")
        db.command("sql", "CREATE EDGE TYPE Knows")

        with db.graph_batch(batch_size=2, parallel_flush=False) as batch:
            alice = batch.create_vertex("Person", name="Alice", age=31)
            bob = batch.create_vertex("Person", name="Bob", age=29)
            carol = batch.create_vertex("Person", name="Carol", age=35)

            batch.new_edge(alice, "Knows", bob, since=2021)
            assert batch.get_buffered_edge_count() == 1

            batch.new_edge(alice.get_rid(), "Knows", carol.get_rid(), since=2023)
            assert batch.get_buffered_edge_count() == 0
            assert batch.get_total_edges_created() == 2

        rows = list(
            db.query(
                "sql",
                "SELECT expand(out('Knows')) FROM Person WHERE name = 'Alice'",
            )
        )
        names = sorted(row.get("name") for row in rows)

        assert names == ["Bob", "Carol"]


def test_graph_batch_create_vertices_returns_rids(temp_db_path):
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE VERTEX TYPE Person")

        with db.graph_batch(parallel_flush=False) as batch:
            rids = batch.create_vertices(
                "Person",
                [
                    {"name": "Alice", "score": 10},
                    None,
                    {"name": "Carol", "score": 30},
                ],
            )

        assert len(rids) == 3
        assert all(rid.startswith("#") for rid in rids)

        second = db.lookup_by_rid(rids[1])
        assert second.get_type_name() == "Person"
        assert second.get("name") is None


def test_graph_batch_rejects_invalid_wal_flush_mode(temp_db_path):
    with arcadedb.create_database(temp_db_path) as db:
        try:
            db.graph_batch(wal_flush="invalid")
        except ValueError as exc:
            assert "Invalid wal_flush mode" in str(exc)
        else:
            raise AssertionError("Expected ValueError for invalid wal_flush mode")


def test_graph_batch_parallel_flush_smoke(temp_db_path):
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE VERTEX TYPE Person")
        db.command("sql", "CREATE PROPERTY Person.Id LONG")
        db.command("sql", "CREATE INDEX ON Person (Id) UNIQUE_HASH")
        db.command("sql", "CREATE EDGE TYPE Knows")

        with db.graph_batch(
            batch_size=2,
            expected_edge_count=4,
            parallel_flush=True,
        ) as batch:
            rids = batch.create_vertices(
                "Person",
                [
                    {"Id": 1, "name": "Alice"},
                    {"Id": 2, "name": "Bob"},
                    {"Id": 3, "name": "Carol"},
                    {"Id": 4, "name": "Dave"},
                ],
            )

            batch.new_edge(rids[0], "Knows", rids[1], since=2021)
            batch.new_edge(rids[0], "Knows", rids[2], since=2022)
            batch.new_edge(rids[1], "Knows", rids[3], since=2023)
            batch.new_edge(rids[2], "Knows", rids[3], since=2024)

        vertex_count = (
            db.query("sql", "SELECT count(*) AS c FROM Person").one().get("c")
        )
        edge_count = (
            db.query("opencypher", "MATCH ()-[r:Knows]->() RETURN count(r) AS c")
            .one()
            .get("c")
        )
        dave_incoming = list(
            db.query(
                "opencypher",
                "MATCH (p)-[:Knows]->(d) WHERE d.Id = 4 RETURN p.name AS name",
            )
        )

        assert int(vertex_count) == 4
        assert int(edge_count) == 4
        assert sorted(row.get("name") for row in dave_incoming) == ["Bob", "Carol"]
