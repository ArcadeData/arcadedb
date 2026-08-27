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


def test_graph_batch_retry_and_memory_knobs(temp_db_path):
    """The four builder knobs added in 26.9.1 are reachable and ingest correctly.

    `commit_retries` / `commit_retry_delay_ms` bound the retry of a vertex commit
    that fails with a transient NeedRetryException; `chunk_cache_capacity` and
    `max_deferred_incoming_edges` (ArcadeDB #5664) bound memory on a long-lived
    stream, the second by running the incoming-edge pass early from flush()
    instead of once at close().

    Values here are deliberately tiny so the bounded paths are the ones taken:
    a 2-entry chunk cache forces head-chunk reloads, and a 1-edge deferred cap
    forces the incoming-edge pass to run during the load. Both are pure
    accelerators, so the answer must come out identical either way, which is
    what this asserts.
    """
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE VERTEX TYPE Person")
        db.command("sql", "CREATE PROPERTY Person.Id LONG")
        db.command("sql", "CREATE INDEX ON Person (Id) UNIQUE_HASH")
        db.command("sql", "CREATE EDGE TYPE Knows")

        with db.graph_batch(
            batch_size=2,
            commit_retries=3,
            commit_retry_delay_ms=50,
            chunk_cache_capacity=2,
            max_deferred_incoming_edges=1,
        ) as batch:
            rids = batch.create_vertices(
                "Person",
                [{"Id": i, "name": f"P{i}"} for i in range(1, 6)],
            )
            for i in range(4):
                batch.new_edge(rids[i], "Knows", rids[i + 1], step=i)

        vertex_count = (
            db.query("sql", "SELECT count(*) AS c FROM Person").one().get("c")
        )
        edge_count = (
            db.query("opencypher", "MATCH ()-[r:Knows]->() RETURN count(r) AS c")
            .one()
            .get("c")
        )
        # The incoming direction is the one the deferred-edge cap governs.
        incoming = list(
            db.query(
                "opencypher",
                "MATCH (p)-[:Knows]->(d) WHERE d.Id = 5 RETURN p.Id AS id",
            )
        )

        assert int(vertex_count) == 5
        assert int(edge_count) == 4
        assert [int(row.get("id")) for row in incoming] == [4]


def test_graph_batch_invalid_knob_values_are_rejected(temp_db_path):
    """Out-of-range knob values raise, which is what proves they reach the builder.

    These four knobs have no observable effect on a correct result, so an
    end-to-end ingest test passes whether or not the wrapper forwards them.
    The engine validates each one, so a rejection is the assertion that
    discriminates a wired parameter from an accepted-and-ignored one. Note
    `max_deferred_incoming_edges=0` is legal (defer everything to close), so
    the negative value is the invalid one there.

    The exception TYPE is what carries the proof, and it is easy to get wrong.
    An unwired keyword raises `TypeError: ... got an unexpected keyword
    argument 'commit_retries'`, and that message contains the parameter name,
    so an assertion that merely greps for "retries" passes on precisely the
    broken code it is supposed to catch. The first version of this test did
    exactly that. `ArcadeDBError` plus the engine's own "must be" wording can
    only come from the value reaching the Java builder.
    """
    cases = [
        {"commit_retries": -1},
        {"commit_retry_delay_ms": -1},
        {"chunk_cache_capacity": 0},
        {"max_deferred_incoming_edges": -1},
    ]
    with arcadedb.create_database(temp_db_path) as db:
        for kwargs in cases:
            try:
                db.graph_batch(**kwargs)
            except TypeError as exc:
                raise AssertionError(
                    f"{kwargs} never reached the builder (unwired): {exc}"
                ) from exc
            except arcadedb.ArcadeDBError as exc:
                assert "must be" in str(exc), (kwargs, str(exc))
            else:
                raise AssertionError(f"Expected {kwargs} to be rejected")
