import contextlib
from datetime import datetime

import arcadedb_embedded as arcadedb
import pytest
from arcadedb_embedded.exceptions import ArcadeDBError


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


def test_graph_batch_refuses_one_way_edges_in_a_two_way_type(temp_db_path):
    """A batch built with `bidirectional=False` stores each edge on its source
    only. Before 26.10.1 it did so even for an edge type declared two-way (the
    `CREATE EDGE TYPE` default), and every query the planner then walked from the
    target end returned 0 rows with no error (ArcadeData/arcadedb#8625, fixed in
    #8628). Now the engine refuses the edge, naming the type, and writes nothing;
    a type declared UNIDIRECTIONAL loads as before."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE VERTEX TYPE Question")
        db.command("sql", "CREATE VERTEX TYPE Tag")
        db.command("sql", "CREATE EDGE TYPE TaggedWith")
        db.command("sql", "CREATE EDGE TYPE OneWay UNIDIRECTIONAL")

        try:
            with db.graph_batch(bidirectional=False) as batch:
                q = batch.create_vertex("Question", k=1)
                t = batch.create_vertex("Tag", k=2)
                batch.new_edge(q, "TaggedWith", t)
        except arcadedb.ArcadeDBError as exc:
            assert "TaggedWith" in str(exc), str(exc)
            assert "bidirectional" in str(exc), str(exc)
        else:
            raise AssertionError(
                "a one-way edge in a two-way type was accepted (ArcadeData/arcadedb#8625)"
            )
        assert (
            db.query("sql", "SELECT count(*) AS n FROM TaggedWith").first().get("n")
            == 0
        )

        with db.graph_batch(bidirectional=False) as batch:
            q = batch.create_vertex("Question", k=3)
            t = batch.create_vertex("Tag", k=4)
            batch.new_edge(q, "OneWay", t)
        assert db.query("sql", "SELECT count(*) AS n FROM OneWay").first().get("n") == 1


def test_one_way_edges_are_seen_by_patterns_not_by_in(temp_db_path):
    """The query contract of an edge type declared UNIDIRECTIONAL (26.10.1,
    ArcadeData/arcadedb#8625 fixed in #8628). Its edges are stored on the source
    vertex only. Patterns, Cypher and SQL MATCH, return every edge whichever
    way they are written, walking from the source or scanning the edge type
    once; `in()`, `inE()`, `both()`, and the vertex API read what the target
    vertex stores, which is nothing. Before 26.10.1 a pattern walked from the
    target returned 0 rows too."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE VERTEX TYPE Question")
        db.command("sql", "CREATE VERTEX TYPE Tag")
        db.command("sql", "CREATE EDGE TYPE TaggedWith UNIDIRECTIONAL")
        with db.graph_batch(bidirectional=False) as batch:
            tags = [batch.create_vertex("Tag", k=i) for i in range(5)]
            for i in range(50):
                q = batch.create_vertex("Question", k=i)
                batch.new_edge(q, "TaggedWith", tags[i % 5])
                batch.new_edge(q, "TaggedWith", tags[(i + 1) % 5])

        def count(language, query):
            return db.query(language, query).first().get("n")

        # Patterns see all 100 edges, from either end.
        assert (
            count(
                "opencypher",
                "MATCH (q:Question)-[:TaggedWith]->(t:Tag) RETURN count(*) AS n",
            )
            == 100
        )
        assert (
            count(
                "opencypher",
                "MATCH (t:Tag)<-[:TaggedWith]-(q:Question) RETURN count(*) AS n",
            )
            == 100
        )
        assert (
            count(
                "opencypher",
                "MATCH (t:Tag)-[:TaggedWith]-(q:Question) RETURN count(*) AS n",
            )
            == 100
        )
        assert (
            len(
                db.query(
                    "sql",
                    "MATCH {type: Tag, as: t}<-TaggedWith-{type: Question, as: q} RETURN t, q",
                ).to_list()
            )
            == 100
        )

        # The traversal functions and the vertex API read the target's own
        # pointers, which a one-way edge does not write.
        for fn in ("in", "inE", "both", "bothE"):
            assert (
                count(
                    "sql",
                    f"SELECT count(*) AS n FROM (SELECT expand({fn}('TaggedWith')) FROM Tag)",  # nosec B608 - fixed function names, no input
                )
                == 0
            ), fn
        assert (
            count(
                "sql",
                "SELECT count(*) AS n FROM (SELECT expand(out('TaggedWith')) FROM Question)",
            )
            == 100
        )
        tag = db.query("sql", "SELECT FROM Tag WHERE k = 0").first().get_vertex()
        assert tag.get_in_edges("TaggedWith") == []


def _declared_edge_types(db):
    """Edge types that declare a nullable INTEGER, a SHORT, and a STRING, one per write path."""
    db.command("sql", "CREATE VERTEX TYPE P")
    for name in ("ViaRecord", "ViaBatch", "ViaBulk"):
        db.command("sql", f"CREATE EDGE TYPE {name}")
        db.command("sql", f"CREATE PROPERTY {name}.weight INTEGER")
        db.command("sql", f"CREATE PROPERTY {name}.small SHORT")
        db.command("sql", f"CREATE PROPERTY {name}.note STRING")


def test_vertex_new_edge_keeps_a_null_and_refuses_an_out_of_range_short(temp_db_path):
    """The workaround in known-issues.md: write edges that carry declared
    properties through Vertex.new_edge, which converts and validates them."""
    with arcadedb.create_database(temp_db_path) as db:
        _declared_edge_types(db)
        with db.transaction():
            a = db.new_vertex("P").set("id", 1).save()
            b = db.new_vertex("P").set("id", 2).save()
            a.new_edge("ViaRecord", b, weight=None, note="hello").save()
        rows = db.query("sql", "SELECT weight, note FROM ViaRecord").to_list()
        assert [(r.get("weight"), r.get("note")) for r in rows] == [(None, "hello")]

        with pytest.raises(Exception):  # noqa: B017 - a Java IllegalArgumentException
            with db.transaction():
                a.new_edge("ViaRecord", b, small=40000).save()
        assert (
            db.query("sql", "SELECT count(*) AS n FROM ViaRecord").first().get("n") == 1
        )


@pytest.mark.parametrize("bulk", [False, True], ids=["new_edge", "new_edges"])
def test_graph_batch_edge_keeps_a_null_in_a_declared_property(temp_db_path, bulk):
    """ArcadeData/arcadedb#9018, fixed in 26.10.1 (upstream PR #9107): a null in a
    declared edge property used to be written as a type tag with no value, so the bytes
    of the next property were read as its value. The null must be followed by another
    property: a null written last read back as null even before the fix."""
    with arcadedb.create_database(temp_db_path) as db:
        _declared_edge_types(db)
        edge_type = "ViaBulk" if bulk else "ViaBatch"
        with db.transaction():
            a = db.new_vertex("P").set("id", 1).save()
            b = db.new_vertex("P").set("id", 2).save()
        with db.graph_batch(parallel_flush=False) as batch:
            if bulk:
                batch.new_edges(
                    [a.get_rid()],
                    edge_type,
                    [b.get_rid()],
                    properties=[{"weight": None, "note": "hello"}],
                )
            else:
                batch.new_edge(
                    a.get_rid(), edge_type, b.get_rid(), weight=None, note="hello"
                )
        query = f"SELECT weight, note FROM {edge_type}"  # nosec B608 - fixed type names
        rows = db.query("sql", query).to_list()
        assert [(r.get("weight"), r.get("note")) for r in rows] == [(None, "hello")]


def test_graph_batch_edge_refuses_an_out_of_range_short(temp_db_path):
    """ArcadeData/arcadedb#9019, fixed in 26.10.1 (upstream PR #9108): a GraphBatch edge
    skipped the declared property's conversion, so 40000 in a SHORT was stored as -25536.
    The engine may refuse the value or store it as the record API does; it must not wrap it.
    """
    with arcadedb.create_database(temp_db_path) as db:
        _declared_edge_types(db)
        with db.transaction():
            a = db.new_vertex("P").set("id", 1).save()
            b = db.new_vertex("P").set("id", 2).save()
        try:
            with db.graph_batch(parallel_flush=False) as batch:
                batch.new_edge(a.get_rid(), "ViaBatch", b.get_rid(), small=40000)
            refused = False
        except Exception:  # noqa: BLE001 - the engine's refusal, whatever its Java type
            refused = True
        stored = [
            r.get("small")
            for r in db.query("sql", "SELECT small FROM ViaBatch").to_list()
        ]
        assert refused and stored == []


def test_graph_batch_create_vertex_keyboard_interrupt_rolls_back(
    temp_db_path, monkeypatch
):
    """A KeyboardInterrupt between create_vertex's own begin() and commit()
    must not leak the transaction it started: `except Exception` let it bypass
    the rollback entirely (#7882)."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE VERTEX TYPE Person")

        with db.graph_batch(parallel_flush=False) as batch:

            def interrupt(_properties):
                raise KeyboardInterrupt

            monkeypatch.setattr(batch, "_to_java_varargs", interrupt)
            with pytest.raises(KeyboardInterrupt):
                batch.create_vertex("Person", name="Alice")
            assert db.is_transaction_active() is False
            monkeypatch.undo()

        assert db.count_type("Person") == 0


def test_graph_batch_create_vertex_failure_still_wraps_and_rolls_back(
    temp_db_path, monkeypatch
):
    """An ordinary failure keeps its ArcadeDBError wrapping (#7882 widened only
    the rollback, not the exception translation)."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE VERTEX TYPE Person")

        with db.graph_batch(parallel_flush=False) as batch:

            def fail(_properties):
                raise TypeError("unconvertible")

            monkeypatch.setattr(batch, "_to_java_varargs", fail)
            with pytest.raises(ArcadeDBError):
                batch.create_vertex("Person", name="Alice")
            assert db.is_transaction_active() is False
            monkeypatch.undo()


def _unique_person_db(db):
    db.command("sql", "CREATE VERTEX TYPE Person")
    db.command("sql", "CREATE PROPERTY Person.id INTEGER")
    db.command("sql", "CREATE INDEX ON Person (id) UNIQUE")
    db.command("sql", "CREATE DOCUMENT TYPE Note")


@pytest.mark.parametrize("bulk", [True, False], ids=["json-bulk", "property-matrix"])
def test_graph_batch_create_vertices_failure_rolls_back(temp_db_path, bulk):
    """A create_vertices that fails for a reason the engine does not retry (a
    duplicate key) must not leave its transaction open: a later write outside
    any transaction was accepted and silently lost at close (#121)."""
    with arcadedb.create_database(temp_db_path) as db:
        _unique_person_db(db)
        # a datetime is not JSON-safe, so it routes to the property-matrix path
        extra = {} if bulk else {"seen": datetime(2026, 10, 3)}
        rows = [{"id": 2, **extra}, {"id": 2, **extra}]

        with db.graph_batch(parallel_flush=False) as batch:
            with pytest.raises(ArcadeDBError):
                batch.create_vertices("Person", rows)
            assert db.is_transaction_active() is False
            # the failed call left nothing behind, the batch is still usable
            assert batch.create_vertices("Person", [{"id": 3, **extra}]) != []

        assert db.is_transaction_active() is False
        with pytest.raises(Exception):  # noqa: B017 - the engine's own refusal
            db.command("sql", "INSERT INTO Note SET text = 'outside a transaction'")
        assert db.count_type("Person") == 1


def test_graph_batch_create_vertices_keeps_the_callers_transaction(temp_db_path):
    """The rollback is for a transaction create_vertices opened itself: one the
    caller already had open is theirs, and a failure inside it leaves it
    active (a call the engine refuses for that transaction does too, #9242, the
    tests after test_graph_batch_outside_the_callers_transactions)."""
    with arcadedb.create_database(temp_db_path) as db:
        _unique_person_db(db)

        with db.graph_batch(parallel_flush=False) as batch:
            db.begin()
            try:
                with pytest.raises(ArcadeDBError):
                    batch.create_vertices("Person", [{"id": 2}, {"id": 2}])
                assert db.is_transaction_active() is True
            finally:
                if db.is_transaction_active():
                    db.rollback()


def test_graph_batch_create_vertices_keyboard_interrupt_rolls_back(
    temp_db_path, monkeypatch
):
    """A KeyboardInterrupt after the engine opened its transaction is not an
    Exception; the rollback must still run (#121, as #7882 for create_vertex)."""
    with arcadedb.create_database(temp_db_path) as db:
        _unique_person_db(db)

        with db.graph_batch(parallel_flush=False) as batch:

            def interrupt(*_args, **_kwargs):
                db.begin()  # what the engine's createVertices has done by now
                raise KeyboardInterrupt

            monkeypatch.setattr(batch, "_create_vertices_json_bulk", interrupt)
            with pytest.raises(KeyboardInterrupt):
                batch.create_vertices("Person", [{"id": 1}])
            assert db.is_transaction_active() is False
            monkeypatch.undo()

        assert db.count_type("Person") == 0


# ArcadeData/arcadedb#9242 (known-issues.md): createVertices, flush() and close() committed a
# transaction the caller opened. Fixed in 26.10.1 (PR #9270): they refuse to run inside it
# (IllegalStateException, an ArcadeDBError here) and leave it untouched, so the caller's
# rollback undoes the caller's write. The tests below assert the refusal itself, not only the
# untouched transaction, so a change that commits quietly again, or one that stops refusing
# without leaving the transaction alone, fails them.
def _is_the_engines_refusal(error):
    import jpype

    return isinstance(error, ArcadeDBError) and isinstance(
        error.__cause__, jpype.JClass("java.lang.IllegalStateException")
    )


def _vertex_edge_note_types(db):
    db.command("sql", "CREATE VERTEX TYPE V")
    db.command("sql", "CREATE EDGE TYPE E")
    db.command("sql", "CREATE DOCUMENT TYPE Note")


def _save_note(db, text):
    db.new_document("Note").set("text", text).save()


def _notes(db):
    return sorted(r.get("text") for r in db.query("sql", "SELECT text FROM Note"))


@contextlib.contextmanager
def _first_vertex_commit_attempt_fails(attempts):
    """Through the engine's own test hook, fail the first commit attempt of each
    createVertices with a ConcurrentModificationException, which it retries.
    Every attempt number the hook sees is appended to `attempts`."""
    import jpype

    graph_batch_class = jpype.JClass("com.arcadedb.graph.GraphBatch")
    retryable = jpype.JClass("com.arcadedb.exception.ConcurrentModificationException")

    @jpype.JImplements("java.util.function.IntConsumer")
    class FailFirstAttempt:
        @jpype.JOverride
        def accept(self, attempt):
            attempts.append(int(attempt))
            if attempt == 1:
                raise retryable("first vertex commit attempt fails (test hook)")

    graph_batch_class.TEST_BEFORE_VERTEX_COMMIT_HOOK = FailFirstAttempt()
    try:
        yield
    finally:
        graph_batch_class.TEST_BEFORE_VERTEX_COMMIT_HOOK = None


def _call_inside_the_callers_transaction(db, call):
    """Begin, save the caller's Note, make the call, then roll back. Returns whether a
    transaction was still active after the call, the Notes left after the rollback, and the
    refusal the call raised, if any."""
    refusal = None
    db.begin()
    try:
        _save_note(db, "caller")
        try:
            call()
        except ArcadeDBError as e:
            refusal = e
        active = db.is_transaction_active()
    finally:
        if db.is_transaction_active():
            db.rollback()
    return active, _notes(db), refusal


def test_graph_batch_outside_the_callers_transactions(temp_db_path):
    """The order that works on every engine, and the only one that does on 26.9.1 and earlier
    (#9242): commit your own writes before the batch and call it outside any transaction of
    yours. Each rollback then undoes only its own writes, and a retried vertex commit loses
    nothing of yours."""
    with arcadedb.create_database(temp_db_path) as db:
        _vertex_edge_note_types(db)
        with db.transaction():
            _save_note(db, "before")

        attempts = []
        with db.graph_batch(parallel_flush=False, commit_retry_delay_ms=1) as batch:
            with _first_vertex_commit_attempt_fails(attempts):
                rids = batch.create_vertices("V", 3)
            batch.new_edges(rids[:-1], "E", rids[1:])
            assert db.is_transaction_active() is False

        db.begin()
        _save_note(db, "rolled back")
        db.rollback()

        # the hook failed the first attempt and the retry committed: the path this covers ran
        assert attempts == [1, 2]
        assert _notes(db) == ["before"]
        assert db.count_type("V") == 3
        assert db.count_type("E") == 2


@pytest.mark.parametrize(
    "rows",
    [2, [{"x": 1}, {"x": 2}], [{"seen": datetime(2026, 10, 5)}, None]],
    ids=["count", "json-bulk", "property-matrix"],
)
def test_graph_batch_create_vertices_leaves_the_callers_transaction(temp_db_path, rows):
    """#9242: create_vertices inside the caller's transaction refuses to run and leaves it to
    the caller. Before the fix it committed it, the caller's Note with it, and no transaction
    was active after.
    """
    with arcadedb.create_database(temp_db_path) as db:
        _vertex_edge_note_types(db)
        with db.graph_batch(parallel_flush=False) as batch:
            active, notes, refusal = _call_inside_the_callers_transaction(
                db, lambda: batch.create_vertices("V", rows)
            )
        assert _is_the_engines_refusal(refusal), f"refusal: {refusal!r}"
        assert (active, notes) == (True, [])
        assert db.count_type("V") == 0


@pytest.mark.parametrize("step", ["flush", "full-buffer", "close"])
def test_graph_batch_flush_and_close_leave_the_callers_transaction(temp_db_path, step):
    """#9242: a flush, explicit or by a new_edges that fills the buffer, and close(), inside
    the caller's transaction refuse to run and leave it to the caller. Before the fix each
    committed it."""
    with arcadedb.create_database(temp_db_path) as db:
        _vertex_edge_note_types(db)
        batch = db.graph_batch(parallel_flush=False, batch_size=2)
        try:
            rids = batch.create_vertices("V", 4)
            if step == "flush":
                batch.new_edge(rids[0], "E", rids[1])
                call = batch.flush
            elif step == "full-buffer":
                # three edges into a buffer of two: new_edges flushes on its own
                def call():
                    batch.new_edges(rids[:3], "E", rids[1:])

            else:
                batch.new_edge(rids[0], "E", rids[1])
                call = batch.close
            active, notes, refusal = _call_inside_the_callers_transaction(db, call)
        finally:
            batch.close()
        assert _is_the_engines_refusal(refusal), f"refusal: {refusal!r}"
        assert (active, notes) == (True, [])


def test_graph_batch_create_vertices_refuses_before_any_commit_attempt(temp_db_path):
    """#9242: the retry that rolled the caller's transaction back needs a commit attempt, and
    the call now refuses before making one. The caller's Note survives and commits as theirs,
    and the retry path itself still runs outside the caller's transaction
    (test_graph_batch_outside_the_callers_transactions)."""
    with arcadedb.create_database(temp_db_path) as db:
        _vertex_edge_note_types(db)
        attempts = []
        with db.graph_batch(parallel_flush=False, commit_retry_delay_ms=1) as batch:
            db.begin()
            try:
                _save_note(db, "caller")
                with _first_vertex_commit_attempt_fails(attempts):
                    with pytest.raises(ArcadeDBError) as refused:
                        batch.create_vertices("V", 2)
                assert _is_the_engines_refusal(refused.value), repr(refused.value)
                db.commit()
            finally:
                if db.is_transaction_active():
                    db.rollback()
        assert attempts == []
        assert _notes(db) == ["caller"]
        assert db.count_type("V") == 0


def test_graph_batch_close_refused_in_the_callers_transaction_can_be_retried(
    temp_db_path,
):
    """#9242: a close() the engine refuses inside the caller's transaction releases nothing:
    the batch stays open with its edges pending, so the same close() after the caller's
    transaction ends writes them, and only then is a new batch on the database allowed.
    """
    with arcadedb.create_database(temp_db_path) as db:
        _vertex_edge_note_types(db)
        batch = db.graph_batch(parallel_flush=False)
        rids = batch.create_vertices("V", 3)
        batch.new_edge(rids[0], "E", rids[1])
        batch.new_edge(rids[1], "E", rids[2])
        try:
            active, notes, refusal = _call_inside_the_callers_transaction(
                db, batch.close
            )
            assert _is_the_engines_refusal(refusal), f"refusal: {refusal!r}"
            assert (active, notes) == (True, [])
            assert db.count_type("E") == 0
            # the wrapper is still open and the edges are still pending
            assert (
                batch.get_buffered_edge_count()
                + batch.get_deferred_incoming_edge_count()
                > 0
            )
            with pytest.raises(ArcadeDBError, match="already in progress"):
                db.graph_batch(parallel_flush=False)
        finally:
            batch.close()
        assert db.count_type("E") == 2
        db.graph_batch(parallel_flush=False).close()


def test_graph_batch_leaves_the_callers_wal_setting_alone(temp_db_path):
    """#9242: with the batch open, a transaction of the caller's commits with the caller's
    WAL setting. Before the fix the batch's own setting (no WAL, by default) stayed on the
    thread between its calls, so the caller's commit wrote no WAL record."""
    with arcadedb.create_database(temp_db_path) as db:
        _vertex_edge_note_types(db)

        def caller_transaction_uses_the_wal():
            db.begin()
            try:
                return bool(db._java_db.getTransaction().isUseWAL())
            finally:
                db.rollback()

        assert caller_transaction_uses_the_wal() is True
        with db.graph_batch(parallel_flush=False) as batch:
            batch.create_vertices("V", 2)
            assert caller_transaction_uses_the_wal() is True
        assert caller_transaction_uses_the_wal() is True
