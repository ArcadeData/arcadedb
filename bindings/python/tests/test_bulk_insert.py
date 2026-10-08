"""Tests for Database.insert_many and AsyncExecutor.create_record."""

import datetime

import pytest


def _count(db, type_name):
    q = f"SELECT count(*) AS n FROM {type_name}"  # nosec B608 - test-controlled
    return int(db.query("sql", q).to_list()[0]["n"])


class TestRecommendedBulkPathsLandEveryRow:
    """A bulk load through each recommended path, counted against what it was given.

    ArcadeData/arcadedb#7615 went unnoticed because no test compared rows
    submitted with rows stored at a size where the loss shows. Before 26.10.1
    (fixed in #7625) the async command path dropped roughly three quarters of a
    9,742-row load at parallel level 4 while raising nothing, logging nothing,
    and returning normally from `wait_completion()`, so only a count caught it.
    These are the counts, on
    the paths the documentation now recommends instead.
    """

    # Sizes from the original report: 9,742 documents, and the 20,000/40,000
    # graph that was measured landing every row through GraphBatch.
    DOCUMENTS = 9_742
    VERTICES = 20_000
    EDGES = 40_000

    def test_insert_many_lands_every_document(self, temp_db):
        temp_db.command("sql", "CREATE DOCUMENT TYPE BulkDoc")
        rows = [{"id": i, "name": f"row_{i}"} for i in range(self.DOCUMENTS)]

        written = temp_db.insert_many("BulkDoc", rows, commit_every=1_000)

        assert written == self.DOCUMENTS
        assert _count(temp_db, "BulkDoc") == self.DOCUMENTS
        # the rows are the ones submitted, not merely the right number of rows
        agg = temp_db.query(
            "sql", "SELECT min(id) AS lo, max(id) AS hi, sum(id) AS total FROM BulkDoc"
        ).to_list()[0]
        assert int(agg["lo"]) == 0
        assert int(agg["hi"]) == self.DOCUMENTS - 1
        assert int(agg["total"]) == self.DOCUMENTS * (self.DOCUMENTS - 1) // 2

    def test_insert_many_parallel_lands_every_document(self, temp_db):
        """insert_many(parallel=True) routes through the executor's createRecord.

        That is a different submission path from `command`, and it is measured
        unaffected by #7615. This test is what keeps that true.
        """
        temp_db.command("sql", "CREATE DOCUMENT TYPE BulkPar")
        rows = [{"id": i} for i in range(self.DOCUMENTS)]

        written = temp_db.insert_many("BulkPar", rows, parallel=True)

        assert written == self.DOCUMENTS
        assert _count(temp_db, "BulkPar") == self.DOCUMENTS

    @pytest.mark.parametrize("parallel_flush", [False, True])
    def test_graph_batch_lands_every_vertex_and_edge(self, temp_db, parallel_flush):
        """GraphBatch flushes edges through the same async executor.

        It is the recommended bulk graph path precisely because it stays exact
        while the SQL command path does not, so the flush runs both ways here.
        """
        temp_db.schema.create_vertex_type("BulkV")
        temp_db.schema.create_edge_type("BulkE")

        # a parallel level the command path would lose records at, to show the
        # executor is busy on more than one worker during the edge flush
        temp_db.async_executor().set_parallel_level(4)

        with temp_db.graph_batch(parallel_flush=parallel_flush) as batch:
            rids = batch.create_vertices(
                "BulkV", [{"k": i} for i in range(self.VERTICES)]
            )
            assert len(rids) == self.VERTICES

            sources = [rids[i % self.VERTICES] for i in range(self.EDGES)]
            targets = [rids[(i * 7 + 1) % self.VERTICES] for i in range(self.EDGES)]
            batch.new_edges(sources, "BulkE", targets)
            batch.flush()

        temp_db.async_executor().wait_completion()

        assert _count(temp_db, "BulkV") == self.VERTICES
        assert _count(temp_db, "BulkE") == self.EDGES


class TestInsertMany:
    def test_basic_roundtrip(self, temp_db):
        temp_db.command("sql", "CREATE DOCUMENT TYPE Item")
        rows = [
            {
                "k": i,
                "name": f"item_{i}",
                "price": i * 1.5,
                "active": i % 2 == 0,
                "tags": ["a", "b"],
                "meta": {"x": i},
            }
            for i in range(500)
        ]
        n = temp_db.insert_many("Item", rows, commit_every=100)
        assert n == 500
        assert _count(temp_db, "Item") == 500
        got = temp_db.query("sql", "SELECT FROM Item WHERE k = 7").to_list()[0]
        assert got["name"] == "item_7"
        assert got["price"] == pytest.approx(10.5)
        assert got["active"] is False
        assert list(got["tags"]) == ["a", "b"]

    def test_null_values(self, temp_db):
        temp_db.command("sql", "CREATE DOCUMENT TYPE Nul")
        n = temp_db.insert_many("Nul", [{"a": 1, "b": None}, {"a": None}])
        assert n == 2
        assert _count(temp_db, "Nul") == 2

    def test_empty(self, temp_db):
        temp_db.command("sql", "CREATE DOCUMENT TYPE Empty")
        assert temp_db.insert_many("Empty", []) == 0

    def test_parallel(self, temp_db):
        temp_db.command("sql", "CREATE DOCUMENT TYPE Par")
        rows = [{"k": i} for i in range(2000)]
        n = temp_db.insert_many("Par", rows, parallel=True)
        assert n == 2000
        assert _count(temp_db, "Par") == 2000

    def test_non_json_fallback(self, temp_db):
        temp_db.command("sql", "CREATE DOCUMENT TYPE Dated")
        rows = [
            {"k": i, "when": datetime.datetime(2026, 7, 25, 12, 0, i)} for i in range(3)
        ]
        n = temp_db.insert_many("Dated", rows)
        assert n == 3
        assert _count(temp_db, "Dated") == 3

    def test_inside_open_transaction(self, temp_db):
        temp_db.command("sql", "CREATE DOCUMENT TYPE Tx")
        temp_db.begin()
        temp_db.insert_many("Tx", [{"k": 1}, {"k": 2}], commit_every=0)
        temp_db.commit()
        assert _count(temp_db, "Tx") == 2


class TestInsertManyParallelReportsFailures:
    """A record the parallel writers fail to store must fail the call.

    The maintainers' advice for an async bulk load (ArcadeData/arcadedb#8478)
    is to register an error callback "so a failed record can't pass silently".
    The parallel mode submitted every record with no callback and returned the
    row count it was given, so a rejected record (here a duplicate key under a
    unique index) was dropped while insert_many reported success.
    """

    def test_duplicate_key_raises_instead_of_dropping(self, temp_db):
        import arcadedb_embedded as arcadedb

        temp_db.command("sql", "CREATE DOCUMENT TYPE ParDup BUCKETS 4")
        temp_db.command("sql", "CREATE PROPERTY ParDup.id INTEGER")
        temp_db.command("sql", "CREATE INDEX ON ParDup (id) UNIQUE")
        rows = [{"id": i % 500} for i in range(1_000)]  # every key twice

        with pytest.raises(arcadedb.ArcadeDBError, match="failed"):
            temp_db.insert_many("ParDup", rows, parallel=True)

        assert _count(temp_db, "ParDup") <= 500

    def test_clean_load_still_returns_the_count(self, temp_db):
        temp_db.command("sql", "CREATE DOCUMENT TYPE ParOk BUCKETS 4")
        temp_db.command("sql", "CREATE PROPERTY ParOk.id INTEGER")
        temp_db.command("sql", "CREATE INDEX ON ParOk (id) UNIQUE")
        rows = [{"id": i} for i in range(1_000)]

        assert temp_db.insert_many("ParOk", rows, parallel=True) == 1_000
        assert _count(temp_db, "ParOk") == 1_000


class _Unsettable:
    """A value json.dumps rejects AND the Java side cannot store (#7882)."""


class _InterruptingRow(dict):
    """A row whose iteration raises KeyboardInterrupt, standing in for a ^C
    landing mid-insert. json.dumps never calls items() on a dict subclass, so
    only the per-row fallback reaches it."""

    def items(self):
        raise KeyboardInterrupt


class TestInsertManyTransactionHygiene:
    """insert_many must leave the transaction state exactly as it found it,
    on every exit path (#7882)."""

    def test_fallback_set_failure_rolls_back(self, temp_db):
        temp_db.command("sql", "CREATE DOCUMENT TYPE FbFail")
        rows = [{"a": datetime.datetime(2026, 1, 1)}, {"b": _Unsettable()}]
        with pytest.raises(Exception):
            temp_db.insert_many("FbFail", rows)
        # Before #7882 the transaction begun by the fallback was left open,
        # holding the first row's write for the next caller to inherit.
        assert temp_db.is_transaction_active() is False
        assert _count(temp_db, "FbFail") == 0

    def test_fallback_failure_keeps_earlier_committed_batches_only(self, temp_db):
        temp_db.command("sql", "CREATE DOCUMENT TYPE FbBatch")
        rows = [{"k": i, "when": datetime.datetime(2026, 1, 1)} for i in range(3)]
        rows.append({"k": 3, "bad": _Unsettable()})
        with pytest.raises(Exception):
            temp_db.insert_many("FbBatch", rows, commit_every=2)
        assert temp_db.is_transaction_active() is False
        # The batch of 2 committed before the failure is durable; row 2, the
        # one in the open batch, is rolled back rather than left pending.
        assert _count(temp_db, "FbBatch") == 2

    def test_fallback_keyboard_interrupt_rolls_back(self, temp_db):
        temp_db.command("sql", "CREATE DOCUMENT TYPE FbIntr")
        rows = [{"a": datetime.datetime(2026, 1, 1)}, _InterruptingRow(b=1)]
        with pytest.raises(KeyboardInterrupt):
            temp_db.insert_many("FbIntr", rows)
        assert temp_db.is_transaction_active() is False
        assert _count(temp_db, "FbIntr") == 0

    def test_fallback_failure_leaves_caller_transaction_alone(self, temp_db):
        temp_db.command("sql", "CREATE DOCUMENT TYPE FbCaller")
        temp_db.begin()
        temp_db.new_document("FbCaller").set("k", "caller").save()
        rows = [{"a": datetime.datetime(2026, 1, 1)}, {"b": _Unsettable()}]
        with pytest.raises(Exception):
            temp_db.insert_many("FbCaller", rows)
        # The caller's transaction is the caller's to end.
        assert temp_db.is_transaction_active() is True
        temp_db.rollback()
        assert _count(temp_db, "FbCaller") == 0

    def test_fast_path_failure_rolls_back(self, temp_db):
        temp_db.command("sql", "CREATE DOCUMENT TYPE FastFail")
        temp_db.command("sql", "CREATE PROPERTY FastFail.req STRING (mandatory true)")
        rows = [{"req": "x"}, {"other": 1}]
        with pytest.raises(Exception):
            temp_db.insert_many("FastFail", rows)
        assert temp_db.is_transaction_active() is False
        assert _count(temp_db, "FastFail") == 0

    def test_fast_path_failure_leaves_caller_transaction_alone(self, temp_db):
        temp_db.command("sql", "CREATE DOCUMENT TYPE FastCaller")
        temp_db.command("sql", "CREATE PROPERTY FastCaller.req STRING (mandatory true)")
        temp_db.begin()
        temp_db.new_document("FastCaller").set("req", "caller").save()
        with pytest.raises(Exception):
            temp_db.insert_many("FastCaller", [{"req": "x"}, {"other": 1}])
        assert temp_db.is_transaction_active() is True
        temp_db.rollback()
        assert _count(temp_db, "FastCaller") == 0

    def test_fast_path_does_not_commit_caller_transaction(self, temp_db):
        temp_db.command("sql", "CREATE DOCUMENT TYPE FastNoCommit")
        temp_db.begin()
        temp_db.insert_many(
            "FastNoCommit", [{"k": i} for i in range(5)], commit_every=2
        )
        assert temp_db.is_transaction_active() is True
        temp_db.rollback()
        # commit_every batches only the transactions insert_many itself owns,
        # as the per-row fallback already did: rolling back the caller's
        # transaction must discard every row.
        assert _count(temp_db, "FastNoCommit") == 0


class TestInsertManyStreams:
    """insert_many reads its rows a chunk at a time (#294).

    It used to read the whole iterable into a list and send it as one JSON
    text, which the engine parsed into one JSON array, so a load held about
    1.5 KB per row at once (Python list, JSON text, parsed rows): a generator
    of 26 million time-series points could not load under 32 GB. Each test
    here fails on that version.
    """

    def _point_type(self, db, name):
        db.command("sql", f"CREATE DOCUMENT TYPE {name}")
        db.command("sql", f"CREATE PROPERTY {name}.k LONG")
        db.command("sql", f"CREATE INDEX ON {name} (k) UNIQUE")

    def test_rows_are_written_while_the_iterable_is_read(self, temp_db):
        from arcadedb_embedded import core

        self._point_type(temp_db, "Stream")
        chunk = core._INSERT_MANY_CHUNK
        seen = {}

        def rows():
            for i in range(3 * chunk):
                if i == 2 * chunk + 1:
                    # what the database holds while row 2*chunk+1 is produced
                    seen["stored"] = _count(temp_db, "Stream")
                yield {"k": i}

        assert temp_db.insert_many("Stream", rows(), commit_every=chunk) == 3 * chunk
        # At most one chunk is pending: the old code had stored nothing yet.
        assert seen["stored"] >= chunk
        assert _count(temp_db, "Stream") == 3 * chunk

    def test_python_memory_does_not_grow_with_the_row_count(self, temp_db):
        import tracemalloc

        self._point_type(temp_db, "StreamMem")
        n = 60_000

        def rows():
            for i in range(n):
                yield {"k": i, "host": f"host_{i % 100}", "v": i * 0.5}

        tracemalloc.start()
        try:
            temp_db.insert_many("StreamMem", rows(), commit_every=10_000)
            peak = tracemalloc.get_traced_memory()[1]
        finally:
            tracemalloc.stop()
        # The old code held about 25 MB here (60,000 dicts plus their JSON
        # text, about 410 bytes a row) and grew linearly with n; streaming
        # holds one 10,000-row chunk, about 4 MB, whatever n is.
        assert peak < 12 * 2**20, f"peak Python allocation {peak / 2**20:.1f} MB"
        assert _count(temp_db, "StreamMem") == n

    def test_commit_every_counts_across_chunks(self, temp_db):
        from arcadedb_embedded import ArcadeDBError, core

        self._point_type(temp_db, "StreamBatch")
        chunk = core._INSERT_MANY_CHUNK
        every = chunk + chunk // 2  # not a multiple of the chunk
        rows = [{"k": i} for i in range(2 * every + 10)]
        rows.append({"k": 0})  # a duplicate key in the third batch
        with pytest.raises(ArcadeDBError):
            temp_db.insert_many("StreamBatch", rows, commit_every=every)
        assert temp_db.is_transaction_active() is False
        # exactly the two committed batches survive
        assert _count(temp_db, "StreamBatch") == 2 * every

    def test_commit_every_zero_is_one_transaction(self, temp_db):
        from arcadedb_embedded import ArcadeDBError, core

        self._point_type(temp_db, "StreamOne")
        rows = [{"k": i} for i in range(2 * core._INSERT_MANY_CHUNK + 5)]
        rows.append({"k": 0})
        with pytest.raises(ArcadeDBError):
            temp_db.insert_many("StreamOne", rows, commit_every=0)
        assert temp_db.is_transaction_active() is False
        assert _count(temp_db, "StreamOne") == 0

    def test_an_error_from_the_iterable_keeps_committed_batches(self, temp_db):
        from arcadedb_embedded import core

        self._point_type(temp_db, "StreamGenErr")
        chunk = core._INSERT_MANY_CHUNK

        def rows():
            for i in range(chunk + 7):
                yield {"k": i}
            raise ValueError("source failed")

        with pytest.raises(ValueError, match="source failed"):
            temp_db.insert_many("StreamGenErr", rows(), commit_every=chunk)
        assert temp_db.is_transaction_active() is False
        assert _count(temp_db, "StreamGenErr") == chunk

    def test_a_chunk_off_the_json_path_does_not_change_the_others(self, temp_db):
        from arcadedb_embedded import core

        chunk = core._INSERT_MANY_CHUNK
        temp_db.command("sql", "CREATE DOCUMENT TYPE StreamMixed")
        rows = [{"k": i} for i in range(2 * chunk)]
        rows[chunk + 3] = {"k": chunk + 3, "when": datetime.datetime(2026, 1, 1)}
        assert temp_db.insert_many("StreamMixed", iter(rows)) == 2 * chunk
        assert _count(temp_db, "StreamMixed") == 2 * chunk

    @pytest.mark.parametrize("with_fallback_chunk", [False, True])
    def test_parallel_streams_every_row(self, temp_db, with_fallback_chunk):
        from arcadedb_embedded import core

        chunk = core._INSERT_MANY_CHUNK
        temp_db.command("sql", "CREATE DOCUMENT TYPE StreamPar BUCKETS 4")
        temp_db.command("sql", "CREATE PROPERTY StreamPar.k LONG")
        temp_db.command("sql", "CREATE INDEX ON StreamPar (k) UNIQUE")
        n = 2 * chunk + 11

        def rows():
            for i in range(n):
                if with_fallback_chunk and i == chunk + 1:
                    yield {"k": i, "when": datetime.datetime(2026, 1, 1)}
                else:
                    yield {"k": i}

        assert temp_db.insert_many("StreamPar", rows(), parallel=True) == n
        assert _count(temp_db, "StreamPar") == n

    def test_parallel_duplicate_in_a_later_chunk_raises(self, temp_db):
        from arcadedb_embedded import ArcadeDBError, core

        chunk = core._INSERT_MANY_CHUNK
        temp_db.command("sql", "CREATE DOCUMENT TYPE StreamParDup BUCKETS 4")
        temp_db.command("sql", "CREATE PROPERTY StreamParDup.k LONG")
        temp_db.command("sql", "CREATE INDEX ON StreamParDup (k) UNIQUE")
        rows = [{"k": i} for i in range(chunk + 5)] + [{"k": 1}]
        # The failure in the last chunk reaches the call; a failed writer batch
        # abandons the records buffered with it, so fewer rows may be stored.
        with pytest.raises(ArcadeDBError, match="failed record"):
            temp_db.insert_many("StreamParDup", rows, parallel=True)
        assert _count(temp_db, "StreamParDup") <= chunk + 5


class TestAsyncCreateRecord:
    def test_create_and_wait(self, temp_db):
        temp_db.command("sql", "CREATE DOCUMENT TYPE ARec")
        ex = temp_db.async_executor()
        for i in range(100):
            doc = temp_db.new_document("ARec")
            doc.set("k", i)
            ex.create_record(doc)
        ex.wait_completion()
        assert _count(temp_db, "ARec") == 100

    def test_callback(self, temp_db):
        temp_db.command("sql", "CREATE DOCUMENT TYPE CRec")
        seen = []
        ex = temp_db.async_executor()
        doc = temp_db.new_document("CRec")
        doc.set("k", 1)
        ex.create_record(doc, callback=lambda rec: seen.append(rec))
        ex.wait_completion()
        assert _count(temp_db, "CRec") == 1
        assert len(seen) == 1


class TestVectorColumns:
    """f4v/f8v columnar export: embedding columns as 2-D numpy arrays."""

    def test_float_array_column_to_columns(self, temp_db):
        import arcadedb_embedded as arcadedb
        import numpy as np

        temp_db.command("sql", "CREATE DOCUMENT TYPE Emb")
        temp_db.command("sql", "CREATE PROPERTY Emb.vid INTEGER")
        temp_db.command("sql", "CREATE PROPERTY Emb.v ARRAY_OF_FLOATS")
        with temp_db.transaction():
            for i in range(50):
                temp_db.command(
                    "sql",
                    "INSERT INTO Emb SET vid = :i, v = :v",
                    {"i": i, "v": arcadedb.to_java_float_array([i, i + 0.5, i + 0.25])},
                )
        cols = temp_db.query("sql", "SELECT vid, v FROM Emb ORDER BY vid").to_columns()
        assert cols is not None
        arr = cols["v"]
        assert isinstance(arr, np.ndarray) and arr.shape == (50, 3)
        assert arr[10][1] == np.float32(10.5)


class TestAppendSamplesNumpy:
    def test_numpy_columns(self, temp_db):
        import numpy as np

        temp_db.command(
            "sql",
            "CREATE TIMESERIES TYPE NpTs TIMESTAMP ts "
            "TAGS (host STRING) FIELDS (val DOUBLE) SHARDS 2",
        )
        n = 10_000
        ts = np.arange(n, dtype=np.int64) * 1000
        vals = np.linspace(0.0, 1.0, n)
        hosts = [f"h{i % 4}" for i in range(n)]
        ex = temp_db.async_executor()
        ex.append_samples("NpTs", ts, hosts, vals)
        ex.wait_completion()
        got = temp_db.query("sql", "SELECT count(*) AS n FROM NpTs").to_list()[0]["n"]
        assert int(got) == n

    def test_primitive_batch_matches_object_path(self, temp_db):
        """primitive=True must store exactly what the Object[] path stores.

        The primitive path exists to skip boxing (engine issue #5474), so the
        only thing that makes it worth having is that the samples it writes are
        indistinguishable from the path it replaces.
        """
        import numpy as np

        for type_name in ("PrimA", "PrimB"):
            temp_db.command(
                "sql",
                f"CREATE TIMESERIES TYPE {type_name} TIMESTAMP ts "
                "TAGS (host STRING) FIELDS (val DOUBLE, cnt LONG) SHARDS 2",
            )

        n = 5_000
        ts = np.arange(n, dtype=np.int64) * 1000 + 1_700_000_000_000
        vals = np.linspace(-5.0, 5.0, n)
        cnts = np.arange(n, dtype=np.int64) % 7
        hosts = [f"h{i % 4}" for i in range(n)]

        ex = temp_db.async_executor()
        ex.append_samples("PrimA", ts, hosts, vals, cnts)
        ex.append_samples("PrimB", ts, hosts, vals, cnts, primitive=True)
        ex.wait_completion()

        def rows(type_name):
            return temp_db.query(
                "sql",
                f"SELECT ts, host, val, cnt FROM {type_name} WHERE host = 'h2' "
                f"AND ts BETWEEN {int(ts[0])} AND {int(ts[0]) + 200_000} "
                "ORDER BY ts",  # nosec B608 - test-owned type name
            ).to_list()

        boxed, primitive = rows("PrimA"), rows("PrimB")
        assert len(boxed) == len(primitive) and len(boxed) > 0
        for row_boxed, row_primitive in zip(boxed, primitive):
            for key in ("ts", "host", "val", "cnt"):
                assert row_boxed.get(key) == row_primitive.get(key), key

        counts = [
            int(
                temp_db.query(
                    "sql",
                    f"SELECT count(*) AS n FROM {t}",  # nosec B608 - test-owned type name
                ).to_list()[0]["n"]
            )
            for t in ("PrimA", "PrimB")
        ]
        assert counts == [n, n]

    def test_repeated_tag_values_are_stored_distinctly(self, temp_db):
        """Memoised string conversion must not conflate or alias tag values.

        append_samples reuses one converted Java object per distinct str,
        because tag columns are low cardinality by design and converting each
        of 2.59M elements separately dominated ten-tag TSBS ingest. Sharing an
        immutable String across rows is safe, but only if the cache is keyed
        correctly, so this checks the two ways it could go wrong: a value must
        not leak into rows that had a different one, and a column mixing
        strings with non-strings must still round-trip.
        """
        temp_db.command(
            "sql",
            "CREATE TIMESERIES TYPE TagMemo TIMESTAMP ts "
            "TAGS (host STRING, region STRING) FIELDS (val DOUBLE) SHARDS 2",
        )
        n = 600
        base = 1_700_000_000_000
        ts = [base + i * 1000 for i in range(n)]
        # Heavy repetition with interleaving, so an off-by-one in the cache
        # would show up as a wrong pairing rather than a wrong count.
        hosts = [f"h{i % 3}" for i in range(n)]
        regions = [f"r{(i // 7) % 5}" for i in range(n)]
        vals = [float(i) for i in range(n)]

        ex = temp_db.async_executor()
        ex.append_samples("TagMemo", ts, hosts, regions, vals)
        ex.wait_completion()

        rows = temp_db.query(
            "sql", "SELECT ts, host, region, val FROM TagMemo ORDER BY ts"
        ).to_list()
        assert len(rows) == n
        for i, row in enumerate(rows):
            assert row["host"] == hosts[i], f"host mismatch at {i}"
            assert row["region"] == regions[i], f"region mismatch at {i}"
            assert float(row["val"]) == vals[i], f"val mismatch at {i}"
        # Every distinct value survived, so the cache did not collapse them.
        assert {r["host"] for r in rows} == set(hosts)
        assert {r["region"] for r in rows} == set(regions)

    def test_primitive_batch_accepts_plain_sequences(self, temp_db):
        """Lists, not just ndarrays: the batch path types each column itself."""
        temp_db.command(
            "sql",
            "CREATE TIMESERIES TYPE PrimList TIMESTAMP ts "
            "TAGS (host STRING) FIELDS (val DOUBLE) SHARDS 1",
        )
        n = 500
        ex = temp_db.async_executor()
        ex.append_samples(
            "PrimList",
            [1_700_000_000_000 + i * 1000 for i in range(n)],
            [f"h{i % 3}" for i in range(n)],
            [float(i) / 4 for i in range(n)],
            primitive=True,
        )
        ex.wait_completion()
        got = temp_db.query("sql", "SELECT count(*) AS n FROM PrimList").to_list()[0][
            "n"
        ]
        assert int(got) == n


class TestVectorColumnsDataFrame:
    def test_vector_column_to_dataframe(self, temp_db):
        import arcadedb_embedded as arcadedb

        pd = __import__("pytest").importorskip("pandas")
        temp_db.command("sql", "CREATE DOCUMENT TYPE EmbDf")
        temp_db.command("sql", "CREATE PROPERTY EmbDf.v ARRAY_OF_FLOATS")
        with temp_db.transaction():
            for i in range(10):
                temp_db.command(
                    "sql",
                    "INSERT INTO EmbDf SET v = :v",
                    {"v": arcadedb.to_java_float_array([i, i + 1.0])},
                )
        df = temp_db.query("sql", "SELECT v FROM EmbDf").to_dataframe()
        assert len(df) == 10
        assert len(df["v"].iloc[3]) == 2


class TestInsertManyInsideACallersTransaction:
    """insert_many inside an open transaction belongs to that transaction.

    The documented contract is that `commit_every` is ignored when a
    transaction is already open, so the caller's commit or rollback decides
    the whole batch. The Java fast path (DocumentBatcher.insertManyJson)
    committed and reopened the caller's transaction every `commit_every` rows
    regardless, so a rollback left every completed chunk behind (found
    2026-09-26 by a docs audit; the Python fallback path already guarded it).
    """

    def test_rollback_after_insert_many_leaves_nothing(self, temp_db):
        temp_db.command("sql", "CREATE DOCUMENT TYPE TxBatch")
        rows = [{"id": i} for i in range(25)]

        class Boom(Exception):
            pass

        with pytest.raises(Boom):
            with temp_db.transaction():
                temp_db.insert_many("TxBatch", rows, commit_every=10)
                raise Boom()

        assert _count(temp_db, "TxBatch") == 0

    def test_commit_after_insert_many_keeps_every_row(self, temp_db):
        temp_db.command("sql", "CREATE DOCUMENT TYPE TxBatchOk")
        rows = [{"id": i} for i in range(25)]

        with temp_db.transaction():
            assert temp_db.insert_many("TxBatchOk", rows, commit_every=10) == 25

        assert _count(temp_db, "TxBatchOk") == 25
