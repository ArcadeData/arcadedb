"""
Tests for enhanced ResultSet and Result functionality.
"""

import arcadedb_embedded as arcadedb
import pytest


def test_resultset_to_list(temp_db_path):
    """Test ResultSet.to_list() method."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE User")

        with db.transaction():
            db.command("sql", "INSERT INTO User SET name = 'Alice', age = 30")
            db.command("sql", "INSERT INTO User SET name = 'Bob', age = 25")
            db.command("sql", "INSERT INTO User SET name = 'Charlie', age = 35")

        result = db.query("sql", "SELECT FROM User ORDER BY name")

        # Test to_list with type conversion
        users_list = result.to_list(convert_types=True)
        assert isinstance(users_list, list)
        assert len(users_list) == 3

        # Verify it's a list of dicts
        assert all(isinstance(item, dict) for item in users_list)

        # Check first user
        assert users_list[0]["name"] == "Alice"
        assert users_list[0]["age"] == 30

        # Check last user
        assert users_list[2]["name"] == "Charlie"
        assert users_list[2]["age"] == 35


def test_resultset_to_dataframe(temp_db_path):
    """Test ResultSet.to_dataframe() method."""
    pd = pytest.importorskip("pandas")

    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE Product")

        with db.transaction():
            db.command(
                "sql",
                "INSERT INTO Product SET name = 'Widget', price = 9.99, stock = 100",
            )
            db.command(
                "sql",
                "INSERT INTO Product SET name = 'Gadget', price = 19.99, stock = 50",
            )
            db.command(
                "sql",
                "INSERT INTO Product SET name = 'Doohickey', price = 14.99, stock = 75",
            )

        result = db.query("sql", "SELECT FROM Product ORDER BY name")

        # Test to_dataframe
        df = result.to_dataframe(convert_types=True)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3

        # Check column names
        assert "name" in df.columns
        assert "price" in df.columns
        assert "stock" in df.columns

        # Check values
        assert df.iloc[1]["name"] == "Gadget"
        assert df.iloc[1]["stock"] == 50

        # Test DataFrame operations
        total_stock = df["stock"].sum()
        assert total_stock == 225


def test_resultset_iter_chunks(temp_db_path):
    """Test ResultSet.iter_chunks() for memory-efficient iteration."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE Item")

        with db.transaction():
            # Insert 250 items
            for i in range(250):
                db.command(
                    "sql",
                    f"INSERT INTO `Item` SET id = {i}, value = {i * 10}, "
                    f"batchNum = {i // 100}",
                )

        result = db.query("sql", "SELECT FROM `Item` ORDER BY id")

        # Test chunked iteration with chunk size 100
        chunks = list(result.iter_chunks(size=100))

        # Should have 3 chunks (100, 100, 50)
        assert len(chunks) == 3
        assert len(chunks[0]) == 100
        assert len(chunks[1]) == 100
        assert len(chunks[2]) == 50

        # Verify chunk data
        first_chunk = chunks[0]
        assert first_chunk[0]["id"] == 0
        assert first_chunk[99]["id"] == 99

        last_chunk = chunks[2]
        assert last_chunk[0]["id"] == 200
        assert last_chunk[49]["id"] == 249


def test_resultset_count(temp_db_path):
    """Test ResultSet.count() method."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE Counter")

        with db.transaction():
            for i in range(50):
                db.command("sql", "INSERT INTO Counter SET num = ?", i)

        result = db.query("sql", "SELECT FROM Counter")

        # Test count without loading all results
        count = result.count()
        assert count == 50


def test_resultset_first(temp_db_path):
    """Test ResultSet.first() method."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE FirstTest")

        with db.transaction():
            db.command("sql", "INSERT INTO FirstTest SET value = 'first'")
            db.command("sql", "INSERT INTO FirstTest SET value = 'second'")
            db.command("sql", "INSERT INTO FirstTest SET value = 'third'")

        # Test first() returns first result
        result = db.query("sql", "SELECT FROM FirstTest ORDER BY value")
        first_record = result.first()

        assert first_record is not None
        assert first_record.get("value") == "first"

        # Test first() returns None for empty results
        result_empty = db.query(
            "sql", "SELECT FROM FirstTest WHERE value = 'nonexistent'"
        )
        first_empty = result_empty.first()
        assert first_empty is None


def test_resultset_one(temp_db_path):
    """Test ResultSet.one() method."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE OneTest")

        with db.transaction():
            db.command("sql", "INSERT INTO OneTest SET id = 1, value = 'unique'")
            db.command("sql", "INSERT INTO OneTest SET id = 2, value = 'multiple'")
            db.command("sql", "INSERT INTO OneTest SET id = 3, value = 'multiple'")

        # Test one() returns single result
        result = db.query("sql", "SELECT FROM OneTest WHERE value = 'unique'")
        record = result.one()
        assert record is not None
        assert record.get("value") == "unique"

        # Test one() raises error for empty results
        try:
            result_empty = db.query(
                "sql", "SELECT FROM OneTest WHERE value = 'nonexistent'"
            )
            result_empty.one()
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "no results" in str(e).lower()

        # Test one() raises error for multiple results
        try:
            result_multi = db.query(
                "sql", "SELECT FROM OneTest WHERE value = 'multiple'"
            )
            result_multi.one()
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "multiple" in str(e).lower() or "more than one" in str(e).lower()


def test_resultset_iteration_patterns(temp_db_path):
    """Test various iteration patterns with ResultSet."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE IterTest")

        with db.transaction():
            for i in range(10):
                db.command("sql", "INSERT INTO IterTest SET num = ?", i)

        # Test traditional iteration
        result = db.query("sql", "SELECT FROM IterTest ORDER BY num")
        nums_iter = [r.get("num") for r in result]
        assert len(nums_iter) == 10
        assert nums_iter[0] == 0
        assert nums_iter[9] == 9

        # Test list conversion for traditional operations
        result2 = db.query("sql", "SELECT FROM IterTest ORDER BY num")
        results_list = list(result2)
        assert len(results_list) == 10

        # Test first on iterated result
        result3 = db.query("sql", "SELECT FROM IterTest ORDER BY num DESC")
        first = result3.first()
        assert first.get("num") == 9  # Descending order


def test_result_representation(temp_db_path):
    """Test Result.__repr__() for better debugging."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE ReprTest")

        with db.transaction():
            db.command(
                "sql",
                "INSERT INTO ReprTest SET name = 'test', value = 42, active = true",
            )

        result = db.query("sql", "SELECT FROM ReprTest")
        record = result.first()

        # Test __repr__
        repr_str = repr(record)
        assert isinstance(repr_str, str)
        assert "Result" in repr_str
        # Should show some properties
        assert "name" in repr_str or "test" in repr_str or "value" in repr_str


def test_resultset_with_complex_queries(temp_db_path):
    """Test ResultSet methods with complex queries."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE Sales")
        db.command("sql", "CREATE PROPERTY Sales.amount DECIMAL")
        db.command("sql", "CREATE PROPERTY Sales.region STRING")

        with db.transaction():
            # Insert sample data
            regions = ["North", "South", "East", "West"]
            for i in range(100):
                region = regions[i % 4]
                amount = 100.0 + (i * 5.5)
                db.command(
                    "sql",
                    f"INSERT INTO Sales SET region = '{region}', amount = {amount}",
                )

        # Test aggregation query with to_list
        result = db.query(
            "sql",
            """
            SELECT region, count(*) as count, sum(amount) as total
            FROM Sales
            GROUP BY region
            ORDER BY region
        """,
        )

        agg_list = result.to_list()
        assert len(agg_list) == 4  # 4 regions

        # Each group should have 25 records
        for item in agg_list:
            assert item["count"] == 25

        # Test filtering with first()
        result2 = db.query(
            "sql",
            "SELECT FROM Sales WHERE region = 'North' ORDER BY amount DESC",
        )
        highest_north = result2.first()
        assert highest_north is not None
        assert highest_north.get("region") == "North"


def test_resultset_empty_handling(temp_db_path):
    """Test ResultSet methods with empty results."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE EmptyTest")

        # Query empty table
        result = db.query("sql", "SELECT FROM EmptyTest")

        # Test to_list on empty result
        empty_list = result.to_list()
        assert empty_list == []

        # Test count on empty result
        result2 = db.query("sql", "SELECT FROM EmptyTest")
        count = result2.count()
        assert count == 0

        # Test first on empty result
        result3 = db.query("sql", "SELECT FROM EmptyTest")
        first = result3.first()
        assert first is None

        # Test iter_chunks on empty result
        result4 = db.query("sql", "SELECT FROM EmptyTest")
        chunks = list(result4.iter_chunks(size=10))
        assert len(chunks) == 0


def test_resultset_reusability(temp_db_path):
    """Test that ResultSet can only be iterated once (Java ResultSet behavior)."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE ReuseTest")

        with db.transaction():
            db.command("sql", "INSERT INTO ReuseTest SET value = 1")
            db.command("sql", "INSERT INTO ReuseTest SET value = 2")

        result = db.query("sql", "SELECT FROM ReuseTest")

        # First iteration works
        first_list = list(result)
        assert len(first_list) == 2

        # Second iteration should be empty (ResultSet is consumed)
        second_list = list(result)
        assert len(second_list) == 0

        # Need new query for fresh ResultSet
        result2 = db.query("sql", "SELECT FROM ReuseTest")
        fresh_list = list(result2)
        assert len(fresh_list) == 2


def test_result_get_rid_and_vertex(temp_db_path):
    """Test get_rid() and get_vertex() methods on Result."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE VERTEX TYPE Person")

        with db.transaction():
            db.command("sql", "INSERT INTO Person SET name = 'Alice'")

        result = db.query("sql", "SELECT FROM Person").first()

        # Test get_rid()
        rid = result.get_rid()
        assert rid is not None
        assert isinstance(rid, str)
        assert rid.startswith("#")

        # Test get_vertex()
        vertex = result.get_vertex()
        assert vertex is not None
        # It should be a Java object
        assert "Vertex" in str(vertex) or "Vertex" in vertex.getClass().getName()

        # Verify we can use the vertex object
        assert vertex.get("name") == "Alice"


def test_result_to_json_with_arrays(temp_db_path):
    """Result.to_json() should serialize list properties as JSON arrays."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE JsonArrayTest")

        with db.transaction():
            db.command(
                "sql",
                "INSERT INTO JsonArrayTest SET tags = ['a', 'b', 'c']",
            )

        result = db.query("sql", "SELECT FROM JsonArrayTest").first()
        json_str = result.to_json()

        assert '"tags"' in json_str
        assert '["a","b","c"]' in json_str or '["a", "b", "c"]' in json_str


def test_to_dict_one_crossing_matches_the_per_property_path(temp_db_path):
    """Result.to_dict() reads a row in one bridge call (RowAccess); it must give
    exactly what reading each property on its own gives, for every value type."""
    from datetime import date, datetime
    from decimal import Decimal

    import arcadedb_embedded as arcadedb
    from arcadedb_embedded.results import _bridge_class
    from arcadedb_embedded.type_conversion import convert_java_to_python

    # Run alone, this is before the JVM starts: the answer must not be cached.
    _bridge_class("RowAccess")
    with arcadedb.create_database(temp_db_path) as db:
        assert (
            _bridge_class("RowAccess") is not None
        ), "the bridge jar must carry RowAccess"
        db.command("sql", "CREATE DOCUMENT TYPE Mixed")
        db.command("sql", "CREATE PROPERTY Mixed.stamp DATETIME")
        db.command("sql", "CREATE PROPERTY Mixed.on_day DATE")
        db.command("sql", "CREATE PROPERTY Mixed.price DECIMAL")
        with db.transaction():
            doc = db.new_document("Mixed")
            doc.set("i", 42).set("big", 2**40).set("f", 3.25).set("s", "text")
            doc.set("b", True).set("nothing", None).set("tags", ["a", "b"])
            doc.set("nested", {"k": 1, "inner": {"x": [1, 2]}})
            doc.set("stamp", datetime(2026, 9, 27, 12, 30, 5))
            doc.set("on_day", date(2026, 9, 27)).set("price", Decimal("12.50"))
            doc.set("blob", b"\xff\x00")
            doc.save()

        for query in (
            "SELECT FROM Mixed",
            "SELECT i, s, nested, price FROM Mixed",
            "SELECT count(*) AS n, max(f) AS top FROM Mixed",
        ):
            row = db.query("sql", query).first()
            per_property = {
                name: convert_java_to_python(row._java_result.getProperty(name))
                for name in (str(n) for n in row._java_result.getPropertyNames())
            }
            one_crossing = row.to_dict()
            assert one_crossing == per_property, query
            assert list(one_crossing) == list(per_property), query  # same key order

        # to_list() fetches rows in batches (TypedRows.nextRows): same dicts,
        # same order, as reading each row and property on its own, including
        # after rows were already taken from the same result set.
        with db.transaction():
            for i in range(1200):
                db.command("sql", "INSERT INTO Mixed SET i = ?, s = ?", i, f"s{i}")
        expected = []
        rs = db.query("sql", "SELECT i, s, price FROM Mixed ORDER BY i")
        for r in rs:
            expected.append(
                {
                    str(n): convert_java_to_python(r._java_result.getProperty(str(n)))
                    for n in r._java_result.getPropertyNames()
                }
            )
        assert (
            db.query("sql", "SELECT i, s, price FROM Mixed ORDER BY i").to_list()
            == expected
        )
        rs = db.query("sql", "SELECT i, s, price FROM Mixed ORDER BY i")
        assert next(iter(rs)) is not None  # one row taken, the set left open
        assert rs.to_list() == expected[1:]


class TestResultSetReleasesTheEngineCursor:
    """An exhausted, or no longer read, result set closes its Java result set.

    Since the engine's parallel scan (ArcadeData/arcadedb#8524, 26.10.1) a
    query whose LIMIT is satisfied keeps its scan's producer threads parked
    until the result set is closed or ten minutes pass, and a few such result
    sets stall the next query that needs the producer pool
    (ArcadeData/arcadedb#8594). Example 05 hung that way on its fifth
    `@rid > <last> LIMIT 5000` page: every page was read to its end, and none
    was ever closed.
    """

    PAGE = 5_000
    DOCS = 300_000

    def _load(self, db):
        db.command("sql", "CREATE DOCUMENT TYPE Paged")  # the default single bucket
        db.insert_many(
            "Paged",
            [{"k": i, "pad": "x" * 40} for i in range(self.DOCS)],
            commit_every=10_000,
        )

    def _walk(self, db, read_page):
        """Every page of the type, in RID order, read with read_page."""
        last, pages, rows = "#-1:-1", 0, 0
        while True:
            q = f"SELECT @rid AS rid, k FROM Paged WHERE @rid > {last} LIMIT {self.PAGE}"  # nosec B608 - test-owned
            page = read_page(db.query("sql", q))
            pages += 1
            rows += len(page)
            if len(page) < self.PAGE:
                return pages, rows
            last = str(page[-1]["rid"])

    def _walk_within(self, db, read_page, seconds=60):
        import threading

        done = {}

        def run():
            done["out"] = self._walk(db, read_page)

        t = threading.Thread(target=run, daemon=True)
        t.start()
        t.join(seconds)
        assert not t.is_alive(), (
            f"RID-paged reads stalled for {seconds} s: result sets read to their "
            "end were not closed (ArcadeData/arcadedb#8594)"
        )
        return done["out"]

    def test_paging_by_iteration_does_not_stall(self, temp_db):
        self._load(temp_db)
        pages, rows = self._walk_within(
            temp_db, lambda rs: [{"rid": r.get("rid"), "k": r.get("k")} for r in rs]
        )
        assert rows == self.DOCS and pages == self.DOCS // self.PAGE + 1

    def test_paging_by_to_list_does_not_stall(self, temp_db):
        self._load(temp_db)
        pages, rows = self._walk_within(temp_db, lambda rs: rs.to_list())
        assert rows == self.DOCS and pages == self.DOCS // self.PAGE + 1

    def test_exhaustion_first_and_one_close_the_java_result_set(self, temp_db):
        temp_db.command("sql", "CREATE DOCUMENT TYPE Few")
        with temp_db.transaction():
            for i in range(3):
                temp_db.command("sql", "INSERT INTO Few SET k = ?", i)

        rs = temp_db.query("sql", "SELECT FROM Few")
        assert len(list(rs)) == 3
        assert rs._closed

        rs = temp_db.query("sql", "SELECT FROM Few")
        assert len(rs.to_list()) == 3 and rs._closed

        rs = temp_db.query("sql", "SELECT FROM Few ORDER BY k")
        assert rs.first().get("k") == 0 and rs._closed

        rs = temp_db.query("sql", "SELECT FROM Few WHERE k = 1")
        assert rs.one().get("k") == 1 and rs._closed

    def test_a_set_closed_before_its_end_raises_when_read_again(self, temp_db):
        # first() closes the set with rows unread. Reading it again used to
        # return whatever the closed Java result set still handed out, which
        # changed between engine builds (the rest of the rows, then nothing);
        # now it says the rows are gone. A set read to its end stays empty.
        temp_db.command("sql", "CREATE DOCUMENT TYPE Some")
        with temp_db.transaction():
            for i in range(5):
                temp_db.command("sql", "INSERT INTO Some SET k = ?", i)
        q = "SELECT k FROM Some ORDER BY k"

        for read in (
            list,
            lambda r: r.to_list(),
            lambda r: r.first(),
            lambda r: r.count(),
            lambda r: list(r.iter_json_batches()),
            lambda r: r.to_columns(),
        ):
            rs = temp_db.query("sql", q)
            assert rs.first().get("k") == 0
            with pytest.raises(arcadedb.ArcadeDBError, match="closed before"):
                read(rs)

        rs = temp_db.query("sql", q)
        with rs:
            next(iter(rs))
        with pytest.raises(arcadedb.ArcadeDBError, match="closed before"):
            rs.to_list()

        rs = temp_db.query("sql", q)
        assert len(rs.to_list()) == 5
        assert list(rs) == [] and rs.to_list() == [] and rs.first() is None
        assert list(rs.iter_json_batches()) == []


class _CountingBridge:
    """Stands in for a bridge class in results._BRIDGE_CLASSES: counts each call and delegates."""

    def __init__(self, real, name):
        self._real, self._name, self.calls = real, name, 0

    def __getattr__(self, attr):
        target = getattr(self._real, attr)
        if attr != self._name:
            return target

        def counted(*args):
            self.calls += 1
            return target(*args)

        return counted


@pytest.fixture
def counting_bridge(monkeypatch):
    """Count the calls the Python layer makes into TypedRows.nextRows and RowBatcher.nextJsonBatch."""
    from arcadedb_embedded import results

    typed_rows = results._bridge_class("TypedRows")
    row_batcher = results._bridge_class("RowBatcher")
    assert typed_rows is not None and row_batcher is not None
    counters = {
        "nextRows": _CountingBridge(typed_rows, "nextRows"),
        "nextJsonBatch": _CountingBridge(row_batcher, "nextJsonBatch"),
    }
    monkeypatch.setitem(results._BRIDGE_CLASSES, "TypedRows", counters["nextRows"])
    monkeypatch.setitem(
        results._BRIDGE_CLASSES, "RowBatcher", counters["nextJsonBatch"]
    )
    return counters


class TestSmallResultsCostOneBridgeCall:
    """A result that fits one batch is read with ONE call into the bridge, and the bridge closes it.

    A JPype call is 3 to 4 microseconds, a fifth of a one-row read. to_list() asked
    TypedRows.nextRows a second time just to see an empty batch (the fix that #144 made for
    to_json_list()), and every drained result set cost one more crossing for close().
    nextRows and nextJsonBatch return fewer rows than asked for only when the result set is
    drained, and close it themselves.
    """

    @pytest.fixture
    def three(self, temp_db):
        temp_db.command("sql", "CREATE DOCUMENT TYPE Few")
        with temp_db.transaction():
            for i in range(3):
                temp_db.command("sql", "INSERT INTO Few SET k = ?", i)
        return temp_db

    def test_to_list_makes_one_call_for_a_short_result(self, three, counting_bridge):
        rs = three.query("sql", "SELECT k FROM Few ORDER BY k")
        assert rs.to_list() == [{"k": 0}, {"k": 1}, {"k": 2}]
        assert counting_bridge["nextRows"].calls == 1
        assert rs._closed and rs._exhausted

    def test_to_list_of_an_empty_result_makes_one_call(self, three, counting_bridge):
        rs = three.query("sql", "SELECT k FROM Few WHERE k = 99")
        assert rs.to_list() == []
        assert counting_bridge["nextRows"].calls == 1
        assert rs._closed and rs._exhausted

    def test_to_json_list_makes_one_call_for_a_short_result(
        self, three, counting_bridge
    ):
        rs = three.query("sql", "SELECT k FROM Few ORDER BY k")
        assert rs.to_json_list() == [{"k": 0}, {"k": 1}, {"k": 2}]
        assert counting_bridge["nextJsonBatch"].calls == 1
        assert rs._closed and rs._exhausted

    def test_a_full_batch_still_ends_on_the_next_call(self, temp_db, counting_bridge):
        """Exactly one batch of rows (the to_list batch): the first batch is full, so one more call finds it drained."""
        from arcadedb_embedded import results

        batch = results._TYPED_BATCH_ROWS
        temp_db.command("sql", "CREATE DOCUMENT TYPE Exact")
        with temp_db.transaction():
            for i in range(batch):
                temp_db.command("sql", "INSERT INTO Exact SET k = ?", i)
        rs = temp_db.query("sql", "SELECT k FROM Exact ORDER BY k")
        rows = rs.to_list()
        assert [r["k"] for r in rows] == list(range(batch))
        assert counting_bridge["nextRows"].calls == 2
        assert rs._closed and rs._exhausted
        assert rs.to_list() == []  # a result set read to its end reads as empty

    def test_a_drained_set_reads_as_empty_and_is_not_an_error(
        self, three, counting_bridge
    ):
        rs = three.query("sql", "SELECT k FROM Few")
        assert len(rs.to_list()) == 3
        assert rs.to_list() == [] and list(rs) == [] and rs.first() is None
        assert list(rs.iter_json_batches()) == []
        rs.close()  # idempotent

    def test_python_does_not_cross_the_bridge_to_close_a_drained_set(
        self, three, counting_bridge, monkeypatch
    ):
        from arcadedb_embedded import results

        closes = []
        real_close = results.ResultSet.close
        monkeypatch.setattr(
            results.ResultSet,
            "close",
            lambda self: (closes.append(1), real_close(self))[1],
        )
        for read in ("to_list", "to_json_list"):
            rs = three.query("sql", "SELECT k FROM Few")
            assert len(getattr(rs, read)()) == 3
            assert rs._closed and rs._exhausted
        assert closes == []

    @pytest.mark.parametrize("batcher", ["nextRows", "nextJsonBatch"])
    def test_the_bridge_closes_what_it_drains_and_only_that(self, three, batcher):
        """Straight at the Java side: nextRows and nextJsonBatch close a result set when it
        has fewer rows than asked for, and leave one alone that still has rows."""
        import jpype
        from arcadedb_embedded import results

        interface = jpype.JClass("com.arcadedb.query.sql.executor.ResultSet")

        class Spy:
            def __init__(self, real):
                self.real, self.closed = real, 0

            def hasNext(self):
                return self.real.hasNext()

            def next(self):
                return self.real.next()

            def close(self):
                self.closed += 1
                self.real.close()

        keep = []  # the Python wrapper closes its Java result set when it is freed

        def spied(query):
            wrapper = three.query("sql", query)
            keep.append(wrapper)
            spy = Spy(wrapper._java_result_set)
            return spy, jpype.JObject(jpype.JProxy(interface, inst=spy), interface)

        bridge = results._bridge_class(
            "RowAccess" if batcher == "nextRows" else "RowBatcher"
        )
        call = getattr(bridge, batcher)

        spy, proxy = spied("SELECT k FROM Few ORDER BY k")
        assert len(call(proxy, 2)) > 0  # a full batch of two rows: left open
        assert spy.closed == 0
        call(proxy, 2)  # one row left: a short batch, drained, closed
        assert spy.closed == 1
        spy, proxy = spied("SELECT k FROM Few WHERE k = 99")
        call(proxy, 10)  # nothing at all
        assert spy.closed == 1

    def test_the_bridge_releases_the_engine_cursor(self, temp_db):
        """The Java result set is closed by the bridge when it drains it: a LIMIT that stops a
        parallel scan early leaves producer threads parked until close (arcadedb #8594), so
        reading page after page by to_list() and to_json_list() without anyone else calling
        close() must not stall."""
        temp_db.command("sql", "CREATE DOCUMENT TYPE Paged2")
        temp_db.insert_many(
            "Paged2",
            [{"k": i, "pad": "x" * 40} for i in range(60_000)],
            commit_every=10_000,
        )
        last = "#-1:-1"
        for read in ("to_list", "to_json_list") * 6:
            q = f"SELECT @rid AS rid, k FROM Paged2 WHERE @rid > {last} LIMIT 5000"  # nosec B608 - test-owned
            page = getattr(temp_db.query("sql", q), read)()
            assert len(page) == 5000
            last = str(page[-1]["rid"])
