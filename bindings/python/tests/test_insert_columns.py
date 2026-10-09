"""Tests for Database.insert_columns, the columnar bulk insert (bindings issue #150).

Each column crosses the bridge once as a typed array and the documents are built
Java-side, instead of one JSON text per batch. These tests hold what a user
relies on: LONG, DOUBLE, BOOLEAN, STRING, and null values land with their declared
types, the failure contract is insert_many's (a transaction the call opened is
rolled back whole, a caller's own is left alone, #7882), bad input is refused
before anything is written, and the rows are the ones insert_many would have
stored.
"""

import time

import pytest

np = pytest.importorskip("numpy")

from arcadedb_embedded.exceptions import ArcadeDBError  # noqa: E402


def _count(db, type_name, where=""):
    q = f"SELECT count(*) AS n FROM {type_name} {where}"  # nosec B608 - test-controlled
    return int(db.query("sql", q).to_list()[0]["n"])


def _make_type(db, name="Col", unique_id=False):
    db.command("sql", f"CREATE DOCUMENT TYPE {name}")
    db.command("sql", f"CREATE PROPERTY {name}.id LONG")
    db.command("sql", f"CREATE PROPERTY {name}.qty INTEGER")
    db.command("sql", f"CREATE PROPERTY {name}.price DOUBLE")
    db.command("sql", f"CREATE PROPERTY {name}.flag BOOLEAN")
    db.command("sql", f"CREATE PROPERTY {name}.label STRING")
    if unique_id:
        db.command("sql", f"CREATE INDEX ON {name} (id) UNIQUE")


def _columns(n, offset=0):
    ids = np.arange(offset, offset + n, dtype=np.int64)
    return {
        "id": ids,
        "qty": (ids % 50).astype(np.int32),
        "price": ids.astype(np.float64) * 0.25,
        "flag": (ids % 3 == 0),
        "label": [f"row_{i}" for i in ids.tolist()],
    }


class TestTypedValuesLand:
    def test_every_kind_lands_with_its_declared_type_and_value(self, temp_db):
        _make_type(temp_db)
        n = 1_000
        written = temp_db.insert_columns("Col", _columns(n))
        assert written == n and _count(temp_db, "Col") == n

        row = temp_db.query("sql", "SELECT FROM Col WHERE id = 7").to_list()[0]
        assert row["id"] == 7 and isinstance(row["id"], int)
        assert row["qty"] == 7 and isinstance(row["qty"], int)
        assert row["price"] == pytest.approx(1.75) and isinstance(row["price"], float)
        assert row["flag"] is False and row["label"] == "row_7"
        assert (
            temp_db.query("sql", "SELECT FROM Col WHERE id = 9").to_list()[0]["flag"]
            is True
        )

        agg = temp_db.query(
            "sql", "SELECT sum(id) AS s, sum(price) AS p, sum(qty) AS q FROM Col"
        ).to_list()[0]
        ids = np.arange(n)
        assert int(agg["s"]) == int(ids.sum())
        assert float(agg["p"]) == pytest.approx(float((ids * 0.25).sum()))
        assert int(agg["q"]) == int((ids % 50).sum())

    def test_nulls_in_a_sequence_column_are_nulls(self, temp_db):
        _make_type(temp_db)
        labels = ["a", None, "c", None, "e"]
        temp_db.insert_columns(
            "Col", {"id": np.arange(5, dtype=np.int64), "label": labels}
        )
        assert _count(temp_db, "Col", "WHERE label IS NULL") == 2
        assert _count(temp_db, "Col", "WHERE label = 'c'") == 1

    def test_a_numpy_string_array_and_an_object_array_with_none_cross_per_element(
        self, temp_db
    ):
        _make_type(temp_db)
        temp_db.insert_columns(
            "Col",
            {"id": np.arange(3, dtype=np.int64), "label": np.array(["a", "bb", "a"])},
        )
        temp_db.insert_columns(
            "Col",
            {
                "id": np.arange(3, 6, dtype=np.int64),
                "label": np.array(["c", None, "e"], dtype=object),
            },
        )
        assert _count(temp_db, "Col") == 6
        assert _count(temp_db, "Col", "WHERE label = 'a'") == 2
        assert _count(temp_db, "Col", "WHERE label = 'bb'") == 1
        assert _count(temp_db, "Col", "WHERE label IS NULL") == 1

    def test_a_python_list_of_numbers_crosses_per_element(self, temp_db):
        _make_type(temp_db)
        temp_db.insert_columns(
            "Col",
            {"id": [1, 2, 3], "price": [0.5, 1.5, 2.5], "flag": [True, False, True]},
        )
        agg = temp_db.query(
            "sql", "SELECT sum(id) AS s, sum(price) AS p FROM Col WHERE flag = true"
        ).to_list()[0]
        assert int(agg["s"]) == 4 and float(agg["p"]) == pytest.approx(3.0)

    def test_a_pandas_frame_and_its_nullable_columns(self, temp_db):
        pd = pytest.importorskip("pandas")
        _make_type(temp_db)
        df = pd.DataFrame(
            {
                "id": pd.array([1, 2, 3], dtype="Int64"),
                "qty": pd.array([10, None, 30], dtype="Int64"),
                "label": pd.array(["x", None, "z"], dtype="string"),
                "price": [1.0, 2.0, 3.0],
            }
        )
        assert temp_db.insert_columns("Col", df) == 3
        assert _count(temp_db, "Col", "WHERE qty IS NULL") == 1
        assert _count(temp_db, "Col", "WHERE label IS NULL") == 1
        assert _count(temp_db, "Col", "WHERE qty = 30 AND label = 'z'") == 1

    def test_the_rows_are_the_ones_insert_many_stores(self, temp_db):
        _make_type(temp_db, "ByCols")
        _make_type(temp_db, "ByRows")
        n = 2_500
        cols = _columns(n)
        temp_db.insert_columns("ByCols", cols, commit_every=1_000)
        rows = [
            {
                k: (v[i].item() if hasattr(v[i], "item") else v[i])
                for k, v in cols.items()
            }
            for i in range(n)
        ]
        temp_db.insert_many("ByRows", rows, commit_every=1_000)

        q = "SELECT count(*) AS n, sum(id) AS s, sum(qty) AS q, sum(price) AS p, count(DISTINCT label) AS l FROM "
        a = temp_db.query("sql", q + "ByCols").to_list()[0]  # nosec B608
        b = temp_db.query("sql", q + "ByRows").to_list()[0]  # nosec B608
        assert {k: a[k] for k in ("n", "s", "q", "l")} == {
            k: b[k] for k in ("n", "s", "q", "l")
        }
        assert float(a["p"]) == pytest.approx(float(b["p"]))

    def test_commit_every_batches_land_every_row(self, temp_db):
        _make_type(temp_db)
        n = 25_000
        assert temp_db.insert_columns("Col", _columns(n), commit_every=10_000) == n
        assert _count(temp_db, "Col") == n


class TestParallelMode:
    """insert_columns(parallel=True): the same columns, handed to the async executor's bucket writers."""

    def test_the_parallel_mode_lands_every_row_with_its_types(self, temp_db):
        temp_db.command("sql", "CREATE DOCUMENT TYPE ColPar BUCKETS 3")
        temp_db.command("sql", "CREATE PROPERTY ColPar.id LONG")
        temp_db.command("sql", "CREATE PROPERTY ColPar.price DOUBLE")
        temp_db.command("sql", "CREATE PROPERTY ColPar.label STRING")
        n = 9_742
        ids = np.arange(n, dtype=np.int64)
        written = temp_db.insert_columns(
            "ColPar",
            {"id": ids, "price": ids * 0.5, "label": [f"r{i}" for i in range(n)]},
            parallel=True,
        )
        assert written == n and _count(temp_db, "ColPar") == n
        agg = temp_db.query(
            "sql", "SELECT sum(id) AS s, sum(price) AS p FROM ColPar"
        ).to_list()[0]
        assert int(agg["s"]) == int(ids.sum()) and float(agg["p"]) == pytest.approx(
            float((ids * 0.5).sum())
        )

    def test_a_record_the_writers_reject_is_reported_not_dropped(self, temp_db):
        temp_db.command("sql", "CREATE DOCUMENT TYPE ColDup")
        temp_db.command("sql", "CREATE PROPERTY ColDup.id LONG")
        temp_db.command("sql", "CREATE INDEX ON ColDup (id) UNIQUE")
        ids = np.arange(200, dtype=np.int64)
        ids[150] = ids[7]
        with pytest.raises(ArcadeDBError, match="failed record"):
            temp_db.insert_columns("ColDup", {"id": ids}, parallel=True)

    def test_a_failed_parallel_load_is_final_when_it_raises(self, temp_db):
        # Rows handed to the writers before a failure are committed by them whatever the call does
        # (CodeRabbit on upstream #9294): the call must wait for them before raising, so nothing lands
        # after the error, and the message must say rows may have been stored, not imply a rollback.
        temp_db.command("sql", "CREATE DOCUMENT TYPE ColBad BUCKETS 3")
        temp_db.command("sql", "CREATE PROPERTY ColBad.n INTEGER")
        values = [str(i) for i in range(2_000)]
        values[1_500] = "not a number"
        with pytest.raises(
            ArcadeDBError, match="may have been stored|failed record"
        ) as err:
            temp_db.insert_columns("ColBad", {"n": values}, parallel=True)
        assert "rolled back" not in str(err.value)
        stored = _count(temp_db, "ColBad")
        time.sleep(0.5)
        assert _count(temp_db, "ColBad") == stored <= 2_000


class TestFailureContract:
    def test_a_failure_rolls_back_the_whole_call(self, temp_db):
        _make_type(temp_db, unique_id=True)
        cols = _columns(300)
        cols["id"][250] = cols["id"][3]  # a duplicate key near the end
        with pytest.raises(ArcadeDBError):
            temp_db.insert_columns("Col", cols, commit_every=100)
        # batches the call had already committed are NOT undone: a failure rolls back the
        # transaction open at that moment, as insert_many does. 300 rows, commit_every=100, the
        # duplicate in the third batch: the first two batches stay, the third is rolled back whole
        assert _count(temp_db, "Col") == 200
        assert temp_db.insert_columns("Col", _columns(10, offset=1_000)) == 10

    def test_a_single_batch_failure_leaves_nothing_behind(self, temp_db):
        _make_type(temp_db, unique_id=True)
        cols = _columns(50)
        cols["id"][40] = cols["id"][1]
        with pytest.raises(ArcadeDBError):
            temp_db.insert_columns("Col", cols, commit_every=10_000)
        assert _count(temp_db, "Col") == 0
        assert not temp_db.is_transaction_active()

    def test_a_callers_own_transaction_is_left_to_the_caller(self, temp_db):
        _make_type(temp_db)
        temp_db.begin()
        assert temp_db.insert_columns("Col", _columns(20), commit_every=5) == 20
        assert temp_db.is_transaction_active()  # not committed behind the caller's back
        temp_db.rollback()
        assert _count(temp_db, "Col") == 0

    def test_a_callers_transaction_survives_a_failed_call_untouched(self, temp_db):
        _make_type(temp_db, unique_id=True)
        temp_db.begin()
        cols = _columns(10)
        cols["id"][5] = cols["id"][0]
        with pytest.raises(ArcadeDBError):
            temp_db.insert_columns("Col", cols)
        assert (
            temp_db.is_transaction_active()
        )  # the failed call did not roll the caller's transaction back
        temp_db.rollback()


class TestBadInputIsRefusedBeforeAnythingIsWritten:
    def test_ragged_columns(self, temp_db):
        _make_type(temp_db)
        with pytest.raises(ValueError, match="differ in length"):
            temp_db.insert_columns("Col", {"id": np.arange(5), "price": np.arange(4.0)})
        assert _count(temp_db, "Col") == 0

    def test_nothing_to_insert(self, temp_db):
        _make_type(temp_db)
        with pytest.raises(ValueError, match="at least one column"):
            temp_db.insert_columns("Col", {})
        assert temp_db.insert_columns("Col", {"id": np.arange(0, dtype=np.int64)}) == 0

    def test_a_name_that_is_not_a_string(self, temp_db):
        _make_type(temp_db)
        with pytest.raises(ValueError, match="strings"):
            temp_db.insert_columns("Col", {1: np.arange(3)})

    def test_a_dtype_that_does_not_cross_natively(self, temp_db):
        _make_type(temp_db)
        with pytest.raises(TypeError, match="insert_many"):
            temp_db.insert_columns(
                "Col",
                {
                    "id": np.arange(3),
                    "when": np.array(["2026-01-01"] * 3, dtype="datetime64[D]"),
                },
            )
        assert _count(temp_db, "Col") == 0

    def test_an_unsigned_value_beyond_the_signed_range(self, temp_db):
        _make_type(temp_db)
        with pytest.raises(ValueError, match="64-bit signed"):
            temp_db.insert_columns(
                "Col", {"id": np.array([1, 2**63 + 5], dtype=np.uint64)}
            )

    @pytest.mark.parametrize("value", ["abc", b"abc", bytearray(b"abc")])
    def test_a_string_or_bytes_column_is_refused_not_split(self, temp_db, value):
        _make_type(temp_db)
        with pytest.raises(TypeError, match="not a sequence of values"):
            temp_db.insert_columns("Col", {"label": value})
        assert _count(temp_db, "Col") == 0

    def test_a_two_dimensional_column(self, temp_db):
        _make_type(temp_db)
        with pytest.raises(ValueError, match="one-dimensional"):
            temp_db.insert_columns("Col", {"id": np.zeros((2, 2), dtype=np.int64)})

    def test_a_closed_database(self, temp_db):
        _make_type(temp_db)
        temp_db.close()
        with pytest.raises(ArcadeDBError):
            temp_db.insert_columns("Col", {"id": np.arange(3)})
