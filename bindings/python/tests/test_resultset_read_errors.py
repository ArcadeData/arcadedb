"""An engine error raised while a result set is read is an ArcadeDBError (#173).

The engine plans and computes rows lazily, so ``query()`` returns and the statement's
error surfaces on the first or a later row. Every way of reading the rows raises it as
``ArcadeDBError`` with the Java message in its text and the Java exception as its cause,
like an error raised by ``query()`` itself. Before, it reached Python as the raw JPype
exception (``java.lang.UnsupportedOperationException`` for ArcadeData/arcadedb#9236).

The errors here are a division by zero (``ArithmeticErrorException``, an ArcadeDB
exception) and a ``format()`` that cannot apply its pattern (``IllegalArgumentException``,
a JDK one), each on the first row or on the second. ``E`` has one bucket, so rows come in
insertion order: ``i`` = 0, 1, 2, with ``v`` = 0, 'x', 2.
"""

import os
import shutil
import tempfile

import arcadedb_embedded as arcadedb
import jpype
import pytest
from arcadedb_embedded import ArcadeDBError

FAILS_ON_THE_FIRST_ROW = [
    pytest.param("SELECT 1 / (i - i) AS f FROM E", id="division-first"),
    pytest.param("SELECT format('%d', 'x' + i) AS f FROM E", id="format-first"),
]
FAILS_ON_THE_SECOND_ROW = [
    pytest.param("SELECT 1 / (i - 1) AS f FROM E", id="division-second"),
    pytest.param("SELECT format('%d', v) AS f FROM E", id="format-second"),
]


def _to_arrow(rs):
    pytest.importorskip("pyarrow")
    return rs.to_arrow()


def _to_dataframe(rs):
    pytest.importorskip("pandas")
    return rs.to_dataframe()


# Every way of reading a result set that reads past its first row.
READERS = [
    pytest.param(list, id="iteration"),
    pytest.param(lambda rs: [next(rs) for _ in range(3)], id="next"),
    pytest.param(lambda rs: rs.to_list(), id="to_list"),
    pytest.param(lambda rs: rs.to_list(convert_types=False), id="to_list-raw"),
    pytest.param(lambda rs: list(rs.iter_dicts()), id="iter_dicts"),
    pytest.param(lambda rs: list(rs.iter_chunks(size=1)), id="iter_chunks"),
    pytest.param(lambda rs: rs.count(), id="count"),
    pytest.param(lambda rs: rs.to_json_list(), id="to_json_list"),
    pytest.param(
        lambda rs: list(rs.iter_json_batches(batch_size=1)), id="json-batches"
    ),
    pytest.param(lambda rs: rs.to_columns(), id="to_columns"),
    pytest.param(lambda rs: rs.to_columns(batch_size=1), id="to_columns-batch-1"),
    pytest.param(_to_arrow, id="to_arrow"),
    pytest.param(_to_dataframe, id="to_dataframe"),
]


@pytest.fixture(scope="module")
def db():
    temp_dir = tempfile.mkdtemp(prefix="arcadedb_test_db_")
    database = arcadedb.create_database(os.path.join(temp_dir, "test_db"))
    database.command("sql", "CREATE DOCUMENT TYPE E BUCKETS 1")
    with database.transaction():
        for i, v in ((0, 0), (1, "x"), (2, 2)):
            database.command("sql", "INSERT INTO E SET i = ?, v = ?", i, v)
    yield database
    database.close()
    shutil.rmtree(temp_dir, ignore_errors=True)


def _assert_wraps_the_java_error(error):
    cause = error.__cause__
    assert isinstance(cause, jpype.JException), f"cause is {type(cause)!r}"
    assert isinstance(cause, jpype.JClass("java.lang.RuntimeException"))
    assert str(cause.getMessage()) in str(error)
    assert str(error).startswith("Reading the result set failed: ")


def test_the_engine_raises_these_errors_while_rows_are_read(db):
    """The premise: query() returns, the rows before the failing one read normally, and
    the failing one raises. If the engine starts raising in query() these tests no longer
    test reading."""
    rs = db.query("sql", "SELECT 1 / (i - 1) AS f FROM E")
    assert next(rs).get("f") == -1
    with pytest.raises(ArcadeDBError) as info:
        next(rs)
    _assert_wraps_the_java_error(info.value)


@pytest.mark.parametrize("read", READERS)
@pytest.mark.parametrize("query", FAILS_ON_THE_FIRST_ROW + FAILS_ON_THE_SECOND_ROW)
def test_every_reader_raises_arcadedb_error(db, query, read):
    rs = db.query("sql", query)
    with pytest.raises(ArcadeDBError) as info:
        read(rs)
    _assert_wraps_the_java_error(info.value)


@pytest.mark.parametrize("query", FAILS_ON_THE_FIRST_ROW)
def test_first_raises_arcadedb_error(db, query):
    rs = db.query("sql", query)
    with pytest.raises(ArcadeDBError) as info:
        rs.first()
    _assert_wraps_the_java_error(info.value)


@pytest.mark.parametrize("query", FAILS_ON_THE_FIRST_ROW + FAILS_ON_THE_SECOND_ROW)
def test_one_raises_the_engine_error_not_value_error(db, query):
    """one() reads a second row to check there is none: on the second-row queries the
    engine error, not 'multiple results', is what it raises."""
    with pytest.raises(ArcadeDBError) as info:
        db.query("sql", query).one()
    _assert_wraps_the_java_error(info.value)


def test_a_command_result_raises_arcadedb_error(db):
    rs = db.command("sql", "SELECT 1 / (i - 1) AS f FROM E")
    with pytest.raises(ArcadeDBError) as info:
        rs.to_list()
    _assert_wraps_the_java_error(info.value)
