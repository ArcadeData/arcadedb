"""Positional and named parameters reach the statement on every entry point (#172).

``Database.query()``, ``Database.command()``, and the async executor hand the engine one
explicitly typed argument: an ``Object[]`` for positional parameters and a
``java.util.Map`` for named ones. Splatted, a lone ``None`` matched the ``Object...``,
``Map``, and ``ContextConfiguration, Object...`` overloads of ``command`` alike, and JPype
raised "Ambiguous overloads" instead of binding null; ``(None, 1)`` did too.
"""

import arcadedb_embedded as arcadedb
import jpype
import pytest

# Three spellings of "one positional parameter, bound to null".
LONE_NULL = [
    pytest.param((None,), id="None"),
    pytest.param(([None],), id="[None]"),
    pytest.param(((None,),), id="(None,)"),
]


def _records(db):
    """Every record of T by its key ``k``, with the properties it has."""
    return {row["k"]: row for row in db.query("sql", "SELECT FROM T").to_list()}


@pytest.fixture
def db(temp_db):
    temp_db.command("sql", "CREATE DOCUMENT TYPE T")
    with temp_db.transaction():
        temp_db.command("sql", "INSERT INTO T SET k = 'set', v = 5, w = 0")
    return temp_db


@pytest.mark.parametrize("args", LONE_NULL)
def test_command_binds_a_lone_null(db, args):
    with db.transaction():
        db.command("sql", "INSERT INTO T SET k = 'new', v = ?", *args)
        db.command("sql", "UPDATE T SET v = ? WHERE k = 'set'", *args)

    records = _records(db)
    assert set(records) == {"set", "new"}
    assert "v" in records["new"] and records["new"]["v"] is None
    assert records["set"]["v"] is None and records["set"]["w"] == 0


@pytest.mark.parametrize("args", LONE_NULL)
def test_command_reads_a_lone_null_in_its_where(db, args):
    with db.transaction():
        db.command("sql", "INSERT INTO T SET k = 'null', v = null, w = 0")
        rows = db.command("sql", "UPDATE T SET w = 1 WHERE v <=> ?", *args).to_list()
    assert rows == [{"count": 1}]
    records = _records(db)
    assert records["null"]["w"] == 1 and records["set"]["w"] == 0


@pytest.mark.parametrize("args", LONE_NULL)
def test_query_binds_a_lone_null(db, args):
    with db.transaction():
        db.command("sql", "INSERT INTO T SET k = 'null', v = null")
    rows = db.query("sql", "SELECT k FROM T WHERE v <=> ?", *args).to_list()
    assert rows == [{"k": "null"}]


@pytest.mark.parametrize("method", ["query", "command"])
def test_null_then_a_value_binds_both(db, method):
    with db.transaction():
        db.command("sql", "INSERT INTO T SET k = 'pair', v = ?, w = ?", None, 1)
        rows = getattr(db, method)(
            "sql", "SELECT k FROM T WHERE v <=> ? AND w = ?", None, 1
        ).to_list()
    assert rows == [{"k": "pair"}]
    pair = _records(db)["pair"]
    assert "v" in pair and pair["v"] is None and pair["w"] == 1


@pytest.mark.parametrize("method", ["query", "command"])
def test_a_dict_binds_named_parameters(db, method):
    with db.transaction():
        db.command(
            "sql", "INSERT INTO T SET k = 'named', v = :v, w = :w", {"v": None, "w": 2}
        )
        rows = getattr(db, method)(
            "sql", "SELECT k FROM T WHERE v <=> :v AND w = :w", {"v": None, "w": 2}
        ).to_list()
    assert rows == [{"k": "named"}]
    named = _records(db)["named"]
    assert "v" in named and named["v"] is None and named["w"] == 2


@pytest.mark.parametrize("method", ["query", "command"])
def test_a_dict_alone_in_a_list_still_binds_named_parameters(db, method):
    """A one-dict list went to the Map overload before (JPype's choice), and openCypher
    refuses an Object[] that holds one map, so it stays the named map."""
    with db.transaction():
        rows = getattr(db, method)("opencypher", "RETURN $p AS p", [{"p": 7}])
        assert rows.to_list() == [{"p": 7}]
        rows = getattr(db, method)("sql", "SELECT :p AS p", [{"p": 8}])
        assert rows.to_list() == [{"p": 8}]


@pytest.mark.parametrize(
    "args, expected_w",
    [
        pytest.param([None], 0, id="[None]"),
        pytest.param((None,), 0, id="(None,)"),
        pytest.param([None, 1], 1, id="[None, 1]"),
    ],
)
def test_async_command_binds_null(db, args, expected_w):
    statement = (
        "INSERT INTO T SET k = 'async', v = ?, w = ?"
        if len(args) == 2
        else "INSERT INTO T SET k = 'async', v = ?, w = 0"
    )
    errors = []
    executor = db.async_executor()
    executor.command("sql", statement, args=args, error_callback=errors.append)
    executor.wait_completion()
    executor.close()
    assert errors == []
    record = _records(db)["async"]
    assert "v" in record and record["v"] is None and record["w"] == expected_w


def test_each_parameter_shape_reaches_one_java_overload(temp_db):
    """JPype gets one typed argument, so it never chooses: an ``Object[]`` reaches the
    ``Object...`` overload with one element per ``?``, and a dict reaches the ``Map``
    overload. A proxy of the ``Database`` interface, which declares every overload that
    ``LocalDatabase`` has, records what arrives."""
    seen = []

    class Recorder:
        def query(self, *args):
            seen.append(args[2:])

        def command(self, *args):
            seen.append(args[2:])

        def close(self):
            pass

    interface = jpype.JClass("com.arcadedb.database.Database")
    proxy = jpype.JObject(jpype.JProxy(interface, inst=Recorder()), interface)
    db = arcadedb.Database(proxy)
    java_map = jpype.JClass("java.util.Map")

    def as_python(value):
        if isinstance(value, java_map):
            return {str(key): item for key, item in value.items()}
        return value

    def received(method, *args):
        getattr(db, method)("sql", "SELECT 1", *args)
        (arg,) = seen.pop()
        if isinstance(arg, java_map):
            return ("Map", as_python(arg))
        assert arg.getClass().getName() == "[Ljava.lang.Object;"
        return ("Object[]", [as_python(item) for item in arg])

    for method in ("query", "command"):
        assert received(method, None) == ("Object[]", [None])
        assert received(method, [None]) == ("Object[]", [None])
        assert received(method, (None,)) == ("Object[]", [None])
        assert received(method, None, 1) == ("Object[]", [None, 1])
        assert received(method, []) == ("Object[]", [])
        assert received(method, {"v": None, "w": 2}) == ("Map", {"v": None, "w": 2})
        assert received(method, [{"p": 1}]) == ("Map", {"p": 1})
        assert received(method, {"p": 1}, 2) == ("Object[]", [{"p": 1}, 2])
    assert seen == []
    db.close()


def test_async_parameters_reach_one_java_overload(temp_db):
    """The async executor's ``args`` reach the engine as one ``Object[]`` too. Splatted, a
    lone ``None`` arrived as a null in place of the whole array, so nothing was bound.
    """
    from arcadedb_embedded.async_executor import AsyncExecutor

    seen = []

    class Recorder:
        def query(self, *args):
            seen.append(args[3:])

        def command(self, *args):
            seen.append(args[3:])

    interface = jpype.JClass("com.arcadedb.database.async.DatabaseAsyncExecutor")
    proxy = jpype.JObject(jpype.JProxy(interface, inst=Recorder()), interface)
    executor = AsyncExecutor(proxy)
    java_map = jpype.JClass("java.util.Map")

    def received(method, args=None, **params):
        if method == "query":
            executor.query("sql", "SELECT 1", lambda row: None, args=args, **params)
        else:
            executor.command("sql", "SELECT 1", args=args, **params)
        (arg,) = seen.pop()
        if isinstance(arg, java_map):
            return ("Map", {str(key): item for key, item in arg.items()})
        assert arg.getClass().getName() == "[Ljava.lang.Object;"
        return ("Object[]", list(arg))

    for method in ("query", "command"):
        assert received(method, [None]) == ("Object[]", [None])
        assert received(method, (None,)) == ("Object[]", [None])
        assert received(method, [None, 1]) == ("Object[]", [None, 1])
        assert received(method, [{"p": 1}]) == ("Map", {"p": 1})
        assert received(method, p=None) == ("Map", {"p": None})
    assert seen == []


class TestScalarParametersTakeTheShortPath:
    """Plain scalars cross as they are, without the per-value conversion walk.

    A dict of str keys with scalar values, or positional scalars, is bound with a HashMap or an
    Object[] built directly: 4 to 6 microseconds a call against 6.5 to 9 for the general path.
    Exact types only; anything else (numpy scalars, Decimal, date, list, a subclass of int) keeps
    the general path and its conversions.
    """

    @pytest.fixture
    def counted(self, monkeypatch):
        from arcadedb_embedded import core

        calls = []
        real = core.convert_python_to_java

        def spy(value):
            calls.append(type(value).__name__)
            return real(value)

        monkeypatch.setattr(core, "convert_python_to_java", spy)
        return calls

    def test_scalar_dict_is_not_converted(self, db, counted):
        rows = db.query(
            "sql", "SELECT k FROM T WHERE v = :v AND k = :k", {"v": 5, "k": "set"}
        ).to_list()
        assert rows == [{"k": "set"}]
        assert counted == []

    def test_positional_scalars_are_not_converted(self, db, counted):
        rows = db.query(
            "sql", "SELECT k FROM T WHERE v = ? AND k = ?", 5, "set"
        ).to_list()
        assert rows == [{"k": "set"}]
        assert counted == []
        rows = db.query("sql", "SELECT k FROM T WHERE v = ?", [5]).to_list()
        assert rows == [{"k": "set"}]
        assert counted == []

    def test_a_java_array_value_is_not_converted_and_binds_as_before(self, db, counted):
        # a vector already converted with to_java_float_array (#276): the general path returned it
        # unchanged after the conversion walk, so the short path passes it as it is
        vec = jpype.JArray(jpype.JFloat)([0.5, 1.5, 2.5])
        with db.transaction():
            db.command(
                "sql", "INSERT INTO T SET k = :k, emb = :e", {"k": "named", "e": vec}
            )
            db.command("sql", "INSERT INTO T SET k = ?, emb = ?", "positional", vec)
        assert counted == []
        rec = _records(db)
        assert [float(x) for x in rec["named"]["emb"]] == [0.5, 1.5, 2.5]
        assert [float(x) for x in rec["positional"]["emb"]] == [0.5, 1.5, 2.5]

    def test_every_scalar_type_binds_as_before(self, db):
        with db.transaction():
            db.command(
                "sql",
                "INSERT INTO T SET k = :k, i = :i, f = :f, b = :b, n = :n, big = :big",
                {"k": "all", "i": 7, "f": 2.5, "b": True, "n": None, "big": 2**40},
            )
            db.command(
                "sql",
                "INSERT INTO T SET k = ?, i = ?, f = ?, b = ?, n = ?, big = ?",
                "pos",
                7,
                2.5,
                False,
                None,
                2**40,
            )
        rec = _records(db)
        assert (
            rec["all"]["i"] == 7 and rec["all"]["f"] == 2.5 and rec["all"]["b"] is True
        )
        assert rec["all"]["n"] is None and rec["all"]["big"] == 2**40
        assert rec["pos"]["b"] is False and rec["pos"]["i"] == 7

    def test_the_overloads_still_see_the_same_kind_of_argument(self, db):
        # a dict goes to the Map overload and positional values to Object...: identical answers
        named = db.query("sql", "SELECT k FROM T WHERE v = :v", {"v": 5}).to_list()
        positional = db.query("sql", "SELECT k FROM T WHERE v = ?", 5).to_list()
        assert named == positional == [{"k": "set"}]

    def test_non_scalars_keep_the_general_path(self, db, counted):
        import decimal

        db.query(
            "sql", "SELECT k FROM T WHERE v = :v", {"v": decimal.Decimal("5")}
        ).to_list()
        assert counted  # converted
        del counted[:]
        db.query("sql", "SELECT k FROM T WHERE v IN :vs", {"vs": [5, 6]}).to_list()
        assert counted

    def test_a_subclass_of_a_scalar_is_not_assumed_to_be_one(self, db, counted):
        class MyInt(int):
            pass

        rows = db.query(
            "sql", "SELECT k FROM T WHERE v = :v", {"v": MyInt(5)}
        ).to_list()
        assert rows == [{"k": "set"}]
        assert counted  # took the general path

    def test_numpy_scalars_keep_the_general_path(self, db, counted):
        np = pytest.importorskip("numpy")
        rows = db.query("sql", "SELECT k FROM T WHERE v = ?", np.int64(5)).to_list()
        assert rows == [{"k": "set"}]
        rows = db.query(
            "sql", "SELECT k FROM T WHERE v = :v", {"v": np.float64(5.0)}
        ).to_list()
        assert rows == [{"k": "set"}]
