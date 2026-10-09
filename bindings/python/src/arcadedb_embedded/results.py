"""
ArcadeDB Python Bindings - Result Set Wrappers

ResultSet and Result classes for wrapping query results.
"""

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

import jpype as _jpype
from jpype import JException

from ._logging import get_logger
from .exceptions import ArcadeDBError
from .graph import Document, Edge, Vertex
from .type_conversion import convert_java_to_python

_logger = get_logger(__name__)
_BRIDGE_CLASSES: dict = {}


# Rows per crossing for ResultSet.to_list(): large enough that the per-batch
# cost (one crossing, one decoder) disappears, small enough that the JSON text
# of a wide batch stays a few megabytes.
_TYPED_BATCH_ROWS = 1000

_TYPED_TAG = "\u0001"
_TYPED_SIMPLE = {
    "n": Decimal,
    "i": int,
    "D": date.fromisoformat,
    "T": datetime.fromisoformat,
    "Z": datetime.fromisoformat,
    "s": set,
}


def _typed_hook(side):
    """The json object_hook that restores the values TypedRows tagged."""

    def hook(obj):
        tagged = obj.get(_TYPED_TAG)
        if tagged is None:
            return obj
        kind, payload = tagged
        restore = _TYPED_SIMPLE.get(kind)
        if restore is not None:
            return restore(payload)
        if kind == "x":
            return convert_java_to_python(side[payload])
        # "r": a row whose property name is the tag key, sent as names and values
        pair = side[payload]
        return {str(n): convert_java_to_python(v) for n, v in zip(pair[0], pair[1])}

    return hook


def _bridge_class(name):
    """Cached bridge-class lookup; None (with one warning) if unavailable.

    In an installed wheel the bridge jar is always on the classpath, so the
    None path only occurs in broken source checkouts."""
    if name in _BRIDGE_CLASSES:
        return _BRIDGE_CLASSES[name]
    import jpype

    if not jpype.isJVMStarted():
        # Asked too early: say so, but do not cache it, or every later call
        # would take the slow path for the life of the process.
        return None
    try:
        cls = jpype.JClass(f"com.arcadedb.python.{name}")
    except Exception:
        cls = None
        _logger.warning(
            "bridge class %s unavailable; falling back to the slow per-row "
            "path (is the bridge jar missing from this build?)",
            name,
        )
    _BRIDGE_CLASSES[name] = cls
    return cls


def _read_error(exc: BaseException) -> ArcadeDBError:
    """The ArcadeDBError for a Java exception raised while rows were read.

    The engine plans lazily, so a statement's error can surface on the first
    or any later row, after ``query()`` or ``command()`` returned (#173). Raise
    it ``from`` the Java exception, which ``str()`` then names as the cause.
    """
    return ArcadeDBError(f"Reading the result set failed: {exc}")


def _java_class_name(value: Any) -> str:
    return str(value.getClass().getName())


def _cast_all_to_string(arrs: list, pa) -> list:
    """
    Cast every array to a string type.

    Tolerates a type pyarrow cannot cast directly (e.g. a vector/list column)
    by going through Python objects instead.
    """
    unified = []
    for a in arrs:
        try:
            unified.append(a.cast(pa.string()))
        except pa.ArrowException:
            unified.append(
                pa.array(
                    [None if v is None else str(v) for v in a.to_pylist()],
                    type=pa.string(),
                )
            )
    return unified


def _decode_strings(data, count: int, mask) -> list:
    """The strings of one column buffer: int32 offsets, then the UTF-8 blob.

    ``mask`` is the null mask (True = null) or None when the column has no nulls.
    """
    import numpy as np

    offs = np.frombuffer(data[: (count + 1) * 4], dtype="<i4")
    chars = bytes(data[(count + 1) * 4 :])
    if mask is None:
        return [chars[offs[i] : offs[i + 1]].decode("utf-8") for i in range(count)]
    return [
        None if mask[i] else chars[offs[i] : offs[i + 1]].decode("utf-8")
        for i in range(count)
    ]


def _decode_decimals(data, count: int, mask) -> list:
    """The DECIMAL values of one column buffer, as ``Decimal`` objects (None for
    null). The bridge writes each ``BigDecimal`` in full as text, so no digit is
    lost to a double."""
    from decimal import Decimal

    return [
        None if s is None else Decimal(s) for s in _decode_strings(data, count, mask)
    ]


def _missing_column_part(count: int, template):
    """A batch of ``count`` rows that lack a column some other batch has: nulls in
    the shape ``to_columns`` uses for that column (NaN for numbers, NaT for
    datetimes, None for everything else)."""
    import numpy as np

    if isinstance(template, np.ndarray):
        if template.ndim == 2:
            return np.full((count, template.shape[1]), np.nan, dtype=template.dtype)
        if template.dtype.kind in "iuf":
            return np.full(count, np.nan)
        if template.dtype.kind == "M":
            return np.full(count, np.datetime64("NaT", "ms"))
        if template.dtype.kind == "O":
            return np.full(count, None, dtype=object)
    return [None] * count


def _is_untyped_arrow_chunk(a, pa) -> bool:
    """True for a chunk that says nothing about its column's type: all values
    null (a batch whose rows lack the property, or carry it as null), or a list
    column holding only empty lists, which Arrow types ``list<null>``."""
    if len(a) == 0 or a.null_count == len(a) or pa.types.is_null(a.type):
        return True
    return pa.types.is_list(a.type) and pa.types.is_null(a.type.value_type)


def _common_decimal_type(types: list, pa):
    """One decimal type every given decimal type fits in (decimal128 up to 38
    digits, decimal256 up to 76), or None when it would need more."""
    scale = max(t.scale for t in types)
    precision = max(t.precision - t.scale for t in types) + scale
    if precision <= 38:
        return pa.decimal128(precision, scale)
    if precision <= 76:
        return pa.decimal256(precision, scale)
    return None


def _unify_arrow_chunk_types(arrs: list, pa) -> list:
    """
    Unify a column's per-batch Arrow arrays onto one Arrow type.

    ArcadeDB is schemaless per document, so a property's Java type can
    legally vary row to row; ColumnBatcher infers each batch's column type
    independently, so two batches of the same result set can produce
    incompatible Arrow types for the same column (#7108) - e.g. int64 in one
    batch, float64 in the next. ``pa.chunked_array`` requires every chunk to
    share one type, so leaving them as-is raises at concatenation time on a
    result set that is otherwise entirely legal.

    Numeric types widen (int64 -> float64, matching to_columns' pandas-style
    promotion) when every value survives the cast exactly; pyarrow's cast is
    safe by default and raises rather than silently lose precision, which an
    int64 outside float64's +-2**53 exact range would (code review on #7108),
    so that case - and anything else, including a json or vector column that
    varied - degrades to string instead, so the column always ends up with
    exactly one type rather than an exception.

    int64/float64 is the only numeric pair worth special-casing: ColumnBatcher
    (bindings/python/src/java/.../ColumnBatcher.java) already widens every
    Java integer type (Long/Integer/Short/Byte) to the same "i8" wire encoding
    and every Java floating type (Double/Float) to "f8" *within* one batch, so
    across batches the only numeric type mismatch this ever has to unify is
    exactly this pair - there is no int32/int64 or bool/int64 case to widen.
    """
    types = {a.type for a in arrs}
    if len(types) <= 1:
        return arrs

    # A chunk that carries no type information (every value null, or only empty
    # lists) takes the type the other chunks agree on, so the column's type does
    # not depend on where the batch boundaries fall (#114).
    untyped = [_is_untyped_arrow_chunk(a, pa) for a in arrs]
    if any(untyped) and not all(untyped):
        typed = _unify_arrow_chunk_types(
            [a for a, u in zip(arrs, untyped) if not u], pa
        )
        target = typed[0].type
        filled = iter(typed)
        out = []
        for a, u in zip(arrs, untyped):
            if not u:
                out.append(next(filled))
            elif a.null_count == len(a):
                out.append(pa.nulls(len(a), type=target))
            else:
                try:
                    out.append(a.cast(target))
                except pa.ArrowException:
                    return _cast_all_to_string(arrs, pa)
        return out

    if types <= {pa.int64(), pa.float64()}:
        try:
            return [a.cast(pa.float64()) for a in arrs]
        except pa.ArrowException:
            pass

    if all(pa.types.is_decimal(t) for t in types):
        common = _common_decimal_type(list(types), pa)
        if common is not None:
            return [a.cast(common) for a in arrs]

    return _cast_all_to_string(arrs, pa)


class ResultSet:
    """Iterator wrapper for ArcadeDB query results.

    The Java result set is closed as soon as it is exhausted (by iteration or
    any of the ``to_*`` methods), by ``first()``/``one()`` once they have their
    row, and when this object is freed. Since the engine's parallel scan
    (ArcadeData/arcadedb#8524, 26.10.1) an unclosed result set keeps its scan's
    producer threads parked for up to ``arcadedb.parallelScanAbandonedTimeout``
    (10 minutes), and a few of them stall the next query that needs the pool
    (ArcadeData/arcadedb#8594).

    A result set read to its end reads as empty afterwards. One closed before
    its end (by ``first()``, ``one()``, ``close()``, or leaving its ``with``
    block) raises ArcadeDBError when read again: the rows it had not returned
    are gone, and returning nothing would hide that. So does one whose
    ``Database`` was closed before it was read to its end: a record row is
    loaded lazily from the open database, and an empty row would hide that.

    A result set keeps its ``Database`` alive, so a function may open a
    database, query it, and return the result without closing anything.

    The engine plans and computes rows lazily, so an error in the statement
    can surface while its rows are read rather than in ``query()``. Every way
    of reading them (iteration, ``to_list()``, ``to_json_list()``, the
    columnar readers, ``first()``, ``one()``, ``count()``) raises it as
    ArcadeDBError, with the Java exception as its cause.
    """

    def __init__(self, java_result_set, database=None):
        self._java_result_set = java_result_set
        self._database = database  # strong reference, see the class docstring
        self._closed = False
        self._exhausted = False

    def __iter__(self) -> Iterator["Result"]:
        return self

    def _readable(self) -> bool:
        """True while rows can still come; False once read to the end.

        Raises ArcadeDBError for a result set closed before its end, or whose
        database was closed before its end. What a closed Java result set
        returns is the engine's business and has changed between builds, so it
        is never asked.
        """
        database = self._database
        if database is not None and database._closed and not self._exhausted:
            raise ArcadeDBError(
                "Database is closed: the rows of this result set cannot be "
                "read after their database was closed. Read them before "
                "closing the database."
            )
        if not self._closed:
            return True
        if self._exhausted:
            return False
        raise ArcadeDBError(
            "This result set was closed before all its rows were read (by "
            "first(), one(), close(), or leaving its with block), so the rows "
            "it had not returned are gone. Run the query again to read them."
        )

    def _finish(self) -> None:
        self._exhausted = True
        self.close()

    def _drained(self) -> None:
        """The bridge returned its last batch and closed the Java result set itself.

        ``RowBatcher.nextJsonBatch`` and ``RowAccess.nextRows`` return fewer rows
        than asked for only when the result set is drained, and close it before
        returning, so Python marks the set finished without another JVM
        crossing (a JPype call is 3 to 4 microseconds, a fifth of a one-row
        read through ``to_json_list()``).
        """
        self._exhausted = True
        self._closed = True

    def __next__(self) -> "Result":
        if self._readable():
            try:
                if self._java_result_set.hasNext():
                    return Result(self._java_result_set.next(), self._database)
            except JException as exc:
                raise _read_error(exc) from exc
        if not self._closed:
            self._finish()
        raise StopIteration

    def to_list(self, convert_types: bool = True) -> List[Dict[str, Any]]:
        """
        Convert all results to list of dictionaries.

        Rows come over from the JVM in batches (the bridge's ``TypedRows``):
        Java writes each batch as JSON, tags the values JSON cannot carry
        exactly (DECIMAL, DATE, DATETIME, sets) and hands everything else
        (RIDs, embedded documents, ``float[]``, ...) over as the engine's own
        object, so each value has the Python type it always had and no value
        crosses the JVM boundary on its own. That is within a few percent of
        ``to_json_list()`` on a wide scan (see the performance guide).

        For large results, ``to_columns()``, ``to_dataframe()`` or
        ``to_arrow()`` move the data as columns and are faster still.
        ``to_json_list()`` returns the same shape with JSON-native values
        (DATE and DATETIME as epoch-millisecond integers, DECIMAL as float),
        which only matters when you want that form.

        Args:
            convert_types: Convert Java types to Python (default: True)

        Returns:
            List of dictionaries with result data

        Example:
            >>> results = db.query("sql", "SELECT FROM User LIMIT 10")
            >>> users = results.to_list()
            >>> print(users[0])
            {'name': 'Alice', 'age': 30, 'email': 'alice@example.com'}
        """
        if convert_types:
            # The whole result is consumed, so rows can come over in batches:
            # one crossing per batch instead of hasNext/next and a Result
            # wrapper per row. iter_dicts() reads one row per crossing instead,
            # because a batch taken ahead would consume rows a lazy caller may
            # still want.
            typed_rows = _bridge_class("TypedRows")
            if typed_rows is not None:
                return self._to_list_typed(typed_rows)
            row_access = _bridge_class("RowAccess")
            if row_access is not None:
                out: List[Dict[str, Any]] = []
                if not self._readable():
                    return out
                while True:
                    try:
                        batch = row_access.nextRows(self._java_result_set, 512)
                    except JException as exc:
                        raise _read_error(exc) from exc
                    count = len(batch)
                    for pair in batch:
                        out.append(
                            {
                                str(name): convert_java_to_python(value)
                                for name, value in zip(pair[0], pair[1])
                            }
                        )
                    if count < 512:
                        # a short batch is the last one, and the bridge closed
                        # the result set: no second call just to see an empty batch
                        self._drained()
                        return out
        return list(self.iter_dicts(convert_types=convert_types))

    def _to_list_typed(self, typed_rows) -> List[Dict[str, Any]]:
        """``to_list()`` through the bridge's ``TypedRows``.

        Java writes each batch as JSON and tags the values JSON cannot carry
        exactly (DECIMAL, dates and datetimes, sets) and refers to everything
        else (RIDs, embedded documents, non-finite floats, ...) by index into
        a side array of the engine's own objects, so every value comes back
        as the type ``convert_java_to_python`` gives it. The C ``json`` module
        parses the text and one hook call per tagged value restores it.
        """
        out: List[Dict[str, Any]] = []
        if not self._readable():
            return out
        while True:
            try:
                java_batch = typed_rows.nextRows(
                    self._java_result_set, _TYPED_BATCH_ROWS
                )
            except JException as exc:
                raise _read_error(exc) from exc
            side = java_batch[1]
            rows = json.loads(str(java_batch[0]), object_hook=_typed_hook(side))
            out.extend(rows)
            if len(rows) < _TYPED_BATCH_ROWS:
                # a short batch is the last one, and the bridge closed the
                # result set: no second call just to see an empty batch
                self._drained()
                return out

    def iter_dicts(self, convert_types: bool = True) -> Iterator[Dict[str, Any]]:
        """
        Iterate results as dictionaries.

        Each row is read when you ask for it, in one crossing into the JVM
        (``TypedRows``, as for ``to_list()``), so stopping early or changing
        records inside the loop behaves as it always did.

        Args:
            convert_types: Convert Java types to Python (default: True)

        Yields:
            Result rows as dictionaries
        """
        typed_rows = _bridge_class("TypedRows") if convert_types else None
        if typed_rows is None:
            for result in self:
                yield result.to_dict(convert_types=convert_types)
            return
        # One row per crossing, read when the caller asks for it, exactly as
        # before (a batch taken ahead would read rows a lazy caller may not
        # want, or that its own loop body is about to change). It replaces the
        # hasNext, next, wrapper, and per-property reads of the row-by-row
        # path with one crossing and one JSON parse.
        while self._readable():
            try:
                java_batch = typed_rows.nextRows(self._java_result_set, 1)
            except JException as exc:
                raise _read_error(exc) from exc
            rows = json.loads(
                str(java_batch[0]), object_hook=_typed_hook(java_batch[1])
            )
            if not rows:
                # nextRows closed the drained result set itself
                self._drained()
                return
            yield rows[0]

    def close(self) -> None:
        """
        Close the underlying Java result set. Idempotent.

        Exhausting the result set closes it already; call this (or use the
        result set as a context manager) when you stop reading early. An
        unclosed result set is not only held memory: since 26.10.1's parallel
        scan it can hold engine threads that other queries need
        (ArcadeData/arcadedb#8594).
        """
        if self._closed:
            return
        self._closed = True
        try:
            self._java_result_set.close()
        except Exception:  # nosec B110 - close() is best-effort hygiene
            pass

    def __del__(self):
        # A result set abandoned mid-iteration (a `break`) is freed right away
        # under CPython's reference counting, so its engine-side cursor is
        # released then rather than after the engine's abandonment timeout.
        # Best effort: the JVM may already be gone at interpreter exit.
        if not getattr(self, "_closed", True):
            try:
                if _jpype.isJVMStarted():
                    self.close()
            except Exception:  # nosec B110 - finalizer must never raise
                pass

    def __enter__(self) -> "ResultSet":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def to_json_list(self, batch_size: int = 10_000) -> List[Dict[str, Any]]:
        """
        Bulk-materialize all rows via batched Java-side JSON serialization.

        The fast path for large result sets: rows are serialized to JSON in
        batches on the Java side (one JPype crossing per batch instead of
        several per row) and parsed with the C json module. Measured ~5.5x
        faster than ``to_list()`` used to be on a 10,000-row, nine-property
        scan (578 ms against 103 ms, laptop, 2026-09-27); ``to_list()`` now
        reads rows the same way and keeps the Python types.

        Values carry JSON-native types. Numbers, strings, booleans, lists and
        nested maps convert as expected, but DATE and DATETIME values arrive
        as epoch-millisecond integers (not ``datetime``) and DECIMALs as
        floats. Use ``to_list()`` when you want ``date``, ``datetime`` and
        ``Decimal`` values: it costs about the same.

        Args:
            batch_size: Rows serialized per Java crossing (default 10000)

        Returns:
            List of dictionaries with JSON-native values

        Example:
            >>> rows = db.query("sql", "SELECT FROM Doc").to_json_list()
        """
        rows: List[Dict[str, Any]] = []
        for batch in self.iter_json_batches(batch_size=batch_size):
            rows.extend(batch)
        return rows

    def iter_json_batches(
        self, batch_size: int = 10_000
    ) -> Iterator[List[Dict[str, Any]]]:
        """
        Yield rows as lists of dicts, one Java-serialized batch at a time.

        Streaming counterpart of :meth:`to_json_list` with the same JSON-native
        type semantics; bounds memory to one batch. Falls back to chunked
        per-row conversion when the bridge jar is unavailable.
        """
        import json

        if int(batch_size) < 1:
            raise ValueError(f"batch_size must be at least 1, got {batch_size}")
        row_batcher = _bridge_class("RowBatcher")
        if row_batcher is None:
            chunk: List[Dict[str, Any]] = []
            for row in self.iter_dicts():
                chunk.append(row)
                if len(chunk) >= batch_size:
                    yield chunk
                    chunk = []
            if chunk:
                yield chunk
            return

        if not self._readable():
            return
        size = int(batch_size)
        while True:
            try:
                java_batch = row_batcher.nextJsonBatch(self._java_result_set, size)
            except JException as exc:
                raise _read_error(exc) from exc
            batch = json.loads(str(java_batch))
            if len(batch) < size:
                # nextJsonBatch stops short only when the result set is drained, so a
                # short batch is the last one: no second call (a JVM crossing, a
                # str(), and a json.loads) just to see "[]". It closed the result
                # set itself, so there is no close() crossing either.
                self._drained()
                if batch:
                    yield batch
                return
            yield batch

    def to_dataframe(self, convert_types: bool = True):
        """
        Convert results to pandas DataFrame.

        Requires pandas to be installed.

        Args:
            convert_types: Convert Java types to Python (default: True)

        Returns:
            pandas DataFrame

        Raises:
            ImportError: If pandas is not installed

        Example:
            >>> results = db.query("sql", "SELECT FROM User")
            >>> df = results.to_dataframe()
            >>> print(df.describe())
        """
        try:
            import pandas as pd
        except ImportError as exc:
            raise ImportError(
                "pandas is required for to_dataframe(). "
                "Install with: pip install pandas"
            ) from exc

        if convert_types:
            # fast path: columnar binary transport straight into typed
            # columns (numpy dtypes, real datetime64) — measured ~2x the JSON
            # row path and ~6x the per-row path on 100k-row scans
            columns = self.to_columns()
            if columns is not None:
                # pandas rejects 2-D ndarrays as column values; vector
                # columns (f4v/f8v) become object columns of row slices
                columns = {
                    k: (list(v) if getattr(v, "ndim", 1) > 1 else v)
                    for k, v in columns.items()
                }
                return pd.DataFrame(columns)

        return pd.DataFrame(self.to_list(convert_types=convert_types))

    def to_columns(
        self, batch_size: int = 25_000, columns: Optional[Sequence[str]] = None
    ):
        """
        Bulk-materialize all rows as columns: dict of column name -> numpy
        array (int64/float64/bool/datetime64[ms]) or Python list (strings and
        JSON-typed values). The fastest bulk path (~1.2x Java-native scans,
        measured), ideal for feeding pandas/numpy.

        Null handling follows pandas conventions: int/datetime columns with
        nulls are promoted to float64 with NaN / datetime64 NaT; string and
        JSON columns use None. A DECIMAL column is an object array of exact
        ``Decimal`` values (#115).

        The columns are the union of every row's property names, in order of
        first appearance, because a document is schemaless: a property the
        first row lacks is still a column (#113). Finding them costs one pass
        over each row's names; pass ``columns`` (a list of names) to read exactly
        those, as a projection would, and skip it. A row lacking one of them
        reads null, and properties not listed are left out.

        Returns None when numpy or the bridge jar is unavailable (callers
        fall back to row-based paths).
        """
        try:
            import numpy as np
        except ImportError:
            return None

        import json

        column_batcher = _bridge_class("ColumnBatcher")
        if column_batcher is None:
            return None

        # Every batch reports the union of its own rows' property names. The
        # result's columns are the union over the batches, in order of first
        # appearance (#113); a batch lacking one gets nulls for it when merged.
        names: List[str] = []
        batches: List[Tuple[int, Dict[str, Any]]] = []

        def decode_batch(buf):
            hlen = int.from_bytes(buf[:4], "little")
            header = json.loads(bytes(buf[4 : 4 + hlen]))
            count = header["count"]
            if count == 0:
                return 0
            batch_parts: Dict[str, Any] = {}
            pos = 4 + hlen
            for col in header["cols"]:
                name, ctype = col["name"], col["type"]
                nulls_len = col["nulls"]
                null_bits = np.frombuffer(buf[pos : pos + nulls_len], dtype=np.uint8)
                has_nulls = bool(null_bits.any())
                if has_nulls:
                    mask = np.unpackbits(null_bits, bitorder="little")[:count].astype(
                        bool
                    )
                pos += nulls_len
                data = buf[pos : pos + col["bytes"]]
                pos += col["bytes"]

                if ctype == "i8":
                    arr = np.frombuffer(data, dtype="<i8")
                    if has_nulls:
                        arr = arr.astype(np.float64)
                        arr[mask] = np.nan
                    values = arr
                elif ctype == "f8":
                    # Java already writes NaN into null slots
                    values = np.frombuffer(data, dtype="<f8")
                elif ctype == "dt":
                    arr = np.frombuffer(data, dtype="<i8").astype("datetime64[ms]")
                    if has_nulls:
                        arr[mask] = np.datetime64("NaT", "ms")
                    values = arr
                elif ctype == "b1":
                    arr = np.frombuffer(data, dtype=np.uint8).astype(bool)
                    if has_nulls:
                        values = [
                            None if mask[i] else bool(arr[i]) for i in range(count)
                        ]
                    else:
                        values = arr
                elif ctype in ("f4v", "f8v"):
                    # fixed-dimension vector column -> 2-D array (count, dim);
                    # Java already writes NaN rows into null slots
                    dim = col["dim"]
                    dt = "<f4" if ctype == "f4v" else "<f8"
                    values = np.frombuffer(data, dtype=dt).reshape(count, dim)
                elif ctype == "json":
                    values = json.loads(bytes(data))
                elif ctype == "dec":
                    # an object array of Decimal (None for null): a double
                    # would lose digits, and the dtype would follow the data
                    values = np.empty(count, dtype=object)
                    values[:] = _decode_decimals(
                        data, count, mask if has_nulls else None
                    )
                else:  # str
                    values = _decode_strings(data, count, mask if has_nulls else None)
                batch_parts[name] = values
                if name not in names:
                    names.append(name)
            batches.append((count, batch_parts))
            return count

        # An empty column spec makes Java derive the column set from the
        # rows of each batch; a pinned one is read as given. JSON, not
        # ";".join: a name may legally contain a semicolon, a quote or a
        # backslash.
        spec = json.dumps(list(columns)) if columns is not None else ""
        if not self._readable():
            return {}
        total = 0
        while True:
            try:
                java_buf = column_batcher.nextColumnBatch(
                    self._java_result_set, int(batch_size), spec
                )
            except JException as exc:
                raise _read_error(exc) from exc
            buf = memoryview(bytes(java_buf))
            count = decode_batch(buf)
            if count == 0:
                self._finish()
                break
            total += count

        if total == 0:
            return {}

        out = {}
        for name in names:
            template = next(p[name] for _, p in batches if name in p)
            parts = [
                p[name] if name in p else _missing_column_part(c, template)
                for c, p in batches
            ]
            np_parts = [p for p in parts if not isinstance(p, list)]
            if parts and len(np_parts) == len(parts):
                try:
                    out[name] = np.concatenate(parts) if len(parts) > 1 else parts[0]
                    continue
                except Exception:  # nosec B110 - fall through to generic merge
                    pass
            column = []
            for p in parts:
                column.extend(p if isinstance(p, list) else p.tolist())
            out[name] = column
        return out

    def to_arrow(
        self, batch_size: int = 25_000, columns: Optional[Sequence[str]] = None
    ):
        """
        Bulk-materialize all rows as a ``pyarrow.Table``.

        Reads the same columnar buffer as :meth:`to_columns` (one JSON header
        plus packed little-endian columns and a null bitmap each), so there is
        no extra work on the Java side. What differs is what happens to nulls
        and to strings.

        ``to_columns`` follows pandas conventions, which are lossy in two
        places: a nullable int64 column is promoted to float64 with NaN, which
        silently loses precision above 2**53 and loses the type; and a nullable
        boolean column falls back to a Python list. Arrow carries a validity
        bitmap alongside the values, so both keep their type here.

        Strings are also cheaper: the buffer already holds int32 offsets
        followed by a UTF-8 blob, which is exactly Arrow's string layout, so
        the column is wrapped rather than decoded one Python str at a time.

        The columns and the optional ``columns`` argument are as in
        :meth:`to_columns`. A column's type does not depend on ``batch_size``
        (#114), and a DECIMAL column is a decimal Arrow column (#115).

        Returns None when pyarrow, numpy, or the bridge jar is unavailable, so
        callers can fall back to :meth:`to_columns` the same way that method
        falls back to the row-based paths.
        """
        try:
            import numpy as np
            import pyarrow as pa
        except ImportError:
            return None

        import json

        column_batcher = _bridge_class("ColumnBatcher")
        if column_batcher is None:
            return None

        # As in to_columns: every batch reports its own rows' union of property
        # names, the table's columns are the union over the batches in order of
        # first appearance (#113), and a batch lacking a column adds nulls.
        names: List[str] = []
        batches: List[Tuple[int, Dict[str, Any]]] = []

        def validity(null_bits, count, has_nulls):
            """Arrow validity is 1=valid; the bridge writes 1=null, so invert.
            Bits past `count` are padding and Arrow ignores them."""
            if not has_nulls:
                return None
            return pa.py_buffer(bytes(np.invert(null_bits).tobytes()))

        def decode_batch(buf):
            hlen = int.from_bytes(buf[:4], "little")
            header = json.loads(bytes(buf[4 : 4 + hlen]))
            count = header["count"]
            if count == 0:
                return 0
            batch_chunks: Dict[str, Any] = {}
            pos = 4 + hlen
            for col in header["cols"]:
                name, ctype = col["name"], col["type"]
                nulls_len = col["nulls"]
                null_bits = np.frombuffer(buf[pos : pos + nulls_len], dtype=np.uint8)
                has_nulls = bool(null_bits.any())
                mask = None
                if has_nulls:
                    mask = np.unpackbits(null_bits, bitorder="little")[:count].astype(
                        bool
                    )
                pos += nulls_len
                data = buf[pos : pos + col["bytes"]]
                pos += col["bytes"]

                if ctype == "i8":
                    # kept as int64 WITH validity, where to_columns would have
                    # promoted the whole column to float64/NaN
                    arr = pa.array(np.frombuffer(data, dtype="<i8"), mask=mask)
                elif ctype == "f8":
                    arr = pa.array(np.frombuffer(data, dtype="<f8"), mask=mask)
                elif ctype == "dt":
                    ts = np.frombuffer(data, dtype="<i8").astype("datetime64[ms]")
                    arr = pa.array(ts, type=pa.timestamp("ms"), mask=mask)
                elif ctype == "b1":
                    # stays a bool column; to_columns degrades this to a list
                    vals = np.frombuffer(data, dtype=np.uint8).astype(bool)
                    arr = pa.array(vals, mask=mask)
                elif ctype in ("f4v", "f8v"):
                    dim = col["dim"]
                    dt = "<f4" if ctype == "f4v" else "<f8"
                    flat = np.frombuffer(data, dtype=dt)
                    child = pa.array(flat)
                    arr = pa.FixedSizeListArray.from_arrays(child, dim)
                    if mask is not None:
                        arr = pa.array(arr.to_pylist(), type=arr.type, mask=mask)
                elif ctype == "json":
                    values = json.loads(bytes(data))
                    try:
                        arr = pa.array(values)
                    except pa.ArrowException:
                        # mixed types in one column (an int in one row, a string
                        # in the next): the same string column a mixed column
                        # across batches ends up as, not a raw ArrowInvalid
                        arr = pa.array(
                            [None if v is None else str(v) for v in values],
                            type=pa.string(),
                        )
                elif ctype == "dec":
                    decimals = _decode_decimals(data, count, mask)
                    try:
                        arr = pa.array(decimals)
                    except pa.ArrowException:
                        # more than the 76 digits decimal256 holds
                        arr = pa.array(
                            [None if d is None else str(d) for d in decimals],
                            type=pa.string(),
                        )
                else:  # str: int32 offsets + utf8 blob IS Arrow's layout
                    off_bytes = bytes(data[: (count + 1) * 4])
                    chars = bytes(data[(count + 1) * 4 :])
                    arr = pa.StringArray.from_buffers(
                        count,
                        pa.py_buffer(off_bytes),
                        pa.py_buffer(chars),
                        validity(null_bits, count, has_nulls),
                    )
                batch_chunks[name] = arr
                if name not in names:
                    names.append(name)
            batches.append((count, batch_chunks))
            return count

        spec = json.dumps(list(columns)) if columns is not None else ""
        if not self._readable():
            return pa.table({})
        total = 0
        while True:
            try:
                java_buf = column_batcher.nextColumnBatch(
                    self._java_result_set, int(batch_size), spec
                )
            except JException as exc:
                raise _read_error(exc) from exc
            buf = memoryview(bytes(java_buf))
            count = decode_batch(buf)
            if count == 0:
                self._finish()
                break
            total += count

        if total == 0 or not names:
            return pa.table({})
        return pa.table(
            {
                n: pa.chunked_array(
                    _unify_arrow_chunk_types(
                        [
                            chunks[n] if n in chunks else pa.nulls(c)
                            for c, chunks in batches
                        ],
                        pa,
                    )
                )
                for n in names
            }
        )

    def iter_chunks(
        self, size: int = 1000, convert_types: bool = True
    ) -> Iterator[List[Dict[str, Any]]]:
        """
        Iterate results in chunks for memory-efficient processing.

        Useful for processing large result sets without loading
        everything into memory at once.

        Args:
            size: Chunk size (default: 1000)
            convert_types: Convert Java types to Python (default: True)

        Yields:
            List of dictionaries (up to size elements)

        Example:
            >>> results = db.query("sql", "SELECT FROM User")
            >>> for chunk in results.iter_chunks(size=1000):
            ...     process_batch(chunk)  # chunk is list of dicts
        """
        chunk = []
        for row in self.iter_dicts(convert_types=convert_types):
            chunk.append(row)
            if len(chunk) >= size:
                yield chunk
                chunk = []

        if chunk:  # Yield remaining items
            yield chunk

    def count(self) -> int:
        """
        Count the remaining results without building a list.

        Note:
            This consumes the remaining rows from the current result set.
            After calling `count()`, the iterator is exhausted.

        Returns:
            Number of remaining results

        Example:
            >>> count = db.query("sql", "SELECT FROM User").count()
            >>> print(f"Found {count} users")
        """
        count = 0
        for _ in self:
            count += 1
        return count

    def first(self) -> Optional["Result"]:
        """
        Get first result or None if no results.

        Returns:
            First Result or None

        Example:
            >>> user = db.query("sql", "SELECT FROM User WHERE id = 1").first()
            >>> if user:
            ...     print(user.get("name"))
        """
        row_access = _bridge_class("RowAccess")
        if row_access is not None and self._readable() and not self._closed:
            # hasNext(), next(), and close() in ONE crossing instead of three
            try:
                java_row = row_access.firstAndClose(self._java_result_set)
            except JException as exc:
                self._closed = True  # firstAndClose closes on every path
                raise _read_error(exc) from exc
            self._closed = True
            if java_row is None:
                self._exhausted = True
                return None
            return Result(java_row, self._database)
        try:
            return next(iter(self))
        except StopIteration:
            return None
        finally:
            # the rest is never read: release the cursor now
            self.close()

    def one(self) -> "Result":
        """
        Get single result, raise error if not exactly one.

        Returns:
            The single Result

        Raises:
            ValueError: If zero or multiple results

        Example:
            >>> user = db.query("sql", "SELECT FROM User WHERE email = ?",
            ...                 "alice@example.com").one()
            >>> print(user.get("name"))
        """
        iterator = iter(self)
        try:
            try:
                result = next(iterator)
            except StopIteration as exc:
                raise ValueError("Query returned no results") from exc

            try:
                next(iterator)
                raise ValueError("Query returned multiple results")
            except StopIteration:
                return result
        finally:
            self.close()


class Result:
    """Wrapper for a single result from a query.

    A result keeps its ``Database`` alive. A row that is a record
    (``SELECT FROM T``) raises ArcadeDBError when read after that database was
    closed, like a record; a projection or a command result holds its own values
    and stays readable.
    """

    def __init__(self, java_result, database=None):
        self._java_result = java_result
        self._database = database  # strong reference, see the class docstring
        self._property_names_cache: Optional[Tuple[str, ...]] = None

    def _check_open(self) -> None:
        database = self._database
        if database is not None and database._closed and self._needs_database():
            raise ArcadeDBError(
                "Database is closed: a record row cannot be read after its "
                "database was closed. Read what you need before closing it."
            )

    def _needs_database(self) -> bool:
        """True for a row that is a record (``SELECT FROM T``): the engine
        loads its properties lazily from the open database. A projection or a
        command result holds its own values and stays readable, as it did
        before results held their database (an IMPORT DATABASE result is read
        after the import's database is closed, in example 16)."""
        try:
            return bool(self._java_result.isElement())
        except Exception:  # nosec B110 - an unreadable row is treated as lazy
            return True

    def _property_names_tuple(self) -> Tuple[str, ...]:
        self._check_open()
        if self._property_names_cache is None:
            self._property_names_cache = tuple(
                str(name) for name in self._java_result.getPropertyNames()
            )
        return self._property_names_cache

    def has_property(self, name: str) -> bool:
        """
        Check if a property exists in the result.

        Args:
            name: Property name

        Returns:
            True if property exists, False otherwise

        Example:
            >>> result = db.query("sql", "SELECT FROM User LIMIT 1").first()
            >>> if result.has_property("email"):
            ...     print(result.get("email"))
        """
        self._check_open()
        return self._java_result.hasProperty(name)

    def get(self, name: str, convert_types: bool = True) -> Any:
        """
        Get a property value from the result with automatic type conversion.

        Args:
            name: Property name

        Returns:
            Property value as a Python type, or None if not found

        Example:
            >>> result = db.query("sql", "SELECT FROM User WHERE id = 1").first()
            >>> age = result.get("age")  # Returns Python int
            >>> email = result.get("email")  # Returns Python str
            >>> phone = result.get("phone")  # Returns None if not found
            >>> phone = result.get("phone") or "unknown"  # Use default pattern
        """
        value = self.get_raw(name)
        if convert_types:
            return convert_java_to_python(value)
        return value

    def get_raw(self, name: str) -> Any:
        """
        Get a property value without Java-to-Python conversion.

        Args:
            name: Property name

        Returns:
            Raw Java-backed property value or None if not found
        """
        self._check_open()
        row_access = _bridge_class("RowAccess")
        if row_access is not None:
            # hasProperty() then getProperty() in ONE crossing
            return row_access.propertyOrNull(self._java_result, name)
        if not self._java_result.hasProperty(name):
            return None
        return self._java_result.getProperty(name)

    def get_rid(self) -> Optional[str]:
        """
        Get the Record ID (RID) if available.

        Returns:
            RID string (e.g., "#10:5") or None

        Example:
            >>> result = db.query("sql", "SELECT FROM User LIMIT 1").first()
            >>> print(result.get_rid())
            #10:5
        """
        identity = self._java_result.getIdentity()
        if identity.isPresent():
            return str(identity.get().toString())
        return None

    def get_vertex(self) -> Optional[Vertex]:
        """
        Get the underlying Vertex object if available.

        Returns:
            Vertex object or None
        """
        self._check_open()
        vertex = self._java_result.getVertex()
        if vertex.isPresent():
            return Vertex(vertex.get(), self._database)
        return None

    def get_edge(self) -> Optional[Edge]:
        """
        Get the underlying Edge object if available.

        Returns:
            Edge object or None
        """
        self._check_open()
        edge = self._java_result.getEdge()
        if edge.isPresent():
            return Edge(edge.get(), self._database)
        return None

    def get_element(self) -> Optional[Document]:
        """
        Get the underlying Element (Document, Vertex, or Edge) if available.

        Returns:
            Document, Vertex, or Edge object or None
        """
        self._check_open()
        element = self._java_result.getElement()
        if element.isPresent():
            return Document.wrap(element.get(), self._database)
        return None

    def get_property_names(self) -> List[str]:
        """
        Get all property names (alternative to property_names property).

        Returns:
            List of property names

        Example:
            >>> result = db.query("sql", "SELECT FROM User LIMIT 1").first()
            >>> names = result.get_property_names()
            >>> print(names)
            ['name', 'email', 'age', 'created_at']
        """
        return list(self._property_names_tuple())

    @property
    def property_names(self) -> List[str]:
        """
        Get all property names in this result.

        Returns:
            List of property names

        Example:
            >>> result = db.query("sql", "SELECT FROM User LIMIT 1").first()
            >>> print(result.property_names)
            ['name', 'email', 'age', 'created_at']
        """
        return list(self._property_names_tuple())

    def to_dict(self, convert_types: bool = True) -> Dict[str, Any]:
        """
        Convert result to dictionary.

        Args:
            convert_types: Convert Java types to Python (default: True)

        Returns:
            Dictionary with all properties

        Example:
            >>> result = db.query("sql", "SELECT FROM User LIMIT 1").first()
            >>> user_dict = result.to_dict()
            >>> print(user_dict)
            {'name': 'Alice', 'age': 30, 'email': 'alice@example.com'}
        """
        self._check_open()
        if convert_types:
            # One crossing for the whole row (names and values) instead of one
            # per property: a JPype call costs microseconds of dispatch, so a
            # small row was dominated by the number of calls. The values are
            # the engine's own objects, converted exactly as below.
            row_access = _bridge_class("RowAccess")
            if row_access is not None:
                pair = row_access.namesAndValues(self._java_result)
                names = tuple(str(name) for name in pair[0])
                if self._property_names_cache is None:
                    self._property_names_cache = names
                return {
                    name: convert_java_to_python(value)
                    for name, value in zip(names, pair[1])
                }

        property_names = self._property_names_tuple()
        if not convert_types:
            return {
                name: self._java_result.getProperty(name) for name in property_names
            }

        return {
            name: convert_java_to_python(self._java_result.getProperty(name))
            for name in property_names
        }

    def to_json(self) -> str:
        """
        Convert result to JSON string.

        Returns:
            JSON string representation

        Example:
            >>> result = db.query("sql", "SELECT FROM User LIMIT 1").first()
            >>> print(result.to_json())
            {"name": "Alice", "age": 30, "email": "alice@example.com"}
        """
        self._check_open()
        return str(self._java_result.toJSON())

    def __repr__(self) -> str:
        """String representation of the result."""
        try:
            rid = self.get_rid()
            property_names = self._property_names_tuple()
            names_preview = ", ".join(repr(name) for name in property_names[:3])
            if len(property_names) > 3:
                names_preview += ", ..."
            if rid is not None:
                return f"Result(rid={rid!r}, properties=[{names_preview}])"
            return f"Result(properties=[{names_preview}])"
        except (AttributeError, RuntimeError, TypeError, ValueError):
            return f"Result({self._java_result})"
