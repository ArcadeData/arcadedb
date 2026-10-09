"""
ArcadeDB Python Bindings - Core Database Classes

Database and DatabaseFactory classes for embedded database access.
"""

import itertools
from collections.abc import Mapping
from datetime import date
from decimal import Decimal
from os import PathLike
from typing import Any, List, Optional

import jpype

from .exceptions import ArcadeDBError
from .graph import Document, Edge, Vertex
from .graph_batch import GraphBatch
from .importer import ImportResult
from .importer import import_documents as run_document_import
from .jvm import start_jvm
from .results import ResultSet
from .transactions import TransactionContext
from .type_conversion import _is_numpy_bool, convert_python_to_java, json_bulk_dumps
from .vector import to_java_float_array

try:  # optional; hoisted to module scope to keep it out of per-call hot paths
    import numpy as _np
except ImportError:  # pragma: no cover - numpy is an optional dependency
    _np = None

# Java class handles resolved once per process (jpype.JClass lookups are not
# free and used to run per record in wrapper dispatch).
_JAVA_CLASSES = {}

# rows per JSON text in insert_many: bounds the memory a load holds (#294)
_INSERT_MANY_CHUNK = 10_000

# Parameter values that cross as they are (exact types, not subclasses): see
# Database._java_parameters.
_SCALAR_PARAM_TYPES = frozenset((int, float, str, bool, type(None)))


_JArray = jpype.JArray

# Resolved on first use, once the JVM is up (see _db_calls and _object_array).
_DB_CALLS = None
_OBJECT_ARRAY = None


def _is_plain_param(value):
    """A parameter value JPype passes as it is: an exact scalar type, or a value that is already a Java array."""
    return type(value) in _SCALAR_PARAM_TYPES or isinstance(value, _JArray)


def _db_calls():
    """``com.arcadedb.python.DbCalls`` from the bridge jar; None (the bridge's usual
    one warning) when the jar is missing, which sends the caller down the plain path."""
    global _DB_CALLS
    if _DB_CALLS is None:
        from .results import _bridge_class

        _DB_CALLS = _bridge_class("DbCalls") or False
    return _DB_CALLS or None


_STRING_ARRAY = None


def _string_array(values):
    """``String[]`` of ``values``, with the array class built once per process."""
    global _STRING_ARRAY
    if _STRING_ARRAY is None:
        _STRING_ARRAY = jpype.JArray(jpype.JString)
    return _STRING_ARRAY(values)


def _object_array(values):
    """``Object[]`` of ``values``, with the array class built once per process (building it
    costs about 0.6 us a call)."""
    global _OBJECT_ARRAY
    if _OBJECT_ARRAY is None:
        _OBJECT_ARRAY = jpype.JArray(jpype.JObject)
    return _OBJECT_ARRAY(values)


_NO_GLUE = object()


def _through_bridge(name, java_db, language, command, args):
    """Run ``name`` ("command" or "query") through ``DbCalls``, or return ``_NO_GLUE``.

    ``DbCalls`` takes the parameters as the varargs of one non-overloaded static
    method, so the call is one crossing where the plain path takes a map or an
    array to build, a cast, an overload resolution, and the call itself. Only
    parameters that cross as they are take it: a lone dict of str keys (named),
    or positional values, each an exact scalar type or a Java array (the same
    test as the short path of :meth:`Database._java_parameters`). Anything else,
    and a lone ``None`` or lone Java array (which JPype would read as the varargs
    array itself), takes the plain path.
    """
    calls = _db_calls()
    if calls is None:
        return _NO_GLUE
    scalar = _SCALAR_PARAM_TYPES
    if len(args) == 1:
        first = args[0]
        if type(first) is dict:
            flat = []
            for key, value in first.items():
                if type(key) is not str or not (
                    type(value) in scalar or isinstance(value, _JArray)
                ):
                    return _NO_GLUE
                flat.append(key)
                flat.append(value)
            call = calls.commandNamed if name == "command" else calls.queryNamed
            return call(java_db, language, command, *flat)
        values = first if isinstance(first, (list, tuple)) else args
    else:
        values = args
    count = len(values)
    if count == 0 or (
        count == 1 and (values[0] is None or isinstance(values[0], _JArray))
    ):
        return _NO_GLUE
    for value in values:
        if type(value) not in scalar and not isinstance(value, _JArray):
            return _NO_GLUE
    call = calls.commandPositional if name == "command" else calls.queryPositional
    return call(java_db, language, command, *values)


def _java_class(name):
    cls = _JAVA_CLASSES.get(name)
    if cls is None:
        import jpype

        cls = jpype.JClass(name)
        _JAVA_CLASSES[name] = cls
    return cls


def _column_to_java(name, values):
    """One column of ``Database.insert_columns`` as ``(java array, length)``.

    A numpy array of an integer, float, or bool kind crosses as ONE buffer copy
    into a ``long[]``, ``double[]``, or ``boolean[]``; a string or object array
    and any other sequence convert per element into an ``Object[]`` (a ``str``
    reuses one Java String per distinct value, ``None`` is a null). A pandas
    ``Series`` is read through ``to_numpy``, with a nullable dtype's ``<NA>``
    as ``None``. Kinds that do not cross natively raise ``TypeError`` rather
    than being stored as something else.
    """
    if hasattr(values, "to_numpy") and not isinstance(values, (list, tuple)):
        dtype = getattr(values, "dtype", None)
        if _np is not None and isinstance(dtype, _np.dtype):
            values = values.to_numpy()
        else:  # a pandas extension dtype (Int64, string, boolean, category): its <NA> is a null
            values = values.to_numpy(dtype=object, na_value=None)
    if _np is not None and isinstance(values, _np.ndarray):
        if values.ndim != 1:
            raise ValueError(
                f"column {name!r} is {values.ndim}-dimensional; a column is one-dimensional"
            )
        kind = values.dtype.kind
        if kind in "iu":
            if (
                kind == "u"
                and values.size
                and int(values.max()) > _np.iinfo(_np.int64).max
            ):
                raise ValueError(
                    f"column {name!r} holds a value beyond the 64-bit signed range"
                )
            return (
                jpype.JArray(jpype.JLong)(
                    _np.ascontiguousarray(values, dtype=_np.int64)
                ),
                int(values.shape[0]),
            )
        if kind == "f":
            return (
                jpype.JArray(jpype.JDouble)(
                    _np.ascontiguousarray(values, dtype=_np.float64)
                ),
                int(values.shape[0]),
            )
        if kind == "b":
            return (
                jpype.JArray(jpype.JBoolean)(
                    _np.ascontiguousarray(values, dtype=_np.bool_)
                ),
                int(values.shape[0]),
            )
        if kind in "MmcV":
            raise TypeError(
                f"column {name!r} has dtype {values.dtype}, which does not cross natively; "
                "convert it to Python values or use insert_many"
            )
        # a string, bytes, or object array: its elements are Python objects (str, None, ...), converted one by one below
        values = list(values.astype(object, copy=False))
    elif not isinstance(values, (list, tuple)):
        values = list(values)
    converted = []
    seen = {}
    for value in values:
        if type(value) is str:
            java = seen.get(value)
            if java is None:
                java = convert_python_to_java(value)
                seen[value] = java
            converted.append(java)
        else:
            converted.append(convert_python_to_java(value))
    return jpype.JArray(jpype.JObject)(converted), len(converted)


def _wrap_java_record(java_record, database=None):
    """Wrap a Java record in the matching Python class (Vertex/Edge/Document)."""
    if java_record is None:
        return None
    if isinstance(java_record, _java_class("com.arcadedb.graph.Vertex")):
        return Vertex(java_record, database)
    if isinstance(java_record, _java_class("com.arcadedb.graph.Edge")):
        return Edge(java_record, database)
    if isinstance(java_record, _java_class("com.arcadedb.database.Document")):
        return Document(java_record, database)
    return java_record


class Database:
    """ArcadeDB Database wrapper."""

    def __init__(self, java_database):
        self._java_db = java_database
        self._closed = False
        self._async_executors = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def get_java_database(self):
        """Expose the wrapped Java database for internal integrations."""
        return self._java_db

    @staticmethod
    def _convert_args(args):
        if not args:
            return []

        # Historical semantics (kept for compatibility): a SINGLE list/tuple
        # argument is the positional-parameter array itself — one element per
        # `?` placeholder — matching how JPype bound it to the Object[]
        # varargs before explicit conversion existed. It expands here, with
        # per-element conversion.
        if len(args) == 1 and isinstance(args[0], (list, tuple)):
            args = tuple(args[0])

        converted_args = []
        for arg in args:
            if _np is not None and isinstance(arg, _np.ndarray):
                converted_args.append(to_java_float_array(arg))
            elif isinstance(arg, (Mapping, list, tuple, set, bytes, bytearray)):
                # A collection AMONG multiple args is a single collection-typed
                # parameter (e.g. a query vector). Plain Python collections
                # don't participate in JPype's varargs overload resolution, so
                # convert them to java.util collections explicitly.
                converted_args.append(convert_python_to_java(arg))
            elif isinstance(arg, (Decimal, date)):
                # Left to JPype, a Decimal reached the engine as a Double (38
                # digits stored as 1.2345678901234567E+19), and a datetime or a
                # date matched no overload at all (#58). datetime is a date.
                converted_args.append(convert_python_to_java(arg))
            elif _is_numpy_bool(arg):
                # Not a bool subclass: left to JPype it is stored as the Double
                # 1.0 or 0.0, and `WHERE ok = true` stops matching it.
                converted_args.append(bool(arg))
            else:
                converted_args.append(arg)

        return converted_args

    @staticmethod
    def _java_parameters(args):
        """The one Java argument that carries the parameters of query()/command().

        A lone mapping is the named-parameter map and goes to the ``Map``
        overload; anything else is the positional list and goes to the
        ``Object...`` overload as an explicit ``Object[]``. Splatting the
        values instead left the choice of overload to JPype, which cannot make
        it for a lone ``None``: ``command(str, str, None)`` matches
        ``Object...``, ``Map``, and ``ContextConfiguration, Object...`` alike
        and raised "Ambiguous overloads" (#172), and so did ``(None, 1)``.

        A mapping alone in a lone list or tuple (``[{...}]``) is still the
        named map, as JPype chose before; SQL reads an ``Object[]`` holding
        only a map that way too, but openCypher would refuse it.
        """
        values = (
            args[0] if len(args) == 1 and isinstance(args[0], (list, tuple)) else args
        )
        # THE COMMON CASE, WITHOUT THE GENERAL MACHINERY: parameters that are all
        # plain scalars (a dict of str keys, or positional values) need no
        # per-value conversion, and JPype boxes them exactly as the general path's
        # unchanged values are boxed. The general path costs 6.5 to 9 us a call on
        # the laptop, the fast one 4.3 to 5.7 us, on a statement that is 15 to
        # 60 us in all. Exact types only: a bool is a bool, and a numpy scalar, a
        # Decimal, or a date is anything else and takes the general path. A value
        # that is already a Java array (a vector from to_java_float_array) passes
        # too: the general path returns it unchanged, after about 11 us of work a
        # call on the dense insert (#276).
        plain = _is_plain_param
        if len(values) == 1:
            first = values[0]
            if type(first) is dict and all(
                type(k) is str and plain(v) for k, v in first.items()
            ):
                params = _java_class("java.util.HashMap")()
                for key, item in first.items():
                    params.put(key, item)
                return jpype.JObject(params, _java_class("java.util.Map"))
        if all(plain(a) for a in values):
            return _object_array(values)
        if len(values) == 1 and isinstance(values[0], Mapping):
            java_map = _java_class("java.util.Map")
            params = values[0]
            if not isinstance(params, java_map):
                params = convert_python_to_java(
                    params if isinstance(params, dict) else dict(params)
                )
            return jpype.JObject(params, java_map)
        return _object_array(Database._convert_args(args))

    def query(self, language: str, command: str, *args) -> ResultSet:
        """Execute a query and return results.

        Parameters bind positionally (``?``) from the extra arguments, or by
        name (``:name``, ``$name``) from a single dict. A single list or tuple
        is the positional list itself; ``None`` binds as null.
        """
        self._check_not_closed()
        try:
            if args:
                java_result = _through_bridge(
                    "query", self._java_db, language, command, args
                )
                if java_result is _NO_GLUE:
                    java_result = self._java_db.query(
                        language, command, self._java_parameters(args)
                    )
            else:
                java_result = self._java_db.query(language, command)
            return ResultSet(java_result, self)
        except Exception as e:
            raise ArcadeDBError(f"Query failed: {e}") from e

    def command(self, language: str, command: str, *args) -> Optional[ResultSet]:
        """Execute a command (non-idempotent operation).

        Parameters bind as in :meth:`query`.
        """
        self._check_not_closed()
        try:
            if args:
                java_result = _through_bridge(
                    "command", self._java_db, language, command, args
                )
                if java_result is _NO_GLUE:
                    java_result = self._java_db.command(
                        language, command, self._java_parameters(args)
                    )
            else:
                java_result = self._java_db.command(language, command)

            if java_result is not None:
                return ResultSet(java_result, self)
            return None
        except Exception as e:
            raise ArcadeDBError(f"Command failed: {e}") from e

    def run_in_transaction(self, fn, retries: int = 12, backoff_s: float = 0.005):
        """
        Execute a callable inside a transaction with automatic retry on
        concurrent-modification conflicts.

        Mirrors the Java API's ``database.transaction(lambda)`` semantics: on
        ``ConcurrentModificationException`` / ``NeedRetryException`` the
        transaction is rolled back and ``fn`` is re-executed, with linear
        backoff. The ``with db.transaction():`` context manager cannot retry
        (a ``with`` block can't be re-entered), so use this for contended
        multi-threaded writes.

        Args:
            fn: Zero-argument callable executed inside the transaction.
            retries: Max retry attempts on conflict (default 12).
            backoff_s: Base sleep between attempts, grows linearly.

        Returns:
            The return value of ``fn``.
        """
        import time as _time

        for attempt in range(retries + 1):
            self.begin()
            try:
                result = fn()
                self.commit()
                return result
            except BaseException as e:
                # Any exit path other than a successful commit must roll back,
                # not just ArcadeDBError - matching LocalDatabase.transaction()'s
                # `catch (final Throwable e)` on the Java side (#7108). An
                # ordinary bug in fn() (TypeError, KeyError, ...) must not leave
                # an open transaction for the next caller to inherit, and neither
                # must KeyboardInterrupt/SystemExit, which `except Exception`
                # does not catch (code review on #7108).
                try:
                    if self.is_transaction_active():
                        self.rollback()
                except Exception:  # nosec B110 - best-effort rollback before retry
                    pass
                if isinstance(e, ArcadeDBError):
                    msg = str(e)
                    retryable = (
                        "ConcurrentModificationException" in msg
                        or "NeedRetryException" in msg
                    )
                    if retryable and attempt < retries:
                        _time.sleep(backoff_s * (attempt + 1))
                        continue
                raise

    def begin(self):
        """Begin a transaction."""
        self._check_not_closed()
        try:
            self._java_db.begin()
        except Exception as e:
            raise ArcadeDBError(f"Failed to begin transaction: {e}") from e

    def commit(self):
        """Commit the current transaction."""
        self._check_not_closed()
        try:
            self._java_db.commit()
        except Exception as e:
            raise ArcadeDBError(f"Failed to commit transaction: {e}") from e

    def rollback(self):
        """Rollback the current transaction."""
        self._check_not_closed()
        try:
            self._java_db.rollback()
        except Exception as e:
            raise ArcadeDBError(f"Failed to rollback transaction: {e}") from e

    def transaction(self) -> TransactionContext:
        """Create a transaction context manager."""
        return TransactionContext(self)

    def new_vertex(self, type_name: str) -> Vertex:
        """Create a new vertex."""
        self._check_not_closed()
        try:
            return Vertex(self._java_db.newVertex(type_name), self)
        except Exception as e:
            raise ArcadeDBError(
                f"Failed to create vertex of type '{type_name}': {e}"
            ) from e

    def new_document(self, type_name: str) -> Document:
        """Create a new document."""
        self._check_not_closed()
        try:
            return Document(self._java_db.newDocument(type_name), self)
        except Exception as e:
            raise ArcadeDBError(
                f"Failed to create document of type '{type_name}': {e}"
            ) from e

    def _parallel_load_failed(self, type_name, error):
        """The error for a parallel load that failed while handing rows to the writers.

        The rows handed over before the failure are already queued, and the async writers commit
        them whatever this call does. So wait for the queue to drain before raising: the caller then
        sees a final state that does not change behind its back, and the message says that those
        rows may have been stored, rather than implying a rollback that cannot happen.
        """
        try:
            self._java_db.async_().waitCompletion()
        except Exception:  # nosec B110 - the original error is the one to report
            pass
        return ArcadeDBError(
            f"Failed to bulk-insert into '{type_name}': {error}; the rows handed to the "
            f"parallel writers before the failure may have been stored (the call waited "
            f"for them before raising)"
        )

    def insert_many(
        self,
        type_name: str,
        rows,
        commit_every: int = 10_000,
        parallel: bool = False,
    ) -> int:
        """Bulk-insert documents with one FFI crossing per chunk of rows.

        The iterable is read 10,000 rows at a time; each chunk is serialized
        to one JSON string and looped Java-side (``DocumentBatcher``),
        avoiding the per-row JNI cost that caps ``new_document``-loop ingest.
        Memory stays bounded by the chunk whatever the row count, so a
        generator of any length can be loaded (#294). Values must be
        JSON-representable (str/int/float/bool/None and nested lists/dicts);
        a chunk containing other types (e.g. datetime, bytes) falls back
        transparently to the per-row path.

        Args:
            type_name: Target document type (must exist).
            rows: Iterable of dicts, one per document. Read lazily, one
                chunk at a time.
            commit_every: Transaction batch size for the synchronous mode.
            parallel: If True, route rows through the async executor's
                parallel bucket writers and wait for completion before
                returning (out-of-order writes). The maintainers' rule
                (ArcadeData/arcadedb#8478): a bucket count equal to, or a
                multiple of, the executor's parallel level
                (``async_executor().get_parallel_level()``, default
                cores - 1), set when the type is created
                (``CREATE DOCUMENT TYPE T BUCKETS n``). Measured on a laptop
                (4 performance cores, parallel level 3, 1,000,000 rows,
                6 runs per arm, engine ``b22b5e9954``, 2026-10-04): 1.11x to
                1.14x faster than the synchronous mode at 1, 3, 4, and 8
                buckets alike (8 buckets no faster than 1). Each writer
                commits every ``arcadedb.asyncTxBatchSize`` records (default
                10,240); ``commit_every`` does not apply to this mode.

        Returns:
            Number of documents inserted.

        Raises:
            Exception: An exception raised by ``rows`` itself propagates
                unchanged; the open batch is rolled back and the batches
                committed before it stay (in the parallel mode, rows already
                handed to the writers are waited for and may be stored).
            ArcadeDBError: If the load fails; in the parallel mode also when
                the writers report any record they could not store (a
                duplicate key, a failed batch commit), after the load
                completes. Records other than the failed ones may have been
                stored. In the parallel mode nothing is rolled back: rows
                handed to the writers before a failure may be stored, and the
                call waits for them before raising.
        """
        self._check_not_closed()
        # Rows are read from the iterable one chunk at a time and each chunk
        # crosses as its own JSON text, so the memory a load holds is bounded
        # by the chunk, not by the input. Reading the whole input into a list
        # and one JSON text (which the engine then parsed into one JSON array)
        # held about 1.5 KB per row at once, Python and Java together: a
        # 26-million-row generator could not load under 32 GB (#294).
        chunk_rows = _INSERT_MANY_CHUNK
        commit_every = int(commit_every)
        rows = iter(rows)
        first = list(itertools.islice(rows, chunk_rows))
        if not first:
            return 0
        if parallel:
            return self._insert_many_parallel(type_name, first, rows)
        # This call owns the transactions it opens: it commits every
        # commit_every rows, counted across chunks, and on any failure rolls
        # back the one still open (earlier batches stay committed, as they
        # always did). A caller's own transaction is the caller's to commit or
        # roll back (#7882).
        was_active = self.is_transaction_active()
        n = 0
        try:
            # begin() inside the try: a ^C landing right after it must still
            # reach the rollback below.
            if not was_active:
                self.begin()
            chunk = first
            while chunk:
                if not was_active and commit_every > 0:
                    # never let a chunk straddle a commit boundary
                    room = commit_every - n % commit_every
                    if len(chunk) > room:
                        rest = chunk[room:]
                        chunk = chunk[:room]
                    else:
                        rest = None
                else:
                    rest = None
                self._insert_many_chunk(type_name, chunk)
                n += len(chunk)
                if not was_active and commit_every > 0 and n % commit_every == 0:
                    self.commit()
                    self.begin()
                if rest:
                    chunk = rest
                else:
                    size = chunk_rows
                    if not was_active and commit_every > 0:
                        size = min(size, commit_every - n % commit_every)
                    chunk = list(itertools.islice(rows, size))
            if not was_active:
                self.commit()
            return n
        except BaseException:
            # Any exit other than the final commit rolls back the transaction
            # this method opened, as run_in_transaction does (#7108, #7882).
            # BaseException so ^C/SystemExit cannot leak it either.
            if not was_active:
                try:
                    if self.is_transaction_active():
                        self.rollback()
                except Exception:  # nosec B110 - best-effort rollback
                    pass
            raise

    def _insert_many_chunk(self, type_name, chunk):
        """Insert one chunk of insert_many rows inside the open transaction."""
        try:
            payload = json_bulk_dumps(chunk)
        except (TypeError, ValueError):
            # Values the JSON text cannot carry unchanged (numpy integer
            # scalars, which json.dumps rejects; an integer beyond 64 bits,
            # NaN or Infinity, a non-str dict key, a lone surrogate, which the
            # engine's JSON parser would store as a different value): this
            # chunk goes row by row.
            for row in chunk:
                doc = self.new_document(type_name)
                for k, v in row.items():
                    doc.set(k, v)
                doc.save()
            return
        try:
            # commitEvery 0 inside a transaction that is already open: the
            # helper only inserts, and the transaction stays with insert_many.
            _java_class("com.arcadedb.python.DocumentBatcher").insertManyJson(
                self._java_db, type_name, payload, 0, False
            )
        except Exception as e:
            raise ArcadeDBError(f"Failed to bulk-insert into '{type_name}': {e}") from e

    def _insert_many_parallel(self, type_name, first, rows):
        """insert_many(parallel=True): hand each chunk to the async writers."""
        batcher = _java_class("com.arcadedb.python.DocumentBatcher")
        n = 0
        chunk_failures = []
        chunk = first
        try:
            while chunk:
                try:
                    payload = json_bulk_dumps(chunk)
                except (TypeError, ValueError):
                    payload = None
                if payload is None:
                    # A chunk the JSON text cannot carry unchanged is written
                    # synchronously, row by row, in its own transaction, after
                    # the writers have drained, as the whole load used to be.
                    self._java_db.async_().waitCompletion()
                    was_active = self.is_transaction_active()
                    try:
                        if not was_active:
                            self.begin()
                        self._insert_many_chunk(type_name, chunk)
                        if not was_active:
                            self.commit()
                    except BaseException:
                        if not was_active:
                            try:
                                if self.is_transaction_active():
                                    self.rollback()
                            except Exception:  # nosec B110 - best-effort rollback
                                pass
                        raise
                else:
                    try:
                        failures = batcher.insertManyJsonParallel(
                            self._java_db, type_name, payload
                        )
                    except Exception as e:
                        raise self._parallel_load_failed(type_name, e) from e
                    # The writers report a rejected record only through its
                    # error callback (and the executor's global one, which by
                    # default just logs), so the count is read rather than
                    # assumed (ArcadeData/arcadedb#8478). Read after the final
                    # waitCompletion, when every writer is done.
                    chunk_failures.append(failures)
                n += len(chunk)
                chunk = list(itertools.islice(rows, _INSERT_MANY_CHUNK))
        except ArcadeDBError:
            raise
        except BaseException:
            # An error from the rows iterable, or ^C: the rows already queued
            # are written whatever happens here, so wait for them and the
            # caller sees a final state (as _parallel_load_failed does).
            try:
                self._java_db.async_().waitCompletion()
            except Exception:  # nosec B110 - the original error is the one to report
                pass
            raise
        try:
            self._java_db.async_().waitCompletion()
        except Exception as e:
            raise ArcadeDBError(f"Failed to bulk-insert into '{type_name}': {e}") from e
        n_failed = 0
        first_failure = None
        for failures in chunk_failures:
            count = int(failures.getCount())
            if count and first_failure is None:
                first_failure = failures.getFirstMessage()
            n_failed += count
        if n_failed:
            raise ArcadeDBError(
                f"Failed to bulk-insert into '{type_name}': the parallel writers "
                f"reported {n_failed} failed record(s) of {n}; the rest "
                f"may have been stored (first failure: {first_failure})"
            )
        return n

    def insert_columns(
        self,
        type_name: str,
        columns,
        commit_every: int = 10_000,
        parallel: bool = False,
    ) -> int:
        """Bulk-insert documents from whole columns, the recommended path for column data.

        Each column crosses the Python/Java bridge ONCE, as one typed array,
        and the documents are built Java-side (``DocumentBatcher.insertColumns``),
        instead of one JSON text per batch that the engine parses and copies
        key by key. Measured on a laptop (first 2,000,000 TPC-H SF1 line items,
        nine typed properties, commit every 10,000, same rows, cores, and
        engine): 8.2 s against 18.4 s for ``insert_many`` (2.24x); every arm
        stored the same sums and count. Use it whenever the data already lives
        in columns (a pandas ``DataFrame``, a parquet batch, numpy arrays);
        ``insert_many`` stays the path for a list of dicts.

        Args:
            type_name: Target document type (must exist).
            columns: ``{property name: column}``, or a pandas ``DataFrame``.
                Every column has the same length. A column is a numpy array
                (integer kinds cross as ``long[]``, float kinds as ``double[]``,
                bool as ``boolean[]``, one buffer copy each) or any sequence of
                Python values (``str``, ``int``, ``float``, ``bool``, ``None``,
                and the types ``insert_many``'s per-row fallback accepts), which
                converts per element. ``None`` is a null; a float ``NaN`` in a
                numpy float column is stored as NaN, not as null (use a
                sequence with ``None`` for nulls). A pandas nullable column
                (``Int64``, ``string``) converts with its ``<NA>`` as null.
            commit_every: Transaction batch size for the synchronous mode.
            parallel: If True, hand the documents to the async executor's
                parallel bucket writers and wait for completion before
                returning, exactly as ``insert_many(parallel=True)`` does (the
                same bucket-count rule, the same out-of-order writes;
                ``commit_every`` does not apply). Only the transport differs:
                columns instead of one JSON text per batch.

        Returns:
            Number of documents inserted.

        Raises:
            ValueError: If ``columns`` is empty, a column has a different
                length from the others, a name is not a string, or an unsigned
                column holds a value beyond the 64-bit signed range. Raised
                before anything is written.
            TypeError: If a numpy column has a dtype that does not cross
                natively (datetime64, timedelta64, complex): convert it to
                Python values, or use ``insert_many``.
                Also if a column is a ``str``, ``bytes``, or ``bytearray``
                (one value, not a column), which would otherwise be split into
                one row per character.
            ArcadeDBError: If the load fails (a duplicate key, a value the
                declared property type refuses): the transaction this call
                opened is rolled back, as for ``insert_many``. In the parallel
                mode also when the writers report any record they could not
                store, after the load completes; the parallel mode rolls
                nothing back: rows handed to the writers before a failure, or
                other than the failed ones, may have been stored, and the call
                waits for them before raising. A transaction the caller opened
                is left to the caller.

        Example:
            >>> db.insert_columns("Reading", {
            ...     "id": np.arange(1_000_000, dtype=np.int64),
            ...     "value": np.random.random(1_000_000),
            ...     "label": ["a", "b"] * 500_000,
            ... })
        """
        self._check_not_closed()
        items = list(columns.items()) if hasattr(columns, "items") else None
        if not items:
            raise ValueError("insert_columns needs at least one column")
        names = []
        for name, values in items:
            if not isinstance(name, str):
                raise ValueError(f"column names must be strings, got {name!r}")
            # A str or bytes value is one value, not a column: list() would split it into
            # characters and store one row per character without an error.
            if isinstance(values, (str, bytes, bytearray)):
                raise TypeError(
                    f"column {name!r} is a {type(values).__name__}, not a sequence of values; "
                    f"wrap a single value in a list"
                )
            names.append(name)
        # Compare the lengths that are known without converting first, so ragged columns are refused
        # before a large one is copied across the JVM bridge; a plain iterable is measured after.
        known = {
            name: len(values) for name, values in items if hasattr(values, "__len__")
        }
        if len(set(known.values())) > 1:
            raise ValueError(f"columns differ in length: {known}")
        java_columns = [_column_to_java(name, values) for name, values in items]
        lengths = {name: n for name, (_arr, n) in zip(names, java_columns)}
        if len(set(lengths.values())) != 1:
            raise ValueError(f"columns differ in length: {lengths}")
        n = next(iter(lengths.values()))
        if n == 0:
            return 0
        try:
            batcher = _java_class("com.arcadedb.python.DocumentBatcher")
            string_array = jpype.JArray(jpype.JString)(names)
            object_array = jpype.JArray(jpype.JObject)(
                [arr for arr, _n in java_columns]
            )
            if not parallel:
                return int(
                    batcher.insertColumns(
                        self._java_db,
                        type_name,
                        string_array,
                        object_array,
                        n,
                        int(commit_every),
                    )
                )
            try:
                failures = batcher.insertColumnsParallel(
                    self._java_db, type_name, string_array, object_array, n
                )
            except Exception as e:
                raise self._parallel_load_failed(type_name, e) from e
            self._java_db.async_().waitCompletion()
            n_failed = int(failures.getCount())
            first_failure = failures.getFirstMessage()
        except ArcadeDBError:
            raise
        except Exception as e:
            raise ArcadeDBError(f"Failed to bulk-insert into '{type_name}': {e}") from e
        # As insert_many does: the writers report a rejected record only through
        # its error callback, so the count is read here rather than assumed.
        if n_failed:
            raise ArcadeDBError(
                f"Failed to bulk-insert into '{type_name}': the parallel writers "
                f"reported {n_failed} failed record(s) of {n}; the rest may have "
                f"been stored (first failure: {first_failure})"
            )
        return n

    def close(self):
        """Close the database."""
        if not self._closed and self._java_db is not None:
            async_close_error = None
            try:
                async_close_error = self._close_async_executors()
                self._java_db.close()
            except Exception as e:
                # A server-managed database is owned by the server lifecycle,
                # not by this handle: the engine raises
                # UnsupportedOperationException rather than closing it. That is
                # the expected outcome for a Database obtained from
                # ArcadeDBServer.get_database(), so treat it as closed here and
                # let the server own the real shutdown.
                if "cannot be closed" in str(e).lower():
                    self._closed = True
                    if async_close_error is not None:
                        raise async_close_error
                    return
                raise ArcadeDBError(f"Failed to close database: {e}") from e
            finally:
                self._closed = True

            if async_close_error is not None:
                raise async_close_error

    def is_open(self) -> bool:
        """Check if database is open."""
        return not self._closed and self._java_db.isOpen()

    def get_name(self) -> str:
        """Get the database name."""
        self._check_not_closed()
        try:
            return self._java_db.getName()
        except Exception as e:
            raise ArcadeDBError(f"Failed to get database name: {e}") from e

    def get_database_path(self) -> str:
        """Get the database path."""
        self._check_not_closed()
        try:
            return self._java_db.getDatabasePath()
        except Exception as e:
            raise ArcadeDBError(f"Failed to get database path: {e}") from e

    def lookup_by_key(self, type_name: str, keys: List[str], values: List[Any]):
        """
        Lookup records by indexed key (index-based: O(1) for a hash index, O(log n)
        for an LSM_TREE index).

        Args:
            type_name: Type name
            keys: List of property names (must be indexed)
            values: List of property values

        Returns:
            Python-wrapped records or None

        Example:
            >>> records = list(db.lookup_by_key("User", ["email"], ["alice@example.com"]))
            >>> if records:
            ...     user = records[0]
        """
        self._check_not_closed()
        try:
            # Converted like every other parameter: a datetime or a date key
            # matched no Java type at all, and a numpy bool was read as a number.
            keys_array = _string_array(keys)
            values_array = _object_array([convert_python_to_java(v) for v in values])

            calls = _db_calls()
            if calls is not None:
                # lookup, hasNext(), next(), and getRecord() in ONE crossing
                java_record = calls.lookupFirst(
                    self._java_db, type_name, keys_array, values_array
                )
                return None if java_record is None else Document.wrap(java_record, self)

            cursor = self._java_db.lookupByKey(type_name, keys_array, values_array)

            # Return first result wrapped, or None
            if cursor.hasNext():
                java_record = cursor.next().getRecord()
                return Document.wrap(java_record, self)
            return None
        except Exception as e:
            raise ArcadeDBError(f"Failed to lookup by key in '{type_name}': {e}") from e

    def lookup_by_rid(self, rid: str) -> Any:
        """
        Lookup a record by its RID.

        Args:
            rid: Record ID string (e.g. "#10:5")

        Returns:
            Record object (Vertex, Document, or Edge)

        Raises:
            ArcadeDBError: If no record has that RID (RecordNotFoundException)

        Example:
            >>> try:
            ...     record = db.lookup_by_rid("#10:5")
            ...     print(record.get("name"))
            ... except ArcadeDBError:
            ...     print("no record with that RID")
        """
        self._check_not_closed()
        try:
            java_rid = self.to_java_rid(rid)
            return self._lookup_by_java_rid(java_rid)
        except Exception as e:
            raise ArcadeDBError(f"Failed to lookup RID '{rid}': {e}") from e

    def _lookup_by_java_rid(self, java_rid) -> Any:
        """Lookup by an already-Java RID, skipping string parsing (hot path)."""
        java_record = self._java_db.lookupByRID(java_rid, True)
        return _wrap_java_record(java_record, self)

    def to_java_rid(self, value):
        self._check_not_closed()

        value = getattr(value, "_java_record", value)
        if hasattr(value, "getIdentity"):
            return value.getIdentity()
        if isinstance(value, str):
            return _java_class("com.arcadedb.database.RID")(value)
        if hasattr(value, "get_identity"):
            return value.get_identity()
        return value

    def _to_java_rid(self, value):
        return self.to_java_rid(value)

    def create_vector_index(
        self,
        vertex_type: str,
        vector_property: str,
        dimensions: int,
        id_property: Optional[str] = None,
        distance_function: str = "cosine",
        max_connections: int = 32,
        beam_width: int = 100,
        quantization: str = "INT8",
        encoding: Optional[str] = None,
        location_cache_size: Optional[int] = None,
        graph_build_cache_size: Optional[int] = None,
        mutations_before_rebuild: Optional[int] = None,
        store_vectors_in_graph: bool = False,
        add_hierarchy: Optional[bool] = True,
        pq_subspaces: Optional[int] = None,
        pq_clusters: Optional[int] = None,
        pq_center_globally: Optional[bool] = None,
        pq_training_limit: Optional[int] = None,
        build_graph_now: bool = True,
    ) -> "VectorIndex":
        """
        Create a vector index for similarity search (JVector implementation).

        This uses JVector (graph index combining HNSW hierarchy with Vamana/DiskANN)
        which provides:
        - No max_items limit (grows dynamically)
        - Fast index construction
        - Automatic indexing of existing records
        - Concurrent construction support

        Args:
            vertex_type: Name of the vertex type
            vector_property: Name of the property containing vectors
            dimensions: Vector dimensionality (e.g., 768 for BERT)
            id_property: Optional property used for key-based vector lookup.
                Defaults to the engine default (usually "id") when omitted.
            distance_function: "cosine", "euclidean", or "dot_product"
            max_connections: Per-layer graph degree (default: 32, matching the
                engine default since #5352). Maps to `maxConnections` in
                JVector, which is a Vamana per-layer degree and is NOT doubled
                at the base layer like hnswlib's M: to reproduce an
                hnswlib-style configuration use max_connections = 2 * M.
            beam_width: Beam width for search/construction (default: 100).
                Maps to `beamWidth` in JVector.
            quantization: Vector quantization type (default: INT8).
                Options: "INT8", "BINARY", "PRODUCT" (PQ).
                Reduces memory usage and speeds up search at the cost of some precision.
                "PRODUCT" enables PQ data for approximate search (zero-disk-I/O path).
                In current ArcadeDB engine builds, PRODUCT also requires enough indexed
                vectors per bucket for PQ training; for tiny corpora, set `pq_clusters`
                explicitly to a small value or prefer INT8/BINARY/NONE.
            encoding: Optional storage encoding for the underlying vector property.
                Use "INT8" when the document property stores pre-quantized bytes in a
                `BINARY` property. When using INT8 encoding, set quantization to "NONE"
                to avoid double quantization.
            location_cache_size: REMOVED by the engine (issues #5559, #5568).
                Passing anything other than None raises ValueError. A vector
                location is the only mapping from a vector id to its record, so
                capping it does not spill to disk, it drops vectors from searches
                and from countEntries(). Size the heap instead. The parameter is
                kept only so that upgrading callers get this explanation rather
                than an unexplained TypeError.
            graph_build_cache_size: Per-index override for the number of vectors
                cached while the graph is built (maps to Java metadata key
                "graphBuildCacheSize"; uses GlobalConfiguration default if None).
                Leave it unset: the default is automatic, sized from the heap the
                engine has free, and caches the whole corpus when it fits. A count
                below the corpus makes the build re-read vectors from the
                documents. Set an absolute count only to bound a build on a
                deliberately small heap.
            mutations_before_rebuild: Per-index override for mutations threshold
                before triggering a graph rebuild (maps to Java metadata key
                "mutationsBeforeRebuild"; uses GlobalConfiguration default if None).
                Typical ranges: 100–300 for freshness-heavy workloads; 300–800 for
                write-heavy workloads and larger graphs.
            pq_subspaces: Number of PQ subspaces (M). Requires quantization="PRODUCT".
            pq_clusters: Clusters per subspace (K). Requires quantization="PRODUCT".
                In current ArcadeDB engine builds, this should not exceed the
                number of indexed vectors available for PQ training in a bucket.
            pq_center_globally: Whether to globally center vectors before PQ.
                Requires quantization="PRODUCT".
            pq_training_limit: Max vectors to use for PQ training. Requires
                quantization="PRODUCT".
            build_graph_now: If True (default), eagerly builds the vector graph
                immediately after index creation. If False, graph preparation is
                deferred and may happen lazily on first search.
            store_vectors_in_graph: Whether to store vectors inline in the graph
                structure (default: False). If True, increases disk usage but
                significantly speeds up search for large datasets by avoiding document
                lookups.
            add_hierarchy: Whether to build hierarchical layers in the HNSW graph
                (Default is True). If None, uses the engine default. Set explicitly to
                True/False to force the behavior.

        Returns:
            VectorIndex object
        """
        self._check_not_closed()

        # The engine removed this in #5559/#5568 and now rejects it in
        # withMetadata(), so a wheel that still forwards it cannot create an
        # index at all. Refuse here instead, with the reason: a vector location
        # is the only mapping from a vector id to its record, so a bound on it
        # is not a cache eviction, it silently drops vectors from searches and
        # from countEntries().
        if location_cache_size is not None:
            raise ValueError(
                "location_cache_size is no longer supported (ArcadeDB issues "
                "#5559 and #5568): a vector location is the only mapping from a "
                "vector id to its record, so capping the location index drops "
                "vectors from searches and from countEntries() instead of "
                "spilling them to disk. Remove the argument and size the heap "
                "for the live vector set instead."
            )

        # Create the index using the Java Builder API directly to pass configuration
        try:
            import jpype

            if any(
                val is not None
                for val in (
                    pq_subspaces,
                    pq_clusters,
                    pq_center_globally,
                    pq_training_limit,
                )
            ):
                if not quantization or quantization.upper() != "PRODUCT":
                    raise ValueError("PQ parameters require quantization='PRODUCT'")

            java_schema = self.schema._java_schema

            # Convert property names to Java array
            java_props = jpype.JArray(jpype.JString)([vector_property])

            # Build the index
            builder = java_schema.buildTypeIndex(vertex_type, java_props)

            # Set type to LSM_VECTOR (this returns TypeLSMVectorIndexBuilder)
            INDEX_TYPE = jpype.JPackage("com").arcadedb.schema.Schema.INDEX_TYPE
            builder = builder.withType(INDEX_TYPE.LSM_VECTOR)

            # Configure
            builder.withDimensions(dimensions)
            builder.withSimilarity(distance_function)
            builder.withMaxConnections(max_connections)
            builder.withBeamWidth(beam_width)

            if id_property:
                builder.withIdProperty(id_property)

            if quantization:
                builder.withQuantization(quantization)

            if encoding:
                if (
                    encoding.upper() == "INT8"
                    and quantization
                    and quantization.upper() == "INT8"
                ):
                    raise ValueError(
                        "encoding='INT8' cannot be combined with "
                        "quantization='INT8'; use quantization='NONE' for native "
                        "INT8 storage"
                    )

                encoding_setter = getattr(builder, "withEncoding", None)
                if encoding_setter is None:
                    raise ValueError(
                        "This ArcadeDB engine build does not support vector encoding; "
                        "update the embedded engine artifacts"
                    )

                encoding_setter(encoding)

            if pq_subspaces is not None:
                builder.withPQSubspaces(int(pq_subspaces))
            if pq_clusters is not None:
                builder.withPQClusters(int(pq_clusters))
            if pq_center_globally is not None:
                builder.withPQCenterGlobally(bool(pq_center_globally))
            if pq_training_limit is not None:
                builder.withPQTrainingLimit(int(pq_training_limit))

            metadata_cfg = {}
            if store_vectors_in_graph:
                metadata_cfg["storeVectorsInGraph"] = True
            if add_hierarchy is not None:
                metadata_cfg["addHierarchy"] = bool(add_hierarchy)
            if graph_build_cache_size is not None:
                metadata_cfg["graphBuildCacheSize"] = int(graph_build_cache_size)
            if mutations_before_rebuild is not None:
                metadata_cfg["mutationsBeforeRebuild"] = int(mutations_before_rebuild)

            if metadata_cfg:
                # Use JSON configuration to avoid JPype overload ambiguity on put()
                import json

                JSONObject = jpype.JPackage("com").arcadedb.serializer.json.JSONObject
                json_cfg = JSONObject(json.dumps(metadata_cfg))
                builder.withMetadata(json_cfg)

            # Create
            java_index = builder.create()

            from .vector import VectorIndex

            index = VectorIndex(java_index, self)
            if build_graph_now:
                index.build_graph_now()

            return index
        except Exception as e:
            raise ArcadeDBError(f"Failed to create vector index: {e}") from e

    def count_type(self, type_name: str) -> int:
        """
        Count records of a specific type.

        Args:
            type_name: Name of the type to count

        Returns:
            Number of records

        Example:
            >>> user_count = db.count_type("User")
            >>> print(f"Total users: {user_count}")
        """
        self._check_not_closed()
        try:
            return self._java_db.countType(type_name, True)  # polymorphic=True
        except Exception as e:
            # If type doesn't exist, return 0
            if "was not found" in str(e) or "SchemaException" in str(e):
                return 0
            raise ArcadeDBError(f"Failed to count type '{type_name}': {e}") from e

    def drop(self):
        """
        Drop the entire database.

        WARNING: This deletes all data permanently!

        Example:
            >>> db = arcade.open_database("./test_db")
            >>> db.drop()  # Database is deleted
        """
        self._check_not_closed()
        try:
            self._java_db.drop()
            self._closed = True
        except Exception as e:
            raise ArcadeDBError(f"Failed to drop database: {e}") from e

    def is_transaction_active(self) -> bool:
        """
        Check if a transaction is currently active.

        Returns:
            True if transaction is active, False otherwise

        Example:
            >>> with db.transaction():
            ...     print(db.is_transaction_active())  # True
            >>> print(db.is_transaction_active())  # False
        """
        self._check_not_closed()
        try:
            return self._java_db.isTransactionActive()
        except Exception as e:
            raise ArcadeDBError(f"Failed to check transaction status: {e}") from e

    def set_wal_flush(self, mode: str):
        """
        Configure the Write-Ahead Log (WAL) flush at commit for this database.

        The setting applies to the transactions of every thread that commits on
        this database (ArcadeData/arcadedb#8352, fixed in #8397 for 26.10.1; on
        engines before that fix it changed only the calling thread). It does not
        reach other databases in the process: for a default that covers every
        database, start the JVM with
        ``jvm_kwargs={"jvm_args": "-Darcadedb.txWalFlush=1"}``, or run the server
        with ``config={"mode": "production"}``, which sets it to 1.

        Args:
            mode: WAL flush mode, one of:
                - 'no': no flush at commit (the default); a commit survives a
                  process crash but not a power cut
                - 'yes_nometadata': flush the data at commit (fdatasync)
                - 'yes_full': flush data and metadata at commit (fsync)

        Raises:
            ValueError: If mode is not valid

        Example:
            >>> db.set_wal_flush('yes_nometadata')  # every commit survives a power cut
            >>> db.set_wal_flush('no')  # commits do not wait for the disk
        """
        self._check_not_closed()
        import jpype

        valid_modes = {
            "no": "NO",
            "yes_nometadata": "YES_NOMETADATA",
            "yes_full": "YES_FULL",
        }
        if mode not in valid_modes:
            raise ValueError(
                f"Invalid WAL flush mode: {mode}. "
                f"Must be one of: {list(valid_modes.keys())}"
            )

        try:
            WALFile = jpype.JPackage("com").arcadedb.engine.WALFile
            flush_type = getattr(WALFile.FlushType, valid_modes[mode])
            self._java_db.setWALFlush(flush_type)
        except Exception as e:
            raise ArcadeDBError(f"Failed to set WAL flush mode: {e}") from e

    def set_read_your_writes(self, enabled: bool):
        """
        Enable or disable read-your-writes consistency.

        When enabled, uncommitted changes in the current transaction are visible
        in subsequent reads. Disabling can improve concurrency but may show stale data.

        Args:
            enabled: True to enable read-your-writes, False to disable

        Example:
            >>> db.set_read_your_writes(True)  # Default behavior
            >>> db.set_read_your_writes(False)  # Better concurrency
        """
        self._check_not_closed()
        try:
            self._java_db.setReadYourWrites(enabled)
        except Exception as e:
            raise ArcadeDBError(f"Failed to set read-your-writes: {e}") from e

    def is_read_your_writes(self) -> bool:
        """Return whether read-your-writes consistency is currently enabled."""
        self._check_not_closed()
        try:
            return bool(self._java_db.isReadYourWrites())
        except Exception as e:
            raise ArcadeDBError(f"Failed to get read-your-writes: {e}") from e

    def set_auto_transaction(self, enabled: bool):
        """
        Enable or disable automatic transaction management.

        Off by default: a write outside a transaction raises ``ArcadeDBError``
        ("Transaction not begun"), so wrap writes in ``with db.transaction():``
        or call begin() yourself. When enabled, each statement outside a
        transaction runs in its own committed transaction. The setting is not
        persisted: a reopened database starts with it off again.

        Args:
            enabled: True to enable auto-transaction, False to disable

        Example:
            >>> db.set_auto_transaction(True)   # each bare write commits alone
            >>> db.command("sql", "INSERT INTO T SET a = 1")
            >>> db.set_auto_transaction(False)  # back to the default
        """
        self._check_not_closed()
        try:
            self._java_db.setAutoTransaction(enabled)
        except Exception as e:
            raise ArcadeDBError(f"Failed to set auto-transaction: {e}") from e

    def async_executor(self):
        """
        Get async executor for parallel operations.

        Returns the database's single async executor, which provides:
        - Parallel record creation
        - Automatic transaction batching
        - Optimized WAL configuration

        Note that this is one executor per database, not a new one per
        call, so ``close()`` on the returned object shuts it down for
        every other caller too.

        Not the recommended bulk-write path:
            ``AsyncExecutor.command`` silently discarded records above
            parallel level 1 before 26.10.1 (ArcadeData/arcadedb#7615, fixed
            in #7625; the measurement is in the ``async_executor`` module
            docstring). Bulk graph loads
            belong in ``graph_batch()``; bulk document loads belong in
            ``insert_many()`` or a batched transaction. ``create_record``,
            ``append_samples``, and ``insert_many(parallel=True)`` do run
            through this executor and are measured unaffected.

        Returns:
            AsyncExecutor instance configured for this database

        Example:
            >>> # create_record: the executor's own record path, unaffected
            >>> async_exec = db.async_executor()
            >>> async_exec.set_commit_every(5000)  # Auto-commit every 5K
            >>>
            >>> for i in range(100000):
            ...     vertex = db.new_vertex("User")
            ...     vertex.set("id", i)
            ...     async_exec.create_record(vertex)
            >>>
            >>> # Wait for completion
            >>> async_exec.wait_completion()
        """
        self._check_not_closed()
        from .async_executor import AsyncExecutor

        # JPype converts 'async' to 'async_' to avoid Python keyword collision
        executor = AsyncExecutor(self._java_db.async_(), owner=self)
        self._async_executors.append(executor)
        return executor

    def graph_batch(
        self,
        *,
        batch_size: Optional[int] = None,
        expected_edge_count: Optional[int] = None,
        edge_list_initial_size: Optional[int] = None,
        light_edges: Optional[bool] = None,
        bidirectional: Optional[bool] = None,
        commit_every: Optional[int] = None,
        use_wal: Optional[bool] = None,
        wal_flush: Optional[str] = None,
        pre_allocate_edge_chunks: Optional[bool] = None,
        parallel_flush: Optional[bool] = None,
        commit_retries: Optional[int] = None,
        commit_retry_delay_ms: Optional[int] = None,
        chunk_cache_capacity: Optional[int] = None,
        max_deferred_incoming_edges: Optional[int] = None,
    ) -> GraphBatch:
        """
        Create a GraphBatch helper for high-throughput graph ingestion.

        This wraps ArcadeDB's builder-backed batch graph API and is intended for
        workloads that need to create many vertices and buffered edges more efficiently
        than per-edge transactional writes.

        This is the recommended path for bulk graph loading, and the reason is
        not only throughput: the alternative of submitting per-record SQL
        through ``async_executor().command(...)`` lost records above parallel
        level 1 before 26.10.1 (ArcadeData/arcadedb#7615, fixed in #7625).
        ``graph_batch`` dispatches its edge
        flush through the same executor and is measured exact, 20,000 vertices
        and 40,000 edges with and without ``parallel_flush``.

        Args:
            batch_size: Maximum buffered edges before auto-flush.
            expected_edge_count: Hint for auto-tuning batch size when not set.
            edge_list_initial_size: Initial edge-segment size in bytes.
            light_edges: Create property-less edges as light edges when True. From engine
                26.11.1 ``new_edge`` raises ArcadeDBError for an edge type that is not declared
                LIGHTWEIGHT; declare the type or leave this unset. On 26.10.1 and earlier the load
                succeeds, but an openCypher one-hop ``count(*)`` over those edges answers 0
                (ArcadeData/arcadedb#9378, fixed in 26.11.1), and a graph loaded that way stays
                wrong.
            bidirectional: Connect incoming edges as well as outgoing edges (the
                default). Pass False only for an edge type declared UNIDIRECTIONAL. From
                26.10.1 a one-way edge in a two-way type is refused: ``new_edge`` raises
                ArcadeDBError naming the type. Before 26.10.1 it was accepted, and any
                query the planner walked from the target end returned 0 rows
                (ArcadeData/arcadedb#8625).
            commit_every: Commit cadence within a flush. `0` means one commit per flush.
            use_wal: Write-ahead log during the import. Off by default, so a crash mid-import can lose its
                tail; pass True for a crash-safe import (ArcadeData/arcadedb#8287).
            wal_flush: WAL flush mode: `"no"`, `"yes_nometadata"`, `"yes_full"`.
            pre_allocate_edge_chunks: Pre-allocate edge chunks during `create_vertex()`.
            parallel_flush: Parallelize flush/close connectivity work across buckets.
            commit_retries: Times a vertex-creation commit is retried on a transient
                `NeedRetryException` (for example a Raft `QuorumNotReachedException`
                during a leader re-election). Default 10; `0` fails fast on the first
                error.
            commit_retry_delay_ms: Initial back-off in milliseconds before the first
                vertex-commit retry. Later retries back off exponentially, capped at
                10000 ms. Default 1000.
            chunk_cache_capacity: Maximum entries retained in each of the OUT/IN
                head-chunk RID lookup caches. Both are pure accelerators, so a miss
                just reloads the vertex's head chunk from disk; bounding them keeps
                memory flat on a long-lived stream instead of growing with the number
                of distinct vertices touched. Default 1,000,000 (roughly 100-150 MB
                per cache).
            max_deferred_incoming_edges: Buffered deferred incoming edges allowed
                before the incoming-edge connection pass runs early from `flush()`
                rather than once at `close()`, amortizing it over the load. Default
                5,000,000; `0` defers everything to `close()` and lets the buffer grow
                unbounded.

        Returns:
            GraphBatch instance. Use it as a context manager when possible.

        Example:
            >>> with db.graph_batch(expected_edge_count=50000) as batch:
            ...     alice = batch.create_vertex("Person", name="Alice")
            ...     bob = batch.create_vertex("Person", name="Bob")
            ...     batch.new_edge(alice, "Knows", bob, since=2024)
        """
        self._check_not_closed()
        return GraphBatch.create(
            self._java_db,
            batch_size=batch_size,
            expected_edge_count=expected_edge_count,
            edge_list_initial_size=edge_list_initial_size,
            light_edges=light_edges,
            bidirectional=bidirectional,
            commit_every=commit_every,
            use_wal=use_wal,
            wal_flush=wal_flush,
            pre_allocate_edge_chunks=pre_allocate_edge_chunks,
            parallel_flush=parallel_flush,
            commit_retries=commit_retries,
            commit_retry_delay_ms=commit_retry_delay_ms,
            chunk_cache_capacity=chunk_cache_capacity,
            max_deferred_incoming_edges=max_deferred_incoming_edges,
        )

    def import_documents(
        self,
        source: str | PathLike[str],
        document_type: str = "Document",
        *,
        file_type: Optional[str] = None,
        delimiter: Optional[str] = None,
        header: Optional[str] = None,
        skip_entries: Optional[int] = None,
        properties_include: Optional[str] = None,
        commit_every: Optional[int] = None,
        parallel: Optional[int] = None,
        wal: Optional[bool] = None,
        verbose_level: Optional[int] = None,
        probe_only: Optional[bool] = None,
        force_database_create: Optional[bool] = None,
        trim_text: Optional[bool] = None,
        on_row_error: Optional[str] = None,
        extra_settings: Optional[Mapping[str, Any]] = None,
    ) -> ImportResult:
        """
        Import document-shaped data through ArcadeDB's Java importer framework.

        This is a narrow Python wrapper for bulk document import. It is intended for
        file-based loads such as CSV into a document type, not graph ingest.

        Args:
            source: Local filesystem path or importer URL.
            document_type: Target document type to import into.
            file_type: Optional importer file type override such as `"csv"`.
            delimiter: Optional delimiter override for delimited formats.
            header: Optional importer header configuration.
            skip_entries: Optional number of entries to skip before import.
            properties_include: Optional property include filter.
            commit_every: Optional importer transaction split interval.
            parallel: Optional importer parallel worker count.
            wal: Optional WAL override during import.
            verbose_level: Optional importer verbose logging level.
            probe_only: Analyze only without writing records when True.
            force_database_create: Recreate database when importer opens its own DB.
            trim_text: Trim textual values during import when True.
            on_row_error: `"abort"` (default) or `"skip"`. `"skip"` logs and
                skips a malformed or out-of-range row instead of failing the whole
                job, but it commits per row, so it needs exclusive control of the
                transaction and raises if one is already active. It also drops the
                async path for vertex imports, making them synchronous and
                single-threaded regardless of `commit_every`/`parallel`. Any other
                value raises `ValueError`, because the engine treats everything
                that is not `"skip"` as `"abort"`.
            extra_settings: Additional raw importer settings.

        Returns:
            ImportResult with normalized metadata and importer statistics.
        """
        self._check_not_closed()
        return run_document_import(
            self._java_db,
            source,
            document_type=document_type,
            file_type=file_type,
            delimiter=delimiter,
            header=header,
            skip_entries=skip_entries,
            properties_include=properties_include,
            commit_every=commit_every,
            parallel=parallel,
            wal=wal,
            verbose_level=verbose_level,
            probe_only=probe_only,
            force_database_create=force_database_create,
            trim_text=trim_text,
            on_row_error=on_row_error,
            extra_settings=extra_settings,
        )

    @property
    def schema(self):
        """
        Get the schema manipulation API for this database.

        The schema API provides type-safe access to schema operations:
        - Type management (document, vertex, edge types)
        - Property management (create, drop properties)
        - Index management (create, drop indexes)

        Returns:
            Schema instance for this database

        Example:
            >>> # Create a vertex type with properties
            >>> db.schema.create_vertex_type("User")
            >>> db.schema.create_property("User", "name", PropertyType.STRING)
            >>> db.schema.create_property("User", "age", PropertyType.INTEGER)
            >>>
            >>> # Create an index (HASH: "name" is only looked up by equality)
            >>> db.schema.create_index("User", ["name"], unique=True, index_type="HASH")
            >>>
            >>> # Create edge type
            >>> db.schema.create_edge_type("Follows")

        Note:
            Schema changes are immediately persisted and visible to all
            database connections. Schema modifications should be done
            carefully in production environments.
        """
        self._check_not_closed()
        from .schema import Schema

        return Schema(self._java_db.getSchema(), self)

    def export_database(
        self,
        file_path: str,
        format: str = "jsonl",
        overwrite: bool = False,
        include_types: Optional[List[str]] = None,
        exclude_types: Optional[List[str]] = None,
        verbose: int = 1,
    ) -> dict:
        """
        Export database to file.

        Writes JSONL, which ``IMPORT DATABASE file://...`` reads back. GraphML and GraphSON need the engine's optional arcadedb-gremlin
        module, which this package does not bundle, so they raise ArcadeDBError.

        Args:
            file_path: Output file path
            format: "jsonl" ("graphml" and "graphson" raise, see above)
            overwrite: Overwrite existing file if True
            include_types: List of types to export (None = all)
            exclude_types: List of types to exclude (None = none)
            verbose: Logging verbosity (0-2)

        Returns:
            Dictionary with export statistics (totalRecords, vertices, edges, etc.)

        Example:
            >>> # Export entire database to JSONL
            >>> stats = db.export_database("backup.jsonl.tgz", overwrite=True)
            >>> print(f"Exported {stats['totalRecords']} records")

            >>> # Export specific types only
            >>> db.export_database(
            ...     "movies.jsonl.tgz",
            ...     include_types=["Movie", "Rating"]
            ... )
        """
        self._check_not_closed()
        from .exporter import export_database

        return export_database(
            self, file_path, format, overwrite, include_types, exclude_types, verbose
        )

    def export_to_csv(
        self,
        query: str,
        file_path: str,
        language: str = "sql",
        fieldnames: Optional[List[str]] = None,
    ):
        """
        Export query results to CSV file.

        Convenience method that executes query and exports results.

        Args:
            query: SQL query to execute
            file_path: Output CSV file path
            language: Query language (default: "sql")
            fieldnames: Header and column order (auto-detected if None). It
                cannot rename: it must name every column the query returns, or
                the export raises after writing the header. Alias columns in
                the query to rename them.

        Example:
            >>> # Export all movies to CSV
            >>> db.export_to_csv("SELECT * FROM Movie", "movies.csv")

            >>> # Renamed columns, in a chosen order
            >>> db.export_to_csv(
            ...     "SELECT userId AS user, movieId AS movie, rating AS score "
            ...     "FROM Rating WHERE rating >= 4.5",
            ...     "high_ratings.csv",
            ...     fieldnames=["score", "user", "movie"]
            ... )
        """
        self._check_not_closed()
        results = self.query(language, query)
        from .exporter import export_to_csv

        export_to_csv(results, file_path, fieldnames)

    def _check_not_closed(self):
        """Check if database is still open."""
        if self._closed:
            raise ArcadeDBError("Database is closed")

    def _discard_async_executor(self, executor) -> None:
        try:
            self._async_executors.remove(executor)
        except ValueError:
            pass

    def _close_async_executors(self):
        first_error = None

        while self._async_executors:
            executor = self._async_executors.pop()
            try:
                executor.close()
            except Exception as e:
                if first_error is None:
                    first_error = ArcadeDBError(f"Failed to close async executor: {e}")

        return first_error

    def __del__(self):
        """Finalizer - ensure database is closed when object is garbage collected.

        Errors during garbage collection are intentionally suppressed: the
        interpreter is shutting down and logging may already be unavailable,
        so we narrow the catch to AttributeError/RuntimeError that JPype can
        raise when the JVM has been torn down before this finalizer runs.
        Server-managed databases raise UnsupportedOperationException on close,
        which close() handles itself and which is also suppressed here.
        """
        try:
            self.close()
        except (AttributeError, RuntimeError):
            # JVM or referenced attributes already gone; nothing to do.
            return
        except Exception:
            return


class DatabaseFactory:
    """Factory for creating/opening ArcadeDB databases."""

    def __init__(
        self,
        path: str,
        jvm_kwargs: Optional[dict] = None,
    ):
        """
        Args:
            path: Database path
            jvm_kwargs: Optional JVM args passed to start_jvm()
                Example: {"heap_size": "8g"}
        """
        start_jvm(**(jvm_kwargs or {}))
        import jpype

        JavaDatabaseFactory = jpype.JClass("com.arcadedb.database.DatabaseFactory")

        self._java_factory = JavaDatabaseFactory(path)

    def create(self) -> Database:
        """Create a new database."""
        try:
            java_db = self._java_factory.create()
            return Database(java_db)
        except Exception as e:
            raise ArcadeDBError(f"Failed to create database: {e}") from e

    def open(self) -> Database:
        """Open an existing database."""
        try:
            java_db = self._java_factory.open()
            return Database(java_db)
        except Exception as e:
            raise ArcadeDBError(f"Failed to open database: {e}") from e

    def exists(self) -> bool:
        """Check if database exists."""
        try:
            return self._java_factory.exists()
        except Exception as e:
            raise ArcadeDBError(f"Failed to check if database exists: {e}") from e


# Convenience functions
def create_database(
    path: str,
    jvm_kwargs: Optional[dict] = None,
) -> Database:
    """Create a new database at the given path.

    Args:
        path: Database path
        jvm_kwargs: Optional JVM args passed to start_jvm()
            Example: {"heap_size": "8g"}
    """
    factory = DatabaseFactory(
        path,
        jvm_kwargs=jvm_kwargs,
    )
    return factory.create()


def open_database(
    path: str,
    jvm_kwargs: Optional[dict] = None,
) -> Database:
    """Open an existing database at the given path.

    Args:
        path: Database path
        jvm_kwargs: Optional JVM args passed to start_jvm()
            Example: {"heap_size": "8g"}
    """
    factory = DatabaseFactory(
        path,
        jvm_kwargs=jvm_kwargs,
    )
    return factory.open()


def database_exists(path: str) -> bool:
    """Check if a database exists at the given path."""
    factory = DatabaseFactory(path)
    return factory.exists()
