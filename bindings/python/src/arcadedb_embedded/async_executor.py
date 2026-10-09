"""
Async API wrapper for ArcadeDB's DatabaseAsyncExecutor.

This module provides a Pythonic interface to ArcadeDB's async execution
capabilities: parallel processing, automatic transaction batching, and
optimized WAL settings.

Known defect -- do not use :meth:`AsyncExecutor.command` for bulk writes
=======================================================================
At a parallel level above 1, SQL commands submitted through this executor
were silently discarded before 26.10.1: ArcadeData/arcadedb#7615, fixed in
#7625 (a failed periodic commit is now retried and otherwise reported
through the command's error callback). Observed on
arcadedb-engine 26.9.1 and 26.6.1, measured 2026-09-15. How much was lost
varied by run and by workload shape: 9,742 single-record ``INSERT``
commands at parallel level 4 stored 2,436, 5,742, and 7,742 rows across
runs. No error reached the per-command callback, nothing was logged, and
``wait_completion()`` returned normally. Only the executor-wide
:meth:`AsyncExecutor.on_error` handler saw anything, one
``ConcurrentModificationException`` per rolled-back batch.

Use instead:

- ``Database.graph_batch(...)`` for bulk graph loading.
- ``Database.insert_many(...)`` or a plain batched transaction for
  documents.

Measured unaffected, so these stay usable as they are: ``create_record``,
``append_samples``, ``insert_many(parallel=True)`` (which routes through
``createRecord``, not through SQL), and the edge flush inside
``graph_batch``. ``command`` at parallel level 1 is also exact.

Example:
    >>> # documents: one FFI crossing per batch, every row lands
    >>> db.insert_many("User", [{"id": i} for i in range(100000)])
    100000
"""

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Callable, Optional, Sequence, Union

import jpype

from ._logging import get_logger, log_swallowed_exception
from .type_conversion import convert_python_to_java

if TYPE_CHECKING:
    from .core import Database

_LOGGER = get_logger(__name__)


def _convert_column(values):
    """Convert one non-numeric column, reusing the Java object per distinct str.

    TIMESERIES tag columns are strings, so they miss the numeric buffer path
    and convert one element at a time. Tags are low cardinality by definition,
    which is the premise the engine's own TAG dictionary rests on (#5574), so
    almost every one of those conversions repeats work already done: in TSBS
    `cpu` the ten tag columns hold 233 distinct values across 2,592,000 rows,
    hostname alone repeating each of 100 values 25,920 times, and `arch`
    repeating each of 2 values 1,296,000 times.

    Memoising per distinct value turns 25.9M conversions into 233. Restricted
    to `str` on purpose: a Java String is immutable, so sharing one reference
    across rows is safe, whereas memoising a converted list or map would alias
    a mutable object across rows and let a later write show up in earlier ones.
    Everything else keeps converting per element exactly as before.
    """
    out = []
    seen = {}
    for value in values:
        if type(value) is str:
            java = seen.get(value)
            if java is None:
                java = convert_python_to_java(value)
                seen[value] = java
            out.append(java)
        else:
            out.append(convert_python_to_java(value))
    return out


class AsyncExecutor:
    """
    Wrapper for Java DatabaseAsyncExecutor with Pythonic interface.

    Provides async command/query execution with automatic batching, parallel
    execution, and WAL optimization. All configuration methods return
    self for method chaining.

    Thread Safety:
        The underlying Java executor is thread-safe. Python callbacks
        are executed in Java worker threads, so they must be thread-safe.

    Bulk writes:
        Do not drive bulk ingest through :meth:`command`. Above parallel
        level 1 it silently dropped records before 26.10.1
        (ArcadeData/arcadedb#7615, fixed in #7625; see the module docstring
        for the measurement and the safe paths).

    Example:
        >>> # a one-off async command, not a bulk load
        >>> async_exec = db.async_executor()
        >>> async_exec.command("sql", "DELETE FROM User WHERE id = ?", args=[7])
        >>> async_exec.wait_completion()
    """

    def __init__(self, java_async_executor, owner: Optional["Database"] = None):
        """
        Initialize AsyncExecutor wrapper.

        Args:
            java_async_executor: Java DatabaseAsyncExecutor instance
        """
        self._java_async = java_async_executor
        self._owner = owner
        self._closed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def is_closed(self) -> bool:
        """Return True once the executor has been closed."""
        return self._closed

    # Configuration methods (fluent interface)

    def set_parallel_level(self, level: int) -> "AsyncExecutor":
        """
        Set number of parallel worker threads.

        Each worker owns a share of a type's buckets, so a type loaded in
        parallel wants as many buckets as there are workers, or a multiple
        (ArcadeData/arcadedb#8478).

        Args:
            level: Number of threads, at least 1. The engine's default
                   (``arcadedb.asyncWorkerThreads``) is the number of cores
                   minus 1, and half the cores minus 1 under the
                   ``high-performance`` profile.

        Returns:
            self for method chaining

        Raises:
            ValueError: If level < 1

        Example:
            >>> async_exec.set_parallel_level(8)  # Use 8 worker threads
        """
        if level < 1:
            raise ValueError("parallel_level must be at least 1")
        self._java_async.setParallelLevel(level)
        return self

    def set_commit_every(self, count: int) -> "AsyncExecutor":
        """
        Auto-commit every N operations.

        Args:
            count: Commit frequency (operations per commit). Must be >= 1;
                   the engine rejects lower values (0 used to be silently
                   accepted but broke async task execution, engine #4961).
                   Recommended: 1000-10000 for bulk inserts.

        Returns:
            self for method chaining

        Example:
            >>> async_exec.set_commit_every(5000)  # Commit every 5K ops
        """
        # engine #4961: commitEvery < 1 breaks async task execution
        if count < 1:
            raise ValueError(
                f"commit_every must be >= 1 (got {count}); the async "
                "executor always commits in batches"
            )
        self._java_async.setCommitEvery(count)
        return self

    def set_transaction_use_wal(self, use_wal: bool) -> "AsyncExecutor":
        """
        Enable/disable Write-Ahead Log for async operations.

        Disabling WAL is faster but less durable (data loss on crash).
        Only disable for bulk imports where data can be re-imported.

        Args:
            use_wal: True to use WAL (default), False to disable

        Returns:
            self for method chaining

        Example:
            >>> # Disable WAL for faster bulk import (less durable)
            >>> async_exec.set_transaction_use_wal(False)
        """
        self._java_async.setTransactionUseWAL(use_wal)
        return self

    def set_transaction_sync(self, sync_mode: str) -> "AsyncExecutor":
        """
        Set WAL flush strategy for durability vs. performance trade-off.

        The async writers stamp this setting on every transaction they open,
        whatever ``arcadedb.txWalFlush`` says for the database, and it
        defaults to "no". A bulk load through the executor (including
        ``insert_many(..., parallel=True)``) that must be as durable as the
        rest of your writes sets it explicitly, e.g. "yes_full" to match
        ``txWalFlush=2`` (ArcadeData/arcadedb#8478).

        Args:
            sync_mode: One of:
                - "no" - No fsync (fastest, least durable)
                - "yes_nometadata" - Sync data but not metadata
                - "yes_full" - Full fsync (slowest, most durable)

        Returns:
            self for method chaining

        Raises:
            ValueError: If sync_mode is invalid

        Example:
            >>> # Use no-sync for maximum performance
            >>> async_exec.set_transaction_sync("no")
        """
        valid_modes = {"no", "yes_nometadata", "yes_full"}
        if sync_mode not in valid_modes:
            raise ValueError(f"sync_mode must be one of {valid_modes}, got {sync_mode}")

        FlushType = jpype.JClass("com.arcadedb.engine.WALFile$FlushType")
        mode_map = {
            "no": FlushType.NO,
            "yes_nometadata": FlushType.YES_NOMETADATA,
            "yes_full": FlushType.YES_FULL,
        }

        self._java_async.setTransactionSync(mode_map[sync_mode])
        return self

    def get_parallel_level(self) -> int:
        return int(self._java_async.getParallelLevel())

    def get_back_pressure(self) -> int:
        return int(self._java_async.getBackPressure())

    def get_commit_every(self) -> int:
        return int(self._java_async.getCommitEvery())

    def is_transaction_use_wal(self) -> bool:
        return bool(self._java_async.isTransactionUseWAL())

    def get_transaction_sync(self) -> str:
        name = str(self._java_async.getTransactionSync().name())
        mapping = {
            "NO": "no",
            "YES_NOMETADATA": "yes_nometadata",
            "YES_FULL": "yes_full",
        }
        return mapping.get(name, name.lower())

    def get_thread_count(self) -> int:
        return int(self._java_async.getThreadCount())

    def is_processing(self) -> bool:
        try:
            return bool(self._java_async.isProcessing())
        except Exception:
            log_swallowed_exception(_LOGGER, "while polling isProcessing()")
            return False

    def kill(self):
        self._java_async.kill()

    def set_back_pressure(self, percentage: int) -> "AsyncExecutor":
        """
        Set queue back-pressure threshold (0-100%).

        When queue fills to this percentage, async operations will block
        until queue drains. Prevents unbounded memory growth.

        Args:
            percentage: Threshold (0-100). The engine's default
                       (``arcadedb.asyncBackPressure``) is 0, no back-pressure.
                       Higher = more buffering, more memory.
                       Lower = less buffering, more blocking.

        Returns:
            self for method chaining

        Raises:
            ValueError: If percentage not in 0-100

        Example:
            >>> async_exec.set_back_pressure(75)  # Block at 75% full
        """
        if percentage < 0 or percentage > 100:
            raise ValueError("back_pressure must be between 0 and 100")
        self._java_async.setBackPressure(percentage)
        return self

    # Graph and time-series operations

    def create_record(
        self,
        document,
        callback: Optional[Callable] = None,
        error_callback: Optional[Callable[[Exception], None]] = None,
    ):
        """Queue a document for asynchronous creation.

        The engine's parallel bucket writers persist it off the calling
        thread; call :meth:`wait_completion` before relying on visibility.
        This is the idiomatic bulk-write path for single documents built
        with ``Database.new_document``; for many uniform rows prefer
        ``Database.insert_many(..., parallel=True)``, which crosses the
        FFI boundary once per batch instead of per document.

        Args:
            document: A ``Document`` (or ``Vertex``/``Edge``) created by
                ``Database.new_document``/``new_vertex`` (not yet saved).
            callback: Optional callable invoked with the record once the writer
                has created it in its transaction, before that batch commits: a
                record the commit then rejects has been through ``callback`` too.
            error_callback: Optional callable invoked with the exception when the
                writers reject this record (a duplicate key under a UNIQUE index, or
                its batch abandoned at a failed commit). Without it, the failure
                reaches only the executor-wide :meth:`on_error` handler, if any.
        """
        java_cb = (
            self._create_new_record_callback(callback) if callback is not None else None
        )
        if error_callback is None:
            self._java_async.createRecord(document._java_document, java_cb)
        else:
            self._java_async.createRecord(
                document._java_document,
                java_cb,
                self._create_error_callback(error_callback),
            )

    def _create_new_record_callback(self, python_callback):
        from .graph import Document

        @jpype.JImplements("com.arcadedb.database.async.NewRecordCallback")
        class _NewRecordCallback:
            @jpype.JOverride
            def call(self, record):
                python_callback(Document.wrap(record))

        return _NewRecordCallback()

    def new_edge(
        self,
        source_vertex,
        edge_type: str,
        destination_vertex_or_rid,
        light: bool = False,
        callback: Optional[Callable[[Any, bool, bool], None]] = None,
        **properties,
    ):
        source_vertex = self._unwrap_record(source_vertex)
        destination_rid = self._to_java_rid(destination_vertex_or_rid)
        java_callback = self._create_new_edge_callback(callback) if callback else None
        props = self._to_java_varargs(properties)
        self._java_async.newEdge(
            source_vertex,
            edge_type,
            destination_rid,
            light,
            java_callback,
            *props,
        )

    def new_edge_by_keys(
        self,
        source_vertex_type: str,
        source_key_names: Union[str, Sequence[str]],
        source_key_values: Union[Any, Sequence[Any]],
        destination_vertex_type: str,
        destination_key_names: Union[str, Sequence[str]],
        destination_key_values: Union[Any, Sequence[Any]],
        create_vertex_if_not_exist: bool,
        edge_type: str,
        bidirectional: bool,
        light: bool,
        callback: Optional[Callable[[Any, bool, bool], None]] = None,
        **properties,
    ):
        java_callback = self._create_new_edge_callback(callback) if callback else None
        props = self._to_java_varargs(properties)

        if isinstance(source_key_names, str) and isinstance(destination_key_names, str):
            source_value = (
                source_key_values[0]
                if isinstance(source_key_values, (list, tuple))
                else source_key_values
            )
            destination_value = (
                destination_key_values[0]
                if isinstance(destination_key_values, (list, tuple))
                else destination_key_values
            )
            self._java_async.newEdgeByKeys(
                source_vertex_type,
                source_key_names,
                convert_python_to_java(source_value),
                destination_vertex_type,
                destination_key_names,
                convert_python_to_java(destination_value),
                create_vertex_if_not_exist,
                edge_type,
                bidirectional,
                light,
                java_callback,
                *props,
            )
            return

        source_names = list(source_key_names)
        source_values = list(source_key_values)
        destination_names = list(destination_key_names)
        destination_values = list(destination_key_values)

        if len(source_names) != len(source_values):
            raise ValueError(
                "source_key_names and source_key_values must have same size"
            )
        if len(destination_names) != len(destination_values):
            raise ValueError(
                "destination_key_names and destination_key_values must have same size"
            )

        JStringArray = jpype.JArray(jpype.JString)
        JObjectArray = jpype.JArray(jpype.JObject)

        self._java_async.newEdgeByKeys(
            source_vertex_type,
            JStringArray(source_names),
            JObjectArray([convert_python_to_java(v) for v in source_values]),
            destination_vertex_type,
            JStringArray(destination_names),
            JObjectArray([convert_python_to_java(v) for v in destination_values]),
            create_vertex_if_not_exist,
            edge_type,
            bidirectional,
            light,
            java_callback,
            *props,
        )

    def append_samples(
        self,
        type_name: str,
        timestamps: Sequence[int],
        *column_values: Sequence[Any],
        primitive: bool = False,
    ):
        """Columnar bulk append into a native TIMESERIES type.

        Columns are positional and must follow the type's declaration order:
        tags first, then fields. Timestamps are epoch values in the type's
        precision (milliseconds by default).

        numpy fast path: an ndarray for timestamps or a numeric field column
        crosses the FFI as one buffer copy (int/uint kinds via boxLongs,
        float kinds via boxDoubles); other sequences convert per element. A
        numpy bool array is a 0/1 numeric column; a Python list of bools is
        not (the engine refuses a Boolean for a numeric field).
        Call wait_completion() before relying on visibility.

        primitive=True routes through the engine's TimeSeriesBatch instead,
        which carries each column as a primitive array and so never boxes a
        numeric sample (ArcadeDB #5474). It needs an engine that ships that
        API; the default stays on the Object[] path.
        """
        if primitive:
            return self._append_samples_primitive(type_name, timestamps, *column_values)
        try:
            import numpy as _np
        except ImportError:
            _np = None
        JLongArray = jpype.JArray(jpype.JLong)
        if _np is not None and isinstance(timestamps, _np.ndarray):
            # buffer-protocol bulk copy: one FFI crossing for the column
            timestamps_java = JLongArray(
                _np.ascontiguousarray(timestamps, dtype=_np.int64)
            )
        else:
            timestamps_java = JLongArray([int(value) for value in timestamps])
        JObjectArray = jpype.JArray(jpype.JObject)
        boxer = None
        columns_java = []
        for values in column_values:
            if (
                _np is not None
                and isinstance(values, _np.ndarray)
                and values.dtype.kind in "fiub"
            ):
                if boxer is None:
                    boxer = jpype.JClass("com.arcadedb.python.DocumentBatcher")
                if values.dtype.kind == "f":
                    col = boxer.boxDoubles(
                        jpype.JArray(jpype.JDouble)(
                            _np.ascontiguousarray(values, dtype=_np.float64)
                        )
                    )
                else:
                    col = boxer.boxLongs(
                        jpype.JArray(jpype.JLong)(
                            _np.ascontiguousarray(values, dtype=_np.int64)
                        )
                    )
                columns_java.append(col)
            else:
                columns_java.append(JObjectArray(_convert_column(values)))
        self._java_async.appendSamples(type_name, timestamps_java, *columns_java)

    def _append_samples_primitive(
        self,
        type_name: str,
        timestamps: Sequence[int],
        *column_values: Sequence[Any],
    ):
        """append_samples over the engine's primitive TimeSeriesBatch.

        Each column crosses the FFI once as a contiguous primitive array and is
        written into the batch Java-side. Filling the batch from Python instead
        would cost one JNI call per value, which is worse than the boxing this
        replaces, so the per-row loop lives in TimeSeriesBatcher.
        """
        try:
            import numpy as _np
        except ImportError:
            _np = None

        if self._owner is None:
            raise RuntimeError(
                "primitive append_samples needs the owning Database; "
                "obtain the executor via Database.async_executor()"
            )

        batcher = jpype.JClass("com.arcadedb.python.TimeSeriesBatcher")
        JLongArray = jpype.JArray(jpype.JLong)
        if _np is not None and isinstance(timestamps, _np.ndarray):
            timestamps_java = JLongArray(
                _np.ascontiguousarray(timestamps, dtype=_np.int64)
            )
        else:
            timestamps_java = JLongArray([int(value) for value in timestamps])

        # A column shorter than the timestamps was padded by the engine with
        # defaults and one longer was cut, with no error.
        for index, values in enumerate(column_values):
            if len(values) != len(timestamps_java):
                raise ValueError(
                    f"column {index} has {len(values)} values for "
                    f"{len(timestamps_java)} timestamps"
                )

        batch = batcher.newBatch(self._owner._java_db, type_name, timestamps_java)

        for index, values in enumerate(column_values):
            if (
                _np is not None
                and isinstance(values, _np.ndarray)
                and values.dtype.kind == "f"
            ):
                batcher.setDoubleColumn(
                    batch,
                    index,
                    jpype.JArray(jpype.JDouble)(
                        _np.ascontiguousarray(values, dtype=_np.float64)
                    ),
                )
            elif (
                _np is not None
                and isinstance(values, _np.ndarray)
                and values.dtype.kind in "iub"
            ):
                # "b": a numpy bool array is a 0/1 numeric column here. It used
                # to reach a numeric field as 1.0 and 0.0 only because JPype
                # read each numpy bool as a number; this keeps that outcome now
                # that a numpy bool converts to a boolean everywhere else.
                batcher.setLongColumn(
                    batch,
                    index,
                    jpype.JArray(jpype.JLong)(
                        _np.ascontiguousarray(values, dtype=_np.int64)
                    ),
                )
            elif len(values) > 0 and all(isinstance(v, str) for v in values):
                batcher.setStringColumn(
                    batch, index, jpype.JArray(jpype.JString)(list(values))
                )
            elif len(values) > 0 and all(isinstance(v, float) for v in values):
                batcher.setDoubleColumn(
                    batch,
                    index,
                    jpype.JArray(jpype.JDouble)([float(v) for v in values]),
                )
            elif len(values) > 0 and all(
                isinstance(v, int) and not isinstance(v, bool) for v in values
            ):
                batcher.setLongColumn(
                    batch, index, jpype.JArray(jpype.JLong)([int(v) for v in values])
                )
            else:
                batcher.setObjectColumn(
                    batch,
                    index,
                    jpype.JArray(jpype.JObject)(
                        [convert_python_to_java(v) for v in values]
                    ),
                )

        self._java_async.appendSamples(type_name, batch)

    # Query operations

    def query(
        self,
        language: str,
        query_text: str,
        callback: Callable[[Any], None],
        args: Optional[Sequence[Any]] = None,
        error_callback: Optional[Callable[[Exception], None]] = None,
        **params,
    ):
        """
        Execute async query with callback for each result.

        Args:
            language: Query language ("sql", "opencypher", etc.)
            query_text: Query string
            callback: Result callback, receives each ResultSet row
            **params: Query parameters

        Example:
            >>> def process_row(row):
            ...     print(row.get("name"))
            >>>
            >>> async_exec.query("sql", "SELECT FROM User", process_row)
            >>> async_exec.wait_completion()
        """
        positional_args = tuple(args or ())

        if positional_args and params:
            raise ValueError("Use either positional args or named params, not both")

        java_callback = self._create_result_callback(callback, error_callback)

        if params:
            self._java_async.query(
                language,
                query_text,
                java_callback,
                self._to_java_map(params),
            )
        elif positional_args:
            self._java_async.query(
                language,
                query_text,
                java_callback,
                self._positional_parameters(positional_args),
            )
        else:
            self._java_async.query(language, query_text, java_callback)

    def command(
        self,
        language: str,
        command_text: str,
        callback: Optional[Callable[[Any], None]] = None,
        args: Optional[Sequence[Any]] = None,
        error_callback: Optional[Callable[[Exception], None]] = None,
        **params,
    ):
        """
        Execute async command with optional callback.

        Args:
            language: Command language ("sql", "opencypher", etc.)
            command_text: Command string
            callback: Optional result callback
            **params: Command parameters

        Not a bulk-write path:
            Before 26.10.1, above parallel level 1 the engine silently
            discarded a share of the commands submitted here
            (ArcadeData/arcadedb#7615, fixed in #7625; the module docstring
            carries the measurement). Nothing was raised, nothing was logged,
            and ``wait_completion()`` returned normally, so a short load
            looked like a fast one. For bulk ingest use
            ``Database.graph_batch(...)`` for graphs and
            ``Database.insert_many(...)`` or a batched transaction for
            documents.

        Performance note:
            Submitting commands without callbacks runs at Java-native speed
            (~8us/op measured). A Python ``callback`` costs ~100us per
            operation, because every completion crosses Java->Python through
            a JPype proxy under the GIL and throttles the executor's
            parallelism. For bulk ingest, submit without callbacks and use
            ``wait_completion()`` (or one callback on the final command)
            instead of per-record callbacks.

        Example:
            >>> async_exec.command("sql", "DELETE FROM User WHERE id = ?",
            ...                    id=123)
            >>> async_exec.wait_completion()
        """
        positional_args = tuple(args or ())

        if positional_args and params:
            raise ValueError("Use either positional args or named params, not both")

        java_callback = (
            self._create_result_callback(callback, error_callback)
            if (callback or error_callback)
            else None
        )

        if params:
            self._java_async.command(
                language,
                command_text,
                java_callback,
                self._to_java_map(params),
            )
        elif positional_args:
            self._java_async.command(
                language,
                command_text,
                java_callback,
                self._positional_parameters(positional_args),
            )
        else:
            self._java_async.command(language, command_text, java_callback)

    def scan_type(
        self,
        type_name: str,
        callback: Callable[[Any], bool],
        polymorphic: bool = True,
        error_callback: Optional[Callable[[Any, Exception], bool]] = None,
    ):
        java_doc_callback = self._create_document_callback(callback)
        if error_callback is None:
            self._java_async.scanType(type_name, polymorphic, java_doc_callback)
        else:
            java_error_callback = self._create_error_record_callback(error_callback)
            self._java_async.scanType(
                type_name,
                polymorphic,
                java_doc_callback,
                java_error_callback,
            )

    def transaction(
        self,
        tx_block: Callable[[], None],
        retries: Optional[int] = None,
        ok_callback: Optional[Callable[[], None]] = None,
        error_callback: Optional[Callable[[Exception], None]] = None,
        slot: Optional[int] = None,
    ):
        java_tx = self._create_transaction_scope(tx_block)

        if (
            retries is None
            and ok_callback is None
            and error_callback is None
            and slot is None
        ):
            self._java_async.transaction(java_tx)
            return

        retries_value = retries if retries is not None else 1
        java_ok = self._create_ok_callback(ok_callback) if ok_callback else None
        java_error = (
            self._create_error_callback(error_callback) if error_callback else None
        )

        if slot is None and java_ok is None and java_error is None:
            self._java_async.transaction(java_tx, retries_value)
        elif slot is None:
            self._java_async.transaction(java_tx, retries_value, java_ok, java_error)
        else:
            self._java_async.transaction(
                java_tx,
                retries_value,
                java_ok,
                java_error,
                slot,
            )

    # Control flow

    def wait_completion(self, timeout_ms: Optional[int] = None):
        """
        Wait for all async operations to complete.

        Blocks until all pending operations finish. If timeout is reached,
        raises TimeoutError.

        Args:
            timeout_ms: Optional timeout in milliseconds.
                       None = wait indefinitely (default)
                       0 = do not wait: return if everything is done,
                       otherwise raise TimeoutError at once
                       negative = rejected with ValueError

        The engine's waitCompletion() clamps any timeout <= 0 to an infinite
        wait, so 0 is never handed to it: it is answered off isProcessing(),
        the same non-blocking poll is_pending() uses. Like is_pending(), it is
        a point-in-time snapshot, not the barrier a positive timeout or no
        argument gives: work a running task schedules after the snapshot is
        not covered by a successful poll.

        Raises:
            TimeoutError: If timeout is reached before completion
            ValueError: If timeout_ms is negative

        Example:
            >>> async_exec.wait_completion()  # Wait forever
            >>> async_exec.wait_completion(30000)  # Wait max 30 seconds
            >>> async_exec.wait_completion(0)  # Poll: raise if not done yet
        """
        if timeout_ms is None:
            self._java_async.waitCompletion()
        elif timeout_ms < 0:
            raise ValueError(f"timeout_ms must be None or >= 0, got {timeout_ms}")
        elif timeout_ms == 0:
            # Not is_processing(): that swallows engine errors as "idle", which
            # here would report completion that never happened.
            if self._java_async.isProcessing():
                raise TimeoutError("Async operations have not completed (0ms poll)")
        else:
            success = self._java_async.waitCompletion(timeout_ms)
            if not success:
                raise TimeoutError(
                    f"Async operations did not complete within {timeout_ms}ms"
                )

    def is_pending(self) -> bool:
        """
        Check if any operations are pending.

        This is a non-blocking poll: it must never wait for the queue to drain.
        A timeout of 0 passed to the engine's waitCompletion() is clamped to an
        infinite wait rather than treated as "poll", so this delegates to
        isProcessing() instead.

        Returns:
            True if operations are still running, False if all complete

        Example:
            >>> if async_exec.is_pending():
            ...     print("Still processing...")
        """
        return self.is_processing()

    def close(self):
        """
        Close the async executor and shutdown worker threads.

        This method should be called when done with the async executor
        to ensure proper cleanup of background threads.

        Example:
            >>> async_exec = db.async_executor()
            >>> # ... do work ...
            >>> async_exec.wait_completion()
            >>> async_exec.close()  # Shutdown threads
        """
        if self._closed:
            return

        try:
            self._java_async.close()
        finally:
            self._closed = True

            if self._owner is not None:
                try:
                    self._owner._discard_async_executor(self)
                finally:
                    self._owner = None

    # Global callbacks

    def on_ok(self, callback: Callable[[], None]) -> "AsyncExecutor":
        """
        Set global success callback for all operations.

        **Note:** Global callbacks have JPype proxy compatibility issues.
        Prefer per-operation callbacks on async SQL/Cypher commands:

            async_exec.command(
                "sql", "INSERT INTO Log SET id = :id", callback=on_success, id=1
            )

        Args:
            callback: Success callback, no args

        Returns:
            self for method chaining
        """
        java_callback = self._create_ok_callback(callback)
        self._java_async.onOk(java_callback)
        return self

    def on_error(self, callback: Callable[[Exception], None]) -> "AsyncExecutor":
        """
        Set the executor-wide error callback.

        It receives the failure of a record operation (``create_record``, for
        example), including one that also has a per-record callback, and a
        batch-level failure such as a failed batch commit. It does not receive
        a ``command()`` or ``query()`` statement's own failure: that goes only
        to the statement's ``error_callback``, and is not raised anywhere when
        there is none.

        Before 26.10.1 it was also the only place the ArcadeData/arcadedb#7615
        record loss became visible: when the executor rolled back a batch, the
        per-command callbacks reported nothing, but this handler received one
        ``ConcurrentModificationException`` per rolled-back batch (fixed in
        #7625: a failed periodic commit is now retried and otherwise reported
        through the command's error callback). Attach it before any load you
        cannot afford to lose silently.

        Args:
            callback: Error callback, receives exception

        Returns:
            self for method chaining

        Example:
            >>> async_exec.on_error(lambda e: print(f"Error: {e}"))
        """
        java_callback = self._create_error_callback(callback)
        self._java_async.onError(java_callback)
        return self

    # Internal callback bridge helpers

    def _create_ok_callback(self, python_callback):
        """Create Java OkCallback from Python function."""
        OkCallback = jpype.JClass("com.arcadedb.database.async.OkCallback")

        # Capture callback in closure
        callback = python_callback

        @jpype.JImplements(OkCallback)
        class PythonOkCallback:
            @jpype.JOverride
            def call(self):
                if callback:
                    callback()

        return PythonOkCallback()

    def _create_new_edge_callback(self, python_callback):
        """Create Java NewEdgeCallback from Python function.

        The callback receives three parameters:
        - edge: The created edge
        - created_source_vertex: True if source vertex was created
        - created_dest_vertex: True if destination vertex was created
        """
        NewEdgeCallback = jpype.JClass("com.arcadedb.database.async.NewEdgeCallback")

        # Capture callback in closure
        callback = python_callback

        @jpype.JImplements(NewEdgeCallback)
        class PythonNewEdgeCallback:
            @jpype.JOverride
            def call(
                self,
                edge,
                created_source_vertex,
                created_dest_vertex,
            ):
                if callback:
                    callback(edge, created_source_vertex, created_dest_vertex)

        return PythonNewEdgeCallback()

    def _create_error_callback(self, python_callback):
        """Create Java ErrorCallback from Python function."""
        ErrorCallback = jpype.JClass("com.arcadedb.database.async.ErrorCallback")

        @jpype.JImplements(ErrorCallback)
        class PythonErrorCallback:
            @jpype.JOverride
            def call(self, exception):
                if python_callback:
                    python_callback(exception)

        return PythonErrorCallback()

    def _create_result_callback(self, python_callback, error_callback=None):
        """Create Java AsyncResultsetCallback from Python functions."""
        AsyncResultsetCallback = jpype.JClass(
            "com.arcadedb.database.async.AsyncResultsetCallback"
        )

        @jpype.JImplements(AsyncResultsetCallback)
        class PythonResultCallback:
            @jpype.JOverride
            def onComplete(self, resultset):
                if not python_callback:
                    return
                from .results import Result

                while resultset.hasNext():
                    python_callback(Result(resultset.next()))

            @jpype.JOverride
            def onError(self, exception):
                if error_callback:
                    error_callback(exception)

        return PythonResultCallback()

    def _create_document_callback(self, python_callback):
        DocumentCallback = jpype.JClass("com.arcadedb.database.DocumentCallback")

        @jpype.JImplements(DocumentCallback)
        class PythonDocumentCallback:
            @jpype.JOverride
            def onRecord(self, record):
                if python_callback is None:
                    return True
                result = python_callback(record)
                return True if result is None else bool(result)

        return PythonDocumentCallback()

    def _create_error_record_callback(self, python_callback):
        ErrorRecordCallback = jpype.JClass("com.arcadedb.engine.ErrorRecordCallback")

        @jpype.JImplements(ErrorRecordCallback)
        class PythonErrorRecordCallback:
            @jpype.JOverride
            def onErrorLoading(self, rid, exception):
                if python_callback is None:
                    return True
                result = python_callback(rid, exception)
                return True if result is None else bool(result)

        return PythonErrorRecordCallback()

    def _create_transaction_scope(self, python_callback):
        TransactionScope = jpype.JClass(
            "com.arcadedb.database.BasicDatabase$TransactionScope"
        )

        @jpype.JImplements(TransactionScope)
        class PythonTransactionScope:
            @jpype.JOverride
            def execute(self):
                python_callback()

        return PythonTransactionScope()

    def _unwrap_record(self, record):
        java_document = getattr(record, "_java_document", None)
        return java_document if java_document is not None else record

    def _positional_parameters(self, values):
        """The one typed Java argument that carries ``args`` (#172).

        An ``Object[]`` with one element per ``?``. Splatted, a lone ``None``
        reached the engine as a null in place of the whole parameter array, so
        nothing was bound. A lone mapping stays the named map it was before,
        as in ``Database.query()``.
        """
        if len(values) == 1 and isinstance(values[0], Mapping):
            return self._to_java_map(values[0])
        return jpype.JArray(jpype.JObject)(
            [convert_python_to_java(value) for value in values]
        )

    def _to_java_map(self, params):
        HashMap = jpype.JClass("java.util.HashMap")
        java_params = HashMap()
        for key, value in params.items():
            java_params.put(key, convert_python_to_java(value))
        # Typed as the interface, so the Map overload is an exact match rather
        # than one JPype picks over Object... (#172).
        return jpype.JObject(java_params, jpype.JClass("java.util.Map"))

    def _to_java_varargs(self, properties):
        varargs = []
        for key, value in properties.items():
            varargs.append(key)
            varargs.append(convert_python_to_java(value))
        return varargs

    def _to_java_rid(self, value):
        value = self._unwrap_record(value)
        if hasattr(value, "getIdentity"):
            return value.getIdentity()
        if isinstance(value, str):
            RID = jpype.JClass("com.arcadedb.database.RID")
            return RID(value)
        return value
