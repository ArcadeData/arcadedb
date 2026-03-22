"""
Async API wrapper for ArcadeDB's DatabaseAsyncExecutor.

This module provides a Pythonic interface to ArcadeDB's powerful async
execution capabilities, enabling parallel processing, automatic batching,
and optimized WAL operations.

Key Benefits:
- 3-5x faster bulk inserts via parallel execution
- Automatic transaction batching (commitEvery parameter)
- Constant memory usage (vs. growing with transaction size)
- 50,000-200,000 records/sec throughput (vs. 15,000-30,000 sequential)

Example:
    >>> async_exec = db.async_executor()
    >>> async_exec.set_parallel_level(8).set_commit_every(5000)
    >>> for i in range(100000):
    ...     async_exec.command(
    ...         "sql",
    ...         "INSERT INTO User SET id = :id",
    ...         id=i,
    ...     )
    >>> async_exec.wait_completion()
"""

from typing import Any, Callable, Optional, Sequence, Union

import jpype

from .type_conversion import convert_python_to_java


class AsyncExecutor:
    """
    Wrapper for Java DatabaseAsyncExecutor with Pythonic interface.

    Provides async command/query execution with automatic batching, parallel
    execution, and WAL optimization. All configuration methods return
    self for method chaining.

    Thread Safety:
        The underlying Java executor is thread-safe. Python callbacks
        are executed in Java worker threads, so they must be thread-safe.

    Example:
        >>> # Configure executor
        >>> async_exec = db.async_executor()
        >>> async_exec.set_parallel_level(4)  # 4 worker threads
        >>> async_exec.set_commit_every(1000)  # Auto-commit every 1K ops
        >>>
        >>> # Execute SQL asynchronously
        >>> for i in range(10000):
        ...     async_exec.command(
        ...         "sql",
        ...         "INSERT INTO User SET id = :id",
        ...         id=i,
        ...     )
        >>>
        >>> # Wait for completion
        >>> async_exec.wait_completion()
    """

    def __init__(self, java_async_executor):
        """
        Initialize AsyncExecutor wrapper.

        Args:
            java_async_executor: Java DatabaseAsyncExecutor instance
        """
        self._java_async = java_async_executor

    # Configuration methods (fluent interface)

    def set_parallel_level(self, level: int) -> "AsyncExecutor":
        """
        Set number of parallel worker threads.

        Args:
            level: Number of threads (1-16). Higher = more parallelism
                   but more memory/CPU usage. Default is CPU cores.

        Returns:
            self for method chaining

        Raises:
            ValueError: If level < 1 or level > 16

        Example:
            >>> async_exec.set_parallel_level(8)  # Use 8 worker threads
        """
        if level < 1 or level > 16:
            raise ValueError("parallel_level must be between 1 and 16")
        self._java_async.setParallelLevel(level)
        return self

    def set_commit_every(self, count: int) -> "AsyncExecutor":
        """
        Auto-commit every N operations.

        Args:
            count: Commit frequency (operations per commit).
                   0 = manual commits only (default).
                   Recommended: 1000-10000 for bulk inserts.

        Returns:
            self for method chaining

        Example:
            >>> async_exec.set_commit_every(5000)  # Commit every 5K ops
        """
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
            if bool(self._java_async.isProcessing()):
                return True
        except Exception:
            pass

        try:
            return not bool(self._java_async.waitCompletion(0))
        except Exception:
            return False

    def kill(self):
        self._java_async.kill()

    def set_back_pressure(self, percentage: int) -> "AsyncExecutor":
        """
        Set queue back-pressure threshold (0-100%).

        When queue fills to this percentage, async operations will block
        until queue drains. Prevents unbounded memory growth.

        Args:
            percentage: Threshold (0-100). Default is 50%.
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
    ):
        JLongArray = jpype.JArray(jpype.JLong)
        timestamps_java = JLongArray([int(value) for value in timestamps])
        JObjectArray = jpype.JArray(jpype.JObject)
        columns_java = [
            JObjectArray([convert_python_to_java(value) for value in values])
            for values in column_values
        ]
        self._java_async.appendSamples(type_name, timestamps_java, *columns_java)

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
            language: Query language ("sql", "cypher", etc.)
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
                *[convert_python_to_java(arg) for arg in positional_args],
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
            language: Command language ("sql", "cypher", etc.)
            command_text: Command string
            callback: Optional result callback
            **params: Command parameters

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
                *[convert_python_to_java(arg) for arg in positional_args],
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

        Raises:
            TimeoutError: If timeout is reached before completion

        Example:
            >>> async_exec.wait_completion()  # Wait forever
            >>> async_exec.wait_completion(30000)  # Wait max 30 seconds
        """
        if timeout_ms is None:
            self._java_async.waitCompletion()
        else:
            success = self._java_async.waitCompletion(timeout_ms)
            if not success:
                raise TimeoutError(
                    f"Async operations did not complete within {timeout_ms}ms"
                )

    def is_pending(self) -> bool:
        """
        Check if any operations are pending.

        Returns:
            True if operations are still running, False if all complete

        Example:
            >>> if async_exec.is_pending():
            ...     print("Still processing...")
        """
        try:
            return not bool(self._java_async.waitCompletion(0))
        except Exception:
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
        self._java_async.close()

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
        Set global error callback for all operations.

        This callback is called for every failed operation if no
        per-operation error callback was provided.

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

    def _to_java_map(self, params):
        HashMap = jpype.JClass("java.util.HashMap")
        java_params = HashMap()
        for key, value in params.items():
            java_params.put(key, convert_python_to_java(value))
        return java_params

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
