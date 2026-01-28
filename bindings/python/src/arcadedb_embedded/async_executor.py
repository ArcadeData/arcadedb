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
    ...     vertex = db.new_vertex("User")
    ...     vertex.set("id", i)
    ...     async_exec.create_record(vertex)
    >>> async_exec.wait_completion()
"""

from typing import Any, Callable, Optional

import jpype

from .graph import Document


class AsyncExecutor:
    """
    Wrapper for Java DatabaseAsyncExecutor with Pythonic interface.

    Provides async record operations with automatic batching, parallel
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
        >>> # Create records asynchronously
        >>> for i in range(10000):
        ...     vertex = db.new_vertex("User")
        ...     vertex.set("id", i)
        ...     async_exec.create_record(vertex)
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

        # Map Python strings to Java WALFile.FlushType enum
        WALFile = jpype.JClass("com.arcadedb.log.WALFile")
        mode_map = {
            "no": WALFile.FLUSH_TYPE.NO,
            "yes_nometadata": WALFile.FLUSH_TYPE.YES_NOMETADATA,
            "yes_full": WALFile.FLUSH_TYPE.YES_FULL,
        }

        self._java_async.setTransactionSync(mode_map[sync_mode])
        return self

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

    # Record operations

    def create_record(
        self,
        record,
        callback: Optional[Callable[[Any], None]] = None,
        error_callback: Optional[Callable[[Exception], None]] = None,
    ):
        """
        Schedule async record creation.

        Args:
            record: MutableDocument or MutableVertex to create
                   (as returned by db.new_vertex() or db.new_document())
                   Can also be a Python wrapper (Document, Vertex, Edge)
            callback: Optional success callback, receives created record
            error_callback: Optional error callback, receives exception

        Example:
            >>> def on_success(record):
            ...     print(f"Created: {record}")
            >>>
            >>> vertex = db.new_vertex("User")
            >>> vertex.set("id", 123)
            >>> async_exec.create_record(vertex, on_success)
        """
        # Unwrap Python wrapper if provided
        if isinstance(record, Document):
            record = record._java_document

        # Java API requires callbacks, so create them even if not provided
        java_callback = self._create_new_record_callback(callback, error_callback)
        self._java_async.createRecord(record, java_callback)

    def update_record(
        self,
        record,
        callback: Optional[Callable[[Any], None]] = None,
    ):
        """
        Schedule async record update.

        Args:
            record: MutableDocument or MutableVertex to update
                   Can also be a Python wrapper (Document, Vertex, Edge)
            callback: Optional success callback, receives updated record

        Example:
            >>> vertex.set("updated", True)
            >>> async_exec.update_record(vertex)

        Note:
            Callbacks use UpdatedRecordCallback which receives the updated record.
            Per-operation callbacks work reliably.
        """
        # Unwrap Python wrapper if provided
        from .graph import Document

        if isinstance(record, Document):
            record = record._java_document

        if callback is None:
            # Use null callback
            self._java_async.updateRecord(record, None)
        else:
            # Create UpdatedRecordCallback for per-operation callback
            java_callback = self._create_updated_callback(callback)
            self._java_async.updateRecord(record, java_callback)

    def delete_record(
        self,
        record,
        callback: Optional[Callable[[], None]] = None,
    ):
        """
        Schedule async record deletion.

        Args:
            record: Document, Vertex, or Edge to delete
            callback: Optional success callback (no args)

        Example:
            >>> async_exec.delete_record(vertex)

        Note:
            Callbacks use DeletedRecordCallback which takes no arguments.
            Per-operation callbacks work reliably.
        """
        # Unwrap Python wrapper if provided
        from .graph import Document

        if isinstance(record, Document):
            record = record._java_document

        if callback is None:
            # Use null callback
            self._java_async.deleteRecord(record, None)
        else:
            # Create DeletedRecordCallback for per-operation callback
            java_callback = self._create_deleted_callback(callback)
            self._java_async.deleteRecord(record, java_callback)

    # Query operations

    def query(
        self,
        language: str,
        query_text: str,
        callback: Callable[[Any], None],
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
        java_callback = self._create_result_callback(callback)

        if params:
            # Convert params to Java map
            HashMap = jpype.JClass("java.util.HashMap")
            java_params = HashMap()
            for key, value in params.items():
                java_params.put(key, value)
            self._java_async.query(language, query_text, java_callback, java_params)
        else:
            self._java_async.query(language, query_text, java_callback)

    def command(
        self,
        language: str,
        command_text: str,
        callback: Optional[Callable[[Any], None]] = None,
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
        java_callback = self._create_result_callback(callback) if callback else None

        # Prepare parameters (empty map if none provided)
        HashMap = jpype.JClass("java.util.HashMap")
        java_params = HashMap()
        if params:
            for key, value in params.items():
                java_params.put(key, value)

        # Java method always requires callback and params (even if null/empty)
        self._java_async.command(language, command_text, java_callback, java_params)

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
        # Check if there are pending operations by trying to get the queue size
        # or by checking if wait would complete immediately
        try:
            # Try waiting with 0 timeout - if it returns True, nothing is pending
            return not self._java_async.waitCompletion(0)
        except Exception:
            # If method doesn't exist or fails, assume not pending
            return False

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
        try:
            self._java_async.close()
        except Exception:
            # Ignore errors during close
            pass

    # Global callbacks

    def on_ok(self, callback: Callable[[Any], None]) -> "AsyncExecutor":
        """
        Set global success callback for all operations.

        **Note:** Global callbacks have JPype proxy compatibility issues.
        Use per-operation callbacks instead:

            async_exec.create_record(vertex, callback=on_success)

        Args:
            callback: Success callback, receives result

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

    def _create_new_record_callback(self, ok_callback, error_callback):
        """Create Java NewRecordCallback from Python functions."""
        NewRecordCallback = jpype.JClass(
            "com.arcadedb.database.async.NewRecordCallback"
        )

        # Capture callbacks in the closure
        python_ok_callback = ok_callback
        python_error_callback = error_callback

        # Define the callback implementation
        @jpype.JImplements(NewRecordCallback)
        class PythonNewRecordCallback:
            @jpype.JOverride
            def call(self, record):
                if python_ok_callback:
                    try:
                        # Pass the Java record directly - no wrapping needed
                        # The user can call .get(), .set(), etc. on it
                        python_ok_callback(record)
                    except Exception as e:
                        if python_error_callback:
                            try:
                                python_error_callback(e)
                            except:
                                pass  # Ignore errors in error callback
                        # Don't re-raise - would crash the async thread

        return PythonNewRecordCallback()

    def _create_ok_callback(self, python_callback):
        """Create Java OkCallback from Python function."""
        OkCallback = jpype.JClass("com.arcadedb.database.async.OkCallback")

        # Capture callback in closure
        callback = python_callback

        @jpype.JImplements(OkCallback)
        class PythonOkCallback:
            @jpype.JOverride
            def call(self, result):
                if callback:
                    try:
                        callback(result)
                    except Exception:
                        # Silently ignore to avoid crashing Java thread
                        pass

        return PythonOkCallback()

    def _create_deleted_callback(self, python_callback):
        """Create Java DeletedRecordCallback from Python function."""
        DeletedRecordCallback = jpype.JClass(
            "com.arcadedb.database.async.DeletedRecordCallback"
        )

        # Capture callback in closure
        callback = python_callback

        @jpype.JImplements(DeletedRecordCallback)
        class PythonDeletedRecordCallback:
            @jpype.JOverride
            def call(self):
                if callback:
                    try:
                        callback()
                    except Exception:
                        # Silently ignore to avoid crashing Java thread
                        pass

        return PythonDeletedRecordCallback()

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
                    try:
                        callback(edge, created_source_vertex, created_dest_vertex)
                    except Exception:
                        # Silently ignore to avoid crashing Java thread
                        pass

        return PythonNewEdgeCallback()

    def _create_updated_callback(self, python_callback):
        """Create Java UpdatedRecordCallback from Python function."""
        UpdatedRecordCallback = jpype.JClass(
            "com.arcadedb.database.async.UpdatedRecordCallback"
        )

        # Capture callback in closure
        callback = python_callback

        @jpype.JImplements(UpdatedRecordCallback)
        class PythonUpdatedRecordCallback:
            @jpype.JOverride
            def call(self, record):
                if callback:
                    try:
                        callback(record)
                    except Exception:
                        # Silently ignore to avoid crashing Java thread
                        pass

        return PythonUpdatedRecordCallback()

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

    def _create_result_callback(self, python_callback):
        """Create Java ResultCallback from Python function."""
        ResultCallback = jpype.JClass("com.arcadedb.database.async.ResultCallback")

        @jpype.JImplements(ResultCallback)
        class PythonResultCallback:
            @jpype.JOverride
            def call(self, result):
                if python_callback:
                    from .results import Result

                    python_result = Result(result)
                    python_callback(python_result)

        return PythonResultCallback()
