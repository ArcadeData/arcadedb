"""
ArcadeDB Python Bindings - GraphBatch wrapper

Pythonic wrapper for ArcadeDB's high-throughput graph ingest API.
"""

from typing import Any, Iterable, Optional

import jpype

from ._logging import get_logger, log_swallowed_exception
from .exceptions import ArcadeDBError
from .graph import Document, Vertex
from .type_conversion import convert_python_to_java, json_bulk_scalar_ok

_LOGGER = get_logger(__name__)


class GraphBatch:
    """Wrapper for Java GraphBatch with builder-backed configuration.

    create_vertices(), flush() and close(), and new_edge()/new_edges() when the
    buffer fills, manage their own transactions. From engine 26.10.1 they refuse
    to run inside a transaction you opened (an ArcadeDBError) and leave it
    untouched; on 26.9.1 and earlier they commit it (ArcadeData/arcadedb#9242).
    Call them outside your own transactions either way; see
    docs/api/graph_batch.md.
    """

    _VALID_WAL_FLUSH_MODES = {
        "no": "NO",
        "yes_nometadata": "YES_NOMETADATA",
        "yes_full": "YES_FULL",
    }

    def __init__(
        self,
        java_database,
        java_graph_batch,
    ):
        self._java_db = java_database
        self._java_graph_batch = java_graph_batch
        self._closed = False

    @classmethod
    def create(
        cls,
        java_database,
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
    ) -> "GraphBatch":
        """Build a GraphBatch from the Java builder API."""
        try:
            builder = java_database.batch()

            if batch_size is not None:
                builder = builder.withBatchSize(batch_size)
            if expected_edge_count is not None:
                builder = builder.withExpectedEdgeCount(expected_edge_count)
            if edge_list_initial_size is not None:
                builder = builder.withEdgeListInitialSize(edge_list_initial_size)
            if light_edges is not None:
                builder = builder.withLightEdges(light_edges)
            if bidirectional is not None:
                builder = builder.withBidirectional(bidirectional)
            if commit_every is not None:
                builder = builder.withCommitEvery(commit_every)
            if use_wal is not None:
                builder = builder.withWAL(use_wal)
            if wal_flush is not None:
                builder = builder.withWALFlush(cls._to_java_wal_flush(wal_flush))
            if pre_allocate_edge_chunks is not None:
                builder = builder.withPreAllocateEdgeChunks(pre_allocate_edge_chunks)
            if parallel_flush is not None:
                builder = builder.withParallelFlush(parallel_flush)
            if commit_retries is not None:
                builder = builder.withCommitRetries(commit_retries)
            if commit_retry_delay_ms is not None:
                # Java takes a long here; JPype picks the overload from the
                # Python int, but be explicit so a large delay cannot land on
                # an int-typed candidate.
                builder = builder.withCommitRetryDelay(
                    jpype.JLong(commit_retry_delay_ms)
                )
            if chunk_cache_capacity is not None:
                builder = builder.withChunkCacheCapacity(chunk_cache_capacity)
            if max_deferred_incoming_edges is not None:
                builder = builder.withMaxDeferredIncomingEdges(
                    max_deferred_incoming_edges
                )

            return cls(java_database, builder.build())
        except ValueError:
            raise
        except Exception as e:
            raise ArcadeDBError(f"Failed to create GraphBatch: {e}") from e

    def __enter__(self) -> "GraphBatch":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def new_vertex(self, type_name: str) -> Vertex:
        """Create an unsaved vertex through the batch API."""
        self._check_not_closed()
        try:
            return Vertex(self._java_graph_batch.newVertex(type_name))
        except Exception as e:
            raise ArcadeDBError(
                f"Failed to create new batch vertex of type '{type_name}': {e}"
            ) from e

    def create_vertex(self, type_name: str, **properties) -> Vertex:
        """Create and save a single vertex, optionally with properties."""
        self._check_not_closed()
        started_transaction = False
        try:
            if not self._java_db.isTransactionActive():
                # Flag first: an interrupt right after begin() must still roll
                # back; the handler re-checks isTransactionActive() anyway.
                started_transaction = True
                self._java_db.begin()

            if properties:
                java_vertex = self._java_graph_batch.createVertex(
                    type_name, *self._to_java_varargs(properties)
                )
            else:
                java_vertex = self._java_graph_batch.createVertex(type_name)

            if started_transaction:
                self._java_db.commit()

            return Vertex(java_vertex)
        except BaseException as e:
            # BaseException, not Exception: a KeyboardInterrupt/SystemExit
            # between the begin() above and the commit() must not leak the
            # transaction this method started (#7882). Only ordinary failures
            # are translated to ArcadeDBError; the rest propagate unchanged.
            if started_transaction:
                try:
                    if self._java_db.isTransactionActive():
                        self._java_db.rollback()
                except Exception:
                    log_swallowed_exception(_LOGGER, "during batch vertex rollback")
            if not isinstance(e, Exception):
                raise
            raise ArcadeDBError(
                f"Failed to create batch vertex of type '{type_name}': {e}"
            ) from e

    def create_vertices(
        self,
        type_name: str,
        count_or_properties: int | Iterable[Optional[dict[str, Any]]],
    ) -> list[str]:
        """
        Create multiple vertices efficiently and return their RIDs.

        Call it outside your own transactions: from engine 26.10.1 it raises
        ArcadeDBError inside one and leaves it untouched; on 26.9.1 and earlier
        it commits the transaction open on this thread, yours included
        (ArcadeData/arcadedb#9242).

        Args:
            type_name: Vertex type name.
            count_or_properties: Either an integer vertex count or an iterable of
                per-vertex property dictionaries. Use `None` or `{}` for vertices
                without properties when passing an iterable.
        """
        self._check_not_closed()
        # From engine 26.10.1 the engine refuses to run inside a transaction the
        # caller opened and leaves it untouched, so the rollback below is only
        # ever for a transaction this call began. On 26.9.1 and earlier it
        # joined the caller's transaction, committed it with the vertices, and
        # on a retryable error rolled it back before retrying in one of its own
        # (ArcadeData/arcadedb#9242).
        owns_transaction = not self._java_db.isTransactionActive()
        try:
            if isinstance(count_or_properties, int):
                java_rids = self._java_graph_batch.createVertices(
                    type_name, count_or_properties
                )
                return [str(rid) for rid in java_rids]

            rows = list(count_or_properties)
            rids = self._create_vertices_json_bulk(type_name, rows)
            if rids is not None:
                return rids

            java_rids = self._java_graph_batch.createVertices(
                type_name,
                self._to_java_property_matrix(rows),
            )
            return [str(rid) for rid in java_rids]
        except BaseException as e:
            # As in create_vertex: a failure other than a retryable one (a
            # duplicate key, a KeyboardInterrupt) leaves the engine's
            # transaction open, so a later write outside any transaction is
            # accepted and lost at close (#121).
            if owns_transaction:
                try:
                    if self._java_db.isTransactionActive():
                        self._java_db.rollback()
                except Exception:
                    log_swallowed_exception(_LOGGER, "during batch vertices rollback")
            if not isinstance(e, Exception):
                raise
            raise ArcadeDBError(
                f"Failed to create batch vertices for type '{type_name}': {e}"
            ) from e

    _BULK_CHUNK = 100_000  # rows per boundary crossing in bulk paths

    def _create_vertices_json_bulk(self, type_name, rows):
        """Bulk path: rows cross as one JSON string, RIDs return as one joined
        string (two bulk copies instead of per-value marshaling — measured
        ~2.4x). Returns None when unavailable/unsuitable so the caller falls
        back to the property-matrix path."""
        try:
            vertex_batcher = jpype.JClass("com.arcadedb.python.VertexBatcher")
        except Exception:
            return None

        for row in rows:
            if row:
                for value in row.values():
                    if not json_bulk_scalar_ok(value):
                        # e.g. datetime/bytes (the matrix path preserves types), or a
                        # value JSON would change: an integer beyond 64 bits, NaN or
                        # Infinity, a lone surrogate
                        return None

        import json

        # chunked so huge ingests never materialize one giant JSON string
        rids = []
        for start in range(0, len(rows), self._BULK_CHUNK):
            chunk = rows[start : start + self._BULK_CHUNK]
            joined = str(
                vertex_batcher.createVerticesJson(
                    self._java_graph_batch,
                    type_name,
                    json.dumps([row or {} for row in chunk]),
                )
            )
            if joined:
                rids.extend(joined.split(";"))
        return rids

    def new_edge(
        self,
        source_vertex_or_rid,
        edge_type: str,
        destination_vertex_or_rid,
        **properties,
    ) -> "GraphBatch":
        """Buffer an edge for creation during flush or close."""
        self._check_not_closed()
        try:
            source_rid = self._to_java_rid(source_vertex_or_rid)
            destination_rid = self._to_java_rid(destination_vertex_or_rid)
            if properties:
                self._java_graph_batch.newEdge(
                    source_rid,
                    edge_type,
                    destination_rid,
                    *self._to_java_varargs(properties),
                )
            else:
                self._java_graph_batch.newEdge(source_rid, edge_type, destination_rid)
            return self
        except Exception as e:
            raise ArcadeDBError(
                f"Failed to buffer batch edge '{edge_type}': {e}"
            ) from e

    def new_edges(
        self,
        source_rids,
        edge_type: str,
        destination_rids,
        properties=None,
    ) -> "GraphBatch":
        """
        Buffer many edges with one JPype crossing per call.

        The bulk counterpart of :meth:`new_edge` (which costs one boundary
        crossing per edge — measured ~24x slower than Java-native for large
        edge lists). RIDs may be strings ("#1:0") or objects with a string
        representation.

        Args:
            source_rids: Sequence of source vertex RIDs.
            edge_type: Edge type name.
            destination_rids: Sequence of destination RIDs (same length).
            properties: Optional sequence of per-edge property dicts (same
                length). JSON-representable values take the bulk path;
                anything else falls back to per-edge buffering.
        """
        self._check_not_closed()
        from .results import _bridge_class

        edge_batcher = _bridge_class("EdgeBatcher")
        if edge_batcher is None:
            # bridge jar unavailable (warned once): per-edge fallback,
            # documented ~24x slower than the bulk path
            props_iter = properties or [None] * len(source_rids)
            for src, dst, p in zip(source_rids, destination_rids, props_iter):
                self.new_edge(src, edge_type, dst, **(p or {}))
            return self

        try:
            if properties is None:
                # One joined string crosses the boundary in a single bulk
                # copy; a Python list of N strings would be converted
                # element-by-element by JPype and eat the batching win.
                # Chunked so huge ingests never build one giant string.
                sources = [str(r) for r in source_rids]
                dests = [str(r) for r in destination_rids]
                for start in range(0, len(sources), self._BULK_CHUNK):
                    edge_batcher.newEdgesJoined(
                        self._java_graph_batch,
                        ";".join(sources[start : start + self._BULK_CHUNK]),
                        edge_type,
                        ";".join(dests[start : start + self._BULK_CHUNK]),
                    )
                return self

            rows = []
            for src, dst, p in zip(source_rids, destination_rids, properties):
                row = {"_src": str(src), "_dst": str(dst)}
                for key, value in (p or {}).items():
                    if key in ("_src", "_dst") or not json_bulk_scalar_ok(value):
                        # non-JSON value (datetime/bytes), or one JSON would change
                        # (an integer beyond 64 bits, NaN or Infinity, a lone
                        # surrogate): per-edge fallback preserves types exactly
                        props_iter = properties
                        for s2, d2, p2 in zip(
                            source_rids, destination_rids, props_iter
                        ):
                            self.new_edge(s2, edge_type, d2, **(p2 or {}))
                        return self
                    row[key] = value
                rows.append(row)

            import json

            for start in range(0, len(rows), self._BULK_CHUNK):
                edge_batcher.newEdgesJson(
                    self._java_graph_batch,
                    edge_type,
                    json.dumps(rows[start : start + self._BULK_CHUNK]),
                )
            return self
        except Exception as e:
            raise ArcadeDBError(
                f"Failed to buffer batch edges '{edge_type}': {e}"
            ) from e

    def flush(self) -> "GraphBatch":
        """Flush buffered edges to disk.

        Call it outside your own transactions: from engine 26.10.1 it raises
        ArcadeDBError inside one when edges are buffered, leaving the
        transaction and the buffer as they were, and so do new_edge() and
        new_edges() when the buffer fills and flushes. On 26.9.1 and earlier
        they commit the transaction open on this thread, yours included
        (ArcadeData/arcadedb#9242).
        """
        self._check_not_closed()
        try:
            self._java_graph_batch.flush()
            return self
        except Exception as e:
            raise ArcadeDBError(f"Failed to flush GraphBatch: {e}") from e

    def close(self):
        """Flush remaining work and finalize deferred incoming edges.

        Leaving a ``with`` block calls this. Call it outside your own
        transactions: from engine 26.10.1, inside one with work still pending,
        it raises ArcadeDBError and the batch stays open, so a later close()
        after your transaction ends writes that work. On 26.9.1 and earlier it
        commits the transaction open on this thread, yours included
        (ArcadeData/arcadedb#9242).
        """
        if self._closed:
            return
        refused = False
        try:
            self._java_graph_batch.close()
        except Exception as e:
            # A close the engine refused because of the caller's transaction
            # released nothing: the batch is still open with its work pending,
            # and marking the wrapper closed would make the retry a no-op that
            # drops that work and keeps the database's batch guard held.
            refused = self._is_callers_transaction_refusal(e)
            raise ArcadeDBError(f"Failed to close GraphBatch: {e}") from e
        finally:
            if not refused:
                self._closed = True

    def _is_callers_transaction_refusal(self, error: Exception) -> bool:
        """True when the engine refused a call because the caller has a
        transaction open (IllegalStateException, ArcadeData/arcadedb#9242)."""
        return isinstance(
            error, jpype.JClass("java.lang.IllegalStateException")
        ) and bool(self._java_db.isTransactionActive())

    def get_total_edges_created(self) -> int:
        self._check_not_closed()
        return int(self._java_graph_batch.getTotalEdgesCreated())

    def get_buffered_edge_count(self) -> int:
        self._check_not_closed()
        return int(self._java_graph_batch.getBufferedEdgeCount())

    def get_deferred_incoming_edge_count(self) -> int:
        self._check_not_closed()
        return int(self._java_graph_batch.getDeferredIncomingEdgeCount())

    def _check_not_closed(self):
        if self._closed:
            raise ArcadeDBError("GraphBatch is closed")

    @classmethod
    def _to_java_wal_flush(cls, mode: str):
        normalized_mode = mode.lower()
        if normalized_mode not in cls._VALID_WAL_FLUSH_MODES:
            raise ValueError(
                f"Invalid wal_flush mode: {mode}. Must be one of: "
                f"{list(cls._VALID_WAL_FLUSH_MODES.keys())}"
            )

        FlushType = jpype.JClass("com.arcadedb.engine.WALFile$FlushType")
        return getattr(FlushType, cls._VALID_WAL_FLUSH_MODES[normalized_mode])

    def _unwrap_record(self, record):
        java_document = getattr(record, "_java_document", None)
        return java_document if java_document is not None else record

    def _to_java_varargs(self, properties: dict[str, Any]) -> list[Any]:
        varargs = []
        for key, value in properties.items():
            varargs.append(key)
            varargs.append(convert_python_to_java(value))
        return varargs

    def _to_java_property_matrix(
        self, properties_iterable: Iterable[Optional[dict[str, Any]]]
    ):
        rows = []
        for properties in properties_iterable:
            if properties is None:
                rows.append(None)
                continue
            if not isinstance(properties, dict):
                raise TypeError("create_vertices() property rows must be dicts or None")
            row = self._to_java_varargs(properties)
            rows.append(jpype.JArray(jpype.JObject)(row))

        return jpype.JArray(jpype.JObject[:])(rows)

    def _to_java_rid(self, value):
        value = self._unwrap_record(value)
        if hasattr(value, "getIdentity"):
            return value.getIdentity()
        if isinstance(value, str):
            RID = jpype.JClass("com.arcadedb.database.RID")
            return RID(value)
        if isinstance(value, Document):
            return value.get_identity()
        return value
