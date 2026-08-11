/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.remote;

import com.arcadedb.database.RID;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Client-side batch graph importer that buffers vertices and edges as JSONL,
 * sending them to the server's /batch endpoint in chunks controlled by {@code flushEvery}.
 * <p>
 * Each {@link #createVertex} returns an auto-generated temporary ID that can be
 * passed to {@link #createEdge}. When a flush sends vertices to the server, the
 * returned id mapping is stored in flat primitive arrays (12 bytes per vertex).
 * Subsequent edges that reference already-flushed vertices are resolved client-side
 * to real RIDs, so they work correctly across flush boundaries.
 * <p>
 * Usage:
 * <pre>
 * try (RemoteGraphBatch batch = remoteDb.batch()
 *     .withBatchSize(100_000)
 *     .withLightEdges(true)
 *     .withFlushEvery(50_000)
 *     .build()) {
 *
 *   String alice = batch.createVertex("Person", "name", "Alice", "age", 30);
 *   String bob   = batch.createVertex("Person", "name", "Bob", "age", 25);
 *   batch.createEdge("KNOWS", alice, bob, "since", 2020);
 * }
 * // batch.getResult().getVerticesCreated() == 2
 * </pre>
 * <p>
 * This class is the JSONL-over-HTTP loader. A connection that speaks another protocol returns its own subclass
 * from {@code batch()} and carries the load over that protocol instead: {@code RemoteGrpcDatabase} returns a
 * loader backed by the {@code GraphBatchLoad} streaming RPC (issue #6070). The public API above is the same for
 * either, and so is the meaning of every {@link Builder} option; what differs is that a transport holding one
 * session for the whole load has the server resolve temporary ids, where this one resolves them client-side
 * because each flush is an independent request the server does not remember.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RemoteGraphBatch implements AutoCloseable {

  static final int DEFAULT_FLUSH_EVERY  = 50_000;
  private static final int DEFAULT_BUFFER_SIZE = 8192;
  private static final int INITIAL_MAPPING_CAPACITY = 1024;

  private final   RemoteDatabase      database;
  private final   Map<String, String> queryParams;
  private final   int                 flushEvery;
  private final   StringBuilder       buffer;
  protected       int                 vertexCounter;
  /** Position of the first vertex of the buffer being filled, i.e. what the server has to number this payload from. */
  private         int                 bufferOrdinalBase;
  private         int                 itemsInBuffer;
  protected       boolean             hasEdges;
  protected       boolean             closed;

  // --- Aggregated result across all flushes ---
  protected long totalVerticesCreated;
  protected long totalEdgesCreated;
  protected long totalElapsedMs;

  // --- Resolved temp ID mapping: flat arrays indexed by vertex counter ---
  // resolvedBucketIds[i] and resolvedPositions[i] hold the real RID for vertex "v<i>"
  private int[]  resolvedBucketIds;
  private long[] resolvedPositions;
  private int    resolvedCount; // number of vertices whose RIDs have been resolved

  RemoteGraphBatch(final RemoteDatabase database, final Map<String, String> queryParams, final int flushEvery) {
    this.database = database;
    this.queryParams = queryParams;
    // Edges buffered in a later flush reference vertices created by an earlier one, so this client cannot work
    // without the mapping and asks for it explicitly: the endpoint stops echoing it on its own past a size no
    // client could consume in one response (issue #5470).
    this.queryParams.put("idMapping", "true");
    // The temporary ids generated below are already the position of the vertex in the load, so the server is told to
    // treat them as such: it then keeps two primitive arrays instead of a map of ids, 12 bytes per vertex instead of
    // ~87, and resolves an edge with an array read (issue #5470). Each flush declares where its own numbering starts,
    // because this counter spans all of them while the server numbers one request at a time.
    this.queryParams.put("refMode", "ordinal");
    this.flushEvery = flushEvery;
    this.buffer = new StringBuilder(DEFAULT_BUFFER_SIZE);
    this.resolvedBucketIds = new int[INITIAL_MAPPING_CAPACITY];
    this.resolvedPositions = new long[INITIAL_MAPPING_CAPACITY];
  }

  /**
   * Constructor for a subclass that carries the load over a different transport. None of the state this class
   * keeps for the JSONL-over-HTTP path is allocated: the payload buffer, the flush accounting and the
   * client-side temporary-id mapping all exist because each flush is an independent stateless request, and a
   * transport that holds one session for the whole load has the server resolve references instead. A subclass
   * using it must override {@link #createVertex}, {@link #createEdge} and {@link #flush}, which would otherwise
   * dereference the buffer this constructor leaves null.
   */
  protected RemoteGraphBatch(final RemoteDatabase database) {
    this.database = database;
    this.queryParams = null;
    this.flushEvery = Integer.MAX_VALUE;
    this.buffer = null;
    this.resolvedBucketIds = null;
    this.resolvedPositions = null;
  }

  /**
   * Buffers a vertex for batch creation. Returns an auto-generated temporary ID
   * that can be passed to {@link #createEdge} as the from/to reference.
   * All vertices must be added before any edges.
   *
   * @param typeName   vertex type name (must exist in schema)
   * @param properties optional key-value pairs (e.g. "name", "Alice", "age", 30)
   * @return temporary ID string for referencing this vertex in edges
   */
  public String createVertex(final String typeName, final Object... properties) {
    checkOpen();
    if (hasEdges)
      throw new IllegalStateException("Cannot add vertices after edges have been added");

    final String tempId = "v" + vertexCounter++;

    buffer.append("{\"@type\":\"v\",\"@class\":");
    appendJsonString(buffer, typeName);
    buffer.append(",\"@id\":");
    appendJsonString(buffer, tempId);
    appendProperties(buffer, properties);
    buffer.append("}\n");

    if (++itemsInBuffer >= flushEvery)
      flush();

    return tempId;
  }

  /**
   * Buffers an edge for batch creation. The from/to parameters can be:
   * <ul>
   *   <li>Temporary IDs returned by {@link #createVertex}</li>
   *   <li>Existing database RID strings (e.g. "#3:42")</li>
   * </ul>
   *
   * @param edgeTypeName edge type name (must exist in schema)
   * @param from         source vertex reference (temp ID or RID string)
   * @param to           destination vertex reference (temp ID or RID string)
   * @param properties   optional key-value pairs
   */
  public void createEdge(final String edgeTypeName, final String from, final String to, final Object... properties) {
    checkOpen();
    hasEdges = true;

    final String resolvedFrom = resolveRef(from);
    final String resolvedTo = resolveRef(to);

    buffer.append("{\"@type\":\"e\",\"@class\":");
    appendJsonString(buffer, edgeTypeName);
    buffer.append(",\"@from\":");
    appendJsonString(buffer, resolvedFrom);
    buffer.append(",\"@to\":");
    appendJsonString(buffer, resolvedTo);
    appendProperties(buffer, properties);
    buffer.append("}\n");

    if (++itemsInBuffer >= flushEvery)
      flush();
  }

  /**
   * Buffers an edge using RID objects for source and destination.
   */
  public void createEdge(final String edgeTypeName, final RID from, final RID to, final Object... properties) {
    createEdge(edgeTypeName, from.toString(), to.toString(), properties);
  }

  /**
   * Sends the current buffer to the server and resets it. The id mapping from
   * any flushed vertices is stored for resolving future edge references.
   * This method can be called explicitly; it is also called automatically
   * when the buffer reaches the {@code flushEvery} threshold.
   */
  public void flush() {
    if (buffer.isEmpty())
      return;

    queryParams.put("ordinalBase", Integer.toString(bufferOrdinalBase));

    final JSONObject response = database.sendBatch(buffer.toString(), queryParams);

    totalVerticesCreated += response.getLong("verticesCreated");
    totalEdgesCreated += response.getLong("edgesCreated");
    totalElapsedMs += response.getLong("elapsedMs");

    if (response.getBoolean("idMappingOmitted", false))
      // Never resolve edges against a mapping that is not there: it would silently drop every cross-flush edge.
      throw new IllegalStateException(
          "The server did not return the temporary-id mapping of the last flush (" + response.getInt("idMappingSize", 0)
              + " ids). Lower flushEvery so each request stays within what the server echoes back");

    // Store resolved temp ID → RID mapping for cross-flush edge references
    if (response.has("idMapping")) {
      final JSONObject idMapping = response.getJSONObject("idMapping");
      for (final String key : idMapping.keySet()) {
        // "123" in ordinal mode, "v123" when the server resolves by temporary id.
        final int idx = Integer.parseInt(key.charAt(0) == 'v' ? key.substring(1) : key);
        final String ridStr = idMapping.getString(key);      // "#3:456"
        final int colonPos = ridStr.indexOf(':');
        final int bucketId = Integer.parseInt(ridStr.substring(1, colonPos));
        final long position = Long.parseLong(ridStr.substring(colonPos + 1));

        ensureMappingCapacity(idx + 1);
        resolvedBucketIds[idx] = bucketId;
        resolvedPositions[idx] = position;
        if (idx >= resolvedCount)
          resolvedCount = idx + 1;
      }
    }

    buffer.setLength(0);
    itemsInBuffer = 0;
    bufferOrdinalBase = vertexCounter;
  }

  /**
   * Returns the result after the batch has been sent. Only available after {@link #close()}.
   */
  public RemoteBatchResult getResult() {
    if (!closed)
      throw new IllegalStateException("Batch has not been executed yet. Call close() first");
    return new RemoteBatchResult(totalVerticesCreated, totalEdgesCreated, totalElapsedMs);
  }

  /**
   * Flushes any remaining buffered records to the server.
   * This method is idempotent - calling it multiple times has no additional effect.
   */
  @Override
  public void close() {
    if (closed)
      return;
    closed = true;
    flush();
  }

  protected void checkOpen() {
    if (closed)
      throw new IllegalStateException("Batch is already closed");
  }

  /**
   * Resolves a vertex reference: if the temp ID was flushed in a previous batch,
   * returns the real RID string; otherwise returns the original reference unchanged
   * (either a RID string or a temp ID for server-side resolution in the current batch).
   */
  private String resolveRef(final String ref) {
    if (ref == null || ref.isEmpty())
      throw new IllegalArgumentException("Vertex reference cannot be null or empty");

    if (ref.charAt(0) != 'v')
      return ref;

    final int idx = Integer.parseInt(ref.substring(1));
    if (idx < resolvedCount)
      return "#" + resolvedBucketIds[idx] + ":" + resolvedPositions[idx];

    return ref; // unresolved: vertex is in the current buffer, server will handle it
  }

  private void ensureMappingCapacity(final int required) {
    if (required <= resolvedBucketIds.length)
      return;
    int newSize = resolvedBucketIds.length;
    while (newSize < required)
      newSize = newSize << 1;
    resolvedBucketIds = Arrays.copyOf(resolvedBucketIds, newSize);
    resolvedPositions = Arrays.copyOf(resolvedPositions, newSize);
  }

  // --- JSON serialization helpers ---
  // Scalars and the typed primitive-array fast paths (appendJsonFloatArray and siblings) are
  // zero-allocation. Map/Collection/JSONArray/boxed-array values are not (iterators, toList(),
  // Array.get() boxing), but those are comparatively rare property shapes, not the per-record
  // scalar hot path this was originally written for.

  static void appendProperties(final StringBuilder sb, final Object[] properties) {
    if (properties == null || properties.length == 0)
      return;
    if (properties.length % 2 != 0)
      throw new IllegalArgumentException("Properties must be key-value pairs (even number of arguments)");

    for (int i = 0; i < properties.length; i += 2) {
      sb.append(',');
      appendJsonString(sb, (String) properties[i]);
      sb.append(':');
      appendJsonValue(sb, properties[i + 1]);
    }
  }

  static void appendJsonValue(final StringBuilder sb, final Object value) {
    if (value == null)
      sb.append("null");
    else if (value instanceof String)
      appendJsonString(sb, (String) value);
    else if (value instanceof Number || value instanceof Boolean)
      sb.append(value);
    else if (value instanceof Map<?, ?> map)
      // JSONObject implements Map<String, Object>, so it (and any nested JSONObject reached
      // through recursion) is already covered here.
      appendJsonMap(sb, map);
    else if (value instanceof Collection<?> collection)
      appendJsonCollection(sb, collection);
    else if (value instanceof JSONArray jsonArray)
      // JSONArray is Iterable<Object> but deliberately not a java.util.Collection (issue #5091),
      // so without this branch it would fall through to value.toString() and get re-quoted as a
      // JSON string instead of emitted as a JSON array - the same bug class this fix addresses for
      // java.util.Map/Collection, just for ArcadeDB's own JSON wrapper type. toList() recursively
      // normalizes any nested JSONObject/JSONArray to Map/List, so the existing
      // appendJsonCollection -> appendJsonValue recursion handles the rest correctly.
      appendJsonCollection(sb, jsonArray.toList());
    else if (value instanceof float[] floats)
      appendJsonFloatArray(sb, floats);
    else if (value instanceof double[] doubles)
      appendJsonDoubleArray(sb, doubles);
    else if (value instanceof int[] ints)
      appendJsonIntArray(sb, ints);
    else if (value instanceof long[] longs)
      appendJsonLongArray(sb, longs);
    else if (value instanceof short[] shorts)
      appendJsonShortArray(sb, shorts);
    else if (value instanceof byte[] bytes)
      appendJsonByteArray(sb, bytes);
    else if (value.getClass().isArray())
      appendJsonArray(sb, value);
    else
      appendJsonString(sb, value.toString());
  }

  // MAP-typed properties must be sent as a real JSON object (not `value.toString()`, which
  // produces a plain string like "{1=1, 2=2}") or DocumentValidator rejects the value on
  // the server as an incompatible type for the declared MAP property - see issue #6061.
  // Keys are stringified with String.valueOf(), assuming the caller uses one consistent key
  // type (ArcadeDB MAP properties are conventionally String-keyed): a map mixing key types that
  // collide once stringified (e.g. Integer 1 and Long 1L) would silently lose an entry to
  // last-write-wins, the same as any JSON object with a duplicate key.
  static void appendJsonMap(final StringBuilder sb, final Map<?, ?> map) {
    sb.append('{');
    boolean first = true;
    for (final Map.Entry<?, ?> entry : map.entrySet()) {
      if (!first)
        sb.append(',');
      first = false;
      appendJsonString(sb, String.valueOf(entry.getKey()));
      sb.append(':');
      appendJsonValue(sb, entry.getValue());
    }
    sb.append('}');
  }

  // Same rationale as appendJsonMap(), for LIST-typed properties.
  static void appendJsonCollection(final StringBuilder sb, final Collection<?> collection) {
    sb.append('[');
    boolean first = true;
    for (final Object item : collection) {
      if (!first)
        sb.append(',');
      first = false;
      appendJsonValue(sb, item);
    }
    sb.append(']');
  }

  // Typed fast paths for the primitive numeric array kinds ArcadeDB uses for vector-embedding
  // properties (ARRAY_OF_FLOATS/_DOUBLES/_INTEGERS/_LONGS/_SHORTS). These avoid the boxing that
  // java.lang.reflect.Array.get() forces in the generic appendJsonArray() fallback below: an
  // embedding property routinely carries hundreds/thousands of dimensions across up to
  // flushEvery (default 50,000) vertices per flush, and this file is documented as
  // "zero-allocation per-record" for exactly this kind of hot path.
  static void appendJsonFloatArray(final StringBuilder sb, final float[] array) {
    sb.append('[');
    for (int i = 0; i < array.length; i++) {
      if (i > 0)
        sb.append(',');
      sb.append(array[i]);
    }
    sb.append(']');
  }

  static void appendJsonDoubleArray(final StringBuilder sb, final double[] array) {
    sb.append('[');
    for (int i = 0; i < array.length; i++) {
      if (i > 0)
        sb.append(',');
      sb.append(array[i]);
    }
    sb.append(']');
  }

  static void appendJsonIntArray(final StringBuilder sb, final int[] array) {
    sb.append('[');
    for (int i = 0; i < array.length; i++) {
      if (i > 0)
        sb.append(',');
      sb.append(array[i]);
    }
    sb.append(']');
  }

  static void appendJsonLongArray(final StringBuilder sb, final long[] array) {
    sb.append('[');
    for (int i = 0; i < array.length; i++) {
      if (i > 0)
        sb.append(',');
      sb.append(array[i]);
    }
    sb.append(']');
  }

  static void appendJsonShortArray(final StringBuilder sb, final short[] array) {
    sb.append('[');
    for (int i = 0; i < array.length; i++) {
      if (i > 0)
        sb.append(',');
      sb.append(array[i]);
    }
    sb.append(']');
  }

  // byte[] (ArcadeDB's BINARY property type) gets the same typed fast path as the numeric array
  // kinds above. Unlike those, the server needed a matching addition: Type.convert() had narrowing
  // branches for float[]/double[]/int[]/long[]/short[] but not byte[], so a JSON array parsed into a
  // List<Number> was left unconverted and silently stored as an untyped List instead of a byte[]
  // (issue #6061 code review follow-up) - see the Collection -> byte[] branch added to Type.convert().
  static void appendJsonByteArray(final StringBuilder sb, final byte[] array) {
    sb.append('[');
    for (int i = 0; i < array.length; i++) {
      if (i > 0)
        sb.append(',');
      sb.append(array[i]);
    }
    sb.append(']');
  }

  // Remaining array kinds (Object[]/String[]/..., boxed Float[]/Double[]/..., boolean[], char[])
  // fall back to reflection - they hit the same bug as Map/List: a plain array is not a Collection,
  // so without this branch it would fall through to value.toString() (e.g. "[F@6b95977c").
  // java.lang.reflect.Array iterates any array type generically, mirroring JSONObject.put()'s handling
  // of the same case. The server-side Type.convert() already knows how to narrow a JSON array (parsed
  // as a List) back to the schema-declared array type for every array kind ArcadeDB defines a
  // property type for, so emitting a plain JSON array here is sufficient - no further client-side
  // type-specific handling is needed.
  static void appendJsonArray(final StringBuilder sb, final Object array) {
    sb.append('[');
    final int length = Array.getLength(array);
    for (int i = 0; i < length; i++) {
      if (i > 0)
        sb.append(',');
      appendJsonValue(sb, Array.get(array, i));
    }
    sb.append(']');
  }

  static void appendJsonString(final StringBuilder sb, final String s) {
    sb.append('"');
    for (int i = 0, len = s.length(); i < len; i++) {
      final char c = s.charAt(i);
      switch (c) {
      case '"':
        sb.append("\\\"");
        break;
      case '\\':
        sb.append("\\\\");
        break;
      case '\n':
        sb.append("\\n");
        break;
      case '\r':
        sb.append("\\r");
        break;
      case '\t':
        sb.append("\\t");
        break;
      default:
        if (c < 0x20)
          sb.append("\\u").append(String.format("%04x", (int) c));
        else
          sb.append(c);
      }
    }
    sb.append('"');
  }

  /**
   * Builder for configuring a {@link RemoteGraphBatch}. Parameters mirror
   * the server-side GraphBatch.Builder options.
   */
  public static class Builder {
    protected final RemoteDatabase database;
    protected       int            flushEvery = DEFAULT_FLUSH_EVERY;

    // Options are held typed rather than pre-rendered into query-string entries so a subclass carrying the load
    // over a transport that is not HTTP can read them back without parsing its own parameters (issue #6070).
    // Boxed because "not set" and "set to the server default" are different: the server only overrides a default
    // for an option the caller actually chose.
    protected Integer batchSize;
    protected Integer expectedEdgeCount;
    protected Integer edgeListInitialSize;
    protected Boolean lightEdges;
    protected Boolean bidirectional;
    protected Integer commitEvery;
    protected Integer vertexBatchSize;
    protected Boolean useWAL;
    protected Boolean preAllocateEdgeChunks;
    protected Boolean parallelFlush;
    protected Integer commitRetries;
    protected Long    commitRetryDelayMs;

    /** Not for direct use: a builder comes from {@link RemoteDatabase#batch()}, which picks the right one for its transport. */
    protected Builder(final RemoteDatabase database) {
      this.database = database;
    }

    /**
     * Number of items (vertices + edges) buffered client-side before an automatic
     * flush to the server. Default: 50,000. Set to 0 to disable auto-flush
     * (everything sent on {@link RemoteGraphBatch#close()}).
     */
    public Builder withFlushEvery(final int flushEvery) {
      if (flushEvery < 0)
        throw new IllegalArgumentException("flushEvery must be >= 0");
      this.flushEvery = flushEvery;
      return this;
    }

    /** Maximum number of edges buffered before an automatic flush on the server. Default: 100,000. */
    public Builder withBatchSize(final int batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    /** Hint for expected total edge count, used for server-side auto-tuning. */
    public Builder withExpectedEdgeCount(final int expectedEdgeCount) {
      this.expectedEdgeCount = expectedEdgeCount;
      return this;
    }

    /** Initial size in bytes for new edge segments. Default: 2048. */
    public Builder withEdgeListInitialSize(final int size) {
      this.edgeListInitialSize = size;
      return this;
    }

    /**
     * If true, property-less edges are stored as light edges. Default: false.
     *
     * @deprecated Declare {@code LIGHTWEIGHT} on the edge type instead.
     */
    @Deprecated
    public Builder withLightEdges(final boolean lightEdges) {
      this.lightEdges = lightEdges;
      return this;
    }

    /** If true, incoming edges are also connected. Default: true. */
    public Builder withBidirectional(final boolean bidirectional) {
      this.bidirectional = bidirectional;
      return this;
    }

    /** Number of edges to process before committing within a server-side flush. Default: 50,000. */
    public Builder withCommitEvery(final int commitEvery) {
      this.commitEvery = commitEvery;
      return this;
    }

    /**
     * Number of vertices the server accumulates before creating and committing them in a single transaction.
     * Default: 10,000. On a replicated database that transaction is shipped as one Raft entry, so lower it
     * when the server warns that a replicated entry approaches the maximum entry size (issue #5470).
     */
    public Builder withVertexBatchSize(final int vertexBatchSize) {
      if (vertexBatchSize < 1)
        throw new IllegalArgumentException("vertexBatchSize must be greater than 0");
      this.vertexBatchSize = vertexBatchSize;
      return this;
    }

    /** If true, enables Write-Ahead Logging during import. Default: false. */
    public Builder withWAL(final boolean useWAL) {
      this.useWAL = useWAL;
      return this;
    }

    /** If true, pre-allocates empty edge segments at vertex creation. Default: true. */
    public Builder withPreAllocateEdgeChunks(final boolean preAllocate) {
      this.preAllocateEdgeChunks = preAllocate;
      return this;
    }

    /** If true, edge connection during flush is parallelized. Default: true. */
    public Builder withParallelFlush(final boolean parallel) {
      this.parallelFlush = parallel;
      return this;
    }

    /**
     * Number of times a vertex-creation commit is retried when it fails with a transient error, such as a
     * quorum lost to a leader re-election on a replicated database. Default: 10. Set to 0 to fail on the first
     * error instead of retrying.
     */
    public Builder withCommitRetries(final int commitRetries) {
      if (commitRetries < 0)
        throw new IllegalArgumentException("commitRetries must be >= 0");
      this.commitRetries = commitRetries;
      return this;
    }

    /**
     * Initial back-off in milliseconds before the first vertex-commit retry; later retries back off
     * exponentially from it. Default: 1000.
     */
    public Builder withCommitRetryDelay(final long commitRetryDelayMs) {
      if (commitRetryDelayMs < 0)
        throw new IllegalArgumentException("commitRetryDelayMs must be >= 0");
      this.commitRetryDelayMs = commitRetryDelayMs;
      return this;
    }

    /** Renders the options chosen by the caller as the query-string parameters of {@code POST /api/v1/batch}. */
    protected Map<String, String> toQueryParams() {
      final Map<String, String> queryParams = new HashMap<>();
      putIfSet(queryParams, "batchSize", batchSize);
      putIfSet(queryParams, "expectedEdgeCount", expectedEdgeCount);
      putIfSet(queryParams, "edgeListInitialSize", edgeListInitialSize);
      putIfSet(queryParams, "lightEdges", lightEdges);
      putIfSet(queryParams, "bidirectional", bidirectional);
      putIfSet(queryParams, "commitEvery", commitEvery);
      putIfSet(queryParams, "vertexBatchSize", vertexBatchSize);
      putIfSet(queryParams, "wal", useWAL);
      putIfSet(queryParams, "preAllocateEdgeChunks", preAllocateEdgeChunks);
      putIfSet(queryParams, "parallelFlush", parallelFlush);
      putIfSet(queryParams, "commitRetries", commitRetries);
      putIfSet(queryParams, "commitRetryDelayMs", commitRetryDelayMs);
      return queryParams;
    }

    private static void putIfSet(final Map<String, String> queryParams, final String name, final Object value) {
      if (value != null)
        queryParams.put(name, value.toString());
    }

    /** Creates the {@link RemoteGraphBatch} ready for buffering vertices and edges. */
    public RemoteGraphBatch build() {
      final int effectiveFlushEvery = flushEvery == 0 ? Integer.MAX_VALUE : flushEvery;
      return new RemoteGraphBatch(database, toQueryParams(), effectiveFlushEvery);
    }
  }
}
