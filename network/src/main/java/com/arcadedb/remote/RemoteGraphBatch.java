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
import com.arcadedb.serializer.json.JSONObject;

import java.util.Arrays;
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
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RemoteGraphBatch implements AutoCloseable {

  static final int DEFAULT_FLUSH_EVERY  = 50_000;
  private static final int DEFAULT_BUFFER_SIZE = 8192;
  private static final int INITIAL_MAPPING_CAPACITY = 1024;

  private final RemoteDatabase      database;
  private final Map<String, String> queryParams;
  private final int                 flushEvery;
  private final StringBuilder       buffer;
  private       int                 vertexCounter;
  private       int                 itemsInBuffer;
  private       boolean             hasEdges;
  private       boolean             closed;

  // --- Aggregated result across all flushes ---
  private long totalVerticesCreated;
  private long totalEdgesCreated;
  private long totalElapsedMs;

  // --- Resolved temp ID mapping: flat arrays indexed by vertex counter ---
  // resolvedBucketIds[i] and resolvedPositions[i] hold the real RID for vertex "v<i>"
  private int[]  resolvedBucketIds;
  private long[] resolvedPositions;
  private int    resolvedCount; // number of vertices whose RIDs have been resolved

  RemoteGraphBatch(final RemoteDatabase database, final Map<String, String> queryParams, final int flushEvery) {
    this.database = database;
    this.queryParams = queryParams;
    this.flushEvery = flushEvery;
    this.buffer = new StringBuilder(DEFAULT_BUFFER_SIZE);
    this.resolvedBucketIds = new int[INITIAL_MAPPING_CAPACITY];
    this.resolvedPositions = new long[INITIAL_MAPPING_CAPACITY];
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

    final JSONObject response = database.sendBatch(buffer.toString(), queryParams);

    totalVerticesCreated += response.getLong("verticesCreated");
    totalEdgesCreated += response.getLong("edgesCreated");
    totalElapsedMs += response.getLong("elapsedMs");

    // Store resolved temp ID → RID mapping for cross-flush edge references
    if (response.has("idMapping")) {
      final JSONObject idMapping = response.getJSONObject("idMapping");
      for (final String key : idMapping.keySet()) {
        final int idx = Integer.parseInt(key.substring(1)); // "v123" → 123
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

  private void checkOpen() {
    if (closed)
      throw new IllegalStateException("Batch is already closed");
  }

  /**
   * Resolves a vertex reference: if the temp ID was flushed in a previous batch,
   * returns the real RID string; otherwise returns the original reference unchanged
   * (either a RID string or a temp ID for server-side resolution in the current batch).
   */
  private String resolveRef(final String ref) {
    if (ref.charAt(0) == '#' || ref.charAt(0) != 'v')
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

  // --- JSON serialization helpers (zero-allocation per-record) ---

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
    else
      appendJsonString(sb, value.toString());
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
    private final RemoteDatabase      database;
    private final Map<String, String> queryParams = new HashMap<>();
    private       int                 flushEvery  = DEFAULT_FLUSH_EVERY;

    Builder(final RemoteDatabase database) {
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
      queryParams.put("batchSize", String.valueOf(batchSize));
      return this;
    }

    /** Hint for expected total edge count, used for server-side auto-tuning. */
    public Builder withExpectedEdgeCount(final int expectedEdgeCount) {
      queryParams.put("expectedEdgeCount", String.valueOf(expectedEdgeCount));
      return this;
    }

    /** Initial size in bytes for new edge segments. Default: 2048. */
    public Builder withEdgeListInitialSize(final int size) {
      queryParams.put("edgeListInitialSize", String.valueOf(size));
      return this;
    }

    /** If true, property-less edges are stored as light edges. Default: false. */
    public Builder withLightEdges(final boolean lightEdges) {
      queryParams.put("lightEdges", String.valueOf(lightEdges));
      return this;
    }

    /** If true, incoming edges are also connected. Default: true. */
    public Builder withBidirectional(final boolean bidirectional) {
      queryParams.put("bidirectional", String.valueOf(bidirectional));
      return this;
    }

    /** Number of edges to process before committing within a server-side flush. Default: 50,000. */
    public Builder withCommitEvery(final int commitEvery) {
      queryParams.put("commitEvery", String.valueOf(commitEvery));
      return this;
    }

    /** If true, enables Write-Ahead Logging during import. Default: false. */
    public Builder withWAL(final boolean useWAL) {
      queryParams.put("wal", String.valueOf(useWAL));
      return this;
    }

    /** If true, pre-allocates empty edge segments at vertex creation. Default: true. */
    public Builder withPreAllocateEdgeChunks(final boolean preAllocate) {
      queryParams.put("preAllocateEdgeChunks", String.valueOf(preAllocate));
      return this;
    }

    /** If true, edge connection during flush is parallelized. Default: true. */
    public Builder withParallelFlush(final boolean parallel) {
      queryParams.put("parallelFlush", String.valueOf(parallel));
      return this;
    }

    /** Creates the {@link RemoteGraphBatch} ready for buffering vertices and edges. */
    public RemoteGraphBatch build() {
      final int effectiveFlushEvery = flushEvery == 0 ? Integer.MAX_VALUE : flushEvery;
      return new RemoteGraphBatch(database, queryParams, effectiveFlushEvery);
    }
  }
}
