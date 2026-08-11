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
package com.arcadedb.remote.grpc;

import com.arcadedb.remote.RemoteGraphBatch;
import com.arcadedb.remote.grpc.utils.ProtoUtils;
import com.arcadedb.server.grpc.GraphBatchChunk;
import com.arcadedb.server.grpc.GraphBatchOptions;
import com.arcadedb.server.grpc.GraphBatchRecord;
import com.arcadedb.server.grpc.GraphBatchResult;

/**
 * Batch graph importer that carries the load over the {@code GraphBatchLoad} streaming RPC, the gRPC transport
 * a {@link RemoteGrpcDatabase} connection was chosen for. Obtained from {@code remoteGrpcDb.batch()}, and
 * interchangeable with the JSONL-over-HTTP {@link RemoteGraphBatch} it extends: same methods, same
 * {@link com.arcadedb.remote.RemoteGraphBatch.Builder} options, same results (issue #6070).
 * <p>
 * What the transport changes is where temporary ids are resolved. Over HTTP each flush is an independent
 * request the server does not remember, so the loader asks for the id mapping back and rewrites the references
 * of later edges itself. Here the whole load is one stream and the server keeps the mapping for its lifetime,
 * so a temporary id stays a temporary id on the wire however many chunks separate the vertex from the edge that
 * references it. That removes the per-flush round trip, the client-side mapping arrays, and the ceiling the
 * HTTP loader has on {@code flushEvery} (past which the server stops echoing a mapping too large to consume).
 * The mapping is not requested at all, which is also what keeps a load of millions of vertices from failing at
 * the very end on the 4 MB default message limit, with everything already committed. Nothing is lost by not
 * asking: {@link com.arcadedb.remote.RemoteBatchResult} carries counters and elapsed time on either transport
 * and has never exposed the mapping, so a caller wanting temporary ids resolved to RIDs queries the vertices
 * back or calls the {@code GraphBatchLoad} RPC directly with {@code return_id_mapping} set.
 * <p>
 * {@code flushEvery} therefore means chunk size here: the number of records per {@code GraphBatchChunk} pushed
 * onto the open stream, not a round trip. {@link #flush()} closes the current chunk and sends it; the result is
 * only complete once {@link #close()} has ended the stream and the server has answered.
 * <p>
 * As with the HTTP loader, all vertices must be created before any edge.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RemoteGrpcGraphBatch extends RemoteGraphBatch {

  /** Cap on how long the whole load may take, not a per-chunk timeout: the RPC is one stream from open to close. */
  public static final long DEFAULT_TIMEOUT_MS = 6 * 60 * 60 * 1000L;

  private final RemoteGrpcDatabase database;
  private final GraphBatchOptions  options;
  private final int                chunkSize;
  private final long               timeoutMs;

  private GraphBatchChunk.Builder chunk;
  private int                     recordsInChunk;
  private GraphBatchLoadStream    stream;
  private boolean                 firstChunkSent;

  RemoteGrpcGraphBatch(final RemoteGrpcDatabase database, final GraphBatchOptions options, final int chunkSize,
      final long timeoutMs) {
    super(database);
    this.database = database;
    this.options = options;
    this.chunkSize = chunkSize;
    this.timeoutMs = timeoutMs;
  }

  @Override
  public String createVertex(final String typeName, final Object... properties) {
    checkOpen();
    if (hasEdges)
      throw new IllegalStateException("Cannot add vertices after edges have been added");

    final String tempId = "v" + vertexCounter++;

    final GraphBatchRecord.Builder record = GraphBatchRecord.newBuilder()
        .setKind(GraphBatchRecord.Kind.VERTEX)
        .setTypeName(typeName)
        .setTempId(tempId);
    putProperties(record, properties);

    addRecord(record.build());

    return tempId;
  }

  @Override
  public void createEdge(final String edgeTypeName, final String from, final String to, final Object... properties) {
    checkOpen();
    if (from == null || from.isEmpty() || to == null || to.isEmpty())
      throw new IllegalArgumentException("Vertex reference cannot be null or empty");
    hasEdges = true;

    // No client-side resolution: the server holds the temporary-id map for the whole stream, so a reference to a
    // vertex sent in an earlier chunk resolves there exactly as one sent in this chunk does.
    final GraphBatchRecord.Builder record = GraphBatchRecord.newBuilder()
        .setKind(GraphBatchRecord.Kind.EDGE)
        .setTypeName(edgeTypeName)
        .setFromRef(from)
        .setToRef(to);
    putProperties(record, properties);

    addRecord(record.build());
  }

  /**
   * Sends the records buffered so far as one chunk on the open stream, opening the stream if this is the first
   * one. Unlike the HTTP loader this is not a round trip and returns nothing: the counters of the load are only
   * known once {@link #close()} has ended the stream and the server has answered with the totals.
   */
  @Override
  public void flush() {
    if (recordsInChunk == 0)
      return;

    if (!firstChunkSent)
      // Database and options are read from the first chunk and ignored on the ones that follow.
      chunk.setDatabase(database.getName()).setCredentials(database.buildCredentials()).setOptions(options);

    // The buffer is handed over before anything that can fail, so nothing this method throws leaves records
    // behind for a later flush to send a second time. An interior auto-flush raises its failure from inside
    // createVertex()/createEdge(), and the close() that follows - from a try-with-resources, which runs whether
    // or not the body threw - would otherwise repeat the whole attempt and answer with a second failure on top
    // of the real one. Both of the things below can throw: opening the call, and sending on it.
    final GraphBatchChunk pending = chunk.build();
    chunk = null;
    recordsInChunk = 0;

    if (stream == null)
      stream = database.openGraphBatchLoadStream(timeoutMs);

    stream.send(pending);

    // Only once a chunk is actually on the wire has the server been told the database and the options, so this
    // stays false if the send above threw and the next attempt stamps them again.
    firstChunkSent = true;
  }

  /**
   * Sends whatever is still buffered, ends the stream and waits for the server's totals. Idempotent.
   * <p>
   * A load that fails here is not a load that rolled back - the batch commits incrementally - so on the way out
   * this records whatever the server reported as already committed. {@link #getResult()} is therefore worth
   * reading after catching the failure: it says how much of the load is in the database, which is what a caller
   * needs to reconcile instead of re-sending everything.
   */
  @Override
  public void close() {
    if (closed)
      return;
    closed = true;

    try {
      flush();
    } catch (final RuntimeException e) {
      // The call has to be released even though the load is failing: a server-side batch holds the database's
      // batching slot for its lifetime, so an abandoned call would stop every later load until its deadline.
      if (stream != null) {
        stream.cancelQuietly("the graph batch load failed while sending its last chunk");
        recordTotals(stream.getPartialResult());
      }
      throw e;
    }

    if (stream == null)
      // Nothing was ever buffered: no stream was opened, so there is nothing to close and the totals stay zero.
      return;

    try {
      recordTotals(stream.complete());
    } catch (final RuntimeException e) {
      recordTotals(stream.getPartialResult());
      throw e;
    }
  }

  private void recordTotals(final GraphBatchResult result) {
    if (result == null)
      return;
    totalVerticesCreated = result.getVerticesCreated();
    totalEdgesCreated = result.getEdgesCreated();
    totalElapsedMs = result.getElapsedMs();
  }

  private void addRecord(final GraphBatchRecord record) {
    if (chunk == null)
      chunk = GraphBatchChunk.newBuilder();
    chunk.addRecords(record);

    if (++recordsInChunk >= chunkSize)
      flush();
  }

  private static void putProperties(final GraphBatchRecord.Builder record, final Object[] properties) {
    if (properties == null || properties.length == 0)
      return;
    if (properties.length % 2 != 0)
      throw new IllegalArgumentException("Properties must be key-value pairs (even number of arguments)");

    for (int i = 0; i < properties.length; i += 2)
      record.putProperties((String) properties[i], ProtoUtils.toGrpcValue(properties[i + 1]));
  }

  /**
   * Builder for a {@link RemoteGrpcGraphBatch}. Inherits every option of the HTTP loader's builder, which mean
   * the same thing here, and translates them into the {@link GraphBatchOptions} the streaming RPC carries.
   */
  public static class Builder extends RemoteGraphBatch.Builder {
    private long timeoutMs = DEFAULT_TIMEOUT_MS;

    Builder(final RemoteGrpcDatabase database) {
      super(database);
    }

    // The inherited setters are re-declared to narrow their return type. Without that, the first inherited call
    // in a chain would hand back the base builder and nothing gRPC-specific could follow it, making
    // withTimeout() usable only in first position. They carry no behaviour of their own.

    @Override
    public Builder withFlushEvery(final int flushEvery) {
      super.withFlushEvery(flushEvery);
      return this;
    }

    @Override
    public Builder withBatchSize(final int batchSize) {
      super.withBatchSize(batchSize);
      return this;
    }

    @Override
    public Builder withExpectedEdgeCount(final int expectedEdgeCount) {
      super.withExpectedEdgeCount(expectedEdgeCount);
      return this;
    }

    @Override
    public Builder withEdgeListInitialSize(final int size) {
      super.withEdgeListInitialSize(size);
      return this;
    }

    @Deprecated
    @Override
    public Builder withLightEdges(final boolean lightEdges) {
      super.withLightEdges(lightEdges);
      return this;
    }

    @Override
    public Builder withBidirectional(final boolean bidirectional) {
      super.withBidirectional(bidirectional);
      return this;
    }

    @Override
    public Builder withCommitEvery(final int commitEvery) {
      super.withCommitEvery(commitEvery);
      return this;
    }

    @Override
    public Builder withVertexBatchSize(final int vertexBatchSize) {
      super.withVertexBatchSize(vertexBatchSize);
      return this;
    }

    @Override
    public Builder withWAL(final boolean useWAL) {
      super.withWAL(useWAL);
      return this;
    }

    @Override
    public Builder withPreAllocateEdgeChunks(final boolean preAllocate) {
      super.withPreAllocateEdgeChunks(preAllocate);
      return this;
    }

    @Override
    public Builder withParallelFlush(final boolean parallel) {
      super.withParallelFlush(parallel);
      return this;
    }

    @Override
    public Builder withCommitRetries(final int commitRetries) {
      super.withCommitRetries(commitRetries);
      return this;
    }

    @Override
    public Builder withCommitRetryDelay(final long commitRetryDelayMs) {
      super.withCommitRetryDelay(commitRetryDelayMs);
      return this;
    }

    /**
     * Deadline for the whole load, from the first chunk to the server's answer, in milliseconds. Default: 6
     * hours. It bounds the stream rather than any single chunk, so it has to cover the entire import.
     * <p>
     * It is not the only limit in play, and raising it does not lift the other one: a single chunk also has a
     * fixed five-minute ceiling on how long it may wait for the transport to drain before the load is given up
     * on. That ceiling is deliberately not settable - it exists to tell a connection that died without saying
     * so from one that is merely busy, and no legitimate load spends five minutes on one chunk's backpressure -
     * but it does mean a link slow enough to breach it fails the load however much of this budget is left.
     */
    public Builder withTimeout(final long timeoutMs) {
      if (timeoutMs < 1)
        throw new IllegalArgumentException("timeoutMs must be greater than 0");
      this.timeoutMs = timeoutMs;
      return this;
    }

    /** Renders the options the caller chose as the {@link GraphBatchOptions} of the first chunk. */
    protected GraphBatchOptions toGraphBatchOptions() {
      final GraphBatchOptions.Builder options = GraphBatchOptions.newBuilder();
      if (batchSize != null)
        options.setBatchSize(batchSize);
      if (expectedEdgeCount != null)
        options.setExpectedEdgeCount(expectedEdgeCount);
      if (edgeListInitialSize != null)
        options.setEdgeListInitialSize(edgeListInitialSize);
      if (lightEdges != null)
        options.setLightEdges(lightEdges);
      if (bidirectional != null)
        options.setBidirectional(bidirectional);
      if (commitEvery != null)
        options.setCommitEvery(commitEvery);
      if (vertexBatchSize != null)
        options.setVertexBatchSize(vertexBatchSize);
      if (useWAL != null)
        options.setWal(useWAL);
      if (preAllocateEdgeChunks != null)
        options.setPreAllocateEdgeChunks(preAllocateEdgeChunks);
      if (parallelFlush != null)
        options.setParallelFlush(parallelFlush);
      if (commitRetries != null)
        options.setCommitRetries(commitRetries);
      if (commitRetryDelayMs != null)
        options.setCommitRetryDelayMs(commitRetryDelayMs);
      // The server resolves temporary ids for the whole stream, so this loader never reads the mapping back.
      // Saying so keeps a load of millions of vertices from building a response too large to send, which would
      // fail the call at the very end with everything already committed.
      options.setReturnIdMapping(false);
      return options.build();
    }

    @Override
    public RemoteGrpcGraphBatch build() {
      final int effectiveChunkSize = flushEvery == 0 ? Integer.MAX_VALUE : flushEvery;
      return new RemoteGrpcGraphBatch((RemoteGrpcDatabase) database, toGraphBatchOptions(), effectiveChunkSize,
          timeoutMs);
    }
  }
}
