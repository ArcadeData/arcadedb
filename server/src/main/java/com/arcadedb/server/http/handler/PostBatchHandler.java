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
package com.arcadedb.server.http.handler;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.batch.BatchRecord;
import com.arcadedb.server.http.handler.batch.BatchRecordStream;
import com.arcadedb.server.http.handler.batch.CsvBatchRecordStream;
import com.arcadedb.server.http.handler.batch.JsonlBatchRecordStream;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.ServerConnection;
import io.undertow.util.HeaderValues;
import org.xnio.Options;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

/**
 * High-performance HTTP handler for bulk-loading vertices and edges using the GraphBatch API.
 * Supports JSONL and CSV input formats with streaming parsing (no full-body buffering).
 * <p>
 * Endpoint: POST /api/v1/batch/{database}
 * <p>
 * Content-Type:
 * - application/x-ndjson or application/jsonl → JSONL format
 * - text/csv → CSV format
 * <p>
 * Input must contain vertices first, then edges. Vertices can have temporary IDs (@id) that
 * edges reference via @from/@to. Edges can also reference existing database RIDs (#bucket:pos).
 * <p>
 * Read timeout: the body is consumed while the load runs, so the pauses the worker thread takes inside a commit
 * (index compaction, replication of a large entry) count towards Undertow's read watchdog. For the duration of
 * the request that watchdog is therefore raised to {@code arcadedb.server.httpStreamingReadTimeout} instead of
 * {@code arcadedb.network.socketTimeout}, which still bounds every single blocking read and so keeps cutting off
 * a client that stops sending (issue #5470). A body that ends early is answered with HTTP 408 and the
 * partial-commit counts, never with a 200 carrying a truncated count.
 * <p>
 * Atomicity: a batch is NOT atomic. GraphBatch commits every {@code commitEvery} records, so a
 * failure mid-stream leaves earlier chunks durably committed. On a client-input error the response
 * carries {@code verticesCreated} / {@code edgesCreated} and a {@code partialCommit} flag; because
 * temporary {@code @id}s are not keys, blindly retrying the whole payload duplicates the
 * already-committed vertices. Those counts are the records <em>attempted</em> before the failure, an
 * upper bound on what is durable: records handled since the last {@code commitEvery} boundary are
 * rolled back, so a client reconciling against them should treat them as "at most this many". Only
 * the client-input (HTTP 400) path is enriched with counts; engine/cluster failures keep their
 * base-handler status (409/503/403/404/500) and are best-effort for partial-commit reporting.
 * <p>
 * Query parameters (all optional, map to GraphBatch.Builder):
 * - batchSize (int, default 100000)
 * - lightEdges (boolean, default false)
 * - wal (boolean, default false)
 * - parallelFlush (boolean, default true)
 * - preAllocateEdgeChunks (boolean, default true)
 * - edgeListInitialSize (int, default 2048)
 * - bidirectional (boolean, default true)
 * - commitEvery (int, default 50000)
 * - expectedEdgeCount (int, default 0)
 * - commitRetries (int, default 10): retries of a vertex-creation commit that fails with a
 *   transient retryable error (e.g. a Raft leader re-election), so a cluster hiccup does not
 *   abort the whole streaming load (issue #4724)
 * - commitRetryDelayMs (long, default 1000): initial back-off before the first retry
 * - vertexBatchSize (int, default 10000): vertices accumulated before they are created and committed in
 *   one transaction. On a replicated database that transaction becomes a single Raft entry, so this is the
 *   knob to lower when the server warns that a replicated entry approaches the maximum entry size
 *   (issue #5470); on an embedded/standalone database it only trades memory for throughput
 */
public class PostBatchHandler extends AbstractServerHttpHandler {

  private static final int        VERTEX_BATCH_SIZE = 10_000;
  private static final HttpClient HTTP_CLIENT       = HttpClient.newHttpClient();

  public PostBatchHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
  }

  @Override
  protected boolean requiresJsonPayload() {
    return false;
  }

  @Override
  protected String parseRequestPayload(final HttpServerExchange e) {
    // Do NOT load full body. We'll stream from the InputStream in execute().
    // Just ensure blocking mode is started.
    if (!e.isInIoThread() && !e.isBlocking())
      e.startBlocking();
    return null;
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) throws Exception {

    final Deque<String> databaseParam = exchange.getQueryParameters().get("database");
    if (databaseParam == null || databaseParam.isEmpty())
      return new ExecutionResponse(400, "{ \"error\" : \"Database parameter is required\"}");

    final String databaseName = databaseParam.getFirst();

    // Enforce database-level authorization (GHSA-x8mg-6r4p-87pf): this handler does not extend
    // DatabaseAbstractHandler. Checked before any leader-forwarding so a follower cannot be used to relay
    // an unauthorized batch.
    checkAuthorizationOnDatabase(user, databaseName);

    // Determine format from Content-Type
    final HeaderValues contentTypeHeader = exchange.getRequestHeaders().get("Content-Type");
    final String contentType = contentTypeHeader != null && !contentTypeHeader.isEmpty()
        ? contentTypeHeader.getFirst().toLowerCase()
        : "application/x-ndjson";

    // Start streaming input. The stream must be created BEFORE relaxing the connection watchdog below:
    // UndertowInputStream captures the read timeout in effect at construction time and uses it to bound
    // every blocking read, so a client that stops sending is still cut off after
    // 'arcadedb.network.socketTimeout' while the asynchronous watchdog moves to the streaming budget.
    final InputStream inputStream = exchange.getInputStream();

    // Applies to the forwarding path too: while the leader is busy the follower cannot drain the client
    // socket either, so its own watchdog would kill the upload it is relaying (issue #5470).
    final Integer previousReadTimeout = relaxConnectionReadTimeout(exchange);
    try {
      // On a follower of a replicated database the request must run on the leader: the bulk
      // path mutates shared state (schema dictionary, type metadata) that only the leader can
      // serialize. Without forwarding, a single batch with several new property keys hits the
      // race in Dictionary.getIdByName as the local state machine apply runs concurrently with
      // the user thread (issue #4122).
      final HAServerPlugin ha = httpServer.getServer().getHA();
      if (ha != null && !ha.isLeader())
        return forwardBatchToLeader(exchange, ha, databaseName, user, contentType);

      final DatabaseInternal database = httpServer.getServer().getDatabase(databaseName, false, false);
      final boolean isCsv = contentType.contains("text/csv");

      // Configure GraphBatch from query parameters
      final GraphBatch.Builder builder = database.batch();
      configureBuilder(exchange, builder);

      return streamRecords(exchange, databaseName, isCsv, builder, inputStream, new HashMap<>(),
          System.currentTimeMillis(), parseVertexBatchSize(exchange));
    } finally {
      restoreConnectionReadTimeout(exchange, previousReadTimeout);
    }
  }

  /**
   * Consumes the streaming request body and feeds it to a {@link GraphBatch}. Extracted from
   * {@link #execute} so the caller can restore the connection read timeout in a {@code finally} block.
   */
  private ExecutionResponse streamRecords(final HttpServerExchange exchange, final String databaseName,
      final boolean isCsv, final GraphBatch.Builder builder, final InputStream inputStream,
      final Map<String, RID> tempIdMap, final long startTime, final int vertexBatchSize) throws Exception {

    long verticesCreated = 0;
    long edgesCreated = 0;

    try (final BatchRecordStream stream = isCsv
        ? new CsvBatchRecordStream(inputStream)
        : new JsonlBatchRecordStream(inputStream);
         final GraphBatch batch = builder.build()) {

      // Phase 1: Vertices — accumulate by type for batch creation
      String currentTypeName = null;
      final List<Object[]> vertexPropsBatch = new ArrayList<>(vertexBatchSize);
      final List<String> vertexTempIds = new ArrayList<>(vertexBatchSize);

      while (stream.hasNext()) {
        final BatchRecord rec = stream.next();

        if (rec.kind == BatchRecord.Kind.EDGE) {
          // Transition to edge phase: flush remaining vertices
          if (!vertexPropsBatch.isEmpty()) {
            verticesCreated += flushVertexBatch(batch, currentTypeName, vertexPropsBatch, vertexTempIds, tempIdMap);
          }

          // Process this first edge record
          processEdge(batch, rec, tempIdMap, stream.getLineNumber());
          edgesCreated++;
          break;
        }

        // Accumulate vertex — flush when type changes or batch is full
        if (currentTypeName != null && !currentTypeName.equals(rec.typeName)) {
          verticesCreated += flushVertexBatch(batch, currentTypeName, vertexPropsBatch, vertexTempIds, tempIdMap);
        }
        currentTypeName = rec.typeName;
        vertexPropsBatch.add(rec.copyProperties());
        vertexTempIds.add(rec.tempId);

        if (vertexPropsBatch.size() >= vertexBatchSize) {
          verticesCreated += flushVertexBatch(batch, currentTypeName, vertexPropsBatch, vertexTempIds, tempIdMap);
        }
      }

      // Flush remaining vertices (e.g., vertex-only import or last batch before EOF)
      if (!vertexPropsBatch.isEmpty())
        verticesCreated += flushVertexBatch(batch, currentTypeName, vertexPropsBatch, vertexTempIds, tempIdMap);

      // Phase 2: Remaining edges
      while (stream.hasNext()) {
        final BatchRecord rec = stream.next();
        if (rec.kind != BatchRecord.Kind.EDGE)
          throw new IllegalArgumentException("Expected edge record but got vertex at line " + stream.getLineNumber()
              + ". All vertices must appear before edges");
        processEdge(batch, rec, tempIdMap, stream.getLineNumber());
        edgesCreated++;
      }

      // batch.close() is called by try-with-resources: flushes edges, connects incoming edges
    } catch (final IllegalArgumentException e) {
      // Client-input failure mid-stream (malformed line, unknown temporary id, bad RID): a batch load
      // is NOT atomic - GraphBatch commits every commitEvery records, so records handled before the
      // failure may already be durable on disk. Surface how many vertices and edges were attempted so
      // far (plus a partialCommit flag) so a client can reconcile rather than blindly re-POSTing the
      // whole payload - a retry would duplicate the already-committed vertices, whose temporary @id
      // values are not keys (issue #5036).
      //
      // Only IllegalArgumentException (HTTP 400) is enriched here. Engine/cluster exceptions
      // (DuplicatedKeyException -> 409, TransactionCommittedRemotelyException -> 409,
      // NeedRetryException -> 503, security -> 403, RecordNotFoundException -> 404, ...) are left to
      // propagate so AbstractServerHttpHandler keeps its status mapping and logs the full stack trace.
      // Downgrading a "do not retry" outcome to a retry-inviting 500 here would duplicate the very
      // committed chunks this change protects.
      final String message = e.getMessage() != null ? e.getMessage() : e.toString();
      LogManager.instance().log(this, Level.WARNING,
          "Batch load on database '%s' failed after %d vertices and %d edges: %s",
          null, databaseName, verticesCreated, edgesCreated, message);

      // Bespoke body (not the shared sendErrorResponse envelope) because the partial-commit counts must
      // be machine-parsable by the client. The message is emitted in `error` even in production on
      // purpose: batch IllegalArgumentExceptions echo client input (line numbers, temp ids, malformed
      // RIDs), so there is nothing engine-internal to conceal, and the client needs the offending
      // location to reconcile. The correlation id is carried through so the 400 stays cross-referenceable
      // with the server log, matching every other endpoint (issue #5036 review).
      final JSONObject error = new JSONObject();
      error.put("error", message);
      error.put("exception", e.getClass().getName());
      final String correlationId = getCorrelationId(exchange);
      if (correlationId != null && !correlationId.isEmpty())
        error.put("requestId", correlationId);
      error.put("verticesCreated", verticesCreated);
      error.put("edgesCreated", edgesCreated);
      error.put("partialCommit", verticesCreated > 0 || edgesCreated > 0);
      return new ExecutionResponse(400, error.toString());
    } catch (final IOException e) {
      // The request body could not be read to the end: the client went away, a proxy cut the upload, or the
      // connection watchdog fired because the server spent longer than its budget committing instead of
      // reading (issue #5470). Never let this look like a completed load: reaching the end of the loop with a
      // truncated body would answer 200 with a partial count and the client would happily move on.
      final String message = e.getMessage() != null ? e.getMessage() : e.toString();
      LogManager.instance().log(this, Level.WARNING,
          "Batch load on database '%s' was interrupted after %d vertices and %d edges because the request body "
              + "could not be read to the end: %s. If the server was busy (index compaction, replication of a large "
              + "entry) raise '%s' (currently %d ms)",
          null, databaseName, verticesCreated, edgesCreated, message,
          GlobalConfiguration.SERVER_HTTP_STREAMING_READ_TIMEOUT.getKey(),
          httpServer.getServer().getConfiguration()
              .getValueAsInteger(GlobalConfiguration.SERVER_HTTP_STREAMING_READ_TIMEOUT));

      final JSONObject error = new JSONObject();
      error.put("error", "Request body was truncated after " + verticesCreated + " vertices and " + edgesCreated
          + " edges: " + message);
      error.put("exception", e.getClass().getName());
      final String correlationId = getCorrelationId(exchange);
      if (correlationId != null && !correlationId.isEmpty())
        error.put("requestId", correlationId);
      error.put("verticesCreated", verticesCreated);
      error.put("edgesCreated", edgesCreated);
      error.put("partialCommit", verticesCreated > 0 || edgesCreated > 0);
      // 408: the request was not fully received. The response often never reaches a client whose connection is
      // already gone, but when it does it carries the counts needed to resume instead of restarting.
      return new ExecutionResponse(408, error.toString());
    }

    final long elapsed = System.currentTimeMillis() - startTime;

    final JSONObject result = new JSONObject();
    result.put("verticesCreated", verticesCreated);
    result.put("edgesCreated", edgesCreated);
    result.put("elapsedMs", elapsed);

    // Include temp ID mapping if any temp IDs were used
    if (!tempIdMap.isEmpty()) {
      final JSONObject mapping = new JSONObject();
      for (final Map.Entry<String, RID> entry : tempIdMap.entrySet())
        mapping.put(entry.getKey(), entry.getValue().toString());
      result.put("idMapping", mapping);
    }

    return new ExecutionResponse(200, result.toString());
  }

  /**
   * Number of vertices accumulated before they are created and committed in a single transaction. On a
   * replicated database that transaction is shipped as one Raft entry, so a load of large records has to lower
   * it to stay below the maximum replicated entry size - which is exactly what the server suggests when it
   * warns that an entry is approaching the limit (issue #5470).
   */
  private int parseVertexBatchSize(final HttpServerExchange exchange) {
    final String value = getQueryParameter(exchange, "vertexBatchSize");
    if (value == null)
      return VERTEX_BATCH_SIZE;

    final int vertexBatchSize = Integer.parseInt(value);
    if (vertexBatchSize < 1)
      throw new IllegalArgumentException("vertexBatchSize must be greater than 0, but was " + vertexBatchSize);
    return vertexBatchSize;
  }

  /**
   * Raises the connection read timeout for the duration of a streaming batch load and returns the previous
   * value so the caller can put it back (issue #5470).
   * <p>
   * Undertow arms an asynchronous watchdog that closes the connection when no {@code read()} is issued on the
   * request channel for {@code arcadedb.network.socketTimeout} milliseconds. On this endpoint the body is
   * consumed while the load runs, so that timer also counts the time the worker thread spends inside a commit:
   * a full index compaction or the replication of a large Raft entry easily blocks it for minutes and the
   * upload is killed halfway through with no way to tell the client. The watchdog is therefore given the
   * {@code arcadedb.server.httpStreamingReadTimeout} budget instead.
   * <p>
   * This does not weaken slow-client protection: {@code UndertowInputStream} captured the original timeout when
   * it was created (before this call) and applies it to every blocking read, so a client that stops sending is
   * still cut off after {@code arcadedb.network.socketTimeout}.
   *
   * @return the previous timeout to restore, or {@code null} when nothing was changed
   */
  private Integer relaxConnectionReadTimeout(final HttpServerExchange exchange) {
    final int streamingTimeout = httpServer.getServer().getConfiguration()
        .getValueAsInteger(GlobalConfiguration.SERVER_HTTP_STREAMING_READ_TIMEOUT);
    if (streamingTimeout <= 0)
      return null;

    try {
      final ServerConnection connection = exchange.getConnection();
      if (!connection.supportsOption(Options.READ_TIMEOUT))
        return null;

      final Integer previous = connection.getOption(Options.READ_TIMEOUT);
      // A previous value of 0/null means the watchdog is already disabled: leave it alone. Never lower a
      // timeout that is already more generous than the streaming budget.
      if (previous == null || previous <= 0 || previous >= streamingTimeout)
        return null;

      connection.setOption(Options.READ_TIMEOUT, streamingTimeout);
      return previous;
    } catch (final IOException | RuntimeException e) {
      LogManager.instance().log(this, Level.FINE,
          "Cannot raise the read timeout of the batch connection, a long server-side pause may abort the upload: %s",
          e.getMessage());
      return null;
    }
  }

  /**
   * Puts back the read timeout saved by {@link #relaxConnectionReadTimeout}: the connection is keep-alive and
   * the relaxed budget must not leak into the next request served on it.
   */
  private void restoreConnectionReadTimeout(final HttpServerExchange exchange, final Integer previous) {
    if (previous == null)
      return;

    try {
      exchange.getConnection().setOption(Options.READ_TIMEOUT, previous);
    } catch (final IOException | RuntimeException e) {
      LogManager.instance().log(this, Level.FINE, "Cannot restore the read timeout of the batch connection: %s",
          e.getMessage());
    }
  }

  private int flushVertexBatch(final GraphBatch batch, final String typeName,
      final List<Object[]> propsBatch, final List<String> tempIds, final Map<String, RID> tempIdMap) {

    final int count = propsBatch.size();
    final Object[][] propsArray = propsBatch.toArray(new Object[count][]);
    final RID[] rids = batch.createVertices(typeName, propsArray);

    for (int i = 0; i < count; i++) {
      final String tempId = tempIds.get(i);
      if (tempId != null)
        tempIdMap.put(tempId, rids[i]);
    }

    propsBatch.clear();
    tempIds.clear();
    return count;
  }

  private void processEdge(final GraphBatch batch, final BatchRecord rec, final Map<String, RID> tempIdMap,
      final int lineNumber) {
    final RID srcRID = resolveRef(rec.fromRef, tempIdMap, lineNumber);
    final RID dstRID = resolveRef(rec.toRef, tempIdMap, lineNumber);
    batch.newEdge(srcRID, rec.typeName, dstRID, rec.copyEdgeProperties());
  }

  private RID resolveRef(final String ref, final Map<String, RID> tempIdMap, final int lineNumber) {
    if (ref.charAt(0) == '#') {
      // Existing RID reference
      final int colonIdx = ref.indexOf(':');
      if (colonIdx < 0)
        throw new IllegalArgumentException("Malformed RID '" + ref + "' at line " + lineNumber);
      try {
        final int bucketId = Integer.parseInt(ref.substring(1, colonIdx));
        final long position = Long.parseLong(ref.substring(colonIdx + 1));
        return new RID(bucketId, position);
      } catch (final NumberFormatException e) {
        // Surface the handler's clear "Malformed RID" message instead of the raw JDK
        // "For input string: ..." NumberFormatException text (issue #5036 review).
        throw new IllegalArgumentException("Malformed RID '" + ref + "' at line " + lineNumber, e);
      }
    }

    // Temporary ID reference
    final RID rid = tempIdMap.get(ref);
    if (rid == null)
      throw new IllegalArgumentException("Unknown temporary ID '" + ref + "' at line " + lineNumber
          + ". Vertices must appear before edges that reference them");
    return rid;
  }

  private void configureBuilder(final HttpServerExchange exchange, final GraphBatch.Builder builder) {
    final String batchSize = getQueryParameter(exchange, "batchSize");
    if (batchSize != null)
      builder.withBatchSize(Integer.parseInt(batchSize));

    final String lightEdges = getQueryParameter(exchange, "lightEdges");
    if (lightEdges != null)
      builder.withLightEdges(Boolean.parseBoolean(lightEdges));

    final String wal = getQueryParameter(exchange, "wal");
    if (wal != null)
      builder.withWAL(Boolean.parseBoolean(wal));

    final String parallelFlush = getQueryParameter(exchange, "parallelFlush");
    if (parallelFlush != null)
      builder.withParallelFlush(Boolean.parseBoolean(parallelFlush));

    final String preAllocate = getQueryParameter(exchange, "preAllocateEdgeChunks");
    if (preAllocate != null)
      builder.withPreAllocateEdgeChunks(Boolean.parseBoolean(preAllocate));

    final String edgeListSize = getQueryParameter(exchange, "edgeListInitialSize");
    if (edgeListSize != null)
      builder.withEdgeListInitialSize(Integer.parseInt(edgeListSize));

    final String bidirectional = getQueryParameter(exchange, "bidirectional");
    if (bidirectional != null)
      builder.withBidirectional(Boolean.parseBoolean(bidirectional));

    final String commitEvery = getQueryParameter(exchange, "commitEvery");
    if (commitEvery != null)
      builder.withCommitEvery(Integer.parseInt(commitEvery));

    final String expectedEdgeCount = getQueryParameter(exchange, "expectedEdgeCount");
    if (expectedEdgeCount != null)
      builder.withExpectedEdgeCount(Integer.parseInt(expectedEdgeCount));

    final String commitRetries = getQueryParameter(exchange, "commitRetries");
    if (commitRetries != null)
      builder.withCommitRetries(Integer.parseInt(commitRetries));

    final String commitRetryDelayMs = getQueryParameter(exchange, "commitRetryDelayMs");
    if (commitRetryDelayMs != null)
      builder.withCommitRetryDelay(Long.parseLong(commitRetryDelayMs));
  }

  /**
   * Forwards the streaming batch payload to the cluster leader. Used when the request lands on
   * a follower: the bulk-load path mutates shared state (schema dictionary, type metadata)
   * that only the leader can safely serialize. Mirrors the engine-level forwarding already used
   * by {@code RaftReplicatedDatabase.command()} for SQL writes.
   */
  private ExecutionResponse forwardBatchToLeader(final HttpServerExchange exchange, final HAServerPlugin ha,
      final String databaseName, final ServerSecurityUser user, final String contentType) throws Exception {

    final String leaderAddress = ha.getLeaderAddress();
    if (leaderAddress == null || leaderAddress.isBlank())
      return new ExecutionResponse(503,
          "{ \"error\" : \"Cannot forward batch to leader: leader address is not available\"}");

    final String clusterToken = ha.getClusterToken();
    if (clusterToken == null || clusterToken.isBlank())
      return new ExecutionResponse(503,
          "{ \"error\" : \"Cannot forward batch to leader: cluster token is not configured\"}");

    if (user == null || user.getName() == null || user.getName().isBlank())
      return new ExecutionResponse(401,
          "{ \"error\" : \"Cannot forward batch to leader: no authenticated user in the current security context\"}");

    String url = "http://" + leaderAddress + "/api/v1/batch/" + databaseName;
    final String queryString = exchange.getQueryString();
    if (queryString != null && !queryString.isEmpty())
      url += "?" + queryString;

    final InputStream body = exchange.getInputStream();
    final HttpRequest.Builder builder = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .header("Content-Type", contentType)
        .header("X-ArcadeDB-Cluster-Token", clusterToken)
        .header("X-ArcadeDB-Forwarded-User", user.getName())
        .POST(HttpRequest.BodyPublishers.ofInputStream(() -> body));

    try {
      final HttpResponse<String> response = HTTP_CLIENT.send(builder.build(), HttpResponse.BodyHandlers.ofString());
      return new ExecutionResponse(response.statusCode(), response.body());
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      LogManager.instance().log(this, Level.WARNING, "Interrupted while forwarding /batch to leader at %s", leaderAddress);
      return new ExecutionResponse(503,
          "{ \"error\" : \"Interrupted while forwarding batch to leader\"}");
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Error forwarding /batch to leader at %s: %s", leaderAddress, e.getMessage());
      return new ExecutionResponse(503,
          "{ \"error\" : \"Error forwarding batch to leader: " + e.getMessage().replace("\"", "'") + "\"}");
    }
  }
}
