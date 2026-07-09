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
import io.undertow.util.HeaderValues;

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
 * Atomicity: a batch is NOT atomic. GraphBatch commits every {@code commitEvery} records, so a
 * failure mid-stream leaves earlier chunks durably committed. On a client-input error the response
 * carries {@code verticesCreated} / {@code edgesCreated} and a {@code partialCommit} flag; because
 * temporary {@code @id}s are not keys, blindly retrying the whole payload duplicates the
 * already-committed vertices. Those counts are the records <em>attempted</em> before the failure, an
 * upper bound on what is durable: records handled since the last {@code commitEvery} boundary are
 * rolled back, so a client reconciling against them should treat them as "at most this many".
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
    GraphBatch.Builder builder = database.batch();
    configureBuilder(exchange, builder);

    final long startTime = System.currentTimeMillis();
    long verticesCreated = 0;
    long edgesCreated = 0;
    final Map<String, RID> tempIdMap = new HashMap<>();

    // Start streaming input
    final InputStream inputStream = exchange.getInputStream();

    try (final BatchRecordStream stream = isCsv
        ? new CsvBatchRecordStream(inputStream)
        : new JsonlBatchRecordStream(inputStream);
         final GraphBatch batch = builder.build()) {

      // Phase 1: Vertices — accumulate by type for batch creation
      String currentTypeName = null;
      List<Object[]> vertexPropsBatch = new ArrayList<>(VERTEX_BATCH_SIZE);
      List<String> vertexTempIds = new ArrayList<>(VERTEX_BATCH_SIZE);

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

        if (vertexPropsBatch.size() >= VERTEX_BATCH_SIZE) {
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

      final JSONObject error = new JSONObject();
      error.put("error", message);
      error.put("verticesCreated", verticesCreated);
      error.put("edgesCreated", edgesCreated);
      error.put("partialCommit", verticesCreated > 0 || edgesCreated > 0);
      return new ExecutionResponse(400, error.toString());
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
      final int bucketId = Integer.parseInt(ref.substring(1, colonIdx));
      final long position = Long.parseLong(ref.substring(colonIdx + 1));
      return new RID(bucketId, position);
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
