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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.batch.BatchRecord;
import com.arcadedb.server.http.handler.batch.BatchRecordStream;
import com.arcadedb.server.http.handler.batch.CsvBatchRecordStream;
import com.arcadedb.server.http.handler.batch.JsonlBatchRecordStream;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderValues;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
 */
public class PostBatchHandler extends AbstractServerHttpHandler {

  private static final int VERTEX_BATCH_SIZE = 10_000;

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

    final DatabaseInternal database = httpServer.getServer().getDatabase(databaseParam.getFirst(), false, false);

    // Determine format from Content-Type
    final HeaderValues contentTypeHeader = exchange.getRequestHeaders().get("Content-Type");
    final String contentType = contentTypeHeader != null && !contentTypeHeader.isEmpty()
        ? contentTypeHeader.getFirst().toLowerCase()
        : "application/x-ndjson";

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
      return new RID(null, bucketId, position);
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
  }
}
