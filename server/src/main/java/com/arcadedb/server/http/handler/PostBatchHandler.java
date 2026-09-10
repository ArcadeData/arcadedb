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
import com.arcadedb.server.HAReplicatedDatabase;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.LeaderForwardContext;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.batch.BatchRecord;
import com.arcadedb.server.http.handler.batch.BatchRecordStream;
import com.arcadedb.server.http.handler.batch.CsvBatchRecordStream;
import com.arcadedb.server.http.handler.batch.JsonlBatchRecordStream;
import com.arcadedb.server.http.handler.batch.MalformedBatchRecordException;
import com.arcadedb.server.http.handler.batch.OrdinalVertexRefResolver;
import com.arcadedb.server.http.handler.batch.TempIdVertexRefResolver;
import com.arcadedb.server.http.handler.batch.VertexRefResolver;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.ServerConnection;
import io.undertow.util.HeaderValues;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;
import org.xnio.Options;

import java.io.BufferedReader;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
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
 * edges reference via @from/@to, or - with {@code refMode=ordinal} - be referenced by their position in the
 * payload, which is what the largest loads want. Edges can also reference existing database RIDs (#bucket:pos).
 * <p>
 * Read timeout: the body is consumed while the load runs, so the pauses the worker thread takes inside a commit
 * (index compaction, replication of a large entry) count towards Undertow's read watchdog. For the duration of
 * the request that watchdog is therefore raised to {@code arcadedb.server.httpStreamingReadTimeout} instead of
 * {@code arcadedb.network.socketTimeout}, which still bounds every single blocking read and so keeps cutting off
 * a client that stops sending (issue #5470). A body that ends early is answered with HTTP 408 and the
 * partial-commit counts, never with a 200 carrying a truncated count: both when the read fails outright and when
 * the body simply stops before the announced {@code Content-Length}, which is what a fixed-length upload cut by a
 * proxy looks like from here. Every response carries {@code bytesRead} for the same reason.
 * <p>
 * Response: {@code verticesCreated}, {@code edgesCreated}, {@code elapsedMs}, {@code bytesRead} and, when temporary
 * ids were used, {@code idMapping} - replaced by {@code idMappingOmitted} / {@code idMappingSize} past
 * {@link #MAX_ID_MAPPING_IN_RESPONSE} entries, unless {@code idMapping=true} demands it. On the streaming
 * encoding the mapping is not in this object at all: it travelled in the acknowledgements, and the terminal line
 * says {@code idMappingStreamed} with the {@code idMappingSize} to check the received pieces against
 * (issue #7353).
 * <p>
 * Streaming response ({@code Accept: application/x-ndjson}): the answer above is a single object written after the
 * whole body has been consumed, so a caller learns nothing about chunk <i>n</i> until chunk <i>n+1</i> and every
 * chunk after it has been sent. That is the one half of the gRPC {@code InsertBidirectional} shape HTTP had no
 * counterpart for (issue #7311). A caller that sends {@code Accept: application/x-ndjson} instead receives a
 * newline-delimited stream, written while its own upload is still being read:
 * <ul>
 * <li>{@code {"progress": {...}}} - emitted at every vertex commit and every {@code commitEvery} edges, carrying
 *     the same counters the final answer carries, plus {@code phase} ({@code vertices} or {@code edges}) and
 *     {@code idMapping}: the temporary ids THIS chunk resolved, and only those. Concatenating them yields what
 *     the buffered encoding returns in one object, which is the point - neither end ever holds the mapping of
 *     the whole load, and the {@link #MAX_ID_MAPPING_IN_RESPONSE} cap that exists because the buffered encoding
 *     does has no counterpart here (issue #7353);</li>
 * <li>{@code {"summary": {...}}} - the last line of a successful load. The object the buffered encoding would
 *     have sent, produced by the same code, plus {@code commitIndex} on a replicated database, and with
 *     {@code idMappingStreamed} / {@code idMappingSize} where that object carries the mapping itself;</li>
 * <li>{@code {"error": {...}}} - the last line of a failed one: the object the buffered encoding would have sent,
 *     plus the {@code status} it would have sent it under. A 200 is already on the wire by then and cannot be
 *     taken back, so the status travels in band and the line is the only terminator a consumer gets - a stream
 *     that ends with neither {@code summary} nor {@code error} did not arrive whole. An engine failure raised
 *     after the stream started adds {@code statusMapped: false}, because the fine-grained status the buffered
 *     encoding would have chosen is decided by a classifier this path cannot reach without copying it
 *     (issue #7396); the {@code exception} class is the discriminator there, and the counters and the bookmark
 *     travel as they do on every other failure.</li>
 * </ul>
 * A progress line is an upper bound on what is durable, exactly like the partial-commit counters below: vertices
 * are committed at each flush, but {@code GraphBatch} buffers edges and writes them at close, so an edge-phase
 * line counts records ACCEPTED. Writing the response while the request is still being read is full duplex over
 * one connection, and the cost of that is not flat: the lines accumulate with the size of the load - roughly one
 * ~200-byte line per {@code vertexBatchSize} records - so a client that uploads millions of records without
 * reading anything until it has finished can eventually fill the response socket buffer and block the worker
 * thread mid-write, which stops it reading the upload too. An ordinary client that reads while it writes never
 * meets this, and a small load cannot reach it at all; bounding it for the large ones is issue #7388.
 * <p>
 * A request that does not negotiate the encoding - no {@code Accept}, another type,
 * or {@code application/x-ndjson;q=0} - receives the same bytes under the same status as before. The
 * {@code X-ArcadeDB-Commit-Index} bookmark (issue #5862) cannot be a header on this encoding because the response
 * has already started when the value becomes known, so it is carried inside the terminal line instead.
 * <p>
 * Line accounting: every answer, successful or not, also carries {@code linesRead} and {@code linesSkipped} (blank
 * lines, plus CSV headers and {@code ---} separators), so {@code linesRead - linesSkipped} is the number of records
 * the parser produced and can be checked against {@code verticesCreated + edgesCreated} - and against the line count
 * of the file the client sent. The server checks it too: a load that read a line and turned it into nothing is
 * answered as failed rather than successful, because "the vertices are in my file and not in the database" must not
 * be something a user has to establish with grep after the fact (issue #5618). {@code verticesWithoutId} appears when
 * the payload created vertices that declared no {@code @id} under {@code refMode=id}: they are loaded and durable,
 * but no edge can ever reference them.
 * <p>
 * Giving up early: a load that fails on the payload is answered as soon as the verdict is reached, without reading
 * the rest of the upload. That is not an optimisation - closing Undertow's request stream reads the body to the end
 * first, so waiting meant a 25M-line load sat silent for fifteen minutes and then could not be answered at all
 * ({@code UT000002}), turning an exact diagnosis into an unexplained hang (issue #5470). The connection is marked
 * non-persistent instead, and only a record that failed to PARSE is checked against
 * {@link #bodyEndedEarly} first, since a cut body can fabricate one of those; a well-formed record with invalid
 * content is final and reported at once. See {@link CountingInputStream#close()} and
 * {@link com.arcadedb.server.http.handler.batch.MalformedBatchRecordException}.
 * <p>
 * The trade-off that buys: declining to read a body means closing a connection the client may still be writing to,
 * and the TCP reset that follows can discard bytes the peer had already received - the response among them. A client
 * that is no longer mid-upload always gets the error; one that reads while it uploads (any ordinary HTTP client)
 * usually does, though not reliably - measured at about four times in five against the JDK client - and one that
 * writes its whole payload before reading anything sees the reset instead. That is the better failure: it is immediate rather than a quarter of an
 * hour, it cannot be mistaken for success, and the exact reason is in the server log either way. The alternative -
 * reading a multi-gigabyte remainder to keep the socket well-mannered - is the bug this replaced, and it pinned a
 * worker thread for the duration.
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
 * - commitEvery (int, default 50000): records written per transaction during an edge flush. It is the edge-phase
 *   counterpart of {@code vertexBatchSize} below, so on a replicated database it bounds the size of the Raft
 *   entry produced by a flush
 * - expectedEdgeCount (int, default 0)
 * - commitRetries (int, default 10): retries of a vertex-creation commit that fails with a
 *   transient retryable error (e.g. a Raft leader re-election), so a cluster hiccup does not
 *   abort the whole streaming load (issue #4724)
 * - commitRetryDelayMs (long, default 1000): initial back-off before the first retry
 * - vertexBatchSize (int, default 10000): vertices accumulated before they are created and committed in
 *   one transaction. On a replicated database that transaction becomes a single Raft entry, so this is the
 *   knob to lower when the server warns that a replicated entry approaches the maximum entry size
 *   (issue #5470); on an embedded/standalone database it only trades memory for throughput
 * - idMapping (auto|true|false, default auto): whether the response returns the temporary-id to RID mapping. On
 *   the buffered encoding it is echoed in the terminal object under a size cap - see {@link #echoIdMapping}; on
 *   the streaming one it is handed back one committed chunk at a time and there is no cap, because nothing is
 *   ever built that a cap would protect - see {@link #streamIdMapping} (issue #7353)
 * - refMode (id|ordinal, default id): how edges name the vertices they connect. {@code id} resolves the
 *   {@code @from} / {@code @to} against the {@code @id} each vertex declared, which costs the id itself plus a hash
 *   slot for every vertex of the request; {@code ordinal} resolves them against the 0-based POSITION of the vertex
 *   in the payload, which stores no id at all - 12 bytes per vertex and an array read per edge instead of a hash
 *   lookup. On a load of millions of vertices that is the difference between ~1.4GB and ~190MB of heap held until
 *   the last edge is written (issue #5470)
 * - expectedVertexCount (int, default 0): hint used to pre-size the vertex references, saving the copies of their
 *   growth. Only a hint: the payload may carry more
 * - expectedRecords (long, default none): how many records (vertices plus edges) the payload carries. When given,
 *   a load that ends with a different count is reported as incomplete instead of successful - the only way to catch
 *   a chunked upload that stopped early, since a chunked body announces no length
 * - ordinalBase (long, default 0): with {@code refMode=ordinal}, the position of the FIRST vertex of this payload.
 *   A client that splits one load into several requests keeps a single counter across all of them, so the second
 *   request starts where the first stopped; positions below the base belong to an earlier request and have to be
 *   referenced by RID
 */
public class PostBatchHandler extends AbstractServerHttpHandler {

  private static final int        VERTEX_BATCH_SIZE     = 10_000;
  /** Value of the {@code phase} field of a progress line while vertices are being committed. */
  private static final String     VERTEX_PHASE          = "vertices";
  /** Value of the {@code phase} field of a progress line while edges are being accepted. */
  private static final String     EDGE_PHASE            = "edges";
  /**
   * Above this many temporary ids the mapping is not echoed back. The response would otherwise hold a second full
   * copy of the map, as JSON, in one string: a bulk load of millions of vertices turns the last step of a successful
   * import into an OutOfMemoryError, and no client streaming that many records can consume a multi-GB object anyway
   * (issue #5470).
   */
  private static final int        MAX_ID_MAPPING_IN_RESPONSE = 10_000;
  /** Temporary ids mapped before the first heap warning; every following warning doubles the threshold. */
  private static final int        ID_MAP_WARNING_THRESHOLD   = 250_000;
  /** Ceiling for the {@code expectedVertexCount} hint, so a wrong one costs a doubling and not the whole heap. */
  private static final int        MAX_PRESIZED_VERTICES      = 1 << 24;
  /** How far the tail of a short body is drained before deciding it is a live client rather than a cut upload. */
  private static final int        TRUNCATION_PROBE_BYTES     = 64 * 1024;
  /**
   * How much of an abandoned request body is consumed to keep its connection reusable. Only bytes that have already
   * arrived are read (see {@link CountingInputStream#close()}), so this bounds work, not waiting: past it the
   * connection is retired instead, because a bulk upload's remainder is not worth a keep-alive.
   */
  private static final int        MAX_ABANDONED_BODY_DRAIN   = 64 * 1024;
  private static final HttpClient HTTP_CLIENT                = HttpClient.newHttpClient();

  /**
   * Emits the "a peer relayed a batch here and this node is not the leader either" notice only once (issue
   * #6191). Per handler instance, so each server in an in-process cluster still gets to say it once.
   */
  private final AtomicBoolean forwardedAgainWarned = new AtomicBoolean(false);

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
  protected boolean supportsNdJsonEncoding() {
    return true;
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

    // Response encoding, negotiated exactly the way #7306 negotiated the streaming query. Read before any work
    // starts because it decides how EVERY answer below is written, the leader-forwarding one included.
    final boolean streaming = isNdJsonRequested(exchange);

    // Start streaming input. The stream must be created BEFORE relaxing the connection watchdog below:
    // UndertowInputStream captures the read timeout in effect at construction time and uses it to bound
    // every blocking read, so a client that stops sending is still cut off after
    // 'arcadedb.network.socketTimeout' while the asynchronous watchdog moves to the streaming budget.
    // The counter is what tells a body that ended early from one that ended (issue #5470): not every premature
    // end of a request body surfaces as an IOException, and a load that silently stops half-way must never be
    // answered with a 200.
    final CountingInputStream inputStream = new CountingInputStream(exchange, exchange.getInputStream());

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
        return forwardBatchToLeader(exchange, ha, databaseName, user, contentType, inputStream, streaming);

      final DatabaseInternal database = httpServer.getServer().getDatabase(databaseName, false, false);
      final boolean isCsv = contentType.contains("text/csv");

      // Configure GraphBatch from query parameters
      final GraphBatch.Builder builder = database.batch();
      configureBuilder(exchange, builder);

      // GraphBatch commits incrementally (commitEvery), so unlike every other write endpoint the
      // READ_YOUR_WRITES bookmark has to be emitted even when the load ends in a partial-commit error
      // (408/400): whatever chunk got through is already durable, and a client must be able to read it
      // back regardless of how the request itself was answered (issue #5862).
      final HAReplicatedDatabase haDb = resolveHAReplicatedDatabase(database);

      // Every query parameter is parsed BEFORE the streaming encoding writes anything, so a request that names
      // an invalid refMode or a negative vertexBatchSize is still refused with the 400 the buffered encoding
      // gives it. Once a line is on the wire the status code can no longer be chosen (issue #7311).
      final VertexRefResolver vertexRefs = newVertexRefResolver(exchange);
      final int vertexBatchSize = parseVertexBatchSize(exchange);
      final long expectedRecords = parseExpectedRecords(exchange);

      if (streaming)
        return streamRecordsAsNdJson(exchange, databaseName, isCsv, builder, inputStream, vertexRefs,
            vertexBatchSize, expectedRecords, haDb);

      try {
        // No mapping sink: the buffered encoding has nowhere to put a chunk of it before the end, so it keeps
        // echoing the whole mapping in the terminal object under the MAX_ID_MAPPING_IN_RESPONSE cap.
        return streamRecords(exchange, databaseName, isCsv, builder, inputStream, vertexRefs,
            System.currentTimeMillis(), vertexBatchSize, expectedRecords, BatchProgressSink.NONE, null);
      } finally {
        emitCommitIndexBookmark(exchange, haDb);
      }
    } finally {
      restoreConnectionReadTimeout(exchange, previousReadTimeout);
    }
  }

  /**
   * Consumes the streaming request body and feeds it to a {@link GraphBatch}. Extracted from
   * {@link #execute} so the caller can restore the connection read timeout in a {@code finally} block.
   *
   * @param progress notified at every commit boundary, so the NDJSON encoding can acknowledge a chunk while the
   *                 client is still uploading the next one (issue #7311).
   *                 {@link BatchProgressSink#NONE} on the buffered encoding, which is what keeps that answer
   *                 byte-identical: the same code produces it, and nothing else in this method knows the
   *                 difference
   */
  private ExecutionResponse streamRecords(final HttpServerExchange exchange, final String databaseName,
      final boolean isCsv, final GraphBatch.Builder builder, final CountingInputStream inputStream,
      final VertexRefResolver vertexRefs, final long startTime, final int vertexBatchSize,
      final long expectedRecords, final BatchProgressSink progress,
      final VertexRefResolver.EntryConsumer mappingSink) throws Exception {

    long verticesCreated = 0;
    long edgesCreated = 0;

    // Held outside the try-with-resources so the line accounting survives into the catch blocks: a load that
    // failed has to report how much of the payload it had read just as precisely as one that succeeded, which is
    // the whole point of counting the lines (issue #5618).
    final BatchRecordStream stream = isCsv
        ? new CsvBatchRecordStream(inputStream)
        : new JsonlBatchRecordStream(inputStream);

    try (stream; final GraphBatch batch = builder.build()) {

      // Phase 1: Vertices — accumulate by type for batch creation
      String currentTypeName = null;
      final List<Object[]> vertexPropsBatch = new ArrayList<>(vertexBatchSize);
      final List<String> vertexTempIds = new ArrayList<>(vertexBatchSize);
      int nextIdMapWarning = ID_MAP_WARNING_THRESHOLD;

      while (stream.hasNext()) {
        // The vertex references live for the whole request: edges arriving at the end of the payload may reference
        // any vertex loaded at the beginning, so nothing can be discarded early. Their size is therefore the memory
        // ceiling of a streaming load, and a load big enough to hit it must say so in the log rather than die of an
        // OutOfMemoryError that leaves no trace but a closed connection (issue #5470).
        if (vertexRefs.size() >= nextIdMapWarning) {
          LogManager.instance().log(this, Level.WARNING,
              "Batch load on database '%s' has mapped %d vertices so far, holding %d MB of heap that cannot be "
                  + "released before the end of the request. Make sure the server heap is sized for it, or use "
                  + "'refMode=ordinal' so the vertices are referenced by their position in the payload (12 bytes "
                  + "each, no id to store)",
              null, databaseName, vertexRefs.size(), vertexRefs.retainedBytes() / (1024 * 1024));
          nextIdMapWarning *= 2;
        }

        final BatchRecord rec = stream.next();

        if (rec.kind == BatchRecord.Kind.EDGE) {
          // Transition to edge phase: flush remaining vertices
          if (!vertexPropsBatch.isEmpty()) {
            verticesCreated += flushVertexBatch(batch, currentTypeName, vertexPropsBatch, vertexTempIds, vertexRefs,
                verticesCreated, mappingSink);
            progress.chunk(VERTEX_PHASE, verticesCreated, edgesCreated, stream, vertexRefs, inputStream);
          }

          // Process this first edge record
          processEdge(batch, rec, vertexRefs, stream.getLineNumber());
          edgesCreated++;
          break;
        }

        // A payload that does not match the chosen refMode must fail on the vertex that breaks it, not later on an
        // edge that cannot be resolved: the line number is what the client needs to fix its generator.
        vertexRefs.checkVertexId(rec.tempId, (int) (verticesCreated + vertexPropsBatch.size()), stream.getLineNumber());

        // Accumulate vertex — flush when type changes or batch is full
        if (currentTypeName != null && !currentTypeName.equals(rec.typeName)) {
          verticesCreated += flushVertexBatch(batch, currentTypeName, vertexPropsBatch, vertexTempIds, vertexRefs,
              verticesCreated, mappingSink);
          progress.chunk(VERTEX_PHASE, verticesCreated, edgesCreated, stream, vertexRefs, inputStream);
        }
        currentTypeName = rec.typeName;
        vertexPropsBatch.add(rec.copyProperties());
        vertexTempIds.add(rec.tempId);

        if (vertexPropsBatch.size() >= vertexBatchSize) {
          verticesCreated += flushVertexBatch(batch, currentTypeName, vertexPropsBatch, vertexTempIds, vertexRefs,
              verticesCreated, mappingSink);
          progress.chunk(VERTEX_PHASE, verticesCreated, edgesCreated, stream, vertexRefs, inputStream);
        }
      }

      // Flush remaining vertices (e.g., vertex-only import or last batch before EOF)
      if (!vertexPropsBatch.isEmpty()) {
        verticesCreated += flushVertexBatch(batch, currentTypeName, vertexPropsBatch, vertexTempIds, vertexRefs,
            verticesCreated, mappingSink);
        progress.chunk(VERTEX_PHASE, verticesCreated, edgesCreated, stream, vertexRefs, inputStream);
      }

      // Edge-phase cadence for the progress stream. commitEvery is what GraphBatch writes per transaction during
      // an edge flush, so it is the closest thing the edge phase has to the vertex phase's commit boundary; when
      // it is 0 the whole flush commits at once and there is no boundary at all, so the vertex cadence is reused
      // rather than emitting nothing for the entire edge phase (issue #7311).
      final int progressEveryEdges = batch.getCommitEvery() > 0 ? batch.getCommitEvery() : vertexBatchSize;
      // Seeded with the edge the vertex loop already consumed on its way out, so the first cadence window is a
      // full one rather than one record short.
      long edgesSinceProgress = edgesCreated;

      // Phase 2: Remaining edges
      while (stream.hasNext()) {
        final BatchRecord rec = stream.next();
        if (rec.kind != BatchRecord.Kind.EDGE)
          throw new IllegalArgumentException("Expected edge record but got vertex at line " + stream.getLineNumber()
              + ". All vertices must appear before edges");
        processEdge(batch, rec, vertexRefs, stream.getLineNumber());
        edgesCreated++;
        if (++edgesSinceProgress >= progressEveryEdges) {
          progress.chunk(EDGE_PHASE, verticesCreated, edgesCreated, stream, vertexRefs, inputStream);
          edgesSinceProgress = 0;
        }
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

      // A cut upload does not always look like an I/O error from up here: Undertow can hand the parser stale bytes
      // from its own connection buffer once the peer is gone, which surfaces as a "malformed record" on a line the
      // client never sent. Answering 400 there sends the user hunting through a file that is perfectly valid, so
      // the missing bytes are checked first (issue #5470).
      //
      // Only for a record that failed to PARSE, though. A well-formed record carrying invalid content - a temporary
      // id no vertex declared, an unparseable RID, a vertex after the first edge - is a final verdict: the record
      // was read in full, so no number of further bytes can make it valid, and the client is answered at once. That
      // distinction is the difference between naming the offending line and reporting the load as truncated with
      // "the last record read is not part of the payload", which was untrue and unfixable: the line WAS in the file.
      if (e instanceof MalformedBatchRecordException && bodyEndedEarly(exchange, inputStream))
        return truncatedBody(exchange, databaseName, verticesCreated, edgesCreated, stream, vertexRefs, inputStream,
            "only " + inputStream.getBytesRead() + " of the " + exchange.getRequestContentLength()
                + " announced bytes were received, the last record read (" + message + ") is not part of the payload",
            null);

      LogManager.instance().log(this, Level.WARNING,
          "Batch load on database '%s' failed on line %d after %d vertices and %d edges (%d lines read, %d skipped): %s",
          null, databaseName, stream.getLineNumber(), verticesCreated, edgesCreated, stream.getLinesRead(),
          stream.getLinesSkipped(), message);

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
      addLineAccounting(error, stream, vertexRefs, inputStream);
      return new ExecutionResponse(400, error.toString());
    } catch (final IOException e) {
      // The request body could not be read to the end: the client went away, a proxy cut the upload, or the
      // connection watchdog fired because the server spent longer than its budget committing instead of
      // reading (issue #5470). Never let this look like a completed load: reaching the end of the loop with a
      // truncated body would answer 200 with a partial count and the client would happily move on.
      final String message = e.getMessage() != null ? e.getMessage() : e.toString();
      return truncatedBody(exchange, databaseName, verticesCreated, edgesCreated, stream, vertexRefs, inputStream,
          message, e.getClass().getName());
    }

    // The stream ended without an error, but the client announced more than what arrived: a connection dropped
    // between two chunks does not always surface as an IOException (a fixed-length body simply reaches EOF), and
    // reporting 200 with a truncated count is the worst possible outcome - the client moves on believing the load
    // completed.
    if (bodyEndedEarly(exchange, inputStream))
      return truncatedBody(exchange, databaseName, verticesCreated, edgesCreated, stream, vertexRefs, inputStream,
          "only " + inputStream.getBytesRead() + " of the " + exchange.getRequestContentLength()
              + " announced bytes were received", null);

    // Every line the parser consumed has to have become a record or be one it declared skipped. Nothing in this
    // handler is allowed to read a line and quietly do nothing with it, so a mismatch is a server-side defect,
    // not a client one - and answering 200 with a count that silently misses records is exactly the failure
    // issue #5618 was opened about. The check costs two field reads per load and turns "vertices disappeared" from
    // something the user has to prove with grep into something the server states with numbers.
    final long accounted = verticesCreated + edgesCreated + stream.getLinesSkipped();
    if (accounted != stream.getLinesRead())
      return recordsDropped(exchange, databaseName, verticesCreated, edgesCreated, stream, vertexRefs, inputStream);

    // A chunked upload announces no length, so the only thing that can prove it arrived whole is the client saying
    // how much it was going to send. Without it a body that ends early - because the producer feeding the stream
    // stopped, not because the connection broke - is indistinguishable from a complete one and is answered 200 with
    // a partial count, which is how a load can silently import a fraction of a file (issue #5470).
    final long records = verticesCreated + edgesCreated;
    if (expectedRecords >= 0 && records != expectedRecords)
      return recordCountMismatch(exchange, databaseName, verticesCreated, edgesCreated, expectedRecords, records,
          stream, vertexRefs, inputStream);

    final long elapsed = System.currentTimeMillis() - startTime;

    final JSONObject result = new JSONObject();
    result.put("verticesCreated", verticesCreated);
    result.put("edgesCreated", edgesCreated);
    result.put("elapsedMs", elapsed);
    addLineAccounting(result, stream, vertexRefs, inputStream);

    // Include temp ID mapping if any temp IDs were used, unless the load was too big for the mapping to be worth
    // (or even possible to) send back - see MAX_ID_MAPPING_IN_RESPONSE and the 'idMapping' parameter.
    if (!vertexRefs.isEmpty()) {
      if (mappingSink != null) {
        // The mapping has been travelling one committed chunk at a time since the load started, so putting it
        // here as well would rebuild in this single object the very thing streaming it was for. What the
        // terminal line owes the client is the count, so it can check it received all of it (issue #7353).
        result.put("idMappingStreamed", true);
        result.put("idMappingSize", vertexRefs.size());
      } else if (echoIdMapping(exchange, vertexRefs.size())) {
        final JSONObject mapping = new JSONObject();
        vertexRefs.forEach((ref, rid) -> mapping.put(ref, rid.toString()));
        result.put("idMapping", mapping);
      } else {
        result.put("idMappingOmitted", true);
        result.put("idMappingSize", vertexRefs.size());
      }
    }

    return new ExecutionResponse(200, result.toString());
  }

  /**
   * Notified at every commit boundary of a load, so a response encoding that can say something before the end
   * of the request has something to say (issue #7311). {@link #NONE} on the buffered encoding, which is how the
   * answer that encoding produces stays byte-identical: one code path, one set of counters, and the only
   * difference is whether anybody is listening.
   */
  @FunctionalInterface
  interface BatchProgressSink {
    /** Ignores every boundary. The buffered encoding, and every path that predates the streaming one. */
    BatchProgressSink NONE = (phase, verticesCreated, edgesCreated, stream, vertexRefs, inputStream) -> {
    };

    /**
     * Deliberately declares no checked exception. {@link #streamRecords} answers an {@link IOException} as a
     * truncated REQUEST body, with the counts a client resumes from, so an implementation that let one escape
     * from writing the RESPONSE would produce a precise and completely wrong diagnosis. Not being able to throw
     * one is what stops the next implementation from reintroducing that.
     */
    void chunk(String phase, long verticesCreated, long edgesCreated, BatchRecordStream stream,
        VertexRefResolver vertexRefs, CountingInputStream inputStream);
  }

  /**
   * Runs a load and answers it as newline-delimited JSON, writing progress lines to the client while its upload
   * is still being read - the acknowledgement half of the gRPC {@code InsertBidirectional} shape, which HTTP had
   * no counterpart for (issue #7311).
   * <p>
   * The load itself is {@link #streamRecords}, unchanged and shared with the buffered encoding: it still returns
   * the one {@link ExecutionResponse} it always returned, and this method turns that into the terminal line
   * rather than into a status line and a body. So the two encodings cannot disagree about what a load did - the
   * object is produced once, by the same code, and only the envelope around it differs.
   * <p>
   * <b>Why the response is started lazily.</b> Nothing is written until the first progress line, so a load that
   * fails before it commits anything is still answered with a real status code. That covers both shapes of
   * failure, which is the part that is easy to get wrong: an exception that PROPAGATES - a security failure, a
   * {@code DuplicatedKeyException} on the very first flush - is rethrown here and travels to
   * {@link AbstractServerHttpHandler}'s error mapping, and a failure {@link #streamRecords} REPORTS BY
   * RETURNING - a malformed record, an unknown temporary id, a body that ended early - is returned unchanged so
   * the pipeline sends it under the 400 or 408 it always carried. Only once a line is on the wire is the status
   * unrecoverable, and only then is a failure reported in band. That is the difference between a client seeing
   * 409 "do not retry" and seeing a 200 whose body it has to parse to discover the same thing.
   * <p>
   * <b>Full duplex on one socket.</b> This writes the response while {@link #streamRecords} reads the request.
   * HTTP/1.1 permits it and Undertow's blocking exchange supports it, but it does mean a client that never
   * reads could in principle fill its receive buffer and deadlock against a server that is not reading either.
   * It cannot happen at these volumes: one ~200-byte line per {@code vertexBatchSize} records (10,000 by
   * default) against a request body measured in megabytes, so the response drains many orders of magnitude
   * faster than it is produced.
   *
   * @return always {@code null} - the response is written here, which {@link AbstractServerHttpHandler#handleRequest}
   *         reads as "the handler sent it itself", the same contract the SSE paths use
   */
  private ExecutionResponse streamRecordsAsNdJson(final HttpServerExchange exchange, final String databaseName,
      final boolean isCsv, final GraphBatch.Builder builder, final CountingInputStream inputStream,
      final VertexRefResolver vertexRefs, final int vertexBatchSize, final long expectedRecords,
      final HAReplicatedDatabase haDb) throws Exception {

    final NdJsonBatchResponse response = new NdJsonBatchResponse(exchange);
    // Counters as of the last acknowledgement, so a failure that cannot reach streamRecords' own counters -
    // an engine exception raised after the stream started - still has something honest to report.
    final long[] lastProgress = new long[2];

    // The temporary-id mapping of the vertices committed since the last acknowledgement, and never more than
    // that: it is drained into every progress line and into the terminal one, so no line - and no server-side
    // object - ever holds the mapping of the whole load (issue #7353). Null when the client asked not to
    // receive the mapping at all, which is the only way to switch the accumulation off.
    final JSONObject[] pendingMapping = { streamIdMapping(exchange) ? new JSONObject() : null };
    final VertexRefResolver.EntryConsumer mappingSink = pendingMapping[0] == null
        ? null
        : (ref, rid) -> pendingMapping[0].put(ref, rid.toString());

    try {
      final ExecutionResponse unary = streamRecords(exchange, databaseName, isCsv, builder, inputStream, vertexRefs,
          System.currentTimeMillis(), vertexBatchSize, expectedRecords,
          (phase, verticesCreated, edgesCreated, stream, refs, in) -> {
            final JSONObject event = new JSONObject();
            event.put("phase", phase);
            event.put("verticesCreated", verticesCreated);
            event.put("edgesCreated", edgesCreated);
            addLineAccounting(event, stream, refs, in);
            drainPendingMapping(pendingMapping, event);
            lastProgress[0] = verticesCreated;
            lastProgress[1] = edgesCreated;
            try {
              // Forced, unlike a query row: a progress line exists to be read now, and there are few enough of
              // them that the syscall the size/interval policy is there to save does not matter here.
              response.open().writeEvent("progress", event, true);
            } catch (final IOException e) {
              // NOT allowed to surface as an IOException. streamRecords catches that and answers "the request
              // body was truncated" with the counts to resume from, which would be a precise, machine-parsable
              // and completely wrong diagnosis: the body arrived, it is the RESPONSE that could not be written.
              throw new BatchResponseWriteException(e);
            }
          }, mappingSink);

      if (unary.getCode() != 200 && !response.hasStarted()) {
        // A failure streamRecords REPORTS BY RETURNING - a malformed record, an unknown temporary id, a
        // truncated body - reaches this branch rather than the catch below, and when it happens before a single
        // acknowledgement has been written the status line is still ours to choose. Choosing 200 there would
        // contradict this method's own promise, the 400 and 408 the OpenAPI document declares for this
        // endpoint, and every client that keys on the status rather than parsing the body. So the buffered
        // answer is sent as-is, which also makes it byte-identical, and the bookmark goes back to being the
        // header it can still be (issue #7311, cycle 3 review).
        emitCommitIndexBookmark(exchange, haDb);
        return unary;
      }

      final JSONObject terminal = new JSONObject(unary.getResponse());
      // Whatever the last flush resolved after the final acknowledgement, so the client's mapping is complete
      // when it reads 'idMappingStreamed' and can check it against 'idMappingSize' (issue #7353). Normally
      // empty - every vertex flush is followed by an acknowledgement - and bounded by one flush when it is not.
      drainPendingMapping(pendingMapping, terminal);
      // The bookmark of issue #5862 cannot be a header on this encoding: by the time the commit index is known
      // the response has usually started, and a header set then is dropped without a word. It carries the same
      // meaning in band, on the same line as the counters a READ_YOUR_WRITES client is reconciling against.
      if (haDb != null) {
        final long lastApplied = haDb.getLastAppliedIndex();
        if (lastApplied >= 0)
          terminal.put("commitIndex", lastApplied);
      }

      if (unary.getCode() == 200)
        response.open().writeEvent("summary", terminal, true);
      else
        // The status the buffered encoding would have sent, in band because a 200 is already on the wire. Every
        // other field is what that encoding would have carried, so a client applies one piece of logic to both.
        response.open().writeEvent("error", terminal.put("status", unary.getCode()), true);
    } catch (final Throwable t) {
      if (!response.hasStarted())
        // Nothing has been sent: the exception can still be answered with a real status code, so let the
        // standard mapping in AbstractServerHttpHandler do exactly what it does for the buffered encoding.
        // response.close() below is a no-op in that state, deliberately - closing the output stream would
        // commit the very 200 this branch exists to avoid sending.
        throw t;

      final Throwable reported = t instanceof BatchResponseWriteException ? t.getCause() : t;
      LogManager.instance().log(this, Level.WARNING,
          "Streaming batch load on database '%s' failed after the response had already started", reported,
          databaseName);
      try {
        final JSONObject error = new JSONObject()
            .put("error", reported.getMessage() != null ? reported.getMessage() : reported.toString())
            .put("exception", reported.getClass().getName())
            // 500 is the unclassified fallback, NOT a classification. The fine-grained mapping - 409 for a
            // duplicated key, 503 for a retryable conflict, 403, 404 - lives in
            // AbstractServerHttpHandler.sendMappedErrorResponse, whose javadoc records that hand-written
            // mirrors of it produced six separate bugs, so a second copy is not made here. The flag says so
            // outright and the exception class is the discriminator a client keys on instead. Making the
            // in-band status exact means extracting that classifier so there is still exactly one: issue #7396.
            .put("status", 500)
            .put("statusMapped", false);
        // What a client reconciles with, and what the buffered encoding still delivers for this same failure:
        // its counters travel in the error body, and its bookmark is emitted by the finally in execute(). Both
        // were dropped here, which is the one place this encoding was worse than the one it extends
        // (cycle 3 review). The counters are as of the last acknowledgement, which is the same upper bound on
        // what is durable that every other count on this endpoint carries.
        error.put("verticesCreated", lastProgress[0]);
        error.put("edgesCreated", lastProgress[1]);
        error.put("partialCommit", lastProgress[0] > 0 || lastProgress[1] > 0);
        if (haDb != null) {
          final long lastApplied = haDb.getLastAppliedIndex();
          if (lastApplied >= 0)
            error.put("commitIndex", lastApplied);
        }
        response.open().writeEvent("error", error, true);
      } catch (final IOException writeFailed) {
        // The connection that could not carry the load cannot carry the explanation either. Nothing is left to
        // tell the client with; the stream simply ends without a terminal line, which is exactly how a consumer
        // recognises an answer that did not arrive whole.
        LogManager.instance().log(this, Level.FINE,
            "Could not write the in-band failure of a streaming batch load on database '%s': %s", null, databaseName,
            writeFailed.getMessage());
      }
    } finally {
      try {
        response.close();
      } catch (final IOException e) {
        // Nothing is left that this could tell anyone. The terminal line is either on the wire or it is not,
        // and letting a close failure out here would replace the answer just written - or, on the rethrow
        // branch above, the exception that still had a status code to be answered with.
        LogManager.instance().log(this, Level.FINE,
            "Could not close the streamed answer of a batch load on database '%s': %s", null, databaseName,
            e.getMessage());
      }
    }
    return null;
  }

  /**
   * A failure writing the streamed RESPONSE, kept distinct from a failure reading the request body. Unchecked and
   * of its own type on purpose: {@link #streamRecords} catches {@link IOException} and answers it as a truncated
   * upload, with the counts a client needs to resume from - a diagnosis that would be precise, machine-parsable
   * and about the wrong end of the connection.
   */
  private static final class BatchResponseWriteException extends RuntimeException {
    private BatchResponseWriteException(final IOException cause) {
      super(cause);
    }
  }

  /**
   * The NDJSON response of a batch load, which does not exist until it has something to say.
   * <p>
   * Undertow commits the status line and headers on the first flushed byte, so deferring the whole set-up until
   * the first line is what keeps the standard error mapping available to a load that fails before it produces
   * one. {@link #close()} is a no-op on a response that never opened, for the same reason: closing the output
   * stream would send the 200 this class exists to avoid sending prematurely.
   */
  private static final class NdJsonBatchResponse implements AutoCloseable {
    private final HttpServerExchange   exchange;
    private       NdJsonResultStream   stream;

    private NdJsonBatchResponse(final HttpServerExchange exchange) {
      this.exchange = exchange;
    }

    private NdJsonResultStream open() {
      if (stream == null) {
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, NdJsonResultStream.CONTENT_TYPE);
        // A buffering reverse proxy would accumulate the stream and defeat the encoding without saying so. Same
        // pair of headers the streaming query and the SSE endpoints set.
        exchange.getResponseHeaders().put(Headers.CACHE_CONTROL, "no-cache");
        exchange.getResponseHeaders().put(X_ACCEL_BUFFERING, "no");
        exchange.setStatusCode(200);
        if (!exchange.isBlocking())
          exchange.startBlocking();
        stream = new NdJsonResultStream(exchange.getOutputStream());
      }
      return stream;
    }

    private boolean hasStarted() {
      return stream != null && stream.hasStarted();
    }

    /**
     * Ends the response, and only a response that exists. Closing the output stream of a load that opened this
     * and then failed before writing anything would commit an empty 200 and take away the status code that
     * failure was still entitled to.
     */
    @Override
    public void close() throws IOException {
      if (hasStarted())
        stream.close();
    }
  }

  /**
   * Whether the request body provably stopped before the announced {@code Content-Length}, i.e. whether the load
   * that just ended (successfully or on a malformed record) was working on a truncated payload.
   * <p>
   * Fewer bytes than announced is not enough on its own - a parse error also stops the reader early - so what the
   * server already holds is consumed to look for the end of the body: reaching it before the announced length proves
   * the rest will never arrive. A chunked upload announces no length and is not checked here: it fails loudly by
   * itself, because an unterminated chunked body IS an I/O error.
   * <p>
   * The probe never BLOCKS. What it is here to catch is a peer that has gone away while Undertow still had bytes of
   * its own buffered - the parser then fails on a line the client never sent - and a peer that has gone away is at
   * end of stream, which needs no waiting to observe. Blocking instead would mean waiting for bytes that, in the one
   * case where the answer matters, are never coming: on a client that is merely slow the handler would stall, and on
   * a client still uploading it would read up to the probe budget for nothing (issue #5470). So the loop stops at
   * the first read that would have to wait, and the load is then reported as what it looks like from here - a
   * payload the client really sent - rather than guessed to be truncated.
   */
  private boolean bodyEndedEarly(final HttpServerExchange exchange, final CountingInputStream inputStream) {
    final long declaredLength = exchange.getRequestContentLength();
    // A declared length only bounds the bytes ON THE WIRE, so with a Content-Encoding it says nothing about how
    // much payload the parser should have seen and this check would compare two different quantities. The endpoint
    // does not decode either - the body is handed to the parser as it arrives - so a compressed upload fails as a
    // malformed record or an I/O error, which is answered on its own terms.
    if (declaredLength < 0 || inputStream.getBytesRead() >= declaredLength
        || exchange.getRequestHeaders().getFirst("Content-Encoding") != null)
      return false;

    // The parser already hit the end of a body shorter than announced: nothing more to establish. Note this is
    // often settled BEFORE this method runs: try-with-resources closes the record stream on the way out of the
    // load, so CountingInputStream.close() has already consumed any buffered remainder and, if that reached the
    // end, recorded it here. The drain deliberately runs ahead of this probe - the two agree because a remainder
    // that ends the body sets endOfBody, and one that does not leaves the question open for the loop below.
    if (inputStream.isEndOfBody())
      return true;

    // Consume no further than the announced length: reaching it means the body was whole after all, which is a
    // spent budget rather than an end of stream.
    final BufferedDrain outcome = inputStream.drainAlreadyBuffered(
        Math.min(TRUNCATION_PROBE_BYTES, declaredLength - inputStream.getBytesRead()));

    // A broken connection settles it the same way an end of stream does: the announced bytes are not coming.
    return outcome == BufferedDrain.END_OF_BODY || outcome == BufferedDrain.BROKEN;
  }

  /**
   * Why {@link CountingInputStream#drainAlreadyBuffered} stopped. The distinction that matters is between reaching
   * the END of the request body - which proves what is left will never arrive - and merely running out of bytes that
   * had already arrived, which proves nothing either way.
   */
  private enum BufferedDrain {
    /** The request body is finished. */
    END_OF_BODY,
    /** Nothing more has arrived yet; the client may still be sending. */
    EXHAUSTED,
    /** The caller's budget ran out before either of the above. */
    BUDGET_SPENT,
    /** The connection failed while reading; nothing more is coming through it. */
    BROKEN
  }

  /**
   * Whether the temporary-id mapping is echoed back. {@code idMapping=auto} (the default) sends it only while it is
   * small enough to be useful: a client that streams millions of vertices in one request cannot consume a mapping of
   * the same size, and building it as one JSON string turns the last step of an otherwise successful import into an
   * OutOfMemoryError (issue #5470). {@code idMapping=true} demands it whatever the size - which is what a client
   * resolving edges across several requests (RemoteGraphBatch) needs - and {@code idMapping=false} never sends it.
   */
  private boolean echoIdMapping(final HttpServerExchange exchange, final int size) {
    final String value = getQueryParameter(exchange, "idMapping");
    if (value == null || value.isEmpty() || "auto".equalsIgnoreCase(value))
      return size <= MAX_ID_MAPPING_IN_RESPONSE;
    return Boolean.parseBoolean(value);
  }

  /**
   * Whether the streaming encoding hands the mapping back one committed chunk at a time (issue #7353).
   * <p>
   * {@code auto} streams it, with no cap: {@link #MAX_ID_MAPPING_IN_RESPONSE} exists because the buffered
   * encoding has to build the whole mapping as one JSON object and one string before it can send anything, and
   * that is what turns the last step of a successful 17-million-vertex import into an OutOfMemoryError. Here
   * neither ever exists - each line carries the vertices of one {@code vertexBatchSize} flush and is dropped
   * after it is written - so the reason to refuse a large mapping is gone, and refusing one anyway would leave
   * the streaming encoding delivering strictly less than the buffered one it is supposed to supersede.
   * <p>
   * {@code idMapping=false} still means never: a client loading vertices nothing will reference has no use for
   * the mapping, and not sending it saves both ends the bytes.
   */
  private boolean streamIdMapping(final HttpServerExchange exchange) {
    final String value = getQueryParameter(exchange, "idMapping");
    if (value == null || value.isEmpty() || "auto".equalsIgnoreCase(value))
      return true;
    return Boolean.parseBoolean(value);
  }

  /**
   * Moves whatever the mapping sink has collected since the previous line onto {@code event} and empties it, so
   * the accumulator never grows past one committed chunk (issue #7353). A no-op when the client asked for no
   * mapping, and when a chunk resolved nothing nameable - an edge-phase acknowledgement, or a vertex flush in
   * which every vertex declared no {@code @id} under {@code refMode=tempId}.
   */
  private static void drainPendingMapping(final JSONObject[] pending, final JSONObject event) {
    final JSONObject mapping = pending[0];
    if (mapping == null || mapping.isEmpty())
      return;
    event.put("idMapping", mapping);
    // A fresh object rather than a clear(): the one just attached belongs to the event being written, and
    // clearing it in place would empty the line that is about to be serialized.
    pending[0] = new JSONObject();
  }

  /**
   * Answers a batch whose request body did not arrive in full with HTTP 408 and the partial-commit counters, so the
   * client can resume instead of restarting - and never mistakes a half-loaded file for a completed load
   * (issue #5470).
   *
   * @param exceptionClass class name of the I/O failure that cut the body, or {@code null} when the body simply
   *                       ended before the announced length
   */
  private ExecutionResponse truncatedBody(final HttpServerExchange exchange, final String databaseName,
      final long verticesCreated, final long edgesCreated, final BatchRecordStream stream,
      final VertexRefResolver vertexRefs, final CountingInputStream inputStream, final String message,
      final String exceptionClass) {

    LogManager.instance().log(this, Level.WARNING,
        "Batch load on database '%s' was interrupted after %d vertices and %d edges because the request body "
            + "could not be read to the end: %s. If the server was busy (index compaction, replication of a large "
            + "entry) raise '%s' (currently %d ms)",
        null, databaseName, verticesCreated, edgesCreated, message,
        GlobalConfiguration.SERVER_HTTP_STREAMING_READ_TIMEOUT.getKey(),
        httpServer.getServer().getConfiguration()
            .getValueAsInteger(GlobalConfiguration.SERVER_HTTP_STREAMING_READ_TIMEOUT));

    // 408: the request was not fully received. The response often never reaches a client whose connection is
    // already gone, but when it does it carries the counts needed to resume instead of restarting.
    return partialPayloadResponse(exchange, 408,
        "Request body was truncated after " + verticesCreated + " vertices and " + edgesCreated + " edges: " + message,
        exceptionClass, verticesCreated, edgesCreated, stream, vertexRefs, inputStream);
  }

  /**
   * Answers a load whose record count does not match what the client declared with {@code expectedRecords}. Fewer
   * records than promised is a payload that ended early - answered like any other truncation, so a client can resume
   * - while more records than promised means the request and its declaration disagree and repeating it blindly would
   * make things worse, hence a 400 (issue #5470).
   */
  private ExecutionResponse recordCountMismatch(final HttpServerExchange exchange, final String databaseName,
      final long verticesCreated, final long edgesCreated, final long expectedRecords, final long records,
      final BatchRecordStream stream, final VertexRefResolver vertexRefs, final CountingInputStream inputStream) {

    final boolean truncated = records < expectedRecords;

    LogManager.instance().log(this, Level.WARNING,
        "Batch load on database '%s' declared %d records but %s: %d loaded (%d vertices and %d edges) out of %d lines "
            + "read. The payload was not what the client announced, so it is reported as incomplete instead of "
            + "successful",
        null, databaseName, expectedRecords, truncated ? "fewer arrived" : "more arrived", records, verticesCreated,
        edgesCreated, stream.getLinesRead());

    return partialPayloadResponse(exchange, truncated ? 408 : 400,
        "Expected " + expectedRecords + " records but " + records + " were received (" + verticesCreated
            + " vertices and " + edgesCreated + " edges)", null, verticesCreated, edgesCreated, stream, vertexRefs,
        inputStream);
  }

  /**
   * Answers a load that read more lines than it turned into records. Nothing in this handler may consume a line and
   * do nothing with it, so this is a defect on OUR side: the whole point of counting the lines is that the failure
   * issue #5618 reports - vertices present in the file and absent from the database, with a successful-looking
   * response - stops being something the user has to discover by grepping their own payload.
   * <p>
   * 500 rather than 400: the payload is not what is wrong. The counters travel with it so the client can still
   * reconcile whatever was committed before giving up.
   */
  private ExecutionResponse recordsDropped(final HttpServerExchange exchange, final String databaseName,
      final long verticesCreated, final long edgesCreated, final BatchRecordStream stream,
      final VertexRefResolver vertexRefs, final CountingInputStream inputStream) {

    final long missing = stream.getLinesRead() - stream.getLinesSkipped() - verticesCreated - edgesCreated;

    LogManager.instance().log(this, Level.SEVERE,
        "Batch load on database '%s' read %d lines (%d skipped) but only created %d vertices and %d edges: %d records "
            + "were dropped without an error. This is a server-side defect, please report it with the payload shape",
        null, databaseName, stream.getLinesRead(), stream.getLinesSkipped(), verticesCreated, edgesCreated, missing);

    return partialPayloadResponse(exchange, 500,
        "The load read " + stream.getLinesRead() + " lines (" + stream.getLinesSkipped() + " skipped) but created "
            + verticesCreated + " vertices and " + edgesCreated + " edges, so " + missing
            + " records were dropped without an error. The load is reported as failed rather than successful; the "
            + "records already committed are counted above, so do not simply re-POST the payload - that would "
            + "duplicate them",
        null, verticesCreated, edgesCreated, stream, vertexRefs, inputStream);
  }

  /**
   * The response shared by every "what arrived is not the whole payload" outcome: the counts have to be
   * machine-parsable, because the load is not atomic and the client needs them to reconcile.
   */
  private ExecutionResponse partialPayloadResponse(final HttpServerExchange exchange, final int status,
      final String message, final String exceptionClass, final long verticesCreated, final long edgesCreated,
      final BatchRecordStream stream, final VertexRefResolver vertexRefs, final CountingInputStream inputStream) {

    final JSONObject error = new JSONObject();
    error.put("error", message);
    if (exceptionClass != null)
      error.put("exception", exceptionClass);
    final String correlationId = getCorrelationId(exchange);
    if (correlationId != null && !correlationId.isEmpty())
      error.put("requestId", correlationId);
    error.put("verticesCreated", verticesCreated);
    error.put("edgesCreated", edgesCreated);
    error.put("partialCommit", verticesCreated > 0 || edgesCreated > 0);
    addLineAccounting(error, stream, vertexRefs, inputStream);
    return new ExecutionResponse(status, error.toString());
  }

  /**
   * Adds what the load did with the payload, line by line, to every answer it can give - success, partial commit and
   * truncation alike. {@code linesRead} minus {@code linesSkipped} is the number of records the parser handed over,
   * so a client can check it against {@code verticesCreated + edgesCreated} without trusting the server to have
   * checked it (the server does, see {@link #recordsDropped}), and can compare it with the line count of its own
   * file - which is what issue #5618 had to be diagnosed with, by hand, after the fact.
   * <p>
   * {@code bytesRead} is on every answer for the same reason. On a chunked body there is nothing to compare it with
   * server-side, so it is what lets a client verify that its whole file arrived (issue #5470); on a truncated one it
   * is how far the server got, and - since issue #6180 - the number that says the server read no further than the
   * client wrote, which it must never do whatever the connection hands it (see
   * {@link CountingInputStream#available()}).
   * <p>
   * {@code verticesWithoutId} is reported only when it is not zero: those vertices are loaded and durable, but no
   * edge of the payload can ever point at them, and today the only symptom is an "unknown temporary ID" much later.
   */
  private void addLineAccounting(final JSONObject json, final BatchRecordStream stream,
      final VertexRefResolver vertexRefs, final CountingInputStream inputStream) {
    json.put("bytesRead", inputStream.getBytesRead());
    json.put("linesRead", stream.getLinesRead());
    json.put("linesSkipped", stream.getLinesSkipped());

    final int withoutId = vertexRefs.unreferenceableVertices();
    if (withoutId > 0)
      json.put("verticesWithoutId", withoutId);
  }

  /**
   * Number of records (vertices plus edges, blank lines excluded) the client states the payload carries, or
   * {@code -1} when it does not. See {@link #recordCountMismatch}.
   */
  private long parseExpectedRecords(final HttpServerExchange exchange) {
    final String value = getQueryParameter(exchange, "expectedRecords");
    if (value == null)
      return -1;

    final long expectedRecords = Long.parseLong(value);
    if (expectedRecords < 0)
      throw new IllegalArgumentException("expectedRecords cannot be negative, but was " + expectedRecords);
    return expectedRecords;
  }

  /**
   * Counts the bytes handed to the parser, so the handler can compare them with the announced
   * {@code Content-Length} - and makes sure that giving up on a load never means reading the rest of it first.
   * <p>
   * It is also where a request body that has FAILED stops being a request body (issue #6180). The first failure -
   * whether it surfaces on a read or on an {@link #available()} probe - is remembered, and every later read of the
   * same body is refused with it instead of being attempted again. See {@link #available()} for what that prevents.
   */
  // Package-private, not private: PostBatchHandlerBodyFailureTest drives it directly, which is the only way to
  // reproduce a request body that fails on a probe and then offers bytes anyway (issue #6180).
  static class CountingInputStream extends FilterInputStream {
    private final HttpServerExchange exchange;
    private       long              bytesRead;
    private       boolean           endOfBody;
    /** The failure that ended this body, or {@code null} while it is still readable. */
    private       IOException       bodyFailure;

    CountingInputStream(final HttpServerExchange exchange, final InputStream in) {
      super(in);
      this.exchange = exchange;
    }

    @Override
    public int read() throws IOException {
      refuseIfBodyFailed();
      final int read;
      try {
        read = super.read();
      } catch (final IOException e) {
        bodyFailure = e;
        throw e;
      }
      if (read >= 0)
        ++bytesRead;
      else
        endOfBody = true;
      return read;
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
      refuseIfBodyFailed();
      final int read;
      try {
        read = super.read(b, off, len);
      } catch (final IOException e) {
        bodyFailure = e;
        throw e;
      }
      if (read > 0)
        bytesRead += read;
      else if (read < 0)
        endOfBody = true;
      return read;
    }

    /**
     * Whether the request body has already failed. A load that reaches this has read every byte the client managed
     * to send: whatever the stream would hand over now is not payload.
     */
    boolean hasBodyFailed() {
      return bodyFailure != null;
    }

    /**
     * Refuses a read of a body that has already failed, with the failure itself rather than a wrapper: it is the
     * reason this read cannot happen, its stack trace points at where the body really ended, and the load is
     * answered with exactly the message it would have carried had the failure surfaced on this read in the first
     * place.
     */
    private void refuseIfBodyFailed() throws IOException {
      if (bodyFailure != null)
        throw bodyFailure;
    }

    /**
     * How much of the body has already arrived, and - the part that matters here - the ONE place a failed body is
     * allowed to be probed without the failure being lost.
     * <p>
     * {@code InputStreamReader} probes the stream between decodes ({@code StreamDecoder.inReady}) and SWALLOWS the
     * {@link IOException} it may raise. That is what made a cut upload apply records the client never sent (issue
     * #6180): the probe reaches {@code UndertowInputStream.readIntoBufferNonBlocking}, which allocates a pooled
     * buffer before the channel read that throws and does not release it, so the buffer stays on the stream holding
     * whatever the POOL last left in it - for a connection whose first read filled it, the request head and the very
     * records already loaded. The next read is then served from that buffer without touching the channel, and the
     * parser is handed a replay of the payload: on a type with a unique index it surfaces as a 409 duplicate key
     * ({@code Issue5470BatchStreamStallIT} on {@code main}), and on one without it as silently duplicated rows and a
     * 200 (issue #6176).
     * <p>
     * So the failure is recorded here rather than raised: the probe answers -1, which is exactly the "the body is
     * finished" convention {@link #drainAlreadyBuffered} already reads from Undertow, the decoder stops asking, and
     * the next read is refused by {@link #refuseIfBodyFailed} - which sends the load down the truncated-body path it
     * was always meant to take, with the counts the client needs to resume.
     */
    @Override
    public int available() {
      if (bodyFailure != null)
        return -1;
      try {
        return super.available();
      } catch (final IOException e) {
        bodyFailure = e;
        return -1;
      }
    }

    /**
     * Releases the request body WITHOUT reading whatever is left of it.
     * <p>
     * {@code UndertowInputStream.close()} loops on {@code readIntoBuffer()} until the request body is finished: it
     * exists so a keep-alive connection can serve the next request, and on any other endpoint the leftovers are a
     * few bytes. Here they are the rest of a bulk upload. So a batch that stopped reading early - on a record it
     * will not accept, or because the client announced more than it sent - would sit waiting for the entire
     * remaining payload before the client could be told anything: on the 25M-line load of issue #5470 that was
     * fifteen minutes, after which the response could no longer even be sent
     * ({@code UT000002: The response has already been started}), so a precise diagnosis the server had reached in
     * two minutes never arrived at all.
     * <p>
     * Nothing needs those bytes. Undertow owns this stream and closes it when the exchange ends, so closing it
     * here buys only the drain, and the drain is exactly what has to be skipped: the connection is marked
     * non-persistent instead, which makes {@code HttpServerConnection.terminateRequestChannel} close the read side
     * before {@code endExchange} touches the stream. The response is written before that happens.
     * <p>
     * A body that WAS read to the end keeps the connection alive as before: there is nothing to drain, so an
     * ordinary client loading an ordinary payload sees no change.
     * <p>
     * Nor does a client whose payload had ALREADY ARRIVED when the load gave up on it - a small batch, fully sent,
     * rejected on a record in the middle. Its remainder is sitting in a buffer, so it is consumed here without ever
     * waiting for the network: the body reaches its end, the connection stays reusable, and - because the client is
     * no longer mid-upload when the socket closes - the answer is not at risk of being lost to a reset. Only a
     * remainder that has NOT arrived, or is larger than {@link #MAX_ABANDONED_BODY_DRAIN}, costs the connection.
     */
    @Override
    public void close() {
      // A body that FAILED is not a body that ended: there is nothing to drain and the connection carrying it is
      // gone, so it is retired below rather than handed back for the next request (issue #6180).
      if (bodyFailure == null && (endOfBody || drainAlreadyBuffered(MAX_ABANDONED_BODY_DRAIN) == BufferedDrain.END_OF_BODY))
        return;

      // Retiring a connection is invisible from the outside, and it is also where an unexpected cost would show up
      // if a body that HAD arrived stopped being recognised as complete: this is the line to raise to see it.
      LogManager.instance().log(this, Level.FINE,
          "Batch load did not read its request body to the end (%d of %s announced bytes); the connection is closed "
              + "rather than reused, so the rest of the upload is not read first", null, bytesRead,
          exchange.getRequestContentLength() >= 0 ? exchange.getRequestContentLength() : "an unannounced number of");

      exchange.setPersistent(false);
    }

    /**
     * Consumes up to {@code budget} bytes of the request body that have ALREADY arrived, and reports why it stopped.
     * <p>
     * Never blocks. {@code available()} bounds every read, so the cost is only what the server already holds and a
     * client that still owes bytes ends this at once with {@link BufferedDrain#EXHAUSTED}. Undertow reports a
     * finished body as -1 available rather than 0, which is the end-of-body signal both callers are looking for;
     * that is Undertow's own convention, not the {@link InputStream} contract (which says >= 0). An upgrade that
     * changed it would not go unnoticed - {@code Issue5470BatchErrorDeliveryIT}'s
     * {@code aRecordCutInHalfIsStillReportedAsATruncatedUpload} and
     * {@code aRejectedButCompletePayloadKeepsItsConnection} both fail when it reports 0 at end of body (verified by
     * mutation) - and if it ever does, the degradation is a worse diagnosis, never a wrong answer.
     */
    BufferedDrain drainAlreadyBuffered(final long budget) {
      final byte[] drain = new byte[8192];
      long drained = 0;
      try {
        while (drained < budget) {
          final int available = in.available();
          if (available < 0)
            return BufferedDrain.END_OF_BODY;
          if (available == 0)
            return BufferedDrain.EXHAUSTED;

          final int read = read(drain, 0, (int) Math.min(Math.min(available, drain.length), budget - drained));
          if (read < 0)
            return BufferedDrain.END_OF_BODY;
          if (read == 0)
            // Cannot happen for a blocking stream asked for len >= 1, but never spin on it either.
            return BufferedDrain.EXHAUSTED;
          drained += read;
        }
      } catch (final IOException e) {
        return BufferedDrain.BROKEN;
      }
      return BufferedDrain.BUDGET_SPENT;
    }

    long getBytesRead() {
      return bytesRead;
    }

    /** Whether the parser reached the end of the request body. */
    boolean isEndOfBody() {
      return endOfBody;
    }
  }

  /**
   * Builds the structure that resolves {@code @from} / {@code @to}, which is the memory ceiling of a streaming load
   * (issue #5470). See the {@code refMode} and {@code expectedVertexCount} parameters.
   */
  private VertexRefResolver newVertexRefResolver(final HttpServerExchange exchange) {
    final String value = getQueryParameter(exchange, "expectedVertexCount");
    int expectedVertices = 0;
    if (value != null) {
      expectedVertices = Integer.parseInt(value);
      if (expectedVertices < 0)
        throw new IllegalArgumentException("expectedVertexCount cannot be negative, but was " + expectedVertices);
      // A hint must never be able to allocate the server out of memory on its own: past this the structure simply
      // grows by doubling, which costs one copy.
      expectedVertices = Math.min(expectedVertices, MAX_PRESIZED_VERTICES);
    }

    final String base = getQueryParameter(exchange, "ordinalBase");
    long ordinalBase = 0;
    if (base != null) {
      ordinalBase = Long.parseLong(base);
      if (ordinalBase < 0)
        throw new IllegalArgumentException("ordinalBase cannot be negative, but was " + ordinalBase);
    }

    final String refMode = getQueryParameter(exchange, "refMode");
    if (refMode == null || refMode.isEmpty() || "id".equalsIgnoreCase(refMode)) {
      if (base != null)
        throw new IllegalArgumentException("ordinalBase only applies to refMode=ordinal");
      return new TempIdVertexRefResolver(expectedVertices);
    }
    if ("ordinal".equalsIgnoreCase(refMode))
      return new OrdinalVertexRefResolver(expectedVertices, ordinalBase);

    throw new IllegalArgumentException(
        "Invalid refMode '" + refMode + "': expected 'id' (edges reference the @id of a vertex) or 'ordinal' "
            + "(edges reference the 0-based position of a vertex in the payload)");
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

  /**
   * @param mappingSink notified of every reference this flush resolved, or {@code null} when the mapping is not
   *                    being streamed back. It is fed here rather than read off the resolver afterwards because
   *                    neither resolver keeps insertion order - and the point of streaming it is to never build
   *                    a structure that does (issue #7353).
   */
  private int flushVertexBatch(final GraphBatch batch, final String typeName,
      final List<Object[]> propsBatch, final List<String> tempIds, final VertexRefResolver vertexRefs,
      final long firstOrdinal, final VertexRefResolver.EntryConsumer mappingSink) {

    final int count = propsBatch.size();
    final Object[][] propsArray = propsBatch.toArray(new Object[count][]);
    final RID[] rids = batch.createVertices(typeName, propsArray);

    for (int i = 0; i < count; i++) {
      vertexRefs.put(tempIds.get(i), (int) (firstOrdinal + i), rids[i]);
      if (mappingSink != null) {
        final String ref = vertexRefs.refOf(tempIds.get(i), firstOrdinal + i);
        if (ref != null)
          mappingSink.accept(ref, rids[i]);
      }
    }

    propsBatch.clear();
    tempIds.clear();
    return count;
  }

  private void processEdge(final GraphBatch batch, final BatchRecord rec, final VertexRefResolver vertexRefs,
      final int lineNumber) {
    final RID srcRID = resolveRef(rec.fromRef, vertexRefs, lineNumber);
    final RID dstRID = resolveRef(rec.toRef, vertexRefs, lineNumber);
    batch.newEdge(srcRID, rec.typeName, dstRID, rec.copyEdgeProperties());
  }

  private RID resolveRef(final String ref, final VertexRefResolver vertexRefs, final int lineNumber) {
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

    // Temporary id or payload position, depending on refMode
    return vertexRefs.get(ref, lineNumber);
  }

  private void configureBuilder(final HttpServerExchange exchange, final GraphBatch.Builder builder) {
    final String batchSize = getQueryParameter(exchange, "batchSize");
    if (batchSize != null)
      builder.withBatchSize(Integer.parseInt(batchSize));

    // Deprecated: declare LIGHTWEIGHT on the edge type instead. A type that declares it is stored lightweight
    // regardless of this parameter, which now only covers types that declare nothing.
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
   * <p>
   * The negotiated encoding travels with the payload (issue #7311). Without it a client that asked a follower
   * for the streaming answer would be handed the leader's buffered one under an {@code application/json}
   * content type - a silent downgrade of the very thing it negotiated, and a body its NDJSON reader cannot
   * parse. With it the leader streams, and {@link #relayNdJsonFromLeader} passes those lines straight through
   * as they arrive, so the acknowledgements keep reaching the client while the follower is still relaying the
   * upload.
   *
   * @param streaming whether the client negotiated {@code Accept: application/x-ndjson}
   */
  private ExecutionResponse forwardBatchToLeader(final HttpServerExchange exchange, final HAServerPlugin ha,
      final String databaseName, final ServerSecurityUser user, final String contentType,
      final CountingInputStream body, final boolean streaming) throws Exception {

    // A peer already relayed this load to what it believed was the leader and it landed here, on a node that
    // is not the leader either. Relaying it on would send it round the cycle that wrong address created, one
    // held request thread and one buffered upload per hop; refuse in one hop instead (issue #6191).
    if (LeaderForwardContext.isAlreadyForwarded()) {
      // Also said once in this node's log: the refusal is relayed back to the peer and from there to the
      // client, so otherwise the only node that can name the misconfiguration never mentions it.
      if (forwardedAgainWarned.compareAndSet(false, true))
        LogManager.instance().log(this, Level.WARNING,
            "A cluster peer forwarded a batch to this node as the leader, but this node is not the leader (db=%s). "
                + "Unless leadership just moved, the HTTP address that peer resolved for the leader does not identify "
                + "it: declare every node's HTTP port explicitly with the 'host:raftPort:httpPort' syntax in %s. The "
                + "load is refused rather than relayed on. This notice is logged only once.",
            databaseName, GlobalConfiguration.HA_SERVER_LIST.getKey());
      return new ExecutionResponse(400, new JSONObject()
          .put("error", "Refusing to forward a batch that a cluster peer already forwarded to the leader: it arrived "
              + "on this node, which is not the leader. Either leadership moved while the request was in flight - "
              + "retry - or the HTTP address that peer resolved for the leader does not identify it, which is what "
              + "declaring every node's HTTP port ('host:raftPort:httpPort') in "
              + GlobalConfiguration.HA_SERVER_LIST.getKey() + " prevents")
          .toString());
    }

    final String leaderAddress = ha.getLeaderAddress();
    if (leaderAddress == null || leaderAddress.isBlank())
      return new ExecutionResponse(503,
          "{ \"error\" : \"Cannot forward batch to leader: leader address is not available\"}");

    // The address resolved for the leader is this node's own: dialing it would come straight back here. The
    // derive fallback produces exactly this on a cluster whose peers share a host and declare no HTTP port,
    // because it pairs the leader's Raft host with THIS node's HTTP port (issue #6191).
    if (ha.isOwnHttpAddress(leaderAddress))
      return new ExecutionResponse(400, new JSONObject()
          .put("error", "Cannot forward batch to leader: the HTTP address resolved for the leader (" + leaderAddress
              + ") is this node's own, and this node is not the leader. Declare every node's HTTP port explicitly "
              + "with the 'host:raftPort:httpPort' syntax in " + GlobalConfiguration.HA_SERVER_LIST.getKey())
          .toString());

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

    // The body travels through the same guarded stream the leader-side load would use, so a cut upload cannot
    // relay a replay of its own bytes on to the leader either (issue #6180).
    final HttpRequest request = buildForwardRequest(url, contentType, clusterToken, user.getName(),
        exchange.getRequestContentLength(), body, streaming ? NdJsonResultStream.CONTENT_TYPE : null);

    try {
      if (streaming)
        // send() returns as soon as the leader's response HEADERS arrive, which on the streaming encoding is at
        // the leader's first progress line - while the JDK client's own executor thread is still publishing the
        // relayed upload. That is what keeps the acknowledgements incremental across the hop.
        return relayNdJsonFromLeader(exchange, databaseName,
            HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofInputStream()));

      final HttpResponse<String> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());

      // ExecutionResponse carries only status + body, so the leader's X-ArcadeDB-Commit-Index bookmark
      // (issue #5862) has to be copied onto this exchange explicitly, or a READ_YOUR_WRITES client that
      // landed on a follower would never see it despite the leader having just emitted it.
      response.headers().firstValue("X-ArcadeDB-Commit-Index")
          .ifPresent(val -> exchange.getResponseHeaders().put(new HttpString("X-ArcadeDB-Commit-Index"), val));

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

  /**
   * Builds the request that relays a batch payload from a follower to the leader. Three things about it are not
   * defaults, and each of them is a way a forwarded load could otherwise lose part of its payload in silence
   * (issue #5618):
   * <ul>
   *   <li><b>the content length travels with the body.</b> {@code BodyPublishers.ofInputStream} declares an unknown
   *   length, so the forwarded request goes out chunked - and a chunked body announces nothing, which switches OFF
   *   the leader's own truncation check ({@link #bodyEndedEarly} returns false as soon as there is no announced
   *   length). A relayed upload that ended early was then answered 200 with a partial count, which is precisely
   *   the safety net issue #5470 added and the forwarding path never had. With the length declared, the JDK client
   *   also fails the request outright if it cannot feed the leader exactly that many bytes;</li>
   *   <li><b>HTTP/1.1 is pinned.</b> The default client negotiates HTTP/2, and on a plaintext connection that means
   *   an {@code h2c} upgrade whose failure mode is re-sending the request - with a body that cannot be rewound;</li>
   *   <li><b>the body is one-shot and says so.</b> {@code ofInputStream} takes a SUPPLIER because the JDK may
   *   subscribe more than once and expects a fresh stream each time. There is only one request body here, so a
   *   second subscription would have handed back a stream already positioned in the middle of the payload and the
   *   leader would have loaded a file missing its beginning, with nothing anywhere saying so. It now fails loudly
   *   instead, and the caller answers 503.</li>
   * </ul>
   *
   * @param contentLength the client's announced body length, or a negative value when it uploaded chunked
   */
  static HttpRequest buildForwardRequest(final String url, final String contentType, final String clusterToken,
      final String userName, final long contentLength, final InputStream body) {
    return buildForwardRequest(url, contentType, clusterToken, userName, contentLength, body, null);
  }

  /**
   * As above, and additionally relays the response encoding the client negotiated (issue #7311). A {@code null}
   * {@code accept} sends no header at all, which is what makes the leader answer a forwarded request exactly as
   * it did before this parameter existed.
   *
   * @param accept the {@code Accept} header to relay, or {@code null} for none
   */
  static HttpRequest buildForwardRequest(final String url, final String contentType, final String clusterToken,
      final String userName, final long contentLength, final InputStream body, final String accept) {

    final AtomicBoolean bodyTaken = new AtomicBoolean(false);
    final Supplier<InputStream> oneShotBody = () -> {
      if (!bodyTaken.compareAndSet(false, true))
        throw new IllegalStateException(
            "The batch payload being forwarded to the leader was requested twice, but a request body can only be "
                + "read once: sending it again would relay a payload missing everything already consumed");
      return body;
    };

    HttpRequest.BodyPublisher publisher = HttpRequest.BodyPublishers.ofInputStream(oneShotBody);
    if (contentLength >= 0)
      publisher = HttpRequest.BodyPublishers.fromPublisher(publisher, contentLength);

    final HttpRequest.Builder forward = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .version(HttpClient.Version.HTTP_1_1)
        .header("Content-Type", contentType)
        .header("X-ArcadeDB-Cluster-Token", clusterToken)
        .header("X-ArcadeDB-Forwarded-User", userName)
        // One hop only: a node that receives this and is not the leader refuses it rather than resolving the
        // same leader address - which may name nobody - and relaying it again (issue #6191).
        .header(LeaderForwardContext.FORWARDED_TO_LEADER_HEADER, "true")
        .POST(publisher);

    if (accept != null)
      forward.header("Accept", accept);

    return forward.build();
  }

  /**
   * Passes the leader's NDJSON answer through to the client, line by line, as it arrives (issue #7311).
   * <p>
   * Copied rather than parsed: a line the leader emitted is already the line this client asked for, so relaying
   * the bytes is both the cheapest thing to do and the only one that cannot make the two nodes disagree about
   * the wire format. Each line is flushed on its own, because a relay that batched them would reintroduce
   * exactly the latency the encoding exists to remove.
   * <p>
   * A leader that answered with anything other than the streaming encoding - an older node, or a refusal issued
   * before the load started, which is a normal status-carrying error response - is relayed as the buffered
   * answer it is, so the follower never invents a stream the leader did not send.
   */
  private ExecutionResponse relayNdJsonFromLeader(final HttpServerExchange exchange, final String databaseName,
      final HttpResponse<InputStream> response) throws IOException {

    final String leaderContentType = response.headers().firstValue("Content-Type").orElse("");
    if (response.statusCode() != 200
        || !leaderContentType.toLowerCase(Locale.ROOT).contains(NdJsonResultStream.CONTENT_TYPE)) {
      // Not a stream: read it whole and answer it the way every other forwarded response is answered.
      try (final InputStream in = response.body()) {
        response.headers().firstValue("X-ArcadeDB-Commit-Index")
            .ifPresent(val -> exchange.getResponseHeaders().put(new HttpString("X-ArcadeDB-Commit-Index"), val));
        return new ExecutionResponse(response.statusCode(),
            new String(in.readAllBytes(), StandardCharsets.UTF_8));
      }
    }

    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, NdJsonResultStream.CONTENT_TYPE);
    exchange.getResponseHeaders().put(Headers.CACHE_CONTROL, "no-cache");
    exchange.getResponseHeaders().put(X_ACCEL_BUFFERING, "no");
    exchange.setStatusCode(200);
    if (!exchange.isBlocking())
      exchange.startBlocking();

    try (final BufferedReader in = new BufferedReader(
        new InputStreamReader(response.body(), StandardCharsets.UTF_8));
        final OutputStream out = exchange.getOutputStream()) {
      for (String line = in.readLine(); line != null; line = in.readLine()) {
        out.write(line.getBytes(StandardCharsets.UTF_8));
        out.write('\n');
        out.flush();
      }
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.WARNING,
          "Error relaying the streamed batch answer of database '%s' from the leader: %s", null, databaseName,
          e.getMessage());
      // The 200 and part of the stream are already on the wire, so there is no status left to change and no
      // terminal line to trust: a consumer that saw neither 'summary' nor 'error' knows it did not get
      // everything, which is the contract the encoding is built on.
    }
    return null;
  }
}
