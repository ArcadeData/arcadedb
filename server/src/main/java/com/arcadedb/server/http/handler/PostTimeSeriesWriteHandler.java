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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.LineProtocolParser;
import com.arcadedb.engine.timeseries.LineProtocolParser.Precision;
import com.arcadedb.engine.timeseries.LineProtocolParser.Sample;
import com.arcadedb.engine.timeseries.TimeSeriesGateway;
import com.arcadedb.engine.timeseries.TimeSeriesGateway.WriteReport;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.StatusCodes;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.zip.GZIPInputStream;

/**
 * HTTP handler for InfluxDB Line Protocol ingestion.
 * Endpoint: POST /api/v1/ts/{database}/write?precision=&lt;ns|us|ms|s&gt;
 * Body: InfluxDB Line Protocol text (one or more lines)
 * <p>
 * The ingest semantics themselves - grouping by measurement, the per-type ACL, the batch append and the drop
 * sets - live in {@link TimeSeriesGateway}, shared with the gRPC {@code TimeSeriesWrite} RPCs (issue #7305).
 * What is left here is the HTTP shape: the body, the precision parameter, the status codes and the
 * partial-write report.
 * <p>
 * On {@link DatabaseAbstractHandler} since issue #7402: a request carrying {@code arcadedb-session-id} is
 * resolved against that session, so it runs under the session's lock and principal, refreshes the session's
 * idle clock instead of letting a client that only ingests have its transaction reaped underneath it, and is
 * refused outright when the id names a session this server no longer knows.
 * <p>
 * The two transaction answers below are deliberately different, and the pair is the whole design question
 * issue #7402 left open for this route:
 * <ul>
 * <li>{@link #requiresTransaction()} is <b>false</b>. An append is not made atomic by wrapping it:
 * {@code TimeSeriesShard.appendSamples} opens its own {@code begin}/{@code commit} around each shard's write,
 * so an outer auto-commit transaction would have nothing of its own to commit - and it would cost the parallel
 * shard dispatch, which {@code TimeSeriesEngine.appendBatch} takes only when no transaction is active on the
 * calling thread (#4957). The partial-write report below is what this endpoint offers in place of atomicity.</li>
 * <li>{@link #rejectsUnresolvableSession()} is <b>true</b>. A read that names a session this server cannot
 * resolve can degrade to reading outside it; a write cannot, because the client believes it is writing
 * something it can still roll back.</li>
 * </ul>
 * A session that DOES resolve is bound onto the request thread, which makes {@code appendBatch} take its
 * in-thread branch for the same #4957 reason - the same thing an embedded caller appending inside its own
 * transaction already gets. It does not make the samples part of that transaction: the nested begin/commit is
 * an independent transaction rather than a savepoint, so they are durable before the caller commits anything.
 * Issue #7410 tracks that divergence from {@code TimeSeriesEngine}'s javadoc.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PostTimeSeriesWriteHandler extends DatabaseAbstractHandler {

  private String rawPayload;

  public PostTimeSeriesWriteHandler(final HttpServer httpServer) {
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
  protected boolean requiresTransaction() {
    return false;
  }

  @Override
  protected boolean rejectsUnresolvableSession() {
    return true;
  }

  @Override
  protected String parseRequestPayload(final HttpServerExchange e) {
    if (!e.isInIoThread() && !e.isBlocking())
      e.startBlocking();

    final AtomicReference<byte[]> bytesRef = new AtomicReference<>();
    e.getRequestReceiver().receiveFullBytes(
        (exchange, data) -> bytesRef.set(data),
        (exchange, err) -> {
          LogManager.instance().log(this, Level.SEVERE, "receiveFullBytes completed with an error: %s", err, err.getMessage());
          exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
          exchange.getResponseSender().send("Invalid Request");
        });

    final byte[] rawBytes = bytesRef.get();
    if (rawBytes == null) {
      rawPayload = null;
      return null;
    }

    final var contentEncoding = e.getRequestHeaders().get(Headers.CONTENT_ENCODING);
    if (contentEncoding != null && !contentEncoding.isEmpty() && "gzip".equalsIgnoreCase(contentEncoding.getFirst())) {
      try (final GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(rawBytes))) {
        rawPayload = new String(gzip.readAllBytes(), DatabaseFactory.getDefaultCharset());
      } catch (final IOException ex) {
        throw new IllegalArgumentException("Failed to decompress gzip body: " + ex.getMessage(), ex);
      }
    } else {
      rawPayload = new String(rawBytes, DatabaseFactory.getDefaultCharset());
    }
    return rawPayload;
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final Database db, final JSONObject payload) throws Exception {

    final DatabaseInternal database = (DatabaseInternal) db;

    // The read-your-writes bookmark is emitted by DatabaseAbstractHandler, on every return path this method
    // has - including the partial-write 400, whose already-inserted samples are durable (issue #5866) - and,
    // through emitCommitIndexBookmarkOnResponseCommit, on a response this method throws out of as well, which
    // the hand-rolled finally this replaced did not cover any better.

    // Get precision from query parameter
    final Deque<String> precisionParam = exchange.getQueryParameters().get("precision");
    final Precision precision = precisionParam != null && !precisionParam.isEmpty()
        ? Precision.fromString(precisionParam.getFirst())
        : Precision.NANOSECONDS;

    if (rawPayload == null || rawPayload.isBlank())
      return new ExecutionResponse(400, "{ \"error\" : \"Request body is empty\"}");

    // Parse line protocol
    final List<Sample> samples = LineProtocolParser.parse(rawPayload, precision);
    if (samples.isEmpty())
      return new ExecutionResponse(204, "");

    // NOTE: this call does NOT make the request atomic. TimeSeriesShard.appendSamples runs its own
    // begin/commit, so every measurement the gateway appends has already committed its shard writes by the
    // time it returns. If a later measurement throws, nothing can undo the measurements already written -
    // the same partial-write shape the 400 response below reports, now at measurement granularity.
    final WriteReport report = TimeSeriesGateway.write(database, samples);

    if (!report.unknownTypes().isEmpty())
      LogManager.instance().log(this, Level.WARNING,
          "Skipped line protocol samples for unknown timeseries type(s): %s", null, report.unknownTypes());

    if (!report.nonTimeSeriesTypes().isEmpty())
      LogManager.instance().log(this, Level.WARNING,
          "Skipped line protocol samples for non-timeseries type(s): %s", null, report.nonTimeSeriesTypes());

    if (!report.unavailableTypes().isEmpty())
      LogManager.instance().log(this, Level.WARNING,
          "Skipped line protocol samples for TimeSeries type(s) with no storage engine available: %s", null,
          report.unavailableTypes());

    // Any dropped sample is a partial write: matching InfluxDB, return 400 naming the dropped
    // measurements (with written/dropped counts) even when some samples were inserted, so the client
    // is not told 204 "all good" while data was silently discarded (issue #5036). The samples that did
    // insert are already committed - this is a partial-write signal, not a full rollback.
    // `dropped` counts individual samples, consistent with `written`; every parsed sample is either
    // inserted or skipped into one of the drop sets.
    if (!report.isComplete()) {
      final StringBuilder msg = new StringBuilder("partial write: ");
      if (!report.unknownTypes().isEmpty())
        msg.append("unknown timeseries type(s): ").append(String.join(", ", report.unknownTypes()))
            .append(" (create the type first with CREATE TIMESERIES TYPE).");
      if (!report.nonTimeSeriesTypes().isEmpty()) {
        if (!report.unknownTypes().isEmpty())
          msg.append(" ");
        msg.append("non-timeseries type(s): ").append(String.join(", ", report.nonTimeSeriesTypes()))
            .append(" (only TIMESERIES types can receive line protocol data).");
      }
      if (!report.unavailableTypes().isEmpty()) {
        if (!report.unknownTypes().isEmpty() || !report.nonTimeSeriesTypes().isEmpty())
          msg.append(" ");
        msg.append("TimeSeries type(s) with no storage engine available: ")
            .append(String.join(", ", report.unavailableTypes()))
            .append(" (see the server log for why each failed to load).");
      }

      final JSONObject error = new JSONObject();
      error.put("error", msg.toString());
      final String correlationId = getCorrelationId(exchange);
      if (correlationId != null && !correlationId.isEmpty())
        error.put("requestId", correlationId);
      error.put("written", report.written());
      error.put("dropped", report.dropped());
      if (!report.unknownTypes().isEmpty())
        error.put("unknownTypes", new JSONArray(report.unknownTypes()));
      if (!report.nonTimeSeriesTypes().isEmpty())
        error.put("nonTimeSeriesTypes", new JSONArray(report.nonTimeSeriesTypes()));
      if (!report.unavailableTypes().isEmpty())
        error.put("unavailableTypes", new JSONArray(report.unavailableTypes()));
      return new ExecutionResponse(400, error.toString());
    }

    return new ExecutionResponse(204, "");
  }
}
