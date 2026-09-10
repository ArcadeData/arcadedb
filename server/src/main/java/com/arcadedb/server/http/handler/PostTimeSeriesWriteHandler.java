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
import com.arcadedb.server.HAReplicatedDatabase;
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
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PostTimeSeriesWriteHandler extends AbstractServerHttpHandler {

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
      final JSONObject payload) throws Exception {

    // Get database from path parameter
    final Deque<String> databaseParam = exchange.getQueryParameters().get("database");
    if (databaseParam == null || databaseParam.isEmpty())
      return new ExecutionResponse(400, "{ \"error\" : \"Database parameter is required\"}");

    // Enforce database-level authorization (GHSA-x8mg-6r4p-87pf): this handler does not extend DatabaseAbstractHandler.
    // Checked before any payload/parameter validation so an unauthorized caller cannot probe the target database.
    checkAuthorizationOnDatabase(user, databaseParam.getFirst());

    final DatabaseInternal database = httpServer.getServer().getDatabase(databaseParam.getFirst(), false, false);

    // Resolved once so the bookmark can be emitted in the finally below regardless of which return path is
    // taken - including the partial-write 400, whose already-inserted samples are durable (issue #5866).
    final HAReplicatedDatabase haDb = resolveHAReplicatedDatabase(database);
    try {
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
    } finally {
      emitCommitIndexBookmark(exchange, haDb);
    }
  }
}
