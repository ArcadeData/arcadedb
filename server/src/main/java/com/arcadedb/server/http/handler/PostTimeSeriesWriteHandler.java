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
import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.engine.timeseries.LineProtocolParser;
import com.arcadedb.engine.timeseries.LineProtocolParser.Precision;
import com.arcadedb.engine.timeseries.LineProtocolParser.Sample;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.StatusCodes;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.zip.GZIPInputStream;

/**
 * HTTP handler for InfluxDB Line Protocol ingestion.
 * Endpoint: POST /api/v1/ts/{database}/write?precision=<ns|us|ms|s>
 * Body: InfluxDB Line Protocol text (one or more lines)
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

    final DatabaseInternal database = httpServer.getServer().getDatabase(databaseParam.getFirst(), false, false);

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

    // Group by measurement and insert
    int inserted = 0;
    final Set<String> unknownTypes = new LinkedHashSet<>();
    final Set<String> nonTimeSeriesTypes = new LinkedHashSet<>();
    database.begin();
    try {
      for (final Sample sample : samples) {
        final String measurement = sample.getMeasurement();

        if (!database.getSchema().existsType(measurement)) {
          unknownTypes.add(measurement);
          continue;
        }

        final DocumentType docType = database.getSchema().getType(measurement);
        if (!(docType instanceof LocalTimeSeriesType tsType) || tsType.getEngine() == null) {
          nonTimeSeriesTypes.add(measurement);
          continue;
        }

        final TimeSeriesEngine engine = tsType.getEngine();
        final List<ColumnDefinition> columns = tsType.getTsColumns();

        final long[] timestamps = new long[] { sample.getTimestampMs() };
        final Object[][] columnValues = new Object[columns.size() - 1][1]; // exclude timestamp

        int colIdx = 0;
        for (int i = 0; i < columns.size(); i++) {
          final ColumnDefinition col = columns.get(i);
          if (col.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
            continue;

          Object value;
          if (col.getRole() == ColumnDefinition.ColumnRole.TAG)
            value = sample.getTags().get(col.getName());
          else
            value = sample.getFields().get(col.getName());

          columnValues[colIdx][0] = value;
          colIdx++;
        }

        engine.appendSamples(timestamps, columnValues);
        inserted++;
      }
      database.commit();
    } catch (final Exception e) {
      database.rollback();
      throw e;
    }

    if (!unknownTypes.isEmpty())
      LogManager.instance().log(this, Level.WARNING,
          "Skipped line protocol samples for unknown timeseries type(s): %s", null, unknownTypes);

    if (!nonTimeSeriesTypes.isEmpty())
      LogManager.instance().log(this, Level.WARNING,
          "Skipped line protocol samples for non-timeseries type(s): %s", null, nonTimeSeriesTypes);

    if (inserted == 0 && (!unknownTypes.isEmpty() || !nonTimeSeriesTypes.isEmpty())) {
      final StringBuilder msg = new StringBuilder();
      if (!unknownTypes.isEmpty())
        msg.append("Unknown timeseries type(s): ").append(String.join(", ", unknownTypes)).append(". Create the type first with CREATE TIMESERIES TYPE.");
      if (!nonTimeSeriesTypes.isEmpty()) {
        if (msg.length() > 0)
          msg.append(" ");
        msg.append("Non-timeseries type(s): ").append(String.join(", ", nonTimeSeriesTypes)).append(". Only TIMESERIES types can receive line protocol data.");
      }
      return new ExecutionResponse(400, new JSONObject().put("error", msg.toString()).toString());
    }

    return new ExecutionResponse(204, "");
  }
}
