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
import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.engine.timeseries.LineProtocolParser;
import com.arcadedb.engine.timeseries.LineProtocolParser.Precision;
import com.arcadedb.engine.timeseries.LineProtocolParser.Sample;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

import java.util.Deque;
import java.util.List;
import java.util.Map;

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
  protected String parseRequestPayload(final io.undertow.server.HttpServerExchange e) {
    // Store the raw payload for Line Protocol parsing
    rawPayload = super.parseRequestPayload(e);
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
    database.begin();
    try {
      for (final Sample sample : samples) {
        final String measurement = sample.getMeasurement();

        if (!database.getSchema().existsType(measurement))
          continue; // skip unknown measurement types

        final DocumentType docType = database.getSchema().getType(measurement);
        if (!(docType instanceof LocalTimeSeriesType tsType) || tsType.getEngine() == null)
          continue; // skip non-timeseries types

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

    // Return 204 No Content (InfluxDB convention) or 200 with count
    if (inserted == 0)
      return new ExecutionResponse(204, "");

    return new ExecutionResponse(204, "");
  }
}
