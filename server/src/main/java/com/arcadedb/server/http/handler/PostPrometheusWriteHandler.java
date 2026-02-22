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
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.TimeSeriesTypeBuilder;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Label;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Sample;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.TimeSeries;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.WriteRequest;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HttpString;
import org.xerial.snappy.Snappy;

import java.util.Deque;
import java.util.List;

/**
 * HTTP handler for Prometheus remote_write protocol.
 * Endpoint: POST /api/v1/ts/{database}/prom/write
 * <p>
 * Receives Snappy-compressed protobuf WriteRequest messages,
 * auto-creates TimeSeries types as needed, and inserts samples.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PostPrometheusWriteHandler extends AbstractBinaryHttpHandler {

  public PostPrometheusWriteHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) throws Exception {

    exchange.getResponseHeaders().put(new HttpString("X-Prometheus-Remote-Write-Version"), "0.1.0");

    final Deque<String> databaseParam = exchange.getQueryParameters().get("database");
    if (databaseParam == null || databaseParam.isEmpty())
      return new ExecutionResponse(400, "{ \"error\" : \"Database parameter is required\"}");

    if (rawBytes == null || rawBytes.length == 0)
      return new ExecutionResponse(400, "{ \"error\" : \"Request body is empty\"}");

    // Snappy decompress
    final byte[] decompressed;
    try {
      decompressed = Snappy.uncompress(rawBytes);
    } catch (final Exception e) {
      return new ExecutionResponse(400, "{ \"error\" : \"Invalid Snappy-compressed data\"}");
    }

    // Decode protobuf WriteRequest
    final WriteRequest writeRequest = WriteRequest.decode(decompressed);
    if (writeRequest.getTimeSeries().isEmpty())
      return new ExecutionResponse(204, "");

    final DatabaseInternal database = httpServer.getServer().getDatabase(databaseParam.getFirst(), false, false);

    database.begin();
    try {
      for (final TimeSeries ts : writeRequest.getTimeSeries()) {
        final String metricName = ts.getMetricName();
        if (metricName == null || metricName.isEmpty())
          continue;

        // Sanitize metric name: dots/hyphens → underscores
        final String typeName = sanitizeTypeName(metricName);

        // Auto-create type if needed
        final LocalTimeSeriesType tsType = getOrCreateType(database, typeName, ts.getLabels());
        final TimeSeriesEngine engine = tsType.getEngine();
        final List<ColumnDefinition> columns = tsType.getTsColumns();

        // Insert each sample
        for (final Sample sample : ts.getSamples()) {
          final long[] timestamps = new long[] { sample.timestampMs() };
          final Object[][] columnValues = new Object[columns.size() - 1][1];

          int colIdx = 0;
          for (int i = 0; i < columns.size(); i++) {
            final ColumnDefinition col = columns.get(i);
            if (col.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
              continue;

            Object value;
            if (col.getRole() == ColumnDefinition.ColumnRole.TAG)
              value = findLabelValue(ts.getLabels(), col.getName());
            else
              value = sample.value(); // the "value" field
            columnValues[colIdx][0] = value;
            colIdx++;
          }

          engine.appendSamples(timestamps, columnValues);
        }
      }
      database.commit();
    } catch (final Exception e) {
      database.rollback();
      throw e;
    }

    return new ExecutionResponse(204, "");
  }

  private LocalTimeSeriesType getOrCreateType(final DatabaseInternal database, final String typeName,
      final List<Label> labels) {
    if (database.getSchema().existsType(typeName)) {
      final DocumentType docType = database.getSchema().getType(typeName);
      if (docType instanceof LocalTimeSeriesType tsType && tsType.getEngine() != null)
        return tsType;
    }

    // Auto-create: timestamp + tags from labels + one DOUBLE field "value"
    final TimeSeriesTypeBuilder builder = new TimeSeriesTypeBuilder((DatabaseInternal) database)
        .withName(typeName)
        .withTimestamp("timestamp");

    for (final Label label : labels) {
      if ("__name__".equals(label.name()))
        continue;
      builder.withTag(sanitizeColumnName(label.name()), Type.STRING);
    }

    builder.withField("value", Type.DOUBLE);
    return builder.create();
  }

  private static String findLabelValue(final List<Label> labels, final String tagName) {
    for (final Label l : labels) {
      if (sanitizeColumnName(l.name()).equals(tagName))
        return l.value();
    }
    return null;
  }

  static String sanitizeTypeName(final String name) {
    return name.replace('.', '_').replace('-', '_').replace(':', '_');
  }

  static String sanitizeColumnName(final String name) {
    return name.replace('.', '_').replace('-', '_').replace(':', '_');
  }
}
