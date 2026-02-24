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
import com.arcadedb.engine.timeseries.TagFilter;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Label;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.LabelMatcher;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.MatchType;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Query;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.QueryResult;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.ReadRequest;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.ReadResponse;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Sample;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.TimeSeries;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HttpString;
import org.xerial.snappy.Snappy;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * HTTP handler for Prometheus remote_read protocol.
 * Endpoint: POST /api/v1/ts/{database}/prom/read
 * <p>
 * Receives Snappy-compressed protobuf ReadRequest messages,
 * queries the TimeSeries engine, and returns Snappy-compressed protobuf ReadResponse.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PostPrometheusReadHandler extends AbstractBinaryHttpHandler {

  public PostPrometheusReadHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) throws Exception {

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

    final ReadRequest readRequest = ReadRequest.decode(decompressed);
    final DatabaseInternal database = httpServer.getServer().getDatabase(databaseParam.getFirst(), false, false);

    final List<QueryResult> queryResults = new ArrayList<>();

    for (final Query query : readRequest.getQueries()) {
      // Find __name__ matcher to determine which type to query
      String metricName = null;
      final List<LabelMatcher> tagMatchers = new ArrayList<>();

      for (final LabelMatcher matcher : query.getMatchers()) {
        if ("__name__".equals(matcher.name()) && matcher.type() == MatchType.EQ)
          metricName = matcher.value();
        else
          tagMatchers.add(matcher);
      }

      if (metricName == null) {
        queryResults.add(new QueryResult(List.of()));
        continue;
      }

      final String typeName = PostPrometheusWriteHandler.sanitizeTypeName(metricName);

      if (!database.getSchema().existsType(typeName)) {
        queryResults.add(new QueryResult(List.of()));
        continue;
      }

      final DocumentType docType = database.getSchema().getType(typeName);
      if (!(docType instanceof LocalTimeSeriesType tsType) || tsType.getEngine() == null) {
        queryResults.add(new QueryResult(List.of()));
        continue;
      }

      final TimeSeriesEngine engine = tsType.getEngine();
      final List<ColumnDefinition> columns = tsType.getTsColumns();

      // Build TagFilter from label matchers (EQ only for now)
      // TagFilter.matches() accesses row[columnIndex + 1], so the index must be
      // the zero-based position among non-timestamp columns.
      TagFilter tagFilter = null;
      for (final LabelMatcher matcher : tagMatchers) {
        if (matcher.type() != MatchType.EQ)
          continue;

        final String colName = PostPrometheusWriteHandler.sanitizeColumnName(matcher.name());
        final int nonTsIndex = findNonTimestampColumnIndex(columns, colName);
        if (nonTsIndex < 0)
          continue;

        if (tagFilter == null)
          tagFilter = TagFilter.eq(nonTsIndex, matcher.value());
        else
          tagFilter = tagFilter.and(nonTsIndex, matcher.value());
      }

      // Query the engine
      final List<Object[]> rows = engine.query(query.getStartTimestampMs(), query.getEndTimestampMs(), null, tagFilter);

      // Group by label combination → TimeSeries
      final Map<String, List<Object[]>> grouped = new LinkedHashMap<>();
      for (final Object[] row : rows) {
        final String key = buildLabelKey(columns, row);
        grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
      }

      // Convert to Prometheus TimeSeries
      final List<TimeSeries> seriesList = new ArrayList<>();
      for (final Map.Entry<String, List<Object[]>> entry : grouped.entrySet()) {
        final List<Object[]> groupRows = entry.getValue();
        final Object[] firstRow = groupRows.getFirst();

        // Build labels — row[i] corresponds to columns.get(i)
        final List<Label> labels = new ArrayList<>();
        labels.add(new Label("__name__", metricName));
        for (int i = 0; i < columns.size(); i++) {
          final ColumnDefinition col = columns.get(i);
          if (col.getRole() == ColumnDefinition.ColumnRole.TAG) {
            final Object tagVal = firstRow[i];
            if (tagVal != null)
              labels.add(new Label(col.getName(), tagVal.toString()));
          }
        }

        // Build samples
        final List<Sample> samples = new ArrayList<>();
        final int valueColIndex = findFieldColumnIndex(columns, "value");
        for (final Object[] row : groupRows) {
          final long ts = (long) row[0];
          double value = 0;
          if (valueColIndex >= 0 && row[valueColIndex] instanceof Number n)
            value = n.doubleValue();
          samples.add(new Sample(value, ts));
        }

        seriesList.add(new TimeSeries(labels, samples));
      }

      queryResults.add(new QueryResult(seriesList));
    }

    // Encode response
    final ReadResponse readResponse = new ReadResponse(queryResults);
    final byte[] responseBytes = readResponse.encode();
    final byte[] compressed = Snappy.compress(responseBytes);

    // Send binary response
    exchange.getResponseHeaders().put(new HttpString("Content-Type"), "application/x-protobuf");
    exchange.setStatusCode(200);
    exchange.getResponseSender().send(ByteBuffer.wrap(compressed));

    return null; // response already sent
  }

  /**
   * Returns the zero-based index among non-timestamp columns for use with TagFilter,
   * which accesses row[columnIndex + 1].
   */
  private static int findNonTimestampColumnIndex(final List<ColumnDefinition> columns, final String name) {
    int nonTsIdx = -1;
    for (int i = 0; i < columns.size(); i++) {
      final ColumnDefinition col = columns.get(i);
      if (col.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
        continue;
      nonTsIdx++;
      if (col.getName().equals(name))
        return nonTsIdx;
    }
    return -1;
  }

  private static int findFieldColumnIndex(final List<ColumnDefinition> columns, final String name) {
    for (int i = 0; i < columns.size(); i++) {
      final ColumnDefinition col = columns.get(i);
      if (col.getRole() == ColumnDefinition.ColumnRole.FIELD && col.getName().equals(name))
        return i;
    }
    return -1;
  }

  private static String buildLabelKey(final List<ColumnDefinition> columns, final Object[] row) {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < columns.size(); i++) {
      if (columns.get(i).getRole() == ColumnDefinition.ColumnRole.TAG) {
        if (!sb.isEmpty())
          sb.append('|');
        final Object val = row[i];
        sb.append(val != null ? val.toString() : "");
      }
    }
    return sb.toString();
  }
}
