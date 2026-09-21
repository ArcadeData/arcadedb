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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.engine.timeseries.TagFilter;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.RequestBodyTooLargeException;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * HTTP handler for Prometheus remote_read protocol.
 * Endpoint: POST /api/v1/ts/{database}/prom/read
 * <p>
 * Receives Snappy-compressed protobuf ReadRequest messages,
 * queries the TimeSeries engine, and returns Snappy-compressed protobuf ReadResponse.
 * <p>
 * Session-aware since issue #7681: {@link AbstractBinaryHttpHandler} was reparented onto
 * {@link DatabaseAbstractHandler} in that change, so a request carrying {@code arcadedb-session-id} reads
 * through that session's transaction, under its lock and on its principal and refreshing its idle timer, and
 * the base class subsumes the {@code checkAuthorizationOnDatabase} call this handler used to make by hand.
 * <p>
 * {@link #requiresTransaction()} is false: remote_read is a read, and an auto-commit wrapper would only add a
 * commit with nothing to commit. An unresolvable session id therefore degrades to a session-less read rather
 * than being refused, the same asymmetry with {@link PostPrometheusWriteHandler} that #7402 established
 * between the {@code /api/v1/ts} reads and write.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PostPrometheusReadHandler extends AbstractBinaryHttpHandler {

  public PostPrometheusReadHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  protected boolean requiresTransaction() {
    return false;
  }

  /**
   * Answered here rather than inherited from {@link AbstractObservabilityHandler}, which this handler cannot
   * extend because its binary body already spends its one superclass (issue #7859). The reason is that class's:
   * a remote-read never writes into the caller's transaction, so the 413 it raises when the answer would exceed
   * {@code arcadedb.server.maxResultRows} must not destroy a transaction the client opened with {@code /begin}
   * and still believes it owns.
   */
  @Override
  protected boolean participatesInSessionTransaction() {
    return false;
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final Database db, final JSONObject payload) throws Exception {

    // THIS request's bytes, off the exchange rather than off a field shared with every concurrent request; see
    // AbstractBinaryHttpHandler.RAW_BINARY_PAYLOAD and issue #7683.
    final byte[] rawBytes = rawBytes(exchange);
    if (rawBytes == null || rawBytes.length == 0)
      return new ExecutionResponse(400, "{ \"error\" : \"Request body is empty\"}");

    // Snappy decompress, under the decoded-body budget (issue #8084): the read side decodes a body exactly as the
    // write side does, so it carries the same exposure and takes the same bound. See PostPrometheusWriteHandler for
    // why the 413 is rethrown past the 400 below.
    final byte[] decompressed;
    try {
      decompressed = CompressedBodyDecoder.snappyUncompress(rawBytes,
          CompressedBodyDecoder.maxDecompressedSize(httpServer.getServer().getConfiguration()));
    } catch (final RequestBodyTooLargeException e) {
      throw e;
    } catch (final Exception e) {
      return new ExecutionResponse(400, "{ \"error\" : \"Invalid Snappy-compressed data\"}");
    }

    final ReadRequest readRequest = ReadRequest.decode(decompressed);
    final DatabaseInternal database = (DatabaseInternal) db;

    // The hard ceiling this endpoint never consulted (issue #7663). One budget for the WHOLE response: a
    // remote-read request carries one Query per selector and they all travel back in a single ReadResponse, so a
    // per-query ceiling would let a request with twenty selectors return twenty times the maximum.
    final TimeSeriesHandlerUtils.RowBudget budget = new TimeSeriesHandlerUtils.RowBudget(getMaxResultRows());

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
      if (!(docType instanceof LocalTimeSeriesType tsType)) {
        queryResults.add(new QueryResult(List.of()));
        continue;
      }

      // Gated accessor (per-type ACL): the query names this metric explicitly, so a denial is answered with a
      // 403 rather than an empty result - an empty result is how a NON-EXISTENT metric answers, and conflating
      // the two would let the caller keep probing a type it has no read right on.
      final TimeSeriesEngine engine = tsType.getEngine(SecurityDatabaseUser.ACCESS.READ_RECORD);
      if (engine == null) {
        queryResults.add(new QueryResult(List.of()));
        continue;
      }
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

        // A PromQL matcher value is always text; coerce it to the column's declared type so it matches
        // what both storage layers hand back (issue #5475).
        final Object coerced = nonTimestampColumn(columns, nonTsIndex).coerceValue(matcher.value());
        if (tagFilter == null)
          tagFilter = TagFilter.eq(nonTsIndex, coerced);
        else
          tagFilter = tagFilter.and(nonTsIndex, coerced);
      }

      // Issue #7663: engine.query() merged every shard's full range into one sorted ArrayList before a single
      // sample was looked at, so a selector over a wide range cost O(matching rows) heap however few series it
      // resolved to. The bounded ascending fetch stops each shard as soon as its own bound is satisfied, and the
      // one row past the budget is what proves the response would have exceeded the ceiling.
      final List<Object[]> rows = engine.queryAscending(query.getStartTimestampMs(), query.getEndTimestampMs(),
          null, tagFilter, budget.fetchLimit(), null);
      // Refused rather than truncated: Prometheus has no way to represent a partial remote-read response, so a
      // silently cut one is indistinguishable from a gap in the data (issue #5719's rule, applied here).
      if (!budget.charge(rows.size()))
        throw resultSetTooLarge(budget.ceiling());

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
  /**
   * The column at the given position among the non-timestamp columns.
   */
  private static ColumnDefinition nonTimestampColumn(final List<ColumnDefinition> columns, final int nonTsIndex) {
    int nonTsIdx = -1;
    for (final ColumnDefinition col : columns) {
      if (col.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
        continue;
      if (++nonTsIdx == nonTsIndex)
        return col;
    }
    throw new IllegalArgumentException("No non-timestamp column at index " + nonTsIndex);
  }

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
