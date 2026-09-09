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
import com.arcadedb.engine.timeseries.promql.PromQLEvaluator;
import com.arcadedb.engine.timeseries.promql.PromQLParser;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.VectorSelector;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityHelper;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * HTTP handler for PromQL series lookup.
 * Endpoint: GET /api/v1/ts/{database}/prom/api/v1/series
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GetPromQLSeriesHandler extends AbstractServerHttpHandler {

  public GetPromQLSeriesHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) throws Exception {

    final Deque<String> databaseParam = exchange.getQueryParameters().get("database");
    if (databaseParam == null || databaseParam.isEmpty())
      return new ExecutionResponse(400, PromQLResponseFormatter.formatError("bad_data", "Database parameter is required"));

    // Enforce database-level authorization (GHSA-x8mg-6r4p-87pf): this handler does not extend DatabaseAbstractHandler.
    // Checked before any payload/parameter validation so an unauthorized caller cannot probe the target database.
    checkAuthorizationOnDatabase(user, databaseParam.getFirst());

    final Deque<String> matchParams = exchange.getQueryParameters().get("match[]");
    if (matchParams == null || matchParams.isEmpty())
      return new ExecutionResponse(400,
          PromQLResponseFormatter.formatError("bad_data", "Missing required parameter: match[]"));

    final String startStr = getQueryParameter(exchange, "start");
    final String endStr = getQueryParameter(exchange, "end");
    final long startMs = startStr != null ? (long) (Double.parseDouble(startStr) * 1000) : Long.MIN_VALUE;
    final long endMs = endStr != null ? (long) (Double.parseDouble(endStr) * 1000) : Long.MAX_VALUE;

    final DatabaseInternal database = httpServer.getServer().getDatabase(databaseParam.getFirst(), false, false);
    // Keyed by the label combination, in first-seen order.
    // The order matters: query() used to hand this loop the rows already sorted by timestamp, so the response
    // came out ordered by when each series first appears. forEachRow visits shard by shard, which would have
    // silently reordered the response - so the ordering the old sort produced is now stated rather than
    // inherited, and it costs one long per distinct series instead of a sort of the whole range (issue #7354).
    final Map<String, ObservedSeries> seriesByKey = new LinkedHashMap<>();

    for (final String matchStr : matchParams) {
      try {
        final PromQLExpr expr = new PromQLParser(matchStr).parse();
        if (!(expr instanceof VectorSelector vs))
          continue;

        final String typeName = PromQLEvaluator.sanitizeTypeName(vs.metricName());
        if (!database.getSchema().existsType(typeName))
          continue;

        final DocumentType docType = database.getSchema().getType(typeName);
        if (!(docType instanceof LocalTimeSeriesType tsType) || tsType.getEngine() == null)
          continue;

        // Series discovery: a type the caller cannot read is omitted rather than reported as an error, the same
        // way SELECT FROM schema:types hides it - a matcher legitimately spans several metrics here.
        if (!SecurityHelper.canAccessType(database, tsType, SecurityDatabaseUser.ACCESS.READ_RECORD))
          continue;

        // forEachRow, not query(): the answer is the number of distinct label COMBINATIONS the metric carries,
        // while query() merges every shard's full range into one ArrayList and sorts it by timestamp - a sort this
        // loop does not use. start/end default to the full range here, so `?match[]=cpu` with no time range used
        // to read a whole series into memory to enumerate a handful of label sets (issue #7354).
        final TimeSeriesEngine engine = tsType.getEngine();
        final List<ColumnDefinition> columns = tsType.getTsColumns();

        // ROW LAYOUT. row[0] is the timestamp by the scan's own contract - every layer builds the row as
        // { ts, non-ts columns... } - but row[i] for i >= 1 lining up with columns.get(i) holds only because the
        // TIMESTAMP column is declared first, which is what every type-creation path happens to do and what
        // nothing in TimeSeriesTypeBuilder actually enforces. Carried over from the query()-based code this
        // replaces rather than introduced here; stated so that a change to that convention is a change someone
        // can find, instead of a tag value silently read out of the wrong slot.
        // The indices of the TAG columns, resolved once per type instead of re-testing every column's role on
        // every row - the loop below runs once per SAMPLE, and the answer is the same for all of them.
        final int[] tagColumns = tagColumnsOf(columns);
        // Reused across rows: the dedup key is built per row because that is what identifies the combination,
        // but the buffer it is built in need not be. The labels map is built only for a combination not seen
        // before, i.e. once per SERIES rather than once per sample (issue #7354).
        //
        // The visitor runs under the shard's read locks (see TimeSeriesRowVisitor): it folds, it does not compute
        // and it never calls back into the engine.
        final StringBuilder key = new StringBuilder(64);

        engine.forEachRow(startMs, endMs, null, null, null, row -> {
          key.setLength(0);
          // The metric name is length-prefixed for the same reason its tags are: two match[] patterns naming
          // different metrics share this map.
          key.append(vs.metricName().length()).append(':').append(vs.metricName());
          for (final int i : tagColumns)
            if (i < row.length && row[i] != null) {
              // LENGTH-PREFIXED, not separated by a character the value is assumed not to carry. A tag value is
              // ingested from a remote-write client, so "realistically never contains this byte" is an assumption
              // about somebody else's data; a length prefix makes the concatenation unambiguous whatever the
              // value holds, and two distinct combinations cannot spell one key. Costs one int per tag.
              final String name = columns.get(i).getName();
              final String value = row[i].toString();
              key.append(name.length()).append(':').append(name)
                  .append(value.length()).append(':').append(value);
            }

          final long timestamp = (long) row[0];
          final String combination = key.toString();
          final ObservedSeries seen = seriesByKey.get(combination);
          if (seen != null)
            seen.earliest = Math.min(seen.earliest, timestamp);
          else
            seriesByKey.put(combination,
                new ObservedSeries(labelsOf(vs.metricName(), columns, tagColumns, row), timestamp));
          return true;
        });
      } catch (final IllegalArgumentException ignored) {
        // Skip malformed match patterns
      }
    }

    // Ordered by the timestamp each series was first observed at. Sorted with List#sort, which is stable, so two
    // series whose earliest sample shares a timestamp keep the order they were first seen in - the same tiebreak
    // the timestamp sort in query() used to give this loop.
    final List<ObservedSeries> observed = new ArrayList<>(seriesByKey.values());
    observed.sort(Comparator.comparingLong(series -> series.earliest));

    final List<Map<String, String>> seriesList = new ArrayList<>(observed.size());
    for (final ObservedSeries series : observed)
      seriesList.add(series.labels);

    return new ExecutionResponse(200, PromQLResponseFormatter.formatSeriesResponse(seriesList));
  }

  /** One distinct label combination and the earliest timestamp any sample carrying it was observed at. */
  private static final class ObservedSeries {
    private final Map<String, String> labels;
    private       long                earliest;

    private ObservedSeries(final Map<String, String> labels, final long earliest) {
      this.labels = labels;
      this.earliest = earliest;
    }
  }

  /** The indices of the TAG columns, in schema order. */
  private static int[] tagColumnsOf(final List<ColumnDefinition> columns) {
    final int[] indices = new int[columns.size()];
    int count = 0;
    for (int i = 0; i < columns.size(); i++)
      if (columns.get(i).getRole() == ColumnDefinition.ColumnRole.TAG)
        indices[count++] = i;
    return Arrays.copyOf(indices, count);
  }

  /** The label set of one row, built once per distinct combination rather than once per sample. */
  private static Map<String, String> labelsOf(final String metricName, final List<ColumnDefinition> columns,
      final int[] tagColumns, final Object[] row) {
    final Map<String, String> labels = new LinkedHashMap<>();
    labels.put("__name__", metricName);
    for (final int i : tagColumns)
      if (i < row.length && row[i] != null)
        labels.put(columns.get(i).getName(), row[i].toString());
    return labels;
  }
}
