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
import com.arcadedb.engine.timeseries.AggregationMetrics;
import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.engine.timeseries.TimeSeriesGateway;
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
import com.arcadedb.server.monitor.TimeSeriesReadMetrics;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * HTTP handler for PromQL series lookup.
 * Endpoint: GET /api/v1/ts/{database}/prom/api/v1/series
 * <p>
 * On {@link DatabaseAbstractHandler} since issue #7681, for the reasons spelled out on
 * {@link PostGrafanaQueryHandler}: a request carrying {@code arcadedb-session-id} reads through that session's
 * transaction, under its lock and on its principal and refreshing its idle timer, and the base class subsumes
 * the {@code checkAuthorizationOnDatabase} call this handler used to make by hand.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GetPromQLSeriesHandler extends AbstractObservabilityHandler {

  public GetPromQLSeriesHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  /**
   * A full-range scan of every sample of every type the {@code match[]} selector resolves to, so never on an
   * Undertow IO thread (issue #7722), for the reason {@code GetPromQLLabelValuesHandler} gives at its own
   * override: the work is bounded by the size of the series, not by anything in the request, and an IO thread
   * parked on it stops serving every other connection it multiplexes.
   * <p>
   * Handler-wide, superseding the per-request override of issue #7681 for the reason given there: the
   * session-less request is the one Grafana and Prometheus actually send.
   */
  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final Database db, final JSONObject payload) throws Exception {

    final Deque<String> matchParams = exchange.getQueryParameters().get("match[]");
    if (matchParams == null || matchParams.isEmpty())
      return new ExecutionResponse(400,
          PromQLResponseFormatter.formatError("bad_data", "Missing required parameter: match[]"));

    // Parsed with the range endpoint's own parser rather than a bare Double.parseDouble (issue #7709, found while
    // giving /label/{name}/values the same bounds). The bare call let a malformed `start` leave this method as a
    // NumberFormatException, which the pipeline maps to a 500 - so a Grafana panel with a typo in its time
    // variable got "internal server error" where Prometheus answers 400 bad_data - and it accepted `Infinity` and
    // `9e15` without a murmur, which is the unbounded-span hazard issue #6807 closed on /query_range.
    final String startStr = getQueryParameter(exchange, "start");
    final String endStr = getQueryParameter(exchange, "end");
    final long startMs;
    final long endMs;
    try {
      startMs = startStr != null && !startStr.isBlank()
          ? GetPromQLQueryRangeHandler.parseTimestampMs("start", startStr) : Long.MIN_VALUE;
      endMs = endStr != null && !endStr.isBlank()
          ? GetPromQLQueryRangeHandler.parseTimestampMs("end", endStr) : Long.MAX_VALUE;
    } catch (final IllegalArgumentException e) {
      return new ExecutionResponse(400, PromQLResponseFormatter.formatError("bad_data", e.getMessage()));
    }
    if (startMs > endMs)
      return new ExecutionResponse(400,
          PromQLResponseFormatter.formatError("bad_data", "end timestamp must not be before start timestamp"));

    final DatabaseInternal database = (DatabaseInternal) db;
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

        // forEachTagCombination, not forEachRow and not query(): the answer is the number of distinct label
        // COMBINATIONS the metric carries, while query() merges every shard's full range into one ArrayList and
        // sorts it by timestamp - a sort this loop does not use. start/end default to the full range here, so
        // `?match[]=cpu` with no time range used to read a whole series into memory to enumerate a handful of
        // label sets (issue #7354), and then still to read every SAMPLE of it to enumerate a handful of tuples
        // (issue #7710). A sealed block that declares one combination now answers with one row off its directory
        // entry; the fold below is unchanged, because those rows have the layout the scan produces.
        final TimeSeriesEngine engine = tsType.getEngine();
        final List<ColumnDefinition> columns = tsType.getTsColumns();

        // PROJECTION, not the whole row (issue #7371). The answer is made of the TAG columns, so those are the
        // only ones the scan decodes: a sealed block stores each column in its own byte range and
        // TimeSeriesSealedStore.decompressColumns() reads only the ranges the projection names, while
        // TimeSeriesBucket.readRow() boxes only those. The DOUBLE value column every Prometheus metric carries is
        // therefore never decoded to enumerate label sets. See the same note in GetPromQLLabelValuesHandler.
        //
        // resolveColumnIndices() answers null - meaning "every column" - for an empty request, and a type with no
        // TAG columns wants the opposite: nothing but the timestamp, whose presence is still what decides whether
        // the metric has a series in the range at all. That projection is spelled out here rather than inherited.
        final List<String> tagColumnNames = tagColumnNamesOf(columns);
        final int[] columnIndices = tagColumnNames.isEmpty()
            ? new int[0]
            : TimeSeriesGateway.resolveColumnIndices(tagColumnNames, columns);

        // ROW LAYOUT. row[0] is the timestamp by the scan's own contract - every layer builds the row as
        // { ts, selected non-ts columns in schema order... } - and the columns after it are exactly what
        // TimeSeriesGateway.selectedColumns() lists, so tagNames[i] names the value in row[i + 1]. The names are
        // read off that list rather than off the schema index, which is what the previous code did and what
        // rested on the TIMESTAMP column being declared first: true of every type-creation path but enforced by
        // none of them, and the difference between a tag value and the neighbouring column's.
        // Resolved once per type, not per row: the visitor below runs once per SAMPLE.
        final List<ColumnDefinition> projected = TimeSeriesGateway.selectedColumns(columns, columnIndices);
        final String[] tagNames = new String[projected.size() - 1];
        for (int i = 1; i < projected.size(); i++)
          tagNames[i - 1] = projected.get(i).getName();
        // Reused across rows: the dedup key is built per row because that is what identifies the combination,
        // but the buffer it is built in need not be. The labels map is built only for a combination not seen
        // before, i.e. once per SERIES rather than once per sample (issue #7354).
        //
        // The visitor holds NO lock (see TimeSeriesRowVisitor, corrected by issue #8052 - this comment used to
        // claim the shard's read locks). It folds, it does not compute and it never calls back into the engine,
        // and since issue #7897 the reason is not that a writer is waiting behind it but that the walk is reading
        // a directory snapshot that goes stale under it: every extra millisecond spent here is a millisecond in
        // which a retention pass can remove a block this scan has not reached yet.
        final StringBuilder key = new StringBuilder(64);

        // What the scan actually did, published to whatever the server's metrics subsystem feeds (issue #7717).
        // null - and therefore free - whenever metrics are off. It counts what forEachTagCombination did, which
        // for a block answered from its directory entry is a SKIPPED block and no materialised row at all - the
        // saving of issue #7710 is therefore visible in these metrics rather than hidden by them.
        final AggregationMetrics readMetrics = TimeSeriesReadMetrics.start();
        try {
          engine.forEachTagCombination(startMs, endMs, columnIndices, readMetrics, row -> {
            key.setLength(0);
            // The metric name is length-prefixed for the same reason its tags are: two match[] patterns naming
            // different metrics share this map.
            key.append(vs.metricName().length()).append(':').append(vs.metricName());
            for (int t = 0; t < tagNames.length; t++)
              if (t + 1 < row.length && row[t + 1] != null) {
                final String name = tagNames[t];
                final String value = row[t + 1].toString();
                // An empty value is an ABSENT label in Prometheus, so it takes no part in the series identity:
                // see PromQLResponseFormatter.isLabelValuePresent (issue #7712). Skipped HERE and not only when
                // the labels map is built, because otherwise a sample whose host is null and one carrying no
                // host at all would spell two keys and be reported as two series that render identically.
                if (!PromQLResponseFormatter.isLabelValuePresent(value))
                  continue;
                // LENGTH-PREFIXED, not separated by a character the value is assumed not to carry. A tag value is
                // ingested from a remote-write client, so "realistically never contains this byte" is an assumption
                // about somebody else's data; a length prefix makes the concatenation unambiguous whatever the
                // value holds, and two distinct combinations cannot spell one key. Costs one int per tag.
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
                  new ObservedSeries(labelsOf(vs.metricName(), tagNames, row), timestamp));
            return true;
          });
        } finally {
          TimeSeriesReadMetrics.publish(readMetrics, database.getName(), typeName,
              TimeSeriesReadMetrics.SURFACE_PROM_SERIES);
        }
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

  /** The names of the TAG columns, in schema order - the projection the scan is asked for. */
  private static List<String> tagColumnNamesOf(final List<ColumnDefinition> columns) {
    final List<String> names = new ArrayList<>(columns.size());
    for (final ColumnDefinition column : columns)
      if (column.getRole() == ColumnDefinition.ColumnRole.TAG)
        names.add(column.getName());
    return names;
  }

  /**
   * The label set of one row, built once per distinct combination rather than once per sample.
   * {@code tagNames[t]} names the value in {@code row[t + 1]}; see the ROW LAYOUT note above.
   * <p>
   * A label whose value is empty is left OUT, because in Prometheus that is what an absent label is - the same
   * rule the dedup key above applies, and the two have to agree or a series would be keyed on a label it is not
   * reported with (issue #7712).
   */
  private static Map<String, String> labelsOf(final String metricName, final String[] tagNames, final Object[] row) {
    final Map<String, String> labels = new LinkedHashMap<>();
    labels.put("__name__", metricName);
    for (int t = 0; t < tagNames.length; t++)
      if (t + 1 < row.length && row[t + 1] != null
          && PromQLResponseFormatter.isLabelValuePresent(row[t + 1].toString()))
        labels.put(tagNames[t], row[t + 1].toString());
    return labels;
  }
}
