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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityHelper;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.monitor.TimeSeriesReadMetrics;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * HTTP handler for listing PromQL label values.
 * Endpoint: GET /api/v1/ts/{database}/prom/api/v1/label/{name}/values
 * <p>
 * On {@link DatabaseAbstractHandler} since issue #7681, for the reasons spelled out on
 * {@link PostGrafanaQueryHandler}: a request carrying {@code arcadedb-session-id} reads through that session's
 * transaction, under its lock and on its principal and refreshing its idle timer, and the base class subsumes
 * the {@code checkAuthorizationOnDatabase} call this handler used to make by hand.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GetPromQLLabelValuesHandler extends AbstractObservabilityHandler {

  public GetPromQLLabelValuesHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  /**
   * A scan of every sample of every type the label appears on, so never on an Undertow IO thread (issue #7722).
   * <p>
   * The work is unbounded in the size of the SERIES rather than in the size of the request: {@code start}/{@code
   * end} narrow it since issue #7709, but they are optional and the request Grafana's variable refresh sends
   * carries neither, so the server still has to be able to answer the whole-series one. An IO thread serves many
   * connections at once, and the cost of parking one is not paid by the caller whose scan it is - it is paid by
   * every unrelated connection multiplexed onto the same thread.
   * <p>
   * This route is on Grafana's variable-refresh path, so a dashboard with one templated variable issues it on
   * every load and on every refresh interval.
   * <p>
   * Answered handler-wide, which SUPERSEDES the per-request override issue #7681 gave this handler. That one
   * dispatched only a request naming a session, deliberately leaving the session-less case as it was - and the
   * session-less case is the one Grafana and Prometheus exercise, since neither ever sends
   * {@code arcadedb-session-id}. Both reasons to leave the IO thread still hold; this is the wider of the two.
   */
  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final Database db, final JSONObject payload) throws Exception {

    final Deque<String> nameParam = exchange.getQueryParameters().get("name");
    if (nameParam == null || nameParam.isEmpty())
      return new ExecutionResponse(400, PromQLResponseFormatter.formatError("bad_data", "Label name parameter is required"));

    final String labelName = nameParam.getFirst();
    final DatabaseInternal database = (DatabaseInternal) db;

    // Prometheus documents this endpoint as answering "in the specified time range", and it used to read neither
    // bound - so a Grafana picker scoped to the last hour was offered every value the type had ever held that
    // retention had not yet expired, and the sibling /series endpoint, which does read them, disagreed with this
    // one about what a range means (issue #7709). Both are optional, and absent they mean the whole series, which
    // is what every request sent before this change was.
    //
    // Parsed with the range endpoint's own parser rather than a second Double.parseDouble: it refuses a
    // non-finite or out-of-epoch value as well as an unparseable one, which is the bound issue #6807 added, and
    // a discovery endpoint has no reason to accept a `start` the query endpoints reject.
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
    // A range the caller inverted selects nothing, and saying so is cheaper and clearer than answering an empty
    // list for a request that cannot have been meant.
    if (startMs > endMs)
      return new ExecutionResponse(400,
          PromQLResponseFormatter.formatError("bad_data", "end timestamp must not be before start timestamp"));

    final boolean rangeRequested = startMs != Long.MIN_VALUE || endMs != Long.MAX_VALUE;

    final Set<String> values = new LinkedHashSet<>();

    if ("__name__".equals(labelName)) {
      // Return the TimeSeries type names, which are the metric names.
      for (final DocumentType type : database.getSchema().getTypes())
        if (type instanceof LocalTimeSeriesType tsType && tsType.getEngine() != null
            && SecurityHelper.canAccessType(database, tsType, SecurityDatabaseUser.ACCESS.READ_RECORD)
            // A metric with no sample in the window is not a metric the range carries. Asked only when a bound
            // was actually sent: an unscoped request must keep naming every type, a type holding no sample at
            // all included, which is what it answered before the bounds existed.
            && (!rangeRequested || hasSamplesInRange(database, tsType, startMs, endMs)))
          // A metric the caller cannot read must not be named back to it.
          values.add(type.getName());
    } else {
      // Scan types that have this TAG column, query distinct values
      for (final DocumentType type : database.getSchema().getTypes()) {
        if (!(type instanceof LocalTimeSeriesType tsType) || tsType.getEngine() == null)
          continue;
        // Same filter for the value scan below, which reads the samples themselves.
        if (!SecurityHelper.canAccessType(database, tsType, SecurityDatabaseUser.ACCESS.READ_RECORD))
          continue;
        final List<ColumnDefinition> columns = tsType.getTsColumns();
        if (!hasTagColumn(labelName, columns))
          continue;

        // The answer is largely already RECORDED, so most of it is read rather than computed (issue #7660): every
        // sealed block's directory entry carries the complete distinct value set of each of its TAG columns, built
        // at seal time from the very rows the block holds. Unioning those entries costs O(blocks x cardinality) and
        // decompresses nothing, which matters because a Grafana datasource calls this to populate a label picker on
        // every dashboard load and every variable refresh - against a type holding millions of samples, the scan
        // this replaces read every one of them to produce a set of five strings.
        //
        // The union is EXACT, not an over-approximation: TimeSeriesSealedStore.collectDistinctTagValues() reads a
        // block rather than its declaration in the one case where the two would disagree, and no path removes rows
        // from a sealed block without recomputing the declaration. That distinction is the whole reason this was
        // left out of issue #7371 - Prometheus documents /label/{name}/values as the values a label actually
        // carries, so naming a series that no surviving sample carries is a behaviour change a user sees in a
        // Grafana picker, not a free win. The mutable bucket has no declaration and is still scanned, on the same
        // one-column projection issue #7371 introduced, but it is bounded by the compaction interval.
        //
        // The range narrows all of that further and costs nothing extra (issue #7709): a block outside
        // [startMs, endMs] is dropped on its directory entry, a block inside it still answers from its
        // declaration, and only the at most two blocks that straddle a bound are decompressed and filtered per row.
        //
        // What the read actually did, published to whatever the server's metrics subsystem feeds (issue #7717):
        // on this route above all, because the ratio of blocks answered from a declaration to blocks that had to
        // be decompressed is exactly what says whether the push-down issue #7660 added is working for a given
        // tenant's data - and an operator could not see it at all. null - and therefore free - when metrics are
        // off.
        final AggregationMetrics readMetrics = TimeSeriesReadMetrics.start();
        try {
          tsType.getEngine().collectDistinctTagValues(labelName, startMs, endMs, values, readMetrics);
        } finally {
          TimeSeriesReadMetrics.publish(readMetrics, database.getName(), tsType.getName(),
              TimeSeriesReadMetrics.SURFACE_PROM_LABEL_VALUES);
        }
      }
    }

    // An empty label value is an ABSENT label in Prometheus, never one of the label's values: see
    // PromQLResponseFormatter.isLabelValuePresent (issue #7712). Removed here, at the point the answer is
    // rendered, rather than inside the fold - the engine must keep reporting what the samples hold, and the
    // same tag column still answers "" through the generic /ts surface.
    values.removeIf(value -> !PromQLResponseFormatter.isLabelValuePresent(value));

    final List<String> sorted = new ArrayList<>(values);
    Collections.sort(sorted);
    return new ExecutionResponse(200, PromQLResponseFormatter.formatLabelsResponse(sorted));
  }

  /**
   * Whether this metric carries a sample in the requested window (issue #7709), counted into the same read
   * metrics as every other read on this route so an operator sees what the probe cost.
   */
  private static boolean hasSamplesInRange(final DatabaseInternal database, final LocalTimeSeriesType tsType,
      final long startMs, final long endMs) throws IOException {
    final AggregationMetrics readMetrics = TimeSeriesReadMetrics.start();
    try {
      return tsType.getEngine().hasRowsInRange(startMs, endMs, readMetrics);
    } finally {
      TimeSeriesReadMetrics.publish(readMetrics, database.getName(), tsType.getName(),
          TimeSeriesReadMetrics.SURFACE_PROM_LABEL_VALUES);
    }
  }

  /** Whether the type declares a TAG column with this name. A FIELD of the same name is not a label. */
  private static boolean hasTagColumn(final String name, final List<ColumnDefinition> columns) {
    for (final ColumnDefinition column : columns)
      if (column.getRole() == ColumnDefinition.ColumnRole.TAG && column.getName().equals(name))
        return true;
    return false;
  }
}
