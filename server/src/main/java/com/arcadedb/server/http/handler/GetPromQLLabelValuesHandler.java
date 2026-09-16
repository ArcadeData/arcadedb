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
import com.arcadedb.engine.timeseries.AggregationMetrics;
import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.engine.timeseries.TimeSeriesGateway;
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
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * HTTP handler for listing PromQL label values.
 * Endpoint: GET /api/v1/ts/{database}/prom/api/v1/label/{name}/values
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GetPromQLLabelValuesHandler extends AbstractServerHttpHandler {

  public GetPromQLLabelValuesHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  /**
   * A full-range scan of every sample of every type the label appears on, so never on an Undertow IO thread
   * (issue #7722).
   * <p>
   * The work is unbounded in the size of the SERIES rather than in the size of the request: the endpoint takes
   * no bound the caller could narrow, and the {@code start}/{@code end} the Prometheus API defines for it are
   * accepted and ignored here, so there is no request the server can answer cheaply. An IO thread serves many
   * connections at once, and the cost of parking one is not paid by the caller whose scan it is - it is paid by
   * every unrelated connection multiplexed onto the same thread.
   * <p>
   * This route is on Grafana's variable-refresh path, so a dashboard with one templated variable issues it on
   * every load and on every refresh interval.
   */
  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
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

    final Deque<String> nameParam = exchange.getQueryParameters().get("name");
    if (nameParam == null || nameParam.isEmpty())
      return new ExecutionResponse(400, PromQLResponseFormatter.formatError("bad_data", "Label name parameter is required"));

    final String labelName = nameParam.getFirst();
    final DatabaseInternal database = httpServer.getServer().getDatabase(databaseParam.getFirst(), false, false);

    final Set<String> values = new LinkedHashSet<>();

    if ("__name__".equals(labelName)) {
      // Return all TimeSeries type names
      for (final DocumentType type : database.getSchema().getTypes())
        if (type instanceof LocalTimeSeriesType tsType && tsType.getEngine() != null
            && SecurityHelper.canAccessType(database, tsType, SecurityDatabaseUser.ACCESS.READ_RECORD))
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

        // PROJECTION, not the whole row (issue #7371). The answer is made of one column, so that is the only one
        // the scan decodes. In the sealed layer that is the larger saving: a block stores each column in its own
        // byte range and TimeSeriesSealedStore.decompressColumns() reads only the ranges the projection names, so
        // the DOUBLE value column every Prometheus metric carries is not read off disk and not Gorilla-decoded.
        // In the mutable layer TimeSeriesBucket.readRow() still walks the row - it has to, to find each column's
        // offset - but calls readColumnValue() only on the projected column, so nothing else is decoded or boxed.
        // Either way the rows handed to the visitor are Object[2] rather than the full width.
        //
        // columnIndices count NON-TIMESTAMP columns, and the projected row is { timestamp, selected columns in
        // schema order } - the contract TimeSeriesGateway.selectedColumns() spells out and the row layout
        // TimeSeriesBucket.readRow() builds. The slot is therefore READ from that list rather than assumed to be
        // the column's schema index, which is what the previous code did and what rested on the TIMESTAMP column
        // being declared first - true of every type-creation path but enforced by none of them.
        final int[] columnIndices = TimeSeriesGateway.resolveColumnIndices(List.of(labelName), columns);
        final int valueSlot = projectedSlotOf(labelName, TimeSeriesGateway.selectedColumns(columns, columnIndices));
        if (valueSlot < 0)
          continue;

        // forEachRow, not query(): the answer is the tag's cardinality - a handful of hosts or regions - while
        // query() merges every shard's full range into one ArrayList and sorts it by timestamp, a sort this loop
        // does not use at all. A Grafana datasource calls this to populate a label picker, on every dashboard load
        // and every variable refresh, so a type holding millions of samples used to allocate the whole series per
        // call. The rows still have to be READ, but they no longer have to be resident (issue #7354).
        // The visitor runs under the shard's read locks (see TimeSeriesRowVisitor): it folds, it does not compute
        // and it never calls back into the engine.
        final TimeSeriesEngine engine = tsType.getEngine();
        // What the scan actually did, published to whatever the server's metrics subsystem feeds (issue #7717):
        // on this route above all, because whether the sealed layer answers a block from its declared tag values
        // or has to decompress it is the difference between a label picker that costs nothing and one that reads
        // the series. null - and therefore free - whenever metrics are off.
        final AggregationMetrics readMetrics = TimeSeriesReadMetrics.start();
        try {
          engine.forEachRow(Long.MIN_VALUE, Long.MAX_VALUE, columnIndices, null, readMetrics, row -> {
            if (valueSlot < row.length && row[valueSlot] != null)
              values.add(row[valueSlot].toString());
            return true;
          });
        } finally {
          TimeSeriesReadMetrics.publish(readMetrics, database.getName(), tsType.getName(),
              TimeSeriesReadMetrics.SURFACE_PROM_LABEL_VALUES);
        }
      }
    }

    final List<String> sorted = new ArrayList<>(values);
    Collections.sort(sorted);
    return new ExecutionResponse(200, PromQLResponseFormatter.formatLabelsResponse(sorted));
  }

  /** Whether the type declares a TAG column with this name. A FIELD of the same name is not a label. */
  private static boolean hasTagColumn(final String name, final List<ColumnDefinition> columns) {
    for (final ColumnDefinition column : columns)
      if (column.getRole() == ColumnDefinition.ColumnRole.TAG && column.getName().equals(name))
        return true;
    return false;
  }

  /**
   * The slot a projected column's value occupies in the scanned row, or {@code -1} when the projection does not
   * carry it. {@code row[0]} is the timestamp on every read path, and the columns
   * {@link TimeSeriesGateway#selectedColumns(List, int[])} lists after it follow in the same order.
   */
  private static int projectedSlotOf(final String name, final List<ColumnDefinition> projected) {
    for (int i = 1; i < projected.size(); i++)
      if (projected.get(i).getName().equals(name))
        return i;
    return -1;
  }
}
