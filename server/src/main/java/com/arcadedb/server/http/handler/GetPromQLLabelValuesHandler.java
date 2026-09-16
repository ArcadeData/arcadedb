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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityHelper;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
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
 * <p>
 * On {@link DatabaseAbstractHandler} since issue #7681, for the reasons spelled out on
 * {@link PostGrafanaQueryHandler}: a request carrying {@code arcadedb-session-id} reads through that session's
 * transaction, under its lock and on its principal and refreshing its idle timer, and the base class subsumes
 * the {@code checkAuthorizationOnDatabase} call this handler used to make by hand.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GetPromQLLabelValuesHandler extends DatabaseAbstractHandler {

  public GetPromQLLabelValuesHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  /**
   * A read: an auto-commit wrapper would only add a commit with nothing to commit, so an unresolvable session
   * id degrades to a session-less read rather than being refused - see
   * {@link DatabaseAbstractHandler#rejectsUnresolvableSession()}.
   */
  @Override
  protected boolean requiresTransaction() {
    return false;
  }

  /**
   * A session-less read is answered on the IO thread, which is what this handler has always done. A request
   * that names a session is not: see {@link DatabaseAbstractHandler#carriesSessionId}.
   */
  @Override
  protected boolean mustExecuteOnWorkerThread(final HttpServerExchange exchange) {
    return carriesSessionId(exchange);
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final Database db, final JSONObject payload) throws Exception {

    final Deque<String> nameParam = exchange.getQueryParameters().get("name");
    if (nameParam == null || nameParam.isEmpty())
      return new ExecutionResponse(400, PromQLResponseFormatter.formatError("bad_data", "Label name parameter is required"));

    final String labelName = nameParam.getFirst();
    final DatabaseInternal database = (DatabaseInternal) db;

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
        tsType.getEngine().collectDistinctTagValues(labelName, values, null);
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
}
