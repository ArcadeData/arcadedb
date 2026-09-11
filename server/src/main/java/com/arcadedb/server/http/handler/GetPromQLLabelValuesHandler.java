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
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GetPromQLLabelValuesHandler extends AbstractServerHttpHandler {

  public GetPromQLLabelValuesHandler(final HttpServer httpServer) {
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
        // The index into the schema doubles as the index into the row below, which holds only because the
        // TIMESTAMP column is declared first - what every type-creation path happens to do and what nothing in
        // TimeSeriesTypeBuilder enforces. Carried over from the query()-based code this replaces; see the same
        // note in GetPromQLSeriesHandler.
        final int colIdx = findColumnIndex(labelName, columns);
        if (colIdx < 0)
          continue;

        // forEachRow, not query(): the answer is the tag's cardinality - a handful of hosts or regions - while
        // query() merges every shard's full range into one ArrayList and sorts it by timestamp, a sort this loop
        // does not use at all. A Grafana datasource calls this to populate a label picker, on every dashboard load
        // and every variable refresh, so a type holding millions of samples used to allocate the whole series per
        // call. The rows still have to be READ, but they no longer have to be resident (issue #7354).
        // The visitor runs under the shard's read locks (see TimeSeriesRowVisitor): it folds, it does not compute
        // and it never calls back into the engine.
        final TimeSeriesEngine engine = tsType.getEngine();
        engine.forEachRow(Long.MIN_VALUE, Long.MAX_VALUE, null, null, null, row -> {
          if (colIdx < row.length && row[colIdx] != null)
            values.add(row[colIdx].toString());
          return true;
        });
      }
    }

    final List<String> sorted = new ArrayList<>(values);
    Collections.sort(sorted);
    return new ExecutionResponse(200, PromQLResponseFormatter.formatLabelsResponse(sorted));
  }

  private int findColumnIndex(final String name, final List<ColumnDefinition> columns) {
    for (int i = 0; i < columns.size(); i++)
      if (columns.get(i).getRole() == ColumnDefinition.ColumnRole.TAG && columns.get(i).getName().equals(name))
        return i;
    return -1;
  }
}
