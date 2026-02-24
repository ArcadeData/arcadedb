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
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.LabelMatcher;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.VectorSelector;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    final Deque<String> matchParams = exchange.getQueryParameters().get("match[]");
    if (matchParams == null || matchParams.isEmpty())
      return new ExecutionResponse(400,
          PromQLResponseFormatter.formatError("bad_data", "Missing required parameter: match[]"));

    final String startStr = getQueryParameter(exchange, "start");
    final String endStr = getQueryParameter(exchange, "end");
    final long startMs = startStr != null ? (long) (Double.parseDouble(startStr) * 1000) : Long.MIN_VALUE;
    final long endMs = endStr != null ? (long) (Double.parseDouble(endStr) * 1000) : Long.MAX_VALUE;

    final DatabaseInternal database = httpServer.getServer().getDatabase(databaseParam.getFirst(), false, false);
    final Set<String> seenKeys = new LinkedHashSet<>();
    final List<Map<String, String>> seriesList = new ArrayList<>();

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

        final TimeSeriesEngine engine = tsType.getEngine();
        final List<ColumnDefinition> columns = tsType.getTsColumns();
        final List<Object[]> rows = engine.query(startMs, endMs, null, null);

        for (final Object[] row : rows) {
          final Map<String, String> labels = new LinkedHashMap<>();
          labels.put("__name__", vs.metricName());
          for (int i = 0; i < columns.size(); i++) {
            final ColumnDefinition col = columns.get(i);
            if (col.getRole() == ColumnDefinition.ColumnRole.TAG && row[i] != null)
              labels.put(col.getName(), row[i].toString());
          }

          final String key = labels.toString();
          if (seenKeys.add(key))
            seriesList.add(labels);
        }
      } catch (final IllegalArgumentException ignored) {
        // Skip malformed match patterns
      }
    }

    return new ExecutionResponse(200, PromQLResponseFormatter.formatSeriesResponse(seriesList));
  }
}
