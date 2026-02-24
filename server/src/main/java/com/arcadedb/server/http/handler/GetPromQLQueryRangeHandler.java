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
import com.arcadedb.engine.timeseries.promql.PromQLEvaluator;
import com.arcadedb.engine.timeseries.promql.PromQLParser;
import com.arcadedb.engine.timeseries.promql.PromQLResult;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

import java.util.Deque;

/**
 * HTTP handler for PromQL range queries.
 * Endpoint: GET /api/v1/ts/{database}/prom/api/v1/query_range
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GetPromQLQueryRangeHandler extends AbstractServerHttpHandler {

  public GetPromQLQueryRangeHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) throws Exception {

    final Deque<String> databaseParam = exchange.getQueryParameters().get("database");
    if (databaseParam == null || databaseParam.isEmpty())
      return new ExecutionResponse(400, PromQLResponseFormatter.formatError("bad_data", "Database parameter is required"));

    final String query = getQueryParameter(exchange, "query");
    if (query == null || query.isBlank())
      return new ExecutionResponse(400, PromQLResponseFormatter.formatError("bad_data", "Missing required parameter: query"));

    final String startStr = getQueryParameter(exchange, "start");
    final String endStr = getQueryParameter(exchange, "end");
    final String stepStr = getQueryParameter(exchange, "step");

    if (startStr == null || endStr == null || stepStr == null)
      return new ExecutionResponse(400,
          PromQLResponseFormatter.formatError("bad_data", "Missing required parameters: start, end, step"));

    final long startMs = (long) (Double.parseDouble(startStr) * 1000);
    final long endMs = (long) (Double.parseDouble(endStr) * 1000);
    final long stepMs = parseStep(stepStr);

    if (stepMs <= 0)
      return new ExecutionResponse(400, PromQLResponseFormatter.formatError("bad_data", "Step must be positive"));

    final DatabaseInternal database = httpServer.getServer().getDatabase(databaseParam.getFirst(), false, false);

    try {
      final PromQLExpr expr = new PromQLParser(query).parse();
      final String lookbackStr = getQueryParameter(exchange, "lookback_delta");
      final PromQLEvaluator evaluator = lookbackStr != null && !lookbackStr.isBlank()
          ? new PromQLEvaluator(database, PromQLParser.parseDuration(lookbackStr))
          : new PromQLEvaluator(database);
      final PromQLResult result = evaluator.evaluateRange(expr, startMs, endMs, stepMs);
      return new ExecutionResponse(200, PromQLResponseFormatter.formatSuccess(result));
    } catch (final IllegalArgumentException e) {
      return new ExecutionResponse(400, PromQLResponseFormatter.formatError("bad_data", e.getMessage()));
    }
  }

  private long parseStep(final String step) {
    try {
      // Try as plain seconds (e.g. "60")
      return (long) (Double.parseDouble(step) * 1000);
    } catch (final NumberFormatException e) {
      // Try as duration (e.g. "1m")
      return PromQLParser.parseDuration(step);
    }
  }
}
