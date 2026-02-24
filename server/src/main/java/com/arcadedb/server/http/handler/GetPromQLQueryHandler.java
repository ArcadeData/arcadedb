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
 * HTTP handler for PromQL instant queries.
 * Endpoint: GET /api/v1/ts/{database}/prom/api/v1/query
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GetPromQLQueryHandler extends AbstractServerHttpHandler {

  public GetPromQLQueryHandler(final HttpServer httpServer) {
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

    final String timeStr = getQueryParameter(exchange, "time");
    final long evalTimeMs;
    if (timeStr != null && !timeStr.isBlank())
      evalTimeMs = (long) (Double.parseDouble(timeStr) * 1000);
    else
      evalTimeMs = System.currentTimeMillis();

    final DatabaseInternal database = httpServer.getServer().getDatabase(databaseParam.getFirst(), false, false);

    try {
      final PromQLExpr expr = new PromQLParser(query).parse();
      final String lookbackStr = getQueryParameter(exchange, "lookback_delta");
      final PromQLEvaluator evaluator = lookbackStr != null && !lookbackStr.isBlank()
          ? new PromQLEvaluator(database, PromQLParser.parseDuration(lookbackStr))
          : new PromQLEvaluator(database);
      final PromQLResult result = evaluator.evaluateInstant(expr, evalTimeMs);
      return new ExecutionResponse(200, PromQLResponseFormatter.formatSuccess(result));
    } catch (final IllegalArgumentException e) {
      return new ExecutionResponse(400, PromQLResponseFormatter.formatError("bad_data", e.getMessage()));
    }
  }
}
