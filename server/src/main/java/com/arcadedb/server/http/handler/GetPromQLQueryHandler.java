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
import com.arcadedb.engine.timeseries.promql.PromQLEvaluator;
import com.arcadedb.engine.timeseries.promql.PromQLParser;
import com.arcadedb.engine.timeseries.promql.PromQLResult;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.monitor.TimeSeriesReadMetrics;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

/**
 * HTTP handler for PromQL instant queries.
 * Endpoint: GET /api/v1/ts/{database}/prom/api/v1/query
 * <p>
 * On {@link DatabaseAbstractHandler} since issue #7681, for the reasons spelled out on
 * {@link PostGrafanaQueryHandler}: a request carrying {@code arcadedb-session-id} reads through that session's
 * transaction, under its lock and on its principal and refreshing its idle timer, and the base class subsumes
 * the {@code checkAuthorizationOnDatabase} call this handler used to make by hand.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GetPromQLQueryHandler extends DatabaseAbstractHandler {

  public GetPromQLQueryHandler(final HttpServer httpServer) {
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
   * Evaluates a PromQL expression over the samples it selects, so never on an Undertow IO thread (issue #7722).
   * <p>
   * A weaker case than the two discovery endpoints - the bound here is the expression's own range rather than
   * the whole series - but the same decision: the read is a scan whose size the server does not know before
   * doing it, and blocking an IO thread on one starves the unrelated connections sharing it. The dispatch costs
   * a hand-off on a path whose answer takes a scan anyway.
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

    final String query = getQueryParameter(exchange, "query");
    if (query == null || query.isBlank())
      return new ExecutionResponse(400, PromQLResponseFormatter.formatError("bad_data", "Missing required parameter: query"));

    // Same validation as the range endpoint's start/end (issue #6807): the raw parseDouble accepted a
    // non-numeric value only to fail with a 500 outside the catch below, and accepted Infinity/1e300 as an
    // evaluation instant. There is no unbounded loop on this path, but the two endpoints must not disagree
    // on what a timestamp is.
    final String timeStr = getQueryParameter(exchange, "time");
    final long evalTimeMs;
    try {
      evalTimeMs = timeStr != null && !timeStr.isBlank()
          ? GetPromQLQueryRangeHandler.parseTimestampMs("time", timeStr)
          : System.currentTimeMillis();
    } catch (final IllegalArgumentException e) {
      return new ExecutionResponse(400, PromQLResponseFormatter.formatError("bad_data", e.getMessage()));
    }

    final DatabaseInternal database = (DatabaseInternal) db;

    try {
      final PromQLExpr expr = new PromQLParser(query).parse();
      final String lookbackStr = getQueryParameter(exchange, "lookback_delta");
      final PromQLEvaluator evaluator = lookbackStr != null && !lookbackStr.isBlank()
          ? new PromQLEvaluator(database, PromQLParser.parseDuration(lookbackStr))
          : new PromQLEvaluator(database);

      // What the selector scans actually did, published to whatever the server's metrics subsystem feeds
      // (issue #7717). null - and therefore free - whenever metrics are off. The type tag is a constant: one
      // expression can select several metrics, so there is no single type to name here.
      final AggregationMetrics readMetrics = TimeSeriesReadMetrics.start();
      evaluator.setReadMetrics(readMetrics);
      final PromQLResult result;
      try {
        result = evaluator.evaluateInstant(expr, evalTimeMs);
      } finally {
        TimeSeriesReadMetrics.publish(readMetrics, database.getName(), TimeSeriesReadMetrics.TYPE_EXPRESSION,
            TimeSeriesReadMetrics.SURFACE_PROM_QUERY);
      }
      return new ExecutionResponse(200, PromQLResponseFormatter.formatSuccess(result));
    } catch (final IllegalArgumentException e) {
      return new ExecutionResponse(400, PromQLResponseFormatter.formatError("bad_data", e.getMessage()));
    }
  }
}
