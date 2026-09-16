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
 * HTTP handler for PromQL range queries.
 * Endpoint: GET /api/v1/ts/{database}/prom/api/v1/query_range
 * <p>
 * On {@link DatabaseAbstractHandler} since issue #7681, for the reasons spelled out on
 * {@link PostGrafanaQueryHandler}: a request carrying {@code arcadedb-session-id} reads through that session's
 * transaction, under its lock and on its principal and refreshing its idle timer, and the base class subsumes
 * the {@code checkAuthorizationOnDatabase} call this handler used to make by hand.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GetPromQLQueryRangeHandler extends DatabaseAbstractHandler {
  // Widest epoch second accepted for start/end. 1e12s is year 33658, ~1600x beyond any real series, and
  // keeps start/end (and therefore their difference in milliseconds) far inside the 64-bit range.
  static final double MAX_TIMESTAMP_SECONDS = 1e12;

  public GetPromQLQueryRangeHandler(final HttpServer httpServer) {
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
   * Evaluates a PromQL expression at every step of a range, so never on an Undertow IO thread (issue #7722),
   * for the reason {@code GetPromQLQueryHandler} gives at its own override - with strictly more work to do,
   * since the expression is evaluated once per step rather than once.
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

    final String query = getQueryParameter(exchange, "query");
    if (query == null || query.isBlank())
      return new ExecutionResponse(400, PromQLResponseFormatter.formatError("bad_data", "Missing required parameter: query"));

    final String startStr = getQueryParameter(exchange, "start");
    final String endStr = getQueryParameter(exchange, "end");
    final String stepStr = getQueryParameter(exchange, "step");

    if (startStr == null || endStr == null || stepStr == null)
      return new ExecutionResponse(400,
          PromQLResponseFormatter.formatError("bad_data", "Missing required parameters: start, end, step"));

    final long startMs;
    final long endMs;
    final long stepMs;
    try {
      startMs = parseTimestampMs("start", startStr);
      endMs = parseTimestampMs("end", endStr);
      stepMs = parseStep(stepStr);
    } catch (final IllegalArgumentException e) {
      return new ExecutionResponse(400, PromQLResponseFormatter.formatError("bad_data", e.getMessage()));
    }

    if (stepMs <= 0)
      return new ExecutionResponse(400, PromQLResponseFormatter.formatError("bad_data", "Step must be positive"));

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
        result = evaluator.evaluateRange(expr, startMs, endMs, stepMs);
      } finally {
        TimeSeriesReadMetrics.publish(readMetrics, database.getName(), TimeSeriesReadMetrics.TYPE_EXPRESSION,
            TimeSeriesReadMetrics.SURFACE_PROM_QUERY_RANGE);
      }
      return new ExecutionResponse(200, PromQLResponseFormatter.formatSuccess(result));
    } catch (final IllegalArgumentException e) {
      return new ExecutionResponse(400, PromQLResponseFormatter.formatError("bad_data", e.getMessage()));
    }
  }

  /**
   * Parses a Prometheus {@code start}/{@code end} parameter (epoch seconds, possibly fractional) into
   * milliseconds, rejecting anything that is not a finite value inside {@link #MAX_TIMESTAMP_SECONDS}.
   * <p>
   * Without the bound, {@code start=-9e15}/{@code end=9e15} produced a span wider than {@code Long.MAX_VALUE}
   * milliseconds, which wrapped negative in the evaluator's step-count guard and left the per-step loop
   * unbounded - one request per worker thread was enough to take the HTTP listener down (issue #6807). The
   * evaluator rejects the overflow on its own too; this is the earlier, clearer error for the caller.
   * Package-private for direct unit testing.
   */
  static long parseTimestampMs(final String name, final String value) {
    final double seconds;
    try {
      seconds = Double.parseDouble(value);
    } catch (final NumberFormatException e) {
      throw new IllegalArgumentException("Invalid " + name + " timestamp: '" + value + "'");
    }

    return secondsToMillis(name + " timestamp", value, seconds);
  }

  /**
   * Parses the {@code step} parameter, which Prometheus accepts either as a number of seconds ({@code 60})
   * or as a duration ({@code 1m}). The numeric form goes through the same finite/in-range validation as
   * {@code start} and {@code end}: {@code Double.parseDouble} accepts {@code Infinity} and {@code 1e300}
   * without throwing, and the resulting {@code (long)(v * 1000)} saturates to {@code Long.MAX_VALUE}, which
   * is positive and therefore sailed past the "step must be positive" test. The evaluator then computed one
   * single step and answered 200 with a one-point series instead of rejecting the request.
   * Package-private for direct unit testing.
   */
  static long parseStep(final String step) {
    final double seconds;
    try {
      // Try as plain seconds (e.g. "60")
      seconds = Double.parseDouble(step);
    } catch (final NumberFormatException e) {
      // Try as duration (e.g. "1m"). This branch does NOT go through secondsToMillis: parseDuration carries
      // its own overflow guard (it refuses a value once `current > Long.MAX_VALUE / unitMs`), so the two
      // forms of `step` are bounded by two different mechanisms. Keep them in step if either bound moves.
      return PromQLParser.parseDuration(step);
    }

    return secondsToMillis("step", step, seconds);
  }

  /**
   * Converts a seconds value that came off the wire into milliseconds, refusing anything not finite or
   * outside {@link #MAX_TIMESTAMP_SECONDS}. The bound is what keeps the millisecond value - and, for
   * {@code start}/{@code end}, the span between two of them - far inside the 64-bit range the evaluator
   * computes in (issue #6807).
   */
  private static long secondsToMillis(final String what, final String raw, final double seconds) {
    if (!Double.isFinite(seconds))
      throw new IllegalArgumentException("Invalid " + what + ": '" + raw + "' is not finite");

    if (Math.abs(seconds) > MAX_TIMESTAMP_SECONDS)
      throw new IllegalArgumentException("Invalid " + what + ": '" + raw
          + "' is outside the supported epoch range of +/- " + MAX_TIMESTAMP_SECONDS + " seconds");

    return (long) (seconds * 1000);
  }
}
