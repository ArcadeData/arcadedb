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
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.monitor.QueryProfile;
import com.arcadedb.server.monitor.ServerQueryProfiler;
import com.arcadedb.server.security.ServerSecurityUser;
import io.micrometer.core.instrument.Metrics;
import io.undertow.server.HttpServerExchange;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class GetQueryHandler extends AbstractQueryHandler {
  public GetQueryHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final Database database,
      final JSONObject payload)
      throws IOException {
    final QueryProfile profile = new QueryProfile();
    QueryProfile.pushCurrent(profile);
    try {
      // "Deserialization" for the GET endpoint is the URL query-parameter parsing (decode + lookup).
      final long deserializationStart = System.nanoTime();
      final String text = getQueryParameter(exchange, "command");
      if (text == null)
        return new ExecutionResponse(400, "{ \"error\" : \"Command text is null\"}");

      final String language = getQueryParameter(exchange, "language");
      if (language == null)
        return new ExecutionResponse(400, "{ \"error\" : \"Language is null\"}");

      String serializer = getQueryParameter(exchange, "serializer");
      if (serializer == null)
        serializer = "record";

      // Issue #5812: off unless the caller explicitly asks for the @props type hint on non-element rows.
      final boolean includeTypeHints = Boolean.parseBoolean(getQueryParameter(exchange, "typeHints"));

      final String limitPar = getQueryParameter(exchange, "limit");
      profile.addDeserializationNanos(System.nanoTime() - deserializationStart);

      // Negotiated up front so the serializer check refuses an unstreamable serializer before any query runs.
      final boolean streaming = isNdJsonRequested(exchange);
      if (streaming)
        ndJsonRowSerializer(serializer, includeTypeHints);
      boolean streamed = false;

      final JSONObject response = new JSONObject();

      ResultSet qResult = null;
      try {
        final long engineStart = System.nanoTime();
        qResult = database.query(language, text);

        // Same precedence as the POST endpoint: the caller's own 'limit' parameter, then the LIMIT the query
        // carries, then the configured default - the only case that can drop rows the caller never asked to
        // drop, and it is reported back with 'truncated' (issue #5711).
        final Integer requestLimit = parseLimitParameter(limitPar, "limit");
        final int planLimit = getPlanLimit(qResult);
        final int limit = resolveLimit(requestLimit, planLimit);
        profile.addEngineNanos(System.nanoTime() - engineStart);

        final long serializationStart = System.nanoTime();
        if (streaming) {
          // The response is written here, row by row, and the method returns null below so the request pipeline
          // does not send a second one (issue #7306).
          final SerializationOutcome outcome = streamResultSetAsNdJson(exchange, database, serializer, limit,
              getMaxResultRows(), qResult, includeTypeHints);
          logIfTruncatedByDefault(database.getName(), text, limit, requestLimit, planLimit, outcome);
          profile.addSerializationNanos(System.nanoTime() - serializationStart);
          streamed = true;
        } else {
          // ... and above all of them the hard ceiling, which no caller can widen: a response that would exceed
          // it is refused with 413 rather than truncated (issue #5719).
          final SerializationOutcome outcome = serializeResultSetBounded(database, serializer, limit, getMaxResultRows(),
              response, qResult, includeTypeHints);
          reportLimits(response, limit, outcome);
          logIfTruncatedByDefault(database.getName(), text, limit, requestLimit, planLimit, outcome);
          profile.addSerializationNanos(System.nanoTime() - serializationStart);
        }

      } finally {
        try {
          Metrics.counter("http.query").increment();
          Metrics.timer("http.query.deserialization").record(profile.getDeserializationNanos(), TimeUnit.NANOSECONDS);
          Metrics.timer("http.query.engine").record(profile.getEngineNanos(), TimeUnit.NANOSECONDS);
          Metrics.timer("http.query.serialization").record(profile.getSerializationNanos(), TimeUnit.NANOSECONDS);
          recordServerProfile(database.getName(), language, text, profile, qResult);
        } finally {
          // Nested finally so that an unchecked exception from profile recording does not
          // skip the close and leak the ResultSet (caught in #4197 audit follow-up review).
          if (qResult != null)
            qResult.close();
        }
      }

      return streamed ? null : new ExecutionResponse(200, response.toString());
    } finally {
      QueryProfile.popCurrent();
    }
  }

  private void recordServerProfile(final String databaseName, final String language, final String queryText,
      final QueryProfile profile, final ResultSet qResult) {
    final ServerQueryProfiler serverProfiler = httpServer.getServer().getQueryProfiler();
    if (serverProfiler == null || !serverProfiler.isRecording())
      return;

    JSONObject planJson = null;
    try {
      if (qResult != null) {
        final var plan = qResult.getExecutionPlan();
        if (plan.isPresent())
          planJson = plan.get().toResult().toJSON();
      }
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.FINE, "Could not extract execution plan for profiling", e);
    }
    serverProfiler.recordQuery(databaseName, language, queryText, profile, planJson);
  }

  @Override
  protected boolean requiresTransaction() {
    return false;
  }

  /**
   * A buffered GET query is short enough to answer on the IO thread, which is what this handler has always done.
   * A streamed one is not: it writes blocking output for as long as the client takes to read it, and blocking an
   * IO thread starves every other connection the server is serving on it.
   */
  @Override
  protected boolean mustExecuteOnWorkerThread(final HttpServerExchange exchange) {
    return isNdJsonRequested(exchange);
  }
}
