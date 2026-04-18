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
import com.arcadedb.query.sql.executor.ExecutionPlan;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.monitor.QueryProfile;
import com.arcadedb.server.monitor.ServerQueryProfiler;
import com.arcadedb.server.security.ServerSecurityUser;
import io.micrometer.core.instrument.Metrics;
import io.undertow.server.HttpServerExchange;

import java.io.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class GetQueryHandler extends AbstractQueryHandler {
  public GetQueryHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final Database database,
      final JSONObject payload)
      throws UnsupportedEncodingException {
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

      final String limitPar = getQueryParameter(exchange, "limit");
      profile.addDeserializationNanos(System.nanoTime() - deserializationStart);

      final JSONObject response = new JSONObject();

      ResultSet qResult = null;
      try {
        final long engineStart = System.nanoTime();
        qResult = database.query(language, text);
        final ExecutionPlan plan = qResult.getExecutionPlan().orElse(null);
        int limit = plan != null ? plan.getLimit() : 0;
        if (limit == 0) {
          if (limitPar == null)
            limit = DEFAULT_LIMIT;
          else
            limit = Integer.parseInt(limitPar);
        }
        profile.addEngineNanos(System.nanoTime() - engineStart);

        final long serializationStart = System.nanoTime();
        serializeResultSet(database, serializer, limit, response, qResult);
        profile.addSerializationNanos(System.nanoTime() - serializationStart);

      } finally {
        Metrics.counter("http.query").increment();
        Metrics.timer("http.query.deserialization").record(profile.getDeserializationNanos(), TimeUnit.NANOSECONDS);
        Metrics.timer("http.query.engine").record(profile.getEngineNanos(), TimeUnit.NANOSECONDS);
        Metrics.timer("http.query.serialization").record(profile.getSerializationNanos(), TimeUnit.NANOSECONDS);
        recordServerProfile(database.getName(), language, text, profile, qResult);
      }

      return new ExecutionResponse(200, response.toString());
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
}
