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
import com.arcadedb.database.async.AsyncResultsetCallback;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.ExecutionPlan;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.ExplainResultSet;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.monitor.QueryProfile;
import com.arcadedb.server.monitor.ServerQueryProfiler;
import com.arcadedb.server.security.ServerSecurityUser;
import io.micrometer.core.instrument.Metrics;
import io.undertow.server.HttpServerExchange;

import java.io.*;
import java.util.*;
import java.util.logging.*;

public class PostCommandHandler extends AbstractQueryHandler {

  public PostCommandHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final Database database,
      final JSONObject json)
      throws IOException {
    if (json == null)
      return new ExecutionResponse(400, "{ \"error\" : \"Command text is null\"}");

    final QueryProfile profile = new QueryProfile();
    QueryProfile.pushCurrent(profile);
    try {

    // Issue #3864 follow-up: use the optimized toMap so JSON numeric arrays (e.g. vector
    // embeddings inside `params.batch[*].vector`) are returned as primitive double[]/long[]
    // instead of List<Double>, avoiding millions of boxed Number allocations per request.
    final long deserializationStart = System.nanoTime();
    final Map<String, Object> requestMap = json.toMap(true);
    profile.addDeserializationNanos(System.nanoTime() - deserializationStart);

    // No HTTP-level leader forwarding here: reads run locally on replicas, and writes are
    // forwarded at the engine level by RaftReplicatedDatabase.command() when
    // QueryEngine.isExecutedByTheLeader() or analyze(query).isDDL() is true.

    if (requestMap.get("command") == null)
      throw new IllegalArgumentException("command missing");

    final String language = (String) requestMap.get("language");
    String command = decode((String) requestMap.get("command"));
    final int limit = (int) requestMap.getOrDefault("limit", DEFAULT_LIMIT);
    final String serializer = (String) requestMap.getOrDefault("serializer", "record");
    final String profileExecution = (String) requestMap.getOrDefault("profileExecution", null);

    if (command == null || command.isEmpty())
      return new ExecutionResponse(400, "{ \"error\" : \"Command text is null\"}");

    if (language == null || language.isEmpty())
      return new ExecutionResponse(400, "{ \"error\" : \"Language is null\"}");

    command = command.trim();

    Map<String, Object> paramMap = (Map<String, Object>) requestMap.get("params");
    if (paramMap == null)
      paramMap = new HashMap<>();

    if (limit != -1) {
      if (language.equalsIgnoreCase("sql") || language.equalsIgnoreCase("sqlScript")) {
        final String commandLC = command.toLowerCase(Locale.ENGLISH).trim();
        if ((commandLC.startsWith("select") || commandLC.startsWith("match")) && !commandLC.endsWith(";")) {
          if (!commandLC.contains(" limit ") && !commandLC.contains("\nlimit ")) {
            command += " limit " + limit;
          } else {
            final String[] lines = commandLC.split("\\R");
            final String[] words = lines[lines.length - 1].split(" ");
            if (words.length > 1) {
              if (!"limit".equals(words[words.length - 2]) && //
                  (words.length < 5 || !"limit".equals(words[words.length - 4])))
                command += " limit " + limit;
            }
          }
        }
      }
    }

    if (language.equalsIgnoreCase("sqlScript") && !command.endsWith(";"))
      command += ";";

    if ("detailed".equalsIgnoreCase(profileExecution))
      paramMap.put("$profileExecution", true);

    boolean awaitResponse = true;
    if (requestMap.containsKey("awaitResponse") && requestMap.get("awaitResponse") instanceof Boolean) {
      awaitResponse = (Boolean) requestMap.get("awaitResponse");
    }

    if (!awaitResponse) {
      executeCommandAsync(database, language, command, paramMap);

      return new ExecutionResponse(202, "{ \"result\": \"Command accepted for asynchronous execution\"}");
    } else {

      final boolean detailedProfile = "detailed".equalsIgnoreCase(profileExecution);

      final long engineStart = System.nanoTime();
      ResultSet qResult = executeCommand(database, language, command, paramMap);

      final JSONObject response = new JSONObject();
      response.put("user", user != null ? user.getName() : null);

      if (qResult instanceof ExplainResultSet) {
        // EXPLAIN (or SQL PROFILE): extract plan, then drain the single record
        // so serializeResultSet produces an empty result structure
        final var executionPlan = qResult.getExecutionPlan().get();
        final String explainText = executionPlan.prettyPrint(0, 2);
        while (qResult.hasNext()) {
          qResult.next();
        }
        profile.addEngineNanos(System.nanoTime() - engineStart);

        final long serializationStart = System.nanoTime();
        serializeResultSet(database, serializer, limit, response, qResult);
        response.put("explain", explainText);
        response.put("explainPlan", executionPlan.toResult().toJSON());
        profile.addSerializationNanos(System.nanoTime() - serializationStart);
      } else {
        if (detailedProfile && qResult != null) {
          // Materialize the ResultSet inside the engine timer so the serialization
          // timer captures only the wire-format conversion and not query work.
          qResult = materializeResultSet(qResult);
        }
        profile.addEngineNanos(System.nanoTime() - engineStart);

        final long serializationStart = System.nanoTime();
        serializeResultSet(database, serializer, limit, response, qResult);

        if (qResult != null && qResult.getExecutionPlan().isPresent() &&
            (profileExecution != null ||
                command.toUpperCase(Locale.ENGLISH).startsWith("PROFILE "))) {
          final var executionPlan = qResult.getExecutionPlan().get();
          response.put("explain", executionPlan.prettyPrint(0, 2));
          response.put("explainPlan", executionPlan.toResult().toJSON());
        }
        profile.addSerializationNanos(System.nanoTime() - serializationStart);
      }

      if (detailedProfile)
        response.put("profile", profile.toJSON());

      Metrics.counter("http.command").increment();
      recordProfilerMetrics("http.command", profile);

      recordServerProfile(database.getName(), language, command, profile, qResult);

      return new ExecutionResponse(200, response.toString());
    }

    } finally {
      QueryProfile.popCurrent();
    }
  }

  protected void recordServerProfile(final String databaseName, final String language, final String command,
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
    serverProfiler.recordQuery(databaseName, language, command, profile, planJson);
  }

  protected static void recordProfilerMetrics(final String prefix, final QueryProfile profile) {
    Metrics.timer(prefix + ".deserialization").record(profile.getDeserializationNanos(), java.util.concurrent.TimeUnit.NANOSECONDS);
    Metrics.timer(prefix + ".engine").record(profile.getEngineNanos(), java.util.concurrent.TimeUnit.NANOSECONDS);
    Metrics.timer(prefix + ".serialization").record(profile.getSerializationNanos(), java.util.concurrent.TimeUnit.NANOSECONDS);
  }

  /**
   * Drains the given {@link ResultSet} into an in-memory list preserving the
   * original execution plan, and returns a new {@link ResultSet} backed by the
   * list. The source {@link ResultSet} is closed. Used by the profiler to
   * separate engine execution time from serialization time.
   */
  private static ResultSet materializeResultSet(final ResultSet source) {
    try {
      final List<Result> rows = new ArrayList<>();
      while (source.hasNext())
        rows.add(source.next());

      final Optional<ExecutionPlan> plan = source.getExecutionPlan();
      return new IteratorResultSet(rows.iterator()) {
        @Override
        public Optional<ExecutionPlan> getExecutionPlan() {
          return plan;
        }
      };
    } finally {
      source.close();
    }
  }

  protected ResultSet executeCommand(final Database database, final String language, final String command,
      final Map<String, Object> paramMap) {
    final Object params = mapParams(paramMap);

    if (params instanceof Object[] objects)
      return database.command(language, command, httpServer.getServer().getConfiguration(), objects);
    return database.command(language, command, httpServer.getServer().getConfiguration(), (Map<String, Object>) params);
  }

  protected void executeCommandAsync(final Database database, final String language, final String command,
      final Map<String, Object> paramMap) {
    final Object params = mapParams(paramMap);

    database.async().command(language, command, new AsyncResultsetCallback() {
      @Override
      public void onComplete(final ResultSet rs) {
        LogManager.instance().log(this, Level.INFO, "Async command in database \"%s\" completed.", null, database.getName());
      }

      @Override
      public void onError(final Exception exception) {
        LogManager.instance().log(this, Level.SEVERE, "Async command in database \"%s\" failed.", null, database.getName());
        LogManager.instance().log(this, Level.SEVERE, "", exception);
      }
    }, params instanceof Object[] os ? os : (Map<String, Object>) params);
  }
}
