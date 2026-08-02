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
import com.arcadedb.query.sql.executor.QueryStatistics;
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

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.regex.Pattern;

public class PostCommandHandler extends AbstractQueryHandler {

  // Precompiled once: recompiling the line-break pattern on every command carrying an explicit LIMIT is wasteful.
  private static final Pattern LINE_BREAK = Pattern.compile("\\R");

  public PostCommandHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  /**
   * Tells whether the given command needs an automatic trailing {@code LIMIT} pushed down, i.e. whether it is a
   * SELECT/MATCH SQL command that states no LIMIT of its own. A command that already carries one has expressed
   * the caller's own expectation, and that expectation - not the default cap - decides how many rows are
   * serialized, so the text is left exactly as it arrived.
   * <p>
   * The historical heuristic is preserved while avoiding a full-command {@code toLowerCase} copy per request:
   * the command is expected to be already trimmed, only case-insensitive prefix/substring probes are used, and
   * the last line is lowercased only when an explicit LIMIT may already be present.
   */
  static boolean requiresAutomaticLimit(final String command, final String language, final int limit) {
    // A non-positive cap means unlimited, exactly as it does in the serializer: pushing 'limit 0' down would
    // instead return no row at all, which is what the previous asymmetry did with a request carrying limit=0.
    if (limit <= 0)
      return false;
    if (!"sql".equalsIgnoreCase(language) && !"sqlScript".equalsIgnoreCase(language))
      return false;

    final boolean isSelect = command.regionMatches(true, 0, "select", 0, 6);
    final boolean isMatch = command.regionMatches(true, 0, "match", 0, 5);
    if ((!isSelect && !isMatch) || command.endsWith(";"))
      return false;

    if (!containsIgnoreCase(command, " limit ") && !containsIgnoreCase(command, "\nlimit "))
      return true;

    // An explicit LIMIT may already be present somewhere: only the last line decides whether to append.
    final String[] lines = LINE_BREAK.split(command);
    final String[] words = lines[lines.length - 1].toLowerCase(Locale.ENGLISH).split(" ");
    return words.length > 1 //
        && !"limit".equals(words[words.length - 2]) //
        && (words.length < 5 || !"limit".equals(words[words.length - 4]));
  }

  /**
   * LIMIT to push down into a command that carries none, one row above the cap the response will honor: the
   * extra row never reaches the client and is what lets the serializer tell a result that ends exactly at the
   * cap from one that was cut short (issue #5711). A non-positive cap means unlimited and is pushed down
   * unchanged, and a cap of {@link Integer#MAX_VALUE} saturates instead of overflowing.
   */
  static int truncationProbeLimit(final int limit) {
    return limit > 0 && limit < Integer.MAX_VALUE ? limit + 1 : limit;
  }

  /**
   * Allocation-free case-insensitive substring test, avoiding the full-command lowercase copy the previous
   * {@code String.contains} check required.
   */
  private static boolean containsIgnoreCase(final String haystack, final String needle) {
    final int needleLen = needle.length();
    if (needleLen == 0)
      return true;
    final int max = haystack.length() - needleLen;
    for (int i = 0; i <= max; i++)
      if (haystack.regionMatches(true, i, needle, 0, needleLen))
        return true;
    return false;
  }

  /**
   * Returns the value of a request field that must be a JSON string, or {@code null} when absent. A present
   * value of the wrong JSON type (number, array, object) is rejected with an {@link IllegalArgumentException},
   * which the HTTP layer maps to a clean 400 Bad Request instead of leaking a raw {@link ClassCastException}
   * as HTTP 500 (issue #5222).
   */
  private static String requireStringField(final Map<String, Object> map, final String field) {
    return requireStringField(map, field, null);
  }

  private static String requireStringField(final Map<String, Object> map, final String field, final String defaultValue) {
    final Object value = map.get(field);
    if (value == null)
      return defaultValue;
    if (value instanceof String s)
      return s;
    throw new IllegalArgumentException("Field '" + field + "' must be a string");
  }

  /**
   * Returns the value of an optional request field that must be a JSON integer, or {@code null} when absent.
   * The absence must stay distinguishable from any value the caller could send: {@code limit} is the caller's
   * explicit statement of the page size it expects, and a default substituted for a missing field would be
   * indistinguishable from that statement.
   */
  private static Integer optionalIntField(final Map<String, Object> map, final String field) {
    final Object value = map.get(field);
    return value == null ? null : requireIntLimit(value, field);
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

    final String language = requireStringField(requestMap, "language");
    // Do NOT HTML-decode the command: the command is already transported losslessly as a JSON string,
    // and a command can legitimately carry HTML entities (e.g. &quot;, &amp;) inside its data. Decoding
    // them here corrupts the payload (e.g. breaks the embedded JSON of an INSERT ... CONTENT { ... }).
    String command = requireStringField(requestMap, "command");
    final Integer requestLimit = optionalIntField(requestMap, "limit");
    final String serializer = requireStringField(requestMap, "serializer", "record");
    final String profileExecution = requireStringField(requestMap, "profileExecution", null);

    if (command == null || command.isEmpty())
      return new ExecutionResponse(400, "{ \"error\" : \"Command text is null\"}");

    if (language == null || language.isEmpty())
      return new ExecutionResponse(400, "{ \"error\" : \"Language is null\"}");

    command = command.trim();

    final Object rawParams = requestMap.get("params");
    Map<String, Object> paramMap;
    if (rawParams instanceof Map<?, ?> m) {
      @SuppressWarnings("unchecked")
      final Map<String, Object> typed = (Map<String, Object>) m;
      paramMap = typed;
    } else if (rawParams instanceof List<?> list) {
      // Positional params forwarded as a JSON array [v0, v1, ...] — convert to ordinal map
      paramMap = new HashMap<>((int) (list.size() / 0.75f) + 1);
      for (int i = 0; i < list.size(); i++)
        paramMap.put("" + i, list.get(i));
    } else if (rawParams != null && rawParams.getClass().isArray()) {
      // Positional params converted to a primitive array by toMap(true) — convert to ordinal map
      final int len = java.lang.reflect.Array.getLength(rawParams);
      paramMap = new HashMap<>((int) (len / 0.75f) + 1);
      for (int i = 0; i < len; i++)
        paramMap.put("" + i, java.lang.reflect.Array.get(rawParams, i));
    } else {
      paramMap = new HashMap<>();
    }

    // Decode typed JSON markers ($bytes, $int8) before any transaction starts so a malformed
    // marker surfaces as a clean HTTP 400 via the IllegalArgumentException catch arm in
    // AbstractServerHttpHandler, instead of being wrapped by the surrounding TransactionException
    // and downgraded to HTTP 500.
    paramMap = AbstractQueryHandler.decodeTypedJsonMarkers(paramMap);

    // The cap used to push a LIMIT down into a command that states none: the caller's own 'limit' when
    // present, the configured default otherwise. A command that already carries a LIMIT is left untouched and
    // its own value decides the response size, resolved from the execution plan after execution.
    final int autoLimit = requestLimit != null ? requestLimit : getDefaultRowLimit();
    final boolean autoLimited = requiresAutomaticLimit(command, language, autoLimit);
    // Kept for the log: a warning must show the operator the query the caller sent, not the rewritten one.
    final String originalCommand = command;
    if (autoLimited)
      // One row above the cap: the extra row is never serialized, it only makes truncation detectable.
      command = command + " limit " + truncationProbeLimit(autoLimit);

    if ("sqlScript".equalsIgnoreCase(language) && !command.endsWith(";"))
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

      try {
        final JSONObject response = new JSONObject();
        response.put("user", user != null ? user.getName() : null);

        // How many rows the response may carry, in decreasing order of explicitness: the request 'limit', the
        // LIMIT the command carries (read back from the execution plan, and ignored when it is the one this
        // handler pushed down itself), the configured default. Only the last one can drop rows the caller
        // never asked to drop, and that is exactly what 'truncated' reports below (issue #5711).
        final int planLimit = autoLimited ? 0 : getPlanLimit(qResult);
        final int limit = resolveLimit(requestLimit, planLimit);
        final SerializationOutcome outcome;

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
          outcome = serializeResultSet(database, serializer, limit, response, qResult);
          response.put("explain", explainText);
          response.put("explainPlan", executionPlan.toResult().toJSON());
          profile.addSerializationNanos(System.nanoTime() - serializationStart);
        } else {
          if (detailedProfile && qResult != null) {
            // Materialize the ResultSet inside the engine timer so the serialization
            // timer captures only the wire-format conversion and not query work.
            // materializeResultSet closes the source and returns a fresh in-memory ResultSet.
            qResult = materializeResultSet(qResult);
          }
          profile.addEngineNanos(System.nanoTime() - engineStart);

          final long serializationStart = System.nanoTime();
          outcome = serializeResultSet(database, serializer, limit, response, qResult);

          if (qResult != null) {
            final var qStats = qResult.getStatistics();
            if (qStats.isPresent() && qStats.get().containsUpdates())
              response.put("stats", qStats.get().toJSON());
          }

          if (qResult != null && qResult.getExecutionPlan().isPresent() &&
              (profileExecution != null ||
                  command.regionMatches(true, 0, "PROFILE ", 0, 8))) {
            final var executionPlan = qResult.getExecutionPlan().get();
            response.put("explain", executionPlan.prettyPrint(0, 2));
            response.put("explainPlan", executionPlan.toResult().toJSON());
          }
          profile.addSerializationNanos(System.nanoTime() - serializationStart);
        }

        reportLimits(response, limit, outcome);
        logIfTruncatedByDefault(database.getName(), originalCommand, limit, requestLimit, planLimit, outcome);

        if (detailedProfile)
          response.put("profile", profile.toJSON());

        Metrics.counter("http.command").increment();
        recordProfilerMetrics("http.command", profile);

        recordServerProfile(database.getName(), language, command, profile, qResult);

        return new ExecutionResponse(200, response.toString());
      } finally {
        if (qResult != null)
          qResult.close();
      }
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
    Metrics.timer(prefix + ".deserialization").record(profile.getDeserializationNanos(), TimeUnit.NANOSECONDS);
    Metrics.timer(prefix + ".engine").record(profile.getEngineNanos(), TimeUnit.NANOSECONDS);
    Metrics.timer(prefix + ".serialization").record(profile.getSerializationNanos(), TimeUnit.NANOSECONDS);
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
      final Optional<QueryStatistics> stats = source.getStatistics();
      final IteratorResultSet materialized = new IteratorResultSet(rows.iterator()) {
        @Override
        public Optional<ExecutionPlan> getExecutionPlan() {
          return plan;
        }
      };
      stats.ifPresent(materialized::setStatistics);
      return materialized;
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
