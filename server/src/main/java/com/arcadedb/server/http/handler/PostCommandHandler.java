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

import com.arcadedb.utility.StringUtils;
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

    if (!StringUtils.containsIgnoreCase(command, " limit ") && !StringUtils.containsIgnoreCase(command, "\nlimit "))
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
    // Issue #5812: off unless the caller explicitly asks for the @props type hint on non-element rows.
    final boolean includeTypeHints = requestMap.get("typeHints") instanceof Boolean b && b;

    // Issue #7306: the caller may ask for the result to be streamed instead of buffered. Refused rather than
    // silently downgraded when the requested serializer cannot produce one object per row, because answering a
    // negotiated streaming request with a buffered body is exactly the surprise negotiation exists to avoid.
    final boolean stream = wantsNdjson(exchange);
    if (stream && !supportsStreaming(serializer))
      return new ExecutionResponse(400, error2json("Serializer '" + serializer + "' cannot be streamed",
          "The 'graph' and 'studio' serializers build response-level vertex and edge arrays de-duplicated across "
              + "the whole result, so no row can be emitted before the last one is read. Use the 'record' "
              + "serializer, or drop the " + NDJSON_CONTENT_TYPE + " Accept header.", null, null, null));

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
    //
    // Bounded by the hard ceiling before it is pushed down, not only when the rows are serialized: an
    // unlimited 'limit' used to push nothing down at all, so the whole result reached the handler - and with
    // 'profileExecution: detailed' it was materialized in full by materializeResultSet before any cap could
    // look at it. With the ceiling in the pushed-down LIMIT the engine stops one row past it (issue #5719).
    // Only the caller's own value needs clamping here: getDefaultRowLimit() is already bounded by the ceiling.
    final int maxResultRows = getMaxResultRows();
    final int autoLimit = requestLimit != null ? applyMaxResultRows(requestLimit, maxResultRows) : getDefaultRowLimit();
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

    // A streamed response cannot be un-sent, and this endpoint runs inside the auto-commit wrapper of
    // DatabaseAbstractHandler: the transaction commits AFTER execute() returns, and a conflict on that commit
    // re-runs execute() up to 'retries' times. For a write command that combination is unsound in two ways at
    // once - the client would receive 200 and the rows before the commit that could still fail, and a retry
    // would write a second copy of the whole stream into a body that has already been sent. So streaming is
    // offered only for a read-only command, which has no commit that can fail and nothing to retry.
    //
    // Idempotency is decided by the engine's own parser, the same source SQL uses to decide whether a statement
    // may run through query() at all - not by a keyword guess here, which would be a second, divergent answer to
    // the question of what counts as a write.
    if (stream) {
      final ExecutionResponse refusal = refuseStreamingIfNotReadOnly(database, language, command);
      if (refusal != null)
        return refusal;
    }

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

        if (stream) {
          // A negotiated streaming request is answered as a stream whatever the command was, so the caller never
          // has to parse a body in a media type it did not ask for. EXPLAIN carries no rows - its whole payload is
          // the plan - so it streams as an empty row sequence whose summary carries the plan; the same place the
          // other response-level fields go, since only rows and the summary have a line of their own.
          final JSONObject summary = new JSONObject().put("user", user != null ? user.getName() : null);
          if (qResult instanceof ExplainResultSet) {
            final var explainPlan = qResult.getExecutionPlan().get();
            summary.put("explain", explainPlan.prettyPrint(0, 2));
            summary.put("explainPlan", explainPlan.toResult().toJSON());
            while (qResult.hasNext())
              qResult.next();
          }
          final SerializationOutcome streamed = streamResultSetAsNdjson(database, serializer, limit, maxResultRows,
              exchange, qResult, includeTypeHints, summary);
          logIfTruncatedByDefault(database.getName(), originalCommand, limit, requestLimit, planLimit, streamed);

          Metrics.counter("http.command").increment();
          recordProfilerMetrics("http.command", profile);
          recordServerProfile(database.getName(), language, command, profile, qResult);

          // null means "the response has already been written", which is how a streamed response reports itself
          // to AbstractServerHttpHandler.
          return null;
        }

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
          outcome = serializeResultSetBounded(database, serializer, limit, maxResultRows, response, qResult,
              includeTypeHints);
          response.put("explain", explainText);
          response.put("explainPlan", executionPlan.toResult().toJSON());
          profile.addSerializationNanos(System.nanoTime() - serializationStart);
        } else {
          if (detailedProfile && qResult != null) {
            // Materialize the ResultSet inside the engine timer so the serialization
            // timer captures only the wire-format conversion and not query work.
            // materializeResultSet closes the source and returns a fresh in-memory ResultSet.
            //
            // Bounded by the same cap the response will honor: this is the one place that drains the whole
            // result set into memory before any cap is applied, so a command carrying its own huge LIMIT -
            // which is left as written and therefore gets no pushed-down bound - could materialize an
            // arbitrary number of rows here (issue #5719). One row above the cap, as the pushdown does, so
            // the truncation stays detectable.
            qResult = materializeResultSet(qResult, truncationProbeLimit(applyMaxResultRows(limit, maxResultRows)));
          }
          profile.addEngineNanos(System.nanoTime() - engineStart);

          final long serializationStart = System.nanoTime();
          outcome = serializeResultSetBounded(database, serializer, limit, maxResultRows, response, qResult,
              includeTypeHints);

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

  /**
   * Returns a 400 when {@code command} is not read-only, or {@code null} when it is safe to stream.
   * <p>
   * A language whose engine cannot analyze the statement is refused too. That is the conservative direction: the
   * cost is that the caller falls back to the buffered response, which returns the same rows, whereas admitting
   * an unanalyzable statement risks streaming a write - and the failure mode there is a client holding a 200 for
   * a transaction that never committed.
   */
  private ExecutionResponse refuseStreamingIfNotReadOnly(final Database database, final String language,
      final String command) {
    boolean idempotent;
    try {
      idempotent = database.getQueryEngine(language).analyze(command).isIdempotent();
    } catch (final RuntimeException e) {
      LogManager.instance().log(this, Level.FINE,
          "Could not analyze a command for streaming (language '%s'), refusing the streamed form: %s", null,
          language, e.getMessage());
      idempotent = false;
    }
    if (idempotent)
      return null;

    return new ExecutionResponse(400, error2json("Only a read-only command can be streamed",
        "The streamed response is written before this endpoint's transaction commits, so a command that writes "
            + "could be acknowledged with rows the commit then fails to make durable. Send the command without "
            + "the " + NDJSON_CONTENT_TYPE + " Accept header to get the buffered response, which is sent after "
            + "the commit.", null, null, null));
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
   *
   * @param maxRows how many rows to drain at most, {@code 0} or less for all of them. Rows past it are dropped
   *                with the source, which is safe only because the caller sizes it above the cap the response
   *                will honor: what is dropped here would have been dropped by the serializer anyway.
   */
  private static ResultSet materializeResultSet(final ResultSet source, final int maxRows) {
    try {
      final List<Result> rows = new ArrayList<>();
      while (source.hasNext() && (maxRows <= 0 || rows.size() < maxRows))
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

    final AsyncResultsetCallback callback = new AsyncResultsetCallback() {
      @Override
      public void onComplete(final ResultSet rs) {
        LogManager.instance().log(this, Level.INFO, "Async command in database \"%s\" completed.", null, database.getName());
      }

      @Override
      public void onError(final Exception exception) {
        LogManager.instance().log(this, Level.SEVERE, "Async command in database \"%s\" failed.", null, database.getName());
        LogManager.instance().log(this, Level.SEVERE, "", exception);
      }
    };

    // Route to the matching overload explicitly (mirroring the synchronous executeCommand): a ternary would give the
    // argument the static type Object, forcing the varargs command(...,Object...) overload and wrapping the params in a
    // single-element array. That makes the polyglot Map path unreachable and every no-param js/map async command throw
    // "positional parameter is not supported".
    if (params instanceof Object[] os)
      database.async().command(language, command, callback, os);
    else
      database.async().command(language, command, callback, (Map<String, Object>) params);
  }
}
