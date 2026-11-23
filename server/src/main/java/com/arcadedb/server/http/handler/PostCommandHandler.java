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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.async.AsyncResultsetCallback;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.ResultRowCapScope;
import com.arcadedb.query.facet.FacetResults;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.ExecutionPlan;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.QueryStatistics;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.ExplainResultSet;
import com.arcadedb.query.sql.parser.MatchStatement;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.query.sql.parser.Statement;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ForwardedRequestIdContext;
import com.arcadedb.server.StandaloneTransactionControl;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.RequestRefusedBeforeExecutionException;
import com.arcadedb.server.monitor.QueryProfile;
import com.arcadedb.server.monitor.ServerQueryProfiler;
import com.arcadedb.server.security.ServerSecurityUser;
import io.micrometer.core.instrument.Metrics;
import io.undertow.server.HttpServerExchange;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class PostCommandHandler extends AbstractQueryHandler {

  public PostCommandHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  /**
   * The position of the first character of {@code command} that is neither whitespace nor inside a comment the SQL grammar hides:
   * a {@code /* ... *}{@code /} block, or a {@code -- } line comment (two dashes and a space, as {@code LINE_COMMENT} in the
   * lexer). An unterminated block comment hides the rest of the text.
   */
  public static int skipLeadingSpaceAndComments(final String command) {
    final int length = command.length();
    int pos = 0;
    while (pos < length) {
      final char c = command.charAt(pos);
      if (Character.isWhitespace(c))
        ++pos;
      else if (command.startsWith("/*", pos)) {
        final int end = command.indexOf("*/", pos + 2);
        pos = end < 0 ? length : end + 2;
      } else if (command.startsWith("-- ", pos)) {
        // ENDED BY A CARRIAGE RETURN OR A LINE FEED, AS THE LEXER ENDS IT; THE OTHER ONE OF A CRLF IS WHITESPACE
        pos += 3;
        while (pos < length && command.charAt(pos) != '\r' && command.charAt(pos) != '\n')
          ++pos;
      } else
        break;
    }
    return pos;
  }

  /**
   * A SQL {@code IMPORT DATABASE} commits its own batches, so it runs without the auto-commit wrapper (issue #11728): inside it
   * the importer took the wrapper for its caller's transaction, which it never commits piecemeal (#6561), and held the whole
   * import in memory until it ended. The text is probed first, so every other command pays a prefix test and nothing more; the
   * parsed statement then decides, through the statement cache the execution reads too (both parse the trimmed text). A command that does not parse is left to
   * the execution, which reports its error.
   */
  @Override
  protected boolean managesItsOwnTransactions(final DatabaseInternal database, final JSONObject payload) {
    if (database == null || payload == null || !"sql".equalsIgnoreCase(payload.getString("language", null)))
      return false;

    final String command = payload.getString("command", null);
    if (command == null)
      return false;

    if (!command.regionMatches(true, skipLeadingSpaceAndComments(command), "import", 0, 6))
      return false;

    try {
      return database.getQueryEngine("sql").analyze(command.trim()).managesItsOwnTransactions();
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.FINE, "Could not analyze the command to decide on the auto-commit wrapper: %s", e,
          e.getMessage());
      return false;
    }
  }

  @Override
  protected void refuseOutsideSession(final DatabaseInternal database, final JSONObject payload) {
    if (payload != null)
      StandaloneTransactionControl.refuse(database, payload.getString("language", null), payload.getString("command", null));
  }

  /**
   * Tells whether the response cap bounds the rows of the given command in the engine: it is a SELECT or a MATCH that states no LIMIT of
   * its own, or a script whose last statement is one. A command that already carries a LIMIT has expressed the caller's own
   * expectation, and that expectation - not the default cap - decides how many rows are serialized, so the engine is told nothing.
   * <p>
   * The cap is handed to the engine as what it is, a bound on the rows the caller reads ({@link ResultRowCapScope}), and not written
   * into the text of the statement as a LIMIT (issue #12016): a LIMIT in the text cannot be told from one the caller wrote, so every
   * planner decision that must respect a real LIMIT was taken against the cap too (a DISTINCT stayed serial, a GROUP BY lost its
   * columnar projection), and the text it was cached under was not the one the caller sent.
   * <p>
   * Read on the parsed statement, from the statement cache the execution takes it from next, so a text no heuristic can read (a
   * LIMIT inside a literal, a comment or a subquery, a trailing TIMEOUT or PARALLEL clause) is read as the parser does. A text that
   * does not parse is left to the execution, which reports its error. A script is parsed only when it holds the word of a query: it
   * has no statement cache, and the statements of a bulk script are the engine's to parse once.
   *
   * @param command  the command, already trimmed as in {@link #execute}
   * @param database the database the command runs against, used only to parse a script with its own parser configuration;
   *                 {@code null} parses with the defaults
   */
  static boolean appliesRowCap(final String command, final String language, final int limit,
      final Database database) {
    // A non-positive cap means unlimited, exactly as it does in the serializer
    if (limit <= 0)
      return false;

    if ("sqlScript".equalsIgnoreCase(language))
      return (StringUtils.containsIgnoreCase(command, "select") || StringUtils.containsIgnoreCase(command, "match"))
          && scriptEndsWithAQueryWithoutLimit(command, database);

    if (!"sql".equalsIgnoreCase(language))
      return false;
    if (!command.regionMatches(true, 0, "select", 0, 6) && !command.regionMatches(true, 0, "match", 0, 5))
      return false;

    final Statement statement;
    try {
      statement = database instanceof DatabaseInternal internal ?
          internal.getStatementCache().getParameterized(command).statement() :
          new SQLAntlrParser(database).parse(command);
    } catch (final CommandSQLParsingException e) {
      return false;
    }
    return hasNoLimit(statement);
  }

  /** Whether the statement is a SELECT or a MATCH with no LIMIT of its own. */
  private static boolean hasNoLimit(final Statement statement) {
    return (statement instanceof SelectStatement || statement instanceof MatchStatement) && statement.getLimit() == null;
  }

  /**
   * Tells whether the script's last statement is a {@code SELECT} or a {@code MATCH} with no LIMIT of its own, i.e. the statement the
   * query engine bounds by the cap is the one whose rows the script returns (issue #8429). A script that does not parse is reported as
   * not ending with one: it is executed as written and fails with the error of its own text. Only the top-level statement list is
   * inspected: a script ending with an {@code IF}, {@code WHILE} or {@code FOREACH} block is not one ending with a query, even when
   * the block's body ends with a {@code SELECT}.
   */
  static boolean scriptEndsWithAQueryWithoutLimit(final String script, final Database database) {
    final List<Statement> statements;
    try {
      statements = new SQLAntlrParser(database).parseScript(script);
    } catch (final CommandSQLParsingException e) {
      return false;
    }
    return statements != null && !statements.isEmpty() && hasNoLimit(statements.getLast());
  }

  /**
   * The rows the engine is told the caller reads at most, one above the cap the response will honor: the extra row is never
   * serialized, it is what lets the serializer tell a result that ends exactly at the cap from one that was cut short (issue #5711).
   * A non-positive cap means unlimited and is passed unchanged, and a cap of {@link Integer#MAX_VALUE} saturates instead of
   * overflowing.
   */
  static int truncationProbeLimit(final int limit) {
    return limit > 0 && limit < Integer.MAX_VALUE ? limit + 1 : limit;
  }

  /**
   * Returns the value of a request field that must be a JSON string, or {@code null} when absent. A present
   * value of the wrong JSON type (number, array, object) is rejected with a {@link RequestRefusedBeforeExecutionException},
   * an {@link IllegalArgumentException} which the HTTP layer maps to a clean 400 Bad Request instead of leaking a raw
   * {@link ClassCastException} as HTTP 500 (issue #5222), and which leaves the session's transaction as it was because the
   * statement has not reached the engine (issue #9852).
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
    throw new RequestRefusedBeforeExecutionException("Field '" + field + "' must be a string");
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

  /**
   * The body is one command, and a follower forwards that command to the leader as it is (issue #8347), so the
   * forward is the client's whole request and relays its key: a retry sent straight to the leader is then replayed
   * from the forward's entry instead of executing the write a second time. The forward posts the client's own body
   * (issue #8359), so that entry is rendered with the client's 'serializer', 'limit' and 'typeHints', and the leader's
   * answer is sent back to the client as it is - see {@link #execute}.
   */
  @Override
  protected boolean commandForwardIsWholeRequest() {
    return true;
  }

  /** Answers {@code Accept: application/x-ndjson} with a streamed result set (issue #7306). */
  @Override
  protected boolean supportsNdJsonEncoding() {
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
      throw new RequestRefusedBeforeExecutionException("command missing");

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

    // Negotiated up front (issue #7306) so a request the stream cannot express is refused before the command
    // runs rather than after. A streamed response is rows and a trailer: the buffered envelope's 'explain',
    // 'explainPlan', 'stats' and 'profile' properties have nowhere to go in it, and silently dropping the one
    // the caller explicitly asked for would be worse than saying so.
    final boolean streaming = isNdJsonRequested(exchange);
    if (streaming) {
      ndJsonRowSerializer(serializer, includeTypeHints);
      if (profileExecution != null)
        throw new RequestRefusedBeforeExecutionException("'profileExecution' reports the plan and the timings in the response "
            + "envelope, which the streaming encoding does not have: request it with 'Accept: application/json'");
    }

    if (command == null || command.isEmpty())
      return new ExecutionResponse(400, "{ \"error\" : \"Command text is null\"}");

    if (language == null || language.isEmpty())
      return new ExecutionResponse(400, "{ \"error\" : \"Language is null\"}");

    command = command.trim();

    if (streaming)
      requireStreamableStatement(database, language, command);

    final Object rawParams = requestMap.get("params");
    Map<String, Object> paramMap;
    if (rawParams instanceof Map<?, ?> m) {
      paramMap = new HashMap<>((int) (m.size() / 0.75f) + 1);
      // A numeric array given as a parameter is a list, as it is when the statement runs embedded with a List: a primitive array
      // would be stored as one element of a LIST property, or as an ARRAY_OF_* instead of a LIST (issue #9002). Only arrays nested
      // deeper in the parameters (batches of vectors) keep the primitive form.
      for (final Map.Entry<?, ?> entry : m.entrySet())
        paramMap.put(String.valueOf(entry.getKey()), numericArrayToList(entry.getValue()));
    } else if (rawParams instanceof List<?> list) {
      // Positional params forwarded as a JSON array [v0, v1, ...] — convert to ordinal map
      paramMap = new HashMap<>((int) (list.size() / 0.75f) + 1);
      for (int i = 0; i < list.size(); i++)
        paramMap.put("" + i, numericArrayToList(list.get(i)));
    } else if (rawParams != null && rawParams.getClass().isArray()) {
      // Positional params converted to a primitive array by toMap(true) — convert to ordinal map
      final int len = Array.getLength(rawParams);
      paramMap = new HashMap<>((int) (len / 0.75f) + 1);
      for (int i = 0; i < len; i++)
        paramMap.put("" + i, boxedNumber(Array.get(rawParams, i)));
    } else {
      paramMap = new HashMap<>();
    }

    // Decode typed JSON markers ($bytes, $int8) before any transaction starts so a malformed
    // marker surfaces as a clean HTTP 400 via the IllegalArgumentException catch arm in
    // AbstractServerHttpHandler, instead of being wrapped by the surrounding TransactionException
    // and downgraded to HTTP 500.
    paramMap = AbstractQueryHandler.decodeTypedJsonMarkers(paramMap);

    // The cap handed to the engine for a command that states no LIMIT of its own: the caller's own 'limit' when present, the configured
    // default otherwise. A command that already carries a LIMIT is left untouched and its own value decides the response size,
    // resolved from the execution plan after execution.
    //
    // Bounded by the hard ceiling before it is handed over, not only when the rows are serialized: an unlimited 'limit' used to bound
    // nothing, so the whole result reached the handler - and with 'profileExecution: detailed' it was materialized in full by
    // materializeResultSet before any cap could look at it. With the ceiling in the cap the engine stops one row past it (issue #5719).
    // Only the caller's own value needs clamping here: getDefaultRowLimit() is already bounded by the ceiling.
    final int maxResultRows = getMaxResultRows();
    final int responseCap = requestLimit != null ? applyMaxResultRows(requestLimit, maxResultRows) : getDefaultRowLimit();
    final boolean rowCapped = appliesRowCap(command, language, responseCap, database);
    // The text as it came, for the log and the running query (the script gets its closing semicolon below)
    final String originalCommand = command;

    if ("sqlScript".equalsIgnoreCase(language) && !command.endsWith(";"))
      command += ";";

    if ("detailed".equalsIgnoreCase(profileExecution))
      paramMap.put("$profileExecution", true);

    // The text the caller sent: it is what the caller will recognize (issue #9680)
    describeRunningQuery(language, originalCommand);

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
      // The statement run as the whole request, and the body it came in: a follower that forwards exactly this statement
      // posts that body to the leader, which answers it in the rendering this request asks for (issue #8359).
      ForwardedRequestIdContext.declareWholeRequestCommand(language, command, exchange.getAttachment(RAW_PAYLOAD));
      ResultSet qResult;
      // One row above the cap: the extra row is never serialized, it only makes truncation detectable. Handed over as a bound on the
      // rows the caller reads, which changes nothing about the plan (issue #12016)
      try (final ResultRowCapScope ignored = ResultRowCapScope.open(rowCapped ? truncationProbeLimit(responseCap) : 0)) {
        qResult = executeCommand(database, language, command, paramMap);
      }

      // The leader answered this request's own body: that answer, not what this node would render again from the rows
      // it parsed back out of it, is the one the client asked for, and the one a retry sent straight to the leader is
      // replayed from. Sent as it is.
      final String leaderAnswer = ForwardedRequestIdContext.takeWholeRequestAnswer();
      if (leaderAnswer != null) {
        if (qResult != null)
          qResult.close();
        Metrics.counter("http.command").increment();
        return new ExecutionResponse(200, leaderAnswer);
      }

      try {
        final JSONObject response = new JSONObject();
        response.put("user", user != null ? user.getName() : null);

        // How many rows the response may carry, in decreasing order of explicitness: the request 'limit', the
        // LIMIT the command carries (read back from the execution plan, and ignored when the command states none: what
        // the plan reports then is the quota of the user's group, which is a limit of the statement and no wish of the
        // caller), the configured default. Only the last one can drop rows the caller never asked to drop, and that is
        // exactly what 'truncated' reports below (issue #5711).
        final int planLimit = rowCapped ? 0 : getPlanLimit(qResult);
        final int limit = resolveLimit(requestLimit, planLimit);
        final SerializationOutcome outcome;

        if (streaming)
          // The same rule as the 'profileExecution' refusal above, for the spelling that carries no request
          // field. Shared with GET /query since issue #7575, which used to reach neither this refusal nor the
          // buffered branch below and answered the plan row as if it were a record.
          requireStreamableResultSet(qResult);

        if (qResult instanceof ExplainResultSet) {
          // EXPLAIN (or SQL PROFILE): extract the plan, then drain the single record so serializeResultSet
          // produces an empty result structure and the plan travels only in the envelope.
          final var executionPlan = drainExplainResultSet(qResult);
          profile.addEngineNanos(System.nanoTime() - engineStart);

          final long serializationStart = System.nanoTime();
          outcome = serializeResultSetBounded(database, serializer, limit, maxResultRows, response, qResult,
              includeTypeHints);
          reportExplainPlan(response, executionPlan);
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
          if (streaming) {
            // Streamed responses carry the rows and the stats trailer only: 'stats', 'explain' and 'explainPlan'
            // below are properties of the buffered envelope, which no longer exists here. A caller that wants
            // them asks for the buffered encoding, where nothing changed.
            outcome = streamResultSetAsNdJson(exchange, database, serializer, limit, maxResultRows, qResult,
                includeTypeHints);
            profile.addSerializationNanos(System.nanoTime() - serializationStart);
            logIfTruncatedByDefault(database.getName(), originalCommand, limit, requestLimit, planLimit, outcome);

            Metrics.counter("http.command").increment();
            recordProfilerMetrics("http.command", profile);
            recordServerProfile(database.getName(), language, command, profile, qResult);
            return null;
          }

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
      final int statementLimit = source.getStatementLimit();
      final Optional<QueryStatistics> stats = source.getStatistics();
      // Before the source is closed: the facets count the matches the drain above did not reach as well
      final FacetResults facets = source.getFacets();
      final IteratorResultSet materialized = new IteratorResultSet(rows.iterator()) {
        @Override
        public Optional<ExecutionPlan> getExecutionPlan() {
          return plan;
        }

        @Override
        public int getStatementLimit() {
          return statementLimit;
        }
      };
      stats.ifPresent(materialized::setStatistics);
      materialized.setFacets(facets);
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

  /** A primitive numeric array parsed from a JSON array parameter becomes the list of its elements, as the embedded API receives it.
   * Elements are narrowed like a scalar parameter the JSON parser reads (a long that fits an int is an int), which is what the embedded
   * caller passing {@code List.of(1, 2, 3)} gives.
   */
  private static Object numericArrayToList(final Object value) {
    return Type.isPrimitiveNumberArray(value) ? Type.primitiveArrayToJsonList(value) : value;
  }

  /** A long that fits an int is an int, as the JSON parser answers a scalar number. */
  private static Object boxedNumber(final Object value) {
    if (value instanceof Long l && l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE)
      return l.intValue();
    return value;
  }
}
