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
package com.arcadedb.query.opencypher.query;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.Document;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.DeferredExistenceChecks;
import com.arcadedb.database.DocumentValidator;
import com.arcadedb.database.ExistingRecordsCheck;
import com.arcadedb.database.Record;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParameterMissingException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.QueryNotIdempotentException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.GraphTraversalProviderRegistry.ViewsPassedOver;
import com.arcadedb.query.OperationType;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.QuerySession;
import com.arcadedb.query.literal.LiteralParameterizer.Lookup;
import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.query.opencypher.ast.ConditionalStatement;
import com.arcadedb.query.opencypher.ast.CypherAdminStatement;
import com.arcadedb.query.opencypher.ast.CypherDDLStatement;
import com.arcadedb.query.opencypher.ast.CypherShowSchemaStatement;
import com.arcadedb.query.opencypher.ast.CypherTypeName;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.IsTypedExpression;
import com.arcadedb.query.opencypher.temporal.TemporalUtil;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.query.opencypher.ast.CypherSessionStatement;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.CypherTransactionStatement;
import com.arcadedb.query.opencypher.executor.CypherExecutionPlan;
import com.arcadedb.query.opencypher.executor.CypherFunctionFactory;
import com.arcadedb.query.opencypher.executor.ExpressionEvaluator;
import com.arcadedb.query.opencypher.optimizer.plan.PhysicalPlan;
import com.arcadedb.query.opencypher.parser.Cypher25AntlrParser.ParsedQuery;
import com.arcadedb.query.opencypher.planner.CypherExecutionPlanner;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.QueryStatistics;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.CollectionUtils;
import com.arcadedb.utility.DateUtils;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.TypeIndexBuilder;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityManager;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.function.sql.DefaultSQLFunctionFactory;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;

import java.math.BigDecimal;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

/**
 * Native OpenCypher query engine for ArcadeDB.
 * Implements direct Cypher query execution using ANTLR4 grammar-based parser.
 */
public class OpenCypherQueryEngine implements QueryEngine {
  public static final String ENGINE_NAME = "opencypher";

  // Shared stateless components - thread-safe and reusable
  private static final DefaultSQLFunctionFactory SQL_FUNCTION_FACTORY = DefaultSQLFunctionFactory.getInstance();
  private static final CypherFunctionFactory CYPHER_FUNCTION_FACTORY = new CypherFunctionFactory(SQL_FUNCTION_FACTORY);
  private static final ExpressionEvaluator EXPRESSION_EVALUATOR = new ExpressionEvaluator(CYPHER_FUNCTION_FACTORY);
  /**
   * Plan-cache key prefixes of the branches of a conditional query: {@code "\0WHEN#<i>\0<query>"} and
   * {@code "\0ELSE\0<query>"}. The NUL inside the key is refused by the lexer outside a string literal, so no query text
   * that plans can be equal to a key.
   */
  private static final String WHEN_BRANCH_KEY_PREFIX = "\u0000WHEN#";
  private static final String ELSE_BRANCH_KEY_PREFIX = "\u0000ELSE\u0000";

  /** Columns of {@code SHOW USERS} and {@code SHOW CURRENT USER}. */
  private static final List<String> USER_COLUMNS = List.of("user", "databases");

  private final DatabaseInternal database;

  protected OpenCypherQueryEngine(final DatabaseInternal database) {
    this.database = database;
  }

  @Override
  public String getLanguage() {
    return ENGINE_NAME;
  }

  @Override
  public AnalyzedQuery analyze(final String query) {
    try {
      // EXPLAIN/PROFILE are prefixes the query()/command() paths strip before parsing, so the grammar never sees them.
      // PROFILE executes what it wraps, so it is classified exactly as the wrapped statement; EXPLAIN only plans, so
      // it stays idempotent while still reporting the wrapped statement's operation types (issue #9313).
      final String stripped = stripLeadingComments(query);
      final boolean explain = stripped.regionMatches(true, 0, "EXPLAIN ", 0, 8);
      final String actual = explain || stripped.regionMatches(true, 0, "PROFILE ", 0, 8) ? stripped.substring(8).trim() : query;

      // Use statement cache to avoid re-parsing
      // The same lookup the execution makes, so the literals of the text are extracted the same way and the cache holds one
      // entry for both (issue #8307)
      final CypherStatement statement = database.getCypherStatementCache().getParameterized(actual).statement().statement();

      return new AnalyzedQuery() {
        @Override
        public boolean isIdempotent() {
          return explain || statement.isReadOnly();
        }

        @Override
        public boolean isDDL() {
          return statement instanceof CypherDDLStatement;
        }

        @Override
        public Set<OperationType> getOperationTypes() {
          if (statement instanceof CypherAdminStatement)
            return CollectionUtils.singletonSet(OperationType.ADMIN);
          // Server-control statements (transaction control and session management) read/write no data
          // themselves; the writes they wrap are gated by their own operation types. They must not be
          // classified as ADMIN (that would lock them behind the MCP admin flag and stop a write-capable
          // agent from committing its own writes). This permission axis is independent of isReadOnly()/HA
          // routing - see CypherStatement.isServerControlStatement() - so keep this check ahead of the
          // isReadOnly()/write-detection fallback below to avoid the write-set misclassification.
          if (statement.isServerControlStatement())
            return CollectionUtils.singletonSet(OperationType.READ);
          if (statement instanceof CypherDDLStatement)
            return CollectionUtils.singletonSet(OperationType.SCHEMA);
          if (statement.isReadOnly())
            return CollectionUtils.singletonSet(OperationType.READ);

          final EnumSet<OperationType> ops = EnumSet.noneOf(OperationType.class);
          if (statement.hasCreate())
            ops.add(OperationType.CREATE);
          if (statement.hasMerge()) {
            ops.add(OperationType.CREATE);
            ops.add(OperationType.UPDATE);
          }
          if (statement.hasDelete())
            ops.add(OperationType.DELETE);
          if (statement.hasSet())
            ops.add(OperationType.UPDATE);
          if (statement.hasRemove())
            ops.add(OperationType.UPDATE);
          return ops.isEmpty() ? Set.of(OperationType.CREATE, OperationType.UPDATE, OperationType.DELETE) : Set.copyOf(ops);
        }
      };
    } catch (final Exception e) {
      throw new CommandParsingException("Error analyzing Cypher query: " + query, e);
    }
  }

  /**
   * Free, and that is why Cypher can answer it (issue #6324, item 5): the parse is a {@code CypherStatementCache}
   * lookup that the execution about to follow repeats with the same key - the same argument that lets SQL answer.
   * <p>
   * Before this, {@code CREATE INDEX} dispatched with {@code awaitResponse=false} worked in SQL and was refused in
   * Cypher, because the routing was SQL's private knowledge. The four Cypher DDL statements -
   * {@code CREATE}/{@code DROP INDEX} and {@code CREATE}/{@code DROP CONSTRAINT} - are exactly the ones that reach
   * the index builder, and so exactly the ones that have to run off the async workers.
   */
  @Override
  public DDLClassification classifyDDL(final String query) {
    return database.getCypherStatementCache().getParameterized(query).statement().statement() instanceof CypherDDLStatement ?
        DDLClassification.DDL :
        DDLClassification.NOT_DDL;
  }

  @Override
  public ResultSet query(final String query, final ContextConfiguration configuration, final Map<String, Object> parameters) {
    try {
      // Check for EXPLAIN or PROFILE prefix
      String actualQuery = stripLeadingComments(query);
      final String upperQuery = actualQuery.toUpperCase(Locale.ROOT);
      boolean explain = false;
      boolean profile = false;

      if (upperQuery.startsWith("EXPLAIN ")) {
        explain = true;
        actualQuery = actualQuery.substring(8).trim();
      } else if (upperQuery.startsWith("PROFILE ")) {
        profile = true;
        actualQuery = actualQuery.substring(8).trim();
      }

      final boolean timeExecution = asksForTimedExecution(parameters);

      // Use statement cache to avoid re-parsing. Carries the parameter names the query references, so the
      // check below costs no extra lookup. The literals of the text may come back extracted into parameters (issue #8307):
      // they are bound with the caller's, and the plan is cached under the key they were extracted into. EXPLAIN and PROFILE
      // plan the text as written, as SQL's do: their plans are never cached, and they show the values the caller wrote.
      final Lookup<ParsedQuery> lookup = explain || profile ?
          database.getCypherStatementCache().getAsWritten(actualQuery) :
          database.getCypherStatementCache().getParameterized(actualQuery);
      final ParsedQuery parsed = lookup.statement();
      final CypherStatement statement = parsed.statement();

      // Make any session parameters (SESSION SET) visible to this query as $name.
      final QuerySession session = currentQuerySession();
      final Map<String, Object> effectiveParameters = lookup.mergeParameters(QuerySession.mergeParameters(
          session != null ? session.getParameters() : null, parameters));

      // EXPLAIN never executes the query, so it is also the one mode that tolerates unbound parameters:
      // Neo4j reports them as a notification there instead of failing, which is what makes EXPLAIN usable
      // for inspecting a plan before the values are known.
      if (!explain)
        checkParametersAreBound(parsed.parameters(), effectiveParameters);

      // EXPLAIN never executes the underlying query, so the idempotency rule does not apply. PROFILE does execute it,
      // so a write under PROFILE is refused here exactly like the bare write (issue #9313); Studio and the command
      // path (database.command) are where a profiled write is legitimate (issue #4366).
      if (!explain && !statement.isReadOnly())
        throw new QueryNotIdempotentException("Query '" + query + "' is not idempotent");

      // SHOW/TERMINATE TRANSACTIONS read no data and are answered by the server's registry, not by a plan (issue #9689)
      if (!explain && statement instanceof CypherAdminStatement admin && admin.isTransactionsCommand())
        return executeAdmin(admin, actualQuery, effectiveParameters);

      // SHOW CONSTRAINTS / SHOW INDEXES are answered from the schema (issue #9706)
      if (!explain && statement instanceof CypherShowSchemaStatement show)
        return CypherSchemaCommands.execute(database, show, actualQuery, effectiveParameters);

      return execute(lookup.cacheKey(), statement, configuration, effectiveParameters, explain, profile, timeExecution);
    } catch (final QueryNotIdempotentException | CommandExecutionException | CommandParsingException | SecurityException
             | NeedRetryException e) {
      // A NeedRetryException keeps its type: Database.transaction(..., retries) retries on it, and SQL lets it through (#9487)
      throw e;
    } catch (final Exception e) {
      throw new CommandExecutionException("Error executing Cypher query: " + query, e);
    }
  }

  @Override
  public ResultSet query(final String query, final ContextConfiguration configuration, final Object... parameters) {
    return query(query, configuration, convertPositionalParameters(parameters));
  }

  /**
   * Trims the query and removes the line and block comments that precede the first token, so a
   * commented EXPLAIN or PROFILE statement is still recognised by its prefix.
   */
  static String stripLeadingComments(final String query) {
    int i = 0;
    final int len = query.length();
    boolean stripped = false;
    while (i < len) {
      final char c = query.charAt(i);
      if (Character.isWhitespace(c))
        i++;
      else if (c == '/' && i + 1 < len && query.charAt(i + 1) == '/') {
        stripped = true;
        // a line comment ends at \n or at a bare \r
        i += 2;
        while (i < len && query.charAt(i) != '\n' && query.charAt(i) != '\r')
          i++;
      } else if (c == '/' && i + 1 < len && query.charAt(i + 1) == '*') {
        final int end = query.indexOf("*/", i + 2);
        if (end < 0)
          break;
        stripped = true;
        i = end + 2;
      } else
        break;
    }
    // no leading comment: nothing to strip beyond the trim
    if (!stripped)
      return query.trim();
    return query.substring(i).trim();
  }

  @Override
  public ResultSet command(final String query, final ContextConfiguration configuration, final Map<String, Object> parameters) {
    try {
      // Check for EXPLAIN or PROFILE prefix
      String actualQuery = stripLeadingComments(query);
      final String upperQuery = actualQuery.toUpperCase(Locale.ROOT);
      boolean explain = false;
      boolean profile = false;

      if (upperQuery.startsWith("EXPLAIN ")) {
        explain = true;
        actualQuery = actualQuery.substring(8).trim();
      } else if (upperQuery.startsWith("PROFILE ")) {
        profile = true;
        actualQuery = actualQuery.substring(8).trim();
      }

      final boolean timeExecution = asksForTimedExecution(parameters);

      // Use statement cache to avoid re-parsing. Carries the parameter names the query references, so the
      // check below costs no extra lookup. The literals of the text may come back extracted into parameters (issue #8307):
      // they are bound with the caller's, and the plan is cached under the key they were extracted into. EXPLAIN and PROFILE
      // plan the text as written, as SQL's do: their plans are never cached, and they show the values the caller wrote.
      final Lookup<ParsedQuery> lookup = explain || profile ?
          database.getCypherStatementCache().getAsWritten(actualQuery) :
          database.getCypherStatementCache().getParameterized(actualQuery);
      final ParsedQuery parsed = lookup.statement();
      final CypherStatement statement = parsed.statement();

      // Make any session parameters (SESSION SET) visible to this command as $name. Resolve the session
      // once and thread it to the merge, the parameter check and executeSession (avoids repeated
      // thread-context lookups). Done before the direct-dispatch statements below so that every path
      // through this method - DDL, admin, transaction control, session and the planner pipeline - checks
      // the parameters the same way.
      final QuerySession session = currentQuerySession();
      final Map<String, Object> effectiveParameters = lookup.mergeParameters(QuerySession.mergeParameters(
          session != null ? session.getParameters() : null, parameters));

      // See the EXPLAIN note in query(): a plan can be inspected before the values are known.
      if (!explain)
        checkParametersAreBound(parsed.parameters(), effectiveParameters);

      // DDL statements (constraints) are executed directly without the planner pipeline
      if (statement instanceof CypherDDLStatement)
        return executeDDL((CypherDDLStatement) statement);

      // SHOW CONSTRAINTS / SHOW INDEXES are answered from the schema (issue #9706)
      if (statement instanceof CypherShowSchemaStatement show)
        return CypherSchemaCommands.execute(database, show, actualQuery, effectiveParameters);

      // Admin statements (user management) are executed directly against the security manager
      if (statement instanceof CypherAdminStatement)
        return executeAdmin((CypherAdminStatement) statement, actualQuery, effectiveParameters);

      // Transaction control statements (START TRANSACTION/COMMIT/ROLLBACK) are executed directly
      // against the database transaction API, bypassing the planner's auto-commit pipeline.
      if (statement instanceof CypherTransactionStatement)
        return executeTransaction((CypherTransactionStatement) statement);

      // Session management statements (SESSION SET/RESET/CLOSE) operate on the server session bound to
      // the current thread; executed directly, no planner pipeline.
      if (statement instanceof CypherSessionStatement)
        return executeSession((CypherSessionStatement) statement, session, effectiveParameters);

      return execute(lookup.cacheKey(), statement, configuration, effectiveParameters, explain, profile, timeExecution);
    } catch (final CommandExecutionException | CommandParsingException | SecurityException | NeedRetryException e) {
      // See query(): a retryable conflict must reach Database.transaction(..., retries) with its own type (#9487)
      throw e;
    } catch (final Exception e) {
      throw new CommandExecutionException("Error executing Cypher command: " + query, e);
    }
  }

  @Override
  public ResultSet command(final String query, final ContextConfiguration configuration, final Object... parameters) {
    return command(query, configuration, convertPositionalParameters(parameters));
  }

  /**
   * Whether the caller asked for the statement to be TIMED - not for it to be run differently.
   * <p>
   * {@code $profileExecution} is injected by the HTTP handler for Studio's {@code profileExecution: "detailed"},
   * and by {@code ServerDatabase} for every statement while the server profiler is recording. It turns on per-step
   * timing over the ordinary streaming execution, and is deliberately NOT folded into the {@code PROFILE} keyword's
   * own flag: doing that rerouted every Cypher statement on a recording server onto
   * {@link CypherExecutionPlan#profile()}, which drains the plan into heap, so the reported cost described a
   * materialising execution the statement never otherwise performs - the diagnostic changed what it was diagnosing
   * (issue #7330).
   * <p>
   * One method rather than a copy in {@link #query} and another in {@link #command}, so the two cannot drift apart
   * the way the folding they replace did.
   */
  private static boolean asksForTimedExecution(final Map<String, Object> parameters) {
    return parameters != null && Boolean.TRUE.equals(parameters.get("$profileExecution"));
  }

  /**
   * Fails the query when it references a parameter the caller never bound.
   * <p>
   * An unbound parameter used to evaluate to null, and null is a legal value everywhere, so the query ran
   * to completion against a value nobody supplied: a filter matched nothing, a predicate came out false
   * and each caller absorbed that into its neutral answer. That silence is what made issue #5501 and its
   * siblings invisible - a de-duplicating {@code WHERE NOT EXISTS { ... $id ... } CREATE ...} guard degraded
   * into an unconditional CREATE rather than failing. Neo4j raises here, and so do we now.
   * <p>
   * Bound to null is not unbound: a caller that explicitly passes {@code null} means it, and the query runs.
   *
   * @param referenced the parameter names the query text mentions, collected at parse time
   *
   * @throws CommandParameterMissingException listing every missing name, in the order the query mentions them
   */
  private static void checkParametersAreBound(final Set<String> referenced, final Map<String, Object> parameters) {
    if (referenced.isEmpty())
      return;

    // The common case is that nothing is missing, so the list is allocated only once one is found.
    List<String> missing = null;
    for (final String name : referenced) {
      if (parameters == null || !parameters.containsKey(name)) {
        if (missing == null)
          missing = new ArrayList<>(referenced.size());
        missing.add(name);
      }
    }

    if (missing != null)
      throw new CommandParameterMissingException(missing.toArray(new String[0]));
  }

  /**
   * The context the conditions of a conditional query are evaluated against: the database, the parameters, the functions.
   * A condition reads parameters only (the parser refuses anything else) and is expected to have no side effect.
   */
  private BasicCommandContext conditionContext(final Map<String, Object> parameters) {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    context.setInputParameters(parameters);
    CypherExecutionPlan.installFunctionResolver(context, CYPHER_FUNCTION_FACTORY);
    return context;
  }

  /**
   * Evaluates the condition of a conditional query branch. Only {@code true} takes the branch: null and false both pass to
   * the next one, as a WHERE does. Anything that is not a boolean is a type error rather than a silent "not taken".
   */
  private static boolean isConditionTrue(final Expression condition, final BasicCommandContext context) {
    final Object value = EXPRESSION_EVALUATOR.evaluate(condition, new ResultInternal(), context);
    if (value == null || value instanceof Boolean)
      return Boolean.TRUE.equals(value);
    throw new CommandSemanticException("Type mismatch: the condition of a conditional query (WHEN ... THEN) must be a BOOLEAN, got "
        + CypherFunctionHelper.cypherTypeName(value));
  }

  /**
   * Executes a parsed Cypher statement.
   *
   * @param queryString   the original query string (for plan cache key)
   * @param statement     the parsed Cypher statement
   * @param configuration context configuration
   * @param parameters    query parameters
   * @param explain       if true, return EXPLAIN output instead of executing
   * @param profile       if true, take the eager PROFILE path: drain the whole plan and return the metrics with it
   * @param timeExecution if true, time each step of the ordinary streaming execution and attach the resulting plan.
   *                      Unlike {@code profile} this changes nothing about what runs - see
   *                      {@link CypherExecutionPlan#execute(com.arcadedb.query.sql.executor.CommandContext, boolean)}
   *                      (issue #7330)
   * @return result set
   */
  private ResultSet execute(final String queryString, final CypherStatement statement, final ContextConfiguration configuration,
      final Map<String, Object> parameters, final boolean explain, final boolean profile, final boolean timeExecution) {
    // A conditional query runs one of its branches, picked now that the parameters its conditions read are known. Each branch
    // is planned and cached under a key of its own, since the same text runs different branches for different parameters.
    // The key carries a NUL inside it ("\0WHEN#i\0<query>"), and the lexer refuses a NUL outside a string literal, so no
    // query text that plans can equal a key.
    if (statement instanceof ConditionalStatement conditional) {
      final List<CypherStatement> branches = conditional.getBranches();
      // The conditions share one context: they read the same parameters
      final BasicCommandContext conditionContext = conditionContext(parameters);
      for (int i = 0; i < branches.size(); i++)
        if (isConditionTrue(conditional.getConditions().get(i), conditionContext))
          return execute(WHEN_BRANCH_KEY_PREFIX + i + "\u0000" + queryString, branches.get(i), configuration, parameters, explain,
              profile, timeExecution);
      if (conditional.getElseBranch() != null)
        return execute(ELSE_BRANCH_KEY_PREFIX + queryString, conditional.getElseBranch(), configuration, parameters, explain,
            profile, timeExecution);
      return new InternalResultSet();
    }

    // Try to get cached physical plan first (saves optimization time: 200-500ms)
    final CypherExecutionPlan plan;
    final DatabaseInternal execDb = executionDatabase(statement);

    // A transaction holding uncommitted changes gets no Graph Analytical View: a cached plan may carry one it would
    // read past those changes, and a plan built now carries none, so it must not reach the cache either
    if (!explain && !profile && !GraphTraversalProviderRegistry.isWithheld(execDb)) {
      // Only use plan cache for normal execution (not explain/profile)
      final PhysicalPlan physicalPlan = database.getCypherPlanCache().get(queryString);
      // A cached plan that reads a view no longer usable (gone stale, dropped), or that walks the records because a view was
      // not ready when it was planned and now is (issue #9587), is planned again, and the new plan replaces it in the cache
      if (physicalPlan != null && !physicalPlan.readsAnUnavailableView() && !physicalPlan.passesOverAReadyView()) {
        // Reuse cached physical plan (avoids expensive statistics collection and optimization)
        plan = new CypherExecutionPlan(
            execDb, statement, parameters, configuration, physicalPlan, EXPRESSION_EVALUATOR);
      } else {
        // Create new plan from scratch and cache it. planningEpoch is read before planning begins so put() can
        // detect a DDL (DROP INDEX/ALTER TYPE/create-index) that invalidated the cache concurrently with this
        // planning and skip caching a plan already built against stale schema/index state (issue #6671).
        final long planningEpoch = database.getCypherPlanCache().getInvalidationEpoch();
        final CypherExecutionPlanner planner = new CypherExecutionPlanner(execDb, statement, parameters,
            EXPRESSION_EVALUATOR);
        try (final ViewsPassedOver passedOver = GraphTraversalProviderRegistry.recordViewsPassedOver()) {
          plan = planner.createExecutionPlan(configuration);

          // Cache the physical plan for future use, with the views it passed over so it is planned again once one is ready
          if (plan.getPhysicalPlan() != null) {
            plan.getPhysicalPlan().setViewsPassedOver(passedOver.getViews());
            database.getCypherPlanCache().put(queryString, plan.getPhysicalPlan(), planningEpoch);
          }
        }
      }
    } else {
      // explain/profile mode, or views withheld: always create new plan without caching
      final CypherExecutionPlanner planner = new CypherExecutionPlanner(execDb, statement, parameters,
          EXPRESSION_EVALUATOR);
      plan = planner.createExecutionPlan(configuration);
    }

    if (explain)
      return plan.explain();

    // A read-only statement writes nothing, so there is no half-written record for the scope to hold and no reason
    // to pay for one. A write statement is drained to completion inside plan.profile()/plan.execute(), so the line
    // after the call is the end of the statement and the moment Neo4j - and now we - enforce existence constraints
    // (issue #7945). A nested plan (CALL subquery, FOREACH body, UNION branch) joins this scope rather than opening
    // one of its own, so the whole statement is covered by this single pair.
    if (statement.isReadOnly())
      return profile ? plan.profile() : plan.execute(null, timeExecution);

    final DeferredExistenceChecks deferredChecks = DeferredExistenceChecks.open(execDb);
    try {
      final ResultSet result = profile ? plan.profile() : plan.execute(null, timeExecution);
      if (deferredChecks != null && !deferredChecks.isEmpty())
        deferredChecks.check();
      return result;
    } catch (final RuntimeException | Error e) {
      // A statement that fails for any other reason leaves nothing provisional behind either: the records it
      // created and never completed are taken back without a second exception hiding this one. This also catches
      // the ValidationException check() itself raises, which lands here having already emptied the pending set -
      // so the discard below is a no-op then, not a second deletion of the records check() just removed.
      if (deferredChecks != null)
        deferredChecks.discard();
      throw e;
    } finally {
      if (deferredChecks != null)
        deferredChecks.close();
    }
  }

  /**
   * The database a Cypher plan executes against, and therefore the one {@code CommandContext.getDatabase()} returns.
   * <p>
   * The write steps auto-commit: {@code SetStep}, {@code DeleteStep}, {@code MergeStep}, {@code RemoveStep},
   * {@code ForeachStep} and a writing {@code CALL} subquery each call {@code begin()}/{@code commit()} directly on
   * {@code context.getDatabase()} when no transaction is already open. Handing them the raw instance sends those
   * commits to {@code LocalDatabase.commit()}, which on an HA leader applies the pages locally and never proposes
   * them to Raft: followers then trail by exactly those page versions and the next replicated entry touching one of
   * them fails the version check (#5492, #5655). Off HA the wrapper is the instance itself, so this is a no-op there.
   * <p>
   * {@code CreateStep} would be safe either way - it wraps its work in {@code database.transaction(...)}, and
   * {@code LocalDatabase.transaction()} drives {@code wrappedDatabaseInstance} internally rather than {@code this}.
   * That is why a bare {@code CREATE} replicated correctly while an equally ordinary {@code SET} did not, and it is
   * the kind of asymmetry that makes a smoke test look green.
   * <p>
   * A read-only statement keeps the raw instance. It cannot commit, so resolving the wrapper would change nothing
   * about durability, while it would put the nested reads the steps issue on a different footing. Being precise
   * about which ones: {@code RaftReplicatedDatabase.lookupByRID}/{@code iterateType}/{@code lookupByKey} delegate
   * straight to the inner instance, so those are indifferent - but its {@code query(language, ...)} overloads open
   * with {@code waitForReadConsistency()}, and {@code ExistsExpression}, {@code CountExpression} and
   * {@code CollectExpression} all evaluate their subquery through exactly that call. Routing a read-only statement
   * through the wrapper would therefore add a read barrier to every {@code EXISTS}/{@code COUNT}/{@code COLLECT}
   * subquery, follower-local ones included. The switch is on {@code isReadOnly()} rather than on the entry point because
   * {@link #query} does not imply read-only here: {@code PROFILE} bypasses the idempotency gate and executes, so a
   * {@code PROFILE MATCH ... SET ...} reaches this method through {@code query()} and does write.
   *
   * One consequence worth knowing before you touch this: inside an explicit transaction the two instances are
   * deliberately mixed. {@code START TRANSACTION} opens on the wrapper, a read-only {@code MATCH} then runs against
   * the raw instance and a write in the same transaction against the wrapper. That is safe because the transaction
   * lives in the thread-local {@link DatabaseContext} keyed by database path, which both handles share, and because
   * a read never commits - so both see one transaction and only one of them can end it.
   *
   * @param statement the statement about to be planned, whose {@code isReadOnly()} already accounts for writes
   *                  nested in a {@code CALL} subquery
   */
  private DatabaseInternal executionDatabase(final CypherStatement statement) {
    return statement.isReadOnly() ? database : database.getWrappedDatabaseInstance();
  }

  /**
   * Executes a DDL statement (constraint creation/deletion) directly against the schema.
   */
  private ResultSet executeDDL(final CypherDDLStatement ddl) {
    final Schema schema = database.getSchema();
    final QueryStatistics stats = new QueryStatistics();

    switch (ddl.getKind()) {
    case CREATE_CONSTRAINT:
      // One constraint statement at a time per database: a name is checked, then the schema is changed, then the name is
      // recorded, and two statements interleaving those steps could both take one name or both find no equivalent (#9706)
      synchronized (CypherConstraintRegistry.lockFor(database)) {
        executeCreateConstraint(ddl, schema, stats);
      }
      break;
    case DROP_CONSTRAINT:
      synchronized (CypherConstraintRegistry.lockFor(database)) {
        executeDropConstraint(ddl, schema, stats);
      }
      break;
    case CREATE_INDEX:
      // Under the constraint lock too: an index name is checked against the constraint names before it is taken (#9705)
      synchronized (CypherConstraintRegistry.lockFor(database)) {
        executeCreateIndex(ddl, schema, stats);
      }
      break;
    case DROP_INDEX:
      synchronized (CypherConstraintRegistry.lockFor(database)) {
        executeDropIndex(ddl, schema, stats);
      }
      break;
    }

    final InternalResultSet result = new InternalResultSet();
    result.setStatistics(stats);
    return result;
  }

  /**
   * Checks whether a type index already covers exactly {@code propertyNames}, looking both at the
   * type's own indexes and any inherited from a parent type. Used by Cypher constraint/index DDL to
   * tell whether an {@code IF NOT EXISTS} create is a genuine no-op, so the DDL result statistics
   * (indexes/constraints added) only count schema changes that actually happened.
   */
  private static boolean indexExistsOnProperties(final Schema schema, final String typeName, final String[] propertyNames) {
    // Both callers auto-create the type before this runs, so it normally exists. Guard defensively:
    // a missing type has no index, and getType would otherwise throw for a future caller.
    if (!schema.existsType(typeName))
      return false;
    final DocumentType type = schema.getType(typeName);
    return type.getIndexByProperties(propertyNames) != null || type.getPolymorphicIndexByProperties(propertyNames) != null;
  }

  /**
   * Like {@link #indexExistsOnProperties} but only reports a pre-existing UNIQUE index. A non-unique
   * index over the same properties does not satisfy a UNIQUE/KEY constraint, so adding the constraint
   * is a genuine schema change that must be counted.
   */
  private static boolean uniqueIndexExistsOnProperties(final Schema schema, final String typeName, final String[] propertyNames) {
    if (!schema.existsType(typeName))
      return false;
    final DocumentType type = schema.getType(typeName);
    final Index own = type.getIndexByProperties(propertyNames);
    if (own != null && own.isUnique())
      return true;
    final Index poly = type.getPolymorphicIndexByProperties(propertyNames);
    return poly != null && poly.isUnique();
  }

  /**
   * Creates the type a DDL statement names when it does not exist yet, since Cypher does not require types to be
   * pre-declared.
   * <p>
   * This is the one place a DDL statement reaches the schema with a label, and it does so without going through
   * {@link Labels#ensureCompositeType}, so it carries that method's separator guard itself: a label containing
   * {@link Labels#LABEL_SEPARATOR} would otherwise occupy the very name that the ordinary label set spelling out
   * its parts computes (issue #8100). The guard deliberately sits behind the existence check - a name containing
   * the separator is exactly what an existing composite type is called, so indexing or constraining one has to
   * keep working. The relationship branch carries the matching guard for edge type names, which landed on main
   * as issue #8118 while this branch was open: vertex and edge types share one namespace, so an edge type named
   * {@code A~B} permanently blocks the composite vertex type that the label set {@code [A, B]} computes.
   */
  private static void autoCreateDDLType(final Schema schema, final CypherDDLStatement ddl, final String typeName) {
    // The blank check runs before the existence check, unlike the separator one: a whitespace-only type name is
    // refused by no layer below this (TypeBuilder.create() tests isEmpty, not isBlank), so such a type can already
    // exist in a database written by SQL or the Java API, and indexing a label no other openCypher path accepts
    // would be the one way left to name it.
    Labels.requireNonBlankLabelName(typeName, "a label in this statement");

    if (schema.existsType(typeName))
      return;

    if (ddl.isForRelationship()) {
      Labels.requireUsableRelationshipTypeName(typeName);
      schema.getOrCreateEdgeType(typeName);
    } else
      schema.getOrCreateVertexType(Labels.requireUsableLabelName(typeName, "a label in this statement"));
  }

  /**
   * Changes the declared type of a property, keeping every other attribute of its declaration and refusing the change when a
   * stored record violates the new declaration (#9304). The schema has no in-place type change, so the property is recreated
   * from its own JSON; a refusal (or any failure, an Error included) puts the previous declaration back before rethrowing. The
   * check scans the whole type, as CREATE PROPERTY does.
   * <p>
   * The attributes no type restricts (MANDATORY, NOTNULL, READONLY, HIDDEN, EXTERNAL, COMPRESSION, custom values) are carried
   * over as they are. The ones expressed in the old type's terms - OF, REGEXP, MIN, MAX and DEFAULT - are carried over
   * only when the new type reads them the same way, and otherwise the statement is refused naming them (#9645): copied blindly
   * they left a property no value could be written through, and the scan of the stored records cannot notice it on a type that
   * holds none.
   */
  private void retypeProperty(final Schema schema, final String typeName, final Property existing, final DeclaredType target) {
    final DocumentType type = schema.getType(typeName);
    final String propName = existing.getName();
    final Database database = schema.getEmbedded().getDatabase();
    final Type newType = target.type();

    final List<String> notCarried = typeDependentAttributesNotCarried(database, existing, newType, target.replacesOfType());
    if (!notCarried.isEmpty())
      throw refuseRetype(typeName, existing, newType, notCarried, null);

    final JSONObject previous = existing.toJSON();
    final JSONObject retyped = new JSONObject(previous.toMap());
    retyped.put("type", newType.name());
    // a LIST<T> names its element type, so the declaration takes it rather than carrying the old one over (#9709)
    if (target.replacesOfType()) {
      if (target.ofType() != null)
        retyped.put("of", target.ofType());
      else
        retyped.remove("of");
    }

    type.dropProperty(propName);
    try {
      final Property created = type.createProperty(propName, retyped);
      requireDefaultReadableAs(database, typeName, existing, created, newType);
      ExistingRecordsCheck.requireDeclaration(database, type, created);
    } catch (final Throwable e) {
      // the schema is not transactional: until the declaration is back, a concurrent write is not validated against it
      try {
        if (type.existsProperty(propName))
          type.dropProperty(propName);
        type.createProperty(propName, previous);
      } catch (final Throwable restoreFailure) {
        e.addSuppressed(restoreFailure);
      }
      throw e;
    }
  }

  /**
   * The OF, REGEXP, MIN and MAX of {@code existing} that {@code newType} cannot hold with the meaning they have now, each
   * described with the reason. An empty list means every one of them, if set, carries over unchanged.
   */
  private static List<String> typeDependentAttributesNotCarried(final Database database, final Property existing,
      final Type newType, final boolean replacesOfType) {
    final Type oldType = existing.getType();
    final List<String> notCarried = new ArrayList<>(4);

    // the element type of a LIST is the value type of a MAP and nothing else: no other type has elements to constrain
    if (!replacesOfType && existing.getOfType() != null && !(hasElementType(oldType) && hasElementType(newType)))
      notCarried.add("OF '" + existing.getOfType() + "' (" + newType + " has no element type)");

    // a pattern is matched against the text form of the value, so it carries over when some value of the new type can be
    // written as a text it matches: '[0-9]+' moves between a LONG and a STRING, '[A-Z]+' refuses every value of a LONG. The
    // same check the declaration itself is held to, so a declaration the schema accepted is always one DROP CONSTRAINT can
    // put back (#9991)
    final String regexpReason = DocumentValidator.unusableRegexp(database, newType, existing.getRegexp());
    if (regexpReason != null)
      notCarried.add("REGEXP '" + existing.getRegexp() + "' (on " + newType + " it " + regexpReason + ")");

    final String minReason = boundNotCarried(database, oldType, newType, existing.getMin());
    if (minReason != null)
      notCarried.add("MIN '" + existing.getMin() + "' (" + minReason + ")");
    final String maxReason = boundNotCarried(database, oldType, newType, existing.getMax());
    if (maxReason != null)
      notCarried.add("MAX '" + existing.getMax() + "' (" + maxReason + ")");

    return notCarried;
  }

  private static boolean hasElementType(final Type type) {
    return type == Type.LIST || type == Type.MAP;
  }

  /**
   * Why a MIN or MAX of a property of {@code oldType} cannot serve the same purpose on {@code newType}, or null when it can:
   * the bound is a length on a STRING, a size on a collection and a value on a number or a date, so it carries over only
   * between two types that measure the same thing and when the new type can read it.
   */
  private static String boundNotCarried(final Database database, final Type oldType, final Type newType, final String bound) {
    if (bound == null)
      return null;
    final String oldMeasure = DocumentValidator.boundMeasure(oldType);
    final String newMeasure = DocumentValidator.boundMeasure(newType);
    if (newMeasure == null)
      return newType + " has no order and no size";
    if (oldMeasure == null)
      return oldType + " has no order and no size, so the bound has no meaning to carry";
    if (!newMeasure.equals(oldMeasure))
      return "a " + oldMeasure + " on " + oldType + ", a " + newMeasure + " on " + newType;
    return DocumentValidator.unreadableBound(database, newType, bound);
  }

  /**
   * The default of the previous declaration, or null when it produces none. A default that already failed to evaluate under
   * the old type counts as producing none: the new type is then only asked not to throw, which it was asked already.
   */
  private static Object evaluateDefaultOrNull(final Property property) {
    try {
      return property.getDefaultValue();
    } catch (final RuntimeException e) {
      return null;
    }
  }

  /**
   * Refuses the retype when the DEFAULT of the previous declaration does not produce a value of {@code newType}: it would be
   * stored, and every record created without the property would then fail on it (#9645). A default that produced a value
   * before and produces none now lost it in the conversion, which is the same failure without the exception.
   * <p>
   * A numeric default carried onto another numeric type has to keep its value: {@code Type.convert} narrows silently, so a
   * DOUBLE {@code 3.7} would become a LONG {@code 3}, and the bounds are refused for the same reason.
   * <p>
   * The new default is evaluated once. The old one is evaluated too only when the new one produced nothing, or when both types
   * are numeric and the new value has to be compared with it, so a default with a side effect (a sequence's next value) is
   * consumed at most twice by this DDL statement, and usually once.
   */
  private static void requireDefaultReadableAs(final Database database, final String typeName, final Property existing,
      final Property created, final Type newType) {
    final Object definition = created.getDefaultValueDefinition();
    if (definition == null)
      return;

    final String notCarried = "DEFAULT `" + definition + "` (it does not evaluate to a " + newType + ")";
    Object value;
    try {
      value = created.getDefaultValue();
      // a default set through the API as a non-String is returned verbatim: whether the new type can take it is asked here
      if (value != null)
        value = Type.convert(database, value, newType.getDefaultJavaType());
    } catch (final RuntimeException e) {
      throw refuseRetype(typeName, existing, newType, List.of(notCarried), e);
    }
    if (value == null) {
      if (evaluateDefaultOrNull(existing) != null)
        throw refuseRetype(typeName, existing, newType, List.of(notCarried), null);
      return;
    }

    if (value instanceof Number converted && NUMERIC_MEASURE.equals(DocumentValidator.boundMeasure(existing.getType()))
        && evaluateDefaultOrNull(existing) instanceof Number original
        && new BigDecimal(original.toString()).compareTo(new BigDecimal(converted.toString())) != 0)
      throw refuseRetype(typeName, existing, newType,
          List.of("DEFAULT `" + definition + "` (" + original + " becomes " + converted + " as a " + newType + ")"), null);
  }

  private static final String NUMERIC_MEASURE = DocumentValidator.boundMeasure(Type.LONG);

  private static CommandExecutionException refuseRetype(final String typeName, final Property existing, final Type newType,
      final List<String> notCarried, final Throwable cause) {
    return new CommandExecutionException(
        "Cannot change the type of property '" + typeName + "." + existing.getName() + "' from "
            + existing.getType() + " to " + newType + ": " + String.join(", ", notCarried)
            + " cannot be carried over to the new type. Change or remove them with ALTER PROPERTY first", cause);
  }

  private void executeCreateConstraint(final CypherDDLStatement ddl, final Schema schema, final QueryStatistics stats) {
    final String typeName = ddl.getLabelName();
    final String[] propertyNames = ddl.getPropertyNames().toArray(new String[0]);

    // A constraint name is unique across the database (#9646). Checked before anything is created, so a refused statement
    // leaves no type, property or index behind; IF NOT EXISTS makes a taken name a no-op, as in Neo4j. The check and the
    // registration at the end are not atomic with the schema changes in between, like every multi-step DDL here: two
    // concurrent CREATE CONSTRAINT statements with one name on different types can both pass it.
    final String constraintName = ddl.getConstraintName();
    if (CypherConstraintRegistry.findOwner(schema, constraintName) != null) {
      if (ddl.isIfNotExists())
        return;
      throw new SchemaException("Constraint '" + constraintName + "' already exists");
    }
    // Indexes and constraints share one namespace, as in Neo4j: since CREATE INDEX <name> names the index (#9705), a
    // constraint taking the same name would make DROP CONSTRAINT <name> drop the index instead
    if (constraintName != null && schema.existsIndex(constraintName)) {
      if (ddl.isIfNotExists())
        return;
      throw new SchemaException("There already exists an index called '" + constraintName + "'");
    }

    // For TYPED constraints, resolve the target type first so properties are created with the correct type
    final boolean isTyped = ddl.getConstraintKind() == CypherDDLStatement.ConstraintKind.TYPED;
    final DeclaredType explicitPropertyType = isTyped ? mapCypherType(ddl.getTypedName()) : null;

    // A property has one declared type, so a type constraint that disagrees with one already in place is refused, as in
    // Neo4j, rather than silently replacing it (#9706). Checked before anything is created, like the name.
    if (isTyped && schema.existsType(typeName)) {
      final String conflicting = CypherConstraintRegistry.findConflictingTyped(schema.getType(typeName), ddl.getPropertyNames(),
          explicitPropertyType.type(), explicitPropertyType.ofType());
      if (conflicting != null)
        throw new SchemaException("Constraint '" + conflicting + "' already requires another type of " + typeName + "."
            + String.join(", " + typeName + ".", ddl.getPropertyNames()) + ": drop it before requiring IS :: " + ddl.getTypedName());
    }

    autoCreateDDLType(schema, ddl, typeName);

    // An existence constraint over a node that lacks the property is refused, as Neo4j does (#9112): the node would stay
    // in the database unable to take any update, and out of any index on the property. Checked first, so a refusal leaves
    // no property or index behind.
    final CypherDDLStatement.ConstraintKind kind = ddl.getConstraintKind();
    if (kind == CypherDDLStatement.ConstraintKind.NOT_NULL || kind == CypherDDLStatement.ConstraintKind.KEY) {
      final Database database = schema.getEmbedded().getDatabase();
      final DocumentType constrainedType = schema.getType(typeName);
      // already MANDATORY (an IF NOT EXISTS that will be a no-op): nothing new to check
      final String[] toCheck = Arrays.stream(propertyNames).filter(n -> {
        final Property declared = constrainedType.getPropertyIfExists(n);
        return declared == null || !declared.isMandatory();
      }).toArray(String[]::new);
      ExistingRecordsCheck.requireExistence(database, constrainedType, toCheck, true, false);
    }

    // Track properties whose type stays undeclared so the index can fall back to STRING keys
    // without coercing future writes (see issue #4222).
    final Type[] defaultKeyTypes = new Type[propertyNames.length];
    boolean anyUndeclared = false;
    // Tracks whether the TYPED branch below actually created/changed a property; if every property
    // already had the requested type this loop is a no-op and must not count toward the statistics.
    boolean typedChanged = false;
    // The properties this statement declared, and the declarations a type constraint replaced: both are undone when the
    // constraint is dropped (#9706)
    final List<String> declared = new ArrayList<>(propertyNames.length);
    final JSONObject previous = new JSONObject();
    for (int i = 0; i < propertyNames.length; i++) {
      final String propName = propertyNames[i];
      final Property existing = schema.getType(typeName).getPropertyIfExists(propName);
      if (isTyped) {
        if (existing == null) {
          schema.getType(typeName).createProperty(propName, explicitPropertyType.type(), explicitPropertyType.ofType());
          declared.add(propName);
          typedChanged = true;
        } else if (!explicitPropertyType.isSatisfiedBy(existing)) {
          final JSONObject replaced = CypherConstraintRegistry.declaration(existing.getType(), existing.getOfType());
          retypeProperty(schema, typeName, existing, explicitPropertyType);
          previous.put(propName, replaced);
          typedChanged = true;
        }
      } else if (existing == null) {
        final Type inferred = inferPropertyTypeFromExistingData(typeName, propName);
        if (inferred != null) {
          schema.getType(typeName).createProperty(propName, inferred);
          declared.add(propName);
        } else {
          defaultKeyTypes[i] = Type.STRING;
          anyUndeclared = true;
        }
      }
    }

    // What this statement created, recorded under the constraint's name so DROP CONSTRAINT can undo exactly that (#9646)
    String ownedIndex = null;
    final List<String> ownedMandatory = new ArrayList<>(propertyNames.length);
    boolean changed;

    switch (ddl.getConstraintKind()) {
    case UNIQUE: {
      final boolean existedBefore = uniqueIndexExistsOnProperties(schema, typeName, propertyNames);
      final TypeIndexBuilder b = schema.buildTypeIndex(typeName, propertyNames);
      b.withType(Schema.INDEX_TYPE.LSM_TREE);
      b.withUnique(true);
      b.withIgnoreIfExists(ddl.isIfNotExists());
      // A constraint says what the data must satisfy, so a plain index already on the property is upgraded rather than
      // reported as being in the way. Neo4j keeps the range index and the constraint as two objects; ArcadeDB has one
      // index per property set, and a unique one indexes the same keys, so this is the equivalent end state (#5675).
      b.withReplaceIfIncompatible(true);
      if (anyUndeclared)
        b.withDefaultKeyTypesForUndeclaredProperties(defaultKeyTypes);
      final TypeIndex created = b.create();
      changed = !existedBefore;
      if (changed) {
        ownedIndex = created.getName();
        stats.incConstraintsAdded();
      }
      break;
    }

    case NOT_NULL: {
      changed = false;
      for (final String propName : propertyNames) {
        Property prop = schema.getType(typeName).getPropertyIfExists(propName);
        if (prop == null) {
          // NOT NULL implies the property must be tracked, declare it with a generic STRING
          // since we have no value yet to infer from.
          prop = schema.getType(typeName).createProperty(propName, Type.STRING);
          declared.add(propName);
          changed = true;
        }
        if (!prop.isMandatory()) {
          prop.setMandatory(true);
          ownedMandatory.add(propName);
          changed = true;
        }
      }
      if (changed)
        stats.incConstraintsAdded();
      break;
    }

    case KEY: {
      // NODE KEY = unique index + mandatory properties
      final boolean existedBefore = uniqueIndexExistsOnProperties(schema, typeName, propertyNames);
      final TypeIndexBuilder b = schema.buildTypeIndex(typeName, propertyNames);
      b.withType(Schema.INDEX_TYPE.LSM_TREE);
      b.withUnique(true);
      b.withIgnoreIfExists(ddl.isIfNotExists());
      // Same reasoning as the UNIQUE arm above: NODE KEY is a constraint, so it upgrades a plain index on the same
      // properties instead of colliding with it.
      b.withReplaceIfIncompatible(true);
      if (anyUndeclared)
        b.withDefaultKeyTypesForUndeclaredProperties(defaultKeyTypes);
      final TypeIndex created = b.create();
      if (!existedBefore)
        ownedIndex = created.getName();
      for (final String propName : propertyNames) {
        Property prop = schema.getType(typeName).getPropertyIfExists(propName);
        if (prop == null) {
          prop = schema.getType(typeName).createProperty(propName, Type.STRING);
          declared.add(propName);
        }
        if (!prop.isMandatory()) {
          prop.setMandatory(true);
          ownedMandatory.add(propName);
        }
      }
      changed = !existedBefore || !ownedMandatory.isEmpty();
      if (!existedBefore)
        stats.incConstraintsAdded();
      break;
    }

    case TYPED:
      // Property type already set above; for IF NOT EXISTS, silently succeed if property already has the correct type
      if (typedChanged)
        requireStoredValuesConform(schema, typeName, propertyNames, ddl.getTypedName(), declared, previous);
      changed = typedChanged;
      if (typedChanged)
        stats.incConstraintsAdded();
      break;

    default:
      throw new IllegalStateException("Unhandled constraint kind: " + ddl.getConstraintKind());
    }

    // An IF NOT EXISTS that found the constraint already in place is a no-op and claims no name, as in Neo4j. A constraint
    // without a name is recorded under the one it is listed with (#9706), unless it changed nothing - what it requires was
    // there already and is listed as it was - or a constraint requiring the same is recorded already
    final DocumentType constrained = schema.getType(typeName);
    final Type recordedType = isTyped ? explicitPropertyType.type() : null;
    final String recordedOfType = isTyped ? explicitPropertyType.ofType() : null;
    String recordAs = constraintName;
    if (recordAs == null && changed && CypherConstraintRegistry.findEquivalent(constrained, kind, ddl.getPropertyNames(), recordedType,
        recordedOfType) == null)
      recordAs = CypherConstraintRegistry.freeGeneratedName(schema, typeName, kind, ddl.getPropertyNames());
    if (recordAs != null && (changed || !ddl.isIfNotExists()))
      CypherConstraintRegistry.register(constrained, recordAs, kind, propertyNames,
          new CypherConstraintRegistry.Ownership(ownedIndex, ownedMandatory, declared, previous, recordedType, recordedOfType,
              isTyped ? ddl.getTypedName().toString() : null));
  }

  /**
   * Refuses a property type constraint that a stored record already violates, as Neo4j does, and puts back the declarations the
   * statement made. The declaration check of the retype accepts any value the new type can convert ({@code 86400} for a
   * DURATION, {@code '12'} for an INTEGER), but nothing rewrites the stored value: the record would keep a value its own
   * constraint answers false for ({@code n.d IS :: DURATION}). So every stored value is read as Cypher reads it and asked
   * {@code IS :: type}, in one scan of the type.
   */
  private void requireStoredValuesConform(final Schema schema, final String typeName, final String[] propertyNames,
      final CypherTypeName cypherType, final List<String> declared, final JSONObject previous) {
    final Iterator<Record> records = database.iterateType(typeName, true);
    while (records.hasNext()) {
      if (!(records.next() instanceof Document document))
        continue;
      for (final String propertyName : propertyNames) {
        final Object value = TemporalUtil.convertFromStorage(document, propertyName);
        if (value == null || IsTypedExpression.conforms(value, cypherType))
          continue;

        // the refusal is what the caller has to see: an undo step that fails is attached to it, never thrown in its place
        final CommandExecutionException refusal = new CommandExecutionException(
            "Cannot require " + typeName + "." + propertyName + " IS :: " + cypherType + ": record " + document.getIdentity()
                + " holds " + value + " (" + CypherFunctionHelper.cypherTypeName(value) + ")");
        final DocumentType type = schema.getType(typeName);
        for (final String undeclared : declared)
          try {
            if (type.existsProperty(undeclared))
              type.dropProperty(undeclared);
          } catch (final RuntimeException e) {
            refusal.addSuppressed(e);
          }
        for (final String retyped : previous.keySet())
          try {
            final JSONObject old = previous.getJSONObject(retyped);
            retypeProperty(schema, typeName, type.getProperty(retyped),
                new DeclaredType(Type.valueOf(old.getString("type")), old.getString("of", null), true));
          } catch (final RuntimeException e) {
            refusal.addSuppressed(e);
          }
        throw refusal;
      }
    }
  }

  /**
   * The ArcadeDB declaration a property type constraint asks for: the property type and, for a list, its element type.
   *
   * @param replacesOfType true when the Cypher type names the element type (a {@code LIST<T>}, where {@code LIST<ANY>} names
   *                       none), so the declaration takes it instead of carrying the previous one over
   */
  private record DeclaredType(Type type, String ofType, boolean replacesOfType) {
    /**
     * Whether a property already declared satisfies the constraint, so it is left as it is. Every DATETIME precision holds a
     * LOCAL DATETIME: a property declared at a coarser precision keeps it rather than being widened behind its owner's back.
     */
    boolean isSatisfiedBy(final Property existing) {
      final Type existingType = existing.getType();
      final boolean sameType = existingType == type || (type == Type.DATETIME_NANOS && isDatetime(existingType));
      return sameType && (!replacesOfType || Objects.equals(existing.getOfType(), ofType));
    }

    private static boolean isDatetime(final Type type) {
      return type == Type.DATETIME || type == Type.DATETIME_SECOND || type == Type.DATETIME_MICROS || type == Type.DATETIME_NANOS;
    }
  }

  /**
   * Maps the Cypher type of a property type constraint onto an ArcadeDB declaration. A {@code LIST<T>} (or {@code T LIST}) is
   * a LIST of the element type {@code T} maps to (#9709); {@code LIST<ANY>} is a LIST of anything. A {@code NOT NULL} on the
   * type itself is refused, as in Neo4j: existence is a constraint of its own ({@code IS NOT NULL}), and a type constraint
   * that silently dropped it would claim more than it enforces.
   */
  private static DeclaredType mapCypherType(final CypherTypeName cypherType) {
    if (cypherType.nonNull())
      throw new CommandParsingException("A property type constraint cannot require NOT NULL (`IS :: " + cypherType
          + "`): declare the type without it and add an existence constraint (`REQUIRE n.p IS NOT NULL`)");
    if (cypherType.isList()) {
      final String element = cypherType.elementName();
      if (element == null || "ANY".equals(element))
        return new DeclaredType(Type.LIST, null, true);
      return new DeclaredType(Type.LIST, mapScalarCypherType(element, cypherType).name(), true);
    }
    return new DeclaredType(mapScalarCypherType(cypherType.name(), cypherType), null, false);
  }

  /**
   * Maps a canonical Cypher type name ({@link CypherTypeName#canonical}) onto an ArcadeDB Type. The GQL numeric width types map
   * onto ArcadeDB's widths (INT8->BYTE, INT16->SHORT, INT32->INTEGER, INTEGER->LONG, FLOAT32->FLOAT, FLOAT->DOUBLE) so a
   * declared property persists and reloads at that width, which is what makes a declared column satisfy its own IS TYPED.
   * The temporal types map onto the native type that holds each without loss: a LOCAL DATETIME at nanoseconds, a ZONED
   * DATETIME with its zone, a ZONED TIME with its offset and a DURATION as a duration (#9709). They used to be DATETIME (which
   * dropped the zone and the sub-millisecond digits), STRING and LONG, which refused every duration. A POINT is the map
   * {@code point()} produces, as it is stored when no type is declared.
   */
  private static Type mapScalarCypherType(final String name, final CypherTypeName written) {
    return switch (name) {
      case "BOOLEAN" -> Type.BOOLEAN;
      case "STRING" -> Type.STRING;
      case "INT8" -> Type.BYTE;
      case "INT16" -> Type.SHORT;
      case "INT32" -> Type.INTEGER;
      case "INTEGER" -> Type.LONG;
      case "FLOAT32" -> Type.FLOAT;
      case "FLOAT" -> Type.DOUBLE;
      case "DATE" -> Type.DATE;
      case "LOCAL DATETIME" -> Type.DATETIME_NANOS;
      case "ZONED DATETIME" -> Type.ZONED_DATETIME;
      case "LOCAL TIME" -> Type.LOCAL_TIME;
      case "ZONED TIME" -> Type.OFFSET_TIME;
      case "DURATION" -> Type.DURATION;
      case "POINT", "MAP" -> Type.MAP;
      default -> throw new CommandParsingException("Unsupported Cypher type in a property type constraint: " + written);
    };
  }

  /**
   * Executes an admin statement (user management) directly against the security manager.
   */
  private ResultSet executeAdmin(final CypherAdminStatement admin, final String query,
      final Map<String, Object> parameters) {
    // SHOW/TERMINATE TRANSACTIONS act on the server's running statements, not on the security manager: every user may run
    // them, and sees and stops only their own statements unless they administer the server (issue #9689)
    if (admin.isTransactionsCommand())
      return new CypherTransactionsCommand(database, EXPRESSION_EVALUATOR, query, parameters).execute(admin);

    final SecurityManager security = database.getSecurity();
    if (security == null)
      throw new CommandExecutionException("User management commands require server mode");

    // All admin commands except SHOW CURRENT USER require updateSecurity permission
    if (admin.getKind() != CypherAdminStatement.Kind.SHOW_CURRENT_USER)
      database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SECURITY);

    final InternalResultSet resultSet = new InternalResultSet();

    switch (admin.getKind()) {
    case SHOW_USERS: {
      final List<List<Object>> rows = new ArrayList<>();
      for (final String userName : security.getUsers()) {
        final Map<String, Object> info = security.getUserInfo(userName);
        rows.add(Arrays.asList(userName, info != null ? info.get("databases") : Set.of()));
      }
      addShowRows(resultSet, USER_COLUMNS, rows, query, parameters);
      break;
    }
    case SHOW_CURRENT_USER: {
      final String currentUser = database.getCurrentUserName();
      final Map<String, Object> info = currentUser != null ? security.getUserInfo(currentUser) : null;
      addShowRows(resultSet, USER_COLUMNS,
          List.of(Arrays.asList(currentUser != null ? currentUser : "", info != null ? info.get("databases") : Set.of())),
          query, parameters);
      break;
    }
    case CREATE_USER: {
      if (admin.isIfNotExists() && security.existsUser(admin.getUserName()))
        break;
      security.createUser(admin.getUserName(), admin.getPassword());
      break;
    }
    case DROP_USER: {
      if (admin.isIfExists()) {
        security.dropUser(admin.getUserName());
      } else {
        if (!security.existsUser(admin.getUserName()))
          throw new CommandExecutionException("User '" + admin.getUserName() + "' does not exist");
        security.dropUser(admin.getUserName());
      }
      break;
    }
    case ALTER_USER: {
      if (!security.existsUser(admin.getUserName()))
        throw new CommandExecutionException("User '" + admin.getUserName() + "' does not exist");
      security.setUserPassword(admin.getUserName(), admin.getPassword());
      break;
    }
    }

    return resultSet;
  }

  /**
   * Adds the rows of a {@code SHOW} command to its result set, with the command's own {@code YIELD}/{@code WHERE}
   * tail applied to them.
   * <p>
   * These commands are answered from the security manager rather than from a query plan, so the tail - which the
   * grammar accepts on every {@code SHOW} - had nothing applying it and was silently dropped, the same gap issue
   * #7946 reports for {@code SHOW DATABASES} on the Bolt wire. It is applied here by {@link ShowCommandTail}, so a
   * predicate means the same thing on either.
   */
  private void addShowRows(final InternalResultSet resultSet, final List<String> columns, final List<List<Object>> rows,
      final String query, final Map<String, Object> parameters) {
    final ShowCommandTail.Table table = ShowCommandTail.apply(database, query, columns, rows, parameters);

    for (final List<Object> row : table.rows()) {
      final ResultInternal result = new ResultInternal();
      for (int i = 0; i < table.fields().size(); i++)
        result.setProperty(table.fields().get(i), i < row.size() ? row.get(i) : null);
      resultSet.add(result);
    }
  }

  /**
   * Executes a transaction control statement (START TRANSACTION/COMMIT/ROLLBACK) directly against the
   * database transaction API. The opened transaction outlives this command so subsequent write commands
   * reuse it and only COMMIT/ROLLBACK finalize it, matching the SQL BEGIN/COMMIT/ROLLBACK semantics.
   * <p>
   * Note on nesting (same behavior as SQL BEGIN): a second START TRANSACTION while one is already active
   * does not error - it opens a <em>nested</em> transaction. A following COMMIT/ROLLBACK then finalizes
   * only that inner transaction, leaving the outer one active. Callers that did not intend to nest must
   * balance each START TRANSACTION with its own COMMIT/ROLLBACK.
   */
  private ResultSet executeTransaction(final CypherTransactionStatement txn) {
    // The transaction this statement drives has to be the replicated one - see executionDatabase(). COMMIT is the
    // only kind where the two instances differ (begin/rollback delegate to the inner database on the Raft wrapper
    // too), but all three resolve it so the statement never straddles both.
    final DatabaseInternal txDatabase = database.getWrappedDatabaseInstance();

    final InternalResultSet resultSet = new InternalResultSet();
    final ResultInternal result = new ResultInternal(txDatabase);

    switch (txn.getKind()) {
    case BEGIN:
      final String isolationLevel = txn.getIsolationLevel();
      if (isolationLevel != null) {
        final Database.TRANSACTION_ISOLATION_LEVEL level;
        try {
          level = Database.TRANSACTION_ISOLATION_LEVEL.valueOf(isolationLevel.toUpperCase(Locale.ENGLISH));
        } catch (final IllegalArgumentException e) {
          final StringJoiner validLevels = new StringJoiner(", ");
          for (final Database.TRANSACTION_ISOLATION_LEVEL l : Database.TRANSACTION_ISOLATION_LEVEL.values())
            validLevels.add(l.name());
          throw new CommandParsingException(
              "Invalid transaction isolation level '" + isolationLevel + "'. Valid values: " + validLevels);
        }
        txDatabase.begin(level);
      } else
        txDatabase.begin();
      result.setProperty("operation", "begin");
      break;
    case COMMIT:
      if (!txDatabase.isTransactionActive())
        throw new CommandExecutionException("No active transaction to COMMIT (issue a START TRANSACTION first)");
      txDatabase.commit();
      result.setProperty("operation", "commit");
      break;
    case ROLLBACK:
      // rollback() is idempotent: with no active transaction it is a no-op, so ROLLBACK is lenient.
      txDatabase.rollback();
      result.setProperty("operation", "rollback");
      break;
    default:
      throw new IllegalStateException("Unhandled transaction kind: " + txn.getKind());
    }

    resultSet.add(result);
    return resultSet;
  }

  /**
   * Returns the {@link QuerySession} attached to this thread's context by the server (HTTP/Bolt), or
   * {@code null} in embedded use. Resolved once per command/query and threaded to both the parameter merge
   * and {@link #executeSession} so the thread-context is not looked up twice. Session parameters become
   * visible to a Cypher command or query as {@code $name} only on this OpenCypher engine path, since they
   * are a GQL concept.
   */
  private QuerySession currentQuerySession() {
    final DatabaseContext.DatabaseContextTL ctx = DatabaseContext.INSTANCE.getContextIfExists(database.getDatabasePath());
    return ctx != null ? ctx.getQuerySession() : null;
  }

  /**
   * Executes a session management statement (SESSION SET/RESET/CLOSE) against the {@link QuerySession}
   * attached to the current thread context by the server. In embedded use (no server session) there is
   * nothing attached, so the statement reports an actionable error rather than silently doing nothing.
   */
  private ResultSet executeSession(final CypherSessionStatement stmt, final QuerySession session,
      final Map<String, Object> parameters) {
    if (session == null)
      throw new CommandExecutionException(
          "SESSION statements require a server session (set the 'arcadedb-session-id' HTTP header); not available in embedded mode");

    final InternalResultSet resultSet = new InternalResultSet();
    final ResultInternal result = new ResultInternal(database);

    switch (stmt.getKind()) {
    case SET:
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);
      // 'parameters' already carries the session parameters (merged once in command()), so a value can
      // reference an earlier session parameter or a request parameter.
      context.setInputParameters(parameters);
      final Object value = EXPRESSION_EVALUATOR.evaluate(stmt.getValueExpression(), new ResultInternal(database), context);
      session.setParameter(stmt.getParameterName(), value);
      result.setProperty("operation", "set");
      result.setProperty("name", stmt.getParameterName());
      result.setProperty("value", value);
      break;
    case RESET:
      session.reset();
      result.setProperty("operation", "reset");
      break;
    case CLOSE:
      // Depending on the owner, close() may roll back the session's transaction (HTTP). It runs while this
      // command is still in flight, so any further DB operation issued in the same request/session after
      // SESSION CLOSE would act on an already-finalized transaction.
      session.close();
      result.setProperty("operation", "close");
      break;
    default:
      throw new IllegalStateException("Unhandled session kind: " + stmt.getKind());
    }

    resultSet.add(result);
    return resultSet;
  }

  /**
   * Drops a constraint by the name {@code SHOW CONSTRAINTS} lists it under, undoing what its {@code CREATE CONSTRAINT}
   * created (#9646, #9706): the name the statement gave it, or the one generated for it when it gave none. The name of a
   * unique index drops the constraint that created it, or the index alone when no recorded constraint did. A requirement
   * declared outside Cypher (a MANDATORY flag, a declared type) is listed and dropped under its generated name too. Any other
   * name raises unless {@code IF EXISTS} is given.
   */
  private void executeDropConstraint(final CypherDDLStatement ddl, final Schema schema, final QueryStatistics stats) {
    final String constraintName = ddl.getConstraintName();
    if (CypherConstraintRegistry.drop(schema, constraintName, this::retypeForConstraintDrop)) {
      stats.incConstraintsRemoved();
      return;
    }
    // A unique index only: a plain one is no constraint, and since CREATE INDEX <name> names the index (#9705) its name
    // would otherwise drop it through DROP CONSTRAINT
    if (schema.existsIndex(constraintName) && schema.getIndexByName(constraintName).isUnique()) {
      final String owner = CypherConstraintRegistry.findIndexOwner(schema, constraintName);
      if (owner != null)
        CypherConstraintRegistry.drop(schema, owner, this::retypeForConstraintDrop);
      else
        schema.dropIndex(constraintName);
      stats.incConstraintsRemoved();
      return;
    }
    if (CypherSchemaCommands.dropUnrecordedConstraint(schema, constraintName)) {
      stats.incConstraintsRemoved();
      return;
    }
    if (!ddl.isIfExists())
      throw new SchemaException("Constraint '" + constraintName + "' does not exist");
  }

  /** Puts back the declaration a type constraint replaced, through the same checks as the retype that replaced it. */
  private void retypeForConstraintDrop(final DocumentType type, final Property property, final Type newType, final String newOfType) {
    retypeProperty(type.getSchema(), type.getName(), property, new DeclaredType(newType, newOfType, true));
  }

  /**
   * Creates the index under the name the statement gives it, or {@code Type[properties]} when it gives none (#9705). Names
   * are unique across indexes and constraints, as in Neo4j: a taken name raises, and with {@code IF NOT EXISTS} it is a
   * no-op, like an equivalent index under another name.
   */
  private void executeCreateIndex(final CypherDDLStatement ddl, final Schema schema, final QueryStatistics stats) {
    final String typeName = ddl.getLabelName();
    final String[] propertyNames = ddl.getPropertyNames().toArray(new String[0]);
    final String indexName = ddl.getConstraintName();

    // Checked before anything is created, so a refused statement leaves no type or property behind
    if (indexName != null) {
      final boolean taken = schema.existsIndex(indexName);
      if (taken || CypherConstraintRegistry.findOwner(schema, indexName) != null) {
        if (ddl.isIfNotExists())
          return;
        throw new SchemaException(
            "There already exists " + (taken ? "an index" : "a constraint") + " called '" + indexName + "'");
      }
    }

    autoCreateDDLType(schema, ddl, typeName);

    // Cypher properties are dynamically typed and travel over Bolt with their actual Java type
    // (e.g. {@code Long} for numeric literals). Hard-coding {@link Type#STRING} when the property
    // is missing from the schema used to coerce every future write to a {@link String}, which
    // breaks Cypher's strict-typed equality on follow-up MATCH (issue #4222). We instead:
    //   1. Try to infer the property type from any existing record on the type, so that the
    //      common pattern "CREATE node; CREATE INDEX" preserves the existing data's type.
    //   2. If no record carries the property, keep the property undeclared and tell the index
    //      builder to fall back to STRING serialisation for its keys. Writes then preserve the
    //      original Java type and downstream Cypher comparisons work correctly.
    final DocumentType type = schema.getType(typeName);
    final Type[] defaultKeyTypes = new Type[propertyNames.length];
    final List<String> declaredHere = new ArrayList<>();
    boolean anyUndeclared = false;
    for (int i = 0; i < propertyNames.length; i++) {
      final String propName = propertyNames[i];
      if (type.getPolymorphicPropertyIfExists(propName) != null)
        continue;
      final Type inferred = inferPropertyTypeFromExistingData(typeName, propName);
      if (inferred != null) {
        type.createProperty(propName, inferred);
        declaredHere.add(propName);
      } else {
        defaultKeyTypes[i] = Type.STRING;
        anyUndeclared = true;
      }
    }

    final boolean existedBefore = indexExistsOnProperties(schema, typeName, propertyNames);
    final TypeIndexBuilder builder = schema.buildTypeIndex(typeName, propertyNames);
    builder.withType(Schema.INDEX_TYPE.LSM_TREE);
    builder.withUnique(false);
    builder.withIgnoreIfExists(ddl.isIfNotExists());
    // An equivalent index under another name satisfies IF NOT EXISTS, as in Neo4j: the name is not asked for then, or the
    // builder would refuse the request over the name alone (#6921)
    if (indexName != null && !(existedBefore && ddl.isIfNotExists()))
      builder.withIndexName(indexName);
    if (anyUndeclared)
      builder.withDefaultKeyTypesForUndeclaredProperties(defaultKeyTypes);
    try {
      builder.create();
    } catch (final RuntimeException e) {
      // The declarations above are part of this statement: a build that fails must not leave the type with properties
      // nobody asked for, which coerce every later write (issue #8999)
      for (final String declared : declaredHere)
        try {
          if (type.existsProperty(declared))
            type.dropProperty(declared);
        } catch (final RuntimeException dropFailure) {
          e.addSuppressed(dropFailure);
        }
      throw e;
    }
    if (!existedBefore)
      stats.incIndexesAdded();
  }

  /**
   * Walks the type's records (polymorphic) and returns the {@link Type} that matches the Java class of every non-null value
   * of {@code propertyName}. Returns {@code null} if no record sets the property, or when the values do not share one type
   * (in which case the property stays undeclared and the index falls back to the canonical STRING keys). Used by Cypher DDL to
   * preserve numeric properties across {@code CREATE INDEX} on an already-populated type (issue #4222).
   * <p>
   * The whole type is read, not a prefix of it: the first value alone used to decide, so {@code 3} followed by {@code 2.5}
   * declared the property {@code INTEGER}, narrowed the float to {@code 2} in every index key, and made range predicates lose
   * the record (issue #8999). Integers of different widths settle on the widest, a {@code FLOAT} and a {@code DOUBLE} on
   * {@code DOUBLE}; integers mixed with floating point numbers, a {@code DECIMAL} with anything else, or a number with a string
   * have no single type that holds every value, so there is none. The scan stops at the first value that disagrees.
   * <p>
   * Temporal values are the one case where the Java class alone is not enough:
   * {@link Type#getTypeByClass} maps every temporal class onto the millisecond {@link Type#DATETIME},
   * while an <em>undeclared</em> temporal property is serialized at the precision of each value
   * (see {@code BinaryTypes.getTypeFromValue}). Declaring {@code DATETIME} would therefore silently
   * truncate to milliseconds every write made after the index was created, on a property whose
   * existing rows carry microseconds or nanoseconds (issue #7164). So when the kind is temporal the scan declares the widest
   * precision the data actually holds. Milliseconds stay the floor - a value that
   * happens to land on a whole second must not declare the property {@code DATETIME_SECOND} and
   * truncate the millisecond writes ArcadeDB has always accepted by default. The sweep stops as soon as it sees
   * nanoseconds (nothing can widen it further).
   * <p>
   * The scan is unconditional, so indexing a column on a very large type costs one extra sequential scan on top of the index
   * build - deliberately, because the alternative is silently narrowing or truncating values that are already persisted. If a
   * slow {@code CREATE INDEX} on a huge type ever needs to be cut down, this is the place, and anything that reintroduces a
   * bound reintroduces the narrowing of issues #7164 and #8999 with it.
   */
  private Type inferPropertyTypeFromExistingData(final String typeName, final String propertyName) {
    try {
      final Iterator<Record> it = database.iterateType(typeName, true);
      ChronoUnit temporalPrecision = null;
      Type settled = null;
      while (it.hasNext()) {
        final Record record = it.next();
        if (!(record instanceof Document doc))
          continue;
        // NO has() GUARD: get() ALREADY ANSWERS null FOR AN ABSENT PROPERTY, AND EACH OF THE TWO MAKES ITS OWN PASS
        // OVER THE RECORD BUFFER - ASKING TWICE DOUBLED THE COST OF EVERY RECORD IN THE UNCAPPED SWEEP
        final Object value = doc.get(propertyName);
        if (value == null)
          continue;

        final ChronoUnit precision = DateUtils.getPrecisionFromValue(value);

        if (precision != null) {
          if (settled != null)
            // a temporal value next to a value of another kind
            return null;
          // MILLIS IS THE FLOOR: NEVER NARROW BELOW THE DEFAULT DATETIME PRECISION
          final ChronoUnit valuePrecision = precision.compareTo(ChronoUnit.MILLIS) < 0 ? precision : ChronoUnit.MILLIS;
          if (temporalPrecision == null || valuePrecision.compareTo(temporalPrecision) < 0)
            temporalPrecision = valuePrecision;
          if (temporalPrecision == ChronoUnit.NANOS)
            break;
          continue;
        }

        if (temporalPrecision != null)
          // a value that is no temporal next to temporal ones reads as an unreadable date, which an index keys under null
          // (issue #8090): it neither resets the kind nor narrows the precision. A temporal value that comes AFTER a value of
          // another kind is not skipped the same way: the first kind settled the type, so there is no DATETIME to fall back to
          continue;

        final Type valueType;
        try {
          valueType = Type.getTypeByClass(value.getClass());
        } catch (final IllegalArgumentException ignored) {
          // Unknown Java class for the value; leave the property undeclared and let the index
          // fall back to STRING serialisation.
          return null;
        }

        if (valueType == null)
          // a class no type is registered for (a LocalDate, say): the values that follow must not settle the declaration
          return null;

        if (settled == null) {
          settled = valueType;
          continue;
        }
        if (settled == valueType)
          continue;

        if (isIntegral(settled) && isIntegral(valueType))
          settled = widerIntegral(settled, valueType);
        else if (isBinaryFloatingPoint(settled) && isBinaryFloatingPoint(valueType))
          settled = Type.DOUBLE;
        else
          // integers next to floating point numbers, or any other pair of kinds: no single type holds both, and picking
          // one narrows or rejects the other (issue #8999)
          return null;
      }

      if (temporalPrecision != null)
        return Type.getByBinaryType(DateUtils.getBestBinaryTypeForPrecision(temporalPrecision));
      return settled;
    } catch (final Exception ignored) {
      // If iteration fails for any reason (e.g. schema in transient state) just skip inference.
    }
    return null;
  }

  private static boolean isIntegral(final Type type) {
    return type == Type.BYTE || type == Type.SHORT || type == Type.INTEGER || type == Type.LONG;
  }

  private static boolean isBinaryFloatingPoint(final Type type) {
    return type == Type.FLOAT || type == Type.DOUBLE;
  }

  private static Type widerIntegral(final Type a, final Type b) {
    return integralRank(a) >= integralRank(b) ? a : b;
  }

  private static int integralRank(final Type type) {
    return switch (type) {
      case BYTE -> 0;
      case SHORT -> 1;
      case INTEGER -> 2;
      case LONG -> 3;
      default -> throw new IllegalArgumentException("Not an integral type: " + type);
    };
  }

  /**
   * Drops the index by its name. An unknown name raises unless {@code IF EXISTS} is given: {@code Schema.dropIndex} returns
   * silently on it, which reported a removal that never happened (#9705). The index a constraint created is refused, as in
   * Neo4j: dropping it alone would leave the constraint listed with nothing enforcing it.
   */
  private void executeDropIndex(final CypherDDLStatement ddl, final Schema schema, final QueryStatistics stats) {
    final String indexName = ddl.getConstraintName();
    if (!schema.existsIndex(indexName)) {
      if (ddl.isIfExists())
        return;
      throw new SchemaException("Index '" + indexName + "' does not exist");
    }
    final String owner = CypherConstraintRegistry.findIndexOwner(schema, indexName);
    if (owner != null)
      throw new SchemaException(
          "Index '" + indexName + "' belongs to constraint '" + owner + "': use DROP CONSTRAINT " + owner + " to drop it");
    schema.dropIndex(indexName);
    stats.incIndexesRemoved();
  }

  /**
   * Get the shared ExpressionEvaluator instance.
   * This is used by other components that need access to the evaluator.
   *
   * @return the shared expression evaluator
   */
  public static ExpressionEvaluator getExpressionEvaluator() {
    return EXPRESSION_EVALUATOR;
  }

  /**
   * Converts positional parameters (Object...) to named parameters map.
   * Uses parameter names like $0, $1, $2, etc.
   *
   * @param parameters positional parameters
   * @return named parameters map
   */
  private Map<String, Object> convertPositionalParameters(final Object... parameters) {
    final Map<String, Object> namedParams = new HashMap<>();
    if (parameters != null && parameters.length > 0) {
      // Cypher uses named parameters, so treat varargs as alternating key-value pairs
      // e.g., database.query("cypher", "RETURN $x", "x", 10) → {"x": 10}
      if (parameters.length % 2 != 0) {
        throw new IllegalArgumentException(
            "Parameters must be provided as key-value pairs (e.g., \"paramName\", paramValue). Found " +
                parameters.length + " arguments.");
      }
      for (int i = 0; i < parameters.length; i += 2) {
        final Object key = parameters[i];
        if (!(key instanceof String)) {
          throw new IllegalArgumentException(
              "Parameter name at index " + i + " must be a String, but got: " +
                  (key != null ? key.getClass().getName() : "null"));
        }
        namedParams.put((String) key, parameters[i + 1]);
      }
    }
    return namedParams;
  }
}
