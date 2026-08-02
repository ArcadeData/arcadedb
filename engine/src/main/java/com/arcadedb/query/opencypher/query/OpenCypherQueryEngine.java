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
import com.arcadedb.database.Record;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParameterMissingException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.QueryNotIdempotentException;
import com.arcadedb.query.OperationType;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.QuerySession;
import com.arcadedb.query.opencypher.ast.CypherAdminStatement;
import com.arcadedb.query.opencypher.ast.CypherDDLStatement;
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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.TypeIndexBuilder;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityManager;
import com.arcadedb.function.sql.DefaultSQLFunctionFactory;
import com.arcadedb.index.Index;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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
      // Use statement cache to avoid re-parsing
      final CypherStatement statement = database.getCypherStatementCache().get(query);

      return new AnalyzedQuery() {
        @Override
        public boolean isIdempotent() {
          return statement.isReadOnly();
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
          if (statement.getSetClause() != null && !statement.getSetClause().isEmpty())
            ops.add(OperationType.UPDATE);
          if (!statement.getRemoveClauses().isEmpty())
            ops.add(OperationType.UPDATE);
          return ops.isEmpty() ? Set.of(OperationType.CREATE, OperationType.UPDATE, OperationType.DELETE) : Set.copyOf(ops);
        }
      };
    } catch (final Exception e) {
      throw new CommandParsingException("Error analyzing Cypher query: " + query, e);
    }
  }

  @Override
  public ResultSet query(final String query, final ContextConfiguration configuration, final Map<String, Object> parameters) {
    try {
      // Check for EXPLAIN or PROFILE prefix
      String actualQuery = query.trim();
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

      // Also check for $profileExecution parameter (set by HTTP handler when Studio sends profileExecution: "detailed")
      if (!profile && parameters != null && Boolean.TRUE.equals(parameters.get("$profileExecution")))
        profile = true;

      // Use statement cache to avoid re-parsing. Carries the parameter names the query references, so the
      // check below costs no extra lookup.
      final ParsedQuery parsed = database.getCypherStatementCache().getParsed(actualQuery);
      final CypherStatement statement = parsed.statement();

      // Make any session parameters (SESSION SET) visible to this query as $name.
      final QuerySession session = currentQuerySession();
      final Map<String, Object> effectiveParameters = QuerySession.mergeParameters(
          session != null ? session.getParameters() : null, parameters);

      // EXPLAIN never executes the query, so it is also the one mode that tolerates unbound parameters:
      // Neo4j reports them as a notification there instead of failing, which is what makes EXPLAIN usable
      // for inspecting a plan before the values are known.
      if (!explain)
        checkParametersAreBound(parsed.parameters(), effectiveParameters);

      // EXPLAIN never executes the underlying query, so the idempotency rule does not apply.
      // PROFILE is treated as idempotent at the wrapper level to match SQL parity
      // (see ProfileStatement/ExplainStatement in the SQL parser, both return isIdempotent()==true).
      // Without this bypass, EXPLAIN/PROFILE of a CREATE/MERGE/DELETE/SET fails the read-only
      // gate even though the user explicitly asked for plan inspection rather than execution
      // (issue #4366).
      if (!explain && !profile && !statement.isReadOnly())
        throw new QueryNotIdempotentException("Query '" + query + "' is not idempotent");

      return execute(actualQuery, statement, configuration, effectiveParameters, explain, profile);
    } catch (final QueryNotIdempotentException | CommandExecutionException | CommandParsingException | SecurityException e) {
      throw e;
    } catch (final Exception e) {
      throw new CommandExecutionException("Error executing Cypher query: " + query, e);
    }
  }

  @Override
  public ResultSet query(final String query, final ContextConfiguration configuration, final Object... parameters) {
    return query(query, configuration, convertPositionalParameters(parameters));
  }

  @Override
  public ResultSet command(final String query, final ContextConfiguration configuration, final Map<String, Object> parameters) {
    try {
      // Check for EXPLAIN or PROFILE prefix
      String actualQuery = query.trim();
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

      // Also check for $profileExecution parameter (set by HTTP handler when Studio sends profileExecution: "detailed")
      if (!profile && parameters != null && Boolean.TRUE.equals(parameters.get("$profileExecution")))
        profile = true;

      // Use statement cache to avoid re-parsing. Carries the parameter names the query references, so the
      // check below costs no extra lookup.
      final ParsedQuery parsed = database.getCypherStatementCache().getParsed(actualQuery);
      final CypherStatement statement = parsed.statement();

      // Make any session parameters (SESSION SET) visible to this command as $name. Resolve the session
      // once and thread it to the merge, the parameter check and executeSession (avoids repeated
      // thread-context lookups). Done before the direct-dispatch statements below so that every path
      // through this method - DDL, admin, transaction control, session and the planner pipeline - checks
      // the parameters the same way.
      final QuerySession session = currentQuerySession();
      final Map<String, Object> effectiveParameters = QuerySession.mergeParameters(
          session != null ? session.getParameters() : null, parameters);

      // See the EXPLAIN note in query(): a plan can be inspected before the values are known.
      if (!explain)
        checkParametersAreBound(parsed.parameters(), effectiveParameters);

      // DDL statements (constraints) are executed directly without the planner pipeline
      if (statement instanceof CypherDDLStatement)
        return executeDDL((CypherDDLStatement) statement);

      // Admin statements (user management) are executed directly against the security manager
      if (statement instanceof CypherAdminStatement)
        return executeAdmin((CypherAdminStatement) statement);

      // Transaction control statements (START TRANSACTION/COMMIT/ROLLBACK) are executed directly
      // against the database transaction API, bypassing the planner's auto-commit pipeline.
      if (statement instanceof CypherTransactionStatement)
        return executeTransaction((CypherTransactionStatement) statement);

      // Session management statements (SESSION SET/RESET/CLOSE) operate on the server session bound to
      // the current thread; executed directly, no planner pipeline.
      if (statement instanceof CypherSessionStatement)
        return executeSession((CypherSessionStatement) statement, session, effectiveParameters);

      return execute(actualQuery, statement, configuration, effectiveParameters, explain, profile);
    } catch (final CommandExecutionException | CommandParsingException | SecurityException e) {
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
   * Executes a parsed Cypher statement.
   *
   * @param queryString   the original query string (for plan cache key)
   * @param statement     the parsed Cypher statement
   * @param configuration context configuration
   * @param parameters    query parameters
   * @param explain       if true, return EXPLAIN output instead of executing
   * @param profile       if true, execute with profiling and return metrics
   * @return result set
   */
  private ResultSet execute(final String queryString, final CypherStatement statement, final ContextConfiguration configuration,
      final Map<String, Object> parameters, final boolean explain, final boolean profile) {
    // Try to get cached physical plan first (saves optimization time: 200-500ms)
    final CypherExecutionPlan plan;
    final DatabaseInternal execDb = executionDatabase(statement);

    if (!explain && !profile) {
      // Only use plan cache for normal execution (not explain/profile)
      final PhysicalPlan physicalPlan = database.getCypherPlanCache().get(queryString);
      if (physicalPlan != null) {
        // Reuse cached physical plan (avoids expensive statistics collection and optimization)
        plan = new CypherExecutionPlan(
            execDb, statement, parameters, configuration, physicalPlan, EXPRESSION_EVALUATOR);
      } else {
        // Create new plan from scratch and cache it
        final CypherExecutionPlanner planner = new CypherExecutionPlanner(execDb, statement, parameters,
            EXPRESSION_EVALUATOR);
        plan = planner.createExecutionPlan(configuration);

        // Cache the physical plan for future use
        if (plan.getPhysicalPlan() != null)
          database.getCypherPlanCache().put(queryString, plan.getPhysicalPlan());
      }
    } else {
      // explain/profile mode: always create new plan without caching
      final CypherExecutionPlanner planner = new CypherExecutionPlanner(execDb, statement, parameters,
          EXPRESSION_EVALUATOR);
      plan = planner.createExecutionPlan(configuration);
    }

    if (explain)
      return plan.explain();
    if (profile)
      return plan.profile();
    return plan.execute();
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
      executeCreateConstraint(ddl, schema, stats);
      break;
    case DROP_CONSTRAINT:
      executeDropConstraint(ddl, schema, stats);
      break;
    case CREATE_INDEX:
      executeCreateIndex(ddl, schema, stats);
      break;
    case DROP_INDEX:
      executeDropIndex(ddl, schema, stats);
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

  private void executeCreateConstraint(final CypherDDLStatement ddl, final Schema schema, final QueryStatistics stats) {
    final String typeName = ddl.getLabelName();
    final String[] propertyNames = ddl.getPropertyNames().toArray(new String[0]);

    // Auto-create the type if it doesn't exist (Cypher does not require types to be pre-declared)
    if (!schema.existsType(typeName)) {
      if (ddl.isForRelationship())
        schema.getOrCreateEdgeType(typeName);
      else
        schema.getOrCreateVertexType(typeName);
    }

    // For TYPED constraints, resolve the target type first so properties are created with the correct type
    final boolean isTyped = ddl.getConstraintKind() == CypherDDLStatement.ConstraintKind.TYPED;
    final Type explicitPropertyType = isTyped ? mapCypherType(ddl.getTypedName()) : null;

    // Track properties whose type stays undeclared so the index can fall back to STRING keys
    // without coercing future writes (see issue #4222).
    final Type[] defaultKeyTypes = new Type[propertyNames.length];
    boolean anyUndeclared = false;
    // Tracks whether the TYPED branch below actually created/changed a property; if every property
    // already had the requested type this loop is a no-op and must not count toward the statistics.
    boolean typedChanged = false;
    for (int i = 0; i < propertyNames.length; i++) {
      final String propName = propertyNames[i];
      final Property existing = schema.getType(typeName).getPropertyIfExists(propName);
      if (isTyped) {
        if (existing == null) {
          schema.getType(typeName).createProperty(propName, explicitPropertyType);
          typedChanged = true;
        } else if (existing.getType() != explicitPropertyType) {
          schema.getType(typeName).dropProperty(propName);
          schema.getType(typeName).createProperty(propName, explicitPropertyType);
          typedChanged = true;
        }
      } else if (existing == null) {
        final Type inferred = inferPropertyTypeFromExistingData(typeName, propName);
        if (inferred != null) {
          schema.getType(typeName).createProperty(propName, inferred);
        } else {
          defaultKeyTypes[i] = Type.STRING;
          anyUndeclared = true;
        }
      }
    }

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
      b.create();
      if (!existedBefore)
        stats.incConstraintsAdded();
      break;
    }

    case NOT_NULL: {
      boolean changed = false;
      for (final String propName : propertyNames) {
        Property prop = schema.getType(typeName).getPropertyIfExists(propName);
        if (prop == null) {
          // NOT NULL implies the property must be tracked, declare it with a generic STRING
          // since we have no value yet to infer from.
          prop = schema.getType(typeName).createProperty(propName, Type.STRING);
          changed = true;
        }
        if (!prop.isMandatory()) {
          prop.setMandatory(true);
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
      b.create();
      for (final String propName : propertyNames) {
        Property prop = schema.getType(typeName).getPropertyIfExists(propName);
        if (prop == null)
          prop = schema.getType(typeName).createProperty(propName, Type.STRING);
        prop.setMandatory(true);
      }
      if (!existedBefore)
        stats.incConstraintsAdded();
      break;
    }

    case TYPED:
      // Property type already set above; for IF NOT EXISTS, silently succeed if property already has the correct type
      if (typedChanged)
        stats.incConstraintsAdded();
      break;
    }
  }

  /**
   * Maps a Cypher type name (from IS TYPED constraints) to an ArcadeDB Type. The GQL numeric width types map
   * onto ArcadeDB's existing widths (INT8->BYTE, INT16->SHORT, INT32->INTEGER, INT64->LONG, FLOAT32->FLOAT,
   * FLOAT64->DOUBLE) so a declared property persists and reloads at that width. The mapping mirrors the read
   * side ({@link com.arcadedb.query.opencypher.ast.IsTypedExpression}): INTEGER is the 64-bit generic, INT
   * aliases INT32. Keeping the two sides aligned is what makes a declared column satisfy its own IS TYPED.
   */
  private static Type mapCypherType(final String cypherType) {
    switch (cypherType) {
    case "BOOLEAN":
    case "BOOL":
      return Type.BOOLEAN;
    case "STRING":
    case "VARCHAR":
      return Type.STRING;
    case "INT8":
    case "INTEGER8":
      return Type.BYTE;
    case "INT16":
    case "INTEGER16":
      return Type.SHORT;
    case "INT":
    case "INT32":
    case "INTEGER32":
      return Type.INTEGER;
    case "INTEGER":
    case "SIGNED INTEGER":
    case "INTEGER64":
    case "INT64":
      return Type.LONG;
    case "FLOAT32":
      return Type.FLOAT;
    case "FLOAT":
    case "FLOAT64":
      return Type.DOUBLE;
    case "DATE":
      return Type.DATE;
    case "LOCAL DATETIME":
    case "TIMESTAMP WITHOUT TIMEZONE":
    case "TIMESTAMP WITHOUT TIME ZONE":
      return Type.DATETIME;
    case "ZONED DATETIME":
    case "TIMESTAMP WITH TIMEZONE":
    case "TIMESTAMP WITH TIME ZONE":
      return Type.DATETIME;
    case "LOCAL TIME":
    case "TIME WITHOUT TIMEZONE":
    case "TIME WITHOUT TIME ZONE":
      return Type.STRING;
    case "ZONED TIME":
    case "TIME WITH TIMEZONE":
    case "TIME WITH TIME ZONE":
      return Type.STRING;
    case "DURATION":
      return Type.LONG;
    case "LIST":
      return Type.LIST;
    case "MAP":
      return Type.MAP;
    case "POINT":
      return Type.STRING;
    default:
      throw new CommandParsingException("Unsupported Cypher type in IS TYPED constraint: " + cypherType);
    }
  }

  /**
   * Executes an admin statement (user management) directly against the security manager.
   */
  private ResultSet executeAdmin(final CypherAdminStatement admin) {
    final SecurityManager security = database.getSecurity();
    if (security == null)
      throw new CommandExecutionException("User management commands require server mode");

    // All admin commands except SHOW CURRENT USER require updateSecurity permission
    if (admin.getKind() != CypherAdminStatement.Kind.SHOW_CURRENT_USER)
      database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SECURITY);

    final InternalResultSet resultSet = new InternalResultSet();

    switch (admin.getKind()) {
    case SHOW_USERS: {
      final Set<String> userNames = security.getUsers();
      for (final String userName : userNames) {
        final Map<String, Object> info = security.getUserInfo(userName);
        final ResultInternal result = new ResultInternal();
        result.setProperty("user", userName);
        result.setProperty("databases", info != null ? info.get("databases") : Set.of());
        resultSet.add(result);
      }
      break;
    }
    case SHOW_CURRENT_USER: {
      final String currentUser = database.getCurrentUserName();
      final ResultInternal result = new ResultInternal();
      result.setProperty("user", currentUser != null ? currentUser : "");
      if (currentUser != null) {
        final Map<String, Object> info = security.getUserInfo(currentUser);
        result.setProperty("databases", info != null ? info.get("databases") : Set.of());
      } else
        result.setProperty("databases", Set.of());
      resultSet.add(result);
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

  private void executeDropConstraint(final CypherDDLStatement ddl, final Schema schema, final QueryStatistics stats) {
    final String constraintName = ddl.getConstraintName();
    if (ddl.isIfExists()) {
      if (schema.existsIndex(constraintName)) {
        schema.dropIndex(constraintName);
        stats.incConstraintsRemoved();
      }
    } else {
      schema.dropIndex(constraintName);
      stats.incConstraintsRemoved();
    }
  }

  private void executeCreateIndex(final CypherDDLStatement ddl, final Schema schema, final QueryStatistics stats) {
    final String typeName = ddl.getLabelName();
    final String[] propertyNames = ddl.getPropertyNames().toArray(new String[0]);

    // Auto-create the type if it doesn't exist (Cypher does not require types to be pre-declared)
    if (!schema.existsType(typeName)) {
      if (ddl.isForRelationship())
        schema.getOrCreateEdgeType(typeName);
      else
        schema.getOrCreateVertexType(typeName);
    }

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
    boolean anyUndeclared = false;
    for (int i = 0; i < propertyNames.length; i++) {
      final String propName = propertyNames[i];
      if (type.getPolymorphicPropertyIfExists(propName) != null)
        continue;
      final Type inferred = inferPropertyTypeFromExistingData(typeName, propName);
      if (inferred != null) {
        type.createProperty(propName, inferred);
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
    if (anyUndeclared)
      builder.withDefaultKeyTypesForUndeclaredProperties(defaultKeyTypes);
    builder.create();
    if (!existedBefore)
      stats.incIndexesAdded();
  }

  /**
   * Walks the type's records (polymorphic, capped) until it finds a non-null value for
   * {@code propertyName} and returns the {@link Type} that matches its Java class. Returns
   * {@code null} if no record sets the property. Used by Cypher DDL to preserve numeric
   * properties across {@code CREATE INDEX} on an already-populated type (issue #4222).
   */
  private Type inferPropertyTypeFromExistingData(final String typeName, final String propertyName) {
    try {
      final Iterator<Record> it = database.iterateType(typeName, true);
      int scanned = 0;
      // 256 is enough to spot the dominant value type without doing a full table scan; users
      // hitting heterogeneous-typed properties can declare the property explicitly via SQL.
      while (it.hasNext() && scanned < 256) {
        final Record record = it.next();
        scanned++;
        if (!(record instanceof Document doc))
          continue;
        if (!doc.has(propertyName))
          continue;
        final Object value = doc.get(propertyName);
        if (value == null)
          continue;
        try {
          return Type.getTypeByClass(value.getClass());
        } catch (final IllegalArgumentException ignored) {
          // Unknown Java class for the value; leave the property undeclared and let the index
          // fall back to STRING serialisation.
          return null;
        }
      }
    } catch (final Exception ignored) {
      // If iteration fails for any reason (e.g. schema in transient state) just skip inference.
    }
    return null;
  }

  private void executeDropIndex(final CypherDDLStatement ddl, final Schema schema, final QueryStatistics stats) {
    final String indexName = ddl.getConstraintName();
    if (ddl.isIfExists()) {
      if (schema.existsIndex(indexName)) {
        schema.dropIndex(indexName);
        stats.incIndexesRemoved();
      }
    } else {
      schema.dropIndex(indexName);
      stats.incIndexesRemoved();
    }
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
