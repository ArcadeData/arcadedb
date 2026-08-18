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
package com.arcadedb.query.opencypher.ast;

import com.arcadedb.function.CypherBuiltinFunctions;
import com.arcadedb.function.CypherFunctionRegistry;
import com.arcadedb.function.sql.DefaultSQLFunctionFactory;
import com.arcadedb.query.opencypher.parser.CypherExpressionWalker;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.opencypher.procedures.CypherProcedureRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Simple implementation of CypherStatement for Phase 1.
 * This is a basic implementation to get the module compiling.
 * Will be replaced with full ANTLR-based implementation in later phases.
 */
public class SimpleCypherStatement implements CypherStatement {
  private final String             originalQuery;
  private final List<MatchClause>  matchClauses;
  private final WhereClause        whereClause;
  private final ReturnClause       returnClause;
  private final OrderByClause      orderByClause;
  private final Expression          skip;
  private final Expression          limit;
  private final CreateClause       createClause;
  private final SetClause          setClause;
  private final DeleteClause       deleteClause;
  private final MergeClause        mergeClause;
  private final List<UnwindClause> unwindClauses;
  private final List<WithClause>   withClauses;
  private final List<CallClause>   callClauses;
  private final List<RemoveClause> removeClauses;
  private final List<ClauseEntry>  clausesInOrder;
  private final boolean            hasCreate;
  private final boolean            hasMerge;
  private final boolean            hasDelete;
  private final boolean            hasRemove;
  private final boolean            readOnly;
  private final boolean            hasVariableLengthPath;
  private final boolean            hasUnwindBeforeMatch;
  private final boolean            hasWithBeforeMatch;
  private final boolean            hasSubquery;
  private final boolean            hasWriteBeforeMatch;
  private final boolean            hasFinishClause;

  private volatile CypherReferencedVariables referencedVariables;

  public SimpleCypherStatement(final String originalQuery, final List<MatchClause> matchClauses,
                               final WhereClause whereClause, final ReturnClause returnClause,
                               final boolean hasCreate, final boolean hasMerge,
                               final boolean hasDelete) {
    this(originalQuery, matchClauses, whereClause, returnClause, null, null, null, null, null, null, null, null, null
        , null,
        hasCreate, hasMerge, hasDelete);
  }

  public SimpleCypherStatement(final String originalQuery, final List<MatchClause> matchClauses,
                               final WhereClause whereClause, final ReturnClause returnClause,
                               final OrderByClause orderByClause,
                               final Expression skip, final Expression limit, final boolean hasCreate,
                               final boolean hasMerge, final boolean hasDelete) {
    this(originalQuery, matchClauses, whereClause, returnClause, orderByClause, skip, limit, null, null, null, null,
        null, null, null,
        hasCreate, hasMerge, hasDelete);
  }

  public SimpleCypherStatement(final String originalQuery, final List<MatchClause> matchClauses,
                               final WhereClause whereClause, final ReturnClause returnClause,
                               final OrderByClause orderByClause,
                               final Expression skip, final Expression limit, final CreateClause createClause,
                               final SetClause setClause,
                               final DeleteClause deleteClause, final MergeClause mergeClause,
                               final List<UnwindClause> unwindClauses,
                               final List<WithClause> withClauses,
                               final boolean hasCreate, final boolean hasMerge, final boolean hasDelete) {
    this(originalQuery, matchClauses, whereClause, returnClause, orderByClause, skip, limit, createClause, setClause,
        deleteClause, mergeClause, unwindClauses, withClauses, null, hasCreate, hasMerge, hasDelete);
  }

  public SimpleCypherStatement(final String originalQuery, final List<MatchClause> matchClauses,
                               final WhereClause whereClause, final ReturnClause returnClause,
                               final OrderByClause orderByClause,
                               final Expression skip, final Expression limit, final CreateClause createClause,
                               final SetClause setClause,
                               final DeleteClause deleteClause, final MergeClause mergeClause,
                               final List<UnwindClause> unwindClauses,
                               final List<WithClause> withClauses, final List<ClauseEntry> clausesInOrder,
                               final boolean hasCreate, final boolean hasMerge, final boolean hasDelete) {
    this(originalQuery, matchClauses, whereClause, returnClause, orderByClause, skip, limit, createClause, setClause,
        deleteClause, mergeClause, unwindClauses, withClauses, null, clausesInOrder, hasCreate, hasMerge, hasDelete);
  }

  public SimpleCypherStatement(final String originalQuery, final List<MatchClause> matchClauses,
                               final WhereClause whereClause, final ReturnClause returnClause,
                               final OrderByClause orderByClause,
                               final Expression skip, final Expression limit, final CreateClause createClause,
                               final SetClause setClause,
                               final DeleteClause deleteClause, final MergeClause mergeClause,
                               final List<UnwindClause> unwindClauses,
                               final List<WithClause> withClauses, final List<CallClause> callClauses,
                               final List<ClauseEntry> clausesInOrder,
                               final boolean hasCreate, final boolean hasMerge, final boolean hasDelete) {
    this(originalQuery, matchClauses, whereClause, returnClause, orderByClause, skip, limit, createClause, setClause,
        deleteClause, mergeClause, unwindClauses, withClauses, callClauses, null, clausesInOrder,
        hasCreate, hasMerge, hasDelete, false);
  }

  public SimpleCypherStatement(final String originalQuery, final List<MatchClause> matchClauses,
                               final WhereClause whereClause, final ReturnClause returnClause,
                               final OrderByClause orderByClause,
                               final Expression skip, final Expression limit, final CreateClause createClause,
                               final SetClause setClause,
                               final DeleteClause deleteClause, final MergeClause mergeClause,
                               final List<UnwindClause> unwindClauses,
                               final List<WithClause> withClauses, final List<CallClause> callClauses,
                               final List<RemoveClause> removeClauses,
                               final List<ClauseEntry> clausesInOrder,
                               final boolean hasCreate, final boolean hasMerge, final boolean hasDelete,
                               final boolean hasRemove) {
    this.originalQuery = originalQuery;
    this.matchClauses = matchClauses != null ? matchClauses : new ArrayList<>();
    this.whereClause = whereClause;
    this.returnClause = returnClause;
    this.orderByClause = orderByClause;
    this.skip = skip;
    this.limit = limit;
    this.createClause = createClause;
    this.setClause = setClause;
    this.deleteClause = deleteClause;
    this.mergeClause = mergeClause;
    this.unwindClauses = unwindClauses != null ? unwindClauses : new ArrayList<>();
    this.withClauses = withClauses != null ? withClauses : new ArrayList<>();
    this.callClauses = callClauses != null ? callClauses : new ArrayList<>();
    this.removeClauses = removeClauses != null ? removeClauses : new ArrayList<>();
    this.clausesInOrder = clausesInOrder != null ? clausesInOrder : new ArrayList<>();
    this.hasCreate = hasCreate;
    this.hasMerge = hasMerge;
    this.hasDelete = hasDelete;
    this.hasRemove = hasRemove;
    final boolean hasForeach = this.clausesInOrder.stream().anyMatch(c -> c.getType() == ClauseEntry.ClauseType.FOREACH);
    final boolean writeSubquery = anyWriteSubquery(this.clausesInOrder);
    final boolean writeProcedureCall = anyWriteProcedureCall(this.callClauses);
    final boolean unresolvedFunctionCall = anyUnresolvedFunctionCall(this);
    this.readOnly = !hasCreate && !hasMerge && !hasDelete && !hasRemove && !hasForeach
        && (setClause == null || setClause.isEmpty()) && !writeSubquery && !writeProcedureCall && !unresolvedFunctionCall;

    // Pre-compute flags used by CypherExecutionPlan.execute() to avoid repeated clause scanning
    this.hasVariableLengthPath = computeHasVariableLengthPath();
    this.hasUnwindBeforeMatch = computeHasClauseBeforeMatch(ClauseEntry.ClauseType.UNWIND);
    this.hasWithBeforeMatch = computeHasClauseBeforeMatch(ClauseEntry.ClauseType.WITH);
    this.hasSubquery = computeHasSubquery();
    this.hasWriteBeforeMatch = computeHasWriteBeforeMatch();
    this.hasFinishClause = this.clausesInOrder.stream()
        .anyMatch(c -> c.getType() == ClauseEntry.ClauseType.FINISH);
  }

  private boolean computeHasWriteBeforeMatch() {
    if (clausesInOrder == null || clausesInOrder.isEmpty())
      // Legacy constructors don't populate clausesInOrder. When both writes and MATCH coexist,
      // the order is unknown - conservatively disable the optimizer fast path so write-then-read
      // visibility is preserved. Mirrors the fallback in computeHasClauseBeforeMatch.
      return !readOnly && !matchClauses.isEmpty();
    int firstWriteOrder = Integer.MAX_VALUE;
    int firstMatchOrder = Integer.MAX_VALUE;
    for (final ClauseEntry entry : clausesInOrder) {
      final ClauseEntry.ClauseType t = entry.getType();
      if (t == ClauseEntry.ClauseType.CREATE
          || t == ClauseEntry.ClauseType.MERGE
          || t == ClauseEntry.ClauseType.SET
          || t == ClauseEntry.ClauseType.DELETE
          || t == ClauseEntry.ClauseType.REMOVE
          || t == ClauseEntry.ClauseType.FOREACH)
        firstWriteOrder = Math.min(firstWriteOrder, entry.getOrder());
      else if (t == ClauseEntry.ClauseType.MATCH)
        firstMatchOrder = Math.min(firstMatchOrder, entry.getOrder());
    }
    return firstWriteOrder < firstMatchOrder;
  }

  private boolean computeHasVariableLengthPath() {
    if (matchClauses == null)
      return false;
    for (final MatchClause matchClause : matchClauses)
      for (final PathPattern path : matchClause.getPathPatterns())
        for (int i = 0; i < path.getRelationshipCount(); i++)
          if (path.getRelationship(i).isVariableLength())
            return true;
    return false;
  }

  private boolean computeHasClauseBeforeMatch(final ClauseEntry.ClauseType clauseType) {
    if (clausesInOrder == null || clausesInOrder.isEmpty()) {
      if (clauseType == ClauseEntry.ClauseType.UNWIND)
        return !unwindClauses.isEmpty() && !matchClauses.isEmpty();
      if (clauseType == ClauseEntry.ClauseType.WITH)
        return !withClauses.isEmpty() && !matchClauses.isEmpty();
      return false;
    }
    int firstClauseOrder = Integer.MAX_VALUE;
    int firstMatchOrder = Integer.MAX_VALUE;
    for (final ClauseEntry entry : clausesInOrder) {
      if (entry.getType() == clauseType)
        firstClauseOrder = Math.min(firstClauseOrder, entry.getOrder());
      else if (entry.getType() == ClauseEntry.ClauseType.MATCH)
        firstMatchOrder = Math.min(firstMatchOrder, entry.getOrder());
    }
    return firstClauseOrder < firstMatchOrder;
  }

  private boolean computeHasSubquery() {
    if (clausesInOrder == null || clausesInOrder.isEmpty())
      return false;
    for (final ClauseEntry entry : clausesInOrder)
      if (entry.getType() == ClauseEntry.ClauseType.SUBQUERY)
        return true;
    return false;
  }

  /**
   * Returns {@code true} when any {@code CALL { ... }} subquery in this statement contains write
   * operations. Writes happening inside a CALL must mark the outer statement as non-read-only so
   * that {@link com.arcadedb.query.opencypher.executor.CypherExecutionPlan#execute} eagerly drains
   * the result set (forcing the side effects to commit) and suppresses output when the outer query
   * has no RETURN clause, matching Neo4j semantics. See issue #4191.
   */
  private static boolean anyWriteSubquery(final List<ClauseEntry> entries) {
    if (entries == null || entries.isEmpty())
      return false;
    for (final ClauseEntry entry : entries) {
      if (entry.getType() != ClauseEntry.ClauseType.SUBQUERY)
        continue;
      final SubqueryClause sub = entry.getTypedClause();
      final CypherStatement inner = sub.getInnerStatement();
      if (inner != null && !inner.isReadOnly())
        return true;
    }
    return false;
  }

  /**
   * Returns {@code true} when any top-level {@code CALL} in this statement targets a registered write
   * {@link CypherProcedure}. Without this, {@code CALL merge.node(...) YIELD node RETURN node} - a statement with
   * no CREATE/SET/MERGE/DELETE/REMOVE/FOREACH clause of its own - classified as read-only, and that flag is what
   * {@code OpenCypherQueryEngine.executionDatabase()} uses to pick between the raw database instance and the
   * Raft-aware wrapper. On HA the raw instance commits pages locally without proposing them to Raft, and a
   * follower runs the statement locally instead of forwarding it to the leader, because the same flag backs
   * {@code analyze().isIdempotent()}. Harmless until #6073 gave {@code CallStep} an auto-commit; live since.
   * See issue #6094, and #5492/#5655 for the same failure class on other write steps.
   * <p>
   * A {@code CALL} nested in a {@code CALL { ... }} subquery is already covered: {@link #anyWriteSubquery}
   * recurses through the inner statement's own {@code isReadOnly()}, which now accounts for this.
   * <p>
   * A name that resolves to no registered procedure falls through {@link #isConfirmedPureFunctionName} - the same
   * check an expression-position call goes through (issue #6418) - because {@code CallStep} resolves a CALL target
   * through the identical chain: {@link CypherProcedureRegistry}, then {@link CypherFunctionRegistry}, then a
   * {@code DEFINE FUNCTION} custom adapter for a dotted name, then the built-in SQL fallback. The registry lookup
   * is a static case-insensitive map read that also strips the {@code apoc.} prefix, so both spellings of one
   * procedure classify identically, and it costs nothing at execution time: this runs once per parse, and parsed
   * statements are cached per query text.
   */
  private static boolean anyWriteProcedureCall(final List<CallClause> calls) {
    if (calls.isEmpty())
      return false;
    for (final CallClause call : calls) {
      final CypherProcedure procedure = CypherProcedureRegistry.get(call.getProcedureName());
      if (procedure != null) {
        // A per-call refinement may only narrow, so a procedure that never writes needs no second look.
        if (procedure.isWriteProcedure() && procedure.isWriteProcedure(literalArguments(call)))
          return true;
        continue;
      }
      if (!isConfirmedPureFunctionName(call.getProcedureName()))
        return true;
    }
    return false;
  }

  /**
   * The constant value of every argument at a call site, with {@code null} wherever the parser produced anything
   * other than a literal (a parameter, a variable, a computed expression). Fed to
   * {@link CypherProcedure#isWriteProcedure(Object[])} so a procedure whose behaviour depends on its arguments -
   * {@code apoc.do.when} runs caller-supplied query strings - can classify one particular call rather than being
   * pinned to the conservative answer it must give for the procedure as a whole.
   */
  private static Object[] literalArguments(final CallClause call) {
    final List<Expression> arguments = call.getArguments();
    final Object[] values = new Object[arguments.size()];
    for (int i = 0; i < values.length; i++)
      if (arguments.get(i) instanceof LiteralExpression literal)
        values[i] = literal.getValue();
    return values;
  }

  /** Explicit access to a built-in SQL function, e.g. {@code RETURN sql.abs(-1)} - always closed and pure. */
  private static final String SQL_FUNCTION_PREFIX = "sql.";

  /**
   * Returns {@code true} when {@code rawName} is confirmed, without any database access, to be unable to resolve to
   * a schema-registered {@code DEFINE FUNCTION} body - the gap issue #6418 is about, reachable both from a CALL
   * clause target ({@link #anyWriteProcedureCall}) and from a function call in expression position
   * ({@link #anyUnresolvedFunctionCall}), since {@code CallStep} and {@code FunctionCallExpression}'s resolver
   * fall through the identical chain: {@link CypherProcedureRegistry} or {@link CypherFunctionRegistry}, then a
   * {@code DEFINE FUNCTION} custom adapter for a dotted name, then the built-in SQL fallback.
   * <p>
   * {@code CustomFunctionAdapter.getOrCreateCustomFunctionAdapter} - the one and only path from either call shape
   * to a schema-registered function body - refuses outright to build an adapter for a name with no {@code .} in
   * it. So a BARE name, whichever of a function's several underscored or dotted spellings the caller used
   * ({@code count}, {@code vector_distance}, ...), can never reach one and needs no lookup at all; only a DOTTED
   * name needs a further check, since a namespaced built-in ({@code text.*}/{@code map.*}/{@code date.*}/
   * {@code vector.*}/...) is dotted too - and the parser rewrites the underscored {@code vector_norm}/
   * {@code vector_distance} spellings into a dotted SQL bridge name of their own ({@code vector.magnitude},
   * {@code vector.l2Distance}, ...) before a {@link FunctionCallExpression} is ever built, so those two land here
   * as dotted regardless of how the query spelled them. A dotted name is confirmed pure when it is registered in
   * {@link CypherFunctionRegistry}, one of the hardcoded names in {@link CypherBuiltinFunctions}
   * ({@code date.truncate}, {@code point.withinbbox}, ...), explicitly bridged to a built-in SQL function via the
   * {@code sql.} prefix, or - matching {@code CypherFunctionFactory}'s own last resort - a name
   * {@link DefaultSQLFunctionFactory} already answers for directly, unprefixed and unmapped (the vector bridge
   * names above are exactly this case: registered as built-in SQL functions under their dotted names, reached with
   * no {@code sql.} prefix and no {@link CypherFunctionRegistry} entry either). Every other dotted name - a
   * {@code library.function} custom function being the shape #6418 is about, but also a plain typo - is
   * conservatively treated as possibly a write: a false "maybe writes" costs a caller its optimization, a false
   * "definitely read-only" costs correctness (a follower serving a write locally instead of forwarding it to the
   * leader, or {@code SubqueryStep} skipping the refresh #6362 needs), the same asymmetry
   * {@link CypherReferencedVariables} applies to its own "shape not modelled" answer.
   */
  private static boolean isConfirmedPureFunctionName(final String rawName) {
    final String name = rawName.toLowerCase(Locale.ROOT);
    return !name.contains(".") || CypherFunctionRegistry.hasFunction(name) || CypherBuiltinFunctions.isBuiltin(name)
        || name.startsWith(SQL_FUNCTION_PREFIX) || DefaultSQLFunctionFactory.getInstance().hasFunction(name);
  }

  /**
   * Returns {@code true} when the statement calls a function whose write-or-not status cannot be confirmed at parse
   * time - a call in expression position ({@code WITH foo() AS x}, {@code RETURN sql.foo()}, nested inside another
   * expression) that {@link #isConfirmedPureFunctionName} cannot clear. {@code SQLFunctionDefinition.execute} runs
   * a {@code DEFINE FUNCTION} body as a full {@code sqlscript} command that can itself write, so a call reaching
   * one here must not be classified read-only. See issue #6418.
   * <p>
   * Reuses {@link CypherExpressionWalker} - the traversal {@link CypherReferencedVariables} relies on for the same
   * kind of "every expression, wherever it appears" question - rather than a partial recursion of its own, so this
   * reaches {@code WHERE}, {@code SET}, {@code CREATE}/{@code MERGE} inline properties, {@code FOREACH} bodies and
   * nested {@code CALL { }}/{@code EXISTS}/{@code COUNT}/{@code COLLECT} subqueries alike.
   */
  private static boolean anyUnresolvedFunctionCall(final CypherStatement statement) {
    final UnresolvedFunctionCallDetector detector = new UnresolvedFunctionCallDetector();
    CypherExpressionWalker.walk(statement, detector);
    return detector.found;
  }

  private static final class UnresolvedFunctionCallDetector implements CypherExpressionWalker.Visitor {
    private boolean found = false;

    @Override
    public void visit(final Expression expression) {
      if (found || !(expression instanceof FunctionCallExpression call))
        return;

      if (!isConfirmedPureFunctionName(call.getFunctionName()))
        found = true;
    }
  }

  @Override
  public boolean isReadOnly() {
    return readOnly;
  }

  @Override
  public List<MatchClause> getMatchClauses() {
    return matchClauses;
  }

  @Override
  public WhereClause getWhereClause() {
    return whereClause;
  }

  @Override
  public ReturnClause getReturnClause() {
    return returnClause;
  }

  @Override
  public boolean hasCreate() {
    return hasCreate;
  }

  @Override
  public boolean hasMerge() {
    return hasMerge;
  }

  @Override
  public boolean hasDelete() {
    return hasDelete;
  }

  @Override
  public OrderByClause getOrderByClause() {
    return orderByClause;
  }

  @Override
  public Expression getSkip() {
    return skip;
  }

  @Override
  public Expression getLimit() {
    return limit;
  }

  @Override
  public CreateClause getCreateClause() {
    return createClause;
  }

  @Override
  public SetClause getSetClause() {
    return setClause;
  }

  @Override
  public DeleteClause getDeleteClause() {
    return deleteClause;
  }

  @Override
  public MergeClause getMergeClause() {
    return mergeClause;
  }

  @Override
  public List<UnwindClause> getUnwindClauses() {
    return unwindClauses;
  }

  @Override
  public List<WithClause> getWithClauses() {
    return withClauses;
  }

  @Override
  public List<ClauseEntry> getClausesInOrder() {
    return clausesInOrder;
  }

  @Override
  public List<CallClause> getCallClauses() {
    return callClauses;
  }

  @Override
  public List<RemoveClause> getRemoveClauses() {
    return removeClauses;
  }

  public boolean hasRemove() {
    return hasRemove;
  }

  @Override
  public boolean hasVariableLengthPath() {
    return hasVariableLengthPath;
  }

  @Override
  public boolean hasUnwindBeforeMatch() {
    return hasUnwindBeforeMatch;
  }

  @Override
  public boolean hasWithBeforeMatch() {
    return hasWithBeforeMatch;
  }

  @Override
  public boolean hasWriteBeforeMatch() {
    return hasWriteBeforeMatch;
  }

  @Override
  public boolean hasSubquery() {
    return hasSubquery;
  }

  @Override
  public boolean hasFinishClause() {
    return hasFinishClause;
  }

  /**
   * Computed on first use and kept, because the statement is immutable and shared while the caller - the plan built
   * for a subquery body - is rebuilt for every outer row. Two threads arriving together collect the same answer from
   * the same tree and one overwrites the other with an equal value, so the field is written once as far as any
   * reader is concerned; {@code volatile} is what publishes the collected set safely.
   */
  @Override
  public CypherReferencedVariables getReferencedVariables() {
    CypherReferencedVariables result = referencedVariables;
    if (result == null)
      referencedVariables = result = CypherReferencedVariables.of(this);
    return result;
  }

  public String getOriginalQuery() {
    return originalQuery;
  }
}
