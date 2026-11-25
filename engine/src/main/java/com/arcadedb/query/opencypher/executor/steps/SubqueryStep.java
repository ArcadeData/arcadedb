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
package com.arcadedb.query.opencypher.executor.steps;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.DeferredExistenceChecks;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.InternalVariables;
import com.arcadedb.query.opencypher.ast.ClauseEntry;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.ReturnClause;
import com.arcadedb.query.opencypher.ast.SubqueryClause;
import com.arcadedb.query.opencypher.ast.UnionStatement;
import com.arcadedb.query.opencypher.ast.VariableExpression;
import com.arcadedb.query.opencypher.ast.WithClause;
import com.arcadedb.query.opencypher.executor.CypherExecutionPlan;
import com.arcadedb.query.opencypher.executor.ExpressionEvaluator;
import com.arcadedb.query.opencypher.executor.LabelReplacements;
import com.arcadedb.query.opencypher.executor.WrittenCopy;
import com.arcadedb.query.opencypher.parser.ReturnScope;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.RidHashSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.UnaryOperator;

/**
 * Execution step for CALL subquery clause.
 * <p>
 * For each input row from the previous step, executes the inner query
 * with the outer row's variables available (imported via WITH inside the subquery).
 * The inner query's RETURN values are merged with the outer row.
 * <p>
 * When IN TRANSACTIONS is specified, the subquery commits changes in batches
 * rather than accumulating all changes in a single transaction.
 * <p>
 * Example:
 * <pre>
 * UNWIND [1, 2, 3] AS x
 * CALL { WITH x RETURN x * 10 AS y }
 * RETURN x, y
 * </pre>
 * Produces: {x:1, y:10}, {x:2, y:20}, {x:3, y:30}
 */
public class SubqueryStep extends AbstractExecutionStep {
  private static final String STAR_SCOPE = "*";

  private final SubqueryClause subqueryClause;
  private final DatabaseInternal database;
  private final Map<String, Object> parameters;
  private final ExpressionEvaluator expressionEvaluator;
  private final Set<String> importedVariables;
  private final boolean importAllVariables;
  private final boolean innerMayWrite;
  // SET BY THE PLANNER FOR A STATEMENT THAT CANNOT WRITE: THE BODY'S ROWS ARE THEN HANDED ON AS THEY ARE READ
  private boolean streamBody;
  // THE BODY RESULT SET BEING STREAMED, CLOSED WHEN IT ENDS OR WHEN THE CONSUMER STOPS EARLY
  private ResultSet streamedBody;
  // THE BODY'S PLAN, BUILT ON THE FIRST OUTER ROW AND RUN FOR EVERY LATER ONE (ISSUE #12136)
  private CypherExecutionPlan innerPlan;
  // THE COLUMNS THE BODY RETURNS, BOUND TO NULL ON A ROW THE BODY ANSWERED NOTHING FOR: COMPUTED ON FIRST USE
  private String[] returnedColumns;

  public SubqueryStep(final SubqueryClause subqueryClause, final CommandContext context,
                       final DatabaseInternal database, final Map<String, Object> parameters,
                       final ExpressionEvaluator expressionEvaluator) {
    super(context);
    this.subqueryClause = subqueryClause;
    this.database = database;
    this.parameters = parameters;
    this.expressionEvaluator = expressionEvaluator;
    this.importedVariables = computeImportedVariables(subqueryClause);
    this.importAllVariables = importedVariables == null;
    // isReadOnly() is exact for every CREATE/SET/MERGE/DELETE/REMOVE/FOREACH shape and for a CALL to a registered
    // CypherProcedure (SimpleCypherStatement.anyWriteProcedureCall), which is every write path Cypher itself
    // exposes. It is NOT exact for a call - anywhere in the statement, CALL or expression position - into a
    // user-defined SQL function (DEFINE FUNCTION): SQLFunctionDefinition.execute runs the definition as a full
    // "sqlscript" command, which can itself CREATE/UPDATE/DELETE. Such a call reads as read-only here and skips
    // the refresh below, which can reintroduce #4182/#6362-style staleness for that one narrow shape. Tracked as
    // a known limitation (issue #6362 review, filed as #6418) rather than fixed here: closing it properly needs the same
    // whole-statement, every-expression-position walk isReadOnly() itself would need, and this flag also backs
    // HA leader routing (issue #6094) - not a change to make inside an unrelated bug-fix batch under time
    // pressure.
    this.innerMayWrite = !subqueryClause.getInnerStatement().isReadOnly();
  }

  /**
   * Computes the set of outer variables that the inner query is allowed to reference.
   * <p>
   * Cypher defines two scope modes for CALL subqueries:
   * <ul>
   *   <li><b>Explicit scope</b> {@code CALL (v1, v2) { ... }} imports only the listed variables.
   *   {@code CALL (*) { ... }} imports all outer variables.</li>
   *   <li><b>Implicit scope</b> {@code CALL { ... }} requires an <i>importing WITH</i> as the first
   *   clause of the body; that clause may only reference outer variables (no expressions, no aggregations).
   *   Without such an importing WITH, no outer variable is visible inside the subquery.</li>
   * </ul>
   * Returning {@code null} signals "import all" (used for {@code CALL (*)} and an importing {@code WITH *}).
   *
   * @return set of imported variable names, or {@code null} to import all outer variables
   */
  private static Set<String> computeImportedVariables(final SubqueryClause clause) {
    final List<String> scope = clause.getScopeVariables();
    if (scope != null) {
      if (scope.size() == 1 && STAR_SCOPE.equals(scope.get(0)))
        return null; // sentinel: import everything
      return new LinkedHashSet<>(scope);
    }
    // Implicit scope: look for an importing WITH as the first clause of each branch.
    return collectImportingWithVariables(clause.getInnerStatement());
  }

  private static Set<String> collectImportingWithVariables(final CypherStatement statement) {
    if (statement instanceof UnionStatement union) {
      final Set<String> combined = new LinkedHashSet<>();
      for (final CypherStatement branch : union.getQueries()) {
        final Set<String> branchImports = collectImportingWithVariables(branch);
        if (branchImports == null)
          return null; // a branch importing with WITH * needs every outer variable
        combined.addAll(branchImports);
      }
      return combined;
    }

    final List<ClauseEntry> clauses = statement.getClausesInOrder();
    if (clauses == null || clauses.isEmpty())
      return Collections.emptySet();

    final ClauseEntry first = clauses.get(0);
    if (first.getType() != ClauseEntry.ClauseType.WITH)
      return Collections.emptySet();

    final WithClause withClause = first.getTypedClause();
    final Set<String> imports = new LinkedHashSet<>();
    for (final ReturnClause.ReturnItem item : withClause.getItems()) {
      // An importing WITH * imports every outer variable, exactly like CALL (*) (issue #10891). Before, it imported none,
      // so a predicate on an outer variable read null and the subquery silently returned no row.
      if (item.isStar())
        return null;
      if (!(item.getExpression() instanceof VariableExpression var))
        continue;
      final String name = var.getVariableName();
      final String alias = item.getAlias();
      if (alias != null && !alias.equals(name))
        continue; // aliased projection, not a pure import
      imports.add(name);
    }
    return imports;
  }

  /**
   * Hands the body's rows on as they are read instead of collecting them all first, so a consumer that stops early (a
   * {@code LIMIT}) stops the body too (issue #11082). Only for a statement that cannot write: collecting is what runs a
   * body's reads to completion before a write that follows the {@code CALL} sees the graph, and what lets the refresh of
   * the outer bindings run after a writing body is done. A body that returns nothing is collected either way.
   */
  public void setStreamBody(final boolean streamBody) {
    this.streamBody = streamBody && !innerMayWrite && subqueryClause.getInnerStatement().getReturnClause() != null;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    if (subqueryClause.isInTransactions())
      return syncPullInTransactions(context, nRecords);

    return syncPullNormal(context, nRecords);
  }

  private ResultSet syncPullNormal(final CommandContext context, final int nRecords) {
    final boolean hasPrevious = prev != null;

    return new ResultSet() {
      private ResultSet prevResults = null;
      private final List<Result> buffer = new ArrayList<>();
      private int bufferIndex = 0;
      private boolean finished = false;
      private Iterator<Result> currentInnerResults = null;
      private Result currentOuterRow = null;

      @Override
      public boolean hasNext() {
        if (bufferIndex < buffer.size())
          return true;
        if (finished)
          return false;
        fetchMore(nRecords);
        return bufferIndex < buffer.size();
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();
        return liveRow(buffer.get(bufferIndex++), context);
      }

      private void fetchMore(final int n) {
        buffer.clear();
        bufferIndex = 0;

        if (prevResults == null) {
          if (hasPrevious)
            prevResults = prev.syncPull(context, nRecords);
          else {
            prevResults = new ResultSet() {
              private boolean consumed = false;

              @Override
              public boolean hasNext() {
                return !consumed;
              }

              @Override
              public Result next() {
                if (consumed)
                  throw new NoSuchElementException();
                consumed = true;
                return new ResultInternal();
              }

              @Override
              public void close() {
              }
            };
          }
        }

        while (buffer.size() < n) {
          // If we have inner results from a previous outer row, continue consuming them
          if (currentInnerResults != null && currentInnerResults.hasNext()) {
            final long begin = context.isProfiling() ? System.nanoTime() : 0;
            try {
              if (context.isProfiling())
                rowCount++;

              final Result innerRow = currentInnerResults.next();
              buffer.add(mergeResults(currentOuterRow, innerRow));
            } finally {
              if (context.isProfiling())
                cost += System.nanoTime() - begin;
            }
            continue;
          }
          closeStreamedBody();
          currentInnerResults = null;

          // Need a new outer row
          if (!prevResults.hasNext()) {
            finished = true;
            break;
          }

          currentOuterRow = prevResults.next();
          final long begin = context.isProfiling() ? System.nanoTime() : 0;
          try {
            if (context.isProfiling())
              rowCount++;

            if (streamBody) {
              // The body reads as many rows at a time as this step was asked for, so a LIMIT after it bounds the body too
              final ResultSet body = executeBody(currentOuterRow, context,
                  Math.max(1, Math.min(nRecords, CypherExecutionPlan.SEEDED_BATCH_SIZE)));
              // Registered before the first pull, so close() also reaches a body that fails on it
              streamedBody = body;
              if (body.hasNext()) {
                currentInnerResults = body;
              } else {
                closeStreamedBody();
                if (subqueryClause.isOptional())
                  buffer.add(withReturnedColumnsNull(currentOuterRow));
              }
              continue;
            }

            // Execute the inner query seeded with the outer row
            final List<Result> innerResults = executeInnerQuery(currentOuterRow, context);
            refreshOuterBindings(currentOuterRow, context);

            if (innerResults.isEmpty()) {
              // OPTIONAL CALL - produce the outer row with nulls for inner columns (issue #11632)
              if (subqueryClause.isOptional())
                buffer.add(withReturnedColumnsNull(currentOuterRow));
              // Non-optional: no output rows for this outer row
              currentInnerResults = null;
            } else {
              currentInnerResults = innerResults.iterator();
            }
          } finally {
            if (context.isProfiling())
              cost += System.nanoTime() - begin;
          }
        }
      }

      @Override
      public void close() {
        SubqueryStep.this.close();
      }
    };
  }

  /**
   * IN TRANSACTIONS mode: collects all input rows, executes the inner query in batches,
   * committing after each batch.
   */
  private ResultSet syncPullInTransactions(final CommandContext context, final int nRecords) {
    final boolean hasPrevious = prev != null;

    // Resolve batch size
    final int batchSize = resolveBatchSize(context);
    final SubqueryClause.OnError onError = subqueryClause.getOnError();
    final String statusVariable = subqueryClause.getReportStatusVariable();

    return new ResultSet() {
      private ResultSet prevResults = null;
      private final List<Result> buffer = new ArrayList<>();
      private int bufferIndex = 0;
      private boolean finished = false;
      // The inner transactions run so far, which numbers each one for REPORT STATUS
      private int batchNumber = 0;
      // ON ERROR BREAK met a failed batch: no later batch runs (issue #11595)
      private boolean broken = false;
      // The records the batches committed so far wrote, re-read in the outer rows produced before (issue #12324). It holds every
      // record the statement writes, at 16 bytes each, until the statement ends: an outer row produced before the first commit
      // can reach any later batch (a WITH ... ORDER BY in front produces them all up front), so no batch's writes can be dropped
      private RidHashSet committedWrites = null;

      @Override
      public boolean hasNext() {
        // A batch whose rows all produced no inner row leaves the buffer empty while input remains: run the next batch
        // instead of reporting the end, or every later batch is silently skipped (issue #10886)
        while (bufferIndex >= buffer.size()) {
          if (finished)
            return false;
          fetchMore();
        }
        return true;
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();
        return liveRow(buffer.get(bufferIndex++), context);
      }

      private void fetchMore() {
        buffer.clear();
        bufferIndex = 0;

        if (prevResults == null) {
          if (hasPrevious)
            prevResults = prev.syncPull(context, nRecords);
          else {
            prevResults = new ResultSet() {
              private boolean consumed = false;

              @Override
              public boolean hasNext() {
                return !consumed;
              }

              @Override
              public Result next() {
                if (consumed)
                  throw new NoSuchElementException();
                consumed = true;
                return new ResultInternal();
              }

              @Override
              public void close() {
              }
            };
          }
        }

        // Collect a batch of outer rows
        final List<Result> batch = new ArrayList<>();
        while (batch.size() < batchSize && prevResults.hasNext())
          batch.add(prevResults.next());

        if (batch.isEmpty()) {
          finished = true;
          return;
        }

        if (broken) {
          // After ON ERROR BREAK the rows are passed on with what the subquery returns bound to null, and no transaction started
          for (final Result outerRow : batch)
            buffer.add(rowOfUncommittedBatch(outerRow, statusVariable, status(false, false, null, null)));
          if (!prevResults.hasNext())
            finished = true;
          return;
        }

        // Execute the batch in its own transaction
        final List<List<Result>> innerResultsPerRow = new ArrayList<>(batch.size());
        final String transactionId = String.valueOf(++batchNumber);
        committedWrites = commitStatementWrites(committedWrites);
        final boolean callerTransaction = database.isTransactionActive();
        final int nesting = database.getNestedTransactions();
        // A label the batch writes replaces the node by a new record, noted for the outer rows to follow: a batch that does not
        // commit takes its notes back with it. Only a body that can write has any, and the map is not created for one that cannot
        final LabelReplacements.Snapshot labelReplacements =
            innerMayWrite && onError != SubqueryClause.OnError.FAIL ? LabelReplacements.of(context).copy() : null;
        database.begin();
        try {
          for (final Result outerRow : batch) {
            final long begin = context.isProfiling() ? System.nanoTime() : 0;
            try {
              if (context.isProfiling())
                rowCount++;

              reloadCommittedRecords(outerRow, committedWrites);
              innerResultsPerRow.add(executeInnerQuery(outerRow, context));
            } finally {
              if (context.isProfiling())
                cost += System.nanoTime() - begin;
            }
          }
          if (innerMayWrite) {
            // What this batch wrote is a version ahead, once it commits, of the copies the outer rows not run yet hold
            final TransactionContext tx = database.getTransactionIfExists();
            if (tx != null && tx.hasWrittenRecords()) {
              if (committedWrites == null)
                committedWrites = new RidHashSet();
              tx.collectWrittenRecordIds(committedWrites);
            }
          }
          // A record the batch left without a MANDATORY property must not be committed with it: checked here, where a violation is a
          // failure of this batch that ON ERROR governs, and not only at the end of the statement, when the batch has long committed
          // and the record can no longer be taken back (issue #13221)
          DeferredExistenceChecks.checkBeforeCommit(database);
          commitBatch();
        } catch (final TimeoutException e) {
          // The statement's deadline ends it whatever ON ERROR says
          database.rollbackAllNested();
          DeferredExistenceChecks.rolledBack(database);
          throw e;
        } catch (final Exception e) {
          if (onError == SubqueryClause.OnError.FAIL) {
            database.rollbackAllNested();
            DeferredExistenceChecks.rolledBack(database);
            throw e;
          }
          // ON ERROR CONTINUE or BREAK (issue #11595): only the batch's own transaction is undone, never the caller's. A failed
          // commit has already ended it
          if (database.getNestedTransactions() > nesting || !callerTransaction && database.isTransactionActive())
            database.rollback();
          // What the batch left provisional went with it
          DeferredExistenceChecks.rolledBack(database);
          if (labelReplacements != null)
            LabelReplacements.of(context).restore(labelReplacements);
          if (onError == SubqueryClause.OnError.BREAK)
            broken = true;
          final Map<String, Object> status = status(true, false, transactionId,
              e.getMessage() != null ? e.getMessage() : e.getClass().getName());
          for (final Result outerRow : batch) {
            refreshOuterBindings(outerRow, context);
            buffer.add(rowOfUncommittedBatch(outerRow, statusVariable, status));
          }
          if (!prevResults.hasNext())
            finished = true;
          return;
        }
        final Map<String, Object> committedStatus = statusVariable != null ? status(true, true, transactionId, null) : null;

        // The outer bindings are refreshed only now, back in the transaction the outer rows belong to: the batch's own
        // transaction does not see what the outer one wrote and has not committed, so a refresh from inside it replaced
        // the record a SET before the CALL had written with the committed one, and RETURN n.k answered the old value
        // (issue #11004)
        for (int i = 0; i < batch.size(); i++) {
          final Result outerRow = batch.get(i);
          refreshOuterBindings(outerRow, context);
          reloadCommittedRecords(outerRow, committedWrites);
          final List<Result> innerResults = innerResultsPerRow.get(i);
          if (innerResults.isEmpty()) {
            if (subqueryClause.isOptional()) {
              final ResultInternal row = withReturnedColumnsNull(outerRow);
              if (committedStatus != null)
                row.setProperty(statusVariable, committedStatus);
              buffer.add(row);
            }
          } else {
            for (final Result innerRow : innerResults) {
              final ResultInternal merged = mergeResults(outerRow, innerRow);
              if (committedStatus != null)
                merged.setProperty(statusVariable, committedStatus);
              buffer.add(merged);
            }
          }
        }

        // If no more input, mark finished
        if (!prevResults.hasNext())
          finished = true;
      }

      @Override
      public void close() {
        SubqueryStep.this.close();
      }
    };
  }

  /**
   * Commits what the statement wrote before a batch of {@code CALL { ... } IN TRANSACTIONS}, then begins its transaction again
   * for the clauses that follow (issue #13172). The batch's own transaction does not see the statement's uncommitted writes, so a
   * label written before the CALL left the body pointed at a record it could not find. IN TRANSACTIONS already gives up the
   * statement's atomicity, so committing these writes is consistent with it. It runs before every batch: the outer rows of a later
   * batch are produced after the earlier batches committed.
   * <p>
   * Only the implicit transaction at the first level is committed: a caller's transaction is refused with IN TRANSACTIONS before
   * anything runs (issue #11588), and a deeper level belongs to someone else. A transaction a server shares between several
   * statements stops being implicit once an earlier one wrote into it, so it is refused there too: the Postgres implicit block of
   * a pipeline does so (issue #13222), and the multi-statement simple query declares its block the client's.
   * <p>
   * It runs outside the batch's error handling: a failed commit fails the statement whatever ON ERROR says, since no batch ran.
   * <p>
   * The commit lands on the transaction context a retry loop sampled (the same context is begun again), so its commit count moves:
   * the HTTP, gRPC and MCP auto-commit wrappers, {@code Database.transaction()} and the asynchronous executor do not replay a request
   * whose writes before the CALL are durable, the conflict reaches the caller instead (issue #13223).
   * <p>
   * The existence constraints deferred for the records written before the CALL are enforced before this commit, as at any commit
   * point of the statement (issue #13221): an incomplete record fails the statement and is not committed.
   *
   * @return the set of the records the outer rows produced before must re-read, with what this commit wrote added to it
   */
  private RidHashSet commitStatementWrites(final RidHashSet committedWrites) {
    if (database.getNestedTransactions() != 1)
      return committedWrites;
    final TransactionContext tx = database.getTransactionIfExists();
    if (tx == null || !tx.isActive() || !tx.isImplicit() || tx.hasChangedNothing())
      return committedWrites;

    // The copies the outer rows produced so far hold are a version behind once this commits
    final RidHashSet written = committedWrites != null ? committedWrites : new RidHashSet();
    tx.collectWrittenRecordIds(written);

    // A record written before the CALL and still incomplete fails the statement here, before it is committed: the check at the
    // end of the statement could not take it back any more (issue #13221)
    DeferredExistenceChecks.checkBeforeCommit(database);

    final Database.TRANSACTION_ISOLATION_LEVEL isolationLevel = tx.getIsolationLevel();
    database.commit();
    database.begin(isolationLevel);
    database.getTransaction().setImplicit(true);
    return written;
  }

  /**
   * Commits the transaction of one batch, and reports the commit to the transaction under it (issue #13223). A batch runs in a
   * transaction nested in the statement's, which its commit pops: the retry loop around the request - the HTTP, gRPC and MCP
   * auto-commit wrappers, {@code Database.transaction()}, the asynchronous executor - sampled the statement's transaction and would
   * not see that anything is durable, so a conflict after the batch replayed the request and applied the batch twice. Reported as
   * {@code BATCH n} reports its boundary ({@code BatchStep}), the loop hands the conflict to the caller instead. When the batch
   * began the only transaction of the thread, its commit already moved the counter of that very context. A batch that wrote
   * nothing (a MATCH ... SET that matched no node) leaves nothing durable, so it is not reported and the block stays replayable.
   */
  private void commitBatch() {
    final TransactionContext batch = database.getTransactionIfExists();
    final boolean wroteSomething = batch != null && !batch.hasChangedNothing();
    database.commit();
    final TransactionContext survivor = database.getTransactionIfExists();
    if (wroteSomething && survivor != null && survivor != batch)
      survivor.reportBatchBoundaryOfNestedTransaction();
  }

  /**
   * The row of an outer row whose batch did not commit, under ON ERROR CONTINUE or BREAK: the outer bindings, null for every
   * variable the subquery returns, and the status when REPORT STATUS asks for it (issue #11595).
   */
  private Result rowOfUncommittedBatch(final Result outerRow, final String statusVariable, final Map<String, Object> status) {
    final ResultInternal row = withReturnedColumnsNull(outerRow);
    if (statusVariable != null)
      row.setProperty(statusVariable, status);
    return row;
  }

  /**
   * The row of an outer row the body answered no row for: the outer bindings, then {@code null} for every column the body
   * returns that the outer row does not already bind, in the order a merged row carries them (issue #11632). Before, an OPTIONAL
   * CALL passed the outer row on as it was, so the body's columns were missing instead of null, and a {@code RETURN *} after it
   * answered fewer keys than the statement declares for a zero-row result. A column the outer row binds (an imported variable a
   * body's {@code RETURN *} hands back) keeps its outer value.
   */
  private ResultInternal withReturnedColumnsNull(final Result outerRow) {
    final ResultInternal row = new ResultInternal();
    for (final String prop : outerRow.getPropertyNames())
      row.setProperty(prop, outerRow.getProperty(prop));
    for (final String column : returnedColumns())
      if (!row.hasProperty(column))
        row.setProperty(column, null);
    return row;
  }

  /**
   * The columns the body returns, by the walk {@code declaredColumns} does: the RETURN items by output name, and for a
   * {@code RETURN *} the body's scope at its RETURN. The branches of a UNION body return the same columns, under the names of the
   * first one; a unit subquery returns none. When the scope of a {@code *} is not known without running (a procedure the engine
   * does not know), only the explicit items are bound.
   */
  private String[] returnedColumns() {
    if (returnedColumns == null) {
      final CypherStatement body = subqueryClause.getInnerStatement();
      final CypherStatement first = body instanceof UnionStatement union ? union.getQueries().getFirst() : body;
      final ReturnClause innerReturn = first != null ? first.getReturnClause() : null;
      if (innerReturn == null)
        returnedColumns = new String[0];
      else {
        List<String> columns = ReturnScope.returnedColumns(first, innerReturn);
        if (columns == null) {
          columns = new ArrayList<>();
          for (final ReturnClause.ReturnItem item : innerReturn.getReturnItems())
            if (!item.isStar())
              columns.add(item.getOutputName());
        }
        returnedColumns = columns.toArray(new String[0]);
      }
    }
    return returnedColumns;
  }

  /** The value REPORT STATUS binds: whether the inner transaction started and committed, its number, and its error. */
  private static Map<String, Object> status(final boolean started, final boolean committed, final String transactionId,
      final String errorMessage) {
    final Map<String, Object> status = new LinkedHashMap<>(4);
    status.put("started", started);
    status.put("committed", committed);
    status.put("transactionId", transactionId);
    status.put("errorMessage", errorMessage);
    return status;
  }

  private int resolveBatchSize(final CommandContext context) {
    final Expression batchSizeExpr = subqueryClause.getBatchSize();
    if (batchSizeExpr == null)
      return subqueryClause.getDefaultBatchSize();

    final Object value = expressionEvaluator.evaluate(batchSizeExpr, new ResultInternal(), context);
    if (value instanceof Number number)
      return number.intValue();

    throw new CommandExecutionException("IN TRANSACTIONS batch size must be a number, got: " + value);
  }

  /**
   * Executes the inner query with the outer row as the initial seed.
   * The seed row provides variables for the inner query's WITH clause.
   * <p>
   * For unit subqueries (no RETURN clause), the inner query is consumed entirely
   * for side effects (e.g. CREATE, DELETE) and a single empty result is returned
   * so the outer row count is preserved exactly once per input row.
   * <p>
   * The caller then refreshes the outer row's bindings with {@link #refreshOuterBindings}, in the transaction the outer
   * row belongs to.
   */
  private List<Result> executeInnerQuery(final Result outerRow, final CommandContext context) {
    final CypherStatement innerStatement = subqueryClause.getInnerStatement();
    final ResultSet resultSet = executeBody(outerRow, context, CypherExecutionPlan.SEEDED_BATCH_SIZE);

    try (resultSet) {
      // Unit subquery: no RETURN clause — consume all inner rows for side effects only.
      // The outer row count must be preserved (one output row per outer row).
      if (innerStatement.getReturnClause() == null) {
        while (resultSet.hasNext())
          resultSet.next();
        return List.of(new ResultInternal());
      }

      final List<Result> results = new ArrayList<>();
      while (resultSet.hasNext())
        results.add(resultSet.next());

      return results;
    }
  }

  /**
   * After the body ran for the outer row, its {@link Document} bindings are refreshed against the transaction cache so
   * that mutations applied inside the subquery (e.g. {@code SET n.score = 2}) are visible through the outer variable.
   * This matches Neo4j semantics where node references expose the current record state rather than the snapshot captured
   * at MATCH time. See issue #4182.
   * <p>
   * Only a subquery that can write has anything to refresh. A read-only one cannot have invalidated a binding, and
   * re-reading it is not free of consequence: lookupByRID answers from the transaction cache, so the snapshot the outer
   * row was carrying is replaced by the live, shared record instance - and a write performed for a LATER row then shows
   * through the binding of an EARLIER one. That is issue #6362: inserting the identity subquery
   * {@code CALL (*) { RETURN 0 AS barrier }} after {@code MERGE ... ON MATCH SET} turned [10, 99] into [99, 99].
   */
  private void refreshOuterBindings(final Result outerRow, final CommandContext context) {
    if (!innerMayWrite)
      return;
    // A label write inside the body rewrote the node under a new type, which means a new record and a new RID: the outer
    // row is still holding the one the body deleted, and refreshing it by RID can only fail. Point it at the replacement
    // first, so the merged output row - and every clause reading through it - answers with a live record instead of a
    // gone one (issue #6977).
    LabelReplacements.of(context).redirect(outerRow);
    refreshDocumentBindings(outerRow);
    // The records the outer row holds in a list or a map, and the members of its paths, follow the body's writes too:
    // WITH n, collect(n) AS ns CALL (n) { SET n.v = 31 } RETURN head(ns).v read the value from before the body (issue
    // #11754). The same walk as before the body, with the same rule: only what the transaction wrote replaces a record
    refreshWrittenBindings(outerRow);
  }

  /**
   * A buffered row as it is served: the body of a LATER row of the same batch may have replaced, by a label write, a node
   * this row holds, after {@link #refreshOuterBindings} pointed it at the then-live record. It is served with the live one,
   * not the record that later body deleted (issue #11764). Only a body that can write replaces anything, and
   * {@code redirect} returns at once while the statement replaced nothing.
   */
  private Result liveRow(final Result row, final CommandContext context) {
    if (innerMayWrite)
      LabelReplacements.of(context).redirect(row);
    return row;
  }

  /**
   * Runs the body seeded with the outer row, {@code batchSize} rows at a time, as far as the returned result set is pulled.
   * The body is planned once for the step, and in a statement that cannot write its step chain is run again for each outer
   * row (issue #12136).
   */
  private ResultSet executeBody(final Result outerRow, final CommandContext context, final int batchSize) {
    if (innerMayWrite)
      refreshWrittenBindings(outerRow);
    if (innerPlan == null)
      innerPlan = new CypherExecutionPlan(database, subqueryClause.getInnerStatement(), parameters, database.getConfiguration(),
          null, expressionEvaluator);
    return innerPlan.executeWithSeedRow(filterSeedRow(outerRow), context, batchSize);
  }

  private void closeStreamedBody() {
    if (streamedBody != null) {
      streamedBody.close();
      streamedBody = null;
    }
  }

  @Override
  public void close() {
    closeStreamedBody();
    super.close();
  }

  /**
   * Before a body that can write runs for the outer row, points the row's nodes and relationships, the members of the paths
   * it carries, and the records nested in its lists and maps, at the copies the transaction wrote, so the body reads what the
   * runs for the earlier rows wrote (issue #11534). The body runs once per row, in order, and each run is meant to see the
   * effects of the ones before it: {@code CALL (p0) { WITH [r IN relationships(p0) | r.k] AS before ... SET r.k = 'K' }}
   * read the value from before the write of an earlier row whenever the row had been produced ahead of that write, which a
   * {@code WITH} or an unrelated aggregating subquery in front of the CALL made happen for a different set of rows, and so
   * changed what the query returned. Importing {@code relationships(p0)} as a list, or inside a map, must give the same
   * answer as importing the path. A body that cannot write keeps the snapshot the row carries: issue #6362 is why.
   * <p>
   * Only what the transaction WROTE replaces a binding, never a read-only cached copy, and a list or map is copied only
   * when one of its elements changes. While the transaction has written nothing the row is not walked at all.
   */
  private void refreshWrittenBindings(final Result row) {
    if (!(row instanceof ResultInternal mutable))
      return;
    final TransactionContext tx = database.getTransactionIfExists();
    if (tx == null || !tx.hasWrittenRecords())
      return;
    replaceRecords(mutable, WrittenCopy::of);
  }

  /**
   * Before the body of an IN TRANSACTIONS batch runs, and once its batch committed, re-reads in the row the records an earlier
   * batch of this subquery committed: the row was produced before that commit, so the copies it holds carry the version from
   * before, and a write through one of them in the next batch's transaction was refused with a
   * {@link com.arcadedb.exception.ConcurrentModificationException} (issue #12324). The nodes and relationships bound directly,
   * the members of the paths and the records nested in lists and maps are all re-read, as {@link #refreshWrittenBindings} lines
   * them up within one transaction. A record deleted by the batch keeps the copy the row holds.
   */
  private void reloadCommittedRecords(final Result row, final RidHashSet committedWrites) {
    if (committedWrites == null || committedWrites.isEmpty() || !(row instanceof ResultInternal mutable))
      return;
    replaceRecords(mutable, document -> {
      final RID rid = document.getIdentity();
      if (rid == null || !rid.isValid() || !committedWrites.contains(rid))
        return document;
      try {
        return database.lookupByRID(rid, true) instanceof Document reloaded ? reloaded : document;
      } catch (final RecordNotFoundException e) {
        return document;
      }
    });
  }

  /** Replaces, in every binding of the row, each record {@code replacement} answers another object for. */
  private static void replaceRecords(final ResultInternal row, final UnaryOperator<Document> replacement) {
    for (final String name : row.getPropertyNames()) {
      final Object value = row.getProperty(name);
      final Object replaced = replaceRecords(value, replacement);
      if (replaced != value)
        row.setProperty(name, replaced);
    }
  }

  /**
   * @return {@code value} with every record in it, the members of a path and the elements of lists and maps included, replaced
   * by what {@code replacement} answers for it, or {@code value} itself when none changed
   */
  private static Object replaceRecords(final Object value, final UnaryOperator<Document> replacement) {
    if (value instanceof Document doc)
      return replacement.apply(doc);
    if (value instanceof TraversalPath path) {
      TraversalPath live = path;
      for (final Vertex vertex : path.getVertices())
        live = withReplacement(live, vertex, replacement);
      for (final Edge edge : path.getEdges())
        live = withReplacement(live, edge, replacement);
      return live;
    }
    if (value instanceof List<?> list) {
      List<Object> copy = null;
      int i = 0;
      for (final Object element : list) {
        final Object replaced = replaceRecords(element, replacement);
        if (replaced != element && copy == null)
          copy = new ArrayList<>(list);
        if (copy != null)
          copy.set(i, replaced);
        ++i;
      }
      return copy != null ? copy : list;
    }
    if (value instanceof Map<?, ?> map) {
      Map<Object, Object> copy = null;
      for (final Map.Entry<?, ?> entry : map.entrySet()) {
        final Object element = entry.getValue();
        final Object replaced = replaceRecords(element, replacement);
        if (replaced != element) {
          if (copy == null)
            copy = new LinkedHashMap<>(map);
          copy.put(entry.getKey(), replaced);
        }
      }
      return copy != null ? copy : map;
    }
    return value;
  }

  private static TraversalPath withReplacement(final TraversalPath path, final Document member,
      final UnaryOperator<Document> replacement) {
    final Document replaced = replacement.apply(member);
    return replaced != member ? path.withRecord(member.getIdentity(), replaced) : path;
  }

  /**
   * Refreshes every {@link Document} binding on the row by re-reading the record
   * through the database. {@code lookupByRID} consults the transaction cache first,
   * so a {@link com.arcadedb.database.MutableDocument} saved by the inner subquery
   * (via {@code SET}/{@code REMOVE}) replaces the pre-mutation snapshot.
   * Deleted records are left as-is: the user will hit the normal "deleted record"
   * error path on access rather than seeing a silent {@code null}.
   */
  private void refreshDocumentBindings(final Result row) {
    if (!(row instanceof ResultInternal mutable))
      return;

    List<String> propsToRefresh = null;
    for (final String name : row.getPropertyNames()) {
      final Object value = row.getProperty(name);
      if (!(value instanceof Document doc))
        continue;
      final RID rid = doc.getIdentity();
      if (rid == null)
        continue;
      if (propsToRefresh == null)
        propsToRefresh = new ArrayList<>();
      propsToRefresh.add(name);
    }

    if (propsToRefresh == null)
      return;

    for (final String name : propsToRefresh) {
      final Document current = (Document) row.getProperty(name);
      final RID rid = current.getIdentity();
      try {
        final Record refreshed = database.lookupByRID(rid, false);
        if (refreshed instanceof Document refreshedDoc && refreshedDoc != current)
          mutable.setProperty(name, refreshedDoc);
      } catch (final RecordNotFoundException ignored) {
        // Record was deleted inside the subquery. Keep the stale binding so callers
        // can observe a non-null (now logically dangling) reference, matching the
        // pre-fix behaviour for DELETE on imported variables.
      }
    }
  }

  /**
   * Returns a seed row exposing only the outer variables that the subquery is allowed to
   * see. For {@code CALL (*)} the whole outer row is passed through minus the generated (internal) bindings; for explicit or
   * implicit imports only the declared variables are retained; otherwise an empty row is
   * returned so inner MATCH variables sharing a name with an outer variable are not
   * silently bound to the outer value (issue #3959).
   */
  private Result filterSeedRow(final Result outerRow) {
    if (importAllVariables) {
      // The outer clauses' generated bindings (  nd0, ...) are not query variables. The inner plan numbers its own
      // anonymous elements from zero, so a leaked one would pre-bind an inner anonymous pattern to the outer value
      // (issue #9205: an OPTIONAL MATCH that matched nothing turned the inner MATCH () into a bound null).
      boolean hasInternal = false;
      for (final String name : outerRow.getPropertyNames())
        if (InternalVariables.isInternal(name)) {
          hasInternal = true;
          break;
        }
      if (!hasInternal)
        return outerRow;
      final ResultInternal filtered = new ResultInternal();
      for (final String name : outerRow.getPropertyNames())
        if (!InternalVariables.isInternal(name))
          filtered.setProperty(name, outerRow.getProperty(name));
      return filtered;
    }
    if (importedVariables.isEmpty())
      return new ResultInternal();

    final ResultInternal filtered = new ResultInternal();
    for (final String name : importedVariables) {
      if (outerRow.hasProperty(name))
        filtered.setProperty(name, outerRow.getProperty(name));
    }
    return filtered;
  }

  /**
   * Merges the outer row and inner row into a single output row.
   * The outer row's properties are preserved, and inner row's properties are added, except the inner plan's generated
   * (internal) bindings, which must not replace the outer ones.
   */
  private ResultInternal mergeResults(final Result outerRow, final Result innerRow) {
    final ResultInternal merged = new ResultInternal();

    // Copy outer row properties
    for (final String prop : outerRow.getPropertyNames())
      merged.setProperty(prop, outerRow.getProperty(prop));

    // Add inner row properties (may override outer if names clash, which is correct per Cypher semantics)
    // except the inner plan's generated bindings, which would otherwise overwrite the outer clauses' own (issue #9205)
    for (final String prop : innerRow.getPropertyNames())
      if (!InternalVariables.isInternal(prop))
        merged.setProperty(prop, innerRow.getProperty(prop));

    return merged;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = "  ".repeat(Math.max(0, depth * indent));
    builder.append(ind);
    if (subqueryClause.isOptional())
      builder.append("+ OPTIONAL ");
    else
      builder.append("+ ");
    builder.append("CALL SUBQUERY { ... }");
    if (subqueryClause.isInTransactions())
      builder.append(" IN TRANSACTIONS");
    if (context.isProfiling())
      builder.append(" (").append(getCostFormatted()).append(")");
    return builder.toString();
  }
}
