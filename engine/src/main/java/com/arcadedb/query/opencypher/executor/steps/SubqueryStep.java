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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
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
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

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
   * Returning {@code null} signals "import all" (used for {@code CALL (*)}).
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
        if (branchImports != null)
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
      if (!(item.getExpression() instanceof VariableExpression var))
        continue;
      final String name = var.getVariableName();
      if (STAR_SCOPE.equals(name))
        continue; // WITH * handled by caller if desired; treat as non-importing here
      final String alias = item.getAlias();
      if (alias != null && !alias.equals(name))
        continue; // aliased projection, not a pure import
      imports.add(name);
    }
    return imports;
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
        return buffer.get(bufferIndex++);
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
                cost += (System.nanoTime() - begin);
            }
            continue;
          }

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

            // Execute the inner query seeded with the outer row
            final List<Result> innerResults = executeInnerQuery(currentOuterRow, context);

            if (innerResults.isEmpty()) {
              if (subqueryClause.isOptional()) {
                // OPTIONAL CALL - produce the outer row with nulls for inner columns
                buffer.add(currentOuterRow);
              }
              // Non-optional: no output rows for this outer row
              currentInnerResults = null;
            } else {
              currentInnerResults = innerResults.iterator();
            }
          } finally {
            if (context.isProfiling())
              cost += (System.nanoTime() - begin);
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

    return new ResultSet() {
      private ResultSet prevResults = null;
      private final List<Result> buffer = new ArrayList<>();
      private int bufferIndex = 0;
      private boolean finished = false;

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
        return buffer.get(bufferIndex++);
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

        // Collect a batch of outer rows
        final List<Result> batch = new ArrayList<>();
        while (batch.size() < batchSize && prevResults.hasNext())
          batch.add(prevResults.next());

        if (batch.isEmpty()) {
          finished = true;
          return;
        }

        // Execute the batch in its own transaction
        database.begin();
        try {
          for (final Result outerRow : batch) {
            final long begin = context.isProfiling() ? System.nanoTime() : 0;
            try {
              if (context.isProfiling())
                rowCount++;

              final List<Result> innerResults = executeInnerQuery(outerRow, context);

              if (innerResults.isEmpty()) {
                if (subqueryClause.isOptional())
                  buffer.add(outerRow);
              } else {
                for (final Result innerRow : innerResults)
                  buffer.add(mergeResults(outerRow, innerRow));
              }
            } finally {
              if (context.isProfiling())
                cost += (System.nanoTime() - begin);
            }
          }
          database.commit();
        } catch (final Exception e) {
          database.rollbackAllNested();
          throw e;
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
   */
  private List<Result> executeInnerQuery(final Result outerRow, final CommandContext context) {
    final CypherStatement innerStatement = subqueryClause.getInnerStatement();

    final CypherExecutionPlan innerPlan = new CypherExecutionPlan(
        database, innerStatement, parameters, database.getConfiguration(), null, expressionEvaluator);

    final ResultSet resultSet = innerPlan.executeWithSeedRow(filterSeedRow(outerRow));

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

  /**
   * Returns a seed row exposing only the outer variables that the subquery is allowed to
   * see. For {@code CALL (*)} the whole outer row is passed through; for explicit or
   * implicit imports only the declared variables are retained; otherwise an empty row is
   * returned so inner MATCH variables sharing a name with an outer variable are not
   * silently bound to the outer value (issue #3959).
   */
  private Result filterSeedRow(final Result outerRow) {
    if (importAllVariables)
      return outerRow;
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
   * The outer row's properties are preserved, and inner row's properties are added.
   */
  private ResultInternal mergeResults(final Result outerRow, final Result innerRow) {
    final ResultInternal merged = new ResultInternal();

    // Copy outer row properties
    for (final String prop : outerRow.getPropertyNames())
      merged.setProperty(prop, outerRow.getProperty(prop));

    // Add inner row properties (may override outer if names clash, which is correct per Cypher semantics)
    for (final String prop : innerRow.getPropertyNames())
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
