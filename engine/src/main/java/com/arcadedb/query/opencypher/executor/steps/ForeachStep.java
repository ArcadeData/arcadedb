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

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.opencypher.ast.ClauseEntry;
import com.arcadedb.query.opencypher.ast.CreateClause;
import com.arcadedb.query.opencypher.ast.DeleteClause;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.ForeachClause;
import com.arcadedb.query.opencypher.ast.MergeClause;
import com.arcadedb.query.opencypher.ast.RemoveClause;
import com.arcadedb.query.opencypher.ast.SetClause;
import com.arcadedb.query.opencypher.executor.CypherFunctionFactory;
import com.arcadedb.query.opencypher.executor.ExpressionEvaluator;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Execution step for FOREACH clause.
 * FOREACH iterates over a list and executes write clauses (CREATE, SET, DELETE, MERGE)
 * for each element, then passes through the original input rows unchanged.
 * <p>
 * Example: FOREACH (i IN [1, 2, 3] | CREATE (:Node {id: i}))
 */
public class ForeachStep extends AbstractExecutionStep {
  private final ForeachClause foreachClause;
  private final ExpressionEvaluator evaluator;
  private final CypherFunctionFactory functionFactory;

  public ForeachStep(final ForeachClause foreachClause, final CommandContext context,
                     final CypherFunctionFactory functionFactory) {
    super(context);
    this.foreachClause = foreachClause;
    this.functionFactory = functionFactory;
    this.evaluator = new ExpressionEvaluator(functionFactory);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final boolean hasPrevious = prev != null;

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
            // Standalone FOREACH - create single empty input row
            prevResults = new IteratorResultSet(List.of((ResultInternal) new ResultInternal()).iterator());
          }
        }

        while (buffer.size() < n && prevResults.hasNext()) {
          final Result inputRow = prevResults.next();
          executeForeach(inputRow, context);
          // Pass through the input row unchanged
          buffer.add(inputRow);
        }

        if (!prevResults.hasNext())
          finished = true;
      }

      @Override
      public void close() {
        ForeachStep.this.close();
      }
    };
  }

  /**
   * Executes the FOREACH body for a single input row.
   * Evaluates the list expression, then for each element, executes the inner clauses.
   */
  private void executeForeach(final Result inputRow, final CommandContext context) {
    final Expression listExpr = foreachClause.getListExpression();
    final Object listValue = evaluator.evaluate(listExpr, inputRow, context);

    if (listValue == null)
      return;

    // Convert to iterable
    final Iterable<?> iterable;
    if (listValue instanceof Collection)
      iterable = (Collection<?>) listValue;
    else if (listValue instanceof Iterable)
      iterable = (Iterable<?>) listValue;
    else
      iterable = List.of(listValue);

    final String variable = foreachClause.getVariable();

    for (final Object element : iterable) {
      // Create a new result row with the iteration variable bound
      final ResultInternal iterationRow = new ResultInternal();

      // Copy all properties from input row
      for (final String prop : inputRow.getPropertyNames())
        iterationRow.setProperty(prop, inputRow.getProperty(prop));

      // Bind the iteration variable
      iterationRow.setProperty(variable, element);

      // Execute each inner clause
      for (final ClauseEntry clauseEntry : foreachClause.getInnerClauses())
        executeInnerClause(clauseEntry, iterationRow, context);
    }
  }

  /**
   * Executes a single inner clause (CREATE, SET, DELETE, MERGE, REMOVE, nested FOREACH).
   */
  private void executeInnerClause(final ClauseEntry clauseEntry, final ResultInternal iterationRow,
                                  final CommandContext context) {
    // Create a step that provides the iteration row as input
    final AbstractExecutionStep inputStep = new AbstractExecutionStep(context) {
      private boolean consumed = false;

      @Override
      public ResultSet syncPull(final CommandContext ctx, final int nRecords) {
        if (consumed)
          return new IteratorResultSet(List.<ResultInternal>of().iterator());
        consumed = true;
        return new IteratorResultSet(List.of(iterationRow).iterator());
      }

      @Override
      public String prettyPrint(final int depth, final int indent) {
        return "  ".repeat(Math.max(0, depth * indent)) + "+ FOREACH INPUT";
      }
    };

    AbstractExecutionStep step = null;
    switch (clauseEntry.getType()) {
      case CREATE:
        final CreateClause createClause = clauseEntry.getTypedClause();
        step = new CreateStep(createClause, context, functionFactory);
        break;
      case SET:
        final SetClause setClause = clauseEntry.getTypedClause();
        step = new SetStep(setClause, context, functionFactory);
        break;
      case DELETE:
        final DeleteClause deleteClause = clauseEntry.getTypedClause();
        step = new DeleteStep(deleteClause, context);
        break;
      case MERGE:
        final MergeClause mergeClause = clauseEntry.getTypedClause();
        step = new MergeStep(mergeClause, context, functionFactory);
        break;
      case REMOVE:
        final RemoveClause removeClause = clauseEntry.getTypedClause();
        step = new RemoveStep(removeClause, context);
        break;
      case FOREACH:
        final ForeachClause nestedForeach = clauseEntry.getTypedClause();
        step = new ForeachStep(nestedForeach, context, functionFactory);
        break;
      default:
        return;
    }

    step.setPrevious(inputStep);

    // Execute the step and consume results to trigger side effects
    final ResultSet resultSet = step.syncPull(context, 100);
    while (resultSet.hasNext())
      resultSet.next();
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = "  ".repeat(Math.max(0, depth * indent));
    builder.append(ind);
    builder.append("+ FOREACH (");
    builder.append(foreachClause.getVariable());
    builder.append(" IN ");
    builder.append(foreachClause.getListExpression().getText());
    builder.append(")");
    if (context.isProfiling())
      builder.append(" (").append(getCostFormatted()).append(")");
    return builder.toString();
  }
}
