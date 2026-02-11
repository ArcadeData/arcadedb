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
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.UnwindClause;
import com.arcadedb.query.opencypher.executor.CypherFunctionFactory;
import com.arcadedb.query.opencypher.executor.ExpressionEvaluator;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Execution step for UNWIND clause.
 * UNWIND expands a list into individual rows, one for each element.
 * <p>
 * Examples:
 * - UNWIND [1, 2, 3] AS x RETURN x → produces 3 rows with x=1, x=2, x=3
 * - UNWIND range(1, 10) AS num RETURN num → produces 10 rows
 * - MATCH (n:Person) UNWIND n.hobbies AS hobby RETURN n.name, hobby
 * - WITH [1, 2] AS list UNWIND list AS x RETURN x
 * <p>
 * The UNWIND clause:
 * - Evaluates the list expression for each input row
 * - For each element in the list, creates a new output row
 * - The new row contains all properties from the input row PLUS the unwound variable
 * - If the list is empty or null, no rows are produced for that input row
 */
public class UnwindStep extends AbstractExecutionStep {
  private final UnwindClause unwindClause;
  private final ExpressionEvaluator evaluator;

  public UnwindStep(final UnwindClause unwindClause, final CommandContext context,
                    final CypherFunctionFactory functionFactory) {
    super(context);
    this.unwindClause = unwindClause;
    this.evaluator = new ExpressionEvaluator(functionFactory);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    // If no previous step, create a single empty result to unwind from
    final boolean hasPrevious = prev != null;

    return new ResultSet() {
      private ResultSet prevResults = null;
      private final List<Result> buffer = new ArrayList<>();
      private int bufferIndex = 0;
      private boolean finished = false;
      private Iterator<?> currentListIterator = null;
      private Result currentInputRow = null;

      @Override
      public boolean hasNext() {
        if (bufferIndex < buffer.size()) {
          return true;
        }

        if (finished) {
          return false;
        }

        // Fetch more results
        fetchMore(nRecords);
        return bufferIndex < buffer.size();
      }

      @Override
      public Result next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return buffer.get(bufferIndex++);
      }

      private void fetchMore(final int n) {
        buffer.clear();
        bufferIndex = 0;

        // Initialize prevResults on first call
        if (prevResults == null) {
          if (hasPrevious) {
            prevResults = prev.syncPull(context, nRecords);
          } else {
            // No previous step - create a single empty input row
            prevResults = new ResultSet() {
              private boolean consumed = false;

              @Override
              public boolean hasNext() {
                return !consumed;
              }

              @Override
              public Result next() {
                if (consumed) {
                  throw new NoSuchElementException();
                }
                consumed = true;
                return new ResultInternal();
              }

              @Override
              public void close() {
              }
            };
          }
        }

        // Produce up to n output rows
        while (buffer.size() < n) {
          // If we're iterating through a list, continue unwinding it
          if (currentListIterator != null && currentListIterator.hasNext()) {
            final long begin = context.isProfiling() ? System.nanoTime() : 0;
            try {
              if (context.isProfiling())
                rowCount++;

              final Object element = currentListIterator.next();
              final ResultInternal unwoundResult = createUnwoundResult(currentInputRow, element);
              buffer.add(unwoundResult);
            } finally {
              if (context.isProfiling())
                cost += (System.nanoTime() - begin);
            }
            continue;
          }

          // Need a new input row
          if (!prevResults.hasNext()) {
            finished = true;
            break;
          }

          currentInputRow = prevResults.next();
          final long begin = context.isProfiling() ? System.nanoTime() : 0;
          try {
            // Evaluate the list expression
            final Expression listExpr = unwindClause.getListExpression();
            final Object listValue = evaluator.evaluate(listExpr, currentInputRow, context);

            // Convert to iterable
            if (listValue == null) {
              // Null list - produces no output rows for this input
              currentListIterator = null;
              continue;
            }

            if (listValue instanceof Collection) {
              currentListIterator = ((Collection<?>) listValue).iterator();
            } else if (listValue instanceof Iterable) {
              currentListIterator = ((Iterable<?>) listValue).iterator();
            } else if (listValue.getClass().isArray()) {
              // Convert array to list
              final List<Object> list = new ArrayList<>();
              if (listValue instanceof Object[]) {
                for (final Object obj : (Object[]) listValue) {
                  list.add(obj);
                }
              } else if (listValue instanceof int[]) {
                for (final int i : (int[]) listValue) {
                  list.add(i);
                }
              } else if (listValue instanceof long[]) {
                for (final long i : (long[]) listValue) {
                  list.add(i);
                }
              } else if (listValue instanceof double[]) {
                for (final double i : (double[]) listValue) {
                  list.add(i);
                }
              } else if (listValue instanceof boolean[]) {
                for (final boolean i : (boolean[]) listValue) {
                  list.add(i);
                }
              }
              currentListIterator = list.iterator();
            } else {
              // Not a list - treat as single element
              final List<Object> singleElementList = new ArrayList<>();
              singleElementList.add(listValue);
              currentListIterator = singleElementList.iterator();
            }

            // If the list is empty, continue to next input row
            if (!currentListIterator.hasNext()) {
              currentListIterator = null;
            }
          } finally {
            if (context.isProfiling())
              cost += (System.nanoTime() - begin);
          }
        }
      }

      @Override
      public void close() {
        UnwindStep.this.close();
      }
    };
  }

  /**
   * Creates an output row by copying the input row and adding the unwound variable.
   */
  private ResultInternal createUnwoundResult(final Result inputRow, final Object unwoundValue) {
    final ResultInternal result = new ResultInternal();

    // Copy all properties from input row
    for (final String prop : inputRow.getPropertyNames()) {
      result.setProperty(prop, inputRow.getProperty(prop));
    }

    // Add the unwound variable
    result.setProperty(unwindClause.getVariable(), unwoundValue);

    return result;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ UNWIND ");
    builder.append(unwindClause.getListExpression().getText());
    builder.append(" AS ");
    builder.append(unwindClause.getVariable());

    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted());
      if (rowCount > 0)
        builder.append(", ").append(getRowCountFormatted());
      builder.append(")");
    }

    return builder.toString();
  }

  private static String getIndent(final int depth, final int indent) {
    return "  ".repeat(Math.max(0, depth * indent));
  }
}
