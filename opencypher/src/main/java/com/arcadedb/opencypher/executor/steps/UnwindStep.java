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
package com.arcadedb.opencypher.executor.steps;

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.opencypher.ast.Expression;
import com.arcadedb.opencypher.ast.UnwindClause;
import com.arcadedb.opencypher.executor.ExpressionEvaluator;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.List;

/**
 * Execution step for UNWIND clause.
 * Expands a list into individual rows, one row for each element in the list.
 *
 * Example:
 *   UNWIND [1, 2, 3] AS x RETURN x
 *
 * This creates 3 rows:
 *   {x: 1}
 *   {x: 2}
 *   {x: 3}
 */
public class UnwindStep extends AbstractExecutionStep {
  private final UnwindClause unwindClause;
  private final ExpressionEvaluator evaluator;
  private final List<Result> expandedResults = new ArrayList<>();
  private boolean initialized = false;

  public UnwindStep(final UnwindClause unwindClause, final CommandContext context,
      final ExpressionEvaluator evaluator) {
    super(context);
    this.unwindClause = unwindClause;
    this.evaluator = evaluator;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    if (!initialized) {
      initialize(context);
      initialized = true;
    }

    final List<Result> results = new ArrayList<>();
    while (results.size() < nRecords && !expandedResults.isEmpty()) {
      results.add(expandedResults.remove(0));
    }

    return new ResultSet() {
      private int index = 0;

      @Override
      public boolean hasNext() {
        return index < results.size();
      }

      @Override
      public Result next() {
        return results.get(index++);
      }

      @Override
      public void close() {
        UnwindStep.this.close();
      }
    };
  }

  private void initialize(final CommandContext context) {
    if (prev == null) {
      // No previous step - evaluate expression in empty context
      final Expression listExpr = unwindClause.getListExpression();
      final Object listValue = evaluator.evaluate(listExpr, new ResultInternal(), context);
      expandList(listValue, new ResultInternal());
    } else {
      // Previous step exists - unwind for each input row
      final ResultSet prevResults = prev.syncPull(context, Integer.MAX_VALUE);
      while (prevResults.hasNext()) {
        final Result inputRow = prevResults.next();
        final Expression listExpr = unwindClause.getListExpression();
        final Object listValue = evaluator.evaluate(listExpr, inputRow, context);
        expandList(listValue, inputRow);
      }
    }
  }

  private void expandList(final Object listValue, final Result baseRow) {
    if (listValue instanceof List) {
      final List<?> list = (List<?>) listValue;
      for (final Object element : list) {
        final ResultInternal expandedRow = new ResultInternal();

        // Copy all properties from base row
        for (final String prop : baseRow.getPropertyNames()) {
          expandedRow.setProperty(prop, baseRow.getProperty(prop));
        }

        // Add the unwound element
        expandedRow.setProperty(unwindClause.getVariable(), element);
        expandedResults.add(expandedRow);
      }
    } else if (listValue == null) {
      // Null list produces no rows
    } else {
      // Not a list - treat as single-element list
      final ResultInternal expandedRow = new ResultInternal();
      for (final String prop : baseRow.getPropertyNames()) {
        expandedRow.setProperty(prop, baseRow.getProperty(prop));
      }
      expandedRow.setProperty(unwindClause.getVariable(), listValue);
      expandedResults.add(expandedRow);
    }
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = "  ".repeat(Math.max(0, depth * indent));
    builder.append(ind);
    builder.append("+ UNWIND ");
    builder.append(unwindClause.getListExpression().getText());
    builder.append(" AS ");
    builder.append(unwindClause.getVariable());

    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted()).append(")");
    }
    return builder.toString();
  }
}
