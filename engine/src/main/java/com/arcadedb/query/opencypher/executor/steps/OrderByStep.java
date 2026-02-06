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
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.OrderByClause;
import com.arcadedb.query.opencypher.executor.CypherFunctionFactory;
import com.arcadedb.query.opencypher.executor.ExpressionEvaluator;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Execution step for ORDER BY clause.
 * Sorts results according to specified order by items.
 * <p>
 * Example: ORDER BY n.name ASC, n.age DESC
 * <p>
 * Note: This step materializes all results in memory for sorting.
 */
public class OrderByStep extends AbstractExecutionStep {
  private final OrderByClause orderByClause;
  private final ExpressionEvaluator evaluator;

  public OrderByStep(final OrderByClause orderByClause, final CommandContext context) {
    this(orderByClause, context, null);
  }

  public OrderByStep(final OrderByClause orderByClause, final CommandContext context,
                     final CypherFunctionFactory functionFactory) {
    super(context);
    this.orderByClause = orderByClause;
    this.evaluator = functionFactory != null ? new ExpressionEvaluator(functionFactory) : null;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious("OrderByStep requires a previous step");

    return new ResultSet() {
      private List<Result> sortedResults = null;
      private int currentIndex = 0;

      @Override
      public boolean hasNext() {
        if (sortedResults == null) {
          materializeAndSort();
        }
        return currentIndex < sortedResults.size();
      }

      @Override
      public Result next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return sortedResults.get(currentIndex++);
      }

      /**
       * Materializes all results from previous step and sorts them.
       */
      private void materializeAndSort() {
        sortedResults = new ArrayList<>();

        // Fetch all results from previous step
        final ResultSet prevResults = prev.syncPull(context, Integer.MAX_VALUE);
        while (prevResults.hasNext()) {
          sortedResults.add(prevResults.next());
        }

        // Sort results according to ORDER BY clause
        if (!orderByClause.isEmpty()) {
          sortedResults.sort(createComparator());
        }
      }

      /**
       * Creates a comparator based on ORDER BY items.
       */
      private Comparator<Result> createComparator() {
        return (r1, r2) -> {
          for (final OrderByClause.OrderByItem item : orderByClause.getItems()) {
            final Object v1 = extractValue(r1, item);
            final Object v2 = extractValue(r2, item);

            final int comparison = compareValues(v1, v2);
            if (comparison != 0) {
              return item.isAscending() ? comparison : -comparison;
            }
          }
          return 0;
        };
      }

      private Object extractValue(final Result result, final OrderByClause.OrderByItem item) {
        // If we have a parsed Expression AST, use ExpressionEvaluator for full expression support
        final Expression exprAST = item.getExpressionAST();
        if (exprAST != null && evaluator != null)
          return evaluator.evaluate(exprAST, result, context);

        // Fallback to string-based extraction
        final String expression = item.getExpression();
        if (expression.contains(".")) {
          final String[] parts = expression.split("\\.", 2);
          final Object obj = result.getProperty(parts[0]);
          if (obj == null)
            return null;

          if (obj instanceof Vertex)
            return ((Vertex) obj).get(parts[1]);
          else if (obj instanceof Edge)
            return ((Edge) obj).get(parts[1]);
        }

        return result.getProperty(expression);
      }

      /**
       * Compares two values for sorting.
       * In Cypher, null is considered the largest value: nulls sort last in ASC, first in DESC.
       */
      @SuppressWarnings({"unchecked", "rawtypes"})
      private int compareValues(final Object v1, final Object v2) {
        if (v1 == null && v2 == null) {
          return 0;
        }
        if (v1 == null) {
          return 1; // null is the largest value
        }
        if (v2 == null) {
          return -1;
        }

        // Both non-null
        if (v1 instanceof Comparable && v2 instanceof Comparable) {
          try {
            return ((Comparable) v1).compareTo(v2);
          } catch (final ClassCastException e) {
            // Fall through to string comparison
          }
        }

        // Fallback to string comparison
        return v1.toString().compareTo(v2.toString());
      }

      @Override
      public void close() {
        OrderByStep.this.close();
      }
    };
  }
}
