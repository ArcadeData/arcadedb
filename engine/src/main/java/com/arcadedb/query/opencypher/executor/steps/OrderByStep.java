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
import com.arcadedb.query.opencypher.temporal.*;
import com.arcadedb.query.opencypher.ast.OrderByClause;
import com.arcadedb.query.opencypher.executor.CypherFunctionFactory;
import com.arcadedb.query.opencypher.executor.ExpressionEvaluator;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import java.time.LocalDate;
import java.time.LocalDateTime;
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
            return convertFromStorage(((Vertex) obj).get(parts[1]));
          else if (obj instanceof Edge)
            return convertFromStorage(((Edge) obj).get(parts[1]));
        }

        return convertFromStorage(result.getProperty(expression));
      }

      /**
       * Convert ArcadeDB-stored values back to Cypher temporal types for proper comparison.
       * Duration, LocalTime, and Time are stored as Strings because ArcadeDB
       * doesn't have native binary types for them.
       */
      private static Object convertFromStorage(final Object value) {
        // Handle collections (lists/arrays of temporal values)
        if (value instanceof java.util.Collection<?> collection) {
          final java.util.List<Object> converted = new java.util.ArrayList<>(collection.size());
          for (final Object item : collection) {
            converted.add(convertFromStorage(item));
          }
          return converted;
        }
        if (value instanceof Object[] array) {
          final Object[] converted = new Object[array.length];
          for (int i = 0; i < array.length; i++) {
            converted[i] = convertFromStorage(array[i]);
          }
          return converted;
        }

        // Handle single values
        if (value instanceof LocalDate ld)
          return new CypherDate(ld);
        if (value instanceof LocalDateTime ldt)
          return new CypherLocalDateTime(ldt);
        if (value instanceof String str) {
          // Duration strings start with P (ISO-8601)
          if (str.length() > 1 && str.charAt(0) == 'P') {
            try {
              return CypherDuration.parse(str);
            } catch (final Exception ignored) {
              // Not a valid duration string
            }
          }
          // DateTime strings: contain 'T' with date part before it and timezone/offset
          final int tIdx = str.indexOf('T');
          if (tIdx >= 4 && tIdx < str.length() - 1 && Character.isDigit(str.charAt(0))) {
            try {
              return CypherDateTime.parse(str);
            } catch (final Exception ignored) {
              // Not a valid datetime string
            }
          }
          // Time strings: HH:MM:SS[.nanos][+/-offset] or HH:MM:SS[.nanos]Z
          if (str.length() >= 8 && str.charAt(2) == ':' && str.charAt(5) == ':') {
            // Check if it has a timezone offset
            final boolean hasOffset = str.contains("+") || str.contains("-") || str.endsWith("Z");
            if (hasOffset) {
              try {
                return CypherTime.parse(str);
              } catch (final Exception ignored) {
                // Not a valid time string
              }
            } else {
              try {
                return CypherLocalTime.parse(str);
              } catch (final Exception ignored) {
                // Not a valid local time string
              }
            }
          }
        }
        return value;
      }

      /**
       * Compares two values for sorting.
       * In Cypher, null is considered the largest value: nulls sort last in ASC, first in DESC.
       * NaN sorts between null and all other values.
       * Type ordering: Map < Node < Relationship < List < Path < String < Boolean < Number < Temporal
       */
      @SuppressWarnings({"unchecked", "rawtypes"})
      private int compareValues(final Object v1, final Object v2) {
        if (v1 == null && v2 == null)
          return 0;
        if (v1 == null)
          return 1; // null is the largest value
        if (v2 == null)
          return -1;

        // Handle NaN: NaN sorts just before null
        final boolean nan1 = v1 instanceof Double && Double.isNaN((Double) v1) ||
            v1 instanceof Float && Float.isNaN((Float) v1);
        final boolean nan2 = v2 instanceof Double && Double.isNaN((Double) v2) ||
            v2 instanceof Float && Float.isNaN((Float) v2);
        if (nan1 && nan2) return 0;
        if (nan1) return 1;
        if (nan2) return -1;

        // Compare lists element-by-element
        if (v1 instanceof List && v2 instanceof List) {
          final List<?> l1 = (List<?>) v1;
          final List<?> l2 = (List<?>) v2;
          final int minSize = Math.min(l1.size(), l2.size());
          for (int i = 0; i < minSize; i++) {
            final int cmp = compareValues(l1.get(i), l2.get(i));
            if (cmp != 0) return cmp;
          }
          return Integer.compare(l1.size(), l2.size());
        }

        // Compare booleans: false < true
        if (v1 instanceof Boolean && v2 instanceof Boolean)
          return Boolean.compare((Boolean) v1, (Boolean) v2);

        // Compare temporal values
        if (v1 instanceof CypherTemporalValue && v2 instanceof CypherTemporalValue) {
          try {
            return ((CypherTemporalValue) v1).compareTo((CypherTemporalValue) v2);
          } catch (final IllegalArgumentException e) {
            // Different temporal types — fall through to type rank
          }
        }

        // Same-type Comparable comparison
        if (v1.getClass().equals(v2.getClass()) && v1 instanceof Comparable) {
          try {
            return ((Comparable) v1).compareTo(v2);
          } catch (final ClassCastException e) {
            // Fall through
          }
        }

        // Cross-type number comparison
        if (v1 instanceof Number && v2 instanceof Number)
          return Double.compare(((Number) v1).doubleValue(), ((Number) v2).doubleValue());

        // Different types: order by type rank
        final int rank1 = typeRank(v1);
        final int rank2 = typeRank(v2);
        if (rank1 != rank2)
          return Integer.compare(rank1, rank2);

        // Same rank, try Comparable
        if (v1 instanceof Comparable && v2 instanceof Comparable) {
          try {
            return ((Comparable) v1).compareTo(v2);
          } catch (final ClassCastException e) {
            // Fall through
          }
        }

        return v1.toString().compareTo(v2.toString());
      }

      private int typeRank(final Object v) {
        if (v instanceof java.util.Map) return 0;
        if (v instanceof Vertex) return 1;
        if (v instanceof Edge) return 2;
        if (v instanceof List) return 3;
        if (v instanceof String) return 5;
        if (v instanceof Boolean) return 6;
        if (v instanceof Number) return 7;
        return 8; // temporal and other types
      }

      @Override
      public void close() {
        OrderByStep.this.close();
      }
    };
  }
}
