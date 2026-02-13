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
import com.arcadedb.query.opencypher.temporal.*;
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
import java.util.PriorityQueue;

/**
 * Execution step for ORDER BY clause.
 * Sorts results according to specified order by items.
 * <p>
 * Example: ORDER BY n.name ASC, n.age DESC
 * <p>
 * **Memory Optimization**: When LIMIT is specified, uses Top-K algorithm with a
 * bounded priority queue (O(K) memory) instead of materializing all results (O(N) memory).
 * <p>
 * Without LIMIT: Materializes all N results in memory for sorting.
 * With LIMIT K: Uses priority queue of size K, keeping only top K results.
 */
public class OrderByStep extends AbstractExecutionStep {
  private final OrderByClause       orderByClause;
  private final ExpressionEvaluator evaluator;
  private final Integer             limit; // Downstream LIMIT value for Top-K optimization

  public OrderByStep(final OrderByClause orderByClause, final CommandContext context) {
    this(orderByClause, context, null, null);
  }

  public OrderByStep(final OrderByClause orderByClause, final CommandContext context,
                     final CypherFunctionFactory functionFactory) {
    this(orderByClause, context, functionFactory, null);
  }

  /**
   * Creates an OrderByStep with optional LIMIT for Top-K optimization.
   *
   * @param orderByClause    ORDER BY clause defining sort order
   * @param context          command context
   * @param functionFactory  function factory for expression evaluation
   * @param limit            downstream LIMIT value (null if no LIMIT present)
   */
  public OrderByStep(final OrderByClause orderByClause, final CommandContext context,
                     final CypherFunctionFactory functionFactory, final Integer limit) {
    super(context);
    this.orderByClause = orderByClause;
    this.evaluator = functionFactory != null ? new ExpressionEvaluator(functionFactory) : null;
    this.limit = limit;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious("OrderByStep requires a previous step");

    return new ResultSet() {
      private List<Result> sortedResults = null;
      private int          currentIndex  = 0;

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
       * Uses Top-K algorithm with bounded priority queue when LIMIT is present.
       */
      private void materializeAndSort() {
        final long begin = context.isProfiling() ? System.nanoTime() : 0;
        try {
          if (context.isProfiling())
            rowCount++;

          // Top-K optimization: Use bounded priority queue when LIMIT is present
          if (limit != null && limit > 0 && !orderByClause.isEmpty()) {
            sortedResults = materializeTopK(limit);
          } else {
            // Standard sorting: materialize all results
            sortedResults = new ArrayList<>();
            final ResultSet prevResults = prev.syncPull(context, Integer.MAX_VALUE);
            while (prevResults.hasNext()) {
              sortedResults.add(prevResults.next());
            }

            // Sort results according to ORDER BY clause
            if (!orderByClause.isEmpty()) {
              sortedResults.sort(createComparator());
            }
          }
        } finally {
          if (context.isProfiling())
            cost += (System.nanoTime() - begin);
        }
      }

      /**
       * Top-K algorithm: Maintains a bounded priority queue of size K.
       * Memory: O(K) instead of O(N) where K << N
       *
       * CRITICAL: Pulls results in batches instead of Integer.MAX_VALUE to avoid
       * causing upstream steps (like OptionalMatchStep) to materialize everything.
       *
       * Algorithm:
       * 1. Create max-heap (priority queue with reversed comparator) of size K
       * 2. For each input row (pulled in batches):
       *    - If heap size < K: add row
       *    - If heap size = K: compare with top element
       *      - If new row is better (smaller), remove top and add new row
       *      - Otherwise, discard new row
       * 3. Extract all K elements and reverse order (heap is max-heap for min-K)
       *
       * Example: ORDER BY score DESC LIMIT 10
       * - Comparator orders DESC (high scores first)
       * - Reversed comparator creates min-heap (low scores at top)
       * - Keeps top 10 highest scores, discards rest
       *
       * @param k the limit value (top K results to return)
       * @return list of top K results in correct sort order
       */
      private List<Result> materializeTopK(final int k) {
        final Comparator<Result> comparator = createComparator();
        // Reverse comparator for max-heap behavior (worst elements at top)
        final Comparator<Result> reversedComparator = comparator.reversed();

        // Priority queue with reversed comparator: worst elements bubble to top
        final PriorityQueue<Result> topK = new PriorityQueue<>(Math.min(k + 1, 1000), reversedComparator);

        // Pull results in batches (NOT Integer.MAX_VALUE!) to avoid forcing
        // upstream steps to materialize everything at once
        final int batchSize = Math.max(1000, k * 10);  // Pull 10x limit or 1000, whichever is larger
        boolean hasMore = true;

        while (hasMore) {
          final ResultSet prevResults = prev.syncPull(context, batchSize);
          hasMore = false;

          while (prevResults.hasNext()) {
            final Result row = prevResults.next();
            hasMore = true;  // We got at least one result in this batch

            if (topK.size() < k) {
              // Haven't reached K elements yet, add unconditionally
              topK.offer(row);
            } else {
              // Heap is full, check if new element is better than worst element
              final Result worst = topK.peek();
              if (comparator.compare(row, worst) < 0) {
                // New row is better than worst row, replace it
                topK.poll();  // Remove worst
                topK.offer(row);  // Add new
              }
              // Otherwise discard the row (it's worse than our current top K)
            }
          }
        }

        // Extract results from heap and reverse to get correct sort order
        final List<Result> results = new ArrayList<>(topK.size());
        while (!topK.isEmpty()) {
          results.add(topK.poll());
        }

        // Heap extracts in reverse order, so reverse the list to get correct order
        java.util.Collections.reverse(results);

        return results;
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
        // First check if the expression text matches a property name in the result
        // This handles ORDER BY on aliased/computed columns (e.g., count(*), n.division)
        final String expression = item.getExpression();
        if (expression != null && result.getPropertyNames().contains(expression))
          return convertFromStorage(result.getProperty(expression));

        // If we have a parsed Expression AST, use ExpressionEvaluator for full expression support
        final Expression exprAST = item.getExpressionAST();
        if (exprAST != null && evaluator != null)
          return evaluator.evaluate(exprAST, result, context);
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
        if (v instanceof com.arcadedb.query.opencypher.traversal.TraversalPath) return 4;
        if (v instanceof String) return 5;
        if (v instanceof Boolean) return 6;
        if (v instanceof Number) return 7;
        if (v instanceof CypherTemporalValue) return 8;
        return 9; // other types
      }

      @Override
      public void close() {
        OrderByStep.this.close();
      }
    };
  }
}
