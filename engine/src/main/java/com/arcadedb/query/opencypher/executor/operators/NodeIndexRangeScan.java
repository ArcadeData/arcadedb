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
package com.arcadedb.query.opencypher.executor.operators;

import com.arcadedb.database.Identifiable;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.RangeIndex;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Physical operator that performs an index range scan for vertices.
 * Uses a range index to efficiently find vertices matching range predicates (>, <, >=, <=).
 *
 * Examples:
 * - WHERE age > 18 AND age < 65
 * - WHERE date >= $start
 * - WHERE price <= 100
 *
 * Cost: O(log N + M) where N is index size, M is matching rows
 * Cardinality: Estimated based on range selectivity (typically 10-30%)
 */
public class NodeIndexRangeScan extends AbstractPhysicalOperator {
  private final String variable;
  private final String label;
  private final String propertyName;
  private final Object lowerBound;      // null if unbounded
  private final boolean lowerInclusive;
  private final Object upperBound;      // null if unbounded
  private final boolean upperInclusive;
  private final String indexName;

  /**
   * Create a range scan operator.
   *
   * @param variable variable name to bind results to
   * @param label vertex type/label
   * @param propertyName indexed property
   * @param lowerBound lower bound value (null for unbounded)
   * @param lowerInclusive whether lower bound is inclusive (>= vs >)
   * @param upperBound upper bound value (null for unbounded)
   * @param upperInclusive whether upper bound is inclusive (<= vs <)
   * @param indexName name of the index being used
   * @param estimatedCost estimated cost from optimizer
   * @param estimatedCardinality estimated result count
   */
  public NodeIndexRangeScan(final String variable, final String label, final String propertyName,
                           final Object lowerBound, final boolean lowerInclusive,
                           final Object upperBound, final boolean upperInclusive,
                           final String indexName,
                           final double estimatedCost, final long estimatedCardinality) {
    super(estimatedCost, estimatedCardinality);
    this.variable = variable;
    this.label = label;
    this.propertyName = propertyName;
    this.lowerBound = lowerBound;
    this.lowerInclusive = lowerInclusive;
    this.upperBound = upperBound;
    this.upperInclusive = upperInclusive;
    this.indexName = indexName;
  }

  @Override
  public ResultSet execute(final CommandContext context, final int nRecords) {
    return new ResultSet() {
      private IndexCursor cursor = null;
      private final List<Result> buffer = new ArrayList<>();
      private int bufferIndex = 0;
      private boolean finished = false;

      @Override
      public boolean hasNext() {
        if (bufferIndex < buffer.size()) {
          return true;
        }

        if (finished) {
          return false;
        }

        fetchMore(nRecords > 0 ? nRecords : 100);
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

        // Initialize cursor on first call
        if (cursor == null) {
          final DocumentType type = context.getDatabase().getSchema().getType(label);
          if (type == null) {
            finished = true;
            return;
          }

          // Get the range index
          final TypeIndex typeIndex = (TypeIndex) type.getPolymorphicIndexByProperties(propertyName);
          if (typeIndex == null || !(typeIndex instanceof RangeIndex)) {
            finished = true;
            return;
          }

          final RangeIndex rangeIndex = (RangeIndex) typeIndex;

          // Determine which range method to use based on bounds
          if (lowerBound != null && upperBound != null) {
            // Both bounds specified: use range()
            final Object[] beginKeys = new Object[]{lowerBound};
            final Object[] endKeys = new Object[]{upperBound};
            cursor = rangeIndex.range(true, beginKeys, lowerInclusive, endKeys, upperInclusive);
          } else if (lowerBound != null) {
            // Only lower bound: use iterator(fromKeys)
            final Object[] fromKeys = new Object[]{lowerBound};
            cursor = rangeIndex.iterator(true, fromKeys, lowerInclusive);
          } else if (upperBound != null) {
            // Only upper bound: iterate from beginning and stop at upper bound
            // Note: This is less efficient - we iterate all and filter
            cursor = rangeIndex.iterator(true);
          } else {
            // No bounds: full scan (shouldn't happen in normal usage)
            cursor = rangeIndex.iterator(true);
          }
        }

        // Fetch up to n matching vertices
        while (buffer.size() < n && cursor.hasNext()) {
          final Identifiable identifiable = cursor.next();

          // Load the actual record from the identifiable (may be RID)
          final Vertex vertex = identifiable.asVertex();

          // If we only have upper bound, we need to manually check and stop
          if (upperBound != null && lowerBound == null) {
            final Object propertyValue = vertex.get(propertyName);
            if (propertyValue != null) {
              // Normalize numeric types to avoid ClassCastException
              final int comparison = compareValues(propertyValue, upperBound);

              if (upperInclusive) {
                if (comparison > 0) {
                  finished = true;
                  break;
                }
              } else {
                if (comparison >= 0) {
                  finished = true;
                  break;
                }
              }
            }
          }

          // Create result with vertex bound to variable
          final ResultInternal result = new ResultInternal();
          result.setProperty(variable, vertex);
          buffer.add(result);
        }

        if (!cursor.hasNext()) {
          finished = true;
        }
      }

      @Override
      public void close() {
        // IndexCursor doesn't need explicit closing
      }
    };
  }

  @Override
  public String getOperatorType() {
    return "NodeIndexRangeScan";
  }

  @Override
  public String explain(final int depth) {
    final StringBuilder sb = new StringBuilder();
    final String indent = getIndent(depth);

    sb.append(indent).append("+ NodeIndexRangeScan");
    sb.append("(").append(variable).append(":").append(label).append(")");
    sb.append(" [index=").append(indexName);
    sb.append(", ").append(propertyName);

    // Build range description
    if (lowerBound != null && upperBound != null) {
      sb.append(" ").append(lowerInclusive ? ">=" : ">").append(" ").append(lowerBound);
      sb.append(" AND ").append(upperInclusive ? "<=" : "<").append(" ").append(upperBound);
    } else if (lowerBound != null) {
      sb.append(" ").append(lowerInclusive ? ">=" : ">").append(" ").append(lowerBound);
    } else if (upperBound != null) {
      sb.append(" ").append(upperInclusive ? "<=" : "<").append(" ").append(upperBound);
    }

    sb.append(", cost=").append(String.format("%.2f", estimatedCost));
    sb.append(", rows=").append(estimatedCardinality);
    sb.append("]\n");

    return sb.toString();
  }

  public String getVariable() {
    return variable;
  }

  public String getLabel() {
    return label;
  }

  public String getPropertyName() {
    return propertyName;
  }

  public Object getLowerBound() {
    return lowerBound;
  }

  public boolean isLowerInclusive() {
    return lowerInclusive;
  }

  public Object getUpperBound() {
    return upperBound;
  }

  public boolean isUpperInclusive() {
    return upperInclusive;
  }

  public String getIndexName() {
    return indexName;
  }

  /**
   * Compares two values, handling numeric type coercion.
   * Converts both values to comparable numbers to avoid ClassCastException.
   */
  @SuppressWarnings("unchecked")
  private static int compareValues(final Object value1, final Object value2) {
    // Handle numeric comparisons with type coercion
    if (value1 instanceof Number && value2 instanceof Number) {
      final Number n1 = (Number) value1;
      final Number n2 = (Number) value2;

      // Compare as doubles to handle mixed types (Integer, Long, Float, Double)
      return Double.compare(n1.doubleValue(), n2.doubleValue());
    }

    // For non-numeric types, use standard comparison
    if (value1 instanceof Comparable) {
      return ((Comparable<Object>) value1).compareTo(value2);
    }

    throw new IllegalArgumentException("Cannot compare non-comparable types: " +
        value1.getClass() + " and " + value2.getClass());
  }
}
