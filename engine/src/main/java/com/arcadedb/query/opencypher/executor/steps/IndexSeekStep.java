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
import com.arcadedb.query.opencypher.executor.operators.NodeIndexSeek;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Execution step wrapper for NodeIndexSeek physical operator.
 * Uses an index to efficiently find vertices matching an equality predicate.
 * <p>
 * Example: MATCH (n:Person {id: 123}) RETURN n
 * - Uses index on Person.id for O(log N + M) lookup
 * - 10-1000x faster than full table scan for selective queries
 */
public class IndexSeekStep extends AbstractExecutionStep {
  private final String variable;
  private final String label;
  private final String propertyName;
  private final Object propertyValue;
  private final String indexName;
  private final double estimatedCost;
  private final long estimatedCardinality;

  /**
   * Creates an index seek step.
   *
   * @param variable             variable name to bind vertices to
   * @param label                vertex type/label to search
   * @param propertyName         property to match on
   * @param propertyValue        value to match
   * @param indexName            name of index to use
   * @param estimatedCost        estimated cost of operation
   * @param estimatedCardinality estimated number of rows
   * @param context              command context
   */
  public IndexSeekStep(final String variable, final String label, final String propertyName,
                      final Object propertyValue, final String indexName,
                      final double estimatedCost, final long estimatedCardinality,
                      final CommandContext context) {
    super(context);
    this.variable = variable;
    this.label = label;
    this.propertyName = propertyName;
    this.propertyValue = propertyValue;
    this.indexName = indexName;
    this.estimatedCost = estimatedCost;
    this.estimatedCardinality = estimatedCardinality;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final boolean hasInput = prev != null;

    return new ResultSet() {
      private ResultSet prevResults = null;
      private ResultSet operatorResults = null;
      private final List<Result> buffer = new ArrayList<>();
      private int bufferIndex = 0;
      private boolean finished = false;
      private Result currentInputResult = null;

      @Override
      public boolean hasNext() {
        if (bufferIndex < buffer.size()) {
          return true;
        }

        if (finished) {
          return false;
        }

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

        if (hasInput) {
          // Chained mode: for each input result, perform index seek and merge
          if (prevResults == null) {
            prevResults = prev.syncPull(context, nRecords);
          }

          while (buffer.size() < n) {
            // If we've exhausted index results for current input, get next input
            if (operatorResults == null || !operatorResults.hasNext()) {
              if (!prevResults.hasNext()) {
                finished = true;
                break;
              }
              currentInputResult = prevResults.next();

              // Execute index seek operator
              final NodeIndexSeek operator = new NodeIndexSeek(
                  variable, label, propertyName, propertyValue, indexName,
                  estimatedCost, estimatedCardinality
              );
              operatorResults = operator.execute(context, n);
            }

            // Merge index results with input result
            if (operatorResults.hasNext()) {
              final Result seekResult = operatorResults.next();

              // Copy input result and add index seek results
              final ResultInternal result = new ResultInternal();
              if (currentInputResult != null) {
                for (final String prop : currentInputResult.getPropertyNames()) {
                  result.setProperty(prop, currentInputResult.getProperty(prop));
                }
              }

              // Add vertex from index seek
              for (final String prop : seekResult.getPropertyNames()) {
                result.setProperty(prop, seekResult.getProperty(prop));
              }

              buffer.add(result);
            }
          }
        } else {
          // Standalone mode: execute index seek operator directly
          if (operatorResults == null) {
            final NodeIndexSeek operator = new NodeIndexSeek(
                variable, label, propertyName, propertyValue, indexName,
                estimatedCost, estimatedCardinality
            );
            operatorResults = operator.execute(context, n);
          }

          // Fetch up to n results
          while (buffer.size() < n && operatorResults.hasNext()) {
            buffer.add(operatorResults.next());
          }

          if (!operatorResults.hasNext()) {
            finished = true;
          }
        }
      }

      @Override
      public void close() {
        if (operatorResults != null) {
          operatorResults.close();
        }
        IndexSeekStep.this.close();
      }
    };
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ INDEX SEEK ");
    builder.append("(").append(variable).append(":").append(label).append(")");
    builder.append(" [").append(propertyName).append("=").append(propertyValue).append("]");
    builder.append(" [index=").append(indexName);
    builder.append(", cost=").append(String.format("%.2f", estimatedCost));
    builder.append(", rows=").append(estimatedCardinality);
    builder.append("]");
    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted()).append(")");
    }
    return builder.toString();
  }

  private static String getIndent(final int depth, final int indent) {
    return "  ".repeat(Math.max(0, depth * indent));
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

  public Object getPropertyValue() {
    return propertyValue;
  }

  public String getIndexName() {
    return indexName;
  }
}
