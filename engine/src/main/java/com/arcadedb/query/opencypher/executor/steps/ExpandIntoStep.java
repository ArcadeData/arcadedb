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
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.executor.operators.ExpandInto;
import com.arcadedb.query.opencypher.executor.operators.PhysicalOperator;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Execution step wrapper for ExpandInto physical operator (semi-join optimization).
 * Checks for existence of relationships between two already-bound vertices.
 * <p>
 * This is 5-10x faster than ExpandAll when both endpoints are known.
 * <p>
 * Example:
 * <pre>
 * MATCH (a:Person {id: 1}), (b:Person {id: 2})
 * MATCH (a)-[r:KNOWS]->(b)
 * RETURN r
 * </pre>
 * <p>
 * Uses Vertex.isConnectedTo() for O(m) RID-level existence checks instead of
 * loading and iterating through all edges.
 */
public class ExpandIntoStep extends AbstractExecutionStep {
  private final String sourceVariable;
  private final String targetVariable;
  private final String edgeVariable;
  private final Direction direction;
  private final String[] edgeTypes;
  private final double estimatedCost;
  private final long estimatedCardinality;

  /**
   * Creates an expand-into step.
   *
   * @param sourceVariable       variable name for source vertex (must be already bound)
   * @param targetVariable       variable name for target vertex (must be already bound)
   * @param edgeVariable         variable name for relationship (can be null)
   * @param direction            direction of relationship (OUT, IN, BOTH)
   * @param edgeTypes            edge types to match (null for any)
   * @param estimatedCost        estimated cost of operation
   * @param estimatedCardinality estimated number of rows
   * @param context              command context
   */
  public ExpandIntoStep(final String sourceVariable, final String targetVariable,
                       final String edgeVariable, final Direction direction,
                       final String[] edgeTypes, final double estimatedCost,
                       final long estimatedCardinality, final CommandContext context) {
    super(context);
    this.sourceVariable = sourceVariable;
    this.targetVariable = targetVariable;
    this.edgeVariable = edgeVariable;
    this.direction = direction;
    this.edgeTypes = edgeTypes;
    this.estimatedCost = estimatedCost;
    this.estimatedCardinality = estimatedCardinality;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious("ExpandIntoStep requires a previous step with both source and target vertices bound");

    return new ResultSet() {
      private ResultSet operatorResults = null;
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

        // Initialize operator on first call
        if (operatorResults == null) {
          // Create a wrapper physical operator that pulls from previous step
          final PhysicalOperator wrapperOperator = new PhysicalOperator() {
            @Override
            public ResultSet execute(final CommandContext ctx, final int nRecs) {
              return prev.syncPull(ctx, nRecs);
            }

            @Override
            public double getEstimatedCost() {
              return 0; // Cost of previous step
            }

            @Override
            public long getEstimatedCardinality() {
              return 0; // Cardinality of previous step
            }

            @Override
            public String getOperatorType() {
              return "PreviousStep";
            }

            @Override
            public String explain(final int depth) {
              return "Previous execution steps";
            }

            @Override
            public PhysicalOperator getChild() {
              return null;
            }

            @Override
            public void setChild(final PhysicalOperator child) {
              throw new UnsupportedOperationException();
            }
          };

          // Create ExpandInto operator with wrapper as child
          final ExpandInto operator = new ExpandInto(
              wrapperOperator,
              sourceVariable,
              targetVariable,
              edgeVariable,
              direction,
              edgeTypes,
              estimatedCost,
              estimatedCardinality
          );

          operatorResults = operator.execute(context, n);
        }

        // Fetch results from operator
        while (buffer.size() < n && operatorResults.hasNext()) {
          final long begin = context.isProfiling() ? System.nanoTime() : 0;
          try {
            if (context.isProfiling())
              rowCount++;

            buffer.add(operatorResults.next());
          } finally {
            if (context.isProfiling())
              cost += (System.nanoTime() - begin);
          }
        }

        if (!operatorResults.hasNext()) {
          finished = true;
        }
      }

      @Override
      public void close() {
        if (operatorResults != null) {
          operatorResults.close();
        }
        ExpandIntoStep.this.close();
      }
    };
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ EXPAND INTO ");
    builder.append("(").append(sourceVariable).append(")");
    builder.append("-[");
    if (edgeVariable != null) {
      builder.append(edgeVariable);
    }
    if (edgeTypes != null && edgeTypes.length > 0) {
      builder.append(":").append(String.join("|", edgeTypes));
    }
    builder.append("]-");
    builder.append(direction == Direction.OUT ? ">" : direction == Direction.IN ? "<" : "");
    builder.append("(").append(targetVariable).append(")");
    builder.append(" [cost=").append(String.format("%.2f", estimatedCost));
    builder.append(", rows=").append(estimatedCardinality);
    builder.append("] ⭐ SEMI-JOIN");
    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted()).append(")");
      if (rowCount > 0)
        builder.append(", ").append(getRowCountFormatted());
      builder.append(")");
    }
    return builder.toString();
  }

  private static String getIndent(final int depth, final int indent) {
    return "  ".repeat(Math.max(0, depth * indent));
  }

  public String getSourceVariable() {
    return sourceVariable;
  }

  public String getTargetVariable() {
    return targetVariable;
  }

  public String getEdgeVariable() {
    return edgeVariable;
  }

  public Direction getDirection() {
    return direction;
  }

  public String[] getEdgeTypes() {
    return edgeTypes;
  }
}
