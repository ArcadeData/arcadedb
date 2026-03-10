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

import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;

/**
 * CSR-backed ExpandInto that uses binary search on sorted CSR arrays for O(log(degree))
 * connectivity checks, without loading any edge objects or traversing linked lists.
 * <p>
 * Selected when both source and target are bound, the edge variable is not captured,
 * and a matching {@link GraphTraversalProvider} is available.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GAVExpandInto extends AbstractPhysicalOperator {
  private final GraphTraversalProvider provider;
  private final String sourceVariable;
  private final String targetVariable;
  private final Direction direction;
  private final String[] edgeTypes;

  public GAVExpandInto(final PhysicalOperator child, final GraphTraversalProvider provider,
                      final String sourceVariable, final String targetVariable,
                      final Direction direction, final String[] edgeTypes,
                      final double estimatedCost, final long estimatedCardinality) {
    super(child, estimatedCost, estimatedCardinality);
    this.provider = provider;
    this.sourceVariable = sourceVariable;
    this.targetVariable = targetVariable;
    this.direction = direction;
    this.edgeTypes = edgeTypes;
  }

  @Override
  public ResultSet execute(final CommandContext context, final int nRecords) {
    final ResultSet inputResults = child.execute(context, nRecords);

    return new ResultSet() {
      private final List<Result> buffer = new ArrayList<>();
      private int bufferIndex = 0;
      private boolean finished = false;

      @Override
      public boolean hasNext() {
        if (bufferIndex < buffer.size())
          return true;
        if (finished)
          return false;
        fetchMore(nRecords > 0 ? nRecords : 100);
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

        while (buffer.size() < n && inputResults.hasNext()) {
          final Result inputResult = inputResults.next();

          final Vertex sourceVertex = inputResult.getProperty(sourceVariable);
          final Vertex targetVertex = inputResult.getProperty(targetVariable);
          if (sourceVertex == null || targetVertex == null)
            continue;

          // CSR connectivity check: O(log(degree)) binary search
          final int srcId = provider.getNodeId(sourceVertex.getIdentity());
          final int tgtId = provider.getNodeId(targetVertex.getIdentity());
          if (srcId < 0 || tgtId < 0)
            continue;

          final Vertex.DIRECTION arcadeDirection = direction.toArcadeDirection();
          if (provider.isConnectedTo(srcId, tgtId, arcadeDirection, edgeTypes)) {
            final ResultInternal result = new ResultInternal();
            for (final String prop : inputResult.getPropertyNames())
              result.setProperty(prop, inputResult.getProperty(prop));
            buffer.add(result);
          }
        }

        if (!inputResults.hasNext())
          finished = true;
      }

      @Override
      public void close() {
        inputResults.close();
      }
    };
  }

  @Override
  public String getOperatorType() {
    return "GAVExpandInto";
  }

  @Override
  public String explain(final int depth) {
    final StringBuilder sb = new StringBuilder();
    final String indent = getIndent(depth);

    sb.append(indent).append("+ GAVExpandInto");
    sb.append("(").append(sourceVariable).append(")-[");
    if (edgeTypes != null && edgeTypes.length > 0)
      sb.append(":").append(String.join("|", edgeTypes));
    sb.append("]-");
    sb.append(direction == Direction.OUT ? ">" : direction == Direction.IN ? "<" : "");
    sb.append("(").append(targetVariable).append(")");
    sb.append(" [provider=").append(provider.getName());
    sb.append(", cost=").append(String.format(Locale.US, "%.2f", estimatedCost));
    sb.append(", rows=").append(estimatedCardinality);
    sb.append("]\n");

    if (child != null)
      sb.append(child.explain(depth + 1));

    return sb.toString();
  }
}
