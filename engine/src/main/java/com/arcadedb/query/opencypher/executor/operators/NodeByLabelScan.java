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
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;

/**
 * Physical operator that performs a full table scan on vertices of a specific type.
 * This is used when no index is available or when the optimizer determines
 * a scan is more efficient than an index seek.
 * <p>
 * Supports optional inline WHERE filter (predicate pushdown) to evaluate
 * predicates during scanning rather than in a separate filter operator.
 *
 * Cost: O(N) where N is the number of vertices of the given type
 * Cardinality: Estimated number of vertices of the type
 */
public class NodeByLabelScan extends AbstractPhysicalOperator {
  private final String            variable;
  private final String            label;
  private final BooleanExpression whereFilter; // Optional inline WHERE predicate (pushdown)

  public NodeByLabelScan(final String variable, final String label,
                        final double estimatedCost, final long estimatedCardinality) {
    this(variable, label, estimatedCost, estimatedCardinality, null);
  }

  public NodeByLabelScan(final String variable, final String label,
                        final double estimatedCost, final long estimatedCardinality,
                        final BooleanExpression whereFilter) {
    super(estimatedCost, estimatedCardinality);
    this.variable = variable;
    this.label = label;
    this.whereFilter = whereFilter;
  }

  @Override
  public ResultSet execute(final CommandContext context, final int nRecords) {
    return new ResultSet() {
      private Iterator<Identifiable> iterator = null;
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

        // Initialize iterator on first call
        if (iterator == null) {
          // Check if type exists before iterating
          // This handles multi-label queries where the composite type may not exist
          if (!context.getDatabase().getSchema().existsType(label)) {
            finished = true;
            return;
          }

          @SuppressWarnings("unchecked")
          final Iterator<Identifiable> iter = (Iterator<Identifiable>) (Object)
              context.getDatabase().iterateType(label, true);
          iterator = iter;
        }

        // Fetch up to n vertices
        while (buffer.size() < n && iterator.hasNext()) {
          final Identifiable identifiable = iterator.next();

          // Load the actual record from the identifiable (may be RID)
          final Vertex vertex = identifiable.asVertex();

          // Create result with vertex bound to variable
          final ResultInternal result = new ResultInternal();
          result.setProperty(variable, vertex);

          // Apply inline WHERE filter (predicate pushdown)
          if (whereFilter != null && !whereFilter.evaluate(result, context))
            continue;

          buffer.add(result);
        }

        if (!iterator.hasNext()) {
          finished = true;
        }
      }

      @Override
      public void close() {
        // No resources to close
      }
    };
  }

  @Override
  public String getOperatorType() {
    return "NodeByLabelScan";
  }

  @Override
  public String explain(final int depth) {
    final StringBuilder sb = new StringBuilder();
    final String indent = getIndent(depth);

    sb.append(indent).append("+ NodeByLabelScan");
    sb.append("(").append(variable).append(":").append(label).append(")");
    if (whereFilter != null)
      sb.append(" [filter: ").append(whereFilter.getText()).append("]");
    sb.append(" [cost=").append(String.format(Locale.US, "%.2f", estimatedCost));
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
}
