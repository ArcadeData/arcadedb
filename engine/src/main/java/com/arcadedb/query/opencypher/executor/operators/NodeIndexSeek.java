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
 * Physical operator that performs an index seek for vertices.
 * Uses a type index to efficiently find vertices matching an equality predicate.
 *
 * This is typically 10-1000x faster than a full table scan for selective queries.
 *
 * Cost: O(log N + M) where N is index size, M is matching rows
 * Cardinality: Estimated based on selectivity
 */
public class NodeIndexSeek extends AbstractPhysicalOperator {
  private final String variable;
  private final String label;
  private final String propertyName;
  private final Object propertyValue;
  private final String indexName;

  public NodeIndexSeek(final String variable, final String label, final String propertyName,
                      final Object propertyValue, final String indexName,
                      final double estimatedCost, final long estimatedCardinality) {
    super(estimatedCost, estimatedCardinality);
    this.variable = variable;
    this.label = label;
    this.propertyName = propertyName;
    this.propertyValue = propertyValue;
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

          // Get the index
          final TypeIndex index = (TypeIndex) type.getPolymorphicIndexByProperties(propertyName);
          if (index == null) {
            finished = true;
            return;
          }

          // Perform index lookup
          final Object[] keys = new Object[]{propertyValue};
          cursor = index.get(keys);
        }

        // Fetch up to n matching vertices
        while (buffer.size() < n && cursor.hasNext()) {
          final Identifiable identifiable = cursor.next();

          // Load the actual record from the identifiable (may be RID)
          final Vertex vertex = identifiable.asVertex();

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
    return "NodeIndexSeek";
  }

  @Override
  public String explain(final int depth) {
    final StringBuilder sb = new StringBuilder();
    final String indent = getIndent(depth);

    sb.append(indent).append("+ NodeIndexSeek");
    sb.append("(").append(variable).append(":").append(label).append(")");
    sb.append(" [index=").append(indexName);
    sb.append(", ").append(propertyName).append("=").append(propertyValue);
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

  public Object getPropertyValue() {
    return propertyValue;
  }

  public String getIndexName() {
    return indexName;
  }
}
