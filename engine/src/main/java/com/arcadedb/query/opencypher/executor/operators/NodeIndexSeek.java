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
import com.arcadedb.database.RID;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.VertexType;

import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.LiteralExpression;
import com.arcadedb.query.opencypher.ast.ParameterExpression;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Set;

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
      private TypeIndex index = null;
      private List<Object> seekKeys = null;   // one entry per value to seek (IN-list expands to N entries)
      private int seekIndex = 0;
      private IndexCursor cursor = null;
      private Set<RID> seen = null;            // de-duplicate RIDs across multiple seek values (IN-list set semantics)
      private final List<Result> buffer = new ArrayList<>();
      private int bufferIndex = 0;
      private boolean initialized = false;
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

      private void initialize() {
        initialized = true;

        final DocumentType type = context.getDatabase().getSchema().getType(label);
        // A non-vertex type with the same name (edge/document type) matches no node pattern (issue #5194)
        if (!(type instanceof VertexType)) {
          finished = true;
          return;
        }

        // Get the index
        index = (TypeIndex) type.getPolymorphicIndexByProperties(propertyName);
        if (index == null) {
          finished = true;
          return;
        }

        // Resolve the seek keys at runtime (parameters change per execution). A single equality value
        // yields one key; an IN-list (issue #5306) yields one key per list element, with duplicates
        // removed and result RIDs de-duplicated to honor set-membership semantics.
        seekKeys = new ArrayList<>();
        if (propertyValue instanceof InListValues inList) {
          seen = new HashSet<>();
          for (final Expression element : inList.getValues()) {
            final Object resolved = resolveValue(element);
            if (resolved instanceof Collection<?> coll) {
              for (final Object v : coll)
                addSeekKey(v);
            } else if (resolved != null && resolved.getClass().isArray()) {
              for (final Object v : MultiValue.getMultiValueAsList(resolved))
                addSeekKey(v);
            } else
              addSeekKey(resolved);
          }
        } else {
          addSeekKey(resolveValue(propertyValue));
        }

        if (seekKeys.isEmpty())
          finished = true;
      }

      private void addSeekKey(final Object value) {
        if (value != null)
          seekKeys.add(value);
      }

      private Object resolveValue(final Object value) {
        if (value instanceof ParameterExpression) {
          final String paramName = ((ParameterExpression) value).getParameterName();
          if (context.getInputParameters() != null)
            return context.getInputParameters().get(paramName);
          return null;
        }
        if (value instanceof LiteralExpression)
          return ((LiteralExpression) value).getValue();
        return value;
      }

      private void fetchMore(final int n) {
        buffer.clear();
        bufferIndex = 0;

        if (!initialized) {
          initialize();
          if (finished)
            return;
        }

        while (buffer.size() < n) {
          // Advance to the next seek value when the current cursor is exhausted.
          if (cursor == null || !cursor.hasNext()) {
            if (seekIndex >= seekKeys.size()) {
              finished = true;
              return;
            }
            cursor = index.get(new Object[] { seekKeys.get(seekIndex++) });
            continue;
          }

          final Identifiable identifiable = cursor.next();

          // Skip already-emitted vertices when seeking multiple IN-list values (set semantics).
          if (seen != null && !seen.add(identifiable.getIdentity()))
            continue;

          // Load the actual record from the identifiable (may be RID)
          final Vertex vertex = identifiable.asVertex();

          // Create result with vertex bound to variable
          final ResultInternal result = new ResultInternal();
          result.setProperty(variable, vertex);
          buffer.add(result);
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
    sb.append(", cost=").append(String.format(Locale.US, "%.2f", estimatedCost));
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
