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
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.VertexType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;

/**
 * Physical operator for label disjunction node patterns such as {@code (n:A|B)}.
 * <p>
 * Scans every VertexType in the schema and returns vertices whose type is an instance of
 * at least one of the given labels. Types are iterated once (no duplicates).
 * <p>
 * Cost: O(N) where N is the total number of vertices matching any of the given labels.
 * <p>
 * Design notes:
 * <ul>
 *   <li>No inline WHERE pushdown (unlike {@link NodeByLabelScan#whereFilter}). Filters are
 *       applied as a separate step; pushdown is a perf optimization tracked separately.</li>
 *   <li>Type iterators are constructed eagerly in {@link #buildMatchingIterators}. Each call
 *       to {@code iterateType} wraps the bucket iterators in a {@code MultiIterator} under
 *       a read lock — the wrapper is cheap and page reads stay lazy inside the bucket
 *       iterators, so eager construction has negligible cost for the typical small label
 *       sets seen in practice.</li>
 *   <li>{@code estimatedCardinality} is inherited from the anchor selector and reflects a
 *       single-label scan (the first label only). The true upper bound for a disjunction
 *       is the sum across all matching types; under-estimation here may bias join-order
 *       choices when a disjunction node competes with other anchor candidates. Tracked as
 *       a follow-up rather than fixed in this PR.</li>
 * </ul>
 */
public class NodeByLabelDisjunctionScan extends AbstractPhysicalOperator {
  private final String       variable;
  private final List<String> labels;

  public NodeByLabelDisjunctionScan(final String variable, final List<String> labels,
      final double estimatedCost, final long estimatedCardinality) {
    super(estimatedCost, estimatedCardinality);
    this.variable = variable;
    this.labels = labels;
  }

  @Override
  public ResultSet execute(final CommandContext context, final int nRecords) {
    return new ResultSet() {
      private Iterator<Identifiable> currentIterator = null;
      private Iterator<Iterator<Identifiable>> typeIteratorCursor = null;
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

        if (typeIteratorCursor == null)
          typeIteratorCursor = buildMatchingIterators(context).iterator();

        while (buffer.size() < n) {
          if (currentIterator == null || !currentIterator.hasNext()) {
            if (!typeIteratorCursor.hasNext()) {
              finished = true;
              return;
            }
            currentIterator = typeIteratorCursor.next();
          }
          while (buffer.size() < n && currentIterator.hasNext()) {
            final Identifiable identifiable = currentIterator.next();
            final Vertex vertex = identifiable.asVertex();
            final ResultInternal result = new ResultInternal();
            result.setProperty(variable, vertex);
            buffer.add(result);
          }
        }

        // Mark exhausted eagerly when both the current iterator and the type queue are drained,
        // avoiding one redundant fetchMore round-trip when a query reads to the end (matching
        // NodeByLabelScan's pattern).
        if ((currentIterator == null || !currentIterator.hasNext()) && !typeIteratorCursor.hasNext())
          finished = true;
      }

      @Override
      public void close() {
        // No resources to close
      }
    };
  }

  /**
   * Builds one iterator per VertexType that is an instance of at least one disjunction label.
   * The outer loop already enumerates every subtype in the schema, so {@code iterateType} is
   * called with {@code polymorphic=false} — using {@code true} would duplicate records of
   * subtypes that match through both the parent type and their own type entry.
   */
  private List<Iterator<Identifiable>> buildMatchingIterators(final CommandContext context) {
    final List<Iterator<Identifiable>> iterators = new ArrayList<>();
    for (final DocumentType type : context.getDatabase().getSchema().getTypes()) {
      if (!(type instanceof VertexType))
        continue;
      for (final String label : labels) {
        if (type.instanceOf(label)) {
          @SuppressWarnings("unchecked")
          final Iterator<Identifiable> iter =
              (Iterator<Identifiable>) (Object) context.getDatabase().iterateType(type.getName(), false);
          iterators.add(iter);
          break;
        }
      }
    }
    return iterators;
  }

  @Override
  public String getOperatorType() {
    return "NodeByLabelDisjunctionScan";
  }

  @Override
  public String explain(final int depth) {
    final StringBuilder sb = new StringBuilder();
    final String indent = getIndent(depth);
    sb.append(indent).append("+ NodeByLabelDisjunctionScan");
    sb.append("(").append(variable).append(":").append(String.join("|", labels)).append(")");
    sb.append(" [cost=").append(String.format(Locale.US, "%.2f", estimatedCost));
    sb.append(", rows=").append(estimatedCardinality);
    sb.append("]\n");
    return sb.toString();
  }

  public String getVariable() {
    return variable;
  }

  /**
   * Returns the disjunction labels. Internal-only consumer API; not defensively copied.
   */
  public List<String> getLabels() {
    return labels;
  }
}
