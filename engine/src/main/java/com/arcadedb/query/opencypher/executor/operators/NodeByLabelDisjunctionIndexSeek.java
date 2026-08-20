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

import com.arcadedb.database.RID;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Physical operator for a label disjunction node pattern such as {@code (n:A|B {id: $x})} whose predicate is
 * resolved by an index on every root type the disjunction visits (issue #6397).
 * <p>
 * Union of one {@link NodeIndexSeek} per root type ({@link com.arcadedb.query.opencypher.optimizer.statistics.StatisticsProvider#getMatchingVertexRootTypes}),
 * run in sequence and de-duplicated by RID: a root's own polymorphic index already reaches every matching
 * subtype of that root, so the union of the roots' seeks covers exactly what
 * {@link NodeByLabelDisjunctionScan} would have scanned. De-duplication is still required - not merely
 * defensive - because a type that multiply-inherits from two accepted alternatives (e.g. {@code C EXTENDS A, B}
 * where the pattern is {@code (n:A|B)}) is reachable through both roots' indexes.
 * <p>
 * {@link com.arcadedb.query.opencypher.optimizer.rules.IndexSelectionRule} only builds this operator when
 * every root has a usable index for the pattern's equality predicate (all-or-nothing); a disjunction with even
 * one non-indexed root still gets the full {@link NodeByLabelDisjunctionScan}.
 */
public class NodeByLabelDisjunctionIndexSeek extends AbstractPhysicalOperator {
  private final String             variable;
  private final List<NodeIndexSeek> perRootSeeks;

  public NodeByLabelDisjunctionIndexSeek(final String variable, final List<NodeIndexSeek> perRootSeeks,
      final double estimatedCost, final long estimatedCardinality) {
    super(estimatedCost, estimatedCardinality);
    this.variable = variable;
    this.perRootSeeks = perRootSeeks;
  }

  @Override
  public ResultSet execute(final CommandContext context, final int nRecords) {
    return new ResultSet() {
      private int              rootIndex = 0;
      private ResultSet        current   = null;
      private final Set<RID>   seen      = new HashSet<>();
      private Result           pending   = null;
      private boolean          finished  = false;

      @Override
      public boolean hasNext() {
        if (pending != null)
          return true;
        if (finished)
          return false;
        pending = fetchNextUnseen();
        if (pending == null)
          finished = true;
        return pending != null;
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();
        final Result result = pending;
        pending = null;
        return result;
      }

      // No WorkGuard of its own here: unlike NodeByLabelDisjunctionScan's single unguarded scan loop, every
      // iteration of the outer while below immediately delegates into one child NodeIndexSeek's own hasNext()/
      // next(), each of which already runs its own WorkGuard.forCommandDeadline(context) check per record. A
      // future child added here that does NOT guard itself would reintroduce an unbounded loop, so keep that
      // invariant in mind before adding one.
      private Result fetchNextUnseen() {
        while (true) {
          if (current == null) {
            if (rootIndex >= perRootSeeks.size())
              return null;
            current = perRootSeeks.get(rootIndex++).execute(context, nRecords);
          }

          if (!current.hasNext()) {
            current.close();
            current = null;
            continue;
          }

          final Result candidate = current.next();
          final Vertex vertex = candidate.getProperty(variable);
          // A root's own vertices are never seen through another root's seek (disjoint subtrees), so
          // the set only ever pays for the multiply-inherited case this operator exists to de-duplicate.
          if (vertex != null && !seen.add(vertex.getIdentity()))
            continue;

          return candidate;
        }
      }

      @Override
      public void close() {
        if (current != null) {
          current.close();
          current = null;
        }
      }
    };
  }

  @Override
  public String getOperatorType() {
    return "NodeByLabelDisjunctionIndexSeek";
  }

  @Override
  public String explain(final int depth) {
    final StringBuilder sb = new StringBuilder();
    final String indent = getIndent(depth);
    sb.append(indent).append("+ NodeByLabelDisjunctionIndexSeek");
    sb.append("(").append(variable).append(")");
    sb.append(" [roots=").append(perRootSeeks.size());
    sb.append(", cost=").append(String.format(Locale.US, "%.2f", estimatedCost));
    sb.append(", rows=").append(estimatedCardinality);
    sb.append("]\n");
    // Each root's own NodeIndexSeek already renders its index, resolved key columns and per-root cost/rows -
    // nested here rather than summarized, so the key values a composite index resolved (issue #6397 review) are
    // visible in EXPLAIN/PROFILE exactly as they would be for a plain (non-disjunction) index seek.
    for (final NodeIndexSeek seek : perRootSeeks)
      sb.append(seek.explain(depth + 1));
    return sb.toString();
  }

  public String getVariable() {
    return variable;
  }

  /**
   * Returns the per-root-type seeks this operator unions. Internal-only consumer API; not defensively copied.
   */
  public List<NodeIndexSeek> getPerRootSeeks() {
    return perRootSeeks;
  }
}
