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

import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.function.BiPredicate;

/**
 * Physical operator that produces a Cartesian product of two independent operators.
 * Used for multi-MATCH queries where each MATCH has independent single-node patterns
 * (e.g., MATCH (a:T) WHERE a.id=$x MATCH (b:T) WHERE b.id=$y CREATE ...).
 * <p>
 * For point lookups (index seeks returning 1 row each), this is O(1) — it simply
 * merges the properties of both results into a single row.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CartesianProduct extends AbstractPhysicalOperator {
  private final PhysicalOperator right;
  private final BiPredicate<Result, Result> pairFilter;

  public CartesianProduct(final PhysicalOperator left, final PhysicalOperator right,
                          final double estimatedCost, final long estimatedCardinality) {
    this(left, right, estimatedCost, estimatedCardinality, null);
  }

  /**
   * @param pairFilter when non-null, tested against every candidate (left, right) pair before it is
   *                    merged into a row; a pair for which it returns {@code false} is skipped
   *                    without ever allocating the merged row
   */
  public CartesianProduct(final PhysicalOperator left, final PhysicalOperator right,
                          final double estimatedCost, final long estimatedCardinality,
                          final BiPredicate<Result, Result> pairFilter) {
    super(left, estimatedCost, estimatedCardinality);
    this.right = right;
    this.pairFilter = pairFilter;
  }

  @Override
  public ResultSet execute(final CommandContext context, final int nRecords) {
    // A cartesian product emits |right| rows per left row without touching a source, so guarding the scans
    // below it bounds nothing here: the check has to be on the row this operator itself produces (issue #6266).
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);

    return new ResultSet() {
      private ResultSet leftResults = null;
      private List<Result> rightResultsCache = null;
      private Result currentLeft = null;
      private int rightIndex = 0;
      private boolean finished = false;
      private boolean initialized = false;
      private Result pendingRight;

      @Override
      public boolean hasNext() {
        ensureInitialized(context, nRecords);
        if (finished)
          return false;
        if (pendingRight != null)
          return true;
        return advance();
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();

        final Result rightResult = pendingRight;
        pendingRight = null;

        // Merge left and right properties into one result
        final ResultInternal merged = new ResultInternal();
        for (final String prop : currentLeft.getPropertyNames())
          merged.setProperty(prop, currentLeft.getProperty(prop));
        for (final String prop : rightResult.getPropertyNames())
          merged.setProperty(prop, rightResult.getProperty(prop));

        return merged;
      }

      // Scans forward from the current (left, right) position for the next pair the filter accepts,
      // leaving it in pendingRight without merging it - a rejected pair never allocates a merged row.
      private boolean advance() {
        while (currentLeft != null) {
          while (rightIndex < rightResultsCache.size()) {
            guard.check();
            final Result candidate = rightResultsCache.get(rightIndex++);
            if (pairFilter == null || pairFilter.test(currentLeft, candidate)) {
              pendingRight = candidate;
              return true;
            }
          }
          if (leftResults.hasNext()) {
            currentLeft = leftResults.next();
            rightIndex = 0;
          } else
            currentLeft = null;
        }
        finished = true;
        return false;
      }

      private void ensureInitialized(final CommandContext ctx, final int n) {
        if (initialized)
          return;
        initialized = true;

        // Execute left and right operators
        leftResults = child.execute(ctx, n);
        final ResultSet rightResults = right.execute(ctx, n);

        // Materialize right side for reuse across left rows
        rightResultsCache = new ArrayList<>();
        while (rightResults.hasNext()) {
          guard.check();
          rightResultsCache.add(rightResults.next());
        }

        // Get first left row
        if (leftResults.hasNext())
          currentLeft = leftResults.next();
        else
          finished = true;
      }

      @Override
      public void close() {
        // nothing to close
      }
    };
  }

  @Override
  public String getOperatorType() {
    return "CartesianProduct";
  }

  @Override
  public String explain(final int depth) {
    final String indent = getIndent(depth);
    final StringBuilder sb = new StringBuilder();
    sb.append(indent).append("+ CartesianProduct");
    if (pairFilter != null)
      sb.append(" [RelationshipUniquenessFilter pushed into join]");
    sb.append(" [cost=").append(String.format(Locale.US, "%.2f", estimatedCost));
    sb.append(", rows=").append(estimatedCardinality);
    sb.append("]\n");
    if (child != null)
      sb.append(child.explain(depth + 1));
    sb.append(right.explain(depth + 1));
    return sb.toString();
  }
}
