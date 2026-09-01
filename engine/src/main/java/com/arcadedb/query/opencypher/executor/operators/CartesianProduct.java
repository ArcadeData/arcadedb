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
 * For point lookups (index seeks returning 1 row each), this is O(1) - it simply
 * merges the properties of both results into a single row.
 * <p>
 * The right input is pulled lazily and buffered as it is consumed, so the first row costs one right
 * row rather than the whole right side, and a consumer that stops early (a LIMIT above this operator)
 * never pays for the rows it did not ask for. The buffer is what lets the second and later left rows
 * replay the right side without re-executing it. Both children are closed once - on exhaustion, or on
 * {@code close()}, whichever comes first (issue #7010).
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
      private ResultSet   leftResults    = null;
      private ResultSet   rightResults   = null;
      private List<Result> rightBuffer   = null;
      private Result      currentLeft    = null;
      private int         rightIndex     = 0;
      private boolean     rightExhausted = false;
      private boolean     finished       = false;
      private boolean     initialized    = false;
      private Result      pendingRight;

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
          Result candidate;
          while ((candidate = nextRight()) != null) {
            guard.check();
            if (pairFilter == null || pairFilter.test(currentLeft, candidate)) {
              pendingRight = candidate;
              return true;
            }
          }
          // An empty right side crosses to nothing, so there is no point walking the remaining left rows.
          if (rightExhausted && rightBuffer.isEmpty())
            break;
          if (leftResults != null && leftResults.hasNext()) {
            currentLeft = leftResults.next();
            rightIndex = 0;
          } else
            currentLeft = null;
        }
        finished = true;
        // Nothing more will be pulled from either side: release both cursors now rather than waiting
        // for a close() the consumer may never call.
        closeChildren();
        return false;
      }

      /**
       * Returns the next right row for the current left row, or null once the right side is exhausted.
       * <p>
       * Issue #7010: the right input used to be drained into a list in full before the first row was
       * emitted, so a LIMIT above this operator paid for the whole right side in scan work and in heap.
       * It is now pulled one row at a time and buffered as it goes - a consumer that stops early never
       * touches the rows it did not ask for, while the buffer is what replays the right side for the
       * second and later left rows.
       */
      private Result nextRight() {
        if (rightIndex < rightBuffer.size())
          return rightBuffer.get(rightIndex++);
        if (rightExhausted)
          return null;

        guard.check();
        if (rightResults != null && rightResults.hasNext()) {
          final Result row = rightResults.next();
          rightBuffer.add(row);
          ++rightIndex;
          return row;
        }

        rightExhausted = true;
        // The right cursor has nothing left to give and the buffer replays it from here on.
        closeRight();
        return null;
      }

      private void ensureInitialized(final CommandContext ctx, final int n) {
        if (initialized)
          return;
        initialized = true;

        // Execute left and right operators. The right side is NOT drained here (issue #7010).
        leftResults = child.execute(ctx, n);
        rightResults = right.execute(ctx, n);
        rightBuffer = new ArrayList<>();

        // Get first left row
        if (leftResults.hasNext())
          currentLeft = leftResults.next();
        else {
          finished = true;
          closeChildren();
        }
      }

      private void closeRight() {
        if (rightResults != null) {
          rightResults.close();
          rightResults = null;
        }
      }

      // Idempotent: a child is closed at most once, whether the release comes from exhaustion or from close().
      private void closeChildren() {
        closeRight();
        if (leftResults != null) {
          leftResults.close();
          leftResults = null;
        }
      }

      /**
       * Issue #7010: this was an empty method, so the close() chain stopped here and an index-backed
       * child kept its cursor open for as long as the plan was retained (see #5635).
       */
      @Override
      public void close() {
        finished = true;
        pendingRight = null;
        currentLeft = null;
        closeChildren();
        if (rightBuffer != null)
          rightBuffer.clear();
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
