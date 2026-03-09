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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;

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

  public CartesianProduct(final PhysicalOperator left, final PhysicalOperator right,
                          final double estimatedCost, final long estimatedCardinality) {
    super(left, estimatedCost, estimatedCardinality);
    this.right = right;
  }

  @Override
  public ResultSet execute(final CommandContext context, final int nRecords) {
    return new ResultSet() {
      private ResultSet leftResults = null;
      private List<Result> rightResultsCache = null;
      private Result currentLeft = null;
      private int rightIndex = 0;
      private boolean finished = false;

      @Override
      public boolean hasNext() {
        if (finished)
          return false;
        ensureInitialized(context, nRecords);
        return currentLeft != null && rightIndex < rightResultsCache.size();
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();

        final Result rightResult = rightResultsCache.get(rightIndex);

        // Merge left and right properties into one result
        final ResultInternal merged = new ResultInternal();
        for (final String prop : currentLeft.getPropertyNames())
          merged.setProperty(prop, currentLeft.getProperty(prop));
        for (final String prop : rightResult.getPropertyNames())
          merged.setProperty(prop, rightResult.getProperty(prop));

        rightIndex++;
        // Advance to next left row if we've exhausted right side
        if (rightIndex >= rightResultsCache.size()) {
          if (leftResults.hasNext()) {
            currentLeft = leftResults.next();
            rightIndex = 0;
          } else
            finished = true;
        }

        return merged;
      }

      private void ensureInitialized(final CommandContext ctx, final int n) {
        if (leftResults != null)
          return;

        // Execute left and right operators
        leftResults = child.execute(ctx, n);
        final ResultSet rightResults = right.execute(ctx, n);

        // Materialize right side for reuse across left rows
        rightResultsCache = new ArrayList<>();
        while (rightResults.hasNext())
          rightResultsCache.add(rightResults.next());

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
    sb.append(" [cost=").append(String.format(Locale.US, "%.2f", estimatedCost));
    sb.append(", rows=").append(estimatedCardinality);
    sb.append("]\n");
    if (child != null)
      sb.append(child.explain(depth + 1));
    sb.append(right.explain(depth + 1));
    return sb.toString();
  }
}
