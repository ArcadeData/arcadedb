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
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.OperationHeapLimit;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Barrier step that reads its whole input before letting the first row through, so that a clause which
 * creates vertices or edges never runs while a clause that reads the graph is still enumerating.
 * <p>
 * openCypher requires a query's reads to be unaffected by that same query's writes; Neo4j gets there by
 * planting an {@code Eager} operator wherever a write conflicts with a read, and this is that operator.
 * The pipeline here is a pull model: a MATCH re-enumerates its pattern once per input row and hands the
 * rows downstream as it goes, so a downstream CREATE/MERGE/write-procedure that adds an edge of a type the
 * MATCH can match adds a row to an enumeration that has not run yet. Issue #7171 is the visible form of
 * that - {@code FOR alias0 IN [a,b,c] MATCH (:l3|l8)<-[r0]-(:l6), ... CALL merge.relationship(...)}
 * returned 480 rows where the graph only ever held 79 matching edges (79 x 6 = 474), the six extra rows
 * being the procedure's own new edges seen by the third {@code alias0} iteration's enumeration.
 * <p>
 * Placed as the write step's immediate input, it turns "read some, write some, read some more" into
 * "read everything, then write", which is the only ordering under which every row of the read sees the
 * same graph. Cost is memory: the input rows are held until the last of them has been handed downstream.
 * The planner therefore inserts it only where the write's shape can actually feed an upstream pattern -
 * see {@code CypherEagernessAnalyzer} - so the common bulk shapes ({@code UNWIND ... CREATE},
 * {@code MATCH (a)-[:KNOWS]->(b) CREATE (a)-[:SCORED]->(b)}) keep streaming untouched.
 * <p>
 * The other thing draining everything costs is a downstream {@code LIMIT}'s short-circuit: a barriered
 * query runs its write for every matched row rather than stopping at the first N. That is inherent to the
 * ordering the barrier exists to impose - stopping early would mean some rows never saw the writes the
 * earlier ones did - and Neo4j's {@code Eager} has the same effect, but it is worth knowing when a write
 * query with a LIMIT suddenly touches more rows than it used to.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class EagerStep extends AbstractExecutionStep {
  /**
   * The drained input, and how far the result sets handed out so far have read into it. Both live on the
   * step rather than on the {@link ResultSet} it returns, so a second {@code syncPull} continues where the
   * first left off instead of re-reading {@code prev} and handing back rows a caller already consumed.
   */
  private List<Result> materialized = null;
  private int          currentIndex = 0;

  // THE ROWS MATERIALIZED, UNDER THE PER-OPERATION CAP AND THE HEAP BUDGET OF ALL THE QUERIES (ISSUES #8585, #8591). ON
  // THE STEP: THE CLOSE() OF A QUERY REACHES THE STEPS, NOT THEIR RESULT SETS
  private OperationHeapLimit heapLimit;

  // HOW MANY ROWS TO KEEP WHEN THE BARRIER SITS RIGHT BEHIND A LIMIT: THE REST ARE DRAINED (SO THE WRITES UPSTREAM RUN FOR EVERY ROW)
  // AND DISCARDED, WHICH KEEPS THE MEMORY AT O(LIMIT) INSTEAD OF O(ROWS). -1 KEEPS EVERYTHING
  private final long keepFirst;

  public EagerStep(final CommandContext context) {
    this(context, -1);
  }

  public EagerStep(final CommandContext context, final long keepFirst) {
    super(context);
    this.keepFirst = keepFirst;
  }

  /**
   * {@code nRecords} is a batching hint here, not a per-pull cap: every step in this pipeline returns a
   * result set that iterates the whole of its input (see {@code OrderByStep}, and {@code ForeachStep}'s
   * refilling buffer), and its consumer pulls once and drains until {@code hasNext()} is false. Capping the
   * returned set at {@code nRecords} would silently truncate the query at one batch.
   */
  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious("EagerStep requires a previous step");

    // Drained on the first pull, not on the first hasNext(): a consumer that never reads a row (a LIMIT 0 above the
    // barrier) must still see the writes behind it run for every row (issues #8826, #8827)
    if (materialized == null)
      materialize(context);

    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return currentIndex < materialized.size();
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();
        final Result result = materialized.get(currentIndex++);
        if (currentIndex == materialized.size()) {
          // EVERY ROW WAS SERVED: THE BUFFER IS NOT NEEDED ANYMORE, EVEN IF THE CONSUMER KEEPS THE RESULT SET OPEN
          materialized.clear();
          currentIndex = 0;
          releaseHeap();
        }
        return result;
      }

      @Override
      public void close() {
        EagerStep.this.close();
      }
    };
  }

  private void materialize(final CommandContext context) {
    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    try {
      materialized = new ArrayList<>();
      long drained = 0;
      // The drain is one uninterrupted region, and the statement-level drain that tests the command
      // deadline per row only starts once this one has finished - so a TIMEOUT clause could not end a
      // long barrier before this guard. Same reason CypherExecutionPlan.execute() carries one (#6266).
      final WorkGuard guard = WorkGuard.forCommandDeadline(context);
      // Integer.MAX_VALUE rather than nRecords: a partial drain would leave the upstream cursor open
      // across the writes, which is the very interleaving this step exists to prevent.
      final ResultSet prevResults = prev.syncPull(context, Integer.MAX_VALUE);
      heapLimit = OperationHeapLimit.of(context, "eager read/write barrier");
      try {
        while (prevResults.hasNext()) {
          guard.check();
          final Result row = prevResults.next();
          drained++;
          if (keepFirst < 0 || materialized.size() < keepFirst) {
            materialized.add(row);
            heapLimit.add(materialized.size(), row);
          }
        }
      } catch (final RuntimeException e) {
        materialized.clear();
        releaseHeap();
        throw e;
      }
      if (context.isProfiling())
        rowCount += drained;
    } finally {
      if (context.isProfiling())
        cost += System.nanoTime() - begin;
    }
  }

  private void releaseHeap() {
    if (heapLimit != null)
      heapLimit.release();
  }

  @Override
  public void close() {
    releaseHeap();
    super.close();
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    builder.append("  ".repeat(Math.max(0, depth * indent)));
    builder.append("+ EAGER (read/write barrier)");
    if (context.isProfiling())
      builder.append(" (").append(getCostFormatted()).append(")");
    return builder.toString();
  }
}
