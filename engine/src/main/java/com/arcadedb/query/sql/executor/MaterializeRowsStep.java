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
package com.arcadedb.query.sql.executor;

import com.arcadedb.exception.TimeoutException;

import java.util.ArrayList;
import java.util.List;

/**
 * A barrier that pulls every row of the previous step BEFORE the first one is passed downstream (issue #10192, the Halloween
 * problem of {@code INSERT INTO T FROM SELECT ... FROM T}).
 * <p>
 * A SELECT that reads the type it is inserting into would otherwise meet its own output: a scan per OR branch finds, in the
 * second branch, the copy the first branch has just inserted, and an index range scan meets the new keys ahead of its cursor
 * and never ends. Draining the SELECT first makes the rows inserted the ones the statement saw when it started, as SQL
 * requires. The rows are the SELECT's projected output, so they are held whole, not by address as
 * {@link MaterializeRecordsStep} does for an UPDATE: a projection computes values that no record holds.
 * <p>
 * The planner adds the barrier only when the SELECT can read a bucket the statement writes to, so a copy between unrelated
 * types keeps streaming. The rows are charged to the heap budget all queries share, but not capped in number: the barrier is
 * what makes the write correct, not a buffer the statement could do without.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MaterializeRowsStep extends AbstractExecutionStep {
  private List<Result>       rows;
  private int                next;
  private OperationHeapLimit heapLimit;

  public MaterializeRowsStep(final CommandContext context) {
    super(context);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final ExecutionStepInternal prevStep = checkForPrevious();
    if (rows == null)
      drain(context, prevStep);

    final InternalResultSet result = new InternalResultSet();
    int served = 0;
    while (served++ < nRecords && next < rows.size()) {
      result.add(rows.get(next));
      // THE ROW IS HANDED OVER: LET IT GO AS SOON AS THE INSERT DOES
      rows.set(next++, null);
    }
    if (next >= rows.size() && heapLimit != null) {
      heapLimit.release();
      heapLimit = null;
    }
    return result;
  }

  private void drain(final CommandContext context, final ExecutionStepInternal prevStep) {
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);
    final List<Result> drained = new ArrayList<>();
    heapLimit = OperationHeapLimit.of(context, "rows", "MATERIALIZE ROWS");
    try {
      while (true) {
        final ResultSet upstream = prevStep.syncPull(context, DEFAULT_FETCH_RECORDS_PER_PULL);
        if (!upstream.hasNext())
          break;
        while (upstream.hasNext()) {
          final long begin = context.isProfiling() ? System.nanoTime() : 0;
          try {
            final Result row = upstream.next();
            guard.checkPeriodically(drained.size());
            heapLimit.chargeElement(row, HeapEstimator.REFERENCE_BYTES);
            drained.add(row);
          } finally {
            if (context.isProfiling())
              cost += System.nanoTime() - begin;
          }
        }
      }
    } catch (final RuntimeException | Error e) {
      // THE STATEMENT FAILS, POSSIBLY BEFORE ITS CALLER HOLDS A RESULT SET TO CLOSE: GIVE THE HEAP BACK NOW
      heapLimit.release();
      heapLimit = null;
      throw e;
    }
    // THE SELECT IS DONE: FREE ITS CURSORS BEFORE THE FIRST ROW IS WRITTEN
    prevStep.close();
    rows = drained;
  }

  @Override
  public void close() {
    if (heapLimit != null) {
      heapLimit.release();
      heapLimit = null;
    }
    // A PULL AFTER THE CLOSE SERVES NOTHING: IT MUST NOT DRAIN THE CLOSED SOURCE AGAIN
    rows = List.of();
    next = 0;
    super.close();
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder result = new StringBuilder(ExecutionStepInternal.getIndent(depth, indent)).append("+ MATERIALIZE ROWS");
    if (context.isProfiling())
      result.append(" (").append(getCostFormatted()).append(")");
    return result.toString();
  }

  @Override
  public ExecutionStep copy(final CommandContext context) {
    return new MaterializeRowsStep(context);
  }
}
