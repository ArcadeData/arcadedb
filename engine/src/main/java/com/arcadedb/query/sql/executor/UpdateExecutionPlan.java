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

/**
 * Created by luigidellaquila on 08/08/16.
 */

import com.arcadedb.exception.CommandExecutionException;

import java.util.ArrayList;
import java.util.List;

import static com.arcadedb.query.sql.executor.AbstractExecutionStep.DEFAULT_FETCH_RECORDS_PER_PULL;

/**
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class UpdateExecutionPlan extends SelectExecutionPlan {

  final List<Result> result = new ArrayList<>();
  int next = 0;

  public UpdateExecutionPlan(final CommandContext context, final int limit) {
    super(context, limit);
  }

  @Override
  public ResultSet fetchNext(final int n) {
    if (next >= result.size()) {
      return new InternalResultSet();//empty
    }

    final IteratorResultSet nextBlock = new IteratorResultSet(result.subList(next, Math.min(next + n, result.size())).iterator());
    next += n;
    return nextBlock;
  }

  @Override
  public void reset(final CommandContext context) {
    result.clear();
    next = 0;
    super.reset(context);
    executeInternal();
  }

  /**
   * #5681: an UPDATE/DELETE with a {@code LIMIT} chains {@code LimitExecutionStep} above the sub-plan that holds the
   * actual scan (e.g. an index scan wrapped in {@link SubQueryStep}). Once the limit is satisfied,
   * {@code LimitExecutionStep} simply stops pulling - it never tells the sub-plan there will be no more requests, so
   * the scan is abandoned mid-flight unless something closes the chain. By the time this method returns, the
   * statement is done: every result the caller will ever see is already buffered in {@link #result}, and
   * {@link #fetchNext(int)} never touches the steps chain again. Closing here - in a {@code finally} so it also runs
   * on the exception path - releases the abandoned scan immediately instead of leaving it for the caller to close
   * the returned {@link ResultSet} (the DML result is very commonly used only for its side effect and never closed)
   * or, failing that, for the GC to reclaim it later.
   * <p>
   * If the drain loop itself throws, {@code close()} still runs (in the {@code finally}) but its own failure - every
   * current {@code close()} in these chains is a simple, exception-safe no-op/idempotent forward, but a future step
   * could change that - is attached as a suppressed exception instead of replacing the original one, so the real
   * cause of the failure always reaches the caller.
   */
  public void executeInternal() throws CommandExecutionException {
    RuntimeException failure = null;
    try {
      while (true) {
        final ResultSet nextBlock = super.fetchNext(DEFAULT_FETCH_RECORDS_PER_PULL);
        if (!nextBlock.hasNext())
          return;

        while (nextBlock.hasNext())
          result.add(nextBlock.next());
      }
    } catch (final RuntimeException e) {
      failure = e;
      throw e;
    } finally {
      try {
        close();
      } catch (final RuntimeException closeFailure) {
        if (failure != null)
          failure.addSuppressed(closeFailure);
        else
          throw closeFailure;
      }
    }
  }

  @Override
  public Result toResult() {
    final ResultInternal res = (ResultInternal) super.toResult();
    res.setProperty("type", "UpdateExecutionPlan");
    return res;
  }

  @Override
  public boolean canBeCached() {
    return false;
  }
}
