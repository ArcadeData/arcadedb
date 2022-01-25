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

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.parser.Timeout;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by luigidellaquila on 08/08/16.
 */
public class AccumulatingTimeoutStep extends AbstractExecutionStep {
  private final Timeout timeout;
  private final long    timeoutMillis;

  private AtomicLong totalTime = new AtomicLong(0);

  public AccumulatingTimeoutStep(Timeout timeout, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.timeout = timeout;
    this.timeoutMillis = this.timeout.getVal().longValue();
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws CommandExecutionException {

    final ResultSet internal = getPrev().get().syncPull(ctx, nRecords);

    if (getPrev().get().isTimedOut())
      fail();

    return new ResultSet() {

      @Override
      public boolean hasNext() {
        if (timedOut || totalTime.get() / 1_000_000 > timeoutMillis) {
          fail();
        }
        long begin = System.nanoTime();

        try {
          return internal.hasNext();
        } finally {
          totalTime.addAndGet(System.nanoTime() - begin);
        }
      }

      @Override
      public Result next() {
        if (totalTime.get() / 1_000_000 > timeoutMillis) {
          fail();
        }
        long begin = System.nanoTime();
        try {
          return internal.next();
        } finally {
          totalTime.addAndGet(System.nanoTime() - begin);
        }
      }

      @Override
      public void close() {
        internal.close();
      }

      @Override
      public Optional<ExecutionPlan> getExecutionPlan() {
        return internal.getExecutionPlan();
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return internal.getQueryStats();
      }
    };
  }

  private void fail() {
    this.timedOut = true;
    sendTimeout();
    if (!Timeout.RETURN.equals(this.timeout.getFailureStrategy())) {
      throw new TimeoutException("Timeout expired");
    }
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public ExecutionStep copy(CommandContext ctx) {
    return new AccumulatingTimeoutStep(timeout.copy(), ctx, profilingEnabled);
  }

  @Override
  public void reset() {
    this.totalTime = new AtomicLong(0);
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    return ExecutionStepInternal.getIndent(depth, indent) + "+ TIMEOUT (" + timeout.getVal().toString() + "ms)";
  }
}
