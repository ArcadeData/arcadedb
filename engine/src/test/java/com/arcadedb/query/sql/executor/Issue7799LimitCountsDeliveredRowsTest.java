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

import com.arcadedb.query.sql.parser.Limit;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7799: {@link LimitExecutionStep#syncPull} used to advance its internal counter by the
 * SIZE OF THE BATCH IT ASKED an upstream step for, rather than by the number of rows the upstream step actually
 * returned. No step on {@code main} currently under-delivers a non-empty batch, so the bug needed a step built here
 * that does - exactly the "one new or changed source step that yields a short batch" scenario the issue describes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7799LimitCountsDeliveredRowsTest {

  /**
   * Always returns exactly ONE row per {@code syncPull} call, no matter how many rows were requested, until
   * {@code available} is exhausted. This is a legal upstream shape: {@code LocalResultSet} treats a short
   * non-empty batch as "pull again", not as "end of results".
   */
  private static class ShortBatchStep extends AbstractExecutionStep {
    private final int available;
    private       int produced = 0;

    ShortBatchStep(final CommandContext context, final int available) {
      super(context);
      this.available = available;
    }

    @Override
    public ResultSet syncPull(final CommandContext context, final int nRecords) {
      final InternalResultSet rs = new InternalResultSet();
      if (nRecords > 0 && produced < available) {
        produced++;
        rs.add(new ResultInternal().setProperty("i", produced));
      }
      return rs;
    }
  }

  private static int drain(final LimitExecutionStep step, final CommandContext context) {
    int total = 0;
    while (true) {
      final ResultSet batch = step.syncPull(context, 100);
      if (!batch.hasNext())
        break;
      while (batch.hasNext()) {
        batch.next();
        total++;
      }
    }
    return total;
  }

  @Test
  void limitReturnsAllRequestedRowsEvenWhenUpstreamDeliversShortBatches() {
    final CommandContext context = new BasicCommandContext();
    final Limit limit = new Limit() {
      @Override
      public int getValue(final CommandContext ctx) {
        return 5;
      }
    };

    final LimitExecutionStep step = new LimitExecutionStep(limit, context);
    step.setPrevious(new ShortBatchStep(context, 20));

    assertThat(drain(step, context))
        .as("LIMIT 5 must deliver 5 rows even when every upstream batch is 1 row short of what was asked for")
        .isEqualTo(5);
  }

  @Test
  void limitStopsAtWhatUpstreamActuallyHasEvenBelowTheLimit() {
    final CommandContext context = new BasicCommandContext();
    final Limit limit = new Limit() {
      @Override
      public int getValue(final CommandContext ctx) {
        return 5;
      }
    };

    final LimitExecutionStep step = new LimitExecutionStep(limit, context);
    step.setPrevious(new ShortBatchStep(context, 3));

    assertThat(drain(step, context)).isEqualTo(3);
  }

  /**
   * claude-review follow-up: the wrapping {@code ResultSet} only forwarded {@code hasNext()}/{@code next()}/
   * {@code close()}, silently dropping whatever {@code getExecutionPlan()}/{@code getStatistics()} the upstream
   * batch carried - the same forwarding {@code TimeoutStep} already does for its own wrapper.
   */
  @Test
  void forwardsExecutionPlanAndStatisticsFromUpstreamBatch() {
    final CommandContext context = new BasicCommandContext();
    final Limit limit = new Limit() {
      @Override
      public int getValue(final CommandContext ctx) {
        return 5;
      }
    };

    final ExecutionPlan marker = new ExecutionPlan() {
      @Override
      public List<ExecutionStep> getSteps() {
        return List.of();
      }

      @Override
      public String prettyPrint(final int depth, final int indent) {
        return "";
      }

      @Override
      public Result toResult() {
        return new ResultInternal();
      }
    };
    final QueryStatistics statsMarker = new QueryStatistics();

    final ExecutionStepInternal step = new LimitExecutionStep(limit, context);
    step.setPrevious(new AbstractExecutionStep(context) {
      @Override
      public ResultSet syncPull(final CommandContext ctx, final int nRecords) {
        final InternalResultSet rs = new InternalResultSet();
        rs.add(new ResultInternal().setProperty("i", 1));
        rs.setPlan(marker);
        rs.setStatistics(statsMarker);
        return rs;
      }
    });

    final ResultSet batch = step.syncPull(context, 100);
    assertThat(batch.getExecutionPlan()).contains(marker);
    assertThat(batch.getStatistics()).contains(statsMarker);
  }
}
