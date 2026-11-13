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

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.parser.Limit;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class LimitExecutionStepTest extends TestHelper {

  @Test
  void shouldLimitResults() {
    database.getSchema().createDocumentType("TestLimit");

    database.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        database.newDocument("TestLimit").set("value", i).save();
      }
    });

    final ResultSet result = database.query("sql", "SELECT FROM TestLimit LIMIT 10");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isEqualTo(10);
    result.close();
  }

  @Test
  void shouldLimitToOne() {
    database.getSchema().createDocumentType("Product");

    database.transaction(() -> {
      database.newDocument("Product").set("name", "A").save();
      database.newDocument("Product").set("name", "B").save();
      database.newDocument("Product").set("name", "C").save();
    });

    final ResultSet result = database.query("sql", "SELECT FROM Product LIMIT 1");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isEqualTo(1);
    result.close();
  }

  @Test
  void shouldLimitZero() {
    database.getSchema().createDocumentType("Data");

    database.transaction(() ->
      database.newDocument("Data").set("value", 1).save());

    final ResultSet result = database.query("sql", "SELECT FROM Data LIMIT 0");
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void shouldLimitMoreThanAvailable() {
    database.getSchema().createDocumentType("Small");

    database.transaction(() -> {
      database.newDocument("Small").set("value", 1).save();
      database.newDocument("Small").set("value", 2).save();
    });

    final ResultSet result = database.query("sql", "SELECT FROM Small LIMIT 100");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isEqualTo(2);
    result.close();
  }

  @Test
  void shouldLimitWithOrderBy() {
    database.getSchema().createDocumentType("Score");

    database.transaction(() -> {
      database.newDocument("Score").set("points", 100).save();
      database.newDocument("Score").set("points", 200).save();
      database.newDocument("Score").set("points", 300).save();
      database.newDocument("Score").set("points", 400).save();
    });

    final ResultSet result = database.query("sql", "SELECT FROM Score ORDER BY points DESC LIMIT 2");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Integer>getProperty("points")).isEqualTo(400);

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Integer>getProperty("points")).isEqualTo(300);

    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void shouldLimitWithWhere() {
    database.getSchema().createDocumentType("User");

    database.transaction(() -> {
      for (int i = 1; i <= 20; i++) {
        database.newDocument("User").set("age", i * 5).save();
      }
    });

    final ResultSet result = database.query("sql", "SELECT FROM User WHERE age > 50 LIMIT 3");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      assertThat(item.<Integer>getProperty("age")).isGreaterThan(50);
      count++;
    }

    assertThat(count).isEqualTo(3);
    result.close();
  }

  @Test
  void shouldLimitDistinct() {
    database.getSchema().createDocumentType("Tag");

    database.transaction(() -> {
      database.newDocument("Tag").set("name", "java").save();
      database.newDocument("Tag").set("name", "python").save();
      database.newDocument("Tag").set("name", "java").save();
      database.newDocument("Tag").set("name", "go").save();
      database.newDocument("Tag").set("name", "rust").save();
    });

    final ResultSet result = database.query("sql", "SELECT DISTINCT name FROM Tag LIMIT 3");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isEqualTo(3);
    result.close();
  }

  @Test
  void shouldLimitGroupBy() {
    database.getSchema().createDocumentType("Order");

    database.transaction(() -> {
      database.newDocument("Order").set("region", "North").set("amount", 100).save();
      database.newDocument("Order").set("region", "South").set("amount", 200).save();
      database.newDocument("Order").set("region", "East").set("amount", 150).save();
      database.newDocument("Order").set("region", "West").set("amount", 175).save();
    });

    final ResultSet result = database.query("sql", "SELECT region, sum(amount) as total FROM Order GROUP BY region LIMIT 2");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isEqualTo(2);
    result.close();
  }

  @Test
  void shouldLimitOnEmptyResult() {
    database.getSchema().createDocumentType("Empty");

    final ResultSet result = database.query("sql", "SELECT FROM Empty LIMIT 10");
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void shouldLimitAboveIntRangeReturnsAllRows() {
    // Issue #5919 - Site A: a LIMIT value above Integer.MAX_VALUE used to narrow with
    // intValue(), wrapping to a negative int and returning 0 rows instead of the whole result.
    database.getSchema().createDocumentType("LimitOverflow");

    database.transaction(() -> {
      for (int i = 0; i < 5; i++) {
        database.newDocument("LimitOverflow").set("value", i).save();
      }
    });

    final ResultSet result = database.query("sql", "SELECT FROM LimitOverflow LIMIT 2147483648");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isEqualTo(5);
    result.close();
  }

  @Test
  void shouldLimitLargeDataset() {
    database.getSchema().createDocumentType("BigData");

    database.transaction(() -> {
      for (int i = 0; i < 1000; i++) {
        database.newDocument("BigData").set("id", i).save();
      }
    });

    final ResultSet result = database.query("sql", "SELECT FROM BigData LIMIT 50");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isEqualTo(50);
    result.close();
  }

  // ---- #7799: LIMIT counts the rows upstream delivered, not the batch size it asked for ----
  // LimitExecutionStep#syncPull used to advance its internal counter by the SIZE OF THE BATCH IT ASKED an upstream step
  // for, rather than by the number of rows the upstream step actually returned. No step on main currently under-delivers
  // a non-empty batch, so the bug needed a step built here that does - exactly the "one new or changed source step that
  // yields a short batch" scenario the issue describes. These tests drive the step directly and do not use the database.

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

  /** Regression test for issue #7799. */
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

  /** Regression test for issue #7799. */
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
   * Regression test for issue #7799.
   * <p>
   * Code review follow-up: the wrapping {@code ResultSet} only forwarded {@code hasNext()}/{@code next()}/
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
