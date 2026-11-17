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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.query.opencypher.ast.OrderByClause;
import com.arcadedb.query.opencypher.executor.steps.OrderByStep;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12341: openCypher ORDER BY evaluated every sort key inside the comparator, about 2·log2(n) times per row, building
 * a set of the row's property names each time. A key that is not deterministic, {@code rand()}, then broke the comparator's
 * contract (an intermittent "Comparison method violates its general contract!") and biased a top-k sort towards the rows read
 * last. Each row's keys are evaluated once, when the row enters the sort.
 */
class CypherOrderByKeysOnceIssue12341Test extends TestHelper {
  private static final int ROWS = 1_000;

  @Test
  void aFullSortReadsEachRowKeyOnce() {
    final AtomicInteger reads = new AtomicInteger();
    final List<Result> sorted = sort(reads, null);

    assertThat(sorted).hasSize(ROWS);
    assertThat(sorted.getFirst().<Integer>getProperty("k")).isEqualTo(0);
    assertThat(sorted.getLast().<Integer>getProperty("k")).isEqualTo(ROWS - 1);
    // the two reads above go through the same counter
    assertThat(reads.get() - 2).as("sort key reads for %d rows", ROWS).isEqualTo(ROWS);
  }

  @Test
  void aTopKSortReadsEachRowKeyOnce() {
    final AtomicInteger reads = new AtomicInteger();
    final List<Result> sorted = sort(reads, 10);

    assertThat(sorted).hasSize(10);
    assertThat(sorted.stream().map(r -> r.<Integer>getProperty("k")).toList()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    assertThat(reads.get() - 10).as("sort key reads for %d rows", ROWS).isEqualTo(ROWS);
  }

  @Test
  void orderByRandWithoutLimitNeverBreaksTheComparator() {
    // a key drawn again at each comparison made TimSort throw in about one run in three at this size
    for (int run = 0; run < 15; run++)
      try (final ResultSet rs = database.query("opencypher", "UNWIND range(1, 20000) AS x WITH x ORDER BY rand() RETURN count(*) AS n")) {
        assertThat(rs.next().<Long>getProperty("n")).isEqualTo(20_000L);
      }
  }

  @Test
  void orderByRandWithLimitPicksAcrossTheWholeInput() {
    // a uniform sample of 100 out of 1..20000 has a mean of 10000 with a standard deviation of about 577; drawing the key
    // again at each heap comparison favoured the last rows read (a mean near 15800)
    try (final ResultSet rs = database.query("opencypher",
        "UNWIND range(1, 20000) AS x WITH x ORDER BY rand() LIMIT 100 RETURN avg(x) AS mean")) {
      assertThat(rs.next().<Number>getProperty("mean").doubleValue()).isBetween(7_000.0, 13_000.0);
    }
  }

  /** Sorts {@link #ROWS} rows on the column {@code k}, read through a counter, in a shuffled order. */
  private List<Result> sort(final AtomicInteger reads, final Integer limit) {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    final List<Integer> keys = new ArrayList<>();
    for (int i = 0; i < ROWS; i++)
      keys.add((i * 7919) % ROWS);

    final OrderByStep step = new OrderByStep(new OrderByClause(List.of(new OrderByClause.OrderByItem("k", true))), context,
        null, limit);
    step.setPrevious(new AbstractExecutionStep(context) {
      private boolean served;

      @Override
      public ResultSet syncPull(final CommandContext ctx, final int nRecords) {
        final InternalResultSet rows = new InternalResultSet();
        if (!served)
          for (final int k : keys)
            rows.add(new CountingResult(k, reads));
        served = true;
        return rows;
      }
    });

    final List<Result> sorted = new ArrayList<>();
    final ResultSet rs = step.syncPull(context, 100);
    while (rs.hasNext())
      sorted.add(rs.next());
    return sorted;
  }

  private static final class CountingResult extends ResultInternal {
    private final AtomicInteger reads;

    CountingResult(final int k, final AtomicInteger reads) {
      super(Map.of("k", k));
      this.reads = reads;
    }

    @Override
    public <T> T getProperty(final String name) {
      if ("k".equals(name))
        reads.incrementAndGet();
      return super.getProperty(name);
    }
  }
}
