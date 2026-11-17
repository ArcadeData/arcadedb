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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.QueryHeapBudget;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issues #12341, #11661 and #12340: openCypher ORDER BY evaluated every sort key inside the comparator, about 2 * log2(n) times
 * per row, and each evaluation of a column key built the row's whole name set. A non-deterministic key took a new value at
 * every comparison, so {@code ORDER BY rand()} failed with "Comparison method violates its general contract!" and the top-k
 * heap of {@code ORDER BY rand() LIMIT k} kept the late rows far more often than the early ones. Each row's keys are now
 * evaluated once, when the row enters the sort.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherOrderByKeysOnceIssue12341Test extends TestHelper {

  private List<Long> values(final String query) {
    final List<Long> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      rs.forEachRemaining(r -> values.add(r.<Number>getProperty("v").longValue()));
    }
    return values;
  }

  private double single(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return rs.next().<Number>getProperty("v").doubleValue();
    }
  }

  /** About one run in ten failed at 1,000 rows before: 40 passing runs on the old code is below 1.5% likely. */
  @Test
  void orderByRandSortsEveryRowAndNeverBreaksTheComparatorContract() {
    final List<Long> all = LongStream.rangeClosed(1, 1000).boxed().toList();
    for (int run = 0; run < 40; run++) {
      assertThat(values("UNWIND range(1, 1000) AS i RETURN i AS v ORDER BY rand()")).containsExactlyInAnyOrderElementsOf(all);
      assertThat(values("UNWIND range(1, 1000) AS i WITH i ORDER BY i % 7, rand() RETURN i AS v"))
          .containsExactlyInAnyOrderElementsOf(all);
    }
  }

  /**
   * A uniform sample of 1,000 of 1..20,000 has a mean of about 10,000 with a standard deviation of about 183, so the bounds are
   * more than 5 deviations wide. The biased top-k heap answered about 15,800.
   */
  @Test
  void orderByRandWithLimitIsAUniformSample() {
    for (int run = 0; run < 3; run++) {
      final double mean = single("UNWIND range(1, 20000) AS x WITH x ORDER BY rand() LIMIT 1000 RETURN avg(x) AS v");
      assertThat(mean).isBetween(9000.0, 11000.0);
    }
  }

  @Test
  void orderByRandWithLimitKeepsDistinctRows() {
    for (int run = 0; run < 20; run++) {
      final List<Long> top = values("UNWIND range(1, 100) AS i RETURN i AS v ORDER BY rand() LIMIT 10");
      assertThat(top).hasSize(10).doesNotHaveDuplicates().allMatch(v -> v >= 1 && v <= 100);
      assertThat(values("UNWIND range(1, 100) AS i RETURN i AS v ORDER BY i % 7, rand() LIMIT 14"))
          .containsExactlyInAnyOrder(7L, 14L, 21L, 28L, 35L, 42L, 49L, 56L, 63L, 70L, 77L, 84L, 91L, 98L);
    }
  }

  /** The top-k heap compares stored keys: ascending, descending, mixed directions, SKIP, and a heap larger than the input. */
  @Test
  void orderByWithLimitKeepsTheTopRowsByTheirKeys() {
    assertThat(values("UNWIND range(1, 100) AS i RETURN i AS v ORDER BY i % 7, i LIMIT 5")).containsExactly(7L, 14L, 21L, 28L,
        35L);
    assertThat(values("UNWIND range(1, 100) AS i RETURN i AS v ORDER BY i % 7 DESC, i DESC LIMIT 4")).containsExactly(97L, 90L,
        83L, 76L);
    assertThat(values("UNWIND range(1, 100) AS i RETURN i AS v ORDER BY i % 7 DESC, i LIMIT 4")).containsExactly(6L, 13L, 20L,
        27L);
    assertThat(values("UNWIND range(1, 100) AS i RETURN i AS v ORDER BY i % 7, i SKIP 12 LIMIT 4")).containsExactly(91L, 98L, 1L,
        8L);
    assertThat(values("UNWIND range(1, 20) AS i RETURN i AS v ORDER BY -i LIMIT 50")).containsExactlyElementsOf(
        LongStream.rangeClosed(1, 20).map(i -> 21 - i).boxed().toList());
  }

  /** Column keys and expression keys over vertex properties, the shapes the issue timed, sort as before. */
  @Test
  void columnAndPropertyKeysSortAsBefore() {
    database.transaction(() -> database.command("opencypher",
        "UNWIND range(0, 199) AS i CREATE (:O {a: 'g' + toString(i % 5), b: toFloat((i * 7919) % 100), c: i})").close());

    final List<Long> byProperty = values("MATCH (o:O) RETURN o.c AS v ORDER BY o.a ASC, o.b DESC, o.c ASC");
    final List<Long> byAlias = values("MATCH (o:O) WITH o, o.a AS a, o.b AS b, o.c AS c ORDER BY a ASC, b DESC, c ASC RETURN c AS v");
    final List<Long> bySql = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT c AS v FROM O ORDER BY a ASC, b DESC, c ASC")) {
      rs.forEachRemaining(r -> bySql.add(r.<Number>getProperty("v").longValue()));
    }
    assertThat(byProperty).hasSize(200).containsExactlyElementsOf(bySql);
    assertThat(byAlias).containsExactlyElementsOf(bySql);
    assertThat(values("MATCH (o:O) RETURN o.c AS v ORDER BY o.a ASC, o.b DESC, o.c ASC LIMIT 7"))
        .containsExactlyElementsOf(bySql.subList(0, 7));
  }

  /**
   * The keys the sort holds are charged to the heap budget with their rows: here each of 10 tiny rows is sorted by a list of
   * 100,000 numbers it does not hold, kept until the sort ends (the whole input) or for the rows the heap keeps (with LIMIT).
   */
  @Test
  void theKeysTheSortHoldsAreChargedToTheHeapBudget() {
    final String sortAll = "UNWIND range(1, 10) AS i RETURN i AS v ORDER BY [x IN range(1, 100000) | x * i] DESC";
    final String topK = sortAll + " LIMIT 5";
    final List<Long> descending = LongStream.rangeClosed(1, 10).map(i -> 11 - i).boxed().toList();
    assertThat(values(sortAll)).containsExactlyElementsOf(descending);
    assertThat(values(topK)).containsExactlyElementsOf(descending.subList(0, 5));

    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(QueryHeapBudget.getReservedBytes() / (1024 * 1024) + 4);
    for (final String query : List.of(sortAll, topK))
      assertThatThrownBy(() -> values(query)).as(query)
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_RAM.getKey());
  }
}
