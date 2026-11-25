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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.parser.ExecutionPlanCache;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A statement whose projection holds count(), sum(), min(), max() or avg(), with or without a GROUP BY, is planned once per
 * statement text and its plan is copied for each execution, as a statement with only a WHERE and plain projections is. The
 * accumulators of the aggregate functions and the groups are made when a copy runs, so no execution sees the rows of another.
 */
class PlanCacheAggregateTest extends TestHelper {
  private static final int RECORDS = 200;

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("Item");
    database.getSchema().getType("Item").createProperty("grp", Type.INTEGER);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Item", "grp");
    database.getSchema().getType("Item").createProperty("seq", Type.INTEGER);
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument("Item").set("grp", i % 10).set("seq", i).set("tag", "t" + (i % 3)).save();
    });
  }

  @Test
  void anAggregateStatementIsPlannedOnce() {
    for (final String sql : new String[] { "SELECT count(*) FROM Item WHERE grp = :g", "SELECT sum(seq), min(seq), max(seq), avg(seq) FROM Item WHERE grp = :g",
        "SELECT tag, count(*) AS c FROM Item WHERE grp = :g GROUP BY tag", "SELECT count(*) FROM Item WHERE grp = :g AND seq > 50" }) {
      final ExecutionPlanCache cache = cache();
      // the first two executions plan: a literal of the text is a parameter from the second one on, which is another cache key
      drain(sql, Map.of("g", 0));
      drain(sql, Map.of("g", 1));
      final long puts = cache.getPutCount();
      final long hits = cache.getHitCount();
      for (int i = 2; i < 6; i++)
        drain(sql, Map.of("g", i));
      assertThat(cache.getPutCount() - puts).as("plans stored after the plan was made for: %s", sql).isZero();
      assertThat(cache.getHitCount() - hits).as("plans taken from the cache for: %s", sql).isEqualTo(4);
    }
  }

  @Test
  void aCachedAggregatePlanAnswersEachExecutionWithItsOwnRows() {
    final String count = "SELECT count(*) AS c FROM Item WHERE grp = :g";
    final String all = "SELECT count(*) AS c, sum(seq) AS s, min(seq) AS lo, max(seq) AS hi, avg(seq) AS a FROM Item WHERE grp = :g";
    // planned by the first execution, copied by the others, in an order that would show an accumulator kept from a run before
    for (int round = 0; round < 3; round++)
      for (final int g : new int[] { 0, 7, 3, 7, 12 }) {
        final List<Integer> own = new ArrayList<>();
        for (int i = 0; i < RECORDS; i++)
          if (i % 10 == g)
            own.add(i);

        assertThat(row(count, Map.of("g", g)).<Number>getProperty("c").longValue()).as("count g=%d", g).isEqualTo(own.size());

        final Result result = row(all, Map.of("g", g));
        assertThat(result.<Number>getProperty("c").longValue()).as("count g=%d", g).isEqualTo(own.size());
        if (own.isEmpty()) {
          // a group with no rows: the single row of an aggregation holds nothing, not what a run before left behind
          assertThat(result.<Object>getProperty("s")).as("sum g=%d", g).isNull();
          assertThat(result.<Object>getProperty("lo")).as("min g=%d", g).isNull();
          assertThat(result.<Object>getProperty("hi")).as("max g=%d", g).isNull();
          assertThat(result.<Object>getProperty("a")).as("avg g=%d", g).isNull();
        } else {
          assertThat(result.<Number>getProperty("s").longValue()).as("sum g=%d", g).isEqualTo(own.stream().mapToLong(Integer::longValue).sum());
          assertThat(result.<Number>getProperty("lo").intValue()).as("min g=%d", g).isEqualTo(own.getFirst());
          assertThat(result.<Number>getProperty("hi").intValue()).as("max g=%d", g).isEqualTo(own.getLast());
          assertThat(result.<Number>getProperty("a").doubleValue()).as("avg g=%d", g)
              .isEqualTo(own.stream().mapToLong(Integer::longValue).sum() / (double) own.size());
        }
      }
  }

  @Test
  void aCachedGroupByPlanDoesNotKeepTheGroupsOfAnExecutionBefore() {
    final String sql = "SELECT tag, count(*) AS c FROM Item WHERE grp = :g GROUP BY tag";
    for (int round = 0; round < 3; round++)
      for (final int g : new int[] { 4, 9, 4, 11 }) {
        final Map<String, Long> groups = new TreeMap<>();
        try (final ResultSet rs = database.query("sql", sql, Map.of("g", g))) {
          while (rs.hasNext()) {
            final Result row = rs.next();
            groups.put(row.getProperty("tag"), row.<Number>getProperty("c").longValue());
          }
        }
        final Map<String, Long> expected = new TreeMap<>();
        for (int i = 0; i < RECORDS; i++)
          if (i % 10 == g)
            expected.merge("t" + (i % 3), 1L, Long::sum);
        assertThat(groups).as("g=%d", g).isEqualTo(expected);
      }
  }

  @Test
  void aCachedAggregatePlanSeesTheRowsWrittenSinceItWasPlanned() {
    final String sql = "SELECT count(*) AS c FROM Item WHERE grp = :g";
    assertThat(row(sql, Map.of("g", 5)).<Number>getProperty("c").longValue()).isEqualTo(RECORDS / 10);
    database.transaction(() -> {
      for (int i = 0; i < 7; i++)
        database.newDocument("Item").set("grp", 5).set("seq", 1000 + i).set("tag", "t0").save();
    });
    assertThat(row(sql, Map.of("g", 5)).<Number>getProperty("c").longValue()).isEqualTo(RECORDS / 10 + 7);
    assertThat(cache().contains(sql)).isTrue();
  }

  @Test
  void aCountWithALimitReadFromAParameterIsStillPlannedForEveryExecution() {
    final String sql = "SELECT tag, count(*) AS c FROM Item WHERE grp = :g GROUP BY tag LIMIT :n";
    for (final int n : new int[] { 1, 3, 2 }) {
      int rows = 0;
      try (final ResultSet rs = database.query("sql", sql, Map.of("g", 4, "n", n))) {
        while (rs.hasNext()) {
          rs.next();
          rows++;
        }
      }
      assertThat(rows).as("LIMIT %d", n).isEqualTo(n);
    }
    assertThat(cache().contains(sql)).isFalse();
  }

  @Test
  void aCachedDistinctCountAndAParameterInsideTheAggregateAreBoundPerExecution() {
    final String distinct = "SELECT count(DISTINCT tag) AS c FROM Item WHERE grp = :g";
    final String shifted = "SELECT sum(seq + :p) AS s, max(seq + :p) AS hi FROM Item WHERE grp = :g";
    for (int round = 0; round < 3; round++)
      for (final int g : new int[] { 2, 6, 2 }) {
        final Set<String> tags = new HashSet<>();
        long sum = 0;
        int max = Integer.MIN_VALUE;
        int n = 0;
        for (int i = 0; i < RECORDS; i++)
          if (i % 10 == g) {
            tags.add("t" + (i % 3));
            sum += i;
            max = Math.max(max, i);
            n++;
          }
        assertThat(row(distinct, Map.of("g", g)).<Number>getProperty("c").longValue()).as("distinct g=%d", g).isEqualTo(tags.size());

        final int p = 1000 * (round + 1);
        final Result result = row(shifted, Map.of("g", g, "p", p));
        assertThat(result.<Number>getProperty("s").longValue()).as("sum g=%d p=%d", g, p).isEqualTo(sum + (long) n * p);
        assertThat(result.<Number>getProperty("hi").longValue()).as("max g=%d p=%d", g, p).isEqualTo(max + p);
      }
    assertThat(cache().contains(distinct)).isTrue();
    assertThat(cache().contains(shifted)).isTrue();
  }

  @Test
  void aCachedAggregatePlanAnswersAParallelScanAndASequentialOneAlike() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 2);
    database.getSchema().createDocumentType("Big", 4);
    final int rows = 20_000;
    database.transaction(() -> {
      for (int i = 0; i < rows; i++)
        database.newDocument("Big").set("grp", i % 10).set("seq", i).set("tag", "t" + (i % 7)).save();
    });

    final String sql = "SELECT tag, count(*) AS c, sum(seq) AS s FROM Big WHERE grp = :g GROUP BY tag";
    assertThat(explain(sql, Map.of("g", 1))).contains("(parallel");
    for (int round = 0; round < 2; round++)
      for (final int g : new int[] { 1, 8, 1 }) {
        final Map<String, Long> expectedCounts = new TreeMap<>();
        final Map<String, Long> expectedSums = new TreeMap<>();
        for (int i = 0; i < rows; i++)
          if (i % 10 == g) {
            expectedCounts.merge("t" + (i % 7), 1L, Long::sum);
            expectedSums.merge("t" + (i % 7), (long) i, Long::sum);
          }
        // outside a transaction the scan runs in parallel, inside one it is sequential: both run copies of the same cached plan
        assertThat(groups(sql, g, "c")).as("parallel count g=%d", g).isEqualTo(expectedCounts);
        assertThat(groups(sql, g, "s")).as("parallel sum g=%d", g).isEqualTo(expectedSums);
        database.transaction(() -> {
          assertThat(groups(sql, g, "c")).as("sequential count g=%d", g).isEqualTo(expectedCounts);
          assertThat(groups(sql, g, "s")).as("sequential sum g=%d", g).isEqualTo(expectedSums);
        });
      }
    assertThat(cache().contains(sql)).isTrue();
  }

  @Test
  void aHardwiredCountOrMaxIsCachedAndSeesTheRowsWrittenSinceItWasPlanned() {
    final String countType = "SELECT count(*) AS c FROM Item";
    final String countIndex = "SELECT count(*) AS c FROM index:`Item[grp]`";
    final String maxIndexed = "SELECT max(grp) AS m FROM Item";
    assertThat(explain(countType, Map.of())).contains("CALCULATE USERTYPE SIZE");
    assertThat(explain(countIndex, Map.of())).contains("CALCULATE INDEX SIZE");
    assertThat(explain(maxIndexed, Map.of())).contains("MAX FROM INDEX");

    for (int round = 0; round < 3; round++) {
      assertThat(row(countType, Map.of()).<Number>getProperty("c").longValue()).isEqualTo(RECORDS + round);
      assertThat(row(countIndex, Map.of()).<Number>getProperty("c").longValue()).isEqualTo(RECORDS + round);
      assertThat(row(maxIndexed, Map.of()).<Number>getProperty("m").intValue()).isEqualTo(round == 0 ? 9 : 100 + round - 1);
      final int grp = 100 + round;
      database.transaction(() -> database.newDocument("Item").set("grp", grp).set("seq", -1).set("tag", "t0").save());
    }
    // planned once: an execution after the first takes the cached plan instead of missing the lookup and planning again
    final ExecutionPlanCache cache = cache();
    assertThat(cache.contains(countType)).isTrue();
    assertThat(cache.contains(countIndex)).isTrue();
    assertThat(cache.contains(maxIndexed)).isTrue();
    final long puts = cache.getPutCount();
    row(countType, Map.of());
    row(maxIndexed, Map.of());
    assertThat(cache.getPutCount()).isEqualTo(puts);
  }

  @Test
  void anAggregateOverConstantsInAWhereIsNotFoldedAtPlanTime() {
    // the marker that lets a plan holding count() or sum() be cached does not make the aggregate a constant: folding it would
    // evaluate the accumulator once at planning and drop or keep every row on that one value
    for (final String sql : new String[] { "SELECT FROM Item WHERE count(1) = 1", "SELECT FROM Item WHERE sum(1) = 5",
        "SELECT FROM Item WHERE max(1) = 1" })
      assertThat(explain(sql, Map.of())).as(sql).contains("WITH FILTER");

    // a scalar call of the same functions over constants still folds
    assertThat(explain("SELECT FROM Item WHERE max(1, 3) = 3", Map.of())).doesNotContain("FILTER");
  }

  private Map<String, Long> groups(final String sql, final int g, final String property) {
    final Map<String, Long> groups = new TreeMap<>();
    try (final ResultSet rs = database.query("sql", sql, Map.of("g", g))) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        groups.put(row.getProperty("tag"), row.<Number>getProperty(property).longValue());
      }
    }
    return groups;
  }

  private String explain(final String sql, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + sql, params)) {
      return rs.next().getProperty("executionPlanAsString");
    }
  }

  private ExecutionPlanCache cache() {
    return ((DatabaseInternal) database).getExecutionPlanCache();
  }

  private void drain(final String sql, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("sql", sql, params)) {
      while (rs.hasNext())
        rs.next();
    }
  }

  private Result row(final String sql, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("sql", sql, params)) {
      final Result row = rs.next();
      assertThat(rs.hasNext()).as("one row for: %s", sql).isFalse();
      return row;
    }
  }
}
