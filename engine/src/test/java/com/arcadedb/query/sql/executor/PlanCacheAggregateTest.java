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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.parser.ExecutionPlanCache;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
