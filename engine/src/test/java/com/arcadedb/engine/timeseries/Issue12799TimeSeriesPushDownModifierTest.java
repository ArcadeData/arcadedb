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
package com.arcadedb.engine.timeseries;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for #12799: a method chained on {@code ts.timeBucket()} or on an aggregate is not a bare bucket or aggregate,
 * so the TIMESERIES push-down must leave it to the generic path instead of dropping the method.
 */
class Issue12799TimeSeriesPushDownModifierTest extends TestHelper {

  @BeforeEach
  void setupData() {
    database.command("sql", "CREATE TIMESERIES TYPE X TIMESTAMP ts TAGS (h STRING) FIELDS (v DOUBLE) SHARDS 1");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO X SET ts = 36000000, h = 'a', v = 1.0");
      database.command("sql", "INSERT INTO X SET ts = 122400000, h = 'a', v = 2.0");
      database.command("sql", "INSERT INTO X SET ts = 39600000, h = 'a', v = 3.0");
    });
  }

  @Test
  void formatOnBucketGroupsByTheFormattedValue() {
    final String sql = "SELECT ts.timeBucket('1h', ts).format('HH') AS hod, count(*) AS c FROM X GROUP BY hod";
    final Map<String, Long> counts = new TreeMap<>();
    for (final Result r : query(sql))
      counts.put(r.getProperty("hod"), ((Number) r.getProperty("c")).longValue());
    assertThat(counts).isEqualTo(Map.of("10", 2L, "11", 1L));
    assertThat(plan(sql)).doesNotContain("AGGREGATE FROM TIMESERIES");
  }

  @Test
  void asLongOnBucketAnswersEpochMillis() {
    final List<Long> buckets = new ArrayList<>();
    for (final Result r : query("SELECT ts.timeBucket('1h', ts).asLong() AS b, count(*) AS c FROM X GROUP BY b"))
      buckets.add(r.getProperty("b"));
    assertThat(buckets).containsExactlyInAnyOrder(36000000L, 39600000L, 122400000L);
  }

  @Test
  void asStringOnAggregateAnswersAString() {
    final List<Result> rows = query("SELECT ts.timeBucket('1h', ts) AS b, max(v).asString() AS m FROM X GROUP BY b");
    assertThat(rows).hasSize(3);
    for (final Result r : rows)
      assertThat((Object) r.getProperty("m")).isInstanceOf(String.class);
  }

  @Test
  void asStringOnLastPointAnswersAString() {
    final String sql = "SELECT ts.last(v, ts).asString() AS l FROM X";
    final List<Result> rows = query(sql);
    assertThat(rows).hasSize(1);
    assertThat((Object) rows.get(0).getProperty("l")).isEqualTo("2.0");
    // The method reshapes the aggregated value only: the newest row alone still answers, so the fetch stays newest-first
    // and stops at one row instead of reading the whole series
    assertThat(plan(sql)).contains("FETCH FROM TIMESERIES DESC X").contains("TOP 1");
  }

  @Test
  void chainedMethodsOnLastPointKeepTheOneRowRead() {
    final String sql = "SELECT ts.last(v, ts).asString().substring(0, 1) AS l FROM X";
    final List<Result> rows = query(sql);
    assertThat(rows).hasSize(1);
    assertThat((Object) rows.get(0).getProperty("l")).isEqualTo("2");
    assertThat(plan(sql)).contains("FETCH FROM TIMESERIES DESC X").contains("TOP 1");
  }

  @Test
  void aggregateInsideTheLastPointModifierReadsEveryRow() {
    // An aggregate in the method argument is about the whole input, so nothing proves the newest row alone answers it
    assertThat(plan("SELECT ts.last(v, ts).append(count(*)) AS l FROM X")).doesNotContain("TOP 1");
  }

  @Test
  void indexOrFieldModifierOnBucketOrAggregateAnswersAsTheParenthesisedSpelling() {
    final String[][] spellings = {
        { "SELECT ts.timeBucket('1h', ts) AS b, max(v)[0] AS m FROM X GROUP BY b",
            "SELECT ts.timeBucket('1h', ts) AS b, (max(v))[0] AS m FROM X GROUP BY b" },
        { "SELECT ts.timeBucket('1h', ts).time AS b, count(*) AS c FROM X GROUP BY b",
            "SELECT (ts.timeBucket('1h', ts)).time AS b, count(*) AS c FROM X GROUP BY b" } };
    for (final String[] pair : spellings) {
      assertThat(plan(pair[0])).doesNotContain("AGGREGATE FROM TIMESERIES");
      assertThat(rowsAsStrings(pair[0])).isEqualTo(rowsAsStrings(pair[1]));
    }
  }

  @Test
  void bareBucketAndAggregateStillPushDown() {
    final String sql = "SELECT ts.timeBucket('1h', ts) AS b, max(v) AS m FROM X GROUP BY b";
    assertThat(plan(sql)).contains("AGGREGATE FROM TIMESERIES");
    assertThat(query(sql)).hasSize(3);
  }

  private List<Result> query(final String sql) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }

  private List<String> rowsAsStrings(final String sql) {
    final List<String> rows = new ArrayList<>();
    for (final Result r : query(sql))
      rows.add(new TreeMap<>(r.toMap()).toString());
    rows.sort(null);
    return rows;
  }

  private String plan(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }
}
