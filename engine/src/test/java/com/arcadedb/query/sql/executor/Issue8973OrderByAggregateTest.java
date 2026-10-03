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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression guard for #8973: an aggregate in {@code ORDER BY} (not aliased) was evaluated per record, so the groups were sorted by the
 * value of their first record instead of by the aggregate.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8973OrderByAggregateTest extends TestHelper {

  @BeforeEach
  void load() {
    database.command("sql", "CREATE VERTEX TYPE G");
    database.command("sql", "CREATE PROPERTY G.k INTEGER");
    database.command("sql", "CREATE PROPERTY G.v INTEGER");
    database.transaction(() -> {
      final int[][] rows = { { 1, 10 }, { 1, 200 }, { 2, 100 }, { 3, 50 }, { 3, 20 }, { 3, 20 } };
      for (final int[] r : rows)
        database.newVertex("G").set("k", r[0], "v", r[1]).save();
    });
  }

  private List<Integer> keys(final String sql) {
    final List<Integer> keys = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        // the generated ORDER BY alias must not leak into the rows
        assertThat(row.getPropertyNames()).as(sql).hasSize(2);
        keys.add(row.<Number>getProperty("k").intValue());
      }
    }
    return keys;
  }

  @Test
  void sumInOrderBy() {
    assertThat(keys("SELECT k, sum(v) AS s FROM G GROUP BY k ORDER BY sum(v) DESC")).containsExactly(1, 2, 3);
    assertThat(keys("SELECT k, sum(v) AS s FROM G GROUP BY k ORDER BY sum(v) ASC")).containsExactly(3, 2, 1);
  }

  @Test
  void maxAndAvgInOrderBy() {
    assertThat(keys("SELECT k, max(v) AS m FROM G GROUP BY k ORDER BY max(v) DESC")).containsExactly(1, 2, 3);
    assertThat(keys("SELECT k, avg(v) AS a FROM G GROUP BY k ORDER BY avg(v) DESC")).containsExactly(1, 2, 3);
  }

  @Test
  void countInOrderByWithLimit() {
    assertThat(keys("SELECT k, count(*) AS c FROM G GROUP BY k ORDER BY count(*) DESC")).containsExactly(3, 1, 2);
    assertThat(keys("SELECT k, count(*) AS c FROM G GROUP BY k ORDER BY count(*) DESC LIMIT 1")).containsExactly(3);
  }

  @Test
  void aggregateInOrderByNotInProjection() {
    final List<Integer> keys = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT k FROM G GROUP BY k ORDER BY count(*) DESC")) {
      while (rs.hasNext())
        keys.add(rs.next().<Number>getProperty("k").intValue());
    }
    assertThat(keys).containsExactly(3, 1, 2);
  }

  @Test
  void aggregateOrderByWithOtherShapes() {
    // each of these must not break the planner: lone *, no GROUP BY, mixed ORDER BY items, expression around the aggregate
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM G ORDER BY count(*)")) {
      assertThat(rs.next().<Number>getProperty("c").intValue()).isEqualTo(6);
    }
    assertThat(keys("SELECT k, sum(v) AS s FROM G GROUP BY k ORDER BY sum(v) + 1 DESC")).containsExactly(1, 2, 3);
    assertThat(keys("SELECT k, sum(v) AS s FROM G GROUP BY k ORDER BY count(*) DESC, k DESC")).containsExactly(3, 1, 2);
  }

  @Test
  void aggregateOrderByWithLoneStarKeepsEveryRecordUnchanged() {
    // a lone * is an early-exit shape of addOrderByProjections: it must not be switched into the aggregate split path, so every record comes back
    final List<Integer> ks = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT * FROM G ORDER BY count(*)")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(row.getPropertyNames()).contains("k", "v");
        ks.add(row.<Number>getProperty("k").intValue());
      }
    }
    assertThat(ks).containsExactlyInAnyOrder(1, 1, 2, 3, 3, 3);
  }

  @Test
  void aggregateOrderByWithUnwindKeepsEveryUnwoundRow() {
    database.command("sql", "CREATE DOCUMENT TYPE U");
    database.transaction(() -> {
      database.newDocument("U").set("k", 1, "tags", List.of("a", "b")).save();
      database.newDocument("U").set("k", 2, "tags", List.of("a")).save();
    });

    final List<String> plain = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT k, tags FROM U ORDER BY k DESC UNWIND tags")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        plain.add(row.<Number>getProperty("k") + ":" + row.getProperty("tags"));
      }
    }
    assertThat(plain).containsExactly("2:a", "1:a", "1:b");

    // UNWIND is an early-exit shape: the aggregate stays out of the split path, and no row is lost or duplicated
    final List<String> withAggregate = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT k, tags FROM U ORDER BY count(*) UNWIND tags")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        withAggregate.add(row.<Number>getProperty("k") + ":" + row.getProperty("tags"));
      }
    }
    assertThat(withAggregate).containsExactlyInAnyOrder("1:a", "1:b", "2:a");
  }

  @Test
  void aggregateOrderByWithoutGroupByKeepsEveryRecord() {
    // no GROUP BY: a non-aggregate projection keeps its per-record shape, the ORDER BY aggregate does not collapse the rows
    assertThat(keysOnly("SELECT k FROM G ORDER BY count(*)")).hasSize(6);
  }

  @Test
  void aggregateOrderByWithGroupByOnAnExpression() {
    // GROUP BY k % 10 already forces the split: the two triggers must compose
    final List<Integer> ks = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT k % 10 AS m, count(*) AS c FROM G GROUP BY k % 10 ORDER BY count(*) DESC")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(row.getPropertyNames()).hasSize(2);
        ks.add(row.<Number>getProperty("m").intValue());
      }
    }
    assertThat(ks).containsExactly(3, 1, 2);
  }

  private List<Integer> keysOnly(final String sql) {
    final List<Integer> ks = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        ks.add(rs.next().<Number>getProperty("k").intValue());
    }
    return ks;
  }

  @Test
  void twoDifferentAggregatesInOrderByAndTwoProjectedAggregates() {
    // the alias numbering must continue across the projection split and the ORDER BY split, or two aggregates share one alias
    assertThat(keysOnly("SELECT k, sum(v) AS s, max(v) AS m FROM G GROUP BY k ORDER BY count(*) DESC, sum(v) ASC")).containsExactly(3, 1, 2);
    assertThat(keysOnly("SELECT k, count(*) AS c FROM G GROUP BY k ORDER BY max(v) DESC, count(*) ASC")).containsExactly(1, 2, 3);

    final List<Number> counts = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT k, count(*) AS c, sum(v) AS s FROM G GROUP BY k ORDER BY sum(v) DESC")) {
      while (rs.hasNext())
        counts.add(rs.next().getProperty("c"));
    }
    assertThat(counts).extracting(Number::intValue).containsExactly(2, 1, 3);
  }
}
