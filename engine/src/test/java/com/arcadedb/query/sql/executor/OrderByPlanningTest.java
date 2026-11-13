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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Planning and execution of SQL {@code ORDER BY}: bounded top-k sorting, index-served ordering through aliased or unprojected
 * sort keys (with and without a range in the WHERE), and aggregates used as sort keys.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class OrderByPlanningTest extends TestHelper {

  // ---- #8802: an ORDER BY bounded by LIMIT keeps its best rows in a heap ----
  // An ORDER BY bounded by LIMIT (or SKIP + LIMIT) keeps its best rows in a heap instead of sorting the buffer every few rows.
  // The answer must be the slice of the unbounded sort, ties included: among equal keys the row that arrived first wins, as the
  // stable sort-and-truncate did.

  private static final int TOP_K_ROWS = 5_000;

  private void loadTopKFixture() {
    database.command("sql", "CREATE VERTEX TYPE V BUCKETS 1");
    database.command("sql", "CREATE PROPERTY V.seq INTEGER");
    database.command("sql", "CREATE PROPERTY V.x INTEGER");
    database.command("sql", "CREATE PROPERTY V.y INTEGER");
    database.transaction(() -> {
      for (int i = 0; i < TOP_K_ROWS; i++)
        // many ties on x (50 distinct values), a second key y to order the ties by
        database.newVertex("V").set("seq", i, "x", (i * 31) % 50, "y", (i * 17) % 7).save();
    });
  }

  private List<Integer> seqs(final String sql) {
    final List<Integer> result = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        result.add(rs.next().<Number>getProperty("seq").intValue());
    }
    return result;
  }

  private void assertBoundedIsSliceOfUnbounded(final String orderBy) {
    final List<Integer> all = seqs("SELECT seq, x, y FROM V ORDER BY " + orderBy);
    assertThat(all).hasSize(TOP_K_ROWS);
    for (final int[] window : new int[][] { { 0, 1 }, { 0, 10 }, { 0, 77 }, { 5, 10 }, { 1000, 10 }, { 4990, 100 }, { 0, TOP_K_ROWS },
        { 0, TOP_K_ROWS + 100 } }) {
      final int skip = window[0], limit = window[1];
      final String sql = "SELECT seq, x, y FROM V ORDER BY " + orderBy + (skip > 0 ? " SKIP " + skip : "") + " LIMIT " + limit;
      final List<Integer> expected = all.subList(Math.min(skip, TOP_K_ROWS), Math.min(skip + limit, TOP_K_ROWS));
      assertThat(seqs(sql)).as(sql).isEqualTo(expected);
    }
  }

  /**
   * Regression test for issue #8802.
   */
  @Test
  void singleKeyWithTies() {
    loadTopKFixture();
    assertBoundedIsSliceOfUnbounded("x ASC");
    assertBoundedIsSliceOfUnbounded("x DESC");
  }

  /**
   * Regression test for issue #8802.
   */
  @Test
  void compositeKeyWithTies() {
    loadTopKFixture();
    assertBoundedIsSliceOfUnbounded("x ASC, y DESC");
    assertBoundedIsSliceOfUnbounded("y DESC, x ASC");
  }

  /**
   * Regression test for issue #8802.
   */
  @Test
  void limitZeroAndSkipPastTheEnd() {
    loadTopKFixture();
    assertThat(seqs("SELECT seq FROM V ORDER BY x LIMIT 0")).isEmpty();
    assertThat(seqs("SELECT seq FROM V ORDER BY x SKIP " + (TOP_K_ROWS + 10) + " LIMIT 5")).isEmpty();
  }

  // ---- #8811: ORDER BY a LIMIT k served from the index through a renamed or unprojected sort key ----
  // ORDER BY a LIMIT k was served from the index on a only when the projection kept a under its own name. A projection that
  // renames the sort key, or leaves it out, made the planner scan and sort.

  private void loadAliasIndexFixture() {
    database.command("sql", "CREATE VERTEX TYPE V");
    database.command("sql", "CREATE PROPERTY V.a LONG (mandatory true, notnull true)");
    database.command("sql", "CREATE PROPERTY V.b LONG");
    database.command("sql", "CREATE INDEX ON V (a) NOTUNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < 200; i++)
        database.newVertex("V").set("a", (long) ((i * 37) % 200), "b", (long) (1000 - i)).save();
    });
  }

  private static String plan(final ResultSet rs) {
    return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
  }

  private void assertIndexOrdered(final String sql, final String column, final List<Long> expected) {
    try (final ResultSet rs = database.query("sql", sql)) {
      final List<Long> got = new ArrayList<>();
      while (rs.hasNext()) {
        final Result row = rs.next();
        // the alias generated for the sort key must not leak into the rows
        assertThat(row.getPropertyNames()).as(sql).containsExactly(column);
        got.add(row.<Number>getProperty(column).longValue());
      }
      assertThat(plan(rs)).as(sql).contains("FETCH FROM INDEX VALUES").contains("V[a]").doesNotContain("FETCH FROM TYPE");
      assertThat(got).as(sql).isEqualTo(expected);
    }
  }

  /**
   * Regression test for issue #8811.
   */
  @Test
  void renamedSortKeyUsesTheIndex() {
    loadAliasIndexFixture();
    assertIndexOrdered("SELECT a AS c FROM V ORDER BY a ASC LIMIT 5", "c", List.of(0L, 1L, 2L, 3L, 4L));
    assertIndexOrdered("SELECT a AS c FROM V ORDER BY c ASC LIMIT 5", "c", List.of(0L, 1L, 2L, 3L, 4L));
    assertIndexOrdered("SELECT a AS c FROM V ORDER BY a DESC LIMIT 5", "c", List.of(199L, 198L, 197L, 196L, 195L));
    assertIndexOrdered("SELECT a AS c FROM V ORDER BY c DESC LIMIT 5", "c", List.of(199L, 198L, 197L, 196L, 195L));
  }

  /**
   * Regression test for issue #8811.
   */
  @Test
  void sortKeyLeftOutOfTheProjectionUsesTheIndex() {
    loadAliasIndexFixture();
    // b = 1000 - i, a = (i * 37) % 200: a = 0 is i = 0, a = 1 is i = 173 (173 * 37 = 6401), ...
    final List<Long> expected = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT b FROM V WHERE b > -1 ORDER BY a ASC LIMIT 5")) {
      while (rs.hasNext())
        expected.add(rs.next().<Number>getProperty("b").longValue());
    }
    assertThat(expected).hasSize(5);
    assertThat(expected.getFirst()).isEqualTo(1000L);
    assertIndexOrdered("SELECT b FROM V ORDER BY a ASC LIMIT 5", "b", expected);
  }

  /**
   * Regression test for issue #8811.
   */
  @Test
  void aliasShadowingTheIndexedPropertyDoesNotUseTheIndex() {
    loadAliasIndexFixture();
    // `b AS a` is the projected `a`: ORDER BY a sorts by b, so the index on the property a must not answer it
    try (final ResultSet rs = database.query("sql", "SELECT b AS a FROM V ORDER BY a ASC LIMIT 3")) {
      final List<Long> got = new ArrayList<>();
      while (rs.hasNext())
        got.add(rs.next().<Number>getProperty("a").longValue());
      assertThat(got).isEqualTo(List.of(801L, 802L, 803L));
      assertThat(plan(rs)).doesNotContain("V[a]");
    }
  }

  /**
   * Regression test for issue #8811.
   */
  @Test
  void computedAliasDoesNotUseTheIndex() {
    loadAliasIndexFixture();
    try (final ResultSet rs = database.query("sql", "SELECT a * -1 AS a FROM V ORDER BY a ASC LIMIT 3")) {
      final List<Long> got = new ArrayList<>();
      while (rs.hasNext())
        got.add(rs.next().<Number>getProperty("a").longValue());
      assertThat(got).isEqualTo(List.of(-199L, -198L, -197L));
      assertThat(plan(rs)).doesNotContain("V[a]");
    }
  }

  /**
   * Regression test for issue #8811.
   */
  @Test
  void duplicateAliasDoesNotUseTheIndexAndMatchesTheUnindexedAnswer() {
    loadAliasIndexFixture();
    final List<Object> got = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT a, b AS a FROM V ORDER BY a ASC LIMIT 3")) {
      while (rs.hasNext())
        got.add(rs.next().getProperty("a"));
      assertThat(plan(rs)).doesNotContain("V[a]");
    }
    // the same sort over a type without the index
    database.command("sql", "CREATE VERTEX TYPE W");
    database.command("sql", "CREATE PROPERTY W.a LONG");
    database.command("sql", "CREATE PROPERTY W.b LONG");
    database.transaction(() -> database.command("sql", "INSERT INTO W SELECT a, b FROM V"));
    final List<Object> expected = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT a, b AS a FROM W ORDER BY a ASC LIMIT 3")) {
      while (rs.hasNext())
        expected.add(rs.next().getProperty("a"));
    }
    assertThat(got).hasSize(3).isEqualTo(expected);
  }

  // ---- #8836: ORDER BY a LIMIT k with a range on a served from the index through a renamed or unprojected sort key ----
  // ORDER BY a LIMIT k with a range on a in the WHERE was served from the index only when a was projected under its own name.
  // A renamed or unprojected sort key made the planner read the range and sort it.

  private void loadAliasRangeIndexFixture() {
    database.command("sql", "CREATE VERTEX TYPE V");
    database.command("sql", "CREATE PROPERTY V.a LONG (mandatory true, notnull true)");
    database.command("sql", "CREATE PROPERTY V.b LONG");
    database.command("sql", "CREATE INDEX ON V (a) NOTUNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < 200; i++)
        database.newVertex("V").set("a", (long) i, "b", (long) (1000 - i)).save();
    });
  }

  private void assertRangeIndexOrdered(final String sql, final String column, final List<Long> expected) {
    try (final ResultSet rs = database.query("sql", sql)) {
      final List<Long> got = new ArrayList<>();
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(row.getPropertyNames()).as(sql).containsExactly(column);
        got.add(row.<Number>getProperty(column).longValue());
      }
      assertThat(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2)).as(sql).contains("V[a]").doesNotContain("ORDER BY")
          .doesNotContain("FETCH FROM TYPE");
      assertThat(got).as(sql).isEqualTo(expected);
    }
  }

  /**
   * Regression test for issue #8836.
   */
  @Test
  void renamedSortKeyWithRangeUsesTheIndex() {
    loadAliasRangeIndexFixture();
    final List<Long> asc = List.of(101L, 102L, 103L, 104L, 105L);
    assertRangeIndexOrdered("SELECT a AS c FROM V WHERE a > 100 ORDER BY a ASC LIMIT 5", "c", asc);
    assertRangeIndexOrdered("SELECT a AS c FROM V WHERE a > 100 ORDER BY c ASC LIMIT 5", "c", asc);
    assertRangeIndexOrdered("SELECT a AS c FROM V WHERE a < 100 ORDER BY a DESC LIMIT 5", "c", List.of(99L, 98L, 97L, 96L, 95L));
    assertRangeIndexOrdered("SELECT a AS c FROM V WHERE a BETWEEN 50 AND 150 ORDER BY a ASC LIMIT 3", "c", List.of(50L, 51L, 52L));
  }

  /**
   * Regression test for issue #8836.
   */
  @Test
  void unprojectedSortKeyWithRangeUsesTheIndex() {
    loadAliasRangeIndexFixture();
    assertRangeIndexOrdered("SELECT b FROM V WHERE a > 100 ORDER BY a ASC LIMIT 3", "b", List.of(899L, 898L, 897L));
  }

  /**
   * Regression test for issue #8836.
   */
  @Test
  void aliasShadowingTheIndexedPropertyStillSorts() {
    loadAliasRangeIndexFixture();
    try (final ResultSet rs = database.query("sql", "SELECT b AS a FROM V WHERE a > 100 ORDER BY a ASC LIMIT 3")) {
      final List<Long> got = new ArrayList<>();
      while (rs.hasNext())
        got.add(rs.next().<Number>getProperty("a").longValue());
      assertThat(got).isEqualTo(List.of(801L, 802L, 803L));
    }
  }

  /**
   * Regression test for issue #8836.
   */
  @Test
  void computedAliasShadowingTheKeyKeepsTheSort() {
    loadAliasRangeIndexFixture();
    try (final ResultSet rs = database.query("sql", "SELECT a * -1 AS a FROM V WHERE a > 100 ORDER BY a ASC LIMIT 3")) {
      final List<Long> got = new ArrayList<>();
      while (rs.hasNext())
        got.add(rs.next().<Number>getProperty("a").longValue());
      assertThat(got).isEqualTo(List.of(-199L, -198L, -197L));
    }
  }

  /**
   * Regression test for issue #8836.
   */
  @Test
  void duplicateAliasKeepsTheSort() {
    loadAliasRangeIndexFixture();
    try (final ResultSet rs = database.query("sql", "SELECT a, b AS a FROM V WHERE a > 100 ORDER BY a ASC LIMIT 3")) {
      final List<Object> got = new ArrayList<>();
      while (rs.hasNext())
        got.add(rs.next().getProperty("a"));
      assertThat(got).hasSize(3);
      assertThat(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2)).contains("ORDER BY");
    }
  }

  /**
   * Regression test for issue #8836.
   */
  @Test
  void distinctWithUnprojectedSortKey() {
    loadAliasRangeIndexFixture();
    try (final ResultSet rs = database.query("sql", "SELECT DISTINCT b FROM V WHERE a > 100 ORDER BY a ASC LIMIT 3")) {
      final List<Long> got = new ArrayList<>();
      while (rs.hasNext())
        got.add(rs.next().<Number>getProperty("b").longValue());
      assertThat(got).isEqualTo(List.of(899L, 898L, 897L));
    }
  }

  /**
   * Regression test for issue #8836.
   */
  @Test
  void parameterizedDirectionKeepsTheSort() {
    loadAliasRangeIndexFixture();
    try (final ResultSet rs = database.command("sql", "SELECT a AS c FROM V WHERE a > 100 ORDER BY c :direction LIMIT 3",
        Map.of("direction", "DESC"))) {
      final List<Long> got = new ArrayList<>();
      while (rs.hasNext())
        got.add(rs.next().<Number>getProperty("c").longValue());
      assertThat(got).isEqualTo(List.of(199L, 198L, 197L));
    }
  }

  /**
   * Regression test for issue #8836.
   */
  @Test
  void parameterizedDirectionWithLimit() {
    loadAliasRangeIndexFixture();
    try (final ResultSet rs = database.command("sql", "SELECT b FROM V ORDER BY b :direction LIMIT 3", Map.of("direction", "DESC"))) {
      final List<Long> got = new ArrayList<>();
      while (rs.hasNext())
        got.add(rs.next().<Number>getProperty("b").longValue());
      assertThat(got).isEqualTo(List.of(1000L, 999L, 998L));
    }
  }

  // ---- #8973: an aggregate in ORDER BY is evaluated per group ----
  // An aggregate in ORDER BY (not aliased) was evaluated per record, so the groups were sorted by the value of their first
  // record instead of by the aggregate.

  private void loadGroupedAggregateFixture() {
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

  private List<Integer> keysOnly(final String sql) {
    final List<Integer> ks = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        ks.add(rs.next().<Number>getProperty("k").intValue());
    }
    return ks;
  }

  /**
   * Regression test for issue #8973.
   */
  @Test
  void sumInOrderBy() {
    loadGroupedAggregateFixture();
    assertThat(keys("SELECT k, sum(v) AS s FROM G GROUP BY k ORDER BY sum(v) DESC")).containsExactly(1, 2, 3);
    assertThat(keys("SELECT k, sum(v) AS s FROM G GROUP BY k ORDER BY sum(v) ASC")).containsExactly(3, 2, 1);
  }

  /**
   * Regression test for issue #8973.
   */
  @Test
  void maxAndAvgInOrderBy() {
    loadGroupedAggregateFixture();
    assertThat(keys("SELECT k, max(v) AS m FROM G GROUP BY k ORDER BY max(v) DESC")).containsExactly(1, 2, 3);
    assertThat(keys("SELECT k, avg(v) AS a FROM G GROUP BY k ORDER BY avg(v) DESC")).containsExactly(1, 2, 3);
  }

  /**
   * Regression test for issue #8973.
   */
  @Test
  void countInOrderByWithLimit() {
    loadGroupedAggregateFixture();
    assertThat(keys("SELECT k, count(*) AS c FROM G GROUP BY k ORDER BY count(*) DESC")).containsExactly(3, 1, 2);
    assertThat(keys("SELECT k, count(*) AS c FROM G GROUP BY k ORDER BY count(*) DESC LIMIT 1")).containsExactly(3);
  }

  /**
   * Regression test for issue #8973.
   */
  @Test
  void aggregateInOrderByNotInProjection() {
    loadGroupedAggregateFixture();
    final List<Integer> keys = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT k FROM G GROUP BY k ORDER BY count(*) DESC")) {
      while (rs.hasNext())
        keys.add(rs.next().<Number>getProperty("k").intValue());
    }
    assertThat(keys).containsExactly(3, 1, 2);
  }

  /**
   * Regression test for issue #8973.
   */
  @Test
  void aggregateOrderByWithOtherShapes() {
    loadGroupedAggregateFixture();
    // each of these must not break the planner: lone *, no GROUP BY, mixed ORDER BY items, expression around the aggregate
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM G ORDER BY count(*)")) {
      assertThat(rs.next().<Number>getProperty("c").intValue()).isEqualTo(6);
    }
    assertThat(keys("SELECT k, sum(v) AS s FROM G GROUP BY k ORDER BY sum(v) + 1 DESC")).containsExactly(1, 2, 3);
    assertThat(keys("SELECT k, sum(v) AS s FROM G GROUP BY k ORDER BY count(*) DESC, k DESC")).containsExactly(3, 1, 2);
  }

  /**
   * Regression test for issue #8973.
   */
  @Test
  void aggregateOrderByWithLoneStarKeepsEveryRecordUnchanged() {
    loadGroupedAggregateFixture();
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

  /**
   * Regression test for issue #8973.
   */
  @Test
  void aggregateOrderByWithUnwindKeepsEveryUnwoundRow() {
    loadGroupedAggregateFixture();
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

  /**
   * Regression test for issue #8973.
   */
  @Test
  void aggregateOrderByWithoutGroupByKeepsEveryRecord() {
    loadGroupedAggregateFixture();
    // no GROUP BY: a non-aggregate projection keeps its per-record shape, the ORDER BY aggregate does not collapse the rows
    assertThat(keysOnly("SELECT k FROM G ORDER BY count(*)")).hasSize(6);
  }

  /**
   * Regression test for issue #8973.
   */
  @Test
  void aggregateOrderByWithGroupByOnAnExpression() {
    loadGroupedAggregateFixture();
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

  /**
   * Regression test for issue #8973.
   */
  @Test
  void twoDifferentAggregatesInOrderByAndTwoProjectedAggregates() {
    loadGroupedAggregateFixture();
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

  /**
   * Regression test for issue #8973.
   */
  @Test
  void aggregateOrderByOnATimeSeriesBucketQuery() {
    loadGroupedAggregateFixture();
    database.command("sql", "CREATE TIMESERIES TYPE Reading8973 TIMESTAMP ts FIELDS (temperature DOUBLE)");
    database.transaction(() -> {
      // hour 0 holds 1 row summing to 100, hour 1 holds 3 rows summing to 3: the bucket with the most rows comes second and has the smaller sum
      database.command("sql", "INSERT INTO Reading8973 SET ts = 1000, temperature = 100.0");
      database.command("sql", "INSERT INTO Reading8973 SET ts = 3600000, temperature = 1.0");
      database.command("sql", "INSERT INTO Reading8973 SET ts = 3601000, temperature = 1.0");
      database.command("sql", "INSERT INTO Reading8973 SET ts = 3602000, temperature = 1.0");
    });

    try (final ResultSet rs = database.query("sql",
        "SELECT ts.timeBucket('1h', ts) AS hour, sum(temperature) AS s FROM Reading8973 GROUP BY hour ORDER BY count(*) DESC LIMIT 1")) {
      final Result row = rs.next();
      assertThat(row.<Number>getProperty("s").doubleValue()).isEqualTo(3.0);
      assertThat(rs.hasNext()).isFalse();
    }

    // the opposite direction must pick the other bucket: with the aggregate never computed both directions would answer the same row
    try (final ResultSet rs = database.query("sql",
        "SELECT ts.timeBucket('1h', ts) AS hour, sum(temperature) AS s FROM Reading8973 GROUP BY hour ORDER BY count(*) ASC LIMIT 1")) {
      assertThat(rs.next().<Number>getProperty("s").doubleValue()).isEqualTo(100.0);
    }
  }

  // ---- #11700: a keyset page on a common equality walks the index of the sort key in order ----
  // WHERE graph_name = ? AND external_id > :after ORDER BY external_id LIMIT n fetched every row the equality matches from its index
  // and sorted them for each page, so walking a partition page by page was quadratic. When the equality matches enough rows, the
  // ordered range on the sort key's index, filtered on the equality, stops after n of them.

  private static final int KEYSET_ROWS = 20_000;
  private static final int KEYSET_PAGE = 50;

  private void loadKeysetFixture() {
    database.command("sql", "CREATE VERTEX TYPE Chunk");
    database.command("sql", "CREATE PROPERTY Chunk.external_id STRING");
    database.command("sql", "CREATE INDEX ON Chunk (external_id) UNIQUE");
    database.command("sql", "CREATE PROPERTY Chunk.graph_name STRING");
    database.command("sql", "CREATE INDEX ON Chunk (graph_name) NOTUNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < KEYSET_ROWS; i++) {
        // the ids are inserted out of order, so the record order of the equality's index is not the order of the sort key
        final int id = (int) ((i * 7919L) % KEYSET_ROWS);
        // g0..g9 hold 10% of the chunks each, 'rare' 0.1% of them (taken from g9)
        database.newVertex("Chunk").set("external_id", String.format("c%06d", id), "graph_name", id % 1000 == 999 ? "rare" : "g" + id % 10)
            .save();
      }
    });
  }

  private static String keysetPage(final String where, final boolean ascending) {
    return "SELECT external_id FROM Chunk WHERE (" + where + ") AND external_id " + (ascending ? ">" : "<") + " :bound ORDER BY external_id "
        + (ascending ? "ASC" : "DESC") + " LIMIT " + KEYSET_PAGE;
  }

  /** Walks the pages of {@link #keysetPage} from the first to the last, asserting the plan of each with {@code planCheck}. */
  private List<String> keysetWalk(final String where, final boolean ascending, final Consumer<String> planCheck) {
    final List<String> walked = new ArrayList<>();
    // 'd' sorts after every 'c...' id
    String bound = ascending ? "" : "d";
    final String sql = keysetPage(where, ascending);
    while (true) {
      final List<String> page = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", sql, Map.of("bound", bound))) {
        while (rs.hasNext())
          page.add(rs.next().getProperty("external_id"));
        planCheck.accept(plan(rs));
      }
      walked.addAll(page);
      if (page.size() < KEYSET_PAGE)
        return walked;
      bound = page.getLast();
    }
  }

  /** The rows of the walk read in one statement without a LIMIT, which keeps the plan of the equality's index and sorts. */
  private List<String> sortedThroughTheEquality(final String where, final boolean ascending) {
    final List<String> result = new ArrayList<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT external_id FROM Chunk WHERE " + where + " ORDER BY external_id " + (ascending ? "ASC" : "DESC"))) {
      while (rs.hasNext())
        result.add(rs.next().getProperty("external_id"));
      assertThat(plan(rs)).contains("FETCH FROM INDEX Chunk[graph_name]").contains("ORDER BY");
    }
    return result;
  }

  private static void walksTheSortKeyIndex(final String plan) {
    assertThat(plan).contains("FETCH FROM INDEX Chunk[external_id]").contains("FILTER ITEMS WHERE").doesNotContain("ORDER BY")
        .doesNotContain("Chunk[graph_name]");
  }

  /**
   * Regression test for issue #11700.
   */
  @Test
  void keysetWalkOnACommonEqualityWalksTheSortKeyIndex() {
    loadKeysetFixture();
    for (final String where : List.of("graph_name = 'g3'", "graph_name IN ['g3', 'g4']"))
      for (final boolean ascending : new boolean[] { true, false }) {
        final List<String> expected = sortedThroughTheEquality(where, ascending);
        assertThat(expected).hasSize(where.contains("IN") ? 4_000 : 2_000);
        assertThat(keysetWalk(where, ascending, OrderByPlanningTest::walksTheSortKeyIndex)).as(where).isEqualTo(expected);
      }
  }

  /**
   * Regression test for issue #11700: an equality matching 20 rows of 20,000 is read from its index, where the ordered walk would
   * read about the whole type for one page.
   */
  @Test
  void keysetWalkOnASelectiveEqualityKeepsItsIndex() {
    loadKeysetFixture();
    final List<String> expected = sortedThroughTheEquality("graph_name = 'rare'", true);
    assertThat(expected).hasSize(20);
    assertThat(keysetWalk("graph_name = 'rare'", true,
        plan -> assertThat(plan).contains("FETCH FROM INDEX Chunk[graph_name]").contains("ORDER BY"))).isEqualTo(expected);
  }

  /**
   * Regression test for issue #11700: a composite index on (equality, sort key) answers the page directly and is kept.
   */
  @Test
  void keysetWalkKeepsACompositeIndexOnTheEqualityAndTheSortKey() {
    loadKeysetFixture();
    final List<String> expected = sortedThroughTheEquality("graph_name = 'g3'", true);
    database.command("sql", "CREATE INDEX ON Chunk (graph_name, external_id) NOTUNIQUE");
    assertThat(keysetWalk("graph_name = 'g3'", true,
        plan -> assertThat(plan).contains("FETCH FROM INDEX Chunk[graph_name,external_id]").doesNotContain("ORDER BY"))).isEqualTo(
        expected);
  }

  /**
   * Regression test for issue #11700: the first page has no bound on the sort key. When no returned record can miss the key the
   * whole index is walked in order; when one can, that walk would also scan the type for the records the index does not hold,
   * and the equality's index is kept.
   */
  @Test
  void firstPageWithoutABoundOnTheSortKey() {
    loadKeysetFixture();
    final List<String> expected = sortedThroughTheEquality("graph_name = 'g3'", true).subList(0, KEYSET_PAGE);

    try (final ResultSet rs = database.query("sql",
        "SELECT external_id FROM Chunk WHERE graph_name = 'g3' ORDER BY external_id LIMIT " + KEYSET_PAGE)) {
      final List<String> got = new ArrayList<>();
      while (rs.hasNext())
        got.add(rs.next().getProperty("external_id"));
      assertThat(got).isEqualTo(expected);
      assertThat(plan(rs)).contains("FETCH FROM INDEX Chunk[graph_name]").contains("ORDER BY");
    }

    // the WHERE clause rules the nulls out, so the walk needs no scan
    try (final ResultSet rs = database.query("sql",
        "SELECT external_id FROM Chunk WHERE graph_name = 'g3' AND external_id IS NOT NULL ORDER BY external_id LIMIT " + KEYSET_PAGE)) {
      final List<String> got = new ArrayList<>();
      while (rs.hasNext())
        got.add(rs.next().getProperty("external_id"));
      assertThat(got).isEqualTo(expected);
      assertThat(plan(rs)).contains("Chunk[external_id]").doesNotContain("ORDER BY").doesNotContain("FETCH FROM TYPE")
          .doesNotContain("Chunk[graph_name]");
    }
  }
}
