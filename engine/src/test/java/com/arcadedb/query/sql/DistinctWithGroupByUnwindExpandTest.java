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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproduces issue #6925: {@code SelectExecutionPlanner.handleProjectionsBlock()} chained the DISTINCT
 * dedup step from a single branch, the one taken when the statement has neither {@code expand()}, nor
 * UNWIND, nor GROUP BY. Since {@code init()} reads {@code info.distinct} off the projection and then
 * clears it, the keyword was accepted, dropped, and never re-applied for the other branch: DISTINCT was
 * silently a no-op for every statement that also had GROUP BY, UNWIND or {@code expand()}.
 *
 * {@code MatchExecutionPlanner} funnels its RETURN clause through the same method, so MATCH ... RETURN
 * DISTINCT with an UNWIND was affected identically.
 */
class DistinctWithGroupByUnwindExpandTest extends TestHelper {

  private static List<Result> toList(final ResultSet rs) {
    final List<Result> rows = new ArrayList<>();
    while (rs.hasNext())
      rows.add(rs.next());
    return rows;
  }

  @Test
  void distinctIsAppliedWithUnwind() {
    database.transaction(() -> {
      database.command("SQL", "CREATE DOCUMENT TYPE U");
      database.command("SQL", "INSERT INTO U SET tag = ['a', 'b', 'a']");

      // Control: the fixture really does produce 3 unwound rows without DISTINCT.
      assertThat(toList(database.query("SQL", "SELECT tag FROM U UNWIND tag"))).hasSize(3);

      final List<Result> rows = toList(database.query("SQL", "SELECT DISTINCT tag FROM U UNWIND tag"));
      assertThat(rows).hasSize(2);
      assertThat(rows.stream().map(r -> (String) r.getProperty("tag")).toList()).containsExactlyInAnyOrder("a", "b");
    });
  }

  @Test
  void distinctIsAppliedWithGroupBy() {
    database.transaction(() -> {
      database.command("SQL", "CREATE DOCUMENT TYPE G");
      database.command("SQL", "INSERT INTO G SET a = 'x'");
      database.command("SQL", "INSERT INTO G SET a = 'y'");
      database.command("SQL", "INSERT INTO G SET a = 'z'");
      database.command("SQL", "INSERT INTO G SET a = 'z'");

      // Control: three groups of sizes 1, 1 and 2 -> two distinct counts.
      assertThat(toList(database.query("SQL", "SELECT count(*) AS c FROM G GROUP BY a"))).hasSize(3);

      final List<Result> rows = toList(database.query("SQL", "SELECT DISTINCT count(*) AS c FROM G GROUP BY a"));
      assertThat(rows).hasSize(2);
      assertThat(rows.stream().map(r -> ((Number) r.getProperty("c")).longValue()).toList()).containsExactlyInAnyOrder(1L, 2L);
    });
  }

  @Test
  void distinctIsAppliedWithExpandOfScalarList() {
    database.transaction(() -> {
      database.command("SQL", "CREATE DOCUMENT TYPE T");
      database.command("SQL", "INSERT INTO T SET tags = ['a', 'b', 'a']");

      assertThat(toList(database.query("SQL", "SELECT expand(tags) FROM T"))).hasSize(3);

      final List<Result> rows = toList(database.query("SQL", "SELECT DISTINCT expand(tags) FROM T"));
      assertThat(rows).hasSize(2);
    });
  }

  @Test
  void distinctIsAppliedWithExpandOfVertices() {
    database.transaction(() -> {
      database.command("SQL", "CREATE VERTEX TYPE Person");
      database.command("SQL", "CREATE EDGE TYPE Knows");
      database.command("SQL", "INSERT INTO Person SET name = 'a'");
      database.command("SQL", "INSERT INTO Person SET name = 'b'");
      database.command("SQL", "INSERT INTO Person SET name = 'target'");
      // Both 'a' and 'b' point at the same vertex, so expand(out()) yields it twice.
      database.command("SQL",
          "CREATE EDGE Knows FROM (SELECT FROM Person WHERE name = 'a') TO (SELECT FROM Person WHERE name = 'target')");
      database.command("SQL",
          "CREATE EDGE Knows FROM (SELECT FROM Person WHERE name = 'b') TO (SELECT FROM Person WHERE name = 'target')");

      assertThat(toList(database.query("SQL", "SELECT expand(out('Knows')) FROM Person"))).hasSize(2);

      final List<Result> rows = toList(database.query("SQL", "SELECT DISTINCT expand(out('Knows')) FROM Person"));
      assertThat(rows).hasSize(1);
      assertThat((String) rows.get(0).getProperty("name")).isEqualTo("target");
    });
  }

  /**
   * SKIP and LIMIT must count the rows the statement returns, not the rows the sort produced: the ORDER BY
   * step is capped at SKIP+LIMIT rows, so a dedup chained after it would otherwise return fewer rows than
   * the LIMIT asked for.
   */
  @Test
  void distinctWithGroupByHonoursOrderByAndLimit() {
    database.transaction(() -> {
      database.command("SQL", "CREATE DOCUMENT TYPE L");
      // groups: k1 -> 1, k2 -> 1, k3 -> 1, k4 -> 2, k5 -> 3 => distinct counts {1, 2, 3}
      database.command("SQL", "INSERT INTO L SET k = 'k1'");
      database.command("SQL", "INSERT INTO L SET k = 'k2'");
      database.command("SQL", "INSERT INTO L SET k = 'k3'");
      database.command("SQL", "INSERT INTO L SET k = 'k4'");
      database.command("SQL", "INSERT INTO L SET k = 'k4'");
      database.command("SQL", "INSERT INTO L SET k = 'k5'");
      database.command("SQL", "INSERT INTO L SET k = 'k5'");
      database.command("SQL", "INSERT INTO L SET k = 'k5'");

      final List<Result> all = toList(database.query("SQL", "SELECT DISTINCT count(*) AS c FROM L GROUP BY k ORDER BY c"));
      assertThat(all.stream().map(r -> ((Number) r.getProperty("c")).longValue()).toList()).containsExactly(1L, 2L, 3L);

      final List<Result> limited = toList(
          database.query("SQL", "SELECT DISTINCT count(*) AS c FROM L GROUP BY k ORDER BY c LIMIT 2"));
      assertThat(limited.stream().map(r -> ((Number) r.getProperty("c")).longValue()).toList()).containsExactly(1L, 2L);

      final List<Result> skipped = toList(
          database.query("SQL", "SELECT DISTINCT count(*) AS c FROM L GROUP BY k ORDER BY c SKIP 1 LIMIT 1"));
      assertThat(skipped.stream().map(r -> ((Number) r.getProperty("c")).longValue()).toList()).containsExactly(2L);

      // Without an ORDER BY the LIMIT is pushed into the aggregation as a cap on the number of GROUPS; the
      // dedup that follows would then collapse those groups into fewer rows than the LIMIT asked for.
      assertThat(toList(database.query("SQL", "SELECT DISTINCT count(*) AS c FROM L GROUP BY k LIMIT 2"))).hasSize(2);
    });
  }

  /**
   * The {@code expand()} branch used to be rejected outright by a (dead) plan-time guard, so it never met
   * ORDER BY / SKIP / LIMIT at all. Same invariant as the GROUP BY case above: the pagination has to count the
   * rows the statement returns, not the rows the sort produced.
   */
  @Test
  void distinctWithExpandHonoursOrderByAndLimit() {
    database.transaction(() -> {
      database.command("SQL", "CREATE DOCUMENT TYPE E");
      database.command("SQL", "INSERT INTO E SET tags = ['b', 'a', 'a', 'c']");

      assertThat(toList(database.query("SQL", "SELECT expand(tags) FROM E"))).hasSize(4);

      final List<Result> all = toList(database.query("SQL", "SELECT DISTINCT expand(tags) FROM E ORDER BY value"));
      assertThat(all.stream().map(r -> (String) r.getProperty("value")).toList()).containsExactly("a", "b", "c");

      final List<Result> limited = toList(
          database.query("SQL", "SELECT DISTINCT expand(tags) FROM E ORDER BY value LIMIT 2"));
      assertThat(limited.stream().map(r -> (String) r.getProperty("value")).toList()).containsExactly("a", "b");

      final List<Result> skipped = toList(
          database.query("SQL", "SELECT DISTINCT expand(tags) FROM E ORDER BY value SKIP 1 LIMIT 1"));
      assertThat(skipped.stream().map(r -> (String) r.getProperty("value")).toList()).containsExactly("b");
    });
  }

  /**
   * Same pagination invariant again, this time for the UNWIND branch. Note the clause order the grammar
   * requires (ORDER BY before UNWIND); the plan still unwinds first and sorts the unwound rows.
   */
  @Test
  void distinctWithUnwindHonoursOrderByAndLimit() {
    database.transaction(() -> {
      database.command("SQL", "CREATE DOCUMENT TYPE W");
      database.command("SQL", "INSERT INTO W SET tag = ['b', 'a', 'a', 'c']");

      final List<Result> all = toList(database.query("SQL", "SELECT DISTINCT tag FROM W ORDER BY tag UNWIND tag"));
      assertThat(all.stream().map(r -> (String) r.getProperty("tag")).toList()).containsExactly("a", "b", "c");

      final List<Result> limited = toList(
          database.query("SQL", "SELECT DISTINCT tag FROM W ORDER BY tag UNWIND tag LIMIT 2"));
      assertThat(limited.stream().map(r -> (String) r.getProperty("tag")).toList()).containsExactly("a", "b");

      final List<Result> skipped = toList(
          database.query("SQL", "SELECT DISTINCT tag FROM W ORDER BY tag UNWIND tag SKIP 1 LIMIT 1"));
      assertThat(skipped.stream().map(r -> (String) r.getProperty("tag")).toList()).containsExactly("b");
    });
  }

  /**
   * GROUP BY and UNWIND in the same statement: the dedup has to see the rows the UNWIND produced, not the
   * groups the aggregation produced, so it can only be chained after both.
   */
  @Test
  void distinctIsAppliedWithGroupByAndUnwindTogether() {
    database.transaction(() -> {
      database.command("SQL", "CREATE DOCUMENT TYPE C");
      database.command("SQL", "INSERT INTO C SET a = 'x'");
      database.command("SQL", "INSERT INTO C SET a = 'x'");
      database.command("SQL", "INSERT INTO C SET a = 'y'");
      database.command("SQL", "INSERT INTO C SET a = 'z'");
      database.command("SQL", "INSERT INTO C SET a = 'z'");

      // Three groups with counts 2, 1, 2; each is unwound into two rows -> 6 rows, of which the two
      // count-2 groups produce the same pair, so DISTINCT leaves 4.
      final String sql = "SELECT count(*) AS c, [1, 2] AS p FROM C GROUP BY a UNWIND p";
      assertThat(toList(database.query("SQL", sql))).hasSize(6);

      final List<Result> rows = toList(database.query("SQL", "SELECT DISTINCT count(*) AS c, [1, 2] AS p FROM C GROUP BY a UNWIND p"));
      assertThat(rows).hasSize(4);
      assertThat(rows.stream().map(r -> ((Number) r.getProperty("c")).longValue() + ":" + r.getProperty("p")).toList())
          .containsExactlyInAnyOrder("2:1", "2:2", "1:1", "1:2");
    });
  }

  @Test
  void distinctIsAppliedWithUnwindInMatch() {
    database.transaction(() -> {
      database.command("SQL", "CREATE VERTEX TYPE M");
      database.command("SQL", "INSERT INTO M SET name = 'n', tag = ['a', 'b', 'a']");

      assertThat(toList(database.query("SQL", "MATCH {type: M, as: m} RETURN m.tag AS tag UNWIND tag"))).hasSize(3);

      final List<Result> rows = toList(
          database.query("SQL", "MATCH {type: M, as: m} RETURN DISTINCT m.tag AS tag UNWIND tag"));
      assertThat(rows).hasSize(2);
      assertThat(rows.stream().map(r -> (String) r.getProperty("tag")).toList()).containsExactlyInAnyOrder("a", "b");
    });
  }

  /**
   * Without GROUP BY, UNWIND or expand() the dedup was already chained; this pins the unaffected branch so a
   * regression there does not hide behind the new coverage.
   */
  @Test
  void distinctStillWorksWithoutGroupByUnwindOrExpand() {
    database.transaction(() -> {
      database.command("SQL", "CREATE DOCUMENT TYPE P");
      database.command("SQL", "INSERT INTO P SET v = 1");
      database.command("SQL", "INSERT INTO P SET v = 1");
      database.command("SQL", "INSERT INTO P SET v = 2");

      assertThat(toList(database.query("SQL", "SELECT DISTINCT v FROM P"))).hasSize(2);
    });
  }
}
