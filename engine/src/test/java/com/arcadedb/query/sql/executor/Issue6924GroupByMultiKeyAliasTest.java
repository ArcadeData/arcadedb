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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproduces issue #6924: {@code SelectExecutionPlanner.addGroupByExpressionsToProjections} synthesized the
 * {@code _$$$GROUP_BY_ALIAS$$$_} name from a counter that was declared {@code final int i = 0} outside the loop and
 * never incremented. Every GROUP BY key that was not already a plain projected identifier was therefore appended to the
 * pre-aggregate projection under the very same alias, so the later values overwrote the earlier ones and the query
 * silently grouped on the last key alone.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue6924GroupByMultiKeyAliasTest extends TestHelper {

  @Test
  void multiKeyGroupByOnNonProjectedIdentifiersKeepsEveryKey() {
    database.transaction(() -> {
      database.command("SQL", "CREATE DOCUMENT TYPE G");
      database.command("SQL", "INSERT INTO G SET a = 1, b = 1, v = 10");
      database.command("SQL", "INSERT INTO G SET a = 1, b = 2, v = 20");
      database.command("SQL", "INSERT INTO G SET a = 2, b = 1, v = 30");

      final List<Long> counts = collectLongs("SELECT count(*) AS c FROM G GROUP BY a, b", "c");

      // (1,1) (1,2) (2,1) are three distinct groups of one record each.
      // Before the fix the plan grouped on `b` alone and returned two rows: 2 and 1.
      assertThat(counts).hasSize(3);
      assertThat(counts).containsExactly(1L, 1L, 1L);
    });
  }

  @Test
  void multiKeyGroupByExposesEachSyntheticAliasSeparately() {
    database.transaction(() -> {
      database.command("SQL", "CREATE DOCUMENT TYPE G2");
      database.command("SQL", "INSERT INTO G2 SET a = 1, b = 1, v = 10");
      database.command("SQL", "INSERT INTO G2 SET a = 1, b = 2, v = 20");
      database.command("SQL", "INSERT INTO G2 SET a = 2, b = 1, v = 30");

      // The GROUP BY keys are projected too, so the grouping must be observable in the output rows.
      final ResultSet rs = database.query("SQL", "SELECT a, b, sum(v) AS s FROM G2 GROUP BY a, b ORDER BY a, b");

      final List<String> rows = new ArrayList<>();
      while (rs.hasNext()) {
        final Result r = rs.next();
        rows.add(r.getProperty("a") + "/" + r.getProperty("b") + "/" + ((Number) r.getProperty("s")).longValue());
      }

      assertThat(rows).containsExactly("1/1/10", "1/2/20", "2/1/30");
    });
  }

  @Test
  void multiKeyGroupByOnComputedExpressionsKeepsEveryKey() {
    database.transaction(() -> {
      database.command("SQL", "CREATE DOCUMENT TYPE G3");
      database.command("SQL", "INSERT INTO G3 SET a = 1, b = 1, v = 10");
      database.command("SQL", "INSERT INTO G3 SET a = 1, b = 2, v = 20");
      database.command("SQL", "INSERT INTO G3 SET a = 2, b = 1, v = 30");

      // `a + 0` / `b + 0` are not base identifiers, so both keys take the synthetic-alias branch.
      final ResultSet rs = database.query("SQL", "SELECT count(*) AS c FROM G3 GROUP BY a + 0, b + 0");

      final List<Long> counts = new ArrayList<>();
      while (rs.hasNext()) {
        final Result r = rs.next();
        counts.add(((Number) r.getProperty("c")).longValue());
        // the synthetic pre-aggregate aliases must never reach the caller
        assertThat(r.getPropertyNames()).allSatisfy(name -> assertThat(name).doesNotContain("_$$$GROUP_BY_ALIAS$$$_"));
      }

      assertThat(counts).hasSize(3);
      assertThat(counts).containsExactly(1L, 1L, 1L);
    });
  }

  @Test
  void threeKeyGroupByWithOneProjectedKeyKeepsTheOtherTwo() {
    database.transaction(() -> {
      database.command("SQL", "CREATE DOCUMENT TYPE G4");
      database.command("SQL", "INSERT INTO G4 SET a = 1, b = 1, c = 1, v = 10");
      database.command("SQL", "INSERT INTO G4 SET a = 1, b = 1, c = 2, v = 20");
      database.command("SQL", "INSERT INTO G4 SET a = 1, b = 2, c = 1, v = 30");

      // `a` is a plain projected identifier and reuses its own projection; `b` and `c` both need a synthetic alias.
      final List<Long> counts = collectLongs("SELECT a, count(*) AS c FROM G4 GROUP BY a, b, c", "c");

      assertThat(counts).hasSize(3);
      assertThat(counts).containsExactly(1L, 1L, 1L);
    });
  }

  @Test
  void singleComputedGroupByKeyStillWorks() {
    database.transaction(() -> {
      database.command("SQL", "CREATE DOCUMENT TYPE G5");
      database.command("SQL", "INSERT INTO G5 SET a = 1, v = 10");
      database.command("SQL", "INSERT INTO G5 SET a = 1, v = 20");
      database.command("SQL", "INSERT INTO G5 SET a = 2, v = 30");

      final List<Long> sums = collectLongs("SELECT sum(v) AS c FROM G5 GROUP BY a", "c");

      assertThat(sums).hasSize(2);
      assertThat(sums).containsExactlyInAnyOrder(30L, 30L);
    });
  }

  private List<Long> collectLongs(final String query, final String property) {
    final ResultSet rs = database.query("SQL", query);
    final List<Long> values = new ArrayList<>();
    while (rs.hasNext())
      values.add(((Number) rs.next().getProperty(property)).longValue());
    return values;
  }
}
