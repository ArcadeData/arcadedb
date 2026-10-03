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
}
