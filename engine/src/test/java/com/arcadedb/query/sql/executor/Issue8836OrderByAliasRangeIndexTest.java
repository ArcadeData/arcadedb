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
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression guard for #8836: {@code ORDER BY a LIMIT k} with a range on {@code a} in the WHERE was served from the index only
 * when {@code a} was projected under its own name. A renamed or unprojected sort key made the planner read the range and sort it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8836OrderByAliasRangeIndexTest extends TestHelper {

  @BeforeEach
  void load() {
    database.command("sql", "CREATE VERTEX TYPE V");
    database.command("sql", "CREATE PROPERTY V.a LONG (mandatory true, notnull true)");
    database.command("sql", "CREATE PROPERTY V.b LONG");
    database.command("sql", "CREATE INDEX ON V (a) NOTUNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < 200; i++)
        database.newVertex("V").set("a", (long) i, "b", (long) (1000 - i)).save();
    });
  }

  private void assertIndexOrdered(final String sql, final String column, final List<Long> expected) {
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

  @Test
  void renamedSortKeyWithRangeUsesTheIndex() {
    final List<Long> asc = List.of(101L, 102L, 103L, 104L, 105L);
    assertIndexOrdered("SELECT a AS c FROM V WHERE a > 100 ORDER BY a ASC LIMIT 5", "c", asc);
    assertIndexOrdered("SELECT a AS c FROM V WHERE a > 100 ORDER BY c ASC LIMIT 5", "c", asc);
    assertIndexOrdered("SELECT a AS c FROM V WHERE a < 100 ORDER BY a DESC LIMIT 5", "c", List.of(99L, 98L, 97L, 96L, 95L));
    assertIndexOrdered("SELECT a AS c FROM V WHERE a BETWEEN 50 AND 150 ORDER BY a ASC LIMIT 3", "c", List.of(50L, 51L, 52L));
  }

  @Test
  void unprojectedSortKeyWithRangeUsesTheIndex() {
    assertIndexOrdered("SELECT b FROM V WHERE a > 100 ORDER BY a ASC LIMIT 3", "b", List.of(899L, 898L, 897L));
  }

  @Test
  void aliasShadowingTheIndexedPropertyStillSorts() {
    try (final ResultSet rs = database.query("sql", "SELECT b AS a FROM V WHERE a > 100 ORDER BY a ASC LIMIT 3")) {
      final List<Long> got = new ArrayList<>();
      while (rs.hasNext())
        got.add(rs.next().<Number>getProperty("a").longValue());
      assertThat(got).isEqualTo(List.of(801L, 802L, 803L));
    }
  }

  @Test
  void computedAliasShadowingTheKeyKeepsTheSort() {
    try (final ResultSet rs = database.query("sql", "SELECT a * -1 AS a FROM V WHERE a > 100 ORDER BY a ASC LIMIT 3")) {
      final List<Long> got = new ArrayList<>();
      while (rs.hasNext())
        got.add(rs.next().<Number>getProperty("a").longValue());
      assertThat(got).isEqualTo(List.of(-199L, -198L, -197L));
    }
  }

  @Test
  void duplicateAliasKeepsTheSort() {
    try (final ResultSet rs = database.query("sql", "SELECT a, b AS a FROM V WHERE a > 100 ORDER BY a ASC LIMIT 3")) {
      final List<Object> got = new ArrayList<>();
      while (rs.hasNext())
        got.add(rs.next().getProperty("a"));
      assertThat(got).hasSize(3);
      assertThat(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2)).contains("ORDER BY");
    }
  }

  @Test
  void distinctWithUnprojectedSortKey() {
    try (final ResultSet rs = database.query("sql", "SELECT DISTINCT b FROM V WHERE a > 100 ORDER BY a ASC LIMIT 3")) {
      final List<Long> got = new ArrayList<>();
      while (rs.hasNext())
        got.add(rs.next().<Number>getProperty("b").longValue());
      assertThat(got).isEqualTo(List.of(899L, 898L, 897L));
    }
  }

  @Test
  void parameterizedDirectionKeepsTheSort() {
    try (final ResultSet rs = database.command("sql", "SELECT a AS c FROM V WHERE a > 100 ORDER BY c :direction LIMIT 3",
        Map.of("direction", "DESC"))) {
      final List<Long> got = new ArrayList<>();
      while (rs.hasNext())
        got.add(rs.next().<Number>getProperty("c").longValue());
      assertThat(got).isEqualTo(List.of(199L, 198L, 197L));
    }
  }
}
