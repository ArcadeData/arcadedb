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
 * Regression guard for #8811: {@code ORDER BY a LIMIT k} was served from the index on {@code a} only when the projection kept
 * {@code a} under its own name. A projection that renames the sort key, or leaves it out, made the planner scan and sort.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8811OrderByAliasIndexTest extends TestHelper {

  @BeforeEach
  void load() {
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

  @Test
  void renamedSortKeyUsesTheIndex() {
    assertIndexOrdered("SELECT a AS c FROM V ORDER BY a ASC LIMIT 5", "c", List.of(0L, 1L, 2L, 3L, 4L));
    assertIndexOrdered("SELECT a AS c FROM V ORDER BY c ASC LIMIT 5", "c", List.of(0L, 1L, 2L, 3L, 4L));
    assertIndexOrdered("SELECT a AS c FROM V ORDER BY a DESC LIMIT 5", "c", List.of(199L, 198L, 197L, 196L, 195L));
    assertIndexOrdered("SELECT a AS c FROM V ORDER BY c DESC LIMIT 5", "c", List.of(199L, 198L, 197L, 196L, 195L));
  }

  @Test
  void sortKeyLeftOutOfTheProjectionUsesTheIndex() {
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

  @Test
  void aliasShadowingTheIndexedPropertyDoesNotUseTheIndex() {
    // `b AS a` is the projected `a`: ORDER BY a sorts by b, so the index on the property a must not answer it
    try (final ResultSet rs = database.query("sql", "SELECT b AS a FROM V ORDER BY a ASC LIMIT 3")) {
      final List<Long> got = new ArrayList<>();
      while (rs.hasNext())
        got.add(rs.next().<Number>getProperty("a").longValue());
      assertThat(got).isEqualTo(List.of(801L, 802L, 803L));
      assertThat(plan(rs)).doesNotContain("V[a]");
    }
  }

  @Test
  void computedAliasDoesNotUseTheIndex() {
    try (final ResultSet rs = database.query("sql", "SELECT a * -1 AS a FROM V ORDER BY a ASC LIMIT 3")) {
      final List<Long> got = new ArrayList<>();
      while (rs.hasNext())
        got.add(rs.next().<Number>getProperty("a").longValue());
      assertThat(got).isEqualTo(List.of(-199L, -198L, -197L));
      assertThat(plan(rs)).doesNotContain("V[a]");
    }
  }

  @Test
  void duplicateAliasDoesNotUseTheIndexAndMatchesTheUnindexedAnswer() {
    final String query = "SELECT a, b AS a FROM V ORDER BY a ASC LIMIT 3";
    final List<Object> got = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext())
        got.add(rs.next().getProperty("a"));
      assertThat(plan(rs)).doesNotContain("V[a]");
    }
    assertThat(got).hasSize(3);
  }
}
