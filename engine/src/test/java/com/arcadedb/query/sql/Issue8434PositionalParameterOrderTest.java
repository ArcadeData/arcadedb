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
import com.arcadedb.engine.Bucket;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.PositionalParameter;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.query.sql.parser.StatementCache;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8434: a positional parameter ({@code ?}) took its number from the order in which the
 * AST builder VISITED the clauses rather than from the order in which the placeholders appear in the statement.
 * {@code LIMIT ? SKIP ?} was built SKIP first, so SKIP bound the first value and LIMIT the second - a silently wrong
 * page. The FROM clause is also built before the projection, so a {@code ?} in the projection and another in the
 * target were swapped the same way.
 */
class Issue8434PositionalParameterOrderTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE T");
      database.command("sql", "CREATE PROPERTY T.uuid STRING");
      database.command("sql", "CREATE INDEX ON T (uuid) UNIQUE");
      for (int i = 0; i < 6; i++)
        database.newVertex("T").set("uuid", "u" + i).save();
    });
  }

  @Test
  void limitThenSkipBindsInSourceOrder() {
    final String sql = "SELECT uuid FROM T ORDER BY uuid LIMIT ? SKIP ?";
    assertThat(uuids(sql, 2, 0)).containsExactly("u0", "u1");
    assertThat(uuids(sql, 2, 3)).containsExactly("u3", "u4");
    assertThat(uuids(sql, 3, 2)).containsExactly("u2", "u3", "u4");
  }

  @Test
  void skipThenLimitStillBindsInSourceOrder() {
    final String sql = "SELECT uuid FROM T ORDER BY uuid SKIP ? LIMIT ?";
    assertThat(uuids(sql, 0, 2)).containsExactly("u0", "u1");
    assertThat(uuids(sql, 3, 2)).containsExactly("u3", "u4");
  }

  @Test
  void whereParameterComesBeforeLimitAndSkip() {
    final String sql = "SELECT uuid FROM T WHERE uuid <> ? ORDER BY uuid LIMIT ? SKIP ?";
    assertThat(uuids(sql, "u0", 2, 1)).containsExactly("u2", "u3");
  }

  @Test
  void limitThenSkipMatchesTheNamedParameterForm() {
    try (final ResultSet rs = database.query("sql", "SELECT uuid FROM T ORDER BY uuid LIMIT :limit SKIP :skip",
        Map.of("limit", 2, "skip", 3))) {
      assertThat(collect(rs)).containsExactly("u3", "u4");
    }
    assertThat(uuids("SELECT uuid FROM T ORDER BY uuid LIMIT ? SKIP ?", 2, 3)).containsExactly("u3", "u4");
  }

  /** The FROM clause is built before the projection; a ? in each must still bind in source order. */
  @Test
  void projectionParameterComesBeforeSubqueryTargetParameter() {
    try (final ResultSet rs = database.query("sql", "SELECT ? AS tag, uuid FROM (SELECT FROM T WHERE uuid = ?)", "x", "u1")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(row.<String>getProperty("tag")).isEqualTo("x");
      assertThat(row.<String>getProperty("uuid")).isEqualTo("u1");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  /** Same as above for a {@code bucket:?} target, which is numbered by its own visitor. */
  @Test
  void projectionParameterComesBeforeBucketParameter() {
    int total = 0;
    for (final Bucket bucket : database.getSchema().getType("T").getBuckets(false))
      try (final ResultSet rs = database.query("sql", "SELECT ? AS tag, uuid FROM bucket:?", "x", bucket.getName())) {
        while (rs.hasNext()) {
          assertThat(rs.next().<String>getProperty("tag")).isEqualTo("x");
          ++total;
        }
      }
    assertThat(total).isEqualTo(6);
  }

  @Test
  void parameterNumbersFollowSourceOrder() {
    final SelectStatement stmt = (SelectStatement) new StatementCache(database, 10).get(
        "SELECT uuid FROM T ORDER BY uuid LIMIT ? SKIP ?");
    assertThat(((PositionalParameter) stmt.getLimit().inputParam).paramNumber).isEqualTo(0);
    assertThat(((PositionalParameter) stmt.getSkip().inputParam).paramNumber).isEqualTo(1);
  }

  /** More placeholders than the collector's initial capacity (8), so its growth path is exercised. */
  @Test
  void manyParametersKeepTheirSourceOrder() {
    final String sql = "SELECT uuid FROM T WHERE uuid IN [?, ?, ?, ?, ?, ?, ?, ?, ?, ?] ORDER BY uuid LIMIT ? SKIP ?";
    assertThat(uuids(sql, "u0", "u1", "u2", "u3", "u4", "x5", "x6", "x7", "x8", "x9", 2, 1)).containsExactly("u1", "u2");
  }

  @Test
  void scriptNumbersParametersAcrossStatementsInSourceOrder() {
    try (final ResultSet rs = database.command("sqlscript",
        "LET a = SELECT uuid FROM T WHERE uuid = ?; SELECT uuid FROM T ORDER BY uuid LIMIT ? SKIP ?;", "u5", 2, 3)) {
      assertThat(collect(rs)).containsExactly("u3", "u4");
    }
  }

  private List<String> uuids(final String sql, final Object... params) {
    try (final ResultSet rs = database.query("sql", sql, params)) {
      return collect(rs);
    }
  }

  private static List<String> collect(final ResultSet rs) {
    final List<String> result = new ArrayList<>();
    while (rs.hasNext())
      result.add(rs.next().getProperty("uuid"));
    return result;
  }
}
