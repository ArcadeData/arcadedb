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
import com.arcadedb.exception.CommandParameterMissingException;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.PositionalParameter;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.query.sql.parser.StatementCache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests, one nested class per reported case.
 */
class SqlParameterBindingTest {
  /**
   * Regression for #11098.
   * <p>
   * Issue #11098: a placeholder with no key in a parameter {@code Map} - or a statement with a placeholder run with no
   * parameters at all - was bound to {@code null} without a word, so an {@code INSERT} stored {@code null} and a
   * {@code SELECT ... WHERE v = ?} matched nothing. The {@code Object...} overloads already refused the missing value (#10721),
   * and openCypher refuses it too ({@code Expected parameter(s): x}). A key bound to an explicit {@code null} still binds
   * {@code null}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class MissingParameterKey extends TestHelper {

    @BeforeEach
    void createType() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      database.transaction(() -> {
        for (int k = 1; k <= 3; k++)
          database.command("sql", "INSERT INTO T SET k = ?, v = ?", k, k);
      });
    }

    @Test
    void positionalPlaceholderWithoutItsKeyIsRefused() {
      assertMissing(() -> database.command("sql", "INSERT INTO T SET k = 10, v = ?", Map.of("a", 1)), "0");
      assertMissing(() -> database.command("sql", "INSERT INTO T SET k = 10, v = ?", Map.of()), "0");
      assertMissing(() -> database.query("sql", "SELECT FROM T WHERE v = ?", Map.of("x", 1)), "0");
      assertThat(storedAt(10)).isEmpty();
    }

    @Test
    void namedPlaceholderWithoutItsKeyIsRefused() {
      assertMissing(() -> database.command("sql", "INSERT INTO T SET k = 10, v = :v", Map.of("w", 1)), "v");
      assertMissing(() -> database.query("sql", "SELECT FROM T WHERE v = :x", Map.of("y", 1)), "x");
      // Every missing name is reported, in the order the statement mentions them
      assertMissing(() -> database.query("sql", "SELECT FROM T WHERE v = :a OR v = :b OR k = :c", Map.of("b", 1)), "a", "c");
      assertThat(storedAt(10)).isEmpty();
    }

    @Test
    void placeholderRunWithoutAnyParameterIsRefused() {
      assertMissing(() -> database.command("sql", "INSERT INTO T SET k = 10, v = ?"), "0");
      assertMissing(() -> database.query("sql", "SELECT FROM T WHERE v = :v"), "v");
      assertThat(storedAt(10)).isEmpty();
    }

    @Test
    void boundKeysStillBind() {
      // the positional slot by its position, as the Object... overloads key it
      database.transaction(() -> database.command("sql", "INSERT INTO T SET k = 10, v = ?", Map.of("0", 7)));
      assertThat(storedAt(10)).containsExactly(7);
      assertThat(keysWhere("v = ?", Map.of("0", 1))).containsExactly(1);
      // a named placeholder by its name, by its prefixed name, or by its position
      assertThat(keysWhere("v = :x", Map.of("x", 2))).containsExactly(2);
      assertThat(keysWhere("v = :x", Map.of(":x", 2))).containsExactly(2);
      assertThat(keysWhere("v = :x", Map.of("0", 3))).containsExactly(3);
    }

    @Test
    void explicitNullStillBindsNull() {
      final Map<String, Object> positional = new HashMap<>();
      positional.put("0", null);
      database.transaction(() -> database.command("sql", "INSERT INTO T SET k = 10, v = ?", positional));
      final Map<String, Object> named = new HashMap<>();
      named.put("v", null);
      database.transaction(() -> database.command("sql", "INSERT INTO T SET k = 11, v = :v", named));

      try (final ResultSet rs = database.query("sql", "SELECT FROM T WHERE k >= 10 ORDER BY k")) {
        for (int i = 0; i < 2; i++) {
          final Result row = rs.next();
          assertThat(row.hasProperty("v")).isTrue();
          assertThat(row.<Object>getProperty("v")).isNull();
        }
      }
    }

    @Test
    void literalsExtractedByTheStatementCacheAreNotParameters() {
      // a repeated text has its literals turned into generated parameters (#8307): they are bound by the engine, never missing
      for (int i = 0; i < 3; i++)
        assertThat(keysWhere("v = 2 AND k = :k", Map.of("k", 2))).containsExactly(2);
    }

    @Test
    void scriptPlaceholderWithoutItsKeyIsRefused() {
      assertMissing(() -> database.command("sqlscript", "INSERT INTO T SET k = 10, v = 1; INSERT INTO T SET k = 11, v = :v;",
          Map.of("w", 1)), "v");
      assertThat(storedAt(10)).isEmpty();

      final Map<String, Object> named = Map.of("v", 5);
      database.transaction(() -> database.command("sqlscript", "INSERT INTO T SET k = 12, v = :v;", named));
      assertThat(storedAt(12)).containsExactly(5);
    }

    @Test
    void sqlFunctionCalledWithFewerArgumentsBindsTheOmittedOnesToNull() {
      database.command("sql", "DEFINE FUNCTION my.pick \"SELECT ifnull(:b, :a) AS r\" PARAMETERS [a, b] LANGUAGE sql");
      try (final ResultSet rs = database.query("sql", "SELECT `my.pick`(4) AS r")) {
        assertThat(rs.next().<Integer>getProperty("r")).isEqualTo(4);
      }
      try (final ResultSet rs = database.query("sql", "SELECT `my.pick`(4, 5) AS r")) {
        assertThat(rs.next().<Integer>getProperty("r")).isEqualTo(5);
      }
    }

    private void assertMissing(final Runnable call, final String... missing) {
      assertThatThrownBy(call::run).isInstanceOfSatisfying(CommandParameterMissingException.class,
          e -> assertThat(e.getMissingParameters()).containsExactly(missing));
    }

    private List<Integer> storedAt(final int k) {
      final List<Integer> values = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT v FROM T WHERE k = ?", k)) {
        while (rs.hasNext())
          values.add(rs.next().getProperty("v"));
      }
      return values;
    }

    private List<Integer> keysWhere(final String condition, final Map<String, Object> parameters) {
      final List<Integer> keys = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT k FROM T WHERE " + condition + " ORDER BY k", parameters)) {
        while (rs.hasNext())
          keys.add(rs.next().getProperty("k"));
      }
      return keys;
    }
  }

  /**
   * Regression for #8434.
   * <p>
   * Regression test for issue #8434: a positional parameter ({@code ?}) took its number from the order in which the
   * AST builder VISITED the clauses rather than from the order in which the placeholders appear in the statement.
   * {@code LIMIT ? SKIP ?} was built SKIP first, so SKIP bound the first value and LIMIT the second - a silently wrong
   * page. The FROM clause is also built before the projection, so a {@code ?} in the projection and another in the
   * target were swapped the same way.
   */
  @Nested
  class PositionalParameterOrder extends TestHelper {

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

    /** The {@code bucket:?} of an INSERT target is numbered by the bucket-identifier visitor, before the SET values. */
    @Test
    void insertIntoBucketParameterComesBeforeSetParameter() {
      final String bucketName = database.getSchema().getType("T").getBuckets(false).getFirst().getName();
      database.transaction(() -> database.command("sql", "INSERT INTO bucket:? SET uuid = ?", bucketName, "u9").close());
      try (final ResultSet rs = database.query("sql", "SELECT uuid FROM bucket:? WHERE uuid = ?", bucketName, "u9")) {
        assertThat(collect(rs)).containsExactly("u9");
      }
    }

    /** The standalone expression and condition entry points number their placeholders by the same source order. */
    @Test
    void standaloneExpressionAndConditionBindInSourceOrder() {
      final SQLAntlrParser parser = new SQLAntlrParser(database);
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);
      context.setInputParameters(new Object[] { 10, 3 });

      assertThat(((Number) parser.parseExpression("? - ?").execute((Result) null, context)).intValue()).isEqualTo(7);
      assertThat(parser.parseCondition("? > ?").matchesFilters(new ResultInternal(), context)).isTrue();
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
}
