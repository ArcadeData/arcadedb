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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.Statement;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #13384: {@code CAST(x AS type)} and {@code x::type}, the two spellings of the standard cast. A value that cannot be the
 * target type raises, where the lenient {@code convert()} method of a record answers null or truncates.
 */
class CastExpressionTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Person");
      for (int i = 1; i <= 5; i++)
        database.command("sql", "INSERT INTO Person SET name = 'p" + i + "', age = " + (i * 10));
    });
  }

  private Object value(final String expression, final Object... args) {
    try (final ResultSet resultSet = database.query("sql", "SELECT " + expression + " AS v", args)) {
      return resultSet.next().getProperty("v");
    }
  }

  @Test
  void bothSpellingsGiveTheSameValueOfTheSameType() {
    final Object[][] cases = { { "'12'", "INTEGER", 12 }, { "'12'", "INT", 12 }, { "'12'", "INT4", 12 }, { "'12'", "BIGINT", 12L },
        { "'12'", "INT8", 12L }, { "'12'", "SMALLINT", (short) 12 }, { "'1.5'", "DOUBLE", 1.5d }, { "'1.5'", "FLOAT8", 1.5d },
        { "'1.5'", "REAL", 1.5f }, { "'1.5'", "NUMERIC", new BigDecimal("1.5") }, { "12", "TEXT", "12" }, { "12", "VARCHAR", "12" },
        { "1", "BOOLEAN", true }, { "0", "BOOL", false }, { "'yes'", "BOOLEAN", true }, { "'off'", "BOOLEAN", false },
        { "3.7", "INTEGER", 4 }, { "(-3.5)", "INTEGER", -4 }, { "2.4", "INTEGER", 2 }, { "' 42 '", "INTEGER", 42 },
        { "12", "DOUBLE", 12d }, { "(true)", "INTEGER", 1 }, { "null", "INTEGER", null }, { "null", "TEXT", null } };
    final List<String> problems = new ArrayList<>();
    for (final Object[] c : cases)
      for (final String text : new String[] { c[0] + "::" + c[1], "CAST(" + c[0] + " AS " + c[1] + ")" })
        try {
          final Object result = value(text);
          if (!Objects.equals(result, c[2]) || (c[2] != null && !c[2].getClass().equals(result.getClass())))
            problems.add(text + " gave " + result + (result == null ? "" : " (" + result.getClass().getSimpleName() + ")") + ", expected " + c[2]);
        } catch (final Exception e) {
          problems.add(text + " raised " + e.getMessage());
        }
    assertThat(problems).isEmpty();
  }

  @Test
  void keywordCaseAndTwoWordTypeName() {
    assertThat(value("cast('1.5' as double precision)")).isEqualTo(1.5d);
    assertThat(value("CAST('7' AS integer)")).isEqualTo(7);
    assertThat(value("'7'::integer")).isEqualTo(7);
    assertThat(value("CAST('2020-01-02' AS DATE)")).isNotNull();
  }

  @Test
  void theCastBindsTighterThanTheOperatorsAroundIt() {
    assertThat(value("1 + '2'::INTEGER")).isEqualTo(3);
    assertThat(value("'2'::INTEGER * 3")).isEqualTo(6);
    assertThat(value("-'2'::INTEGER")).isEqualTo(-2);
    assertThat(value("(1 + 2)::TEXT")).isEqualTo("3");
    assertThat(value("'7'::INTEGER::TEXT")).isEqualTo("7");
    assertThat(value("CAST(CAST('7' AS INTEGER) AS TEXT)")).isEqualTo("7");
  }

  @Test
  void castOfAPropertyInAProjectionAndAWhere() {
    try (final ResultSet resultSet = database.query("sql", "SELECT name, age::TEXT AS t FROM Person WHERE CAST(age AS TEXT) = '30'")) {
      final var row = resultSet.next();
      assertThat(row.<String>getProperty("name")).isEqualTo("p3");
      assertThat(row.<String>getProperty("t")).isEqualTo("30");
      assertThat(resultSet.hasNext()).isFalse();
    }
    try (final ResultSet resultSet = database.query("sql", "SELECT name FROM Person WHERE name::TEXT = 'p2' AND age::BIGINT = 20")) {
      assertThat(resultSet.stream().map(r -> r.<String>getProperty("name")).toList()).containsExactly("p2");
    }
  }

  @Test
  void aBoundParameterIsCast() {
    // the SQLAlchemy shape: a limit that arrives as text
    assertThat(value("$1::INTEGER", "7")).isEqualTo(7);
    assertThat(value("?::INTEGER", "7")).isEqualTo(7);
    try (final ResultSet resultSet = database.query("sql", "SELECT FROM Person ORDER BY age LIMIT $1::INTEGER", "2")) {
      assertThat(resultSet.stream().map(r -> r.<String>getProperty("name")).toList()).containsExactly("p1", "p2");
    }
    try (final ResultSet resultSet = database.query("sql", "SELECT FROM Person ORDER BY age SKIP $1::INTEGER LIMIT $2::INTEGER", "3", "5")) {
      assertThat(resultSet.stream().map(r -> r.<String>getProperty("name")).toList()).containsExactly("p4", "p5");
    }
    try (final ResultSet resultSet = database.query("sql", "SELECT :n::INTEGER AS v", Map.of("n", "7"))) {
      assertThat(resultSet.next().<Integer>getProperty("v")).isEqualTo(7);
    }
  }

  @Test
  void aValueThatCannotBeTheTypeRaises() {
    for (final String expression : List.of("'abc'::INTEGER", "'1.5x'::DOUBLE", "''::INTEGER", "'maybe'::BOOLEAN", "'99999999999'::INTEGER",
        "70000::SMALLINT", "'9223372036854775808'::BIGINT", "CAST('abc' AS INTEGER)", "CAST('x' AS NUMERIC)"))
      assertThatThrownBy(() -> value(expression)).as(expression).isInstanceOf(CommandExecutionException.class);
    // a bound text that is not a number: the same error, not a silent null or zero
    assertThatThrownBy(() -> value("$1::INTEGER", "abc")).isInstanceOf(CommandExecutionException.class);
  }

  @Test
  void doublePrecisionIsOneTypeAfterTheColons() {
    assertThat(value("'1.5'::DOUBLE PRECISION")).isEqualTo(1.5d);
    assertThat(value("'1.5'::double precision")).isEqualTo(1.5d);
    assertThat(value("CAST('1.5' AS DOUBLE PRECISION)")).isEqualTo(1.5d);
    // an alias after the type is still an alias, also a word that is not PRECISION
    assertThat(value("'1.5'::DOUBLE")).isEqualTo(1.5d);
    try (final ResultSet resultSet = database.query("sql", "SELECT '1.5'::DOUBLE PRECISION v, '2'::INTEGER w")) {
      final var row = resultSet.next();
      assertThat(row.<Double>getProperty("v")).isEqualTo(1.5d);
      assertThat(row.<Integer>getProperty("w")).isEqualTo(2);
    }
  }

  @Test
  void aFiniteValueTooBigForAFloatingTypeRaises() {
    for (final String expression : List.of("'1e309'::DOUBLE", "'1e39'::REAL", "CAST('1e400' AS FLOAT8)", "1e39::REAL"))
      assertThatThrownBy(() -> value(expression)).as(expression).isInstanceOf(CommandExecutionException.class);
    assertThat(value("'1.5'::REAL")).isEqualTo(1.5f);
    assertThat(value("'3.4e38'::REAL")).isEqualTo(3.4e38f);
    assertThat(value("'1e308'::DOUBLE")).isEqualTo(1e308d);
  }

  @Test
  void aHugeExponentIsRefusedWithoutBuildingTheNumber() {
    // a short text whose rescaling to a whole number would need a billion digits: refused at once, not after the work
    for (final String expression : List.of("'1e1000000000'::INTEGER", "'-1e1000000000'::BIGINT", "'1e30'::BIGINT"))
      assertThatThrownBy(() -> value(expression)).as(expression).isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("out of range");
    // a tiny one rounds to zero, and the boundary values that round into range still work
    assertThat(value("'1e-1000000000'::INTEGER")).isEqualTo(0);
    assertThat(value("'0.4'::INTEGER")).isEqualTo(0);
    assertThat(value("'0.5'::INTEGER")).isEqualTo(1);
    assertThat(value("'-0.5'::INTEGER")).isEqualTo(-1);
    assertThat(value("'32767.4'::SMALLINT")).isEqualTo((short) 32767);
    assertThat(value("'9223372036854775807'::BIGINT")).isEqualTo(Long.MAX_VALUE);
    assertThatThrownBy(() -> value("'32767.5'::SMALLINT")).isInstanceOf(CommandExecutionException.class);
  }

  @Test
  void aDateHasNoTimeOfDayAndATimestampKeepsIt() {
    final Object timestamp = value("CAST('2020-01-02 12:34:56' AS TIMESTAMP)");
    final Object date = value("CAST(CAST('2020-01-02 12:34:56' AS TIMESTAMP) AS DATE)");
    assertThat(timestamp).isInstanceOf(Date.class);
    assertThat(date).isInstanceOf(Date.class);
    assertThat(((Date) date).getTime() % 86_400_000L).isZero();
    assertThat(((Date) timestamp).getTime() % 86_400_000L).isNotZero();
    assertThat(((Date) timestamp).getTime() - ((Date) date).getTime()).isLessThan(86_400_000L).isPositive();
    // the same through the postfix spelling, and from a text
    assertThat(((Date) value("'2020-01-02 12:34:56'::TIMESTAMP::DATE")).getTime()).isEqualTo(((Date) date).getTime());
    assertThat(((Date) value("'2020-01-02'::DATE")).getTime() % 86_400_000L).isZero();
  }

  @Test
  void aTypeNameThatIsNotAScalarTypeIsRefusedAtParse() {
    final SQLAntlrParser parser = new SQLAntlrParser(null);
    for (final String text : List.of("SELECT 1::FROBNICATE", "SELECT CAST(1 AS FROBNICATE)"))
      assertThatThrownBy(() -> parser.parse(text)).as(text).isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("FROBNICATE");
  }

  @Test
  void bothSpellingsRenderAndCopyTheSame() {
    final SQLAntlrParser parser = new SQLAntlrParser(null);
    final Statement suffix = parser.parse("SELECT age::int4 AS a FROM Person");
    final Statement function = parser.parse("SELECT CAST(age AS INTEGER) AS a FROM Person");
    final StringBuilder rendered = new StringBuilder();
    suffix.toString(null, rendered);
    assertThat(rendered.toString()).contains("CAST(age AS INTEGER)");
    assertThat(suffix).isEqualTo(function).hasSameHashCodeAs(function);
    assertThat(suffix.copy()).isEqualTo(suffix);
    // the rendering is read back as the same statement
    assertThat(parser.parse(rendered.toString())).isEqualTo(suffix);
  }

  @Test
  void aggregateInsideACast() {
    try (final ResultSet resultSet = database.query("sql", "SELECT count(*)::TEXT AS n, sum(age)::DOUBLE AS s FROM Person")) {
      final var row = resultSet.next();
      assertThat(row.<String>getProperty("n")).isEqualTo("5");
      assertThat(row.<Double>getProperty("s")).isEqualTo(150d);
    }
  }

  /** The texts that were valid before a cast existed must read as they did. */
  @Test
  void existingColonSyntaxIsUnchanged() {
    final SQLAntlrParser parser = new SQLAntlrParser(null);
    // bucket::name is one token, a named parameter of the bucket selector, and a map entry may still take a parameter
    assertThat(parser.parse("SELECT FROM bucket::name")).isNotNull();
    for (final String text : List.of("{'a'::x}.a", ":x"))
      try (final ResultSet resultSet = database.query("sql", "SELECT " + text + " AS v", Map.of("x", 5))) {
        assertThat(resultSet.next().<Object>getProperty("v")).as(text).isEqualTo(5);
      }
    assertThat(value("{'a': 1}.a")).isEqualTo(1);
    try (final ResultSet resultSet = database.query("sql", "SELECT name FROM Person WHERE age = :age", Map.of("age", 20))) {
      assertThat(resultSet.next().<String>getProperty("name")).isEqualTo("p2");
    }
    // a record id and an index selector still read their single colon
    assertThat(parser.parse("SELECT FROM #12:0")).isNotNull();
    // two colons that do not touch are not a cast
    assertThatThrownBy(() -> parser.parse("SELECT 1 : : INTEGER")).isInstanceOf(CommandSQLParsingException.class);
    // a function that happens to be called cast, with ordinary arguments, is still a function call
    assertThatThrownBy(() -> parser.parse("SELECT `cast`(1 AS INTEGER)")).isInstanceOf(CommandSQLParsingException.class);
  }
}
