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
package com.arcadedb.function;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.exception.InvalidArgumentException;
import com.arcadedb.function.sql.DefaultSQLFunctionFactory;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11589: SQL and openCypher functions given an argument of the wrong kind, or a null where they need a value, failed as
 * a server fault (HTTP 500) - some with a raw {@code NullPointerException}, {@code ClassCastException} or
 * {@code DateTimeParseException}, the others with a {@code CommandExecutionException} whose message already said the argument
 * was wrong. The wrong argument is the caller's mistake.
 * <p>
 * The census calls every function of both registries - the authority the statements resolve against, not a list of names -
 * with every combination of argument kinds up to two arguments, and asserts that no call fails as a {@link ErrorCategory#SERVER}
 * fault, the category every wire protocol answers as an internal error.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11589FunctionArgumentErrorsTest extends TestHelper {
  private static final String[] SQL_ARGUMENTS    = { "null", "1", "1.5", "'abc'", "''", "[1, 2]", "{\"a\": 1}", "@this" };
  private static final String[] CYPHER_ARGUMENTS = { "null", "1", "1.5", "'abc'", "''", "[1, 2]", "{a: 1}", "n" };

  @Override
  protected void beginTest() {
    database.command("opencypher", "CREATE (:V {id: 1})-[:E]->(:V {id: 2})");
  }

  @Test
  void theReportedStatementsAreClientErrors() {
    for (final String sql : List.of("SELECT out('E', null) AS v FROM V WHERE id = 1", "SELECT outE('E', null) AS v FROM V WHERE id = 1",
        "SELECT in('E', null) AS v FROM V WHERE id = 2", "SELECT both('E', null) AS v FROM V WHERE id = 1",
        "SELECT intersect(@this) AS v FROM V WHERE id = 1", "SELECT geo.lineString(1) AS v FROM V WHERE id = 1",
        "SELECT geo.polygon(@rid) AS v FROM V WHERE id = 1", "SELECT ts.rate(1, 'abc') AS v FROM V WHERE id = 1",
        "SELECT ts.last(1, 'x') AS v FROM V WHERE id = 1", "SELECT duration(1, '') AS v FROM V WHERE id = 1",
        "SELECT cypherrid('a', 'b') AS v FROM V WHERE id = 1", "SELECT fulltext.searchIndexMore('x', 1, 2, 3) AS v FROM V WHERE id = 1"))
      assertThat(failureOf("sql", sql)).as(sql).doesNotStartWith("SERVER");

    for (final String cypher : List.of("MATCH (n:V {id: 1}) RETURN time.truncate(null, true) AS v",
        "MATCH (n:V {id: 1}) RETURN date.fields('abc') AS v", "MATCH (n:V {id: 1}) RETURN duration(1) AS v",
        "MATCH (n:V {id: 1}) RETURN duration.between(n, date()) AS v", "MATCH (n:V {id: 1}) RETURN duration.inDays(1, 2) AS v",
        "MATCH (n:V {id: 1}) RETURN toIntegerList(1) AS v", "MATCH (n:V {id: 1}) RETURN toStringList('a') AS v",
        "MATCH (n:V {id: 1}) RETURN point.distance(1, 2) AS v", "MATCH (n:V {id: 1}) RETURN percentileCont(n.id, 1.5) AS v",
        "MATCH (n:V {id: 1}) RETURN format(1, 'yyyy') AS v"))
      assertThat(failureOf("opencypher", cypher)).as(cypher).doesNotStartWith("SERVER");
  }

  @Test
  void aNullEdgeTypeNamesNoTypeAndIsSkipped() {
    // a lone null already meant every type: a null among names is skipped, so out('E', null) is out('E')
    for (final String function : List.of("out", "outE", "both", "bothE"))
      assertThat(count("SELECT " + function + "('E', null) AS v FROM V WHERE id = 1")).as(function)
          .isEqualTo(count("SELECT " + function + "('E') AS v FROM V WHERE id = 1")).isEqualTo(1);
    assertThat(count("SELECT in('E', null) AS v FROM V WHERE id = 2")).isEqualTo(1);
    assertThat(count("SELECT out(null, null) AS v FROM V WHERE id = 1")).isEqualTo(1);
    assertThat(count("SELECT out('Missing', null) AS v FROM V WHERE id = 1")).isZero();
  }

  @Test
  void aPatternFieldTheValueLacksANullTemporalAndANullSourceAreHandled() {
    // a pattern naming a field the value does not carry: UnsupportedTemporalTypeException reached the client as a 500
    assertThat(failureOf("opencypher", "RETURN format(date('2026-01-02'), 'HH') AS v")).startsWith("VALIDATION InvalidArgumentException");
    assertThat(failureOf("opencypher", "RETURN format(date('2026-01-02'), 'yyyy') AS v")).isEqualTo("OK");

    // a null temporal value is a null answer, as a null unit is, on every truncate function
    for (final String function : List.of("date", "localtime", "time", "localdatetime", "datetime"))
      try (final ResultSet rs = database.query("opencypher", "RETURN " + function + ".truncate('day', null) AS v")) {
        assertThat((Object) rs.next().getProperty("v")).as(function).isNull();
      }

    // a null field name or source RID inside the list is the caller's mistake, not a NullPointerException
    for (final String sql : List.of("SELECT fulltext.searchFieldsMore([null], [#1:0]) AS v FROM V WHERE id = 1",
        "SELECT fulltext.searchFieldsMore(['id'], [null]) AS v FROM V WHERE id = 1",
        "SELECT fulltext.searchIndexMore('x', [null]) AS v FROM V WHERE id = 1"))
      assertThat(failureOf("sql", sql)).as(sql).startsWith("VALIDATION InvalidArgumentException");
  }

  @Test
  void aLargeArgumentIsNotEchoedBackInFull() {
    // the caller's value is quoted in the message, capped: a whole document handed to a parser must not come back in full
    final String large = "x".repeat(100_000);
    for (final String[] statement : new String[][] { { "opencypher", "RETURN convert.fromJsonList($v) AS v" },
        { "opencypher", "RETURN convert.fromJsonMap($v) AS v" }, { "opencypher", "RETURN date.fields($v) AS v" },
        { "sql", "SELECT ts.rate(1, :v) AS v" }, { "sql", "SELECT duration(1, :v) AS v" } }) {
      try (final ResultSet rs = database.command(statement[0], statement[1], Map.of("v", large))) {
        rs.next().toJSON();
        throw new AssertionError(statement[1] + " accepted the argument");
      } catch (final InvalidArgumentException e) {
        assertThat(e.getMessage().length()).as(statement[1]).isLessThan(1_000);
        assertThat(ErrorCategory.of(e)).isEqualTo(ErrorCategory.VALIDATION);
      }
    }
  }

  @Test
  void noSqlFunctionFailsAsAServerFaultForAWrongArgument() {
    // the built-ins, not a function another test of the JVM registered on the shared factory and left behind
    final DefaultSQLFunctionFactory factory = ((SQLQueryEngine) database.getQueryEngine("sql")).getFunctionFactory();
    final Set<String> names = new TreeSet<>();
    for (final String name : factory.getBuiltInFunctionNames())
      if (factory.isBuiltIn(name))
        names.add(name);
    assertThat(names).as("the census must reach the SQL function registry").contains("out", "geo.linestring", "ts.rate");

    final List<String> offenders = new ArrayList<>();
    for (final String name : names)
      for (final String args : argumentLists(SQL_ARGUMENTS)) {
        final String sql = "SELECT " + name + "(" + args + ") AS v FROM V WHERE id = 1";
        final String failure = failureOf("sql", sql);
        if (failure.startsWith("SERVER"))
          offenders.add(sql + " -> " + failure);
      }
    assertThat(offenders).as("SQL function calls failing as a server fault for an argument the caller got wrong").isEmpty();
  }

  @Test
  void noCypherFunctionFailsAsAServerFaultForAWrongArgument() {
    final Set<String> names = new TreeSet<>(CypherBuiltinFunctions.getNames());
    for (final String name : CypherFunctionRegistry.getFunctionNames())
      if (CypherFunctionRegistry.isBuiltInOrUnregistered(name))
        names.add(name);
    assertThat(names).as("the census must reach the openCypher built-ins and the function registry").contains("tointegerlist",
        "duration.between", "date.fields");

    final List<String> offenders = new ArrayList<>();
    for (final String name : names)
      for (final String args : argumentLists(CYPHER_ARGUMENTS)) {
        final String cypher = "MATCH (n:V {id: 1}) RETURN " + name + "(" + args + ") AS v";
        final String failure = failureOf("opencypher", cypher);
        if (failure.startsWith("SERVER"))
          offenders.add(cypher + " -> " + failure);
      }
    assertThat(offenders).as("openCypher function calls failing as a server fault for an argument the caller got wrong").isEmpty();
  }

  /** The number of items of the single row's {@code v}. */
  private long count(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      final Object value = rs.next().getProperty("v");
      long n = 0;
      for (final Object ignored : (Iterable<?>) value)
        ++n;
      return n;
    }
  }

  /** No argument, every kind alone, every pair of kinds. */
  private static List<String> argumentLists(final String[] kinds) {
    final List<String> lists = new ArrayList<>();
    lists.add("");
    for (final String first : kinds) {
      lists.add(first);
      for (final String second : kinds)
        lists.add(first + ", " + second);
    }
    return lists;
  }

  /** "OK" when the statement ran, otherwise the category of its failure followed by the exception. */
  private String failureOf(final String language, final String statement) {
    try (final ResultSet rs = database.command(language, statement)) {
      while (rs.hasNext())
        rs.next().toJSON();
      return "OK";
    } catch (final Throwable e) {
      if (database.isTransactionActive())
        database.rollback();
      return ErrorCategory.of(e) + " " + e.getClass().getSimpleName() + ": " + e.getMessage();
    }
  }
}
