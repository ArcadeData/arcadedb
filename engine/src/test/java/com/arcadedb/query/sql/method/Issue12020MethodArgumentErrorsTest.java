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
package com.arcadedb.query.sql.method;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12020: a SQL METHOD ({@code value.method(args)}) given an argument it is not defined for failed as a server fault (HTTP
 * 500, Postgres XX000, gRPC INTERNAL, ...): {@code format} cast its time zone argument, and a bare {@code %} broke the format
 * string, {@code precision} and {@code hash} refused a unit or algorithm with an error the classifier did not know, and
 * {@code asJson}/{@code toJson} let a JSON parse error out, which only HTTP answered as the caller's mistake. Issue #11589 fixed
 * the same defect for both FUNCTION registries, and its census never ranged over the method registry.
 * <p>
 * This census is the sibling of {@code Issue11589FunctionArgumentErrorsTest} over the method authority: every built-in method of
 * {@link DefaultSQLMethodFactory}, on every receiver kind, with every combination of argument kinds up to two arguments, and no
 * call may fail as a {@link ErrorCategory#SERVER} fault.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue12020MethodArgumentErrorsTest extends TestHelper {
  private static final String[] ARGUMENTS = { "null", "1", "1.5", "'abc'", "''", "'%'", "[1, 2]", "{\"a\": 1}", "@this", "d" };
  private static final String[] RECEIVERS = { "(null)", "(1)", "(1.5)", "('abc')", "('')", "([1, 2])", "({\"a\": 1})", "@this", "d",
      "name", "ds" };

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE Doc");
    database.command("sql", "CREATE PROPERTY Doc.d DATETIME");
    // ds: a list of datetimes, the receiver format() renders element by element
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Doc SET name = 'n', d = '2026-11-15 10:00:00'");
      database.command("sql", "UPDATE Doc SET ds = [d]");
    });
  }

  @Test
  void theReportedStatementsAreClientErrors() {
    for (final String sql : List.of("SELECT d.format('yyyy-MM-dd', 5) AS v FROM Doc", "SELECT name.format('%s', 5) AS v FROM Doc",
        "SELECT ds.format('yyyy-MM-dd', 5) AS v FROM Doc", "SELECT ('abc').format('%') AS v FROM Doc",
        "SELECT d.format('yyyy-MM-dd', 'Not/AZone') AS v FROM Doc",
        "SELECT d.format('yyyy-MM-dd', '+99:00') AS v FROM Doc", "SELECT ('abc').precision(1) AS v FROM Doc",
        "SELECT d.precision('lightyear') AS v FROM Doc", "SELECT ('abc').hash(1) AS v FROM Doc",
        "SELECT ('abc').hash('NOPE-256') AS v FROM Doc", "SELECT ('abc').asJson() AS v FROM Doc",
        "SELECT ('abc').toJson() AS v FROM Doc"))
      assertThat(failureOf(sql)).as(sql).startsWith("VALIDATION");
  }

  /** The well-typed calls still answer, so only the caller's wrong argument fails. */
  @Test
  void theWellTypedCallsStillAnswer() {
    assertThat(value("SELECT d.format('yyyy-MM-dd', 'UTC') AS v FROM Doc")).isEqualTo("2026-11-15");
    assertThat(value("SELECT d.format('yyyy-MM-dd') AS v FROM Doc")).isEqualTo("2026-11-15");
    assertThat(value("SELECT ('abc').format('%s!') AS v FROM Doc")).isEqualTo("abc!");
    // an empty pattern formats to an empty text; it used to fail with a StringIndexOutOfBoundsException
    assertThat(value("SELECT ('abc').format('') AS v FROM Doc")).isEqualTo("");
    // so did a lone quote, read as the start of a quoted field name
    assertThat(value("SELECT ('abc').format('\\'') AS v FROM Doc")).isEqualTo("'");
    assertThat(value("SELECT ('abc').hash('SHA-256') AS v FROM Doc")).isNotNull();
    assertThat(value("SELECT ('{\"a\": 1}').asJson().a AS v FROM Doc")).isEqualTo(1);
    assertThat(value("SELECT d.precision('second') AS v FROM Doc")).isNotNull();
  }

  @Test
  void noSqlMethodFailsAsAServerFaultForAWrongArgument() {
    // the built-ins, not a method another test of the JVM registered on the shared factory and left behind
    final DefaultSQLMethodFactory factory = ((SQLQueryEngine) database.getQueryEngine("sql")).getMethodFactory();
    final Set<String> names = new TreeSet<>();
    for (final String name : factory.getBuiltInMethodNames())
      if (factory.isBuiltIn(name))
        names.add(name);
    assertThat(names).as("the census must reach the SQL method registry").contains("format", "precision", "hash", "asjson",
        "tojson");

    final List<String> argumentLists = argumentLists();
    final List<String> offenders = new ArrayList<>();
    for (final String name : names)
      for (final String receiver : RECEIVERS)
        for (final String args : argumentLists) {
          final String sql = "SELECT " + receiver + "." + name + "(" + args + ") AS v FROM Doc";
          final String failure = failureOf(sql);
          if (failure.startsWith("SERVER"))
            offenders.add(sql + " -> " + failure);
        }
    assertThat(offenders).as("SQL method calls failing as a server fault for an argument the caller got wrong").isEmpty();
  }

  /** No argument, every kind alone, every pair of kinds. */
  private static List<String> argumentLists() {
    final List<String> lists = new ArrayList<>();
    lists.add("");
    for (final String first : ARGUMENTS) {
      lists.add(first);
      for (final String second : ARGUMENTS)
        lists.add(first + ", " + second);
    }
    return lists;
  }

  private Object value(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().getProperty("v");
    }
  }

  /** "OK" when the statement ran, otherwise the category of its failure followed by the exception. */
  private String failureOf(final String sql) {
    try (final ResultSet rs = database.command("sql", sql)) {
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
