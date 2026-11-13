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
import com.arcadedb.database.Database;
import com.arcadedb.utility.TableFormatter;
import org.junit.jupiter.api.Test;

import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.Optional;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
class ExplainStatementExecutionTest extends TestHelper {
  public ExplainStatementExecutionTest() {
    autoStartTx = true;
  }

  @Test
  void explainSelectNoTarget() {
    final ResultSet result = database.query("sql", "explain select 1 as one, 2 as two, 2+3");
    assertThat(result.hasNext()).isTrue();
    final Result next = result.next();
    assertThat(next.<ResultInternal>getProperty("executionPlan")).isNotNull();
    assertThat(next.<String>getProperty("executionPlanAsString")).isNotNull();

    final Optional<ExecutionPlan> plan = result.getExecutionPlan();
    assertThat(plan.isPresent()).isTrue();
    assertThat(plan.get() instanceof SelectExecutionPlan).isTrue();

    result.close();
  }

  @Test
  void regressionIssue1488ExplainSubqueryRecursion() {
    // Test for issue #1488: EXPLAIN should recursively resolve subqueries
    database.getSchema().createVertexType("vec", 1);

    // Insert test data
    database.command("sql", "INSERT INTO vec SET x = 1");
    database.command("sql", "INSERT INTO vec SET x = 2");
    database.getSchema().createEdgeType("vecEdge", 1);

    // Execute EXPLAIN on CREATE EDGE with subquery
    final ResultSet result = database.query("sql", "EXPLAIN CREATE EDGE vecEdge FROM (SELECT FROM vec WHERE x = 1) TO (SELECT FROM vec WHERE x = 2)");
    assertThat(result.hasNext()).isTrue();

    final Result next = result.next();
    final String executionPlanAsString = next.getProperty("executionPlanAsString");

    assertThat(executionPlanAsString).isNotNull();

    // The execution plan should contain details about the subqueries, not just the raw SQL
    // It should show steps like "FETCH FROM TYPE" for the SELECT queries
    // and not just "(SELECT FROM vec WHERE x = 1)"
    assertThat(executionPlanAsString).contains("FETCH FROM TYPE vec");
    assertThat(executionPlanAsString).contains("SCAN WITH FILTER BUCKET");

    // Should NOT contain the raw SQL subquery text
    assertThat(executionPlanAsString).doesNotContain("(SELECT FROM vec WHERE x = 1)");

    result.close();
  }

  /**
   * Regression test for issue #6648: {@code EXPLAIN <write statement>} sent through the
   * {@code sqlscript} language engine used to actually perform the wrapped write, because
   * {@code SQLScriptQueryEngine} chains whatever {@code Statement.createExecutionPlan()} returns
   * and pulls it to completion, and {@code ExplainStatement.createExecutionPlan()} used to pass
   * the wrapped statement's own executable plan straight through. Plain {@code "sql"} was never
   * affected, since it goes through {@code ExplainStatement.execute()}, which never pulls the
   * plan it builds.
   */
  @Test
  void explainUpdateInSqlScriptDoesNotExecuteWrite() {
    final String typeName = "Issue6648UpdateTarget";
    database.getSchema().createDocumentType(typeName);
    database.transaction(() -> database.command("sql", "INSERT INTO " + typeName + " SET bar = 0"));

    database.transaction(() -> {
      final ResultSet result = database.command("sqlscript", "EXPLAIN UPDATE " + typeName + " SET bar = 1;");
      assertThat(result.hasNext()).isTrue();
      final Result next = result.next();
      assertThat(next.<Object>getProperty("executionPlan")).isNotNull();
      assertThat(next.<String>getProperty("executionPlanAsString")).isNotNull();
      result.close();
    });

    final ResultSet rs = database.query("sql", "SELECT bar FROM " + typeName);
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Integer>getProperty("bar")).isEqualTo(0);
    rs.close();
  }

  @Test
  void explainInsertInSqlScriptDoesNotExecuteWrite() {
    final String typeName = "Issue6648InsertTarget";
    database.getSchema().createDocumentType(typeName);

    database.transaction(() -> {
      final ResultSet result = database.command("sqlscript", "EXPLAIN INSERT INTO " + typeName + " SET bar = 1;");
      assertThat(result.hasNext()).isTrue();
      result.close();
    });

    final ResultSet rs = database.query("sql", "SELECT count(*) as count FROM " + typeName);
    assertThat(rs.next().<Long>getProperty("count")).isEqualTo(0L);
    rs.close();
  }

  @Test
  void explainDeleteInSqlScriptDoesNotExecuteWrite() {
    final String typeName = "Issue6648DeleteTarget";
    database.getSchema().createDocumentType(typeName);
    database.transaction(() -> database.command("sql", "INSERT INTO " + typeName + " SET bar = 0"));

    database.transaction(() -> {
      final ResultSet result = database.command("sqlscript", "EXPLAIN DELETE FROM " + typeName + ";");
      assertThat(result.hasNext()).isTrue();
      result.close();
    });

    final ResultSet rs = database.query("sql", "SELECT count(*) as count FROM " + typeName);
    assertThat(rs.next().<Long>getProperty("count")).isEqualTo(1L);
    rs.close();
  }

  /**
   * Same bug, reached through a different chaining caller: {@code IfStep} also chains whatever
   * {@code Statement.createExecutionPlan()} returns for the statements in its branch. Confirms the
   * fix lives where it belongs (in {@code ExplainStatement.createExecutionPlan()}), not as a
   * one-off special case in {@code SQLScriptQueryEngine} alone.
   */
  @Test
  void explainUpdateInsideIfBlockDoesNotExecuteWrite() {
    final String typeName = "Issue6648IfBlockTarget";
    database.getSchema().createDocumentType(typeName);
    database.transaction(() -> database.command("sql", "INSERT INTO " + typeName + " SET bar = 0"));

    database.transaction(() -> {
      final String script = """
              IF(true){
                  EXPLAIN UPDATE %s SET bar = 1;
              }
          """.formatted(typeName);
      final ResultSet result = database.command("sqlscript", script);
      result.close();
    });

    final ResultSet rs = database.query("sql", "SELECT bar FROM " + typeName);
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Integer>getProperty("bar")).isEqualTo(0);
    rs.close();
  }

  /**
   * {@code PROFILE EXPLAIN <statement>} is a third way to reach the same bug:
   * {@code ProfileStatement.execute()} is itself a caller that pulls whatever
   * {@code statement.createExecutionPlan()} hands back (see {@code ProfileStatement.java:58-64}), so
   * before this fix {@code PROFILE EXPLAIN UPDATE ...} silently ran the update exactly like the
   * {@code sqlscript} case did. EXPLAIN's non-execution contract is defined to win regardless of what
   * encloses it, so {@code PROFILE EXPLAIN <statement>} degrades to plan-only output - no execution,
   * no real cost numbers - rather than PROFILE unwrapping the inner statement and running it for real.
   */
  @Test
  void profileExplainUpdateDoesNotExecuteWrite() {
    final String typeName = "Issue6648ProfileExplainTarget";
    database.getSchema().createDocumentType(typeName);
    database.transaction(() -> database.command("sql", "INSERT INTO " + typeName + " SET bar = 0"));

    database.transaction(() -> {
      final ResultSet result = database.command("sql", "PROFILE EXPLAIN UPDATE " + typeName + " SET bar = 1");
      assertThat(result.hasNext()).isTrue();
      final Result next = result.next();
      assertThat(next.<Object>getProperty("executionPlan")).isNotNull();
      assertThat(next.<String>getProperty("executionPlanAsString")).isNotNull();
      result.close();
    });

    final ResultSet rs = database.query("sql", "SELECT bar FROM " + typeName);
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Integer>getProperty("bar")).isEqualTo(0);
    rs.close();
  }

  /**
   * Same bug, reached through {@code ForEachStep}: it chains whatever
   * {@code Statement.createExecutionPlan()} returns for each statement in the loop body exactly like
   * {@code ScriptLineStep} does, so it shared the same silent-write hazard before this fix.
   */
  @Test
  void explainUpdateInsideForEachDoesNotExecuteWrite() {
    final String typeName = "Issue6648ForEachTarget";
    database.getSchema().createDocumentType(typeName);
    database.transaction(() -> database.command("sql", "INSERT INTO " + typeName + " SET bar = 0"));

    database.transaction(() -> {
      final String script = """
              FOREACH ($i IN [1]) {
                  EXPLAIN UPDATE %s SET bar = 1;
              }
          """.formatted(typeName);
      final ResultSet result = database.command("sqlscript", script);
      result.close();
    });

    final ResultSet rs = database.query("sql", "SELECT bar FROM " + typeName);
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Integer>getProperty("bar")).isEqualTo(0);
    rs.close();
  }

  /**
   * Same bug, reached through {@code WhileStep}.
   */
  @Test
  void explainUpdateInsideWhileDoesNotExecuteWrite() {
    final String typeName = "Issue6648WhileTarget";
    database.getSchema().createDocumentType(typeName);
    database.transaction(() -> database.command("sql", "INSERT INTO " + typeName + " SET bar = 0"));

    database.transaction(() -> {
      final String script = """
              LET $i = 0;
              WHILE ($i < 1) {
                  EXPLAIN UPDATE %s SET bar = 1;
                  LET $i = $i + 1;
              }
          """.formatted(typeName);
      final ResultSet result = database.command("sqlscript", script);
      result.close();
    });

    final ResultSet rs = database.query("sql", "SELECT bar FROM " + typeName);
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Integer>getProperty("bar")).isEqualTo(0);
    rs.close();
  }

  /**
   * Same bug, reached through {@code RetryStep}: statements between {@code BEGIN} and
   * {@code COMMIT RETRY n} are collected into a retry block and chained the same way as a plain
   * script line.
   */
  @Test
  void explainUpdateInsideRetryBlockDoesNotExecuteWrite() {
    final String typeName = "Issue6648RetryTarget";
    database.getSchema().createDocumentType(typeName);
    database.transaction(() -> database.command("sql", "INSERT INTO " + typeName + " SET bar = 0"));

    database.transaction(() -> {
      final String script = """
              BEGIN;
              EXPLAIN UPDATE %s SET bar = 1;
              COMMIT RETRY 5;
          """.formatted(typeName);
      final ResultSet result = database.command("sqlscript", script);
      result.close();
    });

    final ResultSet rs = database.query("sql", "SELECT bar FROM " + typeName);
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Integer>getProperty("bar")).isEqualTo(0);
    rs.close();
  }

  /**
   * Same bug, reached through a SELECT's own {@code LET} clause: {@code LetItem.query} is typed as
   * a generic {@code Statement}, so {@code LET $x = (EXPLAIN <statement>)} parses, and
   * {@code GlobalLetQueryStep}/{@code LetQueryStep} (built by {@code SelectExecutionPlanner}) chain
   * whatever {@code query.createExecutionPlan()} returns exactly like every other caller this fix
   * covers.
   * <p>
   * Uses {@code INSERT}, not {@code UPDATE}/{@code DELETE}: reaching this path at all requires
   * {@code Statement.refersToParent()} on the wrapped statement, which {@code UpdateStatement},
   * {@code DeleteStatement}, {@code CreateVertexStatement}, {@code CreateEdgeStatement} and
   * {@code DDLStatement} have never implemented - {@code LET $x = (UPDATE ...)} throws
   * {@code UnsupportedOperationException} regardless of EXPLAIN, so those aren't a silent-write
   * risk here, just a pre-existing, unrelated gap out of scope for this fix.
   * {@code InsertStatement.refersToParent()} already returns {@code false}, so INSERT is the one
   * write statement that actually reaches {@code ExplainExecutionPlan} through this path today.
   */
  @Test
  void explainInsertInsideSelectLetDoesNotExecuteWrite() {
    final String typeName = "Issue6648SelectLetTarget";
    database.getSchema().createDocumentType(typeName);

    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT $explained AS explained LET $explained = (EXPLAIN INSERT INTO " + typeName + " SET bar = 1)");
      assertThat(result.hasNext()).isTrue();
      result.next();
      result.close();
    });

    final ResultSet rs = database.query("sql", "SELECT count(*) as count FROM " + typeName);
    assertThat(rs.next().<Long>getProperty("count")).isEqualTo(0L);
    rs.close();
  }

  // ---- #7921: plan numbers and rendered dates do not follow the JVM default FORMAT locale ----
  // EXPLAIN and PROFILE formatted their cost and row counts with a bare new DecimalFormat(), whose grouping separator
  // follows the JVM default FORMAT locale. The same query over the same data therefore printed 1,234,567μs on an en_US
  // server and 1.234.567μs on a de_DE one, so anything diffing or parsing two servers' plans saw a difference that is not
  // a difference. The same sweep covers two more default-locale formatters on output surfaces the issue names: Result's
  // database == null branch, which rendered a Date into JSON with a pattern-less, locale-less SimpleDateFormat, and
  // TableFormatter's date column, whose pattern was fixed but whose calendar system and digits were not.

  /**
   * Regression test for issue #7921.
   * <p>
   * Both numbers a plan carries, at the one place that formats them. Driven directly rather than through a real
   * PROFILE because a measured cost is not reproducible, and a cost under 1000μs carries no grouping separator at
   * all - the very character under test.
   */
  @Test
  void aPlansCostAndRowCountAreFormattedTheSameWayUnderEveryLocale() {
    final Step step = new Step();
    step.cost = 1_234_567_000L; // nanoseconds: the plan prints microseconds
    step.rowCount = 1_234L;

    for (final Locale locale : new Locale[] { Locale.ROOT, Locale.US, Locale.GERMANY, Locale.ITALY,
        Locale.forLanguageTag("th-TH-u-nu-thai") }) {
      final Locale previous = Locale.getDefault(Locale.Category.FORMAT);
      try {
        Locale.setDefault(Locale.Category.FORMAT, locale);
        assertThat(step.costFormatted()).as("cost under %s", locale).isEqualTo("1,234,567μs");
        assertThat(step.rowCountFormatted()).as("row count under %s", locale).isEqualTo("1,234 rows");
      } finally {
        Locale.setDefault(Locale.Category.FORMAT, previous);
      }
    }
  }

  /**
   * Regression test for issue #7921.
   * <p>
   * And the same through a real PROFILE, so the wiring from the plan to these two methods is covered too.
   */
  @Test
  void aProfiledPlanNeverGroupsWithTheServersSeparator() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.begin();
    try {
      for (int i = 0; i < 1500; i++)
        database.command("sql", "INSERT INTO T SET n = " + i);
    } finally {
      database.commit();
    }

    final String underGerman = profileOf("SELECT FROM T", Locale.GERMANY);

    assertThat(underGerman).contains("μs");
    assertThat(underGerman.replaceAll("[0-9,]+μs", "<cost>"))
        .as("a de_DE server must not group a plan's cost with '.': %s", underGerman)
        .doesNotContain("μs");
  }

  /** Reaches the two protected formatters {@code prettyPrint()} implementations go through. */
  private static class Step extends AbstractExecutionStep {
    Step() {
      super(null);
    }

    String costFormatted() {
      return getCostFormatted();
    }

    String rowCountFormatted() {
      return getRowCountFormatted();
    }

    @Override
    public ResultSet syncPull(final CommandContext context, final int nRecords) {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Regression test for issue #7921.
   * <p>
   * A {@code Date} rendered into JSON by a {@code Result} with no database - {@code Result.valueToJSON}'s fallback
   * branch - used a {@code SimpleDateFormat} built with neither a pattern nor a locale, so it emitted the server's
   * default SHORT date/time: {@code 9/18/26, 3:04 PM} on one host, {@code 18.09.26, 15:04} on another, and neither
   * shape parseable by the format every other {@code Date} in the same answer uses.
   */
  @Test
  void aDateInAResultWithNoDatabaseIsRenderedWithTheProductPatternUnderEveryLocale() {
    final Date date = dateOf(2026, Calendar.SEPTEMBER, 18, 15, 4, 5);

    for (final Locale locale : new Locale[] { Locale.ROOT, Locale.US, Locale.GERMANY,
        Locale.forLanguageTag("th-TH-u-ca-buddhist-nu-thai") }) {
      final Locale previous = Locale.getDefault(Locale.Category.FORMAT);
      try {
        Locale.setDefault(Locale.Category.FORMAT, locale);

        final ResultInternal result = new ResultInternal((Database) null);
        result.setProperty("d", date);

        assertThat(result.toJSON().getString("d")).as("under %s", locale).isEqualTo("2026-09-18 15:04:05");
      } finally {
        Locale.setDefault(Locale.Category.FORMAT, previous);
      }
    }
  }

  /**
   * Regression test for issue #7921.
   * <p>
   * A {@code Date} column in a printed table. The pattern was always explicit here, but the locale never was, and
   * the locale decides the calendar system and the digits - under {@code th-TH} the same instant carried a
   * Buddhist-era year in Thai digits. This pins the rendering rather than reproducing the old failure: the
   * formatter it replaced was a {@code static final}, so it froze whatever locale the JVM started in and a test
   * could not move it afterwards.
   */
  @Test
  void aDateColumnIsRenderedTheSameWayUnderEveryLocale() {
    final Date date = dateOf(2026, Calendar.SEPTEMBER, 18, 15, 4, 5);

    final String underRoot = prettyDate(date, Locale.ROOT);
    final String underThai = prettyDate(date, Locale.forLanguageTag("th-TH-u-ca-buddhist-nu-thai"));

    assertThat(underRoot).isEqualTo("2026-09-18 15:04:05.000");
    assertThat(underThai)
        .as("the server's locale must not decide the calendar system or the digits of a rendered date")
        .isEqualTo(underRoot);
  }

  private static Date dateOf(final int year, final int month, final int day, final int hour, final int minute,
      final int second) {
    final Calendar calendar = Calendar.getInstance(TimeZone.getDefault(), Locale.US);
    calendar.clear();
    calendar.set(year, month, day, hour, minute, second);
    return calendar.getTime();
  }

  private String profileOf(final String query, final Locale locale) {
    final Locale previous = Locale.getDefault(Locale.Category.FORMAT);
    try {
      Locale.setDefault(Locale.Category.FORMAT, locale);
      try (final ResultSet rs = database.query("sql", "PROFILE " + query)) {
        return rs.next().getProperty("executionPlanAsString");
      }
    } finally {
      Locale.setDefault(Locale.Category.FORMAT, previous);
    }
  }

  private static String prettyDate(final Date date, final Locale locale) {
    final Locale previous = Locale.getDefault(Locale.Category.FORMAT);
    try {
      Locale.setDefault(Locale.Category.FORMAT, locale);
      return String.valueOf(TableFormatter.getPrettyFieldValue(date, 10));
    } finally {
      Locale.setDefault(Locale.Category.FORMAT, previous);
    }
  }
}
