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
package com.arcadedb.query.sql.parser;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A comparison of a DATETIME or a DATE with a string literal, or with a string bound to a parameter, converts the string to the
 * class of the compared value once per execution, not once per record (issue #12713).
 * <p>
 * The operators were handed the raw string on every row and converted it with {@code Type.convertOrNull}, so
 * {@code DateUtils.parseDateTime} ran once per record: a scan of 500,000 records spent 70% of its time parsing the one literal.
 * These tests pin the answer of every shape that converts the string, which the change must leave as it was: each operator,
 * {@code BETWEEN} and {@code IN}, a literal and a parameter, a DATE against a datetime string (issue #10206), a string that is no
 * date (#5900), an operand met with a different class on every row, and a parallel scan, whose workers evaluate one condition
 * concurrently. The last tests count the conversions one execution makes, through the memo it converts with.
 */
class ConstantOperandConversionIssue12713Test extends TestHelper {
  private static final String LITERAL = "2024-06-01 00:00:00";
  private static final int    ROWS    = 1_000;

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE E");
    database.command("sql", "CREATE PROPERTY E.id INTEGER");
    database.command("sql", "CREATE PROPERTY E.ts DATETIME");
    database.command("sql", "CREATE PROPERTY E.d DATE");
    // v is not declared: its class changes from one row to the next, so a conversion remembered for one class must not answer
    // the next row
    database.transaction(() -> {
      row(1, LocalDateTime.of(2024, 5, 31, 23, 59, 59), LocalDate.of(2024, 5, 31), LocalDateTime.of(2024, 6, 2, 0, 0));
      row(2, LocalDateTime.of(2024, 6, 1, 0, 0), LocalDate.of(2024, 6, 1), LocalDate.of(2024, 6, 2));
      row(3, LocalDateTime.of(2024, 6, 1, 12, 0), LocalDate.of(2024, 6, 1), LocalDateTime.of(2024, 5, 1, 0, 0));
      row(4, LocalDateTime.of(2024, 6, 2, 0, 0), LocalDate.of(2024, 6, 2), LocalDate.of(2024, 5, 1));
      row(5, LocalDateTime.of(2024, 7, 1, 0, 0), LocalDate.of(2024, 7, 1), "2024-07-01");
      row(6, null, null, 5L);
      row(7, LocalDateTime.of(2024, 7, 15, 8, 30), LocalDate.of(2024, 7, 15), LocalDate.of(2024, 6, 1));
    });
  }

  private void row(final int id, final LocalDateTime ts, final LocalDate d, final Object v) {
    database.newDocument("E").set("id", id, "ts", ts, "d", d, "v", v).save();
  }

  @Test
  void everyOperatorAgainstAStringLiteral() {
    assertThat(ids("SELECT id FROM E WHERE ts > '" + LITERAL + "'")).containsExactly(3, 4, 5, 7);
    assertThat(ids("SELECT id FROM E WHERE ts >= '" + LITERAL + "'")).containsExactly(2, 3, 4, 5, 7);
    assertThat(ids("SELECT id FROM E WHERE ts < '" + LITERAL + "'")).containsExactly(1);
    assertThat(ids("SELECT id FROM E WHERE ts <= '" + LITERAL + "'")).containsExactly(1, 2);
    assertThat(ids("SELECT id FROM E WHERE ts = '" + LITERAL + "'")).containsExactly(2);
    assertThat(ids("SELECT id FROM E WHERE ts <> '" + LITERAL + "'")).containsExactly(1, 3, 4, 5, 7);
    assertThat(ids("SELECT id FROM E WHERE ts != '" + LITERAL + "'")).containsExactly(1, 3, 4, 5, 7);
    assertThat(ids("SELECT id FROM E WHERE ts <=> '" + LITERAL + "'")).containsExactly(2);
    assertThat(ids("SELECT id FROM E WHERE NOT (ts > '" + LITERAL + "')")).containsExactly(1, 2);
    assertThat(ids("SELECT id FROM E WHERE ts > '2024-06-01T00:00:00'")).containsExactly(3, 4, 5, 7);
    assertThat(ids("SELECT id FROM E WHERE ts BETWEEN '" + LITERAL + "' AND '2024-07-01 00:00:00'")).containsExactly(2, 3, 4, 5);
    assertThat(ids("SELECT id FROM E WHERE ts IN ['" + LITERAL + "', '2024-06-02 00:00:00']")).containsExactly(2, 4);
    assertThat(ids("SELECT id FROM E WHERE ts NOT IN ['" + LITERAL + "', '2024-06-02 00:00:00']")).containsExactly(1, 3, 5, 7);
    assertThat(ids("SELECT id FROM E WHERE ts IN '" + LITERAL + "'")).containsExactly(2);
  }

  @Test
  void everyOperatorAgainstAStringParameter() {
    assertThat(ids("SELECT id FROM E WHERE ts > ?", LITERAL)).containsExactly(3, 4, 5, 7);
    assertThat(ids("SELECT id FROM E WHERE ts >= ?", LITERAL)).containsExactly(2, 3, 4, 5, 7);
    assertThat(ids("SELECT id FROM E WHERE ts < ?", LITERAL)).containsExactly(1);
    assertThat(ids("SELECT id FROM E WHERE ts <= ?", LITERAL)).containsExactly(1, 2);
    assertThat(ids("SELECT id FROM E WHERE ts = ?", LITERAL)).containsExactly(2);
    assertThat(ids("SELECT id FROM E WHERE ts <> ?", LITERAL)).containsExactly(1, 3, 4, 5, 7);
    assertThat(ids("SELECT id FROM E WHERE ts BETWEEN ? AND ?", LITERAL, "2024-07-01 00:00:00")).containsExactly(2, 3, 4, 5);
    assertThat(ids("SELECT id FROM E WHERE ts IN ?", List.of(LITERAL, "2024-06-02 00:00:00"))).containsExactly(2, 4);
    assertThat(ids("SELECT id FROM E WHERE ts NOT IN ?", List.of(LITERAL, "2024-06-02 00:00:00"))).containsExactly(1, 3, 5, 7);
    assertThat(ids("SELECT id FROM E WHERE ts IN ?", (Object) new String[] { LITERAL, "2024-06-02 00:00:00" })).containsExactly(2, 4);

    try (final ResultSet rs = database.query("sql", "SELECT id FROM E WHERE ts > :d AND ts < :e",
        Map.of("d", LITERAL, "e", "2024-07-01 00:00:00"))) {
      assertThat(sorted(rs)).containsExactly(3, 4);
    }
  }

  /** A DATE against a string that carries a time of day reads the day the string names (issue #10206). */
  @Test
  void aDateAgainstADatetimeString() {
    assertThat(ids("SELECT id FROM E WHERE d = '2024-06-01 12:00:00'")).containsExactly(2, 3);
    assertThat(ids("SELECT id FROM E WHERE d > '2024-06-01 12:00:00'")).containsExactly(4, 5, 7);
    assertThat(ids("SELECT id FROM E WHERE d >= '2024-06-01 12:00:00'")).containsExactly(2, 3, 4, 5, 7);
    assertThat(ids("SELECT id FROM E WHERE d < '2024-06-01 12:00:00'")).containsExactly(1);
    assertThat(ids("SELECT id FROM E WHERE d <= ?", "2024-06-01 12:00:00")).containsExactly(1, 2, 3);
    assertThat(ids("SELECT id FROM E WHERE d <> '2024-06-01'")).containsExactly(1, 4, 5, 7);
    assertThat(ids("SELECT id FROM E WHERE d BETWEEN '2024-06-01' AND '2024-07-01 23:00:00'")).containsExactly(2, 3, 4, 5);
    assertThat(ids("SELECT id FROM E WHERE d IN ['2024-06-01', '2024-07-15 10:00:00']")).containsExactly(2, 3, 7);
  }

  /** A string that is no date has no defined order against a datetime and is equal to none (#5900). */
  @Test
  void aStringThatIsNoDate() {
    assertThat(ids("SELECT id FROM E WHERE ts > 'not a date'")).isEmpty();
    assertThat(ids("SELECT id FROM E WHERE ts <= 'not a date'")).isEmpty();
    assertThat(ids("SELECT id FROM E WHERE ts = 'not a date'")).isEmpty();
    assertThat(ids("SELECT id FROM E WHERE ts <> 'not a date'")).containsExactly(1, 2, 3, 4, 5, 7);
    assertThat(ids("SELECT id FROM E WHERE ts BETWEEN 'not a date' AND '2025-01-01 00:00:00'")).isEmpty();
    assertThat(ids("SELECT id FROM E WHERE ts BETWEEN '2020-01-01 00:00:00' AND 'not a date'")).isEmpty();
    assertThat(ids("SELECT id FROM E WHERE ts IN ['not a date']")).isEmpty();
    assertThat(ids("SELECT id FROM E WHERE ts NOT IN ['not a date']")).containsExactly(1, 2, 3, 4, 5, 7);
    assertThat(ids("SELECT id FROM E WHERE ts > ?", "")).isEmpty();
  }

  /**
   * One string compared with a value whose class changes from row to row is converted to each class it meets: a LocalDateTime, a
   * LocalDate (the day the string names), a string (compared as a string), a number (no order).
   */
  @Test
  void anOperandMetWithADifferentClassOnEveryRow() {
    assertThat(ids("SELECT id FROM E WHERE v > '" + LITERAL + "'")).containsExactly(1, 2, 5);
    assertThat(ids("SELECT id FROM E WHERE v >= ?", LITERAL)).containsExactly(1, 2, 5, 7);
    assertThat(ids("SELECT id FROM E WHERE v < '" + LITERAL + "'")).containsExactly(3, 4);
    assertThat(ids("SELECT id FROM E WHERE v = '" + LITERAL + "'")).containsExactly(7);
    assertThat(ids("SELECT id FROM E WHERE v <> '" + LITERAL + "'")).containsExactly(1, 2, 3, 4, 5, 6);
    assertThat(ids("SELECT id FROM E WHERE v BETWEEN '" + LITERAL + "' AND '2024-06-30 00:00:00'")).containsExactly(1, 2, 7);
    assertThat(ids("SELECT id FROM E WHERE v IN ['2024-06-02 00:00:00', '2024-06-01']")).containsExactly(1, 2, 7);
    assertThat(ids("SELECT id FROM E WHERE v > '4'")).containsExactly(1, 2, 3, 4, 6, 7);
  }

  /**
   * A string read under the database's datetime format is read under the format of the execution that compares it: a format set
   * between two executions of one cached statement reaches the second.
   */
  @Test
  void aDatetimeFormatSetBetweenTwoExecutionsReachesTheSecond() {
    // the literal stays in the cached statement, the same string object for both executions
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_LITERAL_PARAMETERIZATION, false);
    final String query = "SELECT id FROM E WHERE ts > '01/06/2024 00:00:00'";
    assertThat(ids(query)).isEmpty();

    database.getSchema().setDateTimeFormat("dd/MM/yyyy HH:mm:ss");
    assertThat(ids(query)).containsExactly(3, 4, 5, 7);
  }

  /** The workers of a parallel scan evaluate one condition concurrently, and answer what the sequential scan answers. */
  @Test
  void aParallelScanAnswersWhatTheSequentialScanAnswers() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 2);
    database.getSchema().createDocumentType("P", 4);
    database.command("sql", "CREATE PROPERTY P.ts DATETIME");
    final LocalDateTime start = LocalDateTime.of(2024, 1, 1, 0, 0);
    final int rows = 20_000;
    database.transaction(() -> {
      for (int i = 0; i < rows; i++)
        database.newDocument("P").set("id", i, "ts", start.plusMinutes(37L * i)).save();
    });
    final LocalDateTime from = LocalDateTime.of(2024, 3, 1, 0, 0);
    final LocalDateTime to = LocalDateTime.of(2024, 6, 1, 0, 0);
    int after = 0;
    int between = 0;
    for (int i = 0; i < rows; i++) {
      final LocalDateTime ts = start.plusMinutes(37L * i);
      if (ts.isAfter(to))
        ++after;
      if (!ts.isBefore(from) && !ts.isAfter(to))
        ++between;
    }

    final String query = "SELECT id FROM P WHERE ts > '" + LITERAL + "'";
    final String range = "SELECT id FROM P WHERE ts BETWEEN '2024-03-01 00:00:00' AND '" + LITERAL + "'";
    final String list = "SELECT id FROM P WHERE ts NOT IN ['" + LITERAL + "', '2024-03-01 00:00:00']";
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + query)) {
      assertThat(rs.next().<String>getProperty("executionPlanAsString")).contains("(parallel)");
    }
    assertThat(ids(query)).hasSize(after);
    assertThat(ids("SELECT id FROM P WHERE ts > ?", LITERAL)).hasSize(after);
    assertThat(ids(range)).hasSize(between);
    // 2024-06-01T00:00 is no row (37 minutes do not divide the minutes from the start), 2024-03-01T00:00 is not either
    assertThat(ids(list)).hasSize(rows);

    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, false);
    assertThat(ids(query)).hasSize(after);
    assertThat(ids(range)).hasSize(between);
  }

  /**
   * Each comparison operator converts the string once for an execution, however many records it compares, and a new execution
   * converts it again: its memo lives in the context of the execution, not on the node of the cached plan.
   */
  @Test
  void aComparisonConvertsItsStringOncePerExecution() {
    final LocalDateTime bound = LocalDateTime.of(2024, 6, 1, 0, 0);
    for (final String operator : new String[] { ">", ">=", "<", "<=", "=", "<>", "!=", "<=>" }) {
      final BinaryCondition condition = (BinaryCondition) condition("ts " + operator + " '" + LITERAL + "'");
      final BasicCommandContext context = context();
      for (int i = 0; i < ROWS; i++) {
        final LocalDateTime ts = bound.plusHours(i - ROWS / 2);
        assertThat(condition.evaluate(new ResultInternal(Map.of("ts", ts)), context)).as("%s at %s", operator, ts)
            .isEqualTo(expected(operator, ts.compareTo(bound)));
      }
      final ConstantOperandConversion conversion = condition.rightConversion(context);
      assertThat(conversion.conversions()).as(operator).isEqualTo(1);

      final BasicCommandContext nextExecution = context();
      assertThat(condition.evaluate(new ResultInternal(Map.of("ts", bound)), nextExecution)).as(operator)
          .isEqualTo(expected(operator, 0));
      assertThat(condition.rightConversion(nextExecution)).as(operator).isNotSameAs(conversion);
      assertThat(condition.rightConversion(nextExecution).conversions()).as(operator).isEqualTo(1);
    }
  }

  @Test
  void aParameterIsConvertedOncePerExecution() {
    final LocalDateTime bound = LocalDateTime.of(2024, 6, 1, 0, 0);
    final BinaryCondition condition = (BinaryCondition) condition("ts > ?");
    final BasicCommandContext context = context();
    context.setInputParameters(new Object[] { LITERAL });
    for (int i = 0; i < ROWS; i++) {
      final LocalDateTime ts = bound.plusHours(i - ROWS / 2);
      assertThat(condition.evaluate(new ResultInternal(Map.of("ts", ts)), context)).isEqualTo(ts.isAfter(bound));
    }
    assertThat(condition.rightConversion(context).conversions()).isEqualTo(1);
  }

  /**
   * A DATE and a DATETIME met in turn by one operand (a schemaless property): each row answers for its own class, and the operand
   * is converted once per class, not once per row that alternates.
   */
  @Test
  void anOperandMetWithTwoClassesInTurnIsConvertedOncePerClass() {
    final BinaryCondition condition = (BinaryCondition) condition("v >= '" + LITERAL + "'");
    final BasicCommandContext context = context();
    for (int i = 0; i < ROWS; i++) {
      assertThat(condition.evaluate(new ResultInternal(Map.of("v", LocalDate.of(2024, 6, 1))), context)).isTrue();
      assertThat(condition.evaluate(new ResultInternal(Map.of("v", LocalDateTime.of(2024, 5, 31, 23, 0))), context)).isFalse();
      assertThat(condition.evaluate(new ResultInternal(Map.of("v", LocalDate.of(2024, 5, 31))), context)).isFalse();
      assertThat(condition.evaluate(new ResultInternal(Map.of("v", LocalDateTime.of(2024, 6, 1, 0, 0))), context)).isTrue();
    }
    assertThat(condition.rightConversion(context).conversions()).isEqualTo(2);

    // a third class pushes out the oldest entry and is answered for itself, and the two most recent stay remembered
    assertThat(condition.evaluate(new ResultInternal(Map.of("v", "2024-06-01 00:00:01")), context)).isTrue();
    assertThat(condition.evaluate(new ResultInternal(Map.of("v", 5L)), context)).isFalse();
    assertThat(condition.evaluate(new ResultInternal(Map.of("v", LocalDateTime.of(2024, 6, 1, 0, 0))), context)).isTrue();
    assertThat(condition.evaluate(new ResultInternal(Map.of("v", LocalDate.of(2024, 6, 1))), context)).isTrue();
    assertThat(condition.evaluate(new ResultInternal(Map.of("v", LocalDateTime.of(2024, 6, 2, 0, 0))), context)).isTrue();
  }

  /** An operand that is not a literal or a parameter is converted as it was, with no memo: its string may change per record. */
  @Test
  void aPropertyOperandHasNoMemo() {
    final BinaryCondition condition = (BinaryCondition) condition("ts > s");
    final BasicCommandContext context = context();
    assertThat(condition.evaluate(new ResultInternal(Map.of("ts", LocalDateTime.of(2024, 6, 2, 0, 0), "s", LITERAL)), context)).isTrue();
    assertThat(condition.evaluate(new ResultInternal(Map.of("ts", LocalDateTime.of(2024, 6, 2, 0, 0), "s", "2024-07-01 00:00:00")),
        context)).isFalse();
    assertThat(condition.rightConversion(context).conversions()).isZero();
  }

  @Test
  void aBetweenConvertsEachBoundOncePerExecution() {
    final LocalDateTime from = LocalDateTime.of(2024, 6, 1, 0, 0);
    final LocalDateTime to = LocalDateTime.of(2024, 6, 2, 0, 0);
    final BetweenCondition condition = (BetweenCondition) condition("ts BETWEEN '" + LITERAL + "' AND '2024-06-02 00:00:00'");
    final BasicCommandContext context = context();
    for (int i = 0; i < ROWS; i++) {
      final LocalDateTime ts = from.plusHours(i - ROWS / 2);
      assertThat(condition.evaluate(new ResultInternal(Map.of("ts", ts)), context)).as("%s", ts)
          .isEqualTo(!ts.isBefore(from) && !ts.isAfter(to));
    }
    assertThat(condition.boundConversion(context, true).conversions()).isEqualTo(1);
    assertThat(condition.boundConversion(context, false).conversions()).isEqualTo(1);
  }

  @Test
  void anInListConvertsEachItemOncePerExecution() {
    final LocalDateTime first = LocalDateTime.of(2024, 6, 1, 0, 0);
    final LocalDateTime second = LocalDateTime.of(2024, 6, 2, 0, 0);
    final InListMembership membership = InListMembership.build(List.of(LITERAL, "2024-06-02 00:00:00"), false);
    for (int i = 0; i < ROWS; i++) {
      final LocalDateTime ts = first.plusHours(i - ROWS / 2);
      assertThat(membership.evaluate(ts)).as("%s", ts).isEqualTo(ts.equals(first) || ts.equals(second));
    }
    assertThat(membership.conversions()).isEqualTo(2);
  }

  private static boolean expected(final String operator, final int order) {
    return switch (operator) {
      case ">" -> order > 0;
      case ">=" -> order >= 0;
      case "<" -> order < 0;
      case "<=" -> order <= 0;
      case "=", "<=>" -> order == 0;
      default -> order != 0;
    };
  }

  private static BooleanExpression condition(final String condition) {
    final SelectStatement statement = (SelectStatement) new SQLAntlrParser(null).parse("SELECT FROM E WHERE " + condition);
    return statement.getWhereClause().flatten().getFirst().getSubBlocks().getFirst();
  }

  private BasicCommandContext context() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    return context;
  }

  private List<Integer> ids(final String query, final Object... args) {
    try (final ResultSet rs = database.query("sql", query, args)) {
      return sorted(rs);
    }
  }

  private static List<Integer> sorted(final ResultSet rs) {
    final List<Integer> ids = new ArrayList<>();
    while (rs.hasNext())
      ids.add(rs.next().<Integer>getProperty("id"));
    Collections.sort(ids);
    return ids;
  }
}
