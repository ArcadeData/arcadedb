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

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A call over literals or parameters that answers one value for an execution, such as
 * {@code date('2024-06-01 00:00:00', 'yyyy-MM-dd HH:mm:ss')}, is evaluated once per execution by a comparison or a {@code BETWEEN},
 * not once per record (issue #13523). A call whose answer may change from one record to the next ({@code date()} and
 * {@code sysdate()} read the clock) is still evaluated on every record, and a new execution evaluates the call again.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class ConstantCallOperandIssue13523Test extends TestHelper {
  private static final String LITERAL = "2024-06-01 00:00:00";
  private static final String FORMAT  = "yyyy-MM-dd HH:mm:ss";
  private static final String DATE    = "date('" + LITERAL + "', '" + FORMAT + "')";
  private static final int    ROWS    = 1_000;

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE E");
    database.command("sql", "CREATE PROPERTY E.id INTEGER");
    database.command("sql", "CREATE PROPERTY E.ts DATETIME");
    database.transaction(() -> {
      row(1, LocalDateTime.of(2024, 5, 31, 23, 59, 59));
      row(2, LocalDateTime.of(2024, 6, 1, 0, 0));
      row(3, LocalDateTime.of(2024, 6, 1, 12, 0));
      row(4, LocalDateTime.of(2024, 6, 2, 0, 0));
      row(5, LocalDateTime.of(2024, 7, 1, 0, 0));
      row(6, null);
    });
  }

  private void row(final int id, final LocalDateTime ts) {
    database.newDocument("E").set("id", id, "ts", ts).save();
  }

  /** The call answers what the literal and the parameter answer, on either side of the comparison and as a bound. */
  @Test
  void aCallAnswersWhatTheLiteralAndTheParameterAnswer() {
    final LocalDateTime bound = LocalDateTime.of(2024, 6, 1, 0, 0);
    for (final boolean parameterized : new boolean[] { true, false }) {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_LITERAL_PARAMETERIZATION, parameterized);
      assertThat(ids("SELECT id FROM E WHERE ts > " + DATE)).containsExactly(3, 4, 5);
      assertThat(ids("SELECT id FROM E WHERE ts > '" + LITERAL + "'")).containsExactly(3, 4, 5);
      assertThat(ids("SELECT id FROM E WHERE ts > ?", bound)).containsExactly(3, 4, 5);
      assertThat(ids("SELECT id FROM E WHERE ts >= " + DATE)).containsExactly(2, 3, 4, 5);
      assertThat(ids("SELECT id FROM E WHERE ts < " + DATE)).containsExactly(1);
      assertThat(ids("SELECT id FROM E WHERE ts = " + DATE)).containsExactly(2);
      assertThat(ids("SELECT id FROM E WHERE ts <> " + DATE)).containsExactly(1, 3, 4, 5);
      assertThat(ids("SELECT id FROM E WHERE " + DATE + " < ts")).containsExactly(3, 4, 5);
      assertThat(ids("SELECT id FROM E WHERE ts BETWEEN " + DATE + " AND date('2024-07-01 00:00:00', '" + FORMAT + "')"))
          .containsExactly(2, 3, 4, 5);
      assertThat(ids("SELECT id FROM E WHERE ts BETWEEN '" + LITERAL + "' AND '2024-07-01 00:00:00'")).containsExactly(2, 3, 4, 5);
      assertThat(ids("SELECT id FROM E WHERE ts > date(?, ?)", LITERAL, FORMAT)).containsExactly(3, 4, 5);
      assertThat(ids("SELECT id FROM E WHERE ts > date(?, ?)", "2024-06-02 00:00:00", FORMAT)).containsExactly(5);
      // with no argument the call is the clock, later than every row
      assertThat(ids("SELECT id FROM E WHERE ts < date()")).containsExactly(1, 2, 3, 4, 5);
      assertThat(ids("SELECT id FROM E WHERE ts < sysdate()")).containsExactly(1, 2, 3, 4, 5);
    }
  }

  /** The call is evaluated once for an execution, however many records it compares, and a new execution evaluates it again. */
  @Test
  void aComparisonEvaluatesTheCallOncePerExecution() {
    final LocalDateTime bound = LocalDateTime.of(2024, 6, 1, 0, 0);
    final BinaryCondition condition = (BinaryCondition) condition("ts > " + DATE);
    assertThat(condition.rightOperand().isCallOverConstants()).isTrue();
    assertThat(condition.leftOperand().isCallOverConstants()).isFalse();

    final BasicCommandContext context = context();
    for (int i = 0; i < ROWS; i++) {
      final LocalDateTime ts = bound.plusHours(i - ROWS / 2);
      assertThat(condition.evaluate(new ResultInternal(Map.of("ts", ts)), context)).as("%s", ts).isEqualTo(ts.isAfter(bound));
    }
    final ConstantCallOperand.Memo memo = condition.rightOperand().memo(context);
    assertThat(memo.computations()).isEqualTo(1);
    assertThat(memo.holdsTheValue()).isTrue();

    final BasicCommandContext nextExecution = context();
    assertThat(condition.evaluate(new ResultInternal(Map.of("ts", bound.plusHours(1))), nextExecution)).isTrue();
    assertThat(condition.rightOperand().memo(nextExecution)).isNotSameAs(memo);
    assertThat(condition.rightOperand().memo(nextExecution).computations()).isEqualTo(1);
  }

  @Test
  void aCallOnTheLeftIsEvaluatedOncePerExecution() {
    final LocalDateTime bound = LocalDateTime.of(2024, 6, 1, 0, 0);
    final BinaryCondition condition = (BinaryCondition) condition(DATE + " < ts");
    final BasicCommandContext context = context();
    for (int i = 0; i < ROWS; i++) {
      final LocalDateTime ts = bound.plusHours(i - ROWS / 2);
      assertThat(condition.evaluate(new ResultInternal(Map.of("ts", ts)), context)).as("%s", ts).isEqualTo(ts.isAfter(bound));
    }
    assertThat(condition.leftOperand().memo(context).computations()).isEqualTo(1);
  }

  @Test
  void aBetweenEvaluatesEachBoundOncePerExecution() {
    final LocalDateTime from = LocalDateTime.of(2024, 6, 1, 0, 0);
    final LocalDateTime to = LocalDateTime.of(2024, 6, 2, 0, 0);
    final BetweenCondition condition = (BetweenCondition) condition(
        "ts BETWEEN " + DATE + " AND date('2024-06-02 00:00:00', '" + FORMAT + "')");
    final BasicCommandContext context = context();
    for (int i = 0; i < ROWS; i++) {
      final LocalDateTime ts = from.plusHours(i - ROWS / 2);
      assertThat(condition.evaluate(new ResultInternal(Map.of("ts", ts)), context)).as("%s", ts)
          .isEqualTo(!ts.isBefore(from) && !ts.isAfter(to));
    }
    assertThat(condition.boundOperand(true).memo(context).computations()).isEqualTo(1);
    assertThat(condition.boundOperand(true).memo(context).holdsTheValue()).isTrue();
    assertThat(condition.boundOperand(false).memo(context).computations()).isEqualTo(1);
    assertThat(condition.boundOperand(false).memo(context).holdsTheValue()).isTrue();
  }

  @Test
  void aCallOverParametersIsEvaluatedOncePerExecution() {
    final LocalDateTime bound = LocalDateTime.of(2024, 6, 1, 0, 0);
    final BinaryCondition condition = (BinaryCondition) condition("ts > date(?, ?)");
    final BasicCommandContext context = context();
    context.setInputParameters(new Object[] { LITERAL, FORMAT });
    for (int i = 0; i < ROWS; i++) {
      final LocalDateTime ts = bound.plusHours(i - ROWS / 2);
      assertThat(condition.evaluate(new ResultInternal(Map.of("ts", ts)), context)).isEqualTo(ts.isAfter(bound));
    }
    assertThat(condition.rightOperand().memo(context).computations()).isEqualTo(1);
    assertThat(condition.rightOperand().memo(context).holdsTheValue()).isTrue();
  }

  /**
   * A context that reaches the memo of its parent, with parameters of its own, answers for its own parameters: the value is kept
   * with the parameters it was computed with.
   */
  @Test
  void aNestedContextWithOtherParametersComputesItsOwnValue() {
    final BinaryCondition condition = (BinaryCondition) condition("ts > date(?, ?)");
    final BasicCommandContext parent = context();
    parent.setInputParameters(new Object[] { LITERAL, FORMAT });
    final ResultInternal row = new ResultInternal(Map.of("ts", LocalDateTime.of(2024, 6, 1, 12, 0)));
    assertThat(condition.evaluate(row, parent)).isTrue();

    final BasicCommandContext child = context();
    child.setParent(parent);
    child.setInputParameters(new Object[] { "2024-06-02 00:00:00", FORMAT });
    assertThat(condition.evaluate(row, child)).isFalse();
    assertThat(condition.evaluate(row, child)).isFalse();
    assertThat(condition.evaluate(row, parent)).isTrue();
  }

  /** A cached statement run with other parameters answers for them. */
  @Test
  void aCachedStatementRunWithOtherParametersAnswersForThem() {
    final String query = "SELECT id FROM E WHERE ts > date(?, '" + FORMAT + "')";
    assertThat(ids(query, LITERAL)).containsExactly(3, 4, 5);
    assertThat(ids(query, "2024-06-02 00:00:00")).containsExactly(5);
    assertThat(ids(query, "2024-05-01 00:00:00")).containsExactly(1, 2, 3, 4, 5);
  }

  /** The clock is read on every record: {@code date()} with no argument or a null one, and {@code sysdate()}. */
  @Test
  void theClockIsReadOnEveryRecord() {
    for (final String call : new String[] { "date()", "sysdate()", "date(?)", "date(null)" }) {
      final BinaryCondition condition = (BinaryCondition) condition("ts < " + call);
      final BasicCommandContext context = context();
      context.setInputParameters(new Object[] { null });
      for (int i = 0; i < 10; i++)
        assertThat(condition.evaluate(new ResultInternal(Map.of("ts", LocalDateTime.of(2024, 6, 1, 0, 0))), context)).as(call).isTrue();
      assertThat(condition.rightOperand().memo(context).holdsTheValue()).as(call).isFalse();
    }
  }

  /** A call over a property, or with a modifier, is no call over constants: it is evaluated on every record as before. */
  @Test
  void aCallOverAPropertyIsEvaluatedOnEveryRecord() {
    assertThat(((BinaryCondition) condition("ts > date(s, '" + FORMAT + "')")).rightOperand().isCallOverConstants()).isFalse();
    assertThat(((BinaryCondition) condition("ts > " + DATE + ".format('yyyy')")).rightOperand().isCallOverConstants()).isFalse();
    assertThat(((BinaryCondition) condition("ts > max(1)")).rightOperand().isCallOverConstants()).isFalse();
    assertThat(((BinaryCondition) condition("ts > out('X')")).rightOperand().isCallOverConstants()).isFalse();

    final BinaryCondition condition = (BinaryCondition) condition("ts > date(s, '" + FORMAT + "')");
    final BasicCommandContext context = context();
    assertThat(condition.evaluate(new ResultInternal(Map.of("ts", LocalDateTime.of(2024, 6, 2, 0, 0), "s", LITERAL)), context)).isTrue();
    assertThat(condition.evaluate(new ResultInternal(Map.of("ts", LocalDateTime.of(2024, 6, 2, 0, 0), "s", "2024-07-01 00:00:00")),
        context)).isFalse();
  }

  /** A deterministic built-in over a parameter answers one value for an execution as well. */
  @Test
  void aDeterministicCallOverAParameterIsEvaluatedOncePerExecution() {
    final BinaryCondition condition = (BinaryCondition) condition("v > abs(?)");
    final BasicCommandContext context = context();
    context.setInputParameters(new Object[] { -5 });
    for (int i = 0; i < ROWS; i++)
      assertThat(condition.evaluate(new ResultInternal(Map.of("v", i)), context)).isEqualTo(i > 5);
    assertThat(condition.rightOperand().memo(context).computations()).isEqualTo(1);
    assertThat(condition.rightOperand().memo(context).holdsTheValue()).isTrue();
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
      final List<Integer> ids = new ArrayList<>();
      while (rs.hasNext())
        ids.add(rs.next().<Integer>getProperty("id"));
      Collections.sort(ids);
      return ids;
    }
  }
}
