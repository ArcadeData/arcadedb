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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.SQLScriptQueryEngine;
import com.arcadedb.query.sql.parser.BreakStatement;
import com.arcadedb.query.sql.parser.ReturnStatement;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11892: a FOREACH or WHILE whose body holds a RETURN, followed by another statement, failed with
 * NoSuchElementException. A RETURN executed inside the loop ends the script with its value; a loop that ends without
 * executing it lets the script go on to the next statement.
 */
class Issue11892ScriptLoopReturnTest extends TestHelper {

  @Override
  public void beginTest() {
    database.transaction(() -> database.command("sql", "CREATE DOCUMENT TYPE S"));
  }

  @Test
  void loopAsLastStatementStillReturns() {
    assertThat(values("FOREACH ($it IN [1,2]) { RETURN $it; }")).containsExactly(1);
  }

  @Test
  void returnInForEachEndsTheScript() {
    assertThat(values("FOREACH ($it IN [1,2]) { RETURN \"a\"; } RETURN \"b\";")).containsExactly("a");
  }

  @Test
  void returnOnALaterIterationEndsTheScript() {
    assertThat(values("FOREACH ($it IN [1,2,3]) { IF ($it = 2) { RETURN $it; } } RETURN 10;")).containsExactly(2);
  }

  @Test
  void forEachOverEmptyListGoesOn() {
    assertThat(values("FOREACH ($it IN []) { RETURN \"a\"; } RETURN \"b\";")).containsExactly("b");
  }

  @Test
  void returnInWhileEndsTheScript() {
    assertThat(values("LET $i = 0; WHILE ($i < 2) { LET $i = $i + 1; RETURN $i; } RETURN 99;")).containsExactly(1);
  }

  @Test
  void whileThatNeverRunsGoesOn() {
    assertThat(values("LET $i = 5; WHILE ($i < 2) { LET $i = $i + 1; RETURN $i; } RETURN 99;")).containsExactly(99);
  }

  @Test
  void returnInIfInsideForEachEndsTheScript() {
    assertThat(values("FOREACH ($it IN [1,2]) { IF (1 = 1) { RETURN 9; } } RETURN 10;")).containsExactly(9);
  }

  @Test
  void ifInsideForEachNotTakenGoesOn() {
    assertThat(values("FOREACH ($it IN [1,2]) { IF (1 = 2) { RETURN 9; } } RETURN 10;")).containsExactly(10);
  }

  @Test
  void returnInNestedForEachEndsTheScript() {
    assertThat(values("FOREACH ($it IN [1,2]) { FOREACH ($j IN [3]) { RETURN \"a\"; } } RETURN \"b\";")).containsExactly("a");
  }

  @Test
  void nestedForEachWithoutIterationsGoesOn() {
    assertThat(values("FOREACH ($it IN [1,2]) { FOREACH ($j IN []) { RETURN \"a\"; } } RETURN \"b\";")).containsExactly("b");
  }

  @Test
  void loopWithoutReturnGoesOn() {
    assertThat(values("FOREACH ($it IN [1,2]) { LET $z = 1; } RETURN \"b\";")).containsExactly("b");
  }

  @Test
  void nestedIfStillReturns() {
    assertThat(values("IF (1 = 1) { IF (2 = 2) { RETURN 7; } } RETURN 8;")).containsExactly(7);
  }

  @Test
  void nestedIfNotTakenInsideForEachGoesOn() {
    assertThat(values("FOREACH ($it IN [1,2]) { IF (1 = 1) { IF (1 = 2) { RETURN \"a\"; } } } RETURN \"b\";")).containsExactly("b");
  }

  @Test
  void nestedIfNotTakenInsideWhileGoesOn() {
    assertThat(values("LET $i = 0; WHILE ($i < 2) { LET $i = $i + 1; IF (1 = 1) { IF (1 = 2) { RETURN \"a\"; } } } RETURN \"b\";"))
        .containsExactly("b");
  }

  @Test
  void nestedIfNotTakenGoesOn() {
    assertThat(values("IF (1 = 1) { IF (1 = 2) { RETURN \"a\"; } } RETURN \"b\";")).containsExactly("b");
  }

  @Test
  void ifEndingWithAStatementThatIsNotAReturnGoesOn() {
    assertThat(values("IF (1 = 1) { IF (1 = 2) { RETURN \"a\"; } SELECT 9 AS v; } RETURN \"b\";")).containsExactly("b");
  }

  @Test
  void breakEndingAnIfInsideALoopIsNotAReturn() {
    assertThat(values("FOREACH ($it IN [1,2,3]) { IF ($it = 2) { IF (1 = 2) { RETURN \"a\"; } BREAK; } } RETURN \"b\";"))
        .containsExactly("b");
  }

  @Test
  void breakBeforeReturnLetsTheScriptGoOn() {
    assertThat(values("FOREACH ($it IN [1,2]) { BREAK; RETURN \"a\"; } RETURN \"b\";")).containsExactly("b");
  }

  @Test
  void statementsAfterAReturningLoopDoNotRun() {
    database.transaction(() -> assertThat(values(
        "FOREACH ($it IN [1,2]) { INSERT INTO S SET k = $it; RETURN \"a\"; } INSERT INTO S SET k = 99; RETURN \"b\";"))
        .containsExactly("a"));

    try (final ResultSet rs = database.query("sql", "SELECT k FROM S ORDER BY k")) {
      final List<Object> keys = new ArrayList<>();
      rs.forEachRemaining(r -> keys.add(r.getProperty("k")));
      assertThat(keys).containsExactly(1);
    }
  }

  @Test
  void statementsAfterALoopThatDidNotReturnRun() {
    database.transaction(() -> assertThat(values(
        "FOREACH ($it IN [1,2]) { INSERT INTO S SET k = $it; IF ($it = 5) { RETURN \"a\"; } } INSERT INTO S SET k = 99; RETURN \"b\";"))
        .containsExactly("b"));

    try (final ResultSet rs = database.query("sql", "SELECT k FROM S ORDER BY k")) {
      final List<Object> keys = new ArrayList<>();
      rs.forEachRemaining(r -> keys.add(r.getProperty("k")));
      assertThat(keys).containsExactly(1, 2, 99);
    }
  }

  // Mixed nesting of FOREACH, WHILE and IF: a RETURN has to be found at any depth and in either branch of an IF, or the line holding
  // it is drained and the script silently goes on to the next statement

  @Test
  void returnInWhileInsideForEachEndsTheScript() {
    assertThat(values("FOREACH ($it IN [1]) { WHILE (1 = 1) { RETURN \"a\"; } } RETURN \"b\";")).containsExactly("a");
  }

  @Test
  void whileThatNeverRunsInsideForEachGoesOn() {
    assertThat(values("FOREACH ($it IN [1,2]) { WHILE (1 = 2) { RETURN \"a\"; } } RETURN \"b\";")).containsExactly("b");
  }

  @Test
  void returnInWhileInsideIfInsideWhileEndsTheScript() {
    assertThat(values("LET $i = 0; WHILE ($i < 2) { LET $i = $i + 1; IF (1 = 1) { WHILE ($i < 5) { RETURN \"a\"; } } } RETURN \"b\";"))
        .containsExactly("a");
  }

  @Test
  void whileInsideIfInsideWhileNotTakenGoesOn() {
    assertThat(values("LET $i = 0; WHILE ($i < 2) { LET $i = $i + 1; IF (1 = 2) { WHILE ($i < 5) { RETURN \"a\"; } } } RETURN \"b\";"))
        .containsExactly("b");
  }

  @Test
  void returnInForEachInsideIfInsideForEachEndsTheScript() {
    assertThat(values("FOREACH ($it IN [1,2]) { IF (1 = 1) { FOREACH ($j IN [1]) { RETURN \"a\"; } } } RETURN \"b\";"))
        .containsExactly("a");
  }

  @Test
  void forEachInsideIfInsideForEachWithoutIterationsGoesOn() {
    assertThat(values("FOREACH ($it IN [1,2]) { IF (1 = 1) { FOREACH ($j IN []) { RETURN \"a\"; } } } RETURN \"b\";"))
        .containsExactly("b");
  }

  @Test
  void returnInForEachInsideIfEndsTheScript() {
    assertThat(values("IF (1 = 1) { FOREACH ($j IN [1]) { RETURN \"a\"; } } RETURN \"b\";")).containsExactly("a");
  }

  @Test
  void forEachInsideIfWithoutIterationsGoesOn() {
    assertThat(values("IF (1 = 1) { FOREACH ($j IN []) { RETURN \"a\"; } } RETURN \"b\";")).containsExactly("b");
  }

  @Test
  void returnInWhileInsideIfEndsTheScript() {
    assertThat(values("IF (1 = 1) { WHILE (1 = 1) { RETURN \"a\"; } } RETURN \"b\";")).containsExactly("a");
  }

  @Test
  void whileInsideIfThatNeverRunsGoesOn() {
    assertThat(values("IF (1 = 1) { WHILE (1 = 2) { RETURN \"a\"; } } RETURN \"b\";")).containsExactly("b");
  }

  @Test
  void ifWhoseLoopIsNotTakenGoesOn() {
    assertThat(values("IF (1 = 2) { FOREACH ($j IN [1]) { RETURN \"a\"; } } RETURN \"b\";")).containsExactly("b");
  }

  @Test
  void returnInTheElseBranchEndsTheScript() {
    assertThat(values("IF (1 = 2) { LET $a = 1; } ELSE { RETURN \"a\"; } RETURN \"b\";")).containsExactly("a");
  }

  @Test
  void elseBranchNotTakenGoesOn() {
    assertThat(values("IF (1 = 1) { LET $a = 1; } ELSE { RETURN \"a\"; } RETURN \"b\";")).containsExactly("b");
  }

  @Test
  void returnInTheElseBranchInsideForEachEndsTheScript() {
    assertThat(values("FOREACH ($it IN [1,2]) { IF (1 = 2) { LET $a = 1; } ELSE { RETURN \"a\"; } } RETURN \"b\";"))
        .containsExactly("a");
  }

  @Test
  void returnInALoopInTheElseBranchEndsTheScript() {
    assertThat(values("IF (1 = 2) { LET $a = 1; } ELSE { FOREACH ($j IN [1]) { RETURN \"a\"; } } RETURN \"b\";"))
        .containsExactly("a");
  }

  @Test
  void returnOnALaterPassOfANestedLoopEndsTheScript() {
    assertThat(values("FOREACH ($i IN [1,2]) { FOREACH ($j IN [1,2]) { IF ($i = 2 AND $j = 2) { RETURN $i * 10 + $j; } } } RETURN 0;"))
        .containsExactly(22);
  }

  @Test
  void returnOnALaterPassOfAnIfInsideNestedLoopsEndsTheScript() {
    assertThat(values(
        "FOREACH ($i IN [1,2]) { FOREACH ($j IN [1,2]) { IF ($i = 2) { IF ($j = 2) { RETURN $i * 10 + $j; } } } } RETURN 0;"))
        .containsExactly(22);
  }

  @Test
  void statementsAfterANestedReturnDoNotRun() {
    database.transaction(() -> assertThat(values(
        "FOREACH ($it IN [1,2]) { WHILE (1 = 1) { INSERT INTO S SET k = $it; RETURN \"a\"; } } INSERT INTO S SET k = 99; RETURN \"b\";"))
        .containsExactly("a"));

    try (final ResultSet rs = database.query("sql", "SELECT k FROM S ORDER BY k")) {
      final List<Object> keys = new ArrayList<>();
      rs.forEachRemaining(r -> keys.add(r.getProperty("k")));
      assertThat(keys).containsExactly(1);
    }
  }

  @Test
  void statementsAfterAnIfWithAReturningLoopRunWhenItDidNotReturn() {
    database.transaction(() -> assertThat(values(
        "IF (1 = 1) { FOREACH ($j IN [1,2]) { INSERT INTO S SET k = $j; IF ($j = 5) { RETURN \"a\"; } } } INSERT INTO S SET k = 99; RETURN \"b\";"))
        .containsExactly("b"));

    try (final ResultSet rs = database.query("sql", "SELECT k FROM S ORDER BY k")) {
      final List<Object> keys = new ArrayList<>();
      rs.forEachRemaining(r -> keys.add(r.getProperty("k")));
      assertThat(keys).containsExactly(1, 2, 99);
    }
  }

  @Test
  void aScriptPlanFindsAReturnWrittenOnAnyOfItsLines() {
    final CommandContext context = new BasicCommandContext();

    final ScriptExecutionPlan returnsOnTheSecondLine = new ScriptExecutionPlan(context);
    returnsOnTheSecondLine.chain(new SingleOpExecutionPlan(context, new BreakStatement()));
    returnsOnTheSecondLine.chain(new SingleOpExecutionPlan(context, new ReturnStatement()));
    assertThat(returnsOnTheSecondLine.containsReturn()).isTrue();

    final ScriptExecutionPlan noReturn = new ScriptExecutionPlan(context);
    noReturn.chain(new SingleOpExecutionPlan(context, new BreakStatement()));
    noReturn.chain(new SingleOpExecutionPlan(context, new BreakStatement()));
    assertThat(noReturn.containsReturn()).isFalse();
  }

  @Test
  void containsReturnReadsEveryBlockAtAnyDepth() {
    assertThat(containsReturn("RETURN 1")).isTrue();
    assertThat(containsReturn("LET $a = 1")).isFalse();
    assertThat(containsReturn("IF (1 = 1) { LET $a = 1; }")).isFalse();
    assertThat(containsReturn("IF (1 = 1) { LET $a = 1; } ELSE { RETURN 1; }")).isTrue();
    assertThat(containsReturn("FOREACH ($i IN [1]) { WHILE (1 = 1) { RETURN 1; } }")).isTrue();
    assertThat(containsReturn("WHILE (1 = 1) { IF (1 = 1) { FOREACH ($i IN [1]) { RETURN 1; } } }")).isTrue();
    assertThat(containsReturn("WHILE (1 = 1) { IF (1 = 1) { FOREACH ($i IN [1]) { LET $a = 1; } } }")).isFalse();
    assertThat(containsReturn("FOREACH ($i IN [1]) { IF (1 = 2) { LET $a = 1; } ELSE { WHILE (1 = 1) { RETURN 1; } } }")).isTrue();
  }

  private boolean containsReturn(final String statement) {
    return SQLScriptQueryEngine.parseScript(statement, (DatabaseInternal) database).getFirst().containsReturn();
  }

  private List<Object> values(final String script) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet rs = database.command("sqlscript", script)) {
      rs.forEachRemaining(r -> values.add(r.getProperty("value")));
    }
    return values;
  }
}
