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

  private List<Object> values(final String script) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet rs = database.command("sqlscript", script)) {
      rs.forEachRemaining(r -> values.add(r.getProperty("value")));
    }
    return values;
  }
}
