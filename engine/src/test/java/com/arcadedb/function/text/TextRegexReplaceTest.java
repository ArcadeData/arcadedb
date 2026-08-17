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
package com.arcadedb.function.text;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Regression tests for issue #5886: text.regexReplace() was exposed to catastrophic regex backtracking. */
class TextRegexReplaceTest extends TestHelper {

  @Test
  void basicReplaceStillWorks() {
    final ResultSet rs = database.query("sql", "SELECT text.regexReplace('banana', 'a', 'o') AS r");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("r")).isEqualTo("bonono");
  }

  @Test
  void functionIsSafeWithoutACommandContext() {
    // Established convention for this function family (see TextStatelessFunctionsTest): calling the function
    // directly with a null CommandContext, bypassing the query engine entirely, must not NPE just because this
    // function now also reads arcadedb.command.regexTimeout - it falls back to the compiled-in default instead.
    final TextRegexReplace fn = new TextRegexReplace();
    assertThat(fn.execute(new Object[] { "banana", "a", "o" }, null)).isEqualTo("bonono");
  }

  @Test
  void catastrophicPatternIsAbortedByRegexTimeout() {
    // Issue #5886 follow-up: text.regexReplace() ran Pattern.compile(regex).matcher(str).replaceAll(...) with no
    // time bound - only a 500-char pattern-length cap and a StackOverflowError catch, neither of which bounds
    // the (.*a){20}$ reproducer (11 characters, no deep recursion needed for it to run unbounded).
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_REGEX_TIMEOUT, 200L);

    final String pathological = "a".repeat(40) + "!";

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThatThrownBy(
        () -> database.query("sql", "SELECT text.regexReplace('" + pathological + "', '(.*a){20}$', 'x') AS r").next())
        .isInstanceOf(IllegalArgumentException.class);

    // Generous upper bound: proves the call was aborted near the configured deadline rather than merely being
    // slow (the unbounded replaceAll takes tens of seconds).
    stopwatch.assertGaveUpWithin(5000, "the configured 200ms deadline from an unbounded replaceAll");
  }

  @Test
  void multiRowReplaceSharesOneTimeoutBudgetAcrossRows() {
    // Issue #5886, 12th review pass: text.regexReplace() has a CommandContext available and now shares one
    // deadline across every row it runs against within the same query (the same treatment MatchesCondition/
    // RegexExpression get, and the same rationale as their multi-row tests) - each row getting its own full
    // budget would let a table with many pathological rows cost up to rowCount * regexTimeout instead of one
    // bounded query.
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_REGEX_TIMEOUT, 200L);

    final String pathological = "a".repeat(40) + "!";
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE RegexReplaceMultiRow");
      for (int i = 0; i < 10; i++)
        database.command("sql", "INSERT INTO RegexReplaceMultiRow SET name = '" + pathological + "'");
    });

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThatThrownBy(() -> {
      final ResultSet rs = database.query("sql", "SELECT text.regexReplace(name, '(.*a){20}$', 'x') AS r FROM RegexReplaceMultiRow");
      while (rs.hasNext())
        rs.next();
    }).isInstanceOf(IllegalArgumentException.class);

    // 10 independent 200ms-per-row budgets would take >= 2000ms; a shared deadline keeps the whole query close
    // to the single configured 200ms bound instead.
    stopwatch.assertStayedUnder(1000, "one 200ms budget shared by the whole query, not one per row");
  }

  @Test
  void patternExceedingMaxLengthIsStillRejected() {
    // Unrelated to the regexTimeout wiring, but this guard sits right next to the code that was touched here -
    // confirm it still fires.
    final String tooLong = "a".repeat(501);
    assertThatThrownBy(() -> database.query("sql", "SELECT text.regexReplace('x', :p, 'y') AS r", Map.of("p", tooLong)).next())
        .isInstanceOf(IllegalArgumentException.class);
  }
}
