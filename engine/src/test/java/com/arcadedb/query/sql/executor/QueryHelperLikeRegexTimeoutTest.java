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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.utility.StallAwareStopwatch;
import com.arcadedb.utility.TimeBoundRegex;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #5886, 2nd review pass: SQL LIKE/ILIKE's {@code %}-to-{@code .*} translation was
 * originally audited as "structurally safe" (no grouping/alternation/nested quantifiers), which rules out one
 * class of catastrophic backtracking but not another - a sequence of several {@code .*} segments reproduces the
 * same exponential blowup on its own, with no nesting required.
 * <p>
 * Since issue #10005 LIKE no longer goes through a regular expression, so that pattern is answered at once. What is left
 * to bound is a legitimately long match - a huge value against a long pattern costs up to the product of their lengths -
 * and {@code arcadedb.command.regexTimeout} still bounds it.
 */
class QueryHelperLikeRegexTimeoutTest extends TestHelper {

  // 20 "%a" segments then a literal, non-wildcard tail: LIKE's equivalent of the issue's own (.*a){20}$
  // reproducer. The match is always on the whole input, so no explicit end anchor is needed - the trailing literal
  // plays that role.
  private static final String PATHOLOGICAL_LIKE_PATTERN = "%a".repeat(20) + "c";

  // A value of 200,000 characters against a pattern whose 10,000-character literal tail never matches: the % retries the
  // tail at every position, about 2 * 10^9 steps, seconds of work the 200ms budget below has to cut short
  private static final String LONG_VALUE        = "a".repeat(200_000);
  private static final String LONG_LIKE_PATTERN = "%" + "a".repeat(10_000) + "b";

  @Test
  void likeWithTimeoutMatchesNormally() {
    assertThat(QueryHelper.like("foobar", "%ooba%", 1000)).isTrue();
    assertThat(QueryHelper.like("foobar", "%fff%", 1000)).isFalse();
  }

  @Test
  void likeUntilMatchesNormally() {
    final long deadline = TimeBoundRegex.newDeadline(1000);
    assertThat(QueryHelper.likeUntil("foobar", "%ooba%", deadline)).isTrue();
    assertThat(QueryHelper.likeUntil("foobar", "%fff%", deadline)).isFalse();
  }

  @Test
  void catastrophicBacktrackingPatternIsAnsweredWithoutTimingOut() {
    final String input = "a".repeat(41); // all 'a's: never matches the pattern's literal 'c' tail

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThat(QueryHelper.like(input, PATHOLOGICAL_LIKE_PATTERN, 200)).isFalse();
    assertThat(QueryHelper.like(input + "c", PATHOLOGICAL_LIKE_PATTERN, 200)).isTrue();

    stopwatch.assertGaveUpWithin(5000, "a LIKE match linear in its pattern from the exponential regex backtracking it replaced");
  }

  @Test
  void likeAbortsALongMatchWithinTheDeadline() {
    final String input = "a".repeat(1_000_000);

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThatThrownBy(() -> QueryHelper.like(input, LONG_LIKE_PATTERN, 200)).isInstanceOf(TimeoutException.class)
        // THE 10,002-CHARACTER PATTERN IS QUOTED BY ITS START ONLY
        .hasMessageContaining("of length " + LONG_LIKE_PATTERN.length()).message().hasSizeLessThan(300);

    stopwatch.assertGaveUpWithin(5000, "the configured 200ms deadline from an unbounded LIKE match");
  }

  @Test
  void sqlLikeIsAbortedByRegexTimeout() {
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_REGEX_TIMEOUT, 200L);

    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE LikePathological");
      database.command("sql", "INSERT INTO LikePathological SET name = ?", LONG_VALUE);
    });

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThatThrownBy(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM LikePathological WHERE name LIKE '" + LONG_LIKE_PATTERN + "'");
      while (rs.hasNext())
        rs.next();
    }).isInstanceOf(TimeoutException.class);

    stopwatch.assertGaveUpWithin(5000, "the configured 200ms deadline from an unbounded LIKE match");
  }

  @Test
  void sqlIlikeIsAbortedByRegexTimeout() {
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_REGEX_TIMEOUT, 200L);

    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE ILikePathological");
      database.command("sql", "INSERT INTO ILikePathological SET name = ?", LONG_VALUE.toUpperCase());
    });

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThatThrownBy(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM ILikePathological WHERE name ILIKE '" + LONG_LIKE_PATTERN + "'");
      while (rs.hasNext())
        rs.next();
    }).isInstanceOf(TimeoutException.class);

    stopwatch.assertGaveUpWithin(5000, "the configured 200ms deadline from an unbounded LIKE match");
  }

  @Test
  void multiValueLikeSharesOneTimeoutBudgetAcrossItems() {
    // A multi-value (list) left operand (LikeOperator's BY-ITEM branch, issue #2693) must not multiply the
    // timeout budget by its item count, the same concern MatchesConditionTest#
    // multiValueMatchesSharesOneTimeoutBudgetAcrossItems proves for MATCHES: each long item getting
    // its own full budget would let a crafted 10-item list run for 10 * regexTimeout instead of one
    // evaluation bounded by regexTimeout overall.
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_REGEX_TIMEOUT, 200L);

    final List<String> items = new ArrayList<>();
    for (int i = 0; i < 10; i++)
      items.add(LONG_VALUE);
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE MultiLikePathological");
      database.command("sql", "INSERT INTO MultiLikePathological SET tags = ?", items);
    });

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThatThrownBy(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM MultiLikePathological WHERE tags LIKE '" + LONG_LIKE_PATTERN + "'");
      while (rs.hasNext())
        rs.next();
    }).isInstanceOf(TimeoutException.class);

    // 10 independent 200ms-per-item budgets would take >= 2000ms; a shared deadline keeps the whole evaluation
    // close to the single configured 200ms bound instead. 1000ms leaves generous CI-runner slack on both sides.
    stopwatch.assertStayedUnder(1000, "one 200ms budget shared by the whole evaluation, not one per item");
  }
}
