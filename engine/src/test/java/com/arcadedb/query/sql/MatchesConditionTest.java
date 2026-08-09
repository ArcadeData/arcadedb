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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Regression tests for the MATCHES per-context regex pattern cache. */
class MatchesConditionTest extends TestHelper {

  @Test
  void collidingRegexesDoNotShareCachedPattern() {
    // "Aa.*" and "BB.*" are distinct regexes whose String.hashCode() collide (both 2031100).
    assertThat("Aa.*".hashCode()).isEqualTo("BB.*".hashCode());

    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Item");
      // Each row carries its own regex in 'pattern' and a value in 'name'.
      // 'name' must match that row's own 'pattern' and NOT the colliding sibling pattern.
      database.command("sql", "INSERT INTO Item SET name = 'Aardvark', pattern = 'Aa.*'");
      database.command("sql", "INSERT INTO Item SET name = 'BBking', pattern = 'BB.*'");
      // Control rows whose name does not match its own pattern.
      database.command("sql", "INSERT INTO Item SET name = 'BBking', pattern = 'Aa.*'");
      database.command("sql", "INSERT INTO Item SET name = 'Aardvark', pattern = 'BB.*'");
    });

    // Per-row expression-derived regex: every row is evaluated against the same CommandContext,
    // exercising the per-context pattern cache with two colliding regex strings.
    final ResultSet rs = database.query("sql", "SELECT name, pattern FROM Item WHERE name MATCHES pattern ORDER BY name");

    int count = 0;
    boolean foundAardvark = false;
    boolean foundBBking = false;
    while (rs.hasNext()) {
      final var row = rs.next();
      final String name = row.getProperty("name");
      final String pattern = row.getProperty("pattern");
      if ("Aardvark".equals(name)) {
        assertThat(pattern).isEqualTo("Aa.*");
        foundAardvark = true;
      } else if ("BBking".equals(name)) {
        assertThat(pattern).isEqualTo("BB.*");
        foundBBking = true;
      }
      count++;
    }

    // Exactly the two self-matching rows; the two control rows must be excluded.
    assertThat(count).isEqualTo(2);
    assertThat(foundAardvark).isTrue();
    assertThat(foundBBking).isTrue();
  }

  @Test
  void literalRegexWithMultipleDotsIsAccepted() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Dotted");
      database.command("sql", "INSERT INTO Dotted SET name = 'abc'");
    });

    // The cache key derived from this regex contains three dots. It must never be parsed as a
    // nested property path.
    final ResultSet rs = database.query("sql", "SELECT name FROM Dotted WHERE name MATCHES '.*.*'");

    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("name")).isEqualTo("abc");
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void patternTextDeadlineDoesNotCollideWithTheDeadlineCacheKey() {
    // Issue #5886, 9th review pass: the deadline shared across a query's MATCHES evaluations was originally
    // cached under the key "MATCHES_DEADLINE" - identical to the pattern-cache key the literal pattern text
    // "DEADLINE" produces ("MATCHES_" + "DEADLINE"). A row using that pattern threw ClassCastException
    // (java.util.regex.Pattern cast to java.lang.Long) on the very first evaluation, since both the compiled
    // Pattern and the shared deadline were being stored under the exact same context cache slot. The deadline
    // key now lives entirely outside the "MATCHES_" namespace the pattern cache uses, so it cannot collide
    // with "MATCHES_" + <any regex text>, however that text reads.
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE DeadlineCollision");
      database.command("sql", "INSERT INTO DeadlineCollision SET name = 'DEADLINE'");
      database.command("sql", "INSERT INTO DeadlineCollision SET name = 'other'");
    });

    final ResultSet rs = database.query("sql", "SELECT name FROM DeadlineCollision WHERE name MATCHES 'DEADLINE'");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("name")).isEqualTo("DEADLINE");
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void parameterRegexWithMultipleDotsIsAccepted() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE ParamDotted");
      database.command("sql", "INSERT INTO ParamDotted SET name = 'a.b.c'");
      database.command("sql", "INSERT INTO ParamDotted SET name = 'zzz'");
    });

    final ResultSet rs = database.query("sql", "SELECT name FROM ParamDotted WHERE name MATCHES :regex",
        Map.of("regex", "a\\..\\.."));

    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("name")).isEqualTo("a.b.c");
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void perRowRegexesWithMultipleDotsStayDistinct() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE DottedItem");
      database.command("sql", "INSERT INTO DottedItem SET name = 'a.b.c', pattern = 'a\\\\..\\\\..'");
      database.command("sql", "INSERT INTO DottedItem SET name = 'x.y.z', pattern = 'x\\\\..\\\\..'");
      // Control rows: name does not match its own multi-dot pattern.
      database.command("sql", "INSERT INTO DottedItem SET name = 'x.y.z', pattern = 'a\\\\..\\\\..'");
      database.command("sql", "INSERT INTO DottedItem SET name = 'a.b.c', pattern = 'x\\\\..\\\\..'");
    });

    // All four rows share one CommandContext, so two distinct multi-dot regexes populate the
    // pattern cache within a single execution.
    final ResultSet rs = database.query("sql", "SELECT name FROM DottedItem WHERE name MATCHES pattern ORDER BY name");

    final List<String> names = new ArrayList<>();
    while (rs.hasNext())
      names.add(rs.next().getProperty("name"));

    assertThat(names).containsExactly("a.b.c", "x.y.z");
  }

  @Test
  void literalMatchesReturnCorrectRows() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Word");
      database.command("sql", "INSERT INTO Word SET name = 'Aardvark'");
      database.command("sql", "INSERT INTO Word SET name = 'BBking'");
    });

    final ResultSet first = database.query("sql", "SELECT name FROM Word WHERE name MATCHES 'Aa.*'");
    assertThat(first.hasNext()).isTrue();
    assertThat(first.next().<String>getProperty("name")).isEqualTo("Aardvark");
    assertThat(first.hasNext()).isFalse();

    final ResultSet second = database.query("sql", "SELECT name FROM Word WHERE name MATCHES 'BB.*'");
    assertThat(second.hasNext()).isTrue();
    assertThat(second.next().<String>getProperty("name")).isEqualTo("BBking");
    assertThat(second.hasNext()).isFalse();
  }

  @Test
  void catastrophicPatternIsAbortedByRegexTimeout() {
    // Issue #5886: (.*a){20}$ against "a".repeat(40) + "!" triggers catastrophic backtracking in
    // java.util.regex. Matcher.matches() never polls interrupts or a deadline while backtracking, so
    // arcadedb.command.timeout cannot stop it (still running 30s later per the issue report); only
    // arcadedb.command.regexTimeout, enforced inside the match itself, can.
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_REGEX_TIMEOUT, 200L);

    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Pathological");
      database.command("sql", "INSERT INTO Pathological SET name = '" + "a".repeat(40) + "!'");
    });

    final long begin = System.currentTimeMillis();
    assertThatThrownBy(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM Pathological WHERE name MATCHES '(.*a){20}$'");
      while (rs.hasNext())
        rs.next();
    }).isInstanceOf(TimeoutException.class);
    final long elapsedMillis = System.currentTimeMillis() - begin;

    // Generous upper bound: proves the query was aborted near the configured deadline rather than
    // merely being slow (the unbounded match takes tens of seconds).
    assertThat(elapsedMillis).isLessThan(5000);
  }

  @Test
  void multiValueMatchesSharesOneTimeoutBudgetAcrossItems() {
    // A multi-value (list) property must not multiply the regex timeout budget by its item count: each
    // catastrophic item getting its own full budget would let a crafted 10-item list run for 10 * regexTimeout
    // instead of one evaluation bounded by regexTimeout overall. 10 items (rather than a smaller count) widens
    // the gap between the "shared" (~200-300ms) and "not shared" (~2000ms) outcomes, so the assertion below can
    // use a generous margin without losing the ability to catch a regression.
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_REGEX_TIMEOUT, 200L);

    final String pathological = "a".repeat(40) + "!";
    final String items = "'" + pathological + "'";
    final StringBuilder list = new StringBuilder("[");
    for (int i = 0; i < 10; i++)
      list.append(i == 0 ? "" : ", ").append(items);
    list.append(']');
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE MultiPathological");
      database.command("sql", "INSERT INTO MultiPathological SET tags = " + list);
    });

    final long begin = System.currentTimeMillis();
    assertThatThrownBy(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM MultiPathological WHERE tags MATCHES '(.*a){20}$'");
      while (rs.hasNext())
        rs.next();
    }).isInstanceOf(TimeoutException.class);
    final long elapsedMillis = System.currentTimeMillis() - begin;

    // 10 independent 200ms-per-item budgets would take >= 2000ms; a shared deadline keeps the whole evaluation
    // close to the single configured 200ms bound instead. 1000ms leaves generous CI-runner slack on both sides.
    assertThat(elapsedMillis).isLessThan(1000);
  }

  @Test
  void matchesSharesOneTimeoutBudgetAcrossAllRowsInTheScan() {
    // Issue #5886, 6th review pass: distinct from the multi-value-within-one-row case above, a WHERE ...
    // MATCHES clause scanning many ROWS must not let each row's own evaluation start a fresh regexTimeout
    // budget either - otherwise a table shaped so every row triggers catastrophic backtracking could still
    // cost up to rowCount * regexTimeout overall. The deadline is now cached on the CommandContext (the same
    // mechanism already used to cache the compiled Pattern), computed once for the whole query.
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_REGEX_TIMEOUT, 200L);

    final String pathological = "a".repeat(40) + "!";
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE PerRowPathological");
      for (int i = 0; i < 10; i++)
        database.command("sql", "INSERT INTO PerRowPathological SET name = '" + pathological + "'");
    });

    final long begin = System.currentTimeMillis();
    assertThatThrownBy(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM PerRowPathological WHERE name MATCHES '(.*a){20}$'");
      while (rs.hasNext())
        rs.next();
    }).isInstanceOf(TimeoutException.class);
    final long elapsedMillis = System.currentTimeMillis() - begin;

    // 10 independent 200ms-per-row budgets would take >= 2000ms; a query-wide shared deadline keeps the whole
    // scan close to the single configured 200ms bound instead.
    assertThat(elapsedMillis).isLessThan(1000);
  }
}
