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

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

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
}
