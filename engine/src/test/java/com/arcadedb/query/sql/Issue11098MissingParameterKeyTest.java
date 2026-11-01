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
import com.arcadedb.exception.CommandParameterMissingException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11098: a placeholder with no key in a parameter {@code Map} - or a statement with a placeholder run with no
 * parameters at all - was bound to {@code null} without a word, so an {@code INSERT} stored {@code null} and a
 * {@code SELECT ... WHERE v = ?} matched nothing. The {@code Object...} overloads already refused the missing value (#10721),
 * and openCypher refuses it too ({@code Expected parameter(s): x}). A key bound to an explicit {@code null} still binds
 * {@code null}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11098MissingParameterKeyTest extends TestHelper {

  @BeforeEach
  void createType() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.transaction(() -> {
      for (int k = 1; k <= 3; k++)
        database.command("sql", "INSERT INTO T SET k = ?, v = ?", k, k);
    });
  }

  @Test
  void positionalPlaceholderWithoutItsKeyIsRefused() {
    assertMissing(() -> database.command("sql", "INSERT INTO T SET k = 10, v = ?", Map.of("a", 1)), "0");
    assertMissing(() -> database.command("sql", "INSERT INTO T SET k = 10, v = ?", Map.of()), "0");
    assertMissing(() -> database.query("sql", "SELECT FROM T WHERE v = ?", Map.of("x", 1)), "0");
    assertThat(storedAt(10)).isEmpty();
  }

  @Test
  void namedPlaceholderWithoutItsKeyIsRefused() {
    assertMissing(() -> database.command("sql", "INSERT INTO T SET k = 10, v = :v", Map.of("w", 1)), "v");
    assertMissing(() -> database.query("sql", "SELECT FROM T WHERE v = :x", Map.of("y", 1)), "x");
    // Every missing name is reported, in the order the statement mentions them
    assertMissing(() -> database.query("sql", "SELECT FROM T WHERE v = :a OR v = :b OR k = :c", Map.of("b", 1)), "a", "c");
    assertThat(storedAt(10)).isEmpty();
  }

  @Test
  void placeholderRunWithoutAnyParameterIsRefused() {
    assertMissing(() -> database.command("sql", "INSERT INTO T SET k = 10, v = ?"), "0");
    assertMissing(() -> database.query("sql", "SELECT FROM T WHERE v = :v"), "v");
    assertThat(storedAt(10)).isEmpty();
  }

  @Test
  void boundKeysStillBind() {
    // the positional slot by its position, as the Object... overloads key it
    database.transaction(() -> database.command("sql", "INSERT INTO T SET k = 10, v = ?", Map.of("0", 7)));
    assertThat(storedAt(10)).containsExactly(7);
    assertThat(keysWhere("v = ?", Map.of("0", 1))).containsExactly(1);
    // a named placeholder by its name, by its prefixed name, or by its position
    assertThat(keysWhere("v = :x", Map.of("x", 2))).containsExactly(2);
    assertThat(keysWhere("v = :x", Map.of(":x", 2))).containsExactly(2);
    assertThat(keysWhere("v = :x", Map.of("0", 3))).containsExactly(3);
  }

  @Test
  void explicitNullStillBindsNull() {
    final Map<String, Object> positional = new HashMap<>();
    positional.put("0", null);
    database.transaction(() -> database.command("sql", "INSERT INTO T SET k = 10, v = ?", positional));
    final Map<String, Object> named = new HashMap<>();
    named.put("v", null);
    database.transaction(() -> database.command("sql", "INSERT INTO T SET k = 11, v = :v", named));

    try (final ResultSet rs = database.query("sql", "SELECT FROM T WHERE k >= 10 ORDER BY k")) {
      for (int i = 0; i < 2; i++) {
        final Result row = rs.next();
        assertThat(row.hasProperty("v")).isTrue();
        assertThat(row.<Object>getProperty("v")).isNull();
      }
    }
  }

  @Test
  void literalsExtractedByTheStatementCacheAreNotParameters() {
    // a repeated text has its literals turned into generated parameters (#8307): they are bound by the engine, never missing
    for (int i = 0; i < 3; i++)
      assertThat(keysWhere("v = 2 AND k = :k", Map.of("k", 2))).containsExactly(2);
  }

  @Test
  void scriptPlaceholderWithoutItsKeyIsRefused() {
    assertMissing(() -> database.command("sqlscript", "INSERT INTO T SET k = 10, v = 1; INSERT INTO T SET k = 11, v = :v;",
        Map.of("w", 1)), "v");
    assertThat(storedAt(10)).isEmpty();

    final Map<String, Object> named = Map.of("v", 5);
    database.transaction(() -> database.command("sqlscript", "INSERT INTO T SET k = 12, v = :v;", named));
    assertThat(storedAt(12)).containsExactly(5);
  }

  @Test
  void sqlFunctionCalledWithFewerArgumentsBindsTheOmittedOnesToNull() {
    database.command("sql", "DEFINE FUNCTION my.pick \"SELECT ifnull(:b, :a) AS r\" PARAMETERS [a, b] LANGUAGE sql");
    try (final ResultSet rs = database.query("sql", "SELECT `my.pick`(4) AS r")) {
      assertThat(rs.next().<Integer>getProperty("r")).isEqualTo(4);
    }
    try (final ResultSet rs = database.query("sql", "SELECT `my.pick`(4, 5) AS r")) {
      assertThat(rs.next().<Integer>getProperty("r")).isEqualTo(5);
    }
  }

  private void assertMissing(final Runnable call, final String... missing) {
    assertThatThrownBy(call::run).isInstanceOfSatisfying(CommandParameterMissingException.class,
        e -> assertThat(e.getMissingParameters()).containsExactly(missing));
  }

  private List<Integer> storedAt(final int k) {
    final List<Integer> values = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT v FROM T WHERE k = ?", k)) {
      while (rs.hasNext())
        values.add(rs.next().getProperty("v"));
    }
    return values;
  }

  private List<Integer> keysWhere(final String condition, final Map<String, Object> parameters) {
    final List<Integer> keys = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT k FROM T WHERE " + condition + " ORDER BY k", parameters)) {
      while (rs.hasNext())
        keys.add(rs.next().getProperty("k"));
    }
    return keys;
  }
}
