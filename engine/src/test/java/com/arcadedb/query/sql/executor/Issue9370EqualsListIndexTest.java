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
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9370: {@code WHERE k = <list>} matched nothing on a scan but the rows of every element through an index, so adding an
 * index changed the answer and {@code =} and {@code <>} were not complements.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9370EqualsListIndexTest extends TestHelper {
  private static final String[][] TYPES = { { "Plain", "" }, { "Hash", "UNIQUE_HASH" }, { "Sorted", "UNIQUE" }, { "Multi", "NOTUNIQUE" } };

  @Override
  protected void beginTest() {
    for (final String[] t : TYPES) {
      database.command("sql", "CREATE DOCUMENT TYPE " + t[0]);
      database.command("sql", "CREATE PROPERTY " + t[0] + ".k INTEGER");
      if (!t[1].isEmpty())
        database.command("sql", "CREATE INDEX ON " + t[0] + " (k) " + t[1]);
      database.transaction(() -> {
        for (int i = 1; i <= 5; i++)
          database.command("sql", "INSERT INTO " + t[0] + " SET k = " + i).close();
      });
    }
  }

  private List<Integer> rows(final String sql, final Map<String, Object> params) {
    final List<Integer> out = new ArrayList<>();
    try (final ResultSet rs = params == null ? database.query("sql", sql) : database.query("sql", sql, params)) {
      while (rs.hasNext())
        out.add(rs.next().<Integer>getProperty("k"));
    }
    return out;
  }

  @Test
  void equalsListMatchesNothingWithOrWithoutIndex() {
    for (final String[] t : TYPES) {
      assertThat(rows("SELECT k FROM " + t[0] + " WHERE k = :v", Map.of("v", List.of(1, 2)))).as(t[0] + " bound list").isEmpty();
      assertThat(rows("SELECT k FROM " + t[0] + " WHERE k = :v", Map.of("v", List.of(1)))).as(t[0] + " one-element list").isEmpty();
      assertThat(rows("SELECT k FROM " + t[0] + " WHERE k = [1, 2]", null)).as(t[0] + " literal list").isEmpty();
    }
  }

  @Test
  void compositeKeyExpandsOnlyTheInSlot() {
    database.command("sql", "CREATE DOCUMENT TYPE Pair");
    database.command("sql", "CREATE PROPERTY Pair.a INTEGER");
    database.command("sql", "CREATE PROPERTY Pair.b INTEGER");
    database.command("sql", "CREATE INDEX ON Pair (a, b) NOTUNIQUE");
    database.transaction(() -> {
      for (int a = 1; a <= 3; a++)
        for (int b = 1; b <= 3; b++)
          database.command("sql", "INSERT INTO Pair SET a = " + a + ", b = " + b).close();
    });
    assertThat(count("SELECT FROM Pair WHERE a = 1 AND b IN :v", Map.of("v", List.of(1, 2)))).as("IN in the last slot").isEqualTo(2);
    assertThat(count("SELECT FROM Pair WHERE a IN :v AND b = 1", Map.of("v", List.of(1, 2)))).as("IN in the first slot").isEqualTo(2);
    assertThat(count("SELECT FROM Pair WHERE a = 1 AND b = :v", Map.of("v", List.of(1, 2)))).as("= with a list in the last slot").isZero();
    assertThat(count("SELECT FROM Pair WHERE a = :v AND b = 1", Map.of("v", List.of(1, 2)))).as("= with a list in the first slot").isZero();
    assertThat(count("SELECT FROM Pair WHERE a = 2 AND b = 3", Map.of())).as("scalars").isEqualTo(1);
  }

  private int count(final String sql, final Map<String, Object> params) {
    int count = 0;
    try (final ResultSet rs = database.query("sql", sql, params)) {
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
    }
    return count;
  }

  @Test
  void inStillExpandsAndScalarEqualsStillWorks() {
    for (final String[] t : TYPES) {
      assertThat(rows("SELECT k FROM " + t[0] + " WHERE k IN :v ORDER BY k", Map.of("v", List.of(1, 2)))).as(t[0] + " IN")
          .containsExactly(1, 2);
      assertThat(rows("SELECT k FROM " + t[0] + " WHERE k = :v", Map.of("v", 3))).as(t[0] + " scalar").containsExactly(3);
    }
  }
}
