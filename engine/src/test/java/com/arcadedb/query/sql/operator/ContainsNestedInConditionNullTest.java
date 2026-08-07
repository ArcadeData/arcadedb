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
package com.arcadedb.query.sql.operator;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Regression tests for issue #5837: {@code CONTAINS (nested-condition)} threw a
 * {@code NullPointerException} when the nested condition's {@code evaluate()} returned {@code null}
 * (SQL three-valued UNKNOWN, e.g. an {@code IN} condition whose left side is {@code null}). The same
 * unguarded unboxing pattern existed in {@code CONTAINSALL}/{@code CONTAINSANY}.
 */
class ContainsNestedInConditionNullTest {

  private static void createSchema(final com.arcadedb.database.Database db) {
    db.transaction(() -> {
      db.command("sql", "CREATE DOCUMENT TYPE Product5837 IF NOT EXISTS");
      db.command("sql", "CREATE PROPERTY Product5837.name IF NOT EXISTS STRING");

      db.command("sql", "CREATE DOCUMENT TYPE Supplier5837 IF NOT EXISTS");
      db.command("sql", "CREATE PROPERTY Supplier5837.embedded IF NOT EXISTS EMBEDDED OF Product5837");
      db.command("sql", "CREATE PROPERTY Supplier5837.embedded_list IF NOT EXISTS LIST OF Product5837");
    });
  }

  // Faithful reproduction of the issue's repro: the nested condition references a field
  // ("embedded.name") that does not exist on the iterated item, so it evaluates to null (UNKNOWN).
  @Test
  void containsWithFieldMissingOnNestedItemDoesNotThrow() throws Exception {
    TestHelper.executeInNewDatabase("Contains5837_missingField", db -> {
      createSchema(db);
      db.transaction(() -> db.command("sql",
          "INSERT INTO Supplier5837 SET embedded = {\"@type\":\"Product5837\",\"name\":\"CPU\"}, " +
              "embedded_list = [{\"@type\":\"Product5837\",\"name\":\"CPU\"}]"));

      db.transaction(() -> {
        assertThatCode(() -> {
          try (final ResultSet rs = db.query("sql",
              "SELECT FROM Supplier5837 WHERE embedded_list CONTAINS (embedded.name IN ['CPU', 'motherboard'])")) {
            assertThat(rs.hasNext()).isFalse();
          }
        }).doesNotThrowAnyException();
      });
    });
  }

  @Test
  void containsWithMatchingNestedFieldReturnsTrue() throws Exception {
    TestHelper.executeInNewDatabase("Contains5837_matches", db -> {
      createSchema(db);
      db.transaction(() -> db.command("sql",
          "INSERT INTO Supplier5837 SET embedded_list = [{\"@type\":\"Product5837\",\"name\":\"CPU\"}]"));

      db.transaction(() -> {
        try (final ResultSet rs = db.query("sql",
            "SELECT FROM Supplier5837 WHERE embedded_list CONTAINS (name IN ['CPU', 'motherboard'])")) {
          assertThat(rs.hasNext()).isTrue();
        }
      });
    });
  }

  // A null-producing item earlier in the iteration (UNKNOWN) must not prevent a real match
  // found on a later item.
  @Test
  void containsWithNullItemAmongMatchingItemsStillMatches() throws Exception {
    TestHelper.executeInNewDatabase("Contains5837_nullThenMatch", db -> {
      createSchema(db);
      db.transaction(() -> db.command("sql",
          "INSERT INTO Supplier5837 SET embedded_list = [{\"@type\":\"Product5837\"}, {\"@type\":\"Product5837\",\"name\":\"CPU\"}]"));

      db.transaction(() -> {
        try (final ResultSet rs = db.query("sql",
            "SELECT FROM Supplier5837 WHERE embedded_list CONTAINS (name IN ['CPU', 'motherboard'])")) {
          assertThat(rs.hasNext()).isTrue();
        }
      });
    });
  }

  @Test
  void containsWithNoMatchAndNullItemReturnsFalseNotThrow() throws Exception {
    TestHelper.executeInNewDatabase("Contains5837_nullNoMatch", db -> {
      createSchema(db);
      db.transaction(() -> db.command("sql",
          "INSERT INTO Supplier5837 SET embedded_list = [{\"@type\":\"Product5837\"}, {\"@type\":\"Product5837\",\"name\":\"CPU\"}]"));

      db.transaction(() -> {
        assertThatCode(() -> {
          try (final ResultSet rs = db.query("sql",
              "SELECT FROM Supplier5837 WHERE embedded_list CONTAINS (name IN ['motherboard'])")) {
            assertThat(rs.hasNext()).isFalse();
          }
        }).doesNotThrowAnyException();
      });
    });
  }

  // NOTE: CONTAINSALL (condition) / CONTAINSANY (condition) with a bare, non-OR-wrapped nested
  // condition (e.g. a plain "name IN [...]") cannot currently be exercised from SQL at all: the ANTLR
  // AST builder (SQLASTBuilder#visitContainsAllCondition / #visitContainsAnyCondition) hard-casts the
  // parsed condition to OrBlock, which throws ClassCastException before reaching evaluate() - a
  // separate, pre-existing bug unrelated to the null-unboxing NPE fixed here. The same isTrue() guard
  // was still applied to ContainsAllCondition/ContainsAnyCondition's evaluate() for defense in depth,
  // but no SQL-level regression test could be written for it until that parser bug is fixed too.
}
