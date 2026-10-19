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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #10194, first point: {@code UPDATE ... SET} assigned its items left to right on the record being updated, so a later item saw
 * the new value of an earlier one and {@code SET a = b, b = a} left both columns equal instead of swapping them. In SQL every
 * right-hand side reads the row as it was before the statement.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10194UpdateSetReadsTheRowAsItWasTest extends TestHelper {

  @BeforeEach
  void fill() {
    database.command("sql", "CREATE DOCUMENT TYPE T").close();
    database.command("sql", "CREATE PROPERTY T.id LONG").close();
    database.command("sql", "CREATE PROPERTY T.a INTEGER").close();
    database.command("sql", "CREATE PROPERTY T.b INTEGER").close();
    database.transaction(() -> database.command("sql", "INSERT INTO T SET id = 1, a = 1, b = 2").close());
  }

  private void update(final String statement) {
    database.transaction(() -> database.command("sql", statement).close());
  }

  private void assertRow(final int a, final int b) {
    try (final ResultSet rs = database.query("sql", "SELECT a, b FROM T WHERE id = 1")) {
      final Result row = rs.next();
      assertThat(row.<Number>getProperty("a").intValue()).as("a").isEqualTo(a);
      assertThat(row.<Number>getProperty("b").intValue()).as("b").isEqualTo(b);
    }
  }

  @Test
  void swapExchangesTheValues() {
    update("UPDATE T SET a = b, b = a");

    assertRow(2, 1);
  }

  @Test
  void aLaterItemReadsTheOldValueOfAnEarlierOne() {
    update("UPDATE T SET b = 7, a = b");

    assertRow(2, 7);
  }

  @Test
  void compoundOperatorsReadTheOldRow() {
    update("UPDATE T SET a += b, b += a");

    assertRow(3, 3);
  }

  @Test
  void twoItemsOnTheSamePropertyStillApplyInOrder() {
    // only the right-hand sides are read from the old row: a compound operator extends the value its own property holds by then
    update("UPDATE T SET a = 1, a += 2");

    assertRow(3, 2);
  }

  @Test
  void aRecordBeingCreatedHasNoRowToRead() {
    // a record that is being created has no row as it was: its items are evaluated against nothing, as before this change
    // (INSERT and CREATE EDGE build their record from the evaluated items), so the snapshot is for existing records only
    database.command("sql", "CREATE VERTEX TYPE V").close();
    database.command("sql", "CREATE EDGE TYPE E").close();
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX V SET n = 1").close();
      database.command("sql", "CREATE VERTEX V SET n = 2").close();
    });

    update("CREATE EDGE E FROM (SELECT FROM V WHERE n = 1) TO (SELECT FROM V WHERE n = 2) SET w = 5, z = w");

    try (final ResultSet rs = database.query("sql", "SELECT w, z FROM E")) {
      final Result edge = rs.next();
      assertThat(edge.<Number>getProperty("w").intValue()).isEqualTo(5);
      assertThat(edge.<Object>getProperty("z")).isNull();
    }
  }

  @Test
  void expressionsReadTheOldRow() {
    update("UPDATE T SET a = a + b, b = a * 10");

    assertRow(3, 10);
  }

  @Test
  void returnAfterShowsTheSwappedRow() {
    final int[] seen = new int[2];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "UPDATE T SET a = b, b = a RETURN AFTER")) {
        final Result row = rs.next();
        seen[0] = row.<Number>getProperty("a").intValue();
        seen[1] = row.<Number>getProperty("b").intValue();
      }
    });

    assertThat(seen).containsExactly(2, 1);
    assertRow(2, 1);
  }

  @Test
  void returnBeforeShowsTheOriginalRow() {
    final int[] seen = new int[2];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "UPDATE T SET a = b, b = a RETURN BEFORE")) {
        final Result row = rs.next();
        seen[0] = row.<Number>getProperty("a").intValue();
        seen[1] = row.<Number>getProperty("b").intValue();
      }
    });

    assertThat(seen).containsExactly(1, 2);
  }

  @Test
  void swapAppliesToEveryRowOfTheStatement() {
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET id = 2, a = 10, b = 20").close();
      database.command("sql", "INSERT INTO T SET id = 3, a = 30, b = 40").close();
    });

    update("UPDATE T SET a = b, b = a");

    try (final ResultSet rs = database.query("sql", "SELECT id, a, b FROM T ORDER BY id")) {
      final int[][] expected = { { 1, 2, 1 }, { 2, 20, 10 }, { 3, 40, 30 } };
      for (final int[] e : expected) {
        final Result row = rs.next();
        assertThat(row.<Number>getProperty("id").intValue()).isEqualTo(e[0]);
        assertThat(row.<Number>getProperty("a").intValue()).isEqualTo(e[1]);
        assertThat(row.<Number>getProperty("b").intValue()).isEqualTo(e[2]);
      }
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void swapKeepsTheIndexesInSync() {
    database.command("sql", "CREATE INDEX ON T (a) NOTUNIQUE").close();
    database.command("sql", "CREATE INDEX ON T (b) NOTUNIQUE").close();

    update("UPDATE T SET a = b, b = a");

    try (final ResultSet rs = database.query("sql", "SELECT id FROM T WHERE a = 2 AND b = 1")) {
      assertThat(rs.hasNext()).isTrue();
    }
    try (final ResultSet rs = database.query("sql", "SELECT id FROM T WHERE a = 1")) {
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void nestedAssignmentsReadTheOldRow() {
    database.transaction(() -> database.command("sql", "UPDATE T SET m = {'x': 1}").close());

    update("UPDATE T SET m.x = b, b = 99");

    try (final ResultSet rs = database.query("sql", "SELECT m.x AS x, b FROM T WHERE id = 1")) {
      final Result row = rs.next();
      assertThat(row.<Number>getProperty("x").intValue()).isEqualTo(2);
      assertThat(row.<Number>getProperty("b").intValue()).isEqualTo(99);
    }
  }

  @Test
  void aSingleItemBehavesAsBefore() {
    update("UPDATE T SET a = a + 1");

    assertRow(2, 2);
  }

  @Test
  void upsertSwapsToo() {
    database.command("sql", "CREATE INDEX ON T (id) UNIQUE").close();

    update("UPDATE T SET a = b, b = a UPSERT WHERE id = 1");

    assertRow(2, 1);
  }
}
