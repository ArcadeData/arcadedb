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
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12844: the AST builder cut the flat list of VALUES expressions into rows of "number of fields" values, so the
 * parentheses played no part: {@code INSERT INTO V (a) VALUES (1, 2)} inserted two records.
 */
class Issue12844InsertValuesRowsTest extends TestHelper {
  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE V");
  }

  @Test
  void aRowWithMoreValuesThanFieldsIsRefusedAndInsertsNothing() {
    for (final String sql : new String[] { "INSERT INTO V (a) VALUES (1, 2)", "INSERT INTO V (a, b) VALUES (1, 2, 3, 4)",
        "INSERT INTO V (a, b) VALUES (10, 20), (30, 40, 50, 60)", "INSERT INTO V (a, b) VALUES (3, 'x', 'y')",
        "CREATE VERTEX V (a) VALUES (7, 8)", "CREATE VERTEX V (a, b) VALUES (7, 8, 9, 10)" })
      assertThatThrownBy(() -> database.transaction(() -> database.command("sql", sql).close())).as(sql)
          .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("VALUES");

    assertThat(count()).isZero();
  }

  /** INSERT and CREATE VERTEX refuse the same mistake with the same message: the row, what it holds, what was expected. */
  @Test
  void theRefusalNamesTheRowAndIsNotWrappedByCreateVertex() {
    for (final String sql : new String[] { "INSERT INTO V (a, b) VALUES (1, 2), (3, 4, 5)",
        "CREATE VERTEX V (a, b) VALUES (1, 2), (3, 4, 5)" })
      assertThatThrownBy(() -> database.transaction(() -> database.command("sql", sql).close())).as(sql)
          .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("The row 2 of VALUES holds 3 values for 2 fields")
          .hasMessageNotContaining("Failed to build");

    assertThat(count()).isZero();
  }

  @Test
  void aRowWithFewerValuesThanFieldsIsRefused() {
    for (final String sql : new String[] { "INSERT INTO V (a, b) VALUES (1)", "INSERT INTO V (a, b) VALUES (1, 2), (3)",
        "CREATE VERTEX V (a, b) VALUES (7)" })
      assertThatThrownBy(() -> database.transaction(() -> database.command("sql", sql).close())).as(sql)
          .isInstanceOf(CommandSQLParsingException.class);

    assertThat(count()).isZero();
  }

  /** The first identifier of a column list is a column, not the type: the statement names no type at all. */
  @Test
  void aCreateVertexWithoutATypeDoesNotTakeAColumnForTheType() {
    database.command("sql", "CREATE VERTEX TYPE A");

    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "CREATE VERTEX (A, b) VALUES (1, 2)").close()))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("Missing target");

    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM A")) {
      assertThat(rs.next().<Long>getProperty("c")).isZero();
    }
  }

  @Test
  void wellFormedRowsStillInsertOneRecordEach() {
    database.transaction(() -> {
      database.command("sql", "INSERT INTO V (a, b) VALUES (1, 'x'), (2, 'y'), (3, (1 + 2))").close();
      database.command("sql", "CREATE VERTEX V (a, b) VALUES (4, 'z'), (5, 'w')").close();
      database.command("sql", "INSERT INTO V (a) VALUES (6)").close();
    });

    assertThat(count()).isEqualTo(6);
    try (final ResultSet rs = database.query("sql", "SELECT b FROM V WHERE a = 3")) {
      assertThat((Object) rs.next().getProperty("b")).isEqualTo(3);
    }
  }

  private long count() {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM V")) {
      return rs.next().<Long>getProperty("c");
    }
  }
}
