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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.DeleteStatement;
import com.arcadedb.query.sql.parser.Statement;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11392: PostgreSQL's trailing {@code RETURNING projection} on INSERT, UPDATE and DELETE, what pgjdbc appends for
 * {@code RETURN_GENERATED_KEYS} ({@code RETURNING *} or {@code RETURNING "col"}) and what Hibernate IDENTITY, Spring
 * {@code KeyHolder} and jOOQ rely on. The records as the statement left them for INSERT and UPDATE, as they were before it for
 * DELETE; {@code RETURN} keeps working, and the two cannot be combined.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11392SqlReturningTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> database.getSchema().createDocumentType("Ret"));
  }

  @Test
  void insertReturningStarAnswersTheInsertedRecords() {
    database.transaction(() -> {
      final List<Result> rows = rows(database.command("sql", "INSERT INTO Ret SET a = 1, b = 'x' RETURNING *"));
      assertThat(rows).hasSize(1);
      assertThat(rows.getFirst().<Integer>getProperty("a")).isEqualTo(1);
      assertThat(rows.getFirst().<String>getProperty("b")).isEqualTo("x");
    });
  }

  @Test
  void insertReturningAColumnAnswersItForEveryRow() {
    database.transaction(() -> {
      final List<Result> rows = rows(database.command("sql", "INSERT INTO Ret (a, b) VALUES (1, 'x'), (2, 'y') RETURNING a, @rid"));
      assertThat(rows).extracting(r -> r.<Integer>getProperty("a")).containsExactly(1, 2);
      assertThat(rows).allSatisfy(r -> assertThat(r.<Object>getProperty("@rid")).isNotNull());
    });
  }

  @Test
  void updateReturningAnswersTheRecordsAsTheUpdateLeftThem() {
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Ret (a, b) VALUES (1, 'x'), (2, 'y')").close();
      final List<Result> rows = rows(database.command("sql", "UPDATE Ret SET b = 'z' WHERE a = 1 RETURNING a, b"));
      assertThat(rows).hasSize(1);
      assertThat(rows.getFirst().<String>getProperty("b")).isEqualTo("z");
    });
  }

  @Test
  void deleteReturningAnswersTheRecordsAsTheyWereBeforeTheDelete() {
    database.transaction(() -> database.command("sql", "INSERT INTO Ret (a, b) VALUES (1, 'x'), (2, 'y')").close());

    database.transaction(() -> {
      final List<Result> rows = rows(database.command("sql", "DELETE FROM Ret WHERE a = 2 RETURNING a, b"));
      assertThat(rows).hasSize(1);
      assertThat(rows.getFirst().<Integer>getProperty("a")).isEqualTo(2);
      assertThat(rows.getFirst().<String>getProperty("b")).isEqualTo("y");
    });

    assertThat(database.countType("Ret", false)).isEqualTo(1);
  }

  @Test
  void returnAndReturningCannotBeCombined() {
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO Ret SET a = 1 RETURN @this RETURNING *"))
        .isInstanceOf(CommandSQLParsingException.class);
    assertThatThrownBy(() -> database.command("sql", "UPDATE Ret SET a = 1 RETURN AFTER RETURNING *"))
        .isInstanceOf(CommandSQLParsingException.class);
    assertThatThrownBy(() -> database.command("sql", "DELETE FROM Ret RETURN BEFORE RETURNING *"))
        .isInstanceOf(CommandSQLParsingException.class);
  }

  /** The keyword stays a name: a property called {@code returning} is written and read as before. */
  @Test
  void returningIsStillAPropertyName() {
    database.transaction(() -> database.command("sql", "INSERT INTO Ret SET returning = 7").close());

    try (final ResultSet rs = database.query("sql", "SELECT returning FROM Ret WHERE returning = 7")) {
      assertThat(rs.next().<Integer>getProperty("returning")).isEqualTo(7);
    }
  }

  /** The statement prints back in a form that parses to the same statement, as every cached plan key relies on. */
  @Test
  void aDeleteReturningPrintsBackToItself() {
    final Statement parsed = parse("DELETE FROM Ret WHERE a = 2 RETURNING a, b");
    assertThat(parsed).isInstanceOf(DeleteStatement.class);

    final StringBuilder printed = new StringBuilder();
    parsed.toString(new HashMap<>(), printed);
    assertThat(printed.toString()).contains("RETURNING").doesNotContain("RETURN BEFORE");
    assertThat(parse(printed.toString())).isEqualTo(parsed);
  }

  private Statement parse(final String sql) {
    return ((DatabaseInternal) database).getStatementCache().get(sql);
  }

  private static List<Result> rows(final ResultSet resultSet) {
    final List<Result> rows = new ArrayList<>();
    try (resultSet) {
      while (resultSet.hasNext())
        rows.add(resultSet.next());
    }
    return rows;
  }
}
