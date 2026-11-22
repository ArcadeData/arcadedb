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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12973: the Postgres wire protocol speaks PostgreSQL, whose row-limiting clause is {@code OFFSET} - and the engine
 * refused it with a parse error, so every paginating client of that protocol failed. {@code OFFSET} is now the alias of
 * {@code SKIP}, the clause ArcadeDB spells it, and the two answer the same rows.
 */
class Issue12973OffsetClauseTest extends TestHelper {

  @BeforeEach
  void createSchema() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE P");
      database.command("sql", "CREATE PROPERTY P.id INTEGER");
      for (int i = 0; i < 10; i++)
        database.command("sql", "INSERT INTO P SET id = ?", i);
    });
  }

  @Test
  void offsetWithLimitSkipsTheRowsTheClauseAsks() {
    assertThat(ids("SELECT id FROM P ORDER BY id LIMIT 3 OFFSET 4")).containsExactly(4, 5, 6);
    // the same clause in ArcadeDB's own spelling, which the alias must not change
    assertThat(ids("SELECT id FROM P ORDER BY id LIMIT 3 SKIP 4")).containsExactly(4, 5, 6);
  }

  @Test
  void offsetWithoutLimitSkipsTheRowsAndKeepsTheRest() {
    assertThat(ids("SELECT id FROM P ORDER BY id OFFSET 8")).containsExactly(8, 9);
  }

  @Test
  void offsetBeforeLimitIsTheSameClause() {
    assertThat(ids("SELECT id FROM P ORDER BY id OFFSET 4 LIMIT 3")).containsExactly(4, 5, 6);
  }

  @Test
  void offsetAcceptsAParameter() {
    assertThat(ids("SELECT id FROM P ORDER BY id LIMIT 3 OFFSET ?", 4)).containsExactly(4, 5, 6);
  }

  @Test
  void offsetWorksOnAMatchStatement() {
    assertThat(ids("MATCH {type: P, as: p} RETURN p.id AS id ORDER BY id OFFSET 3 LIMIT 2")).containsExactly(3, 4);
  }

  @Test
  void offsetIsStillAnIdentifier() {
    database.transaction(() -> database.command("sql", "CREATE PROPERTY P.offset STRING"));
    database.transaction(() -> database.command("sql", "UPDATE P SET offset = 'x' WHERE id = 1"));

    assertThat(ids("SELECT id FROM P WHERE offset = 'x'")).containsExactly(1);
  }

  /**
   * The {@code FACETS} clause is read before the row-limiting one: a facet option must keep being read as the facet's own
   * option and not as the row-limiting clause.
   */
  @Test
  void theFacetsClauseIsReadBeforeTheRowLimitingOne() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE H");
      database.command("sql", "CREATE PROPERTY H.ts DATETIME");
      database.command("sql", "INSERT INTO H SET ts = '2024-01-01T00:00:00.000'");
      database.command("sql", "INSERT INTO H SET ts = '2024-01-02T00:00:00.000'");
    });

    try (final ResultSet rs = database.command("sql",
        "SELECT FROM H FACETS h: ts HISTOGRAM CALENDAR '1d' TIMEZONE 'UTC'")) {
      assertThat(rs.hasNext()).isTrue();
    }
  }

  private List<Integer> ids(final String query, final Object... params) {
    final List<Integer> ids = new ArrayList<>();
    try (final ResultSet rs = params.length == 0 ? database.command("sql", query) : database.command("sql", query, params)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        ids.add((Integer) row.getProperty("id"));
      }
    }
    return ids;
  }
}
