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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.schema.MaterializedViewQueryClassifier;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11570: the SQL grammar accepted {@code *} only as the first projection item, so {@code SELECT name, * FROM X} was a
 * syntax error while {@code SELECT *, name FROM X} worked. {@code *} is now accepted at any position of every projection
 * (SELECT, INSERT/UPDATE RETURN, RETURNING). The builder places it first, which is how {@code Projection.toString} has always
 * rendered it, so a statement and its rendering mean the same thing: an explicit item wins over a same-named column of the
 * record wherever the {@code *} is written.
 */
class Issue11570ProjectionStarAfterItemTest extends TestHelper {

  @BeforeEach
  void createData() {
    database.command("sql", "CREATE DOCUMENT TYPE P");
    database.transaction(() -> database.command("sql", "INSERT INTO P SET name = 'a', secret = 's', n = 1"));
  }

  @Test
  void starAfterAnItemAnswersTheSameRowAsStarFirst() {
    final Result after = single("SELECT name, * FROM P");
    final Result first = single("SELECT *, name FROM P");
    assertThat(after.getPropertyNames()).containsExactlyInAnyOrderElementsOf(first.getPropertyNames());
    assertThat(after.<String>getProperty("name")).isEqualTo("a");
    assertThat(after.<String>getProperty("secret")).isEqualTo("s");
    assertThat(after.<Integer>getProperty("n")).isEqualTo(1);
  }

  @Test
  void starBetweenItems() {
    final Result row = single("SELECT n + 1 AS m, *, name AS alias FROM P");
    assertThat(row.<Integer>getProperty("m")).isEqualTo(2);
    assertThat(row.<String>getProperty("alias")).isEqualTo("a");
    assertThat(row.<String>getProperty("secret")).isEqualTo("s");
  }

  @Test
  void anExplicitItemWinsOverTheStarWhereverTheStarIsWritten() {
    // the same answer as SELECT *, 'x' AS name: the record's own name must not overwrite the explicit item
    assertThat(single("SELECT 'x' AS name, * FROM P").<String>getProperty("name")).isEqualTo("x");
    assertThat(single("SELECT *, 'x' AS name FROM P").<String>getProperty("name")).isEqualTo("x");
  }

  @Test
  void theRenderedStatementParsesBackToTheSameMeaning() {
    final SelectStatement stmt = (SelectStatement) ((DatabaseInternal) database).getStatementCache().get("SELECT 'x' AS name, * FROM P");
    final String rendered = stmt.toString();
    assertThat(rendered).startsWith("SELECT *, ");
    assertThat(single(rendered).<String>getProperty("name")).isEqualTo("x");
  }

  @Test
  void anExclusionBeforeTheStarStillExcludes() {
    final Result row = single("SELECT !secret, * FROM P");
    assertThat(row.getPropertyNames()).doesNotContain("secret");
    assertThat(row.<String>getProperty("secret")).isNull();
    assertThat(row.<String>getProperty("name")).isEqualTo("a");
  }

  @Test
  void distinctWithTheStarAfterAnItem() {
    final Result row = single("SELECT DISTINCT name, * FROM P");
    assertThat(row.<String>getProperty("name")).isEqualTo("a");
    assertThat(row.<String>getProperty("secret")).isEqualTo("s");
  }

  @Test
  void insertReturnAndReturningAcceptTheStarAfterAnItem() {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "INSERT INTO P SET name = 'b', n = 2 RETURN n + 10 AS m, *")) {
        final Result row = rs.next();
        assertThat(row.<Integer>getProperty("m")).isEqualTo(12);
        assertThat(row.<String>getProperty("name")).isEqualTo("b");
      }
      try (final ResultSet rs = database.command("sql", "INSERT INTO P SET name = 'c', n = 3 RETURNING n + 10 AS m, *")) {
        final Result row = rs.next();
        assertThat(row.<Integer>getProperty("m")).isEqualTo(13);
        assertThat(row.<String>getProperty("name")).isEqualTo("c");
      }
    });
  }

  @Test
  void updateReturnAndReturningAcceptTheStarAfterAnItem() {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "UPDATE P SET n = 5 RETURN AFTER n + 10 AS m, * WHERE name = 'a'")) {
        final Result row = rs.next();
        assertThat(row.<Integer>getProperty("m")).isEqualTo(15);
        assertThat(row.<String>getProperty("secret")).isEqualTo("s");
      }
      try (final ResultSet rs = database.command("sql", "UPDATE P SET n = 6 WHERE name = 'a' RETURNING n + 10 AS m, *")) {
        final Result row = rs.next();
        assertThat(row.<Integer>getProperty("m")).isEqualTo(16);
        assertThat(row.<String>getProperty("secret")).isEqualTo("s");
      }
    });
  }

  @Test
  void deleteReturningAcceptsTheStarAfterAnItem() {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "DELETE FROM P WHERE name = 'a' RETURNING n + 10 AS m, *")) {
        final Result row = rs.next();
        assertThat(row.<Integer>getProperty("m")).isEqualTo(11);
        assertThat(row.<String>getProperty("secret")).isEqualTo("s");
      }
    });
  }

  /**
   * The classifier entry {@code SELECT name, * FROM Linked} of {@code MaterializedViewPerRecordClassifierTest} used to be
   * rejected because it did not parse; it is now simple, so its rejection comes from the reserved-name guard, and the same
   * shape over a type without the reserved property is accepted.
   */
  @Test
  void thePerRecordClassifierReachesTheReservedNameGuardForAStarAfterAnItem() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Linked").createProperty("_sourceRID", Type.LINK);
      database.getSchema().createDocumentType("Other");
    });
    assertThat(MaterializedViewQueryClassifier.isSimple("SELECT name, * FROM Linked", database)).isTrue();
    assertThat(MaterializedViewQueryClassifier.isPerRecord("SELECT name, * FROM Linked", database)).isFalse();
    assertThat(MaterializedViewQueryClassifier.isPerRecord("SELECT name, * FROM Other", database)).isTrue();
  }

  private Result single(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      assertThat(rs.hasNext()).as(sql).isTrue();
      return rs.next();
    }
  }
}
