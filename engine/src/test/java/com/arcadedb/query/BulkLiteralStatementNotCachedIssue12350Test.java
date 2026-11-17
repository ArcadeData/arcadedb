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
package com.arcadedb.query;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.StringJoiner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12350: a text with more literals than can be extracted into parameters is keyed as written, so only an exact resend can
 * share its cache entry. It was cached anyway, with its whole parse tree: one node chain per literal, 17 to 34 times the text,
 * for each of the 300 entries a cache holds. A bulk write with its data written in pinned gigabytes of heap. Such a text is now
 * parsed for its execution and not cached; a text with fewer literals still is.
 */
class BulkLiteralStatementNotCachedIssue12350Test extends TestHelper {
  /** More literals than {@code LiteralParameterizer.MAX_LITERALS} (4096). */
  private static final int ROWS   = 10;
  private static final int VALUES = 500;

  @Test
  void aSqlInsertWithItsDataWrittenInIsNotCached() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    final StringJoiner rows = new StringJoiner(", ", "[", "]");
    for (int r = 0; r < ROWS; r++)
      rows.add("{\"k\": " + r + ", \"e\": " + numbers(r) + "}");
    final String insert = "INSERT INTO T CONTENT " + rows;

    database.transaction(() -> database.command("sql", insert).close());

    assertThat(count("SELECT count(*) AS n FROM T")).isEqualTo(ROWS);
    assertThat(db().getStatementCache().contains(insert)).as("a text with more literals than can be extracted").isFalse();

    // the control: a text without literals is still cached as written, through the same lookup
    final String small = "SELECT count(*) AS n FROM T";
    assertThat(count(small)).isEqualTo(ROWS);
    assertThat(db().getStatementCache().contains(small)).as("a text the SQL cache keeps").isTrue();
  }

  @Test
  void aCypherUnwindWithItsDataWrittenInIsNotCached() {
    database.command("sql", "CREATE VERTEX TYPE V");
    final StringJoiner rows = new StringJoiner(", ", "[", "]");
    for (int r = 0; r < ROWS; r++)
      rows.add("{k: " + r + ", e: " + numbers(r) + "}");
    final String unwind = "UNWIND " + rows + " AS row CREATE (n:V) SET n = row";

    database.transaction(() -> database.command("opencypher", unwind).close());

    assertThat(count("SELECT count(*) AS n FROM V")).isEqualTo(ROWS);
    assertThat(db().getCypherStatementCache().contains(unwind)).as("a text with more literals than can be extracted").isFalse();

    final String small = "MATCH (n:V) RETURN count(n) AS n";
    try (final ResultSet rs = database.query("opencypher", small)) {
      assertThat(rs.next().<Long>getProperty("n")).isEqualTo((long) ROWS);
    }
    assertThat(db().getCypherStatementCache().contains(small)).as("a text the openCypher cache keeps").isTrue();
  }

  private static String numbers(final int row) {
    final StringJoiner values = new StringJoiner(", ", "[", "]");
    for (int i = 0; i < VALUES; i++)
      values.add(String.valueOf(row * VALUES + i + 0.5));
    return values.toString();
  }

  private long count(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().<Number>getProperty("n").longValue();
    }
  }

  private DatabaseInternal db() {
    return (DatabaseInternal) database;
  }
}
