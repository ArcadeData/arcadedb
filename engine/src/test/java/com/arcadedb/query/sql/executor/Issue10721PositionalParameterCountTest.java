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
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandSQLParsingException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * #10721: positional values were never counted against the placeholders of the statement. A surplus value was dropped without a
 * word, so a list spread into two values for one {@code IN ?} answered one row where the caller meant two, and a missing value was
 * bound to {@code null}. Both are refused before the statement runs, with the two counts in the message.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10721PositionalParameterCountTest extends TestHelper {

  private final RID[] rids = new RID[3];

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE V").close();
    database.transaction(() -> {
      for (int k = 0; k < 3; k++)
        rids[k] = database.newVertex("V").set("k", k).save().getIdentity();
    });
  }

  @Test
  void aSurplusValueIsRefused() {
    assertRefused(() -> query("SELECT FROM V WHERE k = ?", 0, 1), 1, 2);
    assertRefused(() -> query("SELECT FROM V WHERE k = ?", 1, 0), 1, 2);
    assertRefused(() -> query("SELECT FROM V WHERE @rid IN ?", rids[0], rids[1]), 1, 2);
    assertRefused(() -> query("SELECT FROM V", 1), 0, 1);
  }

  @Test
  void aMissingValueIsRefused() {
    assertRefused(() -> query("SELECT FROM V WHERE k = ? AND k < ?", 1), 2, 1);
    assertRefused(() -> query("SELECT FROM V WHERE k = ? OR k = ?", 1), 2, 1);
    assertRefused(() -> query("SELECT FROM V WHERE k = ?"), 1, 0);
    // a named placeholder takes a positional value by its position, so it counts as one too
    assertRefused(() -> query("SELECT FROM V WHERE k = :a OR k = :b", 1), 2, 1);
    // a $N does not excuse a missing value of a ? before it, nor a surplus value past the highest position
    assertRefused(() -> query("SELECT FROM V WHERE k = ? OR k = ? OR k = $2"), 2, 0);
    assertRefused(() -> query("SELECT FROM V WHERE k = $2", 0, 2, 1), 0, 3);
  }

  @Test
  void aWriteIsRefusedBeforeItRuns() {
    database.transaction(() -> assertRefused(() -> database.command("sql", "UPDATE V SET hit = true WHERE k = ?", 0, 1).close(), 1, 2));
    assertThat(query("SELECT FROM V WHERE hit = true")).isZero();

    database.transaction(() -> assertRefused(() -> database.command("sql", "UPDATE V SET hit = ? WHERE k = ?", true).close(), 2, 1));
    assertThat(query("SELECT FROM V WHERE hit = true")).isZero();
  }

  @Test
  void aStatementWithLiteralsExtractedByTheCacheCountsOnlyTheCallersPlaceholders() {
    // the literals 'x' and 1 become generated parameters of the cached statement: they take no positional value
    assertThat(query("SELECT FROM V WHERE k = ? AND 'x' = 'x' AND 1 = 1", 1)).isEqualTo(1);
    assertRefused(() -> query("SELECT FROM V WHERE k = ? AND 'x' = 'x' AND 1 = 1", 1, 2), 1, 2);
    // the same shape with other literals is the same cached statement: still one placeholder, whatever ran before it
    assertThat(query("SELECT FROM V WHERE k = ? AND 'y' = 'y' AND 2 = 2", 2)).isEqualTo(1);
    assertRefused(() -> query("SELECT FROM V WHERE k = ? AND 'z' = 'z' AND 3 = 3"), 1, 0);
  }

  @Test
  void anExactBindingStillAnswers() {
    assertThat(query("SELECT FROM V WHERE @rid IN ?", List.of(rids[0], rids[1]))).isEqualTo(2);
    // a Java array is one value too, the list of its elements, on the RID fetch and on a scan
    assertThat(query("SELECT FROM V WHERE @rid IN ?", (Object) new RID[] { rids[0], rids[1] })).isEqualTo(2);
    assertThat(query("SELECT FROM V WHERE k IN ?", (Object) new int[] { 0, 2 })).isEqualTo(2);
    assertThat(query("SELECT FROM V WHERE k IN ?", (Object) new Integer[] { 0, 2 })).isEqualTo(2);
    assertThat(query("SELECT FROM V WHERE k = ? OR k = ?", 1, 2)).isEqualTo(2);
    assertThat(query("SELECT FROM V WHERE k = :a OR k = :b", 1, 2)).isEqualTo(2);
    assertThat(query("SELECT FROM V WHERE k IN (SELECT k FROM V WHERE k > ?) AND k < ?", 0, 2)).isEqualTo(1);
    assertThat(query("SELECT FROM V")).isEqualTo(3);
    // every place a placeholder can stand is counted: a function argument, a LET, a projection, a nested sub-query
    assertThat(query("SELECT FROM V WHERE k = abs(?)", -1)).isEqualTo(1);
    assertRefused(() -> query("SELECT FROM V WHERE k = abs(?)"), 1, 0);
    assertThat(query("SELECT $a FROM V LET $a = ? WHERE k = ?", "x", 1)).isEqualTo(1);
    assertRefused(() -> query("SELECT $a FROM V LET $a = ? WHERE k = ?", "x"), 2, 1);
    assertThat(query("SELECT ? AS p FROM V WHERE k IN (SELECT k FROM V WHERE k IN (SELECT k FROM V WHERE k = ?))", "x", 2)).isEqualTo(1);
    assertRefused(() -> query("SELECT ? AS p FROM V WHERE k IN (SELECT k FROM V WHERE k IN (SELECT k FROM V WHERE k = ?))", "x"), 2, 1);
    // an explicit null is a value
    assertThat(query("SELECT FROM V WHERE k = ? OR k = ?", 1, null)).isEqualTo(1);
    // the Postgres-style $N reads the value at its position, and a lower position may be left unread, as in Postgres
    assertThat(query("SELECT FROM V WHERE k = $2", 0, 2)).isEqualTo(1);
    assertThat(query("SELECT FROM V WHERE k = ? OR k = $3", 1, null, 2)).isEqualTo(2);
    // named values are not counted: a map binds by name
    try (final ResultSet rs = database.query("sql", "SELECT FROM V WHERE k = :a", Map.of("a", 1, "unused", 2))) {
      assertThat(rs.stream().count()).isEqualTo(1);
    }
  }

  @Test
  void aScriptCountsThePlaceholdersOfAllItsStatements() {
    try (final ResultSet rs = database.command("sqlscript", "LET a = SELECT FROM V WHERE k = ?; SELECT FROM V WHERE k = ?;", 1, 2)) {
      assertThat(rs.stream().count()).isEqualTo(1);
    }
    assertRefused(() -> database.command("sqlscript", "LET a = SELECT FROM V WHERE k = ?; SELECT FROM V WHERE k = ?;", 1).close(), 2,
        1);
    assertRefused(() -> database.command("sqlscript", "SELECT FROM V WHERE k = ?;", 1, 2).close(), 1, 2);
  }

  private long query(final String sql, final Object... args) {
    try (final ResultSet rs = database.query("sql", sql, args)) {
      return rs.stream().count();
    }
  }

  private static void assertRefused(final Runnable call, final int placeholders, final int values) {
    assertThatThrownBy(call::run).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining(placeholders + " positional parameter")
        .hasMessageContaining(values + " value");
  }
}
