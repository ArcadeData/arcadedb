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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12792: a whole-value index on a LIST or MAP property answers a lookup key that is not a collection (a map for a MAP
 * index) with no rows, as the same predicate does without the index, instead of a ClassCastException in the key comparison.
 * Type {@code P} carries the indexes, type {@code Q} holds the same row without them.
 */
class Issue12792CollectionIndexScalarKeyTest extends TestHelper {

  @BeforeEach
  void setUp() {
    database.transaction(() -> {
      for (final String type : List.of("P", "Q")) {
        database.command("sql", "CREATE VERTEX TYPE " + type);
        database.command("sql", "CREATE PROPERTY " + type + ".tags LIST OF STRING");
        database.command("sql", "CREATE PROPERTY " + type + ".li LIST OF INTEGER");
        database.command("sql", "CREATE PROPERTY " + type + ".l LIST");
        database.command("sql", "CREATE PROPERTY " + type + ".m MAP");
        database.command("sql", "INSERT INTO " + type + " SET tags = ['a', 'b'], li = [1, 2], l = [1, 'x'], m = {'a': 1}");
      }
      for (final String property : List.of("tags", "li", "l", "m"))
        database.command("sql", "CREATE INDEX ON P (" + property + ") NOTUNIQUE");
    });
  }

  @ParameterizedTest
  @ValueSource(strings = { //
      "tags = 5", "tags = true", "tags = date('2020-01-01', 'yyyy-MM-dd')", "tags IN [5, 6]", "tags > 5",
      "tags BETWEEN 1 AND 9", //
      "li = 5", "li = true", "li IN [5, 6]", "li > 5", "li < 5", "li BETWEEN 1 AND 9", "li = date('2020-01-01', 'yyyy-MM-dd')", //
      "l = 5", "l = true", "l IN [5, 6]", "l > 5", "l BETWEEN 1 AND 9", //
      "m = 5", "m = 'a'", "m IN [5, 'a']", "m > 5", "m BETWEEN 1 AND 9", //
      "li = [1, 2]", "li IN [[1, 2]]", "tags = ['a', 'b']", "m = {'a': 1}" })
  void selectAnswersAsWithoutTheIndex(final String where) {
    assertThat(sqlCount("SELECT FROM P WHERE " + where)).as(where).isEqualTo(sqlCount("SELECT FROM Q WHERE " + where));
  }

  @Test
  void wholeListLookupStillUsesTheIndex() {
    try (final ResultSet rs = database.query("sql", "SELECT FROM P WHERE li = [1, 2]")) {
      assertThat(rs.stream().count()).isEqualTo(1);
      assertThat(rs.getExecutionPlan().get().prettyPrint(0, 2)).contains("FETCH FROM INDEX P[li]");
    }
  }

  @Test
  void deleteAndUpdateMatchNothing() {
    database.transaction(() -> {
      for (final String type : List.of("P", "Q")) {
        assertThat(count(database.command("sql", "DELETE FROM " + type + " WHERE tags = 5"))).isEqualTo(0L);
        assertThat(count(database.command("sql", "DELETE FROM " + type + " WHERE m = 'a'"))).isEqualTo(0L);
        assertThat(count(database.command("sql", "UPDATE " + type + " SET x = 1 WHERE li = 5"))).isEqualTo(0L);
      }
    });
    assertThat(sqlCount("SELECT FROM P")).isEqualTo(1);
  }

  @Test
  void cypherMatchAndMerge() {
    for (final Object p : List.of(7, true, "a")) {
      final long indexed = database.query("opencypher", "MATCH (n:P) WHERE n.tags = $p RETURN n", Map.of("p", p)).stream().count();
      final long scanned = database.query("opencypher", "MATCH (n:Q) WHERE n.tags = $p RETURN n", Map.of("p", p)).stream().count();
      assertThat(indexed).as("p = %s", p).isEqualTo(scanned).isZero();
    }

    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", "MERGE (n:P {tags: 5}) RETURN n")) {
        assertThat(rs.stream().count()).isEqualTo(1);
      }
      try (final ResultSet rs = database.command("opencypher", "MERGE (n:P {l: 5}) RETURN n")) {
        assertThat(rs.stream().count()).isEqualTo(1);
      }
    });
    assertThat(sqlCount("SELECT FROM P")).isEqualTo(3);
  }

  private long sqlCount(final String query) {
    try (final ResultSet rs = database.query("sql", query)) {
      return rs.stream().count();
    }
  }

  private static long count(final ResultSet rs) {
    final List<Result> rows = new ArrayList<>();
    rs.stream().forEach(rows::add);
    rs.close();
    return rows.isEmpty() ? 0 : rows.getFirst().<Number>getProperty("count").longValue();
  }
}
