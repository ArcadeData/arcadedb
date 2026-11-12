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
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for #11596: an LSM_TREE index on a property declared {@code ARRAY_OF_*} was created, but the
 * comparator had no order for two keys of those types, so the second insert and every lookup failed with "Comparison
 * between type 26 and 26 not supported". The keys are now ordered element by element, as a list is.
 */
class IndexOnPrimitiveArrayPropertyIssue11596Test extends TestHelper {

  @ParameterizedTest
  @ValueSource(strings = { "ARRAY_OF_SHORTS", "ARRAY_OF_INTEGERS", "ARRAY_OF_LONGS", "ARRAY_OF_FLOATS", "ARRAY_OF_DOUBLES" })
  void notUniqueIndexTakesWritesAndAnswersLookups(final String type) {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.k " + type);
    insertRows();
    final List<Integer> withoutIndex = ids("SELECT id FROM T WHERE k = [1, 2] ORDER BY id");
    assertThat(withoutIndex).containsExactly(1, 2);

    database.transaction(() -> database.command("sql", "DELETE FROM T"));
    database.command("sql", "CREATE INDEX ON T (k) NOTUNIQUE");
    insertRows();

    assertThat(ids("SELECT id FROM T WHERE k = [1, 2] ORDER BY id")).isEqualTo(withoutIndex);
    assertThat(ids("SELECT id FROM T WHERE k = [3] ORDER BY id")).containsExactly(3);
    assertThat(ids("SELECT id FROM T WHERE k = [1] ORDER BY id")).isEmpty();
    assertThat(ids("SELECT id FROM T WHERE k = [1, 2, 0] ORDER BY id")).isEmpty();
    assertThat(database.getSchema().getIndexByName("T[k]").countEntries()).isEqualTo(3);
  }

  @Test
  void uniqueIndexRefusesTheSameArrayTwice() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.k ARRAY_OF_DOUBLES");
    database.command("sql", "CREATE INDEX ON T (k) UNIQUE");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET id = 1, k = [1.5, 2.0]");
      database.command("sql", "INSERT INTO T SET id = 2, k = [1.5]");
    });
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO T SET id = 3, k = [1.5, 2.0]")))
        .isInstanceOf(DuplicatedKeyException.class);
    assertThat(ids("SELECT id FROM T WHERE k = [1.5] ORDER BY id")).containsExactly(2);
  }

  @Test
  void indexBuiltOverExistingArrays() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.k ARRAY_OF_FLOATS");
    insertRows();
    database.command("sql", "CREATE INDEX ON T (k) NOTUNIQUE");
    assertThat(ids("SELECT id FROM T WHERE k = [1, 2] ORDER BY id")).containsExactly(1, 2);
  }

  @Test
  void cypherIndexOverVectorsWrittenAsPrimitiveArrays() {
    // A numeric array nested in a parameter row reaches the write as a double[] and is stored as ARRAY_OF_DOUBLES
    final Map<String, Object> params = Map.of("rows", List.of(Map.of("id", 1, "embedding", new double[] { 0.1, 0.2 }),
        Map.of("id", 2, "embedding", new double[] { 0.3, 0.4 }), Map.of("id", 3, "embedding", new double[] { 0.1, 0.2 })));
    database.transaction(() -> database.command("opencypher", "UNWIND $rows AS row CREATE (:Doc {id: row.id, embedding: row.embedding})",
        params));
    database.command("opencypher", "CREATE INDEX FOR (n:Doc) ON (n.embedding)");
    assertThat(ids("SELECT id FROM Doc WHERE embedding = [0.1, 0.2] ORDER BY id")).containsExactly(1, 3);
  }

  private void insertRows() {
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET id = :id, k = :k", Map.of("id", 1, "k", List.of(1, 2)));
      database.command("sql", "INSERT INTO T SET id = :id, k = :k", Map.of("id", 2, "k", List.of(1, 2)));
      database.command("sql", "INSERT INTO T SET id = :id, k = :k", Map.of("id", 3, "k", List.of(3)));
    });
  }

  private List<Integer> ids(final String query) {
    final List<Integer> out = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext())
        out.add(rs.next().<Number>getProperty("id").intValue());
    }
    return out;
  }
}
