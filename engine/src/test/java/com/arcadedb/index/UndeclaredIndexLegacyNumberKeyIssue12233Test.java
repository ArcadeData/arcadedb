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
import com.arcadedb.database.RID;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for #12233, the number counterpart of #12160. Since #9132 an index on a property nothing declares keys a
 * floating point number by its canonical text: {@code 1.0} is the key {@code "1"}, {@code -0.0} is {@code "0"} and {@code 1e300}
 * is its full decimal expansion. 26.9.1 keyed the same numbers by {@code Double.toString()}: {@code "1.0"}, {@code "-0.0"},
 * {@code "1.0E300"}. A database it wrote keeps those entries: a seek missed them, MERGE created a second node, a uniqueness
 * constraint let a copy in and CHECK DATABASE reported them.
 * <p>
 * The entries are written here as 26.9.1 wrote them: the canonical entry of each record is replaced with the
 * {@code Double.toString()} text of its number.
 */
class UndeclaredIndexLegacyNumberKeyIssue12233Test extends TestHelper {

  private static final List<Double> OLD_SPELLED = List.of(1.0, -0.0, 1e300);

  @Test
  void legacyNumberEntriesAreFoundMergedAndRemoved() {
    database.command("opencypher", "CREATE INDEX FOR (n:U) ON (n.v)");
    database.transaction(() -> {
      for (int i = 0; i < OLD_SPELLED.size(); i++)
        database.command("opencypher", "CREATE (:U {id: $id, v: $v})", Map.of("id", i, "v", OLD_SPELLED.get(i)));
      database.command("opencypher", "CREATE (:U {id: 3, v: 2.5})");
      // a STRING that spells the old key of the number of id 0: the same entry text, another value
      database.command("opencypher", "CREATE (:U {id: 4, v: '1.0'})");
    });
    writeLegacyEntries("U[v]", "U");
    assertThat(checkDatabaseErrors()).isZero();

    final String seek = "MATCH (n:U) WHERE n.v = $p RETURN n.id AS id ORDER BY id";
    for (int i = 0; i < OLD_SPELLED.size(); i++)
      assertThat(ids(seek, Map.of("p", OLD_SPELLED.get(i)))).as("seek %s", OLD_SPELLED.get(i)).containsExactly(i);
    assertThat(ids(seek, Map.of("p", 2.5))).containsExactly(3);
    assertThat(ids(seek, Map.of("p", "1.0"))).containsExactly(4);
    assertThat(ids("SELECT id FROM U WHERE v = :p", Map.of("p", 1.0), "sql")).containsExactly(0);

    // MERGE finds the node 26.9.1 indexed rather than creating a second one
    database.transaction(() -> database.command("opencypher", "MERGE (n:U {v: $v}) ON CREATE SET n.id = 100", Map.of("v", 1e300)));
    assertThat(ids("MATCH (n:U) RETURN n.id AS id ORDER BY id", Map.of())).containsExactly(0, 1, 2, 3, 4);

    // an update and a delete take the old entry with them
    database.transaction(() -> database.command("opencypher", "MATCH (n:U {id: 1}) SET n.v = 9.5"));
    database.transaction(() -> database.command("opencypher", "MATCH (n:U {id: 0}) DELETE n"));
    assertThat(ids(seek, Map.of("p", -0.0))).isEmpty();
    assertThat(ids(seek, Map.of("p", 9.5))).containsExactly(1);
    assertThat(ids(seek, Map.of("p", 1.0))).isEmpty();
    assertThat(ids(seek, Map.of("p", "1.0"))).containsExactly(4);
    assertThat(database.getSchema().getIndexByName("U[v]").countEntries()).isEqualTo(4L);
    assertThat(checkDatabaseErrors()).isZero();
  }

  @Test
  void uniqueConstraintRefusesACopyOfALegacyNumber() {
    database.command("opencypher", "CREATE CONSTRAINT FOR (n:K) REQUIRE n.v IS UNIQUE");
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:K {id: 0, v: 1.0})");
      database.command("opencypher", "CREATE (:K {id: 1, v: '-0.0'})");
    });
    writeLegacyEntries("K[v]", "K");
    assertThat(checkDatabaseErrors()).isZero();

    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher", "CREATE (:K {id: 2, v: 1.0})")))
        .isInstanceOf(DuplicatedKeyException.class);
    // the STRING '-0.0' and the number -0.0 were one key in 26.9.1 and are two keys now: the number is no copy of the string
    database.transaction(() -> database.command("opencypher", "CREATE (:K {id: 3, v: $v})", Map.of("v", -0.0)));

    // once the holder of the old key is gone, its value can be written again
    database.transaction(() -> database.command("opencypher", "MATCH (n:K {id: 0}) DELETE n"));
    database.transaction(() -> database.command("opencypher", "CREATE (:K {id: 4, v: 1.0})"));
    assertThat(ids("MATCH (n:K) RETURN n.id AS id ORDER BY id", Map.of())).containsExactly(1, 3, 4);
    assertThat(checkDatabaseErrors()).isZero();
  }

  /** After REBUILD INDEX every entry is under the canonical key: 1.0 and 1 are one key again. */
  @Test
  void rebuildKeysTheLegacyNumbersCanonically() {
    database.command("opencypher", "CREATE INDEX FOR (n:U) ON (n.v)");
    database.transaction(() -> database.command("opencypher", "CREATE (:U {id: 0, v: 1.0})"));
    writeLegacyEntries("U[v]", "U");

    final String seek = "MATCH (n:U) WHERE n.v = $p RETURN n.id AS id ORDER BY id";
    assertThat(ids(seek, Map.of("p", 1.0))).containsExactly(0);
    database.command("sql", "REBUILD INDEX `U[v]`");
    assertThat(ids(seek, Map.of("p", 1.0))).containsExactly(0);
    assertThat(ids(seek, Map.of("p", 1L))).containsExactly(0);
    assertThat(checkDatabaseErrors()).isZero();
  }

  /**
   * Replaces, in the bucket indexes of {@code indexName}, the entry of each record holding a double with the one 26.9.1 wrote
   * for it: the number keyed by {@code Double.toString()}.
   */
  private void writeLegacyEntries(final String indexName, final String typeName) {
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(indexName);
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid, v FROM " + typeName)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          if (!(row.getProperty("v") instanceof Double number))
            continue;
          final RID rid = row.getProperty("rid");
          for (final IndexInternal bucketIndex : typeIndex.getIndexesOnBuckets())
            if (bucketIndex.getAssociatedBucketId() == rid.getBucketId()) {
              bucketIndex.remove(new Object[] { number }, rid);
              bucketIndex.put(new Object[] { number.toString() }, new RID[] { rid });
            }
        }
      }
    });
    // the entries are the old ones now: the canonical key of a whole-number double finds nothing by itself
    try (final IndexCursor cursor = typeIndex.iterator(true)) {
      final List<String> keys = new ArrayList<>();
      while (cursor.hasNext()) {
        cursor.next();
        keys.add(String.valueOf(cursor.getKeys()[0]));
      }
      assertThat(keys).contains("1.0").doesNotContain("1");
    }
  }

  private long checkDatabaseErrors() {
    try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
      return rs.next().<Number>getProperty("totalErrors").longValue();
    }
  }

  private List<Integer> ids(final String query, final Map<String, Object> params) {
    return ids(query, params, "opencypher");
  }

  private List<Integer> ids(final String query, final Map<String, Object> params, final String language) {
    final List<Integer> ids = new ArrayList<>();
    try (final ResultSet rs = database.query(language, query, params)) {
      while (rs.hasNext())
        ids.add(rs.next().<Number>getProperty("id").intValue());
    }
    return ids;
  }
}
