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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An equality (or an IN list) on the key of a UNIQUE index next to an equality on a NOTUNIQUE index fetches from the
 * UNIQUE one (issue #11963). Without recorded statistics every candidate cost the same and the first index the type
 * listed won: a NOTUNIQUE index on a property most records share, read whole to return one record.
 */
class Issue11963UniqueKeyIndexWinsTest extends TestHelper {
  private static final int RECORDS = 200;

  @Override
  protected void beginTest() {
    // Every shape is created in both orders, so the answer cannot come from the order the type happens to list its indexes in
    createType("KeyFirst", "(key) UNIQUE", "(grp) NOTUNIQUE");
    createType("GroupFirst", "(grp) NOTUNIQUE", "(key) UNIQUE");
    createType("CompositeKeyFirst", "(k1, k2) UNIQUE", "(g1, g2) NOTUNIQUE");
    createType("CompositeGroupFirst", "(g1, g2) NOTUNIQUE", "(k1, k2) UNIQUE");
    createType("WideGroupFirst", "(grp, sub) NOTUNIQUE", "(key) UNIQUE");
    createType("WideKeyFirst", "(key) UNIQUE", "(grp, sub) NOTUNIQUE");
  }

  private void createType(final String type, final String firstIndex, final String secondIndex) {
    database.command("sql", "CREATE VERTEX TYPE " + type);
    for (final String property : List.of("key", "grp", "sub", "k1", "k2", "g1", "g2"))
      database.command("sql", "CREATE PROPERTY " + type + "." + property + " STRING");
    database.command("sql", "CREATE INDEX ON " + type + " " + firstIndex);
    database.command("sql", "CREATE INDEX ON " + type + " " + secondIndex);
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++) {
        // The group is shared by most records, the key by none: the unique one is the selective one
        final String group = i % 7 == 0 ? "b" : "a";
        database.newVertex(type).set("key", "k" + i).set("grp", group).set("sub", "s").set("k1", "k" + i).set("k2", "x")
            .set("g1", group).set("g2", "y").save();
      }
    });
  }

  @Test
  void anEqualityOnTheUniqueKeyFetchesFromTheUniqueIndex() {
    for (final String type : List.of("KeyFirst", "GroupFirst"))
      assertFetchesFrom(type, "[key]", "[grp]", "SELECT key FROM " + type + " WHERE key = :key AND grp = :grp",
          Map.of("key", "k101", "grp", "a"), List.of("k101"));
  }

  @Test
  void anInListOnTheUniqueKeyFetchesFromTheUniqueIndex() {
    for (final String type : List.of("KeyFirst", "GroupFirst"))
      assertFetchesFrom(type, "[key]", "[grp]", "SELECT key FROM " + type + " WHERE grp = :grp AND key IN :keys",
          Map.of("keys", List.of("k1", "k2", "k7"), "grp", "a"), List.of("k1", "k2"));
  }

  @Test
  void aWholeKeyOfACompositeUniqueIndexFetchesFromIt() {
    for (final String type : List.of("CompositeKeyFirst", "CompositeGroupFirst"))
      assertFetchesFrom(type, "[k1,k2]", "[g1,g2]",
          "SELECT k1 AS key FROM " + type + " WHERE g1 = :g1 AND g2 = :g2 AND k1 = :k1 AND k2 = :k2",
          Map.of("k1", "k101", "k2", "x", "g1", "a", "g2", "y"), List.of("k101"));
  }

  @Test
  void anOrBranchFetchesFromTheUniqueIndexToo() {
    for (final String type : List.of("KeyFirst", "GroupFirst"))
      assertFetchesFrom(type, "[key]", "[grp]",
          "SELECT key FROM " + type + " WHERE (key = :key1 AND grp = :grp) OR (key = :key2 AND grp = :grp)",
          Map.of("key1", "k101", "key2", "k102", "grp", "a"), List.of("k101", "k102"));
  }

  /** The preference only breaks a tie: an index covering more conditions still wins, as it did before the fix. */
  @Test
  void anIndexCoveringMoreConditionsStillWinsOverTheUniqueKey() {
    for (final String type : List.of("WideKeyFirst", "WideGroupFirst"))
      assertFetchesFrom(type, "[grp,sub]", "[key]",
          "SELECT key FROM " + type + " WHERE key = :key AND grp = :grp AND sub = :sub",
          Map.of("key", "k101", "grp", "a", "sub", "s"), List.of("k101"));
  }

  private void assertFetchesFrom(final String type, final String expectedIndex, final String rejectedIndex, final String sql,
      final Map<String, Object> params, final List<String> expected) {
    try (final ResultSet rs = database.query("sql", sql, params)) {
      final List<String> keys = rs.stream().map(r -> r.<String>getProperty("key")).sorted().toList();
      final String plan = rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
      assertThat(plan).as("%s plans from %s (issue #11963):%n%s", type, expectedIndex, plan)
          .contains("FETCH FROM INDEX " + type + expectedIndex)
          .doesNotContain("FETCH FROM INDEX " + type + rejectedIndex);
      assertThat(keys).isEqualTo(expected);
    }
  }
}
