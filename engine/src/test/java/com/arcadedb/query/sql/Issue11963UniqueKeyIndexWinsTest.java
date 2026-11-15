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
    // The two creation orders, so the answer cannot come from the order the type happens to list its indexes in
    for (final String type : List.of("KeyFirst", "GroupFirst")) {
      database.command("sql", "CREATE VERTEX TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".key STRING");
      database.command("sql", "CREATE PROPERTY " + type + ".grp STRING");
      if (type.equals("KeyFirst")) {
        database.command("sql", "CREATE INDEX ON " + type + " (key) UNIQUE");
        database.command("sql", "CREATE INDEX ON " + type + " (grp) NOTUNIQUE");
      } else {
        database.command("sql", "CREATE INDEX ON " + type + " (grp) NOTUNIQUE");
        database.command("sql", "CREATE INDEX ON " + type + " (key) UNIQUE");
      }
      database.transaction(() -> {
        for (int i = 0; i < RECORDS; i++)
          database.newVertex(type).set("key", "k" + i).set("grp", i % 7 == 0 ? "b" : "a").save();
      });
    }
  }

  @Test
  void anEqualityOnTheUniqueKeyFetchesFromTheUniqueIndex() {
    for (final String type : List.of("KeyFirst", "GroupFirst"))
      assertFetchesFromKeyIndex(type, "SELECT key FROM " + type + " WHERE key = :key AND grp = :grp",
          Map.of("key", "k101", "grp", "a"), List.of("k101"));
  }

  @Test
  void anInListOnTheUniqueKeyFetchesFromTheUniqueIndex() {
    for (final String type : List.of("KeyFirst", "GroupFirst"))
      assertFetchesFromKeyIndex(type, "SELECT key FROM " + type + " WHERE grp = :grp AND key IN :keys",
          Map.of("keys", List.of("k1", "k2", "k7"), "grp", "a"), List.of("k1", "k2"));
  }

  private void assertFetchesFromKeyIndex(final String type, final String sql, final Map<String, Object> params,
      final List<String> expected) {
    try (final ResultSet rs = database.query("sql", sql, params)) {
      final List<String> keys = rs.stream().map(r -> r.<String>getProperty("key")).sorted().toList();
      final String plan = rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
      assertThat(plan).as("%s plans from the UNIQUE index on key (issue #11963):%n%s", type, plan)
          .contains("FETCH FROM INDEX " + type + "[key]")
          .doesNotContain("FETCH FROM INDEX " + type + "[grp]");
      assertThat(keys).isEqualTo(expected);
    }
  }
}
