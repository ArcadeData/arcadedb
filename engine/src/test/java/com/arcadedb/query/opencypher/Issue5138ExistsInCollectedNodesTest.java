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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproducer for issue #5138: {@code EXISTS { ... WHERE node IN collected_nodes }} never matches
 * against an outer-scope collected node list.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5138ExistsInCollectedNodesTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue5138");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    database.transaction(() ->
      database.command("opencypher", """
          CREATE \
          (a:User {id: 1, active: true}), \
          (b:User {id: 2, active: true}), \
          (c:User {id: 3, active: false}), \
          (d:User {id: 4, active: false}), \
          (a)-[:FRIEND]->(c), \
          (b)-[:FRIEND]->(d)"""));
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void existsWithInCollectedNodeList() {
    final List<String> results = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher",
        """
        MATCH (inactive:User {active: false}) \
        WITH collect(DISTINCT inactive) AS inactive_nodes \
        MATCH (u:User {active: true}) \
        RETURN u.id AS id, EXISTS { \
          MATCH (u)-[:FRIEND]->(friend) \
          WHERE friend IN inactive_nodes \
        } AS has_inactive_friend \
        ORDER BY id""")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        results.add(r.getProperty("id") + "=" + r.getProperty("has_inactive_friend"));
      }
    }

    assertThat(results).containsExactly("1=true", "2=true");
  }

  @Test
  void writeGuardedByExistsInCollectedNodeList() {
    database.transaction(() ->
      database.command("opencypher",
          """
          MATCH (inactive:User {active: false}) \
          WITH collect(DISTINCT inactive) AS inactive_nodes \
          MATCH (u:User {active: true}) \
          WHERE EXISTS { \
            MATCH (u)-[:FRIEND]->(friend) \
            WHERE friend IN inactive_nodes \
          } \
          SET u.flagged = true"""));

    long cnt;
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (u:User) WHERE u.flagged = true RETURN count(u) AS cnt")) {
      cnt = ((Number) rs.next().getProperty("cnt")).longValue();
    }
    assertThat(cnt).isEqualTo(2L);
  }
}
