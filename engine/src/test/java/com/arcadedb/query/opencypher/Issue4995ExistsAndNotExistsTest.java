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
 * Reproducer for issue #4995: correlated EXISTS { ... } combined with AND NOT EXISTS { ... }
 * drops a matching row.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4995ExistsAndNotExistsTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue4995");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    database.transaction(() ->
      database.command("opencypher", """
          CREATE \
          (u1:User {id: 1, active: true}), \
          (u2:User {id: 2, active: false}), \
          (u3:User {id: 3, active: true}), \
          (u1)-[:FRIEND]->(u2), \
          (u3)-[:FRIEND]->(u1)"""));
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void existsAndNotExists() {
    database.transaction(() ->
      database.command("opencypher", """
          MATCH (u:User {active: true}) \
          WHERE EXISTS { MATCH (u)-[:FRIEND]->(:User) } \
          AND NOT EXISTS { MATCH (u)-[:FRIEND]->(f:User) WHERE f.active IS NULL OR f.active = true } \
          SET u.flagged = true"""));

    final List<Integer> flagged = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (u:User) WHERE u.flagged = true RETURN u.id AS uid ORDER BY uid")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        flagged.add(((Number) r.getProperty("uid")).intValue());
      }
    }

    assertThat(flagged).containsExactly(1);
  }

  @Test
  void existsWithOrAndTrailingClause() {
    // The correlated EXISTS subquery has a top-level OR in its WHERE followed by a RETURN clause,
    // exercising the branch that injects the correlation before a subsequent clause keyword.
    final List<Integer> matched = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", """
        MATCH (u:User {active: true}) \
        WHERE EXISTS { MATCH (u)-[:FRIEND]->(f:User) WHERE f.active IS NULL OR f.active = true RETURN f } \
        RETURN u.id AS uid ORDER BY uid""")) {
      while (rs.hasNext())
        matched.add(((Number) rs.next().getProperty("uid")).intValue());
    }

    // Only u3 has a friend (u1) that is active; u1's only friend (u2) is inactive.
    assertThat(matched).containsExactly(3);
  }
}
