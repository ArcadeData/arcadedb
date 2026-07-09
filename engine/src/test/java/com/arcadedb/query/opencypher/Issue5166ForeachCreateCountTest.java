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
import org.junit.jupiter.api.TestInfo;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for https://github.com/ArcadeData/arcadedb/issues/5166
 * <p>
 * When {@code FOREACH ... CREATE} is directly followed by {@code RETURN count(var)} (an
 * aggregation over the outer MATCH variable), the {@code TypeCountStep} O(1) optimization
 * short-circuited the whole plan, silently discarding the FOREACH write side effects.
 * Neo4j persists the created node; ArcadeDB must too.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5166ForeachCreateCountTest {
  private Database database;

  @BeforeEach
  void setUp(final TestInfo testInfo) {
    final String databasePath = "./target/databases/testopencypher-5166-" + testInfo.getTestMethod().get().getName();
    final DatabaseFactory factory = new DatabaseFactory(databasePath);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  private long count(final String label) {
    final ResultSet rs = database.query("opencypher", "MATCH (n:" + label + ") RETURN count(n) AS c");
    assertThat(rs.hasNext()).isTrue();
    return ((Number) rs.next().getProperty("c")).longValue();
  }

  @Test
  void foreachCreateFollowedByCountVarPersists() {
    database.transaction(() -> database.command("opencypher", "CREATE (a:A {id: 1})"));

    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher", """
          MATCH (a:A)
          FOREACH (x IN [1] | CREATE (:FB {id: x}))
          RETURN count(a) AS c""");
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      assertThat(((Number) r.getProperty("c")).longValue()).isEqualTo(1L);
    });

    // The FB node created inside FOREACH must be persisted (was silently lost before the fix).
    assertThat(count("FB")).isEqualTo(1L);
  }

  @Test
  void foreachCreateMultipleFollowedByCountVarPersists() {
    database.transaction(() -> database.command("opencypher", "CREATE (a:A {id: 1})"));

    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher", """
          MATCH (a:A)
          FOREACH (x IN [1, 2, 3] | CREATE (:FB {id: x}))
          RETURN count(a) AS c""");
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(1L);
    });

    assertThat(count("FB")).isEqualTo(3L);
  }

  @Test
  void foreachCreateWithIntermediateWithStillWorks() {
    // The documented workaround must keep working.
    database.transaction(() -> database.command("opencypher", "CREATE (a:A {id: 1})"));

    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher", """
          MATCH (a:A)
          FOREACH (x IN [1] | CREATE (:FB {id: x}))
          WITH a
          RETURN count(a) AS c""");
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(1L);
    });

    assertThat(count("FB")).isEqualTo(1L);
  }

  @Test
  void plainMatchCountVarStillOptimized() {
    // Control: the O(1) TypeCountStep optimization must still apply when there is no FOREACH.
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:A {id: 1})");
      database.command("opencypher", "CREATE (:A {id: 2})");
      database.command("opencypher", "CREATE (:A {id: 3})");
    });

    assertThat(count("A")).isEqualTo(3L);
  }
}
