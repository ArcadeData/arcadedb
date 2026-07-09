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
 * Regression test for https://github.com/ArcadeData/arcadedb/issues/5114
 * <p>
 * FOREACH mutations to a matched node must be visible to a RETURN in the same query,
 * matching Neo4j / Memgraph / FalkorDB behaviour (and ArcadeDB's own direct SET behaviour).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5114ForeachReturnVisibilityTest {
  private Database database;

  @BeforeEach
  void setUp(final TestInfo testInfo) {
    final String databasePath = "./target/databases/testopencypher-5114-" + testInfo.getTestMethod().get().getName();
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

  private long commandVal(final String query) {
    final ResultSet rs = database.command("opencypher", query);
    assertThat(rs.hasNext()).isTrue();
    final Result r = rs.next();
    return ((Number) r.getProperty("val")).longValue();
  }

  private long queryVal(final String query) {
    final ResultSet rs = database.query("opencypher", query);
    assertThat(rs.hasNext()).isTrue();
    final Result r = rs.next();
    return ((Number) r.getProperty("val")).longValue();
  }

  @Test
  void foreachSetVisibleToReturn() {
    database.transaction(() -> database.command("opencypher", "CREATE (n:Node {id: 0, val: 0})"));

    database.transaction(() -> {
      final long sameQuery = commandVal("""
          MATCH (n:Node {id: 0})
          FOREACH (x IN [1] | SET n.val = 42)
          RETURN n.val AS val""");
      assertThat(sameQuery).isEqualTo(42L);
    });

    // Subsequent read must still be 42 (mutation persisted).
    assertThat(queryVal("MATCH (n:Node {id: 0}) RETURN n.val AS val")).isEqualTo(42L);
  }

  @Test
  void multipleForeachBlocksVisibleToReturn() {
    database.transaction(() -> database.command("opencypher", "CREATE (n:Node {id: 2, val: 0})"));

    database.transaction(() -> {
      final long sameQuery = commandVal("""
          MATCH (n:Node {id: 2})
          FOREACH (x IN [1] | SET n.val = 10)
          FOREACH (y IN [1] | SET n.val = 20)
          FOREACH (z IN [1] | SET n.val = 30)
          RETURN n.val AS val""");
      assertThat(sameQuery).isEqualTo(30L);
    });

    assertThat(queryVal("MATCH (n:Node {id: 2}) RETURN n.val AS val")).isEqualTo(30L);
  }

  @Test
  void foreachIncrementVisibleToReturn() {
    database.transaction(() -> database.command("opencypher", "CREATE (n:Node {id: 3, val: 0})"));

    database.transaction(() -> {
      final long sameQuery = commandVal("""
          MATCH (n:Node {id: 3})
          FOREACH (x IN [1, 2, 3] | SET n.val = n.val + 1)
          RETURN n.val AS val""");
      assertThat(sameQuery).isEqualTo(3L);
    });

    assertThat(queryVal("MATCH (n:Node {id: 3}) RETURN n.val AS val")).isEqualTo(3L);
  }

  @Test
  void directSetStillVisibleToReturn() {
    // Control: direct SET already works; guard against regressions.
    database.transaction(() -> database.command("opencypher", "CREATE (n:Node {id: 1, val: 0})"));

    database.transaction(() -> {
      final long sameQuery = commandVal("""
          MATCH (n:Node {id: 1})
          SET n.val = 42
          RETURN n.val AS val""");
      assertThat(sameQuery).isEqualTo(42L);
    });
  }
}
