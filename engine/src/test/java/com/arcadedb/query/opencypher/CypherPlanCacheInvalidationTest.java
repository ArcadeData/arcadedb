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

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test: the OpenCypher plan cache must be invalidated on schema changes, mirroring the SQL
 * execution-plan cache. Before the fix, {@code CypherPlanCache.invalidate()} had no callers, so a cached
 * {@code PhysicalPlan} could keep referencing an index/type after it was created or dropped (e.g. a
 * {@code NodeIndexSeek} over an index that no longer exists), silently returning wrong results.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherPlanCacheInvalidationTest {
  private static final String DB_PATH = "./target/databases/cypher-plancache-invalidation";
  private DatabaseInternal database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    database = (DatabaseInternal) factory.create();
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  private long matchRows(final String cypher) {
    long rows = 0;
    try (final ResultSet rs = database.query("cypher", cypher)) {
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
    }
    return rows;
  }

  // A connected relationship pattern reliably goes through the optimizer, which produces the cacheable
  // PhysicalPlan (single-node MATCH may fall back to the legacy step path).
  private static final String Q = "MATCH (a:Foo)-[:REL]->(b:Foo) RETURN b.x AS v";

  @Test
  void planCacheIsFlushedOnSchemaChange() {
    database.transaction(() ->
      database.command("cypher", "CREATE (a:Foo {x:1})-[:REL]->(b:Foo {x:2}), (c:Foo {x:1})-[:REL]->(d:Foo {x:2})"));

    // Populate the plan cache with a node-returning query (its plan depends on the current schema).
    assertThat(matchRows(Q)).isEqualTo(2L);
    assertThat(database.getCypherPlanCache().size()).as("plan should be cached").isGreaterThan(0);

    // A property declaration is a schema mutation and must flush the plan cache.
    database.getSchema().getType("Foo").createProperty("x", Type.INTEGER);
    assertThat(database.getCypherPlanCache().size()).as("plan cache flushed after CREATE PROPERTY").isEqualTo(0);

    assertThat(matchRows(Q)).isEqualTo(2L);
    assertThat(database.getCypherPlanCache().size()).isGreaterThan(0);

    // Any index mutation must flush the plan cache.
    database.getSchema().buildTypeIndex("Foo", new String[] { "x" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();
    assertThat(database.getCypherPlanCache().size()).as("plan cache flushed after CREATE INDEX").isEqualTo(0);

    // Re-plan now can use the index; result must still be correct.
    assertThat(matchRows(Q)).isEqualTo(2L);
    assertThat(database.getCypherPlanCache().size()).isGreaterThan(0);

    // Dropping the index must again flush, so the previously index-based cached plan cannot be reused
    // against a now-missing index.
    database.getSchema().dropIndex("Foo[x]");
    assertThat(database.getCypherPlanCache().size()).as("plan cache flushed after DROP INDEX").isEqualTo(0);

    // Result stays correct with the fresh (scan-based) plan.
    assertThat(matchRows(Q)).isEqualTo(2L);
  }
}
