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
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for Cypher relationship direction handling in CREATE and MERGE operations.
 * Verifies that left-pointing arrows (<-[]-) correctly create edges in the opposite direction
 * compared to right-pointing arrows (-[]->).
 *
 * Fixes regression test for issue #3326: Cypher relations are always created left to right
 * even if arrow is going from right to left.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/3326">Issue #3326</a>
 * @see <a href="https://github.com/ArcadeData/arcadedb/pull/3353">PR #3353</a>
 */
class CypherRelationDirectionTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testcypher-relation-direction").create();
    database.getSchema().createVertexType("A");
    database.getSchema().createVertexType("B");
    database.getSchema().createVertexType("C");
    database.getSchema().createEdgeType("LINK");
    database.getSchema().createEdgeType("KNOWS");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void createWithRightPointingArrow() {
    // Standard direction: A -> B
    database.command("opencypher", "CREATE (a:A {name: 'A1'})-[:LINK]->(b:B {name: 'B1'})");

    final ResultSet result = database.query("opencypher",
        "MATCH (a:A)-[r:LINK]->(b:B) RETURN a.name as from, b.name as to");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat(row.<String>getProperty("from")).isEqualTo("A1");
    assertThat(row.<String>getProperty("to")).isEqualTo("B1");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void createWithLeftPointingArrow() {
    // Reversed direction: A <- B means edge goes from B to A
    database.command("opencypher", "CREATE (a:A {name: 'A2'})<-[:LINK]-(b:B {name: 'B2'})");

    // Query with right-pointing arrow should find: B -> A
    final ResultSet result = database.query("opencypher",
        "MATCH (b:B)-[r:LINK]->(a:A) RETURN b.name as from, a.name as to");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat(row.<String>getProperty("from")).isEqualTo("B2");
    assertThat(row.<String>getProperty("to")).isEqualTo("A2");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void createWithLeftPointingArrowVerifyEdgeDirection() {
    // A <- B means edge OUT from B, IN to A
    database.command("opencypher", "CREATE (a:A {name: 'A3'})<-[:LINK]-(b:B {name: 'B3'})");

    // Verify edge direction using edge properties
    final ResultSet edgeResult = database.query("opencypher", "MATCH ()-[r:LINK]->() RETURN r");
    assertThat(edgeResult.hasNext()).isTrue();
    final Edge edge = (Edge) edgeResult.next().toElement();

    // Edge should go FROM B TO A
    assertThat(edge.getOutVertex().getString("name")).isEqualTo("B3");
    assertThat(edge.getInVertex().getString("name")).isEqualTo("A3");
  }

  @Test
  void createChainWithLeftPointingArrows() {
    // Original issue example: A <- B <- C
    // This should create: C -> B -> A
    database.command("opencypher",
        "CREATE (a:A {name: 'A'})<-[:LINK]-(b:B {name: 'B'})<-[:LINK]-(c:C {name: 'C'})");

    // Verify C -> B edge exists
    final ResultSet cbResult = database.query("opencypher",
        "MATCH (c:C)-[r:LINK]->(b:B) RETURN c.name as from, b.name as to");
    assertThat(cbResult.hasNext()).isTrue();
    Result row = cbResult.next();
    assertThat(row.<String>getProperty("from")).isEqualTo("C");
    assertThat(row.<String>getProperty("to")).isEqualTo("B");

    // Verify B -> A edge exists
    final ResultSet baResult = database.query("opencypher",
        "MATCH (b:B)-[r:LINK]->(a:A) RETURN b.name as from, a.name as to");
    assertThat(baResult.hasNext()).isTrue();
    row = baResult.next();
    assertThat(row.<String>getProperty("from")).isEqualTo("B");
    assertThat(row.<String>getProperty("to")).isEqualTo("A");
  }

  @Test
  void createMixedDirectionChain() {
    // A -> B <- C means: A -> B and C -> B
    database.command("opencypher",
        "CREATE (a:A {name: 'A'})-[:LINK]->(b:B {name: 'B'})<-[:LINK]-(c:C {name: 'C'})");

    // Verify A -> B edge
    final ResultSet abResult = database.query("opencypher",
        "MATCH (a:A)-[r:LINK]->(b:B) RETURN a.name as from, b.name as to");
    assertThat(abResult.hasNext()).isTrue();
    Result row = abResult.next();
    assertThat(row.<String>getProperty("from")).isEqualTo("A");
    assertThat(row.<String>getProperty("to")).isEqualTo("B");

    // Verify C -> B edge
    final ResultSet cbResult = database.query("opencypher",
        "MATCH (c:C)-[r:LINK]->(b:B) RETURN c.name as from, b.name as to");
    assertThat(cbResult.hasNext()).isTrue();
    row = cbResult.next();
    assertThat(row.<String>getProperty("from")).isEqualTo("C");
    assertThat(row.<String>getProperty("to")).isEqualTo("B");
  }

  @Test
  void mergeWithRightPointingArrow() {
    // Standard MERGE direction: A -> B
    database.command("opencypher", "MERGE (a:A {name: 'MA1'})-[:LINK]->(b:B {name: 'MB1'})");

    final ResultSet result = database.query("opencypher",
        "MATCH (a:A)-[r:LINK]->(b:B) RETURN a.name as from, b.name as to");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat(row.<String>getProperty("from")).isEqualTo("MA1");
    assertThat(row.<String>getProperty("to")).isEqualTo("MB1");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void mergeWithLeftPointingArrow() {
    // MERGE with reversed direction: A <- B means edge goes from B to A
    database.command("opencypher", "MERGE (a:A {name: 'MA2'})<-[:LINK]-(b:B {name: 'MB2'})");

    // Query with right-pointing arrow should find: B -> A
    final ResultSet result = database.query("opencypher",
        "MATCH (b:B)-[r:LINK]->(a:A) RETURN b.name as from, a.name as to");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat(row.<String>getProperty("from")).isEqualTo("MB2");
    assertThat(row.<String>getProperty("to")).isEqualTo("MA2");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void mergeWithLeftPointingArrowVerifyEdgeDirection() {
    // A <- B means edge OUT from B, IN to A
    database.command("opencypher", "MERGE (a:A {name: 'MA3'})<-[:LINK]-(b:B {name: 'MB3'})");

    // Verify edge direction using edge properties
    final ResultSet edgeResult = database.query("opencypher", "MATCH ()-[r:LINK]->() RETURN r");
    assertThat(edgeResult.hasNext()).isTrue();
    final Edge edge = (Edge) edgeResult.next().toElement();

    // Edge should go FROM B TO A
    assertThat(edge.getOutVertex().getString("name")).isEqualTo("MB3");
    assertThat(edge.getInVertex().getString("name")).isEqualTo("MA3");
  }

  @Test
  void mergeChainWithLeftPointingArrows() {
    // Original issue example with MERGE: A <- B <- C
    // This should create: C -> B -> A
    database.command("opencypher",
        "MERGE (a:A {name: 'MA'})<-[:LINK]-(b:B {name: 'MB'})<-[:LINK]-(c:C {name: 'MC'})");

    // Verify C -> B edge exists
    final ResultSet cbResult = database.query("opencypher",
        "MATCH (c:C)-[r:LINK]->(b:B) RETURN c.name as from, b.name as to");
    assertThat(cbResult.hasNext()).isTrue();
    Result row = cbResult.next();
    assertThat(row.<String>getProperty("from")).isEqualTo("MC");
    assertThat(row.<String>getProperty("to")).isEqualTo("MB");

    // Verify B -> A edge exists
    final ResultSet baResult = database.query("opencypher",
        "MATCH (b:B)-[r:LINK]->(a:A) RETURN b.name as from, a.name as to");
    assertThat(baResult.hasNext()).isTrue();
    row = baResult.next();
    assertThat(row.<String>getProperty("from")).isEqualTo("MB");
    assertThat(row.<String>getProperty("to")).isEqualTo("MA");
  }

  @Test
  void mergeMixedDirectionChain() {
    // A -> B <- C means: A -> B and C -> B
    database.command("opencypher",
        "MERGE (a:A {name: 'MXA'})-[:LINK]->(b:B {name: 'MXB'})<-[:LINK]-(c:C {name: 'MXC'})");

    // Verify A -> B edge
    final ResultSet abResult = database.query("opencypher",
        "MATCH (a:A)-[r:LINK]->(b:B) RETURN a.name as from, b.name as to");
    assertThat(abResult.hasNext()).isTrue();
    Result row = abResult.next();
    assertThat(row.<String>getProperty("from")).isEqualTo("MXA");
    assertThat(row.<String>getProperty("to")).isEqualTo("MXB");

    // Verify C -> B edge
    final ResultSet cbResult = database.query("opencypher",
        "MATCH (c:C)-[r:LINK]->(b:B) RETURN c.name as from, b.name as to");
    assertThat(cbResult.hasNext()).isTrue();
    row = cbResult.next();
    assertThat(row.<String>getProperty("from")).isEqualTo("MXC");
    assertThat(row.<String>getProperty("to")).isEqualTo("MXB");
  }

  @Test
  void mergeDoesNotDuplicateWithLeftPointingArrow() {
    // First MERGE creates the pattern
    database.command("opencypher", "MERGE (a:A {name: 'DupA'})<-[:LINK]-(b:B {name: 'DupB'})");

    // Second MERGE with same pattern should find existing, not duplicate
    database.command("opencypher", "MERGE (a:A {name: 'DupA'})<-[:LINK]-(b:B {name: 'DupB'})");

    // Count edges - should be exactly 1
    final ResultSet countResult = database.query("opencypher",
        "MATCH ()-[r:LINK]->() RETURN count(r) as cnt");
    assertThat(countResult.hasNext()).isTrue();
    assertThat(countResult.next().<Long>getProperty("cnt")).isEqualTo(1L);
  }
}
