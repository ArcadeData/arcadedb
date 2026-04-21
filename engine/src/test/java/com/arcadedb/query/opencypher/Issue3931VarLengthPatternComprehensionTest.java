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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #3931.
 * <p>
 * A variable-length relationship pattern inside a pattern comprehension
 * (e.g. {@code [(a)-[:KNOWS*2..2]->(f) | f.name]}) must respect the
 * minHops/maxHops bounds. Previously, ArcadeDB ignored the hop range and
 * only performed a single hop.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue3931VarLengthPatternComprehensionTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/testopencypher-varlen-comp-3931").create();
    database.getSchema().createVertexType("VarLengthTest3");
    database.getSchema().createEdgeType("KNOWS");

    database.transaction(() -> database.command("opencypher",
        "CREATE (alice:VarLengthTest3 {name:'Alice'}), " +
            "(bob:VarLengthTest3 {name:'Bob'}), " +
            "(charlie:VarLengthTest3 {name:'Charlie'}), " +
            "(alice)-[:KNOWS]->(bob), " +
            "(bob)-[:KNOWS]->(charlie), " +
            "(alice)-[:KNOWS]->(charlie)"));
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  void variableLengthPatternComprehensionTwoLength() {
    // Only the path alice->bob->charlie has exact length 2, so f = Charlie
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (a:VarLengthTest3 {name:'Alice'}) "
            + "RETURN [(a)-[:KNOWS*2..2]->(f:VarLengthTest3) | f.name] AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result r = resultSet.next();
    final List<Object> list = (List<Object>) r.getProperty("result");
    assertThat(list).containsExactly("Charlie");
    assertThat(resultSet.hasNext()).isFalse();
  }

  @SuppressWarnings("unchecked")
  @Test
  void variableLengthPatternComprehensionOneLength() {
    // GitHub issue #3930: within a pattern comprehension, the Cypher spec does not
    // mandate a specific iteration order. Neo4j happens to return insertion order
    // (["Bob", "Charlie"]); ArcadeDB walks its edge linked-list from newest to oldest
    // so the order is reversed. The outer ORDER BY in the issue's query sorts rows
    // (there is only one here), not the elements inside the list. Both orders are
    // valid Cypher results - we only assert the set membership is correct.
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (a:VarLengthTest3 {name:'Alice'}) "
            + "RETURN [(a)-[:KNOWS*1..1]->(f:VarLengthTest3) | f.name] AS result ORDER BY result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result r = resultSet.next();
    final List<Object> list = (List<Object>) r.getProperty("result");
    assertThat(list).containsExactlyInAnyOrder("Bob", "Charlie");
    assertThat(resultSet.hasNext()).isFalse();
  }

  @SuppressWarnings("unchecked")
  @Test
  void variableLengthPatternComprehensionZeroLength() {
    // GitHub issue #3929: zero-length path returns the anchor node itself.
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (a:VarLengthTest3 {name:'Alice'}) "
            + "RETURN [(a)-[:KNOWS*0..0]->(f:VarLengthTest3) | f.name] AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result r = resultSet.next();
    final List<Object> list = (List<Object>) r.getProperty("result");
    assertThat(list).containsExactly("Alice");
    assertThat(resultSet.hasNext()).isFalse();
  }

  @SuppressWarnings("unchecked")
  @Test
  void variableLengthPatternComprehensionNonExistentLength() {
    // GitHub issue #3932: no path of length 10 exists, must return empty list.
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (a:VarLengthTest3 {name:'Alice'}) "
            + "RETURN [(a)-[:KNOWS*10..10]->(f:VarLengthTest3) | f.name] AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result r = resultSet.next();
    final List<Object> list = (List<Object>) r.getProperty("result");
    assertThat(list).isEmpty();
    assertThat(resultSet.hasNext()).isFalse();
  }

  @SuppressWarnings("unchecked")
  @Test
  void variableLengthPatternComprehensionRange() {
    // Length 1: alice->bob, alice->charlie. Length 2: alice->bob->charlie.
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (a:VarLengthTest3 {name:'Alice'}) "
            + "RETURN [(a)-[:KNOWS*1..2]->(f:VarLengthTest3) | f.name] AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result r = resultSet.next();
    final List<Object> list = (List<Object>) r.getProperty("result");
    // Charlie appears twice: once at distance 1 (direct), once at distance 2 (via Bob)
    assertThat(list).containsExactlyInAnyOrder("Bob", "Charlie", "Charlie");
    assertThat(resultSet.hasNext()).isFalse();
  }
}
