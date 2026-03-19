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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #3563.
 * Tests that inline property filters on target nodes in relationship patterns
 * are correctly applied by MatchRelationshipStep.
 * <p>
 * Example: MATCH (sx:Sx)<-[:E0]-(:S0 {name:'x'}) RETURN sx
 * The {name:'x'} filter on the anonymous (:S0) node must be applied.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherInlinePropertyFilterTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/cypher-inline-property-filter").create();

    // S1 extends S0 (inheritance)
    database.getSchema().createVertexType("S0");
    final var s1Type = database.getSchema().createVertexType("S1");
    s1Type.addSuperType("S0");
    database.getSchema().createVertexType("Sx");
    database.getSchema().createEdgeType("E0");

    database.transaction(() -> {
      // S0 vertex with name='one'
      final MutableVertex s0 = database.newVertex("S0").set("name", "one").save();
      // S1 vertex with name='two' (S1 extends S0)
      final MutableVertex s1 = database.newVertex("S1").set("name", "two").save();
      // Two Sx vertices (no properties)
      final MutableVertex sx1 = database.newVertex("Sx").save();
      final MutableVertex sx2 = database.newVertex("Sx").save();

      // E0 edges: S0 -> Sx1, S1 -> Sx2
      s0.newEdge("E0", sx1);
      s1.newEdge("E0", sx2);
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void inlinePropertyFilterOnAnonymousTargetNode() {
    // This query should return only the S0 vertex with name='one'
    final ResultSet rs = database.query("opencypher",
        "MATCH (s:S0 {name:'one'})-[:E0]->(:Sx) RETURN s");
    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());
    rs.close();

    assertThat(results).hasSize(1);
    final Vertex s = results.get(0).getProperty("s");
    assertThat(s.getString("name")).isEqualTo("one");
  }

  @Test
  void inlinePropertyFilterOnAnonymousTargetNodeNoMatch() {
    // This query should return NO results because no S0 has name='x'
    final ResultSet rs = database.query("opencypher",
        "MATCH (sx:Sx)<-[:E0]-(:S0 {name:'x'}) RETURN sx");
    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());
    rs.close();

    assertThat(results).isEmpty();
  }

  @Test
  void inlinePropertyFilterOnNamedTargetNode() {
    // Same as above but with a named variable - filter should still apply
    final ResultSet rs = database.query("opencypher",
        "MATCH (sx:Sx)<-[:E0]-(s:S0 {name:'x'}) RETURN sx");
    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());
    rs.close();

    assertThat(results).isEmpty();
  }

  @Test
  void inlinePropertyFilterMatchingExists() {
    // Filter with name='one' should return the Sx connected to S0(name='one')
    final ResultSet rs = database.query("opencypher",
        "MATCH (sx:Sx)<-[:E0]-(:S0 {name:'one'}) RETURN sx");
    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());
    rs.close();

    assertThat(results).hasSize(1);
  }

  @Test
  void inlinePropertyFilterWithInheritance() {
    // S1 extends S0, so matching :S0 {name:'two'} should find the S1 vertex
    final ResultSet rs = database.query("opencypher",
        "MATCH (sx:Sx)<-[:E0]-(:S0 {name:'two'}) RETURN sx");
    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());
    rs.close();

    assertThat(results).hasSize(1);
  }
}
