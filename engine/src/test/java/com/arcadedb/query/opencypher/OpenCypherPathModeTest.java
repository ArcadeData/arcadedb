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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for Cypher path mode support: WALK, TRAIL, ACYCLIC.
 * <p>
 * Test graph (cycle + branch):
 * A → B → C → D → A  (cycle of 4)
 * A → E              (branch)
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class OpenCypherPathModeTest {

  private static final String DB_PATH = "target/databases/test-cypher-pathmode";
  private Database database;

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();

    database.transaction(() -> {
      database.getSchema().createVertexType("Node");
      database.getSchema().createEdgeType("LINK");
    });

    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      final MutableVertex e = database.newVertex("Node").set("name", "E").save();

      // Cycle: A → B → C → D → A
      a.newEdge("LINK", b, true, (Object[]) null).save();
      b.newEdge("LINK", c, true, (Object[]) null).save();
      c.newEdge("LINK", d, true, (Object[]) null).save();
      d.newEdge("LINK", a, true, (Object[]) null).save();
      // Branch: A → E
      a.newEdge("LINK", e, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void trailAllowsVertexRevisit() {
    // TRAIL: edges unique, but vertices can repeat.
    // From A with max 5 hops: A→B(1), A→B→C(2), A→B→C→D(3), A→B→C→D→A(4 — vertex A revisited!), A→E(1)
    // The cycle completes because only EDGES are checked, not vertices.
    database.transaction(() -> {
      final List<String> names = queryNames("MATCH TRAIL (a:Node {name: 'A'})-[:LINK*1..5]->(b) RETURN b.name as name");
      // Should include A as destination (vertex revisit after completing the cycle)
      assertThat(names).contains("A");
      assertThat(names).contains("B", "C", "D", "E");
    });
  }

  @Test
  void trailIsDefault() {
    // Without explicit path mode, TRAIL is the default per GQL standard.
    database.transaction(() -> {
      final List<String> withTrail = queryNames("MATCH TRAIL (a:Node {name: 'A'})-[:LINK*1..5]->(b) RETURN b.name as name");
      final List<String> noPrefix = queryNames("MATCH (a:Node {name: 'A'})-[:LINK*1..5]->(b) RETURN b.name as name");
      assertThat(noPrefix).hasSameSizeAs(withTrail);
    });
  }

  @Test
  void acyclicBlocksVertexRevisit() {
    // ACYCLIC: no vertex repeated. Cannot complete the cycle back to A.
    database.transaction(() -> {
      final List<String> names = queryNames("MATCH ACYCLIC (a:Node {name: 'A'})-[:LINK*1..5]->(b) RETURN b.name as name");
      // Should NOT include A (vertex uniqueness prevents revisit)
      assertThat(names).doesNotContain("A");
      assertThat(names).contains("B", "C", "D", "E");
    });
  }

  @Test
  void walkAllowsEdgeAndVertexRepetition() {
    // WALK: no restrictions at all. Can reuse edges and revisit vertices.
    // This produces more results than TRAIL because after A→B→C→D→A,
    // it can continue A→B→C→D→A→B... (reusing edges).
    database.transaction(() -> {
      final long walkCount = queryCount("MATCH WALK (a:Node {name: 'A'})-[:LINK*1..6]->(b) RETURN b");
      final long trailCount = queryCount("MATCH TRAIL (a:Node {name: 'A'})-[:LINK*1..6]->(b) RETURN b");
      // WALK should produce strictly more results than TRAIL on a cyclic graph
      assertThat(walkCount).isGreaterThan(trailCount);
    });
  }

  @Test
  void walkRequiresMaxHops() {
    // WALK without explicit max hops should throw an error (would be infinite on cycles).
    database.transaction(() -> {
      assertThatThrownBy(() -> {
        try (ResultSet rs = database.query("opencypher", "MATCH WALK (a:Node {name: 'A'})-[:LINK*]->(b) RETURN b")) {
          while (rs.hasNext()) rs.next();
        }
      }).hasMessageContaining("WALK");
    });
  }

  @Test
  void trailProducesMoreResultsThanAcyclic() {
    // On a cyclic graph, TRAIL allows vertex revisit (completing the cycle),
    // while ACYCLIC does not. So TRAIL >= ACYCLIC.
    database.transaction(() -> {
      final long trailCount = queryCount("MATCH TRAIL (a:Node {name: 'A'})-[:LINK*1..5]->(b) RETURN b");
      final long acyclicCount = queryCount("MATCH ACYCLIC (a:Node {name: 'A'})-[:LINK*1..5]->(b) RETURN b");
      assertThat(trailCount).isGreaterThan(acyclicCount);
    });
  }

  @Test
  void pathModeWithNamedPath() {
    // Path mode should work with named path variables.
    database.transaction(() -> {
      try (ResultSet rs = database.query("opencypher",
          "MATCH p = TRAIL (a:Node {name: 'A'})-[:LINK*1..4]->(b) RETURN p")) {
        int count = 0;
        while (rs.hasNext()) {
          final Result row = rs.next();
          assertThat((Object) row.getProperty("p")).isNotNull();
          count++;
        }
        assertThat(count).isGreaterThan(0);
      }
    });
  }

  private List<String> queryNames(final String cypher) {
    final List<String> names = new ArrayList<>();
    try (ResultSet rs = database.query("opencypher", cypher)) {
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));
    }
    return names;
  }

  private long queryCount(final String cypher) {
    long count = 0;
    try (ResultSet rs = database.query("opencypher", cypher)) {
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
    }
    return count;
  }
}
