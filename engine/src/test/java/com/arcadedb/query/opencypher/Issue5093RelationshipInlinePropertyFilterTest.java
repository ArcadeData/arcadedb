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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #5093.
 * Tests that an inline property filter on a relationship pattern, e.g.
 * {@code MATCH (a:BugNode)-[r:LINK {w: 1}]->(b:BugNode)}, is correctly applied.
 * Prior to the fix, the optimizer path ignored the inline relationship property map
 * and returned all LINK edges regardless of the {@code w} value.
 * <p>
 * The behavior is verified against the OpenCypher reference implementation (Neo4j),
 * which returns only the edges where {@code w = 1}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5093RelationshipInlinePropertyFilterTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/issue-5093-rel-inline-property").create();

    database.getSchema().createVertexType("BugNode");
    database.getSchema().createEdgeType("LINK");

    database.transaction(() -> {
      final MutableVertex n1 = database.newVertex("BugNode").set("id", 1).save();
      final MutableVertex n2 = database.newVertex("BugNode").set("id", 2).save();
      final MutableVertex n3 = database.newVertex("BugNode").set("id", 3).save();
      final MutableVertex n4 = database.newVertex("BugNode").set("id", 4).save();
      final MutableVertex n5 = database.newVertex("BugNode").set("id", 5).save();

      n1.newEdge("LINK", n2).set("w", 1).save();
      n1.newEdge("LINK", n3).set("w", 2).save();
      n2.newEdge("LINK", n4).set("w", 1).save();
      n4.newEdge("LINK", n5).set("w", 1).save();
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
  void inlineRelationshipPropertyFilterIsApplied() {
    // Only the 3 edges with w=1 must be returned (1->2, 2->4, 4->5).
    final List<int[]> pairs = runPairs(
        "MATCH (a:BugNode)-[r:LINK {w: 1}]->(b:BugNode) RETURN a.id AS a_id, b.id AS b_id ORDER BY a_id, b_id");

    assertThat(pairs).hasSize(3);
    assertThat(pairs.get(0)).containsExactly(1, 2);
    assertThat(pairs.get(1)).containsExactly(2, 4);
    assertThat(pairs.get(2)).containsExactly(4, 5);
  }

  @Test
  void inlineRelationshipPropertyFilterMatchesWhereEquivalent() {
    // The inline filter must be semantically equivalent to the explicit WHERE form.
    final List<int[]> inline = runPairs(
        "MATCH (a:BugNode)-[r:LINK {w: 1}]->(b:BugNode) RETURN a.id AS a_id, b.id AS b_id ORDER BY a_id, b_id");
    final List<int[]> where = runPairs(
        "MATCH (a:BugNode)-[r:LINK]->(b:BugNode) WHERE r.w = 1 RETURN a.id AS a_id, b.id AS b_id ORDER BY a_id, b_id");

    assertThat(inline).hasSameSizeAs(where);
    for (int i = 0; i < inline.size(); i++)
      assertThat(inline.get(i)).containsExactly(where.get(i)[0], where.get(i)[1]);
  }

  @Test
  void inlineRelationshipPropertyFilterNoEdgeVariable() {
    // Same filter but on an anonymous relationship (no 'r' variable).
    final List<int[]> pairs = runPairs(
        "MATCH (a:BugNode)-[:LINK {w: 1}]->(b:BugNode) RETURN a.id AS a_id, b.id AS b_id ORDER BY a_id, b_id");

    assertThat(pairs).hasSize(3);
    assertThat(pairs.get(0)).containsExactly(1, 2);
    assertThat(pairs.get(1)).containsExactly(2, 4);
    assertThat(pairs.get(2)).containsExactly(4, 5);
  }

  @Test
  void inlineRelationshipPropertyFilterOtherValue() {
    // Only the single edge with w=2 (1->3) must be returned.
    final List<int[]> pairs = runPairs(
        "MATCH (a:BugNode)-[r:LINK {w: 2}]->(b:BugNode) RETURN a.id AS a_id, b.id AS b_id ORDER BY a_id, b_id");

    assertThat(pairs).hasSize(1);
    assertThat(pairs.get(0)).containsExactly(1, 3);
  }

  private List<int[]> runPairs(final String cypher) {
    final List<int[]> pairs = new ArrayList<>();
    final ResultSet rs = database.query("opencypher", cypher);
    while (rs.hasNext()) {
      final Result r = rs.next();
      pairs.add(new int[] { ((Number) r.getProperty("a_id")).intValue(), ((Number) r.getProperty("b_id")).intValue() });
    }
    rs.close();
    return pairs;
  }
}
