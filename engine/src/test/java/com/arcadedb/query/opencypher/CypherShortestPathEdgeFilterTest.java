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
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for https://github.com/ArcadeData/arcadedb/issues/5096
 * <p>
 * shortestPath() and allShortestPaths() must enforce inline edge property filters
 * (e.g. {@code shortestPath((a)-[:LINK*1..3 {w: 1}]->(b))}) during traversal.
 * Previously the filter was accepted but silently ignored, so paths crossing edges
 * with a non-matching property value were still returned.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherShortestPathEdgeFilterTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./databases/test-shortestpath-edgefilter").create();

    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    // Graph:
    //   1 --LINK{w:1}--> 2 --LINK{w:1}--> 3
    //   1 --LINK{w:2}--> 4 --LINK{w:2}--> 3
    database.transaction(() -> {
      final MutableVertex n1 = database.newVertex("Node").set("id", 1L).save();
      final MutableVertex n2 = database.newVertex("Node").set("id", 2L).save();
      final MutableVertex n3 = database.newVertex("Node").set("id", 3L).save();
      final MutableVertex n4 = database.newVertex("Node").set("id", 4L).save();

      n1.newEdge("LINK", n2, true, new Object[] { "w", 1 }).save();
      n2.newEdge("LINK", n3, true, new Object[] { "w", 1 }).save();
      n1.newEdge("LINK", n4, true, new Object[] { "w", 2 }).save();
      n4.newEdge("LINK", n3, true, new Object[] { "w", 2 }).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  /**
   * The only edge leaving node 1 towards node 4 has w=2, so a {w:1} filter must exclude it.
   */
  @Test
  void shortestPathEnforcesEdgePropertyFilterNoMatch() {
    final ResultSet rs = database.query("opencypher",
        """
            MATCH (a:Node {id: 1}), (b:Node {id: 4})
            MATCH p = shortestPath((a)-[:LINK*1..3 {w: 1}]->(b))
            RETURN p""");

    assertThat(rs.hasNext()).as("shortestPath must not cross w=2 edges when filter is {w:1}").isFalse();
  }

  /**
   * With filter {w:1}, node 1 -> node 3 must go 1 -> 2 -> 3 (both w=1 edges), never 1 -> 4 -> 3.
   */
  @Test
  void shortestPathEnforcesEdgePropertyFilterMatchingPath() {
    final ResultSet rs = database.query("opencypher",
        """
            MATCH (a:Node {id: 1}), (b:Node {id: 3})
            MATCH p = shortestPath((a)-[:LINK*1..3 {w: 1}]->(b))
            RETURN [n IN nodes(p) | n.id] AS ids""");

    assertThat(rs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Number> ids = (List<Number>) rs.next().<Object>getProperty("ids");
    assertThat(ids).containsExactly(1L, 2L, 3L);
    assertThat(rs.hasNext()).isFalse();
  }

  /**
   * Filter {w:2} forces the longer w=2 corridor 1 -> 4 -> 3.
   */
  @Test
  void shortestPathEnforcesEdgePropertyFilterAlternateValue() {
    final ResultSet rs = database.query("opencypher",
        """
            MATCH (a:Node {id: 1}), (b:Node {id: 3})
            MATCH p = shortestPath((a)-[:LINK*1..3 {w: 2}]->(b))
            RETURN [n IN nodes(p) | n.id] AS ids""");

    assertThat(rs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Number> ids = (List<Number>) rs.next().<Object>getProperty("ids");
    assertThat(ids).containsExactly(1L, 4L, 3L);
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void allShortestPathsEnforcesEdgePropertyFilterNoMatch() {
    final ResultSet rs = database.query("opencypher",
        """
            MATCH (a:Node {id: 1}), (b:Node {id: 4})
            MATCH p = allShortestPaths((a)-[:LINK*1..3 {w: 1}]->(b))
            RETURN p""");

    assertThat(rs.hasNext()).as("allShortestPaths must not cross w=2 edges when filter is {w:1}").isFalse();
  }

  @Test
  void allShortestPathsEnforcesEdgePropertyFilterMatchingPath() {
    final ResultSet rs = database.query("opencypher",
        """
            MATCH (a:Node {id: 1}), (b:Node {id: 3})
            MATCH p = allShortestPaths((a)-[:LINK*1..3 {w: 1}]->(b))
            RETURN [n IN nodes(p) | n.id] AS ids""");

    assertThat(rs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Number> ids = (List<Number>) rs.next().<Object>getProperty("ids");
    assertThat(ids).containsExactly(1L, 2L, 3L);
    assertThat(rs.hasNext()).isFalse();
  }

  /**
   * Control: without any edge filter, shortestPath returns the direct 1 -> 4 corridor unchanged.
   */
  @Test
  void shortestPathWithoutFilterStillWorks() {
    final ResultSet rs = database.query("opencypher",
        """
            MATCH (a:Node {id: 1}), (b:Node {id: 4})
            MATCH p = shortestPath((a)-[:LINK*1..3]->(b))
            RETURN [n IN nodes(p) | n.id] AS ids""");

    assertThat(rs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Number> ids = (List<Number>) rs.next().<Object>getProperty("ids");
    assertThat(ids).containsExactly(1L, 4L);
    assertThat(rs.hasNext()).isFalse();
  }
}
