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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #7177: the {@code refactor.*} write procedures enumerated a vertex's edge
 * list on the instance the row carried, the pattern issue #7174 fixed in {@code merge.relationship}.
 * <p>
 * A row's vertex instance is loaded before the rows ahead of it apply their writes, and appending an edge
 * rewrites the edge-list head pointer of both endpoints. Reading the row's own instance therefore sees a
 * pre-append snapshot and silently skips the edges earlier rows added.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherRefactorProceduresStaleVertexIssue7177Test {
  /** Rows per hub. The clone counts below are closed forms in this number - see the test that uses them. */
  private static final int DRIVERS = 20;

  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/cypher-refactor-stale-vertex-7177").create();
    database.getSchema().createVertexType("Hub");
    database.getSchema().createVertexType("Spoke");
    database.getSchema().createVertexType("Target");
    database.getSchema().createEdgeType("LINK");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /**
   * Two hubs joined by one edge, cloned once per driver row. Cloning a hub copies each of its edges onto the
   * clone, and the end of that copy which was NOT cloned is an ORIGINAL hub - so every row appends an edge to
   * the hub the next row goes on to clone.
   * <p>
   * That makes the edge count a closed form. Write {@code a} for the out-degree of the first hub and {@code b}
   * for the in-degree of the second; both start at 1, cloning the first hub adds {@code a} edges and raises
   * {@code b} by one, cloning the second adds {@code b} and raises {@code a} by one. Driver {@code k}
   * therefore adds {@code k + (k+1)} edges, and {@code 1 + sum(2k+1, k=1..N)} is {@code (N+1)^2}.
   * <p>
   * Reading each hub's edges off the row's own instance stalls that growth: before the fix the run ended with
   * 341 edges rather than 441, one hundred copies never made because the enumeration kept re-reading the
   * snapshot taken before the appends.
   */
  @Test
  void cloneNodesWithRelationshipsSeesTheEdgesAppendedByEarlierRows() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Hub {id: 1})-[:LINK]->(:Hub {id: 2})");
      database.command("opencypher", "UNWIND range(1, " + DRIVERS + ") AS i CREATE (:Spoke {id: i})");
    });

    database.command("opencypher",
        "MATCH (d:Spoke), (h:Hub) CALL refactor.cloneNodesWithRelationships([h], {}) YIELD output RETURN output")
        .close();

    assertThat(countOf("MATCH (h:Hub) RETURN count(h) AS c"))
        .as("the two originals plus one clone per (driver, hub) row").isEqualTo(2 + 2L * DRIVERS);
    assertThat(countOf("MATCH ()-[r:LINK]->() RETURN count(r) AS c"))
        .as("(N+1)^2, so no clone was taken from a stale edge list")
        .isEqualTo((long) (DRIVERS + 1) * (DRIVERS + 1));
  }

  /**
   * A single clone call whose node list holds both endpoints of many parallel edges: the clone of the source is
   * reused as the ORIGIN of every copied edge, and each append moves its edge-list head, so the second append
   * onto it works from what the map stored when the clone was saved.
   * <p>
   * It stays green because the append path substitutes the transaction's own written copy of a vertex by RID
   * ({@code GraphEngine.getOrCreateEdgeList}), which is why {@code cloneEdge} re-reads neither endpoint. This
   * test and its IN-side twin below are what say so: remove that substitution and both go red, while the
   * row-carried enumeration above is the one place with no such safety net.
   */
  @Test
  void cloneNodesWithRelationshipsCopiesEveryParallelEdgeOfOneCall() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Hub {id: 1}), (:Hub {id: 2})");
      database.command("opencypher",
          "UNWIND range(1, 300) AS i MATCH (a:Hub {id:1}), (b:Hub {id:2}) CREATE (a)-[:LINK {seq: i}]->(b)");
    });

    database.command("opencypher",
        "MATCH (a:Hub {id:1}), (b:Hub {id:2}) CALL refactor.cloneNodesWithRelationships([a, b], {}) YIELD output RETURN output")
        .close();

    assertThat(countOf("MATCH ()-[r:LINK]->() RETURN count(r) AS c"))
        .as("300 originals and 300 copies between the two clones").isEqualTo(600);
  }

  /**
   * The same call with the reuse on the other side: 200 distinct sources, all cloned, every one of them
   * pointing at ONE target that is cloned too, so the clone of that target is the IN endpoint of 200 appends
   * in a single call. The counterpart of the test above, and the case that settles that neither end of
   * {@code cloneEdge} needs a re-read.
   */
  @Test
  void cloneNodesWithRelationshipsCopiesEveryEdgeIntoOneSharedTargetOfOneCall() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Target {id: 0})");
      database.command("opencypher",
          "UNWIND range(1, 200) AS i MATCH (t:Target) CREATE (s:Spoke {id: i})-[:LINK {seq: i}]->(t)");
    });

    database.command("opencypher",
        "MATCH (t:Target) WITH t MATCH (s:Spoke) WITH collect(s) + [t] AS nodes "
            + "CALL refactor.cloneNodesWithRelationships(nodes, {}) YIELD output RETURN output").close();

    assertThat(countOf("MATCH (:Spoke)-[r:LINK]->(:Target) RETURN count(r) AS c"))
        .as("200 originals and 200 copies, none lost to a stale edge-list head on the shared target")
        .isEqualTo(400);
    assertThat(countOf("MATCH (t:Target) RETURN count(t) AS c")).as("the target and its clone").isEqualTo(2);
  }

  /**
   * One survivor absorbing a node per row of the same query. The survivor is the instance every row shares,
   * and it is the record each row copies properties onto, saves, and rewires edges to.
   * <p>
   * Unlike the clone test above this shape passed before the re-read too - the rows happened to carry a fresh
   * survivor - so it is coverage for the shape rather than a reproducer. It is what would go red if that
   * incidental freshness ever went away, which is the reason the procedure no longer depends on it.
   */
  @Test
  void mergeNodesAccumulatesOnTheSurvivorHoweverManyRowsReachIt() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Hub {id: 0, tag: 'h'}), (:Target {id: 0})");
      database.command("opencypher",
          "UNWIND range(1, 120) AS i MATCH (t:Target) CREATE (s:Spoke {tag: 'S' + toString(i)})-[:LINK {seq: i}]->(t)");
    });

    database.command("opencypher",
        "MATCH (s:Spoke), (h:Hub) CALL refactor.mergeNodes([h, s], {properties: 'combine'}) YIELD node RETURN node")
        .close();

    assertThat(countOf("MATCH (h:Hub)-[r:LINK]->(:Target) RETURN count(r) AS c"))
        .as("every absorbed node's edge rewired onto the survivor").isEqualTo(120);
    assertThat(countOf("MATCH (s:Spoke) RETURN count(s) AS c")).as("every absorbed node deleted").isZero();
    assertThat(this.<List<?>>propertyOf("MATCH (h:Hub) RETURN h.tag AS c"))
        .as("the survivor's own tag plus one per absorbed node").hasSize(121);
  }

  /**
   * The same survivor absorbing many nodes inside ONE call, which is the other half of the shape: the loop
   * merges each absorbed node's properties onto the survivor, saves it, then rewires that node's edges to it -
   * so from the second iteration on it is saving an instance whose edge list the previous iteration changed.
   * It holds, for the same reason the clone tests above hold: rewiring goes through the edge records, and the
   * append path resolves the endpoint by RID.
   */
  @Test
  void mergeNodesAbsorbsEveryNodeOfOneCall() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Hub {id: 0, tag: 'h'}), (:Target {id: 0})");
      database.command("opencypher",
          "UNWIND range(1, 50) AS i MATCH (t:Target) CREATE (s:Spoke {tag: 'S' + toString(i)})-[:LINK {seq: i}]->(t)");
    });

    database.command("opencypher",
        "MATCH (h:Hub) WITH h MATCH (s:Spoke) WITH [h] + collect(s) AS nodes "
            + "CALL refactor.mergeNodes(nodes, {properties: 'combine'}) YIELD node RETURN node").close();

    assertThat(countOf("MATCH (h:Hub)-[r:LINK]->(:Target) RETURN count(r) AS c"))
        .as("all 50 edges rewired, including the ones rewired by earlier iterations of the same call")
        .isEqualTo(50);
    assertThat(countOf("MATCH (s:Spoke) RETURN count(s) AS c")).isZero();
    assertThat(this.<List<?>>propertyOf("MATCH (h:Hub) RETURN h.tag AS c"))
        .as("and every absorbed node's property accumulated on the survivor").hasSize(51);
  }

  private long countOf(final String query) {
    return ((Number) propertyOf(query)).longValue();
  }

  @SuppressWarnings("unchecked")
  private <T> T propertyOf(final String query) {
    try (final ResultSet resultSet = database.query("opencypher", query)) {
      return (T) resultSet.next().getProperty("c");
    }
  }
}
