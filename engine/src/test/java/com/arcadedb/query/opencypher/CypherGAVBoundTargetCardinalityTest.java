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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The CSR-backed hop onto a pinned target answers with adjacency ids instead of edge records, so it has to
 * derive the multiplicity of the pair from the adjacency array rather than collapsing it to "connected"
 * (issue #5663). It must agree, row for row, with the same query planned without the view.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherGAVBoundTargetCardinalityTest {
  private static final String CYCLE = "MATCH (a:Account {code: 'HUB'})-[:INITIATED]->(t:Txn {ref: 'SHARED'})-[:SETTLED]->(a) "
      + "RETURN count(*) AS c";

  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/cyphergavboundtarget");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    database.transaction(() -> {
      database.getSchema().createVertexType("Account");
      database.getSchema().createVertexType("Txn");
      database.getSchema().createEdgeType("INITIATED");
      database.getSchema().createEdgeType("SETTLED");

      final MutableVertex hub = database.newVertex("Account").set("code", "HUB").save();
      final MutableVertex shared = database.newVertex("Txn").set("ref", "SHARED").save();

      // Two ways out and two ways back, so the cycle can be walked four times
      hub.newEdge("INITIATED", shared, "kind", "payment").save();
      hub.newEdge("INITIATED", shared, "kind", "refund").save();
      shared.newEdge("SETTLED", hub, "seq", 1).save();
      shared.newEdge("SETTLED", hub, "seq", 2).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void theViewCountsEveryWalkOfTheCycle() {
    // The hops carry disjoint edge types and bind nothing, so no edge object is needed and the closing
    // hop is free to run off the view.
    final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
        .withName("cycle-view")
        .withVertexTypes("Account", "Txn")
        .withEdgeTypes("INITIATED", "SETTLED")
        .build();
    try {
      assertThat(planOf(CYCLE)).contains("GAVExpandInto");
      assertThat(countOf(CYCLE)).isEqualTo(4);
    } finally {
      view.drop();
    }

    assertThat(countOf(CYCLE)).isEqualTo(4);
  }

  @Test
  void theViewAgreesWhenNoEdgeClosesTheCycle() {
    final String unclosable = "MATCH (a:Account {code: 'HUB'})-[:SETTLED]->(t:Txn {ref: 'SHARED'})-[:INITIATED]->(a) "
        + "RETURN count(*) AS c";
    assertThat(countOf(unclosable)).isZero();

    final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
        .withName("cycle-view")
        .withVertexTypes("Account", "Txn")
        .withEdgeTypes("INITIATED", "SETTLED")
        .build();
    try {
      assertThat(countOf(unclosable)).isZero();
    } finally {
      view.drop();
    }
  }

  /**
   * The view's overlay masks a pending deletion per (source, target) pair, so deleting one of the two
   * SETTLED edges hides both from the CSR. The count it can no longer state exactly must not become the
   * number of rows: the operator falls back to the edge list, which still holds the surviving edge.
   */
  @Test
  void theViewFallsBackToTheEdgeListWhenADeletionMasksThePair() {
    final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
        .withName("cycle-view-sync")
        .withVertexTypes("Account", "Txn")
        .withEdgeTypes("INITIATED", "SETTLED")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS)
        .build();
    try {
      assertThat(planOf(CYCLE)).contains("GAVExpandInto");
      assertThat(countOf(CYCLE)).isEqualTo(4);

      // Drop one of the two SETTLED edges closing the cycle: 2 first hops x 1 remaining closing edge
      database.transaction(() -> {
        for (final Edge edge : database.query("opencypher", "MATCH (t:Txn {ref: 'SHARED'})-[r:SETTLED]->(:Account) RETURN r")
            .stream().map(r -> (Edge) r.getProperty("r")).toList()) {
          edge.delete();
          break;
        }
      });

      assertThat(countOf(CYCLE)).isEqualTo(2);
    } finally {
      view.drop();
    }
  }

  private long countOf(final String cypher) {
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private String planOf(final String cypher) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + cypher)) {
      return rs.next().getProperty("executionPlanAsString");
    }
  }
}
