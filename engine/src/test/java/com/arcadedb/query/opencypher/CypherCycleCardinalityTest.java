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
 * A hop whose far end is already bound is still an expansion: it matches once per relationship joining the
 * pair, not once per pair. Answering it once collapsed every walk of a cycle over a multigraph into a single
 * row, so anything aggregating over the cycle silently under-reported (issue #5663).
 * <p>
 * The fixture is the smallest graph where the two readings differ: two parallel edges out of {@code hub} and
 * one coming back, so the cycle can be walked twice - each walk binds a different first edge and closes
 * through the same returning edge, which relationship uniqueness allows because the two are distinct edges.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherCycleCardinalityTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/cyphercyclecardinality");
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

      hub.newEdge("INITIATED", shared, "kind", "payment").save();
      hub.newEdge("INITIATED", shared, "kind", "refund").save();
      shared.newEdge("INITIATED", hub, "ref", "REVERSED").save();

      // A leaf the cycle cannot close through, so a hop that answered "connected" would still be caught.
      final MutableVertex lonely = database.newVertex("Txn").set("ref", "LONELY").save();
      hub.newEdge("INITIATED", lonely, "kind", "payment").save();

      // A pair of accounts joined by two edges each way, for the undirected and self-loop shapes.
      final MutableVertex peer = database.newVertex("Account").set("code", "PEER").save();
      hub.newEdge("SETTLED", peer, "seq", 1).save();
      hub.newEdge("SETTLED", peer, "seq", 2).save();
      peer.newEdge("SETTLED", hub, "seq", 3).save();
      peer.newEdge("SETTLED", peer, "seq", 4).save();
      peer.newEdge("SETTLED", peer, "seq", 5).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void aCycleReportsOneRowPerFirstHopEdge() {
    assertThat(valuesOf("MATCH (a:Account {code: 'HUB'})-[r1:INITIATED]->(t:Txn {ref: 'SHARED'})-[r2:INITIATED]->(a) "
        + "RETURN r1.kind AS v")).containsExactlyInAnyOrder("payment", "refund");
  }

  @Test
  void aCycleReportsTheClosingEdgeOncePerFirstHopEdge() {
    assertThat(valuesOf("MATCH (a:Account {code: 'HUB'})-[r1:INITIATED]->(t:Txn {ref: 'SHARED'})-[r2:INITIATED]->(a) "
        + "RETURN r2.ref AS v")).containsExactly("REVERSED", "REVERSED");
  }

  @Test
  void aCycleCountsEveryWalk() {
    assertThat(countOf("MATCH (a:Account {code: 'HUB'})-[r1:INITIATED]->(t:Txn {ref: 'SHARED'})-[r2:INITIATED]->(a) "
        + "RETURN count(*) AS c")).isEqualTo(2);
  }

  @Test
  void anAnonymousCycleCountsEveryWalk() {
    // An unnamed relationship is still a relationship: the pattern matches once per edge either way.
    assertThat(countOf("MATCH (a:Account {code: 'HUB'})-[:INITIATED]->(t:Txn {ref: 'SHARED'})-[:INITIATED]->(a) "
        + "RETURN count(*) AS c")).isEqualTo(2);
  }

  @Test
  void aCycleThatCannotCloseReturnsNothing() {
    assertThat(valuesOf("MATCH (a:Account {code: 'HUB'})-[r1:INITIATED]->(t:Txn {ref: 'LONELY'})-[r2:INITIATED]->(a) "
        + "RETURN r2.ref AS v")).isEmpty();
  }

  @Test
  void aClosingHopStillRefusesToReuseTheEdgeTheFirstHopBound() {
    // Undirected, so either of the three INITIATED edges joining the pair can open the cycle and any other
    // one can close it: 3 x 3 assignments minus the 3 where the closing hop would reuse the edge the first
    // hop bound, which Cypher forbids.
    assertThat(countOf("MATCH (a:Account {code: 'HUB'})-[r1:INITIATED]-(t:Txn {ref: 'SHARED'})-[r2:INITIATED]-(a) "
        + "RETURN count(*) AS c")).isEqualTo(6);

    // and the same when neither relationship is named, which is where the edge has to be tracked implicitly
    assertThat(countOf("MATCH (a:Account {code: 'HUB'})-[:INITIATED]-(t:Txn {ref: 'SHARED'})-[:INITIATED]-(a) "
        + "RETURN count(*) AS c")).isEqualTo(6);
  }

  @Test
  void anUndirectedClosingHopCountsEveryRelationshipJoiningThePair() {
    // HUB->PEER twice and PEER->HUB once: three relationships join the pair, so the undirected hop that
    // closes onto the bound PEER sees all three and keeps the two that are not the edge the first hop
    // bound - twice over, once per directed first hop.
    assertThat(countOf("MATCH (a:Account {code: 'HUB'})-[r1:SETTLED]->(p:Account {code: 'PEER'}) "
        + "RETURN count(*) AS c")).isEqualTo(2);
    assertThat(valuesOf("MATCH (a:Account {code: 'HUB'})-[r1:SETTLED]->(p:Account {code: 'PEER'})-[r2:SETTLED]-(a) "
        + "RETURN r2.seq AS v")).containsExactlyInAnyOrder("2", "3", "1", "3");
  }

  @Test
  void aSelfLoopClosingOnItsOwnVertexIsCountedOnce() {
    // PEER carries two self-loops. An undirected hop reaches each of them from both adjacency lists, but a
    // relationship reached twice is still one relationship.
    assertThat(valuesOf("MATCH (p:Account {code: 'PEER'})-[r:SETTLED]-(p) RETURN r.seq AS v"))
        .containsExactlyInAnyOrder("4", "5");
    assertThat(valuesOf("MATCH (p:Account {code: 'PEER'})-[r:SETTLED]->(p) RETURN r.seq AS v"))
        .containsExactlyInAnyOrder("4", "5");
  }

  /**
   * A pattern is an expansion where it appears in MATCH and a question where it appears as a predicate.
   * The two must not be confused now that the bound-target hop multiplies rows: an existence test over a
   * pair joined twice answers once, or every guard over a multigraph would over-report.
   */
  @Test
  void anExistencePredicateOverAPairJoinedTwiceStillAnswersOnce() {
    assertThat(countOf("MATCH (a:Account {code: 'HUB'}), (t:Txn {ref: 'SHARED'}) "
        + "WHERE (a)-[:INITIATED]->(t) RETURN count(*) AS c")).isEqualTo(1);

    assertThat(countOf("MATCH (a:Account {code: 'HUB'}), (t:Txn {ref: 'SHARED'}) "
        + "WHERE EXISTS { (a)-[:INITIATED]->(t) } RETURN count(*) AS c")).isEqualTo(1);

    // and the negation of the same question still admits a pair nothing joins, exactly once
    assertThat(countOf("MATCH (a:Account {code: 'HUB'}), (t:Txn {ref: 'SHARED'}) "
        + "WHERE NOT EXISTS { (t)-[:SETTLED]->(a) } RETURN count(*) AS c")).isEqualTo(1);

    // ...while the same pattern written in MATCH is an expansion and reports both edges
    assertThat(countOf("MATCH (a:Account {code: 'HUB'}), (t:Txn {ref: 'SHARED'}) "
        + "MATCH (a)-[:INITIATED]->(t) RETURN count(*) AS c")).isEqualTo(2);
  }

  private long countOf(final String cypher) {
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private List<String> valuesOf(final String cypher) {
    final List<String> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        values.add(row.getProperty("v") != null ? row.getProperty("v").toString() : null);
      }
    }
    return values;
  }
}
