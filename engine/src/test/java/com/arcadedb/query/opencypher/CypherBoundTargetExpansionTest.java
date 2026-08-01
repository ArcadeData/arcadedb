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
 * When a relationship pattern's target is already pinned to a vertex, the step narrows the source's
 * edge list to the edges that reach it, filtering on the neighbour pointer in the edge segment rather
 * than materialising every edge and rejecting it afterwards. Without that, a guard such as
 * {@code NOT EXISTS { (a)-[:E {...}]->(t) }} costs one record load per edge of {@code a}, which on a
 * hub vertex is hundreds of thousands of random reads to answer a question about a single edge.
 * <p>
 * These cases pin the value of the optimisation to the thing that must not change: the answer. Each
 * one is a shape where a pre-filter applied too eagerly - before the type, property, direction or
 * uniqueness rules - would silently drop a row.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherBoundTargetExpansionTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/cypherboundtarget");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    database.transaction(() -> {
      database.getSchema().createVertexType("Account");
      database.getSchema().createVertexType("Txn");
      database.getSchema().createEdgeType("INITIATED");
      database.getSchema().createEdgeType("SETTLED");

      final MutableVertex hub = database.newVertex("Account").set("code", "HUB").save();
      final MutableVertex other = database.newVertex("Account").set("code", "OTHER").save();

      // The hub reaches many leaves, so a guard about one of them has to ignore all the others.
      for (int i = 0; i < 200; i++) {
        final MutableVertex txn = database.newVertex("Txn").set("ref", "T" + i).save();
        hub.newEdge("INITIATED", txn, "ref", "T" + i, "kind", "payment").save();
      }

      // A leaf reached twice by different edge types and different properties.
      final MutableVertex shared = database.newVertex("Txn").set("ref", "SHARED").save();
      hub.newEdge("INITIATED", shared, "ref", "SHARED", "kind", "payment").save();
      hub.newEdge("INITIATED", shared, "ref", "SHARED", "kind", "refund").save();
      hub.newEdge("SETTLED", shared, "ref", "SHARED").save();

      // An edge in the opposite direction, and one from a different source.
      shared.newEdge("INITIATED", hub, "ref", "REVERSED").save();
      other.newEdge("INITIATED", shared, "ref", "FROM_OTHER").save();

      // A never-linked leaf: the case a loader's guard hits on every insert.
      database.newVertex("Txn").set("ref", "UNLINKED").save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void existsGuardAnswersTheSameOnLinkedAndUnlinkedLeaves() {
    // SHARED is reached by a payment edge and a refund edge, so both guards see the edge...
    assertThat(guardSaysAbsent("SHARED", "payment")).isFalse();
    assertThat(guardSaysAbsent("SHARED", "refund")).isFalse();
    // ...but a kind that no edge of that pair carries is still reported absent, so narrowing the edge
    // list did not weaken the property filter into a plain connectivity check
    assertThat(guardSaysAbsent("SHARED", "chargeback")).isTrue();
    assertThat(guardSaysAbsent("T0", "payment")).isFalse();
    assertThat(guardSaysAbsent("UNLINKED", "payment")).isTrue();
  }

  private boolean guardSaysAbsent(final String ref, final String kind) {
    final String cypher = "MATCH (a:Account {code: 'HUB'}), (t:Txn {ref: $ref}) "
        + "WHERE NOT EXISTS { (a)-[:INITIATED {ref: $ref, kind: $kind}]->(t) } RETURN a";
    try (final ResultSet rs = database.query("opencypher", cypher, "ref", ref, "kind", kind)) {
      return rs.hasNext();
    }
  }

  @Test
  void narrowingKeepsEveryParallelEdgeBetweenThePair() {
    // Two INITIATED edges reach SHARED; a filter that stopped at the first would lose one.
    assertThat(refsOf("MATCH (a:Account {code: 'HUB'}), (t:Txn {ref: 'SHARED'}) "
        + "MATCH (a)-[r:INITIATED]->(t) RETURN r.kind AS v"))
        .containsExactlyInAnyOrder("payment", "refund");
  }

  @Test
  void narrowingRespectsTheEdgeType() {
    assertThat(refsOf("MATCH (a:Account {code: 'HUB'}), (t:Txn {ref: 'SHARED'}) "
        + "MATCH (a)-[r:SETTLED]->(t) RETURN r.ref AS v")).containsExactly("SHARED");
  }

  @Test
  void narrowingRespectsTheDirection() {
    assertThat(refsOf("MATCH (a:Account {code: 'HUB'}), (t:Txn {ref: 'SHARED'}) "
        + "MATCH (a)<-[r:INITIATED]-(t) RETURN r.ref AS v")).containsExactly("REVERSED");

    assertThat(refsOf("MATCH (a:Account {code: 'HUB'}), (t:Txn {ref: 'SHARED'}) "
        + "MATCH (a)-[r:INITIATED]-(t) RETURN r.ref AS v"))
        .containsExactlyInAnyOrder("SHARED", "SHARED", "REVERSED");
  }

  @Test
  void narrowingRespectsAnInlineWhereOnTheRelationship() {
    assertThat(refsOf("MATCH (a:Account {code: 'HUB'}), (t:Txn {ref: 'SHARED'}) "
        + "MATCH (a)-[r:INITIATED WHERE r.kind = 'refund']->(t) RETURN r.kind AS v"))
        .containsExactly("refund");
  }

  @Test
  void narrowingDoesNotLeakEdgesFromAnotherSource() {
    // OTHER also points at SHARED; pinning the target must not pull that edge into the HUB's expansion.
    assertThat(refsOf("MATCH (a:Account {code: 'HUB'}), (t:Txn {ref: 'SHARED'}) "
        + "MATCH (a)-[r:INITIATED]->(t) RETURN r.ref AS v"))
        .doesNotContain("FROM_OTHER");
  }

  /**
   * A cycle closes on a variable bound by an earlier hop, so the closing hop expands with its target already
   * pinned - the narrowing engages on a source the same statement has been traversing, and must still find the
   * one edge that closes the cycle.
   * <p>
   * The pair is joined by two parallel INITIATED edges going out and one coming back, so the cycle can be walked
   * two ways, one per first-hop edge. The closing hop is an expansion like any other, not a "is the cycle closed?"
   * question asked once for the pair - answering it once under-counted every such walk (issue #5663).
   */
  @Test
  void narrowingHandlesACyclePatternWhereTheLastHopReturnsToABoundVariable() {
    // the closing hop resolves to the only edge that can close the cycle, once per walk that reaches it
    assertThat(refsOf("MATCH (a:Account {code: 'HUB'})-[r1:INITIATED]->(t:Txn {ref: 'SHARED'})-[r2:INITIATED]->(a) "
        + "RETURN r2.ref AS v")).containsExactly("REVERSED", "REVERSED");

    // and the first hop binds each edge of the HUB->SHARED pair exactly once
    assertThat(refsOf("MATCH (a:Account {code: 'HUB'})-[r1:INITIATED]->(t:Txn {ref: 'SHARED'})-[r2:INITIATED]->(a) "
        + "RETURN r1.kind AS v")).containsExactlyInAnyOrder("payment", "refund");

    // a cycle that cannot close returns nothing, so the narrowing is not answering "connected" for any target
    assertThat(refsOf("MATCH (a:Account {code: 'HUB'})-[r1:INITIATED]->(t:Txn {ref: 'T0'})-[r2:INITIATED]->(a) "
        + "RETURN r2.ref AS v")).isEmpty();
  }

  @Test
  void unpinnedTargetStillSeesTheWholeEdgeList() {
    // The narrowing must not engage when the target is free, or the hub's 200 leaves would vanish.
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:Account {code: 'HUB'})-[:INITIATED]->(t:Txn) RETURN count(t) AS c")) {
      assertThat(((Number) rs.next().getProperty("c")).intValue()).isEqualTo(202);
    }
  }

  private List<String> refsOf(final String cypher) {
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
