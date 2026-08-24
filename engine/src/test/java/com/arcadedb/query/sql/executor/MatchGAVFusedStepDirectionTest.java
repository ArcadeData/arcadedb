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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for the {@code MatchGAVFusedStep} sibling of #6670, found during code review: the fused
 * multi-hop chain step ({@link MatchExecutionPlanner#collectFusibleChain}/{@code isFusibleEdge}) derived each
 * hop's direction from the schedule's forward/reverse flag ({@code EdgeTraversal.out}) rather than the pattern's
 * actual out/in/both method - the same root cause this PR fixes for the single-hop expand-into fast path, just
 * in the chain-fusion path instead. {@code isFusibleEdge} explicitly allows {@code in()}/{@code both()} hops
 * into a fused chain, so a chain with one of those and a GAV registered silently queried the wrong direction.
 * <p>
 * {@code createPlanForPattern} always runs the schedule's first edge through the regular (correct)
 * {@code MatchStep}/{@code MatchEdgeTraverser} path - {@code collectFusibleChain} only starts looking from the
 * second edge onward, and only actually uses {@link MatchGAVFusedStep} once it collects 2+ fusible edges there.
 * A 2-hop pattern therefore never fuses (one edge is always peeled off first, leaving only one to "chain"); these
 * tests use a 3-hop pattern so the second and third hops - the ones that actually go through
 * {@code MatchGAVFusedStep} - carry the {@code in()}/{@code both()} method under test.
 * <p>
 * {@code MatchGAVFusedStep} does not evaluate each hop's own {@code where} clause (a separate, pre-existing
 * limitation of the fused path, out of scope here), so each hop in the fixture uses its own edge type -
 * FOLLOWS/KNOWS/KNOWS2 - to stay unambiguous by graph shape alone rather than relying on a filter the fused step
 * would silently ignore. Sharing one KNOWS type across both fused hops would let the middle vertex's own
 * outgoing edge (needed to test the second hop) also satisfy the first hop's direction check, masking exactly
 * the bug this test exists to catch.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class MatchGAVFusedStepDirectionTest extends TestHelper {

  @Test
  void fusedChainRespectsInMethodDirection() {
    // Alice -FOLLOWS-> Bob (hop 0, not fused). Eve -KNOWS-> Bob and Eve -KNOWS2-> Dave (hops 1-2, fused):
    // b.in('KNOWS') must find Eve via the INCOMING edge (Bob has no outgoing KNOWS edge at all, so the buggy
    // OUT-only fused query finds nothing here), then c.out('KNOWS2') must find Dave.
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("FOLLOWS");
    database.getSchema().createEdgeType("KNOWS");
    database.getSchema().createEdgeType("KNOWS2");

    database.begin();
    final MutableVertex alice = database.newVertex("Person").set("name", "Alice").save();
    final MutableVertex bob = database.newVertex("Person").set("name", "Bob").save();
    final MutableVertex eve = database.newVertex("Person").set("name", "Eve").save();
    final MutableVertex dave = database.newVertex("Person").set("name", "Dave").save();
    alice.newEdge("FOLLOWS", bob);
    eve.newEdge("KNOWS", bob);
    eve.newEdge("KNOWS2", dave);
    database.commit();

    // Built via the builder (not `new GraphAnalyticalView(db)`, which never registers itself as a
    // GraphTraversalProvider) so the query planner actually discovers and fuses this chain.
    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withVertexTypes("Person")
        .withEdgeTypes("FOLLOWS", "KNOWS", "KNOWS2")
        .build();
    try {
      database.begin();
      final ResultSet rs = database.query("sql",
          """
          MATCH {type: Person, as: a, where: (name = 'Alice')}.out('FOLLOWS'){as: b}.in('KNOWS'){as: c}\
          .out('KNOWS2'){as: d} \
          RETURN a.name as aName, b.name as bName, c.name as cName, d.name as dName""");

      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(row.<String>getProperty("aName")).isEqualTo("Alice");
      assertThat(row.<String>getProperty("bName")).isEqualTo("Bob");
      assertThat(row.<String>getProperty("cName")).isEqualTo("Eve");
      assertThat(row.<String>getProperty("dName")).isEqualTo("Dave");
      assertThat(rs.hasNext()).isFalse();
      database.commit();
    } finally {
      gav.drop();
    }
  }

  @Test
  void fusedChainRespectsBothMethodDirection() {
    // Same shape as fusedChainRespectsInMethodDirection(), but with both() hops: b.both('KNOWS') must find Eve
    // regardless of edge direction (Bob has only an INCOMING KNOWS edge, so the buggy code - which "both"
    // degrades to whenever the schedule doesn't happen to reverse this hop - finds nothing here), then
    // c.both('KNOWS2') must find Dave.
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("FOLLOWS");
    database.getSchema().createEdgeType("KNOWS");
    database.getSchema().createEdgeType("KNOWS2");

    database.begin();
    final MutableVertex alice = database.newVertex("Person").set("name", "Alice").save();
    final MutableVertex bob = database.newVertex("Person").set("name", "Bob").save();
    final MutableVertex eve = database.newVertex("Person").set("name", "Eve").save();
    final MutableVertex dave = database.newVertex("Person").set("name", "Dave").save();
    alice.newEdge("FOLLOWS", bob);
    eve.newEdge("KNOWS", bob);
    eve.newEdge("KNOWS2", dave);
    database.commit();

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withVertexTypes("Person")
        .withEdgeTypes("FOLLOWS", "KNOWS", "KNOWS2")
        .build();
    try {
      database.begin();
      final ResultSet rs = database.query("sql",
          """
          MATCH {type: Person, as: a, where: (name = 'Alice')}.out('FOLLOWS'){as: b}.both('KNOWS'){as: c}\
          .both('KNOWS2'){as: d} \
          RETURN a.name as aName, b.name as bName, c.name as cName, d.name as dName""");

      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(row.<String>getProperty("aName")).isEqualTo("Alice");
      assertThat(row.<String>getProperty("bName")).isEqualTo("Bob");
      assertThat(row.<String>getProperty("cName")).isEqualTo("Eve");
      assertThat(row.<String>getProperty("dName")).isEqualTo("Dave");
      assertThat(rs.hasNext()).isFalse();
      database.commit();
    } finally {
      gav.drop();
    }
  }
}
