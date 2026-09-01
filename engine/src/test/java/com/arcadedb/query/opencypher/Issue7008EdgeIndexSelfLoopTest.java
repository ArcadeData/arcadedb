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
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproduction for GitHub issue #7008.
 * <p>
 * The edge-index scan seed introduced for issue #740 accepts a single-hop pattern without noticing that
 * the two endpoint variables may be the <em>same</em> variable. For the explicit self-loop shape
 * {@code (a)-[t:TRANSFER]->(a)} the seed bound the edge's OUT vertex to {@code a} and then immediately
 * overwrote it with the IN vertex, so the pattern's implicit {@code out == in} constraint was silently
 * dropped and edges between two different vertices came back as if they were self-loops.
 */
class Issue7008EdgeIndexSelfLoopTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/issue-7008-edge-index-self-loop").create();

    database.getSchema().createVertexType("Account");
    final var transfer = database.getSchema().createEdgeType("TRANSFER");
    transfer.createProperty("transactionId", Type.STRING);
    transfer.createProperty("date", Type.DATE);
    database.getSchema().buildTypeIndex("TRANSFER", new String[] { "transactionId" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();
    // A second, composite index. The seed picks the longest index its equality predicates fully cover, so a
    // WHERE on transactionId alone still seeks the single-property one and a WHERE on both seeks this one.
    database.getSchema().buildTypeIndex("TRANSFER", new String[] { "transactionId", "date" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();

    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Account").set("name", "A").save();
      final MutableVertex b = database.newVertex("Account").set("name", "B").save();
      final MutableVertex c = database.newVertex("Account").set("name", "C").save();

      // A plain edge between two DIFFERENT accounts: no self-loop shape must ever match it.
      a.newEdge("TRANSFER", b).set("transactionId", "74529884").set("date", "2026-02-28").save();
      // A genuine self-loop on C: the self-loop shape must find exactly this one.
      c.newEdge("TRANSFER", c).set("transactionId", "11112222").set("date", "2026-02-28").save();
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
  void repeatedEndpointVariableDoesNotMatchANonSelfLoopEdge() {
    // The edge carrying '74529884' runs A -> B, so the self-loop pattern must return nothing.
    assertThat(namesOf("MATCH (a)-[t:TRANSFER]->(a) WHERE t.transactionId = '74529884' RETURN a.name AS n"))
        .isEmpty();
  }

  @Test
  void controlDistinctEndpointVariablesStillSeeTheNonSelfLoopEdge() {
    // Control for the assertion above: with two distinct variables the same edge IS matched, and its
    // endpoints really are two different accounts.
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (x)-[t:TRANSFER]->(y) WHERE t.transactionId = '74529884' RETURN x.name AS xn, y.name AS yn")) {
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      assertThat((String) r.getProperty("xn")).isEqualTo("A");
      assertThat((String) r.getProperty("yn")).isEqualTo("B");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void repeatedEndpointVariableFindsAGenuineSelfLoop() {
    // The fix must not simply refuse every repeated-variable pattern: the real self-loop is still found.
    assertThat(namesOf("MATCH (a)-[t:TRANSFER]->(a) WHERE t.transactionId = '11112222' RETURN a.name AS n"))
        .containsExactly("C");
  }

  @Test
  void repeatedEndpointVariableHonoursTheIncomingDirectionForm() {
    // The written direction only decides which endpoint binds to which variable; for a repeated variable
    // both forms must behave identically.
    assertThat(namesOf("MATCH (a)<-[t:TRANSFER]-(a) WHERE t.transactionId = '74529884' RETURN a.name AS n"))
        .isEmpty();
    assertThat(namesOf("MATCH (a)<-[t:TRANSFER]-(a) WHERE t.transactionId = '11112222' RETURN a.name AS n"))
        .containsExactly("C");
  }

  @Test
  void repeatedEndpointVariableWithoutAWhereFilterIsStillCorrect() {
    // Without a WHERE on the edge the index seed does not apply at all; this pins the reference semantics
    // the seeded plan has to agree with.
    assertThat(namesOf("MATCH (a)-[t:TRANSFER]->(a) RETURN a.name AS n")).containsExactly("C");
  }

  @Test
  void selfLoopPatternStillUsesTheEdgeIndexSeed() {
    // The self-loop constraint has to be enforced without giving up the index speedup issue #740 added.
    // Pinning the plan shape is what keeps this file honest: the alternative fix - refusing the seed for a
    // repeated variable - would make every assertion above pass while silently losing the optimization.
    assertThat(profiledPlanOf("MATCH (a)-[t:TRANSFER]->(a) WHERE t.transactionId = '11112222' RETURN a.name AS n"))
        .contains("MATCH EDGE BY INDEX");
  }

  @Test
  void selfLoopConstraintSurvivesACompositeIndexSeek() {
    // A multi-column key drives a different branch of the seed's index choice, so the constraint has to hold
    // there too. Assert the composite index really is the one seeded, otherwise this only re-tests the
    // single-property path under a longer WHERE clause.
    final String nonSelfLoop = "MATCH (a)-[t:TRANSFER]->(a) "
        + "WHERE t.transactionId = '74529884' AND t.date = date('2026-02-28') RETURN a.name AS n";
    assertThat(profiledPlanOf(nonSelfLoop)).contains("on transactionId,date");
    assertThat(namesOf(nonSelfLoop)).isEmpty();

    final String selfLoop = "MATCH (a)-[t:TRANSFER]->(a) "
        + "WHERE t.transactionId = '11112222' AND t.date = date('2026-02-28') RETURN a.name AS n";
    assertThat(profiledPlanOf(selfLoop)).contains("on transactionId,date");
    assertThat(namesOf(selfLoop)).containsExactly("C");
  }

  @Test
  void labelledEndpointRoutesToTheOptimizerAndStaysCorrect() {
    // A label on the endpoint makes the cost-based optimizer accept the pattern (ExpandInto BOUND-TARGET)
    // instead of the legacy seed, so this pins the OTHER execution path against the same defect.
    final String nonSelfLoop =
        "MATCH (a:Account)-[t:TRANSFER]->(a) WHERE t.transactionId = '74529884' RETURN a.name AS n";
    assertThat(planOf(nonSelfLoop)).contains("Cost-Based Query Optimizer");
    assertThat(namesOf(nonSelfLoop)).isEmpty();

    final String selfLoop =
        "MATCH (a:Account)-[t:TRANSFER]->(a) WHERE t.transactionId = '11112222' RETURN a.name AS n";
    assertThat(planOf(selfLoop)).contains("Cost-Based Query Optimizer");
    assertThat(namesOf(selfLoop)).containsExactly("C");
  }

  /** Drains a PROFILEd run so the plan reflects what actually executed, not just what was planned. */
  private String profiledPlanOf(final String cypher) {
    try (final ResultSet rs = database.query("opencypher", "PROFILE " + cypher)) {
      while (rs.hasNext())
        rs.next();
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }

  private String planOf(final String cypher) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + cypher)) {
      assertThat(rs.hasNext()).isTrue();
      return rs.next().getProperty("executionPlanAsString");
    }
  }

  private List<String> namesOf(final String cypher) {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      while (rs.hasNext())
        names.add(rs.next().getProperty("n"));
    }
    return names;
  }
}
