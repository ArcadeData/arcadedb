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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A hop whose target is already bound (⭐ BOUND-TARGET in the plan) does not just check that the pair
 * connects - it emits one row per edge joining the pair. {@code DEFAULT_EXPAND_INTO_SELECTIVITY} alone
 * encodes only "does the pair connect at all", which is right on a simple graph but backwards on a
 * multigraph: a pair joined by several parallel edges must scale the estimate up, not clamp it to the
 * bare filter probability (issue #5690).
 */
class CypherExpandIntoMultiplicityCardinalityTest {
  private static final Pattern BOUND_TARGET_ROWS = Pattern.compile("rows=(\\d+)] .*BOUND-TARGET");

  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/cypherexpandintomultiplicity");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    database.transaction(() -> {
      database.getSchema().createVertexType("Account");
      database.getSchema().createVertexType("Txn");
      database.getSchema().createEdgeType("INITIATED");
      database.getSchema().createEdgeType("SETTLED");

      // Ten accounts and ten transactions so the anchor's filtered-scan selectivity heuristic lands on
      // a round, deterministic estimate instead of a single-row type collapsing to zero.
      MutableVertex hub = null;
      for (int i = 0; i < 10; i++) {
        final String code = i == 0 ? "HUB" : "A" + i;
        final MutableVertex account = database.newVertex("Account").set("code", code).save();
        if (i == 0)
          hub = account;
      }

      MutableVertex shared = null;
      for (int i = 0; i < 10; i++) {
        final MutableVertex txn = database.newVertex("Txn").set("ref", "T" + i).save();
        if (i == 0)
          shared = txn;
      }

      // One initiating edge, just to make the cycle walkable; its own estimate is not under test here.
      hub.newEdge("INITIATED", shared, true, (Object[]) null);

      // Five parallel SETTLED edges joining the one connected pair: a real multiplicity of 5.
      for (int i = 0; i < 5; i++)
        shared.newEdge("SETTLED", hub, true, (Object[]) null);

      // Same multiplicity, stored in the opposite direction, so the IN-direction bound-target hop
      // below exercises a distinct edge type and cannot be conflated with SETTLED's statistic.
      database.getSchema().createEdgeType("SETTLEDBACK");
      for (int i = 0; i < 5; i++)
        hub.newEdge("SETTLEDBACK", shared, true, (Object[]) null);

      // A third type with a real multiplicity of 1, distinct from SETTLED's 5, so a union hop over
      // both types can tell "average" (3) apart from "sum" (6).
      database.getSchema().createEdgeType("LOWMULT");
      hub.newEdge("LOWMULT", shared, true, (Object[]) null);
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void boundTargetHopScalesTheEstimateByTheSampledMultiplicity() {
    final String cycle = "MATCH (a:Account {code: 'HUB'})-[:INITIATED]->(t:Txn)-[:SETTLED]->(a) RETURN a, t";

    final String plan = planOf(cycle);
    // Plain ExpandInto, not the CSR-backed GAV variant - no GraphAnalyticalView was ever created here.
    assertThat(plan).contains("ExpandInto").doesNotContain("GAVExpandInto");

    // Anchor 'a': 10 accounts, filtered-scan selectivity 0.1 -> 1.
    // ExpandAll INITIATED (a->t): 1 * DEFAULT_AVG_DEGREE(10) -> 10.
    // ExpandInto SETTLED (t->a, bound-target): 10 * DEFAULT_EXPAND_INTO_SELECTIVITY(0.1) * meanEdgesPerConnectedPair(5) -> 5.
    // Before the fix this line reads rows=1 (the plain 10 * 0.1, ignoring the type's real multiplicity of 5).
    assertThat(boundTargetRows(plan)).isEqualTo(5L);
  }

  @Test
  void boundTargetHopScalesTheEstimateRegardlessOfHopDirection() {
    // Same shape, but the closing hop is written IN (arrow points at the intermediate node) instead
    // of OUT. The statistic is keyed by edge type, not by query direction, so the estimate must still
    // reflect SETTLEDBACK's real multiplicity of 5.
    final String cycle = "MATCH (a:Account {code: 'HUB'})-[:INITIATED]->(t:Txn)<-[:SETTLEDBACK]-(a) RETURN a, t";

    final String plan = planOf(cycle);
    assertThat(plan).contains("ExpandInto").doesNotContain("GAVExpandInto");
    assertThat(boundTargetRows(plan)).isEqualTo(5L);
  }

  @Test
  void boundTargetHopAveragesRatherThanSumsAcrossUnionTypes() {
    // SETTLED has multiplicity 5, LOWMULT has multiplicity 1. Averaging gives 3; summing would give 6.
    final String cycle = "MATCH (a:Account {code: 'HUB'})-[:INITIATED]->(t:Txn)-[:SETTLED|LOWMULT]->(a) RETURN a, t";

    final String plan = planOf(cycle);
    assertThat(plan).contains("ExpandInto").doesNotContain("GAVExpandInto");
    assertThat(boundTargetRows(plan)).isEqualTo(3L);
  }

  @Test
  void untypedBoundTargetHopKeepsThePlainSelectivity() {
    // No edge type restriction on the closing hop: cannot be attributed to any type's statistic, so
    // multiplicity stays 1.0 and the estimate is the plain 10 * DEFAULT_EXPAND_INTO_SELECTIVITY(0.1).
    final String cycle = "MATCH (a:Account {code: 'HUB'})-[:INITIATED]->(t:Txn)-[]->(a) RETURN a, t";

    final String plan = planOf(cycle);
    assertThat(plan).contains("ExpandInto").doesNotContain("GAVExpandInto");
    assertThat(boundTargetRows(plan)).isEqualTo(1L);
  }

  private long boundTargetRows(final String plan) {
    for (final String line : plan.split("\n")) {
      final Matcher matcher = BOUND_TARGET_ROWS.matcher(line);
      if (matcher.find())
        return Long.parseLong(matcher.group(1));
    }
    throw new AssertionError("No BOUND-TARGET operator found in plan:\n" + plan);
  }

  private String planOf(final String cypher) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + cypher)) {
      return rs.next().getProperty("executionPlanAsString");
    }
  }
}
