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
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A relationship variable nobody reads has to be planned as anonymous on both branches of
 * {@code CypherOptimizer.buildExpansionChain}: the {@code ExpandAll} branch already asked whether
 * the variable is read before deciding to materialize the edge, but the {@code ExpandInto} branch
 * (bound-target hops, e.g. the closing edge of a cycle) took {@code relationship.getVariable()}
 * straight off the pattern. That kept a named-but-unread variable off the CSR-backed
 * {@code GAVExpandInto} path even though the identical hop, walked anonymously, would use it.
 */
class CypherExpandIntoUnreadEdgeVariableTest {
  // `r` closes the cycle back onto the anchor (bound-target / ExpandInto branch) but is never read.
  private static final String CYCLE = "MATCH (a:Account {code: 'HUB'})-[:INITIATED]->(t:Txn {ref: 'SHARED'})"
      + "-[r:SETTLED]->(a) RETURN count(*) AS c";

  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/cypherexpandintounreadedgevar");
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

      // Two ways out and two ways back, so the cycle can be walked four times.
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
  void namedButUnreadRelVarOnBoundTargetHopReachesGAVExpandInto() {
    // Without a view covering the edge type there is nothing to pin the hop to, so it plans as the
    // edge-list ExpandInto. Asserting this first pins down that the view below is what flips it.
    assertThat(planOf(CYCLE)).contains("ExpandInto").doesNotContain("GAVExpandInto");
    final long withoutView = countOf(CYCLE);

    final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
        .withName("cycle-view")
        .withVertexTypes("Account", "Txn")
        .withEdgeTypes("INITIATED", "SETTLED")
        .build();
    try {
      // The closing hop names `r` but nothing reads it, so it must be planned exactly like the
      // anonymous first hop: off the CSR adjacency view, not the OLTP edge list.
      assertThat(planOf(CYCLE)).contains("GAVExpandInto");
      assertThat(countOf(CYCLE)).isEqualTo(withoutView);
    } finally {
      view.drop();
    }

    assertThat(countOf(CYCLE)).isEqualTo(withoutView);
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
