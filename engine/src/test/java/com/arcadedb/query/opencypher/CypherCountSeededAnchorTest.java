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
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A {@code COUNT { }} body anchored on the outer row's vertex is answered by the seeded chain push-down, which walks
 * from the vertex the row already holds and sums the last hop without collecting it. Its answer has to be the one the
 * ordinary pipeline gives for the same pattern - here forced by a {@code WITH}, which no push-down accepts - for every
 * direction, a labelled or unlabelled far end, two hops, the anchor at either end, and a vertex whose adjacency changed
 * earlier in the same statement.
 */
class CypherCountSeededAnchorTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-count-seeded").create();
    database.transaction(() -> {
      database.getSchema().createVertexType("P");
      database.getSchema().createVertexType("Q");
      database.getSchema().createEdgeType("L");
      database.getSchema().createEdgeType("M");
      for (int i = 0; i < 60; i++)
        database.newVertex(i % 3 == 0 ? "Q" : "P").set("id", i).set("pad", "x".repeat(i % 4 == 0 ? 20_000 : 10)).save();
      for (int i = 1; i < 60; i++) {
        link("L", i, i / 2);
        if (i % 5 == 0)
          link("L", i, (i * 7) % 60);
        if (i % 4 == 0)
          link("M", i, i - 1);
      }
      link("L", 7, 7); // a self-loop
      link("L", 8, 4); // a second 8 -> 4, so a two-hop walk from 8 reaches 4 along two paths
    });
  }

  private void link(final String type, final int from, final int to) {
    database.command("opencypher", "MATCH (a {id: $a}), (b {id: $b}) CREATE (a)-[:" + type + "]->(b)",
        Map.of("a", from, "b", to));
  }

  private Vertex vertex(final int id) {
    try (final ResultSet rs = database.query("opencypher", "MATCH (n {id: $id}) RETURN n", Map.of("id", id))) {
      return rs.next().getVertex().orElseThrow();
    }
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  private Map<Integer, Long> counts(final String body, final boolean pipeline) {
    final String count = pipeline ? "COUNT { MATCH " + body + " WITH 1 AS one RETURN one }" : "COUNT { " + body + " }";
    final Map<Integer, Long> out = new LinkedHashMap<>();
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (n) RETURN n.id AS id, " + count + " AS c ORDER BY id")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        out.put(((Number) r.getProperty("id")).intValue(), ((Number) r.getProperty("c")).longValue());
      }
    }
    return out;
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "(n)-[:L]->()", "(n)<-[:L]-()", "(n)-[:L]->(:Q)", "(n)<-[:L]-(:P)", "(n)-[:L]->(:Nope)",
      "(n:Q)-[:L]->()", "(n)-[:L]->()-[:M]->()", "(n)<-[:L]-(:P)<-[:M]-()", "()-[:L]->(n)", "(:Q)-[:M]->()-[:L]->(n)" })
  void pushDownAgreesWithThePipeline(final String body) {
    final Map<Integer, Long> pushed = counts(body, false);
    final long total = pushed.values().stream().mapToLong(Long::longValue).sum();
    if (body.contains("Nope"))
      assertThat(total).isZero();
    else
      assertThat(total).as("the data exercises the pattern").isPositive();
    assertThat(pushed).isEqualTo(counts(body, true));
  }

  @Test
  void undirectedCountsWhatThePushDownCountedBefore() {
    // (n)-[:L]-() reads both lists, so the self-loop on 7 is one entry in each; that is the push-down's answer
    // with or without a loaded anchor, and it must not change here.
    final Map<Integer, Long> both = counts("(n)-[:L]-()", false);
    final Map<Integer, Long> out = counts("(n)-[:L]->()", false);
    final Map<Integer, Long> in = counts("(n)<-[:L]-()", false);
    for (final Integer id : both.keySet())
      assertThat(both.get(id)).as("id %d", id).isEqualTo(out.get(id) + in.get(id));
  }

  @Test
  void seesAnEdgeCreatedEarlierInTheSameStatement() {
    // Neither 58 nor 59 has an incoming L before this, so the CREATE gives each of them an incoming-list head the
    // handles the row holds for them predate.
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "MATCH (a {id: 58}), (b {id: 59}) CREATE (b)-[:L]->(a), (a)-[:L]->(b) "
              + "RETURN COUNT { (a)-[:L]->() } AS aOut, COUNT { (b)<-[:L]-() } AS bIn, COUNT { (a)-[:L]-() } AS aBoth")) {
        final Result r = rs.next();
        assertThat(((Number) r.getProperty("aOut")).longValue()).isEqualTo(2L); // 58 -> 29 and the new 58 -> 59
        assertThat(((Number) r.getProperty("bIn")).longValue()).isEqualTo(1L);
        assertThat(((Number) r.getProperty("aBoth")).longValue()).isEqualTo(3L);
      }
    });
  }

  @Test
  void doesNotSeeAnEdgeDeletedEarlierInTheSameStatement() {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "MATCH (a {id: 58})-[r:L]->() DELETE r RETURN COUNT { (a)-[:L]->() } AS aOut, COUNT { (a)-[:L]-() } AS aBoth")) {
        final Result r = rs.next();
        assertThat(((Number) r.getProperty("aOut")).longValue()).isZero();
        assertThat(((Number) r.getProperty("aBoth")).longValue()).isZero();
      }
    });
  }

  @Test
  void anAnchorBoundAsARidAloneIsStillCounted() {
    // Not every bound value is a vertex object: a RID names the anchor without carrying its edge lists, and the
    // walk loads it, as it did for every anchor before this change.
    final RID rid = vertex(8).getIdentity();
    try (final ResultSet rs = database.query("opencypher", "WITH $v AS n RETURN COUNT { (n)-[:L]->() } AS c",
        Map.of("v", rid))) {
      assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(2L); // 8 -> 4 twice
    }
  }

  @Test
  void aStaleHandleIsCountedFromTheTransactionsCopy() {
    // `stale` is a mutable copy the transaction never cached; the edge is added through a second copy, which the
    // transaction does cache. 0 has no outgoing L before, so the edge gives it an outgoing-list head `stale` lacks.
    // The push-down loaded the anchor by RID, which answers the cached copy, so it counted the edge; reading the
    // row's handle must not change that.
    database.transaction(() -> {
      final Vertex loaded = vertex(0);
      final MutableVertex stale = loaded.modify();
      final MutableVertex current = loaded.modify();
      current.newEdge("L", vertex(58));
      try (final ResultSet rs = database.query("opencypher", "WITH $v AS n RETURN COUNT { (n)-[:L]->() } AS c",
          Map.of("v", stale))) {
        assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(1L); // the new 0 -> 58
      }
    });
  }
}
