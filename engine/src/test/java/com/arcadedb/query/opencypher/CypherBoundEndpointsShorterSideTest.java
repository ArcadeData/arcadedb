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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.PageManager;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * MERGE and MATCH of a relationship between two bound nodes read the edges off whichever node has the shorter edge
 * list (issue #11699). They used to walk the list of the node written first, so {@code MERGE (hub)-[:T]->(leaf)} cost
 * the hub's degree and the same statement written {@code MERGE (leaf)<-[:T]-(hub)} cost one entry. An endpoint keyed
 * by an equality on a UNIQUE index is sought first, so {@code MATCH (a {k: 'hub'})-[:T]->(b {k: 'leaf'})} binds both
 * ends too, in the cost-based plan and in the traditional one.
 * <p>
 * Every answer is asserted from both written orders, over parallel edges, a self loop, undirected patterns,
 * relationship properties, lightweight twins and a unidirectional type, for the MATCH shapes that bind both ends
 * (ExpandInto, the relationship step with a bound target) and for MERGE matching and creating.
 */
class CypherBoundEndpointsShorterSideTest extends TestHelper {
  private static final String BOTH_BOUND = "MATCH (a:N {k: 'a'}) MATCH (b:N {k: 'b'}) ";
  private long totalEdges;

  @Test
  void matchFindsEveryRelationshipFromEitherEnd() {
    buildPair();

    for (final String pattern : new String[] { "(a)-[r:T]->(b)", "(b)<-[r:T]-(a)" }) {
      assertThat(ints(BOTH_BOUND + "MATCH " + pattern + " RETURN r.p AS v ORDER BY v")).as(pattern).containsExactly(1, 2, 3);
      assertThat(ints(BOTH_BOUND + "MATCH " + pattern.replace("r:T", "r:T {p: 2}") + " RETURN r.p AS v")).as(pattern)
          .containsExactly(2);
    }
    assertThat(ints("MATCH (a:N {k: 'a'})-[r:T]->(b:N {k: 'b'}) RETURN r.p AS v ORDER BY v")).containsExactly(1, 2, 3);
    assertThat(ints("MATCH (b:N {k: 'b'})<-[r:T]-(a:N {k: 'a'}) RETURN r.p AS v ORDER BY v")).containsExactly(1, 2, 3);
    assertThat(ints("MATCH (a:N {k: 'a'}), (b:N {k: 'b'}) WITH a, b MATCH (a)-[r:T]->(b) RETURN r.p AS v ORDER BY v"))
        .containsExactly(1, 2, 3);

    // BOTH ENDS KEYED INLINE: THE COST-BASED PLAN (NO RELATIONSHIP PROPERTIES) AND THE TRADITIONAL ONE (WITH THEM)
    assertThat(ints("MATCH (a:N {k: 'a'})-[r:T {p: 2}]->(b:N {k: 'b'}) RETURN r.p AS v")).containsExactly(2);
    assertThat(ints("MATCH (b:N {k: 'b'})<-[r:T {p: 2}]-(a:N {k: 'a'}) RETURN r.p AS v")).containsExactly(2);
    assertThat(ints("MATCH (a:N {k: 'a'})-[r:T {p: 9}]-(b:N {k: 'b'}) RETURN r.p AS v")).containsExactly(9);
    assertThat(ints("MATCH p = (a:N {k: 'a'})-[:T]->(b:N {k: 'b'}) RETURN length(p) AS v")).containsExactly(1, 1, 1);
    assertThat(count("MATCH (a:N {k: 'a'})-[:T]->(b:N {k: 'b'}) RETURN count(*) AS v")).isEqualTo(3L);
    assertThat(count("MATCH (a:N {k: 'a'})-[:T {p: 2}]->(b:N {k: 'b'}) RETURN count(*) AS v")).isEqualTo(1L);
    assertThat(count("MATCH (a:N {k: 'a'})-[:T]->(b:N {k: 'missing'}) RETURN count(*) AS v")).isEqualTo(0L);
    assertThat(count("MATCH (a:N {k: 'a'})-[:T {p: 2}]->(b:N {k: 'missing'}) RETURN count(*) AS v")).isEqualTo(0L);
    assertThat(count("MATCH (a:N {k: 'a'})-[:T]->(b:N {k: 'f1'}) RETURN count(*) AS v")).isEqualTo(1L);
    assertThat(count("MATCH (a:N {k: 'a'})-[:T {p: 100}]->(b:N {k: 'f1'}) RETURN count(*) AS v")).isEqualTo(1L);
    assertThat(count("MATCH (a:N {k: 'a'})-[:L]->(b:N {k: 'b'}) RETURN count(*) AS v")).isEqualTo(2L);
    assertThat(count("MATCH (a:N {k: 'a'})-[:U]->(b:N {k: 'b'}) RETURN count(*) AS v")).isEqualTo(1L);
    assertThat(count("MATCH (b:N {k: 'b'})<-[:U]-(a:N {k: 'a'}) RETURN count(*) AS v")).isEqualTo(1L);
    assertThat(count("MATCH (a:N {k: 'a'})-[:T]-(a2:N {k: 'a'}) RETURN count(*) AS v")).isEqualTo(1L);

    // UNDIRECTED: THE REVERSE RELATIONSHIP COUNTS TOO
    assertThat(ints(BOTH_BOUND + "MATCH (a)-[r:T]-(b) RETURN r.p AS v ORDER BY v")).containsExactly(1, 2, 3, 9);
    assertThat(ints(BOTH_BOUND + "MATCH (b)-[r:T]-(a) RETURN r.p AS v ORDER BY v")).containsExactly(1, 2, 3, 9);

    // SELF LOOP: ONE RELATIONSHIP, DIRECTED OR NOT
    assertThat(ints("MATCH (a:N {k: 'a'}) MATCH (a)-[r:T]->(a) RETURN r.p AS v")).containsExactly(7);
    assertThat(ints("MATCH (a:N {k: 'a'}) MATCH (a)-[r:T]-(a) RETURN r.p AS v")).containsExactly(7);

    // LIGHTWEIGHT TWINS ARE TWO RELATIONSHIPS
    assertThat(count(BOTH_BOUND + "MATCH (a)-[r:L]->(b) RETURN count(r) AS v")).isEqualTo(2L);
    assertThat(count(BOTH_BOUND + "MATCH (b)<-[r:L]-(a) RETURN count(r) AS v")).isEqualTo(2L);

    // UNIDIRECTIONAL: STORED ON THE SOURCE ONLY, FOUND FROM EITHER END
    assertThat(count(BOTH_BOUND + "MATCH (a)-[r:U]->(b) RETURN count(r) AS v")).isEqualTo(1L);
    assertThat(count(BOTH_BOUND + "MATCH (b)<-[r:U]-(a) RETURN count(r) AS v")).isEqualTo(1L);

    // THE TARGET KEYED BY A VARIABLE AN EARLIER CLAUSE BOUND (AN UNWIND OR WITH ROW): SOUGHT PER ROW, A MISSING KEY IS NO ROW
    assertThat(ints("UNWIND ['b', 'missing', 'f1'] AS t MATCH (a:N {k: 'a'})-[r:T]->(b:N {k: t}) RETURN r.p AS v ORDER BY v"))
        .containsExactly(1, 2, 3, 100);
    assertThat(ints("UNWIND ['b', 'f1'] AS t MATCH (a:N {k: 'a'})-[r:T {p: 2}]->(b:N {k: t}) RETURN r.p AS v"))
        .containsExactly(2);
    assertThat(ints("WITH 'b' AS t MATCH (b:N {k: t})<-[r:T]-(a:N {k: 'a'}) RETURN r.p AS v ORDER BY v"))
        .containsExactly(1, 2, 3);
    assertThat(ints("UNWIND [{k: 'b'}] AS row MATCH (a:N {k: 'a'})-[r:T]->(b:N {k: row.k}) RETURN r.p AS v ORDER BY v"))
        .containsExactly(1, 2, 3);
    // A KEY READING A VARIABLE OF THE SAME PATTERN HAS NOTHING TO READ BEFORE THE HOP: STILL A FILTER, SAME ANSWER
    assertThat(ints("MATCH (a:N {k: 'a'})-[r:T]->(b:N {k: a.k}) RETURN r.p AS v")).containsExactly(7);
  }

  @Test
  void mergeMatchesFromEitherEnd() {
    buildPair();

    for (final String pattern : new String[] { "(a)-[r:T {p: 2}]->(b)", "(b)<-[r:T {p: 2}]-(a)" })
      assertThat(ints(BOTH_BOUND + "MERGE " + pattern + " RETURN r.p AS v")).as(pattern).containsExactly(2);
    for (final String pattern : new String[] { "(a)-[r:T]->(b)", "(b)<-[r:T]-(a)" })
      assertThat(ints(BOTH_BOUND + "MERGE " + pattern + " RETURN r.p AS v ORDER BY v")).as(pattern).containsExactly(1, 2, 3);
    assertThat(directed("T")).isEqualTo(3L);

    // UNDIRECTED MATCHES THE REVERSE RELATIONSHIP
    assertThat(ints(BOTH_BOUND + "MERGE (a)-[r:T {p: 9}]-(b) RETURN r.p AS v")).containsExactly(9);
    assertThat(ints(BOTH_BOUND + "MERGE (b)-[r:T {p: 9}]-(a) RETURN r.p AS v")).containsExactly(9);

    // SELF LOOP
    assertThat(ints("MATCH (a:N {k: 'a'}) MERGE (a)-[r:T {p: 7}]->(a) RETURN r.p AS v")).containsExactly(7);

    // LIGHTWEIGHT TWINS: BOTH MATCH, NONE CREATED
    assertThat(count(BOTH_BOUND + "MERGE (a)-[r:L]->(b) RETURN count(*) AS v")).isEqualTo(2L);
    assertThat(count(BOTH_BOUND + "MERGE (b)<-[r:L]-(a) RETURN count(*) AS v")).isEqualTo(2L);
    assertThat(directed("L")).isEqualTo(2L);

    // UNIDIRECTIONAL
    assertThat(count(BOTH_BOUND + "MERGE (a)-[r:U]->(b) RETURN count(*) AS v")).isEqualTo(1L);
    assertThat(count(BOTH_BOUND + "MERGE (b)<-[r:U]-(a) RETURN count(*) AS v")).isEqualTo(1L);
    assertThat(directed("U")).isEqualTo(1L);

    assertThat(count("MATCH ()-[r]->() RETURN count(r) AS v")).isEqualTo(totalEdges);
  }

  @Test
  void mergeCreatesOnceFromEitherEnd() {
    buildPair();

    assertThat(ints(BOTH_BOUND + "MERGE (a)-[r:T {p: 5}]->(b) RETURN r.p AS v")).containsExactly(5);
    assertThat(ints(BOTH_BOUND + "MERGE (b)<-[r:T {p: 5}]-(a) RETURN r.p AS v")).containsExactly(5);
    assertThat(ints(BOTH_BOUND + "MERGE (b)<-[r:T {p: 6}]-(a) RETURN r.p AS v")).containsExactly(6);
    assertThat(ints(BOTH_BOUND + "MERGE (a)-[r:T {p: 6}]->(b) RETURN r.p AS v")).containsExactly(6);
    assertThat(ints(BOTH_BOUND + "MERGE (a)-[r:T {p: 10}]-(b) RETURN r.p AS v")).containsExactly(10);
    assertThat(ints(BOTH_BOUND + "MERGE (b)-[r:T {p: 10}]-(a) RETURN r.p AS v")).containsExactly(10);
    assertThat(directed("T")).isEqualTo(6L);
    assertThat(ints(BOTH_BOUND + "MATCH (a)-[r:T]->(b) RETURN r.p AS v ORDER BY v")).containsExactly(1, 2, 3, 5, 6, 10);

    // A BATCH OF ROWS, EACH ITS OWN MERGE: THE SECOND ROW SEES THE EDGE THE FIRST CREATED
    final List<Map<String, Object>> rows = List.of(Map.of("p", 11), Map.of("p", 11), Map.of("p", 12));
    database.command("opencypher",
        "UNWIND $rows AS row " + BOTH_BOUND + "MERGE (b)<-[:T {p: row.p}]-(a)", Map.of("rows", rows));
    database.command("opencypher",
        "UNWIND $rows AS row " + BOTH_BOUND + "MERGE (a)-[:T {p: row.p}]->(b)", Map.of("rows", rows));
    assertThat(directed("T")).isEqualTo(8L);

    assertThat(count("MATCH ()-[r]->() RETURN count(r) AS v")).isEqualTo(totalEdges + 5);
  }

  /**
   * The cost of the statements binding both ends is the leaf's degree: a hub of 32,000 relationships reads the pages a
   * hub of 2,000 reads. Before the fix MERGE loaded every relationship of the hub, and ExpandInto walked the hub's list.
   */
  @Test
  void costDoesNotGrowWithTheHubDegree() {
    database.getConfiguration().setValue(GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD, 0);
    createSchema();
    hub("small", 2_000);
    hub("large", 32_000);

    for (final String statement : new String[] {
        "MATCH (a:N {k: $hub}) MATCH (b:N {k: $leaf}) MERGE (a)-[r:T {p: 1}]->(b) RETURN count(r) AS v",
        "MATCH (a:N {k: $hub}) MATCH (b:N {k: $leaf}) MATCH (a)-[r:T]->(b) RETURN count(r) AS v",
        "MATCH (a:N {k: $hub}) MATCH (b:N {k: $leaf}) MATCH (a)-[r:T {p: 1}]->(b) RETURN count(r) AS v",
        "MATCH (a:N {k: $hub})-[r:T]->(b:N {k: $leaf}) RETURN count(r) AS v",
        "MATCH (a:N {k: $hub})-[:T]->(b:N {k: $leaf}) RETURN count(*) AS v",
        "MATCH (a:N {k: $hub})-[r:T {p: 1}]->(b:N {k: $leaf}) RETURN count(r) AS v",
        "MATCH (b:N {k: $leaf})<-[r:T {p: 1}]-(a:N {k: $hub}) RETURN count(r) AS v",
        "UNWIND [$leaf] AS t MATCH (a:N {k: $hub})-[r:T]->(b:N {k: t}) RETURN count(r) AS v",
        "UNWIND [$leaf] AS t MATCH (a:N {k: $hub})-[r:T {p: 1}]->(b:N {k: t}) RETURN count(r) AS v",
        "WITH $leaf AS t MATCH (a:N {k: $hub})-[r:T]->(b:N {k: t}) RETURN count(r) AS v" }) {
      final long small = pages(statement, "small");
      final long large = pages(statement, "large");
      assertThat(large).as("pages read by %s on a 32,000-edge hub, against %d on a 2,000-edge one", statement, small)
          .isLessThanOrEqualTo(small + 4);
    }
  }

  /** Keys written in WHERE rather than inline: the chain count declines for them as for inline keys. */
  @Test
  void costOfWhereKeyedCountDoesNotGrowWithTheHubDegree() {
    assertCostIndependentOfHubDegree("MATCH (a:N)-[:T]->(b:N) WHERE a.k = $hub AND b.k = $leaf RETURN count(*) AS v");
  }

  /** An anonymous far end keyed on a UNIQUE index is sought as a named one is. */
  @Test
  void costOfAnonymousKeyedFarEndDoesNotGrowWithTheHubDegree() {
    assertCostIndependentOfHubDegree("MATCH (a:N {k: $hub})-[r:T]->(:N {k: $leaf}) RETURN count(r) AS v");
  }

  private void assertCostIndependentOfHubDegree(final String statement) {
    database.getConfiguration().setValue(GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD, 0);
    createSchema();
    hub("small", 2_000);
    hub("large", 32_000);
    final long small = pages(statement, "small");
    final long large = pages(statement, "large");
    assertThat(large).as("pages read by %s on a 32,000-edge hub, against %d on a 2,000-edge one", statement, small)
        .isLessThanOrEqualTo(small + 4);
  }

  private void buildPair() {
    createSchema();
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("N").set("k", "a").save();
      final MutableVertex b = database.newVertex("N").set("k", "b").save();
      // THE HUB'S LISTS ARE LONG, THE LEAF'S SHORT: THE ENGINE READS THE PAIR OFF THE LEAF
      for (int i = 0; i < 300; i++) {
        final MutableVertex filler = database.newVertex("N").set("k", "f" + i).save();
        a.newEdge("T", filler, "p", 100);
        a.newEdge("L", filler);
        a.newEdge("U", filler);
        filler.newEdge("T", a, "p", 100);
      }
      for (int p = 1; p <= 3; p++)
        a.newEdge("T", b, "p", p);
      b.newEdge("T", a, "p", 9);
      a.newEdge("T", a, "p", 7);
      a.newEdge("L", b);
      a.newEdge("L", b);
      a.newEdge("U", b);
    });
    totalEdges = count("MATCH ()-[r]->() RETURN count(r) AS v");
  }

  private void createSchema() {
    database.command("sql", "CREATE VERTEX TYPE N");
    database.command("sql", "CREATE PROPERTY N.k STRING");
    database.command("sql", "CREATE INDEX ON N (k) UNIQUE");
    database.command("sql", "CREATE EDGE TYPE T");
    database.command("sql", "CREATE EDGE TYPE L LIGHTWEIGHT");
    database.command("sql", "CREATE EDGE TYPE U UNIDIRECTIONAL");
  }

  private void hub(final String name, final int degree) {
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("N").set("k", name).save();
      final MutableVertex leaf = database.newVertex("N").set("k", name + "-leaf").save();
      hub.newEdge("T", leaf, "p", 1);
      for (int i = 0; i < degree; i++)
        hub.newEdge("T", database.newVertex("N").set("k", name + i).save(), "p", 2);
    });
  }

  /** The page-cache lookups of one run of {@code statement}, after a warm-up run. */
  private long pages(final String statement, final String hub) {
    final Map<String, Object> params = Map.of("hub", hub, "leaf", hub + "-leaf");
    assertThat(count(statement, params)).isEqualTo(1L);
    final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
    final PageManager.PPageManagerStats before = pageManager.getStats();
    assertThat(count(statement, params)).isEqualTo(1L);
    final PageManager.PPageManagerStats after = pageManager.getStats();
    return after.cacheHits + after.cacheMiss - before.cacheHits - before.cacheMiss;
  }

  private long directed(final String type) {
    return count(BOTH_BOUND + "MATCH (a)-[r:" + type + "]->(b) RETURN count(r) AS v");
  }

  private long count(final String query) {
    return count(query, Map.of());
  }

  private long count(final String query, final Map<String, Object> params) {
    try (final ResultSet rs = database.command("opencypher", query, params)) {
      return ((Number) rs.next().getProperty("v")).longValue();
    }
  }

  private List<Integer> ints(final String query) {
    final List<Integer> values = new ArrayList<>();
    try (final ResultSet rs = database.command("opencypher", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        values.add(((Number) row.getProperty("v")).intValue());
      }
    }
    return values;
  }
}
