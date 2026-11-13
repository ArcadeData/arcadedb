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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code MATCH (a {key: $k})-[r]->(b) WHERE b.key IN $list} with a UNIQUE key on {@code b} is answered per row by the
 * cheaper of walking the relationships of {@code a} and seeking {@code b} per key, each pair then read off its shorter edge
 * list (issue #11715). It used to walk every relationship of {@code a} whatever the list.
 * <p>
 * Every answer is compared with the walk itself: each vertex carries its key twice, {@code k} under a UNIQUE index and
 * {@code m} under none, and the same statement over {@code m} is the plan that walks.
 */
class CypherFarEndInListSeekTest extends TestHelper {
  private static final int    HUB_DEGREE = 4_000;
  private static final String FORWARD    = "MATCH (a:N {k: $h})-[r:T]->(b:N) WHERE b.k IN $ts RETURN b.k AS id, r.p AS p";

  @Test
  void plansTheSeekOrWalkOnlyForAUniqueKey() {
    createSchema();
    database.command("sql", "CREATE PROPERTY N.nu STRING");
    database.command("sql", "CREATE INDEX ON N (nu) NOTUNIQUE");
    database.transaction(() -> database.newVertex("N").set("k", "hub").set("m", "hub").save());

    for (final String query : new String[] { FORWARD,
        "MATCH (a:N)-[r:T]->(b:N) WHERE a.k = $h AND b.k IN $ts RETURN b.k AS id",
        "MATCH (a:N {k: $h})<-[r:T]-(b:N) WHERE b.k IN $ts RETURN b.k AS id",
        "MATCH (a:N {k: $h})-[r:T]-(b:N) WHERE b.k IN $ts RETURN b.k AS id",
        "MATCH (a:N {k: $h})-[:T]->(b:N) WHERE b.k IN ['x1', 'x2'] RETURN b.k AS id",
        "MATCH (a:N {k: $h})-[:T]->(b:N) WHERE b.k = 'x1' OR b.k = 'x2' RETURN b.k AS id",
        "MATCH (a:N {k: $h})-[:T]->(b:N) WHERE b.k IN $ts RETURN count(*) AS c" })
      assertThat(explain(query)).as(query).contains("SeekOrWalkExpand(a)").contains("NodeIndexSeek(b:N)");

    // NOT A UNIQUE KEY, OR NO INDEX: THE HOP IS NOT SOUGHT PER KEY
    for (final String query : new String[] {
        "MATCH (a:N {k: $h})-[r:T]->(b:N) WHERE b.nu IN $ts RETURN b.k AS id",
        "MATCH (a:N {k: $h})-[r:T]->(b:N) WHERE b.m IN $ts RETURN b.k AS id" })
      assertThat(explain(query)).as(query).doesNotContain("SeekOrWalkExpand");
    assertThat(explain("MATCH (a:N {k: $h})-[r:T]->(b:N) WHERE b.m IN $ts RETURN b.k AS id")).contains("ExpandAll(a)-[:T]->(b:N)");
  }

  @Test
  void answersAsTheWalkDoes() {
    buildGraph();

    final List<String> connected = new ArrayList<>();
    for (int i = 0; i < 200; i++)
      connected.add("x" + (i * (HUB_DEGREE / 200)));
    final List<List<String>> lists = new ArrayList<>();
    lists.add(connected.subList(0, 1));
    lists.add(connected.subList(0, 10));
    lists.add(connected);
    lists.add(List.of("x3", "x3", "x5", "x3"));            // DUPLICATES; x3 IS JOINED TWICE TO THE HUB
    lists.add(List.of("free0", "free1", "x5"));             // VERTICES THE HUB DOES NOT REACH
    lists.add(List.of("missing", "nowhere", "x9"));         // NO SUCH VERTEX
    lists.add(Arrays.asList("x5", null, "x7"));             // A NULL ELEMENT MATCHES NOTHING
    lists.add(List.of());
    lists.add(List.of("hub", "x1"));                        // THE HUB'S SELF LOOP
    lists.add(List.of("r0", "r1", "x2"));                   // REVERSE RELATIONSHIPS ONLY

    final List<String> statements = List.of(FORWARD,
        "MATCH (a:N {k: $h})<-[r:T]-(b:N) WHERE b.k IN $ts RETURN b.k AS id, r.p AS p",
        "MATCH (a:N {k: $h})-[r:T]-(b:N) WHERE b.k IN $ts RETURN b.k AS id, r.p AS p",
        "MATCH (a:N)-[r:T]->(b:N) WHERE a.k = $h AND b.k IN $ts RETURN b.k AS id, r.p AS p",
        "MATCH (b:N)<-[r:T]-(a:N {k: $h}) WHERE b.k IN $ts RETURN b.k AS id, r.p AS p",
        "MATCH (a:N {k: $h})-[:T]->(b:N) WHERE b.k IN $ts RETURN b.k AS id, 0 AS p",
        "MATCH (a:N {k: $h})-[:T]->(b:N) WHERE b.k IN $ts RETURN count(*) AS id, 0 AS p",
        "MATCH (a:N {k: $h})-[:L]->(b:N) WHERE b.k IN $ts RETURN b.k AS id, 0 AS p",
        "MATCH (a:N {k: $h})<-[:D]-(b:N) WHERE b.k IN $ts RETURN b.k AS id, 0 AS p",
        "MATCH (a:N {k: $h})-[:D]-(b:N) WHERE b.k IN $ts RETURN b.k AS id, 0 AS p",
        "MATCH (a:N {k: $h}) OPTIONAL MATCH (a)-[r:T]->(b:N) WHERE b.k IN $ts RETURN b.k AS id, r.p AS p",
        "MATCH (a:N {k: $h})-[r1:T]->(b:N)-[r2:T]->(c:N) WHERE b.k IN $ts RETURN b.k + c.k AS id, r1.p AS p");

    // THE HUB SEEKS, THE SMALL VERTEX WALKS (A FEW RELATIONSHIPS, LONG LISTS), THE MISSING ONE MATCHES NOTHING
    for (final String hub : new String[] { "hub", "small", "missing" })
      for (final String statement : statements)
        for (final List<String> list : lists)
          assertThat(rows(statement, hub, list)).as("%s from %s over %s", statement, hub, list)
              .isEqualTo(rows(statement.replace("b.k IN", "b.m IN"), hub, list));

    // KEYS IN A LIST LITERAL AND AS AN OR OF EQUALITIES
    for (final String hub : new String[] { "hub", "small" }) {
      assertThat(rows("MATCH (a:N {k: $h})-[r:T]->(b:N) WHERE b.k IN ['x3', 'x5', null, 'free0'] RETURN b.k AS id, r.p AS p", hub, null))
          .isEqualTo(rows("MATCH (a:N {k: $h})-[r:T]->(b:N) WHERE b.m IN ['x3', 'x5', null, 'free0'] RETURN b.k AS id, r.p AS p", hub, null));
      assertThat(rows("MATCH (a:N {k: $h})-[r:T]->(b:N) WHERE b.k = 'x3' OR b.k = 'x8' RETURN b.k AS id, r.p AS p", hub, null))
          .isEqualTo(rows("MATCH (a:N {k: $h})-[r:T]->(b:N) WHERE b.m = 'x3' OR b.m = 'x8' RETURN b.k AS id, r.p AS p", hub, null));
    }
    assertThat(rows(FORWARD, "hub", List.of("x3", "x5"))).containsExactly("x3/1", "x3/2", "x5/1");
    assertThat(rows(FORWARD, "small", List.of("s1", "s1", "x3"))).containsExactly("s1/1", "x3/4");
  }

  @Test
  void aListThatIsNotOneIsRefusedAsTheWalkRefusesIt() {
    buildGraph();
    for (final String hub : new String[] { "hub", "small" }) {
      final Map<String, Object> params = new HashMap<>();
      params.put("h", hub);
      params.put("ts", "x1");
      assertThatThrownBy(() -> rows(FORWARD, params)).as(hub).hasStackTraceContaining("IN requires a list");
      assertThatThrownBy(() -> rows(FORWARD.replace("b.k IN", "b.m IN"), params)).as(hub)
          .hasStackTraceContaining("IN requires a list");
      params.put("ts", null);
      assertThat(rows(FORWARD, params)).as(hub).isEmpty();
    }
  }

  /**
   * The far end is sought: the pages read for 1, 10 and 200 keys do not grow with the degree of the hub. They used to be the
   * whole relationship list of the hub, and a vertex load per relationship.
   */
  @Test
  void costFollowsTheKeysNotTheDegree() {
    database.getConfiguration().setValue(GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD, 0);
    createSchema();
    hub("small", 4_000);
    hub("large", 32_000);

    for (final int keys : new int[] { 1, 10, 200 }) {
      final long small = pages(FORWARD, "small", keys);
      final long large = pages(FORWARD, "large", keys);
      assertThat(large).as("pages read for %d keys on a 32,000-relationship hub, against %d on a 4,000 one", keys, small)
          .isLessThanOrEqualTo(small + 4);
    }
  }

  /**
   * A vertex with few relationships is walked: a long list costs fewer pages than it has keys, so no key is sought. One
   * seek per key reads at least one page per key.
   */
  @Test
  void aShortListOfRelationshipsIsWalked() {
    database.getConfiguration().setValue(GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD, 0);
    createSchema();
    hub("few", 20);
    hub("many", 10_000);

    // 500 keys: 20 relationships are walked, 10,000 are more than 16 per key and the keys are sought
    final int keys = 500;
    assertThat(pages(FORWARD, "few", keys)).as("pages read walking 20 relationships for %d keys", keys).isLessThan(keys / 10);
    assertThat(pages(FORWARD, "many", keys)).as("pages read seeking %d keys", keys).isGreaterThanOrEqualTo(keys);
  }

  private void createSchema() {
    database.command("sql", "CREATE VERTEX TYPE N");
    database.command("sql", "CREATE PROPERTY N.k STRING");
    database.command("sql", "CREATE INDEX ON N (k) UNIQUE");
    database.command("sql", "CREATE EDGE TYPE T");
    database.command("sql", "CREATE EDGE TYPE L LIGHTWEIGHT");
    database.command("sql", "CREATE EDGE TYPE D UNIDIRECTIONAL");
  }

  /**
   * A hub with {@link #HUB_DEGREE} T relationships to x0..x(n-1), two to x3, a self loop, reverse relationships from r0..r9,
   * lightweight and unidirectional relationships; a small vertex with three; free vertices joined to nothing.
   */
  private void buildGraph() {
    createSchema();
    database.transaction(() -> {
      final MutableVertex hub = vertex("hub");
      final MutableVertex small = vertex("small");
      for (int i = 0; i < HUB_DEGREE; i++) {
        final MutableVertex leaf = vertex("x" + i);
        hub.newEdge("T", leaf, "p", 1);
        if (i % 2 == 0)
          hub.newEdge("L", leaf);
        if (i % 3 == 0)
          leaf.newEdge("D", hub);
        // EVERY LEAF LEADS ON TO ONE MORE, FOR THE TWO-HOP STATEMENT
        if (i < 20)
          leaf.newEdge("T", vertex("y" + i), "p", 3);
      }
      hub.newEdge("T", database.lookupByKey("N", "k", "x3").next().asVertex(), "p", 2);
      hub.newEdge("T", hub, "p", 7);
      for (int i = 0; i < 10; i++)
        vertex("r" + i).newEdge("T", hub, "p", 5);
      for (int i = 0; i < 2; i++)
        vertex("free" + i);
      small.newEdge("T", vertex("s0"), "p", 1);
      small.newEdge("T", vertex("s1"), "p", 1);
      small.newEdge("T", database.lookupByKey("N", "k", "x3").next().asVertex(), "p", 4);
      small.newEdge("D", database.lookupByKey("N", "k", "x5").next().asVertex());
    });
  }

  private MutableVertex vertex(final String key) {
    return database.newVertex("N").set("k", key).set("m", key).save();
  }

  private void hub(final String name, final int degree) {
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("N").set("k", name).save();
      for (int i = 0; i < degree; i++)
        hub.newEdge("T", database.newVertex("N").set("k", name + i).save(), "p", 1);
    });
  }

  /**
   * The page-cache lookups of one run of {@code statement} for {@code keys} keys spread over the vertices the hub reaches,
   * after a warm-up run. Past the degree of the hub the keys name no vertex.
   */
  private long pages(final String statement, final String hub, final int keys) {
    final int degree = (int) count("MATCH (a:N {k: $h})-[:T]->(b:N) RETURN count(b) AS v", Map.of("h", hub));
    final int step = Math.max(1, degree / keys);
    final List<String> list = new ArrayList<>();
    for (int i = 0; i < keys; i++)
      list.add(hub + (i * step));
    final int expected = Math.min(keys, degree);
    final Map<String, Object> params = Map.of("h", hub, "ts", list);
    assertThat(rows(statement, params)).hasSize(expected);
    final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
    final PageManager.PPageManagerStats before = pageManager.getStats();
    assertThat(rows(statement, params)).hasSize(expected);
    final PageManager.PPageManagerStats after = pageManager.getStats();
    return after.cacheHits + after.cacheMiss - before.cacheHits - before.cacheMiss;
  }

  private String explain(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query, Map.of("h", "hub", "ts", List.of("x1")))) {
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }

  private List<String> rows(final String query, final String hub, final List<String> list) {
    final Map<String, Object> params = new HashMap<>();
    params.put("h", hub);
    params.put("ts", list);
    return rows(query, params);
  }

  /** The rows as {@code id/p}, sorted: the plans give the same rows, not in the same order. */
  private List<String> rows(final String query, final Map<String, Object> params) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        rows.add(row.getProperty("id") + "/" + row.getProperty("p"));
      }
    }
    Collections.sort(rows);
    return rows;
  }

  private long count(final String query, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      return ((Number) rs.next().getProperty("v")).longValue();
    }
  }
}
