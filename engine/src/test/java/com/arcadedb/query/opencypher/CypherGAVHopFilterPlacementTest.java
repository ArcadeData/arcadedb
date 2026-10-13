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
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A WHERE conjunct that reads only variables an earlier hop has bound is placed on that hop (#9203). A chain served by a
 * Graph Analytical View that ends in a closing {@code GAVExpandInto} (a triangle) is not fused, runs hop by hop, and used to
 * keep every conjunct above the last hop, so a triangle counted with ordered ids ({@code a.id < b.id AND b.id < c.id}) built every wedge
 * of the chain before the first id comparison. The conjuncts now sit on the hop that binds their variables, as on a plain chain, and a chain
 * of two GAVExpandAll hops still reaches the fused chain with its whole Filter.
 */
class CypherGAVHopFilterPlacementTest {
  private static final String DB_PATH  = "./target/databases/gav-hop-filter-placement";
  private static final String TRIANGLE = "MATCH (a:Person)-[:KNOWS]-(b:Person)-[:KNOWS]-(c:Person)-[:KNOWS]-(a) ";

  private Database database;
  private int[][]  adjacency;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE PROPERTY Person.id LONG");
    database.command("sql", "CREATE INDEX ON Person (id) UNIQUE");
    database.command("sql", "CREATE EDGE TYPE KNOWS");

    final int n = 60;
    final Random r = new Random(5);
    final boolean[][] knows = new boolean[n][n];
    database.begin();
    final RID[] p = new RID[n];
    for (int i = 0; i < n; i++) {
      final MutableVertex v = database.newVertex("Person").set("id", (long) i);
      v.save();
      p[i] = v.getIdentity();
    }
    int edges = 0;
    while (edges < 300) {
      final int x = r.nextInt(n), y = r.nextInt(n);
      if (x != y && !knows[x][y] && !knows[y][x]) {
        knows[x][y] = true;
        p[x].asVertex().newEdge("KNOWS", p[y]).save();
        ++edges;
      }
    }
    database.commit();
    adjacency = new int[n][];
    for (int i = 0; i < n; i++) {
      final Set<Integer> s = new HashSet<>();
      for (int j = 0; j < n; j++)
        if (knows[i][j] || knows[j][i])
          s.add(j);
      adjacency[i] = s.stream().mapToInt(Integer::intValue).sorted().toArray();
    }
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  private void createView() {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gavhop VERTEX TYPES (Person) EDGE TYPES (KNOWS) PROPERTIES (id) UPDATE MODE OFF");
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gavhop");
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
  }

  private long expectedTriangles() {
    long t = 0;
    for (int a = 0; a < adjacency.length; a++)
      for (final int b : adjacency[a])
        if (b > a)
          for (final int c : adjacency[b])
            if (c > b && java.util.Arrays.binarySearch(adjacency[a], c) >= 0)
              ++t;
    return t;
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }

  private String plan(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      return rs.getExecutionPlan().map(pl -> pl.prettyPrint(0, 2)).orElse("");
    }
  }

  @Test
  void orderedIdTriangleFilterSitsOnTheBindingHopAndAnswersTheSame() {
    final String query = TRIANGLE + "WHERE a.id < b.id AND b.id < c.id RETURN count(*) AS n";
    final long expected = expectedTriangles();
    assertThat(expected).isPositive();
    assertThat(count(query)).isEqualTo(expected);

    createView();
    final String plan = plan(query);
    assertThat(plan).contains("GAVExpandInto");
    // the closing hop is the root and no Filter sits above it: the conjuncts are on the hops that bind them
    assertThat(plan.indexOf("GAVExpandInto")).isLessThan(plan.indexOf("Filter"));
    assertThat(plan.indexOf("Filter [predicate=a.id < b.id")).isGreaterThan(plan.indexOf("GAVExpandAll(b)"));
    assertThat(count(query)).isEqualTo(expected);
  }

  @Test
  void conjunctReadingAllThreeNodesSitsOnTheHopThatBindsTheThird() {
    // all of a, b and c are bound once the second hop has run; the closing hop binds nothing new
    final String query = TRIANGLE + "WHERE a.id + b.id + c.id > 90 RETURN count(*) AS n";
    final long without = count(query);
    createView();
    final String plan = plan(query);
    assertThat(plan.indexOf("Filter")).isGreaterThan(plan.indexOf("GAVExpandInto"));
    assertThat(count(query)).isEqualTo(without);
  }

  @Test
  void twoHopChainStillFusesWithItsFilter() {
    final String query = "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) WHERE a.id < b.id AND b.id < c.id RETURN count(*) AS n";
    final long without = count(query);
    createView();
    assertThat(plan(query)).contains("GAVFusedChain");
    assertThat(count(query)).isEqualTo(without);
  }
}
