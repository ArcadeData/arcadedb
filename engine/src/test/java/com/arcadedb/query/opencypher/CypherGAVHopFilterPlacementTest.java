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
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A WHERE conjunct that reads only variables an earlier hop has bound is placed on that hop (#9203). A chain served by a
 * Graph Analytical View that ends in a closing {@code GAVExpandInto} (a triangle) is not fused, runs hop by hop, and used to
 * keep every conjunct above the last hop, so a triangle counted with ordered ids ({@code a.id < b.id AND b.id < c.id}) built every wedge
 * of the chain before the first id comparison. The conjuncts now sit on the hop that binds their variables, as on a plain chain. A chain
 * the fusion takes keeps its whole Filter, and the fused chain checks each conjunct on the hop that binds its variables; a chain the
 * fusion refuses (a branching one) has its conjuncts placed hop by hop.
 */
class CypherGAVHopFilterPlacementTest {
  private static final String DB_PATH  = "./target/databases/gav-hop-filter-placement";
  private static final String TRIANGLE = "MATCH (a:Person)-[:KNOWS]-(b:Person)-[:KNOWS]-(c:Person)-[:KNOWS]-(a) ";

  private Database    database;
  private int[][]     adjacency;
  private boolean[][] knows;

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
    knows = new boolean[n][n];
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
            if (c > b && Arrays.binarySearch(adjacency[a], c) >= 0)
              ++t;
    return t;
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }

  /** The rows of {@code query}, each one as the text of its properties, sorted. */
  private List<String> rows(final String query) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final StringBuilder text = new StringBuilder();
        for (final String property : new TreeSet<>(row.getPropertyNames()))
          text.append(property).append('=').append(row.<Object>getProperty(property)).append(' ');
        rows.add(text.toString());
      }
    }
    rows.sort(null);
    return rows;
  }

  /** The directed paths a->b->c->d of strictly increasing ids, from the edge list. */
  private long expectedIncreasingThreeHopPaths() {
    final int n = knows.length;
    long paths = 0;
    for (int a = 0; a < n; a++)
      for (int b = a + 1; b < n; b++)
        if (knows[a][b])
          for (int c = b + 1; c < n; c++)
            if (knows[b][c])
              for (int d = c + 1; d < n; d++)
                if (knows[c][d])
                  ++paths;
    return paths;
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
  void orderedIdTriangleRowsAreEveryTriangleOnce() {
    final String query = TRIANGLE + "WHERE a.id < b.id AND b.id < c.id RETURN a.id AS a, b.id AS b, c.id AS c";
    final List<String> expected = new ArrayList<>();
    for (int a = 0; a < adjacency.length; a++)
      for (final int b : adjacency[a])
        if (b > a)
          for (final int c : adjacency[b])
            if (c > b && Arrays.binarySearch(adjacency[a], c) >= 0)
              expected.add("a=" + a + " b=" + b + " c=" + c + " ");
    expected.sort(null);
    assertThat(expected).isNotEmpty();
    assertThat(rows(query)).isEqualTo(expected);

    createView();
    assertThat(plan(query)).contains("GAVExpandInto");
    assertThat(rows(query)).isEqualTo(expected);
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
  void branchingChainTheFusionRefusesHasItsConjunctsPlacedHopByHop() {
    // c is the anchor (the range on its id), and the chain branches from it: (c)<-(b), (b)<-(a), then (c)->(d). The fusion
    // refuses a branching chain, so a.id < b.id must be checked before the hop to d multiplies the rows, not above it
    final String counted = "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)-[:KNOWS]->(d:Person) WHERE a.id < b.id AND c.id > 5 "
        + "RETURN count(*) AS n";
    final String listed = "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)-[:KNOWS]->(d:Person) WHERE a.id < b.id AND c.id > 5 "
        + "RETURN a.id AS a, b.id AS b, c.id AS c, d.id AS d";
    final long expectedCount = count(counted);
    final List<String> expectedRows = rows(listed);
    assertThat(expectedCount).isPositive();
    assertThat(expectedRows).hasSize((int) expectedCount);

    createView();
    for (final String query : List.of(counted, listed)) {
      final String plan = plan(query);
      assertThat(plan).doesNotContain("GAVFusedChain");
      final int lastHop = plan.indexOf("-[:KNOWS]->(d:Person)");
      assertThat(lastHop).isNotNegative();
      assertThat(plan.indexOf("Filter [predicate=a.id < b.id")).as(plan).isGreaterThan(lastHop);
    }
    assertThat(count(counted)).isEqualTo(expectedCount);
    assertThat(rows(listed)).isEqualTo(expectedRows);
  }

  @Test
  void fusedChainChecksEachConjunctOnTheHopThatBindsIt() {
    final String pattern = "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)-[:KNOWS]->(d:Person) "
        + "WHERE a.id < b.id AND b.id < c.id AND c.id < d.id ";
    final String counted = pattern + "RETURN count(*) AS n";
    final String listed = pattern + "RETURN a.id AS a, b.id AS b, c.id AS c, d.id AS d";
    final long expected = expectedIncreasingThreeHopPaths();
    assertThat(expected).isPositive();
    assertThat(count(counted)).isEqualTo(expected);
    final List<String> expectedRows = rows(listed);
    assertThat(expectedRows).hasSize((int) expected);

    createView();
    for (final String query : List.of(counted, listed)) {
      final String plan = plan(query);
      assertThat(plan).contains("GAVFusedChain");
      assertThat(plan).doesNotContain("Filter [");
      assertThat(plan).contains("filter on (b)=a.id < b.id", "filter on (c)=b.id < c.id", "filter on (d)=c.id < d.id");
    }
    // the fused count and the fused rows
    assertThat(count(counted)).isEqualTo(expected);
    assertThat(rows(listed)).isEqualTo(expectedRows);
  }

  @Test
  void fusedChainChecksTheSourceConjunctBeforeItsFirstHop() {
    // the range scan anchor does not take b.id < 10, so it is the fused chain's own conjunct on its source
    final String query = "MATCH (b:Person)<-[:KNOWS]-(a:Person)-[:KNOWS]->(c:Person) WHERE b.id < 10 AND c.id > 50 ";
    final String counted = query + "RETURN count(*) AS n";
    final String listed = query + "RETURN a.id AS a, b.id AS b, c.id AS c";
    final long expectedCount = count(counted);
    final List<String> expectedRows = rows(listed);
    assertThat(expectedCount).isPositive();

    createView();
    final String plan = plan(counted);
    assertThat(plan).contains("GAVFusedChain(b)");
    assertThat(plan).contains("filter on (b)=b.id < 10", "filter on (c)=c.id > 50");
    assertThat(count(counted)).isEqualTo(expectedCount);
    assertThat(rows(listed)).isEqualTo(expectedRows);
  }

  @Test
  void whereClausesOfTwoMatchClausesFuseWithTheChain() {
    // two WHERE clauses used to stand as two Filters, and the fusion only takes the one right above the chain
    final String query = "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.id < b.id MATCH (b)-[:KNOWS]->(c:Person) WHERE b.id < c.id ";
    final String counted = query + "RETURN count(*) AS n";
    final String listed = query + "RETURN a.id AS a, b.id AS b, c.id AS c";
    final long expectedCount = count(counted);
    final List<String> expectedRows = rows(listed);
    assertThat(expectedCount).isPositive();
    assertThat(expectedRows).hasSize((int) expectedCount);

    createView();
    final String plan = plan(counted);
    assertThat(plan).contains("GAVFusedChain");
    assertThat(plan).contains("filter on (b)=a.id < b.id", "filter on (c)=b.id < c.id");
    assertThat(count(counted)).isEqualTo(expectedCount);
    assertThat(rows(listed)).isEqualTo(expectedRows);
  }

  @Test
  void sourcesTheStaleViewDoesNotMapAreCheckedAgainstTheWholeFilter() {
    createView();
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gavhop");
    // Kept in use while stale: the vertices created after the build are sources the view does not map
    view.setUseWhenStale(true);
    database.transaction(() -> {
      final List<Vertex> old = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT FROM Person WHERE id < 8 ORDER BY id")) {
        while (rs.hasNext())
          old.add(rs.next().getVertex().get());
      }
      for (int i = 0; i < 4; i++) {
        final MutableVertex x = database.newVertex("Person").set("id", 1000L + i).save();
        x.newEdge("KNOWS", old.get(i));
        x.newEdge("KNOWS", old.get(i + 2));
      }
    });

    final String pattern = "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)-[:KNOWS]->(d:Person) "
        + "WHERE a.id > b.id AND b.id < c.id AND c.id < d.id ";
    final List<String> queries = List.of(pattern + "RETURN count(*) AS n", pattern + "RETURN a.id AS a, b.id AS b, c.id AS c, d.id AS d");
    assertThat(plan(queries.getFirst())).contains("GAVFusedChain", "filter on (b)=a.id > b.id");
    final List<List<String>> withView = new ArrayList<>();
    for (final String query : queries)
      withView.add(rows(query));
    assertThat(withView.get(1).stream().anyMatch(row -> row.startsWith("a=100"))).isTrue();

    database.command("sql", "DROP GRAPH ANALYTICAL VIEW gavhop");
    for (int i = 0; i < queries.size(); i++)
      assertThat(withView.get(i)).as(queries.get(i)).isEqualTo(rows(queries.get(i)));
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
