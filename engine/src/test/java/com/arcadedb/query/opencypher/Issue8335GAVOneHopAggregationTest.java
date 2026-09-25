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

import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8335: with a Graph Analytical View READY, a one-hop Cypher aggregation that needs one row per edge (it reads
 * a property of the far endpoint, or compares the two ends) gained only a fraction of what the view offers, because
 * every edge still became a copied result row carrying a source vertex decoded from its record. Such a query now reads
 * both endpoints from the view and hands the aggregation a two-slot row per edge.
 * <p>
 * The view is a performance feature, so the oracle is the answer without it: every query is answered first with no
 * view, then with one, and the two must match.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8335GAVOneHopAggregationTest extends TestHelper {
  private static final int PERSONS = 300;

  private static final String[] ELIGIBLE = {
      // The issue's per-edge queries
      "MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN p.city AS c, avg(f.age) AS a, count(*) AS n ORDER BY c",
      "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.city = b.city RETURN a.city AS c, count(*) AS n ORDER BY c",
      "MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN avg(f.age) AS a, count(*) AS n",
      // A WHERE clause split between the source and the edge, a null-sensitive comparison, several aggregates
      "MATCH (p:Person)-[:KNOWS]->(f:Person) WHERE p.age > 40 AND f.age < 30 "
          + "RETURN p.city AS c, min(f.age) AS lo, max(f.age) AS hi, sum(f.age) AS s, count(f.age) AS n ORDER BY c",
      // A property the view does not hold, read from the record
      "MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN f.name STARTS WITH 'p1' AS k, count(*) AS n ORDER BY k",
      // Walked backwards, and undirected (with self-loops)
      "MATCH (f:Person)<-[:KNOWS]-(p:Person) RETURN f.city AS c, avg(p.age) AS a, count(*) AS n ORDER BY c",
      "MATCH (a:Person)-[:KNOWS]-(b:Person) RETURN a.city AS c, max(b.age) AS m, count(*) AS n ORDER BY c",
      // Grouped by the far vertex, and fed by an anonymous source. A count per endpoint alone is not in this list: the
      // COUNT EDGES RETURN rewrite already answers it without a row per edge
      "MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN f AS f, avg(p.age) AS a, count(p) AS n ORDER BY f.id",
      "MATCH (:Person)-[:KNOWS]->(f:Person) RETURN sum(f.age) AS s, count(*) AS n",
      "MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN count(DISTINCT f.city) AS n, count(DISTINCT f) AS m",
      // The far label filters the targets: KNOWS also reaches companies
      "MATCH (p:Person)-[:KNOWS]->(c:Company) RETURN p.city AS pc, min(c.city) AS cc, count(*) AS n ORDER BY pc",
      // A sub-type of the source label
      "MATCH (e:Employee)-[:KNOWS]->(f:Person) RETURN e.city AS c, avg(f.age) AS a, count(*) AS n ORDER BY c" };

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE PROPERTY Person.id INTEGER");
    database.command("sql", "CREATE INDEX ON Person (id) UNIQUE");
    database.command("sql", "CREATE VERTEX TYPE Employee EXTENDS Person");
    database.command("sql", "CREATE VERTEX TYPE Company");
    database.command("sql", "CREATE EDGE TYPE KNOWS");

    final Random random = new Random(8335);
    database.transaction(() -> {
      final List<Vertex> people = new ArrayList<>();
      for (int i = 0; i < PERSONS; i++) {
        final MutableVertex person = database.newVertex(i % 10 == 0 ? "Employee" : "Person").set("id", i)
            .set("name", "p" + i).set("city", "city_" + random.nextInt(8));
        // Some ages missing, so an average and a comparison meet nulls
        if (i % 7 != 0)
          person.set("age", 18 + random.nextInt(60));
        people.add(person.save());
      }
      final List<Vertex> companies = new ArrayList<>();
      for (int i = 0; i < 5; i++)
        companies.add(database.newVertex("Company").set("id", i).set("city", "city_" + i).save());

      for (final Vertex person : people) {
        final int degree = random.nextInt(12);
        for (int k = 0; k < degree; k++)
          person.asVertex().modify().newEdge("KNOWS", people.get(random.nextInt(PERSONS)));
        if (random.nextInt(10) == 0)
          person.asVertex().modify().newEdge("KNOWS", person); // self-loop
        if (random.nextInt(4) == 0)
          person.asVertex().modify().newEdge("KNOWS", companies.get(random.nextInt(companies.size())));
      }
    });
  }

  @Test
  void answersMatchTheQueriesWithoutTheView() {
    final Map<String, List<String>> expected = new LinkedHashMap<>();
    for (final String query : ELIGIBLE)
      expected.put(query, answer(query));

    createView("VERTEX TYPES (Person, Employee, Company) EDGE TYPES (KNOWS) PROPERTIES (id, city, age)");

    for (final String query : ELIGIBLE) {
      assertThat(plan(query)).as(query).contains("GAV ONE-HOP SCAN");
      assertThat(answer(query)).as(query).isEqualTo(expected.get(query));
    }
  }

  @Test
  void aSourceLabelThatIsASmallShareOfTheViewIsEnumeratedFromItsBuckets() {
    // Products outnumber the persons: walking the whole view would visit them all to find the few sources
    database.command("sql", "CREATE VERTEX TYPE Product");
    database.transaction(() -> {
      for (int i = 0; i < PERSONS * 3; i++)
        database.newVertex("Product").set("id", i).save();
    });

    final Map<String, List<String>> expected = new LinkedHashMap<>();
    for (final String query : ELIGIBLE)
      expected.put(query, answer(query));

    createView("VERTEX TYPES (Person, Employee, Company, Product) EDGE TYPES (KNOWS) PROPERTIES (id, city, age)");

    for (final String query : ELIGIBLE) {
      assertThat(plan(query)).as(query).contains("GAV ONE-HOP SCAN");
      assertThat(answer(query)).as(query).isEqualTo(expected.get(query));
    }
  }

  @Test
  void profileReportsTheScanAndItsRows() {
    createView("VERTEX TYPES (Person, Employee, Company) EDGE TYPES (KNOWS) PROPERTIES (id, city, age)");
    try (final ResultSet rs = database.query("opencypher",
        "PROFILE MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN p.city AS c, avg(f.age) AS a")) {
      while (rs.hasNext())
        rs.next();
      final String profile = rs.getExecutionPlan().get().prettyPrint(0, 2);
      assertThat(profile).contains("GAV ONE-HOP SCAN (p:Person)-[:KNOWS]->(f:Person)").contains("rows");
    }
  }

  @Test
  void pendingChangesInTheTransactionKeepTheRecordPlan() {
    createView("VERTEX TYPES (Person, Employee, Company) EDGE TYPES (KNOWS) PROPERTIES (id, city, age)");
    final String query = "MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN count(*) AS n";
    final long before = answerCount(query);

    database.transaction(() -> {
      final Vertex a = database.newVertex("Person").set("id", 10_000).set("city", "city_0").save();
      a.modify().newEdge("KNOWS", a);
      assertThat(plan(query)).doesNotContain("GAV ONE-HOP SCAN");
      assertThat(answerCount(query)).isEqualTo(before + 1);
    });
  }

  @Test
  void aPlanCachedOutsideTheTransactionDoesNotReadTheViewPastItsWrites() {
    // Kept up to date on commit, so the view serves the query again once the transaction is over
    createView("VERTEX TYPES (Person, Employee, Company) EDGE TYPES (KNOWS) PROPERTIES (id, city, age) UPDATE MODE SYNCHRONOUS");
    final String query = "MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN p.id AS a, f.id AS b";
    final long before = rows(query); // cached with the view's expansion

    database.transaction(() -> {
      // A new edge between two vertices the view already maps: only the records know it yet
      final Vertex a = database.query("sql", "SELECT FROM Person WHERE id = 1").next().getVertex().get();
      final Vertex b = database.query("sql", "SELECT FROM Person WHERE id = 2").next().getVertex().get();
      a.modify().newEdge("KNOWS", b);
      assertThat(rows(query)).isEqualTo(before + 1);
    });
    assertThat(rows(query)).isEqualTo(before + 1);
    assertThat(plan(query)).contains("GAVExpandAll");
  }

  @Test
  void aCachedPlanDropsAViewThatWentStale() {
    // Not kept up to date: the commit leaves it stale, and a plan cached while it was ready must not keep reading it
    createView("VERTEX TYPES (Person, Employee, Company) EDGE TYPES (KNOWS) PROPERTIES (id, city, age) UPDATE MODE OFF");
    final String query = "MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN p.id AS a, f.id AS b";
    final long before = rows(query);

    database.transaction(() -> {
      final Vertex a = database.query("sql", "SELECT FROM Person WHERE id = 1").next().getVertex().get();
      final Vertex b = database.query("sql", "SELECT FROM Person WHERE id = 2").next().getVertex().get();
      a.modify().newEdge("KNOWS", b);
    });
    assertThat(rows(query)).isEqualTo(before + 1);
  }

  @Test
  void collectKeepsTheOperatorsOrder() {
    createView("VERTEX TYPES (Person, Employee, Company) EDGE TYPES (KNOWS) PROPERTIES (id, city, age)");
    assertThat(plan("MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN p.city AS c, collect(f.age) AS ages"))
        .doesNotContain("GAV ONE-HOP SCAN");
  }

  private long rows(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return rs.stream().count();
    }
  }

  @Test
  void anIndexSeekOnTheSourceIsKept() {
    createView("VERTEX TYPES (Person, Employee, Company) EDGE TYPES (KNOWS) PROPERTIES (id, city, age)");
    final String query = "MATCH (p:Person)-[:KNOWS]->(f:Person) WHERE p.id = 3 RETURN count(*) AS n";
    assertThat(plan(query)).doesNotContain("GAV ONE-HOP SCAN");
  }

  @Test
  void aViewThatMissesALabelIsNotEnumerated() {
    // Companies are not in this view: neither a company target nor an unlabeled one can be answered from it
    createView("VERTEX TYPES (Person, Employee) EDGE TYPES (KNOWS) PROPERTIES (id, city, age)");
    final String company = "MATCH (p:Person)-[:KNOWS]->(c:Company) RETURN count(*) AS n";
    final String any = "MATCH (p:Person)-[:KNOWS]->(x) RETURN count(x.city) AS n";
    assertThat(plan(company)).doesNotContain("GAV ONE-HOP SCAN");
    assertThat(plan(any)).doesNotContain("GAV ONE-HOP SCAN");
  }

  @Test
  void aStaleViewIsNotEnumerated() {
    createView("VERTEX TYPES (Person, Employee, Company) EDGE TYPES (KNOWS) PROPERTIES (id, city, age) UPDATE MODE OFF");
    final String query = "MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN count(*) AS n";
    final long before = answerCount(query);

    database.transaction(() -> {
      final Vertex a = database.newVertex("Person").set("id", 10_000).set("city", "city_0").save();
      a.modify().newEdge("KNOWS", a);
    });

    assertThat(plan(query)).doesNotContain("GAV ONE-HOP SCAN");
    assertThat(answerCount(query)).isEqualTo(before + 1);
  }

  @Test
  void aVertexTheViewDoesNotMapCountsItsSelfLoopOnceOnAnUndirectedHop() {
    // A stale view kept in use: a vertex created after its build is expanded on its record, where an undirected walk
    // meets a self-loop in both adjacency lists
    createView("VERTEX TYPES (Person, Employee, Company) EDGE TYPES (KNOWS) PROPERTIES (id, city, age) UPDATE MODE OFF");
    GraphAnalyticalViewRegistry.get(database, "gav8335").setUseWhenStale(true);
    database.transaction(() -> {
      final Vertex loop = database.newVertex("Person").set("id", 10_000).set("city", "city_0").save();
      loop.modify().newEdge("KNOWS", loop);
    });

    final String query = "MATCH (a:Person)-[:KNOWS]-(b:Person) WHERE a.id = 10000 RETURN count(*) AS n";
    assertThat(plan(query)).contains("provider=gav8335");
    assertThat(answerCount(query)).isEqualTo(1L);
  }

  @Test
  void aNonAggregatingReturnIsLeftToTheOperators() {
    createView("VERTEX TYPES (Person, Employee, Company) EDGE TYPES (KNOWS) PROPERTIES (id, city, age)");
    assertThat(plan("MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN p.city AS c, f.age AS a")).doesNotContain("GAV ONE-HOP SCAN");
    assertThat(plan("MATCH (p:Person)-[r:KNOWS]->(f:Person) RETURN count(r) AS n")).doesNotContain("GAV ONE-HOP SCAN");
  }

  private void createView(final String definition) {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gav8335 " + definition);
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav8335");
    final long deadline = System.currentTimeMillis() + 60_000;
    while (!view.isReady() && System.currentTimeMillis() < deadline)
      Thread.onSpinWait();
    assertThat(view.isReady()).isTrue();
  }

  private String plan(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }

  private long answerCount(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return rs.next().<Number>getProperty("n").longValue();
    }
  }

  private List<String> answer(final String query) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final StringBuilder line = new StringBuilder();
        for (final String name : row.getPropertyNames()) {
          Object value = row.getProperty(name);
          if (value instanceof Double d)
            value = String.format("%.9f", d);
          else if (value instanceof Vertex vertex)
            // The view hands out a proxy of the same vertex, which prints its RID alone
            value = vertex.getIdentity().toString();
          line.append(name).append('=').append(value).append(';');
        }
        rows.add(line.toString());
      }
    }
    return rows;
  }
}
