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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;

import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Cypher queries served by a Graph Analytical View: one-hop aggregation, sub-types, uniqueness, fused chains and variable-length expand.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherGAVTest {
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
  @Nested
  class Issue8335GAVOneHopAggregation extends TestHelper {
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

      // count(b.id), not count(*): since issue #9595 the chain count push-down applies a WHERE on one node itself, and it
      // answers off the stale view's adjacency, which is what useWhenStale accepts; since issue #9600 count(b) is count(*)
      // too. A property count is not a row count, so it pins the one-hop scan.
      final String query = "MATCH (a:Person)-[:KNOWS]-(b:Person) WHERE a.id = 10000 RETURN count(b.id) AS n";
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

  /**
   * Issue #8377: a Cypher label on a pattern node matches the vertices of its sub-types too. Once a Graph Analytical View
   * served the traversal, the fused chain filtered each hop's target on the label's OWN buckets and the deferred
   * expansion compared the exact type name, so every endpoint of a sub-type was silently dropped.
   * <p>
   * The view is a performance feature: the oracle is the answer the same statement gives without it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue8377GAVSubTypeTargetLabel extends TestHelper {
    private static final String[] QUERIES = {
        // The issue's statements: a sub-typed intermediate and a sub-typed endpoint
        "MATCH (a:A)-[:R]->(b:B)-[:R]->(c:C) RETURN count(*) AS n",
        "MATCH (a:A)-[:R]->(b:B)-[:R]->(c:C) RETURN b.id AS bi, c.id AS ci ORDER BY bi",
        // The intermediate is not read downstream
        "MATCH (a:A)-[:R]->(b:B)-[:R]->(c:C) RETURN c.id AS ci ORDER BY ci",
        "MATCH (a:A)-[:R]->(b:B)-[:R]->(c:C) RETURN a.id AS ai, c.id AS ci ORDER BY ci",
        // A branching pattern is not fused: the unread intermediate's expansion defers its target load
        "MATCH (a:A)-[:R]->(b:B)-[:R]->(c:C), (a)-[:S]->(x:D) RETURN c.id AS ci, x.id AS xi ORDER BY ci, xi",
        // Three hops, the sub-type in the middle
        "MATCH (a:A)-[:R]->(b:B)-[:R]->(c:C)-[:R]->(d:D) RETURN a.id AS ai, d.id AS di ORDER BY di",
        // A label that names the sub-type alone still excludes its parent's own vertices
        "MATCH (a:A)-[:R]->(b:Bsub)-[:R]->(c:C) RETURN c.id AS ci ORDER BY ci",
        "MATCH (a:A)-[:R]->(b:B)-[:R]->(c:Csub) RETURN b.id AS bi ORDER BY bi",
        // A label no type carries matches nothing
        "MATCH (a:A)-[:R]->(b:B)-[:R]->(c:Missing) RETURN count(*) AS n" };

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE A");
      database.command("sql", "CREATE VERTEX TYPE B");
      database.command("sql", "CREATE VERTEX TYPE Bsub EXTENDS B");
      database.command("sql", "CREATE VERTEX TYPE C");
      database.command("sql", "CREATE VERTEX TYPE Csub EXTENDS C");
      database.command("sql", "CREATE VERTEX TYPE D");
      database.command("sql", "CREATE EDGE TYPE R");
      database.command("sql", "CREATE EDGE TYPE S");

      database.transaction(() -> {
        final MutableVertex a1 = database.newVertex("A").set("id", 1).save();
        final MutableVertex bsub = database.newVertex("Bsub").set("id", 11).save();
        final MutableVertex b1 = database.newVertex("B").set("id", 12).save();
        final MutableVertex c1 = database.newVertex("C").set("id", 21).save();
        final MutableVertex csub = database.newVertex("Csub").set("id", 22).save();
        final MutableVertex d1 = database.newVertex("D").set("id", 31).save();
        a1.newEdge("R", bsub);
        bsub.newEdge("R", c1);
        a1.newEdge("R", b1);
        b1.newEdge("R", csub);
        csub.newEdge("R", d1);
        c1.newEdge("R", d1);
        a1.newEdge("S", d1);

        // Filler, so the optimizer anchors on A and fuses the chain
        for (int i = 0; i < 60; i++)
          database.newVertex("B").set("id", 1000 + i).save();
        for (int i = 0; i < 200; i++)
          database.newVertex("C").set("id", 2000 + i).save();
        for (int i = 0; i < 200; i++)
          database.newVertex("D").set("id", 3000 + i).save();
      });
    }

    @Test
    void subTypeEndpointsMatchTheParentLabelWithTheView() {
      final Map<String, List<String>> expected = new LinkedHashMap<>();
      for (final String query : QUERIES)
        expected.put(query, answer(query));

      // The oracle itself must see the sub-typed rows, or the comparison below proves nothing
      assertThat(expected.get(QUERIES[0])).containsExactly("n=2;");
      assertThat(expected.get(QUERIES[1])).containsExactly("bi=11;ci=21;", "bi=12;ci=22;");
      assertThat(expected.get(QUERIES[QUERIES.length - 1])).containsExactly("n=0;");

      createView("VERTEX TYPES (A, B, Bsub, C, Csub, D) EDGE TYPES (R, S) PROPERTIES (id)");

      // The deferred expansion is on the plan, not fused away
      assertThat(plan(QUERIES[4])).contains("GAVExpandAll(a)-[:R]->(b:B)");

      for (final String query : QUERIES) {
        assertThat(plan(query)).as(query).contains("provider=gav8377");
        assertThat(answer(query)).as(query).isEqualTo(expected.get(query));
      }
    }

    private void createView(final String definition) {
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gav8377 " + definition);
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav8377");
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

    private List<String> answer(final String query) {
      final List<String> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          final StringBuilder line = new StringBuilder();
          for (final String name : row.getPropertyNames())
            line.append(name).append('=').append(row.<Object>getProperty(name)).append(';');
          rows.add(line.toString());
        }
      }
      return rows;
    }
  }

  /**
   * Issue #8394: with a Graph Analytical View READY, a multi-hop Cypher pattern whose hops share an edge type bound the
   * same relationship twice in one MATCH, because the CSR holds adjacency ids, not edge identities. The view is a
   * performance feature, so the oracle is the answer without it: every query is answered first with no view, then with
   * one, and the two must match.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue8394GAVRelationshipUniqueness extends TestHelper {
    private static final int VERTICES = 30;

    private static final String[] QUERIES = {
        // The issue's table
        "MATCH (a:P)-[:K]->(b:P)-[:K]->(c:P) RETURN count(*) AS n",
        "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P) RETURN count(*) AS n",
        "MATCH (a:P)-[:K]->(b:P), (a)-[:K]->(c:P) RETURN count(*) AS n",
        "MATCH (a:P)-[:K]->(b:P)-[:K]->(c:P)-[:K]->(d:P) RETURN count(*) AS n",
        "MATCH (a:P {id:1})-[:K]-(b:P)-[:K]-(c:P) RETURN count(*) AS n",
        // Rows, not only counts, and grouped
        "MATCH (a:P)-[:K]->(b:P)-[:K]->(c:P) RETURN a.id AS a, b.id AS b, c.id AS c ORDER BY a, b, c",
        "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P) RETURN a.id AS a, count(*) AS n ORDER BY a",
        "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P) RETURN a.id AS a, c.id AS c, count(*) AS n ORDER BY a, c",
        // Aggregated inside the fused chain
        "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P) WITH a, c, count(*) AS n RETURN a.id AS a, c.id AS c, n ORDER BY a, c",
        "MATCH (a:P)-[:K]->(b:P)-[:K]->(c:P) WITH a, count(*) AS n RETURN a.id AS a, n ORDER BY a",
        // Walked backwards and mixed directions
        "MATCH (a:P)<-[:K]-(b:P)<-[:K]-(c:P) RETURN count(*) AS n",
        "MATCH (a:P)-[:K]->(b:P)<-[:K]-(c:P) RETURN count(*) AS n",
        "MATCH (a:P)<-[:K]-(b:P)-[:K]->(c:P) RETURN count(*) AS n",
        // Closing a cycle: the last hop has both ends bound
        "MATCH (a:P)-[:K]->(b:P)-[:K]->(c:P)-[:K]->(a) RETURN count(*) AS n",
        "MATCH (a:P)-[:K]-(b:P)-[:K]-(a) RETURN count(*) AS n",
        "MATCH (a:P)-[:K]->(b:P)-[:K]->(a) RETURN count(*) AS n",
        // Several edge types: equal neighbours reached over different types are different relationships
        "MATCH (a:P)-[:K|L]->(b:P)-[:K|L]->(c:P) RETURN count(*) AS n",
        "MATCH (a:P)-[:K|L]-(b:P)-[:K]-(c:P) RETURN count(*) AS n",
        "MATCH (a:P)-[:K]->(b:P)-[:L]->(c:P) RETURN count(*) AS n",
        "MATCH (a:P)-[]->(b:P)-[]->(c:P) RETURN count(*) AS n",
        // Disconnected components of the same MATCH
        "MATCH (a:P {id:1})-[:K]->(b:P), (c:P {id:1})-[:K]->(d:P) RETURN count(*) AS n",
        "MATCH (a:P)-[:K]->(b:P), (c:P)-[:K]->(d:P) WHERE a.id < 3 AND c.id < 3 RETURN count(*) AS n",
        // A named relationship beside anonymous ones, read and unread
        "MATCH (a:P)-[r:K]->(b:P)-[:K]->(c:P) RETURN count(r) AS n",
        "MATCH (a:P)-[r:K]->(b:P)-[:K]->(c:P) RETURN count(*) AS n",
        "MATCH (a:P)-[:K]->(b:P)-[r:K]->(c:P) RETURN count(r) AS n",
        // A variable-length relationship in the same clause
        "MATCH (a:P)-[:K]->(b:P)-[:K*1..2]->(c:P) RETURN count(*) AS n",
        // Separate MATCH clauses may reuse a relationship
        "MATCH (a:P)-[:K]->(b:P) MATCH (b)<-[:K]-(c:P) RETURN count(*) AS n",
        "MATCH (a:P)-[:K]-(b:P) MATCH (b)-[:K]-(c:P) RETURN count(*) AS n" };

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE P");
      database.command("sql", "CREATE PROPERTY P.id INTEGER");
      database.command("sql", "CREATE EDGE TYPE K");
      database.command("sql", "CREATE EDGE TYPE L");

      final Random random = new Random(8394);
      database.transaction(() -> {
        final List<Vertex> vertices = new ArrayList<>();
        for (int i = 0; i < VERTICES; i++)
          vertices.add(database.newVertex("P").set("id", i).save());
        for (final Vertex v : vertices) {
          v.asVertex().modify().newEdge("K", v); // self-loop
          for (int k = 0; k < 3; k++)
            v.asVertex().modify().newEdge("K", vertices.get(random.nextInt(VERTICES)));
          // Parallel edges: the same pair joined twice by K, and once more by L
          if (random.nextInt(4) == 0) {
            final Vertex other = vertices.get(random.nextInt(VERTICES));
            v.asVertex().modify().newEdge("K", other);
            v.asVertex().modify().newEdge("K", other);
            v.asVertex().modify().newEdge("L", other);
          }
          if (random.nextInt(5) == 0)
            v.asVertex().modify().newEdge("L", v);
        }
      });
    }

    @Test
    void answersMatchTheQueriesWithoutTheView() {
      final Map<String, List<String>> expected = new LinkedHashMap<>();
      for (final String query : QUERIES)
        expected.put(query, answer(query));

      createView("VERTEX TYPES (P) EDGE TYPES (K, L) PROPERTIES (id)");

      final List<String> failures = new ArrayList<>();
      for (final String query : QUERIES) {
        final List<String> actual = answer(query);
        if (!actual.equals(expected.get(query)))
          failures.add(query + "\n  expected " + abbreviate(expected.get(query)) + "\n  actual   " + abbreviate(actual) + "\n" + plan(query));
      }
      assertThat(failures).isEmpty();
    }

    @Test
    void collidingHopsStayOnTheView() {
      createView("VERTEX TYPES (P) EDGE TYPES (K, L) PROPERTIES (id)");
      // The issue's shapes are still answered from the view, now binding relationship labels
      assertThat(plan(QUERIES[0])).contains("GAVFusedChain").contains("unique relationships");
      assertThat(plan(QUERIES[1])).contains("GAVFusedChain").contains("unique relationships");
      assertThat(plan(QUERIES[2])).contains("GAVExpandAll").contains("unique relationships");
      assertThat(plan(QUERIES[3])).contains("GAVFusedChain").contains("unique relationships");
      assertThat(plan("MATCH (a:P)-[:K]->(b:P)-[:K]->(a) RETURN count(*) AS n")).contains("GAVExpandInto")
          .doesNotContain("+ ExpandInto");
      assertThat(plan("MATCH (a:P)-[:K]->(b:P), (c:P)-[:K]->(d:P) RETURN count(*) AS n")).contains("GAVExpandAll")
          .contains("unique relationships");
      // Hops that cannot collide do not pay for the labels
      assertThat(plan("MATCH (a:P)-[:K]->(b:P)-[:L]->(c:P) RETURN count(*) AS n")).contains("GAVFusedChain")
          .doesNotContain("unique relationships");
      // A named relationship binds its edge record, so the whole clause walks the edge records
      assertThat(plan("MATCH (a:P)-[r:K]->(b:P)-[:K]->(c:P) RETURN count(r) AS n")).doesNotContain("GAV");
    }

    @Test
    void aChainSplitAcrossWorkerThreadsKeepsRelationshipsUnique() {
      // Enough sources for the fused chain to walk them in parallel chunks
      database.transaction(() -> {
        Vertex previous = null;
        for (int i = 0; i < 9_000; i++) {
          final Vertex v = database.newVertex("P").set("id", 1_000 + i).save();
          v.modify().newEdge("K", v);
          if (previous != null)
            previous.modify().newEdge("K", v);
          previous = v;
        }
      });
      final String query = "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P) RETURN count(*) AS n";
      final String grouped = "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P) WITH a, count(*) AS n RETURN sum(n) AS n";
      final List<String> expected = answer(query);

      createView("VERTEX TYPES (P) EDGE TYPES (K, L) PROPERTIES (id)");
      assertThat(plan(query)).contains("GAVFusedChain").contains("unique relationships");
      assertThat(answer(query)).isEqualTo(expected);
      assertThat(answer(grouped)).isEqualTo(expected);
    }

    @Test
    void anEdgeSubTypeKeepsTheClauseOnTheView() {
      // A view answers a type polymorphically, from the parent's slice and the sub-type's. The tracked walk reads one slice
      // at a time and labels a relationship with the type of its slice, so one edge has one label whichever hop reads it:
      // the clause stays on the view (issue #10213; until then it walked the edge records)
      database.command("sql", "CREATE EDGE TYPE KC EXTENDS K");
      database.transaction(() -> {
        final Vertex a = database.query("sql", "SELECT FROM P WHERE id = 1").next().getVertex().get();
        final Vertex b = database.query("sql", "SELECT FROM P WHERE id = 2").next().getVertex().get();
        a.modify().newEdge("KC", b);
        b.modify().newEdge("KC", a);
        a.modify().newEdge("KC", a);
      });
      final String[] queries = {
          "MATCH (a:P)-[:K]->(b:P)-[:KC]->(c:P) RETURN count(*) AS n",
          "MATCH (a:P)-[:KC]-(b:P)-[:K]-(c:P) RETURN count(*) AS n",
          "MATCH (a:P)-[:K]->(b:P)-[:K]->(c:P) RETURN count(*) AS n" };
      final Map<String, List<String>> expected = new LinkedHashMap<>();
      for (final String query : queries)
        expected.put(query, answer(query));

      createView("VERTEX TYPES (P) EDGE TYPES (K, L) PROPERTIES (id)");
      for (final String query : queries) {
        assertThat(plan(query)).as(query).contains("unique relationships");
        assertThat(answer(query)).as(query + "\n" + plan(query)).isEqualTo(expected.get(query));
      }
    }

    @Test
    void anUntypedHopWalksAnEdgeTypeTheSnapshotHasNoSliceFor() {
      // A view over every edge type, then a type created after its build: its edges live only in the overlay, and an
      // untyped hop that enumerated the snapshot's types would never see them
      createView("VERTEX TYPES (P) PROPERTIES (id) UPDATE MODE SYNCHRONOUS");
      database.command("sql", "CREATE EDGE TYPE Z");
      database.transaction(() -> {
        final Vertex a = database.query("sql", "SELECT FROM P WHERE id = 1").next().getVertex().get();
        final Vertex b = database.query("sql", "SELECT FROM P WHERE id = 2").next().getVertex().get();
        a.modify().newEdge("Z", b);
        a.modify().newEdge("Z", b);
        b.modify().newEdge("Z", b);
      });
      final String[] queries = {
          "MATCH (a:P {id:1})-[]->(b:P)-[]->(c:P) RETURN count(*) AS n",
          "MATCH (a:P)-[]-(b:P {id:2})-[]-(c:P) RETURN count(*) AS n",
          "MATCH (a:P)-[:Z]->(b:P)-[:Z]->(c:P) RETURN count(*) AS n" };
      final Map<String, List<String>> withView = new LinkedHashMap<>();
      for (final String query : queries)
        withView.put(query, answer(query));
      assertThat(plan(queries[0])).contains("provider=gav8394").contains("unique relationships");

      database.command("sql", "DROP GRAPH ANALYTICAL VIEW gav8394");
      for (final String query : queries)
        assertThat(withView.get(query)).as(query).isEqualTo(answer(query));
    }

    @Test
    void aHubWithManyParallelEdgesKeepsRelationshipsUnique() {
      // Parallel relationships of one type between the same pair are interchangeable to an anonymous hop: each walk
      // numbers them 0..m-1 in its own adjacency slice, whichever physical edge lands on which number. Far past the size
      // where the sort of a slice stops preserving the order of equal entries, so both sides are ordered independently
      database.transaction(() -> {
        final Vertex hub = database.newVertex("P").set("id", 500).save();
        final Vertex x = database.newVertex("P").set("id", 501).save();
        final Vertex y = database.newVertex("P").set("id", 502).save();
        for (int i = 0; i < 150; i++)
          hub.modify().newEdge("K", i % 3 == 0 ? y : x);
        for (int i = 0; i < 70; i++)
          x.modify().newEdge("K", hub);
        for (int i = 0; i < 5; i++)
          hub.modify().newEdge("K", hub);
      });
      final String[] queries = {
          "MATCH (a:P {id:500})-[:K]-(b:P)-[:K]-(c:P) RETURN count(*) AS n",
          "MATCH (a:P {id:500})-[:K]-(b:P)-[:K]-(a) RETURN count(*) AS n",
          "MATCH (a:P {id:500})-[:K]->(b:P)-[:K]->(a) RETURN count(*) AS n",
          "MATCH (a:P {id:500})-[:K]->(b:P), (a)-[:K]->(c:P) RETURN count(*) AS n",
          "MATCH (a:P)-[:K]->(b:P {id:501})<-[:K]-(c:P) RETURN count(*) AS n" };
      final Map<String, List<String>> expected = new LinkedHashMap<>();
      for (final String query : queries)
        expected.put(query, answer(query));

      createView("VERTEX TYPES (P) EDGE TYPES (K, L) PROPERTIES (id)");
      for (final String query : queries) {
        assertThat(plan(query)).as(query).contains("unique relationships");
        assertThat(answer(query)).as(query).isEqualTo(expected.get(query));
      }
    }

    @Test
    void aClauseSplitAcrossTwoViewsWalksTheRecords() {
      // Occurrences numbered in two views, built at different times, need not agree on a pair's parallel relationships
      final String query = "MATCH (a:P)-[:K]->(b:P)-[:K|L]->(c:P) RETURN count(*) AS n";
      final List<String> expected = answer(query);
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gav8394k VERTEX TYPES (P) EDGE TYPES (K) PROPERTIES (id)");
      createView("VERTEX TYPES (P) EDGE TYPES (L) PROPERTIES (id)");
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gav8394kl VERTEX TYPES (P) EDGE TYPES (K, L) PROPERTIES (id)");
      for (final String name : List.of("gav8394k", "gav8394kl")) {
        final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, name);
        assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
      }
      final String plan = plan(query);
      if (plan.contains("provider=gav8394k]") || plan.contains("provider=gav8394k,"))
        assertThat(plan).as("the clause spans two views").doesNotContain("unique relationships");
      assertThat(answer(query)).isEqualTo(expected);
    }

    @Test
    void answersMatchWithChangesServedFromTheOverlay() {
      createView("VERTEX TYPES (P) EDGE TYPES (K, L) PROPERTIES (id) UPDATE MODE SYNCHRONOUS");

      // Parallel edges, self-loops and new vertices committed after the build are served from the view's delta overlay
      database.transaction(() -> {
        final Vertex x = database.newVertex("P").set("id", 100).save();
        final Vertex y = database.query("sql", "SELECT FROM P WHERE id = 1").next().getVertex().get();
        x.modify().newEdge("K", x);
        x.modify().newEdge("K", x);
        x.modify().newEdge("K", y);
        x.modify().newEdge("K", y);
        y.modify().newEdge("K", x);
        y.modify().newEdge("K", y);
      });

      final Map<String, List<String>> withView = new LinkedHashMap<>();
      for (final String query : QUERIES)
        withView.put(query, answer(query));
      assertThat(plan(QUERIES[0])).contains("provider=gav8394");

      database.command("sql", "DROP GRAPH ANALYTICAL VIEW gav8394");
      for (final String query : QUERIES)
        assertThat(withView.get(query)).as(query).isEqualTo(answer(query));
    }

    @Test
    void aVertexTheViewDoesNotMapIsWalkedOnItsRecordsWithUniqueRelationships() {
      // Kept in use while stale: a vertex created after the build is expanded on its edge records
      createView("VERTEX TYPES (P) EDGE TYPES (K, L) PROPERTIES (id) UPDATE MODE OFF");
      GraphAnalyticalViewRegistry.get(database, "gav8394").setUseWhenStale(true);
      database.transaction(() -> {
        final Vertex x = database.newVertex("P").set("id", 100).save();
        x.modify().newEdge("K", x);
        x.modify().newEdge("K", x);
      });

      // Two self-loops: each branch takes one, never both the same
      final String branching = "MATCH (a:P {id:100})-[:K]->(b:P), (a)-[:K]->(c:P) RETURN count(*) AS n";
      assertThat(plan(branching)).contains("GAVExpandAll").contains("unique relationships");
      assertThat(answer(branching)).containsExactly("n=2;");

      // A linear chain is fused: the source the view does not map is expanded on its records, not dropped
      final String chain = "MATCH (a:P {id:100})-[:K]->(b:P)-[:K]->(c:P) RETURN count(*) AS n";
      assertThat(plan(chain)).contains("GAVFusedChain");
      assertThat(answer(chain)).containsExactly("n=2;");
      final String grouped = "MATCH (a:P {id:100})-[:K]->(b:P)-[:K]->(c:P) WITH a, count(*) AS n RETURN a.id AS a, n";
      assertThat(answer(grouped)).containsExactly("a=100;n=2;");
      final String rows = "MATCH (a:P {id:100})-[:K]->(b:P)-[:K]->(c:P) RETURN c.id AS c";
      assertThat(answer(rows)).containsExactly("c=100;", "c=100;");

      final String cycle = "MATCH (a:P {id:100})-[:K]->(b:P)-[:K]->(a) RETURN count(*) AS n";
      assertThat(plan(cycle)).contains("GAVExpandInto");
      assertThat(answer(cycle)).containsExactly("n=2;");
    }

    private static String abbreviate(final List<String> rows) {
      final String s = rows.toString();
      return s.length() > 200 ? rows.size() + " rows: " + s.substring(0, 200) + "..." : s;
    }

    private void createView(final String definition) {
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gav8394 " + definition);
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav8394");
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

    private List<String> answer(final String query) {
      final List<String> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          final StringBuilder line = new StringBuilder();
          for (final String name : row.getPropertyNames())
            line.append(name).append('=').append((Object) row.getProperty(name)).append(';');
          rows.add(line.toString());
        }
      }
      return rows;
    }
  }

  /**
   * Issue #8426, follow-up to #8394/#8425: with {@code KC EXTENDS K}, a Graph Analytical View attributed every edge of a
   * polymorphic bucket to the last type listed, so the answer to {@code [:K]} depended on the order the view listed its
   * edge types, and the count push-down compared edge-type names only, so {@code [:K]} and {@code [:KC]} counted as
   * disjoint although a {@code KC} edge matches both. The view is a performance feature, so the oracle is the answer the
   * same statement gives without it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue8426GAVEdgeSubTypeSlices extends TestHelper {
    private static final int VERTICES = 30;

    private static final String[] QUERIES = {
        // The issue's repro
        "MATCH (a:P)-[:K]->(b:P)-[:KC]->(c:P) RETURN count(*) AS n",
        "MATCH (a:P)-[:KC]->(b:P)-[:K]->(c:P) RETURN count(*) AS n",
        // A type answer is polymorphic, as on the record path
        "MATCH (a:P)-[:K]->(b:P) RETURN count(*) AS n",
        "MATCH (a:P)-[:KC]->(b:P) RETURN count(*) AS n",
        "MATCH (a:P)-[:K]-(b:P) RETURN count(*) AS n",
        "MATCH (a:P)-[]->(b:P) RETURN count(*) AS n",
        "MATCH (a:P)-[:K|L]->(b:P) RETURN count(*) AS n",
        "MATCH (a:P)-[:K]->(b:P)-[:L]->(c:P) RETURN count(*) AS n",
        "MATCH (a:P {id:1})-[:K]->(b:P) RETURN b.id AS b ORDER BY b",
        "MATCH (a:P {id:1})-[:KC]->(b:P) RETURN b.id AS b ORDER BY b",
        // The count push-down: overlapping types are not disjoint, and one inequality guards one pair of hops only
        "MATCH (a:P)-[:K]-(b:P)-[:KC]-(c:P) RETURN count(*) AS n",
        "MATCH (a:P)-[:K]-(b:P)-[:KC]-(c:P) WHERE a <> c RETURN count(*) AS n",
        "MATCH (a:P)-[:K]-(b:P)-[:KC]-(c:P) WHERE a <> b RETURN count(*) AS n",
        "MATCH (a:P)-[:K]->(b:P)-[:KC]->(c:P) WHERE a <> c RETURN count(*) AS n",
        "MATCH (a:P)-[:K]-(b:P)-[:L]-(c:P)-[:K]-(d:P) WHERE a <> d RETURN count(*) AS n",
        "MATCH (a:P)-[:K]-(b:P)-[:L]-(c:P)-[:K]-(d:P) RETURN count(*) AS n",
        "MATCH (a:P)-[:K]-(b:P)-[:L]-(c:P)-[:M]-(d:P) WHERE a <> c RETURN count(*) AS n",
        "MATCH (a:P)-[:K]-(b:P)-[:L]-(c:P) RETURN count(*) AS n",
        "MATCH (a:P)-[:KC]-(b:P)-[:L]-(c:P) RETURN count(*) AS n",
        "MATCH (a:P)-[:K]-(b:P)-[:KC]-(c:P)-[:L]-(d:P) WHERE a <> c RETURN count(*) AS n" };

    private static final String[] VIEWS = {
        "VERTEX TYPES (P) EDGE TYPES (K, KC, L) PROPERTIES (id)",
        "VERTEX TYPES (P) EDGE TYPES (KC, K, L) PROPERTIES (id)",
        "VERTEX TYPES (P) EDGE TYPES (K, L) PROPERTIES (id)",
        "VERTEX TYPES (P) PROPERTIES (id)" };

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE P");
      database.command("sql", "CREATE PROPERTY P.id INTEGER");
      database.command("sql", "CREATE EDGE TYPE K");
      database.command("sql", "CREATE EDGE TYPE KC EXTENDS K");
      database.command("sql", "CREATE EDGE TYPE L");
      database.command("sql", "CREATE EDGE TYPE M");

      // No self-loops: the count push-down counts an undirected self-loop twice, which is a separate defect and would
      // make the two sides of a comparison disagree for a reason that has nothing to do with sub-types
      final Random random = new Random(8426);
      database.transaction(() -> {
        final List<Vertex> vertices = new ArrayList<>();
        for (int i = 0; i < VERTICES; i++)
          vertices.add(database.newVertex("P").set("id", i).save());
        for (int i = 0; i < VERTICES; i++) {
          final Vertex v = vertices.get(i);
          for (int k = 0; k < 3; k++)
            v.asVertex().modify().newEdge("K", otherThan(vertices, i, random));
          if (random.nextInt(3) == 0)
            v.asVertex().modify().newEdge("KC", otherThan(vertices, i, random));
          if (random.nextInt(4) == 0) {
            final Vertex other = otherThan(vertices, i, random);
            v.asVertex().modify().newEdge("KC", other);
            v.asVertex().modify().newEdge("KC", other);
          }
          if (random.nextInt(3) == 0)
            v.asVertex().modify().newEdge("L", otherThan(vertices, i, random));
          v.asVertex().modify().newEdge("M", otherThan(vertices, i, random));
        }
      });
    }

    private static Vertex otherThan(final List<Vertex> vertices, final int index, final Random random) {
      int target;
      do {
        target = random.nextInt(vertices.size());
      } while (target == index);
      return vertices.get(target);
    }

    @Test
    void answersMatchTheQueriesWithoutTheViewWhateverOrderTheTypesAreListed() {
      final Map<String, List<String>> expected = oracle();

      for (final String definition : VIEWS) {
        createView(definition);
        final List<String> failures = new ArrayList<>();
        for (final String query : QUERIES) {
          final List<String> actual = answer(query);
          if (!actual.equals(expected.get(query)))
            failures.add(query + "\n  expected " + expected.get(query) + "\n  actual   " + actual + "\n" + plan(query));
        }
        assertThat(failures).as(definition).isEmpty();
        database.command("sql", "DROP GRAPH ANALYTICAL VIEW gav8426");
      }
    }

    @Test
    void answersMatchWithChangesServedFromTheOverlayOnAViewListingTheSubType() {
      overlayChangesMatchTheOracle("VERTEX TYPES (P) EDGE TYPES (K, KC, L) PROPERTIES (id) UPDATE MODE SYNCHRONOUS");
    }

    @Test
    void answersMatchWithChangesServedFromTheOverlayOnAnUnfilteredView() {
      overlayChangesMatchTheOracle("VERTEX TYPES (P) PROPERTIES (id) UPDATE MODE SYNCHRONOUS");
    }

    private void overlayChangesMatchTheOracle(final String definition) {
      createView(definition);
      // Committed after the build, so served from the overlay
      database.transaction(() -> {
        final Vertex a = database.query("sql", "SELECT FROM P WHERE id = 1").next().getVertex().get();
        final Vertex b = database.query("sql", "SELECT FROM P WHERE id = 2").next().getVertex().get();
        a.modify().newEdge("KC", b);
        b.modify().newEdge("KC", a);
        a.modify().newEdge("K", b);
      });
      final Map<String, List<String>> withView = new LinkedHashMap<>();
      for (final String query : QUERIES)
        withView.put(query, answer(query));
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW gav8426");
      for (final String query : QUERIES)
        assertThat(withView.get(query)).as(query)
            .isEqualTo(answer(query.replace(" RETURN count(*) AS n", " RETURN sum(1) AS n")));
    }

    /** An edge type with no edges when the view was built has no slice, yet an untyped hop must still see its later edges. */
    @Test
    void anUntypedHopSeesTheEdgesOfATypeThatWasEmptyAtBuildTime() {
      database.command("sql", "CREATE EDGE TYPE E0");
      createView("VERTEX TYPES (P) PROPERTIES (id) UPDATE MODE SYNCHRONOUS");
      database.transaction(() -> {
        final Vertex a = database.query("sql", "SELECT FROM P WHERE id = 3").next().getVertex().get();
        final Vertex b = database.query("sql", "SELECT FROM P WHERE id = 4").next().getVertex().get();
        a.modify().newEdge("E0", b);
        a.modify().newEdge("E0", b);
      });
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav8426");
      assertThat(view.getEdgeTypes()).contains("E0");
      assertThat(view.getMaterializedEdgeTypes()).contains("E0");

      final String[] queries = {
          "MATCH (a:P {id:3})-[]->(b:P) RETURN count(*) AS n",
          "MATCH (a:P {id:3})-[]->(b:P {id:4}) RETURN count(*) AS n",
          "MATCH (a:P)-[]-(b:P {id:4}) RETURN count(*) AS n" };
      final Map<String, List<String>> withView = new LinkedHashMap<>();
      for (final String query : queries)
        withView.put(query, answer(query));
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW gav8426");
      for (final String query : queries)
        assertThat(withView.get(query)).as(query).isEqualTo(answer(query));
    }

    /**
     * The self-loop subtraction of an inequality chain counted a returning path once per frontier entry and only tested
     * PRESENCE of the closing neighbour, so a pair joined by parallel edges closed one path instead of one per edge.
     */
    @Test
    void parallelEdgesEachCloseADistinctPathInTheInequalitySubtraction() {
      database.transaction(() -> {
        final Vertex x = database.newVertex("P").set("id", 900).save();
        final Vertex y = database.newVertex("P").set("id", 901).save();
        x.modify().newEdge("K", y);
        y.modify().newEdge("L", x);
        y.modify().newEdge("L", x);
        y.modify().newEdge("L", x);
        x.modify().newEdge("M", y);
      });
      final String query = "MATCH (a:P)-[:K]-(b:P)-[:L]-(c:P)-[:M]-(d:P) WHERE a <> c RETURN count(*) AS n";
      final List<String> expected = answer(query.replace(" RETURN count(*) AS n", " RETURN sum(1) AS n"));
      assertThat(answer(query)).isEqualTo(expected);

      createView("VERTEX TYPES (P) PROPERTIES (id)");
      assertThat(answer(query)).isEqualTo(expected);
    }

    @Test
    void theProviderResolvesATypeToItsSubTypes() {
      createView("VERTEX TYPES (P) EDGE TYPES (K, KC, L) PROPERTIES (id)");
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav8426");
      assertThat(view.resolveEdgeTypes("K")).containsExactly("K", "KC");
      assertThat(view.resolveEdgeTypes("KC")).containsExactly("KC");
      assertThat(view.resolveEdgeTypes("L", "K")).containsExactly("L", "K", "KC");
      assertThat(view.coversEdgeType("KC")).isTrue();
    }

    /**
     * The answer the record path gives, which is what a view has to reproduce. A {@code count(*)} chain is answered by the
     * count push-down with or without a view, so the reference for those goes through {@code sum(1)}, which the push-down
     * does not claim.
     */
    /** getDegrees on a parent type sums its slices once each, on a two- and a three-level hierarchy, with and without an overlay. */
    @Test
    void degreesOfAParentTypeSumTheirSlicesOnce() {
      database.command("sql", "CREATE EDGE TYPE KCC EXTENDS KC");
      database.transaction(() -> {
        final Vertex a = database.query("sql", "SELECT FROM P WHERE id = 5").next().getVertex().get();
        final Vertex b = database.query("sql", "SELECT FROM P WHERE id = 6").next().getVertex().get();
        a.modify().newEdge("KCC", b);
        a.modify().newEdge("KCC", b);
      });
      createView("VERTEX TYPES (P) PROPERTIES (id) UPDATE MODE SYNCHRONOUS");
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav8426");
      assertThat(view.resolveEdgeTypes("KC")).containsExactly("KC", "KCC");

      assertDegreesMatchTheRecords(view);

      database.transaction(() -> {
        final Vertex a = database.query("sql", "SELECT FROM P WHERE id = 5").next().getVertex().get();
        final Vertex b = database.query("sql", "SELECT FROM P WHERE id = 7").next().getVertex().get();
        a.modify().newEdge("KCC", b);
        a.modify().newEdge("KC", b);
      });
      assertDegreesMatchTheRecords(view);
    }

    private void assertDegreesMatchTheRecords(final GraphAnalyticalView view) {
      for (final String type : new String[] { "K", "KC", "KCC" }) {
        final int[] degrees = new int[view.getNodeIdUpperBound()];
        view.getDegrees(degrees, Vertex.DIRECTION.OUT, type);
        for (int id = 0; id < VERTICES; id++) {
          final Vertex v = database.query("sql", "SELECT FROM P WHERE id = " + id).next().getVertex().get();
          assertThat(degrees[view.getNodeId(v.getIdentity())]).as(type + " out-degree of " + id)
              .isEqualTo((int) v.countEdges(Vertex.DIRECTION.OUT, type));
        }
      }
    }

    private Map<String, List<String>> oracle() {
      final Map<String, List<String>> expected = new LinkedHashMap<>();
      for (final String query : QUERIES)
        expected.put(query, answer(query.replace(" RETURN count(*) AS n", " RETURN sum(1) AS n")));
      // The oracle has to see the sub-typed edges, or the comparison proves nothing
      assertThat(expected.get(QUERIES[2])).isNotEqualTo(expected.get(QUERIES[3]));
      return expected;
    }

    private void createView(final String definition) {
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gav8426 " + definition);
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav8426");
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

    private List<String> answer(final String query) {
      final List<String> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          final StringBuilder line = new StringBuilder();
          for (final String name : row.getPropertyNames())
            line.append(name).append('=').append((Object) row.getProperty(name)).append(';');
          rows.add(line.toString());
        }
      }
      return rows;
    }
  }

  /**
   * Issue #8991: once a Graph Analytical View served a single hop, the inline property map on the target node was ignored. The
   * view is a performance feature, so the oracle is the answer the same statements give without it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue8991GAVInlinePropertyMap extends TestHelper {
    private static final String[] QUERIES = {
        "MATCH (x {id: 1})-[:T]->(y {y: 1}) RETURN y.id AS id ORDER BY id",
        "MATCH (x {id: 1})-[:T]->(y:P {y: 1}) RETURN y.id AS id ORDER BY id",
        "MATCH (x:P)-[:T]->(y {y: 1}) RETURN count(*) AS c",
        "MATCH (x:P) OPTIONAL MATCH (x)-[:T]->(y {y: 1}) RETURN x.id AS x, y.id AS y ORDER BY x, y",
        "MATCH (x {id: 1})-[:T]->(y) WHERE y.y = 1 RETURN y.id AS id ORDER BY id" };

    @Test
    void inlinePropertyMapOnTargetStillFiltersWithAView() throws Exception {
      database.transaction(() -> database.command("opencypher", """
          CREATE (a:P {id: 1, y: 1}), (b:P {id: 2, y: 1}), (c:P {id: 3, y: 2}), (d:Q {id: 4, y: 2}),
                 (a)-[:T]->(b), (a)-[:T]->(c), (a)-[:T]->(d), (c)-[:T]->(b)"""));

      final List<List<String>> expected = new ArrayList<>();
      for (final String q : QUERIES)
        expected.add(rows(q));
      assertThat(expected.get(0)).hasSize(1);

      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g");
      GraphAnalyticalViewRegistry.get(database, "g").awaitReady(60, TimeUnit.SECONDS);

      for (int i = 0; i < QUERIES.length; i++)
        assertThat(rows(QUERIES[i])).as(QUERIES[i]).isEqualTo(expected.get(i));
    }

    private List<String> rows(final String query) {
      final List<String> out = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        rs.stream().forEach(r -> out.add(r.toJSON().toString()));
      }
      return out;
    }
  }

  /**
   * Regression test for GitHub issue #9738: with a Graph Analytical View, {@code MATCH (a)-[:K]->(b)-[:K]->(c) RETURN
   * count(*)} was planned as a fused chain feeding a plain aggregation, and the chain built a row for every path and held
   * them all before the aggregation read the first one, so the heap grew with the number of paths and the count ran out
   * of memory on graphs whose view fit in a fraction of it.
   * <p>
   * The count is now folded into the chain's walk, and the rows the chain still produces for everything else are handed
   * over in bounded rounds instead of all at once. The allocation tests run single-threaded (fewer sources than the
   * chain's parallel threshold), so the walk runs on the calling thread and its allocations are that thread's.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9738GAVFusedChainCount {
    private static final String DB_PATH  = "./target/databases/issue-9738-gav-fused-count";
    private static final String TWO_HOPS = "MATCH (a:P)-[:K]->(b:P)-[:K]->(c:P) ";
    private static final String MIXED    = "MATCH (a:P)-[:K]->(b:P)-[:L]->(c:P) ";
    private static final long   MB       = 1024L * 1024L;

    private Database database;
    private int[]    destinations;
    private int      degree;

    @AfterEach
    void tearDown() {
      if (database != null && database.isOpen())
        database.drop();
    }

    @Test
    void plainCountOverTwoHopsAllocatesNothingPerPath() {
      assumeAllocationCounters();
      // 5,000 sources x 20 x 20 = 2,000,000 paths: one row per path costs well over 100 MB of allocations
      buildGraph(5_000, 20);
      createView();
      final String query = TWO_HOPS + "RETURN count(*) AS n";
      assertThat(plan(query)).contains("GAVFusedChain").contains("fused count(*)");

      // Measured on the first run, before the JIT's escape analysis can hide an allocation per path
      final long before = allocatedBytes();
      final long n = scalar(query, "n");
      final long allocated = allocatedBytes() - before;

      assertThat(n).isEqualTo(twoHopPaths());
      assertThat(allocated).as("bytes allocated counting %d paths", n).isLessThan(16 * MB);
    }

    @Test
    void limitOverTwoHopsStopsWalkingOnceServed() {
      assumeAllocationCounters();
      buildGraph(5_000, 20);
      createView();
      final String query = TWO_HOPS + "RETURN a, c LIMIT 10";
      assertThat(plan(query)).contains("GAVFusedChain");

      final long before = allocatedBytes();
      final int rows = rows(query).size();
      final long allocated = allocatedBytes() - before;

      assertThat(rows).isEqualTo(10);
      assertThat(allocated).as("bytes allocated serving 10 of 2,000,000 paths").isLessThan(16 * MB);
    }

    @Test
    void parallelWalkAnswersEveryShape() {
      // More sources than the chain's parallel threshold, so the walk fans out over the query pool
      buildGraph(10_000, 3);
      final List<String> queries = List.of(//
          TWO_HOPS + "RETURN count(*) AS n",//
          TWO_HOPS + "RETURN count(c) AS n",//
          TWO_HOPS + "RETURN sum(c.pid) AS n",//
          TWO_HOPS + "RETURN count(*) AS n, sum(a.pid) AS s",//
          TWO_HOPS + "WHERE c.pid < 5000 RETURN count(*) AS n",//
          TWO_HOPS + "RETURN a.pid AS a, count(*) AS n ORDER BY a LIMIT 20",//
          TWO_HOPS + "WITH a, c, count(*) AS n RETURN sum(n) AS total, count(*) AS pairs",//
          "MATCH (a:P)-[:K]->(b:P)-[:K]->(c:P)-[:K]->(d:P) RETURN count(*) AS n");
      final List<List<String>> expected = new ArrayList<>();
      for (final String query : queries)
        expected.add(rows(query));

      createView();
      for (int i = 0; i < queries.size(); i++)
        assertThat(rows(queries.get(i))).as(queries.get(i)).isEqualTo(expected.get(i));
      assertThat(scalar(TWO_HOPS + "RETURN count(*) AS n", "n")).isEqualTo(twoHopPaths());

      // Every row of the parallel walk, each one exactly once
      final String allRows = TWO_HOPS + "RETURN a.pid AS a, c.pid AS c";
      assertThat(plan(allRows)).contains("GAVFusedChain");
      long checksum = 0;
      long count = 0;
      try (final ResultSet rs = database.query("opencypher", allRows)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          checksum += 1_000_003L * row.<Integer>getProperty("a") + row.<Integer>getProperty("c");
          ++count;
        }
      }
      assertThat(count).isEqualTo(twoHopPaths());
      assertThat(checksum).isEqualTo(twoHopChecksum());

      final List<String> limited = rows(allRows + " LIMIT 7");
      assertThat(limited).hasSize(7);
    }

    @Test
    void countShapesAnswerAsWithoutTheView() {
      buildSmallGraph();
      final List<String> queries = List.of(//
          TWO_HOPS + "RETURN count(*) AS n",//
          TWO_HOPS + "RETURN count(b) AS n",//
          // count of an expression counts its non-null values: never folded into the walk
          TWO_HOPS + "RETURN count(c.name) AS n",//
          TWO_HOPS + "RETURN count(DISTINCT c) AS n",//
          TWO_HOPS + "WHERE c.pid > 10 RETURN count(*) AS n",//
          "MATCH (a:P {pid: -1})-[:K]->(b:P)-[:K]->(c:P) RETURN count(*) AS n",//
          "MATCH (a:P)-[:K]->(b:Q)-[:K]->(c:P) RETURN count(*) AS n",//
          "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P) RETURN count(*) AS n",//
          "MATCH (a:P)-[r1:K]->(b:P)-[r2:K]->(c:P) RETURN count(*) AS n",//
          "MATCH (a:P)-[:K]->(b:P)<-[:K]-(c:P) RETURN count(*) AS n",//
          // Two edge types: no relationship to keep apart, the hops walk their slices untracked
          MIXED + "RETURN count(*) AS n",//
          MIXED + "WITH b, count(*) AS n RETURN b.pid AS b, n ORDER BY b",//
          MIXED + "RETURN a, b, count(*) AS n ORDER BY a.pid, b.pid",//
          MIXED + "RETURN c, count(*) AS n ORDER BY c.pid",//
          MIXED + "WHERE c.pid > 10 RETURN count(*) AS n",//
          "MATCH (a:P)-[:K]->(b:P)-[:L]->(c:Q) RETURN count(*) AS n",//
          "MATCH (a:P)-[:K]->(b:P)-[:L]-(c:P) RETURN count(*) AS n",//
          "MATCH (a:P)-[:L]->(b:P)<-[:L]-(c:P) RETURN count(*) AS n",//
          "MATCH (a:P)-[:K]->(b:P) MATCH (b)-[:L]->(c:P) RETURN count(*) AS n",//
          "MATCH (a:P)-[:K]->(b:P)-[:K]->(c:P)-[:L]->(d:P) RETURN a, count(*) AS n ORDER BY a.pid",//
          MIXED + "RETURN a.pid AS a, b.pid AS b, c.pid AS c ORDER BY a, b, c",//
          TWO_HOPS + "RETURN a.pid AS a, count(*) AS n ORDER BY a",//
          TWO_HOPS + "RETURN a, count(*) AS n ORDER BY n DESC, a.pid LIMIT 5",//
          TWO_HOPS + "RETURN a.pid AS a, c.pid AS c, count(*) AS n ORDER BY a, c",//
          TWO_HOPS + "WITH count(*) AS n RETURN n",//
          TWO_HOPS + "WITH a, count(*) AS n RETURN a.pid AS a, n ORDER BY a",//
          // A WITH between the chain and the aggregation: the chain's rows are not the aggregation's input
          TWO_HOPS + "WITH a, c WITH a, count(*) AS n RETURN a.pid AS a, n ORDER BY a",//
          TWO_HOPS + "WITH a, b, c WITH count(*) AS n RETURN n",//
          TWO_HOPS + "WITH DISTINCT a, c RETURN count(*) AS n",//
          TWO_HOPS + "RETURN a.pid AS a, b.pid AS b, c.pid AS c ORDER BY a, b, c",//
          // The RETURN's own modifiers still apply to the rows a fused count answers
          TWO_HOPS + "RETURN count(*) AS n SKIP 1",//
          TWO_HOPS + "RETURN count(*) AS n LIMIT 0",//
          TWO_HOPS + "RETURN DISTINCT count(*) AS n",//
          TWO_HOPS + "RETURN DISTINCT a, count(*) AS n ORDER BY n DESC, a.pid LIMIT 3",//
          TWO_HOPS + "RETURN a, count(*) AS n ORDER BY n, a.pid SKIP 2 LIMIT 4",//
          TWO_HOPS + "WITH a, count(*) AS n WHERE n > 20 RETURN a.pid AS a, n ORDER BY a",//
          // A cycle back to the source is never one fused chain: its last hop expands INTO a bound vertex
          "MATCH (a:P)-[:K]->(b:P)-[:K]->(a) RETURN count(*) AS n");
      final List<List<String>> expected = new ArrayList<>();
      for (final String query : queries)
        expected.add(rows(query));

      createView();
      for (int i = 0; i < queries.size(); i++)
        assertThat(rows(queries.get(i))).as(queries.get(i)).isEqualTo(expected.get(i));
      assertThat(plan(TWO_HOPS + "RETURN count(*) AS n")).contains("fused count(*)");
      assertThat(plan(TWO_HOPS + "RETURN count(c.name) AS n")).doesNotContain("fused count");
      // A grouping key that is not a chain variable is a value the chain cannot pack: the aggregation step groups it
      assertThat(plan(TWO_HOPS + "RETURN a.pid AS a, count(*) AS n")).doesNotContain("fused count");
      // Two edge types keep no relationship apart: the fused count walks untracked hops (an ungrouped count of that shape
      // never reaches the chain, the CSR count push-down answers it first)
      assertThat(plan(MIXED + "RETURN a, b, count(*) AS n")).contains("fused count(*) AS n BY a, b")
          .doesNotContain("unique relationships");
      assertThat(plan(TWO_HOPS + "WITH count(*) AS n RETURN n")).contains("fused count(*) AS n");
      assertThat(plan(TWO_HOPS + "RETURN a, count(*) AS n")).contains("fused count(*) AS n BY a");
      // The chain feeds a projection, not the aggregation
      assertThat(plan(TWO_HOPS + "WITH a, c WITH a, count(*) AS n RETURN a, n")).doesNotContain("fused count");

      // Vertices and edges committed after the build reach the fused count too
      database.transaction(() -> {
        final Vertex first = database.query("sql", "SELECT FROM P WHERE pid = 0").next().getVertex().get();
        final MutableVertex x = database.newVertex("P").set("pid", 1000).save();
        final MutableVertex y = database.newVertex("P").set("pid", 1001).save();
        x.newEdge("K", y);
        y.newEdge("K", first);
        first.modify().newEdge("K", x);
      });
      final String count = TWO_HOPS + "RETURN count(*) AS n";
      final List<String> withView = rows(count);
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW v9738");
      assertThat(withView).isEqualTo(rows(count));
    }

    /**
     * A rebuild while the rows stream: the walk read the snapshot the statement pinned, so the rounds after the rebuild go on
     * reading it, and every row is right. It used to fail the rest of the walk with a {@link NeedRetryException} (issue #10415).
     */
    @Test
    void aRebuildWhileTheRowsStreamDoesNotMixNumberings() {
      // 2,000 x 10 x 10 = 200,000 paths: many rounds of the single walker
      buildGraph(2_000, 10);
      createView();
      final String query = TWO_HOPS + "RETURN a, c";
      assertThat(plan(query)).contains("GAVFusedChain");

      long rows = 0;
      try (final ResultSet rs = database.query("opencypher", query)) {
        assertThat(rs.hasNext()).isTrue();
        rs.next();
        ++rows;
        // A rebuild numbers the nodes afresh: the walk's IDs would name other records in the new numbering
        database.command("sql", "REBUILD GRAPH ANALYTICAL VIEW v9738");
        assertThat(GraphAnalyticalViewRegistry.get(database, "v9738").awaitReady(60, TimeUnit.SECONDS)).isTrue();
        while (rs.hasNext()) {
          final Result row = rs.next();
          assertThat(row.<Vertex>getProperty("a").getTypeName()).isEqualTo("P");
          assertThat(row.<Vertex>getProperty("c").getTypeName()).isEqualTo("P");
          ++rows;
        }
      }
      assertThat(rows).isEqualTo(twoHopPaths());
    }

    @Test
    void aViewRestoredOnFirstUseAfterAReopenServesTheWalkWithoutARetry() {
      buildSmallGraph();
      final List<String> queries = List.of(//
          TWO_HOPS + "RETURN a, count(*) AS n ORDER BY a.pid",//
          TWO_HOPS + "RETURN a.pid AS a, c.pid AS c ORDER BY a, c");
      final List<List<String>> expected = new ArrayList<>();
      for (final String query : queries)
        expected.add(rows(query));
      createView();

      // Reopened, the view is restored from disk only when something first asks for it: the walk's numbering is the
      // restored one, never the empty view's
      database.close();
      database = new DatabaseFactory(DB_PATH).open();
      for (int i = 0; i < queries.size(); i++) {
        assertThat(plan(queries.get(i))).as(queries.get(i)).contains("GAVFusedChain");
        assertThat(rows(queries.get(i))).as(queries.get(i)).isEqualTo(expected.get(i));
      }
      assertThat(GraphAnalyticalViewRegistry.get(database, "v9738").isRestoredFromPersistedCsr()).isTrue();
    }

    @Test
    void bucketOfEveryNodeIsTheBucketOfItsRid() {
      buildSmallGraph();
      createView();
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "v9738");
      final int built = view.getNodeIdUpperBound();
      // Appended past the built mapping by the synchronous overlay
      database.transaction(() -> {
        for (int i = 0; i < 3; i++)
          database.newVertex(i % 2 == 0 ? "Q" : "P").set("pid", 700 + i).save();
      });
      final int bound = view.getNodeIdUpperBound();
      assertThat(bound).isGreaterThan(built);

      assertThat(view.getBucketId(-1)).isEqualTo(-1);
      // Past the bound too: no RID, no bucket
      for (int nodeId = 0; nodeId < bound + 3; nodeId++) {
        final RID rid = view.getRID(nodeId);
        assertThat(view.getBucketId(nodeId)).as("node %d", nodeId).isEqualTo(rid != null ? rid.getBucketId() : -1);
      }
    }

    @Test
    void sourcesTheViewDoesNotMapAreCountedOnTheirRecords() {
      buildSmallGraph();
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW v9738 VERTEX TYPES (P, Q) EDGE TYPES (K, L) PROPERTIES (pid) "
          + "UPDATE MODE OFF");
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "v9738");
      assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
      // Kept in use while stale: the vertices created after the build are sources the view does not map
      view.setUseWhenStale(true);
      database.transaction(() -> {
        final List<Vertex> old = new ArrayList<>();
        try (final ResultSet rs = database.query("sql", "SELECT FROM P WHERE pid < 6 ORDER BY pid")) {
          while (rs.hasNext())
            old.add(rs.next().getVertex().get());
        }
        MutableVertex previous = null;
        for (int i = 0; i < 4; i++) {
          final MutableVertex x = database.newVertex("P").set("pid", 500 + i).save();
          x.newEdge("K", old.get(i));
          x.newEdge("K", old.get(i + 1));
          if (previous != null)
            x.newEdge("K", previous);
          previous = x;
        }
      });

      final List<String> queries = List.of(//
          TWO_HOPS + "RETURN count(*) AS n",//
          TWO_HOPS + "WHERE c.pid > 10 RETURN count(*) AS n",//
          "MATCH (a:P)-[:K]->(b:P)-[:K]->(c:P)-[:K]->(d:P) RETURN count(*) AS n",//
          "MATCH (a:P)-[:K]->(b:P)-[:K]->(c:P)-[:K]->(d:P) WHERE d.pid < 30 RETURN count(*) AS n",//
          TWO_HOPS + "RETURN a.pid AS a, count(*) AS n ORDER BY a",//
          TWO_HOPS + "RETURN a.pid AS a, c.pid AS c ORDER BY a, c");
      assertThat(plan(queries.get(0))).contains("fused count(*)");
      final List<List<String>> withView = new ArrayList<>();
      for (final String query : queries)
        withView.add(rows(query));

      database.command("sql", "DROP GRAPH ANALYTICAL VIEW v9738");
      for (int i = 0; i < queries.size(); i++)
        assertThat(withView.get(i)).as(queries.get(i)).isEqualTo(rows(queries.get(i)));
    }

    private void buildGraph(final int vertices, final int degree) {
      createDatabase();
      this.degree = degree;
      final Random random = new Random(9738);
      destinations = new int[vertices * degree];
      for (int i = 0; i < vertices; i++)
        for (int k = 0; k < degree; k++)
          destinations[i * degree + k] = (i + 1 + random.nextInt(vertices - 1)) % vertices;

      final RID[] rids = new RID[vertices];
      try (final GraphBatch batch = database.batch().withBatchSize(50_000).withCommitEvery(50_000).build()) {
        final Object[][] properties = new Object[vertices][];
        for (int i = 0; i < vertices; i++)
          properties[i] = new Object[] { "pid", i };
        System.arraycopy(batch.createVertices("P", properties), 0, rids, 0, vertices);
        for (int i = 0; i < vertices; i++)
          for (int k = 0; k < degree; k++)
            batch.newEdge(rids[i], "K", rids[destinations[i * degree + k]]);
      }
    }

    /** A graph with self-loops, parallel edges, a second label and a property that is often missing. */
    private void buildSmallGraph() {
      createDatabase();
      final Random random = new Random(9738);
      database.transaction(() -> {
        final MutableVertex[] vertices = new MutableVertex[60];
        for (int i = 0; i < vertices.length; i++) {
          vertices[i] = database.newVertex(i % 7 == 0 ? "Q" : "P").set("pid", i);
          if (i % 3 != 0)
            vertices[i].set("name", "v" + i);
          vertices[i].save();
        }
        for (int i = 0; i < vertices.length; i++)
          for (int k = 0; k < 4; k++)
            vertices[i].newEdge("K", vertices[random.nextInt(vertices.length)]);
        vertices[5].newEdge("K", vertices[5]);
        vertices[6].newEdge("K", vertices[8]);
        vertices[6].newEdge("K", vertices[8]);
        // A second type, so a chain mixing the two has no relationship to keep apart: its last hop is counted whole
        for (int i = 0; i < vertices.length; i += 2)
          for (int k = 0; k < 3; k++)
            vertices[i].newEdge("L", vertices[random.nextInt(vertices.length)]);
        vertices[8].newEdge("L", vertices[8]);
        vertices[8].newEdge("L", vertices[9]);
        vertices[8].newEdge("L", vertices[9]);
      });
    }

    private void createDatabase() {
      final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("P");
      database.getSchema().createVertexType("Q").addSuperType("P");
      database.getSchema().createEdgeType("K");
      database.getSchema().createEdgeType("L");
    }

    private void createView() {
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW v9738 VERTEX TYPES (P, Q) EDGE TYPES (K, L) PROPERTIES (pid) "
          + "UPDATE MODE SYNCHRONOUS");
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "v9738");
      assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    }

    private long twoHopPaths() {
      // Every edge a -> b continues through each of the degree edges leaving b
      return (long) destinations.length * degree;
    }

    private long twoHopChecksum() {
      long checksum = 0;
      for (int e = 0; e < destinations.length; e++) {
        final int a = e / degree;
        final int b = destinations[e];
        for (int k = 0; k < degree; k++)
          checksum += 1_000_003L * a + destinations[b * degree + k];
      }
      return checksum;
    }

    private String plan(final String query) {
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
        return rs.getExecutionPlan().map(p -> p.prettyPrint(0, 2)).orElse("");
      }
    }

    private long scalar(final String query, final String column) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        return ((Number) rs.next().getProperty(column)).longValue();
      }
    }

    private List<String> rows(final String query) {
      final List<String> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          final StringBuilder sb = new StringBuilder();
          for (final String name : row.getPropertyNames()) {
            final Object value = row.getProperty(name);
            sb.append(name).append('=').append(value instanceof Vertex v ? v.getIdentity() : value).append(';');
          }
          rows.add(sb.toString());
        }
      }
      return rows;
    }

    /** The allocation tests read the per-thread allocation counter of HotSpot-derived JVMs; elsewhere they are skipped. */
    private static void assumeAllocationCounters() {
      assumeTrue(ManagementFactory.getThreadMXBean() instanceof ThreadMXBean bean && bean.isThreadAllocatedMemorySupported()
          && bean.isThreadAllocatedMemoryEnabled(), "per-thread allocation counters not available on this JVM");
    }

    private static long allocatedBytes() {
      final ThreadMXBean bean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
      return bean.getThreadAllocatedBytes(Thread.currentThread().threadId());
    }
  }

  /**
   * Issue #10213: a MATCH clause whose hops may bind the same relationship is walked through a Graph Analytical View also
   * when an edge type it walks has sub-types. The view answers {@code E} from the {@code E} and {@code F EXTENDS E} slices,
   * and the label each hop binds used to name the hop's REQUESTED type, so one {@code F} edge read by a hop on {@code E}
   * and by a hop on {@code F} carried two names and the collision went unseen: such a clause had to walk the edge records
   * (#8394). The walk now reads one concrete slice at a time and names a relationship after its slice.
   * <p>
   * The view is a performance feature, so the oracle is the answer without it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue10213GAVEdgeSubTypeUniqueness extends TestHelper {
    private static final int VERTICES = 25;

    private static final String[] QUERIES = {
        // A chain over the parent, the sub-type, and the two mixed
        "MATCH (a:P)-[:E]->(b:P)-[:E]->(c:P) RETURN count(*) AS n",
        "MATCH (a:P)-[:E]-(b:P)-[:E]-(c:P) RETURN count(*) AS n",
        "MATCH (a:P)-[:E]->(b:P)-[:F]->(c:P) RETURN count(*) AS n",
        "MATCH (a:P)-[:F]-(b:P)-[:E]-(c:P) RETURN count(*) AS n",
        "MATCH (a:P)-[:F]-(b:P)-[:F]-(c:P) RETURN count(*) AS n",
        "MATCH (a:P)-[:E]->(b:P)-[:E]->(c:P)-[:E]->(d:P) RETURN count(*) AS n",
        // A grandchild type, and a type listed together with its sub-type
        "MATCH (a:P)-[:G]-(b:P)-[:E]-(c:P) RETURN count(*) AS n",
        "MATCH (a:P)-[:E|F]->(b:P)-[:F]->(c:P) RETURN count(*) AS n",
        // Rows and groups, not only counts
        "MATCH (a:P)-[:E]->(b:P)-[:F]->(c:P) RETURN a.id AS a, b.id AS b, c.id AS c ORDER BY a, b, c",
        "MATCH (a:P)-[:E]-(b:P)-[:E]-(c:P) WITH a, count(*) AS n RETURN a.id AS a, n ORDER BY a",
        // Branching, and closing a cycle
        "MATCH (a:P)-[:E]->(b:P), (a)-[:F]->(c:P) RETURN count(*) AS n",
        "MATCH (a:P)-[:E]->(b:P)-[:F]->(a) RETURN count(*) AS n",
        "MATCH (a:P)-[:E]-(b:P)-[:E]-(a) RETURN count(*) AS n",
        // Untyped hops walk every type, sub-types included, each once
        "MATCH (a:P)-[]->(b:P)-[]->(c:P) RETURN count(*) AS n",
        "MATCH (a:P)-[]-(b:P)-[:F]-(c:P) RETURN count(*) AS n" };

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE P");
      database.command("sql", "CREATE PROPERTY P.id INTEGER");
      database.command("sql", "CREATE EDGE TYPE E");
      database.command("sql", "CREATE EDGE TYPE F EXTENDS E");
      database.command("sql", "CREATE EDGE TYPE G EXTENDS F");
      database.command("sql", "CREATE EDGE TYPE L");

      final Random random = new Random(10213);
      final String[] types = { "E", "F", "G", "L" };
      database.transaction(() -> {
        final List<Vertex> vertices = new ArrayList<>();
        for (int i = 0; i < VERTICES; i++)
          vertices.add(database.newVertex("P").set("id", i).save());
        for (final Vertex v : vertices) {
          for (int k = 0; k < 4; k++)
            v.asVertex().modify().newEdge(types[random.nextInt(types.length)], vertices.get(random.nextInt(VERTICES)));
          // Self-loops and parallel relationships, of a type and of its sub-type, between the same pair
          if (random.nextInt(3) == 0)
            v.asVertex().modify().newEdge(random.nextBoolean() ? "E" : "F", v);
          if (random.nextInt(3) == 0) {
            final Vertex other = vertices.get(random.nextInt(VERTICES));
            v.asVertex().modify().newEdge("F", other);
            v.asVertex().modify().newEdge("F", other);
            v.asVertex().modify().newEdge("E", other);
          }
        }
      });
    }

    @Test
    void aClauseOverEdgeSubTypesIsServedByTheViewWithTheSameAnswers() {
      final Map<String, List<String>> expected = expectedWithoutView();

      createView("VERTEX TYPES (P) EDGE TYPES (E, L) PROPERTIES (id)");

      final List<String> failures = new ArrayList<>();
      for (final String query : QUERIES) {
        final String plan = plan(query);
        if (!plan.contains("unique relationships"))
          failures.add(query + "\n  not walked through the view with relationship labels\n" + plan);
        final List<String> actual = answer(query);
        if (!actual.equals(expected.get(query)))
          failures.add(query + "\n  expected " + expected.get(query) + "\n  actual   " + actual + "\n" + plan);
      }
      assertThat(failures).isEmpty();
    }

    /**
     * A view over part of the hierarchy only - F and the G under it, not E. A hop on E is not covered and the clause walks
     * the records; hops on F and G are walked through the view, F answering for G, with the same answers.
     */
    @Test
    void aViewOverPartOfTheHierarchyKeepsTheAnswers() {
      final String[] queries = {
          "MATCH (a:P)-[:F]-(b:P)-[:F]-(c:P) RETURN count(*) AS n",
          "MATCH (a:P)-[:F]->(b:P)-[:G]->(c:P) RETURN count(*) AS n",
          "MATCH (a:P)-[:G]-(b:P)-[:F]-(a) RETURN count(*) AS n",
          "MATCH (a:P)-[:E]->(b:P)-[:F]->(c:P) RETURN count(*) AS n" };
      final Map<String, List<String>> expected = new LinkedHashMap<>();
      for (final String query : queries)
        expected.put(query, answer(query));

      createView("VERTEX TYPES (P) EDGE TYPES (F) PROPERTIES (id)");
      for (int i = 0; i < 3; i++)
        assertThat(plan(queries[i])).as(queries[i]).contains("unique relationships");
      for (final String query : queries)
        assertThat(answer(query)).as(query + "\n" + plan(query)).isEqualTo(expected.get(query));
    }

    @Test
    void theIssueRepro() {
      final String query = "MATCH (a:P)-[:E]->(b:P)-[:E]->(c:P) RETURN count(*) AS n";
      final List<String> expected = answer(query);
      createView("VERTEX TYPES (P) EDGE TYPES (E, L) PROPERTIES (id)");
      assertThat(plan(query)).contains("GAVFusedChain").contains("unique relationships");
      assertThat(answer(query)).isEqualTo(expected);
    }

    @Test
    void subTypeEdgesServedFromTheOverlayStayUnique() {
      createView("VERTEX TYPES (P) EDGE TYPES (E, L) PROPERTIES (id) UPDATE MODE SYNCHRONOUS");
      database.transaction(() -> {
        final Vertex x = database.newVertex("P").set("id", 100).save();
        final Vertex y = database.query("sql", "SELECT FROM P WHERE id = 1").next().getVertex().get();
        x.modify().newEdge("F", x);
        x.modify().newEdge("E", x);
        x.modify().newEdge("F", y);
        x.modify().newEdge("F", y);
        y.modify().newEdge("G", x);
      });

      final Map<String, List<String>> withView = new LinkedHashMap<>();
      for (final String query : QUERIES)
        withView.put(query, answer(query));
      assertThat(plan(QUERIES[0])).contains("provider=gav10213");

      database.command("sql", "DROP GRAPH ANALYTICAL VIEW gav10213");
      for (final String query : QUERIES)
        assertThat(withView.get(query)).as(query).isEqualTo(answer(query));
    }

    @Test
    void aVertexTheViewDoesNotMapLabelsItsSubTypeEdgesLikeTheView() {
      // The record walk of an unmapped source names an edge after its own type, which is the slice's name in the view
      createView("VERTEX TYPES (P) EDGE TYPES (E, L) PROPERTIES (id) UPDATE MODE OFF");
      GraphAnalyticalViewRegistry.get(database, "gav10213").setUseWhenStale(true);
      database.transaction(() -> {
        final Vertex x = database.newVertex("P").set("id", 100).save();
        x.modify().newEdge("F", x);
        x.modify().newEdge("E", x);
      });

      // Two self-loops, one F and one E: each branch takes one of them, never the same one twice
      final String branching = "MATCH (a:P {id:100})-[:E]->(b:P), (a)-[:E]->(c:P) RETURN count(*) AS n";
      assertThat(plan(branching)).contains("unique relationships");
      assertThat(answer(branching)).containsExactly("n=2;");
      final String mixed = "MATCH (a:P {id:100})-[:E]->(b:P), (a)-[:F]->(c:P) RETURN count(*) AS n";
      assertThat(answer(mixed)).containsExactly("n=1;");
      final String cycle = "MATCH (a:P {id:100})-[:E]->(b:P)-[:F]->(a) RETURN count(*) AS n";
      assertThat(answer(cycle)).containsExactly("n=1;");
    }

    private Map<String, List<String>> expectedWithoutView() {
      final Map<String, List<String>> expected = new LinkedHashMap<>();
      for (final String query : QUERIES) {
        assertThat(plan(query)).as(query).doesNotContain("GAV");
        expected.put(query, answer(query));
      }
      return expected;
    }

    private void createView(final String definition) {
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gav10213 " + definition);
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav10213");
      assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    }

    private String plan(final String query) {
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
        return rs.getExecutionPlan().get().prettyPrint(0, 2);
      }
    }

    private List<String> answer(final String query) {
      final List<String> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          final StringBuilder line = new StringBuilder();
          for (final String name : row.getPropertyNames())
            line.append(name).append('=').append((Object) row.getProperty(name)).append(';');
          rows.add(line.toString());
        }
      }
      return rows;
    }
  }

  /**
   * Issue #10548: a variable-length hop that reports distinct end vertices walks a ready Graph Analytical View's adjacency
   * ({@code GAVVarLengthExpand}) instead of the edge lists of the records. The view is a performance feature, so the oracle is the
   * answer without it: the path enumeration of the same statement made ineligible by a named path, which no view serves.
   */
  @Nested
  class Issue10548GAVVarLengthExpand extends TestHelper {
    private static final String VIEW     = "gav10548";
    private static final String OPERATOR    = "GAVVarLengthExpand";
    private static final String FUSED_COUNT = "DISTINCT ENDPOINT COUNT";
    private static final int    VERTICES = 7;

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createVertexType("V").createProperty("id", Type.INTEGER);
        database.getSchema().createEdgeType("E");
        database.getSchema().createEdgeType("F");
      });
    }

    /**
     * On small random multigraphs (self loops, parallel and antiparallel edges, cycles, two relationship types) every eligible shape
     * walked through the view answers what the path enumeration answers: OUT, IN and BOTH, every path mode, lower bounds 0 and 1,
     * with and without {@code WHERE b <> a}, from every start vertex and from one seeded by its id.
     */
    @Test
    void everyShapeThroughTheViewAnswersWhatPathEnumerationAnswers() {
      final String[][] directions = { { "-", "->" }, { "<-", "-" }, { "-", "-" } };
      final String[] modes = { "", "WALK ", "ACYCLIC ", "TRAIL " };
      final String[] tails = {
          "RETURN a.id AS s, count(DISTINCT b) AS c ORDER BY s",
          "WHERE b <> a RETURN a.id AS s, count(DISTINCT b) AS c ORDER BY s",
          "RETURN DISTINCT a.id AS s, b.id AS v ORDER BY s, v",
          // the issue's form: the end vertices are counted as the walk reaches them, no row is built per end vertex
          "RETURN count(DISTINCT b) AS c",
          "WHERE a <> b RETURN count(DISTINCT b) AS c",
          "WHERE b <> a RETURN count(DISTINCT a) AS c" };
      int compared = 0;
      int undirectedTrails = 0;
      for (int seed = 0; seed < 4; seed++) {
        buildGraph(seed);
        createView();
        for (final String[] dir : directions)
          for (final String mode : modes)
            for (final int min : new int[] { 0, 1 })
              for (final int max : new int[] { 1, 2, 3, 4 })
                for (final String types : new String[] { ":E", ":E|F", "" }) {
                  final String hop = dir[0] + "[" + types + "*" + min + ".." + max + "]" + dir[1];
                  for (final String start : new String[] { "(a:V)", "(a:V {id: " + (seed % VERTICES) + "})" })
                    // an unlabeled end node is planned by the step-based executor, whose ExpandPathStep walks the view too
                    for (final String end : new String[] { "(b:V) ", "(b) " })
                      for (final String tail : tails) {
                        final String eligible = "MATCH " + mode + start + hop + end + tail;
                        final String reference = "MATCH p = " + mode + start + hop + end + tail;
                        assertThat(profileOf(eligible)).as(eligible).contains("provider=" + VIEW);
                        if (end.equals("(b:V) ")) {
                          assertThat(planOf(eligible)).as(eligible).contains(OPERATOR);
                          // seeded on an unindexed id, the plan may filter the far end above the expansion: that filter is no b <> a
                          if (start.equals("(a:V)") && (tail.endsWith("count(DISTINCT b) AS c") || tail.endsWith("count(DISTINCT a) AS c")))
                            assertThat(profileOf(eligible)).as(eligible).contains(FUSED_COUNT);
                        }
                        assertThat(profileOf(reference)).as(reference).doesNotContain("provider=");
                        compared++;
                        if (dir[0].equals("-") && dir[1].equals("-") && min == 1 && (mode.isEmpty() || mode.equals("TRAIL ")))
                          undirectedTrails++;
                        assertThat(rows(eligible)).as("seed=%d %s", seed, eligible).isEqualTo(rows(reference));
                      }
                }
        dropView();
      }
      assertThat(compared).isGreaterThan(24_000);
      // the undirected trails with a lower bound of 1 decide the start from a cycle search of their own
      assertThat(undirectedTrails).isGreaterThan(1_500);
    }

    /**
     * The issue's statement on a graph wider than one checkpoint interval, from every start vertex in turn: the visited set reused
     * from one start to the next must forget the previous walk, and the answers must be the record walk's.
     */
    @Test
    void manyStartsOnALargerGraphAnswerWhatTheRecordWalkAnswers() {
      final Random random = new Random(10548);
      database.transaction(() -> {
        final List<MutableVertex> vertices = new ArrayList<>();
        for (int i = 0; i < 1500; i++)
          vertices.add(database.newVertex("V").set("id", i).save());
        for (final MutableVertex from : vertices)
          for (int e = 0; e < 3; e++)
            from.newEdge("E", vertices.get(random.nextInt(vertices.size()))).save();
      });

      final String[] queries = {
          "MATCH (a:V)-[:E*1..3]->(b:V) WHERE b <> a RETURN a.id AS s, count(DISTINCT b) AS c ORDER BY s",
          "MATCH (a:V {id: 1})-[:E*1..6]->(b:V) WHERE b <> a RETURN count(DISTINCT b) AS c",
          "MATCH (a:V {id: 1})-[:E*0..]-(b:V) RETURN count(DISTINCT b) AS c",
          "MATCH (a:V {id: 1})<-[:E*1..4]-(b:V) RETURN count(DISTINCT b) AS c" };
      final List<List<String>> expected = new ArrayList<>();
      for (final String query : queries) {
        assertThat(planOf(query)).as(query).contains("DISTINCT ENDPOINTS").doesNotContain(OPERATOR);
        expected.add(rows(query));
      }
      // the record walk itself must reach something, or the comparison below proves nothing
      assertThat(expected.get(1).get(0)).isNotEqualTo("c=0");

      createView();
      for (int i = 0; i < queries.length; i++) {
        assertThat(planOf(queries[i])).as(queries[i]).contains(OPERATOR);
        assertThat(rows(queries[i])).as(queries[i]).isEqualTo(expected.get(i));
      }
    }

    /** The view is not used where it cannot answer alone, and the statement falls back to walking the records. */
    @Test
    void fallsBackWhereTheViewCannotAnswer() {
      buildGraph(3);

      // no view at all
      final String eligible = "MATCH (a:V)-[:E*1..3]->(b:V) RETURN a.id AS s, count(DISTINCT b) AS c ORDER BY s";
      assertThat(planOf(eligible)).contains("DISTINCT ENDPOINTS").doesNotContain(OPERATOR);

      database.transaction(() -> database.command("sql", "UPDATE E SET w = 1"));
      createView();
      assertThat(planOf(eligible)).contains(OPERATOR);

      // the path, the relationship variable read downstream, an inline property map or WHERE: none of them is the view's to answer
      for (final String query : new String[] {
          "MATCH p = (a:V)-[:E*1..3]->(b:V) RETURN a.id AS s, count(DISTINCT b) AS c ORDER BY s",
          "MATCH (a:V)-[r:E*1..3]->(b:V) RETURN a.id AS s, count(DISTINCT b) AS c, collect(DISTINCT size(r)) AS l ORDER BY s",
          "MATCH (a:V)-[:E*1..3 {w: 1}]->(b:V) RETURN a.id AS s, count(DISTINCT b) AS c ORDER BY s",
          "MATCH (a:V)-[r:E*1..3 WHERE r.w = 1]->(b:V) RETURN a.id AS s, count(DISTINCT b) AS c ORDER BY s",
          // a lower bound of 2 enumerates paths
          "MATCH (a:V)-[:E*2..3]->(b:V) RETURN a.id AS s, count(DISTINCT b) AS c ORDER BY s" })
        assertThat(planOf(query)).as(query).doesNotContain(OPERATOR);

      // a view over the types the pattern does not cover is not one that can serve it
      assertThat(planOf("MATCH (a:V)-[:F*1..3]->(b:V) RETURN a.id AS s, count(DISTINCT b) AS c ORDER BY s")).contains(OPERATOR);
      dropView();
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW " + VIEW + " VERTEX TYPES (V) EDGE TYPES (F) UPDATE MODE SYNCHRONOUS");
      awaitReady();
      assertThat(planOf(eligible)).doesNotContain(OPERATOR);
    }

    /**
     * The count is taken from the walks only where nothing else reads the rows: another RETURN item, a predicate besides
     * {@code b <> a}, or a count that is not DISTINCT keeps the rows and the aggregation. Every form answers what the rows answer.
     */
    @Test
    void theCountIsFusedOnlyWhereNothingElseReadsTheRows() {
      buildGraph(7);
      database.transaction(() -> database.command("sql", "CREATE VERTEX TYPE W EXTENDS V"));
      database.transaction(() -> {
        final MutableVertex w = database.newVertex("W").set("id", 50).save();
        w.newEdge("E", database.select().fromType("V").where().property("id").eq().value(1).vertices().next()).save();
        database.select().fromType("V").where().property("id").eq().value(2).vertices().next().newEdge("E", w).save();
      });
      createView();

      final String[] fused = {
          "MATCH (a:V {id: 2})-[:E*1..4]-(b:V) WHERE b <> a RETURN count(DISTINCT b) AS c",
          "MATCH (a:V)-[:E*0..3]->(b:V) RETURN count(DISTINCT b) AS c",
          "MATCH (a:V)-[:E*1..3]->(b:V) WHERE a <> b RETURN count(DISTINCT b) AS c",
          // the end vertex label is tested on the node id, sub-types included
          "MATCH (a:V)-[:E|F*1..3]->(b:W) RETURN count(DISTINCT b) AS c",
          "MATCH (a:W)-[:E*1..3]-(b:V) WHERE b <> a RETURN count(DISTINCT b) AS c",
          // anchored on the other end: the expansion counts the vertices it reaches, whichever end of the pattern they are
          "MATCH (b:V)-[:E*1..2]->(a:V {id: 3}) WHERE a <> b RETURN count(DISTINCT b) AS c",
          // the end the walk starts from: counted when its walk reaches an end vertex
          "MATCH (a:V {id: 2})-[:E*1..3]->(b:V) WHERE b <> a RETURN count(DISTINCT a) AS c",
          "MATCH (a:V)-[:E*1..3]->(b:W) RETURN count(DISTINCT a) AS c",
          "MATCH (a:V)-[:E*1..1]->(b:V) WHERE b <> a RETURN count(DISTINCT a) AS c" };
      final String[] notFused = {
          "MATCH (a:V)-[:E*1..3]->(b:V) WHERE b <> a AND b.id > 1 RETURN count(DISTINCT b) AS c",
          "MATCH (a:V)-[:E*1..3]->(b:V) WHERE b.id <> a.id RETURN count(DISTINCT b) AS c",
          "MATCH (a:V)-[:E*1..3]->(b:V) RETURN count(DISTINCT b) AS c, count(DISTINCT a) AS d",
          "MATCH (a:V)-[:E*1..3]->(b:V) RETURN count(DISTINCT b.id) AS c" };
      for (final String query : fused) {
        assertThat(profileOf(query)).as(query).contains(FUSED_COUNT);
        assertThat(rows(query)).as(query).isEqualTo(rows(query.replace("MATCH ", "MATCH p = ")));
      }
      for (final String query : notFused) {
        assertThat(profileOf(query)).as(query).doesNotContain(FUSED_COUNT);
        assertThat(rows(query)).as(query).isEqualTo(rows(query.replace("MATCH ", "MATCH p = ")));
      }
      // the fused count is not a constant: it is what the rows count
      assertThat(rows(fused[3])).doesNotContain("c=0");
    }

    /**
     * A cached plan that walks a view is planned again once the view stops serving: a view that does not follow commits goes stale
     * on the next one, and the cached plan must not keep reading the graph as it stood before it.
     */
    @Test
    void aCachedPlanStopsReadingAViewThatWentStale() {
      buildGraph(2);
      final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName(VIEW).withVertexTypes("V").withEdgeTypes("E", "F")
          .withUpdateMode(GraphAnalyticalView.UpdateMode.OFF).withUseWhenStale(false).build();
      assertThat(view.isReady()).isTrue();

      final String query = "MATCH (a:V)-[:E*1..2]->(b:V) RETURN a.id AS s, count(DISTINCT b) AS c ORDER BY s";
      final String count = "MATCH (a:V {id: 0})-[:E*1..1]->(b:V) RETURN count(DISTINCT b) AS c";
      assertThat(planOf(query)).contains(OPERATOR);
      // executed twice, so the second run is served by the cached plan
      rows(query);
      rows(count);
      final List<String> before = rows(count);

      database.transaction(() -> {
        final MutableVertex late = database.newVertex("V").set("id", 200).save();
        database.select().fromType("V").where().property("id").eq().value(0).vertices().next().newEdge("E", late).save();
      });
      assertThat(view.isStale()).isTrue();
      assertThat(view.isReady()).isFalse();

      assertThat(rows(query)).isEqualTo(rows(query.replace("MATCH ", "MATCH p = ")));
      assertThat(rows(count)).isNotEqualTo(before).isEqualTo(rows(count.replace("MATCH ", "MATCH p = ")));
    }

    /** Writes committed after the view was built reach the walk: a new vertex, new edges, a deleted edge. */
    @Test
    void writesAfterTheBuildAreSeen() {
      buildGraph(5);
      createView();
      final String query = "MATCH (a:V)-[:E*1..4]-(b:V) RETURN a.id AS s, count(DISTINCT b) AS c ORDER BY s";
      final String reference = "MATCH p = (a:V)-[:E*1..4]-(b:V) RETURN a.id AS s, count(DISTINCT b) AS c ORDER BY s";
      assertThat(rows(query)).isEqualTo(rows(reference));

      database.transaction(() -> {
        final MutableVertex late = database.newVertex("V").set("id", 100).save();
        late.newEdge("E", database.select().fromType("V").where().property("id").eq().value(0).vertices().next()).save();
        database.command("sql", "DELETE FROM E LIMIT 2");
      });
      assertThat(rows(query)).isEqualTo(rows(reference));
      assertThat(rows("MATCH (a:V {id: 100})-[:E*1..1]-(b:V) RETURN count(DISTINCT b) AS c")).containsExactly("c=1");
    }

    private void buildGraph(final int seed) {
      final Random random = new Random(seed);
      database.transaction(() -> {
        database.command("sql", "DELETE FROM V");
        final List<MutableVertex> vertices = new ArrayList<>();
        for (int i = 0; i < VERTICES; i++)
          vertices.add(database.newVertex("V").set("id", i).save());
        final int edges = VERTICES + random.nextInt(VERTICES * 2);
        for (int e = 0; e < edges; e++) {
          final MutableVertex from = vertices.get(random.nextInt(VERTICES));
          // a fifth of the edges are self loops, the rest may repeat or run both ways between a pair
          final MutableVertex to = random.nextInt(5) == 0 ? from : vertices.get(random.nextInt(VERTICES));
          from.newEdge(random.nextInt(3) == 0 ? "F" : "E", to).save();
        }
      });
    }

    private void createView() {
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW " + VIEW + " VERTEX TYPES (V) EDGE TYPES (E, F) UPDATE MODE SYNCHRONOUS");
      awaitReady();
    }

    private void awaitReady() {
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, VIEW);
      final long deadline = System.currentTimeMillis() + 60_000;
      while (!view.isReady() && System.currentTimeMillis() < deadline)
        Thread.onSpinWait();
      assertThat(view.isReady()).isTrue();
    }

    private void dropView() {
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW " + VIEW);
    }

    private String planOf(final String query) {
      try (final ResultSet resultSet = database.query("opencypher", "EXPLAIN " + query)) {
        return String.valueOf(resultSet.next().<Object>getProperty("executionPlan"));
      }
    }

    private String profileOf(final String query) {
      try (final ResultSet resultSet = database.query("opencypher", "PROFILE " + query)) {
        while (resultSet.hasNext())
          resultSet.next();
        return resultSet.getExecutionPlan().get().prettyPrint(0, 2);
      }
    }

    /** The rows as text, with the elements of every list value sorted: a collected list has no order the statement promised. */
    private List<String> rows(final String query) {
      final List<String> rows = new ArrayList<>();
      try (final ResultSet resultSet = database.query("opencypher", query)) {
        while (resultSet.hasNext()) {
          final Result row = resultSet.next();
          final List<String> cells = new ArrayList<>();
          for (final String name : row.getPropertyNames()) {
            final Object value = row.getProperty(name);
            if (value instanceof List<?> list) {
              final List<String> elements = new ArrayList<>(list.size());
              for (final Object element : list)
                elements.add(String.valueOf(element));
              Collections.sort(elements);
              cells.add(name + "=" + elements);
            } else
              cells.add(name + "=" + value);
          }
          rows.add(String.join(",", cells));
        }
      }
      return rows;
    }
  }
}
