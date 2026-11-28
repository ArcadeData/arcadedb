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
import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11874: {@code MATCH ()-[e:K]->() WHERE e.w = 3 RETURN count(e)} scanned every vertex and walked every edge list, where
 * SQL scans the buckets of the edge type. Under a predicate that requires a property of the relationship no light edge (which has
 * no record in the buckets and no property) can be an answer, so the plan now scans the buckets of the relationship types.
 * <p>
 * Every answer of the scan is checked against the walk, which the same question takes when the predicate moves to a {@code WITH}
 * after the {@code MATCH}: same rows, endpoints, relationship, {@code type()}, {@code id()} and properties included. The fixture
 * holds light edges of the same type, edges without the property, a sub-type, another type, self-loops and two labels.
 */
class CypherEdgeTypeScanIssue11874Test {
  private static final String TYPE_SCAN = "MATCH EDGE BY TYPE SCAN";

  @Nested
  class PlanAndAnswers extends TestHelper {
    private int lightEdges;
    private int edgesWithoutW;

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE P");
      database.command("sql", "CREATE VERTEX TYPE Q");
      database.command("sql", "CREATE VERTEX TYPE Rare");
      database.command("sql", "CREATE EDGE TYPE K");
      database.command("sql", "CREATE EDGE TYPE KSub EXTENDS K");
      database.command("sql", "CREATE EDGE TYPE L");

      database.transaction(() -> {
        final List<MutableVertex> p = new ArrayList<>();
        for (int i = 0; i < 60; i++)
          p.add(database.newVertex("P").set("id", i, "grp", i % 3).save());
        final List<MutableVertex> q = new ArrayList<>();
        for (int i = 0; i < 20; i++)
          q.add(database.newVertex("Q").set("id", 1000 + i, "grp", i % 3).save());
        final MutableVertex rare0 = database.newVertex("Rare").set("id", 2000).save();
        final MutableVertex rare1 = database.newVertex("Rare").set("id", 2001).save();

        for (int i = 0; i < 300; i++) {
          final MutableVertex from = p.get(i % 60);
          final MutableVertex to = i % 5 == 0 ? q.get(i % 20) : p.get((i * 7 + 1) % 60);
          final String type = i % 23 == 0 ? "L" : i % 17 == 0 ? "KSub" : "K";
          final var edge = from.newEdge(type, to).set("name", "n" + (i % 13));
          if (i % 11 == 0) {
            // [e:K] matches K and its sub-type KSub, not L
            if (!type.equals("L"))
              ++edgesWithoutW;
          } else
            edge.set("w", i % 10);
          edge.save();
        }
        // self-loops, one of them light
        p.get(3).newEdge("K", p.get(3)).set("w", 3, "name", "loop").save();
        p.get(4).newEdge("K", p.get(4)).set("w", 4, "name", "loop").save();
        p.get(5).newLightEdge("K", p.get(5));
        rare0.newEdge("K", p.get(0)).set("w", 3, "name", "rare").save();
        rare1.newLightEdge("K", p.get(1));
        lightEdges = 2;
        for (int i = 0; i < 25; i++, lightEdges++)
          p.get(i).newLightEdge("K", i % 2 == 0 ? q.get(i % 20) : p.get(i + 1));
      });
    }

    @Test
    void theCountOfTheIssueScansTheEdgeType() {
      final String query = "MATCH ()-[e:K]->() WHERE e.w = 3 RETURN count(e) AS c";
      final String plan = explain(query, Map.of());
      assertThat(plan).contains(TYPE_SCAN + " (  src0)-[e:K]->(  tgt1) [filter: e.w = 3]").doesNotContain("MATCH RELATIONSHIP");
      assertSameAsWalk(query, "MATCH ()-[e:K]->() WITH e WHERE e.w = 3 RETURN count(e) AS c", Map.of());
    }

    @Test
    void endpointsAndRelationshipBindLikeTheWalk() {
      assertSameAsWalk("MATCH (a)-[e:K]->(b) WHERE e.w = 3 RETURN a, e, b",
          "MATCH (a)-[e:K]->(b) WITH a, e, b WHERE e.w = 3 RETURN a, e, b", Map.of());
      assertSameAsWalk(
          "MATCH (a)-[e:K]->(b) WHERE e.w > 6 RETURN type(e) AS t, id(e) AS i, elementId(e) AS ei, startNode(e) AS s, "
              + "endNode(e) AS en, a.id AS aid, b.id AS bid, e.w AS w, e.name AS n",
          "MATCH (a)-[e:K]->(b) WITH a, e, b WHERE e.w > 6 RETURN type(e) AS t, id(e) AS i, elementId(e) AS ei, startNode(e) AS s, "
              + "endNode(e) AS en, a.id AS aid, b.id AS bid, e.w AS w, e.name AS n", Map.of());
    }

    @Test
    void incomingAndUndirectedPatterns() {
      assertSameAsWalk("MATCH (a)<-[e:K]-(b) WHERE e.w IN [1, 2] RETURN a.id AS a, b.id AS b, e.w AS w",
          "MATCH (a)<-[e:K]-(b) WITH a, e, b WHERE e.w IN [1, 2] RETURN a.id AS a, b.id AS b, e.w AS w", Map.of());
      // every edge twice, once per orientation, and a self-loop once
      assertSameAsWalk("MATCH (a)-[e:K]-(b) WHERE e.w = 3 RETURN a.id AS a, b.id AS b, id(e) AS e",
          "MATCH (a)-[e:K]-(b) WITH a, e, b WHERE e.w = 3 RETURN a.id AS a, b.id AS b, id(e) AS e", Map.of());
      assertSameAsWalk("MATCH (a)-[e:K]-(b) WHERE e.w >= 0 RETURN count(*) AS c",
          "MATCH (a)-[e:K]-(b) WITH a, e, b WHERE e.w >= 0 RETURN count(*) AS c", Map.of());
    }

    @Test
    void aRepeatedEndpointVariableKeepsOnlySelfLoops() {
      assertSameAsWalk("MATCH (a)-[e:K]->(a) WHERE e.w IS NOT NULL RETURN a.id AS a, e.w AS w",
          "MATCH (a)-[e:K]->(a) WITH a, e WHERE e.w IS NOT NULL RETURN a.id AS a, e.w AS w", Map.of());
      assertSameAsWalk("MATCH (a)-[e:K]-(a) WHERE e.w IS NOT NULL RETURN a.id AS a, e.w AS w",
          "MATCH (a)-[e:K]-(a) WITH a, e WHERE e.w IS NOT NULL RETURN a.id AS a, e.w AS w", Map.of());
      assertThat(rows("MATCH (a)-[e:K]->(a) WHERE e.w IS NOT NULL RETURN a.id AS a", Map.of())).hasSize(2);
    }

    @Test
    void subTypesAndUnionsOfTypes() {
      assertSameAsWalk("MATCH ()-[e:K]->() WHERE e.w <> 5 RETURN type(e) AS t, count(*) AS c",
          "MATCH ()-[e:K]->() WITH e WHERE e.w <> 5 RETURN type(e) AS t, count(*) AS c", Map.of());
      assertSameAsWalk("MATCH ()-[e:K|KSub]->() WHERE e.w <> 5 RETURN type(e) AS t, count(*) AS c",
          "MATCH ()-[e:K|KSub]->() WITH e WHERE e.w <> 5 RETURN type(e) AS t, count(*) AS c", Map.of());
      assertSameAsWalk("MATCH (a)-[e:KSub|L]->(b) WHERE e.name STARTS WITH 'n1' RETURN a.id AS a, b.id AS b, type(e) AS t",
          "MATCH (a)-[e:KSub|L]->(b) WITH a, e, b WHERE e.name STARTS WITH 'n1' RETURN a.id AS a, b.id AS b, type(e) AS t",
          Map.of());
      assertThat(explain("MATCH ()-[e:K|KSub]->() WHERE e.w <> 5 RETURN count(*)", Map.of())).contains("[e:K]");
    }

    @Test
    void aSubTypeOfTwoOfTheTypesIsAnsweredOnce() {
      // MC is under both MA and MB: a polymorphic scan of each would read its buckets twice, and bind each of its edges twice
      database.command("sql", "CREATE EDGE TYPE MA");
      database.command("sql", "CREATE EDGE TYPE MB");
      database.command("sql", "CREATE EDGE TYPE MC EXTENDS MA, MB");
      database.transaction(() -> {
        final Vertex a = database.query("sql", "SELECT FROM P WHERE id = 1").next().getVertex().get();
        final Vertex b = database.query("sql", "SELECT FROM P WHERE id = 2").next().getVertex().get();
        for (int i = 0; i < 3; i++)
          a.newEdge("MC", b).set("w", 1).save();
        a.newEdge("MA", b).set("w", 1).save();
        b.newEdge("MB", a).set("w", 1).save();
      });

      for (final String types : new String[] { "MA|MB", "MB|MA", "MA|MB|MC" }) {
        final String query = "MATCH (a)-[e:" + types + "]->(b) WHERE e.w = 1 RETURN id(e) AS i, type(e) AS t";
        assertThat(rows(query, Map.of())).as(types).hasSize(5)
            .isEqualTo(rows(query.replace("WHERE e.w = 1", "WITH a, e, b WHERE e.w = 1"), Map.of()));
        assertThat(count("MATCH ()-[e:" + types + "]->() RETURN count(e.w) AS c")).as(types).isEqualTo(5L);
      }
      // one of the two alone reads the sub-type once, and keeps the scan
      assertSameAsWalk("MATCH ()-[e:MA]->() WHERE e.w = 1 RETURN count(e) AS c",
          "MATCH ()-[e:MA]->() WITH e WHERE e.w = 1 RETURN count(e) AS c", Map.of());
      assertThat(count("MATCH ()-[e:MA]->() WHERE e.w = 1 RETURN count(e) AS c")).isEqualTo(4L);
    }

    @Test
    void aTargetAUniqueIndexPinsKeepsTheWalkBackFromIt() {
      // one index lookup and the relationships of one vertex, where the scan reads every edge of the type (issue #12149)
      database.command("sql", "CREATE PROPERTY Q.id INTEGER");
      database.command("sql", "CREATE INDEX ON Q (id) UNIQUE");

      for (final String query : new String[] {
          "MATCH (a)-[e:K]->(b:Q {id: 1000}) WHERE e.w >= 0 OPTIONAL MATCH (b)-[:L]->(c) RETURN a.id AS a, e.w AS w, c.id AS c",
          "MATCH (a)-[e:K]->(b:Q) WHERE e.w >= 0 AND b.id = 1000 OPTIONAL MATCH (b)-[:L]->(c) RETURN a.id AS a, e.w AS w, c.id AS c" }) {
        assertThat(explain(query, Map.of())).as(query).doesNotContain(TYPE_SCAN);
        assertThat(rows(query, Map.of())).as(query).isNotEmpty()
            .isEqualTo(rows(query.replace("WHERE e.w >= 0", "WITH a, e, b WHERE e.w >= 0"), Map.of()));
      }
      // a target no unique index pins is no cheaper a start than the scan
      assertThat(explain(
          "MATCH (a)-[e:K]->(b:Q {grp: 1}) WHERE e.w >= 0 OPTIONAL MATCH (b)-[:L]->(c) RETURN a.id AS a, e.w AS w, c.id AS c",
          Map.of())).contains(TYPE_SCAN);
    }

    @Test
    void everyPredicateThatRequiresAProperty() {
      for (final String predicate : new String[] { "e.w = $w", "e.w < 4", "$w <= e.w", "e.w IN $ws", "e.name CONTAINS '1'",
          "e.name ENDS WITH '2'", "e.name =~ 'n1.*'", "e.w = 1 OR e.name = 'n2'", "e.w = 3 AND e.name <> 'loop'" }) {
        final Map<String, Object> params = Map.of("w", 4, "ws", List.of(2, 7));
        assertSameAsWalk("MATCH (a)-[e:K]->(b) WHERE " + predicate + " RETURN a.id AS a, b.id AS b, e.w AS w, e.name AS n",
            "MATCH (a)-[e:K]->(b) WITH a, e, b WHERE " + predicate + " RETURN a.id AS a, b.id AS b, e.w AS w, e.name AS n", params);
      }
    }

    @Test
    void inlinePropertyMaps() {
      assertSameAsWalk("MATCH (a)-[e:K {w: 3}]->(b) RETURN a.id AS a, b.id AS b, e.name AS n",
          "MATCH (a)-[e:K]->(b) WITH a, e, b WHERE e.w = 3 RETURN a.id AS a, b.id AS b, e.name AS n", Map.of());
      assertSameAsWalk("MATCH ()-[:K {w: $w}]->() RETURN count(*) AS c",
          "MATCH ()-[e:K]->() WITH e WHERE e.w = $w RETURN count(*) AS c", Map.of("w", 3));
    }

    @Test
    void endpointLabelsAndPropertiesAreCheckedOnTheEdgeVertices() {
      assertSameAsWalk("MATCH (a)-[e:K]->(b:Q) WHERE e.w < 5 RETURN a.id AS a, b.id AS b, e.w AS w",
          "MATCH (a)-[e:K]->(b:Q) WITH a, e, b WHERE e.w < 5 RETURN a.id AS a, b.id AS b, e.w AS w", Map.of());
      assertSameAsWalk("MATCH (a)-[e:K]->(b:Q {grp: 1}) WHERE e.w = 0 RETURN a.id AS a, b.id AS b",
          "MATCH (a)-[e:K]->(b:Q {grp: 1}) WITH a, e, b WHERE e.w = 0 RETURN a.id AS a, b.id AS b", Map.of());
      assertSameAsWalk("MATCH (a)<-[e:K]-(b {grp: 1}) WHERE e.w = 2 RETURN a.id AS a, b.id AS b",
          "MATCH (a)<-[e:K]-(b {grp: 1}) WITH a, e, b WHERE e.w = 2 RETURN a.id AS a, b.id AS b", Map.of());
      // a predicate on the target stays with the clause's filter, above the scan
      assertSameAsWalk("MATCH (a)-[e:K]->(b) WHERE e.w = 3 AND b.grp = 1 AND b.id > 10 RETURN a.id AS a, b.id AS b",
          "MATCH (a)-[e:K]->(b) WITH a, e, b WHERE e.w = 3 AND b.grp = 1 AND b.id > 10 RETURN a.id AS a, b.id AS b", Map.of());
    }

    @Test
    void aNarrowedSourceKeepsTheWalk() {
      // the walk reads only the edges of the source vertices that pass, fewer than every edge of the type
      for (final String query : new String[] { "MATCH (a)-[e:K]->(b) WHERE e.w = 3 AND a.grp = 1 RETURN a.id AS a, b.id AS b",
          "MATCH (a {grp: 1})-[e:K]->(b) WHERE e.w = 3 RETURN a.id AS a, b.id AS b" }) {
        assertThat(explain(query, Map.of())).as(query).doesNotContain(TYPE_SCAN);
        assertThat(rows(query, Map.of())).as(query).isNotEmpty()
            .isEqualTo(rows(query.replace("WHERE e.w = 3", "WITH a, e, b WHERE e.w = 3"), Map.of()));
      }
      // a narrowed source keeps the walk for the aggregates too: it reads fewer edges than the scan
      for (final String query : new String[] { "MATCH (a {grp: 1})-[e:K]->() RETURN sum(e.w) AS s",
          "MATCH (a:P {grp: 1})-[e:K]->() RETURN sum(e.w) AS s", "MATCH (a:Rare)-[e:K]->() RETURN sum(e.w) AS s" }) {
        assertThat(explain(query, Map.of())).as(query).doesNotContain(TYPE_SCAN);
        assertThat(rows(query, Map.of())).as(query)
            .isEqualTo(rows(query.replace("RETURN", "WITH e RETURN"), Map.of()));
      }
    }

    @Test
    void aSmallLabelledSourceKeepsTheWalk() {
      final String query = "MATCH (a:Rare)-[e:K]->(b) WHERE e.w = 3 RETURN a.id AS a, b.id AS b";
      assertThat(explain(query, Map.of())).doesNotContain(TYPE_SCAN);
      assertThat(rows(query, Map.of())).containsExactly("a=2000, b=0");
    }

    @Test
    void theCostBasedPlanStartsFromTheTypeScanWhenItCostsLess() {
      final String query = "MATCH (a:P)-[e:K]->(b:P) WHERE e.w = 3 RETURN a.id AS a, b.id AS b, e.name AS n";
      assertThat(explain(query, Map.of())).contains("EdgeTypeScan " + TYPE_SCAN + " (a:P)-[e:K]->(b:P) [filter: e.w = 3]");
      assertSameAsWalk(query, "MATCH (a:P)-[e:K]->(b:P) WITH a, e, b WHERE e.w = 3 RETURN a.id AS a, b.id AS b, e.name AS n",
          Map.of());
      assertSameAsWalk("MATCH (a:P)<-[e:K]-(b:P) WHERE e.w > 5 RETURN a.id AS a, b.id AS b, e.w AS w",
          "MATCH (a:P)<-[e:K]-(b:P) WITH a, e, b WHERE e.w > 5 RETURN a.id AS a, b.id AS b, e.w AS w", Map.of());
      // the chain goes on from the scanned hop's endpoints, and the next hop binds a different edge
      assertThat(rows("MATCH (a:P)-[e:K]->(b:P)-[f:K]->(c) WHERE e.w = 3 RETURN a.id AS a, c.id AS c, f.w AS w", Map.of()))
          .isNotEmpty().isEqualTo(
              rows("MATCH (a:P)-[e:K]->(b:P)-[f:K]->(c) WITH a, e, f, c WHERE e.w = 3 RETURN a.id AS a, c.id AS c, f.w AS w",
                  Map.of()));
      // an endpoint its own predicate narrows is the cheaper start
      assertThat(explain("MATCH (a:P)<-[e:K]-(b:P) WHERE e.w > 5 AND b.grp = 2 RETURN a.id", Map.of())).doesNotContain(TYPE_SCAN);
      // two vertices to expand cost less than every edge of K
      final String rare = "MATCH (a:Rare)-[e:K]->(b:P) WHERE e.w = 3 RETURN a.id AS a, b.id AS b";
      assertThat(explain(rare, Map.of())).doesNotContain(TYPE_SCAN);
      assertThat(rows(rare, Map.of())).containsExactly("a=2000, b=0");
    }

    @Test
    void aStatementThatWritesKeepsTheWalk() {
      final String write = "MATCH (a)-[e:K]->(b) WHERE e.w = 3 CREATE (a)-[:K {w: 3, name: 'copy'}]->(b)";
      assertThat(explain(write, Map.of())).doesNotContain(TYPE_SCAN);
      final long before = count("MATCH ()-[e:K]->() WHERE e.w = 3 RETURN count(e) AS c");
      database.transaction(() -> database.command("opencypher", write));
      assertThat(count("MATCH ()-[e:K]->() WHERE e.w = 3 RETURN count(e) AS c")).isEqualTo(2 * before);
    }

    @Test
    void anotherHopOfTheClauseStillBindsADifferentRelationship() {
      assertSameAsWalk("MATCH ()-[e:K]->(), ()-[f:K]->() WHERE e.w = 3 AND f.w = 3 RETURN count(*) AS c",
          "MATCH ()-[e:K]->(), ()-[f:K]->() WITH e, f WHERE e.w = 3 AND f.w = 3 RETURN count(*) AS c", Map.of());
    }

    @Test
    void aPredicateALightEdgeCanSatisfyKeepsTheWalkAndItsLightEdges() {
      // the walk finds the light edges: these predicates hold for them, so the scan of the buckets would miss answers
      final long withW3 = count("MATCH ()-[e:K]->() WHERE e.w = 3 RETURN count(e) AS c");
      for (final String predicate : new String[] { "e.w IS NULL", "e.w = 3 OR e.w IS NULL", "coalesce(e.w, 3) = 3",
          "NOT e.w IS NOT NULL" }) {
        final String query = "MATCH ()-[e:K]->() WHERE " + predicate + " RETURN count(e) AS c";
        assertThat(explain(query, Map.of())).as(predicate).doesNotContain(TYPE_SCAN);
        final long expected = predicate.contains("3") ? withW3 + lightEdges + edgesWithoutW : lightEdges + edgesWithoutW;
        assertThat(count(query)).as(predicate).isEqualTo(expected);
      }
      assertThat(lightEdges).isGreaterThan(0);
    }

    @Test
    void aggregatesOfARelationshipPropertyScanWithoutAPredicate() {
      // a light edge gives these aggregates a null, which each of them skips: the edges the scan leaves out add nothing
      assertSameAsWalk("MATCH ()-[e:K]->() RETURN count(e.w) AS c",
          "MATCH ()-[e:K]->() WITH e RETURN count(e.w) AS c", Map.of());
      assertThat(explain("MATCH ()-[e:K]->() RETURN count(e.w) AS c", Map.of()))
          .contains(TYPE_SCAN + " (  src0)-[e:K]->(  tgt1)").doesNotContain("MATCH RELATIONSHIP");
      final String aggregates = "count(e.w) AS c, sum(e.w) AS s, avg(e.w) AS a, min(e.w) AS mi, max(e.w) AS ma, "
          + "count(DISTINCT e.name) AS d, min(e.name) AS mn";
      assertSameAsWalk("MATCH (a)-[e:K]->(b) RETURN " + aggregates, "MATCH (a)-[e:K]->(b) WITH a, e, b RETURN " + aggregates,
          Map.of());
      assertSameAsWalk("MATCH (a)-[e:K|L]-(b) RETURN " + aggregates, "MATCH (a)-[e:K|L]-(b) WITH a, e, b RETURN " + aggregates,
          Map.of());
      assertSameAsWalk("MATCH (a)-[e:K]->(b:Q) RETURN " + aggregates,
          "MATCH (a)-[e:K]->(b:Q) WITH a, e, b RETURN " + aggregates, Map.of());
      // a predicate on the relationship a light edge satisfies, decided by the scan on the edges it reads
      assertSameAsWalk("MATCH (a)-[e:K]->(b) WHERE e.w IS NULL OR e.w > 5 RETURN " + aggregates,
          "MATCH (a)-[e:K]->(b) WITH a, e, b WHERE e.w IS NULL OR e.w > 5 RETURN " + aggregates, Map.of());
      // the cost-based plan
      assertThat(explain("MATCH (a:P)-[e:K]->(b:P) RETURN " + aggregates, Map.of())).contains("EdgeTypeScan " + TYPE_SCAN);
      assertSameAsWalk("MATCH (a:P)-[e:K]->(b:P) RETURN " + aggregates,
          "MATCH (a:P)-[e:K]->(b:P) WITH a, e, b RETURN " + aggregates, Map.of());

      // collect() and the standard deviations, whose order or rounding follows the order of the rows
      final String spread = "MATCH ()-[e:K]->() RETURN collect(e.w) AS l, stDev(e.w) AS sd, stDevP(e.w) AS sp";
      assertThat(explain(spread, Map.of())).contains(TYPE_SCAN);
      final Result scanned = single(spread);
      final Result walked = single(spread.replace("RETURN", "WITH e RETURN"));
      // in any order, each value as many times: a scan that dropped an edge and read another twice would not pass
      assertThat(scanned.<List<Object>>getProperty("l")).isNotEmpty()
          .containsExactlyInAnyOrderElementsOf(walked.<List<Object>>getProperty("l"));
      assertThat(scanned.<Number>getProperty("sd").doubleValue()).isCloseTo(walked.<Number>getProperty("sd").doubleValue(),
          Offset.offset(1e-9));
      assertThat(scanned.<Number>getProperty("sp").doubleValue()).isCloseTo(walked.<Number>getProperty("sp").doubleValue(),
          Offset.offset(1e-9));
    }

    @Test
    void aggregatesOfATypeWithOnlyLightEdges() {
      // the walk aggregates only nulls, the scan nothing at all: the same one row
      database.command("sql", "CREATE EDGE TYPE OnlyLight");
      database.transaction(() -> {
        final Vertex a = database.query("sql", "SELECT FROM P WHERE id = 1").next().getVertex().get();
        final Vertex b = database.query("sql", "SELECT FROM P WHERE id = 2").next().getVertex().get();
        a.newLightEdge("OnlyLight", b);
        b.newLightEdge("OnlyLight", a);
      });
      final String aggregates = "count(e.w) AS c, sum(e.w) AS s, avg(e.w) AS a, min(e.w) AS mi, max(e.w) AS ma, "
          + "collect(e.w) AS l, stDev(e.w) AS sd, stDevP(e.w) AS sp";
      assertSameAsWalk("MATCH ()-[e:OnlyLight]->() RETURN " + aggregates,
          "MATCH ()-[e:OnlyLight]->() WITH e RETURN " + aggregates, Map.of());
      assertThat(count("MATCH ()-[e:OnlyLight]->() RETURN count(*) AS c")).isEqualTo(2L);
    }

    @Test
    void rowsOfARelationshipWithoutAPredicateKeepTheWalkAndItsLightEdges() {
      // every row of a light edge is an answer here, and the scan of the buckets does not find it
      final long relationships = database.countType("K", true) + lightEdges;
      for (final String query : new String[] { "MATCH ()-[e:K]->() RETURN e", "MATCH ()-[e:K]->() RETURN e.w AS w",
          "MATCH ()-[e:K]->() WITH e RETURN count(e.w) AS c", "MATCH ()-[e:K]->() RETURN count(e) AS c",
          "MATCH ()-[e:K]->() RETURN count(*) AS c", "MATCH ()-[e:K]->() RETURN count(e.w) + 1 AS c",
          "MATCH ()-[e:K]->() RETURN count(e.w) AS c, count(e) AS r", "MATCH ()-[e:K]->() RETURN e.name AS n, count(e.w) AS c",
          "MATCH ()-[e:K]->() UNWIND [1, 2] AS x RETURN count(e.w) AS c" })
        assertThat(explain(query, Map.of())).as(query).doesNotContain(TYPE_SCAN);
      assertThat(rows("MATCH ()-[e:K]->() RETURN e", Map.of())).hasSize((int) relationships);
      assertThat(count("MATCH ()-[e:K]->() RETURN count(e) AS c")).isEqualTo(relationships);
      // a group of light edges alone is a row of the answer
      assertThat(rows("MATCH ()-[e:K]->() RETURN e.name AS n, count(e.w) AS c", Map.of())).contains("c=0, n=null");
    }

    @Test
    void aWriteInTheSameTransactionIsSeen() {
      database.begin();
      try {
        database.command("opencypher", "MATCH (a:P {id: 7}), (b:P {id: 8}) CREATE (a)-[:K {w: 99}]->(b)");
        assertThat(count("MATCH ()-[e:K]->() WHERE e.w = 99 RETURN count(e) AS c")).isEqualTo(1L);
        assertThat(explain("MATCH ()-[e:K]->() WHERE e.w = 99 RETURN count(e) AS c", Map.of())).contains(TYPE_SCAN);
      } finally {
        database.rollback();
      }
    }

    private long count(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        return ((Number) rs.next().getProperty("c")).longValue();
      }
    }

    private Result single(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        final Result row = rs.next();
        assertThat(rs.hasNext()).isFalse();
        return row;
      }
    }

    private void assertSameAsWalk(final String scanQuery, final String walkQuery, final Map<String, Object> params) {
      assertThat(explain(scanQuery, params)).as(scanQuery).contains(TYPE_SCAN);
      assertThat(explain(walkQuery, params)).as(walkQuery).doesNotContain(TYPE_SCAN);
      final List<String> scanned = rows(scanQuery, params);
      assertThat(scanned).as(scanQuery).isNotEmpty().isEqualTo(rows(walkQuery, params));
    }

    private String explain(final String query, final Map<String, Object> params) {
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query, params)) {
        return rs.next().getProperty("executionPlanAsString");
      }
    }

    private List<String> rows(final String query, final Map<String, Object> params) {
      return CypherEdgeTypeScanIssue11874Test.rows(database, query, params);
    }
  }

  /**
   * The scan of one edge type runs in the workers of a parallel scan, both for the rows of a pushed predicate and for an
   * aggregation that consumes them there, and answers what the sequential scan and the walk answer.
   */
  @Nested
  class Parallel extends TestHelper {
    private static final int EDGES = 20_000;

    @Override
    protected void beginTest() {
      // Small units, so the fixture's few hundred pages are cut in many of them
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 2);
      database.command("sql", "CREATE VERTEX TYPE P");
      database.command("sql", "CREATE EDGE TYPE K");
      database.transaction(() -> {
        final List<MutableVertex> p = new ArrayList<>();
        for (int i = 0; i < 1000; i++)
          p.add(database.newVertex("P").set("id", i).save());
        for (int i = 0; i < EDGES; i++)
          p.get(i % 1000).newEdge("K", p.get((i * 31 + 7) % 1000)).set("id", i, "w", i % 10).save();
        for (int i = 0; i < 100; i++)
          p.get(i).newLightEdge("K", p.get(i + 1));
      });
    }

    @Test
    void aggregationAndRowsRunInTheWorkers() {
      final String count = "MATCH ()-[e:K]->() WHERE e.w = 3 RETURN count(e) AS c, sum(e.id) AS s";
      assertThat(profile(count)).contains(TYPE_SCAN).contains("[parallel]").contains("parallel: ");
      assertThat(rows(count)).isEqualTo(sequential(count))
          .isEqualTo(rows("MATCH ()-[e:K]->() WITH e WHERE e.w = 3 RETURN count(e) AS c, sum(e.id) AS s"))
          .containsExactly("c=" + EDGES / 10 + ", s=" + sumOfIdsWithW3());

      final String ids = "MATCH (a)-[e:K]->(b) WHERE e.w = 3 RETURN e.id AS id, a.id AS a, b.id AS b";
      assertThat(profile(ids)).contains(TYPE_SCAN).contains("[parallel]");
      assertThat(rows(ids)).hasSize(EDGES / 10).isEqualTo(sequential(ids))
          .isEqualTo(rows("MATCH (a)-[e:K]->(b) WITH a, e, b WHERE e.w = 3 RETURN e.id AS id, a.id AS a, b.id AS b"));

      final String inline = "MATCH (a)-[e:K {w: 3}]->(b) RETURN e.id AS id, b.id AS b";
      assertThat(profile(inline)).contains(TYPE_SCAN).contains("[parallel]");
      assertThat(rows(inline)).hasSize(EDGES / 10).isEqualTo(sequential(inline));

      // no predicate: the aggregates skip the null of the 100 light edges
      final String noPredicate = "MATCH ()-[e:K]->() RETURN count(e.w) AS c, sum(e.id) AS s";
      assertThat(profile(noPredicate)).contains(TYPE_SCAN).contains("parallel: ");
      assertThat(rows(noPredicate)).isEqualTo(sequential(noPredicate))
          .isEqualTo(rows("MATCH ()-[e:K]->() WITH e RETURN count(e.w) AS c, sum(e.id) AS s"))
          .containsExactly("c=" + EDGES + ", s=" + (long) EDGES * (EDGES - 1) / 2);
    }

    @Test
    void aLimitTakesItsRowsAndStopsTheScan() {
      final String limited = "MATCH (a)-[e:K]->(b) WHERE e.w = 3 RETURN e.id AS id LIMIT 1";
      assertThat(profile(limited)).contains(TYPE_SCAN).contains("[parallel]");
      for (int i = 0; i < 20; i++)
        assertThat(rows(limited)).hasSize(1);
      // the rows after a closed scan are those of a scan of its own
      assertThat(rows("MATCH (a)-[e:K]->(b) WHERE e.w = 3 RETURN e.id AS id")).hasSize(EDGES / 10);
    }

    private long sumOfIdsWithW3() {
      long sum = 0;
      for (int i = 3; i < EDGES; i += 10)
        sum += i;
      return sum;
    }

    private String profile(final String query) {
      try (final ResultSet rs = database.query("opencypher", "PROFILE " + query)) {
        while (rs.hasNext())
          rs.next();
        return rs.getExecutionPlan().get().prettyPrint(0, 2);
      }
    }

    private List<String> sequential(final String query) {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, false);
      try {
        return rows(query);
      } finally {
        database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, true);
      }
    }

    private List<String> rows(final String query) {
      return CypherEdgeTypeScanIssue11874Test.rows(database, query, Map.of());
    }
  }

  /** The rows of a query as text, sorted: a record as its RID and its properties, so two plans compare as row multisets. */
  private static List<String> rows(final Database database, final String query,
      final Map<String, Object> params) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final StringBuilder text = new StringBuilder();
        for (final String name : new TreeSet<>(row.getPropertyNames())) {
          if (!text.isEmpty())
            text.append(", ");
          final Object value = row.getProperty(name);
          text.append(name).append('=')
              .append(value instanceof Document document ? document.getIdentity() + document.toMap(false).toString() : value);
        }
        rows.add(text.toString());
      }
    }
    rows.sort(null);
    return rows;
  }
}
