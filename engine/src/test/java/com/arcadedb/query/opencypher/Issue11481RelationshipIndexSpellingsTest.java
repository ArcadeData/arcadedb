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
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11481: an openCypher lookup of one relationship by an indexed property used the relationship index for one spelling only -
 * a single type, anonymous endpoints and the predicate in a {@code WHERE} clause. An inline property map, a label on either endpoint
 * and a union of types were planned as a scan of every vertex (of the label) and an expansion of every edge. Every spelling now
 * seeks the index, on both planners: the step-by-step one, and the cost-based one that takes labelled endpoints with a {@code WHERE}
 * clause. Each variant is checked for its plan AND for the rows the scan returns.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11481RelationshipIndexSpellingsTest extends TestHelper {
  private static final int VERTICES = 300;
  private static final int EDGES    = 3_000;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE INDEX entity_uuid IF NOT EXISTS FOR (n:Entity) ON (n.uuid)");
      database.command("opencypher", "CREATE INDEX relates_uuid IF NOT EXISTS FOR ()-[e:RELATES_TO]-() ON (e.uuid)");
      database.command("opencypher", "CREATE INDEX mentions_uuid IF NOT EXISTS FOR ()-[e:MENTIONS]-() ON (e.uuid)");
    });
    database.transaction(() -> database.command("opencypher",
        "UNWIND range(1, $n) AS i CREATE (:Entity {uuid: 'v' + toString(i)})", Map.of("n", VERTICES)));
    database.transaction(() -> database.command("opencypher",
        "UNWIND range(1, $e) AS i MATCH (s:Entity {uuid: 'v' + toString(1 + i % $v)}), (t:Entity {uuid: 'v' + toString(1 + (i * 7 + 1) % $v)}) "
            + "CREATE (s)-[:RELATES_TO {uuid: 'e' + toString(i), fact: 'f' + toString(i % 3)}]->(t)",
        Map.of("e", EDGES, "v", VERTICES)));
    database.transaction(() -> database.command("opencypher",
        "MATCH (s:Entity {uuid: 'v1'}), (t:Entity {uuid: 'v2'}) CREATE (s)-[:MENTIONS {uuid: 'm1'}]->(t)"));
  }

  @Test
  void variantAAnonymousEndsSingleTypeWhere() {
    assertSeeks("MATCH (n)-[e:RELATES_TO]->(m) WHERE e.uuid = $u RETURN e.uuid AS uuid", Map.of("u", "e1500"), "e1500");
  }

  @Test
  void variantBWhereInList() {
    assertSeeks("MATCH (n)-[e:RELATES_TO]->(m) WHERE e.uuid IN $us RETURN e.uuid AS uuid ORDER BY uuid",
        Map.of("us", List.of("e1500", "e1000")), "e1000", "e1500");
  }

  @Test
  void variantCInlineMapOnTheRelationship() {
    assertSeeks("MATCH (n)-[e:RELATES_TO {uuid: $u}]->(m) RETURN e.uuid AS uuid", Map.of("u", "e1500"), "e1500");
  }

  @Test
  void variantDLabelledEndpointsWhereOnTheCostBasedPlan() {
    assertSeeks("MATCH (n:Entity)-[e:RELATES_TO]->(m:Entity) WHERE e.uuid = $u RETURN e.uuid AS uuid", Map.of("u", "e1500"),
        "e1500");

    // The cost-based planner takes this shape, and anchors it on the relationship rather than on a scan of Entity
    final List<String> rows = new ArrayList<>();
    final String plan = run("MATCH (n:Entity)-[e:RELATES_TO]->(m:Entity) WHERE e.uuid = $u RETURN e.uuid AS uuid",
        Map.of("u", "e1500"), rows);
    assertThat(plan).contains("Cost-Based").contains("EdgeIndexSeek").doesNotContain("NodeByLabelScan");

    // A longer pattern goes on from the endpoints the seek bound
    rows.clear();
    final String longer = run("MATCH (n:Entity)-[e:RELATES_TO]->(m:Entity)-[f:RELATES_TO]->(o:Entity) WHERE e.uuid = $u "
        + "RETURN f.uuid AS uuid ORDER BY uuid", Map.of("u", "e10"), rows);
    assertThat(longer).contains("EdgeIndexSeek");
    // e10 reaches v72, whose outgoing edges are the i with 1 + i % 300 = 72
    final List<String> expected = new ArrayList<>();
    for (int i = 71; i <= EDGES; i += VERTICES)
      expected.add("e" + i);
    expected.sort(null);
    assertThat(rows).containsExactlyElementsOf(expected);
  }

  @Test
  void variantELabelledEndpointsAndInlineMap() {
    assertSeeks("MATCH (n:Entity)-[e:RELATES_TO {uuid: $u}]->(m:Entity) RETURN e.uuid AS uuid", Map.of("u", "e1500"), "e1500");
  }

  @Test
  void variantFOneLabelledEndpointWhere() {
    assertSeeks("MATCH (n:Entity)-[e:RELATES_TO]->(m) WHERE e.uuid = $u RETURN e.uuid AS uuid", Map.of("u", "e1500"), "e1500");
  }

  @Test
  void variantGTypeUnionWhere() {
    assertSeeks("MATCH (n)-[e:MENTIONS|RELATES_TO]->(m) WHERE e.uuid = $u RETURN e.uuid AS uuid", Map.of("u", "e1500"), "e1500");
    assertSeeks("MATCH (n)-[e:MENTIONS|RELATES_TO]->(m) WHERE e.uuid = $u RETURN e.uuid AS uuid", Map.of("u", "m1"), "m1");
  }

  @Test
  void variantHTypeUnionAndInlineMapDeletes() {
    assertSeeks("MATCH (n)-[e:MENTIONS|RELATES_TO {uuid: $u}]->(m) RETURN e.uuid AS uuid", Map.of("u", "m1"), "m1");

    // The delete shape of the report: the edge goes, nothing else does
    database.transaction(() -> database.command("opencypher", "MATCH (n)-[e:MENTIONS|RELATES_TO {uuid: $u}]->(m) DELETE e",
        Map.of("u", "e42")));
    assertThat(count("MATCH ()-[e:RELATES_TO]->() RETURN count(e) AS c")).isEqualTo(EDGES - 1);
    assertThat(count("MATCH ()-[e:RELATES_TO {uuid: 'e42'}]->() RETURN count(e) AS c")).isZero();
  }

  @Test
  void theNestedParameterShapeOfIssue11480SeeksTheRelationshipIndexToo() {
    assertSeeks("MATCH ()-[e:RELATES_TO {uuid: $d.u}]->() RETURN e.uuid AS uuid", Map.of("d", Map.of("u", "e7")), "e7");
  }

  @Test
  void theInlineMapStillFiltersOnTheEntriesTheIndexDoesNotHold() {
    // e1500 has fact f0 (1500 % 3): a fact that does not match leaves no row, as the scan does
    assertSeeks("MATCH (n)-[e:RELATES_TO {uuid: $u, fact: 'f0'}]->(m) RETURN e.uuid AS uuid", Map.of("u", "e1500"), "e1500");
    assertSeeks("MATCH (n)-[e:RELATES_TO {uuid: $u, fact: 'f1'}]->(m) RETURN e.uuid AS uuid", Map.of("u", "e1500"));
  }

  @Test
  void anEndpointLabelThatDoesNotMatchLeavesNoRow() {
    database.transaction(() -> database.command("opencypher",
        "MATCH (s:Entity {uuid: 'v1'}) CREATE (s)-[:RELATES_TO {uuid: 'x1'}]->(:Other {uuid: 'o1'})"));
    assertSeeks("MATCH (n:Entity)-[e:RELATES_TO {uuid: 'x1'}]->(m:Entity) RETURN e.uuid AS uuid", Map.of());
    assertSeeks("MATCH (n:Entity)-[e:RELATES_TO {uuid: 'x1'}]->(m:Other) RETURN m.uuid AS uuid", Map.of(), "o1");
    assertSeeks("MATCH (n:Entity)-[e:RELATES_TO]->(m:Entity|Other) WHERE e.uuid = 'x1' RETURN m.uuid AS uuid", Map.of(), "o1");
  }

  @Test
  void theIncomingDirectionBindsTheEndpointsTheWrittenWay() {
    final List<String> rows = new ArrayList<>();
    final String plan = run("MATCH (m:Entity)<-[e:RELATES_TO {uuid: 'e10'}]-(n:Entity) RETURN n.uuid + '->' + m.uuid AS uuid",
        Map.of(), rows);
    assertThat(plan).contains("MATCH EDGE BY INDEX");
    // e10 goes from v(1 + 10 % 300) = v11 to v(1 + 71 % 300) = v72
    assertThat(rows).containsExactly("v11->v72");
  }

  @Test
  void aTypeOfTheUnionWithNoIndexKeepsTheScan() {
    database.transaction(() -> database.command("opencypher",
        "MATCH (s:Entity {uuid: 'v1'}), (t:Entity {uuid: 'v3'}) CREATE (s)-[:NOINDEX {uuid: 'e1500'}]->(t)"));
    final List<String> rows = new ArrayList<>();
    final String plan = run("MATCH (n)-[e:NOINDEX|RELATES_TO]->(m) WHERE e.uuid = $u RETURN type(e) AS uuid ORDER BY uuid",
        Map.of("u", "e1500"), rows);
    assertThat(plan).doesNotContain("MATCH EDGE BY INDEX");
    assertThat(rows).containsExactly("NOINDEX", "RELATES_TO");
  }

  @Test
  void aSubtypeNamedWithItsParentIsReturnedOnce() {
    database.transaction(() -> {
      database.getSchema().createEdgeType("RELATES_TO_CHILD").addSuperType("RELATES_TO");
      database.command("opencypher",
          "MATCH (s:Entity {uuid: 'v1'}), (t:Entity {uuid: 'v3'}) CREATE (s)-[:RELATES_TO_CHILD {uuid: 'c1'}]->(t)");
    });
    assertSeeks("MATCH (n)-[e:RELATES_TO|RELATES_TO_CHILD {uuid: 'c1'}]->(m) RETURN type(e) AS uuid", Map.of(), "RELATES_TO_CHILD");
  }

  @Test
  void aSelectiveEndpointOfANonUniqueSeekStillAnchorsTheNodeIndex() {
    // The endpoint pinned by its index is one vertex: expanding its edges beats a seek of a third of the edges
    database.command("opencypher", "CREATE INDEX relates_fact IF NOT EXISTS FOR ()-[e:RELATES_TO]-() ON (e.fact)");
    final List<String> rows = new ArrayList<>();
    final String plan = run("MATCH (n:Entity)-[e:RELATES_TO]->(m) WHERE n.uuid = 'v11' AND e.fact = 'f1' RETURN e.uuid AS uuid ORDER BY uuid",
        Map.of(), rows);
    assertThat(plan).doesNotContain("MATCH EDGE BY INDEX");
    assertThat(rows).isNotEmpty();
  }

  @Test
  void aViewBackedChainAfterTheSeekKeepsTheRelationshipTheSeekBound() throws InterruptedException {
    // The seek binds e below two hops a graph analytical view walks: fused into one operator, those hops would emit only their own
    // variables, and e.uuid would read null
    database.transaction(() -> {
      database.command("opencypher", "UNWIND range(1, 60) AS i CREATE (:P {name: 'p' + toString(i)})");
      database.command("opencypher",
          "MATCH (a:P {name: 'p1'}), (b:P {name: 'p2'}), (c:P {name: 'p3'}), (d:P {name: 'p4'}) "
              + "CREATE (a)-[:KNOWS {uuid: 'k1'}]->(b)-[:FOLLOWS]->(c)-[:LIKES]->(d)");
      database.command("opencypher",
          "MATCH (a:P), (b:P) WHERE a.name STARTS WITH 'p5' AND b.name = 'p6' CREATE (a)-[:KNOWS {uuid: 'n' + a.name}]->(b)");
    });
    database.command("opencypher", "CREATE CONSTRAINT FOR ()-[k:KNOWS]-() REQUIRE k.uuid IS UNIQUE");
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gav11481 VERTEX TYPES (Entity, P) EDGE TYPES (FOLLOWS, LIKES) UPDATE MODE OFF");
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav11481");
    final long deadline = System.currentTimeMillis() + 60_000;
    while (!view.isReady() && System.currentTimeMillis() < deadline)
      Thread.sleep(20);
    assertThat(view.isReady()).isTrue();
    try {
      final List<String> rows = new ArrayList<>();
      final String plan = run("MATCH (a:P)-[e:KNOWS]->(b:P)-[:FOLLOWS]->(c:P)-[:LIKES]->(d:P) WHERE e.uuid = 'k1' "
          + "RETURN e.uuid + '/' + d.name AS uuid", Map.of(), rows);
      assertThat(plan).contains("EdgeIndexSeek").contains("GAV");
      assertThat(rows).containsExactly("k1/p4");
    } finally {
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW gav11481");
    }
  }

  private void assertSeeks(final String query, final Map<String, Object> parameters, final String... expected) {
    final List<String> rows = new ArrayList<>();
    final String plan = run(query, parameters, rows);
    assertThat(plan).contains("MATCH EDGE BY INDEX");
    assertThat(rows).containsExactly(expected);
  }

  private String run(final String query, final Map<String, Object> parameters, final List<String> rows) {
    try (final ResultSet rs = database.query("opencypher", "PROFILE " + query, parameters)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        rows.add(row.getProperty("uuid"));
      }
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }
}
