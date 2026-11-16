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
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The APOC virtual-graph family: virtual nodes and relationships ({@code create.vNode}, {@code create.vRelationship}), the
 * {@code meta.*} procedures that describe the graph, and {@code path.slice}.
 */
class CypherApocVirtualGraphTest {

  /**
   * Issue #12109: {@code create.vNode} answered a map, {@code create.vRelationship(from, type, props, to)} read its
   * arguments in another order and threw, {@code meta.stats} yielded one map instead of APOC's columns, the
   * {@code *TypeProperties} pair listed declared properties only (none for a graph Cypher created), {@code meta.graph}
   * answered maps, and {@code path.slice} read its length as an end index. Expected values are the answers of Neo4j 5.26
   * with APOC 5.26.33 on the same graph.
   */
  @Nested
  class Issue12109VirtualNodesMetaAndPathSlice extends TestHelper {
    private static final String GRAPH = """
        CREATE (a:P {uid:1, name:'a'})-[:KNOWS {w:1.0}]->(b:P {uid:2, name:'b'}),
               (a)-[:LIKES {w:5.0}]->(b), (b)-[:KNOWS {w:2.0}]->(a)""";

    private void createGraph() {
      database.transaction(() -> database.command("opencypher", GRAPH).close());
    }

    private List<Result> run(final String cypher) {
      final List<Result> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", cypher)) {
        while (rs.hasNext())
          rows.add(rs.next());
      }
      return rows;
    }

    private Result single(final String cypher) {
      final List<Result> rows = run(cypher);
      assertThat(rows).as(cypher).hasSize(1);
      return rows.getFirst();
    }

    private static long longOf(final Object value) {
      return ((Number) value).longValue();
    }

    @SuppressWarnings("unchecked")
    private static List<Object> list(final Object value) {
      return (List<Object>) value;
    }

    @Test
    void vNodeIsANodeWithItsLabelsAndProperties() {
      final Result row = single("""
          WITH apoc.create.vNode(['B', 'A'], {x: 1}) AS n
          RETURN n, labels(n) AS labels, properties(n) AS props, n.x AS x, keys(n) AS keys, id(n) AS id, elementId(n) AS eid""");
      final Object node = row.getProperty("n");
      assertThat(node).isInstanceOf(Vertex.class);
      assertThat(list(row.getProperty("labels"))).containsExactly("A", "B");
      assertThat(longOf(((Map<?, ?>) row.getProperty("props")).get("x"))).isEqualTo(1L);
      assertThat(longOf(row.getProperty("x"))).isEqualTo(1L);
      assertThat(list(row.getProperty("keys"))).containsExactly("x");
      // APOC gives virtual entities a negative id, and an elementId that is that id as text
      final long id = longOf(row.getProperty("id"));
      assertThat(id).isNegative();
      assertThat((String) row.getProperty("eid")).isEqualTo(Long.toString(id));
    }

    @Test
    void vNodeDefaultsAndTheCallForm() {
      assertThat(list(single("RETURN labels(apoc.create.vNode(['P'])) AS l").getProperty("l"))).containsExactly("P");
      assertThat(list(single("RETURN labels(apoc.create.vNode(null)) AS l").getProperty("l"))).isEmpty();

      final Result row = single("CALL apoc.create.vNode(['A'], {x: 1}) YIELD node RETURN node, labels(node) AS l, node.x AS x");
      assertThat(row.<Object>getProperty("node")).isInstanceOf(Vertex.class);
      assertThat(list(row.getProperty("l"))).containsExactly("A");
      assertThat(longOf(row.getProperty("x"))).isEqualTo(1L);

      // two virtual nodes are two nodes, even with the same labels and properties
      assertThat((Boolean) single("RETURN apoc.create.vNode(['A'], {x: 1}) = apoc.create.vNode(['A'], {x: 1}) AS eq")
          .getProperty("eq")).isFalse();
    }

    @Test
    void vRelationshipTakesThePropertiesBeforeTheEndNode() {
      createGraph();
      final Result row = single("""
          MATCH (a:P {uid: 1}), (b:P {uid: 2})
          WITH apoc.create.vRelationship(a, 'V', {w: 1}, b) AS r
          RETURN r, type(r) AS t, startNode(r).uid AS s, endNode(r).uid AS e, r.w AS w, id(r) AS id""");
      assertThat(row.<Object>getProperty("r")).isInstanceOf(Edge.class);
      assertThat((String) row.getProperty("t")).isEqualTo("V");
      assertThat(longOf(row.getProperty("s"))).isEqualTo(1L);
      assertThat(longOf(row.getProperty("e"))).isEqualTo(2L);
      assertThat(longOf(row.getProperty("w"))).isEqualTo(1L);
      assertThat(longOf(row.getProperty("id"))).isNegative();

      final Result call = single("""
          MATCH (a:P {uid: 1}), (b:P {uid: 2})
          CALL apoc.create.vRelationship(a, 'V', {w: 1}, b) YIELD rel
          RETURN type(rel) AS t, endNode(rel).uid AS e""");
      assertThat((String) call.getProperty("t")).isEqualTo("V");
      assertThat(longOf(call.getProperty("e"))).isEqualTo(2L);

      // between virtual nodes too
      assertThat(list(single("""
          WITH apoc.create.vNode(['A'], {}) AS a, apoc.create.vNode(['B'], {}) AS b
          RETURN labels(endNode(apoc.create.vRelationship(a, 'V', {}, b))) AS l""").getProperty("l"))).containsExactly("B");

      assertThatThrownBy(() -> run("MATCH (b:P {uid: 2}) RETURN apoc.create.vRelationship(null, 'V', {}, b) AS r"))
          .hasStackTraceContaining("start node is null");
    }

    @Test
    void metaStatsYieldsApocColumns() {
      createGraph();
      final Result row = single("""
          CALL apoc.meta.stats()
          YIELD labelCount, relTypeCount, nodeCount, relCount, labels, relTypes, relTypesCount, stats
          RETURN labelCount, relTypeCount, nodeCount, relCount, labels, relTypes, relTypesCount, stats""");
      assertThat(longOf(row.getProperty("labelCount"))).isEqualTo(1L);
      assertThat(longOf(row.getProperty("relTypeCount"))).isEqualTo(2L);
      assertThat(longOf(row.getProperty("nodeCount"))).isEqualTo(2L);
      assertThat(longOf(row.getProperty("relCount"))).isEqualTo(3L);
      assertThat((Map<?, ?>) row.getProperty("labels")).isEqualTo(Map.of("P", 2L));
      assertThat((Map<?, ?>) row.getProperty("relTypes")).isEqualTo(Map.of(
          "()-[:KNOWS]->()", 2L, "(:P)-[:KNOWS]->()", 2L, "()-[:KNOWS]->(:P)", 2L,
          "()-[:LIKES]->()", 1L, "(:P)-[:LIKES]->()", 1L, "()-[:LIKES]->(:P)", 1L));
      assertThat((Map<?, ?>) row.getProperty("relTypesCount")).isEqualTo(Map.of("KNOWS", 2L, "LIKES", 1L));
      assertThat(longOf(((Map<?, ?>) row.getProperty("stats")).get("nodeCount"))).isEqualTo(2L);

      // the value column the ArcadeDB documentation shows keeps working
      assertThat(longOf(single("CALL apoc.meta.stats() YIELD value RETURN value.nodeCount AS n").getProperty("n"))).isEqualTo(2L);
    }

    @Test
    void typePropertiesReportThePropertiesFoundInTheData() {
      createGraph();
      final List<Result> nodeRows = run("""
          CALL apoc.meta.nodeTypeProperties()
          YIELD nodeType, nodeLabels, propertyName, propertyTypes, mandatory, propertyObservations, totalObservations
          RETURN nodeType, nodeLabels, propertyName, propertyTypes, mandatory, propertyObservations, totalObservations
          ORDER BY propertyName""");
      assertThat(nodeRows).hasSize(2);
      assertThat((String) nodeRows.get(0).getProperty("nodeType")).isEqualTo(":`P`");
      assertThat(list(nodeRows.get(0).getProperty("nodeLabels"))).containsExactly("P");
      assertThat((String) nodeRows.get(0).getProperty("propertyName")).isEqualTo("name");
      assertThat(list(nodeRows.get(0).getProperty("propertyTypes"))).containsExactly("String");
      assertThat((Boolean) nodeRows.get(0).getProperty("mandatory")).isFalse();
      assertThat(longOf(nodeRows.get(0).getProperty("propertyObservations"))).isEqualTo(2L);
      assertThat(longOf(nodeRows.get(0).getProperty("totalObservations"))).isEqualTo(2L);
      assertThat((String) nodeRows.get(1).getProperty("propertyName")).isEqualTo("uid");
      assertThat(list(nodeRows.get(1).getProperty("propertyTypes"))).containsExactly("Long");

      final List<Result> relRows = run("""
          CALL apoc.meta.relTypeProperties()
          YIELD relType, sourceNodeLabels, targetNodeLabels, propertyName, propertyTypes, propertyObservations, totalObservations
          RETURN relType, sourceNodeLabels, targetNodeLabels, propertyName, propertyTypes, propertyObservations, totalObservations
          ORDER BY relType""");
      assertThat(relRows).hasSize(2);
      assertThat((String) relRows.get(0).getProperty("relType")).isEqualTo(":`KNOWS`");
      assertThat(list(relRows.get(0).getProperty("sourceNodeLabels"))).containsExactly("P");
      assertThat(list(relRows.get(0).getProperty("targetNodeLabels"))).containsExactly("P");
      assertThat((String) relRows.get(0).getProperty("propertyName")).isEqualTo("w");
      assertThat(list(relRows.get(0).getProperty("propertyTypes"))).containsExactly("Double");
      assertThat(longOf(relRows.get(0).getProperty("propertyObservations"))).isEqualTo(2L);
      assertThat(longOf(relRows.get(0).getProperty("totalObservations"))).isEqualTo(2L);
      assertThat((String) relRows.get(1).getProperty("relType")).isEqualTo(":`LIKES`");
      assertThat(longOf(relRows.get(1).getProperty("totalObservations"))).isEqualTo(1L);

      // the optional config map: includeLabels narrows, unknown keys are ignored
      assertThat(run("CALL apoc.meta.nodeTypeProperties({includeLabels: ['Nope']}) YIELD propertyName RETURN propertyName")).isEmpty();
      assertThat(run("CALL apoc.meta.nodeTypeProperties({sample: -1, bogus: 1}) YIELD propertyName RETURN propertyName")).hasSize(2);
    }

    @Test
    void metaGraphAnswersVirtualNodesAndRelationships() {
      createGraph();
      final Result row = single("""
          CALL apoc.meta.graph() YIELD nodes, relationships
          RETURN [n IN nodes | [labels(n), n.name, n.count]] AS n,
                 [r IN relationships | [type(r), labels(startNode(r)), labels(endNode(r)), r.out, r.in, r.count]] AS r""");
      assertThat(list(row.getProperty("n"))).containsExactly(List.of(List.of("P"), "P", 2L));
      assertThat(list(row.getProperty("r"))).containsExactly(
          List.of("KNOWS", List.of("P"), List.of("P"), 2L, 2L, 2L),
          List.of("LIKES", List.of("P"), List.of("P"), 1L, 1L, 1L));
    }

    @Test
    void pathSliceReadsAnOffsetAndALength() {
      createGraph();
      final Result row = single("""
          MATCH p = (:P {uid: 1})-[:KNOWS]->(:P)-[:KNOWS]->(:P)
          RETURN length(apoc.path.slice(p, 1, 1)) AS a, length(apoc.path.slice(p, 0, -1)) AS b,
                 length(apoc.path.slice(p)) AS c, length(apoc.path.slice(p, 1)) AS d,
                 length(apoc.path.slice(p, 1, -2)) AS e, length(apoc.path.slice(p, -1, 1)) AS f,
                 length(apoc.path.slice(p, 5)) AS g, [n IN nodes(apoc.path.slice(p, 1, 1)) | n.uid] AS h,
                 [n IN nodes(apoc.path.slice(p, 5)) | n.uid] AS i""");
      assertThat(longOf(row.getProperty("a"))).isEqualTo(1L);
      assertThat(longOf(row.getProperty("b"))).isEqualTo(2L);
      assertThat(longOf(row.getProperty("c"))).isEqualTo(2L);
      assertThat(longOf(row.getProperty("d"))).isEqualTo(1L);
      assertThat(longOf(row.getProperty("e"))).isEqualTo(0L);
      assertThat(longOf(row.getProperty("f"))).isEqualTo(1L);
      assertThat(longOf(row.getProperty("g"))).isEqualTo(0L);
      assertThat(list(row.getProperty("h")).stream().map(Issue12109VirtualNodesMetaAndPathSlice::longOf).toList()).containsExactly(2L, 1L);
      assertThat(list(row.getProperty("i")).stream().map(Issue12109VirtualNodesMetaAndPathSlice::longOf).toList()).containsExactly(1L);
    }
  }
}
