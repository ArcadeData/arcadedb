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
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.procedures.meta.MetaGraph;
import com.arcadedb.query.opencypher.procedures.meta.MetaNodeTypeProperties;
import com.arcadedb.query.opencypher.procedures.meta.MetaRelTypeProperties;
import com.arcadedb.query.opencypher.procedures.meta.MetaStats;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
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

    private static String stackTraceOf(final Throwable error) {
      final StringWriter writer = new StringWriter();
      error.printStackTrace(new PrintWriter(writer));
      return writer.toString();
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
    void vNodeCallFormTakesTheDefaultedPropertiesToo() {
      // APOC declares props with a default of {} on the procedure as well as on the function: the one-argument CALL is valid
      final Result row = single("CALL apoc.create.vNode(['A']) YIELD node RETURN labels(node) AS l, size(keys(node)) AS k");
      assertThat(list(row.getProperty("l"))).containsExactly("A");
      assertThat(longOf(row.getProperty("k"))).isZero();
    }

    @Test
    void labelExpressionsAnswerOnAVirtualNode() {
      final Result row = single("""
          WITH apoc.create.vNode(['A', 'B'], {x: 1}) AS n
          RETURN n:A AS a, n:A:B AS ab, n:C AS c, n:A|C AS aOrC, n:(A|C) AS aOrCGrouped, n:!B AS notB, n:!C AS notC,
                 n:% AS any, n:A&!B AS aNotB, n:C|!B AS cOrNotB""");
      assertThat((Boolean) row.getProperty("a")).isTrue();
      assertThat((Boolean) row.getProperty("ab")).isTrue();
      assertThat((Boolean) row.getProperty("c")).isFalse();
      assertThat((Boolean) row.getProperty("aOrC")).isTrue();
      assertThat((Boolean) row.getProperty("aOrCGrouped")).isTrue();
      // a node that carries B is not "not B": the negation and the wildcard used to read a virtual node as having no type
      assertThat((Boolean) row.getProperty("notB")).isFalse();
      assertThat((Boolean) row.getProperty("notC")).isTrue();
      assertThat((Boolean) row.getProperty("any")).isTrue();
      assertThat((Boolean) row.getProperty("aNotB")).isFalse();
      assertThat((Boolean) row.getProperty("cOrNotB")).isFalse();

      assertThat(longOf(single("WITH apoc.create.vNode(['A']) AS n WHERE n:A RETURN count(*) AS c").getProperty("c"))).isEqualTo(1L);
      assertThat(longOf(single("WITH apoc.create.vNode(['A']) AS n WHERE n:!A RETURN count(*) AS c").getProperty("c"))).isZero();
      assertThat(longOf(single("WITH apoc.create.vNode(['A']) AS n WHERE n:% RETURN count(*) AS c").getProperty("c"))).isEqualTo(1L);
    }

    @Test
    void labelExpressionsAnswerOnAnUnlabelledVirtualNode() {
      final Result row = single("WITH apoc.create.vNode([], {x: 1}) AS n RETURN n:A AS a, n:!A AS notA, n:% AS any, n:!% AS none");
      assertThat((Boolean) row.getProperty("a")).isFalse();
      assertThat((Boolean) row.getProperty("notA")).isTrue();
      assertThat((Boolean) row.getProperty("any")).isFalse();
      assertThat((Boolean) row.getProperty("none")).isTrue();
    }

    @Test
    void typeExpressionsAnswerOnAVirtualRelationship() {
      final Result row = single("""
          WITH apoc.create.vNode(['A']) AS a, apoc.create.vNode(['B']) AS b
          WITH apoc.create.vRelationship(a, 'V', {w: 1}, b) AS r
          RETURN r:V AS v, r:W AS w, r:V|W AS vOrW, r:(V|W) AS vOrWGrouped, r:!V AS notV, r:!W AS notW, r:% AS any,
                 r:V&!W AS vNotW, r:V&W AS vAndW""");
      assertThat((Boolean) row.getProperty("v")).isTrue();
      assertThat((Boolean) row.getProperty("w")).isFalse();
      assertThat((Boolean) row.getProperty("vOrW")).isTrue();
      assertThat((Boolean) row.getProperty("vOrWGrouped")).isTrue();
      assertThat((Boolean) row.getProperty("notV")).isFalse();
      assertThat((Boolean) row.getProperty("notW")).isTrue();
      assertThat((Boolean) row.getProperty("any")).isTrue();
      assertThat((Boolean) row.getProperty("vNotW")).isTrue();
      assertThat((Boolean) row.getProperty("vAndW")).isFalse();
    }

    @Test
    void aBoundVirtualNodeHasNoStoredRelationships() {
      createGraph();
      assertThat(run("WITH apoc.create.vNode(['P']) AS n MATCH (n)-[r]->(m) RETURN r")).isEmpty();
      assertThat(run("WITH apoc.create.vNode(['P']) AS n MATCH (m)-[r]->(n) RETURN r")).isEmpty();
      assertThat(longOf(single("WITH apoc.create.vNode(['P']) AS n RETURN size([(n)-[r]-() | r]) AS c").getProperty("c"))).isZero();
      assertThat(longOf(single("WITH apoc.create.vNode(['P']) AS n MATCH (n:P) RETURN count(*) AS c").getProperty("c"))).isEqualTo(1L);
      assertThat(run("WITH apoc.create.vNode(['Q']) AS n MATCH (n:P) RETURN n")).isEmpty();
    }

    @Test
    void writesAgainstAVirtualNodeAreRefusedAndStoreNothing() {
      createGraph();
      final List<String> statements = List.of(
          "WITH apoc.create.vNode(['A'], {x: 1}) AS n SET n.x = 2 RETURN n",
          "WITH apoc.create.vNode(['A'], {x: 1}) AS n SET n += {y: 1} RETURN n",
          "WITH apoc.create.vNode(['A'], {x: 1}) AS n SET n = {y: 1} RETURN n",
          "WITH apoc.create.vNode(['A'], {x: 1}) AS n SET n:B RETURN n",
          "WITH apoc.create.vNode(['A'], {x: 1}) AS n REMOVE n.x RETURN n",
          "WITH apoc.create.vNode(['A'], {x: 1}) AS n DELETE n",
          "WITH apoc.create.vNode(['A'], {x: 1}) AS n DETACH DELETE n",
          "MATCH (a:P {uid: 1}) WITH a, apoc.create.vNode(['A']) AS v CREATE (a)-[:R]->(v)",
          "MATCH (a:P {uid: 1}) WITH a, apoc.create.vNode(['A']) AS v CREATE (v)-[:R]->(a)",
          "MATCH (a:P {uid: 1}) WITH a, apoc.create.vNode(['A']) AS v MERGE (a)-[:R]->(v)",
          "WITH apoc.create.vNode(['A']) AS v MERGE (v)-[:R]->(:Q)",
          "WITH apoc.create.vNode(['A']) AS v MERGE (v)",
          "MATCH (a:P {uid: 1}) WITH a, apoc.create.vNode(['A']) AS a2, apoc.create.vNode(['B']) AS b2 " +
              "WITH apoc.create.vRelationship(a2, 'V', {}, b2) AS r SET r.w = 1 RETURN r",
          "WITH apoc.create.vNode(['A']) AS a, apoc.create.vNode(['B']) AS b " +
              "WITH apoc.create.vRelationship(a, 'V', {}, b) AS r DELETE r");

      for (final String statement : statements) {
        assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher", statement).close()))
            .as(statement)
            .satisfies(e -> {
              // refused for what it is, never by a ClassCastException or a NullPointerException out of the storage layer
              final String stack = stackTraceOf(e);
              assertThat(stack).as(statement).doesNotContain("ClassCastException", "NullPointerException");
              assertThat(stack).as(statement).containsIgnoringCase("virtual");
            });
      }

      // no record of the refused writes was stored: the two nodes and three relationships of the graph are all there is
      assertThat(longOf(single("MATCH ()-[r:R]->() RETURN count(r) AS c").getProperty("c"))).isZero();
      assertThat(longOf(single("MATCH (n) RETURN count(n) AS c").getProperty("c"))).isEqualTo(2L);
      assertThat(longOf(single("MATCH ()-[r]->() RETURN count(r) AS c").getProperty("c"))).isEqualTo(3L);
    }

    @Test
    void virtualElementsSerializeLikeStoredOnesForTheHttpApi() {
      final JsonSerializer serializer = JsonSerializer.createJsonSerializer();
      try (final ResultSet rs = database.query("opencypher", """
          WITH apoc.create.vNode([], {x: 1}) AS a, apoc.create.vNode(['B', 'A'], {y: 2}) AS b
          RETURN a, b, apoc.create.vRelationship(a, 'V', {w: 3}, b) AS r""")) {
        final Result row = rs.next();
        final JSONObject json = serializer.serializeResult(database, row);

        // an unlabelled node has no type to name, a labelled one the joined labels
        final JSONObject a = json.getJSONObject("a");
        assertThat(a.getString("@rid")).startsWith("#-1:");
        assertThat(a.has("@type")).isFalse();
        assertThat(a.getString("@cat")).isEqualTo("v");
        assertThat(a.getInt("x")).isEqualTo(1);

        final JSONObject b = json.getJSONObject("b");
        assertThat(b.getString("@type")).isEqualTo("A~B");
        assertThat(b.getString("@cat")).isEqualTo("v");

        final JSONObject r = json.getJSONObject("r");
        assertThat(r.getString("@type")).isEqualTo("V");
        assertThat(r.getString("@cat")).isEqualTo("e");
        assertThat(r.getString("@out")).isEqualTo(a.getString("@rid"));
        assertThat(r.getString("@in")).isEqualTo(b.getString("@rid"));
        assertThat(r.getInt("w")).isEqualTo(3);
      }
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
    void typePropertiesSampleALargeLabelAndHonourTheSampleSetting() {
      database.transaction(() -> database.command("opencypher", "UNWIND range(1, 2500) AS i CREATE (:Big {i: i})").close());

      // 2500 nodes against a sample of 1000: every second node is examined
      assertThat(longOf(single("""
          CALL apoc.meta.nodeTypeProperties() YIELD propertyName, totalObservations
          WHERE propertyName = 'i' RETURN totalObservations AS t""").getProperty("t"))).isEqualTo(1250L);
      assertThat(longOf(single("""
          CALL apoc.meta.nodeTypeProperties({sample: 100}) YIELD propertyName, totalObservations
          WHERE propertyName = 'i' RETURN totalObservations AS t""").getProperty("t"))).isEqualTo(100L);
      // -1 reads every node
      assertThat(longOf(single("""
          CALL apoc.meta.nodeTypeProperties({sample: -1}) YIELD propertyName, totalObservations
          WHERE propertyName = 'i' RETURN totalObservations AS t""").getProperty("t"))).isEqualTo(2500L);

      // the counts do not depend on the sample
      final Result stats = single("CALL apoc.meta.stats() YIELD nodeCount, labels, propertyKeyCount RETURN nodeCount, labels, propertyKeyCount");
      assertThat(longOf(stats.getProperty("nodeCount"))).isEqualTo(2500L);
      assertThat((Map<?, ?>) stats.getProperty("labels")).isEqualTo(Map.of("Big", 2500L));
      assertThat(longOf(stats.getProperty("propertyKeyCount"))).isEqualTo(1L);
    }

    @Test
    void theMetaScansStopWhenTheCommandDeadlinePasses() {
      createGraph();
      // a deadline already in the past: the first record read of each scan has to notice it, as every other scan of the engine
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);
      context.setCommandDeadline(0L, "the test deadline");

      assertThatThrownBy(() -> new MetaStats().execute(new Object[0], null, context)).isInstanceOf(TimeoutException.class);
      assertThatThrownBy(() -> new MetaGraph().execute(new Object[0], null, context)).isInstanceOf(TimeoutException.class);
      assertThatThrownBy(() -> new MetaNodeTypeProperties().execute(new Object[0], null, context)).isInstanceOf(TimeoutException.class);
      assertThatThrownBy(() -> new MetaRelTypeProperties().execute(new Object[0], null, context)).isInstanceOf(TimeoutException.class);
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
