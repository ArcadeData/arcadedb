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
import com.arcadedb.function.graph.IdFunction;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12134: a node or relationship the {@code WHERE} clause pins by {@code elementId()} / {@code id()} is fetched by RID, in
 * the cost-based plan and in the step-by-step one, instead of a scan of the label ({@code NodeByLabelScan [filter: elementId(a) IN
 * ...]}) or a scan of every vertex with a walk of every edge list. Each test asserts the plan chosen and the answer; the answers
 * are also compared with the same predicate written so that no seek can take it.
 */
class CypherIdSeekIssue12134Test {
  private static final int NODES = 40;

  private Database     database;
  private List<String> nodeIds;
  private List<String> edgeIds;
  private String       otherId;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/cypher-id-seek-12134").create();
    database.command("sql", "CREATE VERTEX TYPE N");
    database.command("sql", "CREATE VERTEX TYPE Other");
    database.command("sql", "CREATE EDGE TYPE R");
    database.command("sql", "CREATE EDGE TYPE L LIGHTWEIGHT");
    database.transaction(() -> {
      database.command("opencypher", "UNWIND range(0, $n - 1) AS i CREATE (:N {i: i})", Map.of("n", NODES));
      database.command("opencypher", "MATCH (a:N), (b:N) WHERE b.i = a.i + 1 CREATE (a)-[:R {j: a.i}]->(b)");
      database.command("opencypher", "MATCH (a:N), (b:N) WHERE b.i = a.i + 2 CREATE (a)-[:L]->(b)");
      database.command("opencypher", "CREATE (:Other {i: -1})");
    });
    nodeIds = column("SELECT @rid AS id FROM N ORDER BY i");
    edgeIds = column("SELECT @rid AS id FROM R ORDER BY j");
    otherId = column("SELECT @rid AS id FROM Other").getFirst();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void aLabelledNodePinnedByElementIdIsFetchedByRid() {
    final String query = "MATCH (a:N) WHERE elementId(a) = $id RETURN a.i AS i";
    final Map<String, Object> params = Map.of("id", nodeIds.get(7));

    assertThat(plan(query, params)).contains("NodeByIdSeek(a:N)").doesNotContain("NodeByLabelScan");
    assertThat(ints(query, params)).containsExactly(7);
  }

  @Test
  void aLabelledNodePinnedByAnElementIdListIsFetchedByRid() {
    final String query = "MATCH (a:N) WHERE elementId(a) IN $ids RETURN a.i AS i";
    // a duplicate, a vertex of another label, an id no record has, a text that is no id, and a null
    final List<Object> ids = Arrays.asList(nodeIds.get(3), nodeIds.get(5), nodeIds.get(3), otherId, "#9999:0", "not-an-id", null);
    final Map<String, Object> params = Map.of("ids", ids);

    assertThat(plan(query, params)).contains("NodeByIdSeek(a:N)").doesNotContain("NodeByLabelScan");
    assertThat(ints(query, params)).containsExactlyInAnyOrder(3, 5);
    assertThat(ints(query, params)).containsExactlyInAnyOrderElementsOf(
        ints("MATCH (a:N) WHERE (elementId(a) IN $ids) = true RETURN a.i AS i", params));
  }

  @Test
  void aLabelledNodePinnedByNumericIdsIsFetchedByRid() {
    final List<Long> ids = List.of(numericId(nodeIds.get(1)), numericId(nodeIds.get(2)), numericId(otherId));
    final String query = "MATCH (a:N) WHERE id(a) IN $ids RETURN a.i AS i";

    assertThat(plan(query, Map.of("ids", ids))).contains("NodeByIdSeek(a:N)");
    assertThat(ints(query, Map.of("ids", ids))).containsExactlyInAnyOrder(1, 2);
    assertThat(ints("MATCH (a:N) WHERE $lid = id(a) RETURN a.i AS i", Map.of("lid", numericId(nodeIds.get(9)))))
        .containsExactly(9);
  }

  @Test
  void aNodeSeekKeepsTheRestOfThePatternAndTheOtherPredicates() {
    final String query = "MATCH (a:N)-[r:R]->(b:N) WHERE elementId(a) IN $ids AND b.i > 5 RETURN b.i AS i";
    final Map<String, Object> params = Map.of("ids", List.of(nodeIds.get(2), nodeIds.get(8)));

    assertThat(plan(query, params)).contains("NodeByIdSeek(a:N)");
    assertThat(ints(query, params)).containsExactly(9);
  }

  @Test
  void anUnlabelledNodePinnedByAnElementIdListIsFetchedByRid() {
    final String query = "MATCH (a) WHERE elementId(a) IN $ids RETURN a.i AS i";
    final Map<String, Object> params = Map.of("ids", List.of(nodeIds.get(4), otherId, "#9999:0"));

    assertThat(plan(query, params)).contains("MATCH NODE (a) [id: " + nodeIds.get(4) + ", " + otherId + ", #9999:0]");
    assertThat(ints(query, params)).containsExactlyInAnyOrder(4, -1);
    assertThat(profile(query, params)).contains("MATCH NODE (a) [id: ").contains("2 rows");
  }

  @Test
  void aHopWhoseTargetIsPinnedByIdStartsFromTheTarget() {
    final String query = "MATCH (a)-[:R]->(b) WHERE elementId(b) = $id RETURN a.i AS i";
    final Map<String, Object> params = Map.of("id", nodeIds.get(12));

    final String plan = plan(query, params);
    assertThat(plan).contains("MATCH NODE (b) [id: " + nodeIds.get(12) + "]");
    assertThat(plan.indexOf("MATCH NODE (b)")).isLessThan(plan.indexOf("MATCH RELATIONSHIP"));
    assertThat(ints(query, params)).containsExactly(11);
    assertSameAnswer(query, "MATCH (a)-[:R]->(b) WHERE (elementId(b) = $id) = true RETURN a.i AS i", params);
  }

  @Test
  void aRelationshipPinnedByElementIdIsFetchedWithItsEndpoints() {
    final String query = "MATCH (a)-[r:R]->(b) WHERE elementId(r) IN $ids RETURN a.i AS a, r.j AS j, b.i AS b";
    final Map<String, Object> params = Map.of("ids", List.of(edgeIds.get(4), edgeIds.get(11), nodeIds.get(1)));

    assertThat(plan(query, params)).contains("MATCH EDGE BY ID (a)-[r:R]->(b)").doesNotContain("MATCH RELATIONSHIP");
    assertThat(rows(query, params)).containsExactlyInAnyOrder("4-4-5", "11-11-12");
    assertThat(profile(query, params)).contains("MATCH EDGE BY ID").contains("3 rows");
  }

  @Test
  void anAnonymousEndpointAndAnIncomingArrowBindTheWrittenDirection() {
    final Map<String, Object> params = Map.of("id", edgeIds.get(6));

    final String incoming = "MATCH (b)<-[r:R]-(a) WHERE elementId(r) = $id RETURN a.i AS a, r.j AS j, b.i AS b";
    assertThat(plan(incoming, params)).contains("MATCH EDGE BY ID (b)<-[r:R]-(a)");
    assertThat(rows(incoming, params)).containsExactly("6-6-7");

    final String counted = "MATCH ()-[r:R]->() WHERE elementId(r) = $id RETURN count(r) AS c";
    assertThat(plan(counted, params)).contains("MATCH EDGE BY ID");
    try (final ResultSet rs = database.query("opencypher", counted, params)) {
      assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(1L);
    }

    // the edge exists but the pattern's type and endpoints reject it
    assertThat(rows("MATCH (a)-[r:L]->(b) WHERE elementId(r) = $id RETURN a.i AS a, r.j AS j, b.i AS b", params)).isEmpty();
    assertThat(rows("MATCH (a:Other)-[r:R]->(b) WHERE elementId(r) = $id RETURN a.i AS a, r.j AS j, b.i AS b", params)).isEmpty();
  }

  @Test
  void aRelationshipBetweenLabelledNodesIsSeededByIdInTheCostBasedPlan() {
    final String query = "MATCH (a:N)-[r:R]->(b:N) WHERE elementId(r) IN $ids RETURN a.i AS a, r.j AS j, b.i AS b";
    final Map<String, Object> params = Map.of("ids", List.of(edgeIds.get(0), edgeIds.get(20)));

    assertThat(plan(query, params)).contains("EdgeIdSeek").doesNotContain("NodeByLabelScan");
    assertThat(rows(query, params)).containsExactlyInAnyOrder("0-0-1", "20-20-21");
    assertThat(rows("MATCH (a:N)-[r:R]->(b:Other) WHERE elementId(r) IN $ids RETURN a.i AS a, r.j AS j, b.i AS b", params))
        .isEmpty();
  }

  @Test
  void aLightweightRelationshipTypeKeepsTheWalk() {
    // a lightweight edge has no record for a RID to fetch: only the walk of the edge lists finds it
    final String query = "MATCH (a)-[r:L]->(b) WHERE elementId(r) IN $ids RETURN count(r) AS c";
    final Map<String, Object> params = Map.of("ids", List.of(edgeIds.get(0)));

    assertThat(plan(query, params)).doesNotContain("BY ID");
    assertThat(plan("MATCH (a)-[r]->(b) WHERE elementId(r) IN $ids RETURN count(r) AS c", params)).doesNotContain("BY ID");
  }

  @Test
  void everySpellingAnswersAsThePredicateDoes() {
    final List<Object> mixed = Arrays.asList(nodeIds.get(0), edgeIds.get(0), numericId(nodeIds.get(3)), otherId, "#9999:0", 3.0, null);
    for (final Object value : mixed) {
      final Map<String, Object> one = new HashMap<>();
      one.put("v", value);
      assertSameAnswer("MATCH (a:N) WHERE elementId(a) = $v RETURN a.i AS i", "MATCH (a:N) WHERE (elementId(a) = $v) = true RETURN a.i AS i",
          one);
      assertSameAnswer("MATCH (a:N) WHERE id(a) = $v RETURN a.i AS i", "MATCH (a:N) WHERE (id(a) = $v) = true RETURN a.i AS i", one);
      assertSameAnswer("MATCH (a) WHERE id(a) = $v RETURN a.i AS i", "MATCH (a) WHERE (id(a) = $v) = true RETURN a.i AS i", one);
      assertSameAnswer("MATCH (a)-[r:R]->(b) WHERE elementId(r) = $v RETURN a.i AS i",
          "MATCH (a)-[r:R]->(b) WHERE (elementId(r) = $v) = true RETURN a.i AS i", one);
    }
    final Map<String, Object> list = Map.of("v", mixed.stream().filter(v -> v != null).toList());
    assertSameAnswer("MATCH (a:N) WHERE elementId(a) IN $v RETURN a.i AS i", "MATCH (a:N) WHERE (elementId(a) IN $v) = true RETURN a.i AS i",
        list);
    assertSameAnswer("MATCH (a)-[r:R]->(b) WHERE id(r) IN $v RETURN a.i AS i",
        "MATCH (a)-[r:R]->(b) WHERE (id(r) IN $v) = true RETURN a.i AS i", list);
    assertSameAnswer("MATCH (a:N) WHERE elementId(a) IN [$a, $b] RETURN a.i AS i",
        "MATCH (a:N) WHERE (elementId(a) IN [$a, $b]) = true RETURN a.i AS i", Map.of("a", nodeIds.get(1), "b", nodeIds.get(2)));
    // the address a lightweight edge carries names no record
    assertThat(ints("MATCH (a:N) WHERE elementId(a) = $v RETURN a.i AS i", Map.of("v", "#-1:-1"))).isEmpty();
    assertThat(ints("MATCH (a)-[r:R]->(b) WHERE elementId(r) = $v RETURN a.i AS i", Map.of("v", "#-1:-1"))).isEmpty();
  }

  private void assertSameAnswer(final String seek, final String scan, final Map<String, Object> params) {
    assertThat(ints(seek, params)).as("%s with %s", seek, params).containsExactlyInAnyOrderElementsOf(ints(scan, params));
  }

  private long numericId(final String rid) {
    return IdFunction.encodeRidAsLong(new RID(rid));
  }

  private List<String> column(final String sql) {
    final List<String> values = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        values.add(rs.next().getProperty("id").toString());
    }
    return values;
  }

  private List<Integer> ints(final String query, final Map<String, Object> params) {
    final List<Integer> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      while (rs.hasNext())
        values.add(((Number) rs.next().getProperty("i")).intValue());
    }
    return values;
  }

  private List<String> rows(final String query, final Map<String, Object> params) {
    final List<String> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        values.add(row.getProperty("a") + "-" + row.getProperty("j") + "-" + row.getProperty("b"));
      }
    }
    return values;
  }

  private String plan(final String query, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query, params)) {
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  private String profile(final String query, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("opencypher", "PROFILE " + query, params)) {
      while (rs.hasNext())
        rs.next();
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }
}
