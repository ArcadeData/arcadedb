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
import com.arcadedb.exception.DeletedEntityException;
import com.arcadedb.exception.InvalidPropertyTypeException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Oracle tests for the FOREACH and UNWIND steps. Every expected answer was produced by Neo4j 5.26 on the same data and
 * statement, then confirmed on ArcadeDB: the shapes are the ones the modelhub batched upsert sends (an UNWIND page,
 * a FOREACH gated by a CASE list, a write read back through a list, a map or a path that holds the written node).
 */
class CypherForeachUnwindOracleTest extends TestHelper {
  @Override
  protected void beginTest() {
    database.command("opencypher",
        "CREATE (a:A {id: 1}), (b:A {id: 2}), (c:A {id: 3}) CREATE (a)-[:R {w: 1}]->(b), (b)-[:R {w: 2}]->(c)");
  }

  @Test
  void foreachOverNullListWritesNothingAndKeepsTheRows() {
    final Map<String, Object> params = new HashMap<>();
    params.put("nul", null);
    final List<Map<String, Object>> rows = rows("MATCH (n:A) FOREACH (x IN $nul | SET n.v = 1) RETURN n.id AS id, n.v AS v ORDER BY id", params);
    assertThat(rows).hasSize(3);
    for (final Map<String, Object> row : rows)
      assertThat(row.get("v")).isNull();
  }

  @Test
  void foreachOverEmptyListWritesNothingAndKeepsTheRows() {
    final List<Map<String, Object>> rows = rows("MATCH (n:A) FOREACH (x IN [] | SET n.v = 1) RETURN n.id AS id, n.v AS v ORDER BY id");
    assertThat(rows).hasSize(3);
    for (final Map<String, Object> row : rows)
      assertThat(row.get("v")).isNull();
  }

  @Test
  void foreachOverAScalarParameterRunsTheBodyOnce() {
    final List<Map<String, Object>> rows = rows("MATCH (n:A) FOREACH (x IN $s | SET n.v = x) RETURN n.id AS id, n.v AS v ORDER BY id",
        Map.of("s", 5));
    assertThat(rows).hasSize(3);
    for (final Map<String, Object> row : rows)
      assertThat(((Number) row.get("v")).longValue()).isEqualTo(5L);
  }

  @Test
  void foreachWriteIsReadThroughAListThatHoldsTheNode() {
    final List<Map<String, Object>> rows = rows(
        "MATCH (n:A) WITH n, [n] AS l FOREACH (i IN [1, 2] | SET n.v = i) RETURN n.id AS id, n.v AS v, l[0].v AS lv ORDER BY id");
    assertThat(rows).hasSize(3);
    for (final Map<String, Object> row : rows) {
      assertThat(((Number) row.get("v")).longValue()).isEqualTo(2L);
      assertThat(((Number) row.get("lv")).longValue()).isEqualTo(2L);
    }
  }

  @Test
  void foreachWriteIsReadThroughAMapThatHoldsTheNode() {
    final List<Map<String, Object>> rows = rows(
        "MATCH (n:A) WITH n, {k: n, z: [n]} AS m FOREACH (i IN [1, 2] | SET n.v = i) RETURN n.id AS id, m.k.v AS mv, m.z[0].v AS mz ORDER BY id");
    assertThat(rows).hasSize(3);
    for (final Map<String, Object> row : rows) {
      assertThat(((Number) row.get("mv")).longValue()).isEqualTo(2L);
      assertThat(((Number) row.get("mz")).longValue()).isEqualTo(2L);
    }
  }

  @Test
  void foreachWriteIsReadThroughAListOfLists() {
    final List<Map<String, Object>> rows = rows(
        "MATCH (n:A) WITH n, [[n], [n]] AS l FOREACH (i IN [1] | SET n.v = i) RETURN n.id AS id, l[0][0].v AS a, l[1][0].v AS b ORDER BY id");
    assertThat(rows).hasSize(3);
    for (final Map<String, Object> row : rows) {
      assertThat(((Number) row.get("a")).longValue()).isEqualTo(1L);
      assertThat(((Number) row.get("b")).longValue()).isEqualTo(1L);
    }
  }

  @Test
  void foreachOverThePathNodesIsReadThroughThePath() {
    final List<Map<String, Object>> rows = rows(
        "MATCH p = (a:A {id: 1})-[:R]->(b:A) FOREACH (x IN nodes(p) | SET x.v = x.id * 10) RETURN [y IN nodes(p) | y.v] AS vs");
    assertThat(rows).hasSize(1);
    assertThat(numbers(rows.get(0).get("vs"))).containsExactly(10L, 20L);
  }

  @Test
  void foreachOverACollectedListIsReadThroughTheList() {
    final List<Map<String, Object>> rows = rows(
        "MATCH (n:A) WITH n ORDER BY n.id WITH collect(n) AS ns FOREACH (x IN ns | SET x.v = x.id * 10) RETURN [y IN ns | y.v] AS vs");
    assertThat(rows).hasSize(1);
    assertThat(numbers(rows.get(0).get("vs"))).containsExactly(10L, 20L, 30L);
  }

  @Test
  void foreachLabelWriteIsReadThroughAListAndAMap() {
    final List<Map<String, Object>> rows = rows(
        "MATCH (n:A) WITH n, [n] AS l, {k: n} AS m FOREACH (i IN [1, 2] | SET n:Tag SET n.v = i) "
            + "RETURN n.id AS id, labels(n) AS nl, labels(l[0]) AS ll, labels(m.k) AS ml, m.k.v AS mv ORDER BY id");
    assertThat(rows).hasSize(3);
    for (final Map<String, Object> row : rows) {
      assertThat(strings(row.get("nl"))).containsExactlyInAnyOrder("A", "Tag");
      assertThat(strings(row.get("ll"))).containsExactlyInAnyOrder("A", "Tag");
      assertThat(strings(row.get("ml"))).containsExactlyInAnyOrder("A", "Tag");
      assertThat(((Number) row.get("mv")).longValue()).isEqualTo(2L);
    }
  }

  @Test
  void foreachRemoveIsReadThroughAList() {
    database.command("opencypher", "MATCH (n:A) SET n.q = 1");
    final List<Map<String, Object>> rows = rows(
        "MATCH (n:A) WITH n, [n] AS l FOREACH (i IN [1] | REMOVE n.q) RETURN n.id AS id, n.q AS q, l[0].q AS lq ORDER BY id");
    assertThat(rows).hasSize(3);
    for (final Map<String, Object> row : rows) {
      assertThat(row.get("q")).isNull();
      assertThat(row.get("lq")).isNull();
    }
  }

  @Test
  void nestedForeachOverRowsOfRowsCreatesOneNodePerElement() {
    database.command("opencypher", "FOREACH (row IN [[1, 2], [3]] | FOREACH (x IN row | CREATE (:Z {v: x})))");
    final List<Map<String, Object>> rows = rows("MATCH (z:Z) RETURN z.v AS v ORDER BY v");
    assertThat(rows).hasSize(3);
    assertThat(((Number) rows.get(0).get("v")).longValue()).isEqualTo(1L);
    assertThat(((Number) rows.get(1).get("v")).longValue()).isEqualTo(2L);
    assertThat(((Number) rows.get(2).get("v")).longValue()).isEqualTo(3L);
  }

  @Test
  void nestedForeachAccumulatesOnTheOuterNode() {
    final List<Map<String, Object>> rows = rows(
        "MATCH (n:A) FOREACH (i IN [1, 2] | FOREACH (j IN [10, 20] | SET n.s = coalesce(n.s, 0) + i * j)) RETURN n.id AS id, n.s AS s ORDER BY id");
    assertThat(rows).hasSize(3);
    for (final Map<String, Object> row : rows)
      assertThat(((Number) row.get("s")).longValue()).isEqualTo(90L);
  }

  @Test
  void foreachOverTheSameNodesTwiceVisitsEveryElementTwice() {
    final List<Map<String, Object>> rows = rows(
        "MATCH (n:A) WITH collect(n) AS ns FOREACH (x IN ns + ns | SET x.c = coalesce(x.c, 0) + 1) WITH ns UNWIND ns AS x RETURN x.id AS id, x.c AS c ORDER BY id");
    assertThat(rows).hasSize(3);
    for (final Map<String, Object> row : rows)
      assertThat(((Number) row.get("c")).longValue()).isEqualTo(2L);
  }

  @Test
  void unwindDrivesAForeachWithARangeThatDependsOnTheUnwoundValue() {
    final List<Map<String, Object>> rows = rows(
        "UNWIND [1, 2, 3] AS i MATCH (n:A {id: i}) FOREACH (j IN range(1, i) | SET n.c = coalesce(n.c, 0) + j) RETURN n.id AS id, n.c AS c ORDER BY id");
    assertThat(rows).hasSize(3);
    assertThat(((Number) rows.get(0).get("c")).longValue()).isEqualTo(1L);
    assertThat(((Number) rows.get(1).get("c")).longValue()).isEqualTo(3L);
    assertThat(((Number) rows.get(2).get("c")).longValue()).isEqualTo(6L);
  }

  @Test
  void foreachBodyErrorKeepsItsExceptionTypeAndRollsTheStatementBack() {
    assertThatThrownBy(() -> database.command("opencypher", "FOREACH (x IN [1] | CREATE (:Z {v: {a: 1}}))"))
        .isInstanceOf(InvalidPropertyTypeException.class);
    assertThat(rows("MATCH (z:Z) RETURN z.v AS v")).isEmpty();
  }

  @Test
  void unwindOfNullAndOfAnEmptyListProducesNoRowAndOfAScalarOne() {
    final Map<String, Object> params = new HashMap<>();
    params.put("nul", null);
    assertThat(rows("UNWIND $nul AS x RETURN x", params)).isEmpty();
    assertThat(rows("UNWIND [] AS x RETURN x")).isEmpty();
    final List<Map<String, Object>> scalar = rows("UNWIND $s AS x RETURN x", Map.of("s", 5));
    assertThat(scalar).hasSize(1);
    assertThat(((Number) scalar.get(0).get("x")).longValue()).isEqualTo(5L);
  }

  @Test
  void unwindDoesNotFlattenNestedListsAndKeepsTheNumericKinds() {
    final List<Map<String, Object>> nested = rows("UNWIND [[1, 2], [3]] AS l RETURN l");
    assertThat(nested).hasSize(2);
    assertThat(numbers(nested.get(0).get("l"))).containsExactly(1L, 2L);
    assertThat(numbers(nested.get(1).get("l"))).containsExactly(3L);

    final List<Map<String, Object>> kinds = rows("UNWIND [1, 1.0, 2] AS x RETURN x");
    assertThat(kinds).hasSize(3);
    assertThat(kinds.get(0).get("x")).isInstanceOf(Long.class).isEqualTo(1L);
    assertThat(kinds.get(1).get("x")).isInstanceOf(Double.class).isEqualTo(1.0d);
    assertThat(kinds.get(2).get("x")).isInstanceOf(Long.class).isEqualTo(2L);
  }

  @Test
  void unwindOfMapsWithNullsKeepsTheMissingAndNullKeysApart() {
    final List<Map<String, Object>> rows = rows("UNWIND [{a: 1, b: null}, {a: null}, {}] AS m RETURN m.a AS a, m.b AS b, size(keys(m)) AS k");
    assertThat(rows).hasSize(3);
    assertThat(((Number) rows.get(0).get("a")).longValue()).isEqualTo(1L);
    assertThat(rows.get(0).get("b")).isNull();
    assertThat(rows.get(1).get("a")).isNull();
    assertThat(rows.get(2).get("a")).isNull();
    assertThat(rows.get(2).get("b")).isNull();
    assertThat(((Number) rows.get(2).get("k")).longValue()).isZero();
  }

  @Test
  void foreachMergeOfTheSameNodeUnderAnotherVariableIsReadThroughTheOuterVariableAListAndAMap() {
    final List<Map<String, Object>> rows = rows(
        "MATCH (n:A {id: 1}) WITH n, [n] AS l, {k: n} AS m FOREACH (x IN [1] | MERGE (q:A {id: 1}) SET q.v = 99) RETURN n.v AS nv, l[0].v AS lv, m.k.v AS mv");
    assertThat(rows).hasSize(1);
    assertThat(((Number) rows.get(0).get("nv")).longValue()).isEqualTo(99L);
    assertThat(((Number) rows.get(0).get("lv")).longValue()).isEqualTo(99L);
    assertThat(((Number) rows.get(0).get("mv")).longValue()).isEqualTo(99L);
  }

  @Test
  void foreachMergeOfAnotherNodeLeavesTheHeldNodeAlone() {
    final List<Map<String, Object>> rows = rows(
        "MATCH (n:A {id: 1}) WITH n, [n] AS l FOREACH (x IN [1] | MERGE (q:A {id: 7}) SET q.v = 5) RETURN n.v AS nv, size(l) AS sz");
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).get("nv")).isNull();
    assertThat(((Number) rows.get(0).get("sz")).longValue()).isEqualTo(1L);
    assertThat(((Number) rows("MATCH (q:A {id: 7}) RETURN q.v AS v").get(0).get("v")).longValue()).isEqualTo(5L);
  }

  @Test
  void foreachWriteOnARelationshipIsReadThroughAListAndAMap() {
    final List<Map<String, Object>> rows = rows(
        "MATCH (a:A {id: 1})-[r:R]->(b) WITH a, r, [r] AS rl, {k: r} AS rm FOREACH (x IN [1] | SET r.w = 5) RETURN r.w AS w, rl[0].w AS lw, rm.k.w AS mw");
    assertThat(rows).hasSize(1);
    assertThat(((Number) rows.get(0).get("w")).longValue()).isEqualTo(5L);
    assertThat(((Number) rows.get(0).get("lw")).longValue()).isEqualTo(5L);
    assertThat(((Number) rows.get(0).get("mw")).longValue()).isEqualTo(5L);
  }

  @Test
  void foreachOverACollectedListOfRelationshipsWritesEveryElement() {
    final List<Map<String, Object>> rows = rows(
        "MATCH (a:A)-[r:R]->(b) WITH r ORDER BY r.w WITH collect(r) AS rs FOREACH (x IN rs | SET x.w = x.w * 10) RETURN [y IN rs | y.w] AS ws");
    assertThat(rows).hasSize(1);
    assertThat(numbers(rows.get(0).get("ws"))).containsExactly(10L, 20L);
  }

  @Test
  void foreachDeleteOfABoundNodeKeepsTheListLengthAndRefusesToReadTheDeletedNode() {
    final List<Map<String, Object>> rows = rows("MATCH (n:A {id: 1}) WITH n, [n] AS l FOREACH (x IN [1] | DETACH DELETE n) RETURN size(l) AS sz");
    assertThat(rows).hasSize(1);
    assertThat(((Number) rows.get(0).get("sz")).longValue()).isEqualTo(1L);
    assertThat(rows("MATCH (n:A) RETURN n.id AS id")).hasSize(2);

    assertThatThrownBy(() -> rows("MATCH (n:A {id: 2}) WITH n, [n] AS l FOREACH (x IN [1] | DETACH DELETE n) RETURN l[0].id AS i"))
        .isInstanceOf(DeletedEntityException.class);
    assertThat(rows("MATCH (n:A) RETURN n.id AS id")).hasSize(2);
  }

  @Test
  void foreachDeleteOfTheSameRelationshipListedTwiceDeletesItOnce() {
    final List<Map<String, Object>> rows = rows(
        "MATCH (a:A {id: 1})-[r:R]->(b) WITH collect(r) AS rs FOREACH (x IN rs + rs | DELETE x) WITH size(rs) AS sz MATCH ()-[e:R]->() RETURN sz, count(e) AS remaining");
    assertThat(rows).hasSize(1);
    assertThat(((Number) rows.get(0).get("sz")).longValue()).isEqualTo(1L);
    assertThat(((Number) rows.get(0).get("remaining")).longValue()).isEqualTo(1L);
  }

  @Test
  void foreachDetachDeleteOfABoundNodeRemovesItsRelationships() {
    final List<Map<String, Object>> rows = rows(
        "MATCH (n:A {id: 2}) WITH n, [n] AS l FOREACH (x IN l | DETACH DELETE x) WITH size(l) AS sz MATCH (m:A) OPTIONAL MATCH (m)-[e:R]-() "
            + "RETURN sz, count(DISTINCT m) AS nodes, count(e) AS rels");
    assertThat(rows).hasSize(1);
    assertThat(((Number) rows.get(0).get("sz")).longValue()).isEqualTo(1L);
    assertThat(((Number) rows.get(0).get("nodes")).longValue()).isEqualTo(2L);
    assertThat(((Number) rows.get(0).get("rels")).longValue()).isZero();
  }

  private List<Map<String, Object>> rows(final String query) {
    return rows(query, Map.of());
  }

  private List<Map<String, Object>> rows(final String query, final Map<String, Object> params) {
    final List<Map<String, Object>> rows = new ArrayList<>();
    try (final ResultSet rs = database.command("opencypher", query, params)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        rows.add(r.toMap());
      }
    }
    return rows;
  }

  private static List<Long> numbers(final Object list) {
    final List<Long> out = new ArrayList<>();
    for (final Object o : (List<?>) list)
      out.add(((Number) o).longValue());
    return out;
  }

  private static List<String> strings(final Object list) {
    final List<String> out = new ArrayList<>();
    for (final Object o : (List<?>) list)
      out.add(String.valueOf(o));
    return out;
  }
}
