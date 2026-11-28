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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The SET that follows a MERGE is folded into the entity the MERGE creates when it can be (issues #8735, #12640), and stays a step
 * of its own when it cannot. The two have to give what Cypher gives: the SET runs after the match or the creation, so a property
 * of the pattern that the SET rewrites or removes is no longer found by the next MERGE, which then creates another entity. Every
 * expectation below was checked against Neo4j 5.26 (the same statements, row by row, rows and resulting graph).
 */
class CypherMergeSetFoldingOracleTest extends TestHelper {
  private static final String ENDPOINTS = "MATCH (a:A {id: 1}), (b:A {id: 2}) ";

  @Override
  protected void beginTest() {
    database.transaction(() -> database.command("opencypher", "CREATE (:A {id: 1}), (:A {id: 2}), (:A {id: 3})"));
  }

  @Test
  void onCreateSetRunsBeforeTheSetAndOnlyForACreatedRelationship() {
    final String merge = ENDPOINTS + "MERGE (a)-[r:R {k: 1}]->(b) ON CREATE SET r.c = 1 SET r.d = coalesce(r.d, 0) + 2 RETURN r.c AS c, r.d AS d";
    assertThat(run(merge, Map.of())).containsExactly(Map.of("c", 1L, "d", 2L));
    assertThat(run(merge, Map.of())).containsExactly(Map.of("c", 1L, "d", 4L));
    assertThat(count("MATCH ()-[r:R]->() RETURN count(r) AS c")).isEqualTo(1L);
  }

  @Test
  void aSetThatRewritesAKeyOfThePatternHidesTheRelationshipFromTheNextMerge() {
    final String merge = ENDPOINTS + "MERGE (a)-[r:R {k: 1}]->(b) SET r.k = 2 RETURN r.k AS k";
    assertThat(run(merge, Map.of())).containsExactly(Map.of("k", 2L));
    assertThat(run(merge, Map.of())).containsExactly(Map.of("k", 2L));
    assertThat(count("MATCH ()-[r:R]->() RETURN count(r) AS c")).isEqualTo(2L);
  }

  @Test
  void aMergedMapLiteralNamingAKeyOfThePatternOverwritesIt() {
    final String merge = ENDPOINTS + "MERGE (a)-[r:R {k: 1}]->(b) SET r += {k: 5, z: 1} RETURN r.k AS k, r.z AS z";
    assertThat(run(merge, Map.of())).containsExactly(Map.of("k", 5L, "z", 1L));
    assertThat(run(merge, Map.of())).containsExactly(Map.of("k", 5L, "z", 1L));
    assertThat(count("MATCH ()-[r:R]->() RETURN count(r) AS c")).isEqualTo(2L);
  }

  @Test
  void aReplacedMapLiteralRemovesTheKeysOfThePattern() {
    final String merge = ENDPOINTS + "MERGE (a)-[r:R {k: 1}]->(b) SET r = {x: 1} RETURN r.k AS k, r.x AS x";
    final Map<String, Object> expected = new HashMap<>();
    expected.put("k", null);
    expected.put("x", 1L);
    assertThat(run(merge, Map.of())).containsExactly(expected);
    assertThat(run(merge, Map.of())).containsExactly(expected);
    assertThat(count("MATCH ()-[r:R]->() RETURN count(r) AS c")).isEqualTo(2L);
  }

  @Test
  void aRowMapThatDoesNotCarryTheKeyItWasMergedByDoesNotFindItsRelationshipAgain() {
    final String merge = "UNWIND $rows AS row " + ENDPOINTS + "MERGE (a)-[r:R {uuid: row.id}]->(b) SET r = row RETURN r.uuid AS u, r.id AS i";
    final Map<String, Object> params = Map.of("rows", List.of(Map.of("id", "x", "v", 1)));
    final Map<String, Object> expected = new HashMap<>();
    expected.put("u", null);
    expected.put("i", "x");
    assertThat(run(merge, params)).containsExactly(expected);
    assertThat(run(merge, params)).containsExactly(expected);
    assertThat(count("MATCH ()-[r:R]->() RETURN count(r) AS c")).isEqualTo(2L);
  }

  @Test
  void aRowMapThatCarriesTheKeyIsAnUpsertWhoseLastRowWinsForEveryRowOfTheStatement() {
    final String merge = "UNWIND $rows AS row " + ENDPOINTS + "MERGE (a)-[r:R {uuid: row.uuid}]->(b) SET r = row RETURN r.uuid AS u, r.v AS v";
    final List<Map<String, Object>> rows = new ArrayList<>();
    rows.add(Map.of("uuid", "x", "v", 1));
    rows.add(Map.of("uuid", "x", "v", 2));
    final Map<String, Object> nullValue = new HashMap<>();
    nullValue.put("uuid", "y");
    nullValue.put("v", null);
    rows.add(nullValue);
    final Map<String, Object> y = new HashMap<>();
    y.put("u", "y");
    y.put("v", null);
    for (int i = 0; i < 2; i++)
      assertThat(run(merge, Map.of("rows", rows))).containsExactly(Map.of("u", "x", "v", 2L), Map.of("u", "x", "v", 2L), y);
    assertThat(count("MATCH ()-[r:R]->() RETURN count(r) AS c")).isEqualTo(2L);
    assertThat(count("MATCH ()-[r:R {uuid: 'x'}]->() WHERE r.v = 2 RETURN count(r) AS c")).isEqualTo(1L);
    assertThat(count("MATCH ()-[r:R {uuid: 'y'}]->() WHERE r.v IS NULL RETURN count(r) AS c")).isEqualTo(1L);
  }

  @Test
  void aReplacementMapOfAnotherVariableDropsTheKeyOfThePatternAfterTheMergeOfEveryRowHasRun() {
    final String merge = "UNWIND $rows AS row " + ENDPOINTS + "MERGE (a)-[r:R {uuid: row.uuid}]->(b) SET r = row.other RETURN r.uuid AS u, r.w AS w";
    final Map<String, Object> last = new HashMap<>();
    last.put("u", null);
    last.put("w", 2L);
    // both rows MERGE before either SET runs (the SET rewrites the property the MERGE reads), so the second finds the first's relationship
    assertThat(run(merge, Map.of("rows", List.of(Map.of("uuid", "x", "other", Map.of("w", 1)), Map.of("uuid", "x", "other", Map.of("w", 2))))))
        .containsExactly(last, last);
    assertThat(count("MATCH ()-[r:R]->() RETURN count(r) AS c")).isEqualTo(1L);
  }

  @Test
  void aReplacementMapOfAnotherVariableHidesTheRelationshipFromTheNextStatement() {
    final String merge = "UNWIND $rows AS row " + ENDPOINTS + "MERGE (a)-[r:R {uuid: row.uuid}]->(b) SET r = row.other RETURN r.w AS w";
    assertThat(run(merge, Map.of("rows", List.of(Map.of("uuid", "x", "other", Map.of("w", 1)))))).containsExactly(Map.of("w", 1L));
    assertThat(run(merge, Map.of("rows", List.of(Map.of("uuid", "x", "other", Map.of("w", 2)))))).containsExactly(Map.of("w", 2L));
    assertThat(count("MATCH ()-[r:R]->() RETURN count(r) AS c")).isEqualTo(2L);
    assertThat(count("MATCH ()-[r:R]->() WHERE r.uuid IS NOT NULL RETURN count(r) AS c")).isZero();
  }

  @Test
  void aSetThatReadsThePatternVariablesSeesTheMatchedOrCreatedEntities() {
    final String merge = "UNWIND $rows AS row " + ENDPOINTS
        + "MERGE p = (a)-[r:R {uuid: row.uuid}]->(b) SET r.src = a.id, r.dst = b.id, r.len = length(p) RETURN r.src AS s, r.dst AS d, r.len AS l";
    for (int i = 0; i < 2; i++)
      assertThat(run(merge, Map.of("rows", List.of(Map.of("uuid", "x"))))).containsExactly(Map.of("s", 1L, "d", 2L, "l", 1L));
    assertThat(count("MATCH ()-[r:R]->() RETURN count(r) AS c")).isEqualTo(1L);
  }

  @Test
  void everyItemOfOneSetClauseReadsTheRelationshipAsItWasBeforeTheClause() {
    final String merge = ENDPOINTS + "MERGE (a)-[r:R {k: 1}]->(b) SET r.n = coalesce(r.n, 0) + 1, r.m = coalesce(r.n, 0) * 10 RETURN r.n AS n, r.m AS m";
    // every item of one SET clause reads the entity as it was before the clause: m is ten times the n of the previous run
    for (long i = 1; i <= 3; i++)
      assertThat(run(merge, Map.of())).containsExactly(Map.of("n", i, "m", (i - 1) * 10));
    assertThat(count("MATCH ()-[r:R]->() RETURN count(r) AS c")).isEqualTo(1L);
  }

  @Test
  void aSetToNullRemovesThePropertyAndAMergedNullIsNotStored() {
    database.transaction(() -> database.command("opencypher", ENDPOINTS + "MERGE (a)-[r:R {k: 1}]->(b) SET r.x = 5"));
    final Map<String, Object> nothing = new HashMap<>();
    nothing.put("x", null);
    assertThat(run(ENDPOINTS + "MERGE (a)-[r:R {k: 1}]->(b) SET r.x = null RETURN r.x AS x", Map.of())).containsExactly(nothing);
    assertThat(run("MATCH ()-[r:R]->() RETURN keys(r) AS ks", Map.of())).containsExactly(Map.of("ks", List.of("k")));

    final Map<String, Object> params = new HashMap<>();
    final Map<String, Object> map = new HashMap<>();
    map.put("k", 9);
    map.put("z", null);
    params.put("m", map);
    final Map<String, Object> expected = new HashMap<>();
    expected.put("k", 9L);
    expected.put("z", null);
    final String merge = ENDPOINTS + "MERGE (a)-[r:R {k: 1}]->(b) SET r += $m RETURN r.k AS k, r.z AS z";
    assertThat(run(merge, params)).containsExactly(expected);
    assertThat(count("MATCH ()-[r:R]->() RETURN count(r) AS c")).isEqualTo(1L);
  }

  @Test
  void aNodeSetAfterOnCreateSetAppliesToCreatedAndMatchedNodesAndTheReturnReadsTheFinalValue() {
    final String merge = "UNWIND $rows AS row MERGE (n:N {k: row.k}) ON CREATE SET n.c = 1 SET n.d = row.v RETURN n.k AS k, n.c AS c, n.d AS d";
    final Map<String, Object> third = new HashMap<>();
    third.put("k", 2L);
    third.put("c", 1L);
    third.put("d", null);
    final Map<String, Object> nullRow = new HashMap<>();
    nullRow.put("k", 2);
    nullRow.put("v", null);
    final Map<String, Object> params = Map.of("rows", List.of(Map.of("k", 1, "v", 1), Map.of("k", 1, "v", 2), nullRow));
    for (int i = 0; i < 2; i++)
      assertThat(run(merge, params)).containsExactly(Map.of("k", 1L, "c", 1L, "d", 2L), Map.of("k", 1L, "c", 1L, "d", 2L), third);
    assertThat(count("MATCH (n:N) RETURN count(n) AS c")).isEqualTo(2L);
  }

  @Test
  void aNodeReplacedByItsRowKeepsTheKeyAndLosesWhatTheRowLacks() {
    final String merge = "UNWIND $rows AS row MERGE (n:N {k: row.k}) SET n = row RETURN n.k AS k, n.v AS v";
    final Map<String, Object> nullRow = new HashMap<>();
    nullRow.put("k", 1);
    nullRow.put("v", null);
    final Map<String, Object> afterNull = new HashMap<>();
    afterNull.put("k", 1L);
    afterNull.put("v", null);
    final Map<String, Object> bare = new HashMap<>();
    bare.put("k", 2L);
    bare.put("v", null);
    assertThat(run(merge, Map.of("rows", List.of(Map.of("k", 1, "v", 1), nullRow, Map.of("k", 2))))).containsExactly(afterNull, afterNull,
        bare);
    assertThat(count("MATCH (n:N) WHERE n.v IS NOT NULL RETURN count(n) AS c")).isZero();
    assertThat(count("MATCH (n:N) RETURN count(n) AS c")).isEqualTo(2L);
  }

  @Test
  void aNodeSetThatRewritesTheKeyMovesTheNodeOutOfTheNextMerge() {
    final String merge = "UNWIND $rows AS row MERGE (n:N {k: row.k}) SET n.k = row.k2 RETURN n.k AS k";
    final List<Map<String, Object>> rows = List.of(Map.of("k", 1, "k2", 5), Map.of("k", 1, "k2", 6), Map.of("k", 5, "k2", 7));
    // the SET rewrites the key the MERGE reads, so all three MERGEs run first: rows 1 and 2 are the same node (renamed 5, then 6), row 3 creates the
    // node it renames 7
    assertThat(run(merge, Map.of("rows", rows))).containsExactly(Map.of("k", 6L), Map.of("k", 6L), Map.of("k", 7L));
    assertThat(count("MATCH (n:N {k: 6}) RETURN count(n) AS c")).isEqualTo(1L);
    assertThat(count("MATCH (n:N {k: 7}) RETURN count(n) AS c")).isEqualTo(1L);
    assertThat(count("MATCH (n:N) RETURN count(n) AS c")).isEqualTo(2L);
  }

  @Test
  void aSetOnAMultiHopMergeAppliesToEveryRelationshipOfThePath() {
    final String merge = "MATCH (a:A {id: 1}), (b:A {id: 2}), (c:A {id: 3}) MERGE (a)-[r:R {k: 1}]->(b)<-[s:S {k: 2}]-(c) "
        + "SET r.x = coalesce(r.x, 0) + 1, s.y = 2 RETURN r.x AS x, s.y AS y";
    assertThat(run(merge, Map.of())).containsExactly(Map.of("x", 1L, "y", 2L));
    assertThat(run(merge, Map.of())).containsExactly(Map.of("x", 2L, "y", 2L));
    assertThat(count("MATCH ()-[r:R]->() RETURN count(r) AS c")).isEqualTo(1L);
    assertThat(count("MATCH ()-[s:S]->() RETURN count(s) AS c")).isEqualTo(1L);
  }

  @Test
  void aReplacementMapHeldByAnotherVariableDropsTheKeyWhileTheKeysOwnVariableKeepsIt() {
    final String other = "WITH {uuid: 'x', v: 1} AS row, {v: 2} AS other " + ENDPOINTS
        + "MERGE (a)-[r:R {uuid: row.uuid}]->(b) SET r = other RETURN r.uuid AS u, r.v AS v";
    final Map<String, Object> dropped = new HashMap<>();
    dropped.put("u", null);
    dropped.put("v", 2L);
    assertThat(run(other, Map.of())).containsExactly(dropped);
    assertThat(run(other, Map.of())).containsExactly(dropped);
    assertThat(count("MATCH ()-[r:R]->() RETURN count(r) AS c")).isEqualTo(2L);

    final String same = "WITH {uuid: 'z', v: 1} AS row " + ENDPOINTS + "MERGE (a)-[r:R {uuid: row.uuid}]->(b) SET r = row RETURN r.uuid AS u, r.v AS v";
    assertThat(run(same, Map.of())).containsExactly(Map.of("u", "z", "v", 1L));
    assertThat(run(same, Map.of())).containsExactly(Map.of("u", "z", "v", 1L));
    assertThat(count("MATCH ()-[r:R]->() RETURN count(r) AS c")).isEqualTo(3L);
  }

  @Test
  void mergeThroughALeadingPropertyOfACompositeIndexMatchesEveryNodeOfThePrefix() {
    database.command("sql", "CREATE VERTEX TYPE T");
    database.command("sql", "CREATE PROPERTY T.a INTEGER");
    database.command("sql", "CREATE PROPERTY T.b INTEGER");
    database.command("sql", "CREATE INDEX ON T (a, b) NOTUNIQUE");
    database.transaction(() -> database.command("opencypher", "CREATE (:T {a: 1, b: 2}), (:T {a: 1}), (:T {a: 1, b: 2}), (:T {a: 2, b: 3}), (:T {b: 5})"));

    final Map<String, Object> noB = new HashMap<>();
    noB.put("a", 1L);
    noB.put("b", null);
    assertThat(run("MERGE (n:T {a: 1}) RETURN n.a AS a, n.b AS b ORDER BY b", Map.of())).hasSize(3).contains(Map.of("a", 1L, "b", 2L), noB);
    assertThat(run("MERGE (n:T {a: 1, b: 2}) RETURN n.a AS a, n.b AS b", Map.of())).containsExactly(Map.of("a", 1L, "b", 2L), Map.of("a", 1L, "b", 2L));
    assertThat(count("MATCH (n:T) RETURN count(n) AS c")).isEqualTo(5L);

    // a prefix or a whole key that nothing holds creates one node, and a property the index does not lead with is found by scan
    assertThat(run("MERGE (n:T {a: 9}) RETURN n.a AS a, n.b AS b", Map.of())).hasSize(1);
    assertThat(run("MERGE (n:T {a: 1, b: 7}) RETURN n.a AS a, n.b AS b", Map.of())).containsExactly(Map.of("a", 1L, "b", 7L));
    final Map<String, Object> noA = new HashMap<>();
    noA.put("a", null);
    noA.put("b", 5L);
    assertThat(run("MERGE (n:T {b: 5}) RETURN n.a AS a, n.b AS b", Map.of())).containsExactly(noA);
    assertThat(count("MATCH (n:T) RETURN count(n) AS c")).isEqualTo(7L);
  }

  @Test
  void mergeThroughAUniqueCompositeIndexFindsTheWholeKeyAndTheNullableRemainder() {
    database.command("sql", "CREATE VERTEX TYPE T");
    database.command("sql", "CREATE PROPERTY T.a INTEGER");
    database.command("sql", "CREATE PROPERTY T.b INTEGER");
    database.command("sql", "CREATE INDEX ON T (a, b) UNIQUE");
    database.transaction(() -> database.command("opencypher", "CREATE (:T {a: 1, b: 2}), (:T {a: 2, b: 3}), (:T {a: 1}), (:T {b: 5})"));

    final Map<String, Object> noB = new HashMap<>();
    noB.put("a", 1L);
    noB.put("b", null);
    assertThat(run("MERGE (n:T {a: 1}) RETURN n.a AS a, n.b AS b ORDER BY b", Map.of())).hasSize(2).contains(Map.of("a", 1L, "b", 2L), noB);
    assertThat(run("MERGE (n:T {a: 1, b: 2}) RETURN n.a AS a, n.b AS b", Map.of())).containsExactly(Map.of("a", 1L, "b", 2L));
    assertThat(run("MERGE (n:T {a: 1, b: 7}) RETURN n.a AS a, n.b AS b", Map.of())).containsExactly(Map.of("a", 1L, "b", 7L));
    assertThat(count("MATCH (n:T) RETURN count(n) AS c")).isEqualTo(5L);
  }

  private List<Map<String, Object>> run(final String query, final Map<String, Object> params) {
    final List<Map<String, Object>> rows = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", query, params)) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          final Map<String, Object> row = new HashMap<>();
          for (final String name : r.getPropertyNames())
            row.put(name, widen(r.getProperty(name)));
          rows.add(row);
        }
      }
    });
    return rows;
  }

  /** Cypher has one integer type: a value stored from an Integer parameter reads back as an Integer. */
  private static Object widen(final Object value) {
    return value instanceof Integer i ? Long.valueOf(i) : value;
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }
}
