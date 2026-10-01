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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issues #8826 and #8827: a no-op boundary (a {@code CALL (*) { RETURN 0 }} or an empty {@code OPTIONAL MATCH}) between a
 * write clause and a {@code LIMIT} must not change what the write does to the graph.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8826Issue8827CypherWriteBoundaryTest extends TestHelper {

  private long count(final String cypher) {
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private void drain(final String cypher) {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", cypher)) {
        while (rs.hasNext())
          rs.next();
      }
    });
  }

  private void setup8826() {
    drain("CREATE (b:B {k: 'x'}), (a1:A {id: 1}), (a2:A {id: 2}), (c:C {id: 3}), (b)-[:R]->(a1), (b)-[:R]->(a2), (b)-[:S]->(c)");
    drain("UNWIND range(1, 10) AS i CREATE (:D {id: i}), (f:F {id: i})-[:T]->(e:E {id: i})");
  }

  private static final String MATCH_8826 = "MATCH p0 = TRAIL (n0) <-[]- (n1:B {k: 'x'}) -[:S]-> (x:C), (n2:D), (n3:E) <-[:T]- (y:F) "
      + "WHERE n1.k =~ '.*' FOREACH (i IN range(0, 9) | CREATE (:Z {k: i})) WITH n0, n1, n2, n3, p0 LIMIT 1 ";

  @Test
  void noOpCallDoesNotChangeForeachCardinality() {
    setup8826();
    drain(MATCH_8826 + "RETURN n0, n1, n2, n3, p0");
    final long control = count("MATCH (z:Z) RETURN count(z) AS c");
    drain("MATCH (n) DETACH DELETE n");
    setup8826();

    drain(MATCH_8826 + "WITH n0, n1, n2, n3, p0 CALL (*) { RETURN 0 AS barrier } WITH n0, n1, n2, n3, p0 RETURN n0, n1, n2, n3, p0");
    final long withCall = count("MATCH (z:Z) RETURN count(z) AS c");
    assertThat(withCall).isEqualTo(control);
  }

  @Test
  void foreachRunsForEveryRowBeforeLimit() {
    setup8826();
    final long rows = count("MATCH p0 = TRAIL (n0) <-[]- (n1:B {k: 'x'}) -[:S]-> (x:C), (n2:D), (n3:E) <-[:T]- (y:F) RETURN count(*) AS c");
    drain(MATCH_8826 + "RETURN n0");
    assertThat(count("MATCH (z:Z) RETURN count(z) AS c")).isEqualTo(rows * 10);
  }

  private void setup8827() {
    drain("UNWIND range(1, 5) AS i CREATE (a:A {id: i})-[:R]->(b:B {id: i}), (p:P {k: 0})-[:S]->(q:Q {id: i}), (m:M {k: 'm'})-[:T]->(z:Z {k: 'z'})");
  }

  private static final String HEAD_8827 = "OPTIONAL MATCH (n0:A)-[r0:R]->(), p0 = (p:P {k: 0})-[:S]->(), p1 = (m:M {k: 'm'})-[:T]->({k: 'z'}) "
      + "WHERE n0.id IS NOT NULL AND p0 IS NOT NULL AND p1 IS NOT NULL ";
  private static final String CALL_8827 = "CALL (n0, p0, p1) { SET n0.k = NULL DETACH DELETE n0 RETURN DISTINCT max('pma') AS alias0 } ";

  @Test
  void emptyOptionalMatchDoesNotChangeDetachDeleteState() {
    setup8827();
    drain(HEAD_8827 + "WITH * WHERE n0 IS NOT NULL AND p0 IS NOT NULL AND p1 IS NOT NULL " + CALL_8827 + "WITH alias0 LIMIT 1 RETURN alias0");
    final long control = count("MATCH (n) RETURN count(n) AS c");
    final long controlEdges = count("MATCH ()-[r]->() RETURN count(r) AS c");
    drain("MATCH (n) DETACH DELETE n");

    setup8827();
    final String barrier = "OPTIONAL MATCH (__barrier:NoSuchType) WHERE false ";
    drain(HEAD_8827 + barrier + "WITH n0, r0, p0, p1 WITH * WHERE n0 IS NOT NULL AND p0 IS NOT NULL AND p1 IS NOT NULL " + barrier
        + "WITH n0, r0, p0, p1 " + CALL_8827 + barrier + "WITH alias0 WITH alias0 LIMIT 1 RETURN alias0");
    assertThat(count("MATCH (n) RETURN count(n) AS c")).isEqualTo(control).isEqualTo(25L);
    assertThat(count("MATCH ()-[r]->() RETURN count(r) AS c")).isEqualTo(controlEdges).isEqualTo(10L);
  }

  @Test
  void createRunsForEveryRowBeforeFinalLimit() {
    drain("UNWIND range(1, 500) AS i CREATE (n:Item {i: i}) RETURN n LIMIT 1");
    assertThat(count("MATCH (n:Item) RETURN count(n) AS c")).isEqualTo(500L);
  }

  @Test
  void createRunsForEveryRowBeforeWithLimit() {
    drain("UNWIND range(1, 500) AS i CREATE (n:Item {i: i}) WITH n LIMIT 1 RETURN n");
    assertThat(count("MATCH (n:Item) RETURN count(n) AS c")).isEqualTo(500L);
  }

  private void seedItems() {
    drain("UNWIND range(1, 500) AS i CREATE (:Src {i: i})");
  }

  @Test
  void labeledMatchCreateBeforeReturnLimit() {
    seedItems();
    drain("MATCH (a:Src) CREATE (:Out {i: a.i}) RETURN a LIMIT 1");
    assertThat(count("MATCH (o:Out) RETURN count(o) AS c")).isEqualTo(500L);
  }

  @Test
  void labeledMatchCreateBeforeWithLimit() {
    seedItems();
    drain("MATCH (a:Src) CREATE (:Out {i: a.i}) WITH a LIMIT 1 RETURN a");
    assertThat(count("MATCH (o:Out) RETURN count(o) AS c")).isEqualTo(500L);
  }

  @Test
  void labeledMatchSetBeforeWithLimit() {
    seedItems();
    drain("MATCH (a:Src) SET a.seen = true WITH a LIMIT 1 RETURN a");
    assertThat(count("MATCH (a:Src) WHERE a.seen = true RETURN count(a) AS c")).isEqualTo(500L);
  }

  @Test
  void labeledMatchDeleteBeforeWithLimit() {
    seedItems();
    drain("MATCH (a:Src) DELETE a WITH 1 AS x LIMIT 1 RETURN x");
    assertThat(count("MATCH (a:Src) RETURN count(a) AS c")).isZero();
  }

  @Test
  void mergeBeforeSkipAndLimit() {
    drain("UNWIND range(1, 500) AS i MERGE (:Mg {i: i}) WITH i SKIP 1 LIMIT 1 RETURN i");
    assertThat(count("MATCH (a:Mg) RETURN count(a) AS c")).isEqualTo(500L);
  }

  @Test
  void removeBeforeWithLimit() {
    drain("UNWIND range(1, 500) AS i CREATE (:Rm {i: i, t: 1})");
    drain("MATCH (a:Rm) REMOVE a.t WITH a LIMIT 1 RETURN a");
    assertThat(count("MATCH (a:Rm) WHERE a.t IS NULL RETURN count(a) AS c")).isEqualTo(500L);
  }

  @Test
  void limitZeroAfterWriteStillWrites() {
    drain("UNWIND range(1, 500) AS i CREATE (:Lz {i: i}) WITH i LIMIT 0 RETURN i");
    assertThat(count("MATCH (a:Lz) RETURN count(a) AS c")).isEqualTo(500L);
  }

  @Test
  void readOnlyLimitKeepsStreaming() {
    seedItems();
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN MATCH (a:Src) RETURN a LIMIT 1")) {
      assertThat(rs.next().toString()).doesNotContain("EagerStep");
    }
  }

  @Test
  void standaloneForeachBeforeWithAndReturnLimit() {
    seedItems();
    drain("MATCH (a:Src) FOREACH (i IN [1, 2] | CREATE (:Fe)) WITH a LIMIT 1 RETURN a");
    assertThat(count("MATCH (f:Fe) RETURN count(f) AS c")).isEqualTo(1000L);
    drain("MATCH (a:Src) FOREACH (i IN [1] | CREATE (:Fr)) RETURN a LIMIT 1");
    assertThat(count("MATCH (f:Fr) RETURN count(f) AS c")).isEqualTo(500L);
  }

  @Test
  void writingSubqueryBeforeLimit() {
    seedItems();
    drain("MATCH (a:Src) CALL (a) { CREATE (:Sq {i: a.i}) RETURN 1 AS one } WITH a LIMIT 1 RETURN a");
    assertThat(count("MATCH (s:Sq) RETURN count(s) AS c")).isEqualTo(500L);
  }

  @Test
  void orderByAndAggregationLimitStillWriteForEveryRow() {
    seedItems();
    drain("MATCH (a:Src) CREATE (:Ob {i: a.i}) WITH a ORDER BY a.i LIMIT 1 RETURN a");
    assertThat(count("MATCH (o:Ob) RETURN count(o) AS c")).isEqualTo(500L);
    drain("MATCH (a:Src) CREATE (:Ag {i: a.i}) RETURN count(a) AS n LIMIT 1");
    assertThat(count("MATCH (o:Ag) RETURN count(o) AS c")).isEqualTo(500L);
  }

  @Test
  void writeBeforeLimitPlansTheBarrier() {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN MATCH (a:Src) SET a.x = 1 RETURN a LIMIT 1")) {
      assertThat(rs.next().toString()).contains("EagerStep");
    }
  }

  @Test
  void parameterLimitAfterWriteKeepsTheFirstRowsAndWritesAll() {
    seedItems();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", "MATCH (a:Src) CREATE (:Out {i: a.i}) WITH a SKIP 2 LIMIT $n RETURN a.i AS i",
          Map.of("n", 3))) {
        int rows = 0;
        while (rs.hasNext()) {
          rs.next();
          rows++;
        }
        assertThat(rows).isEqualTo(3);
      }
    });
    assertThat(count("MATCH (o:Out) RETURN count(o) AS c")).isEqualTo(500L);
  }

  @Test
  void filteredLimitAfterWriteStillReturnsMatchingRows() {
    seedItems();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", "MATCH (a:Src) CREATE (:Out {i: a.i}) WITH a WHERE a.i > 400 LIMIT 5 RETURN a.i AS i")) {
        int rows = 0;
        while (rs.hasNext()) {
          rs.next();
          rows++;
        }
        assertThat(rows).isEqualTo(5);
      }
    });
    assertThat(count("MATCH (o:Out) RETURN count(o) AS c")).isEqualTo(500L);
  }

  @Test
  void repeatedLimitsAfterWriteStillWriteForEveryRow() {
    drain("UNWIND range(1, 500) AS i CREATE (:Lz {i: i}) WITH i LIMIT 1 WITH i LIMIT 0 RETURN i");
    assertThat(count("MATCH (a:Lz) RETURN count(a) AS c")).isEqualTo(500L);
  }
}
