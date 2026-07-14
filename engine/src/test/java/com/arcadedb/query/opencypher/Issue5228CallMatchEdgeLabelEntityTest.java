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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #5228: in the new scoped subquery syntax {@code CALL () { ... }} /
 * {@code CALL (vars) { ... }}, an inner {@code MATCH (b:Label)} must match only vertices, never
 * relationships. When the label name collides with an existing edge type, the reporter observed
 * that the pattern <em>materialized the edge entity</em> ({@code labels(b) = []},
 * {@code type(b) = "EdgeLabel"}) so {@code count(b)} returned the edge count instead of {@code 0}.
 * <p>
 * This is the same defect as issue #5226 (the O(1) type-count fast path counted edges) combined
 * with the general MATCH kind guard of issue #5194; both are fixed on main. Where the #5226 suite
 * asserts the aggregated {@code count(b)} and a {@code labels(b)} probe, this suite locks down the
 * reporter's remaining diagnostic angle: returning the raw entity {@code b} and probing
 * {@code type(b)} must yield <em>no rows at all</em>, for every {@code CALL} variant reported.
 * Labels and relationship types are separate namespaces in Cypher; Neo4j returns no rows here.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5228CallMatchEdgeLabelEntityTest extends TestHelper {
  @Override
  protected void beginTest() {
    // Two vertices (A, B) and one edge whose type name is "EdgeLabel"; zero vertices are labeled EdgeLabel.
    database.command("opencypher", "CREATE (a:A {v: 1}), (b:B {v: 2}), (a)-[:EdgeLabel]->(b)");
  }

  private int rowCount(final String cypher) {
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      int rows = 0;
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
      return rows;
    }
  }

  private long count(final String cypher) {
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      return ((Number) row.getProperty("cnt")).longValue();
    }
  }

  /** The reporter's primary reproduction: the count must be 0, not the number of edges. */
  @Test
  void scopedCallNoImportCountReturnsZero() {
    assertThat(count("CALL () { MATCH (b:EdgeLabel) RETURN count(b) AS cnt } RETURN cnt")).isEqualTo(0);
  }

  /** Returning the raw entity must yield no rows (nothing to materialize). */
  @Test
  void scopedCallNoImportReturningEntityYieldsNoRows() {
    assertThat(rowCount("CALL () { MATCH (b:EdgeLabel) RETURN b AS b } RETURN b")).isEqualTo(0);
  }

  /** The reporter's diagnostic probe: labels()/type() must not surface any relationship. */
  @Test
  void scopedCallNoImportTypeAndLabelsProbeYieldsNoRows() {
    assertThat(rowCount(
        "CALL () { MATCH (b:EdgeLabel) RETURN labels(b) AS lbls, type(b) AS t } RETURN lbls, t")).isEqualTo(0);
  }

  /** {@code CALL (a)} importing an outer variable, returning the entity. */
  @Test
  void scopedCallWithImportReturningEntityYieldsNoRows() {
    assertThat(rowCount(
        "MATCH (a:A) CALL (a) { MATCH (b:EdgeLabel) RETURN b AS b } RETURN b")).isEqualTo(0);
  }

  /** Renamed inner variable must not change the result. */
  @Test
  void scopedCallWithImportRenamedVarReturningEntityYieldsNoRows() {
    assertThat(rowCount(
        "MATCH (a:A) CALL (a) { MATCH (x:EdgeLabel) RETURN x AS x } RETURN x")).isEqualTo(0);
  }

  /** Sanity: a real vertex label still materializes vertices inside a scoped CALL. */
  @Test
  void scopedCallOnVertexLabelStillReturnsVertices() {
    assertThat(rowCount("CALL () { MATCH (n:A) RETURN n AS n } RETURN n")).isEqualTo(1);
    assertThat(count("CALL () { MATCH (n:A) RETURN count(n) AS cnt } RETURN cnt")).isEqualTo(1);
  }
}
