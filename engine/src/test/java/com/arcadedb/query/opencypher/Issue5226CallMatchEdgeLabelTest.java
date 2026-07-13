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
 * Regression tests for issue #5226: in the new scoped subquery syntax
 * {@code CALL () { ... }} / {@code CALL (vars) { ... }}, an inner
 * {@code MATCH (n:Label)} must match only vertices. When the label name collides with an
 * existing edge type, the O(1) type-count optimization ({@code TypeCountStep}) wrongly counted
 * edges, so {@code count(n)} returned the edge count instead of {@code 0}. Labels and
 * relationship types are separate namespaces in Cypher; Neo4j returns {@code 0} for all variants
 * below.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5226CallMatchEdgeLabelTest extends TestHelper {
  @Override
  protected void beginTest() {
    database.command("opencypher", "CREATE (a:A {v: 1}), (b:B {v: 2}), (a)-[:EdgeLabel]->(b)");
  }

  private long count(final String cypher) {
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      return ((Number) row.getProperty("cnt")).longValue();
    }
  }

  /** Control: direct MATCH on an edge-type label matches no vertices. */
  @Test
  void directMatchOnEdgeLabelReturnsZero() {
    assertThat(count("MATCH (b:EdgeLabel) RETURN count(b) AS cnt")).isEqualTo(0);
  }

  /** Control: COUNT subquery on an edge-type label returns zero. */
  @Test
  void countSubqueryOnEdgeLabelReturnsZero() {
    assertThat(count("RETURN COUNT { MATCH (b:EdgeLabel) } AS cnt")).isEqualTo(0);
  }

  /** The reporter's failing query: {@code CALL ()} with zero imported variables. */
  @Test
  void scopedCallNoImportOnEdgeLabelReturnsZero() {
    assertThat(count(
        "CALL () { MATCH (b:EdgeLabel) RETURN count(b) AS cnt } RETURN cnt")).isEqualTo(0);
  }

  /** {@code CALL (a)} importing an outer variable, inner var name matches the count arg. */
  @Test
  void scopedCallWithImportOnEdgeLabelReturnsZero() {
    assertThat(count(
        "MATCH (a:A) CALL (a) { MATCH (b:EdgeLabel) RETURN count(b) AS cnt } RETURN cnt"))
        .isEqualTo(0);
  }

  /** Different inner variable name must not change the result. */
  @Test
  void scopedCallWithImportDifferentInnerVarReturnsZero() {
    assertThat(count(
        "MATCH (a:A) CALL (a) { MATCH (x:EdgeLabel) RETURN count(x) AS cnt } RETURN cnt"))
        .isEqualTo(0);
  }

  /** Control: deprecated {@code CALL { WITH ... }} syntax already returns zero. */
  @Test
  void importingWithCallOnEdgeLabelReturnsZero() {
    assertThat(count(
        "MATCH (a:A) CALL { WITH a MATCH (b:EdgeLabel) RETURN count(b) AS cnt } RETURN cnt"))
        .isEqualTo(0);
  }

  /** Control: OPTIONAL MATCH in the scoped CALL already returns zero. */
  @Test
  void scopedCallOptionalMatchOnEdgeLabelReturnsZero() {
    assertThat(count(
        "MATCH (a:A) CALL (a) { OPTIONAL MATCH (b:EdgeLabel) RETURN count(b) AS cnt } RETURN cnt"))
        .isEqualTo(0);
  }

  /** The labels() probe confirms no vertex is matched inside the scoped CALL. */
  @Test
  void scopedCallMatchOnEdgeLabelYieldsNoRows() {
    try (final ResultSet rs = database.query("opencypher",
        "CALL () { MATCH (b:EdgeLabel) RETURN labels(b) AS lbls } RETURN lbls")) {
      assertThat(rs.hasNext()).isFalse();
    }
  }

  /** Sanity: the same optimization still counts real vertex labels correctly inside a scoped CALL. */
  @Test
  void scopedCallMatchOnVertexLabelCountsVertices() {
    assertThat(count(
        "CALL () { MATCH (n:A) RETURN count(n) AS cnt } RETURN cnt")).isEqualTo(1);
  }
}
