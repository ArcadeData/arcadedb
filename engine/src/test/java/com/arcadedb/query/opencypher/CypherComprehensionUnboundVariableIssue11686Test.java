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
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11686: an unbound variable in the projection or WHERE of a pattern comprehension, or in the operand of a list
 * slice, evaluated to null instead of being refused like a top-level {@code RETURN p}.
 */
class CypherComprehensionUnboundVariableIssue11686Test extends TestHelper {
  @Override
  protected void beginTest() {
    database.transaction(() -> database.command("opencypher",
        "CREATE (a:Node {id:1})-[:R {k:1}]->(b:Node {id:2})-[:R {k:2}]->(c:Node {id:3})"));
  }

  @Test
  void topLevelUnboundVariableIsRejected() {
    assertUnbound("RETURN p AS v", "p");
  }

  @Test
  void patternComprehensionProjectionUnboundVariableIsRejected() {
    assertUnbound("RETURN [(x)-[:R]->(y) | p] AS v", "p");
  }

  @Test
  void patternComprehensionWithBoundStartUnboundProjectionIsRejected() {
    assertUnbound("MATCH (a:Node {id:1}) RETURN [(a)-[:R]->(b) | p] AS v", "p");
  }

  @Test
  void patternComprehensionWhereUnboundVariableIsRejected() {
    assertUnbound("RETURN [(x)-[:R]->(y) WHERE q.k = 1 | y] AS v", "q");
  }

  @Test
  void nestedPatternComprehensionUnboundProjectionIsRejected() {
    assertUnbound("RETURN [p IN [(x)-[:R]->(y) | p] | p] AS v", "p");
  }

  @Test
  void outerListComprehensionVariableDoesNotLeakIntoPatternComprehensionSource() {
    assertUnbound("RETURN [p IN [(x)-[:R]->(y) | p] | 1] AS v", "p");
  }

  @Test
  void listSliceUnboundOperandIsRejected() {
    assertUnbound("RETURN l[0..3] AS slice", "l");
  }

  @Test
  void listSliceUnboundBoundIsRejected() {
    assertUnbound("RETURN [1, 2, 3][0..n] AS slice", "n");
  }

  @Test
  void listComprehensionSelfReferenceIsRejected() {
    assertUnbound("RETURN [p IN [p] | p] AS v", "p");
  }

  @Test
  void patternComprehensionOverConstantProjectionStillWorks() {
    final List<Object> v = single("RETURN [p IN [(x)-[:R]->(y) | 1] | p] AS v");
    assertThat(v).containsExactly(1L, 1L);
  }

  @Test
  void patternComprehensionProjectingPatternVariableStillWorks() {
    final List<Object> v = single("RETURN [p IN [(x)-[:R]->(y) | y] | p] AS v");
    assertThat(v).hasSize(2);
  }

  @Test
  void patternComprehensionPathVariableAndRelationshipVariableAreInScope() {
    final List<Object> v = single("MATCH (a:Node {id:1}) RETURN [path = (a)-[r:R]->(b) WHERE r.k = 1 | length(path) + b.id] AS v");
    assertThat(v).containsExactly(3L);
  }

  @Test
  void listSliceOfBoundVariableStillWorks() {
    final List<Object> v = single("WITH [1, 2, 3, 4] AS l RETURN l[0..3] AS v");
    assertThat(v).containsExactly(1L, 2L, 3L);
  }

  @Test
  void patternComprehensionUsingWithCarriedAliasStillWorks() {
    final List<Object> v = single(
        "MATCH (a:Node {id:1}) WITH a AS s RETURN [(s)-[:R]->(b) | b.id] AS v");
    assertThat(v).hasSize(1);
  }

  @Test
  void patternComprehensionInWithUsesOuterVariable() {
    final List<Object> v = single(
        "MATCH (a:Node {id:1}) WITH a, [(a)-[:R]->(b) | b.id] AS ids RETURN ids AS v");
    assertThat(v).hasSize(1);
  }

  @Test
  void patternComprehensionInWithUnboundProjectionIsRejected() {
    assertUnbound("MATCH (a:Node {id:1}) WITH [(a)-[:R]->(b) | p] AS ids RETURN ids AS v", "p");
  }

  @Test
  void patternComprehensionInMatchWhereUsesOuterVariable() {
    final List<Object> v = single(
        "MATCH (a:Node) WHERE size([(a)-[:R]->(b) | b]) > 0 RETURN collect(a.id) AS v");
    assertThat(v).hasSize(2);
  }

  @Test
  void patternComprehensionInMatchWhereUnboundProjectionIsRejected() {
    assertUnbound("MATCH (a:Node) WHERE size([(a)-[:R]->(b) | p]) > 0 RETURN a AS v", "p");
  }

  @Test
  void patternComprehensionInOrderByUsesOuterVariable() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:Node) RETURN a.id AS v ORDER BY size([(a)-[:R]->(b) | b]) DESC, a.id")) {
      assertThat(rs.next().<Number>getProperty("v").longValue()).isEqualTo(1L);
    }
  }

  @Test
  void patternComprehensionInOrderByUnboundProjectionIsRejected() {
    assertUnbound("MATCH (a:Node) RETURN a.id AS v ORDER BY size([(a)-[:R]->(b) | p])", "p");
  }

  /** The inline WHERE of a node or relationship stays on the comprehension's pattern and was not scope-checked. */
  @Test
  void inlineNodeAndRelationshipWhereUnboundVariableIsRejected() {
    assertUnbound("RETURN [(x:Node WHERE q.k = 1)-[:R]->(y) | y] AS v", "q");
    assertUnbound("RETURN [(x:Node)-[:R]->(y:Node WHERE q.k = 1) | y] AS v", "q");
    assertUnbound("RETURN [(x:Node)-[r:R WHERE q.k = 1]->(y) | y] AS v", "q");
    assertThat(single("RETURN [(x:Node WHERE x.id = 1)-[r:R WHERE r.k = 1]->(y WHERE y.id = 2) | y.id] AS v"))
        .extracting(id -> ((Number) id).intValue()).containsExactly(2);
  }

  /** A map projection's source variable and computed values were not scope-checked, so they evaluated to null. */
  @Test
  void mapProjectionUnboundVariableIsRejected() {
    assertUnbound("RETURN [(x)-[:R]->(y) | q{.id}] AS v", "q");
    assertUnbound("RETURN [(x)-[:R]->(y) | y{value: p}] AS v", "p");
    assertUnbound("RETURN [(x)-[:R]->(y) | y{p}] AS v", "p");
    assertUnbound("RETURN q{.id} AS v", "q");
    assertUnbound("MATCH (y:Node) RETURN y{.id, value: p} AS v", "p");
    final List<Object> projected = single("MATCH (a:Node {id: 1}) RETURN [(a)-[:R]->(y) | y{.id, from: a.id}] AS v");
    assertThat(projected).hasSize(1);
    final Map<?, ?> map = (Map<?, ?>) projected.getFirst();
    assertThat(((Number) map.get("id")).intValue()).isEqualTo(2);
    assertThat(((Number) map.get("from")).intValue()).isEqualTo(1);
  }

  private List<Object> single(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      final Result row = rs.next();
      return row.getProperty("v");
    }
  }

  private void assertUnbound(final String query, final String variable) {
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.query("opencypher", query)) {
        rs.next();
      }
    })
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("UndefinedVariable")
        .hasMessageContaining("'" + variable + "'");
  }
}
