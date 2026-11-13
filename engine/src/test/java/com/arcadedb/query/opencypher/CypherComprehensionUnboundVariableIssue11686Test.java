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
