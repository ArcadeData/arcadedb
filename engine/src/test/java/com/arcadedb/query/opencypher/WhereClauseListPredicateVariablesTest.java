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

import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.WhereClause;
import com.arcadedb.query.opencypher.parser.Cypher25AntlrParser;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for {@link WhereClause#collectVariables} with list predicates
 * (any/all/none/single) and list comprehensions.
 * <p>
 * These guard the fix from PR #3891: outer variables referenced inside list-predicate
 * and list-comprehension bodies must be collected, while the loop-scoped iterator
 * variable must not. The filter-pushdown machinery in the optimizer relies on this
 * to decide which predicates depend on which variables.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class WhereClauseListPredicateVariablesTest {
  private static final Cypher25AntlrParser PARSER = new Cypher25AntlrParser();

  @Test
  void anyPredicateInComparisonCollectsOuterVariable() {
    // any(...) wrapped in an explicit comparison reaches the ListPredicateExpression
    // branch of collectExpressionVariables: the outer variable 'p' must be collected.
    assertThat(collect("MATCH (p:Person) WHERE any(x IN ['Alice'] WHERE x = p.name) = true RETURN p"))
        .containsExactly("p");
  }

  @Test
  void allPredicateInComparisonCollectsOuterVariable() {
    assertThat(collect("MATCH (p:Person) WHERE all(x IN ['Alice','Bob'] WHERE x <> p.name) = true RETURN p"))
        .containsExactly("p");
  }

  @Test
  void nonePredicateInComparisonCollectsOuterVariable() {
    assertThat(collect("MATCH (p:Person) WHERE none(x IN ['Alice'] WHERE x = p.name) = true RETURN p"))
        .containsExactly("p");
  }

  @Test
  void singlePredicateInComparisonCollectsOuterVariable() {
    assertThat(collect("MATCH (p:Person) WHERE single(x IN ['Alice'] WHERE x = p.name) = true RETURN p"))
        .containsExactly("p");
  }

  @Test
  void listComprehensionExcludesLoopIterator() {
    // The iterator 'x' must not leak into the collected variable set; only the
    // outer 'p' should appear. This also covers the ListComprehensionExpression
    // branch that was aligned with the list-predicate fix.
    assertThat(collect("MATCH (p:Person) WHERE size([x IN ['Alice'] WHERE x = p.name]) > 0 RETURN p"))
        .containsExactly("p");
  }

  @Test
  void listComprehensionCombinedWithAndCollectsBothOuterVariables() {
    // Combined predicates: 'p' comes from both sides; iterator 'x' must stay hidden.
    assertThat(collect(
        "MATCH (p:Person) WHERE size([x IN ['Alice'] WHERE x = p.name]) > 0 AND p.age > 0 RETURN p"))
        .containsExactly("p");
  }

  private static Set<String> collect(final String query) {
    final CypherStatement stmt = PARSER.parse(query);
    final WhereClause where = stmt.getMatchClauses().get(0).getWhereClause();
    return WhereClause.collectVariables(where.getConditionExpression());
  }
}
