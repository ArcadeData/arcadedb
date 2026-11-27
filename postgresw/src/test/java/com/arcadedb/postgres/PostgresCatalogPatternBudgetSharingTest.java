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
package com.arcadedb.postgres;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The evaluator charges the character reads of a text pattern to the budget its resolver holds, when it holds one, and to the
 * expression node's own budget when it does not: what lets every node of a statement, and every sub-select parsed again for a row,
 * spend from the one budget.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class PostgresCatalogPatternBudgetSharingTest {
  private static final String TEXT   = "a".repeat(100);
  /** Matched against {@link #TEXT} this costs some 170,000 character reads and fails. */
  private static final String COSTLY = "^(.*)(.*)(.*)Y$";

  @Test
  void aResolverThatHoldsABudgetChargesEveryNodeToIt() {
    final PostgresCatalogExpression first = parse("'" + TEXT + "' ~ '" + COSTLY + "'");
    final PostgresCatalogExpression second = parse("'" + TEXT + "' ~ '^(.*)(.*)(.*)Z$'");

    // a node without a budget of the resolver spends its own for this many evaluations...
    final int alone = evaluationsUntilRefused(first, new Row(null));
    assertThat(alone).isGreaterThan(2);

    // ...two nodes of one budget for about half of them each
    final Row shared = new Row(new PostgresCatalogExpression.StatementPatterns());
    int evaluations = 0;
    try {
      while (evaluations < 4 * alone) {
        first.evaluate(shared);
        second.evaluate(shared);
        ++evaluations;
      }
    } catch (final PostgresCatalogExpression.BudgetExceeded e) {
      // refused
    }
    assertThat(evaluations).isGreaterThan(0).isLessThanOrEqualTo(alone / 2 + 2);
  }

  @Test
  void theBudgetSpentIsTheOneOfTheResolverAndNotTheOneOfTheNode() {
    final PostgresCatalogExpression node = parse("'" + TEXT + "' ~ '" + COSTLY + "'");

    // one budget spent to the last read...
    final int first = evaluationsUntilRefused(node, new Row(new PostgresCatalogExpression.StatementPatterns()));
    assertThat(first).isGreaterThan(2);

    // ...and the node answers as many times again on the next one: nothing of it is kept in the node
    assertThat(evaluationsUntilRefused(node, new Row(new PostgresCatalogExpression.StatementPatterns()))).isEqualTo(first);
  }

  @Test
  void aNodeWithoutTheBudgetOfAResolverKeepsItsOwn() {
    final PostgresCatalogExpression node = parse("'" + TEXT + "' ~ '" + COSTLY + "'");

    final int first = evaluationsUntilRefused(node, new Row(null));

    assertThat(first).isGreaterThan(2).isLessThan(100_000);
    // spent: the node answers no more, however it is asked
    assertThat(evaluationsUntilRefused(node, new Row(null))).isZero();
  }

  @Test
  void cheapPatternsOfOneBudgetAreAnsweredManyTimes() {
    final PostgresCatalogExpression node = parse("'Person' ~ '^(Person|Note)$'");
    final Row shared = new Row(new PostgresCatalogExpression.StatementPatterns());

    for (int i = 0; i < 20_000; i++)
      assertThat(node.evaluate(shared)).isEqualTo(Boolean.TRUE);
  }

  // ---------------------------------------------------------------- helpers

  private static int evaluationsUntilRefused(final PostgresCatalogExpression expression, final Row resolver) {
    int evaluations = 0;
    try {
      while (evaluations < 100_000) {
        expression.evaluate(resolver);
        ++evaluations;
      }
    } catch (final PostgresCatalogExpression.BudgetExceeded e) {
      // refused
    }
    return evaluations;
  }

  private static PostgresCatalogExpression parse(final String expression) {
    final PostgresCatalogExpression parsed = PostgresCatalogExpression.parse(PostgresCatalogToken.tokenize(expression));
    assertThat(parsed).as("expression did not parse: %s", expression).isNotNull();
    return parsed;
  }

  /** A made-up row with no column, which belongs to a statement that holds the given budget, or to none when it is null. */
  private record Row(PostgresCatalogExpression.StatementPatterns statementPatterns) implements PostgresCatalogExpression.Resolver {
    @Override
    public Object column(final String qualifier, final String name) {
      return PostgresCatalogExpression.UNKNOWN;
    }

    @Override
    public Object function(final String name, final List<Object> arguments) {
      return PostgresCatalogExpression.UNKNOWN;
    }
  }
}
