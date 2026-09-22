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
import com.arcadedb.exception.CommandParameterMissingException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Parity test for issue #8132, which made the Cypher parse of a query text the statement cache has not seen
 * materially cheaper - it was ~85% of the cost of an indexed point lookup and the whole of the gap against the same
 * lookup in SQL. Four things changed, and every one of them is a claim that the cheaper route answers EXACTLY as the
 * exhaustive one did:
 * <ol>
 *   <li>{@code ExpressionTypeDetector} walks the full-span SPINE instead of the whole subtree, because a descendant
 *   that shares both boundary tokens with the expression can only be on it. The observable consequence is the
 *   span guard of issues #5140 and #5342: a {@code CASE}/{@code EXISTS}/{@code COLLECT}/{@code COUNT}/comprehension
 *   that IS the whole expression is parsed as such, and one that is only a part of a larger comparison or
 *   arithmetic expression is not.</li>
 *   <li>{@code CypherExpressionBuilder.parseExpressionFromText} probes for those same five block constructs in ONE
 *   traversal instead of five, and skips the probe outright once a level above has ruled them out.</li>
 *   <li>{@code CypherExpressionDepthGuard} counts a chain's terms without building a list per rule exit.</li>
 *   <li>Parameter names are collected only when the query text contains a {@code $}.</li>
 * </ol>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8132ParserFastPathParityTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Person");
      database.command("sql", "CREATE EDGE TYPE Knows");
      database.command("opencypher", "CREATE (a:Person {name:'a', age:10}), (b:Person {name:'b', age:20}), "
          + "(c:Person {name:'c', age:30})");
      database.command("opencypher", "MATCH (a:Person {name:'a'}), (b:Person {name:'b'}) CREATE (a)-[:Knows]->(b)");
    });
  }

  /** A block construct that IS the whole expression is parsed as that construct. */
  @Test
  void aBlockConstructSpanningTheWholeExpressionIsParsedAsThatConstruct() {
    assertThat(number("RETURN CASE WHEN true THEN 1 ELSE 2 END AS r")).isEqualTo(1L);
    assertThat(single("RETURN CASE 2 WHEN 1 THEN 'a' WHEN 2 THEN 'b' END AS r")).isEqualTo("b");
    assertThat(single("MATCH (p:Person {name:'a'}) RETURN exists((p)-[:Knows]->()) AS r")).isEqualTo(true);
    assertThat(number("MATCH (p:Person {name:'a'}) RETURN COUNT { (p)-[:Knows]->() } AS r")).isEqualTo(1L);
    assertThat(single("MATCH (p:Person {name:'a'}) RETURN COLLECT { MATCH (p)-[:Knows]->(q) RETURN q.name } AS r"))
        .isEqualTo(List.of("b"));
  }

  /**
   * The counterpart the span guard exists for (#5140, #5342): the SAME construct as only part of a larger
   * expression must NOT swallow the rest of it. A spine walk has to answer here exactly as the subtree search did.
   */
  @Test
  void aBlockConstructThatIsOnlyPartOfTheExpressionDoesNotSwallowTheRest() {
    assertThat(number("RETURN CASE WHEN true THEN 1 ELSE 2 END + 10 AS r")).isEqualTo(11L);
    assertThat(single("MATCH (p:Person {name:'a'}) RETURN COUNT { (p)-[:Knows]->() } = 0 AS r")).isEqualTo(false);
    assertThat(number("MATCH (p:Person {name:'a'}) RETURN COUNT { (p)-[:Knows]->() } + 5 AS r")).isEqualTo(6L);
    assertThat(single("MATCH (p:Person {name:'b'}) RETURN exists((p)-[:Knows]->()) = false AS r")).isEqualTo(true);
    // a block construct nested as a function argument keeps its wrapper
    assertThat(number("RETURN abs(CASE WHEN true THEN -7 ELSE 1 END) AS r")).isEqualTo(7L);
  }

  /** The comprehension family, guarded the same way (#5342: a trailing 2-char operator used to be dropped). */
  @Test
  void comprehensionsKeepTheirTrailingOperator() {
    assertThat(number("RETURN reduce(acc = 0, x IN [1,2,3] | acc + x) AS r")).isEqualTo(6L);
    assertThat(number("RETURN reduce(acc = 0, x IN [1,2,3] | acc + x) / 2 AS r")).isEqualTo(3L);
    assertThat(single("RETURN [x IN [1,2,3] | x * 2] AS r")).isEqualTo(List.of(2L, 4L, 6L));
    assertThat(number("RETURN size([x IN [1,2,3] | x * 2]) + 1 AS r")).isEqualTo(4L);
    assertThat(number("MATCH (p:Person {name:'a'}) RETURN size([(p)-[:Knows]->(q) | q.name]) AS r")).isEqualTo(1L);
  }

  /** The list predicates, whose guard is a text-length tolerance rather than a token span. */
  @Test
  void listPredicatesStillAnswerWholeAndPartial() {
    assertThat(single("RETURN all(x IN [1,2,3] WHERE x > 0) AS r")).isEqualTo(true);
    assertThat(single("RETURN none(x IN [1,2,3] WHERE x > 5) = true AS r")).isEqualTo(true);
    assertThat(single("RETURN any(x IN [1,2,3] WHERE x = 2) AS r")).isEqualTo(true);
  }

  /**
   * The chain-length guard still counts the same terms it always did, now without allocating a list per rule exit.
   * A long-but-legal chain is accepted and evaluated correctly at every precedence level it covers.
   */
  @Test
  void theChainLengthGuardStillCountsTheSameTerms() {
    final StringBuilder sum = new StringBuilder("RETURN 0");
    for (int i = 1; i <= 50; i++)
      sum.append(" + ").append(i);
    assertThat(number(sum + " AS r")).isEqualTo(1275L);

    assertThat(single("RETURN 1 = 1 AND 2 = 2 AND 3 = 3 AND 4 = 4 AS r")).isEqualTo(true);
    assertThat(single("RETURN false OR false OR false OR true AS r")).isEqualTo(true);
    assertThat(single("RETURN true XOR false XOR false AS r")).isEqualTo(true);
    assertThat(single("RETURN NOT NOT NOT false AS r")).isEqualTo(true);
    assertThat(number("RETURN 2 * 3 * 4 AS r")).isEqualTo(24L);
    assertThat(((Number) single("RETURN 2 ^ 3 ^ 1 AS r")).doubleValue()).isEqualTo(8.0);
    assertThat(single("RETURN 'a' + 'b' + 'c' AS r")).isEqualTo("abc");
  }

  /**
   * Parameter names: the {@code $}-free fast path must not lose a parameter the query does reference, and a query
   * that references none must still report none - which is what makes an unbound parameter an error.
   */
  @Test
  void parameterNamesAreStillCollectedExactly() {
    assertThat(number("MATCH (p:Person) WHERE p.name = $name RETURN p.age AS r", Map.of("name", "b"))).isEqualTo(20L);

    // unbound: only detectable because the name WAS collected off the parse tree
    assertThatThrownBy(() -> single("MATCH (p:Person) WHERE p.name = $missing RETURN p.age AS r", Map.of()))
        .isInstanceOf(CommandParameterMissingException.class);

    // a '$' that is not a parameter (inside a string literal) makes the walk run and find nothing, as before
    assertThat(single("RETURN 'costs $5' AS r")).isEqualTo("costs $5");
    assertThat(single("MATCH (p:Person) WHERE p.name = 'a' RETURN '$notaparam' AS r")).isEqualTo("$notaparam");

    // and a query with no '$' at all - the skipped walk - still parses and runs
    assertThat(number("MATCH (p:Person) WHERE p.name = 'c' RETURN p.age AS r")).isEqualTo(30L);
  }

  /** The point lookup of the issue itself, to pin that the optimised parse still produces the right plan. */
  @Test
  void theIndexedPointLookupStillAnswers() {
    database.transaction(() -> {
      database.command("sql", "CREATE PROPERTY Person.age INTEGER");
      database.command("sql", "CREATE INDEX ON Person (age) UNIQUE");
    });

    final List<Object> names = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "MATCH (p:Person) WHERE p.age = 20 RETURN p.name, p.age")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        names.add(row.getProperty("p.name"));
        assertThat(row.<Number>getProperty("p.age").intValue()).isEqualTo(20);
      }
    }
    assertThat(names).containsExactly("b");
  }

  /** The projected value as a long, so an Integer/Long difference in the result type is not the assertion. */
  private long number(final String cypher) {
    return number(cypher, Map.of());
  }

  private long number(final String cypher, final Map<String, Object> parameters) {
    return ((Number) single(cypher, parameters)).longValue();
  }

  private Object single(final String cypher) {
    return single(cypher, Map.of());
  }

  private Object single(final String cypher, final Map<String, Object> parameters) {
    try (final ResultSet rs = database.query("opencypher", cypher, parameters)) {
      assertThat(rs.hasNext()).as("query '%s' returned no row", cypher).isTrue();
      return rs.next().getProperty("r");
    }
  }
}
