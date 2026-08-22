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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #6567.
 * <p>
 * A relationship variable read only inside a WHERE list predicate ({@code any}/{@code all}/
 * {@code none}/{@code single}) was silently anonymized: {@link com.arcadedb.query.opencypher.executor.CypherVariableUsage}
 * decided whether {@code r0} kept its real binding by scanning {@code Expression#getText()} for the
 * name as a standalone word, but that text is ANTLR's default {@code getText()} - it drops every
 * whitespace between tokens, so {@code any(item IN r0.k8 WHERE item IS NOT NULL)} became
 * {@code "any(itemINr0.k8WHEREitemISNOTNULL)"} and "r0" was no longer a standalone word once glued to
 * the "N" of "IN". The relationship pattern step then bound the edge under an internal anonymous name
 * instead of "r0", so the WHERE clause's own (correct, AST-based) evaluation of {@code r0.k8} read a
 * missing binding, silently turned a true predicate false, and dropped every row downstream - including
 * an entire {@code CALL () { ... } IN TRANSACTIONS} subquery whose results a later {@code collect()}
 * should have seen.
 * <p>
 * A value-preserving {@code WITH r0 AS r0} (or any other clause re-mentioning {@code r0} in a way the
 * text scan could parse) "fixed" the symptom by making the variable look referenced again - which is
 * why the issue surfaced as two textually-equivalent queries returning different results, rather than
 * as an outright wrong answer.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6567ListPredicateEdgeVariableTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("opencypher",
        "CREATE (n1:Person {id: 'n1'})-[r1:rt2]->(x:Person {id: 'x', klist: ['a']})-[:rt10]->(n0:Person {id: 'n0'})"
            + "-[r0:rt8 {k9: 267022112, k8: [1]}]->(t:Person {id: 't', k0: true, k8: [1, 2]})");
    database.command("opencypher",
        "CREATE (s:Person {id: 22})-[:rt5]->(n3:Person {id: 'n3', klist: []})-[:rt8]->(n2:Person {id: 'n2', k3: 'e'})");
  }

  /**
   * The issue's own reproducer, minimized: a value-preserving {@code WITH} inserted between the
   * {@code CALL () {...} IN TRANSACTIONS} clause and the final {@code RETURN} must not change the
   * {@code collect()} result, and both forms must see the subquery's row.
   */
  @Test
  void identityWithBetweenCallInTransactionsAndReturnDoesNotChangeTheResult() {
    final String withoutIdentityWith = """
        MATCH
          (n1)-[r1:rt2]->(x {klist: ['a']})-[:rt10]->(n0)-[r0:rt8|rt1|rt7 {k9: 267022112}]->(t {k0: true, k8: [1, 2]})
        WHERE any(item IN r0.k8 WHERE item IS NOT NULL)
        CALL () {
          MATCH (n2), (n3 {klist: []})-[:rt8]->(n2),
                ({id: 22})-[r2:rt5|rt2|rt7]->(n3)
          RETURN n2 AS n15
        } IN TRANSACTIONS OF 10 ROWS
        RETURN collect(toStringOrNull(n15.k3)) AS result
        """;
    final String withIdentityWith = """
        MATCH
          (n1)-[r1:rt2]->(x {klist: ['a']})-[:rt10]->(n0)-[r0:rt8|rt1|rt7 {k9: 267022112}]->(t {k0: true, k8: [1, 2]})
        WHERE any(item IN r0.k8 WHERE item IS NOT NULL)
        CALL () {
          MATCH (n2), (n3 {klist: []})-[:rt8]->(n2),
                ({id: 22})-[r2:rt5|rt2|rt7]->(n3)
          RETURN n2 AS n15
        } IN TRANSACTIONS OF 10 ROWS
        WITH CASE WHEN r0 IS NULL THEN null ELSE r0 END AS r0, n15
        RETURN collect(toStringOrNull(n15.k3)) AS result
        """;

    assertThat(singleResult(withoutIdentityWith)).containsExactly("e");
    assertThat(singleResult(withIdentityWith)).containsExactly("e");
  }

  /** Same defect, without CALL/IN TRANSACTIONS: the any() predicate alone must not lose the edge binding. */
  @Test
  void anyPredicateOnRelationshipPropertyDoesNotDropTheMatch() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (n1)-[r1:rt2]->(x)-[:rt10]->(n0)-[r0:rt8 {k9: 267022112}]->(t) "
            + "WHERE any(item IN r0.k8 WHERE item IS NOT NULL) "
            + "RETURN count(*) AS c")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(1L);
    }
  }

  @SuppressWarnings("unchecked")
  private List<Object> singleResult(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      return (List<Object>) row.getProperty("result");
    }
  }
}
