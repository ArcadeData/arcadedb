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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #6599: a {@code none(item IN r.prop WHERE ...)} list predicate
 * on a relationship variable inside a {@code MATCH REPEATABLE ELEMENT BINDINGS} clause nested in a
 * {@code CALL (...) {...}} subquery silently anonymized the relationship binding - the same root
 * cause as #6567 and #6600, fixed by making
 * {@link com.arcadedb.query.opencypher.executor.CypherVariableUsage} walk the parsed AST instead of
 * scanning {@code Expression#getText()} for the variable name as a standalone word.
 * <p>
 * The reported symptom: the query as written returned {@code __v = false} because the subquery's
 * {@code count(*)} came back 0; appending value-preserving {@code CASE} projections for the
 * subquery's own bound variables made the same predicate see the relationship variable again and
 * the query returned the expected {@code __v = true}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherNonePredicateRepeatableBindingsIssue6599Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("opencypher", "CREATE (a:Outer {k2: true})-[:rt0 {k4: -1230316915, id: 26}]->(b:Outer {k2: false})");
    database.command("opencypher", """
        CREATE (c:l11 {k3: 856175783})<-[f:rt0 {k7: 2081692630, k10: [1386720338, 1309075339]}]-
               (d:Inner {k7: -1751297746})-
               [g:rt1 {k3: -1101784399, k7: 1596251831, k10: [1]}]->(h:l7)
        """);
  }

  /**
   * The issue's own reproducer: the query as written and the same query with identity {@code CASE}
   * projections for the subquery's bound variables must return the same value.
   */
  @Test
  void nonePredicateInsideCallSubqueryDoesNotDropTheMatch() {
    final String control = """
        MATCH (n0:Outer {k2: true})-[:rt0 {k4: -1230316915, id: 26}]->(:Outer {k2: false})
        WHERE (n0.k2 = false OR n0.k2 = true)
        CALL (n0) {
          MATCH REPEATABLE ELEMENT BINDINGS
            p1 = (:l11|l10) <-[r0 {k7: 2081692630, k10: [1386720338, 1309075339]}]-
                 ({k7: -1751297746}) -[r1:rt1 {k3: -1101784399, k7: 1596251831}]-> (:l7)
          WHERE none(item IN r1.k10 WHERE item IS NULL)
          RETURN count(*) AS __expr_count
        }
        RETURN __expr_count > 0 AS __v
        """;
    final String identityProjection = """
        MATCH (n0:Outer {k2: true})-[:rt0 {k4: -1230316915, id: 26}]->(:Outer {k2: false})
        WHERE (n0.k2 = false OR n0.k2 = true)
        CALL (n0) {
          MATCH REPEATABLE ELEMENT BINDINGS
            p1 = (:l11|l10) <-[r0 {k7: 2081692630, k10: [1386720338, 1309075339]}]-
                 ({k7: -1751297746}) -[r1:rt1 {k3: -1101784399, k7: 1596251831}]-> (:l7)
          WHERE none(item IN r1.k10 WHERE item IS NULL)
          WITH CASE WHEN n0 IS NULL THEN null ELSE n0 END AS n0,
               CASE WHEN r0 IS NULL THEN null ELSE r0 END AS r0,
               CASE WHEN r1 IS NULL THEN null ELSE r1 END AS r1,
               CASE WHEN p1 IS NULL THEN null ELSE p1 END AS p1
          RETURN count(*) AS __expr_count
        }
        RETURN __expr_count > 0 AS __v
        """;

    final boolean controlValue = singleBoolean(control);
    final boolean projectionValue = singleBoolean(identityProjection);

    assertThat(controlValue).isTrue();
    assertThat(projectionValue).isTrue();
    assertThat(controlValue).isEqualTo(projectionValue);
  }

  /** Same defect, minimized: a bare {@code none()} predicate on a relationship property alone. */
  @Test
  void nonePredicateAloneDoesNotDropTheMatch() {
    try (final ResultSet rs = database.query("opencypher", """
        MATCH (:Inner) -[r1:rt1 {k3: -1101784399, k7: 1596251831}]-> (:l7)
        WHERE none(item IN r1.k10 WHERE item IS NULL)
        RETURN count(*) AS c
        """)) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(1L);
    }
  }

  private boolean singleBoolean(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).isTrue();
      return rs.next().<Boolean>getProperty("__v");
    }
  }
}
