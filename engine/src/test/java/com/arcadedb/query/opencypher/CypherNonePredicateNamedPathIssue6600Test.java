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

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #6600: a {@code none(item IN r.prop WHERE ...)} list predicate
 * on a relationship variable matched by the traditional (non-optimized) MATCH plan silently
 * anonymized the relationship binding - the same root cause as #6567, fixed by making
 * {@link com.arcadedb.query.opencypher.executor.CypherVariableUsage} walk the parsed AST instead
 * of scanning {@code Expression#getText()} for the variable name as a standalone word.
 * <p>
 * The reported symptom: a {@code MATCH} with a named path and a second comma-separated pattern
 * part, filtered by {@code none()} on a relationship property, returned 0 rows; appending a
 * value-preserving {@code CASE} projection after an {@code UNWIND} made the same predicate see the
 * relationship variable again and the query returned the expected 2 rows.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherNonePredicateNamedPathIssue6600Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("opencypher", """
        CREATE (s {k7: true})-[:e {k3: 'm0eNd'}]->
               (n0 {k3: 'J9qD', k1: true})-[:e]->
               (n1 {k1: true, klist: ['a', 'b']})-
               [:rt7 {k4: -833229964, k9: 'b', k11: [1]}]->
               (u)-[:e]->(v {k6: 1444726425})
        """);
  }

  /**
   * The issue's own reproducer: the query as written and the same query with an identity
   * {@code CASE} projection appended after {@code UNWIND} must return the same rows.
   */
  @Test
  void nonePredicateOnRelationshipDoesNotDropTheMatch() {
    final String control = """
        MATCH p0 = (n1) <-[]- (n0 {k3: 'J9qD', k1: true}) <-[r0 {k3: 'm0eNd'}]- ({k7: true}),
              (n1) -[r1:rt7 {k4: -833229964, k9: 'b'}]-> () -[]-> ({k6: 1444726425})
        WHERE none(item IN r1.k11 WHERE item IS NULL)
        UNWIND coalesce(n1.klist, []) AS alias0
        RETURN n1, n0, p0, alias0 ORDER BY alias0 ASC LIMIT 14
        """;
    final String identityProjection = """
        MATCH p0 = (n1) <-[]- (n0 {k3: 'J9qD', k1: true}) <-[r0 {k3: 'm0eNd'}]- ({k7: true}),
              (n1) -[r1:rt7 {k4: -833229964, k9: 'b'}]-> () -[]-> ({k6: 1444726425})
        WHERE none(item IN r1.k11 WHERE item IS NULL)
        UNWIND coalesce(n1.klist, []) AS alias0
        WITH CASE WHEN n1 IS NULL THEN null ELSE n1 END AS n1,
             CASE WHEN n0 IS NULL THEN null ELSE n0 END AS n0,
             CASE WHEN r0 IS NULL THEN null ELSE r0 END AS r0,
             CASE WHEN r1 IS NULL THEN null ELSE r1 END AS r1,
             CASE WHEN p0 IS NULL THEN null ELSE p0 END AS p0,
             CASE WHEN alias0 IS NULL THEN null ELSE alias0 END AS alias0
        RETURN n1, n0, p0, alias0 ORDER BY alias0 ASC LIMIT 14
        """;

    final List<Object> controlAliases = alias0Values(control);
    final List<Object> projectionAliases = alias0Values(identityProjection);

    assertThat(controlAliases).containsExactly("a", "b");
    assertThat(projectionAliases).containsExactly("a", "b");
  }

  /** Same defect, minimized: a bare {@code none()} predicate on a relationship property alone. */
  @Test
  void nonePredicateAloneDoesNotDropTheMatch() {
    try (final ResultSet rs = database.query("opencypher", """
        MATCH (n1) -[r1:rt7 {k4: -833229964, k9: 'b'}]-> ()
        WHERE none(item IN r1.k11 WHERE item IS NULL)
        RETURN count(*) AS c
        """)) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(1L);
    }
  }

  private List<Object> alias0Values(final String query) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        values.add(row.getProperty("alias0"));
      }
    }
    return values;
  }
}
