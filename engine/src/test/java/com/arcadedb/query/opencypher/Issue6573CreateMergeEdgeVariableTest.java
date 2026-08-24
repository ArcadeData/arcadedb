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
 * Regression test for GitHub issue #6573.
 * <p>
 * {@code CypherVariableUsage.isEdgeVariableReferenced} decides whether a MATCH-bound relationship
 * variable keeps its real binding (vs. being anonymized for the CSR/GAV fast path) by walking every
 * clause type in {@code statement.getClausesInOrder()}. The top-level switch had no
 * {@code case CREATE}/{@code case MERGE} - unlike its sibling {@code foreachReferencesVariable} in the
 * same file, which already treats a nested CREATE/MERGE conservatively - so a top-level CREATE/MERGE
 * fell to {@code default} and was never inspected. That anonymized the edge whenever nothing else in
 * the query mentioned it, so an inline property expression like {@code CREATE (c {since: r.since})}
 * silently read a missing binding and evaluated to null instead of the real value.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6573CreateMergeEdgeVariableTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("opencypher", "CREATE (a:Person {id: 'a'})-[r:KNOWS {since: 2001}]->(b:Person {id: 'b'})");
  }

  /** The issue's own reproducer: a relationship read only inside a CREATE inline property. */
  @Test
  void createReadsTheEdgePropertyThroughAnInlineExpression() {
    try (final ResultSet rs = database.command("opencypher",
        "MATCH (a:Person)-[r:KNOWS]->(b:Person) CREATE (c:Note {since: r.since}) RETURN c.since AS s")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("s").intValue()).isEqualTo(2001);
    }
  }

  /** Same defect via MERGE instead of CREATE. */
  @Test
  void mergeReadsTheEdgePropertyThroughAnInlineExpression() {
    try (final ResultSet rs = database.command("opencypher",
        "MATCH (a:Person)-[r:KNOWS]->(b:Person) MERGE (c:Note {since: r.since}) RETURN c.since AS s")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("s").intValue()).isEqualTo(2001);
    }
  }
}
