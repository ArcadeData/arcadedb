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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #5696.
 * <p>
 * A subquery body ({@code EXISTS { }}, {@code COUNT { }}) seeded directly from an outer row (no leading
 * {@code WITH}) honours a pre-bound outer <i>node</i> variable but not a pre-bound outer
 * <i>relationship</i> variable: a leading {@code MATCH} that re-mentions the outer relationship variable
 * treated it as fresh and unbound, so the correlation silently matched nothing. A {@code WITH} in front
 * of the {@code MATCH}, or a node-only correlation, took the working path - which is what isolates the
 * defect to the relationship binding specifically.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5696ExistsRelationshipVariableCorrelationTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("opencypher",
        "CREATE (:Person {name:'Alice'})-[:KNOWS]->(:Person {name:'Bob'})-[:KNOWS]->(:Person {name:'Charlie'})");
  }

  /** The issue's first reproducer: EXISTS{} re-matching the outer relationship variable. */
  @Test
  void existsCorrelatesOnTheOuterRelationshipVariable() {
    assertThat(count("MATCH (p:Person)-[r:KNOWS]->(q) WHERE EXISTS { MATCH (p)-[r:KNOWS]->(x) } RETURN count(*) AS c"))
        .isEqualTo(2L);
  }

  /** The issue's second reproducer: COUNT{} re-matching the outer relationship variable, per row. */
  @Test
  void countSubqueryCorrelatesOnTheOuterRelationshipVariable() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (p:Person)-[r:KNOWS]->(q) RETURN COUNT { MATCH (p)-[r:KNOWS]->(x) } AS c ORDER BY p.name")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("c").longValue()).as("Alice-Bob row").isEqualTo(1L);
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("c").longValue()).as("Bob-Charlie row").isEqualTo(1L);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  /** {@code COLLECT { }} flows through the same {@code executeWithSeedRow} path as EXISTS/COUNT. */
  @Test
  void collectSubqueryCorrelatesOnTheOuterRelationshipVariable() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (p:Person)-[r:KNOWS]->(q) RETURN COLLECT { MATCH (p)-[r:KNOWS]->(x) RETURN x.name } AS names "
            + "ORDER BY p.name")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<List<Object>>getProperty("names")).as("Alice-Bob row").containsExactly("Bob");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<List<Object>>getProperty("names")).as("Bob-Charlie row").containsExactly("Charlie");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  /** Control: a fresh (non-reused) relationship variable in the body must still behave as an unbound match. */
  @Test
  void aFreshRelationshipVariableInTheBodyIsUnaffected() {
    assertThat(count("MATCH (p:Person)-[r:KNOWS]->(q) WHERE EXISTS { MATCH (p)-[r2:KNOWS]->(x) } RETURN count(*) AS c"))
        .isEqualTo(2L);
  }

  /** Control: an explicit CALL{} with an importing WITH already worked before the fix - must keep working. */
  @Test
  void callSubqueryWithExplicitWithImportStillCorrelates() {
    assertThat(count("MATCH (p:Person)-[r:KNOWS]->(q) CALL { WITH p, r MATCH (p)-[r:KNOWS]->(x) RETURN x } "
        + "RETURN count(*) AS c")).isEqualTo(2L);
  }

  /** Control: a WITH-first EXISTS body already materialized the binding before the fix - must keep working. */
  @Test
  void existsWithLeadingWithStillCorrelates() {
    assertThat(count("MATCH (p:Person)-[r:KNOWS]->(q) WHERE EXISTS { WITH p, r MATCH (p)-[r:KNOWS]->(x) RETURN x } "
        + "RETURN count(*) AS c")).isEqualTo(2L);
  }

  /** A relationship-only correlation (no outer node re-mention) that cannot possibly be satisfied by another edge. */
  @Test
  void existsCorrelatesOnTheRelationshipEvenWhenTheEdgeTypeWouldOtherwiseMatchMoreThanOne() {
    database.command("opencypher", "MATCH (a:Person {name:'Alice'}), (c:Person {name:'Charlie'}) "
        + "CREATE (a)-[:KNOWS]->(c)");

    // Alice now has two outgoing KNOWS edges (to Bob and to Charlie); the outer row for the
    // Alice->Bob edge must correlate to that specific edge, not to Alice's other KNOWS edge.
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (p:Person {name:'Alice'})-[r:KNOWS]->(q:Person {name:'Bob'}) "
            + "WHERE EXISTS { MATCH (p)-[r:KNOWS]->(x) WHERE x.name = 'Bob' } RETURN count(*) AS c")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(1L);
    }
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (p:Person {name:'Alice'})-[r:KNOWS]->(q:Person {name:'Bob'}) "
            + "WHERE EXISTS { MATCH (p)-[r:KNOWS]->(x) WHERE x.name = 'Charlie' } RETURN count(*) AS c")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(0L);
    }
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).isTrue();
      return rs.next().<Number>getProperty("c").longValue();
    }
  }
}
