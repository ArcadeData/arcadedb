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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #5489.
 * <p>
 * A node inline {@code WHERE} predicate may reference the relationship variable bound by the same
 * hop, as in {@code [(a)-[r:E]->(x:A WHERE x.v > r.w) | x]}. In a pattern comprehension the
 * predicate was evaluated against a row copied from the bindings visible <em>before</em> the hop,
 * which does not yet contain {@code r}, so the reference resolved to null and the predicate rejected
 * every candidate. The comprehension returned an empty list while the equivalent {@code MATCH}
 * spelling returned the correct rows.
 * <p>
 * The relationship variable must be visible to the end node's predicate, matching the {@code MATCH}
 * spelling, which hoists the inline predicate into the clause {@code WHERE} where {@code r} is bound.
 */
class Issue5489NodeInlineWhereRelVariableTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/issue-5489-node-inline-where-rel-var").create();
    database.transaction(() -> {
      database.command("opencypher", "CREATE (a:A {v:1}), (b:A {v:10}), (c:A {v:2}), (d:A {v:7})");
      database.command("opencypher", "MATCH (a:A {v:1}), (b:A {v:10}), (c:A {v:2}), (d:A {v:7}) "
          + "CREATE (a)-[:E {tag:'ok', w:5}]->(b), (a)-[:E {tag:'bad', w:99}]->(c), (b)-[:E {tag:'ok', w:1}]->(d)");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /** Returns the single list-valued projection of a one-row result, as ints. */
  private List<Integer> comprehension(final String query) {
    final List<Integer> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).as("query returned no row: %s", query).isTrue();
      final Result row = rs.next();
      final List<Object> list = row.getProperty("vs");
      assertThat(list).as("projection 'vs' was not a list: %s", query).isNotNull();
      for (final Object o : list)
        values.add(o == null ? null : ((Number) o).intValue());
    }
    return values;
  }

  private List<Integer> queryInts(final String query, final String field) {
    final List<Integer> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final Number n = row.getProperty(field);
        values.add(n == null ? null : n.intValue());
      }
    }
    return values;
  }

  // ------------------------------------------------- the defect: same-hop relationship variable

  @Test
  void comprehensionNodeInlineWhereSeesSameHopRelationshipVariable() {
    // b.v = 10 > w = 5 matches; c.v = 2 > w = 99 does not.
    assertThat(comprehension("MATCH (a:A {v:1}) RETURN [(a)-[r:E]->(x:A WHERE x.v > r.w) | x.v] AS vs"))
        .containsExactly(10);
  }

  @Test
  void comprehensionNodeInlineWhereReferencingOnlyTheRelationshipVariable() {
    assertThat(comprehension("MATCH (a:A {v:1}) RETURN [(a)-[r:E]->(x:A WHERE r.w = 5) | x.v] AS vs"))
        .containsExactly(10);
  }

  @Test
  void comprehensionNodeInlineWhereOnRelationshipVariableRejectingEveryCandidate() {
    assertThat(comprehension("MATCH (a:A {v:1}) RETURN [(a)-[r:E]->(x:A WHERE r.w = 12345) | x.v] AS vs"))
        .isEmpty();
  }

  @Test
  void comprehensionCombinesRelationshipInlineWhereWithNodeInlineWhereOnTheSameVariable() {
    assertThat(comprehension(
        "MATCH (a:A {v:1}) RETURN [(a)-[r:E WHERE r.tag = 'ok']->(x:A WHERE x.v > r.w) | x.v] AS vs"))
        .containsExactly(10);
  }

  // ------------------------------------------------- parity with the MATCH spelling

  @Test
  void matchSpellingAgreesWithTheComprehension() {
    final List<Integer> viaMatch = queryInts(
        "MATCH (a:A {v:1})-[r:E]->(x:A WHERE x.v > r.w) RETURN x.v AS v", "v");
    final List<Integer> viaComprehension = comprehension(
        "MATCH (a:A {v:1}) RETURN [(a)-[r:E]->(x:A WHERE x.v > r.w) | x.v] AS vs");
    assertThat(viaMatch).containsExactly(10);
    assertThat(viaComprehension).containsExactlyElementsOf(viaMatch);
  }

  // ------------------------------------------------- controls that must keep working

  @Test
  void comprehensionNodeInlineWhereWithoutRelationshipReferenceStillWorks() {
    assertThat(comprehension("MATCH (a:A {v:1}) RETURN [(a)-[r:E]->(x:A WHERE x.v > 5) | x.v] AS vs"))
        .containsExactly(10);
  }

  @Test
  void comprehensionRelationshipInlineWhereStillWorks() {
    assertThat(comprehension("MATCH (a:A {v:1}) RETURN [(a)-[r:E WHERE r.w = 5]->(x:A) | x.v] AS vs"))
        .containsExactly(10);
  }

  @Test
  void comprehensionNodeInlineWhereSeesRelationshipVariableFromAnEarlierHop() {
    // a -[r1 w=5]-> b -[r2]-> d(v=7): 7 > 5 matches. The other branch (c) has no outgoing edge.
    assertThat(comprehension(
        "MATCH (a:A {v:1}) RETURN [(a)-[r1:E]->(m:A)-[r2:E]->(x:A WHERE x.v > r1.w) | x.v] AS vs"))
        .containsExactly(7);
  }

  @Test
  void comprehensionNodeInlineWhereOnAVariableLengthHopSeesTheRelationshipVariable() {
    // Single-hop bound so the relationship variable is unambiguous for this shape.
    assertThat(comprehension(
        "MATCH (a:A {v:1}) RETURN [(a)-[r:E*1..1]->(x:A WHERE x.v > r.w) | x.v] AS vs"))
        .containsExactly(10);
  }

  /**
   * An anonymous relationship binds no variable, so a predicate that names one is referencing an
   * undefined variable. It resolves to null and the predicate rejects every candidate, which is the
   * engine's pre-existing behavior for any undefined variable rather than anything this hop changes.
   */
  @Test
  void anonymousRelationshipLeavesAPredicateVariableUndefined() {
    assertThat(comprehension("MATCH (a:A {v:1}) RETURN [(a)-[:E]->(x:A WHERE x.v > r.w) | x.v] AS vs"))
        .isEmpty();
    // Naming the relationship is what makes the very same predicate resolve.
    assertThat(comprehension("MATCH (a:A {v:1}) RETURN [(a)-[r:E]->(x:A WHERE x.v > r.w) | x.v] AS vs"))
        .containsExactly(10);
  }

  @Test
  void comprehensionWithoutAnyInlineWhereIsUnaffected() {
    assertThat(comprehension("MATCH (a:A {v:1}) RETURN [(a)-[r:E]->(x:A) | x.v] AS vs"))
        .containsExactlyInAnyOrder(10, 2);
  }
}
