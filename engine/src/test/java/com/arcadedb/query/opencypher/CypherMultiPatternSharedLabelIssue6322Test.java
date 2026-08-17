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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for item 1 of issue #6322: a two-pattern {@code MATCH} returned no rows at all when both
 * shared variables carried a label and at least one of those labels was written on the second pattern.
 * <p>
 * A variable repeated across patterns names one vertex, so the constraints of every occurrence apply to it -
 * {@code MATCH (a)-[:R]->(b), (a:Person)-[:S]->(c)} requires {@code a} to be a Person exactly as if the label
 * had been written on the first pattern. The logical plan kept only the first occurrence of each variable, so a
 * label written later was dropped. That alone would over-count; what made it answer <em>zero</em> is the
 * planner's admission rule, which asks whether every variable is labelled <em>somewhere</em>: a query labelling
 * its variables only on the second pattern was let into the optimizer, which then had no label for the anchor
 * and emitted a scan of the composite type name for "no labels" - {@code V}, a type the schema does not have.
 * <p>
 * Two things follow, and both are asserted here: the occurrences are merged, and a merged label set the
 * physical operators cannot represent - a conjunction, or a disjunction crossed with a further label - declines
 * the optimizer rather than scanning something else. The same merge applies to inline property maps, which were
 * dropped from a second occurrence for the same reason.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherMultiPatternSharedLabelIssue6322Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Person");
      database.getSchema().createVertexType("Bot");
      database.getSchema().createVertexType("Comment");
      database.getSchema().createEdgeType("AUTHORED");
      database.getSchema().createEdgeType("MENTIONS");
      database.getSchema().createEdgeType("KNOWS");

      final MutableVertex p1 = database.newVertex("Person").set("k", "p1").save();
      final MutableVertex p2 = database.newVertex("Person").set("k", "p2").save();
      // The vertex that makes a dropped label visible: an author that is not a Person.
      final MutableVertex b1 = database.newVertex("Bot").set("k", "b1").save();
      final MutableVertex c1 = database.newVertex("Comment").set("k", "c1").save();
      final MutableVertex c2 = database.newVertex("Comment").set("k", "c2").save();

      c1.newEdge("AUTHORED", p1).save();
      c1.newEdge("MENTIONS", p2).save();
      c2.newEdge("AUTHORED", b1).save();
      c2.newEdge("MENTIONS", p2).save();
      p1.newEdge("KNOWS", p2).save();
      b1.newEdge("KNOWS", p2).save();
    });
  }

  // ===================================================================================================
  // the shape the issue reported: labels on the second pattern
  // ===================================================================================================

  /**
   * Both shared variables labelled on the second pattern. Only c1 qualifies: its author p1 is a Person, its
   * mentioned p2 is a Person, and p1 KNOWS p2 - while c2's author b1 is a Bot.
   */
  @Test
  void bothSharedVariablesLabelledOnTheSecondPattern() {
    assertThat(rowCountOf(
        "MATCH (p1)-[:KNOWS]->(p2), (p1:Person)<-[:AUTHORED]-(c:Comment)-[:MENTIONS]->(p2:Person) RETURN c")).isEqualTo(1);
  }

  /** One label on each pattern: the same single vertex satisfies both, whichever pattern wrote which. */
  @Test
  void oneLabelOnEachPattern() {
    assertThat(rowCountOf(
        "MATCH (p1)-[:KNOWS]->(p2:Person), (p1:Person)<-[:AUTHORED]-(c:Comment)-[:MENTIONS]->(p2) RETURN c")).isEqualTo(1);
    assertThat(rowCountOf(
        "MATCH (p1:Person)-[:KNOWS]->(p2), (p1)<-[:AUTHORED]-(c:Comment)-[:MENTIONS]->(p2:Person) RETURN c")).isEqualTo(1);
  }

  /** The spelling that always worked, kept as the reference the three above have to agree with. */
  @Test
  void bothLabelsOnTheFirstPattern() {
    assertThat(rowCountOf(
        "MATCH (p1:Person)-[:KNOWS]->(p2:Person), (p1)<-[:AUTHORED]-(c:Comment)-[:MENTIONS]->(p2) RETURN c")).isEqualTo(1);
  }

  /** One labelled variable and one that is never labelled: the Bot author is not excluded, so both comments match. */
  @Test
  void onlyOneOfTheSharedVariablesIsLabelled() {
    assertThat(rowCountOf(
        "MATCH (p1)-[:KNOWS]->(p2), (p1)<-[:AUTHORED]-(c:Comment)-[:MENTIONS]->(p2:Person) RETURN c")).isEqualTo(2);
    assertThat(rowCountOf(
        "MATCH (p1)-[:KNOWS]->(p2), (p1:Bot)<-[:AUTHORED]-(c:Comment)-[:MENTIONS]->(p2) RETURN c")).isEqualTo(1);
  }

  /** Separate MATCH clauses share their variables the same way two comma-separated patterns do. */
  @Test
  void theLabelIsMergedAcrossMatchClausesToo() {
    assertThat(rowCountOf(
        "MATCH (p1)-[:KNOWS]->(p2) MATCH (p1:Person)<-[:AUTHORED]-(c:Comment)-[:MENTIONS]->(p2:Person) RETURN c"))
        .isEqualTo(1);
  }

  /** The merged label is the one the anchor scan uses, rather than the "no labels" composite name. */
  @Test
  void theMergedLabelReachesTheAnchorScan() {
    final String plan = explainOf(
        "MATCH (p1)-[:KNOWS]->(p2), (p1:Person)<-[:AUTHORED]-(c:Comment)-[:MENTIONS]->(p2:Person) RETURN c");
    assertThat(plan).contains("NodeByLabelScan(p1:Person)");
    assertThat(plan).doesNotContain(":V)");
  }

  // ===================================================================================================
  // merged sets the physical operators cannot represent
  // ===================================================================================================

  /**
   * Two different labels on one variable merge into a conjunction, which the composite type name a scan would
   * use does not stand for - a vertex labelled {@code Person:Bot:Admin} carries type {@code Admin~Bot~Person},
   * which does not extend {@code Bot~Person}. The optimizer declines and the ordinary pipeline, which tests each
   * label in turn, answers: no vertex here is both, so no row.
   */
  @Test
  void aConjunctionArrivedAtByMergingDeclinesTheOptimizer() {
    final String pattern = "MATCH (p1:Person)-[:KNOWS]->(p2:Person), (p1:Bot)<-[:AUTHORED]-(c:Comment)-[:MENTIONS]->(p2)";
    assertThat(explainOf(pattern + " RETURN c")).contains("Traditional Execution");
    assertThat(rowCountOf(pattern + " RETURN c")).isEqualTo(0);
  }

  /**
   * A disjunction crossed with a further label is not a shape one {@code LogicalNode} can hold at all - it
   * carries one flag for the whole list - so it declines as well rather than being flattened into something
   * that means neither.
   */
  @Test
  void aDisjunctionCrossedWithAFurtherLabelDeclinesTheOptimizer() {
    final String pattern = "MATCH (p1:Person|Bot)-[:KNOWS]->(p2:Person), (p1:Person)<-[:AUTHORED]-(c:Comment)-[:MENTIONS]->(p2)";
    assertThat(explainOf(pattern + " RETURN c")).contains("Traditional Execution");
    assertThat(rowCountOf(pattern + " RETURN c")).isEqualTo(1);
  }

  /** The same label written twice is one constraint, not a conjunction, and stays on the optimized path. */
  @Test
  void theSameLabelOnBothPatternsIsStillOneLabel() {
    final String pattern = "MATCH (p1:Person)-[:KNOWS]->(p2:Person), (p1:Person)<-[:AUTHORED]-(c:Comment)-[:MENTIONS]->(p2)";
    assertThat(explainOf(pattern + " RETURN c")).contains("Cost-Based Query Optimizer");
    assertThat(rowCountOf(pattern + " RETURN c")).isEqualTo(1);
  }

  // ===================================================================================================
  // inline property maps, dropped from a later occurrence by the same rule
  // ===================================================================================================

  /** A property map on the second occurrence of a variable constrains the same vertex the first one bound. */
  @Test
  void anInlinePropertyMapOnASecondOccurrenceIsApplied() {
    assertThat(rowCountOf(
        "MATCH (p1:Person)-[:KNOWS]->(p2:Person), (p1 {k:'p1'})<-[:AUTHORED]-(c:Comment)-[:MENTIONS]->(p2) RETURN c"))
        .isEqualTo(1);
    assertThat(rowCountOf(
        "MATCH (p1:Person)-[:KNOWS]->(p2:Person), (p1 {k:'p2'})<-[:AUTHORED]-(c:Comment)-[:MENTIONS]->(p2) RETURN c"))
        .isEqualTo(0);
  }

  /** Two occurrences pinning the same property to different values is a contradiction, not the last one written. */
  @Test
  void twoOccurrencesPinningOnePropertyKeepBothComparisons() {
    assertThat(rowCountOf(
        "MATCH (p1:Person {k:'p1'})-[:KNOWS]->(p2:Person), (p1 {k:'p2'})<-[:AUTHORED]-(c:Comment)-[:MENTIONS]->(p2) RETURN c"))
        .isEqualTo(0);
    assertThat(rowCountOf(
        "MATCH (p1:Person {k:'p1'})-[:KNOWS]->(p2:Person), (p1 {k:'p1'})<-[:AUTHORED]-(c:Comment)-[:MENTIONS]->(p2) RETURN c"))
        .isEqualTo(1);
  }

  // ===================================================================================================
  // helpers
  // ===================================================================================================

  private int rowCountOf(final String query) {
    int count = 0;
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
    }
    return count;
  }

  /** The rendered execution plan of {@code EXPLAIN <query>}. */
  private String explainOf(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      assertThat(rs.hasNext()).as(query).isTrue();
      return rs.next().getProperty("executionPlanAsString");
    }
  }
}
