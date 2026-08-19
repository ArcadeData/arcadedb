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

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for item 2 of issue #6322: five more count push-down detectors read only the first of a
 * node's labels.
 * <p>
 * {@code (a:A:B)} keeps what carries both and {@code (a:A|B)} keeps what carries either, so
 * {@code getLabels().get(0)} turns each of them into {@code (a:A)}, which is neither. #6304 fixed this for the
 * pair-join detector by declining the push-down; the four detectors here do the same, and each one is checked
 * against the row count the ordinary materialization pipeline produces for the same pattern rather than against
 * a number transcribed from what the operator used to answer.
 * <p>
 * {@code nodeLabelsAreTypeDisjoint} is the fifth, and the odd one out: it is a proof rather than a filter. The
 * caller uses it to conclude that two hops of a pattern cannot be the same physical edge and, on the strength of
 * that, skips the per-hop edge tracking Cypher's relationship-uniqueness rule needs. Reading only the first
 * label of a disjunction proves the wrong thing in the direction that drops matches - and, as the pair of
 * assertions in {@link #aDisjunctionIsOnlyDisjointWhenEveryAlternativeIs()} shows, made the answer depend on
 * which of the two labels the query happened to write first.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherCountPushDownLabelIssue6322Test extends TestHelper {

  @Override
  protected void beginTest() {
    // Written in Cypher rather than through the schema API because :Post:Draft is what creates the composite
    // type Draft~Post - the vertex that carries both labels, and the only one a conjunction pattern may count.
    database.command("opencypher", "CREATE (:Author {k:'a1'}), (:Author {k:'a2'})");
    database.command("opencypher", "CREATE (:Post {k:'p1'})");
    database.command("opencypher", "CREATE (:Post:Draft {k:'p2'})");
    database.command("opencypher", "CREATE (:Topic {k:'t1'}), (:Topic {k:'t2'})");

    database.command("opencypher", "MATCH (a:Author {k:'a1'}), (p:Post {k:'p1'}) CREATE (a)-[:WROTE]->(p)");
    database.command("opencypher", "MATCH (a:Author {k:'a1'}), (p:Post {k:'p2'}) CREATE (a)-[:WROTE]->(p)");
    database.command("opencypher", "MATCH (a:Author {k:'a2'}), (p:Post {k:'p1'}) CREATE (a)-[:WROTE]->(p)");
    database.command("opencypher", "MATCH (p:Post {k:'p1'}), (t:Topic {k:'t1'}) CREATE (p)-[:TAGGED]->(t)");
    database.command("opencypher", "MATCH (p:Post {k:'p2'}), (t:Topic {k:'t1'}) CREATE (p)-[:TAGGED]->(t)");

    // A self-loop is the only shape in which one physical edge can serve two same-typed, same-direction hops
    // of a path, which is what makes a skipped uniqueness check observable.
    database.command("opencypher", "MATCH (t:Topic {k:'t1'}) CREATE (t)-[:RELATED]->(t)");
    database.command("opencypher", "MATCH (t:Topic {k:'t1'}) CREATE (t)-[:OTHER]->(t)");
  }

  // ===================================================================================================
  // the four detectors that key an operator on one label per node
  // ===================================================================================================

  /**
   * {@code tryOptimizeMatchCountReturn}: the counted node's label becomes the step's target-type filter. The
   * reference is the same grouped count computed through {@code WITH}, which the push-down does not claim.
   */
  @Test
  void theCountedNodesLabelSetDeclinesTheEdgeCountPushDown() {
    final String query = "MATCH (a:Author)-[:WROTE]->(p:Post:Draft) RETURN a.k AS k, count(p) AS c";
    // Only a1 wrote the one post that is a Draft; a2 wrote only p1, so a2 contributes no row at all.
    assertThat(groupCountsOf(query)).containsExactly("a1=1");
    assertThat(groupCountsOf(query)).isEqualTo(groupCountsOf(
        "MATCH (a:Author)-[:WROTE]->(p:Post:Draft) WITH a.k AS k, count(p) AS c RETURN k, c"));
  }

  /**
   * The push-down the test above declines, on the single-label pattern where it is claimed, filters by the
   * label the way the pattern means it: a vertex written as {@code (:Post:Draft)} carries the composite type
   * {@code Draft~Post}, which extends {@code Post}, so {@code (p:Post)} matches it. The filter compared type
   * names for equality and read the type's own buckets rather than its polymorphic ones, so it counted a
   * strictly smaller set than the pattern asked for - the mirror image of the over-counts above.
   */
  @Test
  void theEdgeCountPushDownMatchesLabelsPolymorphically() {
    final String query = "MATCH (a:Author)-[:WROTE]->(p:Post) RETURN a.k AS k, count(p) AS c";
    assertThat(groupCountsOf(query)).containsExactlyInAnyOrder("a1=2", "a2=1");
    assertThat(groupCountsOf(query)).containsExactlyInAnyOrderElementsOf(groupCountsOf(
        "MATCH (a:Author)-[:WROTE]->(p:Post) WITH a.k AS k, count(p) AS c RETURN k, c"));
  }

  /** {@code tryDetectChainCountStar}: one label per hop in the chain's label array. */
  @Test
  void aChainHopsLabelSetDeclinesTheChainCountPushDown() {
    final String query = "MATCH (a:Author)-[:WROTE]->(p:Post:Draft)-[:TAGGED]->(t:Topic) RETURN count(*) AS c";
    assertThat(explainOf(query)).doesNotContain("COUNT CHAIN PATHS");
    assertThat(scalarOf(query)).isEqualTo(rowCountOf(
        "MATCH (a:Author)-[:WROTE]->(p:Post:Draft)-[:TAGGED]->(t:Topic) RETURN a")).isEqualTo(1);

    final String single = "MATCH (a:Author)-[:WROTE]->(p:Post)-[:TAGGED]->(t:Topic) RETURN count(*) AS c";
    assertThat(explainOf(single)).contains("COUNT CHAIN PATHS");
    assertThat(scalarOf(single)).isEqualTo(3);
  }

  /** {@code tryDetectAntiJoinChainCountStar}: the same label array, on the chain the anti-join filters. */
  @Test
  void aChainHopsLabelSetDeclinesTheAntiJoinCountPushDown() {
    final String query = "MATCH (a:Author)-[:WROTE]->(p:Post:Draft)-[:TAGGED]->(t:Topic)"
        + " WHERE NOT (a)-[:FOLLOWS]-(t) RETURN count(*) AS c";
    assertThat(explainOf(query)).doesNotContain("COUNT ANTI-JOIN CHAIN");
    assertThat(scalarOf(query)).isEqualTo(1);
  }

  /**
   * {@code tryDetectStarCountStar}: the central variable's label is the type the operator enumerates.
   * <p>
   * The contrasting {@code single} query below leaves both arms unlabelled on purpose: an arm endpoint's own
   * label is a separate defect (issue #6337, fixed after this test was written) that declines the push-down
   * regardless of what the central variable's label set looks like, so this comparison keeps that variable out
   * of the case being tested here.
   */
  @Test
  void theCentralVariablesLabelSetDeclinesTheStarCountPushDown() {
    final String query = "MATCH (p:Post:Draft)<-[:WROTE]-(:Author), (p)-[:TAGGED]->(:Topic) RETURN count(*) AS c";
    assertThat(explainOf(query)).doesNotContain("COUNT STAR JOIN");
    assertThat(scalarOf(query)).isEqualTo(1);

    final String single = "MATCH (p:Post)<-[:WROTE]-(), (p)-[:TAGGED]->() RETURN count(*) AS c";
    assertThat(explainOf(single)).contains("COUNT STAR JOIN");
    assertThat(scalarOf(single)).isEqualTo(3);
  }

  /**
   * The central variable is written once per arm, and the operator enumerates one type of central node, so two
   * arms naming different types cannot both be honoured by it.
   * <p>
   * The arms are left unlabelled on purpose, for the same reason as {@code single} above: a labelled arm
   * endpoint declines the push-down on its own (issue #6337), which would make this test pass even if the
   * central-label-conflict check it is named for stopped working.
   */
  @Test
  void conflictingCentralLabelsDeclineTheStarCountPushDown() {
    final String query = "MATCH (p:Post)<-[:WROTE]-(), (p:Draft)-[:TAGGED]->() RETURN count(*) AS c";
    assertThat(explainOf(query)).doesNotContain("COUNT STAR JOIN");
    assertThat(scalarOf(query)).isEqualTo(1);
  }

  // ===================================================================================================
  // the fifth: a disjointness proof that has to hold for every alternative
  // ===================================================================================================

  /**
   * {@code (x:Author|Topic)-[:RELATED]->(y:Topic)-[:RELATED]->(z)} has two hops of the same edge type. They are
   * proven to be different edges only if no vertex can be both the first hop's source and the second hop's
   * source, and t1 - a Topic with a RELATED self-loop - can be both. Reading {@code Author} alone proved a
   * disjointness that is not there, so the uniqueness check was skipped and the one self-loop was walked twice.
   * <p>
   * The second assertion is the same query with the two alternatives written the other way round. It always
   * answered 0; that the first spelling did not is what makes this a defect rather than a policy.
   */
  @Test
  void aDisjunctionIsOnlyDisjointWhenEveryAlternativeIs() {
    assertThat(rowCountOf("MATCH (x:Author|Topic)-[:RELATED]->(y:Topic)-[:RELATED]->(z) RETURN x")).isEqualTo(0);
    assertThat(rowCountOf("MATCH (x:Topic|Author)-[:RELATED]->(y:Topic)-[:RELATED]->(z) RETURN x")).isEqualTo(0);
  }

  /** Two hops of different edge types share no edge, so nothing about them changes: the self-loop pair matches. */
  @Test
  void distinctEdgeTypesAreUnaffected() {
    assertThat(rowCountOf("MATCH (x:Author|Topic)-[:RELATED]->(y:Topic)-[:OTHER]->(z) RETURN x")).isEqualTo(1);
  }

  // ===================================================================================================
  // helpers
  // ===================================================================================================

  /** The single {@code c} value of a one-row query. */
  private long scalarOf(final String query) {
    final List<Long> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        values.add(((Number) rs.next().getProperty("c")).longValue());
    }
    assertThat(values).as(query).hasSize(1);
    return values.getFirst();
  }

  /** The {@code k=c} pairs of a grouped count, as strings, so an assertion reads as the query does. */
  private List<String> groupCountsOf(final String query) {
    final List<String> groups = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final var row = rs.next();
        groups.add(row.getProperty("k") + "=" + ((Number) row.getProperty("c")).longValue());
      }
    }
    return groups;
  }

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
