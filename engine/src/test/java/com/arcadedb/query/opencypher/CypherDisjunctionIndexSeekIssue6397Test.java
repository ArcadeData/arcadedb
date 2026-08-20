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
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6397: a label disjunction ({@code (n:A|B {id: $x})}) always executed as
 * {@code NodeByLabelDisjunctionScan}, a full scan of every alternative's type, even when every alternative has an
 * index that could resolve the equality predicate with a seek.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherDisjunctionIndexSeekIssue6397Test extends TestHelper {
  @Override
  protected void beginTest() {
    database.transaction(() -> {
      final var typeA = database.getSchema().createVertexType("Alpha6397");
      typeA.createProperty("id", String.class);
      typeA.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");

      final var typeB = database.getSchema().createVertexType("Bravo6397");
      typeB.createProperty("id", String.class);
      typeB.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");

      // No index at all: used to prove the all-or-nothing fallback still returns correct rows.
      final var typeC = database.getSchema().createVertexType("Charlie6397");
      typeC.createProperty("id", String.class);

      // A second, non-indexed property on the indexed types: used to prove a disjunction with more than one
      // equality predicate still finds the indexed one regardless of which the planner sees first.
      typeA.createProperty("tag", String.class);
      typeB.createProperty("tag", String.class);

      // A composite index (id, tag), on its own pair of types so it does not interact with the single-column
      // index above: used to prove the disjunction seek's per-root prefix values are propagated, not collapsed
      // to a single-column seek regardless of how many columns the matched index actually has.
      final var typeE = database.getSchema().createVertexType("Echo6397");
      typeE.createProperty("id", String.class);
      typeE.createProperty("tag", String.class);
      typeE.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id", "tag");

      final var typeF = database.getSchema().createVertexType("Foxtrot6397");
      typeF.createProperty("id", String.class);
      typeF.createProperty("tag", String.class);
      typeF.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id", "tag");
    });

    database.transaction(() -> {
      for (int i = 0; i < 50; i++)
        database.command("opencypher", "CREATE (:Alpha6397 {id: $id, tag: 'unindexed'})", Map.of("id", "a" + i));
      for (int i = 0; i < 50; i++)
        database.command("opencypher", "CREATE (:Bravo6397 {id: $id, tag: 'unindexed'})", Map.of("id", "b" + i));
      for (int i = 0; i < 5; i++)
        database.command("opencypher", "CREATE (:Charlie6397 {id: $id})", Map.of("id", "c" + i));
      for (int i = 0; i < 20; i++)
        database.command("opencypher", "CREATE (:Echo6397 {id: $id, tag: $tag})", Map.of("id", "e" + i, "tag", "t" + i));
      for (int i = 0; i < 20; i++)
        database.command("opencypher", "CREATE (:Foxtrot6397 {id: $id, tag: $tag})", Map.of("id", "f" + i, "tag", "t" + i));
    });
  }

  @Test
  void inlinePropertyEqualityOnEveryIndexedAlternativeUsesASeek() {
    final String plan = profilePlan("MATCH (n:Alpha6397|Bravo6397 {id: 'a1'}) RETURN n.id AS k");
    assertThat(plan).as("plan\n%s", plan)
        .contains("NodeByLabelDisjunctionIndexSeek")
        .doesNotContain("NodeByLabelDisjunctionScan");

    assertThat(ids("MATCH (n:Alpha6397|Bravo6397 {id: 'a1'}) RETURN n.id AS k")).containsExactly("a1");
  }

  @Test
  void whereClauseEqualityOnEveryIndexedAlternativeUsesASeek() {
    final String query = "MATCH (n:Alpha6397|Bravo6397) WHERE n.id = 'b7' RETURN n.id AS k";
    final String plan = profilePlan(query);
    assertThat(plan).as("plan\n%s", plan)
        .contains("NodeByLabelDisjunctionIndexSeek")
        .doesNotContain("NodeByLabelDisjunctionScan");

    assertThat(ids(query)).containsExactly("b7");
  }

  @Test
  void oneNonIndexedAlternativeFallsBackToTheFullScanButStaysCorrect() {
    final String plan = profilePlan("MATCH (n:Alpha6397|Charlie6397 {id: 'a2'}) RETURN n.id AS k");
    assertThat(plan).as("all-or-nothing: Charlie6397 has no index, so the whole disjunction stays a scan\n%s", plan)
        .contains("NodeByLabelDisjunctionScan")
        .doesNotContain("NodeByLabelDisjunctionIndexSeek");

    assertThat(ids("MATCH (n:Alpha6397|Charlie6397 {id: 'a2'}) RETURN n.id AS k")).containsExactly("a2");
    assertThat(ids("MATCH (n:Alpha6397|Charlie6397 {id: 'c3'}) RETURN n.id AS k")).containsExactly("c3");
  }

  @Test
  void aValueNoAlternativeHasReturnsNothing() {
    assertThat(ids("MATCH (n:Alpha6397|Bravo6397 {id: 'nope'}) RETURN n.id AS k")).isEmpty();
  }

  @Test
  void aTypeMultiplyInheritingFromTwoIndexedAlternativesIsReturnedOnceNotTwice() {
    database.command("sql", "CREATE VERTEX TYPE Delta6397 EXTENDS Alpha6397, Bravo6397");
    database.transaction(() -> database.command("sql", "INSERT INTO Delta6397 SET id = 'diamond'"));

    final String query = "MATCH (n:Alpha6397|Bravo6397 {id: 'diamond'}) RETURN n.id AS k";
    assertThat(profilePlan(query)).contains("NodeByLabelDisjunctionIndexSeek");
    assertThat(ids(query)).containsExactly("diamond");
  }

  /**
   * A disjunction with two equality predicates, only one of which ({@code id}) has an index on every root, must
   * still be seeked: the planner must not give up just because a {@code HashMap} happened to offer the
   * non-indexed predicate ({@code tag}) first.
   */
  @Test
  void aSecondNonIndexedPredicateDoesNotBlockTheSeekOnTheIndexedOne() {
    final String query = "MATCH (n:Alpha6397|Bravo6397 {id: 'a3', tag: 'unindexed'}) RETURN n.id AS k";
    assertThat(profilePlan(query)).as("plan\n%s", profilePlan(query))
        .contains("NodeByLabelDisjunctionIndexSeek")
        .doesNotContain("NodeByLabelDisjunctionScan");
    assertThat(ids(query)).containsExactly("a3");

    // The non-indexed predicate must still be enforced, not silently dropped by the seek.
    assertThat(ids("MATCH (n:Alpha6397|Bravo6397 {id: 'a3', tag: 'nope'}) RETURN n.id AS k")).isEmpty();
  }

  /**
   * Every root's own composite index (id, tag) must be used with BOTH columns pinned down - a single-column
   * prefix seek would still be correct but strictly weaker, and the plan should show the full key was resolved.
   */
  @Test
  void compositeIndexOnEveryRootResolvesTheFullKeyNotJustAPrefix() {
    final String query = "MATCH (n:Echo6397|Foxtrot6397 {id: 'e5', tag: 't5'}) RETURN n.id AS k";
    final String plan = profilePlan(query);
    assertThat(plan).as("plan\n%s", plan)
        .contains("NodeByLabelDisjunctionIndexSeek")
        .contains("id=e5")
        .contains("tag=t5");

    assertThat(ids(query)).containsExactly("e5");
    // A tag that does not match the id's own row must still be rejected, proving both key columns are live.
    assertThat(ids("MATCH (n:Echo6397|Foxtrot6397 {id: 'e5', tag: 't6'}) RETURN n.id AS k")).isEmpty();
  }

  /**
   * Regression for the review of this fix: a composite-index seek that only pins the leading column (a prefix
   * range, not a single-entry lookup) must not be costed as if the whole key had been resolved. The row estimate
   * for the prefix-only seek (id alone, {@code selectivity=0.1}) must be strictly higher than for the same
   * predicate with both columns bound (id + tag, {@code selectivity=0.001}) - both queries visit the same two
   * types (Echo6397, Foxtrot6397; 40 total rows), so the difference isolates the selectivity the planner picked.
   * This is a cost-estimate concern only: {@link #compositeIndexOnEveryRootResolvesTheFullKeyNotJustAPrefix}
   * already covers that both shapes still return the right rows.
   */
  @Test
  void aPrefixOnlyCompositeSeekIsCostedLessSelectivelyThanAFullyResolvedOne() {
    final String fullKeyPlan = profilePlan("MATCH (n:Echo6397|Foxtrot6397 {id: 'e5', tag: 't5'}) RETURN n.id AS k");
    final String prefixOnlyPlan = profilePlan("MATCH (n:Echo6397|Foxtrot6397 {id: 'e5'}) RETURN n.id AS k");

    assertThat(prefixOnlyPlan).as("plan\n%s", prefixOnlyPlan).contains("NodeByLabelDisjunctionIndexSeek");

    assertThat(rowsEstimate(fullKeyPlan)).as("full-key plan\n%s", fullKeyPlan).isEqualTo(2L);
    assertThat(rowsEstimate(prefixOnlyPlan)).as("prefix-only plan\n%s", prefixOnlyPlan).isEqualTo(4L);
  }

  /** Extracts the outermost {@code rows=N} the NodeByLabelDisjunctionIndexSeek line reports. */
  private long rowsEstimate(final String plan) {
    final Matcher matcher = Pattern.compile("NodeByLabelDisjunctionIndexSeek.*?rows=(\\d+)").matcher(plan);
    assertThat(matcher.find()).as("plan contains a NodeByLabelDisjunctionIndexSeek rows= figure\n%s", plan).isTrue();
    return Long.parseLong(matcher.group(1));
  }

  /**
   * Regression for the review of this fix: {@code AnchorSelection.toString()} (and therefore
   * {@code PhysicalPlan.explain()}/{@code AbstractExecutionStep.prettyPrint()}) must not NPE for a
   * disjunction-index-seek anchor. That combination - {@code useIndex() == true} with no single {@code index} -
   * is only reachable through {@code EXPLAIN} on a UNION branch (each branch is described through
   * {@code appendPlanDescription}'s {@code appendStepChain}, which calls {@code prettyPrint} on every step of the
   * branch's own optimized physical plan, including the wrapper step that prints the anchor) - PROFILE and a
   * plain top-level EXPLAIN both print the anchor through the always-safe
   * {@code PhysicalOperator.explain(0)} tree instead, so neither of those exercises this path.
   */
  @Test
  void explainOnAUnionBranchWithADisjunctionSeekAnchorDoesNotThrow() {
    final String query = """
        MATCH (n:Alpha6397|Bravo6397 {id: 'a1'}) RETURN n.id AS k \
        UNION \
        MATCH (m:Charlie6397) RETURN m.id AS k""";

    final String plan = explain(query);
    assertThat(plan).as("plan\n%s", plan)
        .contains("UNION")
        .contains("Branch 1:")
        .contains("OPTIMIZED MATCH")
        .contains("NodeByLabelDisjunctionIndexSeek");
  }

  private String explain(final String cypher) {
    final StringBuilder plan = new StringBuilder();
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher", "EXPLAIN " + cypher);
      plan.append(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2));
      rs.close();
    });
    return plan.toString();
  }

  private List<String> ids(final String cypher) {
    final List<String> result = new ArrayList<>();
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher", cypher);
      while (rs.hasNext())
        result.add(rs.next().getProperty("k"));
      rs.close();
    });
    return result;
  }

  private String profilePlan(final String cypher) {
    final StringBuilder plan = new StringBuilder();
    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher", "PROFILE " + cypher);
      while (rs.hasNext())
        rs.next();
      plan.append(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2));
      rs.close();
    });
    return plan.toString();
  }
}
