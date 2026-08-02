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
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #5715: the two Cypher count push-downs - the O(1) {@code Type.count()} one and the CSR
 * one - disagreed on their preconditions and on their entry points.
 * <p>
 * One of the five is a wrong answer: the CSR push-down replaced the whole step chain and the {@code SKIP} / {@code
 * LIMIT} steps were never built, so {@code RETURN count(*) LIMIT 0} returned a row. The other four are a plan choice
 * rather than a result, and are measured as {@code readRecord} deltas from the database statistics, which separate
 * the two plans exactly: a push-down answers from the cached type counter or the CSR arrays and reads nothing, the
 * ordinary pipeline reads one record per vertex.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherCountPushDownPreconditionsIssue5715Test extends TestHelper {
  private static final int BIG    = 300;
  private static final int LONELY = 100;

  @Override
  protected void beginTest() {
    database.command("opencypher", "UNWIND range(1, " + BIG + ") AS i CREATE (:Big {k: i})");
    // A chain the CSR count push-down propagates through: 3 vertices, 2 edges.
    database.command("opencypher", "CREATE (q1:Q {k: 1})-[:LINKS]->(q2:Q {k: 2})-[:LINKS]->(q3:Q {k: 3})");
    // Vertices with no edge at all, and an edge type declared with no instance, so a chain over them can only be 0.
    database.command("opencypher", "UNWIND range(1, " + LONELY + ") AS i CREATE (:Lonely {k: i})");
    database.command("sql", "CREATE EDGE TYPE ISOLATED");
    // A lightweight edge type keeps no edge record, so its counter stays 0 while its edges exist.
    database.command("sql", "CREATE EDGE TYPE LIGHT LIGHTWEIGHT");
    database.command("opencypher", "MATCH (a:Q {k: 1}), (b:Q {k: 3}) CREATE (a)-[:LIGHT]->(b)");
  }

  // ===================================================================================================
  // 1. the wrong answer: SKIP and LIMIT apply to the one row a push-down produces
  // ===================================================================================================

  /**
   * The CSR push-down replaces the whole chain, so before this change the {@code SkipStep} and {@code LimitStep} of
   * the statement were never built and the count came back regardless. Neo4j applies both to the one-row aggregate
   * result: {@code LIMIT 0} and {@code SKIP 1} each return no rows.
   */
  @Test
  void skipAndLimitApplyToTheCsrCountPushDown() {
    assertThat(countsOf("MATCH (a:Q)-[:LINKS]->(b:Q) RETURN count(*) AS c")).containsExactly(2L);

    assertThat(countsOf("MATCH (a:Q)-[:LINKS]->(b:Q) RETURN count(*) AS c LIMIT 0")).isEmpty();
    assertThat(countsOf("MATCH (a:Q)-[:LINKS]->(b:Q) RETURN count(*) AS c SKIP 1")).isEmpty();
    assertThat(countsOf("MATCH (a:Q)-[:LINKS]->(b:Q) RETURN count(*) AS c SKIP 5")).isEmpty();

    assertThat(countsOf("MATCH (a:Q)-[:LINKS]->(b:Q) RETURN count(*) AS c LIMIT 1")).containsExactly(2L);
    assertThat(countsOf("MATCH (a:Q)-[:LINKS]->(b:Q) RETURN count(*) AS c SKIP 0 LIMIT 1")).containsExactly(2L);
  }

  /** The other push-down already answered these correctly - by giving up on them. It now keeps the fast path. */
  @Test
  void skipAndLimitApplyToTheTypeCountPushDown() {
    assertThat(countsOf("MATCH (m:Big) RETURN count(m) AS c")).containsExactly((long) BIG);

    assertThat(countsOf("MATCH (m:Big) RETURN count(m) AS c LIMIT 0")).isEmpty();
    assertThat(countsOf("MATCH (m:Big) RETURN count(m) AS c SKIP 1")).isEmpty();

    assertThat(countsOf("MATCH (m:Big) RETURN count(m) AS c LIMIT 1")).containsExactly((long) BIG);
    assertThat(countsOf("MATCH (m:Big) RETURN count(m) AS c SKIP 0")).containsExactly((long) BIG);
  }

  /**
   * The values are evaluated rather than read off the statement, so a parameter is the case that would break if the
   * steps were built with the wrong context - and building them on the push-down path is the whole fix.
   */
  @Test
  void aParametrizedSkipAndLimitApplyToo() {
    final String query = "MATCH (a:Q)-[:LINKS]->(b:Q) RETURN count(*) AS c SKIP $s LIMIT $l";
    assertThat(countsOf(query, Map.of("s", 0, "l", 1))).containsExactly(2L);
    assertThat(countsOf(query, Map.of("s", 0, "l", 0))).isEmpty();
    assertThat(countsOf(query, Map.of("s", 1, "l", 1))).isEmpty();

    final String typeCount = "MATCH (m:Big) RETURN count(m) AS c SKIP $s LIMIT $l";
    assertThat(countsOf(typeCount, Map.of("s", 0, "l", 1))).containsExactly((long) BIG);
    assertThat(countsOf(typeCount, Map.of("s", 1, "l", 1))).isEmpty();
  }

  /**
   * And the point of applying them rather than rejecting the statement that carries them: a count written with a
   * harmless {@code LIMIT 1} costs exactly what the same count costs without one.
   */
  @Test
  void aSkippedOrLimitedCountIsStillAnsweredByThePushDown() {
    for (final String query : List.of("MATCH (m:Big) RETURN count(m) AS c",
        "MATCH (m:Big) RETURN count(*) AS c",
        "MATCH (a:Q)-[:LINKS]->(b:Q) RETURN count(*) AS c")) {
      final long plain = recordsReadBy(query);
      assertThat(recordsReadBy(query + " LIMIT 1")).as(query + " LIMIT 1").isEqualTo(plain);
      assertThat(recordsReadBy(query + " SKIP 1")).as(query + " SKIP 1").isEqualTo(plain);
    }
  }

  /**
   * {@code ORDER BY} cannot reorder a single row, but it is rejected rather than ignored: it is evaluated by the
   * ordinary pipeline, which is the only thing here that knows what an aggregate alias sorts by.
   */
  @Test
  void anOrderedCountIsStillCorrect() {
    assertThat(countsOf("MATCH (a:Q)-[:LINKS]->(b:Q) RETURN count(*) AS c ORDER BY c")).containsExactly(2L);
    assertThat(countsOf("MATCH (m:Big) RETURN count(m) AS c ORDER BY c DESC")).containsExactly((long) BIG);
    assertThat(countsOf("MATCH (m:Big) RETURN count(m) AS c ORDER BY c LIMIT 0")).isEmpty();
  }

  /**
   * A push-down replaces the whole chain, so the query never reaches the optimizer - and {@code EXPLAIN} described it
   * by the physical plan the optimizer had built for it anyway, naming a plan the engine does not run. That gap
   * predates this issue, which widens it by sending the plainest counting queries down the fast path too.
   */
  @Test
  void explainDescribesThePlanThatActuallyRuns() {
    assertThat(explainOf("MATCH (m:Big) RETURN count(m) AS c")).contains("Count Push-Down", "TYPE COUNT");
    assertThat(explainOf("MATCH (m:Big) RETURN count(*) AS c")).contains("Count Push-Down");
    assertThat(explainOf("MATCH (a:Q)-[:LINKS]->(b:Q) RETURN count(*) AS c")).contains("Count Push-Down", "COUNT CHAIN");
    // The SKIP and LIMIT steps this issue added to the fast path are part of the plan, so they are described with it.
    assertThat(explainOf("MATCH (m:Big) RETURN count(m) AS c SKIP 1 LIMIT 2")).contains("SKIP", "LIMIT");
    // The early-out says why it can answer without reading.
    assertThat(explainOf("MATCH (a:Lonely)-[:ISOLATED]->(b:Lonely) RETURN count(*) AS c")).contains("CONSTANT COUNT");

    // A query no push-down claims is still described by whatever does run it.
    assertThat(explainOf("MATCH (m:Big) WHERE m.k <= 5 RETURN count(*) AS c")).doesNotContain("Count Push-Down");
  }

  // ===================================================================================================
  // 2. the simplest counting query there is now takes the fast path
  // ===================================================================================================

  /**
   * {@code tryCreateTypeCountOptimization} was only reachable from {@code buildExecutionStepsWithOrder}, which a
   * top-level query reaches only when the optimizer declines it. A single-label {@code MATCH ... RETURN count(v)}
   * satisfies the optimizer, so it scanned - while the identical body inside a subquery was answered in O(1).
   */
  @Test
  void aTopLevelTypeCountIsAnsweredByThePushDown() {
    assertThat(countsOf("MATCH (m:Big) RETURN count(m) AS c")).containsExactly((long) BIG);
    assertThat(recordsReadBy("MATCH (m:Big) RETURN count(m) AS c")).isZero();
    // The subquery body, which already had it, is unchanged.
    assertThat(recordsReadBy("RETURN COLLECT { MATCH (m:Big) RETURN count(m) } AS c")).isZero();
  }

  /**
   * {@code count(*)} on a single-node pattern fell between the two detectors: the type-count one required the
   * argument to name the MATCH variable, and every CSR detector requires at least one relationship. For a pattern
   * producing exactly one row per node the two counts are the same.
   */
  @Test
  void countStarOnASingleNodePatternIsAnsweredByThePushDown() {
    assertThat(countsOf("MATCH (m:Big) RETURN count(*) AS c")).containsExactly((long) BIG);
    assertThat(recordsReadBy("MATCH (m:Big) RETURN count(*) AS c")).isZero();

    // The variable is not needed when nothing names it.
    assertThat(countsOf("MATCH (:Big) RETURN count(*) AS c")).containsExactly((long) BIG);
    assertThat(recordsReadBy("MATCH (:Big) RETURN count(*) AS c")).isZero();

    assertThat(recordsReadBy("RETURN COLLECT { MATCH (m:Big) RETURN count(*) } AS c")).isZero();
  }

  /** What must keep scanning: a filter, a property, a second label and a non-vertex label are all still refused. */
  @Test
  void aCountThePushDownCannotAnswerStillScans() {
    assertThat(countsOf("MATCH (m:Big) WHERE m.k <= 5 RETURN count(*) AS c")).containsExactly(5L);
    assertThat(countsOf("MATCH (m:Big {k: 7}) RETURN count(*) AS c")).containsExactly(1L);
    assertThat(countsOf("MATCH (m:Big) RETURN count(m.k) AS c")).containsExactly((long) BIG);
    assertThat(countsOf("MATCH (m:LINKS) RETURN count(*) AS c")).containsExactly(0L);
    assertThat(countsOf("MATCH (m:NoSuchLabel) RETURN count(*) AS c")).containsExactly(0L);
  }

  // ===================================================================================================
  // 3. COUNT { } over a body whose row count is the match count
  // ===================================================================================================

  /**
   * A {@code COUNT { }} body with no {@code RETURN} of its own is normalised to one row per match, and the
   * expression counted those rows: a materialised scan for a number the type counter already holds.
   */
  @Test
  void countOverABodyThatProducesOneRowPerMatchIsAnsweredByThePushDown() {
    for (final String query : List.of(
        "RETURN COUNT { MATCH (m:Big) } AS c",
        "RETURN COUNT { (m:Big) } AS c",
        "RETURN COUNT { MATCH (m:Big) RETURN m } AS c",
        "RETURN COUNT { MATCH (m:Big) RETURN 1 } AS c")) {
      assertThat(scalarOf(query)).as(query).isEqualTo(BIG);
      assertThat(recordsReadBy(query)).as(query).isZero();
    }

    final String chain = "RETURN COUNT { MATCH (a:Q)-[:LINKS]->(b:Q) } AS c";
    assertThat(scalarOf(chain)).isEqualTo(2L);
  }

  /**
   * The widest bodies this accepts, pinned because they are outside the shape the other push-down takes - a
   * {@code RETURN} of exactly one count item - and nothing else says which way they go. No projection can add or drop
   * a row, so for an uncorrelated body the row count is the match count whatever it names.
   */
  @Test
  void aBodyProjectingSeveralItemsOrEverythingIsStillOneRowPerMatch() {
    for (final String query : List.of(
        "RETURN COUNT { MATCH (m:Big) RETURN * } AS c",
        "RETURN COUNT { MATCH (m:Big) RETURN m, m.k } AS c",
        "RETURN COUNT { MATCH (m:Big) RETURN m.k AS a, m.k AS b } AS c")) {
      assertThat(scalarOf(query)).as(query).isEqualTo(BIG);
      assertThat(recordsReadBy(query)).as(query).isZero();
    }
  }

  /** A body whose row count is not the match count keeps the ordinary pipeline, and its answer. */
  @Test
  void countOverABodyThatDoesNotProduceOneRowPerMatchIsNotPushedDown() {
    assertThat(scalarOf("RETURN COUNT { MATCH (m:Big) RETURN count(m) } AS c")).isEqualTo(1L);
    assertThat(scalarOf("RETURN COUNT { MATCH (m:Big) RETURN m LIMIT 7 } AS c")).isEqualTo(7L);
    assertThat(scalarOf("RETURN COUNT { MATCH (m:Big) RETURN m SKIP " + (BIG - 4) + " } AS c")).isEqualTo(4L);
    assertThat(scalarOf("RETURN COUNT { MATCH (m:Big) RETURN DISTINCT 1 } AS c")).isEqualTo(1L);
    assertThat(scalarOf("RETURN COUNT { MATCH (m:Big) WHERE m.k <= 5 } AS c")).isEqualTo(5L);
  }

  /** And a correlated body is still answered against the row it is correlated to, not against the whole type. */
  @Test
  void aCorrelatedCountBodyIsStillNotAnsweredByTheGlobalCount() {
    assertThat(countsOf("MATCH (q:Q {k: 1}) RETURN COUNT { (q)-[:LINKS]->(:Q) } AS c")).containsExactly(1L);
    assertThat(countsOf("MATCH (q:Q {k: 1}) RETURN COUNT { MATCH (q:Q) } AS c")).containsExactly(1L);
  }

  // ===================================================================================================
  // 4. a chain that cannot match anything is answered without reading anything
  // ===================================================================================================

  /**
   * The CSR push-down was applied unconditionally: 100 {@code :Lonely} vertices with no edge at all cost 200 record
   * reads to answer 0, and an edge type absent from the schema cost the same.
   */
  @Test
  void aChainThatCannotMatchIsAnsweredWithoutReadingAnything() {
    for (final String query : List.of(
        "MATCH (a:Lonely)-[:NOSUCHTYPE]->(b:Lonely) RETURN count(*) AS c",
        "MATCH (a:Lonely)-[:ISOLATED]->(b:Lonely) RETURN count(*) AS c",
        "MATCH (a:Lonely)-[:ISOLATED]->(b:Lonely)-[:LINKS]->(c:Q) RETURN count(*) AS c",
        "MATCH (a:Q)-[:LINKS]->(b:NoSuchLabel) RETURN count(*) AS c",
        "MATCH (a:NoSuchLabel)-[:LINKS]->(b:Q) RETURN count(*) AS c")) {
      assertThat(countsOf(query)).as(query).containsExactly(0L);
      assertThat(recordsReadBy(query)).as(query).isZero();
    }

    // The same answer through the other door, where the push-down is asked for a row count.
    final String body = "RETURN COUNT { MATCH (a:Lonely)-[:ISOLATED]->(b:Lonely) } AS c";
    assertThat(scalarOf(body)).isZero();
    assertThat(recordsReadBy(body)).isZero();
  }

  /**
   * The one type whose record count says nothing about whether it has instances. A LIGHTWEIGHT edge lives in the
   * vertices' edge lists and writes no record, so its counter is 0 while its edges are there to be counted: reading
   * that 0 as "cannot match" would turn the early-out into a wrong answer.
   */
  @Test
  void aLightweightEdgeTypeIsNeverTakenForAnEmptyOne() {
    assertThat(database.countType("LIGHT", true)).isZero();
    assertThat(countsOf("MATCH (a:Q)-[:LIGHT]->(b:Q) RETURN count(*) AS c")).containsExactly(1L);
    assertThat(rowCountOf("MATCH (a:Q)-[:LIGHT]->(b:Q) RETURN a.k AS c")).isEqualTo(1);
  }

  /** The early-out only fires when the answer must be 0: a chain that does match is untouched. */
  @Test
  void aChainThatCanMatchIsUnaffected() {
    assertThat(countsOf("MATCH (a:Q)-[:LINKS]->(b:Q) RETURN count(*) AS c")).containsExactly(2L);
    assertThat(countsOf("MATCH (a:Q)-[:LINKS]->(b:Q)-[:LINKS]->(c:Q) RETURN count(*) AS c")).containsExactly(1L);
  }

  // ===================================================================================================
  // 5. two wrong answers the same code had, both from a label the operator could not read
  // ===================================================================================================

  /**
   * Every CSR operator walks out from one labelled position of the pattern and enumerates that label's buckets. An
   * <b>unlabelled</b> anchor left it with no bucket set, which each operator read as an empty one and answered 0 -
   * for a pattern that does match. The push-down is not built for such a pattern any more.
   */
  @Test
  void anUnlabelledAnchorIsNotAnsweredWithZero() {
    assertThat(countsOf("MATCH (a)-[:LINKS]->(b) RETURN count(*) AS c")).containsExactly(2L);
    assertThat(countsOf("MATCH (a)-[:LINKS]->(b:Q) RETURN count(*) AS c")).containsExactly(2L);
    assertThat(countsOf("MATCH (a:Q)-[:LINKS]->(b) RETURN count(*) AS c")).containsExactly(2L);
    // The count is what the ordinary pipeline produces one row per.
    assertThat(rowCountOf("MATCH (a)-[:LINKS]->(b) RETURN a.k AS c")).isEqualTo(2);
  }

  /**
   * The mirror of it: a label declared on a hop but absent from the schema was read as "no filter" rather than as
   * "matches nothing", so the chain was counted unfiltered - 2 for a pattern the ordinary pipeline answers with no
   * row at all.
   */
  @Test
  void aHopLabelThatMatchesNothingIsNotIgnored() {
    assertThat(countsOf("MATCH (a:Q)-[:LINKS]->(b:NoSuchLabel) RETURN count(*) AS c")).containsExactly(0L);
    assertThat(rowCountOf("MATCH (a:Q)-[:LINKS]->(b:NoSuchLabel) RETURN b.k AS c")).isZero();
    assertThat(countsOf("MATCH (a:Q)-[:LINKS]->(b:Lonely) RETURN count(*) AS c")).containsExactly(0L);
  }

  // ===================================================================================================
  // helpers
  // ===================================================================================================

  /** The {@code c} column of every row, as longs. An empty list is a query that returned no row at all. */
  private List<Long> countsOf(final String query) {
    return countsOf(query, Map.of());
  }

  private List<Long> countsOf(final String query, final Map<String, Object> parameters) {
    final List<Long> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, parameters)) {
      while (rs.hasNext())
        values.add(((Number) rs.next().getProperty("c")).longValue());
    }
    return values;
  }

  /** The single {@code c} value of a one-row query. */
  private long scalarOf(final String query) {
    final List<Long> values = countsOf(query);
    assertThat(values).as(query).hasSize(1);
    return values.get(0);
  }

  /** The rendered execution plan of {@code EXPLAIN <query>}. */
  private String explainOf(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      assertThat(rs.hasNext()).as(query).isTrue();
      return rs.next().getProperty("executionPlanAsString");
    }
  }

  /** How many rows the query produced, whatever they hold. */
  private int rowCountOf(final String query) {
    int rows = 0;
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
    }
    return rows;
  }

  /** The records the query materialized: zero when the count was answered from the counter or the CSR arrays. */
  private long recordsReadBy(final String query) {
    final long before = readRecords();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        row.getPropertyNames();
      }
    }
    return readRecords() - before;
  }

  private long readRecords() {
    return ((Number) database.getStats().get("readRecord")).longValue();
  }
}
