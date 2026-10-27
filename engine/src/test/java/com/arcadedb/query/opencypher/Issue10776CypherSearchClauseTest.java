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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Issue #10776: the Cypher 25 {@code SEARCH} subclause of a {@code MATCH} was parsed and dropped, so
 * {@code MATCH (d:Doc) SEARCH d IN (VECTOR INDEX ... LIMIT 3)} answered every {@code Doc} and a {@code SCORE AS} variable was
 * undefined. Fixture: 30 {@code Doc} vertices {@code d0..d29} with the EUCLIDEAN vector {@code [i, 1]}, so the neighbors of
 * {@code [0, 1]} are {@code d0, d1, d2, ...} in that order, with the score {@code 1 / (1 + i^2)}.
 */
class Issue10776CypherSearchClauseTest extends TestHelper {
  private static final String              INDEX = "`Doc[vector]`";
  private static final Map<String, Object> Q     = Map.of("q", List.of(0f, 1f));

  @BeforeEach
  void createDocs() {
    database.command("sql", "CREATE VERTEX TYPE Doc");
    database.command("sql", "CREATE PROPERTY Doc.name STRING");
    database.command("sql", "CREATE PROPERTY Doc.vector ARRAY_OF_FLOATS");
    database.command("sql", "CREATE VERTEX TYPE Cat");
    database.command("sql", "CREATE EDGE TYPE IN");
    database.command("sql",
        "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": 2, \"similarity\": \"EUCLIDEAN\" }");
    database.transaction(() -> {
      final MutableVertex cat = database.newVertex("Cat").set("name", "even").save();
      for (int i = 0; i < 30; i++) {
        final MutableVertex doc = database.newVertex("Doc").set("name", "d" + i, "i", i, "vector", new float[] { i, 1f }).save();
        if (i % 2 == 0)
          doc.newEdge("IN", cat).save();
      }
    });
  }

  @Test
  void searchWithoutScoreAnswersOnlyTheKNearest() {
    assertThat(names("MATCH (d:Doc) SEARCH d IN (VECTOR INDEX " + INDEX + " FOR $q LIMIT 3) RETURN d.name AS name", Q))
        .containsExactly("d0", "d1", "d2");
  }

  @Test
  void scoreAsBindsTheNeo4jScore() {
    final List<Result> rows = rows(
        "MATCH (d:Doc) SEARCH d IN (VECTOR INDEX " + INDEX + " FOR $q LIMIT 3) SCORE AS s RETURN d.name AS name, s ORDER BY s DESC",
        Q);
    assertThat(rows).hasSize(3);
    for (int i = 0; i < 3; i++) {
      assertThat(rows.get(i).<String>getProperty("name")).isEqualTo("d" + i);
      assertThat(((Number) rows.get(i).getProperty("s")).doubleValue()).isCloseTo(1.0 / (1.0 + i * i), within(1e-5));
    }
  }

  @Test
  void whereInsideTheSearchFiltersBeforeTheLimit() {
    assertThat(names("MATCH (d:Doc) SEARCH d IN (VECTOR INDEX " + INDEX
        + " FOR $q WHERE d.name <> 'd0' LIMIT 3) SCORE AS s RETURN d.name AS name, s", Q)).containsExactly("d1", "d2", "d3");
  }

  @Test
  void selectiveWhereInsideTheSearchWidensUntilKRowsMatch() {
    // Only d9, d19 and d29 match: the first k=3 candidates hold none of them, so the search must widen past 29 candidates
    assertThat(names("MATCH (d:Doc) SEARCH d IN (VECTOR INDEX " + INDEX
        + " FOR $q WHERE d.i % 10 = 9 LIMIT 3) RETURN d.name AS name", Q)).containsExactly("d9", "d19", "d29");
  }

  @Test
  void whereInsideTheSearchReadsVariablesOfEarlierClauses() {
    assertThat(names("WITH 'd1' AS skip MATCH (d:Doc) SEARCH d IN (VECTOR INDEX " + INDEX
        + " FOR $q WHERE d.name <> skip LIMIT 2) RETURN d.name AS name", Q)).containsExactly("d0", "d2");
  }

  @Test
  void whereOfTheMatchFiltersAfterTheSearch() {
    // After SEARCH: a post-filter of the 3 nearest
    assertThat(names("MATCH (d:Doc) SEARCH d IN (VECTOR INDEX " + INDEX
        + " FOR $q LIMIT 3) WHERE d.name <> 'd0' RETURN d.name AS name", Q)).containsExactly("d1", "d2");
    // Before SEARCH: the same clause WHERE, spelled first
    assertThat(names("MATCH (d:Doc) WHERE d.name <> 'd0' SEARCH d IN (VECTOR INDEX " + INDEX
        + " FOR $q LIMIT 3) RETURN d.name AS name", Q)).containsExactly("d1", "d2");
  }

  @Test
  void patternOfTheMatchFiltersAfterTheSearch() {
    // Of the 3 nearest, only the even ones have an IN edge
    assertThat(names("MATCH (d:Doc)-[:IN]->(c:Cat) SEARCH d IN (VECTOR INDEX " + INDEX
        + " FOR $q LIMIT 3) RETURN d.name AS name, c.name AS cat", Q)).containsExactly("d0", "d2");
  }

  @Test
  void queryVectorAndLimitAreEvaluatedPerInputRow() {
    assertThat(names("UNWIND [[0.0, 1.0], [29.0, 1.0]] AS v MATCH (d:Doc) SEARCH d IN (VECTOR INDEX " + INDEX
        + " FOR v LIMIT 1) RETURN d.name AS name", Map.of())).containsExactly("d0", "d29");
    assertThat(names("MATCH (q:Doc {name: 'd10'}) MATCH (d:Doc) SEARCH d IN (VECTOR INDEX " + INDEX
        + " FOR q.vector LIMIT 1) RETURN d.name AS name", Map.of())).containsExactly("d10");
    assertThat(names("WITH 2 AS k MATCH (d:Doc) SEARCH d IN (VECTOR INDEX " + INDEX
        + " FOR $q LIMIT k) RETURN d.name AS name", Q)).containsExactly("d0", "d1");
  }

  @Test
  void searchRunsInsideACallSubquery() {
    assertThat(names("CALL () { MATCH (d:Doc) SEARCH d IN (VECTOR INDEX " + INDEX
        + " FOR $q LIMIT 2) SCORE AS s RETURN d, s } RETURN d.name AS name ORDER BY s DESC", Q)).containsExactly("d0", "d1");
  }

  @Test
  void indexNameCanBeAStringParameter() {
    assertThat(names("MATCH (d:Doc) SEARCH d IN (VECTOR INDEX $idx FOR $q LIMIT 2) RETURN d.name AS name",
        Map.of("q", List.of(0f, 1f), "idx", "Doc[vector]"))).containsExactly("d0", "d1");
  }

  @Test
  void bindingVariableMustBeANodeOfTheMatchPattern() {
    assertThatThrownBy(() -> names("MATCH (d:Doc) SEARCH x IN (VECTOR INDEX " + INDEX + " FOR $q LIMIT 2) RETURN d", Q))
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("'x'");
  }

  @Test
  void bindingVariableBoundByAnEarlierClauseIsRestrictedNotRebound() {
    // d5 is not among the 2 nearest: the row goes, rather than d being rebound to d0 and d1
    assertThat(names("MATCH (d:Doc {name: 'd5'}) WITH d MATCH (d) SEARCH d IN (VECTOR INDEX " + INDEX
        + " FOR $q LIMIT 2) RETURN d.name AS name", Q)).isEmpty();
    // d1 is: the row stays, with its score
    final List<Result> rows = rows("MATCH (d:Doc {name: 'd1'}) WITH d MATCH (d) SEARCH d IN (VECTOR INDEX " + INDEX
        + " FOR $q LIMIT 2) SCORE AS s RETURN d.name AS name, s", Q);
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst().<String>getProperty("name")).isEqualTo("d1");
    assertThat(((Number) rows.getFirst().getProperty("s")).doubleValue()).isCloseTo(0.5, within(1e-5));
  }

  @Test
  void optionalMatchWithSearchIsRefused() {
    assertThatThrownBy(() -> names("OPTIONAL MATCH (d:Doc) SEARCH d IN (VECTOR INDEX " + INDEX
        + " FOR $q LIMIT 2) RETURN d.name AS name", Q)).isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("OPTIONAL MATCH");
  }

  @Test
  void unknownIndexIsAnError() {
    assertThatThrownBy(() -> names("MATCH (d:Doc) SEARCH d IN (VECTOR INDEX `Doc[name]` FOR $q LIMIT 2) RETURN d", Q))
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("No vector index found on property 'name'");
  }

  @Test
  void whereMatchingNoCandidateStopsAtTheEndOfTheIndex() {
    // Rejects every candidate (false) and answers null for every one (missing property): the widening stops once the index
    // has handed over all it holds, and the answer is empty rather than a hang
    assertThat(names("MATCH (d:Doc) SEARCH d IN (VECTOR INDEX " + INDEX
        + " FOR $q WHERE d.name = 'none' LIMIT 3) RETURN d.name AS name", Q)).isEmpty();
    assertThat(names("MATCH (d:Doc) SEARCH d IN (VECTOR INDEX " + INDEX
        + " FOR $q WHERE d.missing = 1 LIMIT 3) RETURN d.name AS name", Q)).isEmpty();
  }

  @Test
  void labelOfTheMatchFiltersAfterTheSearch() {
    // The 2 nearest are Doc records, which a Cat pattern does not match
    assertThat(names("MATCH (d:Cat) SEARCH d IN (VECTOR INDEX " + INDEX + " FOR $q LIMIT 2) RETURN d.name AS name", Q))
        .isEmpty();
  }

  @Test
  void nullLimitIsAClearError() {
    assertThatThrownBy(() -> names("MATCH (d:Doc) SEARCH d IN (VECTOR INDEX " + INDEX
        + " FOR $q LIMIT null) RETURN d.name AS name", Q)).isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("k must be a number");
  }

  @Test
  void twoSearchesInOneQuery() {
    final List<Result> rows = rows("MATCH (a:Doc) SEARCH a IN (VECTOR INDEX " + INDEX + " FOR $q LIMIT 1) "
        + "MATCH (b:Doc) SEARCH b IN (VECTOR INDEX " + INDEX + " FOR [29.0, 1.0] LIMIT 2) RETURN a.name AS a, b.name AS b", Q);
    assertThat(rows).hasSize(2);
    for (final Result row : rows)
      assertThat(row.<String>getProperty("a")).isEqualTo("d0");
    assertThat(List.of(rows.get(0).<String>getProperty("b"), rows.get(1).<String>getProperty("b"))).containsExactly("d29", "d28");
  }

  @Test
  void backtickQuotedVariables() {
    assertThat(names("MATCH (`my doc`:Doc) SEARCH `my doc` IN (VECTOR INDEX " + INDEX
        + " FOR $q WHERE `my doc`.name <> 'd0' LIMIT 1) SCORE AS `the score` RETURN `my doc`.name AS name, `the score`", Q))
        .containsExactly("d1");
  }

  @Test
  void scoreVariableAlreadyDefinedIsRefused() {
    assertThatThrownBy(() -> names("WITH 1 AS s MATCH (d:Doc) SEARCH d IN (VECTOR INDEX " + INDEX
        + " FOR $q LIMIT 1) SCORE AS s RETURN d.name AS name", Q)).isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("'s'");
  }

  private List<String> names(final String query, final Map<String, Object> params) {
    final List<String> names = new ArrayList<>();
    for (final Result row : rows(query, params))
      names.add(row.getProperty("name"));
    return names;
  }

  private List<Result> rows(final String query, final Map<String, Object> params) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }
}
