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
import com.arcadedb.query.opencypher.ast.CorrelatedSubqueryRewriter;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Regression test for GitHub issue #5461.
 * <p>
 * {@code COUNT { }} and {@code EXISTS { }} bodies are re-executed as standalone queries, and a body
 * that is a bare pattern has to be wrapped into a {@code MATCH}. The wrapper used to recognize only
 * {@code MATCH}, {@code WITH} and {@code RETURN} as clause keywords, so a body opening with any other
 * clause - {@code UNWIND}, {@code CALL}, {@code OPTIONAL MATCH} - was mistaken for a pattern and
 * spliced into an unparseable {@code MATCH UNWIND [1,2] AS y RETURN y RETURN 1}. The parse failure was
 * absorbed into the expression's neutral value, so {@code COUNT} silently returned 0 and
 * {@code EXISTS} silently returned false.
 */
class Issue5461SubqueryLeadingClauseTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/issue-5461-subquery-leading-clause").create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  private Object scalar(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).isTrue();
      return rs.next().getProperty("v");
    }
  }

  private List<Result> rows(final String query) {
    final List<Result> collected = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        collected.add(rs.next());
    }
    return collected;
  }

  @Test
  void countSubqueryStartingWithUnwind() {
    assertThat(scalar("RETURN COUNT { UNWIND [1, 2] AS y RETURN y } AS v")).isEqualTo(2L);
    assertThat(scalar("RETURN COUNT { UNWIND [1] AS y RETURN y } AS v")).isEqualTo(1L);
    assertThat(scalar("RETURN COUNT { UNWIND [1, 2, 3] AS y RETURN y } AS v")).isEqualTo(3L);
    assertThat(scalar("RETURN COUNT { UNWIND [] AS y RETURN y } AS v")).isEqualTo(0L);
  }

  @Test
  void countSubqueryStartingWithUnwindAndNoReturn() {
    assertThat(scalar("RETURN COUNT { UNWIND [1, 2] AS y } AS v")).isEqualTo(2L);
  }

  @Test
  void countSubqueryLeadingWithControlsStillWork() {
    assertThat(scalar("RETURN COUNT { WITH 1 AS dummy UNWIND [1, 2] AS y RETURN y } AS v")).isEqualTo(2L);
    assertThat(scalar("RETURN COUNT { RETURN 1 } AS v")).isEqualTo(1L);
  }

  @Test
  void countSubqueryStartingWithUnwindIsEvaluatedPerOuterRow() {
    final List<Result> results = rows(
        "UNWIND [1, 2, 3] AS x RETURN x, COUNT { UNWIND [1, 2] AS y RETURN y } AS c ORDER BY x");

    assertThat(results).hasSize(3);
    for (int i = 0; i < 3; i++) {
      assertThat(results.get(i).<Number>getProperty("x").intValue()).isEqualTo(i + 1);
      assertThat(results.get(i).<Number>getProperty("c").longValue()).as("count for outer row %d", i + 1).isEqualTo(2L);
    }
  }

  @Test
  void countSubqueryStartingWithCallOrOptionalMatch() {
    assertThat(scalar("RETURN COUNT { CALL { RETURN 1 AS z } RETURN z } AS v")).isEqualTo(1L);
    assertThat(scalar("RETURN COUNT { OPTIONAL MATCH (n:NoSuchLabel) RETURN n } AS v")).isEqualTo(1L);
  }

  @Test
  void existsSubqueryStartingWithUnwind() {
    assertThat(scalar("RETURN EXISTS { UNWIND [1, 2] AS y RETURN y } AS v")).isEqualTo(true);
    assertThat(scalar("RETURN EXISTS { UNWIND [] AS y RETURN y } AS v")).isEqualTo(false);
    assertThat(scalar("RETURN EXISTS { CALL { RETURN 1 AS z } RETURN z } AS v")).isEqualTo(true);
  }

  @Test
  void collectSubqueryStartingWithUnwind() {
    assertThat(scalar("RETURN COLLECT { UNWIND [1, 2] AS y RETURN y } AS v")).isEqualTo(List.of(1L, 2L));
  }

  @Test
  void returningCallSubqueryStartingWithUnwind() {
    final List<Result> results = rows(
        "UNWIND [1, 2, 3] AS x CALL { UNWIND [1, 2] AS y RETURN y } RETURN x, y ORDER BY x, y");

    assertThat(results).hasSize(6);
    final List<String> pairs = new ArrayList<>(6);
    for (final Result row : results)
      pairs.add(row.<Number>getProperty("x").intValue() + "," + row.<Number>getProperty("y").intValue());
    assertThat(pairs).containsExactly("1,1", "1,2", "2,1", "2,2", "3,1", "3,2");
  }

  @Test
  void clauseKeywordTestClassifiesBodiesWithoutThrowing() {
    // The clause-keyword test indexes the first character, so a blank body must not throw.
    assertThatCode(() -> CorrelatedSubqueryRewriter.startsWithClauseKeyword("")).doesNotThrowAnyException();
    assertThat(CorrelatedSubqueryRewriter.startsWithClauseKeyword("")).isFalse();
    assertThat(CorrelatedSubqueryRewriter.startsWithClauseKeyword("   ")).isFalse();

    assertThat(CorrelatedSubqueryRewriter.startsWithClauseKeyword("UNWIND [1] AS y RETURN y")).isTrue();
    assertThat(CorrelatedSubqueryRewriter.startsWithClauseKeyword("unwind [1] AS y RETURN y")).isTrue();
    assertThat(CorrelatedSubqueryRewriter.startsWithClauseKeyword("OPTIONAL MATCH (n) RETURN n")).isTrue();

    // Bare patterns must stay classified as patterns, including one bound to a keyword-prefixed name
    assertThat(CorrelatedSubqueryRewriter.startsWithClauseKeyword("(a)-[:KNOWS]->(b)")).isFalse();
    assertThat(CorrelatedSubqueryRewriter.startsWithClauseKeyword("matches = (a)-->(b)")).isFalse();
    assertThat(CorrelatedSubqueryRewriter.startsWithClauseKeyword("returns = (a)-->(b)")).isFalse();
  }

  @Test
  void correlatedSubqueryStartingWithUnwind() {
    database.transaction(() -> database.command("opencypher", "CREATE (:T {tags: ['a', 'b', 'c']})"));

    assertThat(scalar("MATCH (n:T) RETURN COUNT { UNWIND n.tags AS t RETURN t } AS v")).isEqualTo(3L);
    assertThat(scalar("MATCH (n:T) RETURN EXISTS { UNWIND n.tags AS t RETURN t } AS v")).isEqualTo(true);
    assertThat(scalar("MATCH (n:T) RETURN COLLECT { UNWIND n.tags AS t RETURN t } AS v")).isEqualTo(List.of("a", "b", "c"));
  }

  @Test
  void patternOnlySubqueriesAreStillWrappedIntoMatch() {
    database.transaction(() -> database.command("opencypher", "CREATE (a:P {n:'a'})-[:KNOWS]->(b:P {n:'b'})"));

    assertThat(scalar("MATCH (a:P {n:'a'}) RETURN COUNT { (a)-[:KNOWS]->(:P) } AS v")).isEqualTo(1L);
    assertThat(scalar("MATCH (a:P {n:'a'}) RETURN EXISTS { (a)-[:KNOWS]->(:P) } AS v")).isEqualTo(true);
    assertThat(scalar("MATCH (a:P {n:'b'}) RETURN EXISTS { (a)-[:KNOWS]->(:P) } AS v")).isEqualTo(false);
  }
}
