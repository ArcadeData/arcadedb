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
 * Regression test for issue #12728: a node variable an earlier OPTIONAL MATCH left null cannot be the target of a later hop. The
 * hop treated the variable as unbound and bound a fresh vertex to it, so {@code MATCH (a)-[:S]->(x)} answered a row for a null
 * {@code x} (the source side and a bare {@code (x)} were already right, issues #11761 / #12562).
 */
class CypherNullBoundHopTargetIssue12728Test {
  /** Binds {@code a} to the one A vertex and leaves {@code x} null: A has no NOPE relationship. */
  private static final String NULL_X = "MATCH (a:A) OPTIONAL MATCH (a)-[:NOPE]->(x) ";

  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/cypher12728").create();
    database.transaction(() -> database.command("opencypher", """
        CREATE (a:A {id: 0})-[:S]->(b:B {id: 1})-[:S]->(c:B {id: 2})"""));
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  private List<Result> rows(final String query) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }

  @Test
  void matchedHopToNullTargetMatchesNothing() {
    assertThat(rows(NULL_X + "MATCH (a)-[:S]->(x) RETURN a.id AS a, x.id AS x")).isEmpty();
    assertThat(rows(NULL_X + "MATCH (a)-[r:S]->(x) RETURN a.id AS a, x.id AS x")).isEmpty();
    assertThat(rows(NULL_X + "MATCH (a)-[:S]->(x:B) RETURN a.id AS a, x.id AS x")).isEmpty();
    assertThat(rows(NULL_X + "MATCH (a)-[:S]-(x) RETURN a.id AS a, x.id AS x")).isEmpty();
    assertThat(rows(NULL_X + "MATCH (x)<-[:S]-(a) RETURN a.id AS a, x.id AS x")).isEmpty();
    assertThat(rows(NULL_X + "MATCH (a)-[:S]->(y)-[:S]->(x) RETURN a.id AS a, x.id AS x")).isEmpty();
  }

  @Test
  void variableLengthHopToNullTargetMatchesNothing() {
    assertThat(rows(NULL_X + "MATCH (a)-[:S*1..2]->(x) RETURN a.id AS a, x.id AS x")).isEmpty();
    assertThat(rows(NULL_X + "MATCH (a)-[:S*]->(x) RETURN a.id AS a, x.id AS x")).isEmpty();
    assertThat(rows(NULL_X + "MATCH (a)-[:S*0..2]->(x) RETURN a.id AS a, x.id AS x")).isEmpty();
    // Only the distinct end vertices are asked for: the expansion reports each one once, and still none for a null target
    assertThat(((Number) single(NULL_X + "MATCH (a)-[:S*1..2]->(x) RETURN count(DISTINCT x) AS v")).longValue()).isZero();
  }

  @Test
  void optionalHopToNullTargetKeepsTheRowWithNull() {
    final List<Result> rows = rows(NULL_X + "OPTIONAL MATCH (a)-[:S]->(x) RETURN a.id AS a, x.id AS x");
    assertThat(rows).hasSize(1);
    assertThat(((Number) rows.getFirst().getProperty("a")).intValue()).isZero();
    assertThat((Object) rows.getFirst().getProperty("x")).isNull();

    assertThat(rows(NULL_X + "OPTIONAL MATCH (a)-[:S*1..2]->(x) RETURN a.id AS a, x.id AS x")).hasSize(1)
        .allSatisfy(r -> assertThat((Object) r.getProperty("x")).isNull());

    final List<Result> counts = rows(NULL_X + "OPTIONAL MATCH (a)-[:S]->(x) RETURN count(x) AS c");
    assertThat(((Number) counts.getFirst().getProperty("c")).longValue()).isZero();
  }

  @Test
  void boundNonNullTargetStillConstrainsTheHop() {
    final List<Result> rows = rows(
        "MATCH (a:A), (x:B {id: 2}) MATCH (a)-[:S*1..2]->(x) RETURN a.id AS a, x.id AS x");
    assertThat(rows).hasSize(1);
    assertThat(rows(
        "MATCH (a:A), (x:B {id: 2}) MATCH (a)-[:S]->(x) RETURN a.id AS a, x.id AS x")).isEmpty();
    assertThat(rows(
        "MATCH (a:A), (x:B {id: 1}) MATCH (a)-[:S]->(x) RETURN a.id AS a, x.id AS x")).hasSize(1);
  }

  @Test
  void hopReversedOntoNullTargetMatchesNothing() {
    // An unbound source and a bound target: the planner walks the hop from the target, which is the source side again
    assertThat(rows(NULL_X + "MATCH (y)-[:S]->(x) RETURN y.id AS y, x.id AS x")).isEmpty();
    final List<Result> rows = rows(NULL_X + "OPTIONAL MATCH (y)-[:S]->(x) RETURN y.id AS y, x.id AS x");
    assertThat(rows).hasSize(1);
    assertThat((Object) rows.getFirst().getProperty("y")).isNull();
    assertThat((Object) rows.getFirst().getProperty("x")).isNull();
  }

  @Test
  void quantifiedPathToNullTargetMatchesNothing() {
    assertThat(rows(NULL_X + "MATCH (a) ((p)-[:S]->(q)){1,2} (x) RETURN a.id AS a, x.id AS x")).isEmpty();
    assertThat(rows(NULL_X + "MATCH (a) ((p)-[:S]->(q))+ (x) RETURN a.id AS a, x.id AS x")).isEmpty();

    final List<Result> rows = rows(NULL_X + "OPTIONAL MATCH (a) ((p)-[:S]->(q)){1,2} (x) RETURN a.id AS a, x.id AS x");
    assertThat(rows).hasSize(1);
    assertThat((Object) rows.getFirst().getProperty("x")).isNull();
  }

  @Test
  void nullTargetCarriedByWithMatchesNothing() {
    assertThat(rows(NULL_X + "WITH a, x MATCH (a)-[:S]->(x) RETURN a.id AS a, x.id AS x")).isEmpty();
    assertThat(rows(NULL_X + "WITH a, x MATCH (a)-[:S*1..2]->(x) RETURN a.id AS a, x.id AS x")).isEmpty();
    assertThat(rows(NULL_X + "WITH a, x MATCH (a)-[:S]->(y)-[:S]->(x) RETURN a.id AS a, x.id AS x")).isEmpty();
  }

  @Test
  void subqueryBodySeededWithNullTargetMatchesNothing() {
    // A body seeded straight from the outer row, with no leading WITH, plans with no names bound
    assertThat(single(NULL_X + "RETURN EXISTS { MATCH (a)-[:S]->(x) } AS v")).isEqualTo(false);
    assertThat(single(NULL_X + "RETURN EXISTS { MATCH (a)-[:S*1..2]->(x) } AS v")).isEqualTo(false);
    assertThat(((Number) single(NULL_X + "RETURN COUNT { MATCH (a)-[:S]->(x) } AS v")).longValue()).isZero();
    assertThat(((Number) single(NULL_X + "RETURN COUNT { MATCH (a)-[:S]-(x) } AS v")).longValue()).isZero();
    assertThat(((Number) single(NULL_X + "RETURN COUNT { MATCH (a)-[:S*1..2]->(x) } AS v")).longValue()).isZero();
    assertThat((List<?>) single(NULL_X + "RETURN COLLECT { MATCH (a)-[:S]->(x) RETURN x.id } AS v")).isEmpty();

    // The pattern forms the issue reports as right already stay right
    assertThat(single(NULL_X + "RETURN EXISTS { (a)-[:S]->(x) } AS v")).isEqualTo(false);
    assertThat(((Number) single(NULL_X + "RETURN size([(a)-[:S]->(x) | 1]) AS v")).longValue()).isZero();
  }

  @Test
  void importingSubqueryWithNullTargetMatchesNothing() {
    // The body of a CALL that imports the null variable is planned on its own, with the imported names bound
    assertThat(rows(NULL_X + "CALL { WITH x MATCH (s:A)-[:S*1..2]->(x:B) RETURN s.id AS s } RETURN s")).isEmpty();
    assertThat(rows(NULL_X + "CALL { WITH x MATCH (s:A)-[:S]->(x:B) RETURN s.id AS s } RETURN s")).isEmpty();
    assertThat(rows(NULL_X + "CALL { WITH a, x MATCH (a)-[:S*1..2]->(x) RETURN a.id AS s } RETURN s")).isEmpty();
  }

  private Object single(final String query) {
    final List<Result> rows = rows(query);
    assertThat(rows).hasSize(1);
    return rows.getFirst().getProperty("v");
  }
}
