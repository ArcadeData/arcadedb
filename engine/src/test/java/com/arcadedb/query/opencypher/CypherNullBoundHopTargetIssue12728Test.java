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

  private static final String NULL_X = "MATCH (a:A) OPTIONAL MATCH (a)-[:NOPE]->(x) ";

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
}
