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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #12621: a relationship variable an earlier OPTIONAL MATCH left null cannot be matched by a later
 * hop that reuses it. The hop decided whether the variable was bound from its value, so a null binding read as "free" and the
 * hop bound a fresh edge to it. The relationship sibling of the node fixes #11761 / #12562.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class CypherNullBoundRelationshipReuseIssue12621Test extends TestHelper {
  private static final String NULL_R = "OPTIONAL MATCH (a:P {n: 'A'})-[r:Nonexistent]->(b) WITH r ";

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:P {n: 'A'})-[:K]->(:P {n: 'B'})");
      database.command("opencypher", "CREATE (:P {n: 'C'})-[:K]->(:P {n: 'D'})");
    });
  }

  private List<Result> rows(final String query) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }

  private long count(final String query) {
    final List<Result> rows = rows(query);
    assertThat(rows).hasSize(1);
    return ((Number) rows.getFirst().getProperty("c")).longValue();
  }

  @Test
  void nullBoundRelationshipReusedInAHopMatchesNothing() {
    assertThat(count(NULL_R + "MATCH ()-[r]->() RETURN count(*) AS c")).isZero();
    assertThat(rows(NULL_R + "MATCH ()-[r]->() RETURN r")).isEmpty();
    assertThat(rows(NULL_R + "MATCH (x)-[r]->(y) RETURN x.n AS x, y.n AS y")).isEmpty();
    assertThat(rows(NULL_R + "MATCH (x)-[r:K]->(y) RETURN x.n AS x, y.n AS y")).isEmpty();
    assertThat(rows(NULL_R + "MATCH (x)<-[r]-(y) RETURN x.n AS x, y.n AS y")).isEmpty();
    assertThat(rows(NULL_R + "MATCH (x)-[r]-(y) RETURN x.n AS x, y.n AS y")).isEmpty();
    assertThat(rows(NULL_R + "MATCH (x:P {n: 'A'})-[r]->(y) RETURN x.n AS x, y.n AS y")).isEmpty();
    assertThat(rows(NULL_R + "MATCH p = (x)-[r]->(y) RETURN p")).isEmpty();
  }

  @Test
  void nullBoundRelationshipFromOptionalMatchWithoutWith() {
    assertThat(count("OPTIONAL MATCH (a:P {n: 'A'})-[r:Nonexistent]->(b) MATCH ()-[r]->() RETURN count(*) AS c")).isZero();
    assertThat(rows("MATCH (a:P {n: 'B'}) OPTIONAL MATCH (a)-[r]->(b) MATCH (x)-[r]->(y) RETURN x.n AS x")).isEmpty();
  }

  @Test
  void nullBoundRelationshipInAnOptionalMatchKeepsTheRowWithNulls() {
    final List<Result> rows = rows(NULL_R + "OPTIONAL MATCH (x)-[r]->(y) RETURN x.n AS x, y.n AS y, r");
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst().<Object>getProperty("x")).isNull();
    assertThat(rows.getFirst().<Object>getProperty("y")).isNull();
    assertThat(rows.getFirst().<Object>getProperty("r")).isNull();
  }

  @Test
  void nullBoundRelationshipInAnExistsSubqueryMatchesNothing() {
    assertThat(count(NULL_R + "WHERE EXISTS { MATCH ()-[r]->() } RETURN count(*) AS c")).isZero();
    assertThat(count(NULL_R + "RETURN COUNT { MATCH ()-[r]->() } AS c")).isZero();
  }

  @Test
  void boundRelationshipReusedInAnIndexedSubqueryHop() {
    database.command("sql", "CREATE PROPERTY K.w INTEGER");
    database.command("sql", "CREATE INDEX ON K (w) NOTUNIQUE");
    database.transaction(() -> database.command("opencypher", "MATCH ()-[k:K]->() SET k.w = 1"));
    assertThat(count(NULL_R + "WHERE EXISTS { MATCH ()-[r:K {w: 1}]->() } RETURN count(*) AS c")).isZero();
    assertThat(count(NULL_R + "RETURN COUNT { MATCH ()-[r:K {w: 1}]->() } AS c")).isZero();
    assertThat(count("MATCH (:P {n: 'A'})-[r:K]->() RETURN COUNT { MATCH ()-[r:K {w: 1}]->() } AS c")).isEqualTo(1L);
    assertThat(count("MATCH (:P {n: 'A'})-[r:K]->() MATCH ()-[r:K {w: 1}]->() RETURN count(*) AS c")).isEqualTo(1L);
  }

  @Test
  void nullBoundRelationshipListReusedInAVariableLengthHopMatchesNothing() {
    assertThat(count("""
        OPTIONAL MATCH (a:P {n: 'A'})-[r:Nonexistent*1..2]->(b) WITH r
        MATCH ()-[r*1..2]->() RETURN count(*) AS c""")).isZero();
  }

  @Test
  void nonNullBoundRelationshipStillConstrainsTheHop() {
    final List<Result> rows = rows("MATCH (a:P {n: 'A'})-[r:K]->(b) WITH r MATCH (c)-[r]->(d) RETURN c.n + '->' + d.n AS e");
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst().<String>getProperty("e")).isEqualTo("A->B");
    assertThat(count("MATCH (a:P {n: 'A'})-[r:K]->(b) WITH r MATCH ()-[r]->() RETURN count(*) AS c")).isEqualTo(1L);
    assertThat(count("MATCH ()-[r:K]->() WITH r MATCH ()-[r]->() RETURN count(*) AS c")).isEqualTo(2L);
  }

  @Test
  void freshRelationshipVariableStillBindsEveryEdge() {
    assertThat(count(NULL_R + "MATCH ()-[s]->() RETURN count(*) AS c")).isEqualTo(2L);
    assertThat(count("MATCH ()-[r:K]->() RETURN count(*) AS c")).isEqualTo(2L);
  }

  @Test
  void nullBoundNodeControlStillMatchesNothing() {
    assertThat(count("OPTIONAL MATCH (a:P {n: 'Nobody'}) WITH a MATCH (a) RETURN count(*) AS c")).isZero();
  }
}
