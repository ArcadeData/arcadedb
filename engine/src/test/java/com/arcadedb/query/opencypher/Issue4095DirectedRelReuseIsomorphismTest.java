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
 * Regression test for GitHub issue #4095.
 * <p>
 * Consecutive directed relationship patterns with anonymous (unnamed) relationship variables
 * must not reuse the same relationship for two different positions in the same MATCH pattern.
 * <p>
 * For {@code (s)<-[:KNOWS]-(f)-[:KNOWS]->(d)}, the two KNOWS slots must always bind
 * to distinct edges — returning (Alice,Bob,Alice) or (Charlie,Bob,Charlie) is invalid.
 */
class Issue4095DirectedRelReuseIsomorphismTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-4095").create();
    database.getSchema().createVertexType("Person4095");
    database.getSchema().createEdgeType("KNOWS4095");
    database.transaction(() -> {
      // Diverging graph: Bob has two outgoing KNOWS edges (to Alice and Charlie)
      database.command("opencypher",
          "CREATE (:Person4095 {name:'Alice'})<-[:KNOWS4095]-(:Person4095 {name:'Bob'})-[:KNOWS4095]->(:Person4095 {name:'Charlie'})");
      // Converging graph: Dave and Frank both have outgoing KNOWS edges to Eve
      database.command("opencypher",
          "CREATE (:Person4095 {name:'Dave'})-[:KNOWS4095]->(:Person4095 {name:'Eve'})<-[:KNOWS4095]-(:Person4095 {name:'Frank'})");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /**
   * Diverging anonymous directed pattern: same edge must not fill both outgoing slots.
   * Bob has edges to Alice and Charlie; Alice,Bob,Alice and Charlie,Bob,Charlie are invalid.
   */
  @Test
  void divergingAnonymousDirectedDoesNotReuseRelationship() {
    final ResultSet result = database.query("opencypher",
        """
            MATCH (s:Person4095)<-[:KNOWS4095]-(f:Person4095)-[:KNOWS4095]->(d:Person4095)
            WHERE f.name = 'Bob'
            RETURN s.name AS s, f.name AS f, d.name AS d
            ORDER BY s, d""");

    final List<Result> rows = collect(result);
    assertThat(rows).hasSize(2);

    assertThat((String) rows.get(0).getProperty("s")).isEqualTo("Alice");
    assertThat((String) rows.get(0).getProperty("f")).isEqualTo("Bob");
    assertThat((String) rows.get(0).getProperty("d")).isEqualTo("Charlie");

    assertThat((String) rows.get(1).getProperty("s")).isEqualTo("Charlie");
    assertThat((String) rows.get(1).getProperty("f")).isEqualTo("Bob");
    assertThat((String) rows.get(1).getProperty("d")).isEqualTo("Alice");
  }

  /**
   * Converging anonymous directed pattern: same edge must not fill both incoming slots.
   * Both Dave and Frank point to Eve; (Dave,Eve,Dave) and (Frank,Eve,Frank) are invalid.
   */
  @Test
  void convergingAnonymousDirectedDoesNotReuseRelationship() {
    final ResultSet result = database.query("opencypher",
        """
            MATCH (p1:Person4095)-[:KNOWS4095]->(p2:Person4095)<-[:KNOWS4095]-(p3:Person4095)
            WHERE p2.name = 'Eve'
            RETURN p1.name AS p1, p2.name AS p2, p3.name AS p3
            ORDER BY p1, p3""");

    final List<Result> rows = collect(result);
    assertThat(rows).hasSize(2);

    assertThat((String) rows.get(0).getProperty("p1")).isEqualTo("Dave");
    assertThat((String) rows.get(0).getProperty("p2")).isEqualTo("Eve");
    assertThat((String) rows.get(0).getProperty("p3")).isEqualTo("Frank");

    assertThat((String) rows.get(1).getProperty("p1")).isEqualTo("Frank");
    assertThat((String) rows.get(1).getProperty("p2")).isEqualTo("Eve");
    assertThat((String) rows.get(1).getProperty("p3")).isEqualTo("Dave");
  }

  /**
   * Aggregate on diverging pattern must not be doubled by spurious self-reuse rows.
   * count(d) must equal 2, not 4.
   */
  @Test
  void divergingAnonymousDirectedCountIsCorrect() {
    final ResultSet result = database.query("opencypher",
        """
            MATCH (s:Person4095)<-[:KNOWS4095]-(f:Person4095)-[:KNOWS4095]->(d:Person4095)
            WHERE f.name = 'Bob'
            RETURN count(d) AS c""");

    final List<Result> rows = collect(result);
    assertThat(rows).hasSize(1);
    assertThat(((Number) rows.get(0).getProperty("c")).longValue()).isEqualTo(2L);
  }

  /**
   * Aggregate on converging pattern must not be doubled by spurious self-reuse rows.
   * count(p1) must equal 2, not 4.
   */
  @Test
  void convergingAnonymousDirectedCountIsCorrect() {
    final ResultSet result = database.query("opencypher",
        """
            MATCH (p1:Person4095)-[:KNOWS4095]->(p2:Person4095)<-[:KNOWS4095]-(p3:Person4095)
            WHERE p2.name = 'Eve'
            RETURN count(p1) AS c""");

    final List<Result> rows = collect(result);
    assertThat(rows).hasSize(1);
    assertThat(((Number) rows.get(0).getProperty("c")).longValue()).isEqualTo(2L);
  }

  /**
   * Three-hop anonymous chain of the same type. Exercises multiple synthetic counters
   * and verifies all three slots bind to distinct edges.
   */
  @Test
  void threeHopAnonymousSameTypeChainHasDistinctEdges() {
    database.transaction(() -> database.command("opencypher",
        """
            CREATE (:Person4095 {name:'C1'})-[:KNOWS4095]->(:Person4095 {name:'C2'})
                  -[:KNOWS4095]->(:Person4095 {name:'C3'})
                  -[:KNOWS4095]->(:Person4095 {name:'C4'})"""));

    final ResultSet result = database.query("opencypher",
        """
            MATCH (a:Person4095)-[:KNOWS4095]->(b:Person4095)-[:KNOWS4095]->(c:Person4095)-[:KNOWS4095]->(d:Person4095)
            WHERE a.name STARTS WITH 'C'
            RETURN a.name AS a, b.name AS b, c.name AS c, d.name AS d""");

    final List<Result> rows = collect(result);
    assertThat(rows).hasSize(1);
    assertThat((String) rows.get(0).getProperty("a")).isEqualTo("C1");
    assertThat((String) rows.get(0).getProperty("b")).isEqualTo("C2");
    assertThat((String) rows.get(0).getProperty("c")).isEqualTo("C3");
    assertThat((String) rows.get(0).getProperty("d")).isEqualTo("C4");
  }

  /**
   * Mixed named + anonymous relationship variables in the same MATCH clause: both
   * tracking paths must coexist and exclude each other's edges.
   */
  @Test
  void mixedNamedAndAnonymousSameClauseBindsDistinctEdges() {
    final ResultSet result = database.query("opencypher",
        """
            MATCH (s:Person4095)<-[r:KNOWS4095]-(f:Person4095)-[:KNOWS4095]->(d:Person4095)
            WHERE f.name = 'Bob'
            RETURN s.name AS s, type(r) AS rt, d.name AS d
            ORDER BY s, d""");

    final List<Result> rows = collect(result);
    assertThat(rows).hasSize(2);
    assertThat((String) rows.get(0).getProperty("s")).isEqualTo("Alice");
    assertThat((String) rows.get(0).getProperty("d")).isEqualTo("Charlie");
    assertThat((String) rows.get(1).getProperty("s")).isEqualTo("Charlie");
    assertThat((String) rows.get(1).getProperty("d")).isEqualTo("Alice");
  }

  /**
   * Cross-clause edge reuse must remain valid: relationship uniqueness is per-clause,
   * not per-row, so the same physical edge in two separate MATCH clauses is allowed.
   */
  @Test
  void crossClauseAnonymousEdgeReuseIsAllowed() {
    final ResultSet result = database.query("opencypher",
        """
            MATCH (a:Person4095)-[:KNOWS4095]->(b:Person4095)
            MATCH (b:Person4095)<-[:KNOWS4095]-(c:Person4095)
            WHERE b.name = 'Eve'
            RETURN count(*) AS cnt""");

    final List<Result> rows = collect(result);
    assertThat(rows).hasSize(1);
    // Eve has 2 IN edges (Dave→Eve, Frank→Eve). First MATCH binds (a,b)=(Dave,Eve) or
    // (Frank,Eve). Second MATCH binds c independently from b's IN edges. Same edge can
    // legitimately fill the first MATCH's slot AND the second MATCH's slot, giving 4 rows.
    assertThat(((Number) rows.get(0).getProperty("cnt")).longValue()).isEqualTo(4L);
  }

  private static List<Result> collect(final ResultSet rs) {
    final List<Result> list = new ArrayList<>();
    while (rs.hasNext())
      list.add(rs.next());
    return list;
  }
}
