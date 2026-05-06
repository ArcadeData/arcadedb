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
 * Regression test for GitHub issue #4096.
 * <p>
 * Consecutive undirected relationship patterns must not reuse the same relationship
 * for two different relationship variables in the same MATCH pattern.
 * <p>
 * For {@code (a)-[r1:KNOWS]-(b)-[r2:KNOWS]-(c)}, r1 and r2 must always refer to
 * distinct edges.
 */
class Issue4096UndirectedRelReuseIsomorphismTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-4096").create();
    database.getSchema().createVertexType("Person4096");
    database.getSchema().createEdgeType("KNOWS4096");
    database.transaction(() -> database.command("opencypher",
        "CREATE (a:Person4096 {name:'Alice'}), (b:Person4096 {name:'Bob'}), (c:Person4096 {name:'Charlie'}),"
            + " (a)-[:KNOWS4096]->(b), (b)-[:KNOWS4096]->(c)"));
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /**
   * Named undirected multi-hop: r1 and r2 must be distinct edges.
   * Only Alice-Bob-Charlie and Charlie-Bob-Alice are valid traversals.
   */
  @Test
  void undirectedMultiHopDoesNotReuseRelationship() {
    final ResultSet result = database.query("opencypher",
        """
            MATCH (a:Person4096)-[r1:KNOWS4096]-(b:Person4096)-[r2:KNOWS4096]-(c:Person4096)
            RETURN a.name AS a, b.name AS b, c.name AS c, r1 = r2 AS sameRel
            ORDER BY a, c""");

    final List<Result> rows = collect(result);
    assertThat(rows).hasSize(2);

    assertThat((String) rows.get(0).getProperty("a")).isEqualTo("Alice");
    assertThat((String) rows.get(0).getProperty("b")).isEqualTo("Bob");
    assertThat((String) rows.get(0).getProperty("c")).isEqualTo("Charlie");
    assertThat((Boolean) rows.get(0).getProperty("sameRel")).isFalse();

    assertThat((String) rows.get(1).getProperty("a")).isEqualTo("Charlie");
    assertThat((String) rows.get(1).getProperty("b")).isEqualTo("Bob");
    assertThat((String) rows.get(1).getProperty("c")).isEqualTo("Alice");
    assertThat((Boolean) rows.get(1).getProperty("sameRel")).isFalse();
  }

  /**
   * Named undirected multi-hop: direct count check - exactly 2 distinct-relationship traversals.
   */
  @Test
  void undirectedMultiHopDistinctRelationshipCount() {
    final ResultSet result = database.query("opencypher",
        """
            MATCH (a:Person4096)-[r1:KNOWS4096]-(b:Person4096)-[r2:KNOWS4096]-(c:Person4096)
            WHERE r1 <> r2 RETURN count(*) AS cnt""");

    final List<Result> rows = collect(result);
    assertThat(rows).hasSize(1);
    assertThat(((Number) rows.get(0).getProperty("cnt")).longValue()).isEqualTo(2L);
  }

  /**
   * Cross-clause edge reuse must be allowed: relationship uniqueness only applies
   * within a single MATCH clause. The optimizer merges all MATCH clauses into one
   * logical plan, so the isomorphism check must be scoped per MATCH clause.
   */
  @Test
  void crossClauseEdgeReuseIsAllowed() {
    final ResultSet result = database.query("opencypher",
        """
            MATCH (a:Person4096)-[r1:KNOWS4096]-(b:Person4096)
            MATCH (b:Person4096)-[r2:KNOWS4096]-(c:Person4096)
            RETURN count(*) AS cnt""");

    final List<Result> rows = collect(result);
    assertThat(rows).hasSize(1);
    // Expected breakdown for Alice-KNOWS->Bob-KNOWS->Charlie:
    //   first MATCH (undirected) yields 4 (a,b) pairs;
    //   for each pair, second MATCH from b yields 1 or 2 c's, totalling 6.
    assertThat(((Number) rows.get(0).getProperty("cnt")).longValue()).isEqualTo(6L);
  }

  /**
   * ExpandInto path: when both endpoints of the second hop are bound (via comma-separated
   * pattern with both endpoints anchored), the optimizer picks ExpandInto. The isomorphism
   * check must apply there too.
   */
  @Test
  void expandIntoPathDoesNotReuseRelationship() {
    final ResultSet result = database.query("opencypher",
        """
            MATCH (a:Person4096 {name:'Alice'}), (c:Person4096 {name:'Charlie'})
            MATCH (a)-[r1:KNOWS4096]-(b:Person4096), (b)-[r2:KNOWS4096]-(c)
            RETURN count(*) AS cnt""");

    final List<Result> rows = collect(result);
    assertThat(rows).hasSize(1);
    // Only one valid traversal: Alice-r1-Bob-r2-Charlie (r1 != r2).
    assertThat(((Number) rows.get(0).getProperty("cnt")).longValue()).isEqualTo(1L);
  }

  private static List<Result> collect(final ResultSet rs) {
    final List<Result> list = new ArrayList<>();
    while (rs.hasNext())
      list.add(rs.next());
    return list;
  }
}
