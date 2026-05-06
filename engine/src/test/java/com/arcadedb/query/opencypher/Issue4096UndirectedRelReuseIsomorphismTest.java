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
        "MATCH (a:Person4096)-[r1:KNOWS4096]-(b:Person4096)-[r2:KNOWS4096]-(c:Person4096) "
            + "RETURN a.name AS a, b.name AS b, c.name AS c, r1 = r2 AS sameRel "
            + "ORDER BY a, c");

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
        "MATCH (a:Person4096)-[r1:KNOWS4096]-(b:Person4096)-[r2:KNOWS4096]-(c:Person4096) "
            + "WHERE r1 <> r2 RETURN count(*) AS cnt");

    final List<Result> rows = collect(result);
    assertThat(rows).hasSize(1);
    assertThat(((Number) rows.get(0).getProperty("cnt")).longValue()).isEqualTo(2L);
  }

  /**
   * Named undirected multi-hop: total count must equal the count with explicit r1 <> r2 filter.
   * If the bug is present, the unfiltered query returns more rows than the filtered one.
   */
  @Test
  void undirectedMultiHopTotalCountMatchesExplicitFilter() {
    final ResultSet unfiltered = database.query("opencypher",
        "MATCH (a:Person4096)-[r1:KNOWS4096]-(b:Person4096)-[r2:KNOWS4096]-(c:Person4096) "
            + "RETURN count(*) AS cnt");
    final List<Result> unfilteredRows = collect(unfiltered);

    final ResultSet filtered = database.query("opencypher",
        "MATCH (a:Person4096)-[r1:KNOWS4096]-(b:Person4096)-[r2:KNOWS4096]-(c:Person4096) "
            + "WHERE r1 <> r2 RETURN count(*) AS cnt");
    final List<Result> filteredRows = collect(filtered);

    assertThat(((Number) unfilteredRows.get(0).getProperty("cnt")).longValue())
        .isEqualTo(((Number) filteredRows.get(0).getProperty("cnt")).longValue());
  }

  private static List<Result> collect(final ResultSet rs) {
    final List<Result> list = new ArrayList<>();
    while (rs.hasNext())
      list.add(rs.next());
    return list;
  }
}
