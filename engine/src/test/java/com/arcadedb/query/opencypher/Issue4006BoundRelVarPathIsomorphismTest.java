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
 * Regression test for GitHub issue #4006.
 * <p>
 * When a relationship variable {@code r} is bound in one MATCH clause and then referenced
 * explicitly inside a path pattern containing variable-length segments in a subsequent MATCH,
 * the variable-length segments must not re-traverse {@code r}. OpenCypher path isomorphism
 * applies within a single path, not within a single MATCH clause.
 * <p>
 * This corresponds to TCK scenario {@code Match4 [7]}:
 * "Matching variable length patterns including a bound relationship".
 */
class Issue4006BoundRelVarPathIsomorphismTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-4006").create();
    database.getSchema().createVertexType("Node4006");
    database.getSchema().createEdgeType("EDGE4006");
    database.transaction(() -> database.command("opencypher",
        "CREATE (n0:Node4006)-[:EDGE4006]->(n1:Node4006)-[:EDGE4006]->(n2:Node4006)-[:EDGE4006]->(n3:Node4006)"));
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /**
   * TCK Match4 [7]: variable-length segments flanking a bound relationship must obey path
   * isomorphism - they must not re-traverse the explicitly named {@code r}.
   */
  @Test
  void vlpSegmentsDoNotReuseExplicitlyNamedBoundRelationship() {
    final ResultSet result = database.query("opencypher",
        "MATCH ()-[r:EDGE4006]-()"
            + " MATCH p = (n)-[*0..1]-()-[r]-()-[*0..1]-(m)"
            + " RETURN count(p) AS c");

    final List<Result> rows = collect(result);
    assertThat(rows).hasSize(1);
    assertThat(((Number) rows.get(0).getProperty("c")).longValue()).isEqualTo(32L);
  }

  /**
   * Regression guard for issue #3999: a previously bound relationship that is NOT part
   * of the current path pattern must not block the variable-length traversal.
   */
  @Test
  void unboundedVlpIsNotBlockedByUnrelatedPreviouslyBoundRel() {
    final ResultSet result = database.query("opencypher",
        "MATCH (a:Node4006)-[r:EDGE4006]->(b:Node4006)"
            + " WITH a, b, r"
            + " MATCH path = (a)-[:EDGE4006*1..2]->(b)"
            + " RETURN count(r) AS rc");

    final List<Result> rows = collect(result);
    assertThat(rows).hasSize(1);
    assertThat(((Number) rows.get(0).getProperty("rc")).longValue()).isGreaterThan(0L);
  }

  private static List<Result> collect(final ResultSet rs) {
    final List<Result> list = new ArrayList<>();
    while (rs.hasNext())
      list.add(rs.next());
    return list;
  }
}
