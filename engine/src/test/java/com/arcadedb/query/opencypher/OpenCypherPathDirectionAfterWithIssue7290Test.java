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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7290: a semantically neutral {@code WITH} between the CREATE and the MATCH reversed the node order
 * reported by {@code nodes(p)}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class OpenCypherPathDirectionAfterWithIssue7290Test {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/testopencypher-7290-path-direction").create();
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void pathNodeOrderWithoutWith() {
    assertPathOrder("""
        CREATE (n0 {id: 128, k3: false}) <-[:rt7 {id: 420}]- (:V {id: 129, k2: 'k', k3: true})
        MATCH p0 = (s0 {k3: true, k2: 'k'}) -[r0:rt7 {id: 420}]-> (n0)
        RETURN s0.id AS sourceId, n0.id AS targetId, r0.id AS relationshipId,
               [x IN nodes(p0) | x.id] AS pathNodeIds,
               [x IN relationships(p0) | x.id] AS pathRelationshipIds""");
  }

  @Test
  void pathNodeOrderWithNeutralWith() {
    assertPathOrder("""
        CREATE (n0 {id: 128, k3: false}) <-[:rt7 {id: 420}]- (:V {id: 129, k2: 'k', k3: true})
        WITH n0
        MATCH p0 = (s0 {k3: true, k2: 'k'}) -[r0:rt7 {id: 420}]-> (n0)
        RETURN s0.id AS sourceId, n0.id AS targetId, r0.id AS relationshipId,
               [x IN nodes(p0) | x.id] AS pathNodeIds,
               [x IN relationships(p0) | x.id] AS pathRelationshipIds""");
  }

  /**
   * The same shape reached from the other side: the bound variable is the pattern's LEFT end rather than its
   * right one, so a rule that simply starts from whatever the previous clause bound would get this one right and
   * the two above wrong.
   */
  @Test
  void pathNodeOrderWithBoundStartNode() {
    database.command("opencypher",
        "CREATE (n0 {id: 128, k3: false}) <-[:rt7 {id: 420}]- (:V {id: 129, k2: 'k', k3: true})");

    final ResultSet rs = database.query("opencypher", """
        MATCH (s0 {id: 129})
        WITH s0
        MATCH p0 = (s0) -[r0:rt7]-> (n0)
        RETURN [x IN nodes(p0) | x.id] AS pathNodeIds""");

    assertThat(rs.hasNext()).isTrue();
    assertThat((List<Object>) rs.next().getProperty("pathNodeIds")).containsExactly(129, 128);
  }

  /**
   * The other route into a reversed traversal: an IN hop on a UNIDIRECTIONAL edge type, which stores no
   * incoming links, so the plan scans the written source type and walks OUT to the bound node instead. Same
   * swap of the two ends, so the same path-order rule has to hold, over two hops as well as one.
   */
  @Test
  void pathNodeOrderOnReversedUnidirectionalHops() {
    database.command("sql", "CREATE VERTEX TYPE N");
    database.command("sql", "CREATE EDGE TYPE U UNIDIRECTIONAL");
    database.command("opencypher", "CREATE (:N {id: 1}) <-[:U]- (:N {id: 2})");

    final ResultSet single = database.query("opencypher", """
        MATCH (a:N {id: 1})
        WITH a
        MATCH p = (a) <-[:U]- (b)
        RETURN [x IN nodes(p) | x.id] AS pathNodeIds""");
    assertThat(single.hasNext()).isTrue();
    assertThat(single.next().<List<Object>>getProperty("pathNodeIds")).containsExactly(1, 2);

    database.command("opencypher", "MATCH (b:N {id: 2}) CREATE (b) <-[:U]- (:N {id: 3})");

    final ResultSet twoHops = database.query("opencypher", """
        MATCH (a:N {id: 1})
        WITH a
        MATCH p = (a) <-[:U]- (b) <-[:U]- (c)
        RETURN [x IN nodes(p) | x.id] AS pathNodeIds""");
    assertThat(twoHops.hasNext()).isTrue();
    assertThat(twoHops.next().<List<Object>>getProperty("pathNodeIds")).containsExactly(1, 2, 3);
  }

  private void assertPathOrder(final String query) {
    final ResultSet rs = database.command("opencypher", query);

    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();

    assertThat(((Number) row.getProperty("sourceId")).intValue()).isEqualTo(129);
    assertThat(((Number) row.getProperty("targetId")).intValue()).isEqualTo(128);
    assertThat(((Number) row.getProperty("relationshipId")).intValue()).isEqualTo(420);
    // Cypher requires nodes(p) in traversal order, from the start of the pattern to its end. The pattern is
    // directed s0 -> n0, so 129 comes first however the executor chose to reach it.
    assertThat((List<Object>) row.<List<Object>>getProperty("pathNodeIds")).containsExactly(129, 128);
    assertThat((List<Object>) row.<List<Object>>getProperty("pathRelationshipIds")).containsExactly(420);
    assertThat(rs.hasNext()).isFalse();
  }
}
