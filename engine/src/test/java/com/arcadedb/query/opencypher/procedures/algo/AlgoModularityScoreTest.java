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
package com.arcadedb.query.opencypher.procedures.algo;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the algo.modularityScore Cypher procedure.
 */
class AlgoModularityScoreTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-modularity");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("EDGE");

    // Two communities: {A,B} and {C,D}
    // Dense within communities, sparse between them
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").set("community", 0).save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").set("community", 0).save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").set("community", 1).save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").set("community", 1).save();
      // Within community edges
      a.newEdge("EDGE", b, true, (Object[]) null).save();
      b.newEdge("EDGE", a, true, (Object[]) null).save();
      c.newEdge("EDGE", d, true, (Object[]) null).save();
      d.newEdge("EDGE", c, true, (Object[]) null).save();
      // One cross-community edge
      a.newEdge("EDGE", c, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void modularityScoreReturnsSingleRow() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.modularityScore('community') YIELD modularity, communities, edgeCount "
            + "RETURN modularity, communities, edgeCount");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    assertThat(rs.hasNext()).isFalse();
    final Object modularity = result.getProperty("modularity");
    final Object communities = result.getProperty("communities");
    final Object edgeCount = result.getProperty("edgeCount");
    assertThat(modularity).isNotNull();
    assertThat(communities).isNotNull();
    assertThat(edgeCount).isNotNull();
  }

  @Test
  void modularityScoreCommunityCount() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.modularityScore('community') YIELD communities RETURN communities");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    final Object val = result.getProperty("communities");
    assertThat(((Number) val).intValue()).isEqualTo(2);
  }

  @Test
  void modularityScoreEdgeCount() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.modularityScore('community') YIELD edgeCount RETURN edgeCount");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    final Object val = result.getProperty("edgeCount");
    // 5 directed edges total
    assertThat(((Number) val).longValue()).isEqualTo(5L);
  }

  @Test
  void modularityScoreGoodPartitionIsPositive() {
    // With well-separated communities, modularity should be positive
    final ResultSet rs = database.query("opencypher",
        "CALL algo.modularityScore('community') YIELD modularity RETURN modularity");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    final Object val = result.getProperty("modularity");
    // Good partition with mostly intra-community edges should yield positive modularity
    assertThat(((Number) val).doubleValue()).isGreaterThan(0.0);
  }

  @Test
  void modularityScoreWithRelType() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.modularityScore('community', 'EDGE') YIELD modularity, communities "
            + "RETURN modularity, communities");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    final Object modularityVal = result.getProperty("modularity");
    assertThat(modularityVal).isNotNull();
    final Object communities = result.getProperty("communities");
    assertThat(((Number) communities).intValue()).isEqualTo(2);
  }
}
