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
 * Tests for the algo.simRank Cypher procedure.
 */
class AlgoSimRankTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-simrank");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Item");
    database.getSchema().createEdgeType("LINK");

    // B and C both pointed to by A — they should be similar to each other
    // D is isolated
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Item").set("name", "A").save();
      final MutableVertex b = database.newVertex("Item").set("name", "B").save();
      final MutableVertex c = database.newVertex("Item").set("name", "C").save();
      final MutableVertex d = database.newVertex("Item").set("name", "D").save();
      a.newEdge("LINK", b, true, (Object[]) null).save();
      a.newEdge("LINK", c, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void simRankSameNodeReturnsSimilarityOne() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Item {name:'A'}), (b:Item {name:'A'}) "
            + "CALL algo.simRank(a, b) YIELD similarity "
            + "RETURN similarity");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    final Object val = result.getProperty("similarity");
    assertThat(((Number) val).doubleValue()).isEqualTo(1.0);
  }

  @Test
  void simRankReturnsSingleRow() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (b:Item {name:'B'}), (c:Item {name:'C'}) "
            + "CALL algo.simRank(b, c) YIELD similarity, nodeAId, nodeBId "
            + "RETURN similarity, nodeAId, nodeBId");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    final Object sim = result.getProperty("similarity");
    final Object nodeAId = result.getProperty("nodeAId");
    final Object nodeBId = result.getProperty("nodeBId");
    assertThat(sim).isNotNull();
    assertThat(nodeAId).isNotNull();
    assertThat(nodeBId).isNotNull();
  }

  @Test
  void simRankSimilarityBetweenZeroAndOne() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (b:Item {name:'B'}), (c:Item {name:'C'}) "
            + "CALL algo.simRank(b, c, 'LINK', 0.8, 5) YIELD similarity "
            + "RETURN similarity");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    final Object val = result.getProperty("similarity");
    final double sim = ((Number) val).doubleValue();
    assertThat(sim).isGreaterThanOrEqualTo(0.0);
    assertThat(sim).isLessThanOrEqualTo(1.0);
  }

  @Test
  void simRankNodesWithSharedPredecessorAreSimilar() {
    // B and C both pointed to by A — after iterations they should have sim > 0
    final ResultSet rs = database.query("opencypher",
        "MATCH (b:Item {name:'B'}), (c:Item {name:'C'}) "
            + "CALL algo.simRank(b, c, 'LINK', 0.8, 5) YIELD similarity "
            + "RETURN similarity");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    final Object val = result.getProperty("similarity");
    final double sim = ((Number) val).doubleValue();
    assertThat(sim).isGreaterThan(0.0);
  }
}
