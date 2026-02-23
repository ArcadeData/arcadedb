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
 * Tests for the algo.assortativity Cypher procedure.
 */
class AlgoAssortativityTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-assortativity");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("EDGE");

    // Star graph: hub H connected to leaves A, B, C, D
    // H has high degree, leaves have low degree -> disassortative (negative assortativity)
    database.transaction(() -> {
      final MutableVertex h = database.newVertex("Node").set("name", "H").save();
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      h.newEdge("EDGE", a, true, (Object[]) null).save();
      h.newEdge("EDGE", b, true, (Object[]) null).save();
      h.newEdge("EDGE", c, true, (Object[]) null).save();
      h.newEdge("EDGE", d, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void assortativityReturnsSingleRow() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.assortativity() YIELD assortativity, edgeCount "
            + "RETURN assortativity, edgeCount");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    assertThat(rs.hasNext()).isFalse();
    final Object assort = result.getProperty("assortativity");
    final Object edgeCount = result.getProperty("edgeCount");
    assertThat(assort).isNotNull();
    assertThat(edgeCount).isNotNull();
  }

  @Test
  void assortativityEdgeCount() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.assortativity() YIELD edgeCount RETURN edgeCount");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    final Object val = result.getProperty("edgeCount");
    // 4 directed edges (hub -> each leaf)
    assertThat(((Number) val).longValue()).isEqualTo(4L);
  }

  @Test
  void assortativityStarIsNegative() {
    // Star graph: hub connects to low-degree leaves -> disassortative (negative)
    final ResultSet rs = database.query("opencypher",
        "CALL algo.assortativity() YIELD assortativity RETURN assortativity");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    final Object val = result.getProperty("assortativity");
    // Star graph should have negative (or zero) assortativity
    assertThat(((Number) val).doubleValue()).isLessThanOrEqualTo(0.0);
  }

  @Test
  void assortativityWithRelType() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.assortativity('EDGE') YIELD assortativity, edgeCount "
            + "RETURN assortativity, edgeCount");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    final Object assort = result.getProperty("assortativity");
    assertThat(assort).isNotNull();
  }

  @Test
  void assortativityRangeIsValid() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.assortativity() YIELD assortativity RETURN assortativity");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    final Object val = result.getProperty("assortativity");
    final double r = ((Number) val).doubleValue();
    // Assortativity coefficient is bounded [-1, 1]
    assertThat(r).isGreaterThanOrEqualTo(-1.0);
    assertThat(r).isLessThanOrEqualTo(1.0);
  }
}
