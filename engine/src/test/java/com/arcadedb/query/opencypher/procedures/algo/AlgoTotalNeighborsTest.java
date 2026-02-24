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
 * Tests for the algo.totalNeighbors Cypher procedure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoTotalNeighborsTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-totalneighbors");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");

    // A knows B, C, D
    // E knows B, C, F
    // Total neighbors of A and E: {B,C,D} ∪ {B,C,F} = {B,C,D,F} → size 4
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Person").set("name", "A").save();
      final MutableVertex b = database.newVertex("Person").set("name", "B").save();
      final MutableVertex c = database.newVertex("Person").set("name", "C").save();
      final MutableVertex d = database.newVertex("Person").set("name", "D").save();
      final MutableVertex e = database.newVertex("Person").set("name", "E").save();
      final MutableVertex f = database.newVertex("Person").set("name", "F").save();
      a.newEdge("KNOWS", b, true, (Object[]) null).save();
      a.newEdge("KNOWS", c, true, (Object[]) null).save();
      a.newEdge("KNOWS", d, true, (Object[]) null).save();
      e.newEdge("KNOWS", b, true, (Object[]) null).save();
      e.newEdge("KNOWS", c, true, (Object[]) null).save();
      e.newEdge("KNOWS", f, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void totalNeighborsReturnsCorrectUnionSize() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Person {name: 'A'}), (e:Person {name: 'E'}) " +
            "CALL algo.totalNeighbors(a, e, 'KNOWS', 'BOTH') " +
            "YIELD node1, node2, coefficient RETURN coefficient");

    assertThat(rs.hasNext()).isTrue();
    final long total = ((Number) rs.next().getProperty("coefficient")).longValue();
    // |{B,C,D}| + |{B,C,F}| - |{B,C}| = 3 + 3 - 2 = 4
    assertThat(total).isEqualTo(4L);
  }

  @Test
  void totalNeighborsYieldsNodes() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Person {name: 'A'}), (e:Person {name: 'E'}) " +
            "CALL algo.totalNeighbors(a, e) " +
            "YIELD node1, node2, coefficient RETURN node1, node2, coefficient");

    assertThat(rs.hasNext()).isTrue();
    final Result r = rs.next();
    assertThat((Object) r.getProperty("node1")).isNotNull();
    assertThat((Object) r.getProperty("node2")).isNotNull();
    assertThat(((Number) r.getProperty("coefficient")).longValue()).isGreaterThan(0L);
  }
}
