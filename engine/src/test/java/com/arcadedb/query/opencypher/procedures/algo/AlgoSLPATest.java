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

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the algo.slpa Cypher procedure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoSLPATest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-slpa");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");

    // Two cliques connected by a bridge
    // Clique 1: A-B, A-C, B-C
    // Clique 2: D-E, D-F, E-F
    // Bridge: C-D
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Person").set("name", "A").save();
      final MutableVertex b = database.newVertex("Person").set("name", "B").save();
      final MutableVertex c = database.newVertex("Person").set("name", "C").save();
      final MutableVertex d = database.newVertex("Person").set("name", "D").save();
      final MutableVertex e = database.newVertex("Person").set("name", "E").save();
      final MutableVertex f = database.newVertex("Person").set("name", "F").save();
      a.newEdge("KNOWS", b, true, (Object[]) null).save();
      a.newEdge("KNOWS", c, true, (Object[]) null).save();
      b.newEdge("KNOWS", c, true, (Object[]) null).save();
      d.newEdge("KNOWS", e, true, (Object[]) null).save();
      d.newEdge("KNOWS", f, true, (Object[]) null).save();
      e.newEdge("KNOWS", f, true, (Object[]) null).save();
      c.newEdge("KNOWS", d, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void slpaReturnsEntryForEachNode() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.slpa({iterations: 10, threshold: 0.1, seed: 42}) YIELD node, communities RETURN node, communities");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(6);
  }

  @Test
  void slpaCommunitiesAreNonEmpty() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.slpa({iterations: 10, threshold: 0.1, seed: 42}) YIELD node, communities RETURN communities");

    while (rs.hasNext()) {
      final Object communities = rs.next().getProperty("communities");
      assertThat(communities).isNotNull();
      assertThat((List<?>) communities).isNotEmpty();
    }
  }

  @Test
  void slpaWithDefaultConfig() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.slpa() YIELD node, communities RETURN node, communities");

    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }
    assertThat(count).isEqualTo(6);
  }
}
