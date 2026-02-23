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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the algo.articulationPoints Cypher procedure.
 */
class AlgoArticulationPointsTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-articulation");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("EDGE");

    // Linear path A-B-C: B is the articulation point
    // Bidirectional: A->B, B->A, B->C, C->B
    // Removing B disconnects A from C
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      a.newEdge("EDGE", b, true, (Object[]) null).save();
      b.newEdge("EDGE", a, true, (Object[]) null).save();
      b.newEdge("EDGE", c, true, (Object[]) null).save();
      c.newEdge("EDGE", b, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void atLeastOneArticulationPoint() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.articulationPoints() YIELD node RETURN node");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSizeGreaterThanOrEqualTo(1);
  }

  @Test
  void articulationPointFound() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.articulationPoints() YIELD node RETURN node.name AS name");

    final List<String> names = new ArrayList<>();
    while (rs.hasNext()) {
      final Result result = rs.next();
      final String name = (String) result.getProperty("name");
      names.add(name);
    }

    assertThat(names).contains("B");
  }

  @Test
  void endpointsNotArticulationPoints() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.articulationPoints() YIELD node RETURN node.name AS name");

    final List<String> names = new ArrayList<>();
    while (rs.hasNext()) {
      final Result result = rs.next();
      final String name = (String) result.getProperty("name");
      names.add(name);
    }

    assertThat(names).doesNotContain("A");
    assertThat(names).doesNotContain("C");
  }
}
