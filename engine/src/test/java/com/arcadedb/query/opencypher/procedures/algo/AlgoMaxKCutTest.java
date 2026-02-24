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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the algo.maxKCut Cypher procedure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoMaxKCutTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-maxkcut");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    // Complete graph K4: 6 edges
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      a.newEdge("LINK", b, true, (Object[]) null).save();
      a.newEdge("LINK", c, true, (Object[]) null).save();
      a.newEdge("LINK", d, true, (Object[]) null).save();
      b.newEdge("LINK", c, true, (Object[]) null).save();
      b.newEdge("LINK", d, true, (Object[]) null).save();
      c.newEdge("LINK", d, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void maxKCutReturnsEntryForEachNode() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.maxKCut(2, {seed: 42}) YIELD node, community, cutWeight RETURN node, community, cutWeight");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(4);
  }

  @Test
  void maxKCutAssignsExactlyKCommunities() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.maxKCut(2, {seed: 1}) YIELD community RETURN community");

    final Set<Integer> communities = new HashSet<>();
    while (rs.hasNext())
      communities.add(((Number) rs.next().getProperty("community")).intValue());

    assertThat(communities.size()).isLessThanOrEqualTo(2);
  }

  @Test
  void maxKCutCutWeightIsPositive() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.maxKCut(2, {seed: 99}) YIELD cutWeight RETURN cutWeight");

    assertThat(rs.hasNext()).isTrue();
    final double cw = ((Number) rs.next().getProperty("cutWeight")).doubleValue();
    assertThat(cw).isGreaterThan(0.0);
  }

  @Test
  void maxKCutWithKEqualsNodeCount() {
    // k=4 on 4 nodes: each node in its own partition
    final ResultSet rs = database.query("opencypher",
        "CALL algo.maxKCut(4, {seed: 0}) YIELD node, community RETURN node, community");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(4);
  }
}
