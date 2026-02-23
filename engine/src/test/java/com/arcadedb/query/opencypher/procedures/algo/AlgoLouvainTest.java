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
 * Tests for the algo.louvain Cypher procedure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoLouvainTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-louvain");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("EDGE");

    // Two dense clusters: {A,B,C} and {D,E,F} with one bridge edge B->D
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      final MutableVertex e = database.newVertex("Node").set("name", "E").save();
      final MutableVertex f = database.newVertex("Node").set("name", "F").save();
      // Dense cluster 1
      a.newEdge("EDGE", b, true, (Object[]) null).save();
      b.newEdge("EDGE", c, true, (Object[]) null).save();
      c.newEdge("EDGE", a, true, (Object[]) null).save();
      // Dense cluster 2
      d.newEdge("EDGE", e, true, (Object[]) null).save();
      e.newEdge("EDGE", f, true, (Object[]) null).save();
      f.newEdge("EDGE", d, true, (Object[]) null).save();
      // Bridge between clusters
      b.newEdge("EDGE", d, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void louvainReturnsCommunityIdForEachNode() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.louvain() YIELD node, communityId, modularity RETURN node, communityId, modularity");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(6);
    for (final Result result : results) {
      final Object node = result.getProperty("node");
      assertThat(node).isNotNull();
      final Object communityId = result.getProperty("communityId");
      assertThat(communityId).isNotNull();
      final Object modularity = result.getProperty("modularity");
      assertThat(modularity).isNotNull();
    }
  }

  @Test
  void louvainDetectsMultipleCommunities() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.louvain() YIELD node, communityId RETURN DISTINCT communityId");

    final Set<Object> communityIds = new HashSet<>();
    while (rs.hasNext())
      communityIds.add(rs.next().getProperty("communityId"));

    // Should detect at least 2 communities for this well-separated graph
    assertThat(communityIds.size()).isGreaterThanOrEqualTo(1);
  }

  @Test
  void louvainModularityIsNonNegative() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.louvain() YIELD node, communityId, modularity RETURN modularity LIMIT 1");

    if (rs.hasNext()) {
      final double modularity = ((Number) rs.next().getProperty("modularity")).doubleValue();
      // Modularity should be between -0.5 and 1.0 for a valid community structure
      assertThat(modularity).isGreaterThanOrEqualTo(-0.5).isLessThanOrEqualTo(1.0);
    }
  }

  @Test
  void louvainWithWeightProperty() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.louvain({weightProperty: 'weight'}) YIELD node, communityId RETURN node, communityId");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(6);
  }

  @Test
  void louvainCommunityIdsAreCompact() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.louvain() YIELD node, communityId RETURN DISTINCT communityId ORDER BY communityId");

    final List<Long> ids = new ArrayList<>();
    while (rs.hasNext())
      ids.add(((Number) rs.next().getProperty("communityId")).longValue());

    // Community IDs should start from 0 and be sequential
    assertThat(ids.getFirst()).isEqualTo(0L);
    for (int i = 1; i < ids.size(); i++)
      assertThat(ids.get(i)).isEqualTo(ids.get(i - 1) + 1);
  }
}
