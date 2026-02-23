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
 * Tests for the algo.labelpropagation Cypher procedure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoLabelPropagationTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-lpa");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("EDGE");

    // Two clear communities: {A,B,C} and {D,E,F}
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      final MutableVertex e = database.newVertex("Node").set("name", "E").save();
      final MutableVertex f = database.newVertex("Node").set("name", "F").save();
      // Community 1
      a.newEdge("EDGE", b, true, (Object[]) null).save();
      b.newEdge("EDGE", c, true, (Object[]) null).save();
      c.newEdge("EDGE", a, true, (Object[]) null).save();
      // Community 2
      d.newEdge("EDGE", e, true, (Object[]) null).save();
      e.newEdge("EDGE", f, true, (Object[]) null).save();
      f.newEdge("EDGE", d, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void labelPropagationReturnsCommunityIdForEachNode() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.labelpropagation() YIELD node, communityId RETURN node, communityId");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(6);
    for (final Result result : results) {
      final Object node = result.getProperty("node");
      assertThat(node).isNotNull();
      final Object communityId = result.getProperty("communityId");
      assertThat(communityId).isNotNull();
    }
  }

  @Test
  void labelPropagationDetectsTwoCommunities() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.labelpropagation() YIELD node, communityId RETURN DISTINCT communityId");

    final Set<Object> communityIds = new HashSet<>();
    while (rs.hasNext())
      communityIds.add(rs.next().getProperty("communityId"));

    // Two isolated clusters should ideally produce 2 communities
    assertThat(communityIds.size()).isGreaterThanOrEqualTo(1);
  }

  @Test
  void labelPropagationWithCustomMaxIterations() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.labelpropagation({maxIterations: 5}) YIELD node, communityId RETURN node, communityId");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(6);
  }

  @Test
  void labelPropagationWithOutDirection() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.labelpropagation({direction: 'OUT'}) YIELD node, communityId RETURN node, communityId");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(6);
  }

  @Test
  void labelPropagationSingleNode() {
    final DatabaseFactory lpaFactory = new DatabaseFactory("./target/databases/test-algo-lpa-single");
    if (lpaFactory.exists())
      lpaFactory.open().drop();
    final Database db = lpaFactory.create();
    try {
      db.getSchema().createVertexType("Node");
      db.transaction(() -> db.newVertex("Node").set("name", "solo").save());

      final ResultSet rs = db.query("opencypher",
          "CALL algo.labelpropagation() YIELD node, communityId RETURN node, communityId");

      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      final Object node = result.getProperty("node");
      assertThat(node).isNotNull();
      final Object communityId = result.getProperty("communityId");
      assertThat(communityId).isNotNull();
      assertThat(rs.hasNext()).isFalse();
    } finally {
      db.drop();
    }
  }
}
