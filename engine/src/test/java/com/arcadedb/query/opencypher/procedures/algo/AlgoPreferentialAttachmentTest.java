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
 * Tests for the algo.preferentialAttachment Cypher procedure.
 */
class AlgoPreferentialAttachmentTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-preferential-attachment");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("EDGE");

    // A has degree 3 (A->B, A->C, A->D)
    // E has degree 2 (E->B, E->C)
    // F has degree 1 (F->B)
    // PA score: A vs E = 3*2=6, A vs F = 3*1=3
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      final MutableVertex e = database.newVertex("Node").set("name", "E").save();
      final MutableVertex f = database.newVertex("Node").set("name", "F").save();
      a.newEdge("EDGE", b, true, (Object[]) null).save();
      a.newEdge("EDGE", c, true, (Object[]) null).save();
      a.newEdge("EDGE", d, true, (Object[]) null).save();
      e.newEdge("EDGE", b, true, (Object[]) null).save();
      e.newEdge("EDGE", c, true, (Object[]) null).save();
      f.newEdge("EDGE", b, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void preferentialAttachmentResultHasCorrectFields() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Node {name: 'A'}) " +
            "CALL algo.preferentialAttachment(a, null, 'BOTH') " +
            "YIELD node1, node2, score " +
            "RETURN node1, node2, score");

    while (rs.hasNext()) {
      final Result result = rs.next();
      final Object node1 = result.getProperty("node1");
      final Object node2 = result.getProperty("node2");
      final Object score = result.getProperty("score");
      assertThat(node1).isNotNull();
      assertThat(node2).isNotNull();
      assertThat(score).isNotNull();
    }
  }

  @Test
  void preferentialAttachmentScoresArePositive() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Node {name: 'A'}) " +
            "CALL algo.preferentialAttachment(a, null, 'BOTH') " +
            "YIELD node1, node2, score " +
            "RETURN score");

    while (rs.hasNext()) {
      final Object scoreObj = rs.next().getProperty("score");
      assertThat(((Number) scoreObj).longValue()).isGreaterThan(0L);
    }
  }

  @Test
  void preferentialAttachmentHighDegreeNodeScoresHigher() {
    // E has higher degree than F, so PA(A,E) > PA(A,F)
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Node {name: 'A'}) " +
            "CALL algo.preferentialAttachment(a, null, 'BOTH') " +
            "YIELD node1, node2, score " +
            "RETURN node2.name AS name, score");

    long scoreE = 0, scoreF = 0;
    while (rs.hasNext()) {
      final Result result = rs.next();
      final String name = (String) result.getProperty("name");
      final long score = ((Number) result.getProperty("score")).longValue();
      if ("E".equals(name))
        scoreE = score;
      else if ("F".equals(name))
        scoreF = score;
    }

    assertThat(scoreE).isGreaterThan(scoreF);
  }

  @Test
  void preferentialAttachmentIsolatedNodeExcluded() {
    // Add an isolated node G with no edges; its score should be 0 and must not appear in results
    database.transaction(() -> database.newVertex("Node").set("name", "G").save());

    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Node {name: 'A'}) " +
            "CALL algo.preferentialAttachment(a, null, 'BOTH') " +
            "YIELD node1, node2, score " +
            "RETURN node2.name AS name, score");

    final List<String> names = new ArrayList<>();
    while (rs.hasNext()) {
      final Result result = rs.next();
      names.add((String) result.getProperty("name"));
    }

    assertThat(names).doesNotContain("G");
  }
}
