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
 * Tests for the algo.hits Cypher procedure.
 */
class AlgoHITSTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-hits");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("EDGE");

    // A and D are hubs pointing to B and C (pure authorities)
    // A->B, A->C, D->B, D->C
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      a.newEdge("EDGE", b, true, (Object[]) null).save();
      a.newEdge("EDGE", c, true, (Object[]) null).save();
      d.newEdge("EDGE", b, true, (Object[]) null).save();
      d.newEdge("EDGE", c, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void hitsReturnsFourNodes() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.hits() YIELD node, hubScore, authorityScore RETURN node, hubScore, authorityScore");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(4);
  }

  @Test
  void hitsScoresPositive() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.hits() YIELD node, hubScore, authorityScore RETURN node, hubScore, authorityScore");

    while (rs.hasNext()) {
      final Result result = rs.next();
      final double hubScore = ((Number) result.getProperty("hubScore")).doubleValue();
      final double authorityScore = ((Number) result.getProperty("authorityScore")).doubleValue();
      assertThat(hubScore).isGreaterThanOrEqualTo(0.0);
      assertThat(authorityScore).isGreaterThanOrEqualTo(0.0);
    }
  }

  @Test
  void hitsAuthoritiesHaveHighScore() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.hits() YIELD node, hubScore, authorityScore RETURN node.name AS name, hubScore, authorityScore");

    double scoreAuthA = 0, scoreAuthB = 0, scoreAuthC = 0, scoreAuthD = 0;
    while (rs.hasNext()) {
      final Result result = rs.next();
      final String name = (String) result.getProperty("name");
      final double authorityScore = ((Number) result.getProperty("authorityScore")).doubleValue();
      switch (name) {
        case "A" -> scoreAuthA = authorityScore;
        case "B" -> scoreAuthB = authorityScore;
        case "C" -> scoreAuthC = authorityScore;
        case "D" -> scoreAuthD = authorityScore;
      }
    }

    // B and C are pure authorities (pointed to by both A and D)
    assertThat(scoreAuthB).isGreaterThan(scoreAuthA);
    assertThat(scoreAuthC).isGreaterThan(scoreAuthD);
  }

  @Test
  void hitsHubsHaveHighHubScore() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.hits() YIELD node, hubScore, authorityScore RETURN node.name AS name, hubScore, authorityScore");

    double hubA = 0, hubB = 0, hubC = 0, hubD = 0;
    while (rs.hasNext()) {
      final Result result = rs.next();
      final String name = (String) result.getProperty("name");
      final double hubScore = ((Number) result.getProperty("hubScore")).doubleValue();
      switch (name) {
        case "A" -> hubA = hubScore;
        case "B" -> hubB = hubScore;
        case "C" -> hubC = hubScore;
        case "D" -> hubD = hubScore;
      }
    }

    // A and D are hubs (they point to authoritative nodes B and C)
    assertThat(hubA).isGreaterThan(hubB);
    assertThat(hubD).isGreaterThan(hubC);
  }
}
