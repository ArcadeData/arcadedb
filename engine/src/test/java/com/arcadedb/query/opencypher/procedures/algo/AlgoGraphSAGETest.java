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
 * Tests for the algo.graphsage Cypher procedure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoGraphSAGETest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-graphsage");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    // Chain graph: A-B-C-D
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      a.newEdge("LINK", b, true, (Object[]) null).save();
      b.newEdge("LINK", c, true, (Object[]) null).save();
      c.newEdge("LINK", d, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void graphSAGEReturnsEmbeddingForEachNode() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.graphsage({embeddingDimension: 16, layers: 2, seed: 42}) YIELD node, embedding RETURN node, embedding");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(4);
    for (final Result r : results) {
      assertThat((Object) r.getProperty("node")).isNotNull();
      final List<?> emb = (List<?>) r.getProperty("embedding");
      assertThat(emb).hasSize(16);
    }
  }

  @Test
  void graphSAGEEmbeddingValuesAreFinite() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.graphsage({embeddingDimension: 8, layers: 1, seed: 3}) YIELD embedding RETURN embedding");

    while (rs.hasNext()) {
      final List<?> emb = (List<?>) rs.next().getProperty("embedding");
      for (final Object v : emb) {
        final double d = ((Number) v).doubleValue();
        assertThat(Double.isFinite(d)).isTrue();
      }
    }
  }

  @Test
  void graphSAGEWithDefaultConfig() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.graphsage() YIELD node RETURN node");

    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }
    assertThat(count).isEqualTo(4);
  }
}
