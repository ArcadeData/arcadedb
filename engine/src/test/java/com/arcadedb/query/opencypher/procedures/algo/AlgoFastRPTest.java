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
 * Tests for the algo.fastrp Cypher procedure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoFastRPTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-fastrp");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      a.newEdge("LINK", b, true, (Object[]) null).save();
      b.newEdge("LINK", c, true, (Object[]) null).save();
      a.newEdge("LINK", c, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void fastRPReturnsEmbeddingForEachNode() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.fastrp({dimensions: 16, seed: 42}) YIELD node, embedding RETURN node, embedding");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(3);
    for (final Result r : results) {
      assertThat((Object) r.getProperty("node")).isNotNull();
      final List<?> embedding = (List<?>) r.getProperty("embedding");
      assertThat(embedding).hasSize(16);
    }
  }

  @Test
  void fastRPEmbeddingsAreNormalized() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.fastrp({dimensions: 32, iterations: 2, seed: 7}) YIELD node, embedding RETURN embedding");

    while (rs.hasNext()) {
      final List<?> emb = (List<?>) rs.next().getProperty("embedding");
      double norm = 0.0;
      for (final Object v : emb)
        norm += ((Number) v).doubleValue() * ((Number) v).doubleValue();
      // Should be ~1.0 (unit length after L2 normalisation)
      assertThat(Math.sqrt(norm)).isBetween(0.0, 1.01);
    }
  }

  @Test
  void fastRPWithSeedIsReproducible() {
    // Run twice with same seed and collect all embeddings; sets must be equal
    final String query = "CALL algo.fastrp({dimensions: 8, seed: 123}) YIELD embedding RETURN embedding";
    final List<List<?>> run1 = new ArrayList<>();
    final ResultSet rs1 = database.query("opencypher", query);
    while (rs1.hasNext())
      run1.add((List<?>) rs1.next().getProperty("embedding"));

    final List<List<?>> run2 = new ArrayList<>();
    final ResultSet rs2 = database.query("opencypher", query);
    while (rs2.hasNext())
      run2.add((List<?>) rs2.next().getProperty("embedding"));

    assertThat(run1).hasSize(run2.size());
    // Each embedding from run1 must appear in run2 (order may differ due to graph scan)
    for (final List<?> e : run1)
      assertThat(run2).contains(e);
  }
}
