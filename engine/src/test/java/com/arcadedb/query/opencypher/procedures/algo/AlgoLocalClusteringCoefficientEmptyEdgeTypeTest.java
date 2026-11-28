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
 * A Graph Analytical View materialises a slice only for an edge type that has edges, so the kernel of
 * {@code algo.localClusteringCoefficient} finds no CSR for a declared type nobody has written yet. Asked for that one type, it
 * dereferenced the missing index; the same call without a view, and the call over several types, answer zero coefficients.
 */
class AlgoLocalClusteringCoefficientEmptyEdgeTypeTest {
  private static final String QUERY =
      "CALL algo.localClusteringCoefficient('%s') YIELD node, localClusteringCoefficient RETURN node.name AS name, localClusteringCoefficient AS lcc";

  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-lcc-empty-edge-type");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("N");
    database.getSchema().createEdgeType("USED");
    database.getSchema().createEdgeType("UNUSED");

    database.transaction(() -> {
      final MutableVertex a = database.newVertex("N").set("name", "a").save();
      final MutableVertex b = database.newVertex("N").set("name", "b").save();
      final MutableVertex c = database.newVertex("N").set("name", "c").save();
      a.newEdge("USED", b).save();
      b.newEdge("USED", c).save();
      c.newEdge("USED", a).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void edgeTypeWithoutEdgesAnswersZeroCoefficientsOverAView() {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gav VERTEX TYPES (N) EDGE TYPES (USED, UNUSED) UPDATE MODE OFF").close();

    assertThat(coefficients("UNUSED")).containsExactly(0.0, 0.0, 0.0);
    assertThat(coefficients("USED")).containsExactly(1.0, 1.0, 1.0);
  }

  @Test
  void viewAndRecordPathAgree() {
    final List<Double> withoutView = coefficients("UNUSED");
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gav VERTEX TYPES (N) EDGE TYPES (USED, UNUSED) UPDATE MODE OFF").close();

    assertThat(coefficients("UNUSED")).isEqualTo(withoutView);
  }

  private List<Double> coefficients(final String edgeType) {
    final List<Double> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", String.format(QUERY, edgeType))) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        values.add(((Number) row.getProperty("lcc")).doubleValue());
      }
    }
    return values;
  }
}
