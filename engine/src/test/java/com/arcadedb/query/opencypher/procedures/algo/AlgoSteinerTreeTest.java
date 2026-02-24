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
 * Tests for the algo.steinerTree Cypher procedure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoSteinerTreeTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-steinertree");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("City");
    database.getSchema().createEdgeType("ROAD");

    // Path graph: A-B-C-D-E, terminals A, C, E
    // Steiner tree must connect A, C, E via path A-B-C-D-E
    // With unit weights the Steiner tree cost = 4 (all 4 edges)
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("City").set("name", "A").save();
      final MutableVertex b = database.newVertex("City").set("name", "B").save();
      final MutableVertex c = database.newVertex("City").set("name", "C").save();
      final MutableVertex d = database.newVertex("City").set("name", "D").save();
      final MutableVertex e = database.newVertex("City").set("name", "E").save();
      a.newEdge("ROAD", b, true, (Object[]) null).save();
      b.newEdge("ROAD", c, true, (Object[]) null).save();
      c.newEdge("ROAD", d, true, (Object[]) null).save();
      d.newEdge("ROAD", e, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void steinerTreeConnectsTerminals() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:City {name:'A'}), (c:City {name:'C'}), (e:City {name:'E'}) " +
            "CALL algo.steinerTree([a, c, e]) " +
            "YIELD source, target, weight RETURN source, target, weight");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    // At minimum 2 edges needed (A→C and C→E)
    assertThat(results).isNotEmpty();
    for (final Result r : results) {
      assertThat((Object) r.getProperty("source")).isNotNull();
      assertThat((Object) r.getProperty("target")).isNotNull();
    }
  }

  @Test
  void steinerTreeTotalWeightIsPositive() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:City {name:'A'}), (e:City {name:'E'}) " +
            "CALL algo.steinerTree([a, e]) " +
            "YIELD totalWeight RETURN totalWeight");

    assertThat(rs.hasNext()).isTrue();
    final double tw = ((Number) rs.next().getProperty("totalWeight")).doubleValue();
    assertThat(tw).isGreaterThan(0.0);
  }

  @Test
  void steinerTreeWithTwoTerminals() {
    // Two terminals: A and C — should find path A-B-C (2 edges)
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:City {name:'A'}), (c:City {name:'C'}) " +
            "CALL algo.steinerTree([a, c]) " +
            "YIELD source, target RETURN source, target");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).isNotEmpty();
  }
}
