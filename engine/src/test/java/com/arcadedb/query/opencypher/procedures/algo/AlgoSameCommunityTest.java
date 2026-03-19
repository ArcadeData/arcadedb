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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the algo.sameCommunity Cypher procedure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoSameCommunityTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-samecommunity");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Person");

    database.transaction(() -> {
      database.newVertex("Person").set("name", "Alice").set("community", 1).save();
      database.newVertex("Person").set("name", "Bob").set("community", 1).save();
      database.newVertex("Person").set("name", "Carol").set("community", 2).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void sameCommunityReturnsOneForSameCommunity() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) " +
            "CALL algo.sameCommunity(a, b, 'community') " +
            "YIELD node1, node2, coefficient RETURN coefficient");

    assertThat(rs.hasNext()).isTrue();
    final double coeff = ((Number) rs.next().getProperty("coefficient")).doubleValue();
    assertThat(coeff).isEqualTo(1.0);
  }

  @Test
  void sameCommunityReturnsZeroForDifferentCommunity() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Person {name: 'Alice'}), (c:Person {name: 'Carol'}) " +
            "CALL algo.sameCommunity(a, c, 'community') " +
            "YIELD node1, node2, coefficient RETURN coefficient");

    assertThat(rs.hasNext()).isTrue();
    final double coeff = ((Number) rs.next().getProperty("coefficient")).doubleValue();
    assertThat(coeff).isEqualTo(0.0);
  }

  @Test
  void sameCommunityReturnsBothNodes() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) " +
            "CALL algo.sameCommunity(a, b, 'community') " +
            "YIELD node1, node2, coefficient RETURN node1, node2");

    assertThat(rs.hasNext()).isTrue();
    final Result r = rs.next();
    assertThat((Object) r.getProperty("node1")).isNotNull();
    assertThat((Object) r.getProperty("node2")).isNotNull();
  }
}
