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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test: anonymous middle node in a linear multi-hop chain must match rows.
 * <p>
 * (a:Person)-[:KNOWS]-&gt;(:City)-[:LOCATED_IN]-&gt;(b:Country) must return the same
 * rows as the equivalent pattern with a named middle node.
 */
class Issue4092AnonMiddleNodeTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/issue4092-anon-middle-node").create();
    database.getSchema().createVertexType("Person");
    database.getSchema().createVertexType("City");
    database.getSchema().createVertexType("Country");
    database.getSchema().createEdgeType("KNOWS");
    database.getSchema().createEdgeType("LOCATED_IN");

    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (:Person {name:'Alice'}), (:City {name:'New York'}), (:Country {name:'USA'})");
      database.command("opencypher",
          "MATCH (a:Person {name:'Alice'}), (c:City {name:'New York'}), (u:Country {name:'USA'}) " +
              "CREATE (a)-[:KNOWS]->(c), (c)-[:LOCATED_IN]->(u)");
    });
  }

  @AfterEach
  void tearDown() {
    database.drop();
  }

  /** Anonymous middle node: two-hop chain must match the one row. */
  @Test
  void twoHopChainWithAnonMiddleNode() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:Person)-[:KNOWS]->(:City)-[:LOCATED_IN]->(b:Country) " +
            "RETURN a.name AS person_name, b.name AS country_name")) {
      final List<Result> rows = new ArrayList<>();
      rs.forEachRemaining(rows::add);
      assertThat(rows).hasSize(1);
      assertThat((String) rows.get(0).getProperty("person_name")).isEqualTo("Alice");
      assertThat((String) rows.get(0).getProperty("country_name")).isEqualTo("USA");
    }
  }

  /** Named middle node must match the same row (control case). */
  @Test
  void twoHopChainWithNamedMiddleNode() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:Person)-[:KNOWS]->(c:City)-[:LOCATED_IN]->(b:Country) " +
            "RETURN a.name AS person_name, b.name AS country_name")) {
      final List<Result> rows = new ArrayList<>();
      rs.forEachRemaining(rows::add);
      assertThat(rows).hasSize(1);
      assertThat((String) rows.get(0).getProperty("person_name")).isEqualTo("Alice");
      assertThat((String) rows.get(0).getProperty("country_name")).isEqualTo("USA");
    }
  }

  /** Anonymous middle node with aggregation: collect(a.name) must work. */
  @Test
  void twoHopChainAnonMiddleNodeWithAggregation() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:Person)-[:KNOWS]->(:City)-[:LOCATED_IN]->(b:Country) " +
            "RETURN b.name AS country_name, collect(a.name) AS people")) {
      final List<Result> rows = new ArrayList<>();
      rs.forEachRemaining(rows::add);
      assertThat(rows).hasSize(1);
      assertThat((String) rows.get(0).getProperty("country_name")).isEqualTo("USA");
      final List<?> people = (List<?>) rows.get(0).getProperty("people");
      assertThat(people.stream().map(Object::toString).toList()).containsExactly("Alice");
    }
  }

  /** Anonymous source node in a single hop must still match. */
  @Test
  void singleHopWithAnonSourceNode() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (:Person)-[:KNOWS]->(c:City) RETURN c.name AS city_name")) {
      final List<Result> rows = new ArrayList<>();
      rs.forEachRemaining(rows::add);
      assertThat(rows).hasSize(1);
      assertThat((String) rows.get(0).getProperty("city_name")).isEqualTo("New York");
    }
  }

  /** Anonymous target node in a single hop must still match. */
  @Test
  void singleHopWithAnonTargetNode() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:Person)-[:KNOWS]->(:City) RETURN a.name AS person_name")) {
      final List<Result> rows = new ArrayList<>();
      rs.forEachRemaining(rows::add);
      assertThat(rows).hasSize(1);
      assertThat((String) rows.get(0).getProperty("person_name")).isEqualTo("Alice");
    }
  }
}
