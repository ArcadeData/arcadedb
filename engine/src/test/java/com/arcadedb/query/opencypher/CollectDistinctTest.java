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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test case for collect(DISTINCT ...) functionality.
 * This ensures that DISTINCT is properly handled in the collect() aggregation function.
 */
class CollectDistinctTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/collect-distinct-test").create();
    database.getSchema().createVertexType("Person");
    database.getSchema().createVertexType("City");
    database.getSchema().createEdgeType("LIVES_IN");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void collectDistinctStrings() {
    // Create people with duplicate cities
    database.transaction(() -> {
      MutableVertex alice = database.newVertex("Person");
      alice.set("name", "Alice");
      alice.set("city", "New York");
      alice.save();

      MutableVertex bob = database.newVertex("Person");
      bob.set("name", "Bob");
      bob.set("city", "New York"); // Same city as Alice
      bob.save();

      MutableVertex charlie = database.newVertex("Person");
      charlie.set("name", "Charlie");
      charlie.set("city", "London");
      charlie.save();

      MutableVertex david = database.newVertex("Person");
      david.set("name", "David");
      david.set("city", "London"); // Same city as Charlie
      david.save();
    });

    // Test collect without DISTINCT - should return 4 cities (with duplicates)
    ResultSet rs = database.query("opencypher", "MATCH (p:Person) RETURN collect(p.city) as cities");
    assertThat(rs.hasNext()).isTrue();
    Result result = rs.next();
    @SuppressWarnings("unchecked")
    List<String> allCities = (List<String>) result.getProperty("cities");
    assertThat(allCities).hasSize(4);

    // Test collect with DISTINCT - should return only 2 unique cities
    rs = database.query("opencypher", "MATCH (p:Person) RETURN collect(DISTINCT p.city) as uniqueCities");
    assertThat(rs.hasNext()).isTrue();
    result = rs.next();
    @SuppressWarnings("unchecked")
    List<String> uniqueCities = (List<String>) result.getProperty("uniqueCities");
    assertThat(uniqueCities).hasSize(2);
    assertThat(uniqueCities).containsExactlyInAnyOrder("New York", "London");
  }

  @Test
  void collectDistinctVertices() {
    // Create a graph where the same vertex is matched multiple times
    database.transaction(() -> {
      MutableVertex nyc = database.newVertex("City");
      nyc.set("name", "New York");
      nyc.save();

      MutableVertex london = database.newVertex("City");
      london.set("name", "London");
      london.save();

      // Multiple people living in the same city
      for (int i = 0; i < 5; i++) {
        MutableVertex person = database.newVertex("Person");
        person.set("name", "Person" + i);
        person.save();
        person.newEdge("LIVES_IN", nyc, true, (Object[]) null);
      }

      for (int i = 5; i < 8; i++) {
        MutableVertex person = database.newVertex("Person");
        person.set("name", "Person" + i);
        person.save();
        person.newEdge("LIVES_IN", london, true, (Object[]) null);
      }
    });

    // Without DISTINCT: should return 8 cities (one per person)
    ResultSet rs = database.query("opencypher",
        "MATCH (p:Person)-[:LIVES_IN]->(c:City) RETURN collect(c) as cities");
    assertThat(rs.hasNext()).isTrue();
    Result result = rs.next();
    @SuppressWarnings("unchecked")
    List<?> allCities = (List<?>) result.getProperty("cities");
    assertThat(allCities).hasSize(8);

    // With DISTINCT: should return only 2 unique cities
    rs = database.query("opencypher",
        "MATCH (p:Person)-[:LIVES_IN]->(c:City) RETURN collect(DISTINCT c) as uniqueCities");
    assertThat(rs.hasNext()).isTrue();
    result = rs.next();
    @SuppressWarnings("unchecked")
    List<?> uniqueCities = (List<?>) result.getProperty("uniqueCities");
    assertThat(uniqueCities).hasSize(2);
  }

  @Test
  void collectDistinctWithGroupBy() {
    // Create cities with multiple people having various ages
    database.transaction(() -> {
      MutableVertex nyc = database.newVertex("City");
      nyc.set("name", "New York");
      nyc.save();

      MutableVertex london = database.newVertex("City");
      london.set("name", "London");
      london.save();

      // NYC: people aged 25, 25, 30 (should collect distinct: 25, 30)
      for (int age : new int[]{25, 25, 30}) {
        MutableVertex person = database.newVertex("Person");
        person.set("name", "Person_NYC_" + age);
        person.set("age", age);
        person.save();
        person.newEdge("LIVES_IN", nyc, true, (Object[]) null);
      }

      // London: people aged 20, 20, 20, 35 (should collect distinct: 20, 35)
      for (int age : new int[]{20, 20, 20, 35}) {
        MutableVertex person = database.newVertex("Person");
        person.set("name", "Person_London_" + age);
        person.set("age", age);
        person.save();
        person.newEdge("LIVES_IN", london, true, (Object[]) null);
      }
    });

    // Group by city and collect distinct ages
    ResultSet rs = database.query("opencypher",
        """
        MATCH (p:Person)-[:LIVES_IN]->(c:City) \
        RETURN c.name as city, collect(DISTINCT p.age) as ages""");

    // Collect all results into a map for order-independent checking
    Map<String, List<Integer>> results = new HashMap<>();
    while (rs.hasNext()) {
      Result row = rs.next();
      String city = (String) row.getProperty("city");
      @SuppressWarnings("unchecked")
      List<Integer> ages = (List<Integer>) row.getProperty("ages");
      results.put(city, ages);
    }

    assertThat(results).hasSize(2);

    // Check London
    assertThat(results.containsKey("London")).isTrue();
    List<Integer> londonAges = results.get("London");
    assertThat(londonAges).hasSize(2);
    assertThat(londonAges).containsExactlyInAnyOrder(20, 35);

    // Check New York
    assertThat(results.containsKey("New York")).isTrue();
    List<Integer> nycAges = results.get("New York");
    assertThat(nycAges).hasSize(2);
    assertThat(nycAges).containsExactlyInAnyOrder(25, 30);
  }

  @Test
  void collectDistinctWithNulls() {
    // Create people with some null cities
    database.transaction(() -> {
      MutableVertex alice = database.newVertex("Person");
      alice.set("name", "Alice");
      alice.set("city", "New York");
      alice.save();

      MutableVertex bob = database.newVertex("Person");
      bob.set("name", "Bob");
      // No city set - null
      bob.save();

      MutableVertex charlie = database.newVertex("Person");
      charlie.set("name", "Charlie");
      charlie.set("city", "New York");
      charlie.save();

      MutableVertex david = database.newVertex("Person");
      david.set("name", "David");
      // No city set - null
      david.save();
    });

    // DISTINCT should exclude nulls and return only non-null unique values
    ResultSet rs = database.query("opencypher",
        "MATCH (p:Person) RETURN collect(DISTINCT p.city) as cities");
    assertThat(rs.hasNext()).isTrue();
    Result result = rs.next();
    @SuppressWarnings("unchecked")
    List<String> cities = (List<String>) result.getProperty("cities");
    // Should only contain "New York" (nulls excluded, duplicate excluded)
    assertThat(cities).hasSize(1);
    assertThat(cities).containsExactly("New York");
  }
}
