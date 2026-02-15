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

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

;

/**
 * Tests for implicit GROUP BY in Cypher.
 * When a RETURN clause contains both aggregation functions and non-aggregated expressions,
 * the non-aggregated expressions become grouping keys.
 */
class OpenCypherGroupByTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./databases/test-group-by").create();
    database.getSchema().createVertexType("Person");
    database.getSchema().createVertexType("Product");
    database.getSchema().createEdgeType("PURCHASED");

    // Create test data:
    //   People in different cities with different ages
    //   Alice (30, NYC), Bob (25, NYC), Charlie (35, LA), David (40, LA), Eve (28, SF)
    database.command("opencypher",
        "CREATE (alice:Person {name: 'Alice', age: 30, city: 'NYC'}), " +
            "(bob:Person {name: 'Bob', age: 25, city: 'NYC'}), " +
            "(charlie:Person {name: 'Charlie', age: 35, city: 'LA'}), " +
            "(david:Person {name: 'David', age: 40, city: 'LA'}), " +
            "(eve:Person {name: 'Eve', age: 28, city: 'SF'})");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
    }
  }

  @Test
  void groupByWithCount() {
    // Group by city and count people in each city
    final ResultSet result = database.command("opencypher",
        "MATCH (n:Person) RETURN n.city AS city, count(n) AS peopleCount ORDER BY city");

    // Expecting: LA=2, NYC=2, SF=1
    assertThat(result.hasNext()).isTrue();
    final Result la = result.next();
    assertThat((String) la.getProperty("city")).isEqualTo("LA");
    assertThat(((Number) la.getProperty("peopleCount")).longValue()).isEqualTo(2L);

    assertThat(result.hasNext()).isTrue();
    final Result nyc = result.next();
    assertThat((String) nyc.getProperty("city")).isEqualTo("NYC");
    assertThat(((Number) nyc.getProperty("peopleCount")).longValue()).isEqualTo(2L);

    assertThat(result.hasNext()).isTrue();
    final Result sf = result.next();
    assertThat((String) sf.getProperty("city")).isEqualTo("SF");
    assertThat(((Number) sf.getProperty("peopleCount")).longValue()).isEqualTo(1L);

    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void groupByWithAverage() {
    // Group by city and compute average age
    // Cypher avg() always returns a Double, matching Neo4j semantics
    final ResultSet result = database.command("opencypher",
        "MATCH (n:Person) RETURN n.city AS city, avg(n.age) AS avgAge ORDER BY city");

    final Map<String, Double> expectedAverages = new HashMap<>();
    expectedAverages.put("LA", 37.5);
    expectedAverages.put("NYC", 27.5);
    expectedAverages.put("SF", 28.0);

    while (result.hasNext()) {
      final Result row = result.next();
      final String city = (String) row.getProperty("city");
      final double avgAge = ((Number) row.getProperty("avgAge")).doubleValue();
      assertThat(avgAge).isEqualTo(expectedAverages.get(city));
    }
  }

  @Test
  void groupByWithMultipleAggregations() {
    // Group by city with multiple aggregations: count, avg, min, max
    // Cypher avg() always returns a Double, matching Neo4j semantics
    final ResultSet result = database.command("opencypher",
        "MATCH (n:Person) " +
            "RETURN n.city AS city, count(n) AS cnt, avg(n.age) AS avgAge, " +
            "min(n.age) AS minAge, max(n.age) AS maxAge " +
            "ORDER BY city");

    assertThat(result.hasNext()).isTrue();
    final Result la = result.next();
    assertThat((String) la.getProperty("city")).isEqualTo("LA");
    assertThat(((Number) la.getProperty("cnt")).longValue()).isEqualTo(2L);
    assertThat(((Number) la.getProperty("avgAge")).doubleValue()).isEqualTo(37.5);
    assertThat(((Number) la.getProperty("minAge")).longValue()).isEqualTo(35L);
    assertThat(((Number) la.getProperty("maxAge")).longValue()).isEqualTo(40L);

    assertThat(result.hasNext()).isTrue();
    final Result nyc = result.next();
    assertThat((String) nyc.getProperty("city")).isEqualTo("NYC");
    assertThat(((Number) nyc.getProperty("cnt")).longValue()).isEqualTo(2L);
    assertThat(((Number) nyc.getProperty("avgAge")).doubleValue()).isEqualTo(27.5);
    assertThat(((Number) nyc.getProperty("minAge")).longValue()).isEqualTo(25L);
    assertThat(((Number) nyc.getProperty("maxAge")).longValue()).isEqualTo(30L);

    assertThat(result.hasNext()).isTrue();
    final Result sf = result.next();
    assertThat((String) sf.getProperty("city")).isEqualTo("SF");
    assertThat(((Number) sf.getProperty("cnt")).longValue()).isEqualTo(1L);
    assertThat(((Number) sf.getProperty("avgAge")).doubleValue()).isEqualTo(28.0);
    assertThat(((Number) sf.getProperty("minAge")).longValue()).isEqualTo(28L);
    assertThat(((Number) sf.getProperty("maxAge")).longValue()).isEqualTo(28L);

    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void groupByMultipleKeys() {
    // Create more test data with multiple grouping dimensions
    database.command("opencypher",
        "CREATE (p:Person {name: 'Frank', age: 30, city: 'NYC', department: 'Engineering'}), " +
            "(q:Person {name: 'Grace', age: 35, city: 'NYC', department: 'Engineering'}), " +
            "(r:Person {name: 'Henry', age: 40, city: 'NYC', department: 'Sales'}), " +
            "(s:Person {name: 'Iris', age: 45, city: 'LA', department: 'Engineering'})");

    // Group by both city and department
    final ResultSet result = database.command("opencypher",
        "MATCH (n:Person) WHERE n.department IS NOT NULL " +
            "RETURN n.city AS city, n.department AS dept, count(n) AS cnt " +
            "ORDER BY city, dept");

    assertThat(result.hasNext()).isTrue();
    final Result laEng = result.next();
    assertThat((String) laEng.getProperty("city")).isEqualTo("LA");
    assertThat((String) laEng.getProperty("dept")).isEqualTo("Engineering");
    assertThat(((Number) laEng.getProperty("cnt")).longValue()).isEqualTo(1L);

    assertThat(result.hasNext()).isTrue();
    final Result nycEng = result.next();
    assertThat((String) nycEng.getProperty("city")).isEqualTo("NYC");
    assertThat((String) nycEng.getProperty("dept")).isEqualTo("Engineering");
    assertThat(((Number) nycEng.getProperty("cnt")).longValue()).isEqualTo(2L);

    assertThat(result.hasNext()).isTrue();
    final Result nycSales = result.next();
    assertThat((String) nycSales.getProperty("city")).isEqualTo("NYC");
    assertThat((String) nycSales.getProperty("dept")).isEqualTo("Sales");
    assertThat(((Number) nycSales.getProperty("cnt")).longValue()).isEqualTo(1L);

    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void pureAggregationWithoutGrouping() {
    // Pure aggregation without grouping (should use AggregationStep, not GroupByAggregationStep)
    // Cypher avg() always returns a Double, matching Neo4j semantics
    final ResultSet result = database.command("opencypher",
        "MATCH (n:Person) RETURN count(n) AS total, avg(n.age) AS avgAge");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat(((Number) row.getProperty("total")).longValue()).isEqualTo(5L);
    // Ages: 30, 25, 35, 40, 28 -> sum=158, count=5, avg=31.6
    assertThat(((Number) row.getProperty("avgAge")).doubleValue()).isEqualTo(31.6);
    assertThat(result.hasNext()).isFalse();
  }

  /**
   * Regression test for https://github.com/ArcadeData/arcadedb/issues/3425
   * Cypher avg() was returning integer (61) instead of float (61.8) for integer inputs.
   */
  @Test
  void avgReturnsFloatForIntegerInputs() {
    // Use exact data from the issue
    database.command("opencypher",
        "CREATE (:Person {age: 58}), (:Person {age: 70}), (:Person {age: 55}), (:Person {age: 55}), (:Person {age: 71})");

    // avg of 58+70+55+55+71 = 309, 309/5 = 61.8
    // Note: there are also 5 persons from setUp (ages 30,25,35,40,28), but we filter by age >= 55
    final ResultSet result = database.command("opencypher",
        "MATCH (p:Person) WHERE p.age >= 55 RETURN avg(p.age) as result");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    // Must be 61.8, not 61 (the bug was integer truncation)
    assertThat(((Number) row.getProperty("result")).doubleValue()).isEqualTo(61.8);
    assertThat(result.hasNext()).isFalse();
  }
}
