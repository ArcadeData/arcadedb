/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.query.opencypher.functions;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import org.assertj.core.api.Assertions;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Comprehensive tests for OpenCypher Aggregating functions based on Neo4j Cypher documentation.
 * Tests cover: avg(), collect(), count(), max(), min(), percentileCont(), percentileDisc(), stDev(), stDevP(), sum()
 */
class OpenCypherAggregatingFunctionsComprehensiveTest {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./databases/test-cypher-aggregating-functions");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    // Create test graph matching Neo4j documentation
    database.getSchema().createVertexType("Person");
    database.getSchema().createVertexType("Movie");
    database.getSchema().createEdgeType("ACTED_IN");
    database.getSchema().createEdgeType("KNOWS");

    database.command("opencypher",
        "CREATE " +
            "(keanu:Person {name: 'Keanu Reeves', age: 58}), " +
            "(liam:Person {name: 'Liam Neeson', age: 70}), " +
            "(carrie:Person {name: 'Carrie Anne Moss', age: 55}), " +
            "(guy:Person {name: 'Guy Pearce', age: 55}), " +
            "(kathryn:Person {name: 'Kathryn Bigelow', age: 71}), " +
            "(speed:Movie {title: 'Speed'}), " +
            "(keanu)-[:ACTED_IN]->(speed), " +
            "(keanu)-[:KNOWS]->(carrie), " +
            "(keanu)-[:KNOWS]->(liam), " +
            "(keanu)-[:KNOWS]->(kathryn), " +
            "(carrie)-[:KNOWS]->(guy), " +
            "(liam)-[:KNOWS]->(guy)");
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.drop();
  }

  // ==================== avg() Tests ====================

  @Test
  void avgBasic() {
    final ResultSet result = database.command("opencypher",
        "MATCH (p:Person) RETURN avg(p.age) AS result");
    Assertions.assertThat(result.hasNext()).isTrue();
    assertThat(((Number) result.next().getProperty("result")).doubleValue()).isCloseTo(61.8, within(0.1));
  }

  @Test
  void avgWithNulls() {
    final ResultSet result = database.command("opencypher",
        "UNWIND [1, 2, 3, null, 4] AS val RETURN avg(val) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).doubleValue()).isCloseTo(2.5, within(0.1));
  }

  @Test
  void avgNull() {
    final ResultSet result = database.command("opencypher",
        "UNWIND [null, null] AS val RETURN avg(val) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== collect() Tests ====================

  @Test
  void collectBasic() {
    final ResultSet result = database.command("opencypher",
        "MATCH (p:Person) RETURN collect(p.age) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> ages = (List<Object>) result.next().getProperty("result");
    assertThat(ages).hasSize(5);
    assertThat(ages).contains(58, 70, 55, 71);
  }

  @Test
  void collectWithNulls() {
    final ResultSet result = database.command("opencypher",
        "UNWIND [1, 2, null, 3, null, 4] AS val RETURN collect(val) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> collected = (List<Object>) result.next().getProperty("result");
    assertThat(collected).containsExactly(1L, 2L, 3L, 4L);
  }

  @Test
  void collectNull() {
    final ResultSet result = database.command("opencypher",
        "UNWIND [null, null] AS val RETURN collect(val) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> collected = (List<Object>) result.next().getProperty("result");
    assertThat(collected).isEmpty();
  }

  // ==================== count() Tests ====================

  @Test
  void countStar() {
    final ResultSet result = database.command("opencypher",
        "MATCH (p:Person {name: 'Keanu Reeves'})-->(x) RETURN count(*) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(4);
  }

  @Test
  void countExpression() {
    final ResultSet result = database.command("opencypher",
        "MATCH (p:Person) RETURN count(p.age) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(5);
  }

  @Test
  void countWithNulls() {
    final ResultSet result = database.command("opencypher",
        "UNWIND [1, 2, null, 3, null] AS val RETURN count(val) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(3);
  }

  @Test
  void countNull() {
    final ResultSet result = database.command("opencypher",
        "RETURN count(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(0);
  }

  @Test
  void countDistinct() {
    final ResultSet result = database.command("opencypher",
        "UNWIND [1, 2, 2, 3, 3, 3] AS val RETURN count(DISTINCT val) AS distinct, count(val) AS all");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    assertThat(((Number) row.getProperty("distinct")).intValue()).isEqualTo(3);
    assertThat(((Number) row.getProperty("all")).intValue()).isEqualTo(6);
  }

  @Test
  void countGroupByRelationshipType() {
    final ResultSet result = database.command("opencypher",
        "MATCH (p:Person {name: 'Keanu Reeves'})-[r]->() RETURN type(r) AS relType, count(*) AS count ORDER BY relType");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    int totalCount = 0;
    while (result.hasNext()) {
      final var row = result.next();
      final String relType = (String) row.getProperty("relType");
      final int count = ((Number) row.getProperty("count")).intValue();
      totalCount += count;
      assertThat(relType).isIn("ACTED_IN", "KNOWS");
    }
    assertThat(totalCount).isEqualTo(4);
  }

  // ==================== max() Tests ====================

  @Test
  void maxBasic() {
    final ResultSet result = database.command("opencypher",
        "MATCH (p:Person) RETURN max(p.age) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(71);
  }

  @Test
  void maxMixedTypes() {
    final ResultSet result = database.command("opencypher",
        "UNWIND [1, 'a', null, 0.2, 'b', '1', '99'] AS val RETURN max(val) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    // Numeric values are higher than strings
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(1);
  }

  @Test
  void maxLists() {
    final ResultSet result = database.command("opencypher",
        "UNWIND [[1, 'a', 89], [1, 2]] AS val RETURN max(val) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> maxList = (List<Object>) result.next().getProperty("result");
    assertThat(maxList).hasSize(2);
    assertThat(((Number) maxList.get(0)).intValue()).isEqualTo(1);
    assertThat(((Number) maxList.get(1)).intValue()).isEqualTo(2);
  }

  @Test
  void maxNull() {
    final ResultSet result = database.command("opencypher",
        "UNWIND [null, null] AS val RETURN max(val) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== min() Tests ====================

  @Test
  void minBasic() {
    final ResultSet result = database.command("opencypher",
        "MATCH (p:Person) RETURN min(p.age) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(55);
  }

  @Test
  void minMixedTypes() {
    final ResultSet result = database.command("opencypher",
        "UNWIND [1, 'a', null, 0.2, 'b', '1', '99'] AS val RETURN min(val) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    // Strings are lower than numeric values
    assertThat((String) result.next().getProperty("result")).isEqualTo("1");
  }

  @Test
  void minLists() {
    final ResultSet result = database.command("opencypher",
        "UNWIND ['d', [1, 2], ['a', 'c', 23]] AS val RETURN min(val) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> minList = (List<Object>) result.next().getProperty("result");
    assertThat(minList).hasSize(3);
    assertThat((String) minList.get(0)).isEqualTo("a");
    assertThat((String) minList.get(1)).isEqualTo("c");
    assertThat(((Number) minList.get(2)).intValue()).isEqualTo(23);
  }

  @Test
  void minNull() {
    final ResultSet result = database.command("opencypher",
        "UNWIND [null, null] AS val RETURN min(val) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== percentileCont() Tests ====================

  @Test
  void percentileContBasic() {
    final ResultSet result = database.command("opencypher",
        "MATCH (p:Person) RETURN percentileCont(p.age, 0.4) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    // Should use linear interpolation
    assertThat(((Number) result.next().getProperty("result")).doubleValue()).isCloseTo(56.8, within(1.0));
  }

  @Test
  void percentileContMedian() {
    final ResultSet result = database.command("opencypher",
        "MATCH (p:Person) RETURN percentileCont(p.age, 0.5) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).doubleValue()).isGreaterThan(50.0);
  }

  @Test
  void percentileContNull() {
    final ResultSet result = database.command("opencypher",
        "UNWIND [null, null] AS val RETURN percentileCont(val, 0.5) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== percentileDisc() Tests ====================

  @Test
  void percentileDiscBasic() {
    final ResultSet result = database.command("opencypher",
        "MATCH (p:Person) RETURN percentileDisc(p.age, 0.5) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    // Should return an actual value from the set
    final int percentile = ((Number) result.next().getProperty("result")).intValue();
    assertThat(percentile).isIn(55, 58, 70, 71);
  }

  @Test
  void percentileDiscLowPercentile() {
    final ResultSet result = database.command("opencypher",
        "MATCH (p:Person) RETURN percentileDisc(p.age, 0.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(55);
  }

  @Test
  void percentileDiscNull() {
    final ResultSet result = database.command("opencypher",
        "UNWIND [null, null] AS val RETURN percentileDisc(val, 0.5) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== stDev() Tests ====================

  @Test
  void stDevBasic() {
    final ResultSet result = database.command("opencypher",
        "MATCH (p:Person) WHERE p.name IN ['Keanu Reeves', 'Liam Neeson', 'Carrie Anne Moss'] " +
            "RETURN stDev(p.age) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    // Sample standard deviation for ages 58, 70, 55
    assertThat(((Number) result.next().getProperty("result")).doubleValue()).isCloseTo(7.937, within(0.01));
  }

  @Test
  void stDevNull() {
    final ResultSet result = database.command("opencypher",
        "UNWIND [null, null] AS val RETURN stDev(val) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).doubleValue()).isEqualTo(0.0);
  }

  // ==================== stDevP() Tests ====================

  @Test
  void stDevPBasic() {
    final ResultSet result = database.command("opencypher",
        "MATCH (p:Person) WHERE p.name IN ['Keanu Reeves', 'Liam Neeson', 'Carrie Anne Moss'] " +
            "RETURN stDevP(p.age) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    // Population standard deviation for ages 58, 70, 55
    assertThat(((Number) result.next().getProperty("result")).doubleValue()).isCloseTo(6.481, within(0.01));
  }

  @Test
  void stDevPNull() {
    final ResultSet result = database.command("opencypher",
        "UNWIND [null, null] AS val RETURN stDevP(val) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).doubleValue()).isEqualTo(0.0);
  }

  // ==================== sum() Tests ====================

  @Test
  void sumBasic() {
    final ResultSet result = database.command("opencypher",
        "MATCH (p:Person) RETURN sum(p.age) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(309);
  }

  @Test
  void sumWithNulls() {
    final ResultSet result = database.command("opencypher",
        "UNWIND [1, 2, null, 3, null, 4] AS val RETURN sum(val) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(10);
  }

  @Test
  void sumNull() {
    final ResultSet result = database.command("opencypher",
        "UNWIND [null, null] AS val RETURN sum(val) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(0);
  }

  // ==================== Combined/Integration Tests ====================

  @Test
  void aggregationsCombined() {
    final ResultSet result = database.command("opencypher",
        "MATCH (p:Person) " +
            "RETURN count(p) AS count, avg(p.age) AS avgAge, min(p.age) AS minAge, max(p.age) AS maxAge, sum(p.age) AS sumAge");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    assertThat(((Number) row.getProperty("count")).intValue()).isEqualTo(5);
    assertThat(((Number) row.getProperty("avgAge")).doubleValue()).isCloseTo(61.8, within(0.1));
    assertThat(((Number) row.getProperty("minAge")).intValue()).isEqualTo(55);
    assertThat(((Number) row.getProperty("maxAge")).intValue()).isEqualTo(71);
    assertThat(((Number) row.getProperty("sumAge")).intValue()).isEqualTo(309);
  }

  @Test
  void aggregationWithGrouping() {
    final ResultSet result = database.command("opencypher",
        "MATCH (p:Person {name: 'Keanu Reeves'})-[:KNOWS]-(f:Person) " +
            "RETURN p.name AS person, count(f) AS friendCount, avg(f.age) AS avgFriendAge");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    assertThat((String) row.getProperty("person")).isEqualTo("Keanu Reeves");
    assertThat(((Number) row.getProperty("friendCount")).intValue()).isEqualTo(3);
    assertThat(((Number) row.getProperty("avgFriendAge")).doubleValue()).isGreaterThan(50.0);
  }

  @Test
  void collectAndCount() {
    final ResultSet result = database.command("opencypher",
        "MATCH (p:Person) " +
            "RETURN collect(p.name) AS names, count(p.name) AS nameCount");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    @SuppressWarnings("unchecked")
    final List<String> names = (List<String>) row.getProperty("names");
    final int count = ((Number) row.getProperty("nameCount")).intValue();
    assertThat(names).hasSize(count);
    assertThat(count).isEqualTo(5);
  }
}
