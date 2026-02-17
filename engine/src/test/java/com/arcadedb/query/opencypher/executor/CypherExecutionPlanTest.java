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
package com.arcadedb.query.opencypher.executor;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.ExecutionPlan;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for CypherExecutionPlan.
 * Tests execution plan functionality, explain, profile, and query optimization.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherExecutionPlanTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/test-cypher-execution-plan").create();
    database.getSchema().createVertexType("Person");
    database.getSchema().createVertexType("Company");
    database.getSchema().createEdgeType("WORKS_FOR");
    database.getSchema().createEdgeType("KNOWS");

    database.transaction(() -> {
      final var alice = database.newVertex("Person")
          .set("name", "Alice")
          .set("age", 30)
          .set("city", "New York")
          .save();

      final var bob = database.newVertex("Person")
          .set("name", "Bob")
          .set("age", 25)
          .set("city", "Boston")
          .save();

      final var charlie = database.newVertex("Person")
          .set("name", "Charlie")
          .set("age", 35)
          .set("city", "Chicago")
          .save();

      final var acme = database.newVertex("Company")
          .set("name", "Acme Corp")
          .save();

      alice.newEdge("KNOWS", bob, true, (Object[]) null).save();
      bob.newEdge("KNOWS", charlie, true, (Object[]) null).save();
      alice.newEdge("WORKS_FOR", acme, true, (Object[]) null).save();
      bob.newEdge("WORKS_FOR", acme, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null) {
      database.drop();
    }
  }

  @Test
  void shouldExecuteSimpleQuery() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) RETURN p.name AS name");

    final var results = resultSet.stream().toList();
    assertThat(results).hasSize(3);
    assertThat(results.stream().map(r -> r.<String>getProperty("name")).toList())
        .containsExactlyInAnyOrder("Alice", "Bob", "Charlie");
  }

  @Test
  void shouldExecuteQueryWithWhereClause() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) WHERE p.age > 25 RETURN p.name AS name");

    final var results = resultSet.stream().toList();
    assertThat(results).hasSize(2);
    assertThat(results.stream().map(r -> r.<String>getProperty("name")).toList())
        .containsExactlyInAnyOrder("Alice", "Charlie");
  }

  @Test
  void shouldExecuteQueryWithOrderBy() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) RETURN p.name AS name ORDER BY p.age");

    final var results = resultSet.stream().toList();
    assertThat(results).hasSize(3);
    assertThat(results.get(0).<String>getProperty("name")).isEqualTo("Bob");
    assertThat(results.get(1).<String>getProperty("name")).isEqualTo("Alice");
    assertThat(results.get(2).<String>getProperty("name")).isEqualTo("Charlie");
  }

  @Test
  void shouldExecuteQueryWithLimit() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) RETURN p.name AS name LIMIT 2");

    final var results = resultSet.stream().toList();
    assertThat(results).hasSize(2);
  }

  @Test
  void shouldExecuteQueryWithSkip() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) RETURN p.name AS name ORDER BY p.age SKIP 1");

    final var results = resultSet.stream().toList();
    assertThat(results).hasSize(2);
    assertThat(results.get(0).<String>getProperty("name")).isEqualTo("Alice");
  }

  @Test
  void shouldExecuteQueryWithSkipAndLimit() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) RETURN p.name AS name ORDER BY p.age SKIP 1 LIMIT 1");

    final var results = resultSet.stream().toList();
    assertThat(results).hasSize(1);
    assertThat(results.get(0).<String>getProperty("name")).isEqualTo("Alice");
  }

  @Test
  void shouldExecuteQueryWithAggregation() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) RETURN count(p) AS total, sum(p.age) AS totalAge");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Number total = result.getProperty("total");
    assertThat(total.longValue()).isEqualTo(3L);
    final Number totalAge = result.getProperty("totalAge");
    assertThat(totalAge.longValue()).isEqualTo(90L);
  }

  @Test
  void shouldExecuteQueryWithGroupBy() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) RETURN p.city AS city, count(p) AS total ORDER BY city");

    final var results = resultSet.stream().toList();
    assertThat(results).hasSize(3);
  }

  @Test
  void shouldExecuteQueryWithDistinct() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) RETURN DISTINCT p.age > 25 AS isOlder");

    final var results = resultSet.stream().toList();
    assertThat(results).hasSizeGreaterThanOrEqualTo(1);
  }

  @Test
  void shouldExecuteQueryWithUnion() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) WHERE p.age < 30 RETURN p.name AS name " +
        "UNION " +
        "MATCH (c:Company) RETURN c.name AS name");

    final var results = resultSet.stream().toList();
    assertThat(results).hasSizeGreaterThanOrEqualTo(2);
  }

  @Test
  void shouldExecuteQueryWithUnionAll() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) WHERE p.age < 30 RETURN p.name AS name " +
        "UNION ALL " +
        "MATCH (p:Person) WHERE p.age < 30 RETURN p.name AS name");

    final var results = resultSet.stream().toList();
    assertThat(results).hasSizeGreaterThanOrEqualTo(2);
  }

  @Test
  void shouldExecutePatternMatch() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS from, b.name AS to");

    final var results = resultSet.stream().toList();
    assertThat(results).hasSize(2);
  }

  @Test
  void shouldExecuteMultiHopPattern() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) RETURN DISTINCT a.name AS from, b.name AS to");

    final var results = resultSet.stream().toList();
    assertThat(results).hasSizeGreaterThanOrEqualTo(2);
  }

  @Test
  void shouldExecuteQueryWithOptionalMatch() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(friend:Person) RETURN p.name AS name, friend.name AS friendName");

    final var results = resultSet.stream().toList();
    assertThat(results).hasSize(3);
  }

  @Test
  void shouldExecuteQueryWithWith() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) WITH p.age AS age WHERE age > 25 RETURN age");

    final var results = resultSet.stream().toList();
    assertThat(results).hasSize(2);
  }

  @Test
  void shouldExecuteQueryWithMultipleWith() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) " +
        "WITH p.age AS age WHERE age > 25 " +
        "WITH age * 2 AS doubleAge " +
        "RETURN doubleAge");

    final var results = resultSet.stream().toList();
    assertThat(results).hasSize(2);
  }

  @Test
  void shouldProvideExplainPlan() {
    final ResultSet resultSet = database.query("opencypher",
        "EXPLAIN MATCH (p:Person) WHERE p.age > 25 RETURN p.name");

    assertThat((Object) resultSet).isNotNull();
    final boolean hasNext = resultSet.hasNext();
    assertThat(hasNext).isTrue();
  }

  @Test
  void shouldProvideProfilePlan() {
    final ResultSet resultSet = database.query("opencypher",
        "PROFILE MATCH (p:Person) WHERE p.age > 25 RETURN p.name");

    assertThat((Object) resultSet).isNotNull();
    // Profile returns profile info plus results
    final var results = resultSet.stream().toList();
    assertThat(results.size()).isGreaterThanOrEqualTo(2);
  }

  @Test
  void shouldHandleEmptyResult() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) WHERE p.age > 100 RETURN p.name");

    assertThat(resultSet.hasNext()).isFalse();
  }

  @Test
  void shouldExecuteCreateQuery() {
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (p:Person {name: 'Dave', age: 40})");
    });

    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) WHERE p.name = 'Dave' RETURN p.age AS age");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Number age = result.getProperty("age");
    assertThat(age.intValue()).isEqualTo(40);
  }

  @Test
  void shouldExecuteUpdateQuery() {
    database.transaction(() -> {
      database.command("opencypher",
          "MATCH (p:Person) WHERE p.name = 'Alice' SET p.age = 31");
    });

    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.age AS age");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Number age = result.getProperty("age");
    assertThat(age.intValue()).isEqualTo(31);
  }

  @Test
  void shouldExecuteDeleteQuery() {
    database.transaction(() -> {
      database.command("opencypher",
          "MATCH (p:Person) WHERE p.name = 'Bob' DETACH DELETE p");
    });

    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) RETURN count(p) AS total");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Number total = result.getProperty("total");
    assertThat(total.longValue()).isEqualTo(2L);
  }

  @Test
  void shouldExecuteMergeQuery() {
    database.transaction(() -> {
      database.command("opencypher",
          "MERGE (p:Person {name: 'Eve'}) ON CREATE SET p.age = 28");
    });

    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) WHERE p.name = 'Eve' RETURN p.age AS age");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Number age = result.getProperty("age");
    assertThat(age.intValue()).isEqualTo(28);
  }

  @Test
  void shouldExecuteRemoveQuery() {
    database.transaction(() -> {
      database.command("opencypher",
          "MATCH (p:Person) WHERE p.name = 'Alice' REMOVE p.city");
    });

    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.city AS city");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    assertThat((Object) result.getProperty("city")).isNull();
  }

  @Test
  void shouldExecuteQueryWithParameters() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) WHERE p.age = $age RETURN p.name AS name",
        Map.of("age", 30));

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    assertThat(result.<String>getProperty("name")).isEqualTo("Alice");
  }

  @Test
  void shouldExecuteQueryWithMultipleParameters() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) WHERE p.age >= $minAge AND p.age <= $maxAge RETURN p.name AS name ORDER BY p.age",
        Map.of("minAge", 25, "maxAge", 30));

    final var results = resultSet.stream().toList();
    assertThat(results).hasSize(2);
  }

  @Test
  void shouldHandleComplexPattern() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) " +
        "RETURN a.name AS first, b.name AS second, c.name AS third");

    final var results = resultSet.stream().toList();
    assertThat(results).hasSizeGreaterThanOrEqualTo(1);
  }

  @Test
  void shouldExecuteQueryWithCase() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) RETURN p.name AS name, " +
        "CASE WHEN p.age < 30 THEN 'Young' ELSE 'Senior' END AS category");

    final var results = resultSet.stream().toList();
    assertThat(results).hasSize(3);
    final boolean hasYoung = results.stream().anyMatch(r -> "Young".equals(r.getProperty("category")));
    final boolean hasSenior = results.stream().anyMatch(r -> "Senior".equals(r.getProperty("category")));
    assertThat(hasYoung).isTrue();
    assertThat(hasSenior).isTrue();
  }

  @Test
  void shouldExecuteQueryWithUnwind() {
    final ResultSet resultSet = database.query("opencypher",
        "UNWIND [1, 2, 3] AS num RETURN num");

    final var results = resultSet.stream().toList();
    assertThat(results).hasSize(3);
  }

  @Test
  void shouldExecuteQueryWithCollect() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) RETURN collect(p.name) AS names");

    final boolean hasNext = resultSet.hasNext();
    assertThat(hasNext).isTrue();
    final Result result = resultSet.next();
    assertThat((Object) result.getProperty("names")).isNotNull();
  }

  @Test
  void shouldExecuteQueryWithCountDistinct() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) RETURN count(DISTINCT p.city) AS cities");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Number cities = result.getProperty("cities");
    assertThat(cities.longValue()).isEqualTo(3L);
  }

  @Test
  void shouldExecuteQueryWithAverage() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) RETURN avg(p.age) AS avgAge");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Number avgAge = result.getProperty("avgAge");
    assertThat(avgAge.doubleValue()).isEqualTo(30.0);
  }

  @Test
  void shouldExecuteQueryWithMinMax() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) RETURN min(p.age) AS minAge, max(p.age) AS maxAge");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Number minAge = result.getProperty("minAge");
    final Number maxAge = result.getProperty("maxAge");
    assertThat(minAge.intValue()).isEqualTo(25);
    assertThat(maxAge.intValue()).isEqualTo(35);
  }

  @Test
  void shouldHandleNullValuesInResults() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) RETURN p.middleName AS middleName");

    final var results = resultSet.stream().toList();
    assertThat(results).hasSize(3);
    final boolean allNull = results.stream().allMatch(r -> r.getProperty("middleName") == null);
    assertThat(allNull).isTrue();
  }

  @Test
  void shouldExecuteQueryWithOrderByDesc() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) RETURN p.name AS name ORDER BY p.age DESC");

    final var results = resultSet.stream().toList();
    assertThat(results).hasSize(3);
    assertThat(results.get(0).<String>getProperty("name")).isEqualTo("Charlie");
  }

  @Test
  void shouldExecuteQueryWithMultipleOrderBy() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) RETURN p.name AS name ORDER BY p.age DESC, p.name ASC");

    final var results = resultSet.stream().toList();
    assertThat(results).hasSize(3);
  }

  @Test
  void shouldExecuteComplexAggregationQuery() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) " +
        "RETURN p.age > 30 AS isOlder, count(p) AS total, avg(p.age) AS avgAge " +
        "ORDER BY isOlder");

    final var results = resultSet.stream().toList();
    assertThat(results).hasSizeGreaterThanOrEqualTo(1);
  }
}
