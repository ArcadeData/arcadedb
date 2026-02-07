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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Unit tests for ExpressionEvaluator.
 * Tests evaluation of Cypher expressions in result context.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ExpressionEvaluatorTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/test-expression-evaluator").create();
    database.getSchema().createVertexType("Person");
    database.getSchema().createVertexType("Product");
    database.getSchema().createEdgeType("PURCHASED");

    database.transaction(() -> {
      final var alice = database.newVertex("Person")
          .set("name", "Alice")
          .set("age", 30)
          .set("salary", 50000)
          .save();

      final var bob = database.newVertex("Person")
          .set("name", "Bob")
          .set("age", 25)
          .set("salary", 45000)
          .save();

      final var laptop = database.newVertex("Product")
          .set("name", "Laptop")
          .set("price", 999.99)
          .save();

      alice.newEdge("PURCHASED", laptop, true, new Object[]{"quantity", 1}).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null) {
      database.drop();
    }
  }

  @Test
  void shouldEvaluatePropertyAccess() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.age AS age");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    assertThat(result.<Integer>getProperty("age")).isEqualTo(30);
  }

  @Test
  void shouldEvaluateArithmeticExpression() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.age + 5 AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Number result_value = result.getProperty("result");
    assertThat(result_value.intValue()).isEqualTo(35);
  }

  @Test
  void shouldEvaluateMultiplicationExpression() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.age * 2 AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Number result_value = result.getProperty("result");
    assertThat(result_value.intValue()).isEqualTo(60);
  }

  @Test
  void shouldEvaluateSubtractionExpression() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.salary - 10000 AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Number result_value = result.getProperty("result");
    assertThat(result_value.intValue()).isEqualTo(40000);
  }

  @Test
  void shouldEvaluateDivisionExpression() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.salary / 1000 AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Number result_value = result.getProperty("result");
    assertThat(result_value.intValue()).isEqualTo(50);
  }

  @Test
  void shouldEvaluateComplexArithmeticExpression() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) WHERE p.name = 'Alice' RETURN (p.age + 10) * 2 - 5 AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Number result_value = result.getProperty("result");
    assertThat(result_value.intValue()).isEqualTo(75);
  }

  @Test
  void shouldEvaluateComparisonExpression() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) WHERE p.age > 25 RETURN p.name AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    assertThat(result.<String>getProperty("result")).isEqualTo("Alice");
  }

  @Test
  void shouldEvaluateEqualityExpression() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) WHERE p.age = 30 RETURN p.name AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    assertThat(result.<String>getProperty("result")).isEqualTo("Alice");
  }

  @Test
  void shouldEvaluateNotEqualExpression() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) WHERE p.age <> 25 RETURN p.name AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    assertThat(result.<String>getProperty("result")).isEqualTo("Alice");
  }

  @Test
  void shouldEvaluateLogicalAndExpression() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) WHERE p.age > 25 AND p.salary > 45000 RETURN p.name AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    assertThat(result.<String>getProperty("result")).isEqualTo("Alice");
  }

  @Test
  void shouldEvaluateLogicalOrExpression() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) WHERE p.age < 26 OR p.salary > 48000 RETURN count(*) AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Number count = result.getProperty("result");
    assertThat(count.longValue()).isEqualTo(2L);
  }

  @Test
  void shouldEvaluateNotExpression() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) WHERE NOT p.age = 25 RETURN p.name AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    assertThat(result.<String>getProperty("result")).isEqualTo("Alice");
  }

  @Test
  void shouldEvaluateStringConcatenation() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.name + ' Smith' AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    assertThat(result.<String>getProperty("result")).isEqualTo("Alice Smith");
  }

  @Test
  void shouldEvaluateIsNullExpression() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) WHERE p.middleName IS NULL RETURN count(*) AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Number count = result.getProperty("result");
    assertThat(count.longValue()).isGreaterThan(0L);
  }

  @Test
  void shouldEvaluateIsNotNullExpression() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) WHERE p.name IS NOT NULL RETURN count(*) AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Number count = result.getProperty("result");
    assertThat(count.longValue()).isEqualTo(2L);
  }

  @Test
  void shouldEvaluateInExpression() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) WHERE p.age IN [25, 30, 35] RETURN count(*) AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Number count = result.getProperty("result");
    assertThat(count.longValue()).isEqualTo(2L);
  }

  @Test
  void shouldEvaluateListLiteral() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN [1, 2, 3, 4, 5] AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    assertThat((Object) result.getProperty("result")).isNotNull();
  }

  @Test
  void shouldEvaluateMapLiteral() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN {name: 'Alice', age: 30} AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    assertThat((Object) result.getProperty("result")).isNotNull();
  }

  @Test
  void shouldEvaluateListIndexing() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN [1, 2, 3, 4, 5][2] AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Number result_value = result.getProperty("result");
    assertThat(result_value.intValue()).isEqualTo(3);
  }

  @Test
  void shouldEvaluateListSlicing() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN [1, 2, 3, 4, 5][1..3] AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    assertThat((Object) result.getProperty("result")).isNotNull();
  }

  @Test
  void shouldEvaluateCaseExpression() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) RETURN CASE WHEN p.age > 25 THEN 'Senior' ELSE 'Junior' END AS result");

    final var results = resultSet.stream().toList();
    assertThat(results).hasSize(2);
    final boolean hasSenior = results.stream().anyMatch(r -> "Senior".equals(r.getProperty("result")));
    final boolean hasJunior = results.stream().anyMatch(r -> "Junior".equals(r.getProperty("result")));
    assertThat(hasSenior).isTrue();
    assertThat(hasJunior).isTrue();
  }

  @Test
  void shouldEvaluateFunctionCall() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) WHERE p.name = 'Alice' RETURN abs(p.age - 40) AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Number result_value = result.getProperty("result");
    assertThat(result_value.intValue()).isEqualTo(10);
  }

  @Test
  void shouldEvaluateNestedPropertyAccess() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person)-[r:PURCHASED]->(prod:Product) RETURN r.quantity AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Number result_value = result.getProperty("result");
    assertThat(result_value.intValue()).isEqualTo(1);
  }

  @Test
  void shouldEvaluateExpressionWithMultipleProperties() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.salary / p.age AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Number value = result.getProperty("result");
    assertThat(value.doubleValue()).isCloseTo(1666.67, within(1.0));
  }

  @Test
  void shouldEvaluateExpressionInOrderBy() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) RETURN p.name AS name ORDER BY p.age DESC");

    assertThat(resultSet.hasNext()).isTrue();
    final Result first = resultSet.next();
    assertThat(first.<String>getProperty("name")).isEqualTo("Alice");

    assertThat(resultSet.hasNext()).isTrue();
    final Result second = resultSet.next();
    assertThat(second.<String>getProperty("name")).isEqualTo("Bob");
  }

  @Test
  void shouldEvaluateAggregateExpression() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) RETURN count(p) AS total, sum(p.age) AS totalAge");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Number total = result.getProperty("total");
    assertThat(total.longValue()).isEqualTo(2L);
    final Number totalAge = result.getProperty("totalAge");
    assertThat(totalAge.longValue()).isEqualTo(55L);
  }

  @Test
  void shouldEvaluateDistinctExpression() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) RETURN DISTINCT p.age > 25 AS isOlder");

    final var results = resultSet.stream().toList();
    final int resultSize = results.size();
    assertThat(resultSize).isGreaterThanOrEqualTo(1);
  }

  @Test
  void shouldHandleNullInExpression() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN null + 5 AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    assertThat((Object) result.getProperty("result")).isNull();
  }

  @Test
  void shouldEvaluateComplexNestedExpression() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) WHERE p.name = 'Alice' RETURN ((p.age + 10) * 2 - p.salary / 1000) AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Number result_value = result.getProperty("result");
    assertThat(result_value.intValue()).isEqualTo(30);
  }

  @Test
  void shouldEvaluateExpressionWithCoalesce() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) RETURN coalesce(p.middleName, p.name) AS result");

    final var results = resultSet.stream().toList();
    assertThat(results).hasSize(2);
    final boolean allNonNull = results.stream().allMatch(r -> r.getProperty("result") != null);
    assertThat(allNonNull).isTrue();
  }

  @Test
  void shouldEvaluateExpressionInWithClause() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) WITH p.age * 2 AS doubleAge WHERE doubleAge > 50 RETURN doubleAge");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Number doubleAge = result.getProperty("doubleAge");
    assertThat(doubleAge.intValue()).isGreaterThan(50);
  }

  @Test
  void shouldEvaluateParameterExpression() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) WHERE p.age = $age RETURN p.name AS result",
        java.util.Map.of("age", 30));

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    assertThat(result.<String>getProperty("result")).isEqualTo("Alice");
  }
}
