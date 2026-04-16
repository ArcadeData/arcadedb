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
package com.arcadedb.query.sql.functions;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.assertj.core.api.Fail.fail;

/**
 * Tests that stateless functions from the unified FunctionRegistry
 * are accessible from SQL queries via the bridge in SQLQueryEngine.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
class SQLStatelessFunctionBridgeTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Person");
      database.newDocument("Person").set("name", "John").set("surname", "Smith").save();
      database.newDocument("Person").set("name", "Jon").set("surname", "Snow").save();
      database.newDocument("Person").set("name", "Jane").set("surname", "Doe").save();
      database.newDocument("Person").set("name", "Alice").set("surname", "Wonder").save();
    });
  }

  @Test
  void testJaroWinklerIdenticalStrings() {
    final ResultSet rs = database.query("sql", "SELECT text.jaroWinklerDistance('hello', 'hello') AS score");
    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    assertThat(((Number) result.getProperty("score")).doubleValue()).isCloseTo(1.0, within(0.001));
  }

  @Test
  void testJaroWinklerSimilarStrings() {
    final ResultSet rs = database.query("sql", "SELECT text.jaroWinklerDistance('John', 'Jon') AS score");
    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    final double score = ((Number) result.getProperty("score")).doubleValue();
    assertThat(score).isGreaterThan(0.8);
    assertThat(score).isLessThan(1.0);
  }

  @Test
  void testJaroWinklerWithProperty() {
    final ResultSet rs = database.query("sql",
        "SELECT name, text.jaroWinklerDistance(name, 'John') AS score FROM Person ORDER BY score DESC");
    final List<Result> results = rs.stream().toList();
    assertThat(results).hasSize(4);
    // "John" should have score 1.0
    assertThat(((Number) results.get(0).getProperty("score")).doubleValue()).isCloseTo(1.0, within(0.001));
    assertThat((String) results.get(0).getProperty("name")).isEqualTo("John");
  }

  @Test
  void testJaroWinklerInWhereClause() {
    final ResultSet rs = database.query("sql",
        "SELECT name FROM Person WHERE text.jaroWinklerDistance(name, 'John') > 0.85");
    final List<Result> results = rs.stream().toList();
    // "John" (1.0) and "Jon" (>0.85) should match, but not "Jane" or "Alice"
    assertThat(results.size()).isGreaterThanOrEqualTo(1);
    for (final Result r : results) {
      final String name = r.getProperty("name");
      assertThat(name).isIn("John", "Jon");
    }
  }

  @Test
  void testLevenshteinDistance() {
    final ResultSet rs = database.query("sql", "SELECT text.levenshteinDistance('kitten', 'sitting') AS dist");
    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    assertThat(((Number) result.getProperty("dist")).longValue()).isEqualTo(3L);
  }

  @Test
  void testLevenshteinSimilarity() {
    final ResultSet rs = database.query("sql", "SELECT text.levenshteinSimilarity('hello', 'hello') AS score");
    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    assertThat(((Number) result.getProperty("score")).doubleValue()).isCloseTo(1.0, within(0.001));
  }

  @Test
  void testHammingDistance() {
    final ResultSet rs = database.query("sql", "SELECT text.hammingDistance('karolin', 'kathrin') AS dist");
    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    assertThat(((Number) result.getProperty("dist")).longValue()).isEqualTo(3L);
  }

  @Test
  void testSorensenDiceSimilarity() {
    final ResultSet rs = database.query("sql", "SELECT text.sorensenDiceSimilarity('night', 'nacht') AS score");
    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    final double score = ((Number) result.getProperty("score")).doubleValue();
    assertThat(score).isBetween(0.0, 1.0);
  }

  @Test
  void testMathSigmoid() {
    final ResultSet rs = database.query("sql", "SELECT math.sigmoid(0.0) AS result");
    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    assertThat(((Number) result.getProperty("result")).doubleValue()).isCloseTo(0.5, within(0.001));
  }

  @Test
  void testUnknownNamespacedFunctionThrows() {
    // Use a known namespace with a non-existent function name
    try {
      database.query("sql", "SELECT text.nonExistentFunc('a') AS result");
      fail("Should have thrown");
    } catch (final CommandExecutionException e) {
      assertThat(e.getMessage()).contains("text.nonExistentFunc");
    }
  }

  @Test
  void testNativeSqlFunctionsStillWork() {
    final ResultSet rs = database.query("sql", "SELECT count(*) AS cnt FROM Person");
    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    assertThat(((Number) result.getProperty("cnt")).longValue()).isEqualTo(4L);
  }
}
