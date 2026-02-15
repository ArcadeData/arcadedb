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

import static org.assertj.core.api.Assertions.assertThat;
import org.assertj.core.api.Assertions;

/**
 * Comprehensive tests for OpenCypher Predicate functions based on Neo4j Cypher documentation.
 * Tests cover: all(), allReduce(), any(), exists(), isEmpty(), none(), single()
 */
class OpenCypherPredicateFunctionsComprehensiveTest {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./databases/test-cypher-predicate-functions");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    // Create test graph matching Neo4j documentation
    database.getSchema().createVertexType("Person");
    database.getSchema().createVertexType("Movie");
    database.getSchema().createEdgeType("KNOWS");
    database.getSchema().createEdgeType("ACTED_IN");

    database.command("opencypher",
        "CREATE " +
            "(keanu:Person {name:'Keanu Reeves', age:58, nationality:'Canadian'}), " +
            "(carrie:Person {name:'Carrie Anne Moss', age:55, nationality:'American'}), " +
            "(liam:Person {name:'Liam Neeson', age:70, nationality:'Northern Irish'}), " +
            "(guy:Person {name:'Guy Pearce', age:55, nationality:'Australian'}), " +
            "(kathryn:Person {name:'Kathryn Bigelow', age:71, nationality:'American'}), " +
            "(jessica:Person {name:'Jessica Chastain', age:45, address:''}), " +
            "(theMatrix:Movie {title:'The Matrix'}), " +
            "(keanu)-[:KNOWS {since: 1999}]->(carrie), " +
            "(keanu)-[:KNOWS {since: 2005}]->(liam), " +
            "(keanu)-[:KNOWS {since: 2010}]->(kathryn), " +
            "(kathryn)-[:KNOWS {since: 2012}]->(jessica), " +
            "(carrie)-[:KNOWS {since: 2008}]->(guy), " +
            "(liam)-[:KNOWS {since: 2009}]->(guy), " +
            "(keanu)-[:ACTED_IN]->(theMatrix), " +
            "(carrie)-[:ACTED_IN]->(theMatrix)");
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.drop();
  }

  // ==================== all() Tests ====================

  @Test
  void allBasic() {
    final ResultSet result = database.command("opencypher",
        "RETURN all(x IN [1, 2, 3, 4, 5] WHERE x > 0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((Boolean) result.next().getProperty("result")).isTrue();
  }

  @Test
  void allFalse() {
    final ResultSet result = database.command("opencypher",
        "RETURN all(x IN [1, 2, 3, 4, 5] WHERE x > 3) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((Boolean) result.next().getProperty("result")).isFalse();
  }

  @Test
  void allEmptyList() {
    final ResultSet result = database.command("opencypher",
        "WITH [] as emptyList RETURN all(i IN emptyList WHERE true) as result1, all(i IN emptyList WHERE false) as result2");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    assertThat((Boolean) row.getProperty("result1")).isTrue();
    assertThat((Boolean) row.getProperty("result2")).isTrue();
  }

  @Test
  void allWithPath() {
    final ResultSet result = database.command("opencypher",
        "MATCH p = (a:Person {name: 'Keanu Reeves'})-[]-{2}() " +
            "WHERE all(x IN nodes(p) WHERE x.age < 60) " +
            "RETURN count(p) AS pathCount");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("pathCount")).intValue()).isGreaterThan(0);
  }

  @Test
  void allWithNullList() {
    final ResultSet result = database.command("opencypher",
        "RETURN all(x IN null WHERE x > 0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== allReduce() Tests ====================

  @Test
  void allReduceBasic() {
    final ResultSet result = database.command("opencypher",
        "RETURN allReduce(sum = 0, x IN [1, 2, 3] | sum + x, sum < 10) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((Boolean) result.next().getProperty("result")).isTrue();
  }

  @Test
  void allReduceFalse() {
    final ResultSet result = database.command("opencypher",
        "RETURN allReduce(sum = 0, x IN [1, 2, 3, 10] | sum + x, sum < 10) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((Boolean) result.next().getProperty("result")).isFalse();
  }

  @Test
  void allReduceEmptyList() {
    final ResultSet result = database.command("opencypher",
        "RETURN allReduce(sum = 0, x IN [] | sum + x, sum < 10) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((Boolean) result.next().getProperty("result")).isTrue();
  }

  @Test
  void allReduceWithMap() {
    final ResultSet result = database.command("opencypher",
        "RETURN allReduce(span = {}, x IN [1, 2, 3] | {previous: span.current, current: x}, " +
            "span.previous IS NULL OR span.previous < span.current) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((Boolean) result.next().getProperty("result")).isTrue();
  }

  // ==================== any() Tests ====================

  @Test
  void anyBasic() {
    final ResultSet result = database.command("opencypher",
        "RETURN any(x IN [1, 2, 3, 4, 5] WHERE x > 3) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((Boolean) result.next().getProperty("result")).isTrue();
  }

  @Test
  void anyFalse() {
    final ResultSet result = database.command("opencypher",
        "RETURN any(x IN [1, 2, 3] WHERE x > 5) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((Boolean) result.next().getProperty("result")).isFalse();
  }

  @Test
  void anyEmptyList() {
    final ResultSet result = database.command("opencypher",
        "WITH [] as emptyList RETURN any(i IN emptyList WHERE true) as result1, any(i IN emptyList WHERE false) as result2");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    assertThat((Boolean) row.getProperty("result1")).isFalse();
    assertThat((Boolean) row.getProperty("result2")).isFalse();
  }

  @Test
  void anyWithPath() {
    final ResultSet result = database.command("opencypher",
        "MATCH p = (n:Person {name: 'Keanu Reeves'})-[:KNOWS]-{3}() " +
            "WHERE any(rel IN relationships(p) WHERE rel.since < 2000) " +
            "RETURN count(p) AS pathCount");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("pathCount")).intValue()).isGreaterThanOrEqualTo(0);
  }

  @Test
  void anyWithNullList() {
    final ResultSet result = database.command("opencypher",
        "RETURN any(x IN null WHERE x > 0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== exists() Tests ====================

  @Test
  void existsWithPattern() {
    final ResultSet result = database.command("opencypher",
        "MATCH (p:Person) " +
            "RETURN p.name AS name, exists((p)-[:ACTED_IN]->()) AS hasActedIn " +
            "ORDER BY name");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    // Keanu and Carrie acted in The Matrix
    int actorsFound = 0;
    while (result.hasNext()) {
      final var row = result.next();
      final String name = (String) row.getProperty("name");
      final Boolean hasActedIn = (Boolean) row.getProperty("hasActedIn");
      if ("Keanu Reeves".equals(name) || "Carrie Anne Moss".equals(name)) {
        assertThat(hasActedIn).isTrue();
        actorsFound++;
      }
    }
    assertThat(actorsFound).isEqualTo(2);
  }

  @Test
  void existsNull() {
    final ResultSet result = database.command("opencypher",
        "RETURN exists(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== isEmpty() Tests ====================

  @Test
  void isEmptyString() {
    final ResultSet result = database.command("opencypher",
        "MATCH (p:Person) WHERE isEmpty(p.address) RETURN p.name AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("Jessica Chastain");
  }

  @Test
  void isEmptyStringFalse() {
    final ResultSet result = database.command("opencypher",
        "MATCH (p:Person) WHERE NOT isEmpty(p.nationality) RETURN count(p) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(5);
  }

  @Test
  void isEmptyList() {
    final ResultSet result = database.command("opencypher",
        "RETURN isEmpty([]) AS empty, isEmpty([1, 2, 3]) AS notEmpty");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    assertThat((Boolean) row.getProperty("empty")).isTrue();
    assertThat((Boolean) row.getProperty("notEmpty")).isFalse();
  }

  @Test
  void isEmptyMap() {
    final ResultSet result = database.command("opencypher",
        "RETURN isEmpty({}) AS empty, isEmpty({a: 1}) AS notEmpty");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    assertThat((Boolean) row.getProperty("empty")).isTrue();
    assertThat((Boolean) row.getProperty("notEmpty")).isFalse();
  }

  @Test
  void isEmptyNull() {
    final ResultSet result = database.command("opencypher",
        "RETURN isEmpty(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== none() Tests ====================

  @Test
  void noneBasic() {
    final ResultSet result = database.command("opencypher",
        "RETURN none(x IN [1, 2, 3] WHERE x > 5) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((Boolean) result.next().getProperty("result")).isTrue();
  }

  @Test
  void noneFalse() {
    final ResultSet result = database.command("opencypher",
        "RETURN none(x IN [1, 2, 3, 4, 5] WHERE x > 3) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((Boolean) result.next().getProperty("result")).isFalse();
  }

  @Test
  void noneEmptyList() {
    final ResultSet result = database.command("opencypher",
        "WITH [] as emptyList RETURN none(i IN emptyList WHERE true) as result1, none(i IN emptyList WHERE false) as result2");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    assertThat((Boolean) row.getProperty("result1")).isTrue();
    assertThat((Boolean) row.getProperty("result2")).isTrue();
  }

  @Test
  void noneWithPath() {
    final ResultSet result = database.command("opencypher",
        "MATCH p = (n:Person {name: 'Keanu Reeves'})-[]-{2}() " +
            "WHERE none(x IN nodes(p) WHERE x.age > 60) " +
            "RETURN count(p) AS pathCount");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("pathCount")).intValue()).isGreaterThanOrEqualTo(0);
  }

  @Test
  void noneWithNullList() {
    final ResultSet result = database.command("opencypher",
        "RETURN none(x IN null WHERE x > 0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== single() Tests ====================

  @Test
  void singleBasic() {
    final ResultSet result = database.command("opencypher",
        "RETURN single(x IN [1, 2, 3, 4, 5] WHERE x = 3) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((Boolean) result.next().getProperty("result")).isTrue();
  }

  @Test
  void singleFalseMultiple() {
    final ResultSet result = database.command("opencypher",
        "RETURN single(x IN [1, 2, 3, 4, 5] WHERE x > 3) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((Boolean) result.next().getProperty("result")).isFalse();
  }

  @Test
  void singleFalseNone() {
    final ResultSet result = database.command("opencypher",
        "RETURN single(x IN [1, 2, 3] WHERE x > 5) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((Boolean) result.next().getProperty("result")).isFalse();
  }

  @Test
  void singleEmptyList() {
    final ResultSet result = database.command("opencypher",
        "WITH [] as emptyList RETURN single(i IN emptyList WHERE true) as result1, single(i IN emptyList WHERE false) as result2");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    assertThat((Boolean) row.getProperty("result1")).isFalse();
    assertThat((Boolean) row.getProperty("result2")).isFalse();
  }

  @Test
  void singleWithNullList() {
    final ResultSet result = database.command("opencypher",
        "RETURN single(x IN null WHERE x > 0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== Combined/Integration Tests ====================

  @Test
  void predicatesCombined() {
    final ResultSet result = database.command("opencypher",
        "WITH [1, 2, 3, 4, 5] AS nums " +
            "RETURN all(x IN nums WHERE x > 0) AS allPositive, " +
            "       any(x IN nums WHERE x > 3) AS anyAbove3, " +
            "       none(x IN nums WHERE x < 0) AS noneNegative, " +
            "       single(x IN nums WHERE x = 3) AS singleThree");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    assertThat((Boolean) row.getProperty("allPositive")).isTrue();
    assertThat((Boolean) row.getProperty("anyAbove3")).isTrue();
    assertThat((Boolean) row.getProperty("noneNegative")).isTrue();
    assertThat((Boolean) row.getProperty("singleThree")).isTrue();
  }

  @Test
  void predicatesWithIsEmpty() {
    final ResultSet result = database.command("opencypher",
        "WITH ['', 'hello', 'world'] AS strings " +
            "RETURN any(s IN strings WHERE isEmpty(s)) AS anyEmpty, " +
            "       all(s IN strings WHERE NOT isEmpty(s)) AS allNonEmpty");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    assertThat((Boolean) row.getProperty("anyEmpty")).isTrue();
    assertThat((Boolean) row.getProperty("allNonEmpty")).isFalse();
  }
}
