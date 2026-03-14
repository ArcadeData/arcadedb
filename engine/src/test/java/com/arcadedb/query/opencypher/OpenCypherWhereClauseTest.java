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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

;

/**
 * Tests for WHERE clause with logical operators, NULL checks, and regex matching.
 * Tests AND, OR, NOT operators, IS NULL/IS NOT NULL, and regex (=~) operator.
 */
public class OpenCypherWhereClauseTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypherWhere").create();
    database.getSchema().createVertexType("Person");

    // Create test data
    database.transaction(() -> {
      database.command("opencypher", "CREATE (p:Person {name: 'Alice', age: 30, email: 'alice@example.com'})");
      database.command("opencypher", "CREATE (p:Person {name: 'Bob', age: 25, email: 'bob@test.com'})");
      database.command("opencypher", "CREATE (p:Person {name: 'Charlie', age: 35})"); // No email
      database.command("opencypher", "CREATE (p:Person {name: 'David', age: 40, email: 'david@example.com'})");
      database.command("opencypher", "CREATE (p:Person {name: 'Eve', age: 28})"); // No email
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void whereWithAND() {
    // Test AND operator: age > 25 AND age < 35
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.age > 25 AND p.age < 35 RETURN p.name ORDER BY p.name");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Alice");
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Eve");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void whereWithOR() {
    // Test OR operator: age < 26 OR age > 35
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.age < 26 OR p.age > 35 RETURN p.name ORDER BY p.name");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Bob");
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("David");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void whereWithNOT() {
    // Test NOT operator: NOT(age > 30)
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE NOT p.age > 30 RETURN p.name ORDER BY p.name");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Alice");
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Bob");
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Eve");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void whereWithComplexLogic() {
    // Test combined AND with NOT: age > 35 AND NOT name = 'David'
    // Note: Parenthesized OR expressions need additional parser work
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.age > 35 AND NOT p.name = 'David' RETURN p.name ORDER BY p.name");

    assertThat(result.hasNext()).isFalse(); // No one matches: only David has age > 35
  }

  @Test
  void whereISNULL() {
    // Test IS NULL: email IS NULL
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.email IS NULL RETURN p.name ORDER BY p.name");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Charlie");
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Eve");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void whereISNOTNULL() {
    // Test IS NOT NULL: email IS NOT NULL
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.email IS NOT NULL RETURN p.name ORDER BY p.name");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Alice");
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Bob");
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("David");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void whereWithRegex() {
    // Test regex matching: name =~ 'Alice'
    // Start with exact match first
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.name =~ 'Alice' RETURN p.name ORDER BY p.name");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Alice");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void whereWithRegexPattern() {
    // Test regex with pattern: name =~ 'A.*'
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.name =~ 'A.*' RETURN p.name ORDER BY p.name");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Alice");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void whereWithRegexAndLogic() {
    // Test regex with AND: name =~ 'A.*' AND age = 30
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.name =~ 'A.*' AND p.age = 30 RETURN p.name ORDER BY p.name");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Alice");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void whereWithIN() {
    // Test IN operator: name IN ['Alice', 'Bob']
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.name IN ['Alice', 'Bob'] RETURN p.name ORDER BY p.name");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Alice");
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Bob");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void whereWithINNumbers() {
    // Test IN operator with numbers: age IN [25, 30, 35]
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.age IN [25, 30, 35] RETURN p.name ORDER BY p.name");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Alice");
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Bob");
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Charlie");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void whereWithINAndLogic() {
    // Test IN with AND: name IN ['Alice', 'Bob', 'Charlie'] AND age > 28
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.name IN ['Alice', 'Bob', 'Charlie'] AND p.age > 28 RETURN p.name ORDER BY p.name");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Alice");
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Charlie");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void whereWithNullCheckAndComparison() {
    // Test IS NOT NULL with comparison: email IS NOT NULL AND age >= 30
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.email IS NOT NULL AND p.age >= 30 RETURN p.name ORDER BY p.name");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Alice");
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("David");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void whereAllComparisonOperators() {
    // Test all comparison operators
    ResultSet result;

    // Equals
    result = database.query("opencypher", "MATCH (p:Person) WHERE p.age = 30 RETURN p.name");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Alice");
    assertThat(result.hasNext()).isFalse();

    // Not equals
    result = database.query("opencypher", "MATCH (p:Person) WHERE p.age != 30 RETURN count(p) as cnt");
    assertThat(result.next().<Long>getProperty("cnt")).isEqualTo(4L);

    // Less than
    result = database.query("opencypher", "MATCH (p:Person) WHERE p.age < 30 RETURN count(p) as cnt");
    assertThat(result.next().<Long>getProperty("cnt")).isEqualTo(2L);

    // Greater than
    result = database.query("opencypher", "MATCH (p:Person) WHERE p.age > 30 RETURN count(p) as cnt");
    assertThat(result.next().<Long>getProperty("cnt")).isEqualTo(2L);

    // Less than or equal
    result = database.query("opencypher", "MATCH (p:Person) WHERE p.age <= 30 RETURN count(p) as cnt");
    assertThat(result.next().<Long>getProperty("cnt")).isEqualTo(3L);

    // Greater than or equal
    result = database.query("opencypher", "MATCH (p:Person) WHERE p.age >= 30 RETURN count(p) as cnt");
    assertThat(result.next().<Long>getProperty("cnt")).isEqualTo(3L);
  }

  @Test
  void whereComplexConditions() {
    // Test multiple AND conditions: age > 25 AND age < 35 AND email IS NOT NULL
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.age > 25 AND p.age < 35 AND p.email IS NOT NULL RETURN p.name ORDER BY p.name");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Alice");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void whereStartsWith() {
    // Test STARTS WITH operator
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.name STARTS WITH 'A' RETURN p.name ORDER BY p.name");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Alice");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void whereEndsWith() {
    // Test ENDS WITH operator
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.name ENDS WITH 'e' RETURN p.name ORDER BY p.name");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Alice");
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Charlie");
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Eve");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void whereContains() {
    // Test CONTAINS operator
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.name CONTAINS 'li' RETURN p.name ORDER BY p.name");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Alice");
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Charlie");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void whereStartsWithEmailDomain() {
    // Test STARTS WITH on email property
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.email STARTS WITH 'alice' RETURN p.name");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Alice");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void whereEndsWithEmailDomain() {
    // Test ENDS WITH on email domain
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.email ENDS WITH '@example.com' RETURN p.name ORDER BY p.name");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Alice");
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("David");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void whereStringMatchWithAND() {
    // Test string matching combined with AND
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.name STARTS WITH 'A' AND p.age > 28 RETURN p.name");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Alice");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void whereParenthesizedOR() {
    // Test parenthesized OR with AND (operator precedence)
    // (age < 26 OR age > 35) AND email IS NOT NULL
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE (p.age < 26 OR p.age > 35) AND p.email IS NOT NULL RETURN p.name ORDER BY p.name");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Bob");
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("David");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void whereComplexParenthesizedExpressions() {
    // Test nested parentheses with multiple OR and AND
    // ((age < 28 OR age > 35) AND email IS NOT NULL) OR (name CONTAINS 'li' AND age = 35)
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE ((p.age < 28 OR p.age > 35) AND p.email IS NOT NULL) OR (p.name CONTAINS 'li' AND p.age = 35) RETURN p.name ORDER BY p.name");

    // Should match:
    // - Bob (age 25 < 28, has email)
    // - David (age 40 > 35, has email)
    // - Charlie (name contains 'li', age 35, but no email - still matches)
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Bob");
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Charlie");
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("David");
    assertThat(result.hasNext()).isFalse();
  }

  /** See issue #3128 */
  @Nested
  class IdFunctionInWhereRegression {
    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory("./target/databases/issue-3128-basic").create();
      database.getSchema().createVertexType("Node");
      database.getSchema().createEdgeType("in");
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    @Test
    void idFunctionInWhereClause() {
      database.transaction(() -> {
        database.command("opencypher", "CREATE (a:Node {name: 'Node1'})");
        database.command("opencypher", "CREATE (b:Node {name: 'Node2'})");
      });

      ResultSet result = database.query("opencypher", "MATCH (n:Node) WHERE n.name = 'Node1' RETURN ID(n) AS id");
      String node1Id = (String) result.next().getProperty("id");

      result = database.query("opencypher", "MATCH (n:Node) WHERE n.name = 'Node2' RETURN ID(n) AS id");
      String node2Id = (String) result.next().getProperty("id");

      result = database.query("opencypher",
          "MATCH (a) WHERE ID(a) = $sourceId RETURN a",
          Map.of("sourceId", node1Id));

      int count = 0;
      while (result.hasNext()) {
        Result r = result.next();
        Vertex v = (Vertex) r.toElement();
        count++;
      }

      assertThat(count).isEqualTo(1);

      result = database.query("opencypher",
          "MATCH (b) WHERE ID(b) = $targetId RETURN b",
          Map.of("targetId", node2Id));

      count = 0;
      while (result.hasNext()) {
        Result r = result.next();
        Vertex v = (Vertex) r.toElement();
        count++;
      }

      assertThat(count).isEqualTo(1);
    }

    @Test
    void idFunctionWithMultipleMatches() {
      database.transaction(() -> {
        database.command("opencypher", "CREATE (a:Node {name: 'Source'})");
        database.command("opencypher", "CREATE (b:Node {name: 'Target'})");
      });

      ResultSet result = database.query("opencypher", "MATCH (n:Node) WHERE n.name = 'Source' RETURN ID(n) AS id");
      String sourceId = (String) result.next().getProperty("id");

      result = database.query("opencypher", "MATCH (n:Node) WHERE n.name = 'Target' RETURN ID(n) AS id");
      String targetId = (String) result.next().getProperty("id");

      result = database.query("opencypher",
          """
          MATCH (a) WHERE ID(a) = $sourceId \
          MATCH (b) WHERE ID(b) = $targetId \
          RETURN a, b""",
          Map.of("sourceId", sourceId, "targetId", targetId));

      assertThat(result.hasNext()).isTrue();
      Result r = result.next();
      Vertex a = (Vertex) r.getProperty("a");
      Vertex b = (Vertex) r.getProperty("b");

      assertThat(a.get("name")).isEqualTo("Source");
      assertThat(b.get("name")).isEqualTo("Target");
    }
  }

  /** See issue #3132 */
  @Nested
  class IdFunctionWithInOperatorRegression {
    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory("./target/databases/issue-3132").create();
      database.getSchema().createVertexType("CHUNK");

      database.transaction(() -> {
        for (int i = 0; i < 5; i++) {
          database.command("opencypher", "CREATE (n:CHUNK {text: 'Chunk " + i + "'})");
        }
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    @Test
    void idFunctionInReturnClause() {
      ResultSet result = database.query("opencypher", "MATCH (n:CHUNK) RETURN ID(n) AS id, n.text AS text");

      int count = 0;
      while (result.hasNext()) {
        Result r = result.next();
        String id = (String) r.getProperty("id");
        String text = (String) r.getProperty("text");
        assertThat(id).isNotNull();
        assertThat(text).isNotNull();
        count++;
      }

      assertThat(count).isEqualTo(5);
    }

    @Test
    void idFunctionInWhereClauseWithInOperator() {
      ResultSet result = database.query("opencypher", "MATCH (n:CHUNK) RETURN ID(n) AS id");
      List<String> allIds = new ArrayList<>();
      while (result.hasNext()) {
        String id = (String) result.next().getProperty("id");
        allIds.add(id);
      }

      assertThat(allIds).hasSize(5);

      List<String> queryIds = List.of(allIds.get(0), allIds.get(2), allIds.get(4));

      result = database.query("opencypher",
          "MATCH (n:CHUNK) WHERE ID(n) IN $ids RETURN n.text as text, ID(n) as id",
          Map.of("ids", queryIds));

      List<String> returnedIds = new ArrayList<>();
      List<String> texts = new ArrayList<>();
      while (result.hasNext()) {
        Result r = result.next();
        String id = (String) r.getProperty("id");
        String text = (String) r.getProperty("text");
        returnedIds.add(id);
        texts.add(text);
      }

      assertThat(returnedIds).hasSize(3);
      assertThat(returnedIds).containsExactlyInAnyOrderElementsOf(queryIds);
      assertThat(texts).containsExactlyInAnyOrder("Chunk 0", "Chunk 2", "Chunk 4");
    }

    @Test
    void idFunctionWithAllIds() {
      ResultSet result = database.query("opencypher", "MATCH (n:CHUNK) RETURN ID(n) AS id");
      List<String> allIds = new ArrayList<>();
      while (result.hasNext()) {
        String id = (String) result.next().getProperty("id");
        allIds.add(id);
      }

      List<String> queryIds = List.of(allIds.get(2), allIds.get(1), allIds.get(0), allIds.get(4), allIds.get(3));

      result = database.query("opencypher",
          "MATCH (n:CHUNK) WHERE ID(n) IN $ids RETURN n.text as text, ID(n) as id",
          Map.of("ids", queryIds));

      List<String> returnedIds = new ArrayList<>();
      while (result.hasNext()) {
        Result r = result.next();
        String id = (String) r.getProperty("id");
        returnedIds.add(id);
      }

      assertThat(returnedIds).hasSize(5);
      assertThat(returnedIds).containsExactlyInAnyOrderElementsOf(allIds);
    }

    @Test
    void idFunctionWithTwoIds() {
      ResultSet result = database.query("opencypher", "MATCH (n:CHUNK) RETURN ID(n) AS id ORDER BY id");
      List<String> allIds = new ArrayList<>();
      while (result.hasNext()) {
        String id = (String) result.next().getProperty("id");
        allIds.add(id);
      }

      List<String> queryIds = List.of(allIds.get(1), allIds.get(3));

      result = database.query("opencypher",
          "MATCH (n:CHUNK) WHERE ID(n) IN $ids RETURN n.text as text, ID(n) as id",
          Map.of("ids", queryIds));

      List<String> returnedIds = new ArrayList<>();
      while (result.hasNext()) {
        Result r = result.next();
        String id = (String) r.getProperty("id");
        returnedIds.add(id);
      }

      assertThat(returnedIds).hasSize(2);
      assertThat(returnedIds).containsExactlyInAnyOrderElementsOf(queryIds);
    }
  }

  /** See issue #3320 */
  @Nested
  class EmptyWhereClauseRegression {
    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory("./target/databases/testissue3320").create();
      database.getSchema().createVertexType("Person");
      database.transaction(() -> {
        database.command("opencypher", "CREATE (p:Person {name: 'Alice', age: 30})");
        database.command("opencypher", "CREATE (p:Person {name: 'Bob', age: 25})");
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    @Test
    void emptyWhereClauseShouldError() {
      assertThatThrownBy(() -> database.query("opencypher", "MATCH (n) WHERE RETURN n"))
          .isInstanceOf(CommandParsingException.class);
    }

    @Test
    void emptyWhereClauseWithTypeShouldError() {
      assertThatThrownBy(() -> database.query("opencypher", "MATCH (n:Person) WHERE RETURN n"))
          .isInstanceOf(CommandParsingException.class);
    }

    @Test
    void validWhereClauseShouldStillWork() {
      final ResultSet result = database.query("opencypher", "MATCH (n:Person) WHERE n.age > 26 RETURN n.name");
      final List<Result> results = new ArrayList<>();
      while (result.hasNext())
        results.add(result.next());

      assertThat(results).hasSize(1);
      assertThat(results.get(0).<String>getProperty("n.name")).isEqualTo("Alice");
    }

    @Test
    void matchWithoutWhereShouldWork() {
      final ResultSet result = database.query("opencypher", "MATCH (n:Person) RETURN n.name ORDER BY n.name");
      final List<Result> results = new ArrayList<>();
      while (result.hasNext())
        results.add(result.next());

      assertThat(results).hasSize(2);
      assertThat(results.get(0).<String>getProperty("n.name")).isEqualTo("Alice");
      assertThat(results.get(1).<String>getProperty("n.name")).isEqualTo("Bob");
    }
  }

  /** See issue #3336 */
  @Nested
  class TernaryNullLogicRegression {
    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory("./target/databases/test-issue3336").create();
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    @Test
    void ternaryAndWithNull() {
      try (final ResultSet rs = database.query("opencypher",
          "RETURN (true AND null) AS r1, (false AND null) AS r2")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat(row.<Object>getProperty("r1")).isNull();
        assertThat(row.<Boolean>getProperty("r2")).isEqualTo(false);
      }
    }

    @Test
    void ternaryOrWithNull() {
      try (final ResultSet rs = database.query("opencypher",
          "RETURN (true OR null) AS r3, (false OR null) AS r4")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat(row.<Boolean>getProperty("r3")).isEqualTo(true);
        assertThat(row.<Object>getProperty("r4")).isNull();
      }
    }

    @Test
    void ternaryNotNull() {
      try (final ResultSet rs = database.query("opencypher",
          "RETURN (NOT null) AS r5")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat(row.<Object>getProperty("r5")).isNull();
      }
    }

    @Test
    void ternaryNullAndNull() {
      try (final ResultSet rs = database.query("opencypher",
          "RETURN (null AND null) AS r6, (null OR null) AS r7")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat(row.<Object>getProperty("r6")).isNull();
        assertThat(row.<Object>getProperty("r7")).isNull();
      }
    }

    @Test
    void originalIssueQuery() {
      try (final ResultSet rs = database.query("opencypher",
          "RETURN (true AND null) AS r1, (false AND null) AS r2, (true OR null) AS r3, (NOT null) AS r4")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat(row.<Object>getProperty("r1")).isNull();
        assertThat(row.<Boolean>getProperty("r2")).isEqualTo(false);
        assertThat(row.<Boolean>getProperty("r3")).isEqualTo(true);
        assertThat(row.<Object>getProperty("r4")).isNull();
      }
    }

    @Test
    void nonNullLogicStillWorks() {
      try (final ResultSet rs = database.query("opencypher",
          "RETURN (true AND true) AS r1, (true AND false) AS r2, (true OR false) AS r3, (NOT true) AS r4, (NOT false) AS r5")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat(row.<Boolean>getProperty("r1")).isTrue();
        assertThat(row.<Boolean>getProperty("r2")).isFalse();
        assertThat(row.<Boolean>getProperty("r3")).isTrue();
        assertThat(row.<Boolean>getProperty("r4")).isFalse();
        assertThat(row.<Boolean>getProperty("r5")).isTrue();
      }
    }
  }

  /** See issue #3403 */
  @Nested
  class NotOperatorParenthesesRegression {
    private Database database;
    private static final String DB_PATH = "./target/databases/test-issue3403";

    @BeforeEach
    void setUp() {
      new File(DB_PATH).mkdirs();
      database = new DatabaseFactory(DB_PATH).create();

      database.getSchema().createVertexType("DOCUMENT");
      database.getSchema().createVertexType("CHUNK");
      database.getSchema().createVertexType("IMAGE");
      database.getSchema().createEdgeType("in");

      database.transaction(() -> {
        database.command("opencypher", "CREATE (d:DOCUMENT {id: 1})");
        database.command("opencypher", "CREATE (d:DOCUMENT {id: 2})");
        database.command("opencypher", "CREATE (d:DOCUMENT {id: 3})");
        database.command("opencypher", "CREATE (d:DOCUMENT {id: 4})");
        database.command("opencypher", "CREATE (d:DOCUMENT {id: 5})");

        for (int i = 1; i <= 25; i++) {
          database.command("opencypher", "CREATE (c:CHUNK {id: " + i + "})");
        }

        database.command("opencypher", "CREATE (i:IMAGE {id: 1})");
        database.command("opencypher", "CREATE (i:IMAGE {id: 2})");
        database.command("opencypher", "CREATE (i:IMAGE {id: 3})");

        for (int docId = 1; docId <= 5; docId++) {
          for (int chunkOffset = 0; chunkOffset < 5; chunkOffset++) {
            int chunkId = (docId - 1) * 5 + chunkOffset + 1;
            database.command("opencypher",
                "MATCH (d:DOCUMENT {id: " + docId + "}), (c:CHUNK {id: " + chunkId + "}) " +
                "CREATE (c)-[:in]->(d)");
          }
        }

        for (int chunkId = 1; chunkId <= 17; chunkId++) {
          int imageId = ((chunkId - 1) % 3) + 1;
          database.command("opencypher",
              "MATCH (c:CHUNK {id: " + chunkId + "}), (i:IMAGE {id: " + imageId + "}) " +
              "CREATE (c)-[:connected_to]->(i)");
        }
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    @Test
    void notOperatorWithoutParentheses() {
      final String query1 = """
                            MATCH (nodeDOc:DOCUMENT)<-[rel:in]-(chunk:CHUNK) \
                            WHERE NOT (chunk:CHUNK)--(:IMAGE) \
                            RETURN nodeDOc, chunk""";

      long count1;
      try (final ResultSet rs = database.query("opencypher", query1)) {
        count1 = rs.stream().count();
      }

      assertThat(count1).isGreaterThan(0);
    }

    @Test
    void notOperatorWithParentheses() {
      final String query2 = """
                            MATCH (nodeDOc:DOCUMENT)<-[rel:in]-(chunk:CHUNK) \
                            WHERE (NOT (chunk:CHUNK)--(:IMAGE)) \
                            RETURN nodeDOc, chunk""";

      long count2;
      try (final ResultSet rs = database.query("opencypher", query2)) {
        count2 = rs.stream().count();
      }

      assertThat(count2).isGreaterThan(0);
    }

    @Test
    void bothQueriesReturnSameResults() {
      final String query1 = """
                            MATCH (nodeDOc:DOCUMENT)<-[rel:in]-(chunk:CHUNK) \
                            WHERE NOT (chunk:CHUNK)--(:IMAGE) \
                            RETURN nodeDOc, chunk""";

      final String query2 = """
                            MATCH (nodeDOc:DOCUMENT)<-[rel:in]-(chunk:CHUNK) \
                            WHERE (NOT (chunk:CHUNK)--(:IMAGE)) \
                            RETURN nodeDOc, chunk""";

      long count1;
      try (final ResultSet rs = database.query("opencypher", query1)) {
        count1 = rs.stream().count();
      }

      long count2;
      try (final ResultSet rs = database.query("opencypher", query2)) {
        count2 = rs.stream().count();
      }

      assertThat(count1).isEqualTo(count2)
          .withFailMessage("Expected both queries to return the same number of results, but got " +
                          count1 + " vs " + count2);
    }

    @Test
    void simpleNotPattern() {
      final String query1 = "MATCH (c:CHUNK) WHERE NOT (c)--(:IMAGE) RETURN c";
      final String query2 = "MATCH (c:CHUNK) WHERE (NOT (c)--(:IMAGE)) RETURN c";

      long count1;
      try (final ResultSet rs = database.query("opencypher", query1)) {
        count1 = rs.stream().count();
      }

      long count2;
      try (final ResultSet rs = database.query("opencypher", query2)) {
        count2 = rs.stream().count();
      }

      assertThat(count1).isEqualTo(count2)
          .withFailMessage("Simple NOT pattern: Expected both queries to return the same number of results");
    }

    @Test
    void tripleParentheses() {
      final String query1 = "MATCH (c:CHUNK) WHERE NOT (c)--(:IMAGE) RETURN c";
      final String query2 = "MATCH (c:CHUNK) WHERE (((NOT (c)--(:IMAGE)))) RETURN c";

      long count1;
      try (final ResultSet rs = database.query("opencypher", query1)) {
        count1 = rs.stream().count();
      }

      long count2;
      try (final ResultSet rs = database.query("opencypher", query2)) {
        count2 = rs.stream().count();
      }

      assertThat(count1).isEqualTo(count2)
          .withFailMessage("Triple parentheses: Expected both queries to return the same number of results");
    }

    @Test
    void multipleParenthesesAroundPattern() {
      final String query1 = "MATCH (c:CHUNK) WHERE NOT (c)--(:IMAGE) RETURN c";
      final String query2 = "MATCH (c:CHUNK) WHERE NOT (((c)--(:IMAGE))) RETURN c";

      long count1;
      try (final ResultSet rs = database.query("opencypher", query1)) {
        count1 = rs.stream().count();
      }

      long count2;
      try (final ResultSet rs = database.query("opencypher", query2)) {
        count2 = rs.stream().count();
      }

      assertThat(count1).isEqualTo(count2)
          .withFailMessage("Multiple parentheses around pattern: Expected both queries to return the same number of results");
    }

    @Test
    void complexOriginalQueryWithTripleParentheses() {
      final String query1 = """
                            MATCH (nodeDOc:DOCUMENT)<-[rel:in]-(chunk:CHUNK) \
                            WHERE NOT (chunk:CHUNK)--(:IMAGE) \
                            RETURN nodeDOc, chunk""";

      final String query2 = """
                            MATCH (nodeDOc:DOCUMENT)<-[rel:in]-(chunk:CHUNK) \
                            WHERE (((NOT (chunk:CHUNK)--(:IMAGE)))) \
                            RETURN nodeDOc, chunk""";

      long count1;
      try (final ResultSet rs = database.query("opencypher", query1)) {
        count1 = rs.stream().count();
      }

      long count2;
      try (final ResultSet rs = database.query("opencypher", query2)) {
        count2 = rs.stream().count();
      }

      assertThat(count1).isEqualTo(count2)
          .withFailMessage("Complex query with triple parentheses: Expected both queries to return the same number of results");
    }
  }
}
