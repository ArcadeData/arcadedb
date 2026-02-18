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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for GitHub issue #3331: Cypher Pattern Comprehension are broken.
 * Pattern comprehension like [(a)-->(friend) WHERE friend.name <> 'B' | friend.name]
 * should return a list of mapped values, not a boolean.
 */
class Issue3331Test {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/test-issue3331").create();
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void patternComprehensionFromIssue() {
    // Exact scenario from issue #3331
    database.transaction(() -> {
      database.command("opencypher",
          """
          CREATE (a:Person {name:'A'})-[:KNOWS]->(:Person {name:'B'}), \
          (a)-[:KNOWS]->(:Person {name:'C'})""");
    });

    try (final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:Person {name: 'A'}) \
        RETURN [(a)-->(friend) WHERE friend.name <> 'B' | friend.name] AS result""")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      final Object resultObj = row.getProperty("result");
      assertThat(resultObj).isInstanceOf(List.class);
      @SuppressWarnings("unchecked")
      final List<Object> resultList = (List<Object>) resultObj;
      assertThat(resultList).containsExactly("C");
    }
  }

  @Test
  void patternComprehensionNoFilter() {
    // Pattern comprehension without WHERE clause
    database.transaction(() -> {
      database.command("opencypher",
          """
          CREATE (a:Person {name:'A'})-[:KNOWS]->(:Person {name:'B'}), \
          (a)-[:KNOWS]->(:Person {name:'C'})""");
    });

    try (final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:Person {name: 'A'}) \
        RETURN [(a)-->(friend) | friend.name] AS result""")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      final Object resultObj = row.getProperty("result");
      assertThat(resultObj).isInstanceOf(List.class);
      @SuppressWarnings("unchecked")
      final List<Object> resultList = (List<Object>) resultObj;
      assertThat(resultList).containsExactlyInAnyOrder("B", "C");
    }
  }

  @Test
  void patternComprehensionWithRelType() {
    // Pattern comprehension with specific relationship type
    database.transaction(() -> {
      database.command("opencypher",
          """
          CREATE (a:Person {name:'A'})-[:KNOWS]->(:Person {name:'B'}), \
          (a)-[:LIKES]->(:Person {name:'C'})""");
    });

    try (final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:Person {name: 'A'}) \
        RETURN [(a)-[:KNOWS]->(friend) | friend.name] AS result""")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      final Object resultObj = row.getProperty("result");
      assertThat(resultObj).isInstanceOf(List.class);
      @SuppressWarnings("unchecked")
      final List<Object> resultList = (List<Object>) resultObj;
      assertThat(resultList).containsExactly("B");
    }
  }

  @Test
  void patternComprehensionEmptyResult() {
    // Pattern comprehension that matches nothing
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (:Person {name:'A'})");
    });

    try (final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:Person {name: 'A'}) \
        RETURN [(a)-->(friend) | friend.name] AS result""")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      final Object resultObj = row.getProperty("result");
      assertThat(resultObj).isInstanceOf(List.class);
      @SuppressWarnings("unchecked")
      final List<Object> resultList = (List<Object>) resultObj;
      assertThat(resultList).isEmpty();
    }
  }
}
