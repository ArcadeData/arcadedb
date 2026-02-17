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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for GitHub issue #3333: Cypher String Matching (STARTS WITH / ENDS WITH / CONTAINS) broken in RETURN expressions.
 * These operators work in WHERE clauses but return null when used in RETURN projections.
 */
class Issue3333Test {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/test-issue3333").create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void stringMatchingInReturn() {
    // Exact scenario from issue #3333
    try (final ResultSet rs = database.query("opencypher",
        "WITH 'Hello World' AS txt " +
            "RETURN txt STARTS WITH 'He' AS a, " +
            "txt CONTAINS 'lo' AS b, " +
            "txt ENDS WITH 'rld' AS c")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(row.<Boolean>getProperty("a")).isTrue();
      assertThat(row.<Boolean>getProperty("b")).isTrue();
      assertThat(row.<Boolean>getProperty("c")).isTrue();
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void stringMatchingInReturnFalse() {
    // Test that false results are returned correctly too
    try (final ResultSet rs = database.query("opencypher",
        "WITH 'Hello World' AS txt " +
            "RETURN txt STARTS WITH 'Xyz' AS a, " +
            "txt CONTAINS 'xyz' AS b, " +
            "txt ENDS WITH 'xyz' AS c")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(row.<Boolean>getProperty("a")).isFalse();
      assertThat(row.<Boolean>getProperty("b")).isFalse();
      assertThat(row.<Boolean>getProperty("c")).isFalse();
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void stringMatchingWithPropertyInReturn() {
    // Test with node property access
    database.getSchema().createVertexType("Person");
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Person {name: 'Alice Johnson'})");
    });

    try (final ResultSet rs = database.query("opencypher",
        "MATCH (p:Person) " +
            "RETURN p.name STARTS WITH 'Ali' AS startsWithAli, " +
            "p.name CONTAINS 'John' AS containsJohn, " +
            "p.name ENDS WITH 'son' AS endsWithSon")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(row.<Boolean>getProperty("startsWithAli")).isTrue();
      assertThat(row.<Boolean>getProperty("containsJohn")).isTrue();
      assertThat(row.<Boolean>getProperty("endsWithSon")).isTrue();
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void regexInReturn() {
    // Test that regex (=~) also works in RETURN
    try (final ResultSet rs = database.query("opencypher",
        "WITH 'Hello World' AS txt " +
            "RETURN txt =~ 'Hello.*' AS matchesRegex")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(row.<Boolean>getProperty("matchesRegex")).isTrue();
      assertThat(rs.hasNext()).isFalse();
    }
  }
}
