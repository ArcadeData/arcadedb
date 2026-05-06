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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #4112.
 * <p>
 * The literal boolean {@code false} predicate in {@code WHERE} must reject all rows.
 * Variants {@code false AND ...}, {@code ... AND false}, {@code true AND false},
 * {@code NOT true}, etc., must behave the same as Neo4j.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4112WhereFalseTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/issue-4112-where-false").create();
    database.transaction(() -> database.command("opencypher", "CREATE (:Person {name:'Alice'})"));
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void whereFalseReturnsZeroRows() {
    final ResultSet rs = database.query("opencypher", "MATCH (p:Person) WHERE false RETURN p.name AS name");
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void whereTrueReturnsAllRows() {
    final ResultSet rs = database.query("opencypher", "MATCH (p:Person) WHERE true RETURN p.name AS name");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("name")).isEqualTo("Alice");
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void whereFalseAndPredicateReturnsZeroRows() {
    final ResultSet rs = database.query("opencypher", "MATCH (p:Person) WHERE false AND p.name = 'Alice' RETURN p.name AS name");
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void wherePredicateAndFalseReturnsZeroRows() {
    final ResultSet rs = database.query("opencypher", "MATCH (p:Person) WHERE p.name = 'Alice' AND false RETURN p.name AS name");
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void whereFalseOrPredicateReturnsMatchingRows() {
    final ResultSet rs = database.query("opencypher", "MATCH (p:Person) WHERE false OR p.name = 'Alice' RETURN p.name AS name");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("name")).isEqualTo("Alice");
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void whereTrueEqualsFalseReturnsZeroRows() {
    final ResultSet rs = database.query("opencypher", "MATCH (p:Person) WHERE true = false RETURN p.name AS name");
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void whereNotTrueReturnsZeroRows() {
    final ResultSet rs = database.query("opencypher", "MATCH (p:Person) WHERE NOT true RETURN p.name AS name");
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void whereNotFalseReturnsAllRows() {
    final ResultSet rs = database.query("opencypher", "MATCH (p:Person) WHERE NOT false RETURN p.name AS name");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("name")).isEqualTo("Alice");
    assertThat(rs.hasNext()).isFalse();
  }
}
