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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for https://github.com/ArcadeData/arcadedb/issues/3320
 * MATCH (n) WHERE RETURN n should error, not silently ignore the empty WHERE clause.
 */
public class Issue3320Test {
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
    // MATCH (n) WHERE RETURN n - the WHERE clause has no predicate, should error
    assertThatThrownBy(() -> database.query("opencypher", "MATCH (n) WHERE RETURN n"))
        .isInstanceOf(CommandParsingException.class);
  }

  @Test
  void emptyWhereClauseWithTypeShouldError() {
    // MATCH (n:Person) WHERE RETURN n - same with a type label
    assertThatThrownBy(() -> database.query("opencypher", "MATCH (n:Person) WHERE RETURN n"))
        .isInstanceOf(CommandParsingException.class);
  }

  @Test
  void validWhereClauseShouldStillWork() {
    // Verify that a valid WHERE clause still works
    final ResultSet result = database.query("opencypher", "MATCH (n:Person) WHERE n.age > 26 RETURN n.name");
    final List<Result> results = new ArrayList<>();
    while (result.hasNext())
      results.add(result.next());

    assertThat(results).hasSize(1);
    assertThat(results.get(0).<String>getProperty("n.name")).isEqualTo("Alice");
  }

  @Test
  void matchWithoutWhereShouldWork() {
    // Verify that MATCH without WHERE still works
    final ResultSet result = database.query("opencypher", "MATCH (n:Person) RETURN n.name ORDER BY n.name");
    final List<Result> results = new ArrayList<>();
    while (result.hasNext())
      results.add(result.next());

    assertThat(results).hasSize(2);
    assertThat(results.get(0).<String>getProperty("n.name")).isEqualTo("Alice");
    assertThat(results.get(1).<String>getProperty("n.name")).isEqualTo("Bob");
  }
}
