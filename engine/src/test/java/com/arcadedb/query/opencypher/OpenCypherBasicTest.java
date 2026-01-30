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

;

/**
 * Basic tests for OpenCypher query engine.
 * Phase 1: Tests for module compilation and query parsing.
 */
public class OpenCypherBasicTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher").create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void openCypherEngineRegistered() {
    // Test that the opencypher engine is registered
    database.getSchema().createVertexType("TestVertex");

    // Create a test vertex
    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:TestVertex {name: 'Test'})");
    });

    final ResultSet result = database.query("opencypher", "MATCH (n:TestVertex) RETURN n");

    assertThat((Object) result).isNotNull();
    assertThat(result.hasNext()).as("Result set should have results").isTrue();

    final Result firstResult = result.next();
    assertThat((Object) firstResult).isNotNull();
    // Single-variable RETURN should unwrap the element directly
    assertThat(firstResult.isElement()).as("RETURN n should produce element results, not projections").isTrue();
  }

  @Test
  void basicMatchQuery() {
    // Test basic MATCH query parsing
    database.getSchema().createVertexType("Person");

    final ResultSet result = database.query("opencypher", "MATCH (n:Person) RETURN n");

    assertThat((Object) result).isNotNull();
  }

  @Test
  void matchWithWhereQuery() {
    // Test MATCH with WHERE clause
    database.getSchema().createVertexType("Person");

    final ResultSet result = database.query("opencypher", "MATCH (n:Person) WHERE n.age > 25 RETURN n");

    assertThat((Object) result).isNotNull();
  }
}
