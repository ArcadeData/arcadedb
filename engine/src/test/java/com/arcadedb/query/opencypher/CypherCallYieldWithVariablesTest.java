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

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for GitHub issue #3996:
 * CALL ... YIELD may null out variables carried in from WITH.
 */
class CypherCallYieldWithVariablesTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-call-yield-with").create();
    final var personType = database.getSchema().createVertexType("Person");
    personType.createProperty("name", String.class);
    database.getSchema().createEdgeType("KNOWS");
    database.transaction(() -> database.command("opencypher", "CREATE (:Person {name: 'Alice'})"));
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void withLiteralPreservedAcrossCallDbLabels() {
    final ResultSet rs = database.query("opencypher",
        "WITH 1 AS x CALL db.labels() YIELD label RETURN x, label");

    final List<Result> rows = new ArrayList<>();
    while (rs.hasNext())
      rows.add(rs.next());

    assertThat(rows).isNotEmpty();
    for (final Result row : rows) {
      final Number x = row.getProperty("x");
      assertThat(x).as("x must not be null across CALL db.labels()").isNotNull();
      assertThat(x.longValue()).isEqualTo(1L);
      assertThat((Object) row.getProperty("label")).as("label must not be null").isNotNull();
    }
  }

  @Test
  void withLiteralPreservedAcrossCallDbRelationshipTypes() {
    final ResultSet rs = database.query("opencypher",
        "WITH 1 AS x CALL db.relationshipTypes() YIELD relationshipType RETURN x, relationshipType");

    final List<Result> rows = new ArrayList<>();
    while (rs.hasNext())
      rows.add(rs.next());

    assertThat(rows).isNotEmpty();
    for (final Result row : rows) {
      final Number x = row.getProperty("x");
      assertThat(x).as("x must not be null across CALL db.relationshipTypes()").isNotNull();
      assertThat(x.longValue()).isEqualTo(1L);
      assertThat((Object) row.getProperty("relationshipType")).as("relationshipType must not be null").isNotNull();
    }
  }

  @Test
  void withLiteralPreservedAcrossCallDbPropertyKeys() {
    final ResultSet rs = database.query("opencypher",
        "WITH 1 AS x CALL db.propertyKeys() YIELD propertyKey RETURN x, propertyKey");

    final List<Result> rows = new ArrayList<>();
    while (rs.hasNext())
      rows.add(rs.next());

    assertThat(rows).isNotEmpty();
    for (final Result row : rows) {
      final Number x = row.getProperty("x");
      assertThat(x).as("x must not be null across CALL db.propertyKeys()").isNotNull();
      assertThat(x.longValue()).isEqualTo(1L);
      assertThat((Object) row.getProperty("propertyKey")).as("propertyKey must not be null").isNotNull();
    }
  }

  @Test
  void aggregatedValuePreservedAcrossCallDbLabels() {
    // Aggregated values carried through WITH must also survive CALL ... YIELD
    final ResultSet rs = database.query("opencypher",
        "MATCH (:Person) WITH count(*) AS c CALL db.labels() YIELD label RETURN c, label");

    final List<Result> rows = new ArrayList<>();
    while (rs.hasNext())
      rows.add(rs.next());

    assertThat(rows).isNotEmpty();
    for (final Result row : rows) {
      final Number c = row.getProperty("c");
      assertThat(c).as("count(*) must not be null across CALL db.labels()").isNotNull();
      assertThat(c.longValue()).as("count must be 1").isEqualTo(1L);
      assertThat((Object) row.getProperty("label")).as("label must not be null").isNotNull();
    }
  }
}
