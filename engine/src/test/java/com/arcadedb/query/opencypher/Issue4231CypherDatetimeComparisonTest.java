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
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for comparing DATETIME-typed properties directly with datetime() in Cypher
 * without an explicit datetime() conversion wrapper.
 * <p>
 * MATCH (c:Channel) WHERE c.updatedAt &lt; datetime() must work when updatedAt is DATETIME-typed,
 * without requiring the user to write WHERE datetime(c.updatedAt) &lt; datetime().
 */
class Issue4231CypherDatetimeComparisonTest {

  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/issue-4231-datetime-comparison").create();
    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("Channel");
      type.createProperty("name", Type.STRING);
      type.createProperty("updatedAt", Type.DATETIME);
    });
    database.transaction(() -> {
      // past: 2020-01-01T00:00:00
      database.command("cypher", "CREATE (c:Channel {name: 'past', updatedAt: localdatetime('2020-01-01T00:00:00')})");
      // future: year 2099
      database.command("cypher", "CREATE (c:Channel {name: 'future', updatedAt: localdatetime('2099-06-01T00:00:00')})");
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
  void datetimePropertyLessThanDatetimeFunction() {
    // Both 2020 nodes and the future node are present; only the past node satisfies updatedAt < now
    final ResultSet rs = database.query("cypher", "MATCH (c:Channel) WHERE c.updatedAt < datetime() RETURN c.name");
    final List<String> names = rs.stream().map(r -> (String) r.getProperty("c.name")).collect(Collectors.toList());
    assertThat(names).containsExactlyInAnyOrder("past");
  }

  @Test
  void datetimePropertyGreaterThanDatetimeFunction() {
    final ResultSet rs = database.query("cypher", "MATCH (c:Channel) WHERE c.updatedAt > datetime() RETURN c.name");
    final List<String> names = rs.stream().map(r -> (String) r.getProperty("c.name")).collect(Collectors.toList());
    assertThat(names).containsExactlyInAnyOrder("future");
  }

  @Test
  void datetimeFunctionLessThanDatetimeProperty() {
    // datetime() < future node → only future satisfies this
    final ResultSet rs = database.query("cypher", "MATCH (c:Channel) WHERE datetime() < c.updatedAt RETURN c.name");
    final List<String> names = rs.stream().map(r -> (String) r.getProperty("c.name")).collect(Collectors.toList());
    assertThat(names).containsExactlyInAnyOrder("future");
  }

  @Test
  void datetimePropertyLessThanEqualDatetimeFunction() {
    final ResultSet rs = database.query("cypher",
        "MATCH (c:Channel) WHERE c.updatedAt <= datetime('2020-01-01T00:00:00Z') RETURN c.name");
    final List<String> names = rs.stream().map(r -> (String) r.getProperty("c.name")).collect(Collectors.toList());
    assertThat(names).containsExactlyInAnyOrder("past");
  }

  @Test
  void datetimePropertyGreaterThanEqualDatetimeFunction() {
    final ResultSet rs = database.query("cypher",
        "MATCH (c:Channel) WHERE c.updatedAt >= datetime('2099-06-01T00:00:00Z') RETURN c.name");
    final List<String> names = rs.stream().map(r -> (String) r.getProperty("c.name")).collect(Collectors.toList());
    assertThat(names).containsExactlyInAnyOrder("future");
  }

  @Test
  void datetimePropertyEqualsDatetime() {
    // Equality: stored 2020-01-01T00:00:00 (treated as UTC) == datetime('2020-01-01T00:00:00Z')
    final ResultSet rs = database.query("cypher",
        "MATCH (c:Channel) WHERE c.updatedAt = datetime('2020-01-01T00:00:00Z') RETURN c.name");
    final List<String> names = rs.stream().map(r -> (String) r.getProperty("c.name")).collect(Collectors.toList());
    assertThat(names).containsExactlyInAnyOrder("past");
  }
}
