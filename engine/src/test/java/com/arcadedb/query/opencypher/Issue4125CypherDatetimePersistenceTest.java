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
import com.arcadedb.query.opencypher.temporal.CypherLocalDateTime;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for datetime() value persistence on DATETIME-typed properties via Cypher.
 * <p>
 * CREATE or SET with datetime() on a DATETIME-typed column must persist the value so that
 * a subsequent MATCH returns a non-null result.
 */
class Issue4125CypherDatetimePersistenceTest {

  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/issue-4125-datetime-persistence").create();
    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("Foo");
      type.createProperty("id", Type.STRING);
      type.createProperty("t", Type.DATETIME);
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
  void createWithDatetimePersists() {
    final LocalDateTime before = LocalDateTime.now().minusMinutes(1);
    database.command("opencypher", "CREATE (n:Foo {id: 'a', t: datetime()})");
    final LocalDateTime after = LocalDateTime.now().plusMinutes(1);

    try (final ResultSet rs = database.query("opencypher", "MATCH (n:Foo {id: 'a'}) RETURN n.t AS t")) {
      final Object t = rs.next().getProperty("t");
      assertThat(t).as("datetime() set via CREATE must persist on DATETIME property")
          .isNotNull().isInstanceOf(CypherLocalDateTime.class);
      assertThat(((CypherLocalDateTime) t).getValue()).isBetween(before, after);
    }
  }

  @Test
  void setDatetimeNowPersists() {
    database.command("opencypher", "CREATE (n:Foo {id: 'b'})");
    final LocalDateTime before = LocalDateTime.now().minusMinutes(1);
    database.command("opencypher", "MATCH (n:Foo {id: 'b'}) SET n.t = datetime()");
    final LocalDateTime after = LocalDateTime.now().plusMinutes(1);

    try (final ResultSet rs = database.query("opencypher", "MATCH (n:Foo {id: 'b'}) RETURN n.t AS t")) {
      final Object t = rs.next().getProperty("t");
      assertThat(t).as("datetime() set via SET must persist on DATETIME property")
          .isNotNull().isInstanceOf(CypherLocalDateTime.class);
      assertThat(((CypherLocalDateTime) t).getValue()).isBetween(before, after);
    }
  }

  @Test
  void setExplicitDatetimeStringPersists() {
    database.command("opencypher", "CREATE (n:Foo {id: 'c'})");
    database.command("opencypher", "MATCH (n:Foo {id: 'c'}) SET n.t = datetime('2026-01-01T00:00:00')");

    try (final ResultSet rs = database.query("opencypher", "MATCH (n:Foo {id: 'c'}) RETURN n.t AS t")) {
      final Object t = rs.next().getProperty("t");
      assertThat(t).as("datetime(string) set via SET must persist on DATETIME property")
          .isNotNull().isInstanceOf(CypherLocalDateTime.class);
      assertThat(((CypherLocalDateTime) t).getValue()).isEqualTo(LocalDateTime.of(2026, 1, 1, 0, 0, 0));
    }
  }

  @Test
  void mergeOnCreateSetDatetimePersists() {
    final LocalDateTime before = LocalDateTime.now().minusMinutes(1);
    database.command("opencypher", "MERGE (n:Foo {id: 'merge-a'}) ON CREATE SET n.t = datetime()");
    final LocalDateTime after = LocalDateTime.now().plusMinutes(1);

    try (final ResultSet rs = database.query("opencypher", "MATCH (n:Foo {id: 'merge-a'}) RETURN n.t AS t")) {
      final Object t = rs.next().getProperty("t");
      assertThat(t).as("datetime() set via MERGE ON CREATE SET must persist")
          .isNotNull().isInstanceOf(CypherLocalDateTime.class);
      assertThat(((CypherLocalDateTime) t).getValue()).isBetween(before, after);
    }
  }

  @Test
  void mergeOnMatchSetDatetimePersists() {
    database.command("opencypher", "CREATE (n:Foo {id: 'merge-b'})");
    final LocalDateTime before = LocalDateTime.now().minusMinutes(1);
    database.command("opencypher", "MERGE (n:Foo {id: 'merge-b'}) ON MATCH SET n.t = datetime()");
    final LocalDateTime after = LocalDateTime.now().plusMinutes(1);

    try (final ResultSet rs = database.query("opencypher", "MATCH (n:Foo {id: 'merge-b'}) RETURN n.t AS t")) {
      final Object t = rs.next().getProperty("t");
      assertThat(t).as("datetime() set via MERGE ON MATCH SET must persist")
          .isNotNull().isInstanceOf(CypherLocalDateTime.class);
      assertThat(((CypherLocalDateTime) t).getValue()).isBetween(before, after);
    }
  }
}
