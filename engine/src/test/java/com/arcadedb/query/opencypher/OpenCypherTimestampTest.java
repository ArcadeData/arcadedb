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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #3419: Cypher timestamp() should return an integer (millis since epoch),
 * not a date in string format.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class OpenCypherTimestampTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./databases/test-timestamp").create();
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void timestampReturnsLong() {
    final long before = System.currentTimeMillis();
    try (final ResultSet rs = database.command("opencypher", "RETURN timestamp() AS ts")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      final Object ts = row.getProperty("ts");
      assertThat(ts).isInstanceOf(Long.class);
      final long tsValue = (Long) ts;
      final long after = System.currentTimeMillis();
      assertThat(tsValue).isBetween(before, after);
    }
  }

  @Test
  void timestampIsNumeric() {
    try (final ResultSet rs = database.command("opencypher", "RETURN timestamp() > 0 AS positive")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((Boolean) row.getProperty("positive")).isTrue();
    }
  }

  @Test
  void timestampArithmetic() {
    try (final ResultSet rs = database.command("opencypher", "RETURN timestamp() - timestamp() AS diff")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      final Object diff = row.getProperty("diff");
      assertThat(diff).isInstanceOf(Long.class);
      // The difference should be very small (close to 0)
      assertThat(Math.abs((Long) diff)).isLessThan(1000L);
    }
  }

  /**
   * Opens an isolated database with a Foo vertex type that has DATETIME-typed properties. Used by the datetime
   * persistence and comparison tests below which need their own typed schema separate from the shared timestamp
   * test database.
   */
  private Database newDatetimeDatabase(final String tag, final String propertyName) {
    final Database db = new DatabaseFactory("./target/databases/test-timestamp-" + tag + "-" + UUID.randomUUID()).create();
    db.transaction(() -> {
      final VertexType type = db.getSchema().createVertexType("Foo");
      type.createProperty("id", Type.STRING);
      type.createProperty(propertyName, Type.DATETIME);
    });
    return db;
  }

  // Issue #4125: CREATE with datetime() persists on DATETIME-typed property
  @Test
  void createWithDatetimePersists() {
    final Database db = newDatetimeDatabase("create-datetime", "t");
    try {
      final LocalDateTime before = LocalDateTime.now().minusMinutes(1);
      db.command("opencypher", "CREATE (n:Foo {id: 'a', t: datetime()})");
      final LocalDateTime after = LocalDateTime.now().plusMinutes(1);

      try (final ResultSet rs = db.query("opencypher", "MATCH (n:Foo {id: 'a'}) RETURN n.t AS t")) {
        final Object t = rs.next().getProperty("t");
        assertThat(t).as("datetime() set via CREATE must persist on DATETIME property")
            .isNotNull().isInstanceOf(CypherLocalDateTime.class);
        assertThat(((CypherLocalDateTime) t).getValue()).isBetween(before, after);
      }
    } finally {
      db.drop();
    }
  }

  // Issue #4125: SET n.t = datetime() persists on DATETIME-typed property
  @Test
  void setDatetimeNowPersists() {
    final Database db = newDatetimeDatabase("set-datetime", "t");
    try {
      db.command("opencypher", "CREATE (n:Foo {id: 'b'})");
      final LocalDateTime before = LocalDateTime.now().minusMinutes(1);
      db.command("opencypher", "MATCH (n:Foo {id: 'b'}) SET n.t = datetime()");
      final LocalDateTime after = LocalDateTime.now().plusMinutes(1);

      try (final ResultSet rs = db.query("opencypher", "MATCH (n:Foo {id: 'b'}) RETURN n.t AS t")) {
        final Object t = rs.next().getProperty("t");
        assertThat(t).as("datetime() set via SET must persist on DATETIME property")
            .isNotNull().isInstanceOf(CypherLocalDateTime.class);
        assertThat(((CypherLocalDateTime) t).getValue()).isBetween(before, after);
      }
    } finally {
      db.drop();
    }
  }

  // Issue #4125: SET n.t = datetime('<string>') persists on DATETIME-typed property
  @Test
  void setExplicitDatetimeStringPersists() {
    final Database db = newDatetimeDatabase("set-datetime-string", "t");
    try {
      db.command("opencypher", "CREATE (n:Foo {id: 'c'})");
      db.command("opencypher", "MATCH (n:Foo {id: 'c'}) SET n.t = datetime('2026-01-01T00:00:00')");

      try (final ResultSet rs = db.query("opencypher", "MATCH (n:Foo {id: 'c'}) RETURN n.t AS t")) {
        final Object t = rs.next().getProperty("t");
        assertThat(t).as("datetime(string) set via SET must persist on DATETIME property")
            .isNotNull().isInstanceOf(CypherLocalDateTime.class);
        assertThat(((CypherLocalDateTime) t).getValue()).isEqualTo(LocalDateTime.of(2026, 1, 1, 0, 0, 0));
      }
    } finally {
      db.drop();
    }
  }

  // Issue #4125: MERGE ON CREATE SET with datetime() persists
  @Test
  void mergeOnCreateSetDatetimePersists() {
    final Database db = newDatetimeDatabase("merge-create-datetime", "t");
    try {
      final LocalDateTime before = LocalDateTime.now().minusMinutes(1);
      db.command("opencypher", "MERGE (n:Foo {id: 'merge-a'}) ON CREATE SET n.t = datetime()");
      final LocalDateTime after = LocalDateTime.now().plusMinutes(1);

      try (final ResultSet rs = db.query("opencypher", "MATCH (n:Foo {id: 'merge-a'}) RETURN n.t AS t")) {
        final Object t = rs.next().getProperty("t");
        assertThat(t).as("datetime() set via MERGE ON CREATE SET must persist")
            .isNotNull().isInstanceOf(CypherLocalDateTime.class);
        assertThat(((CypherLocalDateTime) t).getValue()).isBetween(before, after);
      }
    } finally {
      db.drop();
    }
  }

  // Issue #4125: MERGE ON MATCH SET with datetime() persists
  @Test
  void mergeOnMatchSetDatetimePersists() {
    final Database db = newDatetimeDatabase("merge-match-datetime", "t");
    try {
      db.command("opencypher", "CREATE (n:Foo {id: 'merge-b'})");
      final LocalDateTime before = LocalDateTime.now().minusMinutes(1);
      db.command("opencypher", "MERGE (n:Foo {id: 'merge-b'}) ON MATCH SET n.t = datetime()");
      final LocalDateTime after = LocalDateTime.now().plusMinutes(1);

      try (final ResultSet rs = db.query("opencypher", "MATCH (n:Foo {id: 'merge-b'}) RETURN n.t AS t")) {
        final Object t = rs.next().getProperty("t");
        assertThat(t).as("datetime() set via MERGE ON MATCH SET must persist")
            .isNotNull().isInstanceOf(CypherLocalDateTime.class);
        assertThat(((CypherLocalDateTime) t).getValue()).isBetween(before, after);
      }
    } finally {
      db.drop();
    }
  }

  /**
   * Opens an isolated Channel database seeded with past/future DATETIME rows, used by the datetime comparison
   * tests below.
   */
  private Database newDatetimeComparisonDatabase(final String tag) {
    final Database db = new DatabaseFactory("./target/databases/test-timestamp-" + tag + "-" + UUID.randomUUID()).create();
    db.transaction(() -> {
      final VertexType type = db.getSchema().createVertexType("Channel");
      type.createProperty("name", Type.STRING);
      type.createProperty("updatedAt", Type.DATETIME);
    });
    db.transaction(() -> {
      // past: 2020-01-01T00:00:00
      db.command("cypher", "CREATE (c:Channel {name: 'past', updatedAt: localdatetime('2020-01-01T00:00:00')})");
      // future: year 2099
      db.command("cypher", "CREATE (c:Channel {name: 'future', updatedAt: localdatetime('2099-06-01T00:00:00')})");
    });
    return db;
  }

  // Issue #4231: DATETIME-typed property < datetime() works without explicit conversion wrapper
  @Test
  void datetimePropertyLessThanDatetimeFunction() {
    final Database db = newDatetimeComparisonDatabase("dt-lt");
    try {
      final ResultSet rs = db.query("cypher", "MATCH (c:Channel) WHERE c.updatedAt < datetime() RETURN c.name");
      final List<String> names = rs.stream().map(r -> (String) r.getProperty("c.name")).collect(Collectors.toList());
      assertThat(names).containsExactlyInAnyOrder("past");
    } finally {
      db.drop();
    }
  }

  // Issue #4231: DATETIME-typed property > datetime()
  @Test
  void datetimePropertyGreaterThanDatetimeFunction() {
    final Database db = newDatetimeComparisonDatabase("dt-gt");
    try {
      final ResultSet rs = db.query("cypher", "MATCH (c:Channel) WHERE c.updatedAt > datetime() RETURN c.name");
      final List<String> names = rs.stream().map(r -> (String) r.getProperty("c.name")).collect(Collectors.toList());
      assertThat(names).containsExactlyInAnyOrder("future");
    } finally {
      db.drop();
    }
  }

  // Issue #4231: datetime() < DATETIME-typed property
  @Test
  void datetimeFunctionLessThanDatetimeProperty() {
    final Database db = newDatetimeComparisonDatabase("dt-fn-lt");
    try {
      final ResultSet rs = db.query("cypher", "MATCH (c:Channel) WHERE datetime() < c.updatedAt RETURN c.name");
      final List<String> names = rs.stream().map(r -> (String) r.getProperty("c.name")).collect(Collectors.toList());
      assertThat(names).containsExactlyInAnyOrder("future");
    } finally {
      db.drop();
    }
  }

  // Issue #4231: DATETIME-typed property <= datetime('<literal>')
  @Test
  void datetimePropertyLessThanEqualDatetimeFunction() {
    final Database db = newDatetimeComparisonDatabase("dt-le");
    try {
      final ResultSet rs = db.query("cypher",
          "MATCH (c:Channel) WHERE c.updatedAt <= datetime('2020-01-01T00:00:00Z') RETURN c.name");
      final List<String> names = rs.stream().map(r -> (String) r.getProperty("c.name")).collect(Collectors.toList());
      assertThat(names).containsExactlyInAnyOrder("past");
    } finally {
      db.drop();
    }
  }

  // Issue #4231: DATETIME-typed property >= datetime('<literal>')
  @Test
  void datetimePropertyGreaterThanEqualDatetimeFunction() {
    final Database db = newDatetimeComparisonDatabase("dt-ge");
    try {
      final ResultSet rs = db.query("cypher",
          "MATCH (c:Channel) WHERE c.updatedAt >= datetime('2099-06-01T00:00:00Z') RETURN c.name");
      final List<String> names = rs.stream().map(r -> (String) r.getProperty("c.name")).collect(Collectors.toList());
      assertThat(names).containsExactlyInAnyOrder("future");
    } finally {
      db.drop();
    }
  }

  // Issue #4231: DATETIME-typed property = datetime('<literal>') (cross-type equality)
  @Test
  void datetimePropertyEqualsDatetime() {
    final Database db = newDatetimeComparisonDatabase("dt-eq");
    try {
      final ResultSet rs = db.query("cypher",
          "MATCH (c:Channel) WHERE c.updatedAt = datetime('2020-01-01T00:00:00Z') RETURN c.name");
      final List<String> names = rs.stream().map(r -> (String) r.getProperty("c.name")).collect(Collectors.toList());
      assertThat(names).containsExactlyInAnyOrder("past");
    } finally {
      db.drop();
    }
  }
}
