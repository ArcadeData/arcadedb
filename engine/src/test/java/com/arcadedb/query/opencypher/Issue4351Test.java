/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Reproducer for <a href="https://github.com/ArcadeData/arcadedb/issues/4351">issue #4351</a>:
 * MERGE on a UNIQUE-indexed property throws {@code DuplicatedKeyException} when the same
 * key appears twice in a single transaction (or batch). Neo4j matches the second
 * occurrence to the first MERGE's result; ArcadeDB used to fail because the second
 * MERGE went through the create branch and collided with the just-inserted index entry.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4351Test {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue4351");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    final VertexType worker = database.getSchema().createVertexType("Worker");
    worker.createProperty("name", Type.STRING);
    worker.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "name");
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  /**
   * Two MERGE statements on the same UNIQUE key inside a single transaction must
   * not throw: the second MERGE has to match the row created by the first.
   */
  @Test
  void cypherMergeSameKeyTwiceInOneTransaction() {
    assertThatCode(() -> database.transaction(() -> {
      database.command("opencypher", "MERGE (w:Worker {name: 'John'}) ON CREATE SET w.role = 'eng'");
      database.command("opencypher", "MERGE (w:Worker {name: 'John'}) ON CREATE SET w.role = 'pm'");
    })).doesNotThrowAnyException();

    try (final ResultSet rs = database.query("opencypher", "MATCH (w:Worker {name: 'John'}) RETURN w.role AS role, count(w) AS cnt")) {
      assertThat(rs.hasNext()).isTrue();
      final var row = rs.next();
      assertThat(((Number) row.getProperty("cnt")).longValue()).isEqualTo(1L);
      // The second MERGE matched the first, so ON CREATE SET only ran on the first occurrence.
      assertThat(row.<String>getProperty("role")).isEqualTo("eng");
    }
  }

  /**
   * Three MERGE statements on the same UNIQUE key inside one transaction:
   * one create + two matches, all observing the same vertex.
   */
  @Test
  void cypherMergeSameKeyThreeTimesInOneTransaction() {
    assertThatCode(() -> database.transaction(() -> {
      database.command("opencypher", "MERGE (w:Worker {name: 'Anna'}) ON CREATE SET w.created = 1 ON MATCH SET w.matched = coalesce(w.matched, 0) + 1");
      database.command("opencypher", "MERGE (w:Worker {name: 'Anna'}) ON CREATE SET w.created = 2 ON MATCH SET w.matched = coalesce(w.matched, 0) + 1");
      database.command("opencypher", "MERGE (w:Worker {name: 'Anna'}) ON CREATE SET w.created = 3 ON MATCH SET w.matched = coalesce(w.matched, 0) + 1");
    })).doesNotThrowAnyException();

    try (final ResultSet rs = database.query("opencypher", "MATCH (w:Worker {name: 'Anna'}) RETURN w.created AS created, w.matched AS matched, count(w) AS cnt")) {
      assertThat(rs.hasNext()).isTrue();
      final var row = rs.next();
      assertThat(((Number) row.getProperty("cnt")).longValue()).isEqualTo(1L);
      assertThat(((Number) row.getProperty("created")).intValue()).isEqualTo(1);
      assertThat(((Number) row.getProperty("matched")).intValue()).isEqualTo(2);
    }
  }

  /**
   * The SQL-flavour reproduction: an UPDATE ... UPSERT against a unique-indexed
   * property must match the row inserted earlier in the same transaction instead of
   * throwing {@code DuplicatedKeyException}.
   */
  @Test
  void sqlUpsertSameKeyTwiceInOneTransaction() {
    assertThatCode(() -> database.transaction(() -> {
      database.command("sql", "UPDATE Worker SET name = 'John', role = 'eng' UPSERT WHERE name = 'John'");
      database.command("sql", "UPDATE Worker SET name = 'John', role = 'pm' UPSERT WHERE name = 'John'");
    })).doesNotThrowAnyException();

    try (final ResultSet rs = database.query("sql", "SELECT role, count(*) AS cnt FROM Worker WHERE name = 'John' GROUP BY role")) {
      assertThat(rs.hasNext()).isTrue();
      final var row = rs.next();
      assertThat(((Number) row.getProperty("cnt")).longValue()).isEqualTo(1L);
      // The second UPSERT matched the first and updated it, so role is 'pm'.
      assertThat(row.<String>getProperty("role")).isEqualTo("pm");
    }
  }

  /**
   * The issue's example phrasing was "a batch of statements" - exercise the
   * {@code sqlscript} engine, which runs all statements inside a single managed
   * transaction without the caller wrapping a transaction block.
   */
  @Test
  void sqlScriptUpsertSameKeyTwiceInBatch() {
    assertThatCode(() -> database.command("sqlscript", """
        BEGIN;
        UPDATE Worker SET name = 'John', role = 'eng' UPSERT WHERE name = 'John';
        UPDATE Worker SET name = 'John', role = 'pm' UPSERT WHERE name = 'John';
        COMMIT;
        """)).doesNotThrowAnyException();

    try (final ResultSet rs = database.query("sql", "SELECT role, count(*) AS cnt FROM Worker WHERE name = 'John' GROUP BY role")) {
      assertThat(rs.hasNext()).isTrue();
      final var row = rs.next();
      assertThat(((Number) row.getProperty("cnt")).longValue()).isEqualTo(1L);
      assertThat(row.<String>getProperty("role")).isEqualTo("pm");
    }
  }

  /**
   * Larger UNWIND batch with the duplicate key sprinkled across the array.
   * Mirrors the blog post's reported case more faithfully.
   */
  @Test
  void cypherUnwindMergeLargerBatchWithDuplicates() {
    final List<Map<String, Object>> batch = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      batch.add(Map.of("name", "John", "role", "role_" + i));
    }
    final Map<String, Object> params = Map.of("batch", batch);

    assertThatCode(() -> database.command("opencypher",
        "UNWIND $batch AS row MERGE (w:Worker {name: row.name}) ON CREATE SET w.role = row.role",
        params)).doesNotThrowAnyException();

    try (final ResultSet rs = database.query("opencypher",
        "MATCH (w:Worker {name: 'John'}) RETURN count(w) AS cnt")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("cnt")).longValue()).isEqualTo(1L);
    }
  }

  /**
   * The original blog-post scenario: a single {@code UNWIND $batch AS row MERGE ...}
   * statement where the input batch contains duplicate keys.  Neo4j matches the
   * second occurrence to the first MERGE's result; ArcadeDB used to walk down
   * the create branch and trip {@code DuplicatedKeyException} on the unique
   * index entry created by the prior iteration.
   */
  @Test
  void cypherUnwindMergeWithDuplicateKeyInBatch() {
    final Map<String, Object> params = Map.of("batch", List.of(
        Map.of("name", "John", "role", "eng"),
        Map.of("name", "John", "role", "pm"),
        Map.of("name", "Jane", "role", "ops")));

    assertThatCode(() -> database.command("opencypher",
        "UNWIND $batch AS row MERGE (w:Worker {name: row.name}) ON CREATE SET w.role = row.role",
        params)).doesNotThrowAnyException();

    try (final ResultSet rs = database.query("opencypher",
        "MATCH (w:Worker) RETURN w.name AS name, w.role AS role ORDER BY w.name")) {
      assertThat(rs.hasNext()).isTrue();
      var row = rs.next();
      assertThat(row.<String>getProperty("name")).isEqualTo("Jane");
      assertThat(row.<String>getProperty("role")).isEqualTo("ops");

      assertThat(rs.hasNext()).isTrue();
      row = rs.next();
      assertThat(row.<String>getProperty("name")).isEqualTo("John");
      // ON CREATE SET only fires on the first occurrence; second batch row matched.
      assertThat(row.<String>getProperty("role")).isEqualTo("eng");

      assertThat(rs.hasNext()).isFalse();
    }
  }

  /**
   * Mixed batch: an INSERT then a MERGE on the same key. The INSERT writes the
   * unique index entry; the subsequent MERGE must find it instead of going
   * down the create branch and tripping the index conflict.
   */
  @Test
  void cypherCreateThenMergeSameKeyInOneTransaction() {
    assertThatCode(() -> database.transaction(() -> {
      database.command("opencypher", "CREATE (w:Worker {name: 'Zoe', role: 'eng'})");
      database.command("opencypher", "MERGE (w:Worker {name: 'Zoe'}) ON CREATE SET w.role = 'pm' ON MATCH SET w.matched = true");
    })).doesNotThrowAnyException();

    try (final ResultSet rs = database.query("opencypher", "MATCH (w:Worker {name: 'Zoe'}) RETURN w.role AS role, w.matched AS matched, count(w) AS cnt")) {
      assertThat(rs.hasNext()).isTrue();
      final var row = rs.next();
      assertThat(((Number) row.getProperty("cnt")).longValue()).isEqualTo(1L);
      assertThat(row.<String>getProperty("role")).isEqualTo("eng");
      assertThat(row.<Boolean>getProperty("matched")).isTrue();
    }
  }
}
