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
package com.arcadedb.integration.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.integration.importer.graph.CsvRowSource;
import com.arcadedb.integration.importer.graph.GraphImporter;
import com.arcadedb.integration.importer.graph.JsonlRowSource;
import com.arcadedb.integration.importer.graph.XmlRowSource;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * An empty value on a DATETIME property means "not set", exactly as it does for every other property
 * type {@code GraphImporter.readProperty} dispatches on, and never aborts the import (issue #7265).
 * <p>
 * A nullable timestamp with blank cells is the normal shape of an optional date exported from almost
 * any system, so every source that can hand {@code readProperty} a non-null empty string gets its
 * own case: {@code JsonlRowSource} and {@code XmlRowSource} return it verbatim, while
 * {@code CsvRowSource} already maps it to {@code null} and is pinned here so it stays that way.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GraphImporterEmptyDatetimeTest {

  private static final String DB_PATH   = "target/databases/graph-importer-empty-datetime-test";
  private static final String DATA_PATH = "target/test-data/graph-importer-empty-datetime";

  private Database database;
  private File     dataDir;

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    FileUtils.deleteRecursively(new File(DATA_PATH));
    dataDir = new File(DATA_PATH);
    dataDir.mkdirs();
    database = new DatabaseFactory(DB_PATH).create();
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      // An import that aborts mid-row leaves the batch's transaction open, and the instance is not
      // released until it is rolled back - which would turn one real failure here into a cascade of
      // "already in use" errors in every later test of the class.
      if (database.isTransactionActive())
        database.rollback();
      if (database.isOpen())
        database.drop();
      database = null;
    }
    FileUtils.deleteRecursively(new File(DB_PATH));
    FileUtils.deleteRecursively(new File(DATA_PATH));
  }

  private String write(final String fileName, final String... lines) throws Exception {
    final File f = new File(dataDir, fileName);
    Files.write(f.toPath(), String.join("\n", lines).getBytes(StandardCharsets.UTF_8));
    return f.getAbsolutePath();
  }

  /**
   * The vertex path with a JSONL source: {@code JsonlRecordReader.get} returns the empty string
   * verbatim for an explicit {@code ""}, so this is the shape that reaches {@code LocalDateTime.parse("")}.
   */
  @Test
  void emptyJsonlDatetimeLeavesTheVertexPropertyUnset() throws Exception {
    final String vertices = write("empty-datetime-vertices.jsonl",
        "{\"id\": \"1\", \"name\": \"alice\", \"deletedAt\": \"\"}",
        "{\"id\": \"2\", \"name\": \"bob\",   \"deletedAt\": \"2026-01-05 10:00:00\"}",
        "{\"id\": \"3\", \"name\": \"carol\"}");

    database.transaction(() -> database.getSchema().createVertexType("User"));

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("User", new JsonlRowSource(vertices), v -> {
          v.id("id");
          v.property("name", "name");
          v.datetimeProperty("deletedAt", "deletedAt");
        })
        .build()) {

      importer.run();
      assertThat(importer.getVertexCount()).isEqualTo(3);
    }

    assertThat(deletedAtOf("alice")).isNull();
    assertThat(deletedAtOf("bob")).isEqualTo(LocalDateTime.of(2026, 1, 5, 10, 0, 0));
    // an absent attribute already worked and must keep working
    assertThat(deletedAtOf("carol")).isNull();
  }

  /**
   * The vertex path with an XML attribute source, and with an explicit datetime format: the empty
   * check has to happen before the formatter is chosen, or a custom format fails the same way.
   */
  @Test
  void emptyXmlDatetimeAttributeLeavesTheVertexPropertyUnset() throws Exception {
    final String vertices = write("empty-datetime-vertices.xml",
        "<users>",
        "  <row Id=\"1\" Name=\"alice\" DeletedAt=\"\" />",
        "  <row Id=\"2\" Name=\"bob\" DeletedAt=\"05/01/2026 10:00:00\" />",
        "</users>");

    database.transaction(() -> database.getSchema().createVertexType("User"));

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("User", new XmlRowSource(vertices), v -> {
          v.id("Id");
          v.property("name", "Name");
          v.datetimeProperty("deletedAt", "DeletedAt", "dd/MM/yyyy HH:mm:ss");
        })
        .build()) {

      importer.run();
      assertThat(importer.getVertexCount()).isEqualTo(2);
    }

    assertThat(deletedAtOf("alice")).isNull();
    assertThat(deletedAtOf("bob")).isEqualTo(LocalDateTime.of(2026, 1, 5, 10, 0, 0));
  }

  /**
   * {@code CsvRowSource.get} already maps an empty cell to {@code null}, so the issue's own CSV
   * repro never reached the parse. Pinned so the CSV source cannot start returning {@code ""}
   * without this failing.
   */
  @Test
  void emptyCsvDatetimeCellLeavesTheVertexPropertyUnset() throws Exception {
    final String vertices = write("empty-datetime-vertices.csv",
        "id,name,deletedAt",
        "1,alice,",
        "2,bob,2026-01-05 10:00:00");

    database.transaction(() -> database.getSchema().createVertexType("User"));

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("User", new CsvRowSource(vertices), v -> {
          v.id("id");
          v.property("name", "name");
          v.datetimeProperty("deletedAt", "deletedAt");
        })
        .build()) {

      importer.run();
      assertThat(importer.getVertexCount()).isEqualTo(2);
    }

    assertThat(deletedAtOf("alice")).isNull();
    assertThat(deletedAtOf("bob")).isEqualTo(LocalDateTime.of(2026, 1, 5, 10, 0, 0));
  }

  /**
   * The edge-source path: DATETIME falls into {@code processEdgeSource}'s default branch, which
   * routes it through the same {@code readProperty} the vertex pass uses and then drops a
   * {@code null} in {@code EdgeCollector.appendProperties}.
   */
  @Test
  void emptyEdgeSourceDatetimeLeavesTheEdgePropertyUnset() throws Exception {
    final String vertices = write("empty-datetime-edge-vertices.jsonl",
        "{\"id\": \"1\", \"name\": \"alice\"}",
        "{\"id\": \"2\", \"name\": \"bob\"}");
    final String edges = write("empty-datetime-edges.jsonl",
        "{\"from\": \"1\", \"to\": \"2\", \"kind\": \"blank\",  \"since\": \"\"}",
        "{\"from\": \"2\", \"to\": \"1\", \"kind\": \"filled\", \"since\": \"2026-01-05 10:00:00\"}");

    database.transaction(() -> {
      database.getSchema().createVertexType("User");
      database.getSchema().createEdgeType("Knows");
    });

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("User", new JsonlRowSource(vertices), v -> {
          v.id("id");
          v.property("name", "name");
        })
        .edgeSource("Knows", new JsonlRowSource(edges), e -> {
          e.from("from", "User");
          e.to("to", "User");
          e.property("kind", "kind");
          e.datetimeProperty("since", "since");
        })
        .build()) {

      importer.run();
      assertThat(importer.getEdgeCount()).isEqualTo(2);
    }

    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM Knows WHERE kind = 'blank'")) {
        assertThat(rs.hasNext()).isTrue();
        final Edge e = rs.next().getEdge().get();
        assertThat(e.has("since")).isFalse();
      }
      try (final ResultSet rs = database.query("sql", "SELECT FROM Knows WHERE kind = 'filled'")) {
        assertThat(rs.hasNext()).isTrue();
        final Edge e = rs.next().getEdge().get();
        assertThat(e.getLocalDateTime("since")).isEqualTo(LocalDateTime.of(2026, 1, 5, 10, 0, 0));
      }
    });
  }

  /**
   * Empty means "not set"; a value that is present but not a datetime is still a data error, and
   * still names the property and the source attribute.
   */
  @Test
  void aMalformedDatetimeIsStillReported() throws Exception {
    final String vertices = write("malformed-datetime-vertices.jsonl",
        "{\"id\": \"1\", \"name\": \"alice\", \"deletedAt\": \"not-a-date\"}");

    database.transaction(() -> database.getSchema().createVertexType("User"));

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("User", new JsonlRowSource(vertices), v -> {
          v.id("id");
          v.property("name", "name");
          v.datetimeProperty("deletedAt", "deletedAt");
        })
        .build()) {

      assertThatThrownBy(importer::run)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("deletedAt")
          .hasMessageContaining("declared as a datetime");
    }
  }

  private LocalDateTime deletedAtOf(final String name) {
    final LocalDateTime[] result = new LocalDateTime[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM User WHERE name = ?", name)) {
        assertThat(rs.hasNext()).isTrue();
        final Vertex v = rs.next().getVertex().get();
        result[0] = v.has("deletedAt") ? v.getLocalDateTime("deletedAt") : null;
      }
    });
    return result[0];
  }
}
