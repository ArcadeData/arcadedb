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
package com.arcadedb.integration.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Vertex;
import com.arcadedb.integration.importer.graph.GraphImporter;
import com.arcadedb.integration.importer.graph.JsonlRowSource;
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

/**
 * Issue #10596: a {@code datetimeFormat} that reads an offset out of the text stores the instant the text names, as every other
 * write path does (issues #10489, #10498). The importer parsed straight into a {@code LocalDateTime}, which takes the local fields of
 * the parse and drops the offset the pattern had just read, so the same instant was stored hours away from the one an HTTP, SQL or
 * Bolt client stores.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10596GraphImporterDatetimeOffsetTest {

  private static final String DB_PATH   = "target/databases/issue-10596-graph-importer-datetime-offset-test";
  private static final String DATA_PATH = "target/test-data/issue-10596-graph-importer-datetime-offset";

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
      if (database.isTransactionActive())
        database.rollback();
      if (database.isOpen())
        database.drop();
      database = null;
    }
    FileUtils.deleteRecursively(new File(DB_PATH));
    FileUtils.deleteRecursively(new File(DATA_PATH));
  }

  @Test
  void anOffsetInTheFormatIsConvertedToTheUtcWallClock() throws Exception {
    final File file = new File(dataDir, "vertices.jsonl");
    Files.write(file.toPath(), String.join("\n",
        "{\"id\": \"1\", \"name\": \"east\", \"at\": \"2020-01-02T03:04:05+02:00\"}",
        "{\"id\": \"2\", \"name\": \"utc\",  \"at\": \"2020-01-02T03:04:05Z\"}",
        "{\"id\": \"3\", \"name\": \"west\", \"at\": \"2020-01-02T03:04:05-05:00\"}").getBytes(StandardCharsets.UTF_8));

    database.transaction(() -> database.getSchema().createVertexType("User"));

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("User", new JsonlRowSource(file.getAbsolutePath()), v -> {
          v.id("id");
          v.property("name", "name");
          v.datetimeProperty("at", "at", "yyyy-MM-dd'T'HH:mm:ssXXX");
        })
        .build()) {
      importer.run();
      assertThat(importer.getVertexCount()).isEqualTo(3);
    }

    assertThat(atOf("east")).as("03:04:05 at +02:00 is 01:04:05 UTC").isEqualTo(LocalDateTime.of(2020, 1, 2, 1, 4, 5));
    assertThat(atOf("utc")).as("Z keeps its wall clock").isEqualTo(LocalDateTime.of(2020, 1, 2, 3, 4, 5));
    assertThat(atOf("west")).as("03:04:05 at -05:00 is 08:04:05 UTC").isEqualTo(LocalDateTime.of(2020, 1, 2, 8, 4, 5));
  }

  @Test
  void aFormatWithoutAnOffsetKeepsTheWallClock() throws Exception {
    final File file = new File(dataDir, "vertices-plain.jsonl");
    Files.write(file.toPath(), String.join("\n",
        "{\"id\": \"1\", \"name\": \"plain\", \"at\": \"05/01/2026 10:00:00\"}").getBytes(StandardCharsets.UTF_8));

    database.transaction(() -> database.getSchema().createVertexType("User"));

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("User", new JsonlRowSource(file.getAbsolutePath()), v -> {
          v.id("id");
          v.property("name", "name");
          v.datetimeProperty("at", "at", "dd/MM/yyyy HH:mm:ss");
        })
        .build()) {
      importer.run();
    }

    assertThat(atOf("plain")).isEqualTo(LocalDateTime.of(2026, 1, 5, 10, 0, 0));
  }

  private LocalDateTime atOf(final String name) {
    final LocalDateTime[] result = new LocalDateTime[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM User WHERE name = ?", name)) {
        assertThat(rs.hasNext()).isTrue();
        final Vertex v = rs.next().getVertex().get();
        result[0] = v.getLocalDateTime("at");
      }
    });
    return result[0];
  }
}
