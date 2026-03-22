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
import com.arcadedb.integration.importer.graph.GraphImporter;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIf;

import java.io.File;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Imports 10K rows per file from the StackOverflow dataset using JSON configuration
 * (same path as command-line usage). Verifies the graph structure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EnabledIf("datasetExists")
class StackOverflowImporterConfigTest {

  private static final String DATA_DIR = "/Users/luca/Downloads/stackoverflow-large";
  private static final String DB_PATH  = "target/databases/stackoverflow-generic-test";
  private static final long   LIMIT    = 10_000;

  private Database database;

  static boolean datasetExists() {
    return new File(DATA_DIR, "Posts.xml").exists();
  }

  @BeforeAll
  void importData() throws Exception {
    FileUtils.deleteRecursively(new File(DB_PATH));

    // Load JSON config from test resources (same file a CLI user would write)
    final String json = new String(Files.readAllBytes(
        new File("src/test/resources/stackoverflow-import.json").toPath()));

    // Inject the test limit
    final JSONObject config = new JSONObject(json);
    config.put("limit", LIMIT);

    final Database db = new DatabaseFactory(DB_PATH).create();

    // Auto-create schema + run import — same as GraphImporter.main() does
    GraphImporter.createSchemaFromConfig(db, config);

    try (final GraphImporter importer = GraphImporter.fromJSON(db, config, DATA_DIR)) {
      importer.run();
      assertThat(importer.getVertexCount()).isGreaterThan(30_000);
      assertThat(importer.getEdgeCount()).isGreaterThan(10_000);
    }

    database = db;
  }

  @AfterAll
  void cleanup() {
    if (database != null)
      database.close();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void vertexCounts() {
    database.transaction(() -> {
      assertThat(count("SELECT count(*) as c FROM Tag")).isGreaterThan(0);
      assertThat(count("SELECT count(*) as c FROM User")).isEqualTo(LIMIT);
      assertThat(count("SELECT count(*) as c FROM Post")).isEqualTo(LIMIT);
      assertThat(count("SELECT count(*) as c FROM Comment")).isEqualTo(LIMIT);
      assertThat(count("SELECT count(*) as c FROM Badge")).isEqualTo(LIMIT);
    });
  }

  @Test
  void bidirectionalEdges() {
    database.transaction(() -> {
      // OUT edges
      assertThat(count("SELECT count(*) as c FROM User WHERE out('Posted').size() > 0")).isGreaterThan(0);
      assertThat(count("SELECT count(*) as c FROM Post WHERE out('HasTag').size() > 0")).isGreaterThan(0);
      assertThat(count("SELECT count(*) as c FROM Post WHERE out('AnswerOf').size() > 0")).isGreaterThan(0);
      // IN edges
      assertThat(count("SELECT count(*) as c FROM Post WHERE in('Posted').size() > 0")).isGreaterThan(0);
      assertThat(count("SELECT count(*) as c FROM Tag WHERE in('HasTag').size() > 0")).isGreaterThan(0);
    });
  }

  @Test
  void answerOfConnectsAnswerToQuestion() {
    database.transaction(() -> {
      try (ResultSet rs = database.query("sql", "SELECT FROM Post WHERE out('AnswerOf').size() > 0 LIMIT 1")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().getVertex().get().getInteger("postType")).isEqualTo(2);
      }
    });
  }

  private long count(final String sql) {
    try (ResultSet rs = database.query("sql", sql)) {
      return rs.hasNext() ? ((Number) rs.next().getProperty("c")).longValue() : 0;
    }
  }
}
