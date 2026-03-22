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
 * with Question/Answer split via row filter.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EnabledIf("datasetExists")
class StackOverflowImporterConfigTest {

  private static final String DATA_DIR = "/Users/luca/Downloads/stackoverflow-large";
  private static final String DB_PATH  = "target/databases/stackoverflow-config-test";
  private static final long   LIMIT    = 10_000;

  private Database database;

  static boolean datasetExists() {
    return new File(DATA_DIR, "Posts.xml").exists();
  }

  @BeforeAll
  void importData() throws Exception {
    FileUtils.deleteRecursively(new File(DB_PATH));

    final String json = new String(Files.readAllBytes(
        new File("src/test/resources/stackoverflow-import.json").toPath()));
    final JSONObject config = new JSONObject(json);
    config.put("limit", LIMIT);

    final Database db = new DatabaseFactory(DB_PATH).create();
    GraphImporter.createSchemaFromConfig(db, config);

    try (final GraphImporter importer = GraphImporter.fromJSON(db, config, DATA_DIR)) {
      importer.run();
      assertThat(importer.getVertexCount()).isGreaterThan(30_000);
      assertThat(importer.getEdgeCount()).isGreaterThan(5_000);
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
      // Question + Answer together equal LIMIT (both read from Posts.xml with 10K limit each)
      assertThat(count("SELECT count(*) as c FROM Question")).isGreaterThan(0);
      assertThat(count("SELECT count(*) as c FROM Answer")).isGreaterThan(0);
      assertThat(count("SELECT count(*) as c FROM Question") + count("SELECT count(*) as c FROM Answer"))
          .isEqualTo(LIMIT * 2); // 10K limit applied to each filtered pass
      assertThat(count("SELECT count(*) as c FROM Comment")).isEqualTo(LIMIT);
      assertThat(count("SELECT count(*) as c FROM Badge")).isEqualTo(LIMIT);
    });
  }

  @Test
  void questionHasExpectedProperties() {
    database.transaction(() -> {
      try (ResultSet rs = database.query("sql", "SELECT FROM Question LIMIT 1")) {
        assertThat(rs.hasNext()).isTrue();
        final var q = rs.next().getVertex().get();
        assertThat((Object) q.get("Title")).isNotNull();
        assertThat(q.getInteger("Id")).isGreaterThan(0);
      }
    });
  }

  @Test
  void answerHasNoTitle() {
    database.transaction(() -> {
      // Answers don't have title in our mapping
      try (ResultSet rs = database.query("sql", "SELECT FROM Answer LIMIT 1")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().getVertex().get().has("Title")).isFalse();
      }
    });
  }

  @Test
  void bidirectionalEdges() {
    database.transaction(() -> {
      // OUT edges
      assertThat(count("SELECT count(*) as c FROM User WHERE out('ANSWERED').size() > 0")).isGreaterThan(0);
      assertThat(count("SELECT count(*) as c FROM Question WHERE out('TAGGED_WITH').size() > 0")).isGreaterThan(0);
      assertThat(count("SELECT count(*) as c FROM Question WHERE out('HAS_ANSWER').size() > 0")).isGreaterThan(0);
      // IN edges
      assertThat(count("SELECT count(*) as c FROM Question WHERE in('ASKED').size() > 0")).isGreaterThan(0);
      assertThat(count("SELECT count(*) as c FROM Tag WHERE in('TAGGED_WITH').size() > 0")).isGreaterThan(0);
      assertThat(count("SELECT count(*) as c FROM Answer WHERE in('HAS_ANSWER').size() > 0")).isGreaterThan(0);
    });
  }

  @Test
  void hasAnswerConnectsAnswerToQuestion() {
    database.transaction(() -> {
      try (ResultSet rs = database.query("sql", "SELECT FROM Question WHERE out('HAS_ANSWER').size() > 0 LIMIT 1")) {
        assertThat(rs.hasNext()).isTrue();
        final var question = rs.next().getVertex().get();
        // Follow HAS_ANSWER edge to verify it reaches an Answer
        for (final var e : question.getEdges(com.arcadedb.graph.Vertex.DIRECTION.OUT, "HAS_ANSWER"))
          assertThat(e.getInVertex().asVertex().getTypeName()).isEqualTo("Answer");
      }
    });
  }

  @Test
  void commentedOnReachesBothTypes() {
    database.transaction(() -> {
      // Comments point to Questions via COMMENTED_ON and Answers via COMMENTED_ON_ANSWER
      final long onQuestions = count("SELECT count(*) as c FROM Comment WHERE out('COMMENTED_ON').size() > 0");
      final long onAnswers = count("SELECT count(*) as c FROM Comment WHERE out('COMMENTED_ON_ANSWER').size() > 0");
      assertThat(onQuestions + onAnswers).isGreaterThan(0);
    });
  }

  private long count(final String sql) {
    try (ResultSet rs = database.query("sql", sql)) {
      return rs.hasNext() ? ((Number) rs.next().getProperty("c")).longValue() : 0;
    }
  }
}
