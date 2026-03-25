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
package com.arcadedb.index.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that CREATE INDEX ... LSM_VECTOR builds the HNSW graph immediately by default,
 * so that vector.neighbors queries work right after index creation without needing
 * an explicit buildVectorGraphNow() call.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class VectorIndexBuildGraphOnCreateTest extends TestHelper {

  @Test
  void buildGraphImmediatelyByDefault() {
    // Insert data BEFORE creating the index
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Movie");
      database.command("sql", "CREATE PROPERTY Movie.title STRING");
      database.command("sql", "CREATE PROPERTY Movie.embedding ARRAY_OF_FLOATS");

      database.command("sql", "INSERT INTO Movie SET title = 'A', embedding = [1.0, 0.0, 0.0, 0.0]");
      database.command("sql", "INSERT INTO Movie SET title = 'B', embedding = [0.9, 0.1, 0.0, 0.0]");
      database.command("sql", "INSERT INTO Movie SET title = 'C', embedding = [0.0, 1.0, 0.0, 0.0]");
      database.command("sql", "INSERT INTO Movie SET title = 'D', embedding = [0.0, 0.0, 1.0, 0.0]");
      database.command("sql", "INSERT INTO Movie SET title = 'E', embedding = [0.0, 0.0, 0.0, 1.0]");
    });

    // Create vector index — graph should be built immediately (default behavior)
    database.command("sql", """
        CREATE INDEX ON Movie (embedding) LSM_VECTOR
        METADATA {
          "dimensions": 4,
          "similarity": "COSINE"
        }""");

    // Query should return results immediately without any manual buildVectorGraphNow()
    try (final ResultSet rs = database.query("sql",
        "SELECT `vector.neighbors`('Movie[embedding]', ?, 3) as neighbors FROM Movie LIMIT 1",
        new float[] { 1.0f, 0.0f, 0.0f, 0.0f })) {
      assertThat(rs.hasNext()).isTrue();
      final Object neighbors = rs.next().getProperty("neighbors");
      assertThat(neighbors).isNotNull();
      @SuppressWarnings("unchecked")
      final List<Map<String, Object>> neighborList = (List<Map<String, Object>>) neighbors;
      assertThat(neighborList).isNotEmpty();
      assertThat(neighborList.size()).isLessThanOrEqualTo(3);
    }
  }

  @Test
  void buildGraphDisabledExplicitly() {
    // Insert data BEFORE creating the index
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Item");
      database.command("sql", "CREATE PROPERTY Item.embedding ARRAY_OF_FLOATS");

      database.command("sql", "INSERT INTO Item SET embedding = [1.0, 0.0, 0.0]");
      database.command("sql", "INSERT INTO Item SET embedding = [0.0, 1.0, 0.0]");
      database.command("sql", "INSERT INTO Item SET embedding = [0.0, 0.0, 1.0]");
    });

    // Create vector index with buildGraphNow: false — graph should NOT be built
    database.command("sql", """
        CREATE INDEX ON Item (embedding) LSM_VECTOR
        METADATA {
          "dimensions": 3,
          "similarity": "COSINE",
          "buildGraphNow": false
        }""");

    // Query should still eventually work (lazy build on first search),
    // but the graph was not built eagerly at CREATE INDEX time
    // We verify the index exists and is queryable
    try (final ResultSet rs = database.query("sql",
        "SELECT `vector.neighbors`('Item[embedding]', ?, 2) as neighbors FROM Item LIMIT 1",
        new float[] { 1.0f, 0.0f, 0.0f })) {
      assertThat(rs.hasNext()).isTrue();
    }
  }
}
