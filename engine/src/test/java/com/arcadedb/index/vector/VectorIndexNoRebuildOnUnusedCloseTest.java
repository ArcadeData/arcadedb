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

import java.io.File;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A session that never searches the vector index must not rewrite its HNSW graph on close.
 * <p>
 * {@code GraphState.LOADING} means "not loaded into memory", which is not the same as "not
 * persisted on disk": the graph is lazily loaded on the first search, so a session that only
 * reads documents leaves the state at LOADING even when a complete graph is already on disk.
 * Treating that as "never built" makes close rebuild the graph from scratch to produce a file
 * that already exists, which is O(n log n) in the index size and lands on sessions that never
 * used the index at all.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/5747">issue #5747</a>
 */
class VectorIndexNoRebuildOnUnusedCloseTest extends TestHelper {

  private static final int VECTORS = 300;

  @Test
  void closingWithoutSearchingDoesNotRewriteTheGraph() throws Exception {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      for (int i = 0; i < VECTORS; i++)
        database.command("sql", "INSERT INTO Doc SET name = ?, embedding = ?", "d" + i, embedding(i));
    });

    database.command("sql", """
        CREATE INDEX ON Doc (embedding) LSM_VECTOR
        METADATA {
          "dimensions": 4,
          "similarity": "COSINE"
        }""");

    // Search once so the graph is loaded and persisted in a settled state, then close.
    assertThat(search()).isNotZero();
    reopenDatabase();

    final File graph = graphFile();
    final byte[] contentBefore = Files.readAllBytes(graph.toPath());
    final long modifiedBefore = graph.lastModified();

    // A session that reads documents and never touches the vector index. Nothing here
    // changes the index, so closing must leave the persisted graph exactly as it was.
    try (final ResultSet rs = database.query("sql", "SELECT name FROM Doc LIMIT 10")) {
      while (rs.hasNext())
        rs.next();
    }
    reopenDatabase();

    assertThat(graphFile()).exists();
    assertThat(Files.readAllBytes(graphFile().toPath()))
        .as("closing a database whose vector index was never used must not rewrite the graph")
        .isEqualTo(contentBefore);
    assertThat(graphFile().lastModified())
        .as("the persisted graph was rewritten on close by a session that never searched")
        .isEqualTo(modifiedBefore);

    // And the index is still usable afterwards.
    assertThat(search()).isNotZero();
  }

  private int search() {
    int rows = 0;
    try (final ResultSet rs = database.query("sql",
        "SELECT name FROM (SELECT expand(vectorNeighbors('Doc[embedding]', [1.0, 0.0, 0.0, 0.0], 5)))")) {
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
    }
    return rows;
  }

  private File graphFile() {
    final File[] found = new File(getDatabasePath())
        .listFiles((dir, name) -> name.contains(LSMVectorIndexGraphFile.FILE_EXT));
    assertThat(found).as("persisted graph file").isNotNull().isNotEmpty();
    return found[0];
  }

  private static float[] embedding(final int i) {
    // Spread over the unit sphere so the graph has real structure to preserve.
    final double a = i * 0.05;
    return new float[] {(float) Math.cos(a), (float) Math.sin(a), (float) (i % 7) / 7f, (float) (i % 3) / 3f};
  }
}
