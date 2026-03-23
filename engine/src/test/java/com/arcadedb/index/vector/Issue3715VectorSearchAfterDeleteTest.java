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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test to reproduce issue #3715: NullPointerException in GraphSearcher.search() when
 * searching a vector index that contains deleted entries.
 * <p>
 * The HNSW graph still references ordinals of deleted vectors. When JVector traverses the
 * graph during search, it calls getVector() on deleted ordinals, gets null, and throws NPE.
 * <p>
 * The bug manifests with large graphs (>=1000 vectors) where the rebuild is async and may not
 * happen before the next search. With few deletions (below the mutations threshold), no rebuild
 * is triggered at all, and the stale graph is used directly.
 * <p>
 * <a href="https://github.com/ArcadeData/arcadedb/issues/3715">GitHub Issue #3715</a>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue3715VectorSearchAfterDeleteTest extends TestHelper {

  private static final int DIMENSIONS = 64;
  // Must be >= 1000 to trigger async rebuild path (ASYNC_REBUILD_MIN_GRAPH_SIZE)
  private static final int TOTAL_VECTORS = 1500;
  // Delete a large portion to maximize chance of search traversing through deleted ordinals.
  // Even if this exceeds the mutation threshold, the async rebuild won't complete before search.
  private static final int VECTORS_TO_DELETE = 500;

  @Test
  void vectorSearchAfterDeleteShouldNotThrowNPE() {
    // Set very high mutation threshold so the graph is NOT rebuilt after deletions.
    // This forces the search to use the stale graph with edges to deleted ordinals.
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);

    // Phase 1: Create schema with vector index
    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("VectorDoc");
      type.createProperty("name", Type.STRING);
      type.createProperty("embedding", Type.ARRAY_OF_FLOATS);

      database.getSchema().buildTypeIndex("VectorDoc", new String[] { "embedding" })
          .withLSMVectorType()
          .withDimensions(DIMENSIONS)
          .withSimilarity("COSINE")
          .withMaxConnections(16)
          .withBeamWidth(100)
          .create();
    });

    // Phase 2: Insert enough vectors to exceed ASYNC_REBUILD_MIN_GRAPH_SIZE (1000)
    final List<RID> insertedRIDs = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < TOTAL_VECTORS; i++) {
        final var vertex = database.newVertex("VectorDoc");
        vertex.set("name", "doc" + i);
        final float[] vector = new float[DIMENSIONS];
        for (int j = 0; j < DIMENSIONS; j++)
          vector[j] = (float) Math.random();
        vertex.set("embedding", vector);
        vertex.save();
        insertedRIDs.add(vertex.getIdentity());
      }
    });

    // Phase 3: Force graph build by doing a search first
    database.transaction(() -> {
      final float[] queryVector = new float[DIMENSIONS];
      Arrays.fill(queryVector, 0.5f);
      final ResultSet rs = database.query("sql",
          "SELECT vectorNeighbors('VectorDoc[embedding]', ?, 10) AS neighbors",
          queryVector);
      assertThat(rs.hasNext()).isTrue();
      rs.close();
    });

    // Phase 4: Delete vectors (below the mutation threshold so NO rebuild is triggered)
    // This leaves the HNSW graph with stale edges to deleted ordinals
    database.transaction(() -> {
      for (int i = 0; i < VECTORS_TO_DELETE; i++)
        insertedRIDs.get(i).asDocument().delete();
    });

    // Phase 5: Search multiple times with different query vectors.
    // The HNSW graph still has edges to deleted ordinals. Before the fix,
    // getVector() returned null for deleted ordinals, causing NPE in JVector.
    database.transaction(() -> {
      for (int s = 0; s < 20; s++) {
        final float[] queryVector = new float[DIMENSIONS];
        for (int j = 0; j < DIMENSIONS; j++)
          queryVector[j] = (float) Math.random();
        final ResultSet rs = database.query("sql",
            "SELECT vectorNeighbors('VectorDoc[embedding]', ?, 10) AS neighbors",
            queryVector);
        assertThat(rs.hasNext()).isTrue();
        final Result result = rs.next();
        final List<?> neighbors = result.getProperty("neighbors");
        assertThat(neighbors).isNotNull();
        assertThat(neighbors.size()).isGreaterThan(0);
        assertThat(neighbors.size()).isLessThanOrEqualTo(10);
        rs.close();
      }
    });
  }

  @Test
  void vectorSearchAfterDeleteWithReopenShouldNotThrowNPE() {
    // Same scenario but with database reopen between delete and search
    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("VectorDoc2");
      type.createProperty("name", Type.STRING);
      type.createProperty("embedding", Type.ARRAY_OF_FLOATS);

      database.getSchema().buildTypeIndex("VectorDoc2", new String[] { "embedding" })
          .withLSMVectorType()
          .withDimensions(DIMENSIONS)
          .withSimilarity("COSINE")
          .withMaxConnections(16)
          .withBeamWidth(100)
          .create();
    });

    final List<RID> insertedRIDs = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < TOTAL_VECTORS; i++) {
        final var vertex = database.newVertex("VectorDoc2");
        vertex.set("name", "doc" + i);
        final float[] vector = new float[DIMENSIONS];
        for (int j = 0; j < DIMENSIONS; j++)
          vector[j] = (float) Math.random();
        vertex.set("embedding", vector);
        vertex.save();
        insertedRIDs.add(vertex.getIdentity());
      }
    });

    // Force graph build
    database.transaction(() -> {
      final float[] queryVector = new float[DIMENSIONS];
      Arrays.fill(queryVector, 0.5f);
      final ResultSet rs = database.query("sql",
          "SELECT vectorNeighbors('VectorDoc2[embedding]', ?, 10) AS neighbors",
          queryVector);
      assertThat(rs.hasNext()).isTrue();
      rs.close();
    });

    // Delete vectors
    database.transaction(() -> {
      for (int i = 0; i < VECTORS_TO_DELETE; i++)
        insertedRIDs.get(i).asDocument().delete();
    });

    // Reopen database (simulates the user's scenario where deletions persist on disk)
    reopenDatabase();

    // Search after reopen — should NOT throw NPE
    database.transaction(() -> {
      final float[] queryVector = new float[DIMENSIONS];
      Arrays.fill(queryVector, 0.5f);
      final ResultSet rs = database.query("sql",
          "SELECT vectorNeighbors('VectorDoc2[embedding]', ?, 10) AS neighbors",
          queryVector);
      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      final List<?> neighbors = result.getProperty("neighbors");
      assertThat(neighbors).isNotNull();
      assertThat(neighbors.size()).isGreaterThan(0);
      rs.close();
    });
  }
}
