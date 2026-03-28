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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test to reproduce issue #3722 (reopened): vectorNeighbors returns fewer results than expected
 * when deletion tombstones exist in vector index pages.
 * <p>
 * Root cause: {@code persistDeletionTombstones()} does not write the quantization type byte,
 * but {@code LSMVectorIndexPageParser.parsePages()} always calls {@code skipQuantizationData()}
 * which reads at least 1 byte. This misalignment corrupts parsing of all entries after a tombstone
 * on the same page, causing those vectors to be silently lost.
 * <p>
 * Scenario: insert N vectors, delete one (creating a tombstone), add more vectors.
 * After reopen, the page parser can't read entries after the tombstone, so the graph
 * is built with only the vectors before the tombstone.
 * <p>
 * <a href="https://github.com/ArcadeData/arcadedb/issues/3722">GitHub Issue #3722</a>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue3722VectorTombstoneCorruptionTest extends TestHelper {

  private static final int DIMENSIONS = 64;
  private static final int INITIAL_VECTORS = 66;
  private static final int ADDITIONAL_VECTORS = 480;
  private static final int TOTAL_VECTORS = INITIAL_VECTORS + ADDITIONAL_VECTORS;

  @Test
  void vectorNeighborsShouldReturnAllVectorsAfterDeleteAndReopen() {
    // High mutation threshold so the graph is NOT auto-rebuilt
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    // Disable inactivity timer
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);

    // Phase 1: Create schema with vector index
    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("ImageEmbedding");
      type.createProperty("name", Type.STRING);
      type.createProperty("vector", Type.ARRAY_OF_FLOATS);

      database.getSchema().buildTypeIndex("ImageEmbedding", new String[] { "vector" })
          .withLSMVectorType()
          .withDimensions(DIMENSIONS)
          .withSimilarity("COSINE")
          .withMaxConnections(16)
          .withBeamWidth(100)
          .create();
    });

    // Phase 2: Insert initial batch of vectors
    final RID[] rids = new RID[TOTAL_VECTORS];
    database.transaction(() -> {
      for (int i = 0; i < INITIAL_VECTORS; i++) {
        final MutableVertex vertex = database.newVertex("ImageEmbedding");
        vertex.set("name", "img" + i);
        vertex.set("vector", createDeterministicVector(i));
        vertex.save();
        rids[i] = vertex.getIdentity();
      }
    });

    // Phase 3: Force graph build and persist
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("ImageEmbedding[vector]");
    final LSMVectorIndex index = (LSMVectorIndex) typeIndex.getSubIndexes().iterator().next();
    index.buildVectorGraphNow();

    // Phase 4: Delete one vector (this writes a tombstone WITHOUT quantization type byte)
    database.transaction(() -> {
      final var vertex = rids[0].asVertex();
      vertex.delete();
    });

    // Phase 5: Add many more vectors (these go AFTER the tombstone on the same page)
    database.transaction(() -> {
      for (int i = INITIAL_VECTORS; i < TOTAL_VECTORS; i++) {
        final MutableVertex vertex = database.newVertex("ImageEmbedding");
        vertex.set("name", "img" + i);
        vertex.set("vector", createDeterministicVector(i));
        vertex.save();
        rids[i] = vertex.getIdentity();
      }
    });

    // Phase 6: Reopen database (forces reload from pages)
    reopenDatabase();
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);

    // Phase 7: Verify that all vectors are found
    database.transaction(() -> {
      final TypeIndex ti = (TypeIndex) database.getSchema().getIndexByName("ImageEmbedding[vector]");
      final LSMVectorIndex idx = (LSMVectorIndex) ti.getSubIndexes().iterator().next();
      final Map<String, Long> stats = idx.getStats();
      final long totalVectors = stats.get("totalVectors");
      final long activeVectors = stats.get("activeVectors");

      // vectorIndex should have loaded all vectors from pages (545 active = 546 - 1 deleted)
      assertThat(activeVectors)
          .as("All %d active vectors should be loaded from pages after reopen (initial %d - 1 deleted + %d additional)",
              TOTAL_VECTORS - 1, INITIAL_VECTORS, ADDITIONAL_VECTORS)
          .isEqualTo(TOTAL_VECTORS - 1);

      // Search for neighbors - should find most of the vectors
      final float[] queryVector = new float[DIMENSIONS];
      Arrays.fill(queryVector, 0.5f);
      final int k = TOTAL_VECTORS;
      final ResultSet rs = database.query("sql",
          "SELECT vectorNeighbors('ImageEmbedding[vector]', ?, ?) AS neighbors",
          queryVector, k);

      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      final List<?> neighbors = result.getProperty("neighbors");
      assertThat(neighbors).isNotNull();

      // After fix: should find close to TOTAL_VECTORS - 1 (all active vectors)
      // Before fix: would find only ~INITIAL_VECTORS - 1 (entries before the tombstone)
      assertThat(neighbors.size())
          .as("vectorNeighbors should return vectors from BOTH batches, not just the %d before the tombstone",
              INITIAL_VECTORS - 1)
          .isGreaterThan(TOTAL_VECTORS / 2);

      rs.close();
    });
  }

  @Test
  void vectorNeighborsShouldReturnAllVectorsAfterUpdateAndReopen() {
    // High mutation threshold so the graph is NOT auto-rebuilt
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);

    // Phase 1: Create schema with vector index
    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("ImageEmbedding");
      type.createProperty("name", Type.STRING);
      type.createProperty("vector", Type.ARRAY_OF_FLOATS);

      database.getSchema().buildTypeIndex("ImageEmbedding", new String[] { "vector" })
          .withLSMVectorType()
          .withDimensions(DIMENSIONS)
          .withSimilarity("COSINE")
          .withMaxConnections(16)
          .withBeamWidth(100)
          .create();
    });

    // Phase 2: Insert all vectors
    final RID[] rids = new RID[TOTAL_VECTORS];
    database.transaction(() -> {
      for (int i = 0; i < TOTAL_VECTORS; i++) {
        final MutableVertex vertex = database.newVertex("ImageEmbedding");
        vertex.set("name", "img" + i);
        vertex.set("vector", createDeterministicVector(i));
        vertex.save();
        rids[i] = vertex.getIdentity();
      }
    });

    // Phase 3: Force graph build and persist
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("ImageEmbedding[vector]");
    final LSMVectorIndex index = (LSMVectorIndex) typeIndex.getSubIndexes().iterator().next();
    index.buildVectorGraphNow();

    // Phase 4: Update a few vectors (triggers delete + re-insert in the index, writing tombstones)
    database.transaction(() -> {
      for (int i = 0; i < 5; i++) {
        final var vertex = rids[i].asVertex().modify();
        vertex.set("vector", createDeterministicVector(TOTAL_VECTORS + i));
        vertex.save();
      }
    });

    // Phase 5: Reopen database
    reopenDatabase();
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);

    // Phase 6: Verify that all vectors are found
    database.transaction(() -> {
      // Search for neighbors
      final float[] queryVector = new float[DIMENSIONS];
      Arrays.fill(queryVector, 0.5f);
      final int k = TOTAL_VECTORS;
      final ResultSet rs = database.query("sql",
          "SELECT vectorNeighbors('ImageEmbedding[vector]', ?, ?) AS neighbors",
          queryVector, k);

      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      final List<?> neighbors = result.getProperty("neighbors");
      assertThat(neighbors).isNotNull();

      // After fix: should find close to TOTAL_VECTORS (all vectors, some updated)
      // Before fix: would find far fewer due to corrupted page parsing
      assertThat(neighbors.size())
          .as("vectorNeighbors should return all %d vectors after update and reopen", TOTAL_VECTORS)
          .isGreaterThan(TOTAL_VECTORS / 2);

      rs.close();
    });
  }

  @Test
  void tombstoneShouldNotCorruptPageParsing() {
    // This test directly verifies that deletion tombstones don't corrupt page parsing.
    // It inserts vectors, deletes one (creating a tombstone), inserts more, then
    // directly parses the pages and checks all entries are found.
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);

    // Create schema
    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("Vec");
      type.createProperty("name", Type.STRING);
      type.createProperty("vector", Type.ARRAY_OF_FLOATS);

      database.getSchema().buildTypeIndex("Vec", new String[] { "vector" })
          .withLSMVectorType()
          .withDimensions(DIMENSIONS)
          .withSimilarity("COSINE")
          .withMaxConnections(16)
          .withBeamWidth(100)
          .create();
    });

    // Insert 10 vectors
    final RID[] rids = new RID[10];
    database.transaction(() -> {
      for (int i = 0; i < 10; i++) {
        final MutableVertex vertex = database.newVertex("Vec");
        vertex.set("name", "v" + i);
        vertex.set("vector", createDeterministicVector(i));
        vertex.save();
        rids[i] = vertex.getIdentity();
      }
    });

    // Delete vector 5 (creates a tombstone in the page)
    database.transaction(() -> rids[5].asVertex().delete());

    // Insert 10 more vectors
    database.transaction(() -> {
      for (int i = 10; i < 20; i++) {
        final MutableVertex vertex = database.newVertex("Vec");
        vertex.set("name", "v" + i);
        vertex.set("vector", createDeterministicVector(i));
        vertex.save();
      }
    });

    // Now directly parse pages and count entries
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Vec[vector]");
    final LSMVectorIndex idx = (LSMVectorIndex) typeIndex.getSubIndexes().iterator().next();

    // Parse all entries from pages
    final java.util.List<LSMVectorIndexPageParser.VectorEntry> allEntries =
        LSMVectorIndexPageParser.parseAllEntries(
            (com.arcadedb.database.DatabaseInternal) database,
            idx.getFileId(),
            idx.getTotalPages(),
            idx.getPageSize(),
            false);

    // Should have 21 entries: 10 initial + 1 tombstone + 10 additional
    // The tombstone must be parseable without corrupting subsequent entries
    int regularCount = 0;
    int tombstoneCount = 0;
    for (final LSMVectorIndexPageParser.VectorEntry entry : allEntries) {
      if (entry.deleted)
        tombstoneCount++;
      else
        regularCount++;
    }

    assertThat(tombstoneCount)
        .as("Should have 1 tombstone entry")
        .isEqualTo(1);

    assertThat(regularCount)
        .as("Should have 20 regular entries (10 initial + 10 additional)")
        .isEqualTo(20);

    assertThat(allEntries.size())
        .as("Total entries should be 21 (20 regular + 1 tombstone)")
        .isEqualTo(21);

    // Verify that vectorIds are correct (not corrupted by tombstone parsing)
    final java.util.Set<Integer> expectedRegularIds = new java.util.HashSet<>();
    for (int i = 0; i < 20; i++)
      expectedRegularIds.add(i);
    // After delete of vectorId 5, vectorIndex would re-number, but in pages the original IDs persist
    // vectorIds are assigned sequentially: 0-9 for first batch, then 10-19 for second batch
    // tombstone also has vectorId=5

    final java.util.Set<Integer> foundRegularIds = new java.util.HashSet<>();
    final java.util.Set<Integer> foundTombstoneIds = new java.util.HashSet<>();
    for (final LSMVectorIndexPageParser.VectorEntry entry : allEntries) {
      if (entry.deleted)
        foundTombstoneIds.add(entry.vectorId);
      else
        foundRegularIds.add(entry.vectorId);
    }

    // Verify that exactly 1 tombstone exists and 20 regular entries exist.
    // VectorIds are assigned during commit (sorted by ComparableVector, not insertion order),
    // so we can't predict exact IDs but can verify structural integrity.
    assertThat(foundTombstoneIds)
        .as("Exactly one tombstone should exist (found: %s)", foundTombstoneIds)
        .hasSize(1);

    // The tombstone's vectorId must be in the range [0, 9] (from the first batch of 10)
    final int tombstoneVid = foundTombstoneIds.iterator().next();
    assertThat(tombstoneVid)
        .as("Tombstone vectorId should be in range 0-9")
        .isBetween(0, 9);

    // All 20 regular vectorIds (0-9 from first batch, 10-19 from second batch) should be present
    // VectorIds are assigned sequentially: 0-9 for first commit, 10-19 for second commit
    // The tombstone overrides one ID from the first batch with deleted=true
    assertThat(foundRegularIds)
        .as("Should have 20 regular vectorIds from both batches (0-9 minus tombstone, plus 10-19).\n" +
            "Found regular IDs: %s", foundRegularIds)
        .hasSize(20);

    // All regular IDs should be valid (in range 0-19, no garbage IDs from corrupted parsing)
    for (final int vid : foundRegularIds) {
      assertThat(vid)
          .as("Regular vectorId %d should be in valid range [0, 19]", vid)
          .isBetween(0, 19);
    }
  }

  private float[] createDeterministicVector(final int index) {
    final float[] vector = new float[DIMENSIONS];
    for (int j = 0; j < DIMENSIONS; j++)
      vector[j] = (float) Math.sin(index * 0.1 + j * 0.3) * 0.5f + 0.5f;
    return vector;
  }
}
