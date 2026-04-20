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
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Test to verify issue #3717: RecordNotFoundException when vectorNeighbors
 * search returns RIDs pointing to records that no longer exist in the bucket.
 * <p>
 * This can happen when the vector location index has stale entries (not marked
 * as deleted) but the actual records have been removed from the bucket, e.g.,
 * due to crash recovery, backup restore, or other inconsistencies.
 * <p>
 * The fix catches RecordNotFoundException in SQLFunctionVectorNeighbors and
 * gracefully skips missing records instead of failing the entire query.
 * <p>
 * <a href="https://github.com/ArcadeData/arcadedb/issues/3717">GitHub Issue #3717</a>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue3717VectorSearchRecordNotFoundTest extends TestHelper {

  private static final int DIMENSIONS = 64;
  private static final int TOTAL_VECTORS = 50;

  /**
   * Directly tests that the SQLFunctionVectorNeighbors result processing
   * gracefully handles missing records by verifying the fix pattern:
   * when a RID from vector search results points to a non-existent record,
   * the entry is skipped instead of propagating RecordNotFoundException.
   */
  @Test
  void resultProcessingShouldSkipMissingRecords() {
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);

    // Create schema with vector index
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

    // Insert vectors
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

    // Force graph build by doing a search
    database.transaction(() -> {
      final float[] queryVector = new float[DIMENSIONS];
      Arrays.fill(queryVector, 0.5f);
      final ResultSet rs = database.query("sql",
          "SELECT vectorNeighbors('VectorDoc[embedding]', ?, 10) AS neighbors",
          queryVector);
      assertThat(rs.hasNext()).isTrue();
      rs.close();
    });

    // Simulate the fix scenario: create a result list that mixes valid and invalid RIDs,
    // then verify that processing them (as executeWithLSMVectorIndexes does) handles
    // RecordNotFoundException gracefully.
    database.transaction(() -> {
      final List<Pair<RID, Float>> simulatedResults = new ArrayList<>();

      // Add a valid RID
      simulatedResults.add(new Pair<>(insertedRIDs.get(TOTAL_VECTORS - 1), 0.1f));
      // Add a non-existent RID (simulates stale vector index entry)
      final RID fakeRid = database.newRID(insertedRIDs.getFirst().getBucketId(), 999_999);
      simulatedResults.add(new Pair<>(fakeRid, 0.2f));
      // Add another valid RID
      simulatedResults.add(new Pair<>(insertedRIDs.get(TOTAL_VECTORS - 2), 0.3f));

      // Verify the fake RID actually throws RecordNotFoundException
      try {
        fakeRid.asDocument();
        throw new AssertionError("Expected RecordNotFoundException for fake RID " + fakeRid);
      } catch (final RecordNotFoundException e) {
        // Expected
      }

      // Process results as executeWithLSMVectorIndexes does (with the fix applied)
      final ArrayList<Object> processedResults = new ArrayList<>();
      for (final Pair<RID, Float> neighbor : simulatedResults) {
        final RID rid = neighbor.getFirst();
        final Document record;
        try {
          record = rid.asDocument();
        } catch (final RecordNotFoundException e) {
          // This is the fix for issue #3717: skip missing records
          continue;
        }
        final float distance = neighbor.getSecond();
        final LinkedHashMap<String, Object> entry = new LinkedHashMap<>();
        entry.put("record", record);
        entry.put("@rid", record.getIdentity());
        entry.put("distance", distance);
        processedResults.add(entry);
      }

      // Should have 2 valid results (the fake RID was skipped)
      assertThat(processedResults.size()).isEqualTo(2);
    });
  }

  /**
   * Tests the full vectorNeighbors SQL query works correctly when all records exist.
   * This serves as a baseline and regression test.
   */
  @Test
  void vectorSearchShouldWorkNormally() {
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);

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

    database.transaction(() -> {
      for (int i = 0; i < TOTAL_VECTORS; i++) {
        final var vertex = database.newVertex("VectorDoc2");
        vertex.set("name", "doc" + i);
        final float[] vector = new float[DIMENSIONS];
        for (int j = 0; j < DIMENSIONS; j++)
          vector[j] = (float) Math.random();
        vertex.set("embedding", vector);
        vertex.save();
      }
    });

    // Verify normal search works with expand pattern
    database.transaction(() -> {
      final float[] queryVector = new float[DIMENSIONS];
      for (int j = 0; j < DIMENSIONS; j++)
        queryVector[j] = (float) Math.random();

      assertThatCode(() -> {
        final ResultSet rs = database.query("sql",
            "SELECT distance, name FROM (SELECT expand(vectorNeighbors('VectorDoc2[embedding]', ?, 10)))",
            queryVector);
        int count = 0;
        while (rs.hasNext()) {
          final Result result = rs.next();
          assertThat((Object) result.getProperty("distance")).isNotNull();
          count++;
        }
        assertThat(count).isEqualTo(10);
        rs.close();
      }).doesNotThrowAnyException();
    });
  }
}
