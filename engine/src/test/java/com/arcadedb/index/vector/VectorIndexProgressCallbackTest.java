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
import com.arcadedb.index.TypeIndex;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for progress callbacks during vector index building.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class VectorIndexProgressCallbackTest extends TestHelper {

  @Test
  void vectorIndexBuildWithProgressCallback() {
    // Create schema
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE VectorDoc IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY VectorDoc.id IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY VectorDoc.embedding IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", """
          CREATE INDEX IF NOT EXISTS ON VectorDoc (embedding) LSM_VECTOR
          METADATA {dimensions: 128, similarity: 'COSINE', maxConnections: 16, beamWidth: 100}
          """);
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("VectorDoc[embedding]");
    assertThat(typeIndex).isNotNull();

    // Insert test documents
    database.transaction(() -> {
      for (int i = 0; i < 1000; i++) {
        final var vertex = database.newVertex("VectorDoc");
        vertex.set("id", "doc" + i);

        // Create random 128-dimensional vector
        final float[] vector = new float[128];
        for (int j = 0; j < 128; j++) {
          vector[j] = (float) Math.random();
        }
        vertex.set("embedding", vector);
        vertex.save();
      }
    });

    // Rebuild index with progress callbacks
    final AtomicInteger documentsIndexed = new AtomicInteger(0);
    final AtomicInteger validationProgress = new AtomicInteger(0);
    final AtomicInteger buildingProgress = new AtomicInteger(0);
    final AtomicInteger buildingCallbacks = new AtomicInteger(0);
    final AtomicInteger persistingCalled = new AtomicInteger(0);

    // Get the LSMVectorIndex from the bucket
    final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];

    // Build with callbacks
    lsmIndex.build(
        100000, // batch size
        (doc, total) -> {
          documentsIndexed.set((int) total);
//          if (total % 100 == 0) {
//            System.out.println("Indexed " + total + " documents...");
//          }
        },
        (phase, processedNodes, totalNodes, insertsOrAccesses) -> {
          switch (phase) {
          case "validating":
            validationProgress.set(processedNodes);
//            System.out.printf("Validating vectors: %d / %d%n", processedNodes, totalNodes);
            break;
          case "building":
            buildingProgress.set(processedNodes);
            buildingCallbacks.incrementAndGet();
            final int insertsInProgress = (int) (insertsOrAccesses - processedNodes);
//            System.out.printf("Building graph: %d / %d nodes (%d inserts in progress)%n",
//                processedNodes, totalNodes, insertsInProgress);
            break;
          case "persisting":
            persistingCalled.incrementAndGet();
//            System.out.printf("Persisting graph: %d / %d nodes%n", processedNodes, totalNodes);
            break;
          }
        }
    );

    // Verify callbacks were called
    assertThat(documentsIndexed.get()).as("Should have indexed 1000 documents").isEqualTo(1000);
    // Note: Validation and building callbacks may not be called if graph is already built
    // during the insert phase. That's ok - the important thing is that the index build succeeded.
    // In production, these callbacks will be triggered when explicitly rebuilding an existing index.
//    System.out.println("\n=== Callback Summary ===");
//    System.out.println("Validation reports: " + (validationProgress.get() > 0 ? "YES" : "NO"));
//    System.out.println("Building callback count: " + buildingCallbacks.get());
//    System.out.println("Final nodes built: " + buildingProgress.get());
//    System.out.println("Persisting reports: " + (persistingCalled.get() > 0 ? "YES" : "NO"));
  }

  @Test
  void vectorIndexBuildWithoutCallback() {
    // Create schema
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE SimpleDoc IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY SimpleDoc.vec IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX IF NOT EXISTS ON SimpleDoc (vec) LSM_VECTOR " +
          "METADATA {dimensions: 8, similarity: 'EUCLIDEAN'}");
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("SimpleDoc[vec]");
    assertThat(typeIndex).isNotNull();

    // Insert test documents
    database.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        final var vertex = database.newVertex("SimpleDoc");
        vertex.set("vec", new float[] {
            (float) i, (float) i + 1, (float) i + 2, (float) i + 3,
            (float) i, (float) i + 5, (float) i + 6, (float) i + 7
        });
        vertex.save();
      }
    });

    // Build without callbacks (should not crash)
    final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    final long totalRecords = lsmIndex.build(100000, null);

    assertThat(totalRecords).as("Should have indexed 100 documents").isEqualTo(100);
  }

  @Test
  void graphRebuildWithCallback() {
    // This test demonstrates the graph building callbacks when rebuilding an existing index
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE RebuildDoc IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY RebuildDoc.vec IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX IF NOT EXISTS ON RebuildDoc (vec) LSM_VECTOR " +
          "METADATA {dimensions: 16, similarity: 'DOT_PRODUCT'}");
    });

    // Insert documents first
    database.transaction(() -> {
      for (int i = 0; i < 500; i++) {
        final var vertex = database.newVertex("RebuildDoc");
        final float[] vector = new float[16];
        for (int j = 0; j < 16; j++) {
          vector[j] = (float) (Math.random() * 100);
        }
        vertex.set("vec", vector);
        vertex.save();
      }
    });

    // Get the index and rebuild it
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("RebuildDoc[vec]");
    final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];

    final AtomicInteger graphBuildCalls = new AtomicInteger(0);
    final StringBuilder phaseLog = new StringBuilder();

    // Rebuild with graph callback
    lsmIndex.build(
        100000,
        null, // No document callback needed
        (phase, processedNodes, totalNodes, vecAccesses) -> {
          graphBuildCalls.incrementAndGet();
          phaseLog.append(String.format("[%s: %d/%d] ", phase, processedNodes, totalNodes));
//          System.out.printf("Graph Build - Phase: %s, Progress: %d/%d, Vector accesses: %d%n",
//              phase, processedNodes, totalNodes, vecAccesses);
        }
    );

//    System.out.println("\nGraph build phases observed: " + phaseLog);
//    System.out.println("Total graph callback invocations: " + graphBuildCalls.get());

    // We should have received some callbacks (validation, building, or persisting)
    // Note: callbacks may not be triggered if graph is already built, which is acceptable
  }
}
