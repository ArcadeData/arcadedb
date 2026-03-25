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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for PRODUCT quantization search returning too few results after database reopen.
 * The root cause was FusedPQ approximate scoring producing incorrect results when PQ codes
 * were persisted through ArcadeDB's page-based storage, causing beam search to not expand
 * beyond the entry node.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PQSearchDebugTest {
  private static final String DB_PATH = "target/test-databases/PQSearchDebugTest";
  private static final int NUM_VECTORS = 10_000;
  private static final int DIMENSIONS = 128;
  private static final int K = 10;
  private static final int EF_SEARCH = 50;

  @Test
  void productQuantizationSearchShouldReturnKResults() {
    testPQConfig(true);
  }

  @Test
  void productQuantizationWithoutInlineVectorsShouldReturnKResults() {
    testPQConfig(false);
  }

  private void testPQConfig(final boolean storeVectorsInGraph) {
    FileUtils.deleteRecursively(new File(DB_PATH));
    GlobalConfiguration.PROFILE.setValue("high-performance");

    final Random rng = new Random(42L);
    final float[][] dataVectors = generateVectors(NUM_VECTORS, DIMENSIONS, rng);
    final float[] queryVector = generateVectors(1, DIMENSIONS, rng)[0];

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      // Phase 1: Create, populate, and verify search works before close
      try (final Database db = factory.create()) {
        final String storeInGraph = storeVectorsInGraph ? ", \"storeVectorsInGraph\": true" : "";
        db.transaction(() -> {
          final var type = db.getSchema().createDocumentType("Vec");
          type.createProperty("id", Type.INTEGER);
          type.createProperty("embedding", Type.ARRAY_OF_FLOATS);
          db.command("sql",
              "CREATE INDEX ON Vec (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
                  + ", \"similarity\": \"COSINE\", \"quantization\": \"PRODUCT\"" + storeInGraph + " }");
        });

        int inserted = 0;
        db.begin();
        for (int i = 0; i < NUM_VECTORS; i++) {
          db.newDocument("Vec").set("id", i).set("embedding", dataVectors[i]).save();
          inserted++;
          if (inserted % 5000 == 0) {
            db.commit();
            db.begin();
          }
        }
        if (db.isTransactionActive())
          db.commit();

        final LSMVectorIndex indexBeforeClose = (LSMVectorIndex) db.getSchema().getType("Vec")
            .getPolymorphicIndexByProperties("embedding").getIndexesOnBuckets()[0];
        final List<Pair<RID, Float>> resultsBeforeClose = indexBeforeClose.findNeighborsFromVector(queryVector, K, EF_SEARCH);
        assertThat(resultsBeforeClose.size()).as("Results before close should be K").isEqualTo(K);
      }

      // Phase 2: Reopen and verify search still returns K results
      try (final Database db = factory.open()) {
        final LSMVectorIndex index = (LSMVectorIndex) db.getSchema().getType("Vec")
            .getPolymorphicIndexByProperties("embedding").getIndexesOnBuckets()[0];

        final List<Pair<RID, Float>> results = index.findNeighborsFromVector(queryVector, K, EF_SEARCH);
        assertThat(results.size()).as("Results after reopen should be K").isEqualTo(K);
      }
    }
  }

  private float[][] generateVectors(final int count, final int dims, final Random rng) {
    final float[][] vectors = new float[count][dims];
    for (int i = 0; i < count; i++) {
      float norm = 0;
      for (int d = 0; d < dims; d++) {
        vectors[i][d] = (float) rng.nextGaussian();
        norm += vectors[i][d] * vectors[i][d];
      }
      norm = (float) Math.sqrt(norm);
      for (int d = 0; d < dims; d++)
        vectors[i][d] /= norm;
    }
    return vectors;
  }
}
