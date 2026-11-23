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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A search that scores a vector the search cache does not hold reads its quantized row from the index page. The answer must be
 * the one a resident vector gives, whichever way the cache is sized (all resident, far smaller than the index so almost every
 * candidate misses, or off), and the miss must not allocate a vector per candidate: with INT8 the row is dequantized into
 * a buffer the search reuses.
 */
class SearchVectorMissPathTest {
  private static final String DB_PATH     = "target/test-databases/SearchVectorMissPathTest";
  private static final int    DIMENSIONS  = 512;
  private static final int    NUM_VECTORS = 2_000;
  private static final int    QUERIES     = 25;
  private static final int    K           = 10;
  private static final int    EF_SEARCH   = 100;

  @BeforeEach
  @AfterEach
  void cleanup() {
    GlobalConfiguration.VECTOR_INDEX_SEARCH_CACHE_SIZE.reset();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  private record Run(List<List<Pair<RID, Float>>> answers, long fetches, long bytesPerFetch) {
  }

  @Test
  void theAnswerDoesNotDependOnTheSizeOfTheSearchCache() {
    final float[][] vectors = createIndex();

    final Run resident = searchWith(0, vectors);
    final Run missing = searchWith(100, vectors);
    final Run off = searchWith(-1, vectors);

    assertThat(resident.answers().getFirst()).hasSize(K);
    assertThat(missing.answers()).isEqualTo(resident.answers());
    assertThat(off.answers()).isEqualTo(resident.answers());
    assertThat(missing.fetches()).as("a cache of 100 vectors misses on most candidates").isGreaterThan(QUERIES * 50L);
  }

  @Test
  void aMissDoesNotAllocateAVectorPerCandidate() {
    final float[][] vectors = createIndex();

    // 100 slots for 2,000 vectors: nearly every candidate is read from its page. The row was dequantized into a new float[512]
    // (2 KB), after a new byte[512], and wrapped for every one of them
    final Run missing = searchWith(100, vectors);
    assertThat(missing.fetches()).isGreaterThan(QUERIES * 50L);
    assertThat(missing.bytesPerFetch()).as("bytes allocated per vector read from its page").isLessThan(1_000);
  }

  private float[][] createIndex() {
    final Random rnd = new Random(77);
    final float[][] vectors = new float[NUM_VECTORS][DIMENSIONS];
    for (int i = 0; i < NUM_VECTORS; i++)
      for (int d = 0; d < DIMENSIONS; d++)
        vectors[i][d] = (float) rnd.nextGaussian();

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      try (final Database db = factory.create()) {
        db.transaction(() -> {
          final var type = db.getSchema().createDocumentType("Doc");
          type.createProperty("id", Type.INTEGER);
          type.createProperty("vector", Type.ARRAY_OF_FLOATS);
          db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
              + ", \"similarity\": \"COSINE\", \"quantization\": \"INT8\" }");
        });
        db.begin();
        for (int i = 0; i < NUM_VECTORS; i++) {
          db.newDocument("Doc").set("id", i).set("vector", vectors[i]).save();
          if (i % 500 == 499) {
            db.commit();
            db.begin();
          }
        }
        db.commit();
      }
    }
    return vectors;
  }

  private Run searchWith(final int searchCacheSize, final float[][] vectors) {
    GlobalConfiguration.VECTOR_INDEX_SEARCH_CACHE_SIZE.setValue(searchCacheSize);
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      try (final Database db = factory.open()) {
        final LSMVectorIndex index = (LSMVectorIndex) db.getSchema().getType("Doc")
            .getPolymorphicIndexByProperties("vector").getIndexesOnBuckets()[0];
        // THE FIRST SEARCH LOADS OR BUILDS THE GRAPH; THE NEXT ONES WARM THE CODE
        for (int i = 0; i < 10; i++)
          index.findNeighborsFromVector(vectors[1000 + i], K, EF_SEARCH);

        final List<List<Pair<RID, Float>>> answers = new ArrayList<>();
        final long fetchesBefore = fetches(index);
        final com.sun.management.ThreadMXBean mx = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        final long bytesBefore = mx.getCurrentThreadAllocatedBytes();
        for (int i = 0; i < QUERIES; i++)
          answers.add(index.findNeighborsFromVector(vectors[i * 7], K, EF_SEARCH));
        final long bytes = mx.getCurrentThreadAllocatedBytes() - bytesBefore;
        final long fetches = fetches(index) - fetchesBefore;
        return new Run(answers, fetches, fetches == 0 ? 0 : bytes / fetches);
      }
    }
  }

  private static long fetches(final LSMVectorIndex index) {
    final Map<String, Long> stats = index.getStats();
    return stats.getOrDefault("vectorFetchFromQuantized", 0L);
  }
}
