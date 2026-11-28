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
import com.arcadedb.index.TypeIndex;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The graph build of an INT8 COSINE index (what modelhub creates) leaves out the records it cannot score - the zero vector, one
 * whose squared norm the float cosine cannot hold - whichever way it holds the corpus: as float vectors in its build cache, or, when the
 * cache is smaller than the corpus (issue #10543), as the quantized rows the pages store, reconstructed to be checked. The write path
 * accepts such a record, so the build is the only place that refuses it. The oracle is the answer of a search that asks for more rows
 * than the index holds: the scorable records, each once, and none of the others (an index whose graph skipped every record would
 * answer the same, from its buffer, so the graph's node count is the oracle of what the build kept).
 */
@Tag("vector")
class VectorQuantizedBuildUnscorableOracleTest extends TestHelper {
  private static final int DIMENSIONS = 8;
  private static final int SCORABLE   = 1200;

  /** A build cache of 0 asks for the engine's own size (every vector fits); the others hold part or little of the corpus. */
  @ParameterizedTest
  @ValueSource(ints = { 0, 700, 100 })
  void aBuildLeavesOutTheRecordsItCannotScoreWhateverItsCacheHolds(final int buildCacheSize) {
    if (buildCacheSize > 0)
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE, buildCacheSize);
    final String type = "Q" + buildCacheSize;
    database.command("sql", "CREATE DOCUMENT TYPE " + type + " BUCKETS 1");
    database.command("sql", "CREATE PROPERTY " + type + ".embedding ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX ON " + type + " (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
        + ", \"similarity\": \"COSINE\", \"quantization\": \"INT8\" }");

    final Random random = new Random(21);
    final Set<RID> scorable = new HashSet<>();
    final Set<RID> unscorable = new HashSet<>();
    database.transaction(() -> {
      for (int i = 0; i < SCORABLE; i++) {
        final float[] vector = new float[DIMENSIONS];
        for (int d = 0; d < DIMENSIONS; d++)
          vector[d] = (float) random.nextGaussian();
        vector[0] += 0.5f;
        scorable.add(save(type, vector));
        // spread them through the corpus, so that some fall in the part the store holds and some outside it
        if (i % 300 == 150)
          unscorable.add(save(type, new float[DIMENSIONS]));
        if (i % 400 == 200)
          unscorable.add(save(type, filled(1e-30f)));
        if (i % 600 == 300)
          unscorable.add(save(type, filled(1e30f)));
      }
    });
    assertThat(unscorable).hasSize(4 + 3 + 2);

    final LSMVectorIndex index = (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName(type + "[embedding]"))
        .getIndexesOnBuckets()[0];
    index.buildVectorGraphNow();
    assertThat(index.getStats().get("graphNodeCount")).as("the graph holds the scorable records and no other").isEqualTo(SCORABLE);

    final float[] query = new float[DIMENSIONS];
    Arrays.fill(query, 1f);
    final List<RID> answer = new ArrayList<>();
    for (final Pair<RID, Float> hit : index.findNeighborsFromVector(query, SCORABLE + 50, SCORABLE + 50))
      answer.add(hit.getFirst());

    assertThat(answer).as("no record that cannot be scored is answered").doesNotContainAnyElementsOf(unscorable);
    assertThat(answer).as("each scorable record once").doesNotHaveDuplicates().containsExactlyInAnyOrderElementsOf(scorable);
  }

  private RID save(final String type, final float[] vector) {
    return database.newDocument(type).set("embedding", vector).save().getIdentity();
  }

  private static float[] filled(final float value) {
    final float[] vector = new float[DIMENSIONS];
    Arrays.fill(vector, value);
    return vector;
  }
}
