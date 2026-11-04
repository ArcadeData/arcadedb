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
import com.arcadedb.schema.Type;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10786: the PQ search paired the published graph with the PQ codes in hand by COUNT only. A rebuild that keeps the
 * count but reassigns the ordinals (one delete and one insert) publishes its graph and ordinal map first and its codes only
 * once the graph is persisted, and in between every search walked the new graph through the previous build's codes: each
 * ordinal scored with the code of another vector, silently. The codes now answer only for the ordinal map they were encoded
 * with, which a search pins beside the graph.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("vector")
class Issue10786PqCodesBoundToTheirOrdinalMapTest extends TestHelper {

  private static final int DIMENSIONS = 32;
  /** Above the PQ clusters, so PQ trains, and above ASYNC_REBUILD_MIN_GRAPH_SIZE like the other PQ fixtures. */
  private static final int VECTORS    = 1100;
  private static final int PROBE      = 500;

  private final Random        random  = new Random(10786);
  private final List<RID>     rids    = new ArrayList<>();
  private final List<float[]> vectors = new ArrayList<>();

  @Test
  void aSameCountRebuildIsNotWalkedThroughThePreviousBuildsCodes() {
    final LSMVectorIndex index = createIndex();
    insert(VECTORS);
    index.buildVectorGraphNow();

    final LSMVectorIndex.PQCodes previous = index.pqCodesForTest();
    assertThat(previous).as("the fixture must have PQ codes to go stale").isNotNull();
    assertThat(index.pqCodesForPublishedGraphForTest()).as("a settled build searches with its own codes")
        .isSameAs(previous.codes());

    // The rebuild that keeps the count: the first vector goes, one comes, and every ordinal now names the next vector
    database.transaction(() -> rids.getFirst().asDocument().delete());
    insert(1);
    index.buildVectorGraphNow();
    final int[] rebuiltMap = index.getOrdinalToVectorIdForTest();
    assertThat(rebuiltMap.length).as("the point of the fixture is a map of the same size").isEqualTo(previous.codes().count());

    // Nothing may rebuild from here on, or the window would close before the search reads it
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO, 100f);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MAX_PENDING_MUTATIONS, 0);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);

    final float[] query = vectors.get(PROBE);
    final RID probe = rids.get(PROBE);

    // The control: what a count-only pairing answered. The previous codes, paired with the rebuilt map, score the probe's
    // own ordinal with its neighbour's code, so the nearest PQ code is the probe's own and the row it names is the next one
    index.setPqCodesForTest(previous.boundTo(rebuiltMap));
    assertThat(nearest(index, query)).as("codes of the previous build answer for another record (the defect)").isNotEqualTo(probe);

    // The window the issue is about: the graph and the map of the rebuild are published, the codes are still the previous
    // build's, bound to the previous map
    index.setPqCodesForTest(previous);
    assertThat(index.pqCodesForPublishedGraphForTest()).as("the previous codes cannot be had for the rebuilt map").isNull();
    assertThat(index.isPQSearchAvailable()).as("the PQ path is still the one asked, so the refusal is what is tested")
        .isTrue();
    assertThat(nearest(index, query)).as("refused, the search answers exactly").isEqualTo(probe);
  }

  @Test
  void aBuildPublishesItsCodesBoundToItsOwnMap() {
    final LSMVectorIndex index = createIndex();
    insert(VECTORS);
    index.buildVectorGraphNow();

    final LSMVectorIndex.PQCodes codes = index.pqCodesForTest();
    assertThat(codes.ordinalMap()).isSameAs(index.getOrdinalToVectorIdForTest());
    assertThat(nearest(index, vectors.get(PROBE))).isEqualTo(rids.get(PROBE));
  }

  @Test
  void aReopenAdoptsTheCodesForTheGraphItPublishes() {
    createIndex();
    insert(VECTORS);
    indexOf().buildVectorGraphNow();
    assertThat(indexOf().pqCodesForPublishedGraphForTest()).isNotNull();

    reopenDatabase();

    final LSMVectorIndex reopened = indexOf();
    // The graph is loaded by the first search; the codes read from the file are adopted for it there
    assertThat(nearest(reopened, vectors.get(PROBE))).isEqualTo(rids.get(PROBE));
    assertThat(reopened.pqCodesForPublishedGraphForTest())
        .as("codes recorded for the persisted graph must serve it after a reopen, or PQ search is silently off").isNotNull();
    assertThat(reopened.pqCodesForTest().ordinalMap()).isSameAs(reopened.getOrdinalToVectorIdForTest());
  }

  private RID nearest(final LSMVectorIndex index, final float[] query) {
    final List<Pair<RID, Float>> rows = index.findNeighborsFromVectorApproximate(query, 10, -1, 0);
    assertThat(rows).isNotEmpty();
    return rows.getFirst().getFirst();
  }

  private LSMVectorIndex createIndex() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Embedding").createProperty("vector", Type.ARRAY_OF_FLOATS);
      database.command("sql", """
          CREATE INDEX ON Embedding (vector) LSM_VECTOR
          METADATA { "dimensions": %d, "similarity": "EUCLIDEAN", "quantization": "PRODUCT", "pqClusters": 32 }"""
          .formatted(DIMENSIONS));
    });
    return indexOf();
  }

  private LSMVectorIndex indexOf() {
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Embedding[vector]");
    return (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
  }

  private void insert(final int count) {
    database.transaction(() -> {
      for (int i = 0; i < count; i++) {
        final float[] vector = randomUnitVector();
        vectors.add(vector);
        rids.add(database.newVertex("Embedding").set("vector", vector).save().getIdentity());
      }
    });
  }

  private float[] randomUnitVector() {
    final float[] vector = new float[DIMENSIONS];
    float norm = 0;
    for (int i = 0; i < DIMENSIONS; i++) {
      vector[i] = random.nextFloat() * 2 - 1;
      norm += vector[i] * vector[i];
    }
    norm = (float) Math.sqrt(norm);
    for (int i = 0; i < DIMENSIONS; i++)
      vector[i] /= norm;
    return vector;
  }
}
