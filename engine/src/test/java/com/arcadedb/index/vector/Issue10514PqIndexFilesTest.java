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
import io.github.jbellis.jvector.graph.ImmutableGraphIndex;
import io.github.jbellis.jvector.graph.ListRandomAccessVectorValues;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.disk.feature.FeatureId;
import io.github.jbellis.jvector.quantization.PQVectors;
import io.github.jbellis.jvector.quantization.ProductQuantization;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10514, the index files and the training set of a {@code PRODUCT} LSM_VECTOR index.
 * <ul>
 *   <li>The persisted graph stored the PQ codes of all neighbours inline in every node (jvector's {@code FusedPQ}
 *   feature) although no search reads them: the graph file of a PRODUCT index was several times the size of the file
 *   of the same data without quantization.</li>
 *   <li>{@code pqTrainingLimit} was read by nothing but a log line that claimed the codebook had been trained on
 *   that many vectors.</li>
 * </ul>
 */
@Tag("vector")
class Issue10514PqIndexFilesTest extends TestHelper {
  private static final int DIMENSIONS = 64;
  private static final int VECTORS    = 3000;

  @Test
  void aProductGraphFileIsAboutAsLargeAsTheGraphFileWithoutQuantization() {
    createIndex("DocNone", "");
    createIndex("DocPq", "\"quantization\": \"PRODUCT\", ");
    insert("DocNone", VECTORS);
    insert("DocPq", VECTORS);

    final LSMVectorIndex none = indexOf("DocNone");
    final LSMVectorIndex pq = indexOf("DocPq");
    none.buildVectorGraphNow();
    pq.buildVectorGraphNow();
    assertThat(pq.isPQSearchAvailable()).as("the scenario needs a trained codebook").isTrue();

    final long noneBytes = none.getGraphFile().getLastWrittenGraphBytes();
    final long pqBytes = pq.getGraphFile().getLastWrittenGraphBytes();
    assertThat(noneBytes).isGreaterThan(0L);
    assertThat(pqBytes)
        .as("the PQ codes live in the .vecpq file; the graph file must not repeat them (none=%d, product=%d bytes)",
            noneBytes, pqBytes)
        .isLessThan((long) (noneBytes * 1.3));
  }

  @Test
  void aProductGraphIsWrittenWithoutTheFusedPqFeature() throws Exception {
    createIndex("DocPq", "\"quantization\": \"PRODUCT\", ");
    insert("DocPq", VECTORS);
    final LSMVectorIndex index = indexOf("DocPq");
    index.buildVectorGraphNow();
    assertThat(index.isPQSearchAvailable()).isTrue();

    try (final OnDiskGraphIndex graph = index.getGraphFile().loadGraph()) {
      assertThat(graph.getFeatureSet()).doesNotContain(FeatureId.FUSED_PQ);
    }
  }

  @Test
  void anIndexWrittenWithTheFusedPqFeatureStillOpensAndAnswers() throws Exception {
    createIndex("DocPq", "\"quantization\": \"PRODUCT\", ");
    insert("DocPq", VECTORS);
    final LSMVectorIndex index = indexOf("DocPq");
    index.buildVectorGraphNow();

    final ProductQuantization pq = (ProductQuantization) field("productQuantization").get(index);
    final PQVectors pqVectors = (PQVectors) field("pqVectors").get(index);
    final ImmutableGraphIndex graph = (ImmutableGraphIndex) field("graphIndex").get(index);
    assertThat(pq).isNotNull();
    assertThat(pqVectors).isNotNull();
    assertThat(graph).isNotNull();

    // Rewrite the graph file the way releases before this fix wrote it: with the FusedPQ feature.
    final LSMVectorIndexGraphFile graphFile = index.getGraphFile();
    database.transaction(() -> graphFile.writeGraph(graph, new ListRandomAccessVectorValues(List.<VectorFloat<?>>of(), DIMENSIONS),
        0, null, pq, pqVectors, true));
    try (final OnDiskGraphIndex old = graphFile.loadGraph()) {
      assertThat(old.getFeatureSet()).contains(FeatureId.FUSED_PQ);
      assertThat(old.getIdUpperBound()).isEqualTo(VECTORS);
    }

    // A fresh session opens the old file and searches it.
    database.close();
    database = factory.open();
    final LSMVectorIndex reopened = indexOf("DocPq");
    final float[] query = embedding(7);
    assertThat(reopened.findNeighborsFromVector(query, 10)).hasSize(10);
  }

  @Test
  void theCodebookIsTrainedOnTheConfiguredNumberOfVectors() {
    createIndex("DocPq", "\"quantization\": \"PRODUCT\", \"pqClusters\": 32, \"pqTrainingLimit\": 500, ");
    insert("DocPq", VECTORS);
    final LSMVectorIndex index = indexOf("DocPq");
    index.buildVectorGraphNow();

    assertThat(index.isPQSearchAvailable()).isTrue();
    assertThat(index.getLastPqTrainingSampleSize())
        .as("pqTrainingLimit is 500, so the codebook is trained on 500 of the %d vectors", VECTORS)
        .isEqualTo(500);
  }

  @Test
  void aTrainingLimitAboveTheVectorCountTrainsOnAllVectors() {
    createIndex("DocPq", "\"quantization\": \"PRODUCT\", \"pqClusters\": 32, \"pqTrainingLimit\": 100000, ");
    insert("DocPq", VECTORS);
    final LSMVectorIndex index = indexOf("DocPq");
    index.buildVectorGraphNow();

    assertThat(index.getLastPqTrainingSampleSize()).isEqualTo(VECTORS);
  }

  @Test
  void theTrainingSampleIsSortedDistinctInRangeAndTheSameEveryTime() {
    for (final int[] shape : new int[][] { { 3000, 500 }, { 10, 10 }, { 1_000_000, 37 }, { 5, 1 } }) {
      final int[] sample = LSMVectorIndex.sampleOrdinals(shape[0], shape[1]);
      assertThat(sample).hasSize(shape[1]);
      assertThat(sample).isSorted().doesNotHaveDuplicates();
      assertThat(sample[0]).isGreaterThanOrEqualTo(0);
      assertThat(sample[sample.length - 1]).isLessThan(shape[0]);
      assertThat(LSMVectorIndex.sampleOrdinals(shape[0], shape[1])).isEqualTo(sample);
    }
  }

  private void createIndex(final String type, final String extraMetadata) {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON " + type + " (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
          + ", \"similarity\": \"EUCLIDEAN\", " + extraMetadata + "\"storeVectorsInGraph\": false, \"addHierarchy\": false }");
    });
  }

  private void insert(final String type, final int count) {
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        database.command("sql", "INSERT INTO " + type + " SET embedding = ?", (Object) embedding(i));
    });
  }

  private static float[] embedding(final int doc) {
    final Random random = new Random(10514L * 31 + doc);
    final float[] v = new float[DIMENSIONS];
    for (int j = 0; j < DIMENSIONS; j++)
      v[j] = (float) random.nextGaussian();
    return v;
  }

  private LSMVectorIndex indexOf(final String type) {
    return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName(type + "[embedding]")).getIndexesOnBuckets()[0];
  }

  private static Field field(final String name) throws Exception {
    final Field f = LSMVectorIndex.class.getDeclaredField(name);
    f.setAccessible(true);
    return f;
  }
}
