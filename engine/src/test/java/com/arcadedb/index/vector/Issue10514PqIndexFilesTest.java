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
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;

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
  void anIndexWrittenWithTheFusedPqFeatureByAnOlderReleaseIsSearchedThroughItWithoutTopUps() throws Exception {
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

    // Rewrite the graph file the way releases before #10516 wrote it: with the FusedPQ feature, whatever the index option says.
    final LSMVectorIndexGraphFile graphFile = index.getGraphFile();
    database.transaction(() -> graphFile.writeGraph(graph, new ListRandomAccessVectorValues(List.<VectorFloat<?>>of(), DIMENSIONS),
        0, null, pq, pqVectors, true));
    try (final OnDiskGraphIndex old = graphFile.loadGraph()) {
      assertThat(old.getFeatureSet()).contains(FeatureId.FUSED_PQ);
      assertThat(old.getIdUpperBound()).isEqualTo(VECTORS);
    }

    // A fresh session opens the old file and searches it through the fused codes: the walk finds its k results by itself
    database.close();
    database = factory.open();
    final LSMVectorIndex reopened = indexOf("DocPq");
    final long scansBefore = reopened.metrics.getBruteForceScans();
    for (int q = 0; q < 20; q++)
      assertThat(reopened.findNeighborsFromVector(embedding(7 + q), 10)).hasSize(10);
    assertThat(reopened.metrics.getBruteForceScans() - scansBefore).as("brute-force top-ups of a short walk").isZero();
  }

  @Test
  void withTheFusedPqOptionOnTheGraphFileHasTheFeature() throws Exception {
    createIndex("DocPq", "\"quantization\": \"PRODUCT\", \"fusedPQ\": true, ");
    createIndex("DocNone", "");
    insert("DocPq", VECTORS);
    insert("DocNone", VECTORS);
    indexOf("DocPq").buildVectorGraphNow();
    indexOf("DocNone").buildVectorGraphNow();

    try (final OnDiskGraphIndex graph = indexOf("DocPq").getGraphFile().loadGraph()) {
      assertThat(graph.getFeatureSet()).contains(FeatureId.FUSED_PQ);
    }
    assertThat(indexOf("DocPq").getGraphFile().getLastWrittenGraphBytes())
        .isGreaterThan(indexOf("DocNone").getGraphFile().getLastWrittenGraphBytes() * 2);
  }

  @Test
  void anIndexCreatedOverExistingRowsWithTheFusedPqOptionKeepsTheFeatureAcrossAReopen() throws Exception {
    // CREATE INDEX over rows that exist runs build(), which persists the graph in a second pass of its own
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE DocPq");
      database.command("sql", "CREATE PROPERTY DocPq.embedding ARRAY_OF_FLOATS");
    });
    insert("DocPq", VECTORS);
    database.transaction(() -> database.command("sql", "CREATE INDEX ON DocPq (embedding) LSM_VECTOR METADATA { \"dimensions\": "
        + DIMENSIONS + ", \"similarity\": \"EUCLIDEAN\", \"quantization\": \"PRODUCT\", \"fusedPQ\": true, "
        + "\"storeVectorsInGraph\": false, \"addHierarchy\": false }"));

    database.close();
    database = factory.open();
    final LSMVectorIndex index = indexOf("DocPq");
    final long scansBefore = index.metrics.getBruteForceScans();
    for (int q = 0; q < 20; q++)
      assertThat(index.findNeighborsFromVector(embedding(7 + q), 10)).hasSize(10);
    assertThat(index.metrics.getBruteForceScans() - scansBefore).as("brute-force top-ups of a short walk").isZero();
    try (final OnDiskGraphIndex graph = index.getGraphFile().loadGraph()) {
      assertThat(graph.getFeatureSet()).contains(FeatureId.FUSED_PQ);
    }
  }

  @Test
  void anIndexRebuiltOverExistingRowsWithTheFusedPqOptionKeepsTheFeatureAcrossAReopen() throws Exception {
    // CREATE INDEX over rows that exist runs build(), which persists the graph in a second pass of its own
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE DocPq");
      database.command("sql", "CREATE PROPERTY DocPq.embedding ARRAY_OF_FLOATS");
    });
    insert("DocPq", VECTORS);
    database.transaction(() -> database.command("sql", "CREATE INDEX ON DocPq (embedding) LSM_VECTOR METADATA { \"dimensions\": "
        + DIMENSIONS + ", \"similarity\": \"EUCLIDEAN\", \"quantization\": \"PRODUCT\", \"fusedPQ\": true, "
        + "\"storeVectorsInGraph\": false, \"addHierarchy\": false }"));

    database.close();
    database = factory.open();
    database.command("sql", "REBUILD INDEX `DocPq[embedding]`");
    database.close();
    database = factory.open();
    final LSMVectorIndex index = indexOf("DocPq");
    final long scansBefore = index.metrics.getBruteForceScans();
    for (int q = 0; q < 20; q++)
      assertThat(index.findNeighborsFromVector(embedding(7 + q), 10)).hasSize(10);
    assertThat(index.metrics.getBruteForceScans() - scansBefore).as("brute-force top-ups of a short walk").isZero();
    try (final OnDiskGraphIndex graph = index.getGraphFile().loadGraph()) {
      assertThat(graph.getFeatureSet()).contains(FeatureId.FUSED_PQ);
    }
  }

  @Test
  void aGraphFileWithoutTheFusedPqFeatureOfAnIndexWithoutThe256ClustersStillOpensAndAnswers() throws Exception {
    // FusedPQ needs 256 clusters: even with the option on, this index has a trained codebook and a graph file without it
    createIndex("DocPq", "\"quantization\": \"PRODUCT\", \"pqClusters\": 32, \"fusedPQ\": true, ");
    insert("DocPq", VECTORS);
    indexOf("DocPq").buildVectorGraphNow();
    try (final OnDiskGraphIndex graph = indexOf("DocPq").getGraphFile().loadGraph()) {
      assertThat(graph.getFeatureSet()).doesNotContain(FeatureId.FUSED_PQ);
    }
    database.close();
    database = factory.open();
    assertThat(indexOf("DocPq").findNeighborsFromVector(embedding(7), 10)).hasSize(10);
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

  @Test
  void fusedPqWithAnotherClusterCountWarnsOnceAtCreateIndexAndDoesNotFailTheStatement() {
    final List<String> warnings = new ArrayList<>();
    final Logger previous = LogManager.instance().getLogger();
    try {
      LogManager.instance().setLogger(new WarningCollector(warnings));
      createIndex("DocWarn", "\"quantization\": \"PRODUCT\", \"pqClusters\": 32, \"fusedPQ\": true, ");
      createIndex("DocQuiet", "\"quantization\": \"PRODUCT\", \"fusedPQ\": true, ");
    } finally {
      LogManager.instance().setLogger(previous);
    }

    assertThat(warnings).hasSize(1);
    assertThat(warnings.getFirst()).contains("DocWarn").contains("fusedPQ").contains("pqClusters is 32");
    assertThat(indexOf("DocWarn").getMetadata().fusedPQ).isTrue();
  }

  private record WarningCollector(List<String> warnings) implements Logger {
    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
        final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5, final Object arg6, final Object arg7,
        final Object arg8, final Object arg9, final Object arg10, final Object arg11, final Object arg12, final Object arg13,
        final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
      log(requester, level, message, exception, context, new Object[] { arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11,
          arg12, arg13, arg14, arg15, arg16, arg17 });
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
        final Object... args) {
      if (level != Level.WARNING || message == null || !message.contains("fusedPQ"))
        return;
      final List<Object> bound = new ArrayList<>();
      if (args != null)
        for (final Object arg : args)
          if (arg != null)
            bound.add(arg);
      warnings.add(bound.isEmpty() ? message : String.format(message, bound.toArray()));
    }

    @Override
    public void flush() {
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
