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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import com.arcadedb.utility.VectorApiSupport;
import com.sun.management.ThreadMXBean;
import io.github.jbellis.jvector.disk.IndexWriter;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.ImmutableGraphIndex;
import io.github.jbellis.jvector.graph.ListRandomAccessVectorValues;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.disk.feature.FeatureId;
import io.github.jbellis.jvector.graph.disk.feature.FusedPQ;
import io.github.jbellis.jvector.graph.similarity.DefaultSearchScoreProvider;
import io.github.jbellis.jvector.graph.similarity.ScoreFunction;
import io.github.jbellis.jvector.quantization.MutablePQVectors;
import io.github.jbellis.jvector.quantization.PQVectors;
import io.github.jbellis.jvector.quantization.ProductQuantization;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.ByteSequence;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Product quantization (PQ) codes, codebooks and fused PQ graphs. Each nested class keeps the fixture of the regression test it was folded from.
 */
class ProductQuantizationTest {
  /**
   * Regression for #10514: fused pq.
   * <p>
   * Issue #10514: a PRODUCT index with the {@code fusedPQ} option walks its graph on the PQ codes fused into the nodes of the
   * graph file (jvector's {@code FusedPQ}) and re-ranks what it finds on the stored vectors.
   * <p>
   * The reader of that feature was switched off in March 2026 because the search returned a single result. The fused score
   * function leaves the packed codes and the neighbour list of a node in the graph view it was given, and the walk reads the
   * neighbour list of the view of its own searcher; the reader passed it a fresh view, so the walk saw no neighbour of the entry
   * node. It has to be given the view of the searcher.
   * <p>
   * 20,000 vectors of 64 dimensions in 100 clusters, a fixed seed: the graph file spans many pages, and the nodes (a
   * neighbour list and the codes of up to 32 neighbours) are not a divisor of a page, so nodes cross page boundaries.
   */
  @Nested
  @Tag("vector")
  class FusedPq extends TestHelper {
    private static final int DIMENSIONS = 64;
    private static final int VECTORS    = 20_000;
    private static final int CLUSTERS   = 100;
    private static final int QUERIES    = 100;
    private static final int K          = 10;
    private static final int EF_SEARCH  = 100;

    private static final VectorTypeSupport VTS = VectorizationProvider.getInstance().getVectorTypeSupport();

    @Test
    void aReopenedFusedPqGraphAnswersAsWellAsTheSameGraphWalkedOnTheNonFusedPqCodes() throws Exception {
      final float[][] base = clustered(new Random(10514L), VECTORS);
      final float[][] queries = clustered(new Random(105140L), QUERIES);
      load(base);
      indexOf().buildVectorGraphNow();

      // A fresh session: the index is searched through the graph file, not through the graph it was built as in memory
      database.close();
      database = factory.open();
      final LSMVectorIndex index = indexOf();
      final long readsBefore = index.metrics.getVectorFetchFromDocuments();
      assertThat(index.findNeighborsFromVector(queries[0], K, EF_SEARCH, null)).as("the first search of the session").hasSize(K);
      // The walk is on the PQ codes fused into the nodes: the stored vectors are read to re-rank the beam only (the cache is cold)
      assertThat(index.metrics.getVectorFetchFromDocuments() - readsBefore).as("stored vectors read by the first search of the session")
          .isLessThanOrEqualTo(2L * EF_SEARCH);
      assertThat(index.isPQSearchAvailable()).isTrue();

      try (final OnDiskGraphIndex graph = index.getGraphFile().loadGraph()) {
        assertThat(graph.getFeatureSet()).contains(FeatureId.FUSED_PQ);
        assertThat(index.getGraphFile().getTotalPages()).as("pages of the graph file").isGreaterThan(30);
        assertThat(graph.getIdUpperBound()).isEqualTo(VECTORS);
      }

      final PQVectors pqVectors = index.pqCodesForPublishedGraphForTest();
      assertThat(pqVectors).isNotNull();
      final VectorSimilarityFunction similarity = VectorSimilarityFunction.EUCLIDEAN;
      // The graph numbers its nodes by ordinal; the vid of an ordinal is found by the vector it holds
      final java.lang.reflect.Method searchVectorValues = LSMVectorIndex.class.getDeclaredMethod("searchVectorValues", int[].class);
      searchVectorValues.setAccessible(true);
      final int[] ordinalToVectorId = index.getOrdinalToVectorIdForTest();
      assertThat(ordinalToVectorId).hasSize(VECTORS);
      final RandomAccessVectorValues stored = (RandomAccessVectorValues) searchVectorValues.invoke(index, (Object) ordinalToVectorId);
      final java.util.Map<Integer, Integer> vidOfHash = new java.util.HashMap<>();
      for (int vid = 0; vid < VECTORS; vid++)
        vidOfHash.put(java.util.Arrays.hashCode(base[vid]), vid);
      final int[] vidOfOrdinal = new int[VECTORS];
      for (int ordinal = 0; ordinal < VECTORS; ordinal++) {
        final VectorFloat<?> vector = stored.getVector(ordinal);
        final float[] copy = new float[DIMENSIONS];
        for (int j = 0; j < DIMENSIONS; j++)
          copy[j] = vector.get(j);
        vidOfOrdinal[ordinal] = vidOfHash.get(java.util.Arrays.hashCode(copy));
      }

      double exact = 0, fused = 0, nonFused = 0;
      final long scansBefore = index.metrics.getBruteForceScans();
      try (final OnDiskGraphIndex graph = index.getGraphFile().loadGraph()) {
        for (final float[] query : queries) {
          final Set<Integer> truth = nearest(base, query, K);
          final List<Pair<RID, Float>> answer = index.findNeighborsFromVector(query, K, EF_SEARCH, null);
          assertThat(answer).as("a search of the fused graph returns k results, not one").hasSize(K);
          fused += recall(truth, answer);

          // The same graph walked on the PQ codes of the PQ file instead of the codes fused into its nodes
          final VectorFloat<?> q = VTS.createFloatVector(query);
          final ScoreFunction.ExactScoreFunction rerank = node -> similarity.compare(q, VTS.createFloatVector(base[vidOfOrdinal[node]]));
          try (final GraphSearcher searcher = new GraphSearcher(graph)) {
            final SearchResult result = searcher.search(new DefaultSearchScoreProvider(
                pqVectors.precomputedScoreFunctionFor(q, similarity), rerank), K, EF_SEARCH, 0.0f, 0.0f, Bits.ALL);
            nonFused += recall(truth, result, vidOfOrdinal);
          }
          try (final GraphSearcher searcher = new GraphSearcher(graph)) {
            final SearchResult result = searcher.search(new DefaultSearchScoreProvider(rerank, rerank), K, EF_SEARCH, 0.0f, 0.0f,
                Bits.ALL);
            exact += recall(truth, result, vidOfOrdinal);
          }
        }
      }
      // A walk that finds one result is topped up by a brute-force scan, which hides it behind a full and exact answer
      assertThat(index.metrics.getBruteForceScans() - scansBefore).as("brute-force scans that topped up a short walk").isZero();
      exact /= QUERIES;
      fused /= QUERIES;
      nonFused /= QUERIES;

      assertThat(exact).as("the exact walk of this graph").isGreaterThan(0.9);
      assertThat(fused).as("the fused-code walk (non-fused PQ walk %.4f, exact walk %.4f)", nonFused, exact)
          .isCloseTo(nonFused, org.assertj.core.data.Offset.offset(0.01));
      assertThat(fused).isGreaterThan(0.9 * exact);
    }

    @Test
    void theFusedScoreFunctionHasToBeGivenTheViewOfTheSearcher() throws Exception {
      final float[][] base = clustered(new Random(1L), 5000);
      final float[] query = clustered(new Random(2L), 1)[0];
      load(base);
      indexOf().buildVectorGraphNow();

      final VectorSimilarityFunction similarity = VectorSimilarityFunction.EUCLIDEAN;
      final VectorFloat<?> q = VTS.createFloatVector(query);
      final ScoreFunction.ExactScoreFunction exact = node -> similarity.compare(q, VTS.createFloatVector(base[node]));

      try (final OnDiskGraphIndex graph = indexOf().getGraphFile().loadGraph()) {
        final FusedPQ fusedPQ = (FusedPQ) graph.getFeatures().get(FeatureId.FUSED_PQ);

        // The view of the searcher: the walk reaches the neighbours of the entry node and finds k results
        try (final GraphSearcher searcher = new GraphSearcher(graph)) {
          final ScoreFunction.ApproximateScoreFunction fused = fusedPQ.approximateScoreFunctionFor(q, similarity,
              (OnDiskGraphIndex.View) searcher.getView(), exact);
          assertThat(searcher.search(new DefaultSearchScoreProvider(fused, exact), K, EF_SEARCH, 0.0f, 0.0f, Bits.ALL).getNodes())
              .hasSize(K);
        }

        // Any other view, as the reader switched off in March 2026 did: the codes and the neighbour list of a node are left in
        // the view given to the score function, and the walk looks for them in its own
        try (final GraphSearcher searcher = new GraphSearcher(graph)) {
          final ScoreFunction.ApproximateScoreFunction fused = fusedPQ.approximateScoreFunctionFor(q, similarity, graph.getView(),
              exact);
          assertThat(searcher.search(new DefaultSearchScoreProvider(fused, exact), K, EF_SEARCH, 0.0f, 0.0f, Bits.ALL).getNodes())
              .as("results of a walk scored through a view that is not its own").hasSizeLessThan(K);
        }
      }
    }

    private double recall(final Set<Integer> truth, final List<Pair<RID, Float>> answer) {
      int hits = 0;
      for (final Pair<RID, Float> neighbour : answer)
        if (truth.contains(database.lookupByRID(neighbour.getFirst(), true).asDocument().getInteger("vid")))
          hits++;
      return (double) hits / K;
    }

    private static double recall(final Set<Integer> truth, final SearchResult result, final int[] vidOfOrdinal) {
      int hits = 0;
      for (final SearchResult.NodeScore node : result.getNodes())
        if (truth.contains(vidOfOrdinal[node.node]))
          hits++;
      return (double) hits / K;
    }

    private LSMVectorIndex indexOf() {
      return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[embedding]")).getIndexesOnBuckets()[0];
    }

    private void load(final float[][] base) {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Doc");
        database.command("sql", "CREATE PROPERTY Doc.vid INTEGER");
        database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
        database.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
            + ", \"similarity\": \"EUCLIDEAN\", \"quantization\": \"PRODUCT\", \"fusedPQ\": true, \"storeVectorsInGraph\": false, "
            + "\"addHierarchy\": false }");
      });
      database.transaction(() -> {
        for (int i = 0; i < base.length; i++) {
          final MutableDocument doc = database.newDocument("Doc");
          doc.set("vid", i);
          doc.set("embedding", base[i]);
          doc.save();
        }
      });
    }

    /** {@code count} vectors around {@link #CLUSTERS} centres; the centres come from their own fixed seed, so base and queries share them. */
    private static float[][] clustered(final Random random, final int count) {
      final Random centreRandom = new Random(42L);
      final float[][] centres = new float[CLUSTERS][DIMENSIONS];
      for (final float[] centre : centres)
        for (int j = 0; j < DIMENSIONS; j++)
          centre[j] = (float) (centreRandom.nextGaussian() * 3.0);
      final float[][] vectors = new float[count][DIMENSIONS];
      for (int i = 0; i < count; i++) {
        final float[] centre = centres[random.nextInt(CLUSTERS)];
        for (int j = 0; j < DIMENSIONS; j++)
          vectors[i][j] = centre[j] + (float) random.nextGaussian();
      }
      return vectors;
    }

    private static Set<Integer> nearest(final float[][] base, final float[] query, final int k) {
      final double[] distance = new double[base.length];
      final Integer[] order = new Integer[base.length];
      for (int i = 0; i < base.length; i++) {
        distance[i] = squaredDistance(query, base[i]);
        order[i] = i;
      }
      java.util.Arrays.sort(order, java.util.Comparator.comparingDouble(i -> distance[i]));
      final Set<Integer> top = new HashSet<>();
      for (int i = 0; i < k; i++)
        top.add(order[i]);
      return top;
    }

    private static float squaredDistance(final float[] a, final float[] b) {
      double sum = 0;
      for (int i = 0; i < a.length; i++) {
        final double d = a[i] - b[i];
        sum += d * d;
      }
      return (float) sum;
    }
  }

  /**
   * Regression for #10514: pq index files.
   * <p>
   * Issue #10514, the index files and the training set of a {@code PRODUCT} LSM_VECTOR index.
   * <ul>
   *   <li>The persisted graph stored the PQ codes of all neighbours inline in every node (jvector's {@code FusedPQ}
   *   feature) although no search reads them: the graph file of a PRODUCT index was several times the size of the file
   *   of the same data without quantization.</li>
   *   <li>{@code pqTrainingLimit} was read by nothing but a log line that claimed the codebook had been trained on
   *   that many vectors.</li>
   * </ul>
   */
  @Nested
  @Tag("vector")
  class PqIndexFiles extends TestHelper {
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

      final ProductQuantization pq = index.pqCodesForTest().quantization();
      final PQVectors pqVectors = index.pqCodesForPublishedGraphForTest();
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

  /**
   * Regression for #10698: pq codes adopted only for their graph.
   * <p>
   * Regression test for issue #10698: the PQ codes of a PRODUCT-quantized index record which graph they were built for
   * (issue #10584), and every reopen route that publishes a persisted graph must adopt them only for that graph. The
   * exact-cover reopen already asked; the ordinary walk and the stale-prefix reuse published the same persisted graph and
   * kept whatever codes the PQ file held. Each test puts codes that are NOT the graph's next to it and checks that the
   * reopen rebuilds them for the graph it publishes, without rebuilding the graph itself.
   */
  @Nested
  @Tag("vector")
  class PqCodesAdoptedOnlyForTheirGraph {
    private static final String            DB_ROOT     = "target/test-databases/Issue10698PqCodesAdoptedOnlyForTheirGraphTest";
    private static final int               DIMENSIONS  = 32;
    private static final int               NUM_VECTORS = 2_000;
    private static final VectorTypeSupport VTS         = VectorizationProvider.getInstance().getVectorTypeSupport();
    private static final String            WALK_LINE   = "Loaded graph from disk for index";
    private static final String            REBUILT     = "building PQ for the";

    private String dbPath;
    private Path   graphPath;
    private Path   pqPath;

    @BeforeEach
    void setUp(final TestInfo testInfo) {
      dbPath = DB_ROOT + "-" + testInfo.getTestMethod().orElseThrow().getName();
      FileUtils.deleteRecursively(new File(dbPath));
    }

    @AfterEach
    void tearDown() {
      FileUtils.deleteRecursively(new File(dbPath));
    }

    /**
     * The issue's own repro: a PQ sidecar saved aside, a vector added and graph and PQ persisted again, the old sidecar put
     * back. The codes number one less than the graph's ordinals, so the exact-cover reopen refuses them, and the walk used
     * to publish the graph with them anyway - leaving every approximate search on the exact path for good (issue #5417).
     */
    @Test
    void anOlderPqSidecarRestoredBesideANewerGraphIsRebuiltByTheWalk() throws IOException {
      build(NUM_VECTORS);
      final byte[] olderSidecar = Files.readAllBytes(pqPath);

      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          insert(db, NUM_VECTORS, NUM_VECTORS + 1);
          vectorIndex(db).buildVectorGraphNow();
        } finally {
          db.close();
        }
      }
      assertThat(manifest().vectorCount()).as("precondition: the newer graph covers the added vector").isEqualTo(NUM_VECTORS + 1);
      Files.write(pqPath, olderSidecar);

      final List<String> logs = new ArrayList<>();
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          final LSMVectorIndex index = vectorIndex(db);
          capturingLogs(logs, () -> index.findNeighborsFromVector(embedding(1), 10, 64));

          assertThat(logs).as("the walk published the persisted graph").anyMatch(m -> m.contains(WALK_LINE));
          assertThat(index.getStats().get("graphRebuildCount")).as("the graph itself is reused, not rebuilt").isZero();
          assertThat(index.getPQVectorCount()).as("the codes are the newer graph's, one per ordinal").isEqualTo(NUM_VECTORS + 1);
          assertThat(index.pqCodesGraphFingerprintForTest()).as("and record the graph the manifest certifies")
              .isEqualTo(manifest().fingerprint());
          assertThat(index.pqCodesForPublishedGraphForTest()).as("and serve the graph the walk published (issue #10786)")
              .isNotNull();
          assertThat(logs).anyMatch(m -> m.contains(REBUILT));
        } finally {
          db.close();
        }
      }

      assertTheNextReopenTakesTheOrdinalMap();
    }

    /**
     * Same count, codes of another assignment of ordinals: every vector's code is the code of another vector. Searching
     * with them is a wrong-but-plausible answer; the walk must rebuild them before the graph is searched through them.
     */
    @Test
    void codesOfAnotherGraphOfTheSameSizeAreRebuiltByTheWalk() throws IOException {
      build(NUM_VECTORS);
      final LSMVectorIndexPQFile pqFile = pqFile();
      assertThat(pqFile.loadPQ()).isTrue();
      final ProductQuantization pq = pqFile.getProductQuantization();
      final long fingerprint = pqFile.graphFingerprintOf(pqFile.getPQVectors());
      final MutablePQVectors shuffled = new MutablePQVectors(pq);
      for (int i = 0; i < NUM_VECTORS; i++)
        shuffled.encodeAndSet(i, VTS.createFloatVector(embedding(NUM_VECTORS - 1 - i)));
      pqFile.writePQ(pq, shuffled, fingerprint ^ 1L);

      final List<String> logs = new ArrayList<>();
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          final LSMVectorIndex index = vectorIndex(db);
          final int probe = 1;
          final List<Pair<RID, Float>> results = capturingLogs(logs,
              () -> index.findNeighborsFromVectorApproximate(embedding(probe), 10));

          assertThat(logs).as("the walk published the persisted graph").anyMatch(m -> m.contains(WALK_LINE));
          assertThat(index.getStats().get("graphRebuildCount")).isZero();
          assertThat(index.pqCodesGraphFingerprintForTest()).isEqualTo(manifest().fingerprint());
          assertThat(index.pqCodesForPublishedGraphForTest()).isNotNull();
          assertThat(results.stream().map(Pair::getFirst)).as("a vector is its own nearest neighbour once its code is its own")
              .contains(ridOf(db, probe));
          assertThat(logs).anyMatch(m -> m.contains("were built for another graph than the persisted one") && m.contains(REBUILT));
        } finally {
          db.close();
        }
      }

      assertTheNextReopenTakesTheOrdinalMap();
    }

    /**
     * A PQ file written before the identity was recorded: the state of every PRODUCT index upgraded from an earlier build.
     * The walk cannot vouch for those codes, so it rebuilds them once, recorded, and the next reopen is the fast one.
     */
    @Test
    void codesThatRecordNoGraphAreRebuiltOnceAndTheNextReopenTakesTheOrdinalMap() throws IOException {
      build(NUM_VECTORS);
      final byte[] bytes = Files.readAllBytes(pqPath);
      Files.write(pqPath, Arrays.copyOf(bytes, bytes.length - LSMVectorIndexPQFile.GRAPH_IDENTITY_BYTES));

      final List<String> logs = new ArrayList<>();
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          final LSMVectorIndex index = vectorIndex(db);
          capturingLogs(logs, () -> index.findNeighborsFromVector(embedding(1), 10, 64));

          assertThat(index.getStats().get("graphRebuildCount")).isZero();
          assertThat(index.pqCodesGraphFingerprintForTest()).isEqualTo(manifest().fingerprint());
          assertThat(index.pqCodesForPublishedGraphForTest()).isNotNull();
          assertThat(logs).anyMatch(m -> m.contains("record no graph they were built for") && m.contains(REBUILT));
        } finally {
          db.close();
        }
      }

      assertTheNextReopenTakesTheOrdinalMap();
    }

    /** Codes that are the graph's must be kept by the walk: no PQ rebuild on a reopen that does not need one. */
    @Test
    void codesOfThePersistedGraphAreKeptByTheWalk() throws IOException {
      build(NUM_VECTORS);
      // No ordinal map: the reopen takes the walk, with codes that ARE the graph's
      Files.delete(new LSMVectorIndexGraphManifest(graphPath.toString()).getOrdinalMapFilePath());

      final List<String> logs = new ArrayList<>();
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          final LSMVectorIndex index = vectorIndex(db);
          capturingLogs(logs, () -> index.findNeighborsFromVector(embedding(1), 10, 64));

          assertThat(logs).as("the walk ran").anyMatch(m -> m.contains(WALK_LINE));
          assertThat(index.getPQVectorCount()).isEqualTo(NUM_VECTORS);
          assertThat(index.pqCodesForPublishedGraphForTest()).as("bound to the graph it published (issue #10786)").isNotNull();
          assertThat(logs).as("and kept the codes").noneMatch(m -> m.contains(REBUILT))
              .noneMatch(m -> m.contains("Building Product Quantization"));
        } finally {
          db.drop();
        }
      }
    }

    /**
     * The stale-prefix reuse (issue #6655): vectors added after the graph was persisted and the session crashed, so the
     * reopen reuses the graph as a prefix. Codes that are not that graph's must be rebuilt for the prefix it publishes.
     */
    @Test
    void codesOfAnotherGraphAreRebuiltByTheStalePrefixReuse() throws IOException {
      build(NUM_VECTORS);

      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          insert(db, NUM_VECTORS, NUM_VECTORS + 50);
          ((DatabaseInternal) db).kill();
          db.close();
        } finally {
          if (db.isOpen())
            db.close();
        }
      }

      final LSMVectorIndexPQFile pqFile = pqFile();
      assertThat(pqFile.loadPQ()).isTrue();
      final PQVectors codes = pqFile.getPQVectors();
      pqFile.writePQ(pqFile.getProductQuantization(), codes, pqFile.graphFingerprintOf(codes) ^ 1L);

      final List<String> logs = new ArrayList<>();
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try (final ResultSet rs = db.query("sql", "SELECT count(*) as cnt FROM Doc")) {
          assertThat(rs.next().<Long>getProperty("cnt")).as("precondition: the crashed session's records were recovered")
              .isEqualTo((long) NUM_VECTORS + 50);
          final LSMVectorIndex index = vectorIndex(db);
          LSMVectorIndex.acquireAllRebuildPermitsForTest();
          try {
            final List<Pair<RID, Float>> results = capturingLogs(logs,
                () -> index.findNeighborsFromVector(embedding(NUM_VECTORS + 10), 5, 64));

            assertThat(index.getStats().get("stalePrefixGraphReuses")).as("precondition: the prefix reuse ran").isEqualTo(1L);
            assertThat(index.getStats().get("graphRebuildCount")).isZero();
            assertThat(index.getPQVectorCount()).as("one code per ordinal of the reused graph").isEqualTo(NUM_VECTORS);
            assertThat(index.pqCodesGraphFingerprintForTest()).as("recording the graph the manifest certifies")
                .isEqualTo(manifest().fingerprint());
            assertThat(index.pqCodesForPublishedGraphForTest()).as("bound to the prefix it published (issue #10786)")
                .isNotNull();
            assertThat(logs).anyMatch(m -> m.contains("were built for another graph than the persisted one")
                && m.contains("reused graph prefix"));
            assertThat(results.stream().map(Pair::getFirst)).as("the gap is still searchable")
                .contains(ridOf(db, NUM_VECTORS + 10));
          } finally {
            LSMVectorIndex.releaseAllRebuildPermitsForTest();
          }
        } finally {
          db.drop();
        }
      }
    }

    private void assertTheNextReopenTakesTheOrdinalMap() {
      final List<String> logs = new ArrayList<>();
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          final LSMVectorIndex index = vectorIndex(db);
          capturingLogs(logs, () -> index.findNeighborsFromVector(embedding(1), 10, 64));
          assertThat(index.getStats().get("graphLoadsFromOrdinalMap"))
              .as("the rebuilt codes were persisted with their graph's identity, so the next reopen is the fast one").isEqualTo(1L);
          assertThat(index.pqCodesForPublishedGraphForTest()).as("bound to the map it published (issue #10786)").isNotNull();
          assertThat(logs).noneMatch(m -> m.contains(WALK_LINE)).noneMatch(m -> m.contains(REBUILT));
        } finally {
          db.drop();
        }
      }
    }

    private LSMVectorIndexGraphManifest.Content manifest() {
      final LSMVectorIndexGraphManifest.Content content = new LSMVectorIndexGraphManifest(graphPath.toString()).read();
      assertThat(content).isNotNull();
      return content;
    }

    private LSMVectorIndexPQFile pqFile() {
      final String file = pqPath.toString();
      return new LSMVectorIndexPQFile(file.substring(0, file.length() - ("." + LSMVectorIndexPQFile.FILE_EXT).length()));
    }

    private void build(final int count) {
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        try {
          db.transaction(() -> {
            final var type = db.getSchema().createDocumentType("Doc");
            type.createProperty("id", Type.INTEGER);
            type.createProperty("vector", Type.ARRAY_OF_FLOATS);
            db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
                + ", \"similarity\": \"COSINE\", \"quantization\": \"PRODUCT\" }");
          });
          insert(db, 0, count);
          final LSMVectorIndex index = vectorIndex(db);
          index.buildVectorGraphNow();
          graphPath = index.getGraphFilePathForTest();
          pqPath = index.getProductQuantizationFilePath();
        } finally {
          if (db.isOpen())
            db.close();
        }
      }
      assertThat(Files.exists(pqPath)).as("precondition: the build persisted its PQ").isTrue();
    }

    private static void insert(final Database db, final int from, final int to) {
      db.begin();
      for (int i = from; i < to; i++) {
        db.newDocument("Doc").set("id", i).set("vector", embedding(i)).save();
        if (i % 1000 == 999) {
          db.commit();
          db.begin();
        }
      }
      db.commit();
    }

    private static RID ridOf(final Database db, final int id) {
      try (final ResultSet rs = db.query("sql", "SELECT @rid as rid FROM Doc WHERE id = ?", id)) {
        return rs.next().getProperty("rid");
      }
    }

    private static <T> T capturingLogs(final List<String> into, final Supplier<T> action) {
      final Logger previous = LogManager.instance().getLogger();
      try {
        LogManager.instance().setLogger(new Collector(into));
        return action.get();
      } finally {
        LogManager.instance().setLogger(previous);
      }
    }

    private record Collector(List<String> lines) implements Logger {
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
        if (message == null || level.intValue() < Level.INFO.intValue())
          return;
        final List<Object> bound = new ArrayList<>();
        if (args != null)
          for (final Object arg : args)
            if (arg != null)
              bound.add(arg);
        String formatted;
        try {
          formatted = bound.isEmpty() ? message : String.format(message, bound.toArray());
        } catch (final RuntimeException e) {
          formatted = message;
        }
        synchronized (lines) {
          lines.add(formatted);
        }
      }

      @Override
      public void flush() {
      }
    }

    private static float[] embedding(final int id) {
      final Random random = new Random(0x10698L * 31 + id);
      final float[] vector = new float[DIMENSIONS];
      for (int d = 0; d < DIMENSIONS; d++)
        vector[d] = random.nextFloat();
      return vector;
    }

    private static LSMVectorIndex vectorIndex(final Database db) {
      return (LSMVectorIndex) db.getSchema().getType("Doc").getPolymorphicIndexByProperties("vector")
          .getIndexesOnBuckets()[0];
    }
  }

  /**
   * Regression for #10713: fused pq sql walk.
   * <p>
   * Issue #10713: a PRODUCT index with {@code fusedPQ: true} stores the PQ codes of every node's neighbours in its graph file, and
   * the search that SQL {@code vector.neighbors}, the openCypher procedure {@code db.index.vector.queryNodes} and the Java
   * {@code findNeighbors} share (the PQ walk of {@code findNeighborsFromVectorApproximate}) never read them: it scored the walk
   * on the PQ codes held in memory, so the setting cost disk and bought nothing. Only the exact walk of
   * {@code findNeighborsFromVector} read the fused codes.
   * <p>
   * The PQ walk now scores the neighbours of the node it expands from the codes fused into that node when the graph file has
   * them, and the {@code fusedPQWalks} statistic counts the walks that did.
   */
  @Nested
  @Tag("vector")
  class FusedPqSqlWalk extends TestHelper {
    private static final int DIMENSIONS = 64;
    private static final int VECTORS    = 6000;
    private static final int CLUSTERS   = 60;
    private static final int QUERIES    = 20;
    private static final int K          = 10;

    private static final String SQL         = "SELECT vid, distance FROM (SELECT expand(`vector.neighbors`('Doc[embedding]', ?, ?)))";
    private static final String SQL_OPTIONS = "SELECT vid, distance FROM (SELECT expand(`vector.neighbors`('Doc[embedding]', ?, ?, ?)))";
    private static final String CYPHER      = "CALL db.index.vector.queryNodes('Doc[embedding]', " + K
        + ", $v) YIELD node, score RETURN node.vid AS vid, score";

    @Test
    void sqlCypherAndJavaWalkTheFusedCodesOfAReopenedFusedPqIndex() {
      final float[][] base = clustered(new Random(10713L), VECTORS);
      final float[][] queries = clustered(new Random(107130L), QUERIES);
      load(base, true);
      indexOf().buildVectorGraphNow();

      // A fresh session: the index is searched through its graph file, not through the graph it was built as in memory
      database.close();
      database = factory.open();
      final LSMVectorIndex index = indexOf();
      assertThat(index.isPQSearchAvailable()).as("the scenario needs a trained codebook").isTrue();

      long walks = fusedWalks(index);
      double recall = 0;
      for (final float[] query : queries) {
        final List<Result> rows = sql(query);
        assertThat(rows).as("a SQL search of the fused graph returns k rows").hasSize(K);
        recall += recall(nearest(base, query), rows);
      }
      assertThat(fusedWalks(index) - walks).as("SQL vector.neighbors walks on the codes fused into the graph file").isEqualTo(QUERIES);
      assertThat(recall / QUERIES).as("recall@10 of SQL on the fused codes, re-ranked on the stored vectors").isGreaterThan(0.9);

      walks = fusedWalks(index);
      assertThat(cypher(queries[0])).hasSize(K);
      assertThat(fusedWalks(index) - walks).as("db.index.vector.queryNodes walks on the fused codes too").isEqualTo(1L);

      // The PQ scores alone: the walk on the fused codes, the answer scored on the PQ codes held in memory
      walks = fusedWalks(index);
      assertThat(sql(queries[1], Map.of("rerank", 0))).hasSize(K);
      final List<Pair<RID, Float>> pqOnly = index.findNeighborsFromVectorApproximate(queries[2], K);
      assertThat(pqOnly).hasSize(K);
      assertThat(fusedWalks(index) - walks).as("the PQ-scores-only walks of SQL and Java").isEqualTo(2L);
      for (int i = 1; i < pqOnly.size(); i++)
        assertThat(pqOnly.get(i).getSecond()).isGreaterThanOrEqualTo(pqOnly.get(i - 1).getSecond());

      // The exact walk read them before this fix and still does
      walks = fusedWalks(index);
      assertThat(index.findNeighborsFromVector(queries[3], K)).hasSize(K);
      assertThat(fusedWalks(index) - walks).as("the exact walk of findNeighborsFromVector").isEqualTo(1L);
    }

    @Test
    void aProductIndexWithoutFusedCodesIsWalkedOnItsInMemoryCodes() throws Exception {
      final float[][] base = clustered(new Random(1L), VECTORS);
      load(base, false);
      indexOf().buildVectorGraphNow();
      database.close();
      database = factory.open();
      final LSMVectorIndex index = indexOf();
      assertThat(index.isPQSearchAvailable()).isTrue();
      try (final OnDiskGraphIndex graph = index.getGraphFile().loadGraph()) {
        assertThat(graph.getFeatureSet()).doesNotContain(FeatureId.FUSED_PQ);
      }

      final long walks = fusedWalks(index);
      final float[] query = clustered(new Random(2L), 1)[0];
      assertThat(sql(query)).hasSize(K);
      assertThat(cypher(query)).hasSize(K);
      assertThat(index.findNeighborsFromVectorApproximate(query, K)).hasSize(K);
      assertThat(fusedWalks(index) - walks).as("walks on fused codes of a graph file that has none").isZero();
    }

    private static long fusedWalks(final LSMVectorIndex index) {
      return index.getStats().getOrDefault("fusedPQWalks", -1L);
    }

    private List<Result> sql(final float[] query) {
      final List<Result> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", SQL, query, K)) {
        rs.forEachRemaining(rows::add);
      }
      return rows;
    }

    private List<Result> sql(final float[] query, final Map<String, Object> options) {
      final List<Result> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", SQL_OPTIONS, query, K, options)) {
        rs.forEachRemaining(rows::add);
      }
      return rows;
    }

    private List<Result> cypher(final float[] query) {
      final List<Result> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", CYPHER, Map.of("v", query))) {
        rs.forEachRemaining(rows::add);
      }
      return rows;
    }

    private static double recall(final Set<Integer> truth, final List<Result> rows) {
      int hits = 0;
      for (final Result row : rows)
        if (truth.contains(row.<Integer>getProperty("vid")))
          hits++;
      return (double) hits / K;
    }

    private LSMVectorIndex indexOf() {
      return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[embedding]")).getIndexesOnBuckets()[0];
    }

    private void load(final float[][] base, final boolean fusedPQ) {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Doc");
        database.command("sql", "CREATE PROPERTY Doc.vid INTEGER");
        database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
        database.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
            + ", \"similarity\": \"EUCLIDEAN\", \"quantization\": \"PRODUCT\", \"pqSubspaces\": 16, \"pqClusters\": 256, \"fusedPQ\": "
            + fusedPQ + " }");
      });
      database.transaction(() -> {
        for (int i = 0; i < base.length; i++) {
          final MutableDocument doc = database.newDocument("Doc");
          doc.set("vid", i);
          doc.set("embedding", base[i]);
          doc.save();
        }
      });
    }

    /** {@code count} vectors around {@link #CLUSTERS} centres; the centres come from their own fixed seed, so base and queries share them. */
    private static float[][] clustered(final Random random, final int count) {
      final Random centreRandom = new Random(42L);
      final float[][] centres = new float[CLUSTERS][DIMENSIONS];
      for (final float[] centre : centres)
        for (int j = 0; j < DIMENSIONS; j++)
          centre[j] = (float) (centreRandom.nextGaussian() * 3.0);
      final float[][] vectors = new float[count][DIMENSIONS];
      for (int i = 0; i < count; i++) {
        final float[] centre = centres[random.nextInt(CLUSTERS)];
        for (int j = 0; j < DIMENSIONS; j++)
          vectors[i][j] = centre[j] + (float) random.nextGaussian();
      }
      return vectors;
    }

    private static Set<Integer> nearest(final float[][] base, final float[] query) {
      final double[] distance = new double[base.length];
      final Integer[] order = new Integer[base.length];
      for (int i = 0; i < base.length; i++) {
        double sum = 0;
        for (int j = 0; j < DIMENSIONS; j++) {
          final double d = query[j] - base[i][j];
          sum += d * d;
        }
        distance[i] = sum;
        order[i] = i;
      }
      Arrays.sort(order, Comparator.comparingDouble(i -> distance[i]));
      final Set<Integer> top = new HashSet<>();
      for (int i = 0; i < K; i++)
        top.add(order[i]);
      return top;
    }
  }

  /**
   * Regression for #10786: pq codes bound to their ordinal map.
   * <p>
   * Issue #10786: the PQ search paired the published graph with the PQ codes in hand by COUNT only. A rebuild that keeps the
   * count but reassigns the ordinals (one delete and one insert) publishes its graph and ordinal map first and its codes only
   * once the graph is persisted, and in between every search walked the new graph through the previous build's codes: each
   * ordinal scored with the code of another vector, silently. The codes now answer only for the ordinal map they were encoded
   * with, which a search pins beside the graph.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @Tag("vector")
  class PqCodesBoundToTheirOrdinalMap extends TestHelper {

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

  /**
   * Regression for #10847: pq codebook reuse.
   * <p>
   * Issue #10847: every graph rebuild of a PRODUCT-quantized index trained a new codebook and encoded every vector again, also
   * when a few percent of them had changed. The codebook is now kept while it fits the data - the index has not outgrown it
   * ({@link GlobalConfiguration#VECTOR_INDEX_PQ_RETRAIN_GROWTH_FACTOR}) and the vectors added since are not encoded measurably
   * worse than the ones it was trained on ({@link GlobalConfiguration#VECTOR_INDEX_PQ_RETRAIN_ERROR_RATIO}) - and only the
   * vectors added since are encoded.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @Tag("vector")
  class PqCodebookReuse extends TestHelper {
    private static final int               DIMENSIONS = 32;
    private static final int               VECTORS    = 1100;
    private static final VectorTypeSupport VTS        = VectorApiSupport.jvectorProvider().getVectorTypeSupport();

    private final Random             random  = new Random(10847);
    private final List<RID>          rids    = new ArrayList<>();
    private final Map<RID, float[]>  vectors = new HashMap<>();

    /** A rebuild after a few percent of churn keeps the codebook, carries every surviving code over and encodes the rest. */
    @Test
    void aRebuildAfterLittleChurnKeepsTheCodebookAndEncodesOnlyTheNewVectors() {
      final LSMVectorIndex index = createIndex();
      insert(VECTORS, false);
      index.buildVectorGraphNow();
      assertThat(index.getLastPqReusedCodes()).as("the first build trains").isZero();
      final LSMVectorIndex.PQCodes first = index.pqCodesForTest();
      assertThat(first.stats().trainedAtCount()).isEqualTo(VECTORS);
      assertThat(first.stats().baselineError()).isPositive();

      // 5% NEW, 10 DELETED
      insert(55, false);
      delete(10);
      index.buildVectorGraphNow();

      final LSMVectorIndex.PQCodes second = index.pqCodesForTest();
      assertThat(second.quantization()).as("the codebook is kept").isSameAs(first.quantization());
      assertThat(second.stats()).as("and so are the statistics it was trained with").isEqualTo(first.stats());
      assertThat(index.getLastPqReusedCodes()).as("every surviving vector keeps its code").isEqualTo(VECTORS - 10);
      assertThat(second.codes().count()).isEqualTo(VECTORS + 55 - 10);

      // A carried-over code is the code the codebook gives the vector: each ordinal is checked against a fresh encoding
      assertCodesAreTheEncodingOfTheirVectors(index, second);
      assertNearestIsItself(index);
    }

    /** Past the growth factor the codebook is trained again and every vector encoded again. */
    @Test
    void anIndexThatOutgrewItsCodebookTrainsANewOne() {
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_PQ_RETRAIN_GROWTH_FACTOR, 1.5f);
      final LSMVectorIndex index = createIndex();
      insert(VECTORS, false);
      index.buildVectorGraphNow();
      final ProductQuantization first = index.pqCodesForTest().quantization();

      insert(VECTORS / 2 + 10, false);
      index.buildVectorGraphNow();

      assertThat(index.pqCodesForTest().quantization()).isNotSameAs(first);
      assertThat(index.getLastPqReusedCodes()).isZero();
      assertThat(index.pqCodesForTest().stats().trainedAtCount()).isEqualTo(VECTORS + VECTORS / 2 + 10);
    }

    /**
     * New vectors from another distribution are encoded far worse than the ones the codebook was trained on: the codebook no
     * longer fits, and is trained again although the index has barely grown.
     */
    @Test
    void aDriftedDistributionTrainsANewCodebook() {
      final LSMVectorIndex index = createIndex();
      insert(VECTORS, false);
      index.buildVectorGraphNow();
      final ProductQuantization first = index.pqCodesForTest().quantization();

      insert(100, true);
      index.buildVectorGraphNow();

      assertThat(index.pqCodesForTest().quantization()).as("drift retrains").isNotSameAs(first);
      assertThat(index.getLastPqReusedCodes()).isZero();
      assertNearestIsItself(index);
    }

    /** With the error test off, the same drift keeps the codebook: the growth factor alone decides. */
    @Test
    void theErrorTestCanBeTurnedOff() {
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_PQ_RETRAIN_ERROR_RATIO, 0f);
      final LSMVectorIndex index = createIndex();
      insert(VECTORS, false);
      index.buildVectorGraphNow();
      final ProductQuantization first = index.pqCodesForTest().quantization();

      insert(100, true);
      index.buildVectorGraphNow();

      assertThat(index.pqCodesForTest().quantization()).isSameAs(first);
      assertThat(index.getLastPqReusedCodes()).isEqualTo(VECTORS);
    }

    /** A growth factor of 0 retrains on every rebuild, the behavior before #10847. */
    @Test
    void aZeroGrowthFactorRetrainsOnEveryRebuild() {
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_PQ_RETRAIN_GROWTH_FACTOR, 0f);
      final LSMVectorIndex index = createIndex();
      insert(VECTORS, false);
      index.buildVectorGraphNow();
      final ProductQuantization first = index.pqCodesForTest().quantization();

      insert(5, false);
      index.buildVectorGraphNow();

      assertThat(index.pqCodesForTest().quantization()).isNotSameAs(first);
      assertThat(index.getLastPqReusedCodes()).isZero();
    }

    /** The statistics travel with the PQ file, so the first rebuild after a reopen keeps the codebook too. */
    @Test
    void theCodebookIsKeptAcrossAReopen() {
      createIndex();
      insert(VECTORS, false);
      indexOf().buildVectorGraphNow();
      final LSMVectorIndexPQFile.CodebookStats stats = indexOf().pqCodesForTest().stats();

      reopenDatabase();
      final LSMVectorIndex reopened = indexOf();
      // The first search loads the graph and adopts the codes read from the file for it
      assertNearestIsItself(reopened);
      assertThat(reopened.pqCodesForTest().stats()).as("read back from the PQ file").isEqualTo(stats);
      final ProductQuantization loaded = reopened.pqCodesForTest().quantization();

      insert(20, false);
      reopened.buildVectorGraphNow();

      assertThat(reopened.pqCodesForTest().quantization()).isSameAs(loaded);
      assertThat(reopened.getLastPqReusedCodes()).isEqualTo(VECTORS);
      assertNearestIsItself(reopened);
    }

    /** {@code REBUILD INDEX} builds a new index, and with it a new codebook. */
    @Test
    void rebuildIndexTrainsANewCodebook() {
      createIndex();
      insert(VECTORS, false);
      indexOf().buildVectorGraphNow();
      final ProductQuantization first = indexOf().pqCodesForTest().quantization();

      database.command("sql", "REBUILD INDEX `Embedding[vector]`");
      // rebuilt in place: the compaction that ends the rebuild builds the graph right away, with a codebook of its own
      final LSMVectorIndex rebuilt = indexOf();
      assertThat(rebuilt.pqCodesForTest()).isNotNull();
      assertThat(rebuilt.pqCodesForTest().quantization()).isNotSameAs(first);
      assertThat(rebuilt.getLastPqReusedCodes()).isZero();
    }

    /**
     * A compaction renumbers every vector id densely. The codes in hand are re-bound to the renumbered map, so the next build
     * still carries each code over to the vector it encodes, never to the vector that inherited its old id.
     */
    @Test
    void codesCarriedOverAcrossARenumberingCompactionStillEncodeTheirVectors() {
      createIndex();
      insert(VECTORS, false);
      indexOf().buildVectorGraphNow();

      // THE LOWEST IDS GO, SO THE RENUMBERING SHIFTS EVERY SURVIVOR DOWN
      delete(50);
      database.command("sql", "COMPACT INDEX `Embedding[vector]`");
      final LSMVectorIndex index = indexOf();
      assertThat(index.getVectorIndex().getMaxVectorId()).as("the compaction renumbered the live set")
          .isEqualTo(index.getVectorIndex().size() - 1);

      insert(20, false);
      index.buildVectorGraphNow();

      assertThat(index.getLastPqReusedCodes()).as("the surviving codes are carried over").isPositive();
      assertCodesAreTheEncodingOfTheirVectors(index, index.pqCodesForTest());
      assertNearestIsItself(index);
    }

    /**
     * Codes bound to a map other than the resident one name their vectors by ids a renumbering reissues: they lose their
     * binding, and the next build trains a new codebook instead of carrying them over.
     */
    @Test
    void codesBoundToAnotherMapAreNotCarriedOverAcrossARenumbering() {
      createIndex();
      insert(VECTORS, false);
      final LSMVectorIndex built = indexOf();
      built.buildVectorGraphNow();
      final LSMVectorIndex.PQCodes held = built.pqCodesForTest();
      // Same content, another array: bound to a map that is not the resident one
      built.setPqCodesForTest(held.boundTo(held.ordinalMap().clone()));

      delete(50);
      database.command("sql", "COMPACT INDEX `Embedding[vector]`");
      final LSMVectorIndex index = indexOf();
      final LSMVectorIndex.PQCodes afterCompaction = index.pqCodesForTest();
      assertThat(afterCompaction.ordinalMap() == null || afterCompaction.ordinalMap() == index.getOrdinalToVectorIdForTest())
          .as("never left bound to the map of the reissued ids").isTrue();

      insert(20, false);
      index.buildVectorGraphNow();

      assertThat(index.pqCodesForTest().quantization()).as("a new codebook, nothing carried over by stale ids")
          .isNotSameAs(held.quantization());
      assertCodesAreTheEncodingOfTheirVectors(index, index.pqCodesForTest());
    }

    /** The merge of two ascending ordinal maps: a vector id found in both names its previous ordinal, any other one -1. */
    @Test
    void previousOrdinalsAreMatchedByVectorId() {
      assertThat(LSMVectorIndex.previousOrdinalsOf(new int[] { 1, 3, -1, 7, 9 }, new int[] { 0, 1, 7, 8, 9, 12 }))
          .containsExactly(-1, 0, 3, -1, 4, -1);
      assertThat(LSMVectorIndex.previousOrdinalsOf(new int[0], new int[] { 4 })).containsExactly(-1);
    }

    private void assertCodesAreTheEncodingOfTheirVectors(final LSMVectorIndex index, final LSMVectorIndex.PQCodes codes) {
      final int[] ordinalMap = codes.ordinalMap();
      final VectorLocationIndex locations = index.residentLocationsForTest();
      assertThat(locations).as("the build materialised the locations").isNotNull();
      for (int ordinal = 0; ordinal < ordinalMap.length; ordinal++) {
        final float[] vector = vectors.get(locations.getRid(ordinalMap[ordinal]));
        assertThat(vector).as("ordinal %d names a live vector", ordinal).isNotNull();
        final ByteSequence<?> expected = codes.quantization().encode(VTS.createFloatVector(vector));
        final ByteSequence<?> actual = codes.codes().get(ordinal);
        for (int k = 0; k < expected.length(); k++)
          assertThat(actual.get(k)).as("ordinal %d, subspace %d", ordinal, k).isEqualTo(expected.get(k));
      }
    }

    private void assertNearestIsItself(final LSMVectorIndex index) {
      final RID probe = rids.get(rids.size() / 2);
      final List<Pair<RID, Float>> rows = index.findNeighborsFromVectorApproximate(vectors.get(probe), 10, -1, 0);
      assertThat(rows).isNotEmpty();
      assertThat(rows.getFirst().getFirst()).isEqualTo(probe);
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

    private void insert(final int count, final boolean drifted) {
      database.transaction(() -> {
        for (int i = 0; i < count; i++) {
          final float[] vector = drifted ? driftedVector() : randomUnitVector();
          final RID rid = database.newVertex("Embedding").set("vector", vector).save().getIdentity();
          rids.add(rid);
          vectors.put(rid, vector);
        }
      });
    }

    private void delete(final int count) {
      database.transaction(() -> {
        for (int i = 0; i < count; i++) {
          final RID rid = rids.removeFirst();
          vectors.remove(rid);
          rid.asDocument().delete();
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

    /** Far from every vector the codebook was trained on: a unit vector moved to around (5, 5, ..., 5). */
    private float[] driftedVector() {
      final float[] vector = randomUnitVector();
      for (int i = 0; i < DIMENSIONS; i++)
        vector[i] += 5;
      return vector;
    }
  }

  /**
   * Regression for #10993: size from held vectors before aput.
   * <p>
   * Issue #10993: a node that holds the vectors of an index created to take its dimensions from its first vector, but did not see
   * the size decided (a follower promoted by a failover: a commit of records ships no schema, so it never learned the size), has
   * to refuse a vector of another length as any sized index does, and not size the index a second time from it.
   */
  @Nested
  class SizeFromHeldVectorsBeforeAPut extends TestHelper {

    @Test
    void aVectorOfAnotherLengthIsRefusedByANodeThatDidNotSeeTheSizeDecided() {
      database.command("opencypher", "CREATE VECTOR INDEX emb FOR (n:Doc) ON n.embedding");
      database.transaction(() -> {
        for (int i = 0; i < 5; i++)
          database.newVertex("Doc").set("id", i, "embedding", new float[] { i + 1, 2, 3, 4 }).save();
      });

      final LSMVectorIndex index = indexOf();
      assertThat(index.getDimensions()).isEqualTo(4);
      // a follower that applied the replicated pages and never saw the put
      index.getMetadata().dimensions = 0;
      index.getMetadata().sizeOnFirstVector = true;

      assertThatThrownBy(() -> database.transaction(() -> database.newVertex("Doc").set("id", 100, "embedding", new float[] { 1, 2 }).save()))
          .hasStackTraceContaining("Vector dimension does not match index dimension 4");
      assertThat(index.getDimensions()).isEqualTo(4);

      database.transaction(() -> database.newVertex("Doc").set("id", 101, "embedding", new float[] { 1, 2, 3, 5 }).save());
      assertThat(index.getDimensions()).isEqualTo(4);
    }

    /**
     * Issue #11141: the batch entry point (commit replay, an index built over existing records) decides the size too, and must
     * derive it from the vectors the node holds before it lets the first vector of the batch decide.
     */
    @Test
    void aBatchOfAnotherLengthIsRefusedByANodeThatDidNotSeeTheSizeDecided() {
      database.command("opencypher", "CREATE VECTOR INDEX emb FOR (n:Doc) ON n.embedding");
      database.transaction(() -> {
        for (int i = 0; i < 5; i++)
          database.newVertex("Doc").set("id", i, "embedding", new float[] { i + 1, 2, 3, 4 }).save();
      });

      final LSMVectorIndex index = indexOf();
      final long before = index.countEntries();
      index.getMetadata().dimensions = 0;
      index.getMetadata().sizeOnFirstVector = true;

      index.putBatch(List.<Object[]>of(new Object[] { new float[] { 1, 2 } }), List.of(new RID(0, 999)));

      assertThat(index.getDimensions()).isEqualTo(4);
      assertThat(index.countEntries()).isEqualTo(before);
    }

    @Test
    void anIndexHoldingNoVectorIsStillSizedByTheFirstOneItReceives() {
      database.command("opencypher", "CREATE VECTOR INDEX emb FOR (n:Doc) ON n.embedding");
      assertThat(indexOf().getDimensions()).isZero();

      database.transaction(() -> database.newVertex("Doc").set("id", 1, "embedding", new float[] { 1, 2, 3 }).save());

      assertThat(indexOf().getDimensions()).isEqualTo(3);
    }

    private LSMVectorIndex indexOf() {
      for (final IndexInternal bucketIndex : ((TypeIndex) database.getSchema().getIndexByName("emb")).getIndexesOnBuckets())
        if (bucketIndex instanceof LSMVectorIndex vector)
          return vector;
      throw new AssertionError("no vector index behind emb");
    }
  }

  /**
   * Regression for #10993: sized on first vector product quantization.
   * <p>
   * Issue #10993: a {@code quantization: 'PRODUCT'} index created without {@code vector.dimensions} fixes its dimensions on the
   * first vector it receives (issue #10915), and {@code pqSubspaces} is computed at graph build from that decided dimension. No
   * test built a PQ graph over an index sized this way and looked at the codebook it got: a build that ran before the size was
   * known would derive the subspaces from 0 dimensions and train nothing, silently, since PQ is optional and its failure is only
   * logged.
   */
  @Nested
  class SizedOnFirstVectorProductQuantization extends TestHelper {
    private static final int DIMENSIONS = 64;
    private static final int VECTORS    = 300;
    private static final int CLUSTERS   = 16;

    @Test
    void theCodebookOfAnIndexSizedOnItsFirstVectorIsTrainedOnTheDecidedDimensions() {
      createIndexAndLoad();
      final LSMVectorIndex index = indexOf();
      assertThat(index.getDimensions()).isEqualTo(DIMENSIONS);

      index.buildVectorGraphNow();

      assertPqGraph(index);
      assertThat(index.findNeighborsFromVector(vector(7), 1)).extracting(Pair::getFirst).containsExactly(ridOf(7));
    }

    @Test
    void theCodebookIsTrainedOnTheDimensionsDerivedFromTheDataWhenTheNodeDidNotSeeThePut() {
      createIndexAndLoad();
      final LSMVectorIndex index = indexOf();

      // what a follower applying replicated pages, or a database recovered before the schema was saved, holds
      index.getMetadata().dimensions = 0;
      index.getMetadata().sizeOnFirstVector = true;

      index.buildVectorGraphNow();

      assertThat(index.getDimensions()).isEqualTo(DIMENSIONS);
      assertPqGraph(index);
    }

    @Test
    void theCodebookIsTrainedWhenTheFirstSearchOfAReopenedDatabaseBuildsTheGraph() {
      createIndexAndLoad();

      reopenDatabase();

      final LSMVectorIndex index = indexOf();
      assertThat(index.getDimensions()).isEqualTo(DIMENSIONS);
      assertThat(index.findNeighborsFromVector(vector(11), 1)).extracting(Pair::getFirst).containsExactly(ridOf(11));
      assertPqGraph(index);
    }

    private void assertPqGraph(final LSMVectorIndex index) {
      assertThat(index.isPQSearchAvailable()).as("a PQ codebook was trained").isTrue();
      final PQVectors codes = index.pqCodesForPublishedGraphForTest();
      assertThat(codes).as("codes laid out by the published graph").isNotNull();
      final ProductQuantization quantization = codes.getCompressor();
      // dimensions / 4 subspaces, the default
      assertThat(quantization.getSubspaceCount()).isEqualTo(DIMENSIONS / 4);
      assertThat(quantization.getOriginalDimension()).isEqualTo(DIMENSIONS);
      assertThat(codes.count()).isEqualTo(VECTORS);
    }

    private void createIndexAndLoad() {
      database.command("opencypher",
          "CREATE VECTOR INDEX emb FOR (n:Doc) ON n.embedding OPTIONS {indexConfig: {`vector.similarity_function`: 'euclidean', "
              + "quantization: 'PRODUCT', pqClusters: " + CLUSTERS + "}}");
      assertThat(indexOf().getDimensions()).isZero();
      assertThat(indexOf().getMetadata().sizeOnFirstVector).isTrue();

      database.transaction(() -> {
        for (int i = 0; i < VECTORS; i++)
          database.newVertex("Doc").set("id", i, "embedding", vector(i)).save();
      });
    }

    private RID ridOf(final int id) {
      return database.query("sql", "SELECT FROM Doc WHERE id = ?", id).next().getIdentity().get();
    }

    private LSMVectorIndex indexOf() {
      for (final IndexInternal bucketIndex : ((TypeIndex) database.getSchema().getIndexByName("emb")).getIndexesOnBuckets())
        if (bucketIndex instanceof LSMVectorIndex vector)
          return vector;
      throw new AssertionError("no vector index behind emb");
    }

    private static float[] vector(final int id) {
      final Random random = new Random(10993L * 31 + id);
      final float[] vector = new float[DIMENSIONS];
      for (int d = 0; d < DIMENSIONS; d++)
        vector[d] = (float) random.nextGaussian();
      return vector;
    }
  }

  /**
   * Regression for #10543: quantized graph build.
   * <p>
   * Issue #10543: a graph build over an INT8 or BINARY index whose corpus does not fit the float cache re-read the
   * missing vectors from the index pages, dequantized each into a new array and published it into the cache, evicting an
   * older one. On a 13.8M x 768 corpus in a 24 GB heap that churn kept the heap between 17 and 24 GB and the build
   * advanced one vector every five seconds, until it was killed after 21 hours.
   * <p>
   * The build now keeps the quantized rows themselves, four times smaller for INT8, in arrays that are written once
   * (see {@link QuantizedBuildVectors}), and reads the ones that do not fit into a scratch vector. These tests pin what
   * that has to preserve: the graph is as good as the one built from float vectors, and what it did not need to read
   * from the pages it did not read.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class QuantizedGraphBuild extends TestHelper {
    private static final int DIMENSIONS  = 32;
    private static final int NUM_VECTORS = 600;

    private final int previousCacheSize = GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.getValueAsInteger();

    @AfterEach
    void restoreConfiguration() {
      GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.setValue(previousCacheSize);
    }

    /** A float cache of 150 vectors costs 150 x (32 x 4 + 64) bytes, which holds all 600 quantized rows of 40 bytes. */
    @Test
    void int8RowsThatFitTheBudgetAreAllResidentAndNoPageIsReadDuringTheBuild() {
      final float[][] vectors = createIndexedType("Resident", VectorQuantizationType.INT8, NUM_VECTORS, 1);
      final LSMVectorIndex lsm = indexOf("Resident");

      GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.setValue(150);
      final long fetchesBefore = lsm.metrics.getVectorFetchFromQuantized();
      lsm.buildGraphFromScratchReleasingResidentGraph(null);

      assertThat(lsm.metrics.getGraphBuildQuantizedResident())
          .as("the whole corpus is kept as quantized rows")
          .isEqualTo(NUM_VECTORS);
      assertThat(lsm.metrics.getVectorFetchFromQuantized() - fetchesBefore)
          .as("a build that holds the corpus in memory has no reason to read a vector from the index pages")
          .isZero();

      assertThat(selfRecall(lsm, vectors)).as("self-recall of the graph built from the quantized rows")
          .isGreaterThanOrEqualTo((int) (NUM_VECTORS * 0.9));
    }

    /** 20 float vectors' worth of budget buys 96 quantized rows of the 600: the remaining ones are read from the pages. */
    @Test
    void rowsThatDoNotFitTheBudgetAreReadFromThePagesAndTheGraphIsStillRight() {
      final float[][] vectors = createIndexedType("Partial", VectorQuantizationType.INT8, NUM_VECTORS, 2);
      final LSMVectorIndex lsm = indexOf("Partial");

      GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.setValue(20);
      final long fetchesBefore = lsm.metrics.getVectorFetchFromQuantized();
      lsm.buildGraphFromScratchReleasingResidentGraph(null);

      final long resident = lsm.metrics.getGraphBuildQuantizedResident();
      assertThat(resident).as("what 20 float vectors' budget buys in quantized rows").isEqualTo(96L);
      assertThat(lsm.metrics.getVectorFetchFromQuantized() - fetchesBefore)
          .as("the rows that are not resident are read from the pages")
          .isGreaterThan(0L);

      assertThat(selfRecall(lsm, vectors)).as("self-recall of the graph built with most rows read from the pages")
          .isGreaterThanOrEqualTo((int) (NUM_VECTORS * 0.9));
    }

    /** A store that cannot hold even one row must not break the build: everything is read from the pages. */
    @Test
    void aBudgetTooSmallForASingleRowStillBuilds() {
      final float[][] vectors = createIndexedType("Tiny", VectorQuantizationType.INT8, 200, 3);
      final LSMVectorIndex lsm = indexOf("Tiny");

      GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.setValue(1);
      lsm.buildGraphFromScratchReleasingResidentGraph(null);

      assertThat(lsm.metrics.getGraphBuildQuantizedResident()).isLessThanOrEqualTo(4L);
      assertThat(selfRecall(lsm, vectors)).isGreaterThanOrEqualTo((int) (200 * 0.9));
    }

    /** A float cache that already holds the corpus stays what it was: the fastest path for an index that fits. */
    @Test
    void aCorpusThatFitsTheFloatCacheKeepsFloatVectors() {
      final float[][] vectors = createIndexedType("Fits", VectorQuantizationType.INT8, NUM_VECTORS, 4);
      final LSMVectorIndex lsm = indexOf("Fits");

      GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.setValue(NUM_VECTORS * 2);
      lsm.buildGraphFromScratchReleasingResidentGraph(null);

      assertThat(lsm.metrics.getGraphBuildQuantizedResident()).as("no quantized store when the float cache holds it all").isZero();
      assertThat(selfRecall(lsm, vectors)).isGreaterThanOrEqualTo((int) (NUM_VECTORS * 0.9));
    }

    /** Without quantization the vectors live in the documents, which the store cannot stand in for. */
    @Test
    void anIndexWithoutQuantizationDoesNotUseTheStore() {
      final float[][] vectors = createIndexedType("Plain", VectorQuantizationType.NONE, 300, 5);
      final LSMVectorIndex lsm = indexOf("Plain");

      GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.setValue(20);
      lsm.buildGraphFromScratchReleasingResidentGraph(null);

      assertThat(lsm.metrics.getGraphBuildQuantizedResident()).isZero();
      assertThat(selfRecall(lsm, vectors)).isGreaterThanOrEqualTo((int) (300 * 0.9));
    }

    /** A BINARY row is 4 bytes of bits and a 4-byte median for 32 dimensions: 40 float vectors' budget keeps 960 of them. */
    @Test
    void binaryRowsAreKeptToo() {
      createIndexedType("Bits", VectorQuantizationType.BINARY, NUM_VECTORS, 6);
      final LSMVectorIndex lsm = indexOf("Bits");

      GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.setValue(40);
      final long fetchesBefore = lsm.metrics.getVectorFetchFromQuantized();
      lsm.buildGraphFromScratchReleasingResidentGraph(null);

      assertThat(lsm.metrics.getGraphBuildQuantizedResident()).isEqualTo(NUM_VECTORS);
      assertThat(lsm.metrics.getVectorFetchFromQuantized() - fetchesBefore).isZero();

      database.transaction(() -> {
        final float[] query = new float[DIMENSIONS];
        for (int i = 0; i < DIMENSIONS; i++)
          query[i] = i % 2 == 0 ? 1f : -1f;
        assertThat(lsm.findNeighborsFromVector(query, 5)).isNotEmpty();
      });
    }

    /** 5 float vectors' budget keeps 120 BINARY rows of the 600: the rest come from the pages, as packed bits. */
    @Test
    void binaryRowsThatDoNotFitAreReadFromThePagesToo() {
      createIndexedType("BitsPartial", VectorQuantizationType.BINARY, NUM_VECTORS, 11);
      final LSMVectorIndex lsm = indexOf("BitsPartial");

      GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.setValue(5);
      final long fetchesBefore = lsm.metrics.getVectorFetchFromQuantized();
      lsm.buildGraphFromScratchReleasingResidentGraph(null);

      assertThat(lsm.metrics.getGraphBuildQuantizedResident()).isEqualTo(120L);
      assertThat(lsm.metrics.getVectorFetchFromQuantized() - fetchesBefore).isGreaterThan(0L);

      database.transaction(() -> {
        final float[] query = new float[DIMENSIONS];
        for (int i = 0; i < DIMENSIONS; i++)
          query[i] = i % 3 == 0 ? 1f : -1f;
        assertThat(lsm.findNeighborsFromVector(query, 5)).hasSize(5);
      });
    }

    /** The vectors of a document type the build can not use are left out of the corpus, whatever store holds the rest. */
    @Test
    void anAllZeroVectorIsLeftOutOfTheGraphLikeBefore() {
      final float[][] vectors = createIndexedType("WithZero", VectorQuantizationType.INT8, 200, 7);
      final LSMVectorIndex lsm = indexOf("WithZero");
      database.transaction(() -> database.newDocument("WithZero").set("id", 100_000).set("embedding", new float[DIMENSIONS]).save());

      GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.setValue(100);
      lsm.buildGraphFromScratchReleasingResidentGraph(null);

      // 200 real vectors; the all-zero one cannot be scored against under COSINE
      assertThat(lsm.metrics.getGraphBuildQuantizedResident()).isEqualTo(200L);
      assertThat(selfRecall(lsm, vectors)).isGreaterThanOrEqualTo((int) (200 * 0.9));
    }

    /**
     * What the fallback costs per vector. The page view is the page manager's and is the same for both reads; the old
     * read added a new float array, the quantized bytes and their metadata on top of it.
     */
    @Test
    void readingARowFromThePageAllocatesAFractionOfDequantizingTheVector() throws Exception {
      final int wide = 768;
      createIndexedType("Wide", VectorQuantizationType.INT8, 30, 10, wide);
      final LSMVectorIndex lsm = indexOf("Wide");

      final Method vectorIndex = LSMVectorIndex.class.getDeclaredMethod("vectorIndex");
      vectorIndex.setAccessible(true);
      final VectorLocationIndex locations = (VectorLocationIndex) vectorIndex.invoke(lsm);
      final long offsetAndFlag = locations.getOffsetAndFlag(locations.getAllVectorIds().findFirst().orElseThrow());
      final long offset = VectorLocationIndex.offsetOf(offsetAndFlag);
      final boolean compacted = VectorLocationIndex.isCompactedOf(offsetAndFlag);

      final byte[] row = new byte[wide];
      final float[] scales = new float[2];
      final Object managementBean = ManagementFactory.getThreadMXBean();
      assumeTrue(managementBean instanceof ThreadMXBean, "per-thread allocation counters are unavailable on this JVM");
      final ThreadMXBean threads = (ThreadMXBean) managementBean;
      assumeTrue(threads.isThreadAllocatedMemorySupported(), "per-thread allocation counters are unsupported on this JVM");
      threads.setThreadAllocatedMemoryEnabled(true);
      assumeTrue(threads.isThreadAllocatedMemoryEnabled(), "per-thread allocation counters are disabled on this JVM");
      final int rounds = 20_000;
      for (int i = 0; i < rounds; i++) { // warm up
        assertThat(lsm.readQuantizedRowFromOffset(offset, compacted, row, 0, scales)).isEqualTo(LSMVectorIndex.ROW_OK);
        assertThat(lsm.readVectorFromOffset(offset, compacted)).hasSize(wide);
      }

      final long rowBefore = threads.getCurrentThreadAllocatedBytes();
      for (int i = 0; i < rounds; i++)
        lsm.readQuantizedRowFromOffset(offset, compacted, row, 0, scales);
      final long rowPerRead = (threads.getCurrentThreadAllocatedBytes() - rowBefore) / rounds;

      final long vectorBefore = threads.getCurrentThreadAllocatedBytes();
      for (int i = 0; i < rounds; i++)
        lsm.readVectorFromOffset(offset, compacted);
      final long vectorPerRead = (threads.getCurrentThreadAllocatedBytes() - vectorBefore) / rounds;

      assertThat(vectorPerRead).as("the vector read allocates its float array: 768 x 4 bytes and more").isGreaterThan(3_000L);
      assertThat(rowPerRead).as("the row read allocates the page view and nothing else").isLessThan(vectorPerRead / 4);
      // and it reads the same bytes: the row dequantizes to exactly the vector the other read returns
      final float[] expected = lsm.readVectorFromOffset(offset, compacted);
      final float[] actual = new float[wide];
      QuantizedBuildVectors.dequantizeInt8(row, 0, wide, scales[0], scales[1], actual);
      assertThat(actual).containsExactly(expected);
    }

    private float[][] createIndexedType(final String typeName, final VectorQuantizationType quantization, final int count,
        final long seed) {
      return createIndexedType(typeName, quantization, count, seed, DIMENSIONS);
    }

    private float[][] createIndexedType(final String typeName, final VectorQuantizationType quantization, final int count,
        final long seed, final int dimensions) {
      final float[][] vectors = new float[count][];
      final Random rng = new Random(seed);

      database.transaction(() -> {
        final DocumentType docType = database.getSchema().createDocumentType(typeName);
        docType.createProperty("id", Type.INTEGER);
        docType.createProperty("embedding", Type.ARRAY_OF_FLOATS);

        database.getSchema().buildTypeIndex(typeName, new String[] { "embedding" })
            .withLSMVectorType()
            .withDimensions(dimensions)
            .withSimilarity("COSINE")
            .withQuantization(quantization)
            .create();

        for (int i = 0; i < count; i++) {
          vectors[i] = randomNormalized(rng, dimensions);
          database.newDocument(typeName).set("id", i).set("embedding", vectors[i]).save();
        }
      });
      return vectors;
    }

    private LSMVectorIndex indexOf(final String typeName) {
      final TypeIndex idx = (TypeIndex) database.getSchema().getIndexByName(typeName + "[embedding]");
      return (LSMVectorIndex) idx.getIndexesOnBuckets()[0];
    }

    /** How many vectors find themselves as their own nearest neighbour. */
    private int selfRecall(final LSMVectorIndex lsm, final float[][] vectors) {
      final int[] selfIsTop = { 0 };
      database.transaction(() -> {
        for (int i = 0; i < vectors.length; i++) {
          final List<Pair<RID, Float>> neighbors = lsm.findNeighborsFromVector(vectors[i], 3);
          assertThat(neighbors).as("query %d returns results", i).isNotEmpty();
          final int topId = database.lookupByRID(neighbors.get(0).getFirst(), true).asDocument().getInteger("id");
          if (topId == i)
            selfIsTop[0]++;
        }
      });
      return selfIsTop[0];
    }

    private static float[] randomNormalized(final Random rng, final int dimensions) {
      final float[] v = new float[dimensions];
      double norm = 0;
      for (int i = 0; i < dimensions; i++) {
        v[i] = (float) rng.nextGaussian();
        norm += v[i] * (double) v[i];
      }
      norm = Math.sqrt(norm);
      for (int i = 0; i < dimensions; i++)
        v[i] /= (float) norm;
      return v;
    }
  }

  /**
   * Regression for #10568: product reopen ordinal map.
   * <p>
   * Regression test for issue #10568: a PRODUCT-quantized index with nothing deleted must reopen through the ordinal map
   * recorded next to its graph (issue #8852) instead of the per-vector document walk, and must not log that vectors were
   * deleted when none were. With a deletion the PRODUCT index keeps the rebuild (issue #7842), and an index persisted
   * without a map still opens.
   */
  @Nested
  @Tag("vector")
  class ProductReopenOrdinalMap {
    private static final String DB_ROOT     = "target/test-databases/Issue10568ProductReopenOrdinalMapTest";
    private static final int    DIMENSIONS  = 32;
    private static final int    NUM_VECTORS = 2_000;
    private static final VectorTypeSupport VTS = VectorizationProvider.getInstance().getVectorTypeSupport();

    private static final String FALSE_LINE  = "Deleted vectors detected";
    private static final String WALK_LINE   = "Loaded graph from disk for index";

    private String dbPath;

    @BeforeEach
    void setUp(final TestInfo testInfo) {
      dbPath = DB_ROOT + "-" + testInfo.getTestMethod().orElseThrow().getName();
      FileUtils.deleteRecursively(new File(dbPath));
    }

    @AfterEach
    void tearDown() {
      FileUtils.deleteRecursively(new File(dbPath));
    }

    @Test
    void aProductIndexWithNothingDeletedReopensThroughItsOrdinalMap() {
      final Built built = build("PRODUCT");

      assertThat(built.ordinalMapSize).as("precondition: a PRODUCT index records the ordinal map of its graph")
          .isEqualTo(NUM_VECTORS);
      assertThat(built.pqFileExists).as("precondition: and has its PQ on disk").isTrue();

      final List<String> logs = new ArrayList<>();
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          final LSMVectorIndex index = vectorIndex(db);
          final List<Pair<RID, Float>> results = capturingLogs(logs, () -> index.findNeighborsFromVector(embedding(1), 10, 64));

          assertThat(neighbours(results)).as("the answer after the reopen is the one the session that built it gave")
              .isEqualTo(built.neighbours);
          assertThat(index.getStats().get("graphLoadsFromOrdinalMap"))
              .as("the ordinal map must replace the per-vector document walk for a PRODUCT index too").isEqualTo(1L);
          assertThat(index.getStats().get("graphRebuildCount")).isZero();
          assertThat(index.getStats().get("graphNodeCount")).isEqualTo((long) NUM_VECTORS);
          assertThat(logs).as("nothing was deleted, so nothing may say so").noneMatch(m -> m.contains(FALSE_LINE));
          assertThat(logs).as("and the walk that re-reads every vector's document must not have run")
              .noneMatch(m -> m.contains(WALK_LINE));
        } finally {
          db.drop();
        }
      }
    }

    @Test
    void aProductIndexWithADeletionStillRebuildsAndSaysWhy() {
      build("PRODUCT");

      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          db.begin();
          db.command("sql", "DELETE FROM Doc WHERE id = ?", 0);
          db.commit();

          ((DatabaseInternal) db).kill();
          db.close();
        } finally {
          if (db.isOpen())
            db.close();
        }
      }

      final List<String> logs = new ArrayList<>();
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          final LSMVectorIndex index = vectorIndex(db);
          assertThat(index.getStats().get("deletedVectors"))
              .as("precondition: read before the first search, which is what discards the tombstone set").isEqualTo(1L);

          final List<Pair<RID, Float>> results = capturingLogs(logs, () -> index.findNeighborsFromVector(embedding(1), 5, 64));

          assertThat(results).hasSize(5);
          assertThat(index.getStats().get("graphLoadsFromOrdinalMap"))
              .as("a deletion must never take the exact-cover path").isZero();
          assertThat(index.getStats().get("graphReusesWithTombstonedNodes"))
              .as("a PRODUCT index must never take the tombstone-tolerant reuse path").isZero();
          assertThat(index.getStats().get("graphRebuildCount"))
              .as("it pays the full rebuild, which keeps its codebook and its graph on one dense ordinal space")
              .isEqualTo(1L);
          assertThat(logs).as("and the line about the deletion is still there")
              .anyMatch(m -> m.contains(FALSE_LINE) && m.contains("PRODUCT quantization"));
        } finally {
          db.drop();
        }
      }
    }

    @Test
    void aProductIndexPersistedWithoutAMapStillOpensThroughTheWalk() throws IOException {
      final Built built = build("PRODUCT");

      // What an index persisted before the map was recorded for PRODUCT looks like: the same manifest, no .vecordmap
      final Path map = new LSMVectorIndexGraphManifest(built.graphPath.toString()).getOrdinalMapFilePath();
      assertThat(Files.deleteIfExists(map)).as("precondition: there was a map to remove").isTrue();

      final List<String> logs = new ArrayList<>();
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          final LSMVectorIndex index = vectorIndex(db);
          final List<Pair<RID, Float>> results = capturingLogs(logs, () -> index.findNeighborsFromVector(embedding(1), 10, 64));

          assertThat(neighbours(results)).as("the walk answers as it always did").isEqualTo(built.neighbours);
          assertThat(index.getStats().get("graphLoadsFromOrdinalMap")).as("there is no map to load through").isZero();
          assertThat(index.getStats().get("graphRebuildCount")).as("the persisted graph is reused, not rebuilt").isZero();
          assertThat(index.getStats().get("graphNodeCount")).isEqualTo((long) NUM_VECTORS);
          assertThat(logs).as("the walk ran").anyMatch(m -> m.contains(WALK_LINE));
          assertThat(logs).as("and nothing was deleted, so nothing may say so").noneMatch(m -> m.contains(FALSE_LINE));
        } finally {
          db.drop();
        }
      }
    }

    @Test
    void aNonQuantizedIndexStillReopensThroughItsOrdinalMap() {
      final Built built = build("NONE");

      final List<String> logs = new ArrayList<>();
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          final LSMVectorIndex index = vectorIndex(db);
          final List<Pair<RID, Float>> results = capturingLogs(logs, () -> index.findNeighborsFromVector(embedding(1), 10, 64));

          assertThat(neighbours(results)).isEqualTo(built.neighbours);
          assertThat(index.getStats().get("graphLoadsFromOrdinalMap")).isEqualTo(1L);
          assertThat(index.getStats().get("graphRebuildCount")).isZero();
          assertThat(logs).noneMatch(m -> m.contains(FALSE_LINE)).noneMatch(m -> m.contains(WALK_LINE));
        } finally {
          db.drop();
        }
      }
    }

    @Test
    void pqCodesNotOnePerOrdinalOfTheMapTakeTheWalk() throws IOException {
      final Built built = build("PRODUCT");

      // A PQ file of another generation of the graph that happens to carry the right identity: only the count tells
      final LSMVectorIndexPQFile pqFile = pqFileOf(built);
      assertThat(pqFile.loadPQ()).isTrue();
      final ProductQuantization pq = pqFile.getProductQuantization();
      final Long fingerprint = pqFile.graphFingerprintOf(pqFile.getPQVectors());
      assertThat(fingerprint).as("precondition: a PQ written with its graph records which graph that is").isNotNull();
      final MutablePQVectors fewer = new MutablePQVectors(pq);
      for (int i = 0; i < NUM_VECTORS - 100; i++)
        fewer.encodeAndSet(i, VTS.createFloatVector(embedding(i)));
      pqFile.writePQ(pq, fewer, fingerprint);

      final List<String> logs = new ArrayList<>();
      reopenAndSearch(logs, (index, results) -> {
        assertThat(results).hasSize(10);
        assertThat(index.getStats().get("graphLoadsFromOrdinalMap")).as("the count says these codes are not the map's")
            .isZero();
        assertThat(logs).anyMatch(m -> m.contains("PQ codes of index") && m.contains("not built for it"));
      });
    }

    @Test
    void pqCodesOfAnotherGraphOfTheSameSizeTakeTheWalk() throws IOException {
      final Built built = build("PRODUCT");

      // Same count, same codes, but recorded as built for a different assignment of ordinals
      final LSMVectorIndexPQFile pqFile = pqFileOf(built);
      assertThat(pqFile.loadPQ()).isTrue();
      final PQVectors codes = pqFile.getPQVectors();
      final long fingerprint = pqFile.graphFingerprintOf(codes);
      pqFile.writePQ(pqFile.getProductQuantization(), codes, fingerprint ^ 1L);

      final List<String> logs = new ArrayList<>();
      reopenAndSearch(logs, (index, results) -> {
        assertThat(neighbours(results)).isEqualTo(built.neighbours);
        assertThat(index.getStats().get("graphLoadsFromOrdinalMap"))
            .as("a count that matches is not an identity that matches").isZero();
        assertThat(index.getStats().get("graphRebuildCount")).as("the persisted graph is still reused, by the walk").isZero();
        assertThat(logs).anyMatch(m -> m.contains("were built for another graph than the persisted one"));
        assertThat(logs).as("the walk ran").anyMatch(m -> m.contains(WALK_LINE));
      });
    }

    @Test
    void aPqFileWithoutARecordedGraphTakesTheWalk() throws IOException {
      final Built built = build("PRODUCT");

      // What a PQ file written before the identity was recorded looks like: the same bytes without the 16-byte trailer
      final Path pq = built.pqPath;
      final byte[] bytes = Files.readAllBytes(pq);
      Files.write(pq, Arrays.copyOf(bytes, bytes.length - LSMVectorIndexPQFile.GRAPH_IDENTITY_BYTES));
      final LSMVectorIndexPQFile check = pqFileOf(built);
      assertThat(check.loadPQ()).as("precondition: it still loads, the trailer is not part of what JVector reads").isTrue();
      assertThat(check.graphFingerprintOf(check.getPQVectors())).isNull();

      final List<String> logs = new ArrayList<>();
      reopenAndSearch(logs, (index, results) -> {
        assertThat(neighbours(results)).isEqualTo(built.neighbours);
        assertThat(index.getStats().get("graphLoadsFromOrdinalMap")).isZero();
        assertThat(index.getStats().get("graphRebuildCount")).isZero();
        assertThat(logs).anyMatch(m -> m.contains("record no graph they were built for"));
        assertThat(logs).as("the walk ran").anyMatch(m -> m.contains(WALK_LINE));
      });
    }

    @Test
    void aVectorDeletedAfterTheCountWasReadTakesTheWalk() {
      build("PRODUCT");

      // The exact-cover reuse is only called when nothing is deleted. A delete landing after that decision is the one a
      // test cannot time, so this makes it land first and calls the reuse directly: the validation of the map must see it
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          final LSMVectorIndex index = vectorIndex(db);
          index.findNeighborsFromVector(embedding(1), 5, 64); // the persisted graph and its PQ are loaded
          db.begin();
          db.command("sql", "DELETE FROM Doc WHERE id = ?", 0);
          db.commit();
          assertThat(index.getStats().get("deletedVectors")).as("precondition: one tombstone").isEqualTo(1L);

          final List<String> logs = new ArrayList<>();
          final boolean answered = capturingLogs(logs, index::exactCoverReopenAnswersForTest);

          assertThat(answered).as("with a hole in the dense ordinal space the PQ pairing no longer holds").isFalse();
          assertThat(logs).anyMatch(m -> m.contains("was deleted while its persisted graph was being validated"));
        } finally {
          db.drop();
        }
      }
    }

    @Test
    void anIndexCreatedOverExistingDataRecordsItsGraphAndReopensThroughTheMap() throws IOException {
      createOverExistingData(true);
      assertPqRecordsItsGraphAndReopenTakesTheMap();
    }

    @Test
    void anIndexCreatedOverExistingDataAndClosedBeforeAnySearchRecordsItsGraphToo() throws IOException {
      // No search in the creating session: the PQ is whatever the build and the close left behind
      createOverExistingData(false);
      assertThat(Files.exists(pqPathAfterCreate)).as("precondition: the creating session left a PQ file").isTrue();
      assertPqRecordsItsGraphAndReopenTakesTheMap();
    }

    @Test
    void aGraphRebuiltForAMissingPqFileRecordsItsGraphWithTheNewCodes() throws IOException {
      final Built built = build("PRODUCT");

      // No PQ file: the next open rebuilds the graph and the codes together, and the codes have to be recorded as built
      // for the graph that rebuild certified, not for whichever manifest happens to be on disk
      assertThat(Files.deleteIfExists(built.pqPath)).as("precondition: there was a PQ file to remove").isTrue();
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          final List<String> logs = new ArrayList<>();
          final LSMVectorIndex index = vectorIndex(db);
          final List<Pair<RID, Float>> results = capturingLogs(logs, () -> index.findNeighborsFromVector(embedding(1), 10, 64));
          assertThat(results).hasSize(10);
          assertThat(index.getStats().get("graphRebuildCount")).as("graph and codes are rebuilt together").isEqualTo(1L);
          assertThat(logs).anyMatch(m -> m.contains("there is no usable PQ file"));
        } finally {
          if (db.isOpen())
            db.close();
        }
      }

      final LSMVectorIndexPQFile pqFile = pqFileOf(built);
      assertThat(pqFile.loadPQ()).as("precondition: the rebuild persisted its codes").isTrue();
      final LSMVectorIndexGraphManifest.Content manifest = new LSMVectorIndexGraphManifest(built.graphPath.toString()).read();
      assertThat(manifest).isNotNull();
      assertThat(pqFile.graphFingerprintOf(pqFile.getPQVectors()))
          .as("the rebuilt codes record the fingerprint certified for the rebuilt graph").isEqualTo(manifest.fingerprint());

      final List<String> logs = new ArrayList<>();
      reopenAndSearch(logs, (index, results) -> {
        assertThat(results).hasSize(10);
        assertThat(index.getStats().get("graphLoadsFromOrdinalMap")).as("so the next reopen takes the ordinal map").isEqualTo(1L);
        assertThat(index.getStats().get("graphRebuildCount")).isZero();
        assertThat(logs).as("and not the walk").noneMatch(m -> m.contains(WALK_LINE));
      });
    }

    private Path pqPathAfterCreate;
    private Path graphPathAfterCreate;

    /** CREATE INDEX over documents that are already there, the common case; the database is left closed. */
    private void createOverExistingData(final boolean searchBeforeClosing) {
      final Path graphPath;
      final Path pqPath;
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        try {
          db.transaction(() -> {
            final var type = db.getSchema().createDocumentType("Doc");
            type.createProperty("id", Type.INTEGER);
            type.createProperty("vector", Type.ARRAY_OF_FLOATS);
          });
          db.begin();
          for (int i = 0; i < NUM_VECTORS; i++) {
            db.newDocument("Doc").set("id", i).set("vector", embedding(i)).save();
            if (i % 1000 == 999) {
              db.commit();
              db.begin();
            }
          }
          db.commit();
          db.transaction(() -> db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
              + ", \"similarity\": \"COSINE\", \"quantization\": \"PRODUCT\" }"));

          final LSMVectorIndex index = vectorIndex(db);
          if (searchBeforeClosing)
            assertThat(index.findNeighborsFromVector(embedding(1), 10, 64)).hasSize(10);
          graphPath = index.getGraphFilePathForTest();
          pqPath = index.getProductQuantizationFilePath();
        } finally {
          if (db.isOpen())
            db.close();
        }
      }
      graphPathAfterCreate = graphPath;
      pqPathAfterCreate = pqPath;
    }

    private void assertPqRecordsItsGraphAndReopenTakesTheMap() throws IOException {
      final Path graphPath = graphPathAfterCreate;
      final Path pqPath = pqPathAfterCreate;
      final Built built = new Built(Set.of(), graphPath, NUM_VECTORS, Files.exists(pqPath), pqPath);
      assertThat(built.pqFileExists).as("precondition: the first session left a PQ file").isTrue();
      final LSMVectorIndexPQFile pqFile = pqFileOf(built);
      assertThat(pqFile.loadPQ()).isTrue();
      final Long recorded = pqFile.graphFingerprintOf(pqFile.getPQVectors());
      final LSMVectorIndexGraphManifest.Content manifest = new LSMVectorIndexGraphManifest(graphPath.toString()).read();
      assertThat(manifest).isNotNull();
      assertThat(recorded).as("the PQ of an index created over existing data records the graph it was built for").isNotNull();
      assertThat(recorded).isEqualTo(manifest.fingerprint());

      final List<String> logs = new ArrayList<>();
      reopenAndSearch(logs, (index, results) -> {
        assertThat(results).hasSize(10);
        assertThat(index.getStats().get("graphLoadsFromOrdinalMap")).as("the reopen takes the ordinal map").isEqualTo(1L);
        assertThat(index.getStats().get("graphRebuildCount")).isZero();
        assertThat(logs).as("and not the walk").noneMatch(m -> m.contains(WALK_LINE));
      });
    }

    private LSMVectorIndexPQFile pqFileOf(final Built built) {
      final String file = built.pqPath.toString();
      return new LSMVectorIndexPQFile(file.substring(0, file.length() - ("." + LSMVectorIndexPQFile.FILE_EXT).length()));
    }

    private void reopenAndSearch(final List<String> logs,
        final BiConsumer<LSMVectorIndex, List<Pair<RID, Float>>> checks) {
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          final LSMVectorIndex index = vectorIndex(db);
          final List<Pair<RID, Float>> results = capturingLogs(logs, () -> index.findNeighborsFromVector(embedding(1), 10, 64));
          // Whichever route the reopen took, codes it kept or rebuilt are bound to the graph it published: codes left bound
          // to no map would silently turn every approximate search into an exact one (issue #10786)
          if (index.isPQSearchAvailable())
            assertThat(index.pqCodesForPublishedGraphForTest()).as("the PQ codes serve the graph the reopen published")
                .isNotNull();
          checks.accept(index, results);
        } finally {
          db.drop();
        }
      }
    }

    /** What the building session leaves behind and what it answered, for the reopens to be compared with. */
    private record Built(Set<RID> neighbours, Path graphPath, int ordinalMapSize, boolean pqFileExists, Path pqPath) {
    }

    private Built build(final String quantization) {
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        try {
          db.transaction(() -> {
            final var type = db.getSchema().createDocumentType("Doc");
            type.createProperty("id", Type.INTEGER);
            type.createProperty("vector", Type.ARRAY_OF_FLOATS);
            db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
                + ", \"similarity\": \"COSINE\", \"quantization\": \"" + quantization + "\" }");
          });
          db.begin();
          for (int i = 0; i < NUM_VECTORS; i++) {
            db.newDocument("Doc").set("id", i).set("vector", embedding(i)).save();
            if (i % 1000 == 999) {
              db.commit();
              db.begin();
            }
          }
          db.commit();

          final LSMVectorIndex index = vectorIndex(db);
          index.buildVectorGraphNow();
          final Set<RID> neighbours = neighbours(index.findNeighborsFromVector(embedding(1), 10, 64));
          final Path graphPath = index.getGraphFilePathForTest();
          final LSMVectorIndexOrdinalMapFile.Content map = new LSMVectorIndexGraphManifest(graphPath.toString()).readOrdinalMap();
          final Path pq = index.getProductQuantizationFilePath();
          return new Built(neighbours, graphPath, map != null ? map.size() : -1, pq != null && Files.exists(pq), pq);
        } finally {
          if (db.isOpen())
            db.close();
        }
      }
    }

    private static Set<RID> neighbours(final List<Pair<RID, Float>> results) {
      return results.stream().map(Pair::getFirst).collect(Collectors.toSet());
    }

    private static <T> T capturingLogs(final List<String> into, final Supplier<T> action) {
      final Logger previous = LogManager.instance().getLogger();
      try {
        LogManager.instance().setLogger(new Collector(into));
        return action.get();
      } finally {
        LogManager.instance().setLogger(previous);
      }
    }

    private record Collector(List<String> lines) implements Logger {
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
        if (message == null || level.intValue() < Level.INFO.intValue())
          return;
        final List<Object> bound = new ArrayList<>();
        if (args != null)
          for (final Object arg : args)
            if (arg != null)
              bound.add(arg);
        String formatted;
        try {
          formatted = bound.isEmpty() ? message : String.format(message, bound.toArray());
        } catch (final RuntimeException e) {
          formatted = message;
        }
        synchronized (lines) {
          lines.add(formatted);
        }
      }

      @Override
      public void flush() {
      }
    }

    private static float[] embedding(final int id) {
      final Random random = new Random(0x10568L * 31 + id);
      final float[] vector = new float[DIMENSIONS];
      for (int d = 0; d < DIMENSIONS; d++)
        vector[d] = random.nextFloat();
      return vector;
    }

    private static LSMVectorIndex vectorIndex(final Database db) {
      return (LSMVectorIndex) db.getSchema().getType("Doc").getPolymorphicIndexByProperties("vector")
          .getIndexesOnBuckets()[0];
    }
  }

  /**
   * Regression for #9796: writePQ never destroys the live .vecpq file.
   * <p>
   * Regression test for issue #9796: {@code LSMVectorIndexPQFile.writePQ} rewrote the LIVE {@code .vecpq} file in place
   * ({@code TRUNCATE_EXISTING}), so the previous, complete PQ data was destroyed before the first byte of the
   * replacement was written and anything that interrupted the stream left a fragment that {@code exists()} reported as
   * present while {@code loadPQ()} could not read it.
   * <p>
   * The tests assert the RULE, not a round trip: a rewrite that fails at a chosen point leaves the previous file in
   * place, byte for byte, and still loadable.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class PqFileAtomicRewrite {

    private static final VectorTypeSupport VTS = VectorizationProvider.getInstance().getVectorTypeSupport();

    private static final int DIMENSIONS = 16;
    private static final int SUBSPACES  = 4;
    private static final int CLUSTERS   = 16;

    @TempDir
    Path tempDir;

    @Test
    void aFailedRewriteLeavesThePreviousFileInPlaceAndLoadable() throws IOException {
      final String base = tempDir.resolve("index").toString();
      final LSMVectorIndexPQFile pqFile = new LSMVectorIndexPQFile(base);

      final Fixture first = new Fixture(1, 300);
      pqFile.writePQ(first.pq, first.codes);
      final byte[] firstBytes = Files.readAllBytes(pqFile.getFilePath());
      assertThat(firstBytes.length).isGreaterThan(0);

      // The second write dies after the codebook and the first stretch of codes have been streamed: the mid-stream
      // failure that used to leave a fragment where the complete first file had been.
      final Fixture second = new Fixture(2, 600);
      final MutablePQVectors failing = new FailingPQVectors(second.pq, 600, 512);
      assertThatThrownBy(() -> pqFile.writePQ(second.pq, failing)).isInstanceOf(IndexException.class);

      assertThat(Files.readAllBytes(pqFile.getFilePath()))
          .as("a rewrite that fails must leave the previous PQ file byte-identical, not a truncated fragment")
          .isEqualTo(firstBytes);
      assertThat(listTemporaries()).as("the failed attempt must not leave its temporary file behind").isEmpty();

      final LSMVectorIndexPQFile reopened = new LSMVectorIndexPQFile(base);
      assertThat(reopened.loadPQ()).as("the previous file must still load").isTrue();
      assertThat(reopened.getPQVectors().count()).isEqualTo(first.codes.count());
      assertThat(reopened.hasUsableData()).isTrue();

      // The handler that failed keeps serving what it already held: nothing was published.
      assertThat(pqFile.getPQVectors()).isSameAs(first.codes);
      assertThat(pqFile.getProductQuantization()).isSameAs(first.pq);
    }

    @Test
    void anErrorDuringTheRewriteLeavesThePreviousFileAndNoTemporary() throws IOException {
      final String base = tempDir.resolve("index").toString();
      final LSMVectorIndexPQFile pqFile = new LSMVectorIndexPQFile(base);

      final Fixture first = new Fixture(1, 300);
      pqFile.writePQ(first.pq, first.codes);
      final byte[] firstBytes = Files.readAllBytes(pqFile.getFilePath());

      // An OutOfMemoryError while encoding or streaming a large file is not an Exception: it used to skip the cleanup
      final Fixture second = new Fixture(2, 600);
      final MutablePQVectors failing = new FailingPQVectors(second.pq, 600, 512, new OutOfMemoryError("injected"));
      assertThatThrownBy(() -> pqFile.writePQ(second.pq, failing)).isInstanceOf(OutOfMemoryError.class);

      assertThat(Files.readAllBytes(pqFile.getFilePath())).isEqualTo(firstBytes);
      assertThat(listTemporaries()).as("an Error must not leave the temporary file behind either").isEmpty();
      assertThat(pqFile.getPQVectors()).isSameAs(first.codes);
    }

    @Test
    void aSuccessfulRewriteReplacesThePreviousFileAndPublishesTheNewOne() throws IOException {
      final String base = tempDir.resolve("index").toString();
      final LSMVectorIndexPQFile pqFile = new LSMVectorIndexPQFile(base);

      final Fixture first = new Fixture(1, 300);
      pqFile.writePQ(first.pq, first.codes);

      final Fixture second = new Fixture(2, 600);
      pqFile.writePQ(second.pq, second.codes);

      assertThat(pqFile.getPQVectors()).isSameAs(second.codes);
      assertThat(listTemporaries()).isEmpty();

      final LSMVectorIndexPQFile reopened = new LSMVectorIndexPQFile(base);
      assertThat(reopened.loadPQ()).isTrue();
      assertThat(reopened.getPQVectors().count()).isEqualTo(600);
    }

    @Test
    void anInterruptDuringTheRewriteDoesNotLeaveAFragment() throws IOException {
      final String base = tempDir.resolve("index").toString();
      final LSMVectorIndexPQFile pqFile = new LSMVectorIndexPQFile(base);

      final Fixture first = new Fixture(1, 300);
      pqFile.writePQ(first.pq, first.codes);

      // LSMVectorIndex builds on a pool it cancels with shutdownNow(), which interrupts the thread that is writing the
      // PQ data. A pending interrupt closes a FileChannel on its first I/O (ClosedByInterruptException).
      final Fixture second = new Fixture(2, 600);
      Thread.currentThread().interrupt();
      boolean stillInterrupted;
      try {
        pqFile.writePQ(second.pq, second.codes);
      } finally {
        stillInterrupted = Thread.interrupted();
      }

      assertThat(stillInterrupted).as("the caller must still be told it was interrupted").isTrue();
      assertThat(listTemporaries()).isEmpty();

      // Whatever the write decided, the file on disk is complete: the new data, never a fragment.
      final LSMVectorIndexPQFile reopened = new LSMVectorIndexPQFile(base);
      assertThat(reopened.loadPQ()).as("the file on disk must load after an interrupted rewrite").isTrue();
      assertThat(reopened.getPQVectors().count()).isEqualTo(600);
    }

    @Test
    void theWriterCompletesItsStreamAcrossAnInterrupt() throws IOException {
      final Path path = tempDir.resolve("stream.bin");
      final byte[] payload = new byte[100_000];
      new Random(9796).nextBytes(payload);

      Thread.currentThread().interrupt();
      boolean stillInterrupted;
      try (SimpleFileIndexWriter writer = new SimpleFileIndexWriter(path)) {
        writer.write(payload);
        writer.writeLong(42L);
        assertThat(writer.position()).isEqualTo(payload.length + 8L);
      } finally {
        stillInterrupted = Thread.interrupted();
      }

      assertThat(stillInterrupted).isTrue();
      final byte[] written = Files.readAllBytes(path);
      assertThat(written).hasSize(payload.length + 8);
      assertThat(Arrays.copyOf(written, payload.length)).isEqualTo(payload);
    }

    @Test
    void aLeftoverTemporaryFromACrashedWriteIsRemoved() throws IOException {
      final String base = tempDir.resolve("index").toString();
      final LSMVectorIndexPQFile pqFile = new LSMVectorIndexPQFile(base);

      final Path leftover = tempDir.resolve("index." + LSMVectorIndexPQFile.FILE_EXT + ".deadbeef.tmp");
      Files.write(leftover, new byte[] { 1, 2, 3 });
      final Path unrelated = tempDir.resolve("other.tmp");
      Files.write(unrelated, new byte[] { 4 });

      final Fixture fixture = new Fixture(1, 300);
      pqFile.writePQ(fixture.pq, fixture.codes);

      assertThat(leftover).as("a process killed between the write and the rename leaves a temporary nothing else removes")
          .doesNotExist();
      assertThat(unrelated).as("only this file's own temporaries are touched").exists();

      Files.write(leftover, new byte[] { 1, 2, 3 });
      pqFile.deletePQFile();
      assertThat(leftover).doesNotExist();
      assertThat(pqFile.getFilePath()).doesNotExist();
    }

    @Test
    void aFileNameWithGlobMetacharactersStillGetsItsLeftoversRemoved() throws IOException {
      // A directory-stream glob built from the file name would throw (unclosed '[' and '{') or match the wrong files; the
      // failure is swallowed, so the leftovers would silently never be cleaned
      final String base = tempDir.resolve("in[d{ex*?").toString();
      final LSMVectorIndexPQFile pqFile = new LSMVectorIndexPQFile(base);

      final Path leftover = tempDir.resolve("in[d{ex*?." + LSMVectorIndexPQFile.FILE_EXT + ".deadbeef.tmp");
      Files.write(leftover, new byte[] { 1, 2, 3 });
      final Path neighbour = tempDir.resolve("inXdYex." + LSMVectorIndexPQFile.FILE_EXT + ".deadbeef.tmp");
      Files.write(neighbour, new byte[] { 4 });

      final Fixture fixture = new Fixture(1, 300);
      pqFile.writePQ(fixture.pq, fixture.codes);

      assertThat(leftover).doesNotExist();
      assertThat(neighbour).as("a name the glob would have matched is not this file's temporary").exists();
    }

    @Test
    void deletePQFileReportsWhetherTheFileIsReallyGone() throws IOException {
      final String base = tempDir.resolve("index").toString();
      final LSMVectorIndexPQFile pqFile = new LSMVectorIndexPQFile(base);
      assertThat(pqFile.deletePQFile()).as("nothing to delete is a file that is gone").isTrue();

      final Fixture fixture = new Fixture(1, 300);
      pqFile.writePQ(fixture.pq, fixture.codes);
      assertThat(pqFile.deletePQFile()).isTrue();
      assertThat(pqFile.getFilePath()).doesNotExist();

      // Something that cannot be deleted: a non-empty directory under the file's name. The caller that drops a stale file
      // has to be told, because a stale file left on disk is paired with the next graph on the following open.
      Files.createDirectory(pqFile.getFilePath());
      Files.write(pqFile.getFilePath().resolve("child"), new byte[] { 1 });
      assertThat(pqFile.deletePQFile()).as("the delete failed: the file is still there").isFalse();
      assertThat(pqFile.exists()).isTrue();
    }

    @Test
    void anUnloadableFileIsNotUsableDataEvenThoughItExists() throws IOException {
      final String base = tempDir.resolve("index").toString();
      final LSMVectorIndexPQFile pqFile = new LSMVectorIndexPQFile(base);
      assertThat(pqFile.hasUsableData()).as("no file, no data").isFalse();

      // The fragment the pre-fix writer left behind: the start of a real file.
      final Fixture fixture = new Fixture(1, 300);
      pqFile.writePQ(fixture.pq, fixture.codes);
      final byte[] whole = Files.readAllBytes(pqFile.getFilePath());
      Files.write(pqFile.getFilePath(), Arrays.copyOf(whole, whole.length / 3));

      final LSMVectorIndexPQFile reopened = new LSMVectorIndexPQFile(base);
      assertThat(reopened.loadPQ()).isFalse();
      assertThat(reopened.exists()).as("the fragment is on disk").isTrue();
      assertThat(reopened.hasUsableData())
          .as("but \"is there PQ data?\" must give the same answer as isPQReady() for it - the open-time decision keys "
              + "on this")
          .isFalse();
    }

    private List<Path> listTemporaries() throws IOException {
      final List<Path> temporaries = new ArrayList<>();
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(tempDir, "*.tmp")) {
        stream.forEach(temporaries::add);
      }
      return temporaries;
    }

    /** A trained codebook and the codes of {@code count} random vectors. */
    static final class Fixture {
      final ProductQuantization pq;
      final MutablePQVectors    codes;

      Fixture(final long seed, final int count) {
        final Random random = new Random(seed);
        final List<VectorFloat<?>> vectors = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
          final float[] values = new float[DIMENSIONS];
          for (int d = 0; d < DIMENSIONS; d++)
            values[d] = random.nextFloat();
          vectors.add(VTS.createFloatVector(values));
        }
        final ListRandomAccessVectorValues ravv = new ListRandomAccessVectorValues(vectors, DIMENSIONS);
        this.pq = ProductQuantization.compute(ravv, SUBSPACES, CLUSTERS, false);
        this.codes = new MutablePQVectors(pq);
        for (int i = 0; i < count; i++)
          codes.encodeAndSet(i, vectors.get(i));
      }
    }

    /** Codes that fail after streaming part of themselves, like a full disk or an I/O error halfway through. */
    static final class FailingPQVectors extends MutablePQVectors {
      private final int       vectorCount;
      private final int       failAfterBytes;
      private final Throwable failure;

      FailingPQVectors(final ProductQuantization pq, final int vectorCount, final int failAfterBytes) {
        this(pq, vectorCount, failAfterBytes, new IOException("injected failure after " + failAfterBytes + " bytes of codes"));
      }

      FailingPQVectors(final ProductQuantization pq, final int vectorCount, final int failAfterBytes, final Throwable failure) {
        super(pq);
        this.vectorCount = vectorCount;
        this.failAfterBytes = failAfterBytes;
        this.failure = failure;
      }

      @Override
      public int count() {
        return vectorCount;
      }

      @Override
      public void write(final IndexWriter out, final int version) throws IOException {
        out.write(new byte[failAfterBytes]);
        if (failure instanceof IOException e)
          throw e;
        if (failure instanceof RuntimeException e)
          throw e;
        throw (Error) failure;
      }
    }
  }

  /**
   * Regression for #9796: the index around a PQ file that is never destroyed.
   * <p>
   * Issue #9796 at the index level: what {@code LSMVectorIndex} does around the PQ file now that
   * {@code LSMVectorIndexPQFile.writePQ} never destroys the file it replaces.
   * <ul>
   *   <li>A failed persist must not leave the previous file standing. It describes the previous graph's ordinals and the
   *   new graph is already certified, so the next open would pair the two. The file is dropped (the next open rebuilds
   *   graph and PQ together) and the codes in hand are still published to this session.</li>
   *   <li>A file that is on disk but cannot be read is no PQ data: the open rebuilds, and ends with a loadable file.</li>
   * </ul>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @Tag("vector")
  class PqFileIndex extends TestHelper {

    private static final int EMBEDDING_DIM = 32;
    private static final int VECTORS       = 1100;

    private static final Duration SETTLE_TIMEOUT = Duration.ofMinutes(2);

    @Test
    void aFailedPersistDropsThePreviousFileAndStillPublishesTheCodesInHand() throws Exception {
      assertFailedPersistDropsThePreviousFile(null);
    }

    @Test
    void aRuntimeFailureOfThePersistDropsThePreviousFileToo() throws Exception {
      assertFailedPersistDropsThePreviousFile(new IllegalStateException("injected runtime failure"));
    }

    @Test
    void anErrorDuringThePersistDropsThePreviousFileBeforeItPropagates() throws Exception {
      // The build's handler catches Throwable and carries on: whatever it decides, the stale file must already be gone
      final Throwable thrown = assertFailedPersistDropsThePreviousFile(new OutOfMemoryError("injected"));
      assertThat(thrown).isInstanceOf(OutOfMemoryError.class);
    }

    /**
     * @param failure what the codes' write throws after streaming part of itself, or {@code null} for an I/O error
     *
     * @return what {@code persistProductQuantization} let escape, or {@code null} when it absorbed the failure
     */
    private Throwable assertFailedPersistDropsThePreviousFile(final Throwable failure) throws Exception {
      final LSMVectorIndex index = buildIndexWithPersistedPQ();
      final LSMVectorIndexPQFile pqFile = pqFileOf(index);
      final Path pqPath = pqFile.getFilePath();
      assertThat(pqPath).exists();

      final ProductQuantization pq = index.pqCodesForTest().quantization();
      final PQVectors liveCodes = index.pqCodesForPublishedGraphForTest();
      assertThat(pq).isNotNull();
      assertThat(liveCodes).isNotNull();

      // Codes whose write dies halfway through: the persist after a rebuild whose disk gave out.
      final PqFileAtomicRewrite.FailingPQVectors failing = failure == null ?
          new PqFileAtomicRewrite.FailingPQVectors(pq, liveCodes.count(), 64) :
          new PqFileAtomicRewrite.FailingPQVectors(pq, liveCodes.count(), 64, failure);

      final Method persist = LSMVectorIndex.class.getDeclaredMethod("persistProductQuantization", ProductQuantization.class,
          PQVectors.class, int[].class, Long.class, LSMVectorIndexPQFile.CodebookStats.class);
      persist.setAccessible(true);
      Throwable escaped = null;
      try {
        persist.invoke(index, pq, failing, index.getOrdinalToVectorIdForTest(), null, index.pqCodesForTest().stats());
      } catch (final InvocationTargetException e) {
        escaped = e.getCause();
      }

      assertThat(pqPath)
          .as("the previous file belongs to the previous graph: it must not survive a failed persist")
          .doesNotExist();
      assertThat(temporariesNextTo(pqPath)).isEmpty();
      assertThat(pqFile.hasUsableData()).isFalse();
      if (escaped == null)
        assertThat(index.pqCodesForPublishedGraphForTest()).as("the codes in hand are right for the graph in memory")
            .isSameAs(failing);
      return escaped;
    }

    @Test
    void anUnreadablePQFileIsRebuiltOnReopen() throws Exception {
      final LSMVectorIndex index = buildIndexWithPersistedPQ();
      final Path pqPath = pqFileOf(index).getFilePath();
      final long completeSize = Files.size(pqPath);
      assertThat(completeSize).isGreaterThan(0L);

      // The fragment the in-place rewrite used to leave behind, on a database that is closed.
      database.close();
      final byte[] whole = Files.readAllBytes(pqPath);
      Files.write(pqPath, Arrays.copyOf(whole, whole.length / 3));
      database = factory.open();

      final LSMVectorIndex reopened = indexOf(database);

      // The open found the fragment, tried to load it and could not: "is there PQ data?" must say no, although the file
      // is there. This is the answer needsGraphRebuildForPQ keys on.
      reopened.countEntries();
      final LSMVectorIndexPQFile fragment = pqFileOf(reopened);
      assertThat(fragment.exists()).as("the fragment is on disk").isTrue();
      assertThat(fragment.isPQReady()).isFalse();
      assertThat(fragment.hasUsableData()).as("but it is not PQ data").isFalse();

      final Random random = new Random(9796);
      Awaitility.await("the PQ is rebuilt next to the graph after the reopen")
          .atMost(SETTLE_TIMEOUT)
          .pollInterval(Duration.ofMillis(50))
          .untilAsserted(() -> {
            reopened.findNeighborsFromVector(randomUnitVector(random), 10);
            assertThat(reopened.isPQSearchAvailable()).isTrue();
            assertThat(PendingDeltaVectors.of(reopened)).isZero();
          });

      assertThat(reopened.findNeighborsFromVectorApproximate(randomUnitVector(random), 10)).isNotEmpty();

      final LSMVectorIndexPQFile fresh = new LSMVectorIndexPQFile(stripExtension(pqPath));
      assertThat(fresh.loadPQ()).as("the file on disk must load after the rebuild").isTrue();
      assertThat(fresh.getPQVectors().count()).isGreaterThanOrEqualTo(VECTORS);
      assertThat(temporariesNextTo(pqPath)).isEmpty();
    }

    /**
     * The async rebuild path reaches the persisted-graph decision without the deferred vector load having run, and the
     * PQ is loaded by that load: a fragment nobody has tried to read yet answers "usable". The decision has to try the
     * load itself before it asks.
     */
    @Test
    void theGraphDecisionTriesToLoadThePQBeforeAskingWhetherItIsUsable() throws Exception {
      final LSMVectorIndex index = buildIndexWithPersistedPQ();
      final Path pqPath = pqFileOf(index).getFilePath();

      database.close();
      final byte[] whole = Files.readAllBytes(pqPath);
      Files.write(pqPath, Arrays.copyOf(whole, whole.length / 3));
      database = factory.open();

      // Nothing has touched the reopened index: the locations and the PQ are still to be loaded
      final LSMVectorIndex reopened = indexOf(database);
      final LSMVectorIndexPQFile fragment = pqFileOf(reopened);
      assertThat(fragment.exists()).isTrue();
      assertThat(fragment.isPQReady()).isFalse();
      assertThat(fragment.hasUsableData()).as("no load has been tried, so the fragment still reads as usable").isTrue();

      final Object graphFile = field("graphFile").get(reopened);
      assertThat(graphFile).as("the scenario needs a persisted graph to decide about").isNotNull();
      final Method hasPersistedGraph = graphFile.getClass().getDeclaredMethod("hasPersistedGraph");
      hasPersistedGraph.setAccessible(true);
      assertThat((boolean) hasPersistedGraph.invoke(graphFile)).isTrue();

      final ReentrantLock graphBuildLock = (ReentrantLock) field("graphBuildLock").get(reopened);
      final Method decide = LSMVectorIndex.class.getDeclaredMethod("loadPersistedGraphOrDecidePrefix");
      decide.setAccessible(true);
      final Object check;
      graphBuildLock.lock();
      try {
        check = decide.invoke(reopened);
      } finally {
        graphBuildLock.unlock();
      }

      final Method fullyLoaded = check.getClass().getDeclaredMethod("fullyLoaded");
      fullyLoaded.setAccessible(true);
      final Method prefix = check.getClass().getDeclaredMethod("prefix");
      prefix.setAccessible(true);
      assertThat((boolean) fullyLoaded.invoke(check))
          .as("PRODUCT quantization without PQ data to use must rebuild the graph from scratch, not load it")
          .isFalse();
      assertThat(prefix.invoke(check)).isNull();
      assertThat(fragment.hasUsableData()).as("the decision tried the load, and the fragment failed it").isFalse();
    }

    private LSMVectorIndex buildIndexWithPersistedPQ() throws Exception {
      final Random random = new Random(9796);

      database.transaction(() -> {
        database.getSchema().createVertexType("Embedding");
        database.getSchema().getType("Embedding").createProperty("vector", Type.ARRAY_OF_FLOATS);
        database.command("sql", """
            CREATE INDEX ON Embedding (vector) LSM_VECTOR
            METADATA {
                "dimensions": %d,
                "similarity": "EUCLIDEAN",
                "quantization": "PRODUCT",
                "pqClusters": 32
            }""".formatted(EMBEDDING_DIM));
      });

      database.transaction(() -> {
        for (int i = 0; i < VECTORS; i++)
          database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) randomUnitVector(random));
      });

      final LSMVectorIndex index = indexOf(database);
      Awaitility.await("the graph and its PQ settle")
          .atMost(SETTLE_TIMEOUT)
          .pollInterval(Duration.ofMillis(50))
          .untilAsserted(() -> {
            index.findNeighborsFromVector(randomUnitVector(random), 10);
            assertThat(PendingDeltaVectors.of(index)).isZero();
            assertThat(index.isPQSearchAvailable()).isTrue();
            assertThat(pqFileOf(index).getFilePath()).exists();
          });
      return index;
    }

    private static LSMVectorIndex indexOf(final Database db) {
      final TypeIndex typeIndex = (TypeIndex) db.getSchema().getIndexByName("Embedding[vector]");
      return (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    }

    private static LSMVectorIndexPQFile pqFileOf(final LSMVectorIndex index) throws Exception {
      return (LSMVectorIndexPQFile) field("pqFile").get(index);
    }

    private static Field field(final String name) throws Exception {
      final Field field = LSMVectorIndex.class.getDeclaredField(name);
      field.setAccessible(true);
      return field;
    }

    private static String stripExtension(final Path pqPath) {
      final String name = pqPath.toString();
      return name.substring(0, name.length() - ("." + LSMVectorIndexPQFile.FILE_EXT).length());
    }

    private static List<Path> temporariesNextTo(final Path pqPath) throws IOException {
      final List<Path> temporaries = new ArrayList<>();
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(pqPath.getParent(), pqPath.getFileName() + ".*.tmp")) {
        stream.forEach(temporaries::add);
      }
      return temporaries;
    }

    private float[] randomUnitVector(final Random random) {
      final float[] vector = new float[EMBEDDING_DIM];
      float norm = 0;
      for (int i = 0; i < EMBEDDING_DIM; i++) {
        vector[i] = random.nextFloat() * 2 - 1;
        norm += vector[i] * vector[i];
      }
      norm = (float) Math.sqrt(norm);
      if (norm > 0)
        for (int i = 0; i < EMBEDDING_DIM; i++)
          vector[i] /= norm;
      return vector;
    }
  }
}
