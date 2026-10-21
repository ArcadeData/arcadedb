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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.Pair;
import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
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
class Issue10543QuantizedGraphBuildTest extends TestHelper {
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
