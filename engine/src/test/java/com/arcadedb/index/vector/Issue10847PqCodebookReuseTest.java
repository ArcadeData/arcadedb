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
import com.arcadedb.utility.VectorApiSupport;
import io.github.jbellis.jvector.quantization.ProductQuantization;
import io.github.jbellis.jvector.vector.types.ByteSequence;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10847: every graph rebuild of a PRODUCT-quantized index trained a new codebook and encoded every vector again, also
 * when a few percent of them had changed. The codebook is now kept while it fits the data - the index has not outgrown it
 * ({@link GlobalConfiguration#VECTOR_INDEX_PQ_RETRAIN_GROWTH_FACTOR}) and the vectors added since are not encoded measurably
 * worse than the ones it was trained on ({@link GlobalConfiguration#VECTOR_INDEX_PQ_RETRAIN_ERROR_RATIO}) - and only the
 * vectors added since are encoded.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("vector")
class Issue10847PqCodebookReuseTest extends TestHelper {
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
    final LSMVectorIndex rebuilt = indexOf();
    rebuilt.buildVectorGraphNow();

    assertThat(rebuilt.pqCodesForTest().quantization()).isNotSameAs(first);
    assertThat(rebuilt.getLastPqReusedCodes()).isZero();
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
