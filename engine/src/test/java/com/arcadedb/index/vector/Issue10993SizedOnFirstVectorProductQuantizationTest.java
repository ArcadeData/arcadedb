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
import com.arcadedb.database.RID;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.utility.Pair;
import io.github.jbellis.jvector.quantization.PQVectors;
import io.github.jbellis.jvector.quantization.ProductQuantization;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10993: a {@code quantization: 'PRODUCT'} index created without {@code vector.dimensions} fixes its dimensions on the
 * first vector it receives (issue #10915), and {@code pqSubspaces} is computed at graph build from that decided dimension. No
 * test built a PQ graph over an index sized this way and looked at the codebook it got: a build that ran before the size was
 * known would derive the subspaces from 0 dimensions and train nothing, silently, since PQ is optional and its failure is only
 * logged.
 */
class Issue10993SizedOnFirstVectorProductQuantizationTest extends TestHelper {
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
