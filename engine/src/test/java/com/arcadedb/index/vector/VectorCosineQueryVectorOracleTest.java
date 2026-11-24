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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The query vectors a search refuses, and which refusal it names. A COSINE index cannot score the all-zero vector (the similarity is
 * undefined) nor one whose squared norm the float cosine cannot hold (issue #11635), and says which of the two it is, because the
 * second is a vector the caller can rescale and the first is not a vector at all. A non-finite component is refused for every
 * similarity. EUCLIDEAN scores the origin like any other point, and so does DOT_PRODUCT (a zero vector is a valid, useless query
 * there: its score against every row is the same).
 */
@Tag("vector")
class VectorCosineQueryVectorOracleTest extends TestHelper {
  private static final int DIMENSIONS = 8;
  private static final int ROWS       = 40;

  @Test
  void aCosineIndexRefusesTheZeroVectorAndSaysItIsZero() {
    final LSMVectorIndex index = indexOf("COSINE");

    assertThatThrownBy(() -> index.findNeighborsFromVector(new float[DIMENSIONS], 5))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("cannot be a zero vector");
  }

  @Test
  void aCosineIndexRefusesAQueryWhoseNormItCannotHoldAndSaysItIsOutsideTheRange() {
    final LSMVectorIndex index = indexOf("COSINE");
    final float[] tiny = new float[DIMENSIONS];
    Arrays.fill(tiny, 1e-30f);
    final float[] huge = new float[DIMENSIONS];
    Arrays.fill(huge, 1e30f);

    for (final float[] query : List.of(tiny, huge))
      assertThatThrownBy(() -> index.findNeighborsFromVector(query, 5)).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("outside the range COSINE similarity can score").hasMessageNotContaining("zero vector");
  }

  @Test
  void aCosineIndexAnswersAQueryOfTheSmallestAndLargestNormItCanHold() {
    final LSMVectorIndex index = indexOf("COSINE");
    final float[] small = new float[DIMENSIONS];
    Arrays.fill(small, 1e-9f);
    final float[] large = new float[DIMENSIONS];
    Arrays.fill(large, 1e9f);

    // the bounds are 2^-63 and 2^63 of the squared norm: 8e-18 and 8e18 are inside them
    for (final float[] query : List.of(small, large))
      assertThat(index.findNeighborsFromVector(query, 5)).hasSize(5);
  }

  @Test
  void everySimilarityRefusesAComponentThatIsNotFinite() {
    for (final String similarity : List.of("COSINE", "EUCLIDEAN", "DOT_PRODUCT")) {
      final LSMVectorIndex index = indexOf(similarity);
      for (final float bad : new float[] { Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY }) {
        final float[] query = new float[DIMENSIONS];
        Arrays.fill(query, 1f);
        query[3] = bad;
        assertThatThrownBy(() -> index.findNeighborsFromVector(query, 5)).as("%s with %s", similarity, bad)
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("must be finite numbers");
      }
    }
  }

  @Test
  void aEuclideanIndexScoresTheOriginLikeAnyOtherPoint() {
    final LSMVectorIndex index = indexOf("EUCLIDEAN");

    assertThat(index.findNeighborsFromVector(new float[DIMENSIONS], 5)).hasSize(5);
  }

  /** The record type is created per call: each test names its own similarity. */
  private LSMVectorIndex indexOf(final String similarity) {
    final String type = "Q" + similarity.replace("_", "");
    database.command("sql", "CREATE DOCUMENT TYPE " + type + " BUCKETS 1");
    database.command("sql", "CREATE PROPERTY " + type + ".embedding ARRAY_OF_FLOATS");
    database.command("sql",
        "CREATE INDEX ON " + type + " (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS + ", \"similarity\": \""
            + similarity + "\" }");
    final Random random = new Random(3);
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++) {
        final float[] vector = new float[DIMENSIONS];
        for (int d = 0; d < DIMENSIONS; d++)
          vector[d] = (float) random.nextGaussian() + 0.5f;
        database.newDocument(type).set("embedding", vector).save();
      }
    });
    return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName(type + "[embedding]")).getIndexesOnBuckets()[0];
  }
}
