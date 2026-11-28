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
package com.arcadedb.function.sql.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.index.vector.VectorUtils;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The cosine similarity is documented to lie in [-1, 1]. JVector forms the dot product and the norms in float, so two vectors of the
 * same direction (a vector and a scaled copy of it) came out a rounding step above 1, and 1 - cosine, the cosine distance, below 0.
 */
class VectorCosineSimilarityRangeTest extends TestHelper {

  @Test
  void aVectorAndAScaledCopyOfItAreNeverBeyondOne() {
    // the one-dimension pair the fuzz found first: 1.0000001 before the clamp
    assertThat(VectorUtils.cosineSimilarity(new float[] { 0.24067684f }, new float[] { 1.6616266f })).isLessThanOrEqualTo(1.0f);

    final Random random = new Random(20261125L);
    for (int round = 0; round < 3000; round++) {
      final int dims = 1 + random.nextInt(64);
      final float[] vector = new float[dims];
      for (int i = 0; i < dims; i++)
        vector[i] = (float) random.nextGaussian();
      final float scale = 0.3f + random.nextFloat() * 3.0f;
      final float[] same = new float[dims];
      final float[] opposite = new float[dims];
      for (int i = 0; i < dims; i++) {
        same[i] = vector[i] * scale;
        opposite[i] = -same[i];
      }
      assertThat(VectorUtils.cosineSimilarity(vector, same)).isBetween(-1.0f, 1.0f);
      assertThat(VectorUtils.cosineSimilarity(vector, opposite)).isBetween(-1.0f, 1.0f);
    }
  }

  @Test
  void theSqlFunctionAndTheCosineDistanceStayInRange() {
    final float[] vector = { 0.24067684f };
    final float[] scaled = { 1.6616266f };
    try (final ResultSet rs = database.query("sql", "SELECT `vector.cosineSimilarity`(?, ?) AS r", vector, scaled)) {
      assertThat(((Number) rs.next().getProperty("r")).floatValue()).isLessThanOrEqualTo(1.0f);
    }
    // vector.distance.cosine is 1 - cosine: a distance is never negative
    try (final ResultSet rs = database.query("opencypher", "RETURN vector.distance.cosine($a, $b) AS r",
        Map.of("a", vector, "b", scaled))) {
      assertThat(((Number) rs.next().getProperty("r")).doubleValue()).isGreaterThanOrEqualTo(0.0);
    }
  }
}
