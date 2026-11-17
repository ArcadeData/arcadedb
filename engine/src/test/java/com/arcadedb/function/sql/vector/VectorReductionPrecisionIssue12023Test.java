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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.withinPercentage;

/**
 * Issue #12023: the scalar vector reductions formed each element product (or difference) in float, or summed in a float, so a
 * pair of vectors whose true inner product or distance is an ordinary float answered NaN, Infinity, or a sum that had stopped
 * growing. COSINE already widened before multiplying (#4583); the other reductions now do too, and a SIMD answer that came back
 * non-finite is recomputed in double.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class VectorReductionPrecisionIssue12023Test {

  // 9e38 - 9e38 + 35: each product overflows a float, the inner product is 35
  private static final float[] A = { 3.0e19f, 3.0e19f, 5.0f };
  private static final float[] B = { 3.0e19f, -3.0e19f, 7.0f };

  @Nested
  class Engine {
    @Test
    void dotProductOfOverflowingProductsIsTheTrueInnerProduct() {
      assertThat(VectorUtils.dotProduct(A, B)).isEqualTo(35.0f);
    }

    @Test
    void dotProductOnTheSimdPathRecomputesANonFiniteAnswer() {
      // 4096 components and more take the JVector path, which multiplies in float
      final float[] a = new float[4096];
      final float[] b = new float[4096];
      System.arraycopy(A, 0, a, 0, A.length);
      System.arraycopy(B, 0, b, 0, B.length);
      assertThat(VectorUtils.dotProduct(a, b)).isEqualTo(35.0f);
    }

    @Test
    void aTrulyUnrepresentableInnerProductStaysInfinite() {
      // The control: when the true answer is beyond Float.MAX, Infinity IS the answer
      assertThat(VectorUtils.dotProduct(new float[] { 3.0e19f, 3.0e19f }, new float[] { 3.0e19f, 3.0e19f })).isInfinite();
    }

    @Test
    void l2DistanceWhoseSquareOverflowsIsTheTrueDistance() {
      // the squared difference 9e38 overflows a float, the distance 3e19 does not
      assertThat(VectorUtils.l2Distance(new float[] { 2.0e19f, 0f }, new float[] { -1.0e19f, 0f })).isCloseTo(3.0e19f,
          withinPercentage(0.0001));
      // the difference 4e38 itself overflows a float
      assertThat(VectorUtils.squaredL2DistanceDouble(new float[] { 2.0e38f }, new float[] { -2.0e38f })).isCloseTo(1.6e77,
          withinPercentage(0.0001));
    }

    @Test
    void l2DistanceOfOrdinaryVectorsIsUnchanged() {
      assertThat(VectorUtils.l2Distance(new float[] { 0f, 0f }, new float[] { 3f, 4f })).isEqualTo(5.0f);
    }

    @Test
    void manhattanDistanceKeepsGrowingPastTheFloatPrecisionOfItsRunningSum() {
      // 1e8 + 1000 x 1: a float running sum stays at 1e8 (its ulp there is 8), the distance is 100001000
      final float[] a = new float[1001];
      a[0] = 1.0e8f;
      for (int i = 1; i < a.length; i++)
        a[i] = 1f;
      assertThat(VectorUtils.manhattanDistance(a, new float[a.length])).isEqualTo(100001000f);
    }

    @Test
    void sparseDotProductAndMagnitudeWidenBeforeMultiplying() {
      final SparseVector a = new SparseVector(new int[] { 0, 1, 2 }, A, 3);
      final SparseVector b = new SparseVector(new int[] { 0, 1, 2 }, B, 3);
      assertThat(a.dotProduct(b)).isEqualTo(35.0f);
      // squared components 4e38 overflow a float, the magnitude 2e19 does not
      assertThat(new SparseVector(new int[] { 0 }, new float[] { 2.0e19f }, 1).magnitude()).isEqualTo(2.0e19f);
    }
  }

  @Nested
  class Sql extends TestHelper {
    @Test
    void theReportedDotProduct() {
      assertThat(sql("SELECT vector.dotProduct(?, ?) AS r", A, B)).isEqualTo(35.0f);
    }

    @Test
    void l2Distance() {
      assertThat((float) sql("SELECT vector.l2Distance(?, ?) AS r", new float[] { 2.0e19f, 0f }, new float[] { -1.0e19f, 0f })).isCloseTo(3.0e19f, withinPercentage(0.0001));
    }

    @Test
    void l1DistanceAndItsManhattanAlias() {
      final float[] a = new float[1001];
      a[0] = 1.0e8f;
      for (int i = 1; i < a.length; i++)
        a[i] = 1f;
      assertThat(sql("SELECT vector.l1Distance(?, ?) AS r", a, new float[a.length])).isEqualTo(100001000f);
      assertThat(sql("SELECT vector.manhattanDistance(?, ?) AS r", a, new float[a.length])).isEqualTo(100001000f);
    }

    @Test
    void l1Norm() {
      final float[] a = new float[1001];
      a[0] = 1.0e8f;
      for (int i = 1; i < a.length; i++)
        a[i] = -1f;
      assertThat(sql("SELECT vector.l1Norm(?) AS r", (Object) a)).isEqualTo(100001000f);
    }

    @Test
    void multiScoreAverages() {
      // 3e38 + 3e38 overflows a float running sum, the average 1e38 does not
      final float[] scores = { 3.0e38f, 3.0e38f, -3.0e38f };
      assertThat(sql("SELECT vector.multiScore(?, 'AVG') AS r", (Object) scores)).isEqualTo(1.0e38f);
      assertThat(sql("SELECT vector.multiScore(?, 'WEIGHTED', ?) AS r", scores, new float[] { 1f, 1f, 1f })).isEqualTo(1.0e38f);
      // each score x weight product overflows a float, the weighted sum 35 over the weight sum 7 does not
      assertThat(sql("SELECT vector.multiScore(?, 'WEIGHTED', ?) AS r", A, B)).isEqualTo(5.0f);
      // the weight sum 6e38 overflows a float, the weighted average 1 does not
      assertThat(sql("SELECT vector.multiScore(?, 'WEIGHTED', ?) AS r", new float[] { 1f, 1f },
          new float[] { 3.0e38f, 3.0e38f })).isEqualTo(1.0f);
    }

    @Test
    void cypherVectorDistanceEuclideanSquared() {
      try (final ResultSet rs = database.query("opencypher", "RETURN vector_distance($a, $b, EUCLIDEAN_SQUARED) AS r",
          Map.of("a", new float[] { 2.0e19f }, "b", new float[] { -1.0e19f }))) {
        final double r = ((Number) rs.next().getProperty("r")).doubleValue();
        assertThat(r).isCloseTo(9.0e38, withinPercentage(0.0001));
      }
    }

    private Object sql(final String query, final Object... args) {
      try (final ResultSet rs = database.query("sql", query, args)) {
        return rs.next().getProperty("r");
      }
    }
  }
}
