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
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Regression tests for the vector SQL functions, one nested class per fixed issue.
 */
class VectorApproxDistanceTest {
  /**
   * Regression tests for issue #6386: {@code vector.approxDistance} computed wrong distances for
   * both quantization modes, silently corrupting top-k ordering. The binary (Hamming) path
   * sign-extended {@code byte} operands into {@link Integer#bitCount}; the int8 path masked
   * signed, order-preserving codes with {@code & 0xFF}, breaking the ordering across the
   * mid-range.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ApproxDistanceModesTest {

    private final SQLFunctionVectorQuantizeBinary quantizeBinary = new SQLFunctionVectorQuantizeBinary();
    private final SQLFunctionVectorApproxDistance approxDistance = new SQLFunctionVectorApproxDistance();
    private final BasicCommandContext             context        = new BasicCommandContext();

    @Test
    void binaryApproxDistanceIgnoresTheByteSignBit() {
      // Both quantize to 8 bits with median 1.0; the two vectors differ only on the sign of the
      // last component, so only bit 7 differs -> true Hamming distance is 1, not ~25.
      final Object q1 = quantizeBinary.execute(null, null, null,
          new Object[] { new float[] { 1, 1, 1, 1, 1, 1, 1, 9.0f } }, context);
      final Object q2 = quantizeBinary.execute(null, null, null,
          new Object[] { new float[] { 1, 1, 1, 1, 1, 1, 1, -9.0f } }, context);

      final Object result = approxDistance.execute(null, null, null, new Object[] { q1, q2 }, context);
      assertThat((Float) result).isEqualTo(0.125f, within(0.0001f)); // 1 / 8 bits
    }

    @Test
    void int8ApproxDistanceUsesSignedDifferenceAcrossTheMidpoint() {
      // Codes straddling the signed/unsigned midpoint (-1 and 0): the true (pre-shift) values are
      // 127 and 128, one apart. Masking with & 0xFF would instead read them as 255 and 0.
      final byte[] q1 = { -1 };
      final byte[] q2 = { 0 };

      final Object result = approxDistance.execute(null, null, null, new Object[] { q1, q2, "INT8" }, context);
      assertThat((Float) result).isEqualTo(1.0f, within(0.0001f));
    }

    @Test
    void int8ApproxDistancePreservesTopKOrderingAcrossTheMidpoint() {
      // Query code -1 (raw 127) should rank a neighbor at 0 (raw 128, distance 1) closer than one
      // at 100 (raw 228, distance 101). The pre-fix "& 0xFF" reinterpretation inverted this: it
      // read -1 as 255, making the near neighbor (0) look like the far one (distance 255).
      final byte[] query = { -1 };
      final byte[] near = { 0 };
      final byte[] far = { 100 };

      final float distanceToNear = (Float) approxDistance.execute(null, null, null, new Object[] { query, near, "INT8" }, context);
      final float distanceToFar = (Float) approxDistance.execute(null, null, null, new Object[] { query, far, "INT8" }, context);

      assertThat(distanceToNear).isLessThan(distanceToFar);
    }
  }

  /**
   * Regression tests for issue #9776: {@code vector.approxDistance} on two {@code vector.quantizeInt8()} results dropped
   * each result's min/max and differenced the raw codes, which are positions inside two DIFFERENT bands. It answered 0 for
   * {@code v} against {@code v + 100}, {@code 10 * v} or a constant vector 1990 away, and its ordering on a mixed-scale
   * candidate set agreed with the true L2 ordering no better than chance.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ApproxDistanceInt8BandTest extends TestHelper {
    private final SQLFunctionVectorQuantizeInt8   quantize       = new SQLFunctionVectorQuantizeInt8();
    private final SQLFunctionVectorApproxDistance approxDistance = new SQLFunctionVectorApproxDistance();
    private       BasicCommandContext             context;

    @BeforeEach
    void setupContext() {
      context = new BasicCommandContext();
      context.setDatabase(database);
    }

    @Test
    void sameShapeDifferentOffsetIsNotDistanceZero() {
      final float[] a = { 0.1f, 0.5f, 0.9f };
      final float[] b = { 100.1f, 100.5f, 100.9f };
      assertThat(approx(a, b)).isCloseTo(l2(a, b), within(0.5f));
      assertThat(approx(a, b)).isGreaterThan(170f);
    }

    @Test
    void sameShapeDifferentScaleIsNotDistanceZero() {
      final float[] a = { 0.1f, 0.5f, 0.9f };
      final float[] b = { 1.0f, 5.0f, 9.0f };
      assertThat(approx(a, b)).isCloseTo(l2(a, b), within(0.1f));
    }

    @Test
    void constantVectorsKeepTheirValue() {
      // A constant vector quantizes to all-zero codes on a zero-width band: only the band tells two of them apart
      final float[] a = { 5, 5, 5, 5 };
      final float[] b = { 1000, 1000, 1000, 1000 };
      assertThat(approx(a, b)).isCloseTo(1990f, within(0.01f));
      assertThat(approx(a, a)).isEqualTo(0f);
    }

    @Test
    void nearIsCloserThanFar() {
      final float[] query = { 0, 1, 2, 3 };
      final float[] near = { 0, 1, 2, 3.01f };
      final float[] far = { 0, 10, 20, 30 };
      assertThat(approx(query, near)).isLessThan(approx(query, far));
      assertThat(approx(query, far)).isCloseTo(l2(query, far), within(0.2f));
    }

    @Test
    void orderingAgreesWithTrueL2OnAMixedScaleCandidateSet() {
      // The class javadoc's promise, measured: 200 random 16-dimension candidates with per-candidate scale 0.1..100 and
      // offset -10..10. Raw-code differencing disagreed on ~50% of the pairs, which is what a random ordering produces.
      final Random random = new Random(9776);
      final int candidates = 200;
      final int dims = 16;
      final float[] query = new float[dims];
      for (int d = 0; d < dims; d++)
        query[d] = (float) random.nextGaussian();

      final double[] trueDistance = new double[candidates];
      final double[] approxDistanceValue = new double[candidates];
      final Object quantizedQuery = quantize(query);
      for (int c = 0; c < candidates; c++) {
        final float scale = 0.1f + random.nextFloat() * 99.9f;
        final float offset = -10f + random.nextFloat() * 20f;
        final float[] v = new float[dims];
        for (int d = 0; d < dims; d++)
          v[d] = (float) random.nextGaussian() * scale + offset;
        trueDistance[c] = l2(query, v);
        approxDistanceValue[c] = (Float) approxDistance.execute(null, null, null, new Object[] { quantizedQuery, quantize(v) },
            context);
      }

      int disagreements = 0;
      int pairs = 0;
      for (int i = 0; i < candidates; i++)
        for (int j = i + 1; j < candidates; j++) {
          pairs++;
          if ((trueDistance[i] < trueDistance[j]) != (approxDistanceValue[i] < approxDistanceValue[j]))
            disagreements++;
        }
      assertThat(disagreements).as("ordering disagreements out of %d pairs", pairs).isLessThan(pairs / 1000);

      assertThat(topK(approxDistanceValue, 10)).containsExactlyInAnyOrderElementsOf(topK(trueDistance, 10));
    }

    @Test
    void rawCodeArraysStillMeasureInCodeUnits() {
      // Two raw arrays carry no band: they share the caller's calibration and the distance stays in code units
      final Object result = approxDistance.execute(null, null, null,
          new Object[] { new byte[] { -1, 10 }, new byte[] { 2, 6 }, "INT8" }, context);
      assertThat((Float) result).isEqualTo(5f, within(0.0001f));
    }

    @Test
    void rawCodeListsAreAccepted() {
      final Object result = approxDistance.execute(null, null, null, new Object[] { List.of(-1, 10), List.of(2, 6), "INT8" },
          context);
      assertThat((Float) result).isEqualTo(5f, within(0.0001f));
    }

    @Test
    void rawCodesOutsideTheInt8RangeAreRefusedNotWrapped() {
      // byteValue() would turn 200 into -56, the opposite end of the scale
      assertThatThrownBy(() -> approxDistance.execute(null, null, null, new Object[] { List.of(200, 0), List.of(0, 0), "INT8" },
          context)).isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("[-128, 127]");
      assertThatThrownBy(() -> approxDistance.execute(null, null, null, new Object[] { List.of(1.5, 0), List.of(0, 0), "INT8" },
          context)).isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("integers");
      assertThatThrownBy(() -> approxDistance.execute(null, null, null,
          new Object[] { new Object[] { -129, 0 }, new Object[] { 0, 0 }, "INT8" }, context))
          .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("[-128, 127]");
      // Any Number kind is checked on its value, not wrapped through byteValue()
      assertThatThrownBy(() -> approxDistance.execute(null, null, null, new Object[] { List.of(300L), List.of(0L), "INT8" },
          context)).isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("[-128, 127]");
      assertThatThrownBy(() -> approxDistance.execute(null, null, null,
          new Object[] { List.of(new BigDecimal("0.5")), List.of(BigDecimal.ZERO), "INT8" }, context))
          .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("integers");
      assertThat((Float) approxDistance.execute(null, null, null,
          new Object[] { List.of(-3L, new BigDecimal("4")), List.of(0L, BigDecimal.ZERO), "INT8" }, context)).isEqualTo(5f,
          within(0.0001f));
      // The bounds themselves are codes
      final Object result = approxDistance.execute(null, null, null, new Object[] { List.of(-128), List.of(127.0), "INT8" },
          context);
      assertThat((Float) result).isEqualTo(255f, within(0.0001f));
    }

    @Test
    void nonFiniteValuesCannotBeQuantized() {
      // A NaN or an infinity has no position inside a [min, max] band: it would turn every distance against it into NaN
      assertThatThrownBy(() -> quantize(new float[] { 1f, Float.NaN, 3f })).isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("finite");
      assertThatThrownBy(() -> quantize(new float[] { Float.NaN, 1f })).isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("finite");
      assertThatThrownBy(() -> quantize(new float[] { 1f, Float.POSITIVE_INFINITY })).isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("finite");
    }

    @Test
    void bandWiderThanAFloatStaysFinite() {
      // max - min overflows a float here; the band is carried in double so the distance stays exact
      final float[] a = { -3e38f, 0f, 3e38f };
      final float[] b = { -3e38f, 0f, 3e38f };
      assertThat(approx(a, b)).isEqualTo(0f);
      final float[] back = (float[]) new SQLFunctionVectorDequantizeInt8().execute(null, null, null, new Object[] { quantize(a) },
          context);
      assertThat(back[0]).isEqualTo(-3e38f);
      assertThat(back[2]).isCloseTo(3e38f, within(3e36f));
    }

    @Test
    void resultAgainstRawArrayIsRefused() {
      final Object result = quantize(new float[] { 0.1f, 0.5f, 0.9f });
      assertThatThrownBy(() -> approxDistance.execute(null, null, null, new Object[] { result, new byte[] { 1, 2, 3 }, "INT8" },
          context)).isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("raw int8 array");
      assertThatThrownBy(() -> approxDistance.execute(null, null, null, new Object[] { new byte[] { 1, 2, 3 }, result, "INT8" },
          context)).isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("raw int8 array");
    }

    @Test
    void explicitTypeOnTwoResultsAppliesTheBandsToo() {
      final Object a = quantize(new float[] { 0.1f, 0.5f, 0.9f });
      final Object b = quantize(new float[] { 100.1f, 100.5f, 100.9f });
      final float distance = (Float) approxDistance.execute(null, null, null, new Object[] { a, b, "INT8" }, context);
      assertThat(distance).isCloseTo(173.2051f, within(0.5f));
    }

    @Test
    void sqlQueryAppliesTheBands() {
      try (final ResultSet rs = database.query("sql",
          "SELECT `vector.approxDistance`(`vector.quantizeInt8`([0.1, 0.5, 0.9]), `vector.quantizeInt8`([100.1, 100.5, 100.9])) AS d")) {
        final float d = rs.next().<Number>getProperty("d").floatValue();
        assertThat(d).isCloseTo(173.2051f, within(0.5f));
      }
    }

    private float approx(final float[] a, final float[] b) {
      return (Float) approxDistance.execute(null, null, null, new Object[] { quantize(a), quantize(b) }, context);
    }

    private Object quantize(final float[] v) {
      return quantize.execute(null, null, null, new Object[] { v }, context);
    }

    private static float l2(final float[] a, final float[] b) {
      double sum = 0;
      for (int i = 0; i < a.length; i++) {
        final double d = (double) a[i] - b[i];
        sum += d * d;
      }
      return (float) Math.sqrt(sum);
    }

    private static List<Integer> topK(final double[] distances, final int k) {
      final Integer[] indexes = new Integer[distances.length];
      for (int i = 0; i < indexes.length; i++)
        indexes[i] = i;
      Arrays.sort(indexes, (x, y) -> Double.compare(distances[x], distances[y]));
      return Arrays.asList(indexes).subList(0, k);
    }
  }

  /**
   * Issue #10931: the widened narrowing scan found {@code vector.dequantizeInt8} reading the codes of a raw list through
   * {@code Number.byteValue()}, so 200 became -56 (the opposite end of the band) and 1.5 became 1, silently. It now reads
   * them through the same strict reader as {@code vector.approxDistance} (#9776): an integer in [-128, 127] or a refusal.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class DequantizeInt8CodeRangeTest extends TestHelper {
    private final SQLFunctionVectorDequantizeInt8 dequantize = new SQLFunctionVectorDequantizeInt8();
    private       BasicCommandContext             context;

    @BeforeEach
    void setupContext() {
      context = new BasicCommandContext();
      context.setDatabase(database);
    }

    @Test
    void codesOutsideTheInt8RangeAreRefusedNotWrapped() {
      assertThatThrownBy(() -> dequantize.execute(null, null, null, new Object[] { List.of(200, 0), 0.0, 1.0 }, context))
          .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("[-128, 127]");
      assertThatThrownBy(() -> dequantize.execute(null, null, null, new Object[] { new Object[] { -129, 0 }, 0.0, 1.0 }, context))
          .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("[-128, 127]");
      assertThatThrownBy(() -> dequantize.execute(null, null, null, new Object[] { List.of(300L), 0.0, 1.0 }, context))
          .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("[-128, 127]");
    }

    @Test
    void fractionalCodesAreRefusedNotTruncated() {
      assertThatThrownBy(() -> dequantize.execute(null, null, null, new Object[] { List.of(1.5, 0), 0.0, 1.0 }, context))
          .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("integers");
      assertThatThrownBy(() -> dequantize.execute(null, null, null, new Object[] { List.of(new BigDecimal("0.5")), 0.0, 1.0 },
          context)).isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("integers");
      // A FRACTION A DOUBLE CANNOT HOLD, AND NaN OR AN INFINITY, WHICH A CAST WOULD TURN INTO 0
      assertThatThrownBy(() -> dequantize.execute(null, null, null,
          new Object[] { List.of(new BigDecimal("1.00000000000000000001")), 0.0, 1.0 }, context))
          .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("integers");
      assertThatThrownBy(() -> dequantize.execute(null, null, null, new Object[] { List.of(Double.NaN), 0.0, 1.0 }, context))
          .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("integers");
      assertThatThrownBy(() -> dequantize.execute(null, null, null,
          new Object[] { new Object[] { Float.POSITIVE_INFINITY }, 0.0, 1.0 }, context))
          .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("integers");
      assertThatThrownBy(() -> dequantize.execute(null, null, null, new Object[] { List.of("x"), 0.0, 1.0 }, context))
          .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("must be numbers");
    }

    @Test
    void codesInsideTheRangeOfAnyNumberKindDequantizeAsBefore() {
      // -128 IS THE BOTTOM OF THE BAND, 127 THE TOP
      final float[] fromInts = (float[]) dequantize.execute(null, null, null, new Object[] { List.of(-128, 127), 0.0, 255.0 },
          context);
      final float[] fromMixed = (float[]) dequantize.execute(null, null, null,
          new Object[] { new Object[] { -128L, new BigDecimal("127") }, 0.0, 255.0 }, context);
      assertThat(fromInts[0]).isCloseTo(0f, within(0.001f));
      assertThat(fromInts[1]).isCloseTo(255f, within(0.001f));
      assertThat(fromMixed).containsExactly(fromInts);
    }

    @Test
    void theSqlCallRefusesAnOutOfRangeCode() {
      assertThatThrownBy(() -> {
        try (final ResultSet rs = database.query("sql", "SELECT vector.dequantizeInt8([200, 0], 0.0, 1.0) AS v")) {
          rs.next();
        }
      }).hasMessageContaining("[-128, 127]");
    }
  }
}
