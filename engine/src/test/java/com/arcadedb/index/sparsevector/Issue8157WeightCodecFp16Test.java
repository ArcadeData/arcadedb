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
package com.arcadedb.index.sparsevector;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * https://github.com/ArcadeData/arcadedb/issues/8157
 * <p>
 * {@code WeightCodec}'s fp16 arm documents itself as IEEE 754 half-precision, "round-to-nearest-even,
 * subnormal-preserving". Its decoder normalised the subnormal significand counting the shifts in a variable
 * incremented at the TOP of the loop, so the exponent it rebuilt was one binade too high and EVERY one of the 2046
 * subnormal patterns decoded to exactly TWICE the value it denotes. Half subnormals cover |w| &lt; 2^-14 (about
 * 6.1e-5), which for a sparse-vector posting list is an ordinary small term weight, so a document's contribution for
 * such a dimension was scored at double its stored weight: wrong similarity scores and wrong top-K order, silently,
 * in the quantization mode that exists precisely to be MORE accurate than the INT8 default.
 * <p>
 * The encoder agreed with the JDK on magnitude but rounded half-AWAY-from-zero rather than to even, worth 1 ulp on
 * an exact tie.
 * <p>
 * The reference is the JDK's own implementation of the same format ({@code Float.float16ToFloat} /
 * {@code Float.floatToFloat16}), and the sweep is EXHAUSTIVE over the whole 16-bit domain: the defect lived entirely
 * inside a range that the in-tree test's eight sampled magnitudes straddled without ever landing in (its smallest,
 * 1e-4, is just above the subnormal threshold).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8157WeightCodecFp16Test {

  /** The number of half-precision patterns whose exponent field is zero and significand is not: 2 signs x 1023. */
  private static final int SUBNORMAL_PATTERNS = 2046;

  @Test
  void everyHalfPatternDecodesExactlyAsTheJdkDoes() {
    int subnormalsChecked = 0;
    for (int h = 0; h <= 0xFFFF; h++) {
      final short pattern = (short) h;
      final float decoded = WeightCodec.fromFp16(pattern);
      final float reference = Float.float16ToFloat(pattern);

      if (Float.isNaN(reference)) {
        assertThat(Float.isNaN(decoded)).as("pattern 0x%04X must stay NaN", h).isTrue();
        continue;
      }
      assertThat(Float.floatToRawIntBits(decoded))
          .as("pattern 0x%04X decoded to %s, the format says %s", h, decoded, reference)
          .isEqualTo(Float.floatToRawIntBits(reference));

      if (((h >>> 10) & 0x1F) == 0 && (h & 0x3FF) != 0)
        ++subnormalsChecked;
    }
    // The sweep has to have actually visited the broken range, not merely covered the normals around it.
    assertThat(subnormalsChecked).isEqualTo(SUBNORMAL_PATTERNS);
  }

  @Test
  void subnormalWeightsRoundTripAtTheirOwnMagnitudeAndNotAtTwiceIt() {
    // Every one of these is below the 2^-14 subnormal threshold and used to come back doubled.
    final float[] subnormalWeights = { 6.0e-5f, 5.0e-5f, 3.0e-5f, 1.0e-5f, 1.0e-6f, 1.0e-7f, -5.0e-5f, -1.0e-6f };
    for (final float w : subnormalWeights) {
      final float back = WeightCodec.fromFp16(WeightCodec.toFp16(w));
      assertThat(back).as("weight %s", w).isNotZero();
      // What the stored pattern denotes, exactly.
      assertThat(back).as("weight %s", w).isEqualTo(Float.float16ToFloat(Float.floatToFloat16(w)));
      // And the defect itself: a subnormal's quantization error grows towards the bottom of the range (2^-24 is the
      // step there, so 1e-7 legitimately comes back ~19% high), but it was never a FACTOR OF TWO. 1.5 separates the
      // two readings for every magnitude in the range and is not a precision claim.
      assertThat(Math.abs(back)).as("weight %s came back as %s", w, back).isLessThan(Math.abs(w) * 1.5f);
    }
  }

  @Test
  void theSmallestSubnormalIsTwoToTheMinusTwentyFour() {
    // The single pattern the issue opens with: significand 1, exponent 0. It denotes 2^-24, and answered 2^-23.
    assertThat(WeightCodec.fromFp16((short) 0x0001)).isEqualTo(Math.scalb(1.0f, -24));
    assertThat(WeightCodec.fromFp16((short) 0x8001)).isEqualTo(-Math.scalb(1.0f, -24));
  }

  @Test
  void encodingMatchesTheJdkForEveryFiniteInRangeValue() {
    final Random random = new Random(8157);
    int checked = 0;
    for (int i = 0; i < 2_000_000; i++) {
      final float f = i < 0x10000 ? Float.float16ToFloat((short) i) : Float.intBitsToFloat(random.nextInt());
      if (Float.isNaN(f) || Math.abs(f) > 65504.0f)
        // Both are deliberate departures, asserted on their own below.
        continue;
      assertThat(WeightCodec.toFp16(f)).as("value %s", f).isEqualTo(Float.floatToFloat16(f));
      ++checked;
    }
    assertThat(checked).isGreaterThan(1_000_000);
  }

  @Test
  void finiteOverflowSaturatesInsteadOfBecomingInfinity() {
    // The segment format's own rule, kept: a weight has to stay a number that can be summed into a score.
    assertThat(WeightCodec.fromFp16(WeightCodec.toFp16(1.0e6f))).isEqualTo(65504.0f);
    assertThat(WeightCodec.fromFp16(WeightCodec.toFp16(-1.0e6f))).isEqualTo(-65504.0f);
    assertThat(WeightCodec.fromFp16(WeightCodec.toFp16(Float.MAX_VALUE))).isEqualTo(65504.0f);
    // An input that already IS infinite stays infinite, which is what the previous implementation did too.
    assertThat(WeightCodec.fromFp16(WeightCodec.toFp16(Float.POSITIVE_INFINITY))).isEqualTo(Float.POSITIVE_INFINITY);
    assertThat(WeightCodec.fromFp16(WeightCodec.toFp16(Float.NEGATIVE_INFINITY))).isEqualTo(Float.NEGATIVE_INFINITY);
  }

  /**
   * A NaN weight's fp16 image can land exactly on {@link SegmentFormat#FP16_TOMBSTONE_SENTINEL} - a negative NaN
   * whose high significand bit is the only one set encodes to 0xFE00 - and would then read back as a DELETED
   * posting. INT8 and fp32 already refuse a NaN weight for the same reason; fp16 was the one that did not.
   */
  @Test
  void nanIsRefusedRatherThanCollidingWithTheTombstoneSentinel() {
    assertThatThrownBy(() -> WeightCodec.toFp16(Float.NaN))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("NaN");
    assertThatThrownBy(() -> WeightCodec.toFp16(Float.intBitsToFloat(0xFFC00000)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void noFiniteWeightEncodesToTheTombstoneSentinel() {
    final Random random = new Random(20260922);
    for (int i = 0; i < 500_000; i++) {
      final float f = Float.intBitsToFloat(random.nextInt());
      if (Float.isNaN(f))
        continue;
      assertThat(WeightCodec.toFp16(f)).isNotEqualTo(SegmentFormat.FP16_TOMBSTONE_SENTINEL);
    }
  }
}
