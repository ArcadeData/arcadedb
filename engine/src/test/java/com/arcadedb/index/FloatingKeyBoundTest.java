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
package com.arcadedb.index;

import com.arcadedb.serializer.BinaryTypes;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class FloatingKeyBoundTest {
  private static final long TWO_53 = 1L << 53;
  private static final long TWO_24 = 1L << 24;

  @Test
  void onlyFloatingKeyTypesAreFloating() {
    assertThat(FloatingKeyBound.isFloating(BinaryTypes.TYPE_DOUBLE)).isTrue();
    assertThat(FloatingKeyBound.isFloating(BinaryTypes.TYPE_FLOAT)).isTrue();
    assertThat(FloatingKeyBound.isFloating(BinaryTypes.TYPE_LONG)).isFalse();
    assertThat(FloatingKeyBound.isFloating(BinaryTypes.TYPE_STRING)).isFalse();
  }

  @Test
  void anIntegerIsLossyOnlyPastTheRangeTheKeyHoldsExactly() {
    assertThat(FloatingKeyBound.isLossy(BinaryTypes.TYPE_DOUBLE, TWO_53)).isFalse();
    assertThat(FloatingKeyBound.isLossy(BinaryTypes.TYPE_DOUBLE, -TWO_53)).isFalse();
    assertThat(FloatingKeyBound.isLossy(BinaryTypes.TYPE_DOUBLE, TWO_53 + 1)).isTrue();
    assertThat(FloatingKeyBound.isLossy(BinaryTypes.TYPE_DOUBLE, -TWO_53 - 1)).isTrue();
    assertThat(FloatingKeyBound.isLossy(BinaryTypes.TYPE_DOUBLE, Integer.MAX_VALUE)).isFalse();
    assertThat(FloatingKeyBound.isLossy(BinaryTypes.TYPE_DOUBLE, Long.MIN_VALUE)).isTrue();
    assertThat(FloatingKeyBound.isLossy(BinaryTypes.TYPE_DOUBLE, Long.MAX_VALUE)).isTrue();
    assertThat(FloatingKeyBound.isLossy(BinaryTypes.TYPE_DOUBLE, BigInteger.valueOf(TWO_53))).isFalse();
    assertThat(FloatingKeyBound.isLossy(BinaryTypes.TYPE_DOUBLE, BigInteger.valueOf(TWO_53 + 1))).isTrue();
    assertThat(FloatingKeyBound.isLossy(BinaryTypes.TYPE_DOUBLE, BigInteger.TWO.pow(70))).isTrue();
    assertThat(FloatingKeyBound.isLossy(BinaryTypes.TYPE_FLOAT, TWO_24)).isFalse();
    assertThat(FloatingKeyBound.isLossy(BinaryTypes.TYPE_FLOAT, TWO_24 + 1)).isTrue();
    assertThat(FloatingKeyBound.isLossy(BinaryTypes.TYPE_FLOAT, Integer.MAX_VALUE)).isTrue();
    assertThat(FloatingKeyBound.isLossy(BinaryTypes.TYPE_FLOAT, (short) 100)).isFalse();
  }

  @Test
  void aDecimalIsLossyUnlessTheScanReadsTheKeyAsIt() {
    assertThat(FloatingKeyBound.isLossy(BinaryTypes.TYPE_DOUBLE, new BigDecimal("1.5"))).isFalse();
    assertThat(FloatingKeyBound.isLossy(BinaryTypes.TYPE_DOUBLE, new BigDecimal("0.1"))).as("the decimal the scan reads 0.1d as").isFalse();
    assertThat(FloatingKeyBound.isLossy(BinaryTypes.TYPE_DOUBLE, new BigDecimal("0.100000000000000006"))).isTrue();
    assertThat(FloatingKeyBound.isLossy(BinaryTypes.TYPE_FLOAT, new BigDecimal("0.5"))).isFalse();
    assertThat(FloatingKeyBound.isLossy(BinaryTypes.TYPE_FLOAT, new BigDecimal("0.1"))).as("the scan reads a float as its double").isTrue();
    assertThat(FloatingKeyBound.isLossy(BinaryTypes.TYPE_FLOAT, new BigDecimal("0.10000000149"))).isTrue();
    assertThat(FloatingKeyBound.isLossy(BinaryTypes.TYPE_DOUBLE, new BigDecimal("1e400"))).as("overflows to an infinity").isTrue();
    assertThat(FloatingKeyBound.isLossy(BinaryTypes.TYPE_DOUBLE, new BigDecimal(0.1d)))
        .as("the binary expansion of 0.1 is not the 0.1 the scan reads the key as").isTrue();
    assertThat(FloatingKeyBound.isLossy(BinaryTypes.TYPE_FLOAT, new BigDecimal(0.1f))).isTrue();
  }

  @Test
  void aFloatingPointBoundAndAnotherKeyTypeAreNeverLossy() {
    assertThat(FloatingKeyBound.isLossy(BinaryTypes.TYPE_DOUBLE, 0.1d)).isFalse();
    assertThat(FloatingKeyBound.isLossy(BinaryTypes.TYPE_FLOAT, 0.1d)).isFalse();
    assertThat(FloatingKeyBound.isLossy(BinaryTypes.TYPE_FLOAT, 0.1f)).isFalse();
    assertThat(FloatingKeyBound.isLossy(BinaryTypes.TYPE_DOUBLE, "text")).isFalse();
    assertThat(FloatingKeyBound.isLossy(BinaryTypes.TYPE_DOUBLE, null)).isFalse();
    assertThat(FloatingKeyBound.isLossy(BinaryTypes.TYPE_LONG, TWO_53 + 1)).isFalse();
  }

  @Test
  void theRoundedKeyIsTheOneTheKeyTypeStores() {
    assertThat(FloatingKeyBound.rounded(BinaryTypes.TYPE_DOUBLE, TWO_53 + 1)).isEqualTo((double) TWO_53).isInstanceOf(Double.class);
    assertThat(FloatingKeyBound.rounded(BinaryTypes.TYPE_DOUBLE, new BigDecimal("0.1"))).isEqualTo(0.1d);
    assertThat(FloatingKeyBound.rounded(BinaryTypes.TYPE_FLOAT, TWO_24 + 1)).isEqualTo((float) TWO_24).isInstanceOf(Float.class);
  }

  @Test
  void theWidenedBoundsSurroundTheRoundedKeyInTheKeyType() {
    final double rounded = (double) (TWO_53 + 3);
    assertThat(FloatingKeyBound.below(BinaryTypes.TYPE_DOUBLE, TWO_53 + 3)).isEqualTo(Math.nextDown(rounded)).isInstanceOf(Double.class);
    assertThat(FloatingKeyBound.above(BinaryTypes.TYPE_DOUBLE, TWO_53 + 3)).isEqualTo(Math.nextUp(rounded)).isInstanceOf(Double.class);
    final float roundedFloat = (float) (TWO_24 + 1);
    assertThat(FloatingKeyBound.below(BinaryTypes.TYPE_FLOAT, TWO_24 + 1)).isEqualTo(Math.nextDown(roundedFloat)).isInstanceOf(Float.class);
    assertThat(FloatingKeyBound.above(BinaryTypes.TYPE_FLOAT, TWO_24 + 1)).isEqualTo(Math.nextUp(roundedFloat)).isInstanceOf(Float.class);
  }
}
