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
package com.arcadedb.utility;

import org.junit.jupiter.api.Test;

import java.util.SplittableRandom;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * JDK17: the expected strings are what {@link Float#toString(float)} and {@link Double#toString(double)} answer on
 * Java 19+. Java 17 renders several of them longer (1.00000003E16 for 1.0E16f), which is what {@link ShortestDecimal}
 * exists to hide.
 */
class ShortestDecimalTest {

  @Test
  void floatsRenderAsTheirShortestDecimal() {
    assertThat(ShortestDecimal.toString(1.0E16f)).isEqualTo("1.0E16");
    assertThat(ShortestDecimal.toString(-1.0E16f)).isEqualTo("-1.0E16");
    assertThat(ShortestDecimal.toString(33554448f)).isEqualTo("3.355445E7");
    assertThat(ShortestDecimal.toString(Float.MIN_VALUE)).isEqualTo("1.4E-45");
    assertThat(ShortestDecimal.toString(Float.MAX_VALUE)).isEqualTo("3.4028235E38");
    assertThat(ShortestDecimal.toString(0.1f)).isEqualTo("0.1");
    assertThat(ShortestDecimal.toString(1e-3f)).isEqualTo("0.001");
    assertThat(ShortestDecimal.toString(1e7f)).isEqualTo("1.0E7");
    assertThat(ShortestDecimal.toString(9999999f)).isEqualTo("9999999.0");
    assertThat(ShortestDecimal.toString(100f)).isEqualTo("100.0");
  }

  @Test
  void doublesRenderAsTheirShortestDecimal() {
    assertThat(ShortestDecimal.toString(2e23)).isEqualTo("2.0E23");
    assertThat(ShortestDecimal.toString(1e23)).isEqualTo("1.0E23");
    assertThat(ShortestDecimal.toString(Double.MIN_VALUE)).isEqualTo("4.9E-324");
    assertThat(ShortestDecimal.toString(Double.MAX_VALUE)).isEqualTo("1.7976931348623157E308");
    assertThat(ShortestDecimal.toString(0.1 + 0.2)).isEqualTo("0.30000000000000004");
    assertThat(ShortestDecimal.toString(-0.001)).isEqualTo("-0.001");
  }

  @Test
  void zeroAndNonFiniteKeepTheJdkText() {
    assertThat(ShortestDecimal.toString(0f)).isEqualTo("0.0");
    assertThat(ShortestDecimal.toString(-0d)).isEqualTo("-0.0");
    assertThat(ShortestDecimal.toString(Float.NaN)).isEqualTo("NaN");
    assertThat(ShortestDecimal.toString(Double.NEGATIVE_INFINITY)).isEqualTo("-Infinity");
  }

  @Test
  void everyRenderingRoundsBackToItsValue() {
    final SplittableRandom random = new SplittableRandom(7);
    for (int i = 0; i < 20_000; i++) {
      final float f = Float.intBitsToFloat(random.nextInt());
      if (Float.isFinite(f))
        assertThat(Float.parseFloat(ShortestDecimal.toString(f))).isEqualTo(f);
      final double d = Double.longBitsToDouble(random.nextLong());
      if (Double.isFinite(d))
        assertThat(Double.parseDouble(ShortestDecimal.toString(d))).isEqualTo(d);
    }
  }
}
