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
package com.arcadedb.schema;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Where {@link Type#numbersEqual} stops reading a DOUBLE/BigDecimal pair as loose as the double's ulp (issue #8882) and
 * starts comparing it exactly (issue #8872): past 17 significant digits, counted without trailing zeros, as the SQL parser
 * counts them when it decides whether a literal is a double.
 */
class DoubleDecimalEqualityBoundaryTest {

  @Test
  void seventeenDigitsReadAsTheDoubleTheyNarrowTo() {
    assertThat(Type.numbersEqual(0.1d, new BigDecimal("0.10000000000000001"))).isTrue();
    assertThat(Type.numbersEqual(new BigDecimal("0.10000000000000001"), 0.1d)).isTrue();
  }

  @Test
  void eighteenDigitsAreComparedExactly() {
    assertThat(Type.numbersEqual(0.1d, new BigDecimal("0.100000000000000006"))).isFalse();
    assertThat(Type.numbersEqual(new BigDecimal("0.100000000000000006"), 0.1d)).isFalse();
    // the exact binary expansion of the stored double still equals it
    assertThat(Type.numbersEqual(0.1d, new BigDecimal(0.1d))).isTrue();
  }

  @Test
  void trailingZerosAreNotSignificantDigits() {
    // 18 digits as written, one significant: the same 0.1 a double holds
    assertThat(Type.numbersEqual(0.1d, new BigDecimal("0.100000000000000000"))).isTrue();
    assertThat(Type.numbersEqual(new BigDecimal("0.100000000000000000"), 0.1d)).isTrue();
    assertThat(Type.numbersEqual(12.0d, new BigDecimal("12.0000000000000000000"))).isTrue();
    assertThat(Type.numbersEqual(1e20d, new BigDecimal("100000000000000000000"))).isTrue();
  }

  @Test
  void aNonFiniteDoubleNeverEqualsADecimal() {
    for (final double d : new double[] { Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY }) {
      assertThat(Type.numbersEqual(d, new BigDecimal("1"))).as("%s", d).isFalse();
      assertThat(Type.numbersEqual(new BigDecimal("0.100000000000000006"), d)).as("%s", d).isFalse();
    }
  }
}
