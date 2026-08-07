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

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NumberUtilsTest {

  @Test
  void parsePositiveIntegerWithValidNumber() {
    assertThat(NumberUtils.parsePositiveInteger("123")).isEqualTo(123);
    assertThat(NumberUtils.parsePositiveInteger("0")).isEqualTo(0);
    assertThat(NumberUtils.parsePositiveInteger("999999")).isEqualTo(999999);
  }

  @Test
  void parsePositiveIntegerWithNegativeNumberReturnsNull() {
    // parsePositiveInteger returns null for non-digit chars (including minus sign)
    assertThat(NumberUtils.parsePositiveInteger("-1")).isNull();
  }

  @Test
  void parsePositiveIntegerWithInvalidStringReturnsNull() {
    // parsePositiveInteger returns null for invalid strings
    assertThat(NumberUtils.parsePositiveInteger("abc")).isNull();
  }

  @Test
  void parsePositiveIntegerWithEmptyStringThrows() {
    // Empty string: loop doesn't run (no chars), then Integer.parseInt("") throws NumberFormatException
    assertThatThrownBy(() -> NumberUtils.parsePositiveInteger(""))
        .isInstanceOf(NumberFormatException.class);
  }

  @Test
  void parsePositiveIntegerWithWhitespaceReturnsNull() {
    // Whitespace is not a digit, so returns null
    assertThat(NumberUtils.parsePositiveInteger(" ")).isNull();
  }

  @Test
  void isIntegerNumberWithValidInteger() {
    assertThat(NumberUtils.isIntegerNumber("123")).isTrue();
    assertThat(NumberUtils.isIntegerNumber("-456")).isTrue();
    assertThat(NumberUtils.isIntegerNumber("+789")).isTrue();
    assertThat(NumberUtils.isIntegerNumber("0")).isTrue();
  }

  @Test
  void isIntegerNumberWithInvalidInput() {
    assertThat(NumberUtils.isIntegerNumber("abc")).isFalse();
    assertThat(NumberUtils.isIntegerNumber("12.34")).isFalse();
    // Empty string returns true (no invalid characters to check)
    assertThat(NumberUtils.isIntegerNumber("")).isTrue();
  }

  @Test
  void isIntegerNumberWithSpaces() {
    // Spaces are invalid characters
    assertThat(NumberUtils.isIntegerNumber(" ")).isFalse();
    assertThat(NumberUtils.isIntegerNumber(" 123 ")).isFalse();
  }

  @Test
  void parsePositiveIntegerWithLeadingZeros() {
    assertThat(NumberUtils.parsePositiveInteger("007")).isEqualTo(7);
    assertThat(NumberUtils.parsePositiveInteger("00123")).isEqualTo(123);
  }

  @Test
  void parsePositiveIntegerWithMaxValue() {
    assertThat(NumberUtils.parsePositiveInteger(String.valueOf(Integer.MAX_VALUE)))
        .isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  void saturateToIntWithValueInRangeIsUnchanged() {
    assertThat(NumberUtils.saturateToInt(42)).isEqualTo(42);
    assertThat(NumberUtils.saturateToInt(-42)).isEqualTo(-42);
    assertThat(NumberUtils.saturateToInt(0)).isEqualTo(0);
    assertThat(NumberUtils.saturateToInt(Integer.MAX_VALUE)).isEqualTo(Integer.MAX_VALUE);
    assertThat(NumberUtils.saturateToInt(Integer.MIN_VALUE)).isEqualTo(Integer.MIN_VALUE);
  }

  @Test
  void saturateToIntWithLongBeyondIntRangeSaturates() {
    // Issue #5919 - Site A: a Long above Integer.MAX_VALUE (e.g. LIMIT 2147483648) must not wrap
    // to a negative int via a plain narrowing cast.
    assertThat(NumberUtils.saturateToInt(Integer.MAX_VALUE + 1L)).isEqualTo(Integer.MAX_VALUE);
    assertThat(NumberUtils.saturateToInt(Long.MAX_VALUE)).isEqualTo(Integer.MAX_VALUE);
    assertThat(NumberUtils.saturateToInt(Integer.MIN_VALUE - 1L)).isEqualTo(Integer.MIN_VALUE);
    assertThat(NumberUtils.saturateToInt(Long.MIN_VALUE)).isEqualTo(Integer.MIN_VALUE);
  }

  @Test
  void saturateToIntWithBigIntegerBeyondLongRangeSaturates() {
    // PR #5921 review: Number.longValue() on a BigInteger far outside the Long range is
    // documented as lossy in an unspecified way (it can even flip sign), so a BigInteger bind
    // param needs its own magnitude check instead of going through longValue() first.
    final BigInteger huge = BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.TEN);
    assertThat(NumberUtils.saturateToInt(huge)).isEqualTo(Integer.MAX_VALUE);

    final BigInteger hugeNegative = BigInteger.valueOf(Long.MIN_VALUE).multiply(BigInteger.TEN);
    assertThat(NumberUtils.saturateToInt(hugeNegative)).isEqualTo(Integer.MIN_VALUE);

    assertThat(NumberUtils.saturateToInt(BigInteger.valueOf(7))).isEqualTo(7);
  }

  @Test
  void saturateToIntWithBigDecimalBeyondLongRangeSaturates() {
    final BigDecimal huge = BigDecimal.valueOf(Long.MAX_VALUE).multiply(BigDecimal.TEN);
    assertThat(NumberUtils.saturateToInt(huge)).isEqualTo(Integer.MAX_VALUE);

    final BigDecimal hugeNegative = BigDecimal.valueOf(Long.MIN_VALUE).multiply(BigDecimal.TEN);
    assertThat(NumberUtils.saturateToInt(hugeNegative)).isEqualTo(Integer.MIN_VALUE);

    assertThat(NumberUtils.saturateToInt(BigDecimal.valueOf(7))).isEqualTo(7);
  }
}
