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
package com.arcadedb.function.number;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8567: {@code number.format()} rounded every argument to a double, so an exact integer
 * beyond 2^53 was rendered as a different number.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8567NumberFormatExactIntegerTest {
  private final NumberFormat function = new NumberFormat();

  @Test
  void longBeyondDoublePrecisionIsExact() {
    assertThat(function.execute(new Object[] { 123456789012345678L }, null)).isEqualTo("123,456,789,012,345,678");
    assertThat(function.execute(new Object[] { 9007199254740993L }, null)).isEqualTo("9,007,199,254,740,993");
  }

  @Test
  void bigNumbersAreExact() {
    assertThat(function.execute(new Object[] { new BigInteger("123456789012345678901234567890") }, null)).isEqualTo(
        "123,456,789,012,345,678,901,234,567,890");
    assertThat(function.execute(new Object[] { new BigDecimal("9007199254740993.125") }, null)).isEqualTo("9,007,199,254,740,993.125");
  }

  @Test
  void floatingPointAndPatternStillWork() {
    assertThat(function.execute(new Object[] { 1234.5678 }, null)).isEqualTo("1,234.568");
    assertThat(function.execute(new Object[] { 7, "000.00" }, null)).isEqualTo("007.00");
  }

  @Test
  void otherNumberSubclassesFallBackToDouble() {
    assertThat(function.execute(new Object[] { new java.util.concurrent.atomic.LongAdder() }, null)).isEqualTo("0");
    assertThat(function.execute(new Object[] { (short) 12, "000" }, null)).isEqualTo("012");
    assertThat(function.execute(new Object[] { 1.5f }, null)).isEqualTo("1.5");
  }
}
