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
package com.arcadedb.function.sql;

import com.arcadedb.exception.CommandSQLParsingException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10611: {@link FunctionOptions#getInt} and {@link FunctionOptions#getLong} cut a {@link Number} down with
 * {@code intValue()} / {@code longValue()}, so {@code rerank: 2.5} was read as 2 and {@code groupSize: 3000000000}
 * wrapped into a negative int. An option that names a whole number now refuses a value that is not one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class FunctionOptionsTest {
  private static final Set<String> ALLOWED = Set.of("n");

  private static FunctionOptions with(final Object value) {
    return new FunctionOptions("test.fn", Map.of("n", value), ALLOWED);
  }

  @Test
  void aWholeNumberOfAnyNumericTypeIsRead() {
    assertThat(with(7).getInt("n", -1)).isEqualTo(7);
    assertThat(with(7L).getInt("n", -1)).isEqualTo(7);
    assertThat(with((short) 7).getInt("n", -1)).isEqualTo(7);
    assertThat(with((byte) 7).getInt("n", -1)).isEqualTo(7);
    assertThat(with(7.0d).getInt("n", -1)).isEqualTo(7);
    assertThat(with(7.0f).getInt("n", -1)).isEqualTo(7);
    assertThat(with(new BigDecimal("7.000")).getInt("n", -1)).isEqualTo(7);
    assertThat(with(BigInteger.valueOf(7)).getInt("n", -1)).isEqualTo(7);
    assertThat(with("7").getInt("n", -1)).isEqualTo(7);
    assertThat(with(-3.0d).getInt("n", 0)).isEqualTo(-3);
    assertThat(with(Integer.MAX_VALUE).getInt("n", 0)).isEqualTo(Integer.MAX_VALUE);
    assertThat(with(Integer.MIN_VALUE).getInt("n", 0)).isEqualTo(Integer.MIN_VALUE);
  }

  @Test
  void anAbsentOptionAnswersTheDefault() {
    final FunctionOptions empty = new FunctionOptions("test.fn", Map.of(), ALLOWED);
    assertThat(empty.getInt("n", 42)).isEqualTo(42);
    assertThat(empty.getLong("n", 42L)).isEqualTo(42L);
  }

  @Test
  void aNumberWithAFractionalPartIsRefusedByGetInt() {
    for (final Object fractional : new Object[] { 2.5d, 2.5f, new BigDecimal("2.5"), -0.5d, 1e-9d })
      assertThatThrownBy(() -> with(fractional).getInt("n", -1)).as("getInt(%s)", fractional)
          .isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("'n'")
          .hasMessageContaining("test.fn")
          .hasMessageContaining("must be an integer")
          .hasMessageContaining(String.valueOf(fractional));
  }

  @Test
  void aNumberThatIsNotFiniteIsRefused() {
    for (final Object notFinite : new Object[] { Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Float.NaN })
      assertThatThrownBy(() -> with(notFinite).getInt("n", -1)).as("getInt(%s)", notFinite)
          .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("must be an integer");
  }

  @Test
  void aNumberOutsideTheIntRangeIsRefusedInsteadOfWrapping() {
    for (final Object tooBig : new Object[] { 3_000_000_000L, -3_000_000_000L, 3e9d, BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE),
        new BigDecimal("1e30") })
      assertThatThrownBy(() -> with(tooBig).getInt("n", -1)).as("getInt(%s)", tooBig)
          .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("must be an integer");
  }

  @Test
  void aTextThatIsNotAnIntegerIsStillRefused() {
    for (final String text : new String[] { "2.5", "abc", "" })
      assertThatThrownBy(() -> with(text).getInt("n", -1)).isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("must be an integer");
  }

  @Test
  void getLongRefusesAFractionAndAnOverflowToo() {
    assertThat(with(7).getLong("n", -1L)).isEqualTo(7L);
    assertThat(with(3_000_000_000L).getLong("n", -1L)).isEqualTo(3_000_000_000L);
    assertThat(with(3e9d).getLong("n", -1L)).isEqualTo(3_000_000_000L);
    assertThat(with(Long.MAX_VALUE).getLong("n", -1L)).isEqualTo(Long.MAX_VALUE);
    assertThat(with(Long.MIN_VALUE).getLong("n", -1L)).isEqualTo(Long.MIN_VALUE);
    assertThat(with(new BigDecimal("12.000")).getLong("n", -1L)).isEqualTo(12L);

    for (final Object bad : new Object[] { 2.5d, new BigDecimal("2.5"), Double.NaN, Double.POSITIVE_INFINITY, 1e30d,
        BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE) })
      assertThatThrownBy(() -> with(bad).getLong("n", -1L)).as("getLong(%s)", bad)
          .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("must be a long");
  }
}
