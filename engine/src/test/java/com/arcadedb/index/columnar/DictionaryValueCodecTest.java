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
package com.arcadedb.index.columnar;

import com.arcadedb.database.Binary;
import com.arcadedb.database.RID;
import com.arcadedb.engine.block.BlockCorruptionException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DictionaryValueCodecTest {
  private static final List<Object> VALUES = List.of("", "hello", "café 中文", 0L, -1L, Long.MIN_VALUE, Long.MAX_VALUE,
      new BigDecimal("1.5"), new BigDecimal("-12345678901234567890.123456789"), new BigDecimal("1E+20"), 2.5d, Double.NaN,
      Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, -0.0d, true, false, new RID(3, 7), new RID(0, 0), new RID(2147483647, 1L << 40));

  @Test
  void everySupportedTypeRoundTrips() {
    for (final Object value : VALUES) {
      assertThat(DictionaryValueCodec.isSupported(value)).as(String.valueOf(value)).isTrue();
      final Binary out = new Binary(16);
      final int written = DictionaryValueCodec.encode(value, out);
      assertThat(written).isEqualTo(out.size());
      assertThat(DictionaryValueCodec.decode(new Binary(out.toByteArray()))).isEqualTo(value);
    }
  }

  @Test
  void unsupportedValuesAreRejected() {
    assertThat(DictionaryValueCodec.isSupported(null)).isFalse();
    assertThat(DictionaryValueCodec.isSupported(Map.of())).isFalse();
    assertThat(DictionaryValueCodec.isSupported(1)).isFalse();
    assertThat(DictionaryValueCodec.isSupported(List.of())).isFalse();
    assertThatThrownBy(() -> DictionaryValueCodec.encode(Map.of(), new Binary(16))).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void compareValuesOrdersByClassThenNaturalOrder() {
    assertThat(DictionaryValueCodec.compareValues("zzz", 1L)).isNegative();
    assertThat(DictionaryValueCodec.compareValues(9d, false)).isNegative();
    assertThat(DictionaryValueCodec.compareValues(Double.NaN, false)).isNegative();
    assertThat(DictionaryValueCodec.compareValues(true, new RID(0, 0))).isNegative();
    assertThat(DictionaryValueCodec.compareValues("B", "a")).isNegative();
    assertThat(DictionaryValueCodec.compareValues("a", "a")).isZero();
    assertThat(DictionaryValueCodec.compareValues(-5L, 3L)).isNegative();
    assertThat(DictionaryValueCodec.compareValues(new BigDecimal("1.5"), new BigDecimal("1.25"))).isPositive();
    assertThat(DictionaryValueCodec.compareValues(false, true)).isNegative();
    assertThat(DictionaryValueCodec.compareValues(new RID(1, 9), new RID(2, 0))).isNegative();
    assertThat(DictionaryValueCodec.compareValues(-0.0d, 0.0d)).isNegative();
  }

  @Test
  void numbersOfAllThreeTypesFormOneExactOrder() {
    final List<Object> ascending = List.of(Double.NEGATIVE_INFINITY, new BigDecimal("-1E+30"), Long.MIN_VALUE, -2.5d, -1L, 0L, 1L,
        new BigDecimal("1.5"), 2L, Long.MAX_VALUE, new BigDecimal("9223372036854775807.5"), 1.0E19d, new BigDecimal("1E+40"),
        Double.POSITIVE_INFINITY, Double.NaN);
    for (int i = 0; i < ascending.size(); i++)
      for (int j = 0; j < ascending.size(); j++)
        assertThat(Integer.signum(DictionaryValueCodec.compareValues(ascending.get(i), ascending.get(j))))
            .as(ascending.get(i) + " vs " + ascending.get(j)).isEqualTo(Integer.signum(Integer.compare(i, j)));
    // a double right next to a long it cannot represent: 2^63 is above Long.MAX_VALUE, 2^53 + 1 is not a double
    assertThat(DictionaryValueCodec.compareValues(9.223372036854775807E18d, Long.MAX_VALUE)).isPositive();
    assertThat(DictionaryValueCodec.compareValues(9007199254740992d, 9007199254740993L)).isNegative();
    assertThat(DictionaryValueCodec.compareValues(0.5d, 0L)).isPositive();
    assertThat(DictionaryValueCodec.compareValues(-0.5d, 0L)).isNegative();
    assertThat(DictionaryValueCodec.compareValues(new BigDecimal("0.1"), 0.1d)).isNegative();
  }

  @Test
  void corruptBytesThrowBlockCorruptionException() {
    final Binary out = new Binary(16);
    DictionaryValueCodec.encode(new BigDecimal("123456.789"), out);
    final byte[] good = out.toByteArray();
    // every truncation of a valid encoding is corrupt, never an unchecked buffer exception
    for (int len = 1; len < good.length; len++) {
      final byte[] cut = Arrays.copyOf(good, len);
      assertThatThrownBy(() -> DictionaryValueCodec.decode(new Binary(cut))).as("len " + len).isInstanceOf(BlockCorruptionException.class);
    }
    assertThatThrownBy(() -> DictionaryValueCodec.decode(new Binary(new byte[] { 99, 1, 2 }))).isInstanceOf(BlockCorruptionException.class);
    assertThatThrownBy(() -> DictionaryValueCodec.decode(new Binary(new byte[] { DictionaryValueCodec.STRING, 120 })))
        .isInstanceOf(BlockCorruptionException.class);
    assertThatThrownBy(() -> DictionaryValueCodec.decode(new Binary(new byte[0]))).isInstanceOf(BlockCorruptionException.class);
  }
}
