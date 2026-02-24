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
package com.arcadedb.engine.timeseries.codec;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class DictionaryCodecTest {

  @Test
  void testEmpty() throws IOException {
    assertThat(DictionaryCodec.decode(DictionaryCodec.encode(new String[0]))).isEmpty();
    assertThat(DictionaryCodec.decode(DictionaryCodec.encode(null))).isEmpty();
  }

  @Test
  void testSingleValue() throws IOException {
    final String[] input = { "sensor_a" };
    assertThat(DictionaryCodec.decode(DictionaryCodec.encode(input))).containsExactly(input);
  }

  @Test
  void testSingleUniqueRepeated() throws IOException {
    final String[] input = { "host1", "host1", "host1", "host1", "host1" };
    final byte[] encoded = DictionaryCodec.encode(input);
    assertThat(DictionaryCodec.decode(encoded)).containsExactly(input);

    // Very compact: 1 dict entry + 5 × 2-byte indices
    assertThat(encoded.length).isLessThan(input.length * 10);
  }

  @Test
  void testMultipleUnique() throws IOException {
    final String[] input = new String[100];
    for (int i = 0; i < input.length; i++)
      input[i] = "sensor_" + (i % 10);

    assertThat(DictionaryCodec.decode(DictionaryCodec.encode(input))).containsExactly(input);
  }

  @Test
  void testEmptyStrings() throws IOException {
    final String[] input = { "", "", "a", "", "b" };
    assertThat(DictionaryCodec.decode(DictionaryCodec.encode(input))).containsExactly(input);
  }

  @Test
  void testUnicodeStrings() throws IOException {
    final String[] input = { "温度", "湿度", "温度", "气压", "湿度" };
    assertThat(DictionaryCodec.decode(DictionaryCodec.encode(input))).containsExactly(input);
  }

  @Test
  void testManyUniqueValues() throws IOException {
    final String[] input = new String[1000];
    for (int i = 0; i < input.length; i++)
      input[i] = "unique_tag_" + i;

    assertThat(DictionaryCodec.decode(DictionaryCodec.encode(input))).containsExactly(input);
  }

  @Test
  void testPreservesOrder() throws IOException {
    final String[] input = { "c", "a", "b", "a", "c", "b" };
    assertThat(DictionaryCodec.decode(DictionaryCodec.encode(input))).containsExactly(input);
  }

  @Test
  void testMalformedDataThrowsIOException() {
    final byte[] malformed = new byte[] { 0, 0, 0, 5 }; // count=5 but no data follows
    assertThatThrownBy(() -> DictionaryCodec.decode(malformed))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("malformed");
  }

  @Test
  void testUtf8LengthGuard() {
    // A string whose UTF-8 encoding exceeds 65535 bytes must be rejected with a clear error.
    // Each '豆' character encodes to 3 UTF-8 bytes, so 21845 repetitions = 65535 bytes.
    final String longEntry = "豆".repeat(21846); // 21846 × 3 = 65538 bytes > 65535
    final String[] input = { longEntry };
    assertThatThrownBy(() -> DictionaryCodec.encode(input))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("too long");
  }

  @Test
  void testDictionaryOverflow() {
    // More than MAX_DICTIONARY_SIZE distinct values must be rejected.
    final String[] input = new String[DictionaryCodec.MAX_DICTIONARY_SIZE + 1];
    for (int i = 0; i <= DictionaryCodec.MAX_DICTIONARY_SIZE; i++)
      input[i] = "v_" + i;
    assertThatThrownBy(() -> DictionaryCodec.encode(input))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Dictionary overflow");
  }
}
