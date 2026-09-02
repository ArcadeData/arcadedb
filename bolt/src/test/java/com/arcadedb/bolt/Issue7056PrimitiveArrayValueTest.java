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
package com.arcadedb.bolt;

import com.arcadedb.bolt.packstream.PackStreamReader;
import com.arcadedb.bolt.packstream.PackStreamWriter;
import com.arcadedb.bolt.structure.BoltStructureMapper;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;

/**
 * Regression test for issue #7056: a property declared ARRAY_OF_FLOATS (or any other primitive array type) is stored
 * as the matching Java primitive array, which {@code instanceof Object[]} never matches. Such a value fell through
 * every branch of the value mapper and reached the {@code toString()} default, so a Bolt client read back the array's
 * identity string - {@code [F@294b13ce} - with no error to say the value had been lost. The very same row read over
 * HTTP/SQL returned the values, so only the Bolt read path was affected.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7056PrimitiveArrayValueTest {

  @Test
  void floatArrayIsMappedToAListOfNumbers() {
    final Object mapped = BoltStructureMapper.toPackStreamValue(new float[] { 0.1f, 0.2f, 0.3f, 0.4f });

    assertThat(mapped).as("an embedding must reach the wire as a list, not as the array's toString()").isInstanceOf(List.class);

    final List<?> values = (List<?>) mapped;
    assertThat(values).hasSize(4).allSatisfy(item -> assertThat(item).isInstanceOf(Double.class));
    // A float widened to double is not exactly the decimal literal, hence the tolerance.
    assertThat(((Number) values.get(0)).doubleValue()).isCloseTo(0.1, offset(1e-6));
    assertThat(((Number) values.get(3)).doubleValue()).isCloseTo(0.4, offset(1e-6));
  }

  @Test
  void everyPrimitiveArrayTypeIsMappedToAList() {
    assertThat(BoltStructureMapper.toPackStreamValue(new double[] { 1.5, 2.5 })).isEqualTo(List.of(1.5d, 2.5d));
    assertThat(BoltStructureMapper.toPackStreamValue(new int[] { 1, 2 })).isEqualTo(List.of(1L, 2L));
    assertThat(BoltStructureMapper.toPackStreamValue(new long[] { 1L, 2L })).isEqualTo(List.of(1L, 2L));
    assertThat(BoltStructureMapper.toPackStreamValue(new short[] { 1, 2 })).isEqualTo(List.of(1L, 2L));
    assertThat(BoltStructureMapper.toPackStreamValue(new boolean[] { true, false })).isEqualTo(List.of(true, false));
    assertThat(BoltStructureMapper.toPackStreamValue(new char[] { 'a', 'b' })).isEqualTo(List.of("a", "b"));
    assertThat(BoltStructureMapper.toPackStreamValue(new String[] { "a", "b" })).isEqualTo(List.of("a", "b"));
  }

  @Test
  void binaryStaysBytesAndIsNotExpandedToAList() {
    final byte[] binary = { 1, 2, 3 };
    assertThat(BoltStructureMapper.toPackStreamValue(binary))
        .as("a byte[] is the BINARY scalar and travels as PackStream Bytes, not as a list of numbers")
        .isSameAs(binary);
  }

  @Test
  void aSetIsMappedToAListLikeAnyOtherCollection() {
    final Set<String> set = new LinkedHashSet<>(List.of("a", "b"));
    assertThat(BoltStructureMapper.toPackStreamValue(set)).isEqualTo(List.of("a", "b"));
  }

  @Test
  void theWriterAlsoRefusesToStringifyAMultiValueItIsHandedDirectly() throws IOException {
    // Defense in depth: even if a raw array reaches the writer unconverted, it must go out as a PackStream list
    // rather than as the array's identity string.
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeValue(new float[] { 1.0f, 2.0f });

    final Object read = new PackStreamReader(writer.toByteArray()).readValue();
    assertThat(read).isInstanceOf(List.class);
    assertThat(((List<?>) read)).hasSize(2);
    assertThat(((Number) ((List<?>) read).get(1)).doubleValue()).isEqualTo(2.0);
  }
}
