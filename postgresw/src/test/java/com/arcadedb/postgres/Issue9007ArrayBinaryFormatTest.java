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
package com.arcadedb.postgres;

import com.arcadedb.database.Binary;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #9007: an array column requested in binary format was sent as the text literal {@code {1,2,3}}
 * instead of PostgreSQL's binary array layout, so pgjdbc failed with BufferUnderflowException from the 6th execution on.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9007ArrayBinaryFormatTest {

  private static byte[] encode(final PostgresType arrayType, final Object value) {
    final Binary buffer = new Binary();
    arrayType.serializeAsBinary(arrayType, buffer, value);
    buffer.flip();
    final int length = buffer.getInt();
    final byte[] data = new byte[length];
    buffer.getByteArray(data);
    assertThat(buffer.size() - buffer.position()).as("the declared length covers the whole value").isZero();
    return data;
  }

  @Test
  void intArrayUsesTheBinaryArrayHeader() {
    final ByteBuffer data = ByteBuffer.wrap(encode(PostgresType.ARRAY_INT, List.of(1, 2, 3)));
    assertThat(data.getInt()).as("dimensions").isEqualTo(1);
    assertThat(data.getInt()).as("has null").isZero();
    assertThat(data.getInt()).as("element OID").isEqualTo(PostgresType.INTEGER.code);
    assertThat(data.getInt()).as("dimension length").isEqualTo(3);
    assertThat(data.getInt()).as("lower bound").isEqualTo(1);
    for (int i = 1; i <= 3; i++) {
      assertThat(data.getInt()).as("element length").isEqualTo(4);
      assertThat(data.getInt()).isEqualTo(i);
    }
    assertThat(data.remaining()).isZero();
  }

  @Test
  void everyArrayTypeRoundTripsThroughTheBinaryDecoder() {
    assertRoundTrip(PostgresType.ARRAY_INT, List.of(1, -2, 3), List.of(1, -2, 3));
    assertRoundTrip(PostgresType.ARRAY_LONG, List.of(1L, Long.MAX_VALUE), List.of(1L, Long.MAX_VALUE));
    assertRoundTrip(PostgresType.ARRAY_REAL, List.of(1.5f, -2.25f), List.of(1.5f, -2.25f));
    assertRoundTrip(PostgresType.ARRAY_DOUBLE, List.of(1.5d, 1e300), List.of(1.5d, 1e300));
    assertRoundTrip(PostgresType.ARRAY_BOOLEAN, List.of(true, false), List.of(true, false));
    assertRoundTrip(PostgresType.ARRAY_TEXT, List.of("a", "b,\"c\"", "é😀"), List.of("a", "b,\"c\"", "é😀"));
    assertRoundTrip(PostgresType.ARRAY_NUMERIC, List.of(new BigDecimal("1.50"), new BigDecimal("-3")),
        List.of(new BigDecimal("1.50"), new BigDecimal("-3")));
  }

  @Test
  void nullElementsAreFlaggedAndRoundTrip() {
    final List<Object> value = new ArrayList<>(Arrays.asList(1, null, 3));
    final byte[] data = encode(PostgresType.ARRAY_INT, value);
    assertThat(ByteBuffer.wrap(data).getInt(4)).as("has null").isEqualTo(1);
    assertThat(PostgresType.deserialize(PostgresType.ARRAY_INT.code, 1, data)).isEqualTo(value);
  }

  @Test
  void emptyArrayHasNoDimensions() {
    final byte[] data = encode(PostgresType.ARRAY_INT, List.of());
    assertThat(data).hasSize(12);
    assertThat(ByteBuffer.wrap(data).getInt()).isZero();
    assertThat(PostgresType.deserialize(PostgresType.ARRAY_INT.code, 1, data)).isEqualTo(List.of());
  }

  @Test
  void primitiveArrayIsEncodedToo() {
    assertThat(PostgresType.deserialize(PostgresType.ARRAY_INT.code, 1, encode(PostgresType.ARRAY_INT, new int[] { 7, 8 })))
        .isEqualTo(List.of(7, 8));
  }

  @Test
  void textArrayHoldingNonStringsEncodesTheirText() {
    assertThat(PostgresType.deserialize(PostgresType.ARRAY_TEXT.code, 1, encode(PostgresType.ARRAY_TEXT, List.of(1, "x", List.of(1, 2)))))
        .isEqualTo(List.of("1", "x", "[1,2]"));
  }

  @Test
  void mixedNumericListsAreConvertedToTheAnnouncedElementType() {
    assertRoundTrip(PostgresType.ARRAY_LONG, List.of(1L, 2, (short) 3), List.of(1L, 2L, 3L));
    assertRoundTrip(PostgresType.ARRAY_INT, List.of(1, 2L, (short) 3), List.of(1, 2, 3));
    assertRoundTrip(PostgresType.ARRAY_DOUBLE, List.of(1.5d, 2, 3L), List.of(1.5d, 2.0d, 3.0d));
  }

  @Test
  void anElementOfAnotherTypeFailsWithAClearError() {
    assertThatThrownBy(() -> encode(PostgresType.ARRAY_LONG, List.of(1L, "x"))).isInstanceOf(PostgresProtocolException.class)
        .hasMessageContaining("String element").hasMessageContaining("_int8");
  }

  @Test
  void aNonArrayValueIsRefusedForAnArrayColumn() {
    assertThatThrownBy(() -> encode(PostgresType.ARRAY_TEXT, "not a list")).isInstanceOf(PostgresProtocolException.class)
        .hasMessageContaining("_text");
  }

  @Test
  void charAndNestedListArraysEncode() {
    assertRoundTrip(PostgresType.ARRAY_CHAR, List.of('a', 'b'), List.of("a", "b"));
    assertRoundTrip(PostgresType.ARRAY_JSON, List.of(List.of(1, 2)), List.of("[1,2]"));
  }

  private static void assertRoundTrip(final PostgresType arrayType, final List<?> value, final List<?> expected) {
    final Object decoded = PostgresType.deserialize(arrayType.code, 1, encode(arrayType, value));
    assertThat(decoded).as(arrayType.name()).isEqualTo(expected);
  }
}
