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
import com.arcadedb.bolt.packstream.PackStreamStructure;
import com.arcadedb.bolt.packstream.PackStreamWriter;
import com.arcadedb.bolt.structure.BoltNode;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Edge case tests for PackStream serialization to improve code coverage.
 */
class PackStreamEdgeCasesTest {

  // ============ Integer edge cases ============

  @Test
  void integerBoundaryTinyInt() throws Exception {
    // Test boundary values for TINY_INT range [-16, 127]
    testIntegerRoundTrip(-16);  // Min TINY_INT
    testIntegerRoundTrip(127);  // Max TINY_INT
    testIntegerRoundTrip(0);
    testIntegerRoundTrip(-1);
  }

  @Test
  void integerBoundaryInt8() throws Exception {
    // Test values that require INT_8 encoding
    testIntegerRoundTrip(-17);           // Just below TINY_INT range
    testIntegerRoundTrip(Byte.MIN_VALUE); // -128
    testIntegerRoundTrip(-100);
  }

  @Test
  void integerBoundaryInt16() throws Exception {
    // Test values that require INT_16 encoding
    testIntegerRoundTrip(128);            // Just above Byte.MAX_VALUE
    testIntegerRoundTrip(-129);           // Just below Byte.MIN_VALUE
    testIntegerRoundTrip(Short.MAX_VALUE); // 32767
    testIntegerRoundTrip(Short.MIN_VALUE); // -32768
    testIntegerRoundTrip(1000);
    testIntegerRoundTrip(-1000);
  }

  @Test
  void integerBoundaryInt32() throws Exception {
    // Test values that require INT_32 encoding
    testIntegerRoundTrip(32768);           // Just above Short.MAX_VALUE
    testIntegerRoundTrip(-32769);          // Just below Short.MIN_VALUE
    testIntegerRoundTrip(Integer.MAX_VALUE);
    testIntegerRoundTrip(Integer.MIN_VALUE);
    testIntegerRoundTrip(100000);
    testIntegerRoundTrip(-100000);
  }

  @Test
  void integerBoundaryInt64() throws Exception {
    // Test values that require INT_64 encoding
    testIntegerRoundTrip((long) Integer.MAX_VALUE + 1);
    testIntegerRoundTrip((long) Integer.MIN_VALUE - 1);
    testIntegerRoundTrip(Long.MAX_VALUE);
    testIntegerRoundTrip(Long.MIN_VALUE);
  }

  private void testIntegerRoundTrip(long value) throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeInteger(value);
    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    assertThat(reader.readValue()).isEqualTo(value);
  }

  // ============ String edge cases ============

  @Test
  void stringTiny() throws Exception {
    // TINY_STRING: 0-15 bytes
    testStringRoundTrip("");
    testStringRoundTrip("a");
    testStringRoundTrip("a".repeat(15)); // Max tiny string
  }

  @Test
  void string8() throws Exception {
    // STRING_8: 16-255 bytes
    testStringRoundTrip("a".repeat(16));  // Min STRING_8
    testStringRoundTrip("a".repeat(100));
    testStringRoundTrip("a".repeat(255)); // Max STRING_8
  }

  @Test
  void string16() throws Exception {
    // STRING_16: 256-65535 bytes
    testStringRoundTrip("a".repeat(256));   // Min STRING_16
    testStringRoundTrip("a".repeat(1000));
    testStringRoundTrip("a".repeat(65535)); // Max STRING_16
  }

  @Test
  void string32() throws Exception {
    // STRING_32: > 65535 bytes (only test minimum to save memory)
    final String longString = "a".repeat(65536); // Min STRING_32
    testStringRoundTrip(longString);
  }

  @Test
  void stringNull() throws Exception {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeString(null);
    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    assertThat(reader.readValue()).isNull();
  }

  private void testStringRoundTrip(String value) throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeString(value);
    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    assertThat(reader.readValue()).isEqualTo(value);
  }

  // ============ Bytes edge cases ============

  @Test
  void bytes8() throws Exception {
    // BYTES_8: 0-255 bytes
    testBytesRoundTrip(new byte[0]);
    testBytesRoundTrip(new byte[] { 1, 2, 3 });
    testBytesRoundTrip(new byte[255]);
  }

  @Test
  void bytes16() throws Exception {
    // BYTES_16: 256-65535 bytes
    testBytesRoundTrip(new byte[256]);
    testBytesRoundTrip(new byte[1000]);
  }

  @Test
  void bytesNull() throws Exception {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeBytes(null);
    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    assertThat(reader.readValue()).isNull();
  }

  private void testBytesRoundTrip(byte[] value) throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeBytes(value);
    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    assertThat((byte[]) reader.readValue()).isEqualTo(value);
  }

  // ============ List edge cases ============

  @Test
  void listTiny() throws Exception {
    // TINY_LIST: 0-15 items
    testListRoundTrip(List.of());
    testListRoundTrip(List.of(1));
    testListRoundTrip(createIntList(15)); // Max tiny list
  }

  @Test
  void list8() throws Exception {
    // LIST_8: 16-255 items
    testListRoundTrip(createIntList(16));  // Min LIST_8
    testListRoundTrip(createIntList(100));
    testListRoundTrip(createIntList(255)); // Max LIST_8
  }

  @Test
  void list16() throws Exception {
    // LIST_16: 256-65535 items
    testListRoundTrip(createIntList(256));  // Min LIST_16
    testListRoundTrip(createIntList(1000));
  }

  @Test
  void listNull() throws Exception {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeList(null);
    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    assertThat(reader.readValue()).isNull();
  }

  private List<Object> createIntList(int size) {
    final List<Object> list = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      list.add(i);
    }
    return list;
  }

  @SuppressWarnings("unchecked")
  private void testListRoundTrip(List<Object> value) throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeList(value);
    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    final List<Object> result = (List<Object>) reader.readValue();
    assertThat(result).hasSize(value.size());
  }

  // ============ Map edge cases ============

  @Test
  void mapTiny() throws Exception {
    // TINY_MAP: 0-15 entries
    testMapRoundTrip(Map.of());
    testMapRoundTrip(Map.of("a", 1));
    testMapRoundTrip(createIntMap(15)); // Max tiny map
  }

  @Test
  void map8() throws Exception {
    // MAP_8: 16-255 entries
    testMapRoundTrip(createIntMap(16));  // Min MAP_8
    testMapRoundTrip(createIntMap(100));
    testMapRoundTrip(createIntMap(255)); // Max MAP_8
  }

  @Test
  void map16() throws Exception {
    // MAP_16: 256-65535 entries
    testMapRoundTrip(createIntMap(256)); // Min MAP_16
  }

  @Test
  void mapNull() throws Exception {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeMap(null);
    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    assertThat(reader.readValue()).isNull();
  }

  private Map<String, Object> createIntMap(int size) {
    final Map<String, Object> map = new LinkedHashMap<>(size);
    for (int i = 0; i < size; i++) {
      map.put("key" + i, i);
    }
    return map;
  }

  @SuppressWarnings("unchecked")
  private void testMapRoundTrip(Map<String, Object> value) throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeMap(value);
    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    final Map<String, Object> result = (Map<String, Object>) reader.readValue();
    assertThat(result).hasSize(value.size());
  }

  // ============ Structure tests ============

  @Test
  void structureTooManyFields() {
    final PackStreamWriter writer = new PackStreamWriter();
    assertThatThrownBy(() -> writer.writeStructureHeader((byte) 0x4E, 16))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Structure field count too large");
  }

  @Test
  void structureWithPackStreamStructure() throws Exception {
    final BoltNode node = new BoltNode(1L, List.of("Label"), Map.of("key", "value"), "#1:0");
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeValue(node);

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    final Object result = reader.readValue();
    assertThat(result).isInstanceOf(PackStreamReader.StructureValue.class);
  }

  // ============ WriteValue type detection ============

  @Test
  void writeValueBoolean() throws Exception {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeValue(Boolean.TRUE);
    writer.writeValue(Boolean.FALSE);

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    assertThat(reader.readValue()).isEqualTo(true);
    assertThat(reader.readValue()).isEqualTo(false);
  }

  @Test
  void writeValueNumbers() throws Exception {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeValue((byte) 10);
    writer.writeValue((short) 100);
    writer.writeValue(1000);
    writer.writeValue(10000L);
    writer.writeValue(3.14f);
    writer.writeValue(3.14159);

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    assertThat(reader.readValue()).isEqualTo(10L);
    assertThat(reader.readValue()).isEqualTo(100L);
    assertThat(reader.readValue()).isEqualTo(1000L);
    assertThat(reader.readValue()).isEqualTo(10000L);
    assertThat((Double) reader.readValue()).isEqualTo(3.14f, Offset.offset(0.01));
    assertThat((Double) reader.readValue()).isEqualTo(3.14159, Offset.offset(0.00001));
  }

  @Test
  void writeValueUnknownTypeConvertsToString() throws Exception {
    final Object customObject = new Object() {
      @Override
      public String toString() {
        return "CustomToString";
      }
    };

    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeValue(customObject);

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    assertThat(reader.readValue()).isEqualTo("CustomToString");
  }

  // ============ Writer utility methods ============

  @Test
  void writerSize() throws Exception {
    final PackStreamWriter writer = new PackStreamWriter();
    assertThat(writer.size()).isEqualTo(0);

    writer.writeInteger(42);
    assertThat(writer.size()).isGreaterThan(0);
  }

  @Test
  void writerReset() throws Exception {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeString("test");
    assertThat(writer.size()).isGreaterThan(0);

    writer.reset();
    assertThat(writer.size()).isEqualTo(0);
  }

  @Test
  void writerRawMethods() throws Exception {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeRawByte(0x01);          // 1 byte
    writer.writeRawBytes(new byte[] { 0x02, 0x03 }); // 2 bytes
    writer.writeRawShort(0x0405);       // 2 bytes
    writer.writeRawInt(0x06070809);     // 4 bytes

    assertThat(writer.toByteArray()).hasSize(9); // 1 + 2 + 2 + 4 = 9 bytes
  }

  @Test
  void writerWithCustomBuffer() throws Exception {
    final ByteArrayOutputStream customBuffer = new ByteArrayOutputStream();
    final PackStreamWriter writer = new PackStreamWriter(customBuffer);
    writer.writeString("test");

    assertThat(customBuffer.size()).isGreaterThan(0);
    assertThat(writer.toByteArray()).isEqualTo(customBuffer.toByteArray());
  }

  // ============ Float edge cases ============

  @Test
  void floatSpecialValues() throws Exception {
    testFloatRoundTrip(0.0);
    testFloatRoundTrip(-0.0);
    testFloatRoundTrip(Double.MAX_VALUE);
    testFloatRoundTrip(Double.MIN_VALUE);
    testFloatRoundTrip(Double.MIN_NORMAL);
    testFloatRoundTrip(Double.POSITIVE_INFINITY);
    testFloatRoundTrip(Double.NEGATIVE_INFINITY);
  }

  @Test
  void floatNaN() throws Exception {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeFloat(Double.NaN);
    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    assertThat(Double.isNaN((Double) reader.readValue())).isTrue();
  }

  private void testFloatRoundTrip(double value) throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeFloat(value);
    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    assertThat(reader.readValue()).isEqualTo(value);
  }

  // ============ ListHeader tests ============

  @Test
  void listHeaderSizes() throws Exception {
    final PackStreamWriter writer = new PackStreamWriter();

    // Tiny list header (0-15)
    writer.writeListHeader(0);
    writer.writeListHeader(15);

    // LIST_8 (16-255)
    writer.writeListHeader(16);
    writer.writeListHeader(255);

    // LIST_16 (256-65535)
    writer.writeListHeader(256);
    writer.writeListHeader(65535);

    // LIST_32 (> 65535)
    writer.writeListHeader(65536);

    assertThat(writer.size()).isGreaterThan(0);
  }

  // ============ MapHeader tests ============

  @Test
  void mapHeaderSizes() throws Exception {
    final PackStreamWriter writer = new PackStreamWriter();

    // Tiny map header (0-15)
    writer.writeMapHeader(0);
    writer.writeMapHeader(15);

    // MAP_8 (16-255)
    writer.writeMapHeader(16);
    writer.writeMapHeader(255);

    // MAP_16 (256-65535)
    writer.writeMapHeader(256);
    writer.writeMapHeader(65535);

    // MAP_32 (> 65535)
    writer.writeMapHeader(65536);

    assertThat(writer.size()).isGreaterThan(0);
  }

  // ============ Complex nested structures ============

  @Test
  void deeplyNestedStructure() throws Exception {
    // Create a deeply nested structure
    Map<String, Object> level3 = Map.of("value", "deep");
    Map<String, Object> level2 = Map.of("nested", level3);
    Map<String, Object> level1 = Map.of("nested", level2);
    List<Object> list = List.of(level1, level2, level3);

    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeValue(list);

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    @SuppressWarnings("unchecked")
    final List<Object> result = (List<Object>) reader.readValue();
    assertThat(result).hasSize(3);
  }

  @Test
  void mixedTypeList() throws Exception {
    final List<Object> mixed = new ArrayList<>();
    mixed.add(null);
    mixed.add(true);
    mixed.add(false);
    mixed.add((byte) 1);
    mixed.add((short) 2);
    mixed.add(3);
    mixed.add(4L);
    mixed.add(5.5f);
    mixed.add(6.6);
    mixed.add("string");
    mixed.add(new byte[] { 1, 2, 3 });
    mixed.add(List.of(1, 2));
    mixed.add(Map.of("key", "value"));

    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeList(mixed);

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    @SuppressWarnings("unchecked")
    final List<Object> result = (List<Object>) reader.readValue();
    assertThat(result).hasSize(13);
  }
}
