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

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for PackStream serialization and deserialization.
 */
class PackStreamTest {

  @Test
  void testNull() throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeNull();

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    assertThat(reader.readValue()).isNull();
  }

  @Test
  void booleanTrue() throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeBoolean(true);

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    assertThat(reader.readValue()).isEqualTo(true);
  }

  @Test
  void booleanFalse() throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeBoolean(false);

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    assertThat(reader.readValue()).isEqualTo(false);
  }

  @Test
  void tinyIntPositive() throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeInteger(42);

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    assertThat(reader.readValue()).isEqualTo(42L);
  }

  @Test
  void tinyIntNegative() throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeInteger(-10);

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    assertThat(reader.readValue()).isEqualTo(-10L);
  }

  @Test
  void int8() throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeInteger(-100);

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    assertThat(reader.readValue()).isEqualTo(-100L);
  }

  @Test
  void int16() throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeInteger(1000);

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    assertThat(reader.readValue()).isEqualTo(1000L);
  }

  @Test
  void int32() throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeInteger(100000);

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    assertThat(reader.readValue()).isEqualTo(100000L);
  }

  @Test
  void int64() throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeInteger(Long.MAX_VALUE);

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    assertThat(reader.readValue()).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void testFloat() throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeFloat(3.14159);

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    assertThat((Double) reader.readValue()).isCloseTo(3.14159, Offset.offset(0.00001));
  }

  @Test
  void tinyString() throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeString("hello");

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    assertThat(reader.readValue()).isEqualTo("hello");
  }

  @Test
  void string8() throws IOException {
    final String longString = "a".repeat(100);
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeString(longString);

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    assertThat(reader.readValue()).isEqualTo(longString);
  }

  @Test
  void string16() throws IOException {
    final String longString = "a".repeat(1000);
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeString(longString);

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    assertThat(reader.readValue()).isEqualTo(longString);
  }

  @Test
  void emptyString() throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeString("");

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    assertThat(reader.readValue()).isEqualTo("");
  }

  @Test
  void unicodeString() throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeString("Hello 世界! 🌍");

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    assertThat(reader.readValue()).isEqualTo("Hello 世界! 🌍");
  }

  @Test
  void bytes() throws IOException {
    final byte[] data = { 0x01, 0x02, 0x03, 0x04, 0x05 };
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeBytes(data);

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    assertThat((byte[]) reader.readValue()).isEqualTo(data);
  }

  @Test
  void tinyList() throws IOException {
    final List<Object> list = List.of(1, 2, 3);
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeList(list);

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    @SuppressWarnings("unchecked")
    final List<Object> result = (List<Object>) reader.readValue();
    assertThat(result).containsExactly(1L, 2L, 3L);
  }

  @Test
  void mixedList() throws IOException {
    final List<Object> list = List.of(1, "hello", true, 3.14);
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeList(list);

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    @SuppressWarnings("unchecked")
    final List<Object> result = (List<Object>) reader.readValue();
    assertThat(result).hasSize(4);
    assertThat(result.get(0)).isEqualTo(1L);
    assertThat(result.get(1)).isEqualTo("hello");
    assertThat(result.get(2)).isEqualTo(true);
    assertThat((Double) result.get(3)).isCloseTo(3.14, Offset.offset(0.001));
  }

  @Test
  void tinyMap() throws IOException {
    final Map<String, Object> map = Map.of("name", "Alice", "age", 30);
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeMap(map);

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    @SuppressWarnings("unchecked")
    final Map<String, Object> result = (Map<String, Object>) reader.readValue();
    assertThat(result).containsEntry("name", "Alice");
    assertThat(result).containsEntry("age", 30L);
  }

  @Test
  void nestedStructures() throws IOException {
    final Map<String, Object> map = Map.of(
        "list", List.of(1, 2, 3),
        "nested", Map.of("key", "value")
    );
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeMap(map);

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    @SuppressWarnings("unchecked")
    final Map<String, Object> result = (Map<String, Object>) reader.readValue();
    assertThat(result).containsKey("list");
    assertThat(result).containsKey("nested");
  }

  @Test
  void structure() throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeStructureHeader((byte) 0x4E, 4); // Node signature
    writer.writeInteger(123); // id
    writer.writeList(List.of("Person")); // labels
    writer.writeMap(Map.of("name", "Alice")); // properties
    writer.writeString("#1:0"); // element_id

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    final PackStreamReader.StructureValue struct = (PackStreamReader.StructureValue) reader.readValue();

    assertThat(struct.getSignature()).isEqualTo((byte) 0x4E);
    assertThat(struct.getFieldCount()).isEqualTo(4);
    assertThat(struct.getField(0)).isEqualTo(123L);
  }

  @Test
  void autoTypeDetection() throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeValue(null);
    writer.writeValue(true);
    writer.writeValue(42);
    writer.writeValue(3.14);
    writer.writeValue("hello");
    writer.writeValue(List.of(1, 2));
    writer.writeValue(Map.of("key", "value"));

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    assertThat(reader.readValue()).isNull();
    assertThat(reader.readValue()).isEqualTo(true);
    assertThat(reader.readValue()).isEqualTo(42L);
    assertThat((Double) reader.readValue()).isCloseTo(3.14, Offset.offset(0.001));
    assertThat(reader.readValue()).isEqualTo("hello");
    assertThat(reader.readValue()).isEqualTo(List.of(1L, 2L));
    @SuppressWarnings("unchecked")
    final Map<String, Object> map = (Map<String, Object>) reader.readValue();
    assertThat(map).containsEntry("key", "value");
  }
}
