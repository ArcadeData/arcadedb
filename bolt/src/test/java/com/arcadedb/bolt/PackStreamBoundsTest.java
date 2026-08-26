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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.bolt.packstream.PackStreamReader;
import com.arcadedb.bolt.packstream.PackStreamWriter;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #5918: the PackStream decoder read a client-supplied, unauthenticated 32-bit
 * length/size off the wire and used it directly to size an allocation (BYTES_32/STRING_32/LIST_32/MAP_32),
 * and recursed once per nesting level with no depth cap - both reachable on the very first message, before the
 * BOLT handshake or authentication. A handful of bytes could trigger a multi-gigabyte allocation
 * ({@link OutOfMemoryError}) or a {@link StackOverflowError}, both {@code Error}s that escape a
 * {@code catch (Exception)} net. Every case below must fail with a plain {@link IOException} - never let an
 * {@link Error} propagate - and must do so without the runaway allocation/recursion actually happening.
 */
class PackStreamBoundsTest {

  /**
   * Issue's minimal trigger: 5 bytes - {@code CE 7F FF FF FF} (BYTES_32, declared length 2147483647). Previously
   * allocated a ~2GB byte array before any bound was checked.
   */
  @Test
  void bytes32DeclaredLengthAboveConfiguredMaxRejectedBeforeAllocation() {
    final byte[] data = { (byte) 0xCE, 0x7F, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF };
    final PackStreamReader reader = new PackStreamReader(data);

    assertThatThrownBy(reader::readValue)
        .isExactlyInstanceOf(IOException.class)
        .hasMessageContaining("BYTES_32");
  }

  @Test
  void string32DeclaredLengthAboveConfiguredMaxRejectedBeforeAllocation() {
    final byte[] data = { (byte) 0xD2, 0x7F, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF };
    final PackStreamReader reader = new PackStreamReader(data);

    assertThatThrownBy(reader::readValue)
        .isExactlyInstanceOf(IOException.class)
        .hasMessageContaining("STRING_32");
  }

  @Test
  void list32DeclaredSizeAboveConfiguredMaxRejectedBeforeAllocation() {
    final byte[] data = { (byte) 0xD6, 0x7F, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF };
    final PackStreamReader reader = new PackStreamReader(data);

    assertThatThrownBy(reader::readValue)
        .isExactlyInstanceOf(IOException.class)
        .hasMessageContaining("LIST_32");
  }

  @Test
  void map32DeclaredSizeAboveConfiguredMaxRejectedBeforeAllocation() {
    final byte[] data = { (byte) 0xDA, 0x7F, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF };
    final PackStreamReader reader = new PackStreamReader(data);

    assertThatThrownBy(reader::readValue)
        .isExactlyInstanceOf(IOException.class)
        .hasMessageContaining("MAP_32");
  }

  /**
   * A negative 32-bit length (e.g. {@code CE 80 00 00 00}) previously fell through to
   * {@code new byte[length]}, yielding a raw {@link NegativeArraySizeException} instead of a clear protocol error.
   */
  @Test
  void negativeBytes32LengthRejectedWithClearProtocolError() {
    final byte[] data = { (byte) 0xCE, (byte) 0x80, 0x00, 0x00, 0x00 };
    final PackStreamReader reader = new PackStreamReader(data);

    assertThatThrownBy(reader::readValue)
        .isExactlyInstanceOf(IOException.class)
        .hasMessageContaining("negative length");
  }

  /**
   * A declared length that is comfortably under the configured ceiling is still impossible once it exceeds the
   * bytes actually remaining in this message - the exact, un-configurable bound. Message: BYTES_32 declaring
   * length 1000, with zero bytes following (5-byte message total).
   */
  @Test
  void declaredLengthExceedingActualRemainingBytesRejectedRegardlessOfConfiguredMax() {
    final byte[] data = { (byte) 0xCE, 0x00, 0x00, 0x03, (byte) 0xE8 }; // BYTES_32, length = 1000
    final PackStreamReader reader = new PackStreamReader(data);

    assertThatThrownBy(reader::readValue)
        .isExactlyInstanceOf(IOException.class)
        .hasMessageContaining("remaining message bytes");
  }

  /**
   * Issue's minimal recursion trigger: N bytes of {@code 0x91} (TINY_LIST, size 1) then {@code 0xC0} (NULL).
   * Previously overflowed the connection thread's JVM stack; must now fail cleanly at the configured depth
   * instead.
   */
  @Test
  void deeplyNestedTinyListRejectedWithoutStackOverflow() {
    final int nestingLevels = 200_000;
    final byte[] data = new byte[nestingLevels + 1];
    for (int i = 0; i < nestingLevels; i++)
      data[i] = (byte) 0x91; // TINY_LIST, size 1
    data[nestingLevels] = (byte) 0xC0; // NULL

    final PackStreamReader reader = new PackStreamReader(data);

    assertThatThrownBy(reader::readValue)
        .isExactlyInstanceOf(IOException.class)
        .hasMessageContaining("nesting exceeds the maximum allowed depth");
  }

  /**
   * Depth bound is exact: exactly {@code maxDepth} nested TINY_LISTs (depth 0..maxDepth) still parses, one level
   * deeper does not. Uses the explicit-bounds constructor to keep the fixture small and the assertion precise.
   */
  @Test
  void depthBoundIsExactAtConfiguredLimit() throws Exception {
    final int maxDepth = 5;

    final byte[] withinBound = nestedTinyLists(maxDepth);
    assertThat(new PackStreamReader(withinBound, 1024, 1024, maxDepth).readValue()).isNotNull();

    final byte[] oneLevelTooDeep = nestedTinyLists(maxDepth + 1);
    assertThatThrownBy(() -> new PackStreamReader(oneLevelTooDeep, 1024, 1024, maxDepth).readValue())
        .isExactlyInstanceOf(IOException.class)
        .hasMessageContaining("nesting exceeds the maximum allowed depth");
  }

  private static byte[] nestedTinyLists(final int levels) {
    final byte[] data = new byte[levels + 1];
    for (int i = 0; i < levels; i++)
      data[i] = (byte) 0x91; // TINY_LIST, size 1
    data[levels] = (byte) 0xC0; // NULL
    return data;
  }

  /**
   * A single value declaring an element/entry count under the configured element-count ceiling but above the
   * bytes actually remaining is rejected the same way as the raw-length case.
   */
  @Test
  void declaredListSizeExceedingActualRemainingBytesRejected() {
    final byte[] data = { (byte) 0xD6, 0x00, 0x00, 0x03, (byte) 0xE8 }; // LIST_32, size = 1000
    final PackStreamReader reader = new PackStreamReader(data);

    assertThatThrownBy(reader::readValue)
        .isExactlyInstanceOf(IOException.class)
        .hasMessageContaining("remaining message bytes");
  }

  /**
   * As {@link #declaredListSizeExceedingActualRemainingBytesRejected}, for MAP_32 - checkElementCount is shared
   * code between LIST_32/MAP_32, but each marker's decoding branch is a separate call site worth covering.
   */
  @Test
  void declaredMapSizeExceedingActualRemainingBytesRejected() {
    final byte[] data = { (byte) 0xDA, 0x00, 0x00, 0x03, (byte) 0xE8 }; // MAP_32, size = 1000
    final PackStreamReader reader = new PackStreamReader(data);

    assertThatThrownBy(reader::readValue)
        .isExactlyInstanceOf(IOException.class)
        .hasMessageContaining("remaining message bytes");
  }

  /**
   * As {@link #negativeBytes32LengthRejectedWithClearProtocolError}, for a negative LIST_32/MAP_32 declared size.
   */
  @Test
  void negativeListSizeRejectedWithClearProtocolError() {
    final byte[] data = { (byte) 0xD6, (byte) 0x80, 0x00, 0x00, 0x00 }; // LIST_32, size = Integer.MIN_VALUE
    final PackStreamReader reader = new PackStreamReader(data);

    assertThatThrownBy(reader::readValue)
        .isExactlyInstanceOf(IOException.class)
        .hasMessageContaining("negative size");
  }

  @Test
  void negativeMapSizeRejectedWithClearProtocolError() {
    final byte[] data = { (byte) 0xDA, (byte) 0x80, 0x00, 0x00, 0x00 }; // MAP_32, size = Integer.MIN_VALUE
    final PackStreamReader reader = new PackStreamReader(data);

    assertThatThrownBy(reader::readValue)
        .isExactlyInstanceOf(IOException.class)
        .hasMessageContaining("negative size");
  }

  /**
   * A PackStream map key must be a string; previously a non-string key (e.g. an integer) fell through to a raw
   * {@code (String) value} cast, yielding an uncontrolled {@link ClassCastException} instead of a clear protocol
   * error - the same class of gap #5918 fixed for declared lengths/sizes, just for map keys.
   */
  @Test
  void nonStringMapKeyRejectedWithClearProtocolErrorInsteadOfClassCastException() {
    final byte[] data = { (byte) 0xA1, 0x01, (byte) 0xC0 }; // TINY_MAP(1): key = 1 (TINY_INT), value = NULL
    final PackStreamReader reader = new PackStreamReader(data);

    assertThatThrownBy(reader::readValue)
        .isExactlyInstanceOf(IOException.class)
        .hasMessageContaining("map key");
  }

  // ============ Regression: legitimate, in-bounds values still parse correctly ============

  @Test
  void withinBoundsStringStillRoundTrips() throws Exception {
    final PackStreamWriter writer = new PackStreamWriter();
    final String value = "hello, ArcadeDB BOLT!";
    writer.writeString(value);

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    assertThat(reader.readValue()).isEqualTo(value);
  }

  @Test
  void withinBoundsBytesStillRoundTrip() throws Exception {
    final PackStreamWriter writer = new PackStreamWriter();
    final byte[] value = new byte[10_000];
    for (int i = 0; i < value.length; i++)
      value[i] = (byte) i;
    writer.writeBytes(value);

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    assertThat((byte[]) reader.readValue()).isEqualTo(value);
  }

  @Test
  void withinBoundsNestedListAndMapStillRoundTrip() throws Exception {
    final PackStreamWriter writer = new PackStreamWriter();

    final List<Object> inner = new ArrayList<>();
    inner.add(1L);
    inner.add(2L);
    inner.add(3L);

    final Map<String, Object> map = new LinkedHashMap<>();
    map.put("numbers", inner);
    map.put("name", "arcade");

    writer.writeMap(map);

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    @SuppressWarnings("unchecked")
    final Map<String, Object> result = (Map<String, Object>) reader.readValue();

    assertThat(result.get("name")).isEqualTo("arcade");
    assertThat(result.get("numbers")).isEqualTo(inner);
  }

  @Test
  void withinBoundsLargeListOfManyElementsStillParses() throws Exception {
    final PackStreamWriter writer = new PackStreamWriter();
    final List<Object> list = new ArrayList<>();
    for (int i = 0; i < 5000; i++)
      list.add((long) i);
    writer.writeList(list);

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    assertThat(reader.readValue()).isEqualTo(list);
  }

  // ============ Explicit-bounds constructor: element-count and value-length ceilings ============

  @Test
  void explicitMaxElementsBoundRejectsOversizedDeclaredListSize() throws Exception {
    final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    final DataOutputStream out = new DataOutputStream(buffer);
    out.writeByte(0xD6); // LIST_32
    out.writeInt(2000);  // declared size, above the 100-element bound below, under available bytes
    for (int i = 0; i < 2000; i++)
      out.writeByte(0x01); // 2000 filler TINY_INT bytes, so the size passes the available() check

    final PackStreamReader reader = new PackStreamReader(buffer.toByteArray(), 1024, 100, 1000);
    assertThatThrownBy(reader::readValue)
        .isExactlyInstanceOf(IOException.class)
        .hasMessageContaining("LIST_32")
        .hasMessageContaining("maximum allowed");
  }

  @Test
  void explicitMaxValueLengthBoundRejectsOversizedDeclaredStringLength() throws Exception {
    final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    final DataOutputStream out = new DataOutputStream(buffer);
    out.writeByte(0xD2); // STRING_32
    out.writeInt(2000);  // declared length, above the 100-byte bound below, under available bytes
    out.write(new byte[2000]);

    final PackStreamReader reader = new PackStreamReader(buffer.toByteArray(), 100, 1024, 1000);
    assertThatThrownBy(reader::readValue)
        .isExactlyInstanceOf(IOException.class)
        .hasMessageContaining("STRING_32")
        .hasMessageContaining("maximum allowed");
  }

  // ============ Misconfigured-limit fallback (GlobalConfiguration below the usable floor) ============

  /**
   * A {@code maxDepth} of 0 (or negative) would reject essentially every real BOLT message outright, since even
   * a bare HELLO's extra map is one level deeper than the top-level struct itself. The config-reading
   * constructor must fall back to the built-in default rather than enforcing the misconfigured value literally.
   */
  @Test
  void depthBelowUsableFloorFallsBackToDefaultInsteadOfRejectingEveryMessage() throws Exception {
    final int original = GlobalConfiguration.BOLT_PACKSTREAM_MAX_DEPTH.getValueAsInteger();
    GlobalConfiguration.BOLT_PACKSTREAM_MAX_DEPTH.setValue(0);
    try {
      final PackStreamWriter writer = new PackStreamWriter();
      writer.writeList(List.of(1L, 2L, 3L)); // one level of nesting: would be rejected outright by a literal maxDepth=0

      final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
      assertThat(reader.readValue()).isEqualTo(List.of(1L, 2L, 3L));
    } finally {
      GlobalConfiguration.BOLT_PACKSTREAM_MAX_DEPTH.setValue(original);
    }
  }

  @Test
  void maxValueLengthBelowUsableFloorFallsBackToDefault() throws Exception {
    final int original = GlobalConfiguration.BOLT_PACKSTREAM_MAX_VALUE_LENGTH.getValueAsInteger();
    GlobalConfiguration.BOLT_PACKSTREAM_MAX_VALUE_LENGTH.setValue(0);
    try {
      final PackStreamWriter writer = new PackStreamWriter();
      writer.writeString("a".repeat(70_000)); // forces STRING_32; would be rejected outright by a literal maxValueLength=0

      final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
      assertThat(reader.readValue()).isEqualTo("a".repeat(70_000));
    } finally {
      GlobalConfiguration.BOLT_PACKSTREAM_MAX_VALUE_LENGTH.setValue(original);
    }
  }

  @Test
  void maxElementsBelowUsableFloorFallsBackToDefault() throws Exception {
    final int original = GlobalConfiguration.BOLT_PACKSTREAM_MAX_ELEMENTS.getValueAsInteger();
    GlobalConfiguration.BOLT_PACKSTREAM_MAX_ELEMENTS.setValue(0);
    try {
      final PackStreamWriter writer = new PackStreamWriter();
      final List<Object> list = new ArrayList<>();
      for (int i = 0; i < 70_000; i++)
        list.add((long) i); // forces LIST_32; would be rejected outright by a literal maxElements=0
      writer.writeList(list);

      final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
      assertThat(reader.readValue()).isEqualTo(list);
    } finally {
      GlobalConfiguration.BOLT_PACKSTREAM_MAX_ELEMENTS.setValue(original);
    }
  }
}
