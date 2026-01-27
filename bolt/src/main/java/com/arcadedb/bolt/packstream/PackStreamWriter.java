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
package com.arcadedb.bolt.packstream;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * PackStream writer for Neo4j BOLT protocol binary serialization.
 * Supports all PackStream data types including structures for graph elements.
 */
public class PackStreamWriter {
  // Markers for null and boolean
  private static final byte NULL   = (byte) 0xC0;
  private static final byte FALSE  = (byte) 0xC2;
  private static final byte TRUE   = (byte) 0xC3;
  private static final byte FLOAT  = (byte) 0xC1;

  // Markers for integers
  private static final byte INT_8  = (byte) 0xC8;
  private static final byte INT_16 = (byte) 0xC9;
  private static final byte INT_32 = (byte) 0xCA;
  private static final byte INT_64 = (byte) 0xCB;

  // Markers for strings
  private static final byte STRING_8  = (byte) 0xD0;
  private static final byte STRING_16 = (byte) 0xD1;
  private static final byte STRING_32 = (byte) 0xD2;

  // Markers for bytes
  private static final byte BYTES_8  = (byte) 0xCC;
  private static final byte BYTES_16 = (byte) 0xCD;
  private static final byte BYTES_32 = (byte) 0xCE;

  // Markers for lists
  private static final byte LIST_8  = (byte) 0xD4;
  private static final byte LIST_16 = (byte) 0xD5;
  private static final byte LIST_32 = (byte) 0xD6;

  // Markers for dictionaries/maps
  private static final byte MAP_8  = (byte) 0xD8;
  private static final byte MAP_16 = (byte) 0xD9;
  private static final byte MAP_32 = (byte) 0xDA;

  // Tiny type base markers (size encoded in lower nibble)
  private static final byte TINY_STRING = (byte) 0x80;  // 0x80 - 0x8F
  private static final byte TINY_LIST   = (byte) 0x90;  // 0x90 - 0x9F
  private static final byte TINY_MAP    = (byte) 0xA0;  // 0xA0 - 0xAF
  private static final byte TINY_STRUCT = (byte) 0xB0;  // 0xB0 - 0xBF

  private final ByteArrayOutputStream buffer;
  private final DataOutputStream      out;

  public PackStreamWriter() {
    this.buffer = new ByteArrayOutputStream();
    this.out = new DataOutputStream(buffer);
  }

  public PackStreamWriter(final ByteArrayOutputStream buffer) {
    this.buffer = buffer;
    this.out = new DataOutputStream(buffer);
  }

  /**
   * Write null value.
   */
  public void writeNull() throws IOException {
    out.writeByte(NULL);
  }

  /**
   * Write boolean value.
   */
  public void writeBoolean(final boolean value) throws IOException {
    out.writeByte(value ? TRUE : FALSE);
  }

  /**
   * Write integer value with optimal encoding based on size.
   */
  public void writeInteger(final long value) throws IOException {
    if (value >= -16 && value <= 127) {
      // TINY_INT: single byte, value range [-16, 127]
      out.writeByte((byte) value);
    } else if (value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE) {
      out.writeByte(INT_8);
      out.writeByte((byte) value);
    } else if (value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) {
      out.writeByte(INT_16);
      out.writeShort((short) value);
    } else if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
      out.writeByte(INT_32);
      out.writeInt((int) value);
    } else {
      out.writeByte(INT_64);
      out.writeLong(value);
    }
  }

  /**
   * Write floating point value (IEEE 754 double).
   */
  public void writeFloat(final double value) throws IOException {
    out.writeByte(FLOAT);
    out.writeDouble(value);
  }

  /**
   * Write string value.
   */
  public void writeString(final String value) throws IOException {
    if (value == null) {
      writeNull();
      return;
    }

    final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    final int length = bytes.length;

    if (length < 16) {
      // TINY_STRING: marker contains size
      out.writeByte(TINY_STRING | length);
    } else if (length <= 255) {
      out.writeByte(STRING_8);
      out.writeByte(length);
    } else if (length <= 65535) {
      out.writeByte(STRING_16);
      out.writeShort(length);
    } else {
      out.writeByte(STRING_32);
      out.writeInt(length);
    }
    out.write(bytes);
  }

  /**
   * Write byte array.
   */
  public void writeBytes(final byte[] value) throws IOException {
    if (value == null) {
      writeNull();
      return;
    }

    final int length = value.length;

    if (length <= 255) {
      out.writeByte(BYTES_8);
      out.writeByte(length);
    } else if (length <= 65535) {
      out.writeByte(BYTES_16);
      out.writeShort(length);
    } else {
      out.writeByte(BYTES_32);
      out.writeInt(length);
    }
    out.write(value);
  }

  /**
   * Write list header. After calling this, write each list element.
   */
  public void writeListHeader(final int size) throws IOException {
    if (size < 16) {
      out.writeByte(TINY_LIST | size);
    } else if (size <= 255) {
      out.writeByte(LIST_8);
      out.writeByte(size);
    } else if (size <= 65535) {
      out.writeByte(LIST_16);
      out.writeShort(size);
    } else {
      out.writeByte(LIST_32);
      out.writeInt(size);
    }
  }

  /**
   * Write complete list with values.
   */
  public void writeList(final List<?> list) throws IOException {
    if (list == null) {
      writeNull();
      return;
    }

    writeListHeader(list.size());
    for (final Object item : list) {
      writeValue(item);
    }
  }

  /**
   * Write map/dictionary header. After calling this, write key-value pairs.
   */
  public void writeMapHeader(final int size) throws IOException {
    if (size < 16) {
      out.writeByte(TINY_MAP | size);
    } else if (size <= 255) {
      out.writeByte(MAP_8);
      out.writeByte(size);
    } else if (size <= 65535) {
      out.writeByte(MAP_16);
      out.writeShort(size);
    } else {
      out.writeByte(MAP_32);
      out.writeInt(size);
    }
  }

  /**
   * Write complete map with key-value pairs.
   */
  public void writeMap(final Map<String, Object> map) throws IOException {
    if (map == null) {
      writeNull();
      return;
    }

    writeMapHeader(map.size());
    for (final Map.Entry<String, Object> entry : map.entrySet()) {
      writeString(entry.getKey());
      writeValue(entry.getValue());
    }
  }

  /**
   * Write structure header. After calling this, write structure fields.
   */
  public void writeStructureHeader(final byte signature, final int fieldCount) throws IOException {
    if (fieldCount < 16) {
      out.writeByte(TINY_STRUCT | fieldCount);
    } else {
      throw new IOException("Structure field count too large: " + fieldCount);
    }
    out.writeByte(signature);
  }

  /**
   * Write any value with automatic type detection.
   */
  @SuppressWarnings("unchecked")
  public void writeValue(final Object value) throws IOException {
    if (value == null) {
      writeNull();
    } else if (value instanceof Boolean) {
      writeBoolean((Boolean) value);
    } else if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
      writeInteger(((Number) value).longValue());
    } else if (value instanceof Float || value instanceof Double) {
      writeFloat(((Number) value).doubleValue());
    } else if (value instanceof String) {
      writeString((String) value);
    } else if (value instanceof byte[]) {
      writeBytes((byte[]) value);
    } else if (value instanceof List) {
      writeList((List<?>) value);
    } else if (value instanceof Map) {
      writeMap((Map<String, Object>) value);
    } else if (value instanceof PackStreamStructure) {
      ((PackStreamStructure) value).writeTo(this);
    } else {
      // Default: convert to string
      writeString(value.toString());
    }
  }

  /**
   * Get the raw bytes written so far.
   */
  public byte[] toByteArray() {
    return buffer.toByteArray();
  }

  /**
   * Get the current size of the buffer.
   */
  public int size() {
    return buffer.size();
  }

  /**
   * Reset the writer for reuse.
   */
  public void reset() {
    buffer.reset();
  }

  /**
   * Write raw bytes directly.
   */
  public void writeRawBytes(final byte[] bytes) throws IOException {
    out.write(bytes);
  }

  /**
   * Write raw byte directly.
   */
  public void writeRawByte(final int b) throws IOException {
    out.writeByte(b);
  }

  /**
   * Write raw short directly (big-endian).
   */
  public void writeRawShort(final int value) throws IOException {
    out.writeShort(value);
  }

  /**
   * Write raw int directly (big-endian).
   */
  public void writeRawInt(final int value) throws IOException {
    out.writeInt(value);
  }
}
