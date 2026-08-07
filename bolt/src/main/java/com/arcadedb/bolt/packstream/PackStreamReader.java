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

import com.arcadedb.GlobalConfiguration;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * PackStream reader for Neo4j BOLT protocol binary deserialization.
 * Supports all PackStream data types including structures for graph elements.
 */
public class PackStreamReader {
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

  private final DataInputStream in;
  private int                   bytesRead = 0;

  // Bounds on client-supplied, unauthenticated size fields (issue #5918): every *_32 length/size is read off the
  // wire and used directly to size an allocation, and readValue() recurses once per nesting level, before the
  // BOLT handshake or authentication ever runs. Read once per message rather than cached statically so a runtime
  // change to the setting takes effect on the next message.
  private final int maxValueLength;
  private final int maxElements;
  private final int maxDepth;

  public PackStreamReader(final byte[] data) {
    this(data, GlobalConfiguration.BOLT_PACKSTREAM_MAX_VALUE_LENGTH.getValueAsInteger(),
        GlobalConfiguration.BOLT_PACKSTREAM_MAX_ELEMENTS.getValueAsInteger(),
        GlobalConfiguration.BOLT_PACKSTREAM_MAX_DEPTH.getValueAsInteger());
  }

  /**
   * As {@link #PackStreamReader(byte[])}, with the client-supplied-size bounds passed explicitly instead of read
   * from {@link GlobalConfiguration}, so unit tests can exercise a bound without mutating global server config.
   */
  public PackStreamReader(final byte[] data, final int maxValueLength, final int maxElements, final int maxDepth) {
    this.in = new DataInputStream(new ByteArrayInputStream(data));
    this.maxValueLength = maxValueLength;
    this.maxElements = maxElements;
    this.maxDepth = maxDepth;
  }

  public PackStreamReader(final DataInputStream in) {
    this(in, GlobalConfiguration.BOLT_PACKSTREAM_MAX_VALUE_LENGTH.getValueAsInteger(),
        GlobalConfiguration.BOLT_PACKSTREAM_MAX_ELEMENTS.getValueAsInteger(),
        GlobalConfiguration.BOLT_PACKSTREAM_MAX_DEPTH.getValueAsInteger());
  }

  /**
   * As {@link #PackStreamReader(DataInputStream)}, with the client-supplied-size bounds passed explicitly.
   */
  public PackStreamReader(final DataInputStream in, final int maxValueLength, final int maxElements, final int maxDepth) {
    this.in = in;
    this.maxValueLength = maxValueLength;
    this.maxElements = maxElements;
    this.maxDepth = maxDepth;
  }

  /**
   * Read the next value of any type.
   */
  public Object readValue() throws IOException {
    return readValue(0);
  }

  /**
   * Read the next value of any type, tracking nesting depth so a stream of nesting markers cannot recurse the
   * connection thread's JVM stack into a {@link StackOverflowError} (issue #5918).
   */
  private Object readValue(final int depth) throws IOException {
    if (depth > maxDepth)
      throw new IOException("PackStream value nesting exceeds the maximum allowed depth (" + maxDepth + ")");
    final int marker = readMarker();
    return readValueWithMarker(marker, depth);
  }

  /**
   * Read a value given its marker byte.
   */
  private Object readValueWithMarker(final int marker, final int depth) throws IOException {
    // NULL
    if (marker == (NULL & 0xFF)) {
      return null;
    }

    // BOOLEAN
    if (marker == (FALSE & 0xFF)) {
      return false;
    }
    if (marker == (TRUE & 0xFF)) {
      return true;
    }

    // FLOAT
    if (marker == (FLOAT & 0xFF)) {
      return readFloat();
    }

    // TINY_INT positive: 0x00 - 0x7F (0 to 127)
    if (marker >= 0x00 && marker <= 0x7F) {
      return (long) marker;
    }

    // TINY_INT negative: 0xF0 - 0xFF (-16 to -1)
    if (marker >= 0xF0 && marker <= 0xFF) {
      return (long) (marker - 256);
    }

    // INT_8
    if (marker == (INT_8 & 0xFF)) {
      return (long) in.readByte();
    }

    // INT_16
    if (marker == (INT_16 & 0xFF)) {
      return (long) in.readShort();
    }

    // INT_32
    if (marker == (INT_32 & 0xFF)) {
      return (long) in.readInt();
    }

    // INT_64
    if (marker == (INT_64 & 0xFF)) {
      return in.readLong();
    }

    // TINY_STRING: 0x80 - 0x8F
    if (marker >= 0x80 && marker <= 0x8F) {
      final int length = marker & 0x0F;
      return readStringBytes(length);
    }

    // STRING_8
    if (marker == (STRING_8 & 0xFF)) {
      final int length = in.readUnsignedByte();
      return readStringBytes(length);
    }

    // STRING_16
    if (marker == (STRING_16 & 0xFF)) {
      final int length = in.readUnsignedShort();
      return readStringBytes(length);
    }

    // STRING_32
    if (marker == (STRING_32 & 0xFF)) {
      final int length = in.readInt();
      return readStringBytes(checkValueLength(length, "STRING_32"));
    }

    // BYTES_8
    if (marker == (BYTES_8 & 0xFF)) {
      final int length = in.readUnsignedByte();
      return readBytes(length);
    }

    // BYTES_16
    if (marker == (BYTES_16 & 0xFF)) {
      final int length = in.readUnsignedShort();
      return readBytes(length);
    }

    // BYTES_32
    if (marker == (BYTES_32 & 0xFF)) {
      final int length = in.readInt();
      return readBytes(checkValueLength(length, "BYTES_32"));
    }

    // TINY_LIST: 0x90 - 0x9F
    if (marker >= 0x90 && marker <= 0x9F) {
      final int size = marker & 0x0F;
      return readListItems(size, depth);
    }

    // LIST_8
    if (marker == (LIST_8 & 0xFF)) {
      final int size = in.readUnsignedByte();
      return readListItems(size, depth);
    }

    // LIST_16
    if (marker == (LIST_16 & 0xFF)) {
      final int size = in.readUnsignedShort();
      return readListItems(size, depth);
    }

    // LIST_32
    if (marker == (LIST_32 & 0xFF)) {
      final int size = in.readInt();
      return readListItems(checkElementCount(size, "LIST_32"), depth);
    }

    // TINY_MAP: 0xA0 - 0xAF
    if (marker >= 0xA0 && marker <= 0xAF) {
      final int size = marker & 0x0F;
      return readMapEntries(size, depth);
    }

    // MAP_8
    if (marker == (MAP_8 & 0xFF)) {
      final int size = in.readUnsignedByte();
      return readMapEntries(size, depth);
    }

    // MAP_16
    if (marker == (MAP_16 & 0xFF)) {
      final int size = in.readUnsignedShort();
      return readMapEntries(size, depth);
    }

    // MAP_32
    if (marker == (MAP_32 & 0xFF)) {
      final int size = in.readInt();
      return readMapEntries(checkElementCount(size, "MAP_32"), depth);
    }

    // TINY_STRUCT: 0xB0 - 0xBF
    if (marker >= 0xB0 && marker <= 0xBF) {
      final int fieldCount = marker & 0x0F;
      final byte signature = in.readByte();
      return readStructure(signature, fieldCount, depth);
    }

    throw new IOException("Unknown PackStream marker: 0x" + Integer.toHexString(marker));
  }

  /**
   * Read marker byte.
   */
  private int readMarker() throws IOException {
    final int b = in.readUnsignedByte();
    bytesRead++;
    return b;
  }

  /**
   * Read a float (IEEE 754 double).
   */
  private double readFloat() throws IOException {
    return in.readDouble();
  }

  /**
   * Validates a BYTES_32/STRING_32 declared length before it is used to size an allocation (issue #5918): rejects
   * a negative length, one above the configured ceiling, and - the exact, un-configurable bound - one larger than
   * the bytes actually remaining in this message, which the declared length can never legitimately exceed.
   */
  private int checkValueLength(final int length, final String what) throws IOException {
    if (length < 0)
      throw new IOException("PackStream " + what + " has an invalid negative length: " + length);
    if (length > maxValueLength)
      throw new IOException(
          "PackStream " + what + " length " + length + " exceeds the maximum allowed (" + maxValueLength + ")");
    if (length > in.available())
      throw new IOException(
          "PackStream " + what + " declared length " + length + " exceeds the remaining message bytes (" + in.available() + ")");
    return length;
  }

  /**
   * Validates a LIST_32/MAP_32 declared size before it is used to size a collection's backing array (issue
   * #5918): each element/entry needs at least one wire byte to encode, so a declared count larger than the bytes
   * remaining in this message is impossible and rejected without allocating.
   */
  private int checkElementCount(final int size, final String what) throws IOException {
    if (size < 0)
      throw new IOException("PackStream " + what + " has an invalid negative size: " + size);
    if (size > maxElements)
      throw new IOException("PackStream " + what + " size " + size + " exceeds the maximum allowed (" + maxElements + ")");
    if (size > in.available())
      throw new IOException(
          "PackStream " + what + " declared size " + size + " exceeds the remaining message bytes (" + in.available() + ")");
    return size;
  }

  /**
   * Read a string of given byte length.
   */
  private String readStringBytes(final int length) throws IOException {
    final byte[] bytes = new byte[length];
    in.readFully(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  /**
   * Read raw bytes.
   */
  private byte[] readBytes(final int length) throws IOException {
    final byte[] bytes = new byte[length];
    in.readFully(bytes);
    return bytes;
  }

  /**
   * Read list items.
   */
  private List<Object> readListItems(final int size, final int depth) throws IOException {
    final List<Object> list = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      list.add(readValue(depth + 1));
    }
    return list;
  }

  /**
   * Read map entries.
   */
  private Map<String, Object> readMapEntries(final int size, final int depth) throws IOException {
    final Map<String, Object> map = new LinkedHashMap<>(size);
    for (int i = 0; i < size; i++) {
      final String key = (String) readValue(depth + 1);
      final Object value = readValue(depth + 1);
      map.put(key, value);
    }
    return map;
  }

  /**
   * Read a structure with given signature and field count.
   * Returns a StructureValue containing the signature and fields.
   */
  private StructureValue readStructure(final byte signature, final int fieldCount, final int depth) throws IOException {
    final List<Object> fields = new ArrayList<>(fieldCount);
    for (int i = 0; i < fieldCount; i++) {
      fields.add(readValue(depth + 1));
    }
    return new StructureValue(signature, fields);
  }

  /**
   * Read raw bytes directly.
   */
  public byte[] readRawBytes(final int length) throws IOException {
    final byte[] bytes = new byte[length];
    in.readFully(bytes);
    return bytes;
  }

  /**
   * Read raw short (big-endian).
   */
  public int readRawShort() throws IOException {
    return in.readUnsignedShort();
  }

  /**
   * Read raw int (big-endian).
   */
  public int readRawInt() throws IOException {
    return in.readInt();
  }

  /**
   * Read raw byte.
   */
  public int readRawByte() throws IOException {
    return in.readUnsignedByte();
  }

  /**
   * Check if there's more data available.
   */
  public int available() throws IOException {
    return in.available();
  }

  /**
   * Structure value holder for parsed structures.
   */
  public static class StructureValue {
    private final byte         signature;
    private final List<Object> fields;

    public StructureValue(final byte signature, final List<Object> fields) {
      this.signature = signature;
      this.fields = fields;
    }

    public byte getSignature() {
      return signature;
    }

    public List<Object> getFields() {
      return fields;
    }

    public Object getField(final int index) {
      return fields.get(index);
    }

    public int getFieldCount() {
      return fields.size();
    }

    @Override
    public String toString() {
      return "Structure[sig=0x" + Integer.toHexString(signature & 0xFF) + ", fields=" + fields + "]";
    }
  }
}
