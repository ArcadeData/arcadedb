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
import com.arcadedb.log.LogManager;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

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

  /**
   * Sentinel returned by {@link #readValueWithMarker} when a container marker (list/map/struct) pushed a new
   * {@link Frame} instead of producing a value: the caller loops back to read that frame's first element/
   * entry/field rather than treating this as a completed value.
   */
  private static final Object OPEN_FRAME = new Object();

  private final DataInputStream in;
  private int                   bytesRead = 0;

  // Bounds on client-supplied, unauthenticated size fields (issue #5918): every *_32 length/size is read off the
  // wire and used directly to size an allocation, before the BOLT handshake or authentication ever runs.
  // maxDepth bounds nesting complexity/memory (readValue() decodes containers iteratively via an explicit Frame
  // stack, not JVM recursion, so it is not a stack-overflow guard). Read once per message rather than cached
  // statically so a runtime change to the setting takes effect on the next message.
  private final int maxValueLength;
  private final int maxElements;
  private final int maxDepth;

  // A misconfigured protocol-limit setting is re-read (and re-validated) on every new PackStreamReader, since
  // every BOLT message constructs a fresh one and the value can change at runtime; this only bounds the WARNING
  // about it to once per setting per JVM, so a busy server churning through messages against a static bad value
  // does not flood the log (mirrors RedisNetworkExecutor.sanitizedLimit, issue #5918 review).
  private static final Set<GlobalConfiguration> WARNED_MISCONFIGURED_LIMITS = ConcurrentHashMap.newKeySet();

  /**
   * Reads a protocol-limit setting, falling back to its built-in default (with a warning) if configured below 1.
   * A limit of 0 or negative would reject essentially every message outright - e.g. {@code maxDepth=0} rejects
   * even a bare HELLO struct, since its extra map is already one level deeper than the top-level struct itself -
   * so it is treated as a misconfiguration rather than an intentional (if impractical) lockdown.
   */
  private static int sanitizedLimit(final GlobalConfiguration setting) {
    final int configured = setting.getValueAsInteger();
    if (configured < 1) {
      final int fallback = ((Number) setting.getDefValue()).intValue();
      if (WARNED_MISCONFIGURED_LIMITS.add(setting))
        LogManager.instance().log(PackStreamReader.class, Level.WARNING,
            "BOLT PackStream: '%s' is set to %d, below the minimum usable value (1); falling back to the default (%d)",
            setting.getKey(), configured, fallback);
      return fallback;
    }
    return configured;
  }

  public PackStreamReader(final byte[] data) {
    this(data, sanitizedLimit(GlobalConfiguration.BOLT_PACKSTREAM_MAX_VALUE_LENGTH),
        sanitizedLimit(GlobalConfiguration.BOLT_PACKSTREAM_MAX_ELEMENTS),
        sanitizedLimit(GlobalConfiguration.BOLT_PACKSTREAM_MAX_DEPTH));
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

  /**
   * Not used in production (only the {@link #PackStreamReader(byte[])} constructor is, against an already fully
   * reassembled message). {@link #checkValueLength}/{@link #checkElementCount} bound a declared length/size
   * against {@code in.available()}, which is exact only when {@code in} is backed by a fully-buffered source
   * (e.g. {@link ByteArrayInputStream}): wiring this constructor to a live socket/stream would make
   * {@code available()} reflect only currently-buffered bytes, not the true remaining message size, weakening
   * that bound to a false sense of safety rather than the exact one it provides today.
   */
  public PackStreamReader(final DataInputStream in) {
    this(in, sanitizedLimit(GlobalConfiguration.BOLT_PACKSTREAM_MAX_VALUE_LENGTH),
        sanitizedLimit(GlobalConfiguration.BOLT_PACKSTREAM_MAX_ELEMENTS),
        sanitizedLimit(GlobalConfiguration.BOLT_PACKSTREAM_MAX_DEPTH));
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
   * <p>
   * Implemented iteratively with an explicit heap-allocated {@link Frame} stack rather than JVM recursion.
   * PackStream nests lists/maps/structures arbitrarily deep, and a naive recursive-descent decoder recurses once
   * per nesting level - CI observed a real {@link StackOverflowError} at a nesting depth (1000) that a local run
   * with a larger default thread stack did not: native JVM stack budget per recursion level is platform/JIT
   * dependent, not something this class can rely on as a safety bound (issue #5918). A heap-allocated frame has
   * no such risk, so {@link #maxDepth} below now bounds nesting complexity/memory rather than guarding against a
   * stack overflow, and stays safe at any configured value on any platform.
   */
  public Object readValue() throws IOException {
    final Deque<Frame> stack = new ArrayDeque<>();

    while (true) {
      if (stack.size() > maxDepth)
        throw new IOException("PackStream value nesting exceeds the maximum allowed depth (" + maxDepth + ")");

      final int marker = readMarker();
      Object value = readValueWithMarker(marker, stack);
      if (value == OPEN_FRAME)
        continue; // a new container frame was pushed; read its first element/entry/field next

      // Attach the completed value to the frame on top of the stack, popping and re-attaching any frame that
      // becomes complete as a result, until either the stack is empty (this value is the final result) or a
      // frame still needs more input (go back around to read it off the wire).
      while (true) {
        final Frame top = stack.peek();
        if (top == null)
          return value;

        final Object result = top.attach(value);
        if (result == Frame.NEEDS_MORE)
          break;

        stack.pop();
        value = result;
      }
    }
  }

  /**
   * Read a value given its marker byte. For a container marker (list/map/struct) with at least one
   * element/entry/field, pushes a new {@link Frame} onto {@code stack} and returns {@link #OPEN_FRAME} instead
   * of a value; an empty container is returned directly, matching a zero-element loop never recursing.
   */
  private Object readValueWithMarker(final int marker, final Deque<Frame> stack) throws IOException {
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
      return openList(size, stack);
    }

    // LIST_8
    if (marker == (LIST_8 & 0xFF)) {
      final int size = in.readUnsignedByte();
      return openList(size, stack);
    }

    // LIST_16
    if (marker == (LIST_16 & 0xFF)) {
      final int size = in.readUnsignedShort();
      return openList(size, stack);
    }

    // LIST_32
    if (marker == (LIST_32 & 0xFF)) {
      final int size = in.readInt();
      return openList(checkElementCount(size, "LIST_32"), stack);
    }

    // TINY_MAP: 0xA0 - 0xAF
    if (marker >= 0xA0 && marker <= 0xAF) {
      final int size = marker & 0x0F;
      return openMap(size, stack);
    }

    // MAP_8
    if (marker == (MAP_8 & 0xFF)) {
      final int size = in.readUnsignedByte();
      return openMap(size, stack);
    }

    // MAP_16
    if (marker == (MAP_16 & 0xFF)) {
      final int size = in.readUnsignedShort();
      return openMap(size, stack);
    }

    // MAP_32
    if (marker == (MAP_32 & 0xFF)) {
      final int size = in.readInt();
      return openMap(checkElementCount(size, "MAP_32"), stack);
    }

    // TINY_STRUCT: 0xB0 - 0xBF
    if (marker >= 0xB0 && marker <= 0xBF) {
      final int fieldCount = marker & 0x0F;
      final byte signature = in.readByte();
      if (fieldCount == 0)
        return new StructureValue(signature, new ArrayList<>(0));
      stack.push(new StructFrame(signature, fieldCount));
      return OPEN_FRAME;
    }

    throw new IOException("Unknown PackStream marker: 0x" + Integer.toHexString(marker));
  }

  private static Object openList(final int size, final Deque<Frame> stack) {
    if (size == 0)
      return new ArrayList<>(0);
    stack.push(new ListFrame(size));
    return OPEN_FRAME;
  }

  private static Object openMap(final int size, final Deque<Frame> stack) {
    if (size == 0)
      return new LinkedHashMap<>(0);
    stack.push(new MapFrame(size));
    return OPEN_FRAME;
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
   * #5918): a LIST_32 element needs at least one wire byte to encode and a MAP_32 entry needs at least two (a key
   * plus a value), so a declared count larger than the bytes remaining in this message is impossible - loosely
   * for maps, since this check uses the same one-byte-per-count floor for both - and rejected without allocating.
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
   * A container (list/map/struct) whose element/entry/field count is not yet fully read. Pushed onto the work
   * stack in {@link #readValue()} in place of a recursive call; {@link #attach} feeds it one completed child
   * value at a time until it reports completion by returning something other than {@link #NEEDS_MORE}.
   */
  private abstract static class Frame {
    static final Object NEEDS_MORE = new Object();

    abstract Object attach(Object value) throws IOException;
  }

  private static final class ListFrame extends Frame {
    private final List<Object> list;
    private int                remaining;

    ListFrame(final int size) {
      this.list = new ArrayList<>(size);
      this.remaining = size;
    }

    @Override
    Object attach(final Object value) {
      list.add(value);
      return --remaining == 0 ? list : NEEDS_MORE;
    }
  }

  private static final class MapFrame extends Frame {
    private final Map<String, Object> map;
    private       int                 remainingEntries;
    private       boolean             expectingKey = true;
    private       String              pendingKey;

    MapFrame(final int size) {
      this.map = new LinkedHashMap<>(size);
      this.remainingEntries = size;
    }

    @Override
    Object attach(final Object value) throws IOException {
      if (expectingKey) {
        // A PackStream map key must be a string; a hostile/malformed client can send any value type here
        // (e.g. an integer), which previously fell through to an uncontrolled ClassCastException instead of a
        // clear protocol error - the same class of gap issue #5918 fixed for declared lengths/sizes.
        if (!(value instanceof String))
          throw new IOException("PackStream map key must be a string, got: "
              + (value != null ? value.getClass().getSimpleName() : "null"));
        pendingKey = (String) value;
        expectingKey = false;
        return NEEDS_MORE;
      }
      map.put(pendingKey, value);
      expectingKey = true;
      return --remainingEntries == 0 ? map : NEEDS_MORE;
    }
  }

  private static final class StructFrame extends Frame {
    private final byte         signature;
    private final List<Object> fields;
    private       int          remaining;

    StructFrame(final byte signature, final int fieldCount) {
      this.signature = signature;
      this.fields = new ArrayList<>(fieldCount);
      this.remaining = fieldCount;
    }

    @Override
    Object attach(final Object value) {
      fields.add(value);
      return --remaining == 0 ? new StructureValue(signature, fields) : NEEDS_MORE;
    }
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
