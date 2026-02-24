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
package com.arcadedb.server.http.handler.prometheus;

/**
 * Minimal protobuf wire format decoder. Supports only the wire types needed
 * by the Prometheus remote_write / remote_read protocol:
 * varint (0), 64-bit fixed (1), and length-delimited (2).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class ProtobufDecoder {

  // Wire types
  public static final int WIRETYPE_VARINT           = 0;
  public static final int WIRETYPE_FIXED64          = 1;
  public static final int WIRETYPE_LENGTH_DELIMITED = 2;

  private final byte[] data;
  private int          offset;

  public ProtobufDecoder(final byte[] data) {
    this(data, 0);
  }

  public ProtobufDecoder(final byte[] data, final int offset) {
    this.data = data;
    this.offset = offset;
  }

  public boolean hasRemaining() {
    return offset < data.length;
  }

  public int getOffset() {
    return offset;
  }

  public int readTag() {
    return (int) readVarint();
  }

  public static int fieldNumber(final int tag) {
    return tag >>> 3;
  }

  public static int wireType(final int tag) {
    return tag & 0x07;
  }

  public long readVarint() {
    long result = 0;
    int shift = 0;
    while (offset < data.length) {
      final byte b = data[offset++];
      result |= (long) (b & 0x7F) << shift;
      if ((b & 0x80) == 0)
        return result;
      shift += 7;
      if (shift >= 64)
        throw new IllegalStateException("Varint too long (exceeds 64 bits)");
    }
    throw new IllegalStateException("Truncated varint");
  }

  public long readSInt64() {
    final long raw = readVarint();
    return (raw >>> 1) ^ -(raw & 1);
  }

  public double readFixed64AsDouble() {
    if (offset + 8 > data.length)
      throw new IllegalStateException("Truncated fixed64");
    long bits = 0;
    for (int i = 0; i < 8; i++)
      bits |= (long) (data[offset++] & 0xFF) << (i * 8);
    return Double.longBitsToDouble(bits);
  }

  public long readFixed64AsLong() {
    if (offset + 8 > data.length)
      throw new IllegalStateException("Truncated fixed64");
    long bits = 0;
    for (int i = 0; i < 8; i++)
      bits |= (long) (data[offset++] & 0xFF) << (i * 8);
    return bits;
  }

  public byte[] readLengthDelimited() {
    final int len = (int) readVarint();
    if (len < 0 || len > data.length - offset)
      throw new IllegalStateException("Invalid length-delimited field length: " + len);
    final byte[] result = new byte[len];
    System.arraycopy(data, offset, result, 0, len);
    offset += len;
    return result;
  }

  public String readString() {
    final byte[] bytes = readLengthDelimited();
    return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
  }

  public ProtobufDecoder readSubMessage() {
    final byte[] sub = readLengthDelimited();
    return new ProtobufDecoder(sub);
  }

  public void skipField(final int wireType) {
    switch (wireType) {
    case WIRETYPE_VARINT:
      readVarint();
      break;
    case WIRETYPE_FIXED64:
      offset += 8;
      break;
    case WIRETYPE_LENGTH_DELIMITED:
      final int len = (int) readVarint();
      if (len < 0 || len > data.length - offset)
        throw new IllegalStateException("Invalid length-delimited skip length: " + len);
      offset += len;
      break;
    default:
      throw new IllegalStateException("Unknown wire type: " + wireType);
    }
  }
}
