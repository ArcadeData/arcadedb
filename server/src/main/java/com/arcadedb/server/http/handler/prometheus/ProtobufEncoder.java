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

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Minimal protobuf wire format encoder. Supports only the wire types needed
 * by the Prometheus remote_write / remote_read protocol.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class ProtobufEncoder {

  private final ByteArrayOutputStream buffer;

  public ProtobufEncoder() {
    this.buffer = new ByteArrayOutputStream(256);
  }

  public ProtobufEncoder(final int initialCapacity) {
    this.buffer = new ByteArrayOutputStream(initialCapacity);
  }

  public void writeTag(final int fieldNumber, final int wireType) {
    writeVarint((fieldNumber << 3) | wireType);
  }

  public void writeVarint(long value) {
    while ((value & ~0x7FL) != 0) {
      buffer.write((int) ((value & 0x7F) | 0x80));
      value >>>= 7;
    }
    buffer.write((int) value);
  }

  public void writeFixed64(final double value) {
    final long bits = Double.doubleToRawLongBits(value);
    writeFixed64Raw(bits);
  }

  public void writeFixed64Raw(final long bits) {
    for (int i = 0; i < 8; i++)
      buffer.write((int) ((bits >>> (i * 8)) & 0xFF));
  }

  public void writeLengthDelimited(final byte[] data) {
    writeVarint(data.length);
    buffer.write(data, 0, data.length);
  }

  public void writeString(final int fieldNumber, final String value) {
    writeTag(fieldNumber, ProtobufDecoder.WIRETYPE_LENGTH_DELIMITED);
    final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    writeLengthDelimited(bytes);
  }

  public void writeSubMessage(final int fieldNumber, final byte[] encodedMessage) {
    writeTag(fieldNumber, ProtobufDecoder.WIRETYPE_LENGTH_DELIMITED);
    writeLengthDelimited(encodedMessage);
  }

  public byte[] toByteArray() {
    return buffer.toByteArray();
  }

  public int size() {
    return buffer.size();
  }
}
