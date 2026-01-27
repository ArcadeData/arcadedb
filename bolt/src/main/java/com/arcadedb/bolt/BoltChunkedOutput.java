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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Handles chunked message framing for BOLT protocol.
 * Messages are sent as: [chunk_size (2 bytes)][chunk_data]...[0x00 0x00]
 */
public class BoltChunkedOutput {
  private static final int MAX_CHUNK_SIZE = 65535;

  private final DataOutputStream out;

  public BoltChunkedOutput(final OutputStream out) {
    this.out = new DataOutputStream(out);
  }

  /**
   * Write a complete message with chunked framing.
   */
  public void writeMessage(final byte[] messageData) throws IOException {
    int offset = 0;
    final int length = messageData.length;

    while (offset < length) {
      final int chunkSize = Math.min(length - offset, MAX_CHUNK_SIZE);

      // Write chunk header (2-byte size, big-endian)
      out.writeShort(chunkSize);

      // Write chunk data
      out.write(messageData, offset, chunkSize);

      offset += chunkSize;
    }

    // Write end marker (two zero bytes)
    out.writeShort(0);
    out.flush();
  }

  /**
   * Write raw bytes directly (for handshake).
   */
  public void writeRaw(final byte[] data) throws IOException {
    out.write(data);
    out.flush();
  }

  /**
   * Write raw int (for handshake version response).
   */
  public void writeRawInt(final int value) throws IOException {
    out.writeInt(value);
    out.flush();
  }

  public void flush() throws IOException {
    out.flush();
  }
}
