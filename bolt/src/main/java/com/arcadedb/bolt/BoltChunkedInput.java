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
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Handles chunked message framing for BOLT protocol input.
 * Messages are received as: [chunk_size (2 bytes)][chunk_data]...[0x00 0x00]
 */
public class BoltChunkedInput {
  private final DataInputStream in;

  public BoltChunkedInput(final InputStream in) {
    this.in = new DataInputStream(in);
  }

  /**
   * Read a complete message by dechunking.
   * Reads chunks until a zero-length chunk is encountered.
   */
  public byte[] readMessage() throws IOException {
    final ByteArrayOutputStream buffer = new ByteArrayOutputStream();

    while (true) {
      // Read chunk size (2 bytes, big-endian)
      final int chunkSize = in.readUnsignedShort();

      if (chunkSize == 0) {
        // End of message
        break;
      }

      // Read chunk data
      final byte[] chunk = new byte[chunkSize];
      in.readFully(chunk);
      buffer.write(chunk);
    }

    return buffer.toByteArray();
  }

  /**
   * Read raw bytes directly (for handshake).
   */
  public byte[] readRaw(final int length) throws IOException {
    final byte[] data = new byte[length];
    in.readFully(data);
    return data;
  }

  /**
   * Read raw int (for handshake).
   */
  public int readRawInt() throws IOException {
    return in.readInt();
  }

  /**
   * Read raw unsigned short (for chunk sizes).
   */
  public int readRawShort() throws IOException {
    return in.readUnsignedShort();
  }

  /**
   * Check if data is available.
   */
  public int available() throws IOException {
    return in.available();
  }
}
