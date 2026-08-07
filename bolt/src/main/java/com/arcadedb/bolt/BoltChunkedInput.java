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
import com.arcadedb.log.LogManager;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

/**
 * Handles chunked message framing for BOLT protocol input.
 * Messages are received as: [chunk_size (2 bytes)][chunk_data]...[0x00 0x00]
 */
public class BoltChunkedInput {
  private final DataInputStream in;
  private final int             maxMessageSize;

  // See PackStreamReader.WARNED_MISCONFIGURED_LIMITS: bounds the misconfiguration WARNING to once per JVM
  // rather than once per connection (issue #5918 review).
  private static final Set<GlobalConfiguration> WARNED_MISCONFIGURED_LIMITS = ConcurrentHashMap.newKeySet();

  /**
   * Reads {@link GlobalConfiguration#BOLT_MAX_MESSAGE_SIZE}, falling back to its built-in default (with a
   * warning) if configured below 1: a limit that low would reject essentially every message outright, so it is
   * treated as a misconfiguration rather than an intentional (if impractical) lockdown.
   */
  private static int sanitizedMaxMessageSize() {
    final int configured = GlobalConfiguration.BOLT_MAX_MESSAGE_SIZE.getValueAsInteger();
    if (configured < 1) {
      final int fallback = ((Number) GlobalConfiguration.BOLT_MAX_MESSAGE_SIZE.getDefValue()).intValue();
      if (WARNED_MISCONFIGURED_LIMITS.add(GlobalConfiguration.BOLT_MAX_MESSAGE_SIZE))
        LogManager.instance().log(BoltChunkedInput.class, Level.WARNING,
            "BOLT: '%s' is set to %d, below the minimum usable value (1); falling back to the default (%d)",
            GlobalConfiguration.BOLT_MAX_MESSAGE_SIZE.getKey(), configured, fallback);
      return fallback;
    }
    return configured;
  }

  public BoltChunkedInput(final InputStream in) {
    this(in, sanitizedMaxMessageSize());
  }

  /**
   * As {@link #BoltChunkedInput(InputStream)}, with the reassembled-message size bound passed explicitly instead
   * of read from {@link GlobalConfiguration}, so unit tests can exercise the bound without mutating global config.
   */
  public BoltChunkedInput(final InputStream in, final int maxMessageSize) {
    this.in = new DataInputStream(in);
    this.maxMessageSize = maxMessageSize;
  }

  /**
   * Read a complete message by dechunking.
   * Reads chunks until a zero-length chunk is encountered.
   */
  public byte[] readMessage() throws IOException {
    final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    int totalSize = 0;

    while (true) {
      // Read chunk size (2 bytes, big-endian)
      final int chunkSize = in.readUnsignedShort();

      if (chunkSize == 0) {
        // End of message
        break;
      }

      // A client that never sends the terminating zero-length chunk would otherwise grow this buffer unbounded
      // (issue #5918), before the BOLT handshake or authentication ever runs. Checked as "chunkSize > remaining"
      // rather than "totalSize + chunkSize > maxMessageSize": the latter can wrap totalSize negative (permanently
      // defeating the bound) once enough chunks accumulate against a maxMessageSize configured close to
      // Integer.MAX_VALUE, since chunkSize is only bounded by the 16-bit chunk-size field, not by this check.
      if (chunkSize > maxMessageSize - totalSize)
        throw new IOException("BOLT message too large: exceeds " + maxMessageSize + " bytes after chunk reassembly");
      totalSize += chunkSize;

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
