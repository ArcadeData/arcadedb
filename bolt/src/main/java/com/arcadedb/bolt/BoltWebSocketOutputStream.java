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
 * OutputStream that wraps data in WebSocket binary frames.
 * Buffers writes and sends them as a frame on flush(), or as soon as {@link #MAX_BUFFERED_BYTES} are pending, so a long
 * unflushed run of messages (a PULL streaming a large result) never accumulates on the heap. A WebSocket message may
 * be split across frames of a byte stream, and the Bolt chunking inside is untouched.
 * Used to transport Bolt protocol over WebSocket connections (e.g. Neo4j Desktop).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BoltWebSocketOutputStream extends OutputStream {
  static final int MAX_BUFFERED_BYTES = 64 * 1024;

  private final DataOutputStream      out;
  private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();

  BoltWebSocketOutputStream(final OutputStream out) {
    this.out = new DataOutputStream(out);
  }

  @Override
  public void write(final int b) throws IOException {
    buffer.write(b);
    if (buffer.size() >= MAX_BUFFERED_BYTES)
      emitFrame();
  }

  @Override
  public void write(final byte[] b, final int off, final int len) throws IOException {
    buffer.write(b, off, len);
    if (buffer.size() >= MAX_BUFFERED_BYTES)
      emitFrame();
  }

  @Override
  public void flush() throws IOException {
    emitFrame();
    out.flush();
  }

  private void emitFrame() throws IOException {
    if (buffer.size() == 0)
      return;
    writeFrame(buffer);
    buffer.reset();
  }

  private void writeFrame(final ByteArrayOutputStream payload) throws IOException {
    final int length = payload.size();
    // FIN bit + binary opcode (0x82)
    out.writeByte(0x82);

    // Server-to-client frames are NOT masked
    if (length < 126) {
      out.writeByte(length);
    } else if (length < 65536) {
      out.writeByte(126);
      out.writeShort(length);
    } else {
      out.writeByte(127);
      out.writeLong(length);
    }

    payload.writeTo(out);
  }
}
