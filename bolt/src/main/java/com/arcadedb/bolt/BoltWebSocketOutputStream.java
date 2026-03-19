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
 * Buffers writes and sends as a single frame on flush().
 * Used to transport Bolt protocol over WebSocket connections (e.g. Neo4j Desktop).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BoltWebSocketOutputStream extends OutputStream {
  private final DataOutputStream      out;
  private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();

  BoltWebSocketOutputStream(final OutputStream out) {
    this.out = new DataOutputStream(out);
  }

  @Override
  public void write(final int b) throws IOException {
    buffer.write(b);
  }

  @Override
  public void write(final byte[] b, final int off, final int len) throws IOException {
    buffer.write(b, off, len);
  }

  @Override
  public void flush() throws IOException {
    if (buffer.size() > 0) {
      final byte[] data = buffer.toByteArray();
      buffer.reset();
      writeFrame(data);
    }
    out.flush();
  }

  private void writeFrame(final byte[] payload) throws IOException {
    // FIN bit + binary opcode (0x82)
    out.writeByte(0x82);

    // Server-to-client frames are NOT masked
    if (payload.length < 126) {
      out.writeByte(payload.length);
    } else if (payload.length < 65536) {
      out.writeByte(126);
      out.writeShort(payload.length);
    } else {
      out.writeByte(127);
      out.writeLong(payload.length);
    }

    out.write(payload);
  }
}
