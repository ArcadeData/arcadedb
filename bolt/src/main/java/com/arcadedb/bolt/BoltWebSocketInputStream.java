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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * InputStream that reads WebSocket frames and returns the unframed payload bytes.
 * Used to transport Bolt protocol over WebSocket connections (e.g. Neo4j Desktop).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BoltWebSocketInputStream extends InputStream {
  private final DataInputStream in;
  private byte[]  buffer;
  private int     bufferPos;
  private int     bufferLen;
  private boolean closed;

  BoltWebSocketInputStream(final InputStream in) {
    this.in = new DataInputStream(in);
  }

  @Override
  public int read() throws IOException {
    if (closed)
      return -1;
    while (bufferPos >= bufferLen) {
      if (!readNextFrame())
        return -1;
    }
    return buffer[bufferPos++] & 0xFF;
  }

  @Override
  public int read(final byte[] b, final int off, final int len) throws IOException {
    if (closed)
      return -1;
    int totalRead = 0;
    while (totalRead < len) {
      while (bufferPos >= bufferLen) {
        if (!readNextFrame())
          return totalRead > 0 ? totalRead : -1;
      }
      final int available = bufferLen - bufferPos;
      final int toRead = Math.min(available, len - totalRead);
      System.arraycopy(buffer, bufferPos, b, off + totalRead, toRead);
      bufferPos += toRead;
      totalRead += toRead;
    }
    return totalRead;
  }

  private boolean readNextFrame() throws IOException {
    while (true) {
      final int b0 = in.readUnsignedByte();
      final int b1 = in.readUnsignedByte();

      final int opcode = b0 & 0x0F;
      final boolean masked = (b1 & 0x80) != 0;
      long payloadLen = b1 & 0x7F;

      if (payloadLen == 126)
        payloadLen = in.readUnsignedShort();
      else if (payloadLen == 127)
        payloadLen = in.readLong();

      byte[] maskKey = null;
      if (masked) {
        maskKey = new byte[4];
        in.readFully(maskKey);
      }

      final byte[] payload = new byte[(int) payloadLen];
      if (payloadLen > 0)
        in.readFully(payload);

      if (masked) {
        for (int i = 0; i < payload.length; i++)
          payload[i] ^= maskKey[i % 4];
      }

      switch (opcode) {
      case 0x1: // text frame
      case 0x2: // binary frame
        buffer = payload;
        bufferPos = 0;
        bufferLen = payload.length;
        return true;
      case 0x8: // close frame
        closed = true;
        return false;
      case 0x9: // ping - skip (pong requires output access)
      case 0xA: // pong - skip
      default:
        continue;
      }
    }
  }
}
