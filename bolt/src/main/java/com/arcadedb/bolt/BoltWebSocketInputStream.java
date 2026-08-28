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
 * InputStream that reads WebSocket frames and returns the unframed payload bytes.
 * Used to transport Bolt protocol over WebSocket connections (e.g. Neo4j Desktop).
 * <p>
 * RFC 6455 lets a peer split one message across a data frame with FIN=0 followed by any number of continuation
 * frames (opcode 0x0), and nothing in the BOLT WebSocket upgrade negotiates that away, so a client stack is free
 * to fragment a large send (a big parameter map, a long query string) at any byte boundary. Fragments are
 * reassembled here before the payload reaches the BOLT dechunker: previously the continuation frames were read,
 * unmasked and then dropped through the {@code default} branch, handing the dechunker a truncated byte stream
 * rather than an error (issue #6802).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BoltWebSocketInputStream extends InputStream {
  private final DataInputStream in;
  private final long    maxFrameSize;
  private byte[]  buffer;
  private int     bufferPos;
  private int     bufferLen;
  private boolean closed;

  /**
   * Payload accumulated so far for a message whose first data frame carried FIN=0. Non-null exactly while a
   * fragmented message is in progress, which is also what tells a legal continuation frame from a stray one.
   * Allocated only when a client actually fragments, so the common unfragmented path stays allocation-free.
   */
  private ByteArrayOutputStream fragments;

  BoltWebSocketInputStream(final InputStream in, final long maxFrameSize) {
    this.in = new DataInputStream(in);
    this.maxFrameSize = maxFrameSize;
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

      final boolean fin = (b0 & 0x80) != 0;
      final int opcode = b0 & 0x0F;
      final boolean masked = (b1 & 0x80) != 0;
      long payloadLen = b1 & 0x7F;

      if (payloadLen == 126)
        payloadLen = in.readUnsignedShort();
      else if (payloadLen == 127)
        payloadLen = in.readLong();

      if (payloadLen < 0 || payloadLen > maxFrameSize)
        throw new IOException("BOLT WebSocket frame too large: " + payloadLen + " bytes (max " + maxFrameSize + ")");

      // A control frame (0x8-0xF) must not be fragmented and carries at most 125 payload bytes (RFC 6455 5.5).
      // Rejected from the header alone, before the payload is read, so a malformed control frame cannot be used
      // to smuggle bytes into - or out of - the reassembly buffer of the message it is interleaved with.
      if (opcode >= 0x8) {
        if (!fin)
          throw new IOException("BOLT WebSocket control frame must not be fragmented (opcode 0x" + Integer.toHexString(opcode) + ")");
        if (payloadLen > 125)
          throw new IOException("BOLT WebSocket control frame payload too large: " + payloadLen + " bytes (max 125)");
      }

      // The accumulated message, not just this frame, is what has to stay inside the cap: otherwise a client
      // could fragment its way past maxFrameSize one legal-looking frame at a time. Only a continuation frame
      // adds to the accumulation, so a ping legally interleaved between two fragments must not be charged
      // against it - otherwise a cap-sized message would be rejected because someone kept the socket alive.
      if (opcode == 0x0 && fragments != null && fragments.size() + payloadLen > maxFrameSize)
        throw new IOException(
            "BOLT WebSocket fragmented message too large: " + (fragments.size() + payloadLen) + " bytes (max " + maxFrameSize + ")");

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
      case 0x0: // continuation of the data frame that opened with FIN=0
        if (fragments == null)
          throw new IOException("BOLT WebSocket continuation frame without a preceding fragmented data frame");
        fragments.writeBytes(payload);
        if (!fin)
          continue; // more fragments to come
        deliver(fragments.toByteArray());
        fragments = null;
        return true;
      case 0x1: // text frame
      case 0x2: // binary frame
        if (fragments != null)
          throw new IOException("BOLT WebSocket data frame received while a fragmented message was still open");
        if (fin) {
          deliver(payload); // the common case: one whole message in one frame, no copy
          return true;
        }
        fragments = new ByteArrayOutputStream(payload.length);
        fragments.writeBytes(payload);
        continue;
      case 0x8: // close frame
        closed = true;
        return false;
      case 0x9: // ping - skip (pong requires output access)
      case 0xA: // pong - skip
        // Control frames may legally be interleaved between the fragments of a message, so skipping one must
        // leave any in-progress reassembly untouched.
        continue;
      default:
        // Reserved opcode. Silently skipping it is what let the continuation frames go missing in the first
        // place, so fail the connection instead, as RFC 6455 5.2 requires of an unknown opcode.
        throw new IOException("BOLT WebSocket unsupported frame opcode: 0x" + Integer.toHexString(opcode));
      }
    }
  }

  private void deliver(final byte[] payload) {
    buffer = payload;
    bufferPos = 0;
    bufferLen = payload.length;
  }
}
