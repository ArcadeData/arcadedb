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

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #5894: a BOLT WebSocket frame with an unbounded, unauthenticated
 * client-supplied length must never be allowed to drive a byte-array allocation.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BoltWebSocketInputStreamTest {

  private static final long MAX_FRAME_SIZE = 1024; // small cap so tests stay fast

  private static byte[] maskedFrame(final byte[] payload) {
    final byte[] mask = { 0x01, 0x02, 0x03, 0x04 };
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(0x82); // FIN + binary opcode
    if (payload.length < 126) {
      out.write(0x80 | payload.length); // masked + 7-bit length
    } else {
      out.write(0x80 | 127); // masked + 64-bit length marker
      for (int shift = 56; shift >= 0; shift -= 8)
        out.write((int) (payload.length >>> shift) & 0xFF);
    }
    out.writeBytes(mask);
    for (int i = 0; i < payload.length; i++)
      out.write(payload[i] ^ mask[i % 4]);
    return out.toByteArray();
  }

  /**
   * Frame header declares a huge extended length, then the socket never delivers the payload
   * (or the mask) - mirrors the 14-byte attack in the report. The stream must reject the frame
   * from the length field alone, without allocating and without blocking on the missing bytes.
   */
  @Test
  void rejectsOversizedFrameBeforeAllocating() throws Exception {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(0x82); // FIN + binary opcode
    out.write(0x80 | 127); // masked + 64-bit length marker
    final long declaredLength = 0x7FFFFFFFL; // ~2GB
    for (int shift = 56; shift >= 0; shift -= 8)
      out.write((int) (declaredLength >>> shift) & 0xFF);
    // no mask key, no payload follows - attacker stops here

    final BoltWebSocketInputStream stream = new BoltWebSocketInputStream(new ByteArrayInputStream(out.toByteArray()), MAX_FRAME_SIZE);

    assertThatThrownBy(() -> stream.read()).isInstanceOf(IOException.class).hasMessageContaining("too large");
  }

  @Test
  void rejectsNegativeExtendedLength() throws Exception {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(0x82);
    out.write(0x80 | 127);
    // 0x8000000000000000 decodes as a negative long when read with readLong()
    out.write(0x80);
    for (int i = 0; i < 7; i++)
      out.write(0x00);

    final BoltWebSocketInputStream stream = new BoltWebSocketInputStream(new ByteArrayInputStream(out.toByteArray()), MAX_FRAME_SIZE);

    assertThatThrownBy(() -> stream.read()).isInstanceOf(IOException.class).hasMessageContaining("too large");
  }

  @Test
  void acceptsFrameWithinLimit() throws Exception {
    final byte[] payload = "hello bolt".getBytes();
    final byte[] frame = maskedFrame(payload);

    final BoltWebSocketInputStream stream = new BoltWebSocketInputStream(new ByteArrayInputStream(frame), MAX_FRAME_SIZE);

    final byte[] read = new byte[payload.length];
    final int n = stream.read(read, 0, read.length);

    assertThat(n).isEqualTo(payload.length);
    assertThat(read).isEqualTo(payload);
  }

  @Test
  void rejectsFrameJustOverLimit() throws Exception {
    final byte[] payload = new byte[(int) MAX_FRAME_SIZE + 1];
    final byte[] frame = maskedFrame(payload);

    final BoltWebSocketInputStream stream = new BoltWebSocketInputStream(new ByteArrayInputStream(frame), MAX_FRAME_SIZE);

    assertThatThrownBy(() -> stream.read()).isInstanceOf(IOException.class).hasMessageContaining("too large");
  }
}
