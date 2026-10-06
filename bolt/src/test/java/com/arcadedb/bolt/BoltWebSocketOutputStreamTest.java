/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #9372: unflushed Bolt messages must not accumulate on the heap behind the WebSocket transport.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BoltWebSocketOutputStreamTest {

  @Test
  void smallWritesBecomeOneFrameOnFlush() throws Exception {
    final ByteArrayOutputStream sink = new ByteArrayOutputStream();
    final BoltWebSocketOutputStream out = new BoltWebSocketOutputStream(sink);

    out.write(new byte[] { 1, 2, 3 }, 0, 3);
    out.write(new byte[] { 4, 5 }, 0, 2);
    assertThat(sink.size()).isZero();

    out.flush();

    assertThat(sink.toByteArray()).containsExactly(0x82, 5, 1, 2, 3, 4, 5);
  }

  @Test
  void unflushedBytesAreEmittedAsFramesInsteadOfAccumulating() throws Exception {
    final ByteArrayOutputStream sink = new ByteArrayOutputStream();
    final BoltWebSocketOutputStream out = new BoltWebSocketOutputStream(sink);

    final byte[] record = new byte[10_000];
    for (int i = 0; i < 100; i++)
      out.write(record, 0, record.length);

    // 1 MB written and never flushed: all but the last partial buffer must already be on the wire
    assertThat(sink.size()).isGreaterThan(900_000);

    out.flush();
    assertThat(payloadOf(sink.toByteArray())).hasSize(1_000_000);
  }

  @Test
  void splitFramesCarryTheBytesInOrder() throws Exception {
    final ByteArrayOutputStream sink = new ByteArrayOutputStream();
    final BoltWebSocketOutputStream out = new BoltWebSocketOutputStream(sink);

    final byte[] expected = new byte[300_000];
    for (int i = 0; i < expected.length; i++)
      expected[i] = (byte) (i * 31);
    for (int off = 0; off < expected.length; off += 7_000)
      out.write(expected, off, Math.min(7_000, expected.length - off));
    out.flush();

    assertThat(Arrays.equals(payloadOf(sink.toByteArray()), expected)).isTrue();
  }

  @Test
  void singleWriteLargerThan64KbUsesTheEightByteLengthEncoding() throws Exception {
    final ByteArrayOutputStream sink = new ByteArrayOutputStream();
    final BoltWebSocketOutputStream out = new BoltWebSocketOutputStream(sink);

    final byte[] big = new byte[100_000];
    Arrays.fill(big, (byte) 7);
    out.write(big, 0, big.length);
    out.flush();

    final byte[] wire = sink.toByteArray();
    assertThat(wire[0]).isEqualTo((byte) 0x82);
    assertThat(wire[1]).isEqualTo((byte) 127);
    assertThat(payloadOf(wire)).hasSize(100_000);
  }

  /** Decodes consecutive unmasked binary frames and concatenates their payloads. */
  private static byte[] payloadOf(final byte[] wire) throws IOException {
    final DataInputStream in = new DataInputStream(new ByteArrayInputStream(wire));
    final ByteArrayOutputStream payload = new ByteArrayOutputStream();
    while (in.available() > 0) {
      assertThat(in.readUnsignedByte()).isEqualTo(0x82);
      final int len7 = in.readUnsignedByte();
      final long len = len7 < 126 ? len7 : len7 == 126 ? in.readUnsignedShort() : in.readLong();
      final byte[] frame = new byte[(int) len];
      in.readFully(frame);
      payload.write(frame);
    }
    return payload.toByteArray();
  }
}
