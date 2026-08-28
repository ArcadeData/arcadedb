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
 * Regression tests for issue #5894 - a BOLT WebSocket frame with an unbounded, unauthenticated client-supplied
 * length must never be allowed to drive a byte-array allocation - and for issue #6802: RFC 6455 lets a client
 * split one message across a data frame with FIN=0 plus continuation frames (opcode 0x0), and the transport used
 * to read, unmask and then silently DISCARD every continuation, handing the BOLT dechunker a truncated byte
 * stream rather than an error.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BoltWebSocketInputStreamTest {

  private static final long MAX_FRAME_SIZE = 1024; // small cap so tests stay fast

  private static byte[] maskedFrame(final byte[] payload) {
    return maskedFrame(true, 0x2, payload);
  }

  /**
   * Builds one masked client-to-server frame with an explicit FIN bit and opcode, so a fragmented message can be
   * assembled as {@code [FIN=0, op=0x2, part1][FIN=1, op=0x0, part2]}.
   */
  private static byte[] maskedFrame(final boolean fin, final int opcode, final byte[] payload) {
    final byte[] mask = { 0x01, 0x02, 0x03, 0x04 };
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write((fin ? 0x80 : 0x00) | opcode);
    if (payload.length < 126) {
      out.write(0x80 | payload.length); // masked + 7-bit length
    } else if (payload.length <= 0xFFFF) {
      out.write(0x80 | 126); // masked + 16-bit length marker
      out.write((payload.length >>> 8) & 0xFF);
      out.write(payload.length & 0xFF);
    } else {
      out.write(0x80 | 127); // masked + 64-bit length marker
      final long length = payload.length; // widen: >>> on an int shifts modulo 32, so 56 would wrap to 24
      for (int shift = 56; shift >= 0; shift -= 8)
        out.write((int) ((length >>> shift) & 0xFF));
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

  private static byte[] concat(final byte[]... frames) {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (final byte[] frame : frames)
      out.writeBytes(frame);
    return out.toByteArray();
  }

  private static byte[] readFully(final BoltWebSocketInputStream stream, final int length) throws IOException {
    final byte[] read = new byte[length];
    int off = 0;
    while (off < length) {
      final int n = stream.read(read, off, length - off);
      if (n < 0)
        break;
      off += n;
    }
    assertThat(off).as("stream delivered fewer bytes than the message contained").isEqualTo(length);
    return read;
  }

  /**
   * The report's exact shape: one message split in two. Before the fix, {@code part2} was read, unmasked and
   * dropped, so the reader saw only {@code part1}.
   */
  @Test
  void reassemblesAFragmentedMessage() throws Exception {
    final byte[] part1 = "hello ".getBytes();
    final byte[] part2 = "fragmented bolt".getBytes();

    final BoltWebSocketInputStream stream = new BoltWebSocketInputStream(
        new ByteArrayInputStream(concat(maskedFrame(false, 0x2, part1), maskedFrame(true, 0x0, part2))), MAX_FRAME_SIZE);

    assertThat(readFully(stream, part1.length + part2.length)).isEqualTo("hello fragmented bolt".getBytes());
  }

  @Test
  void reassemblesAMessageSplitAcrossThreeFrames() throws Exception {
    final BoltWebSocketInputStream stream = new BoltWebSocketInputStream(
        new ByteArrayInputStream(concat(
            maskedFrame(false, 0x2, "aaa".getBytes()),
            maskedFrame(false, 0x0, "bbb".getBytes()),
            maskedFrame(true, 0x0, "ccc".getBytes()))), MAX_FRAME_SIZE);

    assertThat(readFully(stream, 9)).isEqualTo("aaabbbccc".getBytes());
  }

  /**
   * RFC 6455 lets a control frame be interleaved between the fragments of a message. Skipping the ping must
   * leave the reassembly in progress untouched.
   */
  @Test
  void pingBetweenFragmentsDoesNotBreakReassembly() throws Exception {
    final BoltWebSocketInputStream stream = new BoltWebSocketInputStream(
        new ByteArrayInputStream(concat(
            maskedFrame(false, 0x2, "abc".getBytes()),
            maskedFrame(true, 0x9, "ping".getBytes()),
            maskedFrame(true, 0x0, "def".getBytes()))), MAX_FRAME_SIZE);

    assertThat(readFully(stream, 6)).isEqualTo("abcdef".getBytes());
  }

  /**
   * Two whole messages, the first fragmented: the reader must not carry state from one into the other.
   */
  @Test
  void aFragmentedMessageIsFollowedNormallyByAnUnfragmentedOne() throws Exception {
    final BoltWebSocketInputStream stream = new BoltWebSocketInputStream(
        new ByteArrayInputStream(concat(
            maskedFrame(false, 0x2, "ab".getBytes()),
            maskedFrame(true, 0x0, "cd".getBytes()),
            maskedFrame("efgh".getBytes()))), MAX_FRAME_SIZE);

    assertThat(readFully(stream, 8)).isEqualTo("abcdefgh".getBytes());
  }

  @Test
  void rejectsAContinuationWithoutAnOpenFragmentedMessage() {
    final BoltWebSocketInputStream stream = new BoltWebSocketInputStream(
        new ByteArrayInputStream(maskedFrame(true, 0x0, "orphan".getBytes())), MAX_FRAME_SIZE);

    assertThatThrownBy(stream::read).isInstanceOf(IOException.class).hasMessageContaining("continuation frame");
  }

  @Test
  void rejectsANewDataFrameWhileAFragmentedMessageIsStillOpen() {
    final BoltWebSocketInputStream stream = new BoltWebSocketInputStream(
        new ByteArrayInputStream(concat(maskedFrame(false, 0x2, "ab".getBytes()), maskedFrame("cd".getBytes()))),
        MAX_FRAME_SIZE);

    assertThatThrownBy(() -> readFully(stream, 4)).isInstanceOf(IOException.class)
        .hasMessageContaining("fragmented message was still open");
  }

  /**
   * The cap has to apply to the reassembled total, or a client fragments its way past it one legal-looking
   * frame at a time.
   */
  @Test
  void rejectsFragmentsThatAccumulatePastTheFrameSizeLimit() {
    final byte[] half = new byte[(int) MAX_FRAME_SIZE - 8];

    final BoltWebSocketInputStream stream = new BoltWebSocketInputStream(
        new ByteArrayInputStream(concat(maskedFrame(false, 0x2, half), maskedFrame(true, 0x0, half))), MAX_FRAME_SIZE);

    assertThatThrownBy(() -> readFully(stream, half.length * 2)).isInstanceOf(IOException.class)
        .hasMessageContaining("fragmented message too large");
  }

  /**
   * The cap bounds what is accumulated, and a ping accumulates nothing: a message that fills the cap exactly must
   * still be delivered when someone keeps the socket alive in the middle of it.
   */
  @Test
  void anInterleavedPingIsNotChargedAgainstTheFragmentedMessageLimit() throws Exception {
    final byte[] first = new byte[(int) MAX_FRAME_SIZE - 24];
    final byte[] last = new byte[24];

    final BoltWebSocketInputStream stream = new BoltWebSocketInputStream(
        new ByteArrayInputStream(concat(
            maskedFrame(false, 0x2, first),
            maskedFrame(true, 0x9, "ping".getBytes()),
            maskedFrame(true, 0x0, last))), MAX_FRAME_SIZE);

    assertThat(readFully(stream, (int) MAX_FRAME_SIZE)).hasSize((int) MAX_FRAME_SIZE);
  }

  /**
   * A reserved opcode used to fall through the same {@code default} branch that swallowed the continuations.
   * RFC 6455 5.2 requires failing the connection instead.
   */
  @Test
  void rejectsAReservedOpcode() {
    final BoltWebSocketInputStream stream = new BoltWebSocketInputStream(
        new ByteArrayInputStream(maskedFrame(true, 0x3, "reserved".getBytes())), MAX_FRAME_SIZE);

    assertThatThrownBy(stream::read).isInstanceOf(IOException.class).hasMessageContaining("unsupported frame opcode");
  }

  @Test
  void rejectsAFragmentedControlFrame() {
    final BoltWebSocketInputStream stream = new BoltWebSocketInputStream(
        new ByteArrayInputStream(maskedFrame(false, 0x9, "ping".getBytes())), MAX_FRAME_SIZE);

    assertThatThrownBy(stream::read).isInstanceOf(IOException.class)
        .hasMessageContaining("control frame must not be fragmented");
  }

  @Test
  void rejectsAnOversizedControlFrame() {
    final BoltWebSocketInputStream stream = new BoltWebSocketInputStream(
        new ByteArrayInputStream(maskedFrame(true, 0x9, new byte[126])), MAX_FRAME_SIZE);

    assertThatThrownBy(stream::read).isInstanceOf(IOException.class)
        .hasMessageContaining("control frame payload too large");
  }
}
