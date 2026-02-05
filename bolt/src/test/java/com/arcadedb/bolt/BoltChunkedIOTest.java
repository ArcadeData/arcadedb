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
import java.io.IOException;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for BOLT chunked I/O classes.
 */
class BoltChunkedIOTest {

  // ============ BoltChunkedOutput tests ============

  @Test
  void writeEmptyMessage() throws IOException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final BoltChunkedOutput output = new BoltChunkedOutput(baos);

    output.writeMessage(new byte[0]);

    final byte[] result = baos.toByteArray();
    // Should only contain end marker (0x00 0x00)
    assertThat(result).hasSize(2);
    assertThat(result[0]).isEqualTo((byte) 0x00);
    assertThat(result[1]).isEqualTo((byte) 0x00);
  }

  @Test
  void writeSmallMessage() throws IOException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final BoltChunkedOutput output = new BoltChunkedOutput(baos);

    final byte[] message = { 0x01, 0x02, 0x03, 0x04, 0x05 };
    output.writeMessage(message);

    final byte[] result = baos.toByteArray();
    // 2 bytes for size + 5 bytes data + 2 bytes end marker
    assertThat(result).hasSize(9);
    // Check chunk size (big-endian)
    assertThat(result[0]).isEqualTo((byte) 0x00);
    assertThat(result[1]).isEqualTo((byte) 0x05);
    // Check data
    assertThat(result[2]).isEqualTo((byte) 0x01);
    assertThat(result[3]).isEqualTo((byte) 0x02);
    assertThat(result[4]).isEqualTo((byte) 0x03);
    assertThat(result[5]).isEqualTo((byte) 0x04);
    assertThat(result[6]).isEqualTo((byte) 0x05);
    // Check end marker
    assertThat(result[7]).isEqualTo((byte) 0x00);
    assertThat(result[8]).isEqualTo((byte) 0x00);
  }

  @Test
  void writeMessageExactlyMaxChunkSize() throws IOException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final BoltChunkedOutput output = new BoltChunkedOutput(baos);

    // Create message exactly 65535 bytes (max chunk size)
    final byte[] message = new byte[65535];
    Arrays.fill(message, (byte) 0xAB);
    output.writeMessage(message);

    final byte[] result = baos.toByteArray();
    // 2 bytes for size + 65535 bytes data + 2 bytes end marker
    assertThat(result).hasSize(65539);
    // Check chunk size (0xFFFF = 65535)
    assertThat(result[0]).isEqualTo((byte) 0xFF);
    assertThat(result[1]).isEqualTo((byte) 0xFF);
  }

  @Test
  void writeMessageLargerThanMaxChunkSize() throws IOException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final BoltChunkedOutput output = new BoltChunkedOutput(baos);

    // Create message larger than max chunk size (65535 + 100 = 65635 bytes)
    final byte[] message = new byte[65635];
    Arrays.fill(message, 0, 65535, (byte) 0xAA);
    Arrays.fill(message, 65535, 65635, (byte) 0xBB);
    output.writeMessage(message);

    final byte[] result = baos.toByteArray();
    // First chunk: 2 bytes size + 65535 bytes
    // Second chunk: 2 bytes size + 100 bytes
    // End marker: 2 bytes
    assertThat(result).hasSize(65641);

    // First chunk size (0xFFFF)
    assertThat(result[0]).isEqualTo((byte) 0xFF);
    assertThat(result[1]).isEqualTo((byte) 0xFF);

    // Second chunk size (0x0064 = 100)
    assertThat(result[65537]).isEqualTo((byte) 0x00);
    assertThat(result[65538]).isEqualTo((byte) 0x64);

    // End marker at the end
    assertThat(result[65639]).isEqualTo((byte) 0x00);
    assertThat(result[65640]).isEqualTo((byte) 0x00);
  }

  @Test
  void writeRawBytes() throws IOException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final BoltChunkedOutput output = new BoltChunkedOutput(baos);

    final byte[] raw = { 0x60, 0x60, (byte) 0xB0, 0x17 }; // BOLT magic
    output.writeRaw(raw);

    assertThat(baos.toByteArray()).isEqualTo(raw);
  }

  @Test
  void writeRawInt() throws IOException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final BoltChunkedOutput output = new BoltChunkedOutput(baos);

    output.writeRawInt(0x00000104); // BOLT version 4.4

    final byte[] result = baos.toByteArray();
    assertThat(result).hasSize(4);
    assertThat(result[0]).isEqualTo((byte) 0x00);
    assertThat(result[1]).isEqualTo((byte) 0x00);
    assertThat(result[2]).isEqualTo((byte) 0x01);
    assertThat(result[3]).isEqualTo((byte) 0x04);
  }

  // ============ BoltChunkedInput tests ============

  @Test
  void readEmptyMessage() throws IOException {
    // Just end marker
    final byte[] input = { 0x00, 0x00 };
    final BoltChunkedInput chunkedInput = new BoltChunkedInput(new ByteArrayInputStream(input));

    final byte[] message = chunkedInput.readMessage();
    assertThat(message).isEmpty();
  }

  @Test
  void readSmallMessage() throws IOException {
    // Size (5) + data + end marker
    final byte[] input = {
        0x00, 0x05,  // chunk size = 5
        0x01, 0x02, 0x03, 0x04, 0x05,  // data
        0x00, 0x00   // end marker
    };
    final BoltChunkedInput chunkedInput = new BoltChunkedInput(new ByteArrayInputStream(input));

    final byte[] message = chunkedInput.readMessage();
    assertThat(message).containsExactly(0x01, 0x02, 0x03, 0x04, 0x05);
  }

  @Test
  void readMultiChunkMessage() throws IOException {
    // Two chunks: 3 bytes + 2 bytes
    final byte[] input = {
        0x00, 0x03,  // first chunk size = 3
        0x0A, 0x0B, 0x0C,  // first chunk data
        0x00, 0x02,  // second chunk size = 2
        0x0D, 0x0E,  // second chunk data
        0x00, 0x00   // end marker
    };
    final BoltChunkedInput chunkedInput = new BoltChunkedInput(new ByteArrayInputStream(input));

    final byte[] message = chunkedInput.readMessage();
    assertThat(message).containsExactly(0x0A, 0x0B, 0x0C, 0x0D, 0x0E);
  }

  @Test
  void readRawBytes() throws IOException {
    final byte[] input = { 0x60, 0x60, (byte) 0xB0, 0x17, 0x00, 0x00 };
    final BoltChunkedInput chunkedInput = new BoltChunkedInput(new ByteArrayInputStream(input));

    final byte[] raw = chunkedInput.readRaw(4);
    assertThat(raw).containsExactly(0x60, 0x60, 0xB0, 0x17);
  }

  @Test
  void readRawInt() throws IOException {
    final byte[] input = { 0x00, 0x00, 0x01, 0x04 };
    final BoltChunkedInput chunkedInput = new BoltChunkedInput(new ByteArrayInputStream(input));

    final int value = chunkedInput.readRawInt();
    assertThat(value).isEqualTo(0x00000104);
  }

  @Test
  void readRawShort() throws IOException {
    final byte[] input = { 0x00, 0x05 };
    final BoltChunkedInput chunkedInput = new BoltChunkedInput(new ByteArrayInputStream(input));

    final int value = chunkedInput.readRawShort();
    assertThat(value).isEqualTo(5);
  }

  @Test
  void readLargeChunkSize() throws IOException {
    // Chunk size 0xFFFF (65535)
    final byte[] input = new byte[65535 + 4]; // size header + data + end marker
    input[0] = (byte) 0xFF;
    input[1] = (byte) 0xFF;
    Arrays.fill(input, 2, 65537, (byte) 0xCC);
    input[65537] = 0x00;
    input[65538] = 0x00;

    final BoltChunkedInput chunkedInput = new BoltChunkedInput(new ByteArrayInputStream(input));
    final byte[] message = chunkedInput.readMessage();

    assertThat(message).hasSize(65535);
    assertThat(message[0]).isEqualTo((byte) 0xCC);
    assertThat(message[message.length - 1]).isEqualTo((byte) 0xCC);
  }

  @Test
  void available() throws IOException {
    final byte[] input = { 0x01, 0x02, 0x03 };
    final BoltChunkedInput chunkedInput = new BoltChunkedInput(new ByteArrayInputStream(input));

    assertThat(chunkedInput.available()).isEqualTo(3);
  }

  // ============ Round-trip tests ============

  @Test
  void roundTripSmallMessage() throws IOException {
    final byte[] originalMessage = { 0x01, 0x02, 0x03, 0x04, 0x05 };

    // Write
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final BoltChunkedOutput output = new BoltChunkedOutput(baos);
    output.writeMessage(originalMessage);

    // Read
    final BoltChunkedInput input = new BoltChunkedInput(new ByteArrayInputStream(baos.toByteArray()));
    final byte[] readMessage = input.readMessage();

    assertThat(readMessage).isEqualTo(originalMessage);
  }

  @Test
  void roundTripLargeMessage() throws IOException {
    // Message larger than max chunk size
    final byte[] originalMessage = new byte[100000];
    for (int i = 0; i < originalMessage.length; i++) {
      originalMessage[i] = (byte) (i % 256);
    }

    // Write
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final BoltChunkedOutput output = new BoltChunkedOutput(baos);
    output.writeMessage(originalMessage);

    // Read
    final BoltChunkedInput input = new BoltChunkedInput(new ByteArrayInputStream(baos.toByteArray()));
    final byte[] readMessage = input.readMessage();

    assertThat(readMessage).isEqualTo(originalMessage);
  }

  @Test
  void roundTripEmptyMessage() throws IOException {
    final byte[] originalMessage = new byte[0];

    // Write
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final BoltChunkedOutput output = new BoltChunkedOutput(baos);
    output.writeMessage(originalMessage);

    // Read
    final BoltChunkedInput input = new BoltChunkedInput(new ByteArrayInputStream(baos.toByteArray()));
    final byte[] readMessage = input.readMessage();

    assertThat(readMessage).isEqualTo(originalMessage);
  }
}
