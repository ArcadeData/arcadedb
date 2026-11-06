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
package com.arcadedb.engine.block;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.zip.CRC32;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BlockHeaderTest {
  private static final int MAGIC = 0x46435331;

  private static byte[] bytes(final int n, final int seed) {
    final byte[] b = new byte[n];
    for (int i = 0; i < n; i++)
      b[i] = (byte) (seed + i * 31);
    return b;
  }

  private static BlockHeader sample() {
    return BlockHeader.forColumns(MAGIC, 1, 42, bytes(10, 1), bytes(0, 2), bytes(7, 3));
  }

  /** Header followed by its three columns, as a heap buffer positioned at 0. */
  private static ByteBuffer block() {
    final BlockHeader h = sample();
    final ByteBuffer buf = ByteBuffer.allocate(h.totalSize());
    h.write(buf);
    buf.put(bytes(10, 1)).put(bytes(0, 2)).put(bytes(7, 3));
    buf.flip();
    return buf;
  }

  @Test
  void roundTripPreservesFieldsAndOffsets() {
    final BlockHeader h = sample();
    final ByteBuffer buf = ByteBuffer.allocate(h.headerSize());
    h.write(buf);
    buf.flip();
    final BlockHeader r = BlockHeader.read(buf);
    assertThat(r.magic()).isEqualTo(MAGIC);
    assertThat(r.version()).isEqualTo(1);
    assertThat(r.rowCount()).isEqualTo(42);
    assertThat(r.columnCount()).isEqualTo(3);
    assertThat(r.payloadCrc32()).isEqualTo(h.payloadCrc32());
    assertThat(r.headerSize()).isEqualTo(32);
    assertThat(r.columnOffset(0)).isEqualTo(32);
    assertThat(r.columnOffset(1)).isEqualTo(42);
    assertThat(r.columnOffset(2)).isEqualTo(42);
    assertThat(r.columnSize(0)).isEqualTo(10);
    assertThat(r.columnSize(1)).isEqualTo(0);
    assertThat(r.columnSize(2)).isEqualTo(7);
    assertThat(r.totalSize()).isEqualTo(49);
  }

  @Test
  void writeAndReadAdvanceThePosition() {
    final BlockHeader h = sample();
    final ByteBuffer buf = ByteBuffer.allocate(5 + h.headerSize());
    buf.position(5);
    h.write(buf);
    assertThat(buf.position()).isEqualTo(5 + h.headerSize());
    buf.position(5);
    BlockHeader.read(buf);
    assertThat(buf.position()).isEqualTo(5 + h.headerSize());
  }

  @Test
  void byteOrderOfTheBufferIsIgnored() {
    final BlockHeader h = sample();
    final ByteBuffer little = ByteBuffer.allocate(h.headerSize()).order(ByteOrder.LITTLE_ENDIAN);
    h.write(little);
    little.flip();
    final ByteBuffer direct = ByteBuffer.allocateDirect(h.headerSize()).order(ByteOrder.BIG_ENDIAN);
    direct.put(little).flip();
    final BlockHeader r = BlockHeader.read(direct);
    assertThat(r.magic()).isEqualTo(h.magic());
    assertThat(r.rowCount()).isEqualTo(h.rowCount());
    assertThat(r.payloadCrc32()).isEqualTo(h.payloadCrc32());
    assertThat(r.totalSize()).isEqualTo(h.totalSize());
    // the layout is big-endian whatever the buffer says
    assertThat(little.order(ByteOrder.BIG_ENDIAN).getInt(0)).isEqualTo(MAGIC);
  }

  @Test
  void corruptedColumnSizeIsRefusedByTheHeaderCrc() {
    final ByteBuffer buf = block();
    buf.put(12, (byte) (buf.get(12) ^ 1));
    assertThatThrownBy(() -> BlockHeader.read(buf)).isInstanceOf(BlockCorruptionException.class).hasMessageContaining("header CRC");
  }

  @Test
  void everyFlippedHeaderByteIsRefused() {
    final int headerSize = sample().headerSize();
    for (int i = 0; i < headerSize; i++)
      for (int bit = 0; bit < 8; bit += 7) {
        final ByteBuffer buf = block();
        buf.put(i, (byte) (buf.get(i) ^ (1 << bit)));
        final int at = i;
        assertThatThrownBy(() -> BlockHeader.read(buf)).as("byte %d", at).isInstanceOf(BlockCorruptionException.class);
      }
  }

  @Test
  void aCorruptedSizeWithAValidCrcIsRefusedAsInvalidSizes() {
    // forge a header whose CRC is valid but whose sizes overflow int: two sizes of Integer.MAX_VALUE
    final ByteBuffer buf = ByteBuffer.allocate(FORGED);
    buf.putInt(MAGIC).putShort((short) 1).putShort((short) 2).putInt(1).putInt(Integer.MAX_VALUE).putInt(Integer.MAX_VALUE).putInt(0);
    buf.putInt(BlockHeader.crc32(buf, 0, 24));
    buf.flip();
    assertThatThrownBy(() -> BlockHeader.read(buf)).isInstanceOf(BlockCorruptionException.class).hasMessageContaining("size");

    // a negative size with a valid CRC
    final ByteBuffer neg = ByteBuffer.allocate(FORGED);
    neg.putInt(MAGIC).putShort((short) 1).putShort((short) 2).putInt(1).putInt(-1).putInt(5).putInt(0);
    neg.putInt(BlockHeader.crc32(neg, 0, 24));
    neg.flip();
    assertThatThrownBy(() -> BlockHeader.read(neg)).isInstanceOf(BlockCorruptionException.class);
  }

  private static final int FORGED = 28;

  @Test
  void truncatedHeaderIsRefused() {
    final BlockHeader h = sample();
    final ByteBuffer full = ByteBuffer.allocate(h.headerSize());
    h.write(full);
    for (final int len : new int[] { 0, 5, 10, 11, 12, h.headerSize() - 1 }) {
      final ByteBuffer cut = ByteBuffer.wrap(Arrays.copyOf(full.array(), len));
      assertThatThrownBy(() -> BlockHeader.read(cut)).as("len %d", len).isInstanceOf(BlockCorruptionException.class);
    }
  }

  @Test
  void aHugeColumnCountOnATruncatedBufferDoesNotAllocate() {
    final ByteBuffer buf = ByteBuffer.allocate(16);
    buf.putInt(MAGIC).putShort((short) 1).putShort((short) 0xFFFF).putInt(1).flip();
    assertThatThrownBy(() -> BlockHeader.read(buf)).isInstanceOf(BlockCorruptionException.class);
  }

  @Test
  void payloadMismatchIsRefused() {
    final ByteBuffer buf = block();
    final BlockHeader h = BlockHeader.read(buf.duplicate());
    h.verifyPayload(buf, 0);
    buf.put(40, (byte) (buf.get(40) ^ 0x10));
    assertThatThrownBy(() -> h.verifyPayload(buf, 0)).isInstanceOf(BlockCorruptionException.class);
  }

  @Test
  void verifyPayloadHonoursBlockStartAndRefusesATruncatedBlock() {
    final BlockHeader h = sample();
    final ByteBuffer buf = ByteBuffer.allocate(9 + h.totalSize());
    buf.position(9);
    buf.put(block());
    h.verifyPayload(buf, 9);
    final ByteBuffer cut = ByteBuffer.wrap(Arrays.copyOf(buf.array(), buf.capacity() - 1));
    assertThatThrownBy(() -> h.verifyPayload(cut, 9)).isInstanceOf(BlockCorruptionException.class);
  }

  @Test
  void wrongMagicIsRefused() {
    final ByteBuffer buf = block();
    assertThatThrownBy(() -> BlockHeader.read(buf, 0x12345678)).isInstanceOf(BlockCorruptionException.class).hasMessageContaining("magic");
    assertThat(BlockHeader.read(buf, MAGIC).magic()).isEqualTo(MAGIC);
  }

  @Test
  void crc32MatchesTheJdkAndKeepsThePosition() {
    final byte[] data = bytes(200, 9);
    final CRC32 jdk = new CRC32();
    jdk.update(data, 3, 100);
    final ByteBuffer heap = ByteBuffer.wrap(data);
    heap.position(7);
    assertThat(BlockHeader.crc32(heap, 3, 100)).isEqualTo((int) jdk.getValue());
    assertThat(heap.position()).isEqualTo(7);
    final ByteBuffer direct = ByteBuffer.allocateDirect(200);
    direct.put(data).position(11);
    assertThat(BlockHeader.crc32(direct, 3, 100)).isEqualTo((int) jdk.getValue());
    assertThat(direct.position()).isEqualTo(11);
  }

  @Test
  void zeroColumnsIsValid() {
    final BlockHeader h = BlockHeader.forColumns(MAGIC, 1, 0);
    assertThat(h.headerSize()).isEqualTo(20);
    assertThat(h.totalSize()).isEqualTo(20);
    final ByteBuffer buf = ByteBuffer.allocate(20);
    h.write(buf);
    buf.flip();
    assertThat(BlockHeader.read(buf).columnCount()).isZero();
  }

  @Test
  void invalidShapesAreRefusedAtConstruction() {
    assertThatThrownBy(() -> new BlockHeader(MAGIC, 1, 0, new int[65536], 0)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new BlockHeader(MAGIC, 70000, 0, new int[0], 0)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new BlockHeader(MAGIC, -1, 0, new int[0], 0)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new BlockHeader(MAGIC, 1, 0, new int[] { -1 }, 0)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new BlockHeader(MAGIC, 1, -1, new int[0], 0)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new BlockHeader(MAGIC, 1, 0, new int[] { Integer.MAX_VALUE, Integer.MAX_VALUE }, 0)).isInstanceOf(
        IllegalArgumentException.class);
  }

  @Test
  void constructorCopiesTheSizes() {
    final int[] sizes = { 3, 4 };
    final BlockHeader h = new BlockHeader(MAGIC, 1, 0, sizes, 0);
    sizes[0] = 100;
    assertThat(h.columnSize(0)).isEqualTo(3);
    assertThat(h.totalSize()).isEqualTo(h.headerSize() + 7);
  }

  @Test
  void crc32RefusesARegionOutsideTheBufferLimit() {
    final ByteBuffer buf = ByteBuffer.allocate(100);
    buf.limit(50).position(7);
    assertThatThrownBy(() -> BlockHeader.crc32(buf, 40, 20)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> BlockHeader.crc32(buf, 0, -1)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> BlockHeader.crc32(buf, -1, 4)).isInstanceOf(IllegalArgumentException.class);
    assertThat(buf.position()).isEqualTo(7);
    assertThat(buf.limit()).isEqualTo(50);
  }
}
