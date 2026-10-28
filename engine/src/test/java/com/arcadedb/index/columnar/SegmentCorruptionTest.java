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
package com.arcadedb.index.columnar;

import com.arcadedb.engine.block.BlockCorruptionException;
import com.arcadedb.engine.block.BlockHeader;
import com.arcadedb.engine.codec.BitPackedCodec;
import com.arcadedb.engine.codec.BlockwiseLinearCodec;
import com.arcadedb.engine.codec.LinearCodec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

import static com.arcadedb.index.columnar.SegmentRoundTripTest.ALL;
import static com.arcadedb.index.columnar.SegmentRoundTripTest.Data;
import static com.arcadedb.index.columnar.SegmentRoundTripTest.Pairs;
import static com.arcadedb.index.columnar.SegmentRoundTripTest.everyNth;
import static com.arcadedb.index.columnar.SegmentRoundTripTest.readBlock;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SegmentCorruptionTest {
  private static final int BLOCK_ROWS = 4096;

  @TempDir
  Path dir;

  private Data data;
  private Path file;

  private void writeSegment() throws IOException {
    data = new Data(10_000, 77, 1000);
    file = dir.resolve(SegmentFormat.segmentFileName(5));
    data.write(file, ALL, BLOCK_ROWS, 5);
  }

  private int[] location(final int kind, final int index) throws IOException {
    try (final SegmentReader reader = SegmentReader.open(file, null)) {
      return reader.blockLocationForTesting(kind, index);
    }
  }

  private void writeAt(final long offset, final byte[] bytes) throws IOException {
    try (final FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
      channel.write(ByteBuffer.wrap(bytes), offset);
    }
  }

  private void flipByte(final long offset) throws IOException {
    try (final FileChannel channel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
      final ByteBuffer one = ByteBuffer.allocate(1);
      channel.read(one, offset);
      one.put(0, (byte) (one.get(0) ^ 0x5A));
      one.rewind();
      channel.write(one, offset);
    }
  }

  private void readBrand() throws IOException {
    try (final SegmentReader reader = SegmentReader.open(file, null)) {
      reader.column("brand").forEachOrdinal(everyNth(data, 1), new Pairs());
    }
  }

  @Test
  void footerOffsetPastTheEndIsBlockCorruption() throws IOException {
    writeSegment();
    final long size = Files.size(file);
    writeAt(size - 12, ByteBuffer.allocate(4).putInt((int) size + 100).array());
    assertThatThrownBy(() -> SegmentReader.open(file, null)).isInstanceOf(BlockCorruptionException.class);
  }

  @Test
  void smallFooterOffsetIsRefusedBeforeTheFooterIsRead() throws IOException {
    writeSegment();
    // the footer would span almost the whole file: the layout check refuses it before any allocation or CRC of that size
    final long size = Files.size(file);
    writeAt(size - 12, ByteBuffer.allocate(4).putInt(location(SegmentFormat.KIND_ROWS, 0)[0]).array());
    assertThatThrownBy(() -> SegmentReader.open(file, null)).isInstanceOf(BlockCorruptionException.class)
        .hasMessageContaining("footer layout does not match");
  }

  @Test
  void codecCountMismatchIsBlockCorruption() throws IOException {
    writeSegment();
    final int[] location = location(SegmentFormat.KIND_ROWS, 0);
    final byte[] block = readBlock(file, location);
    final ByteBuffer buffer = ByteBuffer.wrap(block);
    final BlockHeader header = BlockHeader.read(buffer, SegmentFormat.BLOCK_MAGIC);
    final byte[][] columns = new byte[header.columnCount()][];
    for (int c = 0; c < columns.length; c++)
      columns[c] = Arrays.copyOfRange(block, header.columnOffset(c), header.columnOffset(c) + header.columnSize(c));

    // the values sub-column of the brand column (index 0 * 3 + 1): one less value than the block has rows
    final byte[] values = columns[1];
    final int countOffset = switch (values[0]) {
      case BitPackedCodec.CODEC_ID, BlockwiseLinearCodec.CODEC_ID -> 4;
      case LinearCodec.CODEC_ID -> LinearCodec.HEADER_BYTES + 4;
      default -> throw new AssertionError("unknown codec " + values[0]);
    };
    final ByteBuffer le = ByteBuffer.wrap(values).order(ByteOrder.LITTLE_ENDIAN);
    assertThat(le.getInt(countOffset)).isEqualTo(BLOCK_ROWS);
    le.putInt(countOffset, BLOCK_ROWS - 1);

    // a well-formed block again: both CRCs recomputed over the rewritten payload
    final BlockHeader rewritten = BlockHeader.forColumns(header.magic(), header.version(), header.rowCount(), columns);
    final ByteBuffer out = ByteBuffer.allocate(rewritten.totalSize());
    rewritten.write(out);
    for (final byte[] column : columns)
      out.put(column);
    assertThat(out.capacity()).isEqualTo(location[1]);
    writeAt(location[0], out.array());

    assertThatThrownBy(this::readBrand).isInstanceOf(BlockCorruptionException.class);
  }

  @Test
  void flippedPayloadByteIsBlockCorruption() throws IOException {
    writeSegment();
    final int[] location = location(SegmentFormat.KIND_ROWS, 1);
    final byte[] block = readBlock(file, location);
    final BlockHeader header = BlockHeader.read(ByteBuffer.wrap(block), SegmentFormat.BLOCK_MAGIC);
    // a byte inside the brand values of row block 1
    flipByte(location[0] + header.columnOffset(1) + header.columnSize(1) / 2);
    try (final SegmentReader reader = SegmentReader.open(file, null)) {
      // row block 0 is intact
      assertThat(reader.column("brand").isPresent(data.positions[3])).isEqualTo(data.brandPresent[3]);
      assertThatThrownBy(() -> reader.column("brand").isPresent(data.positions[BLOCK_ROWS + 3])).isInstanceOf(BlockCorruptionException.class);
    }
  }

  @Test
  void truncatedFileIsBlockCorruption() throws IOException {
    writeSegment();
    try (final FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
      channel.truncate(channel.size() - 7);
    }
    assertThatThrownBy(() -> SegmentReader.open(file, null)).isInstanceOf(BlockCorruptionException.class);

    // far shorter than a trailer
    try (final FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
      channel.truncate(5);
    }
    assertThatThrownBy(() -> SegmentReader.open(file, null)).isInstanceOf(BlockCorruptionException.class);
  }

  @Test
  void corruptColumnSizeNeverAllocates() throws IOException {
    writeSegment();
    // the high byte of the size of sub-column 1 in the header of row block 0: a size of hundreds of MB
    final int[] location = location(SegmentFormat.KIND_ROWS, 0);
    flipByte(location[0] + 12 + 4);
    assertThatThrownBy(this::readBrand).isInstanceOf(BlockCorruptionException.class);
  }

  @Test
  void forgedColumnSizeWithValidCrcIsBlockCorruption() throws IOException {
    writeSegment();
    final int[] location = location(SegmentFormat.KIND_ROWS, 0);
    final byte[] block = readBlock(file, location);
    final BlockHeader header = BlockHeader.read(ByteBuffer.wrap(block), SegmentFormat.BLOCK_MAGIC);
    // a header with valid CRCs that claims 500 MB for the brand values: only the size guard can refuse it
    final int[] sizes = new int[header.columnCount()];
    for (int c = 0; c < sizes.length; c++)
      sizes[c] = header.columnSize(c);
    sizes[1] += 500_000_000;
    final BlockHeader forged = new BlockHeader(header.magic(), header.version(), header.rowCount(), sizes, header.payloadCrc32());
    final ByteBuffer out = ByteBuffer.allocate(forged.headerSize());
    forged.write(out);
    writeAt(location[0], out.array());
    assertThatThrownBy(this::readBrand).isInstanceOf(BlockCorruptionException.class).hasMessageContaining("does not match its directory entry");
  }

  @Test
  void corruptFileHeaderIsBlockCorruption() throws IOException {
    writeSegment();
    // a byte of the size of the column definitions JSON of the file header block
    flipByte(12 + 4);
    assertThatThrownBy(() -> SegmentReader.open(file, null)).isInstanceOf(BlockCorruptionException.class);
  }

  @Test
  void corruptPresenceBlockIsBlockCorruption() throws IOException {
    writeSegment();
    final int[] location = location(SegmentFormat.KIND_PRESENCE, 0);
    flipByte(location[0] + location[1] - 3);
    try (final SegmentReader reader = SegmentReader.open(file, null)) {
      assertThatThrownBy(() -> reader.row(data.positions[0])).isInstanceOf(BlockCorruptionException.class);
    }
  }
}
