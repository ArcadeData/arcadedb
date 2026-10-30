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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.CRC32;

/**
 * Self-describing header of a columnar block. Always big-endian, whatever the byte order of the buffer it is written to or read from.
 * <pre>
 * offset  size  field
 * 0       4     magic (chosen by the format using it)
 * 4       2     version, unsigned
 * 6       2     column count n, unsigned
 * 8       4     row count
 * 12      4n    column sizes
 * 12+4n   4     payload CRC32 (the column bytes that follow the header, in order)
 * 16+4n   4     header CRC32 (bytes [0, 16+4n))
 * </pre>
 * {@link #read(ByteBuffer)} checks the header CRC before building the size array, so a corrupted size is refused before a caller can
 * allocate from it. Column offsets are relative to the first byte of the header.
 */
public final class BlockHeader {
  public static final int FIXED_BYTES = 20;

  private static final int MAX_UNSIGNED_SHORT = 0xFFFF;
  private static final int COUNT_OFFSET = 6;
  private static final int SIZES_OFFSET = 12;

  private final int   magic;
  private final int   version;
  private final int   rowCount;
  private final int   payloadCrc32;
  private final int[] columnSizes;
  private final int[] columnOffsets;
  private final int   totalSize;

  public BlockHeader(final int magic, final int version, final int rowCount, final int[] columnSizes, final int payloadCrc32) {
    if (version < 0 || version > MAX_UNSIGNED_SHORT)
      throw new IllegalArgumentException("Block version " + version + " is outside 0.." + MAX_UNSIGNED_SHORT);
    if (columnSizes.length > MAX_UNSIGNED_SHORT)
      throw new IllegalArgumentException("Block column count " + columnSizes.length + " is outside 0.." + MAX_UNSIGNED_SHORT);
    if (rowCount < 0)
      throw new IllegalArgumentException("Block row count " + rowCount + " is negative");
    this.magic = magic;
    this.version = version;
    this.rowCount = rowCount;
    this.payloadCrc32 = payloadCrc32;
    this.columnSizes = columnSizes.clone();
    this.columnOffsets = new int[columnSizes.length];
    long offset = headerSizeFor(columnSizes.length);
    for (int i = 0; i < columnSizes.length; i++) {
      if (columnSizes[i] < 0)
        throw new IllegalArgumentException("Block column " + i + " has negative size " + columnSizes[i]);
      if (offset > Integer.MAX_VALUE)
        throw new IllegalArgumentException("Block total size exceeds " + Integer.MAX_VALUE);
      columnOffsets[i] = (int) offset;
      offset += columnSizes[i];
    }
    if (offset > Integer.MAX_VALUE)
      throw new IllegalArgumentException("Block total size exceeds " + Integer.MAX_VALUE);
    this.totalSize = (int) offset;
  }

  public static BlockHeader forColumns(final int magic, final int version, final int rowCount, final byte[]... columns) {
    final int[] sizes = new int[columns.length];
    final CRC32 crc = new CRC32();
    for (int i = 0; i < columns.length; i++) {
      sizes[i] = columns[i].length;
      crc.update(columns[i]);
    }
    return new BlockHeader(magic, version, rowCount, sizes, (int) crc.getValue());
  }

  public static BlockHeader read(final ByteBuffer buffer) {
    final ByteBuffer in = buffer.duplicate().order(ByteOrder.BIG_ENDIAN);
    final int start = in.position();
    if (in.remaining() < SIZES_OFFSET)
      throw new BlockCorruptionException("Truncated block header: " + in.remaining() + " bytes, at least " + SIZES_OFFSET + " needed");
    final int count = in.getShort(start + COUNT_OFFSET) & MAX_UNSIGNED_SHORT;
    final int headerSize = headerSizeFor(count);
    // checked against the real remaining bytes before the size array is allocated
    if (in.remaining() < headerSize)
      throw new BlockCorruptionException("Truncated block header: " + in.remaining() + " bytes, " + headerSize + " needed");
    final int crcOffset = start + headerSize - 4;
    final int stored = in.getInt(crcOffset);
    final int computed = crc32(in, start, headerSize - 4);
    if (stored != computed)
      throw new BlockCorruptionException(
          "Block header CRC mismatch: stored " + Integer.toHexString(stored) + ", computed " + Integer.toHexString(computed));

    final int magic = in.getInt(start);
    final int version = in.getShort(start + 4) & MAX_UNSIGNED_SHORT;
    final int rowCount = in.getInt(start + 8);
    final int[] sizes = new int[count];
    for (int i = 0; i < count; i++)
      sizes[i] = in.getInt(start + SIZES_OFFSET + 4 * i);
    final int payloadCrc = in.getInt(crcOffset - 4);
    final BlockHeader header;
    try {
      header = new BlockHeader(magic, version, rowCount, sizes, payloadCrc);
    } catch (final IllegalArgumentException e) {
      throw new BlockCorruptionException("Invalid block header sizes: " + e.getMessage());
    }
    buffer.position(start + headerSize);
    return header;
  }

  public static BlockHeader read(final ByteBuffer buffer, final int expectedMagic) {
    final int start = buffer.position();
    final BlockHeader header = read(buffer);
    if (header.magic != expectedMagic) {
      buffer.position(start);
      throw new BlockCorruptionException(
          "Wrong block magic: found " + Integer.toHexString(header.magic) + ", expected " + Integer.toHexString(expectedMagic));
    }
    return header;
  }

  public void write(final ByteBuffer buffer) {
    final ByteBuffer out = buffer.duplicate().order(ByteOrder.BIG_ENDIAN);
    final int start = out.position();
    out.putInt(magic).putShort((short) version).putShort((short) columnSizes.length).putInt(rowCount);
    for (final int size : columnSizes)
      out.putInt(size);
    out.putInt(payloadCrc32);
    out.putInt(crc32(out, start, out.position() - start));
    buffer.position(out.position());
  }

  public int magic() {
    return magic;
  }

  public int version() {
    return version;
  }

  public int rowCount() {
    return rowCount;
  }

  public int columnCount() {
    return columnSizes.length;
  }

  public int payloadCrc32() {
    return payloadCrc32;
  }

  /** Offset of the column from the first byte of the header. */
  public int columnOffset(final int column) {
    return columnOffsets[column];
  }

  public int columnSize(final int column) {
    return columnSizes[column];
  }

  public int headerSize() {
    return headerSizeFor(columnSizes.length);
  }

  public int totalSize() {
    return totalSize;
  }

  /** Verifies the CRC32 of the column bytes, [blockStart + headerSize(), blockStart + totalSize()). */
  public void verifyPayload(final ByteBuffer buffer, final int blockStart) {
    final int from = blockStart + headerSize();
    final long to = (long) blockStart + totalSize;
    if (blockStart < 0 || to > buffer.limit())
      throw new BlockCorruptionException("Truncated block payload: block ends at " + to + ", buffer limit is " + buffer.limit());
    final int computed = crc32(buffer, from, totalSize - headerSize());
    if (computed != payloadCrc32)
      throw new BlockCorruptionException(
          "Block payload CRC mismatch: stored " + Integer.toHexString(payloadCrc32) + ", computed " + Integer.toHexString(computed));
  }

  /** CRC32 of an absolute region. Does not move the position of {@code data}; works on heap, direct and mapped buffers. */
  public static int crc32(final ByteBuffer data, final int offset, final int length) {
    if (offset < 0 || length < 0 || (long) offset + length > data.limit())
      throw new IllegalArgumentException("Region [" + offset + ", +" + length + ") is outside the buffer limit " + data.limit());
    final ByteBuffer region = data.duplicate();
    region.limit(offset + length).position(offset);
    final CRC32 crc = new CRC32();
    crc.update(region);
    return (int) crc.getValue();
  }

  private static int headerSizeFor(final int columnCount) {
    return FIXED_BYTES + 4 * columnCount;
  }
}
