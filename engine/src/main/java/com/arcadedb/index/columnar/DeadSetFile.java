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
import com.arcadedb.utility.FileUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * The persisted dead set of one segment: the positions of its rows that a newer write superseded or deleted (the complement of its
 * live bitmap within its rows). One immutable file per publish that changed it, {@code seg-<id>.dead.<version>}; the manifest names
 * the version. The file is one {@link BlockHeader} block, big-endian:
 * <pre>
 * DEAD_MAGIC, version 1, row count = page count n, 3 columns:
 *   [maxRecordsInPage int][n page numbers int, strictly ascending][n stripes of maxRecordsInPage / 64 longs]
 * </pre>
 * Both CRCs are verified on read; every malformed file surfaces as {@link BlockCorruptionException}.
 */
final class DeadSetFile {
  static final int DEAD_MAGIC = 0x41434453; // "ACDS"
  static final int VERSION    = 1;

  private static final int COLUMNS    = 3;
  private static final int META_BYTES = 4;

  private DeadSetFile() {
  }

  /**
   * Writes and fsyncs a new dead set file. The caller fsyncs the directory before publishing a manifest that names it.
   *
   * @throws java.nio.file.FileAlreadyExistsException when the file exists: a dead version is written once
   */
  static void write(final Path file, final PositionBitmap dead) throws IOException {
    final int[] pages = dead.pages();
    final int words = dead.maxRecordsInPage() / 64;
    final int headerSize = BlockHeader.FIXED_BYTES + 4 * COLUMNS;
    final long pagesBytes = 4L * pages.length;
    final long stripesBytes = 8L * pages.length * words;
    final long total = headerSize + META_BYTES + pagesBytes + stripesBytes;
    if (total > Integer.MAX_VALUE)
      throw new IllegalArgumentException("Dead set of " + pages.length + " pages does not fit one file block");

    final ByteBuffer buffer = ByteBuffer.allocate((int) total);
    buffer.position(headerSize);
    buffer.putInt(dead.maxRecordsInPage());
    for (final int page : pages)
      buffer.putInt(page);
    for (final int page : pages)
      for (final long word : dead.stripe(page))
        buffer.putLong(word);
    final int payloadCrc = BlockHeader.crc32(buffer, headerSize, (int) total - headerSize);
    buffer.position(0);
    new BlockHeader(DEAD_MAGIC, VERSION, pages.length, new int[] { META_BYTES, (int) pagesBytes, (int) stripesBytes }, payloadCrc).write(
        buffer);
    buffer.position(0);

    // CREATE_NEW refuses an existing file before anything is written; a file this call created is removed when the write fails, so
    // no partial file stays behind under a name a retry reuses
    final FileChannel channel = FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
    try (channel) {
      FileUtils.writeFully(channel, buffer, 0);
      channel.force(true);
    } catch (final IOException | RuntimeException e) {
      Files.deleteIfExists(file);
      throw e;
    }
  }

  /**
   * @param maxRecordsInPage slots per page of the segment the dead set belongs to
   *
   * @throws BlockCorruptionException when the file fails a CRC or format check, or was written for another page size
   */
  static PositionBitmap read(final Path file, final int maxRecordsInPage) throws IOException {
    try (final FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
      final long size = channel.size();
      if (size < BlockHeader.FIXED_BYTES + 4 * COLUMNS + META_BYTES || size > Integer.MAX_VALUE)
        throw new BlockCorruptionException("Dead set " + file + " has an invalid size of " + size + " bytes");
      // sized from the real file length, never from bytes read
      final ByteBuffer buffer = ByteBuffer.allocate((int) size);
      if (FileUtils.readFully(channel, buffer, 0) != size)
        throw new BlockCorruptionException("Dead set " + file + " is truncated");
      buffer.flip();
      return decode(file, buffer, maxRecordsInPage);
    }
  }

  private static PositionBitmap decode(final Path file, final ByteBuffer buffer, final int maxRecordsInPage) {
    final BlockHeader header = BlockHeader.read(buffer, DEAD_MAGIC);
    if (header.version() != VERSION)
      throw new BlockCorruptionException("Dead set " + file + " has unsupported version " + header.version());
    if (header.columnCount() != COLUMNS || header.totalSize() != buffer.limit())
      throw new BlockCorruptionException("Dead set " + file + " has " + header.columnCount() + " columns and " + header.totalSize()
          + " bytes, expected " + COLUMNS + " columns and " + buffer.limit() + " bytes");
    header.verifyPayload(buffer, 0);

    final int stored = buffer.getInt(header.columnOffset(0));
    if (header.columnSize(0) != META_BYTES || stored != maxRecordsInPage)
      throw new BlockCorruptionException(
          "Dead set " + file + " was written for " + stored + " slots per page, the segment has " + maxRecordsInPage);
    final int words = maxRecordsInPage / 64;
    final int count = header.rowCount();
    if (header.columnSize(1) != 4L * count || header.columnSize(2) != 8L * count * words)
      throw new BlockCorruptionException("Dead set " + file + " column sizes do not match its " + count + " pages");

    final PositionBitmap dead = new PositionBitmap(maxRecordsInPage);
    final long[] stripe = new long[words];
    int previous = -1;
    for (int i = 0; i < count; i++) {
      final int page = buffer.getInt(header.columnOffset(1) + 4 * i);
      if (page <= previous)
        throw new BlockCorruptionException("Dead set " + file + " pages are not strictly ascending at " + page);
      previous = page;
      final int stripeOffset = header.columnOffset(2) + 8 * words * i;
      for (int w = 0; w < words; w++)
        stripe[w] = buffer.getLong(stripeOffset + 8 * w);
      dead.orStripe(page, stripe, 0);
    }
    return dead;
  }
}
