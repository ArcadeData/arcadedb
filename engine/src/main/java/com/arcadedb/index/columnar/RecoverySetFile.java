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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * The persisted recovery set of a database (spec 6.5), {@code <db>/columnar/recovery.pages}: for every file a projection follows
 * (a projected bucket, or the external-property bucket of one, R7), the pages that were dirty or applied but not flushed when it was
 * written. A WAL file is dropped only after this file names every page its commits marked (the housekeeping hook of
 * {@code TransactionManager.cleanWALFiles}), so a crash after the drop still re-applies them at the next open. Big-endian:
 * <pre>
 * RECOVERY_MAGIC int, VERSION int, file count int,
 *   per file: fileId int, page count int, page numbers int[]
 * CRC32 int of every byte before it
 * </pre>
 * Replaced whole through {@link ColumnarStore#writeAtomically}; one writer at a time (the caller serializes).
 */
final class RecoverySetFile {
  static final String FILE_NAME      = "recovery.pages";
  static final int    RECOVERY_MAGIC = 0x41435253; // "ACRS"
  static final int    VERSION        = 1;

  private static final int HEADER_BYTES = 12;
  private static final int CRC_BYTES    = 4;

  private RecoverySetFile() {
  }

  /**
   * Writes the recovery set atomically: file {@code fileIds[i]} with the first {@code counts[i]} pages of {@code pages[i]} (R15).
   *
   * @throws IllegalArgumentException when the arrays differ in length or a count is outside its page array
   */
  static void write(final Path file, final int[] fileIds, final int[][] pages, final int[] counts) throws IOException {
    if (fileIds.length != pages.length || fileIds.length != counts.length)
      throw new IllegalArgumentException("Recovery set of " + fileIds.length + " files with " + pages.length + " page lists and "
          + counts.length + " counts");
    long size = HEADER_BYTES + CRC_BYTES;
    for (int i = 0; i < fileIds.length; i++) {
      if (counts[i] < 0 || counts[i] > pages[i].length)
        throw new IllegalArgumentException("Invalid page count " + counts[i] + " of file " + fileIds[i]);
      size += 8L + 4L * counts[i];
    }
    if (size > Integer.MAX_VALUE)
      throw new IllegalArgumentException("Recovery set of " + size + " bytes does not fit one file");

    final ByteBuffer buffer = ByteBuffer.allocate((int) size);
    buffer.putInt(RECOVERY_MAGIC).putInt(VERSION).putInt(fileIds.length);
    for (int i = 0; i < fileIds.length; i++) {
      buffer.putInt(fileIds[i]).putInt(counts[i]);
      for (int p = 0; p < counts[i]; p++)
        buffer.putInt(pages[i][p]);
    }
    buffer.putInt(crc(buffer.array(), buffer.position()));
    ColumnarStore.writeAtomically(file, buffer.array());
  }

  /**
   * Cold path (an open): the pages by file id, an empty map when the file does not exist. A file id written twice keeps its last list.
   *
   * @throws BlockCorruptionException when the file fails its CRC or format check
   */
  static Map<Integer, int[]> read(final Path file) throws IOException {
    final byte[] bytes;
    try {
      bytes = Files.readAllBytes(file);
    } catch (final NoSuchFileException e) {
      return new HashMap<>();
    }
    if (bytes.length < HEADER_BYTES + CRC_BYTES)
      throw new BlockCorruptionException("Columnar recovery set " + file + " is truncated: " + bytes.length + " bytes");
    final ByteBuffer buffer = ByteBuffer.wrap(bytes);
    final int body = bytes.length - CRC_BYTES;
    if (buffer.getInt(body) != crc(bytes, body))
      throw new BlockCorruptionException("Columnar recovery set " + file + " fails its CRC check");
    if (buffer.getInt() != RECOVERY_MAGIC)
      throw new BlockCorruptionException("Columnar recovery set " + file + " has a wrong magic number");
    final int version = buffer.getInt();
    if (version != VERSION)
      throw new BlockCorruptionException("Columnar recovery set " + file + " has the unsupported version " + version);
    final int files = buffer.getInt();
    // every file takes 8 bytes at least: a count beyond that is no reason to allocate anything
    if (files < 0 || files > (body - HEADER_BYTES) / 8)
      throw new BlockCorruptionException("Columnar recovery set " + file + " declares " + files + " files in " + bytes.length + " bytes");
    final Map<Integer, int[]> result = new HashMap<>(files * 2);
    for (int i = 0; i < files; i++) {
      if (body - buffer.position() < 8)
        throw new BlockCorruptionException("Columnar recovery set " + file + " is truncated in the entry of file " + i);
      final int fileId = buffer.getInt();
      final int count = buffer.getInt();
      if (count < 0 || count > (body - buffer.position()) / 4)
        throw new BlockCorruptionException("Columnar recovery set " + file + " declares " + count + " pages of file " + fileId
            + " past its end");
      final int[] pages = new int[count];
      for (int p = 0; p < count; p++)
        pages[p] = buffer.getInt();
      result.put(fileId, pages);
    }
    if (buffer.position() != body)
      throw new BlockCorruptionException("Columnar recovery set " + file + " holds " + (body - buffer.position()) + " bytes past its last entry");
    return result;
  }

  private static int crc(final byte[] bytes, final int length) {
    final CRC32 crc = new CRC32();
    crc.update(bytes, 0, length);
    return (int) crc.getValue();
  }
}
