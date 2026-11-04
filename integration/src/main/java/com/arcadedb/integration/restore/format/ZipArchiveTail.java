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
package com.arcadedb.integration.restore.format;

import com.arcadedb.integration.restore.RestoreException;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Whether a ZIP archive is TERMINATED: whether it ends with the end-of-central-directory record that only a backup that
 * finished writes, and whether that record agrees with the archive it ends (issue #11041).
 * <p>
 * The backup writes the central directory last, and a backup that fails part way aborts without writing it
 * ({@code BackupArchiveWriter.abort()}), so that what is left on disk, should its delete fail, restores from nothing. The
 * parallel restore opens the archive with {@code ZipFile}, which reads the central directory and refuses an archive without
 * one. The sequential walk reads it with {@code ZipInputStream}, which reads the local headers only, so an archive cut at an
 * entry boundary was a successful walk that stopped early: the restore reported success over whatever fraction of the
 * records had been written, and {@code CHECK DATABASE} found nothing wrong with it. That walk is the only one an encrypted or
 * an http(s) archive can take, which includes every server-side {@code restore database <name> <url>}.
 * <p>
 * The central directory is the archive's own completeness manifest, written last by every writer that ever produced a
 * backup, so asking for it needs no change of the archive format and holds for every archive already on disk. This class
 * reads the last {@link #CAPACITY} bytes of the archive - all the end records can occupy - and checks that:
 * <ul>
 *   <li>they end with an end-of-central-directory record whose comment runs exactly to the end of the archive;</li>
 *   <li>the central directory it locates ends exactly where the end records begin (the ZIP64 ones when present);</li>
 *   <li>and, from the caller, that it declares the number of entries the walk extracted.</li>
 * </ul>
 * The same check runs on the bytes the sequential walk streamed ({@link TailCapturingInputStream}) and, for an archive on
 * disk, before anything is restored, on the tail of the file.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class ZipArchiveTail {
  private static final int  EOCD_SIZE          = 22;
  private static final int  MAX_COMMENT_SIZE   = 0xFFFF;
  private static final int  ZIP64_LOCATOR_SIZE = 20;
  private static final int  ZIP64_EOCD_SIZE    = 56;
  private static final long EOCD_SIGNATURE     = 0x06054b50L;
  private static final long ZIP64_LOCATOR_SIG  = 0x07064b50L;
  private static final long ZIP64_EOCD_SIG     = 0x06064b50L;

  /** The most the end records of an archive can occupy: a ZIP64 end record, its locator, the end record and its comment. */
  static final int CAPACITY = ZIP64_EOCD_SIZE + ZIP64_LOCATOR_SIZE + EOCD_SIZE + MAX_COMMENT_SIZE;

  private ZipArchiveTail() {
  }

  /**
   * The number of entries the central directory of the archive declares, read from its end records.
   *
   * @param tail          the last {@code length} bytes of the archive, from offset 0
   * @param archiveLength the length of the whole archive (of its ZIP stream, after decryption)
   * @param archiveName   what the refusal names
   * @param encrypted     whether the archive was decrypted, so a refusal also names the wrong key, which reads the same way
   *
   * @throws RestoreException when the archive is not terminated, or its end records do not describe it
   */
  static long declaredEntries(final byte[] tail, final int length, final long archiveLength, final String archiveName,
      final boolean encrypted) {
    final int eocd = findEndOfCentralDirectory(tail, length);
    if (eocd < 0)
      throw new RestoreException(("The archive '%s' is incomplete: it does not end with the central directory a backup writes "
          + "last, so it is a backup that did not finish or a truncated copy of one, and restoring it would silently lose "
          + "every entry after the cut%s").formatted(archiveName,
          encrypted ? " (an encrypted archive read with the wrong key looks the same)" : ""));

    long entries = readUnsignedShort(tail, eocd + 10);
    long centralDirectorySize = readUnsignedInt(tail, eocd + 12);
    long centralDirectoryOffset = readUnsignedInt(tail, eocd + 16);
    final long tailStart = archiveLength - length;
    long endRecordsOffset = tailStart + eocd;

    // THE ZIP64 END RECORD IS AUTHORITATIVE WHEN ITS LOCATOR PRECEDES THE END RECORD, WHICH A WRITER EMITS ALSO FOR AN ARCHIVE
    // WHOSE 32-BIT FIELDS DO NOT OVERFLOW (A SINGLE ENTRY OVER 4 GB IS ENOUGH)
    final int locator = eocd - ZIP64_LOCATOR_SIZE;
    if (locator >= 0 && readUnsignedInt(tail, locator) == ZIP64_LOCATOR_SIG) {
      final long zip64Offset = readLong(tail, locator + 8);
      final long zip64InTail = zip64Offset - tailStart;
      if (zip64InTail < 0 || zip64InTail + ZIP64_EOCD_SIZE > locator || readUnsignedInt(tail, (int) zip64InTail) != ZIP64_EOCD_SIG)
        throw inconsistent(archiveName, "its ZIP64 locator points to no ZIP64 end record");
      entries = readLong(tail, (int) zip64InTail + 32);
      centralDirectorySize = readLong(tail, (int) zip64InTail + 40);
      centralDirectoryOffset = readLong(tail, (int) zip64InTail + 48);
      endRecordsOffset = zip64Offset;
    }

    if (centralDirectoryOffset < 0 || centralDirectorySize < 0 || centralDirectoryOffset + centralDirectorySize != endRecordsOffset)
      throw inconsistent(archiveName, "its central directory (%,d bytes at offset %,d) does not end where its end records begin (offset %,d)"
          .formatted(centralDirectorySize, centralDirectoryOffset, endRecordsOffset));
    return entries;
  }

  /**
   * Refuses an archive whose central directory declares a number of entries other than the number the walk extracted.
   */
  static void checkEntriesRead(final long declared, final long read, final String archiveName) {
    if (declared != read)
      throw inconsistent(archiveName, "its central directory declares %,d entries and %,d were read".formatted(declared, read));
  }

  private static RestoreException inconsistent(final String archiveName, final String why) {
    return new RestoreException("The archive '%s' is not a complete backup: %s".formatted(archiveName, why));
  }

  /**
   * The position of the end-of-central-directory record in {@code tail}, or -1. Searched from the end, and accepted only
   * where its comment length runs exactly to the end of the archive, so a signature that happens to appear inside a
   * comment or inside compressed data is not taken for one.
   */
  private static int findEndOfCentralDirectory(final byte[] tail, final int length) {
    final int lowest = Math.max(0, length - EOCD_SIZE - MAX_COMMENT_SIZE);
    for (int i = length - EOCD_SIZE; i >= lowest; --i)
      if (readUnsignedInt(tail, i) == EOCD_SIGNATURE && readUnsignedShort(tail, i + 20) == length - EOCD_SIZE - i)
        return i;
    return -1;
  }

  private static int readUnsignedShort(final byte[] b, final int offset) {
    return (b[offset] & 0xFF) | (b[offset + 1] & 0xFF) << 8;
  }

  private static long readUnsignedInt(final byte[] b, final int offset) {
    return readUnsignedShort(b, offset) | (long) readUnsignedShort(b, offset + 2) << 16;
  }

  private static long readLong(final byte[] b, final int offset) {
    return readUnsignedInt(b, offset) | readUnsignedInt(b, offset + 4) << 32;
  }

  /**
   * Passes a ZIP stream through unchanged and keeps its last {@link #CAPACITY} bytes and its length, so the end records of an
   * archive that can only be read once (an http(s) download, a decrypting cipher stream) are checked without reading it twice.
   * Placed under the {@code ZipInputStream}, so every byte the walk consumes - including the ones it reads ahead - passes
   * through it once; {@link #drain()} then reads what the walk left, which is the central directory and the end records.
   */
  static final class TailCapturingInputStream extends FilterInputStream {
    private final byte[] ring = new byte[CAPACITY];
    private       long   total;

    TailCapturingInputStream(final InputStream in) {
      super(in);
    }

    @Override
    public int read() throws IOException {
      final int b = in.read();
      if (b >= 0) {
        ring[(int) (total % CAPACITY)] = (byte) b;
        ++total;
      }
      return b;
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
      final int read = in.read(b, off, len);
      if (read > 0)
        capture(b, off, read);
      return read;
    }

    /** Read through rather than skipped underneath, so a skipped byte cannot be missing from the tail. */
    @Override
    public long skip(final long n) throws IOException {
      final byte[] discard = new byte[(int) Math.min(n, 8192)];
      long skipped = 0;
      while (skipped < n) {
        final int read = read(discard, 0, (int) Math.min(discard.length, n - skipped));
        if (read < 0)
          break;
        skipped += read;
      }
      return skipped;
    }

    @Override
    public boolean markSupported() {
      return false;
    }

    /** Reads the stream to its end: what follows the last entry the walk read is the central directory and its end records. */
    void drain() throws IOException {
      final byte[] buffer = new byte[8192];
      while (read(buffer, 0, buffer.length) >= 0) {
        // CAPTURED BY read()
      }
    }

    long length() {
      return total;
    }

    /** The last {@link #tailLength()} bytes read, in order. */
    byte[] tail() {
      final int length = tailLength();
      final byte[] tail = new byte[length];
      final int start = (int) ((total - length) % CAPACITY);
      final int firstPart = Math.min(length, CAPACITY - start);
      System.arraycopy(ring, start, tail, 0, firstPart);
      System.arraycopy(ring, 0, tail, firstPart, length - firstPart);
      return tail;
    }

    int tailLength() {
      return (int) Math.min(total, CAPACITY);
    }

    private void capture(final byte[] b, int off, int len) {
      if (len >= CAPACITY) {
        // ONLY THE LAST CAPACITY BYTES CAN STILL BE PART OF THE TAIL
        total += len - CAPACITY;
        off += len - CAPACITY;
        len = CAPACITY;
      }
      final int position = (int) (total % CAPACITY);
      final int firstPart = Math.min(len, CAPACITY - position);
      System.arraycopy(b, off, ring, position, firstPart);
      System.arraycopy(b, off + firstPart, ring, 0, len - firstPart);
      total += len;
    }
  }
}
