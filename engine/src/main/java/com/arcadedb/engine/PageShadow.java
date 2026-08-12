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
package com.arcadedb.engine;

import com.arcadedb.log.LogManager;
import com.arcadedb.utility.LongLongHashMap;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.logging.Level;

/**
 * Holds the page pre-images captured by one {@link PageSnapshot} window: RAM first, spilling to a scratch file once
 * the RAM budget is exhausted (issue #6075).
 * <p>
 * <b>Why RAM first.</b> The shadow only ever holds the pages DIRTIED while the window is open, once each, so on a
 * short backup at a moderate write rate it is small. Keeping it in memory up to a budget removes the extra write of
 * challenge C5 entirely in that common case; past the budget the spill is strictly append-only, so the extra write
 * stays sequential and never seeks.
 * <p>
 * <b>Why the index is a primitive open-addressing map</b> (challenge C6). It is probed on every page write while a
 * window is open, from the flush thread. A {@code HashMap<PageId, Long>} would box the value and allocate an entry
 * per shadowed page on the flush hot path; {@link LongLongHashMap} costs 16 bytes per entry with no allocation at
 * all, so a million shadowed pages is 16 MB of index. Keys pack {@code fileId} and {@code pageNumber} into one
 * {@code long}, so they are always non-negative and can never collide with that map's {@code Long.MIN_VALUE} empty
 * marker - which is also what makes {@code Long.MIN_VALUE} usable here as the "not shadowed" lookup default.
 * <p>
 * <b>Concurrency.</b> The index is serialized on this instance, but the I/O is deliberately NOT: the capture side is
 * called from inside {@code PageManager.concurrentPageAccess}'s per-page write slot, so two threads never capture the
 * SAME page but they do capture different pages concurrently, and holding one monitor across a spill write would put
 * every writer of the database behind one thread's disk write for the whole window. {@link #store} therefore reserves
 * its spill offset under the monitor, writes at that absolute position outside it (positional
 * {@link FileChannel#write} is thread safe and the offsets are disjoint by construction), and only then publishes the
 * index entry; {@link #read} resolves the slot under the monitor and copies outside it.
 * <p>
 * That publication order is also what makes the reader safe: the bytes are complete before the entry exists, and
 * {@link #read} looks the entry up under the same monitor, so a reader that observes an entry is guaranteed to
 * observe the complete pre-image. It is what lets {@link PageSnapshot} read runs of pages in bulk with no per-page
 * lock at all, re-checking the shadow afterwards for anything a writer touched underneath it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class PageShadow implements AutoCloseable {
  /** A RAM slot index is stored as-is; a spill offset as {@code -(offset + 1)}, so the two never collide. */
  private static final long NOT_FOUND = Long.MIN_VALUE;

  private final File spillFile;
  private final long maxRAMBytes;
  private final long maxTotalBytes;

  private LongLongHashMap index = new LongLongHashMap();

  private byte[][] ramSlots  = new byte[64][];
  private int      ramCount  = 0;
  private long     ramBytes  = 0L;

  private FileChannel spillChannel;
  private long        spillBytes = 0L;

  private boolean closed = false;

  PageShadow(final File spillFile, final long maxRAMBytes, final long maxTotalBytes) {
    this.spillFile = spillFile;
    this.maxRAMBytes = maxRAMBytes;
    this.maxTotalBytes = maxTotalBytes;
  }

  /** Packs a page coordinate into the primitive index key. Both components are non-negative by construction. */
  static long key(final int fileId, final int pageNumber) {
    return ((long) fileId << 32) | (pageNumber & 0xFFFFFFFFL);
  }

  synchronized boolean contains(final long key) {
    return !closed && index.get(key, NOT_FOUND) != NOT_FOUND;
  }

  /**
   * Stores the pre-image of a page not yet shadowed.
   *
   * @return {@code false} when the configured cap would be breached: the caller must invalidate the whole window
   *     (challenge C4 - a shadow that silently stops capturing would produce a torn snapshot).
   */
  boolean store(final long key, final byte[] content, final int length) throws IOException {
    final FileChannel channel;
    final long offset;

    synchronized (this) {
      if (closed)
        return false;
      if (index.get(key, NOT_FOUND) != NOT_FOUND)
        // ALREADY CAPTURED BY AN EARLIER WRITE TO THE SAME PAGE: THE FIRST PRE-IMAGE IS THE SNAPSHOT ONE, KEEP IT
        return true;

      if (maxTotalBytes > 0 && ramBytes + spillBytes + length > maxTotalBytes)
        return false;

      if (ramBytes + length <= maxRAMBytes) {
        final byte[] copy = new byte[length];
        System.arraycopy(content, 0, copy, 0, length);
        if (ramCount == ramSlots.length) {
          final byte[][] larger = new byte[ramSlots.length * 2][];
          System.arraycopy(ramSlots, 0, larger, 0, ramCount);
          ramSlots = larger;
        }
        ramSlots[ramCount] = copy;
        index.put(key, ramCount++);
        ramBytes += length;
        return true;
      }

      if (spillChannel == null)
        spillChannel = FileChannel.open(spillFile.toPath(), StandardOpenOption.CREATE_NEW, StandardOpenOption.READ,
            StandardOpenOption.WRITE);
      channel = spillChannel;
      // RESERVE THE RANGE, SO CONCURRENT SPILLS GET DISJOINT OFFSETS AND THE CAP ACCOUNTING STAYS EXACT EVEN WHILE
      // THE WRITES THEMSELVES ARE IN FLIGHT
      offset = spillBytes;
      spillBytes += length;
    }

    // OUTSIDE THE MONITOR: ONE THREAD'S DISK WRITE MUST NOT BLOCK EVERY OTHER WRITER OF THE DATABASE FROM SHADOWING
    // AN UNRELATED PAGE. A FAILURE HERE LEAVES A HOLE IN THE SPILL FILE AND NO INDEX ENTRY, AND THE CALLER
    // INVALIDATES THE WHOLE WINDOW - WHICH IS CORRECT, SINCE THAT PAGE'S PRE-IMAGE IS GONE
    final ByteBuffer buffer = ByteBuffer.wrap(content, 0, length);
    long pos = offset;
    while (buffer.hasRemaining())
      pos += channel.write(buffer, pos);

    synchronized (this) {
      // UNREACHABLE IN PRACTICE - PageSnapshot.close() DRAINS THE IN-FLIGHT CAPTURES BEFORE CLOSING THE SHADOW - AND
      // DELIBERATELY NOT REPORTED AS A CAP BREACH: NOBODY WILL EVER READ A WINDOW THAT IS BEING CLOSED
      if (closed)
        return true;
      // PUBLISHED ONLY NOW: A READER THAT SEES THE ENTRY IS GUARANTEED TO SEE THE COMPLETE BYTES
      index.put(key, -(offset + 1));
    }
    return true;
  }

  /**
   * Copies the pre-image of {@code key} into {@code dst} when the page has been shadowed.
   *
   * @return {@code false} when the page is not in the shadow, in which case the on-disk image is still the snapshot
   *     one and the caller reads it from the data file.
   */
  boolean read(final long key, final byte[] dst, final int dstOffset, final int length) throws IOException {
    final long slot;
    final byte[] ramContent;
    final FileChannel channel;

    synchronized (this) {
      if (closed)
        return false;

      slot = index.get(key, NOT_FOUND);
      if (slot == NOT_FOUND)
        return false;

      // A STORED PRE-IMAGE IS IMMUTABLE, SO BOTH THE byte[] AND THE SPILL RANGE CAN BE COPIED OUTSIDE THE MONITOR
      ramContent = slot >= 0 ? ramSlots[(int) slot] : null;
      channel = spillChannel;
    }

    if (ramContent != null) {
      if (ramContent.length != length)
        throw new IOException(
            "Shadowed page size mismatch: expected " + length + " bytes, the pre-image holds " + ramContent.length);
      System.arraycopy(ramContent, 0, dst, dstOffset, length);
      return true;
    }

    final ByteBuffer buffer = ByteBuffer.wrap(dst, dstOffset, length);
    long pos = -(slot + 1);
    while (buffer.hasRemaining()) {
      final int r = channel.read(buffer, pos);
      if (r < 0)
        throw new IOException("Unexpected EOF reading the snapshot shadow file '" + spillFile.getName() + "' at " + pos);
      pos += r;
    }
    return true;
  }

  /** Number of pages currently shadowed. */
  synchronized int getPageCount() {
    return index.size();
  }

  /** Bytes held in RAM plus bytes spilled to disk: what {@code PAGE_SNAPSHOT_MAX_SIZE} caps. */
  synchronized long getSizeInBytes() {
    return ramBytes + spillBytes;
  }

  synchronized long getSpilledBytes() {
    return spillBytes;
  }

  @Override
  public synchronized void close() {
    if (closed)
      return;
    closed = true;

    ramSlots = new byte[0][];
    ramCount = 0;
    ramBytes = 0L;
    // RELEASE THE INDEX TOO: EVERY LOOKUP IS GATED ON closed ABOVE, SO NOTHING PROBES IT AGAIN
    index = new LongLongHashMap(16);

    if (spillChannel != null) {
      try {
        spillChannel.close();
      } catch (final IOException e) {
        LogManager.instance().log(this, Level.WARNING, "Error on closing the snapshot shadow file '%s'", e, spillFile);
      }
      spillChannel = null;
    }

    // PURE SCRATCH (CHALLENGE C8): NOTHING EVER READS IT AGAIN, AND A CRASH THAT LEAVES ONE BEHIND IS CLEANED UP AT
    // THE NEXT DATABASE OPEN
    if (spillFile.exists()) {
      try {
        Files.delete(spillFile.toPath());
      } catch (final IOException e) {
        LogManager.instance().log(this, Level.WARNING, "Error on deleting the snapshot shadow file '%s'", e, spillFile);
      }
    }
  }

}
