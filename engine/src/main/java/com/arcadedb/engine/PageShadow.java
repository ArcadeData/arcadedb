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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
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
 * per shadowed page on the flush hot path; this costs 16 bytes per entry with no allocation at all, so a million
 * shadowed pages is 16 MB of index. Keys pack {@code fileId} and {@code pageNumber} into one {@code long} and are
 * therefore always non-negative, which frees {@code -1} as the empty slot marker.
 * <p>
 * <b>Concurrency.</b> Every public method is serialized on this instance. The capture side is called from inside
 * {@code PageManager.concurrentPageAccess}'s per-page write slot, so two threads never capture the SAME page, but
 * they do capture different pages concurrently and a snapshot reader probes the index in parallel. The monitor is
 * also what publishes a stored pre-image to that reader: {@link #store} copies the bytes and inserts the index entry
 * under it, {@link #read} looks the entry up under it, so a reader that observes the entry is guaranteed to observe
 * the complete bytes. That ordering is what lets {@link PageSnapshot} read runs of pages in bulk with no per-page
 * lock at all, re-checking the shadow afterwards for anything a writer touched underneath it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class PageShadow implements AutoCloseable {
  /** Keys pack two non-negative ints, so they are never negative and -1 is free as the empty-slot marker. */
  private static final long EMPTY_KEY          = -1L;
  private static final int  INITIAL_CAPACITY   = 1024;
  /** A RAM slot index is stored as-is; a spill offset as {@code -(offset + 1)}, so the two never collide. */
  private static final long NOT_FOUND          = Long.MIN_VALUE;

  private final File spillFile;
  private final long maxRAMBytes;
  private final long maxTotalBytes;

  private long[] indexKeys;
  private long[] indexValues;
  private int    indexSize;
  private int    indexMask;
  private int    indexThreshold;

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
    allocateIndex(INITIAL_CAPACITY);
  }

  /** Packs a page coordinate into the primitive index key. Both components are non-negative by construction. */
  static long key(final int fileId, final int pageNumber) {
    return ((long) fileId << 32) | (pageNumber & 0xFFFFFFFFL);
  }

  synchronized boolean contains(final long key) {
    return !closed && lookup(key) != NOT_FOUND;
  }

  /**
   * Stores the pre-image of a page not yet shadowed.
   *
   * @return {@code false} when the configured cap would be breached: the caller must invalidate the whole window
   *     (challenge C4 - a shadow that silently stops capturing would produce a torn snapshot).
   */
  synchronized boolean store(final long key, final byte[] content, final int length) throws IOException {
    if (closed)
      return false;
    if (lookup(key) != NOT_FOUND)
      // ALREADY CAPTURED BY AN EARLIER WRITE TO THE SAME PAGE: THE FIRST PRE-IMAGE IS THE SNAPSHOT ONE, KEEP IT
      return true;

    if (maxTotalBytes > 0 && ramBytes + spillBytes + length > maxTotalBytes)
      return false;

    final long slot;
    if (ramBytes + length <= maxRAMBytes) {
      final byte[] copy = new byte[length];
      System.arraycopy(content, 0, copy, 0, length);
      if (ramCount == ramSlots.length) {
        final byte[][] larger = new byte[ramSlots.length * 2][];
        System.arraycopy(ramSlots, 0, larger, 0, ramCount);
        ramSlots = larger;
      }
      ramSlots[ramCount] = copy;
      slot = ramCount++;
      ramBytes += length;
    } else {
      if (spillChannel == null)
        spillChannel = FileChannel.open(spillFile.toPath(), StandardOpenOption.CREATE_NEW, StandardOpenOption.READ,
            StandardOpenOption.WRITE);

      final ByteBuffer buffer = ByteBuffer.wrap(content, 0, length);
      long pos = spillBytes;
      while (buffer.hasRemaining())
        pos += spillChannel.write(buffer, pos);

      slot = -(spillBytes + 1);
      spillBytes += length;
    }

    insert(key, slot);
    return true;
  }

  /**
   * Copies the pre-image of {@code key} into {@code dst} when the page has been shadowed.
   *
   * @return {@code false} when the page is not in the shadow, in which case the on-disk image is still the snapshot
   *     one and the caller reads it from the data file.
   */
  synchronized boolean read(final long key, final byte[] dst, final int dstOffset, final int length) throws IOException {
    if (closed)
      return false;

    final long slot = lookup(key);
    if (slot == NOT_FOUND)
      return false;

    if (slot >= 0) {
      final byte[] content = ramSlots[(int) slot];
      if (content.length != length)
        throw new IOException(
            "Shadowed page size mismatch: expected " + length + " bytes, the pre-image holds " + content.length);
      System.arraycopy(content, 0, dst, dstOffset, length);
      return true;
    }

    final ByteBuffer buffer = ByteBuffer.wrap(dst, dstOffset, length);
    long pos = -(slot + 1);
    while (buffer.hasRemaining()) {
      final int r = spillChannel.read(buffer, pos);
      if (r < 0)
        throw new IOException("Unexpected EOF reading the snapshot shadow file '" + spillFile.getName() + "' at " + pos);
      pos += r;
    }
    return true;
  }

  /** Number of pages currently shadowed. */
  synchronized int getPageCount() {
    return indexSize;
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
    // RELEASE THE INDEX TOO: EVERY LOOKUP IS GATED ON closed ABOVE, SO NOTHING PROBES THESE ARRAYS AGAIN
    indexKeys = new long[0];
    indexValues = new long[0];
    indexMask = 0;
    indexSize = 0;

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

  // ------------------------------------------------------------------------------------- PRIMITIVE INDEX

  private void allocateIndex(final int capacity) {
    int size = INITIAL_CAPACITY;
    while (size < capacity)
      size <<= 1;
    indexKeys = new long[size];
    indexValues = new long[size];
    Arrays.fill(indexKeys, EMPTY_KEY);
    indexMask = size - 1;
    indexSize = 0;
    // LOAD FACTOR 0.5: LINEAR PROBING DEGRADES SHARPLY PAST THAT, AND THE MEMORY IS TRIVIAL NEXT TO THE PAGE IMAGES
    indexThreshold = size >> 1;
  }

  private long lookup(final long key) {
    int pos = hash(key) & indexMask;
    while (true) {
      final long k = indexKeys[pos];
      if (k == EMPTY_KEY)
        return NOT_FOUND;
      if (k == key)
        return indexValues[pos];
      pos = (pos + 1) & indexMask;
    }
  }

  private void insert(final long key, final long value) {
    if (indexSize >= indexThreshold)
      rehash();

    int pos = hash(key) & indexMask;
    while (true) {
      final long k = indexKeys[pos];
      if (k == EMPTY_KEY) {
        indexKeys[pos] = key;
        indexValues[pos] = value;
        ++indexSize;
        return;
      }
      if (k == key) {
        indexValues[pos] = value;
        return;
      }
      pos = (pos + 1) & indexMask;
    }
  }

  private void rehash() {
    final long[] oldKeys = indexKeys;
    final long[] oldValues = indexValues;
    allocateIndex(oldKeys.length * 2);
    for (int i = 0; i < oldKeys.length; i++)
      if (oldKeys[i] != EMPTY_KEY)
        insert(oldKeys[i], oldValues[i]);
  }

  /**
   * Fibonacci hashing of the packed key. Page numbers are dense and sequential, so the low bits alone would map a
   * whole file onto one contiguous run of slots; multiplying by the golden-ratio constant spreads them.
   */
  private static int hash(final long key) {
    final long h = key * 0x9E3779B97F4A7C15L;
    return (int) (h ^ (h >>> 32)) & 0x7FFFFFFF;
  }
}
