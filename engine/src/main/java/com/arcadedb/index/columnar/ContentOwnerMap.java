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
import com.arcadedb.utility.LongLongHashMap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

/**
 * Reverse map from a content page to the positions of the records that own content on it: the content record behind a placeholder,
 * the continuation chunks of a multi-page record, or (for the paired map of an external-property bucket, R7) the external record of
 * a primary record. Owner positions are positions of the bucket the projection belongs to. An apply pass that finds a content page
 * dirty re-reads its owners.
 * <p>
 * Primitive storage, no boxing: an open-addressing table of content pages ({@code keys}, -1 empty) pointing to the head of a doubly
 * linked owner list in a pool of 3-long nodes {@code [owner][page << 32 | next node of the owner][previous << 32 | next node on the
 * page]}, and a {@link LongLongHashMap} from an owner to the first node of its own chain, so its old pages are unlinked without a
 * scan. An emptied page keeps its slot, and a removed owner its entry (-1), until the next rehash. Growth doubles.
 * <p>
 * Single writer, not thread safe: the apply thread owns it; anything read by another thread is a {@link #copy()}.
 * <p>
 * File format, one {@link BlockHeader} block, big-endian: OWNER_MAGIC, version 1, row count = entries n, 2 columns:
 * {@code [n owners long][n pages int]}, entries strictly ascending by (owner, page). Every malformed file surfaces as
 * {@link BlockCorruptionException}.
 */
public final class ContentOwnerMap {
  static final int OWNER_MAGIC = 0x41434f4d; // "ACOM"
  static final int VERSION     = 1;

  private static final int   COLUMNS       = 2;
  private static final int   NODE_LONGS    = 3;
  private static final int   NONE          = -1;
  private static final long  REMOVED       = -1L;
  private static final long  ABSENT        = -2L;
  private static final int   MIN_CAPACITY  = 16;
  private static final float LOAD_FACTOR   = 0.75f;
  private static final int   COMPACT_FLOOR = 1024;

  private int[]           keys;
  private int[]           head;
  private int[]           counts;
  private int             mask;
  private int             threshold;
  private int             usedSlots;
  private int             emptiedSlots;
  private long[]          pool;
  private int             nodeHighWater;
  private int             freeNode = NONE;
  private int             liveNodes;
  private long            modifications;
  private LongLongHashMap ownerToFirstPageNode;
  private int             removedOwners;

  public ContentOwnerMap() {
    this(MIN_CAPACITY, MIN_CAPACITY, MIN_CAPACITY);
  }

  private ContentOwnerMap(final int pageCapacity, final int nodeCapacity, final int ownerCapacity) {
    allocateTable(pageCapacity);
    pool = new long[NODE_LONGS * Math.max(MIN_CAPACITY, nodeCapacity)];
    ownerToFirstPageNode = new LongLongHashMap(ownerCapacity);
  }

  /**
   * Replaces the owner's content pages with the first {@code count} entries of {@code contentPages} (duplicates count once); a count
   * of 0 is {@link #removeOwner}.
   *
   * @throws IllegalArgumentException for a negative owner or page, or a count outside {@code contentPages}
   */
  public void setOwnerPages(final long owner, final int[] contentPages, final int count) {
    modifications++;
    if (owner < 0)
      throw new IllegalArgumentException("Invalid owner position " + owner);
    if (count < 0 || count > contentPages.length)
      throw new IllegalArgumentException("Invalid count " + count + " of " + contentPages.length + " pages");
    for (int i = 0; i < count; i++)
      if (contentPages[i] < 0)
        throw new IllegalArgumentException("Invalid content page " + contentPages[i]);

    final long previous = ownerToFirstPageNode.get(owner, ABSENT);
    if (previous >= 0)
      unlinkChain((int) previous);
    int chain = NONE;
    for (int i = 0; i < count; i++)
      if (!chainHolds(chain, contentPages[i]))
        chain = link(owner, contentPages[i], chain);

    if (chain != NONE) {
      ownerToFirstPageNode.put(owner, chain);
      if (previous == REMOVED)
        removedOwners--;
    } else if (previous >= 0)
      markRemoved(owner);
  }

  /** Drops every content page of the owner; nothing happens for an unknown owner. */
  public void removeOwner(final long owner) {
    modifications++;
    final long first = ownerToFirstPageNode.get(owner, ABSENT);
    if (first < 0)
      return;
    unlinkChain((int) first);
    markRemoved(owner);
  }

  /**
   * Copies the owners of the content page into {@code out}, in no particular order, and returns their count (0 for a page nobody owns);
   * at most {@code out.length} are copied, so size it with {@link #ownerCountUpperBound}.
   */
  public int owners(final int contentPage, final long[] out) {
    final int slot = slotOf(contentPage);
    if (slot < 0)
      return 0;
    int count = 0;
    for (int node = head[slot]; node != NONE; node = nextOnPage(node)) {
      if (count < out.length)
        out[count] = pool[NODE_LONGS * node];
      count++;
    }
    return count;
  }

  /** Number of {@link #setOwnerPages} and {@link #removeOwner} calls on this instance, so a flush can tell the map is unchanged. */
  long modifications() {
    return modifications;
  }

  /** True when no owner has a content page. */
  public boolean isEmpty() {
    return liveNodes == 0;
  }

  /** The number of owners of the content page (exact, so an upper bound for {@link #owners}), 0 for a page nobody owns. */
  public int ownerCountUpperBound(final int contentPage) {
    final int slot = slotOf(contentPage);
    return slot < 0 ? 0 : counts[slot];
  }

  /**
   * Writes and fsyncs a new owner map file. The caller fsyncs the directory before publishing a manifest that names it.
   *
   * @throws java.nio.file.FileAlreadyExistsException when the file exists: an owner map file is written once
   */
  public void writeTo(final Path file) throws IOException {
    final long[] owners = liveOwnersSorted();
    final int headerSize = BlockHeader.FIXED_BYTES + 4 * COLUMNS;
    final long total = headerSize + 12L * liveNodes;
    if (total > Integer.MAX_VALUE)
      throw new IllegalArgumentException("Owner map of " + liveNodes + " entries does not fit one file block");

    final ByteBuffer buffer = ByteBuffer.allocate((int) total);
    final int ownersOffset = headerSize;
    final int pagesOffset = headerSize + 8 * liveNodes;
    int[] pages = new int[16];
    int entry = 0;
    for (final long owner : owners) {
      int count = 0;
      for (int node = (int) ownerToFirstPageNode.get(owner, ABSENT); node != NONE; node = nextOfOwner(node)) {
        if (count == pages.length)
          pages = Arrays.copyOf(pages, count * 2);
        pages[count++] = pageOf(node);
      }
      Arrays.sort(pages, 0, count);
      for (int i = 0; i < count; i++, entry++) {
        buffer.putLong(ownersOffset + 8 * entry, owner);
        buffer.putInt(pagesOffset + 4 * entry, pages[i]);
      }
    }
    final int payloadCrc = BlockHeader.crc32(buffer, headerSize, (int) total - headerSize);
    buffer.position(0);
    new BlockHeader(OWNER_MAGIC, VERSION, liveNodes, new int[] { 8 * liveNodes, 4 * liveNodes }, payloadCrc).write(buffer);
    buffer.position(0);

    // CREATE_NEW refuses an existing file before anything is written; a partial file this call created is removed
    final FileChannel channel = FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
    try (channel) {
      FileUtils.writeFully(channel, buffer, 0);
      channel.force(true);
    } catch (final IOException | RuntimeException e) {
      Files.deleteIfExists(file);
      throw e;
    }
  }

  /** @throws BlockCorruptionException when the file fails a CRC or format check */
  public static ContentOwnerMap readFrom(final Path file) throws IOException {
    try (final FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
      final long size = channel.size();
      if (size < BlockHeader.FIXED_BYTES + 4 * COLUMNS || size > Integer.MAX_VALUE)
        throw new BlockCorruptionException("Owner map " + file + " has an invalid size of " + size + " bytes");
      // sized from the real file length, never from bytes read
      final ByteBuffer buffer = ByteBuffer.allocate((int) size);
      if (FileUtils.readFully(channel, buffer, 0) != size)
        throw new BlockCorruptionException("Owner map " + file + " is truncated");
      buffer.flip();
      return decode(file, buffer);
    }
  }

  private static ContentOwnerMap decode(final Path file, final ByteBuffer buffer) {
    final BlockHeader header = BlockHeader.read(buffer, OWNER_MAGIC);
    if (header.version() != VERSION)
      throw new BlockCorruptionException("Owner map " + file + " has unsupported version " + header.version());
    if (header.columnCount() != COLUMNS || header.totalSize() != buffer.limit())
      throw new BlockCorruptionException("Owner map " + file + " has " + header.columnCount() + " columns and " + header.totalSize()
          + " bytes, expected " + COLUMNS + " columns and " + buffer.limit() + " bytes");
    final int n = header.rowCount();
    if (n < 0 || header.columnSize(0) != 8L * n || header.columnSize(1) != 4L * n)
      throw new BlockCorruptionException("Owner map " + file + " column sizes do not match its " + n + " entries");
    header.verifyPayload(buffer, 0);

    // n is bounded by the verified file length
    final ContentOwnerMap map = new ContentOwnerMap(n, n, n);
    long previousOwner = -1;
    int previousPage = -1;
    int chain = NONE;
    for (int i = 0; i < n; i++) {
      final long owner = buffer.getLong(header.columnOffset(0) + 8 * i);
      final int page = buffer.getInt(header.columnOffset(1) + 4 * i);
      if (owner < 0 || page < 0 || owner < previousOwner || owner == previousOwner && page <= previousPage)
        throw new BlockCorruptionException("Owner map " + file + " entry " + i + " (" + owner + ", " + page + ") is out of order");
      if (owner != previousOwner) {
        if (chain != NONE)
          map.ownerToFirstPageNode.put(previousOwner, chain);
        chain = NONE;
      }
      chain = map.link(owner, page, chain);
      previousOwner = owner;
      previousPage = page;
    }
    if (chain != NONE)
      map.ownerToFirstPageNode.put(previousOwner, chain);
    return map;
  }

  /** An independent deep copy, for publication with a manifest; removed owners and emptied pages are not carried over. */
  public ContentOwnerMap copy() {
    final ContentOwnerMap copy = new ContentOwnerMap(MIN_CAPACITY, MIN_CAPACITY, MIN_CAPACITY);
    copy.keys = keys.clone();
    copy.head = head.clone();
    copy.counts = counts.clone();
    copy.mask = mask;
    copy.threshold = threshold;
    copy.usedSlots = usedSlots;
    copy.emptiedSlots = emptiedSlots;
    copy.pool = pool.clone();
    copy.nodeHighWater = nodeHighWater;
    copy.freeNode = freeNode;
    copy.liveNodes = liveNodes;
    copy.ownerToFirstPageNode = liveOwnerIndex();
    return copy;
  }

  /** Bytes of every array the map holds, summed from their lengths. */
  long heapBytesForTesting() {
    return 4L * (keys.length + head.length + counts.length) + 8L * pool.length + 16L * ownerToFirstPageNode.capacity();
  }

  // ---- owner chains

  private boolean chainHolds(final int chain, final int page) {
    for (int node = chain; node != NONE; node = nextOfOwner(node))
      if (pageOf(node) == page)
        return true;
    return false;
  }

  /** Adds a node (owner, page) in front of the owner's chain and of the page's list; returns the node, the chain's new first. */
  private int link(final long owner, final int page, final int chain) {
    final int node = allocateNode();
    final int base = NODE_LONGS * node;
    final int slot = slotOrInsert(page);
    final int first = head[slot];
    pool[base] = owner;
    pool[base + 1] = pack(page, chain);
    pool[base + 2] = pack(NONE, first);
    if (first != NONE)
      setPreviousOnPage(first, node);
    head[slot] = node;
    counts[slot]++;
    return node;
  }

  private void unlinkChain(final int first) {
    int node = first;
    while (node != NONE) {
      final int next = nextOfOwner(node);
      final int previousOnPage = previousOnPage(node);
      final int nextOnPage = nextOnPage(node);
      final int slot = slotOf(pageOf(node));
      if (previousOnPage == NONE)
        head[slot] = nextOnPage;
      else
        setNextOnPage(previousOnPage, nextOnPage);
      if (nextOnPage != NONE)
        setPreviousOnPage(nextOnPage, previousOnPage);
      if (--counts[slot] == 0)
        emptiedSlots++;
      releaseNode(node);
      node = next;
    }
  }

  private void markRemoved(final long owner) {
    ownerToFirstPageNode.put(owner, REMOVED);
    removedOwners++;
    if (removedOwners > COMPACT_FLOOR && removedOwners > ownerToFirstPageNode.size() - removedOwners) {
      ownerToFirstPageNode = liveOwnerIndex();
      removedOwners = 0;
    }
  }

  /** A new owner index holding only the live owners of this one. */
  private LongLongHashMap liveOwnerIndex() {
    final LongLongHashMap live = new LongLongHashMap(Math.max(MIN_CAPACITY, 2 * (ownerToFirstPageNode.size() - removedOwners)));
    ownerToFirstPageNode.forEach((owner, first) -> {
      if (first >= 0)
        live.put(owner, first);
    });
    return live;
  }

  private long[] liveOwnersSorted() {
    final long[] owners = new long[ownerToFirstPageNode.size() - removedOwners];
    final int[] count = { 0 };
    ownerToFirstPageNode.forEach((owner, first) -> {
      if (first >= 0)
        owners[count[0]++] = owner;
    });
    Arrays.sort(owners);
    return owners;
  }

  // ---- node pool

  private int allocateNode() {
    final int node;
    if (freeNode != NONE) {
      node = freeNode;
      freeNode = nextOfOwner(node);
    } else {
      if (NODE_LONGS * (nodeHighWater + 1) > pool.length)
        pool = Arrays.copyOf(pool, pool.length * 2);
      node = nodeHighWater++;
    }
    liveNodes++;
    return node;
  }

  private void releaseNode(final int node) {
    pool[NODE_LONGS * node + 1] = pack(NONE, freeNode);
    freeNode = node;
    liveNodes--;
  }

  private int pageOf(final int node) {
    return (int) (pool[NODE_LONGS * node + 1] >>> 32);
  }

  private int nextOfOwner(final int node) {
    return (int) pool[NODE_LONGS * node + 1];
  }

  private int previousOnPage(final int node) {
    return (int) (pool[NODE_LONGS * node + 2] >>> 32);
  }

  private int nextOnPage(final int node) {
    return (int) pool[NODE_LONGS * node + 2];
  }

  private void setPreviousOnPage(final int node, final int previous) {
    pool[NODE_LONGS * node + 2] = pack(previous, nextOnPage(node));
  }

  private void setNextOnPage(final int node, final int next) {
    pool[NODE_LONGS * node + 2] = pack(previousOnPage(node), next);
  }

  private static long pack(final int high, final int low) {
    return (long) high << 32 | (low & 0xFFFFFFFFL);
  }

  // ---- content page table

  private void allocateTable(final int expected) {
    int capacity = MIN_CAPACITY;
    while (capacity * LOAD_FACTOR <= expected)
      capacity <<= 1;
    keys = new int[capacity];
    Arrays.fill(keys, NONE);
    head = new int[capacity];
    counts = new int[capacity];
    mask = capacity - 1;
    threshold = (int) (capacity * LOAD_FACTOR);
    usedSlots = 0;
    emptiedSlots = 0;
  }

  private static int hash(final int page) {
    final int h = page * 0x9E3779B9;
    return h ^ (h >>> 16);
  }

  private int slotOf(final int page) {
    for (int slot = hash(page) & mask; ; slot = (slot + 1) & mask) {
      final int key = keys[slot];
      if (key == page)
        return slot;
      if (key == NONE)
        return -1;
    }
  }

  private int slotOrInsert(final int page) {
    final int found = slotOf(page);
    if (found >= 0) {
      if (counts[found] == 0)
        emptiedSlots--;
      return found;
    }
    if (usedSlots + 1 > threshold)
      // mostly emptied pages: rehash in place; otherwise double
      rehash(emptiedSlots >= usedSlots / 2 ? keys.length : keys.length * 2);
    int slot = hash(page) & mask;
    while (keys[slot] != NONE)
      slot = (slot + 1) & mask;
    keys[slot] = page;
    head[slot] = NONE;
    counts[slot] = 0;
    usedSlots++;
    return slot;
  }

  /** Rebuilds the table at the capacity, dropping emptied pages. */
  private void rehash(final int capacity) {
    final int[] oldKeys = keys;
    final int[] oldHead = head;
    final int[] oldCounts = counts;
    keys = new int[capacity];
    Arrays.fill(keys, NONE);
    head = new int[capacity];
    counts = new int[capacity];
    mask = capacity - 1;
    threshold = (int) (capacity * LOAD_FACTOR);
    usedSlots = 0;
    emptiedSlots = 0;
    for (int i = 0; i < oldKeys.length; i++) {
      if (oldKeys[i] == NONE || oldCounts[i] == 0)
        continue;
      int slot = hash(oldKeys[i]) & mask;
      while (keys[slot] != NONE)
        slot = (slot + 1) & mask;
      keys[slot] = oldKeys[i];
      head[slot] = oldHead[i];
      counts[slot] = oldCounts[i];
      usedSlots++;
    }
  }
}
