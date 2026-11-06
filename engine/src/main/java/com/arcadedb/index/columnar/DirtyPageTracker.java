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

import com.arcadedb.utility.IntHashSet;

import java.util.Arrays;
import java.util.BitSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntPredicate;
import java.util.function.LongSupplier;

/**
 * Epoch-based set of the pages of one bucket file that changed and are not yet reflected in the published projection (spec 6.2).
 * A page lives in exactly one of four places, and moves strictly in this order: the current <b>epoch</b> (marked by a commit or a
 * Raft apply, not yet drained), <b>in flight</b> (drained, being applied to the overlay), <b>unflushed</b> (applied to the overlay,
 * not yet in a flushed segment), gone. Every move adds to the destination before removing from the source, so a reader that reads
 * the places in that same order always finds a page in at least one of them.
 * <p>
 * A pending page (in an epoch or in flight) is either <b>whole</b> (any record of it may have changed) or carries the <b>records</b>
 * that changed (Task 15 of #10662): a local commit names the records it wrote, at most {@link ColumnarIndexManager#MAX_SLOT_MARKS_PER_PAGE}
 * per page and epoch, and every other path marks whole pages ({@link #mark}). A page mark, or one more record than the bound, makes the
 * page whole for the rest of its epoch, and a page in flight whole stays whole until its pass completes. The recovery set and the
 * unflushed pages stay page granular.
 * <p>
 * Concurrency: {@link #mark} and {@link #markRecords} are the only calls on the commit path and take no lock: they register as a
 * writer of the current epoch, set bits with atomic ORs and unregister. {@link #drainInto} (apply thread) swaps in a fresh epoch, waits
 * for the writers of the sealed one, and only then reads its bits, so a mark that returned before the drain began is always seen, and
 * a mark racing the swap lands in exactly one of the two epochs (the loser of the race retries into the new one). A mark writes the
 * whole bit or the records of a page before its pending bit, so a reader that sees the pending bit sees what the mark said about the
 * page; a reader that finds a pending page with neither (a drain clearing it under the reader) takes it whole. Everything past the
 * epochs is guarded by {@code movementLock}, which the commit path never takes. {@code drainInto}, {@code completed} and
 * {@code flushed} belong to one logical writer at a time (the apply pass, then the flusher, serialized by the projection); the read
 * methods may be called from any thread.
 * <p>
 * A projection owns one tracker per file it follows: the primary bucket (2048 slots per page) and, when the bucket has a paired
 * external-property bucket, a second one (256 slots per page) whose pages translate to owners only, see
 * {@link #addPendingOwners}. Only the primary tracker receives record marks.
 */
final class DirtyPageTracker {
  /** Pages per chunk of an epoch: 1024 words of 64 bits. */
  private static final int CHUNK_WORDS   = 1024;
  private static final int CHUNK_SHIFT   = 16;
  /** Chunks per middle array, and middle arrays in the top array: 32 x 1024 chunks x 65_536 pages cover 2^31 pages. */
  private static final int MIDDLE_SHIFT  = 10;
  private static final int MIDDLE_SIZE   = 1 << MIDDLE_SHIFT;
  private static final int TOP_SIZE      = 1 << (31 - CHUNK_SHIFT - MIDDLE_SHIFT);
  private static final int PAGE_IN_CHUNK = (1 << CHUNK_SHIFT) - 1;

  /** Records a page can carry before it is whole. */
  private static final int  MAX_RECORDS = ColumnarIndexManager.MAX_SLOT_MARKS_PER_PAGE;
  /** Per page: four words of 16-bit lanes (slot + 1, 0 = free), then the count of lanes taken. */
  private static final int  LANE_WORDS  = (MAX_RECORDS + 3) / 4;
  private static final int  COUNT       = LANE_WORDS;
  private static final int  PAGE_STRIDE = LANE_WORDS + 1;
  /** A lane holds slot + 1 in 16 bits: a page with more slots than that is always marked whole. */
  private static final int  LANE_LIMIT  = 0xFFFF;
  private static final long LANE_MASK   = 0xFFFFL;

  /** The marks of 65_536 pages of an epoch. */
  private static final class Chunk {
    /** Words [0, CHUNK_WORDS): the page is pending; [CHUNK_WORDS, 2 x CHUNK_WORDS): the mark covers the whole page. */
    final AtomicLongArray                       bits  = new AtomicLongArray(2 * CHUNK_WORDS);
    /**
     * Per word of pages, the record lanes of its 64 pages ({@link #PAGE_STRIDE} longs each): CAS-created by the first record mark of
     * one of them and kept when the epoch is recycled, so a record mark allocates nothing in steady state.
     */
    final AtomicReferenceArray<AtomicLongArray> lanes = new AtomicReferenceArray<>(CHUNK_WORDS);
  }

  /** One generation of marks. Recycled by the drain once its bits are moved on, so a drain allocates nothing in steady state. */
  private static final class Epoch {
    /** Lazy three-level directory: top (32 slots) -> middle (1024 slots, CAS-created) -> chunk (CAS-created). */
    final AtomicReferenceArray<AtomicReferenceArray<Chunk>> top     = new AtomicReferenceArray<>(TOP_SIZE);
    final AtomicInteger                                    writers = new AtomicInteger();
    /** Clock of the first mark into the epoch, 0 while the epoch is idle. */
    volatile long                                          startMs;
  }

  /** What a scan learns of one pending page: {@code count} slots in {@code slots}, or -1 when the page is whole. */
  @FunctionalInterface
  private interface PageMarks {
    void page(int page, int[] slots, int count);
  }

  private final int          maxRecordsInPage;
  private final LongSupplier clockMs;
  private final boolean      recordsFit;

  private volatile Epoch current = new Epoch();
  /** The epoch being drained: still readable after {@link #current} moved on and until its pages are in {@link #inFlight}. */
  private volatile Epoch draining;
  private          Epoch spare;

  private final ReentrantLock          movementLock     = new ReentrantLock();
  // guarded by movementLock
  private final BitSet                 inFlight         = new BitSet();
  /** The pages in flight that carry records; the other pages in flight are whole. */
  private final BitSet                 inFlightByRecord = new BitSet();
  /**
   * The record positions of the pages of {@link #inFlightByRecord}, unordered, possibly repeated; a page that turned whole in flight
   * leaves its positions here until it completes, so readers keep only the positions of the pages still in {@code inFlightByRecord}.
   */
  private       long[]                 inFlightRecords  = new long[16];
  private       int                    inFlightRecordCount;
  private final int[]                  drainSlots       = new int[MAX_RECORDS];
  private final PageMarks              moveToInFlight   = this::moveToInFlight;
  private final BitSet                 unflushed        = new BitSet();
  private volatile long                inFlightStartMs;

  DirtyPageTracker(final int maxRecordsInPage, final LongSupplier clockMs) {
    this.maxRecordsInPage = maxRecordsInPage;
    this.clockMs = clockMs;
    this.recordsFit = maxRecordsInPage <= LANE_LIMIT;
  }

  /**
   * Marks the first {@code count} pages dirty, whole. Commit path: no lock, one writer registration per call, two atomic ORs per page.
   */
  void mark(final int[] pages, final int count) {
    if (count == 0)
      return;
    final Epoch epoch = enter();
    try {
      for (int i = 0; i < count; i++) {
        final int page = pages[i];
        final Chunk chunk = chunk(epoch, page);
        final int inChunk = page & PAGE_IN_CHUNK;
        final long bit = 1L << inChunk;
        or(chunk.bits, CHUNK_WORDS + (inChunk >>> 6), bit);
        or(chunk.bits, inChunk >>> 6, bit);
      }
    } finally {
      epoch.writers.decrementAndGet();
    }
  }

  /**
   * Marks the records at the first {@code count} positions dirty, grouped by page (the positions ascending, or at least the ones of a
   * page next to each other). A page whose records would exceed {@link ColumnarIndexManager#MAX_SLOT_MARKS_PER_PAGE} in the epoch is
   * marked whole. Commit path: no lock, one writer registration per call, per record an atomic OR into its page's lanes unless the
   * record is there already, per page one atomic OR; nothing allocated once the page's lane group exists.
   */
  void markRecords(final long[] positions, final int count) {
    if (count == 0)
      return;
    final Epoch epoch = enter();
    try {
      for (int i = 0; i < count; ) {
        final int page = (int) (positions[i] / maxRecordsInPage);
        int end = i + 1;
        while (end < count && positions[end] / maxRecordsInPage == page)
          end++;
        final Chunk chunk = chunk(epoch, page);
        final int inChunk = page & PAGE_IN_CHUNK;
        final long bit = 1L << inChunk;
        // a page already whole in the epoch takes no more records
        if ((chunk.bits.get(CHUNK_WORDS + (inChunk >>> 6)) & bit) == 0
            && (!recordsFit || end - i > MAX_RECORDS || !addRecords(chunk, inChunk, positions, i, end)))
          or(chunk.bits, CHUNK_WORDS + (inChunk >>> 6), bit);
        or(chunk.bits, inChunk >>> 6, bit);
        i = end;
      }
    } finally {
      epoch.writers.decrementAndGet();
    }
  }

  /** Registers as a writer of the current epoch and stamps its start; the caller unregisters. */
  private Epoch enter() {
    Epoch epoch;
    for (;;) {
      epoch = current;
      epoch.writers.incrementAndGet();
      if (epoch == current)
        break;
      epoch.writers.decrementAndGet();
    }
    if (epoch.startMs == 0)
      epoch.startMs = Math.max(1L, clockMs.getAsLong());
    return epoch;
  }

  /** The chunk of the page in the epoch, created on the first mark. */
  private static Chunk chunk(final Epoch epoch, final int page) {
    final int chunkIndex = page >>> CHUNK_SHIFT;
    AtomicReferenceArray<Chunk> middle = epoch.top.get(chunkIndex >>> MIDDLE_SHIFT);
    if (middle == null) {
      final AtomicReferenceArray<Chunk> created = new AtomicReferenceArray<>(MIDDLE_SIZE);
      middle = epoch.top.compareAndExchange(chunkIndex >>> MIDDLE_SHIFT, null, created);
      if (middle == null)
        middle = created;
    }
    Chunk chunk = middle.get(chunkIndex & (MIDDLE_SIZE - 1));
    if (chunk == null) {
      final Chunk created = new Chunk();
      chunk = middle.compareAndExchange(chunkIndex & (MIDDLE_SIZE - 1), null, created);
      if (chunk == null)
        chunk = created;
    }
    return chunk;
  }

  /**
   * Adds the slots of {@code positions[from, to)} to the lanes of the page; false when the page has (or would get) more records than
   * its lanes hold, and must be marked whole. A record already in a lane is not added again (the two marks of a commit name the same
   * records); two writers racing on one record may both add it, which only uses a lane more.
   */
  private boolean addRecords(final Chunk chunk, final int inChunk, final long[] positions, final int from, final int to) {
    AtomicLongArray lanes = chunk.lanes.get(inChunk >>> 6);
    if (lanes == null) {
      final AtomicLongArray created = new AtomicLongArray(64 * PAGE_STRIDE);
      lanes = chunk.lanes.compareAndExchange(inChunk >>> 6, null, created);
      if (lanes == null)
        lanes = created;
    }
    final int base = (inChunk & 63) * PAGE_STRIDE;
    for (int i = from; i < to; i++) {
      final long lane = positions[i] % maxRecordsInPage + 1;
      final long taken = lanes.get(base + COUNT);
      if (taken > MAX_RECORDS)
        return false;
      if (hasLane(lanes, base, (int) taken, lane))
        continue;
      final long index = lanes.getAndIncrement(base + COUNT);
      if (index >= MAX_RECORDS)
        return false;
      or(lanes, base + (int) (index >>> 2), lane << ((index & 3) << 4));
    }
    return true;
  }

  private static boolean hasLane(final AtomicLongArray lanes, final int base, final int taken, final long lane) {
    for (int w = 0; w << 2 < taken; w++) {
      final long word = lanes.get(base + w);
      for (int k = 0; k < 4 && (w << 2) + k < taken; k++)
        if ((word >>> (k << 4) & LANE_MASK) == lane)
          return true;
    }
    return false;
  }

  private static void or(final AtomicLongArray words, final int index, final long bits) {
    for (long old = words.get(index); (old & bits) != bits; old = words.get(index))
      if (words.compareAndSet(index, old, old | bits))
        return;
  }

  /**
   * True when the page is marked in the current epoch: a mark that landed after the last drain (Task 15b). Read by the apply pass, which
   * is the only drainer, so the current epoch cannot be sealed under the read. Lock free, nothing allocated.
   */
  boolean isMarkedSinceDrain(final int page) {
    final int chunkIndex = page >>> CHUNK_SHIFT;
    final AtomicReferenceArray<Chunk> middle = current.top.get(chunkIndex >>> MIDDLE_SHIFT);
    if (middle == null)
      return false;
    final Chunk chunk = middle.get(chunkIndex & (MIDDLE_SIZE - 1));
    if (chunk == null)
      return false;
    final int inChunk = page & PAGE_IN_CHUNK;
    return (chunk.bits.get(inChunk >>> 6) & (1L << inChunk)) != 0;
  }

  /** {@link #drainInto(IntHashSet, PositionBitmap)} for a caller that only needs the pages. */
  int drainInto(final IntHashSet target) {
    return drainInto(target, null);
  }

  /**
   * Seals the current epoch, waits for its writers, moves its pages (with their records) to in flight and adds every page in flight to
   * {@code target} (apply thread); the record positions of the pages in flight that carry records go to {@code records}, so a page of
   * {@code target} without a position in {@code records} is whole. Pages a failed pass left in flight are offered again, so a pass can
   * always be retried. Returns the number of pages offered.
   *
   * @param records null when the caller takes every page whole
   */
  int drainInto(final IntHashSet target, final PositionBitmap records) {
    // the whole swap is under movementLock (the commit path never takes it), so drains cannot interleave on the spare epoch
    movementLock.lock();
    try {
      final Epoch sealed = current;
      // readers that miss the new current epoch find the sealed one here
      draining = sealed;
      Epoch next = spare;
      spare = null;
      if (next == null)
        next = new Epoch();
      current = next;
      while (sealed.writers.get() != 0)
        Thread.onSpinWait();

      final long startMs = sealed.startMs;
      scan(sealed, true, moveToInFlight, drainSlots);
      if (startMs != 0 && !inFlight.isEmpty() && inFlightStartMs == 0)
        inFlightStartMs = startMs;
      draining = null;
      sealed.startMs = 0;
      spare = sealed;

      int offered = 0;
      for (int page = inFlight.nextSetBit(0); page >= 0; page = inFlight.nextSetBit(page + 1)) {
        target.add(page);
        offered++;
      }
      if (records != null)
        for (int i = 0; i < inFlightRecordCount; i++)
          if (inFlightByRecord.get((int) (inFlightRecords[i] / maxRecordsInPage)))
            records.set(inFlightRecords[i]);
      return offered;
    } finally {
      movementLock.unlock();
    }
  }

  /** Drain, under movementLock: a page of the sealed epoch joins in flight; whole wins over records, records of one page add up. */
  private void moveToInFlight(final int page, final int[] slots, final int count) {
    if (count < 0) {
      inFlight.set(page);
      inFlightByRecord.clear(page);
      return;
    }
    if (inFlight.get(page)) {
      if (!inFlightByRecord.get(page))
        return; // in flight whole: stays whole
    } else {
      inFlight.set(page);
      inFlightByRecord.set(page);
    }
    if (inFlightRecordCount + count > inFlightRecords.length)
      inFlightRecords = Arrays.copyOf(inFlightRecords, Math.max(inFlightRecords.length * 2, inFlightRecordCount + count));
    final long base = (long) page * maxRecordsInPage;
    for (int k = 0; k < count; k++)
      inFlightRecords[inFlightRecordCount++] = base + slots[k];
  }

  /** The first {@code count} pages were applied to the overlay: in flight -> unflushed. */
  void completed(final int[] pages, final int count) {
    movementLock.lock();
    try {
      for (int i = 0; i < count; i++)
        unflushed.set(pages[i]);
      for (int i = 0; i < count; i++) {
        inFlight.clear(pages[i]);
        inFlightByRecord.clear(pages[i]);
      }
      // the records of the completed pages leave with them
      int kept = 0;
      for (int i = 0; i < inFlightRecordCount; i++)
        if (inFlightByRecord.get((int) (inFlightRecords[i] / maxRecordsInPage)))
          inFlightRecords[kept++] = inFlightRecords[i];
      inFlightRecordCount = kept;
      if (inFlight.isEmpty())
        inFlightStartMs = 0;
    } finally {
      movementLock.unlock();
    }
  }

  /**
   * A flush was published: removes from unflushed every one of the first {@code count} pages for which {@code stillInLiveOverlay}
   * is false. A page re-applied into the live overlay after the flush froze its rows (R14) is not in the flushed segment with its
   * newer value, so it stays unflushed.
   */
  void flushed(final int[] pages, final int count, final IntPredicate stillInLiveOverlay) {
    movementLock.lock();
    try {
      for (int i = 0; i < count; i++)
        if (!stillInLiveOverlay.test(pages[i]))
          unflushed.clear(pages[i]);
    } finally {
      movementLock.unlock();
    }
  }

  /** The unflushed pages, ascending: what a flush freezing the overlay now covers once it is published. */
  int[] unflushedPages() {
    movementLock.lock();
    try {
      final int[] pages = new int[unflushed.cardinality()];
      int i = 0;
      for (int page = unflushed.nextSetBit(0); page >= 0; page = unflushed.nextSetBit(page + 1))
        pages[i++] = page;
      return pages;
    } finally {
      movementLock.unlock();
    }
  }

  /** The number of pages applied to the overlay and not yet flushed. */
  int unflushedCount() {
    movementLock.lock();
    try {
      return unflushed.cardinality();
    } finally {
      movementLock.unlock();
    }
  }

  /** True when some page was applied to the overlay and not yet flushed. */
  boolean hasUnflushed() {
    movementLock.lock();
    try {
      return !unflushed.isEmpty();
    } finally {
      movementLock.unlock();
    }
  }

  /**
   * Adds epochs, in flight and unflushed pages to {@code out}, read in movement order. Guarantee: every page whose mark returned
   * before the sample started is in the sample, or was flushed by an apply that read it after it became visible. Dropping WAL
   * files on the strength of a sample additionally needs the pending-marks guard of Task 13. Page granular: a page marked by record
   * is in the set whole.
   */
  void collectRecoverySet(final IntHashSet out) {
    final PageMarks add = (page, slots, count) -> out.add(page);
    scanEpochs(add, null);
    movementLock.lock();
    try {
      forEachBit(inFlight, out::add);
      forEachBit(unflushed, out::add);
    } finally {
      movementLock.unlock();
    }
  }

  /**
   * Record positions a reader must not trust the projection for: every slot of every whole pending page and the marked records of the
   * other pending pages (epochs and in flight), plus the owners of every pending page as a content page (placeholder targets and chunk
   * chains whose bytes live there).
   * <p>
   * Precondition: the caller holds the projection's {@code ownersLock} read lock, because {@link ContentOwnerMap} is single
   * writer and not thread safe. Call it before reading the {@code ProjectionState}: pages are read in movement order.
   */
  PositionBitmap pendingPositions(final ContentOwnerMap owners) {
    final PositionBitmap out = new PositionBitmap(maxRecordsInPage);
    final long[][] holder = { new long[16] };
    scanEpochs((page, slots, count) -> {
      if (count < 0)
        out.setPage(page);
      else {
        final long base = (long) page * maxRecordsInPage;
        for (int k = 0; k < count; k++)
          out.set(base + slots[k]);
      }
      holder[0] = addOwners(owners, page, holder[0], out);
    }, new int[MAX_RECORDS]);
    movementLock.lock();
    try {
      for (int page = inFlight.nextSetBit(0); page >= 0; page = inFlight.nextSetBit(page + 1)) {
        if (!inFlightByRecord.get(page))
          out.setPage(page);
        holder[0] = addOwners(owners, page, holder[0], out);
      }
      for (int i = 0; i < inFlightRecordCount; i++)
        if (inFlightByRecord.get((int) (inFlightRecords[i] / maxRecordsInPage)))
          out.set(inFlightRecords[i]);
    } finally {
      movementLock.unlock();
    }
    return out;
  }

  /**
   * External tracker (R7): sets into {@code out} only the owners, which are primary record positions, of the pending pages of the
   * external file. It never sets a whole page: page 5 of the external file has nothing to do with page 5 of the primary.
   * Same precondition as {@link #pendingPositions}.
   */
  void addPendingOwners(final ContentOwnerMap owners, final PositionBitmap out) {
    final long[][] holder = { new long[16] };
    scanEpochs((page, slots, count) -> holder[0] = addOwners(owners, page, holder[0], out), null);
    movementLock.lock();
    try {
      forEachBit(inFlight, page -> holder[0] = addOwners(owners, page, holder[0], out));
    } finally {
      movementLock.unlock();
    }
  }

  /**
   * Start of the oldest epoch not yet applied (undrained or in flight), 0 when nothing is pending. It is an upper bound on the
   * lag: after a partial {@link #completed} it still reports the drained epoch until every in-flight page is completed.
   */
  long oldestPendingEpochStartMs() {
    // movement order: current, then draining, then in flight
    long oldest = current.startMs;
    final Epoch sealed = draining;
    if (sealed != null)
      oldest = older(oldest, sealed.startMs);
    return older(oldest, inFlightStartMs);
  }

  /** True when no page is in an epoch, in flight or unflushed. */
  boolean isEmpty() {
    if (current.startMs != 0)
      return false;
    final Epoch sealed = draining;
    if (sealed != null && sealed.startMs != 0)
      return false;
    movementLock.lock();
    try {
      return inFlight.isEmpty() && unflushed.isEmpty();
    } finally {
      movementLock.unlock();
    }
  }

  /** @param slots null to visit the pages only (every page reported whole) */
  private void scanEpochs(final PageMarks consumer, final int[] slots) {
    scan(current, false, consumer, slots);
    final Epoch sealed = draining;
    if (sealed != null)
      scan(sealed, false, consumer, slots);
  }

  private static long older(final long a, final long b) {
    return a == 0 ? b : b == 0 ? a : Math.min(a, b);
  }

  private static void forEachBit(final BitSet bits, final IntHashSet.IntConsumer consumer) {
    for (int page = bits.nextSetBit(0); page >= 0; page = bits.nextSetBit(page + 1))
      consumer.accept(page);
  }

  private static long[] addOwners(final ContentOwnerMap owners, final int page, long[] buffer, final PositionBitmap out) {
    final int upperBound = owners.ownerCountUpperBound(page);
    if (upperBound == 0)
      return buffer;
    if (buffer.length < upperBound)
      buffer = new long[upperBound];
    final int count = owners.owners(page, buffer);
    for (int i = 0; i < count; i++)
      out.set(buffer[i]);
    return buffer;
  }

  /**
   * Visits the pending pages of the epoch, each with its records unless {@code slots} is null; with {@code clear} the lanes of a page
   * and then each word are reset after their pages were handed to the consumer (the epoch has no writers left).
   */
  private static void scan(final Epoch epoch, final boolean clear, final PageMarks consumer, final int[] slots) {
    for (int t = 0; t < TOP_SIZE; t++) {
      final AtomicReferenceArray<Chunk> middle = epoch.top.get(t);
      if (middle == null)
        continue;
      for (int m = 0; m < MIDDLE_SIZE; m++) {
        final Chunk chunk = middle.get(m);
        if (chunk == null)
          continue;
        final int base = ((t << MIDDLE_SHIFT) + m) << CHUNK_SHIFT;
        for (int word = 0; word < CHUNK_WORDS; word++) {
          long bits = chunk.bits.get(word);
          if (bits == 0)
            continue;
          final long whole = slots == null ? -1L : chunk.bits.get(CHUNK_WORDS + word);
          final AtomicLongArray lanes = slots == null && !clear ? null : chunk.lanes.get(word);
          while (bits != 0) {
            final int bit = Long.numberOfTrailingZeros(bits);
            final int page = base + (word << 6) + bit;
            final int laneBase = bit * PAGE_STRIDE;
            if ((whole & (1L << bit)) != 0 || lanes == null)
              consumer.page(page, slots, -1);
            else
              consumer.page(page, slots, readLanes(lanes, laneBase, slots));
            // a page marked whole may hold lanes of the records marked before: the recycled epoch must start without them
            if (clear && lanes != null)
              for (int k = 0; k < PAGE_STRIDE; k++)
                lanes.set(laneBase + k, 0L);
            bits &= bits - 1;
          }
          if (clear) {
            chunk.bits.set(CHUNK_WORDS + word, 0L);
            chunk.bits.set(word, 0L);
          }
        }
      }
    }
  }

  /**
   * Copies the slots of the page's lanes into {@code slots}: their count, or -1 (whole) when the page has no lane (a mark clearing or
   * escalating under the reader) or more lanes taken than it holds. A lane taken and not written yet belongs to a mark still running.
   */
  private static int readLanes(final AtomicLongArray lanes, final int base, final int[] slots) {
    final long taken = lanes.get(base + COUNT);
    if (taken <= 0 || taken > MAX_RECORDS)
      return -1;
    int count = 0;
    for (int k = 0; k < taken; k++) {
      final long lane = lanes.get(base + (k >>> 2)) >>> ((k & 3) << 4) & LANE_MASK;
      if (lane != 0)
        slots[count++] = (int) lane - 1;
    }
    return count == 0 ? -1 : count;
  }

  /** Number of directory and chunk arrays (a chunk and each of its lane groups count one) the current and spare epochs hold. */
  int allocatedArraysForTesting() {
    int arrays = 0;
    for (final Epoch epoch : new Epoch[] { current, spare }) {
      if (epoch == null)
        continue;
      arrays++;
      for (int t = 0; t < TOP_SIZE; t++) {
        final AtomicReferenceArray<Chunk> middle = epoch.top.get(t);
        if (middle == null)
          continue;
        arrays++;
        for (int m = 0; m < MIDDLE_SIZE; m++) {
          final Chunk chunk = middle.get(m);
          if (chunk == null)
            continue;
          arrays++;
          for (int w = 0; w < CHUNK_WORDS; w++)
            if (chunk.lanes.get(w) != null)
              arrays++;
        }
      }
    }
    return arrays;
  }
}
