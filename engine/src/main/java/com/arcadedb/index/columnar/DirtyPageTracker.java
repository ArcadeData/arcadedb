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
 * Concurrency: {@link #mark} is the only call on the commit path and takes no lock: it registers as a writer of the current
 * epoch, sets bits with atomic ORs and unregisters. {@link #drainInto} (apply thread) swaps in a fresh epoch, waits for the
 * writers of the sealed one, and only then reads its bits, so a mark that returned before the drain began is always seen, and a
 * mark racing the swap lands in exactly one of the two epochs (the loser of the race retries into the new one). Everything past
 * the epochs is guarded by {@code movementLock}, which the commit path never takes. {@code drainInto}, {@code completed} and
 * {@code flushed} belong to one logical writer at a time (the apply pass, then the flusher, serialized by the projection); the
 * read methods may be called from any thread.
 * <p>
 * A projection owns one tracker per file it follows: the primary bucket (2048 slots per page) and, when the bucket has a paired
 * external-property bucket, a second one (256 slots per page) whose pages translate to owners only, see
 * {@link #addPendingOwners}.
 */
final class DirtyPageTracker {
  /** Pages per chunk of an epoch: 1024 words of 64 bits. */
  private static final int  CHUNK_WORDS      = 1024;
  private static final int  CHUNK_SHIFT      = 16;
  /** Chunks per middle array, and middle arrays in the top array: 32 x 1024 chunks x 65_536 pages cover 2^31 pages. */
  private static final int  MIDDLE_SHIFT     = 10;
  private static final int  MIDDLE_SIZE      = 1 << MIDDLE_SHIFT;
  private static final int  TOP_SIZE         = 1 << (31 - CHUNK_SHIFT - MIDDLE_SHIFT);
  private static final int  PAGE_IN_CHUNK    = (1 << CHUNK_SHIFT) - 1;

  /** One generation of marks. Recycled by the drain once its bits are moved on, so a drain allocates nothing in steady state. */
  private static final class Epoch {
    /** Lazy three-level directory: top (32 slots) -> middle (1024 slots, CAS-created) -> chunk (1024 words, CAS-created). */
    final AtomicReferenceArray<AtomicReferenceArray<AtomicLongArray>> top = new AtomicReferenceArray<>(TOP_SIZE);
    final AtomicInteger                         writers    = new AtomicInteger();
    /** Clock of the first mark into the epoch, 0 while the epoch is idle. */
    volatile long                               startMs;
  }

  private final int          maxRecordsInPage;
  private final LongSupplier clockMs;

  private volatile Epoch current  = new Epoch();
  /** The epoch being drained: still readable after {@link #current} moved on and until its pages are in {@link #inFlight}. */
  private volatile Epoch draining;
  private Epoch          spare;

  private final ReentrantLock movementLock = new ReentrantLock();
  // guarded by movementLock
  private final BitSet        inFlight     = new BitSet();
  private final BitSet        unflushed    = new BitSet();
  private final IntHashSet.IntConsumer moveToInFlight = inFlight::set;
  private volatile long       inFlightStartMs;

  DirtyPageTracker(final int maxRecordsInPage, final LongSupplier clockMs) {
    this.maxRecordsInPage = maxRecordsInPage;
    this.clockMs = clockMs;
  }

  /**
   * Marks the first {@code count} pages dirty. Commit path: no lock, one writer registration per call, one atomic OR per page.
   */
  void mark(final int[] pages, final int count) {
    if (count == 0)
      return;
    Epoch epoch;
    for (;;) {
      epoch = current;
      epoch.writers.incrementAndGet();
      if (epoch == current)
        break;
      epoch.writers.decrementAndGet();
    }
    try {
      if (epoch.startMs == 0)
        epoch.startMs = Math.max(1L, clockMs.getAsLong());
      for (int i = 0; i < count; i++) {
        final int page = pages[i];
        final int chunkIndex = page >>> CHUNK_SHIFT;
        AtomicReferenceArray<AtomicLongArray> middle = epoch.top.get(chunkIndex >>> MIDDLE_SHIFT);
        if (middle == null) {
          final AtomicReferenceArray<AtomicLongArray> created = new AtomicReferenceArray<>(MIDDLE_SIZE);
          middle = epoch.top.compareAndExchange(chunkIndex >>> MIDDLE_SHIFT, null, created);
          if (middle == null)
            middle = created;
        }
        AtomicLongArray chunk = middle.get(chunkIndex & (MIDDLE_SIZE - 1));
        if (chunk == null) {
          final AtomicLongArray created = new AtomicLongArray(CHUNK_WORDS);
          chunk = middle.compareAndExchange(chunkIndex & (MIDDLE_SIZE - 1), null, created);
          if (chunk == null)
            chunk = created;
        }
        final int inChunk = page & PAGE_IN_CHUNK;
        final int word = inChunk >>> 6;
        final long bit = 1L << inChunk;
        for (long old = chunk.get(word); (old & bit) == 0; old = chunk.get(word))
          if (chunk.compareAndSet(word, old, old | bit))
            break;
      }
    } finally {
      epoch.writers.decrementAndGet();
    }
  }

  /**
   * Seals the current epoch, waits for its writers, moves its pages to in flight and adds every page in flight to {@code target}
   * (apply thread). Pages a failed pass left in flight are offered again, so a pass can always be retried. Returns the number of
   * pages offered.
   */
  int drainInto(final IntHashSet target) {
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
      scan(sealed, true, moveToInFlight);
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
      return offered;
    } finally {
      movementLock.unlock();
    }
  }

  /** The first {@code count} pages were applied to the overlay: in flight -> unflushed. */
  void completed(final int[] pages, final int count) {
    movementLock.lock();
    try {
      for (int i = 0; i < count; i++)
        unflushed.set(pages[i]);
      for (int i = 0; i < count; i++)
        inFlight.clear(pages[i]);
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
   * files on the strength of a sample additionally needs the pending-marks guard of Task 13.
   */
  void collectRecoverySet(final IntHashSet out) {
    scanEpochs(out::add);
    movementLock.lock();
    try {
      forEachBit(inFlight, out::add);
      forEachBit(unflushed, out::add);
    } finally {
      movementLock.unlock();
    }
  }

  /**
   * Record positions a reader must not trust the projection for: every slot of every pending page (epochs and in flight), plus
   * the owners of those pages as content pages (placeholder targets and chunk chains whose bytes live there).
   * <p>
   * Precondition: the caller holds the projection's {@code ownersLock} read lock, because {@link ContentOwnerMap} is single
   * writer and not thread safe. Call it before reading the {@code ProjectionState}: pages are read in movement order.
   */
  PositionBitmap pendingPositions(final ContentOwnerMap owners) {
    final PositionBitmap out = new PositionBitmap(maxRecordsInPage);
    long[] buffer = new long[16];
    final long[][] holder = { buffer };
    forEachPending(page -> {
      out.setPage(page);
      holder[0] = addOwners(owners, page, holder[0], out);
    });
    return out;
  }

  /**
   * External tracker (R7): sets into {@code out} only the owners, which are primary record positions, of the pending pages of the
   * external file. It never sets a whole page: page 5 of the external file has nothing to do with page 5 of the primary.
   * Same precondition as {@link #pendingPositions}.
   */
  void addPendingOwners(final ContentOwnerMap owners, final PositionBitmap out) {
    final long[][] holder = { new long[16] };
    forEachPending(page -> holder[0] = addOwners(owners, page, holder[0], out));
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

  private void forEachPending(final IntHashSet.IntConsumer consumer) {
    scanEpochs(consumer);
    movementLock.lock();
    try {
      forEachBit(inFlight, consumer);
    } finally {
      movementLock.unlock();
    }
  }

  private void scanEpochs(final IntHashSet.IntConsumer consumer) {
    scan(current, false, consumer);
    final Epoch sealed = draining;
    if (sealed != null)
      scan(sealed, false, consumer);
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
   * Visits the pages of the epoch; with {@code clear} each word is reset after its pages were handed to the consumer (the epoch
   * has no writers left).
   */
  private static void scan(final Epoch epoch, final boolean clear, final IntHashSet.IntConsumer consumer) {
    for (int t = 0; t < TOP_SIZE; t++) {
      final AtomicReferenceArray<AtomicLongArray> middle = epoch.top.get(t);
      if (middle == null)
        continue;
      for (int m = 0; m < MIDDLE_SIZE; m++) {
        final AtomicLongArray chunk = middle.get(m);
        if (chunk == null)
          continue;
        final int base = ((t << MIDDLE_SHIFT) + m) << CHUNK_SHIFT;
        for (int word = 0; word < CHUNK_WORDS; word++) {
          long bits = chunk.get(word);
          if (bits == 0)
            continue;
          while (bits != 0) {
            final int bit = Long.numberOfTrailingZeros(bits);
            consumer.accept(base + (word << 6) + bit);
            bits &= bits - 1;
          }
          if (clear)
            chunk.set(word, 0L);
        }
      }
    }
  }

  /** Number of directory and chunk arrays the current and spare epochs hold. */
  int allocatedArraysForTesting() {
    int arrays = 0;
    for (final Epoch epoch : new Epoch[] { current, spare }) {
      if (epoch == null)
        continue;
      arrays++;
      for (int t = 0; t < TOP_SIZE; t++) {
        final AtomicReferenceArray<AtomicLongArray> middle = epoch.top.get(t);
        if (middle == null)
          continue;
        arrays++;
        for (int m = 0; m < MIDDLE_SIZE; m++)
          if (middle.get(m) != null)
            arrays++;
      }
    }
    return arrays;
  }
}
