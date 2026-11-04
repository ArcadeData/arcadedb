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
import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class DirtyPageTrackerTest {
  private static final int PAGE_SLOTS = 2048;

  private static int[] sorted(final IntHashSet set) {
    final int[] values = set.toArray();
    Arrays.sort(values);
    return values;
  }

  private static DirtyPageTracker tracker() {
    return new DirtyPageTracker(PAGE_SLOTS, System::currentTimeMillis);
  }

  @Test
  void markThenDrainReturnsEachPageOnce() {
    final DirtyPageTracker tracker = tracker();
    tracker.mark(new int[] { 3, 70_000, 3, 5, 200_000 }, 5);
    tracker.mark(new int[] { 5, 9 }, 1); // count bounds the array: 9 is not marked

    final IntHashSet drained = new IntHashSet();
    assertThat(tracker.drainInto(drained)).isEqualTo(4);
    assertThat(sorted(drained)).containsExactly(3, 5, 70_000, 200_000);

    // nothing marked since: a completed pass leaves nothing to drain
    tracker.completed(new int[] { 3, 5, 70_000, 200_000 }, 4);
    final IntHashSet again = new IntHashSet();
    assertThat(tracker.drainInto(again)).isZero();
    assertThat(again.isEmpty()).isTrue();
  }

  @Test
  void reMarkAfterDrainIsPickedUpNextPass() {
    final DirtyPageTracker tracker = tracker();
    tracker.mark(new int[] { 5 }, 1);
    final IntHashSet first = new IntHashSet();
    tracker.drainInto(first);
    assertThat(sorted(first)).containsExactly(5);
    tracker.completed(new int[] { 5 }, 1);

    tracker.mark(new int[] { 5 }, 1);
    final IntHashSet second = new IntHashSet();
    assertThat(tracker.drainInto(second)).isEqualTo(1);
    assertThat(sorted(second)).containsExactly(5);
  }

  @Test
  void pagesLeftInFlightByAFailedPassAreOfferedAgain() {
    final DirtyPageTracker tracker = tracker();
    tracker.mark(new int[] { 5, 6 }, 2);
    tracker.drainInto(new IntHashSet());
    // the pass failed: no completed(). The next pass must retry them.
    tracker.mark(new int[] { 7 }, 1);
    final IntHashSet retry = new IntHashSet();
    assertThat(tracker.drainInto(retry)).isEqualTo(3);
    assertThat(sorted(retry)).containsExactly(5, 6, 7);
  }

  @Test
  @Timeout(120)
  void markDuringDrainIsNeverLost() throws Exception {
    final int threads = 8;
    final int marksPerThread = 100_000;
    final int pageRange = 5_000;
    final DirtyPageTracker tracker = tracker();
    final boolean[][] marked = new boolean[threads][pageRange];
    final boolean[] drainedSeen = new boolean[pageRange];
    final CountDownLatch start = new CountDownLatch(1);
    final AtomicBoolean writing = new AtomicBoolean(true);
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    final Thread[] writers = new Thread[threads];
    final CountDownLatch writersDone = new CountDownLatch(threads);
    for (int t = 0; t < threads; t++) {
      final int index = t;
      writers[t] = new Thread(() -> {
        try {
          final Random random = new Random(index);
          final int[] one = new int[1];
          start.await();
          for (int i = 0; i < marksPerThread; i++) {
            one[0] = random.nextInt(pageRange);
            marked[index][one[0]] = true;
            tracker.mark(one, 1);
          }
        } catch (final Throwable e) {
          failure.set(e);
        } finally {
          writersDone.countDown();
        }
      });
      writers[t].start();
    }

    final Thread applier = new Thread(() -> {
      try {
        start.await();
        while (writing.get()) {
          drainOnce(tracker, drainedSeen);
        }
      } catch (final Throwable e) {
        failure.set(e);
      }
    });
    applier.start();
    start.countDown();
    writersDone.await();
    writing.set(false);
    applier.join();
    // every mark has returned: one last drain must collect whatever the loop did not
    drainOnce(tracker, drainedSeen);

    assertThat(failure.get()).isNull();
    for (int page = 0; page < pageRange; page++) {
      boolean expected = false;
      for (int t = 0; t < threads; t++)
        expected |= marked[t][page];
      assertThat(drainedSeen[page]).as("page %d", page).isEqualTo(expected);
    }
  }

  private static void drainOnce(final DirtyPageTracker tracker, final boolean[] seen) {
    final IntHashSet target = new IntHashSet();
    tracker.drainInto(target);
    final int[] pages = target.toArray();
    for (final int page : pages)
      seen[page] = true;
    tracker.completed(pages, pages.length);
  }

  @Test
  @Timeout(60)
  void drainWaitsForAMarkInsideItsCriticalSection() throws Exception {
    // The clock runs inside mark(), after the writer is registered and before the bit is set: blocking it holds a writer in the
    // critical section of the epoch the drain is about to seal.
    final CountDownLatch inCriticalSection = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final DirtyPageTracker tracker = new DirtyPageTracker(PAGE_SLOTS, () -> {
      inCriticalSection.countDown();
      try {
        release.await();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return 1000L;
    });

    final Thread marker = new Thread(() -> tracker.mark(new int[] { 42 }, 1));
    marker.start();
    assertThat(inCriticalSection.await(10, TimeUnit.SECONDS)).isTrue();

    final IntHashSet drained = new IntHashSet();
    final AtomicBoolean drainReturned = new AtomicBoolean();
    final Thread drainer = new Thread(() -> {
      tracker.drainInto(drained);
      drainReturned.set(true);
    });
    drainer.start();
    Thread.sleep(200);
    assertThat(drainReturned.get()).as("drain must wait for the writer inside the sealed epoch").isFalse();

    release.countDown();
    drainer.join();
    marker.join();
    assertThat(sorted(drained)).containsExactly(42);
  }

  @Test
  void aMarkRacingTheSwapIsNeverLost() throws Exception {
    // a marker that read the old epoch must notice the swap and retry into the new one: marks interleaved with many drains are
    // counted exactly once in total (a retry that marks both would still be one page; a lost mark would be missing)
    final DirtyPageTracker tracker = tracker();
    final int rounds = 20_000;
    final boolean[] seen = new boolean[rounds];
    final CountDownLatch done = new CountDownLatch(1);
    final Thread marker = new Thread(() -> {
      final int[] one = new int[1];
      for (int i = 0; i < rounds; i++) {
        one[0] = i;
        tracker.mark(one, 1);
      }
      done.countDown();
    });
    marker.start();
    while (done.getCount() > 0)
      drainOnce(tracker, seen);
    marker.join();
    drainOnce(tracker, seen);
    for (int i = 0; i < rounds; i++)
      assertThat(seen[i]).as("page %d", i).isTrue();
  }

  @Test
  @Timeout(60)
  void recoverySetCoversEverySetAPageIsIn() throws Exception {
    final DirtyPageTracker tracker = tracker();
    tracker.mark(new int[] { 1 }, 1);
    final IntHashSet drainedA = new IntHashSet();
    tracker.drainInto(drainedA); // 1 in flight
    tracker.mark(new int[] { 2 }, 1); // 2 in the epoch
    tracker.completed(new int[] { 1 }, 1); // 1 unflushed
    tracker.mark(new int[] { 3 }, 1);
    tracker.drainInto(new IntHashSet()); // 2, 3 in flight

    final IntHashSet recovery = new IntHashSet();
    tracker.collectRecoverySet(recovery);
    assertThat(sorted(recovery)).containsExactly(1, 2, 3);

    // pages keep moving epoch -> in flight -> unflushed while the recovery set is sampled: every page whose mark had returned
    // before a sample began is in that sample (a page only ever moves forward, and the sample reads in the same order)
    final int pageCount = 20_000;
    final DirtyPageTracker moving = tracker();
    final AtomicInteger markedCount = new AtomicInteger();
    final AtomicBoolean moverDone = new AtomicBoolean();
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread mover = new Thread(() -> {
      try {
        final int[] one = new int[1];
        final IntHashSet sink = new IntHashSet();
        for (int page = 0; page < pageCount; page++) {
          one[0] = page;
          moving.mark(one, 1);
          markedCount.set(page + 1);
          if (page % 3 == 0) {
            sink.clear();
            moving.drainInto(sink);
            final int[] drained = sink.toArray();
            moving.completed(drained, drained.length);
          }
        }
      } catch (final Throwable e) {
        failure.set(e);
      } finally {
        moverDone.set(true);
      }
    });
    mover.start();
    final IntHashSet sample = new IntHashSet();
    int samples = 0;
    while (!moverDone.get() || samples < 100) {
      final int expected = markedCount.get();
      sample.clear();
      moving.collectRecoverySet(sample);
      for (int page = 0; page < expected; page++)
        if (!sample.contains(page)) {
          mover.join();
          throw new AssertionError("page " + page + " absent from sample " + samples + " (" + expected + " marked)");
        }
      samples++;
    }
    mover.join();
    assertThat(failure.get()).isNull();
  }

  @Test
  void lagIsOldestUndrainedEpochStart() {
    final AtomicLong clock = new AtomicLong();
    final DirtyPageTracker tracker = new DirtyPageTracker(PAGE_SLOTS, clock::get);

    clock.set(1000);
    tracker.mark(new int[] { 1 }, 1);
    clock.set(1500);
    tracker.drainInto(new IntHashSet());
    clock.set(1700);
    tracker.mark(new int[] { 2 }, 1);
    assertThat(tracker.oldestPendingEpochStartMs()).isEqualTo(1000);

    tracker.completed(new int[] { 1 }, 1);
    assertThat(tracker.oldestPendingEpochStartMs()).isEqualTo(1700);

    tracker.drainInto(new IntHashSet());
    tracker.completed(new int[] { 2 }, 1);
    assertThat(tracker.oldestPendingEpochStartMs()).isZero();
  }

  @Test
  void directoryIsLazy() {
    final DirtyPageTracker tracker = tracker();
    assertThat(tracker.allocatedArraysForTesting()).isEqualTo(1); // the top array of the current epoch only
    tracker.mark(new int[] { 0, 1, 65_535 }, 3); // one top slot, one chunk
    assertThat(tracker.allocatedArraysForTesting()).isEqualTo(3);
    tracker.mark(new int[] { 2_000_000_000 }, 1); // far away: another middle array and chunk, nothing in between
    assertThat(tracker.allocatedArraysForTesting()).isEqualTo(5);
  }

  @Test
  void idleEpochReportsNoLag() {
    final DirtyPageTracker tracker = new DirtyPageTracker(PAGE_SLOTS, () -> 5000L);
    assertThat(tracker.oldestPendingEpochStartMs()).isZero();
    assertThat(tracker.drainInto(new IntHashSet())).isZero();
    assertThat(tracker.oldestPendingEpochStartMs()).isZero();
    assertThat(tracker.isEmpty()).isTrue();
  }

  @Test
  void isEmptyCoversEpochInFlightAndUnflushed() {
    final DirtyPageTracker tracker = tracker();
    tracker.mark(new int[] { 4 }, 1);
    assertThat(tracker.isEmpty()).isFalse();
    tracker.drainInto(new IntHashSet());
    assertThat(tracker.isEmpty()).isFalse();
    tracker.completed(new int[] { 4 }, 1);
    assertThat(tracker.isEmpty()).isFalse();
    tracker.flushed(new int[] { 4 }, 1, p -> false);
    assertThat(tracker.isEmpty()).isTrue();
  }

  @Test
  void pendingPositionsCoverWholePagesAndOwners() {
    final DirtyPageTracker tracker = tracker();
    final ContentOwnerMap owners = new ContentOwnerMap();
    owners.setOwnerPages(10L, new int[] { 2 }, 1); // record 10 (page 0) has content on page 2
    tracker.mark(new int[] { 2 }, 1);

    final PositionBitmap pending = tracker.pendingPositions(owners);
    assertThat(pending.cardinality()).isEqualTo(PAGE_SLOTS + 1);
    assertThat(pending.get(10L)).isTrue();
    assertThat(pending.get(2L * PAGE_SLOTS)).isTrue();
    assertThat(pending.get(3L * PAGE_SLOTS - 1)).isTrue();
    assertThat(pending.get(3L * PAGE_SLOTS)).isFalse();
    assertThat(pending.get(2L * PAGE_SLOTS - 1)).isFalse();

    // still pending while in flight, gone once completed
    tracker.drainInto(new IntHashSet());
    assertThat(tracker.pendingPositions(owners).cardinality()).isEqualTo(PAGE_SLOTS + 1);
    tracker.completed(new int[] { 2 }, 1);
    assertThat(tracker.pendingPositions(owners).isEmpty()).isTrue();
  }

  @Test
  void pageReappliedAfterFreezeStaysUnflushed() {
    final DirtyPageTracker tracker = tracker();
    tracker.mark(new int[] { 5, 6 }, 2);
    tracker.drainInto(new IntHashSet());
    tracker.completed(new int[] { 5, 6 }, 2);

    tracker.flushed(new int[] { 5, 6 }, 2, p -> p == 5);

    final IntHashSet recovery = new IntHashSet();
    tracker.collectRecoverySet(recovery);
    assertThat(sorted(recovery)).containsExactly(5);
  }

  @Test
  void recordMarksPendOnlyTheirRecords() {
    final DirtyPageTracker tracker = tracker();
    final ContentOwnerMap owners = new ContentOwnerMap();
    final long base = 7L * PAGE_SLOTS;
    tracker.markRecords(new long[] { base + 3, base + 700, base + 2047 }, 3);

    final PositionBitmap pending = tracker.pendingPositions(owners);
    assertThat(positions(pending)).containsExactly(base + 3, base + 700, base + 2047);

    // the drain offers the page with its records, which stay pending while in flight
    final IntHashSet drained = new IntHashSet();
    final PositionBitmap slices = new PositionBitmap(PAGE_SLOTS);
    assertThat(tracker.drainInto(drained, slices)).isEqualTo(1);
    assertThat(sorted(drained)).containsExactly(7);
    assertThat(positions(slices)).containsExactly(base + 3, base + 700, base + 2047);
    assertThat(positions(tracker.pendingPositions(owners))).containsExactly(base + 3, base + 700, base + 2047);

    tracker.completed(new int[] { 7 }, 1);
    assertThat(tracker.pendingPositions(owners).isEmpty()).isTrue();
    final IntHashSet recovery = new IntHashSet();
    tracker.collectRecoverySet(recovery);
    assertThat(sorted(recovery)).containsExactly(7);
  }

  @Test
  void seventeenRecordsOfAPageEscalateToTheWholePage() {
    final int limit = ColumnarIndexManager.MAX_SLOT_MARKS_PER_PAGE;
    final DirtyPageTracker tracker = tracker();
    final ContentOwnerMap owners = new ContentOwnerMap();
    // the same records marked twice, as the two marks of one commit do: still records, not the page
    tracker.markRecords(range(0, limit), limit);
    tracker.markRecords(range(0, limit), limit);
    assertThat(tracker.pendingPositions(owners).cardinality()).isEqualTo(limit);

    // one more record of the page, in a later mark of the same epoch: the page is whole
    tracker.markRecords(new long[] { limit }, 1);
    assertThat(tracker.pendingPositions(owners).cardinality()).isEqualTo(PAGE_SLOTS);

    // seventeen records in one mark of another page: whole at once
    final DirtyPageTracker other = tracker();
    other.markRecords(range(PAGE_SLOTS, limit + 1), limit + 1);
    final PositionBitmap pending = other.pendingPositions(owners);
    assertThat(pending.cardinality()).isEqualTo(PAGE_SLOTS);
    assertThat(pending.get(2L * PAGE_SLOTS - 1)).isTrue();
    final PositionBitmap slices = new PositionBitmap(PAGE_SLOTS);
    other.drainInto(new IntHashSet(), slices);
    assertThat(slices.isEmpty()).as("a whole page offers no records").isTrue();
  }

  @Test
  void aPageMarkMakesTheRecordMarksOfItsPageWhole() {
    final ContentOwnerMap owners = new ContentOwnerMap();
    final DirtyPageTracker recordsFirst = tracker();
    recordsFirst.markRecords(new long[] { 5 }, 1);
    recordsFirst.mark(new int[] { 0 }, 1);
    assertThat(recordsFirst.pendingPositions(owners).cardinality()).isEqualTo(PAGE_SLOTS);

    final DirtyPageTracker pageFirst = tracker();
    pageFirst.mark(new int[] { 0 }, 1);
    pageFirst.markRecords(new long[] { 5 }, 1);
    assertThat(pageFirst.pendingPositions(owners).cardinality()).isEqualTo(PAGE_SLOTS);

    // the records of another page keep their granularity
    pageFirst.markRecords(new long[] { PAGE_SLOTS + 9L }, 1);
    final PositionBitmap slices = new PositionBitmap(PAGE_SLOTS);
    final IntHashSet drained = new IntHashSet();
    pageFirst.drainInto(drained, slices);
    assertThat(sorted(drained)).containsExactly(0, 1);
    assertThat(positions(slices)).containsExactly(PAGE_SLOTS + 9L);
  }

  @Test
  void recordsInFlightMergeWithTheMarksOfTheNextEpoch() {
    final DirtyPageTracker tracker = tracker();
    final ContentOwnerMap owners = new ContentOwnerMap();
    tracker.markRecords(new long[] { 3 }, 1);
    tracker.drainInto(new IntHashSet(), new PositionBitmap(PAGE_SLOTS));
    // the pass failed (no completed): the records of the next epoch join the ones in flight
    tracker.markRecords(new long[] { 5 }, 1);
    PositionBitmap slices = new PositionBitmap(PAGE_SLOTS);
    tracker.drainInto(new IntHashSet(), slices);
    assertThat(positions(slices)).containsExactly(3L, 5L);
    assertThat(positions(tracker.pendingPositions(owners))).containsExactly(3L, 5L);

    // a page mark makes the page in flight whole, and later record marks cannot narrow it again
    tracker.mark(new int[] { 0 }, 1);
    tracker.drainInto(new IntHashSet(), new PositionBitmap(PAGE_SLOTS));
    tracker.markRecords(new long[] { 9 }, 1);
    slices = new PositionBitmap(PAGE_SLOTS);
    final IntHashSet drained = new IntHashSet();
    tracker.drainInto(drained, slices);
    assertThat(sorted(drained)).containsExactly(0);
    assertThat(slices.isEmpty()).isTrue();
    assertThat(tracker.pendingPositions(owners).cardinality()).isEqualTo(PAGE_SLOTS);

    // completed: nothing pending, and the next record mark of the page is records only again
    tracker.completed(new int[] { 0 }, 1);
    tracker.markRecords(new long[] { 11 }, 1);
    assertThat(positions(tracker.pendingPositions(owners))).containsExactly(11L);
  }

  @Test
  void recordMarksKeepTheOwnersOfTheirPage() {
    final DirtyPageTracker tracker = tracker();
    final ContentOwnerMap owners = new ContentOwnerMap();
    owners.setOwnerPages(10L, new int[] { 2 }, 1); // record 10 (page 0) has content on page 2
    tracker.markRecords(new long[] { 2L * PAGE_SLOTS + 1 }, 1);
    assertThat(positions(tracker.pendingPositions(owners))).containsExactly(10L, 2L * PAGE_SLOTS + 1);
  }

  @Test
  void recordMarkAllocatesNothingOnceThePageGroupExists() {
    final DirtyPageTracker tracker = tracker();
    final long[] positions = { 3, 9, PAGE_SLOTS + 4L };
    final IntHashSet target = new IntHashSet();
    for (int i = 0; i < 1_000; i++) {
      tracker.markRecords(positions, positions.length);
      if (i % 10 == 0) {
        target.clear();
        tracker.drainInto(target);
        tracker.completed(new int[] { 0, 1 }, 2);
      }
    }
    final ThreadMXBean threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    final long before = threads.getCurrentThreadAllocatedBytes();
    for (int i = 0; i < 10_000; i++)
      tracker.markRecords(positions, positions.length);
    assertThat(threads.getCurrentThreadAllocatedBytes() - before).isZero();
  }

  @Test
  @Timeout(120)
  void recordMarksDuringDrainsAreNeverLost() throws Exception {
    final int threads = 4;
    final int marksPerThread = 50_000;
    final int pageRange = 64;
    final DirtyPageTracker tracker = tracker();
    final ContentOwnerMap noOwners = new ContentOwnerMap();
    // every mark that returned, in return order: a record position + 1, or -(page + 1) for a page mark
    final AtomicLongArray returned = new AtomicLongArray(threads * marksPerThread);
    final AtomicInteger returnedCount = new AtomicInteger();
    // what an apply pass took, set BEFORE the pass completes its pages: a mark absent from a pending sample must be here
    final AtomicIntegerArray applied = new AtomicIntegerArray(pageRange * PAGE_SLOTS);
    final CountDownLatch start = new CountDownLatch(1);
    final AtomicBoolean writing = new AtomicBoolean(true);
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final AtomicInteger samples = new AtomicInteger();

    final Thread[] writers = new Thread[threads];
    final CountDownLatch writersDone = new CountDownLatch(threads);
    for (int t = 0; t < threads; t++) {
      final int index = t;
      writers[t] = new Thread(() -> {
        try {
          final Random random = new Random(index);
          final long[] one = new long[1];
          final int[] page = new int[1];
          start.await();
          for (int i = 0; i < marksPerThread; i++) {
            final long encoded;
            if (i % 10 == 9) {
              // now and then a page mark (a Raft apply, a retry): the page is whole for the rest of its epoch
              page[0] = random.nextInt(pageRange);
              tracker.mark(page, 1);
              encoded = -(page[0] + 1L);
            } else {
              // a few slots per page, so most pages stay records only and some escalate
              one[0] = (long) random.nextInt(pageRange) * PAGE_SLOTS + random.nextInt(24);
              tracker.markRecords(one, 1);
              encoded = one[0] + 1;
            }
            returned.set(returnedCount.getAndIncrement(), encoded);
          }
        } catch (final Throwable e) {
          failure.set(e);
        } finally {
          writersDone.countDown();
        }
      });
      writers[t].start();
    }

    final Thread applier = new Thread(() -> {
      try {
        start.await();
        while (writing.get())
          drainOnce(tracker, applied);
      } catch (final Throwable e) {
        failure.set(e);
      }
    });
    // pendingPositions read while drains seal, clear and move the epochs: every mark that returned before a sample began is in the
    // sample (a page mark as the whole page) or was taken by a pass
    final Thread reader = new Thread(() -> {
      try {
        start.await();
        while (writing.get()) {
          final int count = returnedCount.get();
          final PositionBitmap sample = tracker.pendingPositions(noOwners);
          for (int i = 0; i < count; i++) {
            final long encoded = returned.get(i);
            if (encoded == 0)
              continue; // its index was taken, the value not written yet
            // a page mark: a slot no record mark ever names must be pending
            final long position = encoded > 0 ? encoded - 1 : (-encoded - 1) * PAGE_SLOTS + PAGE_SLOTS - 1;
            if (!sample.get(position) && applied.get((int) position) == 0)
              throw new AssertionError((encoded > 0 ? "record " + position : "page " + (-encoded - 1)) + " returned before sample "
                  + samples.get() + " and is neither pending nor applied");
          }
          samples.incrementAndGet();
        }
      } catch (final Throwable e) {
        failure.set(e);
      }
    });
    applier.start();
    reader.start();
    start.countDown();
    writersDone.await();
    writing.set(false);
    applier.join();
    reader.join();
    drainOnce(tracker, applied);

    assertThat(failure.get()).isNull();
    assertThat(samples.get()).as("pending samples taken during the drains").isPositive();
    for (int i = 0; i < returnedCount.get(); i++) {
      final long encoded = returned.get(i);
      final long position = encoded > 0 ? encoded - 1 : (-encoded - 1) * PAGE_SLOTS + PAGE_SLOTS - 1;
      assertThat(applied.get((int) position)).as("mark %d (%d) applied", i, encoded).isEqualTo(1);
    }
  }

  /** One pass: what it offers is recorded as applied (a whole page as every slot of it) before its pages complete. */
  private static void drainOnce(final DirtyPageTracker tracker, final AtomicIntegerArray applied) {
    final IntHashSet target = new IntHashSet();
    final PositionBitmap slices = new PositionBitmap(PAGE_SLOTS);
    tracker.drainInto(target, slices);
    final int[] pages = target.toArray();
    for (final int page : pages)
      if (slices.stripeForTesting(page) == null)
        for (int slot = 0; slot < PAGE_SLOTS; slot++)
          applied.set(page * PAGE_SLOTS + slot, 1);
    slices.forEach(position -> applied.set((int) position, 1));
    tracker.completed(pages, pages.length);
  }

  private static long[] range(final long from, final int count) {
    final long[] positions = new long[count];
    for (int i = 0; i < count; i++)
      positions[i] = from + i;
    return positions;
  }

  private static long[] positions(final PositionBitmap bitmap) {
    final long[] count = new long[1];
    bitmap.forEach(p -> count[0]++);
    final long[] positions = new long[(int) count[0]];
    final int[] i = new int[1];
    bitmap.forEach(p -> positions[i[0]++] = p);
    return positions;
  }

  @Test
  void externalTrackerAddsOnlyOwners() {
    final DirtyPageTracker external = new DirtyPageTracker(256, () -> 1L);
    final ContentOwnerMap externalOwners = new ContentOwnerMap();
    externalOwners.setOwnerPages(4100L, new int[] { 3 }, 1);
    external.mark(new int[] { 3 }, 1);

    final PositionBitmap out = new PositionBitmap(PAGE_SLOTS);
    external.addPendingOwners(externalOwners, out);

    assertThat(out.cardinality()).isEqualTo(1);
    assertThat(out.get(4100L)).isTrue();
    // no slot of primary page 3 is set
    assertThat(out.get(3L * PAGE_SLOTS)).isFalse();
  }
}
