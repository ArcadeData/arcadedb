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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
