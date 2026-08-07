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
package com.arcadedb.index.vector;

import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Spliterator;
import java.util.stream.StreamSupport;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5588: {@link VectorLocationIndex} lays its locations out in primitive arrays indexed by vector id instead
 * of in two {@code ConcurrentHashMap}s, taking the retained heap from ~90 bytes per live vector to ~32.
 * <p>
 * Every test here is written against a specific wrong implementation, because most of what this rewrite has to get
 * right is invisible in a functional test:
 * <ul>
 *   <li>a FLAT id-indexed array instead of released chunks - functionally perfect, and it retains ~190MB where the
 *       map it replaces retained ~360KB on the workload issue #5516 exists for</li>
 *   <li>the offset and its compacted flag in two arrays instead of one word - correct until a replica re-points a
 *       live id at the compacted file mid-search</li>
 *   <li>a reverse index that loses an entry across a rehash, or that sizes the rehash from its capacity rather than
 *       from the live count</li>
 *   <li>an id traversal that is not ascending, which silently breaks the {@code Arrays.binarySearch} the
 *       allow-list search does over {@code ordinalToVectorId}</li>
 * </ul>
 * The one thing deliberately NOT tested is the release/acquire on the presence word itself: dropping it is
 * invisible on x86, so a green test would only be evidence about the hardware it ran on.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5588PrimitiveLocationIndexTest {

  /**
   * The reason the arrays are chunked. Ids are handed out monotonically and an update tombstones the id it
   * supersedes, so a re-embedding workload leaves a live set orders of magnitude smaller than the id space: issue
   * #5516 measured 9.3M ids for 4K live vectors. Residency has to follow the live set, and a flat array indexed by
   * id follows the id space instead.
   */
  @Test
  void aDrainedIdSpaceIsHandedBack() {
    final VectorLocationIndex index = new VectorLocationIndex();

    final int liveVectors = 100;
    for (int i = 0; i < liveVectors; i++)
      index.addVector(false, 1000 + i, new RID(1, i));

    // Hand out and immediately retire ids the way re-embedding does.
    final int churn = 200_000;
    for (int i = 0; i < churn; i++)
      index.markDeleted(index.addVector(false, 500_000 + i, new RID(2, i)));

    assertThat(index.getNextId()).as("every id was really handed out").isEqualTo(liveVectors + churn);
    assertThat(index.size()).as("and only the pinned ones are live").isEqualTo(liveVectors);
    assertThat(index.getDeletedCount()).isEqualTo(churn);

    assertThat(index.chunkCount())
        .as("the drained chunks must be released, not kept for the ids they no longer hold")
        .isLessThanOrEqualTo(4);

    // A flat array would be ~21 bytes per id HANDED OUT (~4.2MB here). The bound is expressed per id handed out
    // precisely so that layout fails it: what survives is the deleted-id bitset, which issue #5516 already
    // accepted at one bit per id.
    assertThat(index.estimatedRetainedBytes())
        .as("residency must follow the live vectors, not the id space")
        .isLessThan(4L * (liveVectors + churn));

    // ...and the live set is still intact, so the release did not take anything with it.
    for (int i = 0; i < liveVectors; i++) {
      assertThat(index.isLive(i)).as("live id %d", i).isTrue();
      assertThat(index.getRid(i)).isEqualTo(new RID(1, i));
      assertThat(VectorLocationIndex.offsetOf(index.getOffsetAndFlag(i))).isEqualTo(1000 + i);
      assertThat(index.getVectorIdsForRid(new RID(1, i))).containsExactly(i);
    }
  }

  /**
   * The offset and the compacted flag must come out of one 64-bit word.
   * <p>
   * {@code LSMVectorIndex.applyReplicatedPageUpdate} re-points a live id at the compacted file, on a replica, with
   * no index lock held, while searches run. Split across two arrays, a reader can pair the new flag with the stale
   * offset; {@code readVectorFromOffset} then reads a misaligned region of an INT8/BINARY page, which parses into a
   * well-formed vector of the right dimension that passes every guard and is scored at the wrong distance.
   * <p>
   * The writer here encodes the flag in the offset, so a torn read is not a rare interleaving to get lucky with -
   * it is wrong on about half of them, and the test fails in milliseconds.
   */
  @Test
  void theOffsetAndItsCompactedFlagCannotBeReadFromDifferentGenerations() throws Exception {
    final VectorLocationIndex index = new VectorLocationIndex();
    final long base = 1_000_000L;
    final RID rid = new RID(7, 42);
    final int id = index.addVector(false, base, rid);

    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicReference<String> failure = new AtomicReference<>();
    final AtomicLong reads = new AtomicLong();

    final Thread writer = new Thread(() -> {
      for (long n = 0; !stop.get(); n++)
        index.addOrUpdate(id, n % 2 == 0, base + n, rid, false);
    }, "issue5588-writer");

    final List<Thread> readers = new ArrayList<>();
    for (int r = 0; r < 4; r++)
      readers.add(new Thread(() -> {
        while (!stop.get()) {
          final long packed = index.getOffsetAndFlag(id);
          if (packed == VectorLocationIndex.ABSENT) {
            failure.compareAndSet(null, "the id was retired by an update that did not change its RID");
            return;
          }
          final long offset = VectorLocationIndex.offsetOf(packed);
          final boolean compacted = VectorLocationIndex.isCompactedOf(packed);
          if (compacted != ((offset - base) % 2 == 0)) {
            failure.compareAndSet(null,
                "torn read: offset " + offset + " came from a different generation than isCompacted " + compacted);
            return;
          }
          reads.incrementAndGet();
        }
      }, "issue5588-reader-" + r));

    writer.start();
    readers.forEach(Thread::start);
    Thread.sleep(500);
    stop.set(true);
    writer.join(30_000);
    for (final Thread reader : readers)
      reader.join(30_000);

    assertThat(failure.get()).isNull();
    assertThat(reads.get()).as("the readers must really have observed the id being rewritten").isGreaterThan(1000);
  }

  /**
   * The reverse index must never lose a live entry, however hard the table is churned.
   * <p>
   * Everything churned here shares the pinned RID, so every dead id lands in the same probe chain and the table
   * rehashes constantly. Two mutations die on this: publishing a rehashed table before it is fully populated, and
   * sizing the rehash from the current capacity instead of from the live count - the latter grows the table without
   * bound, because the pressure on it is dead entries and the live set never moves.
   */
  @Test
  void theReverseIndexNeverLosesALiveEntryAcrossARehash() throws Exception {
    final VectorLocationIndex index = new VectorLocationIndex();
    final RID rid = new RID(3, 99);
    final int pinned = index.addVector(false, 10, rid);

    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicReference<String> failure = new AtomicReference<>();
    final AtomicInteger churned = new AtomicInteger();
    final CountDownLatch started = new CountDownLatch(1);

    final Thread writer = new Thread(() -> {
      started.countDown();
      while (!stop.get() && churned.get() < 100_000) {
        final int id = index.addVector(false, 1000 + churned.get(), rid);
        index.markDeleted(id);
        churned.incrementAndGet();
      }
    }, "issue5588-churn");

    final Thread reader = new Thread(() -> {
      try {
        started.await();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
      while (!stop.get()) {
        final int[] ids = index.getVectorIdsForRid(rid);
        boolean seen = false;
        for (final int id : ids)
          if (id == pinned)
            seen = true;
        if (!seen) {
          failure.compareAndSet(null, "the pinned id disappeared from the reverse index");
          return;
        }
        // Several ids can legitimately come back: the probe walk is not atomic, so ids the churn thread retired
        // between two steps of one walk were each genuinely live when they were verified. What is never legitimate
        // is the SAME id twice, which is one id owning two slots that both answer.
        for (int i = 1; i < ids.length; i++)
          if (ids[i] == ids[i - 1]) {
            failure.compareAndSet(null, "one id answered twice: " + Arrays.toString(ids));
            return;
          }
      }
    }, "issue5588-reverse-reader");

    writer.start();
    reader.start();
    writer.join(60_000);
    stop.set(true);
    reader.join(30_000);

    assertThat(failure.get()).isNull();
    assertThat(churned.get()).as("the churn must really have run").isEqualTo(100_000);
    assertThat(index.getVectorIdsForRid(rid)).as("and only the live id is mapped at the end").containsExactly(pinned);
    assertThat(index.reverseTableCapacity())
        .as("dead entries must be cleaned in place, not grow the table: the live set never left 1 entry")
        .isLessThanOrEqualTo(64);
  }

  /**
   * The id traversal is ascending and says so, which is a correctness claim rather than an optimisation.
   * <p>
   * {@code LSMVectorIndex} builds {@code ordinalToVectorId} from this stream and then reads that array with
   * {@code Arrays.binarySearch} to resolve an allow-listed RID to its graph ordinal. The {@code .sorted()} on the
   * way in is a no-op only because the spliterator reports {@link Spliterator#SORTED}; if the traversal were not
   * really ascending, the binary search would quietly stop finding ordinals and every allow-list-filtered search
   * would return fewer results, with nothing failing.
   */
  @Test
  void idsAreTraversedInAscendingOrderAndTheStreamSaysSo() {
    final VectorLocationIndex index = new VectorLocationIndex();
    index.addOrUpdate(5000, false, 1, new RID(1, 1), false);
    index.addOrUpdate(3, false, 2, new RID(1, 2), false);
    index.addOrUpdate(1_000_000, false, 3, new RID(1, 3), false);
    index.addOrUpdate(700, false, 4, new RID(1, 4), false);
    // Drains the chunk id 700 lived in, so the walk has to step over a released slot in the middle.
    index.markDeleted(700);

    assertThat(index.getAllVectorIds().toArray()).containsExactly(3, 5000, 1_000_000);
    assertThat(index.getActiveVectorIds().toArray()).containsExactly(3, 5000, 1_000_000);
    assertThat(index.getAllVectorIds().spliterator().hasCharacteristics(Spliterator.SORTED))
        .as("SORTED is what makes the .sorted() at the call sites a no-op")
        .isTrue();
    assertThat(index.getAllVectorIds().filter(id -> true).spliterator().hasCharacteristics(Spliterator.SORTED))
        .as("and it has to survive the filter() the call sites apply before sorting")
        .isTrue();
    assertThat(index.getAllVectorIds().sorted().toArray()).containsExactly(3, 5000, 1_000_000);

    // That the flag really elides the sort is a JDK behaviour worth pinning rather than asserting from memory:
    // IntPipeline.sorted() returns the sink unchanged when StreamOpFlag.SORTED is known, and the flag propagates
    // from a spliterator whose getComparator() is null. A spliterator that CLAIMS sorted and yields descending
    // values comes back descending if the sort was elided, and ascending if it was not.
    final Spliterator.OfInt liesAboutBeingSorted = new Spliterator.OfInt() {
      private int next = 5;

      @Override
      public boolean tryAdvance(final java.util.function.IntConsumer action) {
        if (next <= 0)
          return false;
        action.accept(next--);
        return true;
      }

      @Override
      public OfInt trySplit() {
        return null;
      }

      @Override
      public long estimateSize() {
        return 5;
      }

      @Override
      public int characteristics() {
        return ORDERED | DISTINCT | SORTED | NONNULL;
      }

      @Override
      public java.util.Comparator<? super Integer> getComparator() {
        return null;
      }
    };
    assertThat(StreamSupport.intStream(liesAboutBeingSorted, false).filter(id -> true).sorted().toArray())
        .as("IntStream.sorted() elides the sort when SORTED is known, so declaring it is load-bearing")
        .containsExactly(5, 4, 3, 2, 1);
    assertThat(index.getAllVectorIds().max().orElse(-1)).isEqualTo(1_000_000);

    assertThat(index.getNextId()).as("the sequence follows the highest id ever handed out").isEqualTo(1_000_001);
    assertThat(index.getMaxVectorId()).isEqualTo(1_000_000);
  }

  /**
   * Tombstoning an id that holds no location must not be counted.
   * <p>
   * {@code LSMVectorIndex} discards the persisted graph and rebuilds it from scratch whenever
   * {@code getDeletedCount() > 0} (issue #3135), so an unconditional count turns every database open into a full
   * rebuild - a large startup regression with no functional symptom to notice it by.
   */
  @Test
  void tombstoningAnIdThatHoldsNoLocationIsNotCounted() {
    final VectorLocationIndex index = new VectorLocationIndex();
    index.markDeleted(12_345);
    assertThat(index.getDeletedCount()).isZero();
    assertThat(index.isDeleted(12_345)).isFalse();

    final int id = index.addVector(false, 100, new RID(1, 1));
    index.markDeleted(id);
    index.markDeleted(id);
    assertThat(index.getDeletedCount()).as("a second tombstone on the same id is not a second deletion").isEqualTo(1);

    // The page-load path is deliberately different: a tombstone read off a page is recorded whether or not the
    // live entry preceding it was ever seen, because the id was handed out either way.
    index.addOrUpdate(999, false, 0, new RID(1, 2), true);
    assertThat(index.getDeletedCount()).isEqualTo(2);
    assertThat(index.isDeleted(999)).isTrue();
    assertThat(index.getNextId()).as("a tombstone still advances the sequence").isEqualTo(1000);
  }

  /**
   * A vector must be visible to a lock-free reader as soon as the writer that added it returns.
   * <p>
   * Honest about what this can and cannot kill: it exercises directory growth and chunk allocation against
   * concurrent readers, so it catches a reader that walks a stale directory length or dereferences a released
   * chunk. It does NOT prove the release/acquire on the directory slot - the handoff through the queue below is
   * itself an ordering edge, and on x86 a plain store would pass anyway. That ordering is enforced by review.
   */
  @Test
  void aVectorIsVisibleToAConcurrentReaderAsSoonAsItIsAdded() throws Exception {
    final VectorLocationIndex index = new VectorLocationIndex();
    final AtomicInteger published = new AtomicInteger(-1);
    final AtomicReference<String> failure = new AtomicReference<>();
    final int total = 200_000;

    final Thread reader = new Thread(() -> {
      int last = -1;
      while (last < total - 1 && failure.get() == null) {
        final int id = published.get();
        if (id < 0 || id == last)
          continue;
        last = id;
        if (!index.isLive(id)) {
          failure.compareAndSet(null, "id " + id + " was added but is not visible");
          return;
        }
        if (!index.isLocationOf(id, new RID(1, id))) {
          failure.compareAndSet(null, "id " + id + " resolved to the wrong RID");
          return;
        }
      }
    }, "issue5588-visibility-reader");

    reader.start();
    for (int i = 0; i < total; i++)
      published.set(index.addVector(false, 1000 + i, new RID(1, i)));
    reader.join(60_000);

    assertThat(failure.get()).isNull();
    assertThat(index.size()).isEqualTo(total);
  }

  /**
   * The whole observable surface, against a reference model, over a random operation sequence.
   * <p>
   * This is what catches the off-by-one in a live counter, a reverse-index entry that was never inserted, and the
   * id reuse {@link VectorLocationIndex#clear()} allows - it is the one call that resets the sequence, so it is the
   * one place a stale chunk could resolve a fresh id to the record the old one belonged to.
   * <p>
   * {@code size()} and {@code getActiveCount()} are asserted separately on purpose: the implementation derives them
   * from two independent things (a counter and a popcount over the presence bits), so agreeing is evidence and not
   * a tautology.
   */
  @Test
  void theIndexMatchesAReferenceModelUnderRandomOperations() {
    final VectorLocationIndex index = new VectorLocationIndex();
    final Map<Integer, long[]> liveModel = new HashMap<>();     // id -> {bucketId, position, offset, compacted}
    final Set<Integer> deletedModel = new HashSet<>();
    final Random random = new Random(5588);
    int modelNextId = 0;

    // A small RID pool so several ids can share one RID, which is what makes the reverse index non-trivial.
    final RID[] rids = new RID[16];
    for (int i = 0; i < rids.length; i++)
      rids[i] = new RID(i % 3, i);

    for (int step = 0; step < 4000; step++) {
      final int operation = random.nextInt(100);
      final RID rid = rids[random.nextInt(rids.length)];
      final long offset = 1 + random.nextInt(1_000_000);
      final boolean compacted = random.nextBoolean();

      if (operation < 45) {
        final int id = index.addVector(compacted, offset, rid);
        assertThat(id).as("addVector must hand out the next id in sequence").isEqualTo(modelNextId);
        liveModel.put(id, new long[] { rid.getBucketId(), rid.getPosition(), offset, compacted ? 1 : 0 });
        modelNextId++;
      } else if (operation < 70) {
        // An out-of-sequence id, the way the page-load path replays what the file holds.
        final int id = random.nextInt(modelNextId + 200);
        index.addOrUpdate(id, compacted, offset, rid, false);
        liveModel.put(id, new long[] { rid.getBucketId(), rid.getPosition(), offset, compacted ? 1 : 0 });
        deletedModel.remove(id);
        modelNextId = Math.max(modelNextId, id + 1);
      } else if (operation < 85) {
        final int id = random.nextInt(Math.max(1, modelNextId));
        index.addOrUpdate(id, compacted, offset, rid, true);
        liveModel.remove(id);
        deletedModel.add(id);
        modelNextId = Math.max(modelNextId, id + 1);
      } else if (operation < 98) {
        final int id = random.nextInt(Math.max(1, modelNextId));
        index.markDeleted(id);
        if (liveModel.remove(id) != null)
          deletedModel.add(id);
      } else {
        index.clear();
        liveModel.clear();
        deletedModel.clear();
        modelNextId = 0;
      }

      assertModelMatches(index, liveModel, deletedModel, modelNextId, rids, step);
    }
  }

  private static void assertModelMatches(final VectorLocationIndex index, final Map<Integer, long[]> liveModel,
      final Set<Integer> deletedModel, final int modelNextId, final RID[] rids, final int step) {
    assertThat(index.size()).as("size() at step %d", step).isEqualTo(liveModel.size());
    assertThat(index.getActiveCount()).as("getActiveCount() at step %d", step).isEqualTo(liveModel.size());
    assertThat(index.getDeletedCount()).as("getDeletedCount() at step %d", step).isEqualTo(deletedModel.size());
    assertThat(index.getNextId()).as("getNextId() at step %d", step).isEqualTo(modelNextId);

    final int[] expectedIds = liveModel.keySet().stream().mapToInt(Integer::intValue).sorted().toArray();
    assertThat(index.getAllVectorIds().toArray()).as("the id traversal at step %d", step).containsExactly(expectedIds);
    assertThat(index.getActiveVectorIds().toArray()).as("the active id traversal at step %d", step)
        .containsExactly(expectedIds);

    for (final Map.Entry<Integer, long[]> entry : liveModel.entrySet()) {
      final int id = entry.getKey();
      final long[] expected = entry.getValue();
      final RID expectedRid = new RID((int) expected[0], expected[1]);

      assertThat(index.isLive(id)).as("isLive(%d) at step %d", id, step).isTrue();
      assertThat(index.getRid(id)).as("getRid(%d) at step %d", id, step).isEqualTo(expectedRid);
      assertThat(index.getBucketId(id)).isEqualTo((int) expected[0]);
      assertThat(index.getPosition(id)).isEqualTo(expected[1]);
      assertThat(index.isLocationOf(id, expectedRid)).isTrue();

      final long packed = index.getOffsetAndFlag(id);
      assertThat(VectorLocationIndex.offsetOf(packed)).as("offset of %d at step %d", id, step).isEqualTo(expected[2]);
      assertThat(VectorLocationIndex.isCompactedOf(packed)).isEqualTo(expected[3] == 1);

      final VectorLocationIndex.VectorLocation location = index.getLocation(id);
      assertThat(location).isNotNull();
      assertThat(location.absoluteFileOffset).isEqualTo(expected[2]);
      assertThat(location.isCompacted).isEqualTo(expected[3] == 1);
      assertThat(location.rid).isEqualTo(expectedRid);
      assertThat(location.deleted).as("a resident location is never a tombstone since issue #5516").isFalse();
    }

    for (final int id : deletedModel) {
      assertThat(index.isDeleted(id)).as("isDeleted(%d) at step %d", id, step).isTrue();
      if (!liveModel.containsKey(id)) {
        assertThat(index.getLocation(id)).as("getLocation(%d) at step %d", id, step).isNull();
        assertThat(index.getOffsetAndFlag(id)).isEqualTo(VectorLocationIndex.ABSENT);
        assertThat(index.getRid(id)).isNull();
      }
    }

    for (final RID rid : rids) {
      final int[] expected = liveModel.entrySet().stream()
          .filter(e -> e.getValue()[0] == rid.getBucketId() && e.getValue()[1] == rid.getPosition())
          .mapToInt(Map.Entry::getKey)
          .sorted()
          .toArray();
      assertThat(index.getVectorIdsForRid(rid)).as("reverse index for %s at step %d", rid, step)
          .containsExactly(expected);
    }
  }

  /**
   * Re-pointing a live vector id at a different record retires the id for the duration of the write, so a
   * lock-free search running through that window does not see a vector that is live. The branch does the safe
   * thing - the alternative is a torn RID, which is how a delete of record A tombstones the vector of record B -
   * but no engine path may take it, and {@code LSMVectorIndexTombstoneMemoryTest} pins that on the re-embedding
   * workload that would trip it first. This is the other half: proof the counter that pins it is not dead code.
   */
  @Test
  void rewritingALiveIdsRidInPlaceIsCountedAndStillCorrect() {
    final VectorLocationIndex index = new VectorLocationIndex();
    final RID first = new RID(1, 10);
    final RID second = new RID(2, 20);

    final int id = index.addVector(false, 100, first);
    assertThat(index.inPlaceRidRewriteCount()).as("an ordinary insert is not a rewrite").isZero();

    // A re-publication of the same id at a new offset is not a rewrite either - it is what a replicated page
    // replay does on every compaction - and counting it would make the pin above meaningless.
    index.addOrUpdate(id, true, 200, first, false);
    assertThat(index.inPlaceRidRewriteCount()).isZero();
    assertThat(index.getVectorIdsForRid(first)).containsExactly(id);

    index.addOrUpdate(id, false, 300, second, false);
    assertThat(index.inPlaceRidRewriteCount()).as("re-pointing the id at another record is").isEqualTo(1);

    // ...and having taken the branch, the index is still consistent: the old RID resolves to nothing, the new one
    // to the id, and the location reads back whole.
    assertThat(index.getVectorIdsForRid(first)).isEmpty();
    assertThat(index.getVectorIdsForRid(second)).containsExactly(id);
    assertThat(index.getRid(id)).isEqualTo(second);
    assertThat(VectorLocationIndex.offsetOf(index.getOffsetAndFlag(id))).isEqualTo(300);
    assertThat(index.size()).isEqualTo(1);
    assertThat(index.getActiveCount()).isEqualTo(1);
  }

  /**
   * The acceptance criterion of issue #5588, measured off the arrays the index has actually allocated rather than
   * inferred from object headers.
   */
  @Test
  void theRetainedHeapPerLiveVectorIsFarBelowTheMapItReplaces() {
    final VectorLocationIndex index = new VectorLocationIndex();
    final int vectors = 100_000;
    for (int i = 0; i < vectors; i++)
      index.addVector(i % 2 == 0, 1000L + i, new RID(1 + i % 8, i));

    assertThat(index.size()).isEqualTo(vectors);
    assertThat(index.getActiveCount()).isEqualTo(vectors);

    final long perVector = index.estimatedRetainedBytes() / vectors;
    assertThat(perVector).as("the layout this issue replaces retained ~90 bytes per live vector").isLessThan(40L);
    assertThat(perVector).as("and the chunk store alone is ~21, the reverse index another 8 to 16")
        .isBetween(20L, 40L);
  }

  /**
   * A capacity hint pre-sizes the structures; it is not, and can never again be, an eviction limit (issue #5559).
   */
  @Test
  void theCapacityHintNeitherEvictsNorAcceptsTheOldUnlimitedSentinel() {
    final VectorLocationIndex index = new VectorLocationIndex(8);
    final List<Integer> ids = new ArrayList<>();
    for (int i = 0; i < 512; i++)
      ids.add(index.addVector(false, 100 + i, new RID(4, i)));

    assertThat(index.size()).as("nothing may be dropped past the hint").isEqualTo(512);
    for (int i = 0; i < 512; i++) {
      assertThat(index.isLive(ids.get(i))).isTrue();
      assertThat(index.getVectorIdsForRid(new RID(4, i))).containsExactly(ids.get(i));
    }

    assertThat(Arrays.stream(index.getAllVectorIds().toArray()).boxed().toList()).isEqualTo(ids);

    try {
      new VectorLocationIndex(-1);
      throw new AssertionError("the old -1 = unlimited sentinel must be refused, not read as a capacity");
    } catch (final IllegalArgumentException e) {
      assertThat(e.getMessage()).contains("eviction limit");
    }
  }

  /**
   * The offset shares its word with the compacted flag, so the range it may take is checked at the door rather
   * than silently wrapping into the flag and reading back as a compacted entry elsewhere in the file.
   * <p>
   * {@code -1} is inside that range and has to be: the document-scan fallback of a rebuild registers a vector it
   * found on a record but not on any page with exactly that offset, meaning "no page entry, read the document".
   */
  @Test
  void theOffsetRangeIsCheckedAndKeepsTheNoPageEntrySentinel() {
    final VectorLocationIndex index = new VectorLocationIndex();

    final int id = index.addVector(false, VectorLocationIndex.NO_FILE_OFFSET, new RID(1, 1));
    final long packed = index.getOffsetAndFlag(id);
    assertThat(packed).isNotEqualTo(VectorLocationIndex.ABSENT);
    assertThat(VectorLocationIndex.offsetOf(packed))
        .as("a vector with no page entry stays readable, it just has to come from its document")
        .isEqualTo(VectorLocationIndex.NO_FILE_OFFSET);
    assertThat(VectorLocationIndex.isCompactedOf(packed)).isFalse();
    assertThat(index.getLocation(id).absoluteFileOffset).isEqualTo(VectorLocationIndex.NO_FILE_OFFSET);

    // The same sentinel in the compacted file, so the flag survives a negative offset.
    final int compactedId = index.addVector(true, VectorLocationIndex.NO_FILE_OFFSET, new RID(1, 2));
    assertThat(VectorLocationIndex.offsetOf(index.getOffsetAndFlag(compactedId)))
        .isEqualTo(VectorLocationIndex.NO_FILE_OFFSET);
    assertThat(VectorLocationIndex.isCompactedOf(index.getOffsetAndFlag(compactedId))).isTrue();

    for (final long invalid : new long[] { -2L, Long.MIN_VALUE, Long.MAX_VALUE })
      try {
        index.addVector(false, invalid, new RID(1, 3));
        throw new AssertionError("an out-of-range absolute file offset must be refused: " + invalid);
      } catch (final IllegalArgumentException e) {
        assertThat(e.getMessage()).contains("compacted flag");
      }

    assertThat(index.size()).as("and a refused offset leaves nothing behind").isEqualTo(2);
  }

  /** A latch-free sanity check that the concurrency helpers above do not silently skip their bodies. */
  @Test
  void concurrentAddsAndReadsAgreeOnTheFinalState() throws Exception {
    final VectorLocationIndex index = new VectorLocationIndex();
    final int threads = 8;
    final int perThread = 4000;
    final CountDownLatch ready = new CountDownLatch(threads);
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final List<Thread> workers = new ArrayList<>();

    for (int t = 0; t < threads; t++) {
      final int bucket = t;
      workers.add(new Thread(() -> {
        ready.countDown();
        try {
          ready.await();
          for (int i = 0; i < perThread; i++) {
            index.addVector(false, 1000L + i, new RID(bucket, i));
            index.getActiveVectorIds().count();
            index.getAllVectorIds().count();
            index.getActiveCount();
          }
        } catch (final Throwable e) {
          failure.compareAndSet(null, e);
        }
      }, "issue5588-worker-" + t));
    }

    workers.forEach(Thread::start);
    for (final Thread worker : workers) {
      worker.join(120_000);
      assertThat(worker.isAlive()).as("%s must finish", worker.getName()).isFalse();
    }

    assertThat(failure.get()).isNull();
    assertThat(index.size()).isEqualTo(threads * perThread);
    assertThat(index.getActiveCount()).isEqualTo(threads * perThread);
    assertThat(index.getAllVectorIds().count()).isEqualTo(threads * perThread);
    for (int t = 0; t < threads; t++)
      for (int i = 0; i < perThread; i++)
        assertThat(index.getVectorIdsForRid(new RID(t, i))).as("bucket %d position %d", t, i).hasSize(1);
  }
}
