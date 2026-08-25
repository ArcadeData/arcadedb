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
package com.arcadedb.index.sparsevector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.schema.LocalSchema;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.logging.Level;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Regression tests for issue #6379: a <i>partial</i> compaction must never invert the "newest
 * wins" precedence between the segments it merged and the segments it left alone.
 * <p>
 * Before the fix, a merged segment was given a globally-new highest segment id and appended last
 * in the segment array, which is exactly where {@link DimCursor} looks for the newest version of a
 * posting. A partial compaction that included an older segment holding a live posting while
 * leaving out a younger segment holding that posting's tombstone therefore produced a merged
 * segment that outranked the tombstone - resurrecting a deleted document, and permanently, because
 * the next full compaction then dropped the tombstone as the loser.
 * <p>
 * The fix has two halves and the tests below separate them:
 * <ul>
 *   <li><b>Epoch.</b> Every segment carries a persisted recency epoch and a merge inherits the
 *       epoch of its newest input, so a merged segment lands where that input sat instead of
 *       jumping to the newest end. Exercised through {@code compactOldest}, which selects a
 *       contiguous prefix and so isolates this half.</li>
 *   <li><b>Adjacency.</b> Inheriting the newest input's epoch is only sound when nothing sits
 *       between the inputs, so the automatic triggers select contiguous runs. Exercised through
 *       the size-tiered and tombstone-ratio triggers on segment sets whose old selectors returned
 *       a gapped pick.</li>
 * </ul>
 * The tier arithmetic these scenarios lean on is {@code tierOf(p) = floor(log_fanout(p / base))}
 * for {@code p > base}, and tier 0 for {@code p <= base}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PaginatedSparseVectorEngineCompactionRecencyTest extends TestHelper {

  private static final int  DIM              = 0;
  private static final long DELETED_POSITION = 1L;

  /**
   * The epoch half, in isolation. {@code compactOldest(3)} merges a contiguous prefix and leaves
   * the younger tombstone segment alone; the merged segment must take the prefix's place in the
   * order, not the front of it.
   */
  @Test
  void partialCompactionOfOlderSegmentsKeepsDeletedRidInvisible() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final RID deleted = new RID(0, DELETED_POSITION);
    try (final PaginatedSparseVectorEngine engine = nonCompactingEngine(db, "PartialPrefixRecencyTest")) {
      // S1 holds the live posting, S2/S3 are filler, S4 is the tombstone.
      engine.put(DIM, deleted, 0.9f);
      fill(engine, 100L, 4);
      engine.flush();
      fill(engine, 200L, 5);
      engine.flush();
      fill(engine, 300L, 5);
      engine.flush();
      engine.remove(DIM, deleted);
      engine.flush();
      assertThat(engine.segmentCount()).isEqualTo(4);
      assertThat(topKRids(engine)).doesNotContain(deleted);

      engine.compactOldest(3);

      assertThat(engine.segmentCount()).isEqualTo(2);
      assertThat(topKRids(engine))
          .as("a partial compaction must not resurrect a RID whose tombstone it left out")
          .doesNotContain(deleted);
      assertThat(engine.segmentEpochs())
          .as("the merged segment inherits its newest input's epoch, so it stays behind the tombstone")
          .containsExactly(3L, 4L);
      assertThat(engine.segmentIds())
          .as("the merged segment still owns a brand-new, never-reused file id")
          .containsExactly(4L, 5L);

      // The full compaction is what used to make the resurrection permanent: it drops tombstones,
      // so it has to see the tombstone as the newest version rather than as the loser.
      engine.compactAll();
      assertThat(engine.segmentCount()).isEqualTo(1);
      assertThat(topKRids(engine))
          .as("compactAll after a partial compaction must still see the tombstone as newest")
          .doesNotContain(deleted);
    }
  }

  /**
   * The adjacency half, through the size-tiered primary trigger. It buckets by posting count, so a
   * small segment lands in a different tier from the fat data segments around it and used to be
   * jumped over by a tier overflow merge that did include the older segment holding the live
   * posting it masks.
   * <p>
   * fanout=4, base=2: tier 0 is 1..7 postings, tier 2 is 32..127. The tombstone segment is kept at
   * a 25% tombstone ratio so the secondary tombstone-ratio trigger (30%) stays quiet and the
   * primary trigger is the one under test.
   */
  @Test
  void sizeTieredPartialCompactionKeepsDeletedRidInvisible() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final RID deleted = new RID(0, DELETED_POSITION);
    try (final PaginatedSparseVectorEngine engine = tieredEngine(db, "SizeTieredRecencyTest", 4, 2L)) {
      // S1 (tier 2: 40 postings) holds the live posting for the RID we are going to delete.
      engine.put(DIM, deleted, 0.9f);
      fill(engine, 100L, 39);
      engine.flush();

      // S2 (tier 0: 4 postings, 25% tombstones) carries the tombstone. It is younger than S1, so
      // the RID is invisible from here on.
      engine.remove(DIM, deleted);
      fill(engine, 200L, 3);
      engine.flush();
      assertThat(topKRids(engine))
          .as("the tombstone in the younger segment must mask the older live posting")
          .doesNotContain(deleted);

      // S3..S6 (tier 2: 40 postings each). The old selector fired on the flush of S5, taking the
      // four tier-2 segments S1, S3, S4, S5 and jumping over S2. The adjacent run only reaches
      // four members with S6, and it is S3..S6 - S1 stays where it is, behind the tombstone.
      for (int s = 0; s < 4; s++) {
        fill(engine, 1_000L + s * 100L, 40);
        engine.flush();
      }

      assertThat(engine.segmentCount())
          .as("the adjacent tier-2 run S3..S6 must have merged, leaving S1 and S2 untouched")
          .isEqualTo(3);
      assertThat(engine.segmentEpochs()).isSorted();
      assertThat(topKRids(engine))
          .as("a partial compaction must not resurrect a RID whose tombstone it left out")
          .doesNotContain(deleted);
    }
  }

  /**
   * The adjacency half, through the secondary tombstone-ratio trigger, which used to pair the
   * delete-heavy segment with the globally oldest neighbours regardless of where they sat in
   * recency order - skipping whatever lived in between.
   * <p>
   * fanout=3, base=2: tier 0 is 1..5 postings, tier 1 is 6..17, tier 2 is 18..53. Every segment
   * sits in a tier of its own, so the primary trigger never fires.
   */
  @Test
  void tombstoneRatioPartialCompactionKeepsDeletedRidInvisible() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final RID deleted = new RID(0, DELETED_POSITION);
    try (final PaginatedSparseVectorEngine engine = tieredEngine(db, "TombstoneRatioRecencyTest", 3, 2L)) {
      // S1 (tier 0: 2 postings): the live posting.
      engine.put(DIM, deleted, 0.9f);
      fill(engine, 100L, 1);
      engine.flush();
      // S2 (tier 1: 6 postings): unrelated filler.
      fill(engine, 200L, 6);
      engine.flush();
      // S3 (tier 2: 18 postings): the tombstone for the RID inserted by S1, at a 5.5% ratio so it
      // is not itself the delete-heavy segment the trigger goes looking for.
      engine.remove(DIM, deleted);
      fill(engine, 300L, 17);
      engine.flush();
      assertThat(topKRids(engine)).doesNotContain(deleted);

      // S4 (tier 0: 2 postings, 100% tombstones for RIDs that live nowhere) is past the 30% ratio
      // trigger. The old selector paired it with the two globally oldest segments S1 and S2,
      // skipping S3 - the one holding the tombstone. The window is now S2..S4.
      engine.remove(DIM, new RID(0, 900L));
      engine.remove(DIM, new RID(0, 901L));
      engine.flush();

      assertThat(engine.segmentCount())
          .as("the tombstone-ratio window must have merged 3 of the 4 segments")
          .isEqualTo(2);
      assertThat(engine.segmentEpochs()).isSorted();
      assertThat(topKRids(engine))
          .as("the tombstone-ratio trigger must not resurrect a RID whose tombstone it left out")
          .doesNotContain(deleted);
    }
  }

  /**
   * The tombstone-ratio window reaches backwards from the offender, so when the offender is within
   * {@code tierFanout - 1} of the <i>oldest</i> end there are not enough older neighbours and the
   * window starts at 0 and extends forwards past it instead. That is the one case where the window
   * holds something newer than the offender, and it is the branch the (removed) upper clamp was
   * nominally guarding: pin that it still produces an in-bounds window and still resolves a
   * tombstone held by a segment newer than the offender.
   */
  @Test
  void tombstoneRatioWindowAtTheOldestEndKeepsDeletedRidInvisible() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final RID deleted = new RID(0, DELETED_POSITION);
    try (final PaginatedSparseVectorEngine engine = tieredEngine(db, "TombstoneOldestEndTest", 3, 2L)) {
      // S1 (tier 0: 2 postings, 50% tombstones) is the offender AND the oldest segment, so the
      // window cannot reach back and starts at 0.
      engine.put(DIM, deleted, 0.9f);
      engine.remove(DIM, new RID(0, 900L));
      engine.flush();
      // S2 (tier 1: 6 postings): filler.
      fill(engine, 200L, 6);
      engine.flush();
      // S3 (tier 2: 18 postings): holds the tombstone for the RID S1 inserted, at a ratio well
      // under the trigger so S1 stays the offender.
      engine.remove(DIM, deleted);
      fill(engine, 300L, 17);
      engine.flush();

      assertThat(engine.segmentCount())
          .as("the window starting at 0 must span all three segments")
          .isEqualTo(1);
      assertThat(topKRids(engine))
          .as("a window that extends forwards must still let the newer tombstone win")
          .doesNotContain(deleted);
    }
  }

  /**
   * The same inversion applied to an <i>update</i>: a younger segment lowers a posting's weight,
   * and a partial merge that leaves it out must not restore the stale weight.
   */
  @Test
  void sizeTieredPartialCompactionKeepsNewestWeight() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final RID updated = new RID(0, DELETED_POSITION);
    try (final PaginatedSparseVectorEngine engine = tieredEngine(db, "SizeTieredWeightRecencyTest", 4, 2L)) {
      engine.put(DIM, updated, 0.9f);
      fill(engine, 100L, 39);
      engine.flush();

      // Tier-0 single-posting segment carrying the new, lower weight.
      engine.put(DIM, updated, 0.1f);
      engine.flush();

      for (int s = 0; s < 4; s++) {
        fill(engine, 1_000L + s * 100L, 40);
        engine.flush();
      }

      assertThat(scoreOf(engine.topK(new int[] { DIM }, new float[] { 1.0f }, 512), updated))
          .as("a partial compaction must not restore the stale weight of a RID it re-merged")
          .isEqualTo(0.1f, within(0.02f));
    }
  }

  /**
   * The recency epoch is what makes the ordering survive a reopen - and on HA, a follower that
   * rebuilds its snapshot purely from the shipped files depends on nothing else. Pins that a
   * merged segment persists an epoch below its own segment id and that the order it implies still
   * holds after the engine is closed and re-opened from disk.
   */
  @Test
  void mergedSegmentRecencyEpochSurvivesReopen() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final RID deleted = new RID(0, DELETED_POSITION);
    try (final PaginatedSparseVectorEngine engine = nonCompactingEngine(db, "RecencyEpochReopenTest")) {
      engine.put(DIM, deleted, 0.9f);
      fill(engine, 100L, 4);
      engine.flush();
      fill(engine, 200L, 5);
      engine.flush();
      fill(engine, 300L, 5);
      engine.flush();
      engine.remove(DIM, deleted);
      engine.flush();
      engine.compactOldest(3);
      assertThat(engine.segmentEpochs()).containsExactly(3L, 4L);
    }

    try (final PaginatedSparseVectorEngine reopened = nonCompactingEngine(db, "RecencyEpochReopenTest")) {
      assertThat(reopened.segmentEpochs())
          .as("the persisted recency epochs must round-trip through the manifest, in order")
          .containsExactly(3L, 4L);
      assertThat(reopened.segmentIds())
          .as("the merged segment keeps its own, higher, file id across the reopen")
          .containsExactly(4L, 5L);
      assertThat(topKRids(reopened))
          .as("the deleted RID must stay invisible across a reopen")
          .doesNotContain(deleted);
    }
  }

  /**
   * Restricting the automatic triggers to adjacent runs introduces a way to stall: a size
   * distribution that interleaves tiers never grows an adjacent same-tier run, so without the
   * shape guard the segment count would climb forever. Alternating small and large flushes is
   * exactly that distribution; the count must stay bounded and every live RID must survive the
   * merges the guard performs.
   */
  @Test
  void interleavedTiersStillCompactUnderTheShapeGuard() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    try (final PaginatedSparseVectorEngine engine = tieredEngine(db, "ShapeGuardTest", 2, 2L)) {
      final List<RID> expected = new ArrayList<>();
      long next = 100L;
      for (int i = 0; i < 12; i++) {
        // Alternating 2 and 4 postings: tier 0 and tier 1 with fanout 2, so no two adjacent
        // segments ever share a tier.
        final int count = (i % 2 == 0) ? 2 : 4;
        for (int j = 0; j < count; j++)
          expected.add(new RID(0, next + j));
        fill(engine, next, count);
        next += count;
        engine.flush();
      }

      assertThat(engine.segmentCount())
          .as("the shape guard must keep an interleaved index from accumulating a segment per flush")
          .isLessThan(12);
      assertThat(engine.segmentEpochs()).isSorted();
      assertThat(topKRids(engine))
          .as("every live posting must survive the guard's merges")
          .containsAll(expected);
    }
  }

  /**
   * {@link PaginatedSparseVectorEngine#contiguousClosure} is the safety net the whole "a future
   * policy can only over-merge, never corrupt" claim rests on, and no production selector can
   * reach its widening branch - every one of them already returns a run. Exercise it directly, so
   * the fallback is pinned rather than merely asserted in a comment.
   */
  @Test
  void contiguousClosureWidensAGappedPick() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    try (final PaginatedSparseVectorEngine engine = nonCompactingEngine(db, "ContiguousClosureTest")) {
      for (int i = 0; i < 4; i++) {
        fill(engine, 100L + i * 10L, 3);
        engine.flush();
      }
      final PaginatedSegmentReader[] active = engine.segmentsForTest();
      assertThat(active).hasSize(4);

      // A pick that jumps over the two middle segments must come back as the whole run, so the
      // merged segment cannot inherit an epoch that leapfrogs what it skipped.
      assertThat(engine.contiguousClosure(active, new PaginatedSegmentReader[] { active[0], active[3] }))
          .as("a gapped pick must widen to the contiguous run that spans it")
          .containsExactly(active[0], active[1], active[2], active[3]);

      // An already-contiguous pick is returned untouched, oldest first - the common case, and the
      // reason widening never costs anything in practice.
      assertThat(engine.contiguousClosure(active, new PaginatedSegmentReader[] { active[2], active[1] }))
          .as("an already-contiguous pick must survive unchanged and oldest-first")
          .containsExactly(active[1], active[2]);

      // A pick naming a segment that is not in the live snapshot is a programming error: merging
      // it would drop a component the engine never owned.
      final PaginatedSegmentReader[] shortSnapshot = { active[0], active[1] };
      assertThatThrownBy(() -> engine.contiguousClosure(shortSnapshot, new PaginatedSegmentReader[] { active[0], active[3] }))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("must come from the live snapshot");
    }
  }

  /**
   * {@link SparseSegmentBuilder#setSegmentId} re-defaults the epoch to the id, so the two setters
   * are order-dependent. Pin that the wrong order fails loudly instead of silently discarding the
   * epoch - a merged segment quietly carrying its own id as its epoch is precisely the #6379 bug
   * the field exists to prevent, and it would show up as resurrected documents rather than as
   * anything traceable to the builder.
   */
  @Test
  void setRecencyEpochBeforeSetSegmentIdIsRejected() {
    final DatabaseInternal db = (DatabaseInternal) database;
    database.transaction(() -> {
      final SparseSegmentComponent component = newComponent(db, "recency-order-guard");
      try (final SparseSegmentBuilder b = new SparseSegmentBuilder(component, SegmentParameters.defaults())) {
        assertThatThrownBy(() -> b.setRecencyEpoch(7L))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("must be called before setRecencyEpoch");

        // The documented order is accepted and the explicit epoch wins over the id default.
        b.setSegmentId(9L);
        b.setRecencyEpoch(7L);
      }
    });
  }

  private static SparseSegmentComponent newComponent(final DatabaseInternal db, final String name) {
    try {
      final SparseSegmentComponent c = new SparseSegmentComponent(db, name, db.getDatabasePath() + "/" + name,
          ComponentFile.MODE.READ_WRITE, SparseSegmentComponent.DEFAULT_PAGE_SIZE);
      ((LocalSchema) db.getSchema().getEmbedded()).registerFile(c);
      return c;
    } catch (final IOException e) {
      throw new RuntimeException("failed to create sparse segment component '" + name + "'", e);
    }
  }

  /**
   * The path a Raft follower actually rebuilds its snapshot through is
   * {@link PaginatedSparseVectorEngine#refreshSegmentsFromFileManager}, not the open-time scan: the
   * merged component arrives as a shipped file and the engine reconciles against the FileManager on
   * its next query. It has to arrive at the same precedence the leader published, from the files
   * alone, which is the whole reason the epoch is persisted rather than derived.
   * <p>
   * Reproduced without an HA harness by holding a second engine open over the same index while the
   * first one compacts: the second sees the merged file appear and the inputs vanish exactly as a
   * follower does, and its next query drives the reconcile.
   */
  @Test
  void followerRefreshRebuildsPrecedenceFromTheShippedFiles() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final RID deleted = new RID(0, DELETED_POSITION);
    try (final PaginatedSparseVectorEngine leader = nonCompactingEngine(db, "FollowerRefreshRecencyTest")) {
      leader.put(DIM, deleted, 0.9f);
      fill(leader, 100L, 4);
      leader.flush();
      fill(leader, 200L, 5);
      leader.flush();
      fill(leader, 300L, 5);
      leader.flush();
      leader.remove(DIM, deleted);
      leader.flush();

      // Opened before the compaction, so its snapshot holds the four pre-merge segments and it can
      // only learn about the merge the way a follower does - by reconciling against the FileManager.
      try (final PaginatedSparseVectorEngine follower = nonCompactingEngine(db, "FollowerRefreshRecencyTest")) {
        assertThat(follower.segmentCount()).isEqualTo(4);
        assertThat(topKRids(follower)).doesNotContain(deleted);

        leader.compactOldest(3);

        // The query is what triggers the reconcile; the follower must land on the same order the
        // leader holds, with the merged segment behind the tombstone despite owning the higher id.
        assertThat(topKRids(follower))
            .as("a follower rebuilding from shipped files must not resurrect the deleted RID")
            .doesNotContain(deleted);
        assertThat(follower.segmentCount()).isEqualTo(2);
        assertThat(follower.segmentEpochs())
            .as("the follower's precedence must match the leader's, reconstructed from the manifests alone")
            .containsExactly(leader.segmentEpochs());
        assertThat(follower.segmentIds()).containsExactly(4L, 5L);
      }
    }
  }

  /**
   * A segment merged before #6379 keeps its old ordering on disk after the upgrade, and nothing
   * about the index looks wrong: it answers queries happily while a deleted document is visible
   * again. The open-time scan is the only thing that tells an operator a {@code REBUILD INDEX} is
   * what fixes it, so pin both that it fires on a legacy-shaped segment and that it stays quiet
   * for segments written by this version.
   * <p>
   * "Legacy-shaped" is synthesised the way the old code produced it: parents recorded, epoch left
   * at the segment id. The detection is exact - a merge written today always inherits an epoch
   * strictly below its own id, and a flush has no parents - so neither half can false-positive.
   */
  @Test
  void preRecencyEpochMergedSegmentIsReportedAtOpen() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;

    // A healthy index built entirely by this version: flushes plus a real compaction.
    try (final PaginatedSparseVectorEngine engine = nonCompactingEngine(db, "LegacyScanCleanTest")) {
      for (int i = 0; i < 3; i++) {
        fill(engine, 100L + i * 10L, 3);
        engine.flush();
      }
      engine.compactOldest(3);
    }
    assertThat(warningsWhileOpening(db, "LegacyScanCleanTest"))
        .as("an index whose merges all carry an inherited epoch must not nag the operator")
        .noneMatch(w -> w.contains("#6379"));

    // Now the legacy shape: a segment that records parents but never had its epoch set, exactly
    // what a pre-fix merge left on disk.
    database.transaction(() -> {
      final SparseSegmentComponent component = newComponent(db, "LegacyScanStaleTest_seg1");
      try (final SparseSegmentBuilder b = new SparseSegmentBuilder(component, SegmentParameters.defaults())) {
        b.setSegmentId(1L);
        // 0 is what a pre-fix builder left in the slot; setSegmentId defaults it to the id, so the
        // fixture has to put it back to reproduce the on-disk shape rather than merely resemble it.
        b.setRecencyEpoch(0L);
        b.setParentSegments(new long[] { 90L, 91L });
        b.startDim(DIM);
        b.appendPosting(new RID(0, 500L), 0.5f);
        b.endDim();
        b.finish();
      }
    });
    // No "Run 'REBUILD INDEX ...'" clause: this class only knows the physical per-bucket sub-index
    // name, not the logical one the command actually takes, so it leaves the remedy to
    // LocalSchema.reportUpgradeWarning() (PR #6720 review) - not exercised here, which opens the
    // engine directly rather than through a full database/schema load.
    assertThat(warningsWhileOpening(db, "LegacyScanStaleTest"))
        .as("a segment with parents whose epoch equals its id predates the fix and must be reported")
        .anyMatch(w -> w.contains("#6379") && !w.contains("REBUILD INDEX"));
  }

  /**
   * The warning has to survive the index continuing to be used. A legacy segment stops looking like
   * one the moment an ordinary compaction absorbs it - the result has a fresh id, so the shape that
   * identified it no longer matches - while the suspect ordering rides along inside it unchanged.
   * Since {@code flush()} cascades the size-tiered trigger on every call, that can happen within
   * the first few writes after an upgrade, which is exactly when nobody has read the log yet.
   * <p>
   * Drives that sequence for real: seal a legacy-shaped segment, let automatic compaction consume
   * it, and require the next open to still say so.
   */
  @Test
  void legacyEpochDoubtSurvivesBeingAbsorbedByCompaction() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;

    // A legacy merge: parents recorded, epoch never set, so it reads back as epoch == id.
    database.transaction(() -> {
      final SparseSegmentComponent component = newComponent(db, "LegacyAbsorbTest_seg1");
      try (final SparseSegmentBuilder b = new SparseSegmentBuilder(component, SegmentParameters.defaults())) {
        b.setSegmentId(1L);
        b.setRecencyEpoch(0L);   // the pre-fix on-disk shape: slot never written
        b.setParentSegments(new long[] { 90L, 91L });
        b.startDim(DIM);
        for (int i = 0; i < 10; i++)
          b.appendPosting(new RID(0, 500L + i), 0.5f);
        b.endDim();
        b.finish();
      }
    });
    assertThat(warningsWhileOpening(db, "LegacyAbsorbTest"))
        .as("the legacy segment is reported before anything absorbs it")
        .anyMatch(w -> w.contains("#6379"));

    // Ordinary post-upgrade write traffic. fanout 2 with a matching tier makes the size-tiered
    // trigger fire on the flush that follows, consuming the legacy segment into a merge whose id is
    // freshly allocated - the exact step that used to launder the doubt away.
    try (final PaginatedSparseVectorEngine engine = tieredEngine(db, "LegacyAbsorbTest", 2, 2L)) {
      assertThat(engine.segmentCount()).isEqualTo(1);
      fill(engine, 700L, 10);
      engine.flush();
      assertThat(engine.segmentCount())
          .as("the size-tiered trigger must have merged the legacy segment away")
          .isEqualTo(1);
      assertThat(engine.segmentIds())
          .as("and the merged segment must own a new id, so the old shape no longer identifies it")
          .doesNotContain(1L);
    }

    assertThat(warningsWhileOpening(db, "LegacyAbsorbTest"))
        .as("the doubt must be inherited by the merge, not erased by it")
        .anyMatch(w -> w.contains("#6379"));
  }

  /** Open an engine over {@code indexName} with a capturing logger installed, and return what it warned. */
  private static List<String> warningsWhileOpening(final DatabaseInternal db, final String indexName) {
    final List<String> warnings = Collections.synchronizedList(new ArrayList<>());
    final Logger original = LogManager.instance().getLogger();
    LogManager.instance().setLogger(new CapturingLogger(warnings, original));
    try (final PaginatedSparseVectorEngine ignored = nonCompactingEngine(db, indexName)) {
      // Opening is the whole point; the scan runs in loadExistingSegments.
    } finally {
      LogManager.instance().setLogger(original);
    }
    return warnings;
  }

  /**
   * Captures WARNING-and-above messages while forwarding everything to the real logger. The
   * fixed-arity overload must NOT delegate to the varargs one: the fixed arity is the exact match,
   * so it would recurse until the stack dies.
   * <p>
   * {@link LogManager#setLogger} is process-wide, so this relies on the engine module running its
   * test classes sequentially (Surefire {@code forkCount=1}, {@code reuseForks=true}, no JUnit
   * parallel config). The capture is scoped to a single engine open and restored in a
   * {@code finally}, so a concurrent test would see at worst a forwarded log line rather than a
   * lost one - but the assertions here would become flaky if class-level parallelism were ever
   * enabled for this module.
   */
  private static final class CapturingLogger implements Logger {
    private final List<String> warnings;
    private final Logger       delegate;

    CapturingLogger(final List<String> warnings, final Logger delegate) {
      this.warnings = warnings;
      this.delegate = delegate;
    }

    private void capture(final Level level, final String message, final Object... args) {
      if (message == null || level.intValue() < Level.WARNING.intValue())
        return;
      String formatted = message;
      if (args != null && args.length > 0) {
        try {
          formatted = message.formatted(args);
        } catch (final Exception ignored) {
          // Raw template is good enough for the substring matching the assertions do.
        }
      }
      warnings.add(formatted);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4,
        final Object arg5, final Object arg6, final Object arg7, final Object arg8, final Object arg9,
        final Object arg10, final Object arg11, final Object arg12, final Object arg13, final Object arg14,
        final Object arg15, final Object arg16, final Object arg17) {
      capture(level, message, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14,
          arg15, arg16, arg17);
      delegate.log(requester, level, message, exception, context, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9,
          arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object... args) {
      capture(level, message, args);
      delegate.log(requester, level, message, exception, context, args);
    }

    @Override
    public void flush() {
      delegate.flush();
    }
  }

  private static PaginatedSparseVectorEngine tieredEngine(final DatabaseInternal db, final String name,
      final int tierFanout, final long tierBasePostings) {
    return new PaginatedSparseVectorEngine(db, name, SegmentParameters.defaults(),
        /* memtableFlushThreshold */ 100_000L, tierFanout, tierBasePostings);
  }

  /** An engine whose automatic compaction gate cannot fire, so a test drives compaction by hand. */
  private static PaginatedSparseVectorEngine nonCompactingEngine(final DatabaseInternal db, final String name) {
    return new PaginatedSparseVectorEngine(db, name, SegmentParameters.defaults(),
        /* memtableFlushThreshold */ 100_000L,
        /* tierFanout */ 1_000_000,
        /* tierBasePostings */ 1L);
  }

  /** Append {@code count} unrelated live postings starting at position {@code firstPosition}. */
  private static void fill(final PaginatedSparseVectorEngine engine, final long firstPosition, final int count) {
    for (int i = 0; i < count; i++)
      engine.put(DIM, new RID(0, firstPosition + i), 0.5f);
  }

  private static List<RID> topKRids(final PaginatedSparseVectorEngine engine) throws Exception {
    final List<RidScore> hits = engine.topK(new int[] { DIM }, new float[] { 1.0f }, 512);
    final List<RID> out = new ArrayList<>(hits.size());
    for (final RidScore h : hits)
      out.add(h.rid());
    return out;
  }

  private static float scoreOf(final List<RidScore> hits, final RID rid) {
    for (final RidScore h : hits)
      if (rid.equals(h.rid()))
        return h.score();
    throw new AssertionError("RID " + rid + " is missing from the result set");
  }
}
