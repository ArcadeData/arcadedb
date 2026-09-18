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
import com.arcadedb.index.IndexReplayUndo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * What one transaction's commit replay published into an {@link LSMVectorIndex}'s process-wide in-memory state, so
 * that a rollback can take it back (issue #7931).
 * <p>
 * This is a RECORD, not behaviour: {@link LSMVectorIndex#undoReplay(VectorIndexReplayUndo)} owns the reversal,
 * because reversing it needs the index's write lock and its private fields. The journal is filled in by the three
 * replay entry points ({@code put}, {@code putBatch} and {@code remove}) as they go.
 * <p>
 * <b>Why it is recorded rather than derived.</b> Issue #7931 suggested rebuilding the compensation set from the
 * transaction's own queued index operations. That is not enough: a queued REMOVE carries only a RID and a dummy
 * key, and the vector ids it tombstoned were resolved from the location index AT REPLAY TIME - and then released
 * by the very tombstone, so they can no longer be read back from it afterwards. Nor do the queued operations say
 * which ids the replay allocated, or which delta entries it dropped. Recording as we go is the only way the set is
 * exact, and it also costs nothing on the paths that never abort.
 * <p>
 * <b>Thread confinement.</b> A transaction belongs to one thread, and both the filling and the reversal happen on
 * it, so nothing here is synchronized. The reversal additionally runs while the transaction still holds the index
 * file's commit lock, which is what keeps a concurrent transaction from having moved the same ids meanwhile.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class VectorIndexReplayUndo implements IndexReplayUndo {
  private final LSMVectorIndex index;

  // Primitive arrays rather than lists of boxes: a bulk load replays thousands of operations per transaction, and
  // this has to stay off the garbage collector's back on the path that DOES commit, where the journal is simply
  // dropped at the end.
  int[] allocatedIds = null;
  int   allocatedCount;

  int[]  tombstonedIds           = null;
  long[] tombstonedOffsetAndFlag = null;
  RID[]  tombstonedRids          = null;
  int    tombstonedCount;

  /** The delta-buffer entries this replay's deletes dropped, which have to go back in. Null until one is dropped. */
  List<LSMVectorIndex.DeltaVectorEntry> droppedDeltaEntries = null;

  /** What this replay added to {@code mutationsSinceSerialize}. */
  int mutationsCharged;

  /** How many data pages this replay created, all of which go away with the transaction's own pages. */
  int mutablePagesCreated;

  /** The state this replay flipped the graph away from, or null if it found it already MUTABLE. */
  LSMVectorIndex.GraphState graphStateFlippedFrom = null;

  /**
   * The location index the replay wrote into. Compared by IDENTITY at undo time: a compaction or a rebuild
   * republishes {@code residentLocations} wholesale, from the COMMITTED pages, and the replacement therefore
   * never saw this transaction's entries at all. Compensating id by id against a replacement would be worse than
   * unnecessary - the offsets recorded here address the file the compaction has already replaced.
   */
  final VectorLocationIndex locationsAtReplay;

  VectorIndexReplayUndo(final LSMVectorIndex index, final VectorLocationIndex locationsAtReplay) {
    this.index = index;
    this.locationsAtReplay = locationsAtReplay;
  }

  /** An id this replay allocated: its location, its delta entry and its mutation have to go away on abort. */
  void recordAllocated(final int id) {
    if (allocatedIds == null)
      allocatedIds = new int[8];
    else if (allocatedCount == allocatedIds.length)
      allocatedIds = Arrays.copyOf(allocatedIds, allocatedCount * 2);
    allocatedIds[allocatedCount++] = id;
  }

  /**
   * An id this replay tombstoned, together with the location it held. Both are needed: {@code markDeleted} releases
   * the location, so un-tombstoning has to put the exact offset back rather than leave the id live-but-unaddressed.
   *
   * @param id             the tombstoned vector id
   * @param offsetAndFlag  its packed location, read BEFORE the tombstone released it
   * @param rid            the record it pointed at
   */
  void recordTombstoned(final int id, final long offsetAndFlag, final RID rid) {
    if (tombstonedIds == null) {
      tombstonedIds = new int[8];
      tombstonedOffsetAndFlag = new long[8];
      tombstonedRids = new RID[8];
    } else if (tombstonedCount == tombstonedIds.length) {
      tombstonedIds = Arrays.copyOf(tombstonedIds, tombstonedCount * 2);
      tombstonedOffsetAndFlag = Arrays.copyOf(tombstonedOffsetAndFlag, tombstonedCount * 2);
      tombstonedRids = Arrays.copyOf(tombstonedRids, tombstonedCount * 2);
    }
    tombstonedIds[tombstonedCount] = id;
    tombstonedOffsetAndFlag[tombstonedCount] = offsetAndFlag;
    tombstonedRids[tombstonedCount] = rid;
    ++tombstonedCount;
  }

  void recordDroppedDelta(final LSMVectorIndex.DeltaVectorEntry entry) {
    if (droppedDeltaEntries == null)
      droppedDeltaEntries = new ArrayList<>();
    droppedDeltaEntries.add(entry);
  }

  /** Only the FIRST flip is recorded: that is the state the index was in before this transaction touched it. */
  void recordGraphFlip(final LSMVectorIndex.GraphState previous) {
    if (graphStateFlippedFrom == null)
      graphStateFlippedFrom = previous;
  }

  @Override
  public void undoIndexReplay() {
    index.undoReplay(this);
  }
}
