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
import com.arcadedb.index.IndexReplayConclusion;
import com.arcadedb.utility.IntHashSet;
import io.github.jbellis.jvector.vector.types.VectorFloat;

import java.util.Arrays;

/**
 * What one transaction's commit replay did to an {@link LSMVectorIndex}, and what it holds back from other readers until
 * the commit concludes (issues #7931 and #11962).
 * <p>
 * This is a RECORD, not behaviour: {@link LSMVectorIndex#publishReplay(VectorIndexReplayUndo)} owns the publication and
 * {@link LSMVectorIndex#undoReplay(VectorIndexReplayUndo)} the reversal, because both need the index's write lock and its
 * private fields. The journal is filled in by the three replay entry points ({@code put}, {@code putBatch} and
 * {@code remove}) as they go.
 * <p>
 * <b>What the replay does and what it holds back.</b> The replay allocates vector ids and writes them ONTO the
 * transaction's own index pages, so the allocation has to be decided while those pages are being written, not after: the
 * ids, the page entries, the mutation counter and the graph state are the replay's. What other readers see of a vector -
 * its location in the location index and its entry in the delta buffer, and the tombstone of a vector a REMOVE retires -
 * is held here and published by {@link #publishIndexReplay()} once the transaction's pages are (issue #11962). Published
 * at replay, as it used to be, a search in the window - the whole quorum round trip on a Raft leader - answered the RID of
 * a record nobody could read yet, or of one whose commit was then refused, and missed a record an update had already
 * retired the committed vector of; and the rollback had to take each of those back from under whatever a rebuild had
 * republished meanwhile (issue #7931). The sparse index holds its postings back the same way (issue #7933).
 * <p>
 * The records the replay puts are also marked in flight in the index until the transaction concludes (issue #13323), so a
 * search that hands its rows on without reading the records reads those of a commit in flight.
 * <p>
 * <b>Why it is recorded rather than derived.</b> A queued REMOVE carries only a RID, and the vector ids it retires are
 * resolved from the location index AT REPLAY TIME, which the queued operations cannot say afterwards; nor do they say
 * which ids the replay allocated or where it wrote them. Recording as we go is the only way the set is exact.
 * <p>
 * <b>Why nothing here is synchronized.</b> Not because a transaction is strictly thread-confined - the split
 * commit of issue #6965 concludes the 2nd phase on the Raft apply thread ({@code RaftReplicatedDatabase} calls
 * {@code completeCommit()} / {@code concludeFailedPhase2()} there), and either conclusion can run there. It is because
 * the journal is never touched by two threads at once and the handoff that gives the apply thread this transaction
 * carries the happens-before edge for everything the replay wrote: the originating thread is parked on the
 * acknowledgement while that thread works.
 * <p>
 * Both conclusions run while the transaction still holds the index file's commit lock, which is what keeps a concurrent
 * transaction - and a compaction, which takes the same lock - from having moved the same ids meanwhile. A full rebuild is
 * not covered by that lock and can republish the locations in between: the publication is applied to the generation in
 * force when it runs, and a rebuild that read the pages before them carries it over (see
 * {@code LSMVectorIndex.publishLocationIndex}).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class VectorIndexReplayUndo implements IndexReplayConclusion {
  private final LSMVectorIndex index;

  // Primitive arrays rather than lists of boxes: a bulk load replays thousands of operations per transaction, and
  // this has to stay off the garbage collector's back on the path that DOES commit.
  /** The vectors this replay added: published as one location and one delta entry each when the commit stands. */
  int[]            addedIds     = null;
  RID[]            addedRids    = null;
  long[]           addedOffsets = null;
  VectorFloat<?>[] addedVectors = null;
  int              addedCount;

  /** The vectors this replay's REMOVEs retire: tombstoned, and dropped from the delta buffer, when the commit stands. */
  int[]      retiredIds  = null;
  RID[]      retiredRids = null;
  int        retiredCount;
  /**
   * The same ids, for the membership test a REMOVE replayed twice for one RID asks: the ordered lane can replay it twice when a
   * same-transaction rewrite collapses into one queued key, and the location still calls the id live until the publication.
   */
  IntHashSet retiredSet  = null;

  /**
   * How many records one replay marks in flight one by one. Past it the replay marks the whole index instead, so a bulk load does
   * not hold a map entry per record for the length of its commit: searches then read every record until it concludes, as they did
   * before issue #10973.
   */
  static final int MAX_TRACKED_PUTS = 4096;

  /**
   * The records whose vector this replay put, marked in flight in the index until the transaction concludes (issue #13323), at
   * most {@link #MAX_TRACKED_PUTS}. Held here so the conclusion ends exactly the marks this replay raised.
   */
  private RID[]   putRids = null;
  private int     putCount;
  /** Whether this replay put more records than it tracks, and marked the whole index in flight instead. */
  private boolean bulkPut;

  /** What this replay added to {@code mutationsSinceSerialize}. */
  int mutationsCharged;

  /** How many data pages this replay created, all of which go away with the transaction's own pages. */
  int mutablePagesCreated;

  /** The state this replay flipped the graph away from, or null if it found it already MUTABLE. */
  LSMVectorIndex.GraphState graphStateFlippedFrom = null;

  /**
   * Set by the one conclusion this journal answers. A second one would publish or refund a second time - silently, since
   * neither is idempotent - so it is refused loudly instead. The throw is safe: {@code TransactionContext} catches it and
   * logs, precisely so a failure here cannot stop the commit or the rollback from releasing its file locks.
   */
  private boolean concluded;

  VectorIndexReplayUndo(final LSMVectorIndex index) {
    this.index = index;
  }

  /**
   * A vector this replay added: the id it allocated, the record, the absolute file offset of the page entry it wrote, and
   * the vector the delta scan scores until a rebuild takes it into the graph.
   */
  void recordAdded(final int id, final RID rid, final long offset, final VectorFloat<?> vector) {
    if (addedIds == null) {
      addedIds = new int[8];
      addedRids = new RID[8];
      addedOffsets = new long[8];
      addedVectors = new VectorFloat<?>[8];
    } else if (addedCount == addedIds.length) {
      addedIds = Arrays.copyOf(addedIds, addedCount * 2);
      addedRids = Arrays.copyOf(addedRids, addedCount * 2);
      addedOffsets = Arrays.copyOf(addedOffsets, addedCount * 2);
      addedVectors = Arrays.copyOf(addedVectors, addedCount * 2);
    }
    addedIds[addedCount] = id;
    addedRids[addedCount] = rid;
    addedOffsets[addedCount] = offset;
    addedVectors[addedCount] = vector;
    ++addedCount;
  }

  /**
   * A record whose vector this replay is about to put: marked in flight in the index before the replay goes on, so a search that
   * answers a vector of the record before its page is published reads the record instead of handing it on unread (issue #13323).
   */
  void recordPut(final RID rid) {
    if (bulkPut)
      return;
    if (putCount == MAX_TRACKED_PUTS) {
      bulkPut = true;
      index.bulkPutInFlight();
      return;
    }
    if (putRids == null)
      putRids = new RID[8];
    else if (putCount == putRids.length)
      putRids = Arrays.copyOf(putRids, putCount * 2);
    putRids[putCount++] = rid;
    index.putInFlight(rid);
  }

  /** Whether a REMOVE of this replay already retired {@code id}. */
  boolean retires(final int id) {
    return retiredSet != null && retiredSet.contains(id);
  }

  /** A vector this replay's REMOVE retires, and the record it belonged to when the replay resolved it. */
  void recordRetired(final int id, final RID rid) {
    if (retiredIds == null) {
      retiredIds = new int[8];
      retiredRids = new RID[8];
      retiredSet = new IntHashSet(16);
    } else if (retiredCount == retiredIds.length) {
      retiredIds = Arrays.copyOf(retiredIds, retiredCount * 2);
      retiredRids = Arrays.copyOf(retiredRids, retiredCount * 2);
    }
    retiredIds[retiredCount] = id;
    retiredRids[retiredCount] = rid;
    ++retiredCount;
    retiredSet.add(id);
  }

  /** Only the FIRST flip is recorded: that is the state the index was in before this transaction touched it. */
  void recordGraphFlip(final LSMVectorIndex.GraphState previous) {
    if (graphStateFlippedFrom == null)
      graphStateFlippedFrom = previous;
  }

  /** The transaction's changes stand: what the replay held back becomes visible to every reader. */
  @Override
  public void publishIndexReplay() {
    conclude("published");
    try {
      index.publishReplay(this);
    } finally {
      // After the vectors are published, and the pages before them: the records this replay put are no longer in flight (issue #13323)
      index.putsConcluded(putRids, putCount, bulkPut);
    }
  }

  /** The transaction rolled back: what it held back is dropped, and what the replay charged the index is refunded. */
  @Override
  public void undoIndexReplay() {
    conclude("undone");
    try {
      index.undoReplay(this);
    } finally {
      // After the vectors are taken back: a search that no longer finds a record marked finds its vector gone
      index.putsConcluded(putRids, putCount, bulkPut);
    }
  }

  private void conclude(final String how) {
    if (concluded)
      throw new IllegalStateException("the replay of index '" + index.getName() + "' was already concluded; it cannot be "
          + how + " a second time");
    concluded = true;
  }
}
