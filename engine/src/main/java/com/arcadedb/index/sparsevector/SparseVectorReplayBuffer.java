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

import com.arcadedb.database.RID;
import com.arcadedb.index.IndexReplayConclusion;

import java.util.Arrays;

/**
 * The postings one transaction's commit replay produced for an {@link LSMSparseVectorIndex}, held back until the
 * transaction's conclusion is known (issue #7933).
 * <p>
 * <b>Why they are held back rather than published and undone.</b> {@code TransactionContext.commit1stPhase()}
 * replays the queued index operations BEFORE it validates the page versions, so a transaction that then loses the
 * MVCC check has already carried out every one of its index operations. The sparse index writes those straight into
 * the engine's shared {@link Memtable} - an instance field of {@link PaginatedSparseVectorEngine} that every
 * transaction on the index writes to - and a rollback cannot reach it. A phantom posting from a rolled-back insert
 * surfaces in {@code topK}; a phantom TOMBSTONE from a rolled-back delete or vector rewrite is worse, because the
 * scorer reads a tombstone-aligned cursor as a whole-document delete and a live document silently vanishes from
 * every query mentioning that dim.
 * <p>
 * The sibling defect in {@code LSMVectorIndex} (issue #7931) was fixed by publishing during the replay and
 * journalling enough to take it back. That answer does NOT carry over here, and the reason is the flush: the
 * memtable seals itself into a {@code .sparseseg} segment on its own schedule, on whatever thread's post-commit
 * callback reaches the threshold first, so an eagerly-published posting can be baked into an immutable on-disk
 * segment in the window between the replay and the rollback. No in-memory compensation can reach a sealed segment,
 * and a compensation that rewrote one would have to rewrite an immutable file that concurrent queries are reading.
 * Publishing late removes the window instead of trying to cover it.
 * <p>
 * <b>What deferring costs.</b> Nothing in visibility: the replay is already the first moment a queued posting
 * becomes visible to a query, and the publication now happens later in the same commit, on the same thread, with
 * the same locks held. It also makes the visibility
 * HONEST, since a posting is no longer observable for a transaction that turns out never to have committed. The
 * cost is this buffer, which holds the transaction's postings from the replay to the conclusion, in parallel
 * primitive arrays that allocate nothing per posting.
 * <p>
 * <b>Why nothing here is synchronized.</b> Not because a transaction is strictly thread-confined - the split commit
 * of issue #6965 can conclude the 2nd phase on the Raft apply thread ({@code RaftReplicatedDatabase} calls
 * {@code completeCommit()} / {@code concludeFailedPhase2()}) - but because the buffer is never touched by two
 * threads at once and the handoff that gives that thread this transaction carries the happens-before edge for
 * everything the replay recorded: the originating thread is parked on the acknowledgement while it works. The same
 * argument {@code VectorIndexReplayUndo} makes for its journal.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class SparseVectorReplayBuffer implements IndexReplayConclusion {
  private static final int INITIAL_CAPACITY = 16;

  private final PaginatedSparseVectorEngine engine;

  // Parallel primitive arrays rather than a list of tuples: one bulk-load transaction defers one entry per non-zero
  // dimension of every record it writes, which on a learned-sparse corpus is hundreds per record, and this has to
  // stay off the garbage collector's back on the path that DOES commit - where the whole buffer is walked once and
  // dropped. Storing the fields rather than a marker object also serves the OTHER entry point: an index write
  // issued during the commit itself never gets a SparsePostingReplayKey, and would have to allocate one to be
  // recorded.
  //
  // A NaN weight is the REMOVE: it is the same sentinel the Memtable itself stores for a tombstone, and
  // PaginatedSparseVectorEngine.put rejects NaN outright, so no real weight can ever be mistaken for one.
  private int[]   dims    = new int[INITIAL_CAPACITY];
  private RID[]   rids    = new RID[INITIAL_CAPACITY];
  private float[] weights = new float[INITIAL_CAPACITY];
  private int     size;

  /**
   * Set by whichever of the two conclusions ran. Neither is idempotent - publishing twice would re-apply a
   * tombstone the index may since have overwritten - and the guard is here as well as in {@code TransactionContext}
   * because a journal outlives the map that held it for as long as a stack frame keeps a reference to it.
   */
  private boolean concluded;

  SparseVectorReplayBuffer(final PaginatedSparseVectorEngine engine) {
    this.engine = engine;
  }

  /**
   * Records one posting the commit is holding back. A {@code remove} keeps no weight: the engine's delete primitive
   * takes none.
   * <p>
   * The guard is a tripwire, not a supported case: a posting arriving after the conclusion belongs to a transaction
   * whose buffer has already been published or dropped, so it would be silently lost. Named here rather than left
   * to the {@link NullPointerException} the released arrays would raise, because the name is what tells the next
   * reader which invariant broke.
   */
  void record(final int dim, final RID rid, final float weight, final boolean isRemove) {
    if (concluded)
      throw new IllegalStateException(
          "a sparse vector posting was replayed after its transaction had already concluded; it would be lost");
    if (size == dims.length) {
      dims = Arrays.copyOf(dims, size * 2);
      rids = Arrays.copyOf(rids, size * 2);
      weights = Arrays.copyOf(weights, size * 2);
    }
    dims[size] = dim;
    rids[size] = rid;
    weights[size] = isRemove ? Float.NaN : weight;
    ++size;
  }

  /**
   * The transaction's changes stand. Replays the buffer into the engine in the exact order it was recorded, which
   * is the order {@code TransactionIndexContext}'s append-only lane replayed it in - and that order is the whole
   * correctness argument for the lane: the last operation on a given {@code (dim, rid)} wins by construction, so
   * an insert-then-delete ends tombstoned and a delete-then-reinsert ends live. See {@link SparsePostingReplayKey}.
   */
  @Override
  public void publishIndexReplay() {
    if (concluded)
      return;
    concluded = true;

    for (int i = 0; i < size; i++)
      if (Float.isNaN(weights[i]))
        engine.remove(dims[i], rids[i]);
      else
        engine.put(dims[i], rids[i], weights[i]);

    release();
  }

  /**
   * The transaction rolled back. Nothing was ever published, so there is nothing to reverse - dropping the buffer
   * IS the compensation, and that is the point of deferring in the first place.
   */
  @Override
  public void undoIndexReplay() {
    if (concluded)
      return;
    concluded = true;
    release();
  }

  /**
   * Frees the arrays at the conclusion. Nulling them rather than only resetting the size is what keeps a
   * transaction's RIDs from being held alive by a context that is pooled and reused.
   */
  private void release() {
    dims = null;
    rids = null;
    weights = null;
    size = 0;
  }
}
