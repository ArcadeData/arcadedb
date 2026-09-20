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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.database.TransactionIndexContext;
import com.arcadedb.database.TransactionIndexContext.ComparableKey;
import com.arcadedb.database.TransactionIndexContext.IndexKey;
import com.arcadedb.database.TransactionIndexContext.IndexKey.IndexKeyOperation;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.vector.LSMVectorIndex.DeltaVectorEntry;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * What one open transaction has written to a dense vector index and not yet committed, in the form the search paths
 * can merge (issue #7378).
 * <p>
 * <b>The defect this closes.</b> Every index entry of a transaction is queued on
 * {@link TransactionIndexContext} while the transaction is {@code BEGUN} and applied to the index only during commit
 * replay - see {@code LSMVectorIndex.isTransactionalCall}. {@code LSMTreeIndex.get()} compensates by merging the
 * queued keys into its answer, so a full-text write is visible to its own transaction immediately. The dense vector
 * search had no such merge, so a caller that inserted a row and searched for it in the same transaction got nothing
 * back until the commit - silently returning fewer results rather than failing, which an application cannot tell
 * apart from "no match".
 * <p>
 * <b>What it contributes, and why exactly this.</b> The overlay is deliberately a model of what
 * {@code TransactionIndexContext.commit()} would apply, entry for entry, so a search inside the transaction and the
 * same search after the commit cannot disagree:
 * <ul>
 *   <li>{@code ADD} and {@code REPLACE} contribute the vector they carry - the two operations {@code commit()}
 *       replays through {@code putBatch}/{@code putReplay}. One row per RID: a transaction that saves the same
 *       record's embedding more than once queues an entry per save, and replaying them all leaves ONE live vector,
 *       because a new location for a RID tombstones the id it supersedes ({@link VectorLocationIndex}'s class
 *       javadoc: "an update tombstones the id it supersedes"). The overlay keeps the last such entry across this index's lanes for
 *       that reason and not as a tidy-up: contributing both would rank the record twice inside the transaction and
 *       once after the commit. "Last" is taken in the lanes' own iteration order, which is the order
 *       {@code commit()} replays them in, so the survivor is the same one either way whatever order the saves
 *       happened in.</li>
 *   <li>{@code REMOVE}, and a {@code REPLACE}'s {@code oldRid}, supersede whatever the committed index holds for
 *       that RID - the two {@code commit()} replays through {@code removeReplay}. The queued key cannot be used for
 *       this: {@code LSMVectorIndex.remove()} has no vector to queue and enqueues a zero-vector placeholder, so a
 *       removal is identified by its RID, which is also the only thing the real {@code remove()} uses
 *       ({@code VectorLocationIndex.getVectorIdsForRid}).</li>
 * </ul>
 * A RID carrying both a {@code REMOVE} and an {@code ADD} - what re-embedding a record inside a transaction
 * produces - is therefore superseded on the committed side and contributed on the pending side, so it appears once,
 * scored on the vector the transaction wrote. That is also the order {@code commit()} applies them in: it replays
 * every {@code REMOVE} of the lane before any {@code ADD} of it, regardless of the order they were queued in.
 * <p>
 * <b>Cost.</b> {@link #open} returns {@code null}, with no allocation at all, for a search on a thread holding no
 * transaction, one still in a status where nothing is queued, or one that has not written to this index. The first
 * two answer in O(1); the third costs the lane lookup described on
 * {@code TransactionIndexContext.getIndexKeys(IndexInternal)}, which walks the lanes of the other indexes this
 * transaction has touched before answering {@code null}. When there IS something to resolve, the cost is one pass
 * over the entries this transaction queued for this index plus one {@link VectorFloat} conversion per pending row -
 * bounded by the transaction's own write set rather than by the size of the index, but paid again on every search,
 * with no amortization guard of the kind the committed-side delta scan has. See issue #7967.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class TransactionVectorOverlay {
  /**
   * The {@code vectorId} of a pending entry. A vector id is minted by {@code allocateVectorId()} during commit
   * replay, so a row that has not been replayed yet has none, and nothing that keys off a vector id - the tombstone
   * set above all - may be asked about one of these.
   */
  static final int PENDING_VECTOR_ID = -1;

  private final List<DeltaVectorEntry> pending;
  private final Set<RID>               superseded;

  private TransactionVectorOverlay(final List<DeltaVectorEntry> pending, final Set<RID> superseded) {
    this.pending = pending;
    this.superseded = superseded;
  }

  /**
   * The overlay for {@code index} in the calling thread's transaction, or {@code null} when there is nothing to
   * resolve.
   * <p>
   * {@code getTransactionIfExists()} rather than {@code getTransaction()}: a search must never bring a transaction
   * into existence on a thread that has none. The status gate is {@code BEGUN} alone, matching
   * {@code LSMTreeIndex.get()}: from {@code COMMIT_1ST_PHASE} onwards the queue is being drained into the index
   * itself, so merging it again would double-count every row.
   */
  static TransactionVectorOverlay open(final DatabaseInternal database, final IndexInternal index,
      final VectorTypeSupport vts) {
    final TransactionContext tx = database.getTransactionIfExists();
    if (tx == null || tx.getStatus() != TransactionContext.STATUS.BEGUN)
      return null;

    final TransactionIndexContext changes = tx.getIndexChanges();
    if (changes == null)
      return null;

    // Every lane this index owns, in replay order. More than one is possible: a compaction that renames the index
    // mid-transaction makes the next write open a second lane under the new name, and commit() replays both, so a
    // reader that took only one would answer with part of this transaction's own writes missing.
    final List<TreeMap<ComparableKey, Map<IndexKey, IndexKey>>> lanes = changes.getIndexKeyLanes(index);
    if (lanes.isEmpty())
      return null;

    // Keyed by RID so a second save of one record replaces the first rather than adding a second row; the
    // iteration order is the lanes' own, which is the order commit() replays them in.
    LinkedHashMap<RID, DeltaVectorEntry> pending = null;
    Set<RID> superseded = null;

    for (final TreeMap<ComparableKey, Map<IndexKey, IndexKey>> lane : lanes)
      for (final Map<IndexKey, IndexKey> bucket : lane.values()) {
        for (final IndexKey entry : bucket.values()) {
          if (entry == null || entry.rid == null)
            continue;

          if (entry.operation == IndexKeyOperation.REMOVE) {
            if (superseded == null)
              superseded = new HashSet<>();
            superseded.add(entry.rid);
            continue;
          }

          // ADD and REPLACE, the two operations commit() turns into a put. A REPLACE also retires the RID it
          // replaced, which commit() removes separately.
          //
          // REPLACE cannot actually reach a dense vector index today: TransactionIndexContext.addIndexKeyLock only
          // promotes an ADD to a REPLACE under `index.isUnique()`, and LSMVectorIndex.isUnique() returns a
          // hard-coded false. It is handled anyway because this method's contract is to model what commit() applies,
          // and commit()'s vector branch reads `ADD || REPLACE` - so the two stay one rule rather than two that have
          // to be kept in step.
          if (entry.oldRid != null) {
            if (superseded == null)
              superseded = new HashSet<>();
            superseded.add(entry.oldRid);
          }

          final float[] vector = vectorOf(entry.keyValues);
          if (vector == null)
            // Not a vector this index can score. Nothing to contribute, and deliberately not an exception: commit
            // replay is where a malformed queued key is diagnosed, and a search must not be the thing that fails.
            continue;

          // The committed copy of a re-embedded row must not be ranked alongside the pending one, so an ADD
          // supersedes its own RID as well. Harmless when the row is new: there is no committed copy to suppress.
          if (superseded == null)
            superseded = new HashSet<>();
          superseded.add(entry.rid);

          if (pending == null)
            pending = new LinkedHashMap<>();
          pending.put(entry.rid, new DeltaVectorEntry(PENDING_VECTOR_ID, entry.rid, vts.createFloatVector(vector)));
        }
      }

    if (pending == null && superseded == null)
      return null;

    return new TransactionVectorOverlay(pending == null ? List.of() : new ArrayList<>(pending.values()),
        superseded == null ? Set.of() : superseded);
  }

  /** The queued key, as the float array {@code LSMVectorIndex.put()} wrapped it in, or {@code null}. */
  private static float[] vectorOf(final Object[] keyValues) {
    if (keyValues == null || keyValues.length == 0)
      return null;
    return keyValues[0] instanceof final ComparableVector c ? c.vector : null;
  }

  /**
   * Whether the committed index's own entries for {@code rid} are superseded by this transaction - because it
   * removed the row, or because it rewrote the vector and the pending version is the one that must be ranked.
   */
  boolean supersedes(final RID rid) {
    return superseded.contains(rid);
  }

  /** The RIDs {@link #supersedes} answers true for, for the {@code Bits} filter of a graph walk. Never null. */
  Set<RID> supersededRIDs() {
    return superseded;
  }

  /** How many rows this transaction contributes, for the callers that size an allocation on the candidate count. */
  int pendingCount() {
    return pending.size();
  }

  /**
   * {@code committedDelta} with this transaction's view applied: its superseded rows dropped, its pending rows
   * appended. Returns the argument itself - no copy - when the overlay changes nothing about it, which is the case
   * whenever the transaction has only written rows the buffer never held.
   * <p>
   * <b>The result is read-only to the caller.</b> In the no-op cases it is the index's own {@code deltaVectors}
   * snapshot or this overlay's {@code pending} list, not a defensive copy, so mutating it would corrupt the
   * buffer every other search reads or this overlay's own rows. Every caller today only iterates it - the scans
   * read entries, and {@code ScoredCandidateCursor}/{@code GroupedSearchState} index back into it by position -
   * and a caller that needs to modify the merged view has to copy it first.
   */
  List<DeltaVectorEntry> augment(final List<DeltaVectorEntry> committedDelta) {
    if (committedDelta.isEmpty())
      return pending;

    boolean anySuperseded = false;
    if (!superseded.isEmpty())
      for (final DeltaVectorEntry entry : committedDelta)
        if (superseded.contains(entry.rid)) {
          anySuperseded = true;
          break;
        }

    if (!anySuperseded && pending.isEmpty())
      return committedDelta;

    final List<DeltaVectorEntry> merged = new ArrayList<>(committedDelta.size() + pending.size());
    for (final DeltaVectorEntry entry : committedDelta)
      if (!anySuperseded || !superseded.contains(entry.rid))
        merged.add(entry);
    merged.addAll(pending);
    return merged;
  }
}
