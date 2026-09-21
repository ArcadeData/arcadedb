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
import com.arcadedb.database.TransactionIndexContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The sparse postings the calling transaction has queued for one index, resolved into something a search can
 * score (issue #7966).
 * <p>
 * A sparse-vector write does not reach {@link PaginatedSparseVectorEngine}'s memtable at {@code save()} time: it is
 * queued on {@code TransactionIndexContext} and replayed at commit. {@code topK} reads "an atomic snapshot of the
 * current memtable + segments", so a caller that wrote a sparse vector and searched for it in the same transaction
 * got nothing back - fewer results rather than an error, which an application cannot tell apart from "no match".
 * The dense index closed the same asymmetry in issue #7378; this is its sparse twin.
 * <p>
 * <b>Why the postings are the whole vector.</b> Every engine path that writes this index writes a record's WHOLE
 * sparse vector: {@code DocumentIndexer} removes every posting of the previous value and adds every posting of the
 * new one, and {@link LSMSparseVectorIndex#put}/{@link LSMSparseVectorIndex#remove} take the full parallel arrays.
 * So the postings this overlay ends up holding for a RID ARE that record's current vector, and a RID left with
 * none is one the transaction deleted. No merge with the committed posting lists is needed - which is what makes
 * the overlay resolvable on the caller's thread rather than inside the DAAT loop.
 * <p>
 * <b>Why it is built on the caller's thread.</b> {@code SQLFunctionVectorSparseNeighbors} fans its per-bucket
 * searches out to {@code SparseVectorScoringPool}, where {@code getTransactionIfExists()} answers null. An overlay
 * resolved inside the index would therefore do something on the serial plan and nothing on the parallel one - two
 * different answers for one query, decided by a fan-out heuristic. It is resolved once, up front, and handed to
 * both plans.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class SparseTransactionOverlay {
  /** Per pending RID, the weight of each dimension the transaction leaves it with. */
  private final Map<RID, Map<Integer, Float>> pending;

  private SparseTransactionOverlay(final Map<RID, Map<Integer, Float>> pending) {
    this.pending = pending;
  }

  /**
   * Replays a transaction's append-only lane for one index into an overlay, or answers {@code null} when the lane
   * holds nothing - the shape every read-only query has, and the one this must cost nothing on.
   * <p>
   * Replayed in queue order, exactly the way {@code TransactionIndexContext.commit()} replays it, so the overlay
   * and the commit agree on what the transaction leaves behind: the last operation on a {@code (dim, rid)} wins.
   *
   * @param lane the entries this transaction has queued for the index, or {@code null}
   */
  static SparseTransactionOverlay of(final List<TransactionIndexContext.IndexKey> lane) {
    if (lane == null || lane.isEmpty())
      return null;

    final Map<RID, Map<Integer, Float>> pending = new HashMap<>();
    for (int i = 0; i < lane.size(); i++) {
      final TransactionIndexContext.IndexKey entry = lane.get(i);
      if (entry.keyValues == null || entry.keyValues.length == 0
          || !(entry.keyValues[0] instanceof final SparsePostingReplayKey posting))
        // Not one of this index's markers. Nothing else rides this lane today, but a lane is keyed by name and the
        // overlay must never invent a posting out of something it does not understand.
        continue;

      final Map<Integer, Float> vector = pending.computeIfAbsent(posting.rid(), rid -> new HashMap<>());
      if (entry.operation == TransactionIndexContext.IndexKey.IndexKeyOperation.REMOVE)
        vector.remove(posting.dim());
      else
        vector.put(posting.dim(), posting.weight());
    }

    return pending.isEmpty() ? null : new SparseTransactionOverlay(pending);
  }

  /**
   * Every RID this transaction has written to, whether it ends up with a vector or not. The committed side of the
   * search must skip all of them: a RID with pending postings is scored here instead, from the vector it now has
   * rather than the one it had, and a RID the transaction deleted must not come back at all.
   */
  public Set<RID> touchedRIDs() {
    return Collections.unmodifiableSet(pending.keySet());
  }

  /** How many RIDs the committed side has to skip, so a caller can widen its fetch by exactly that much. */
  public int touchedCount() {
    return pending.size();
  }

  /**
   * Scores every pending RID against the query and returns the best {@code k} of them, highest score first.
   * <p>
   * A dot product over the query's dimensions, so the cost is O(pending RIDs x query dimensions) with one hash
   * lookup each - the query carries a handful of dimensions even on learned-sparse corpora. RIDs the transaction
   * deleted carry no dimensions and are dropped rather than scored at 0: a deleted record is absent, not distant.
   *
   * @param queryDims    the query's dimensions
   * @param queryWeights the query's weights, already IDF-scaled by the caller when the index asks for it
   * @param allowedRIDs  optional whitelist, applied here exactly as the committed side applies it
   * @param k            how many to keep
   */
  public List<RidScore> topK(final int[] queryDims, final float[] queryWeights, final Set<RID> allowedRIDs, final int k) {
    if (k <= 0)
      return List.of();

    final boolean filtered = allowedRIDs != null && !allowedRIDs.isEmpty();
    final List<RidScore> scored = new ArrayList<>(Math.min(k, pending.size()));

    for (final Map.Entry<RID, Map<Integer, Float>> entry : pending.entrySet()) {
      final Map<Integer, Float> vector = entry.getValue();
      if (vector.isEmpty())
        // Deleted by this transaction: it has no vector to be near anything.
        continue;
      if (filtered && !allowedRIDs.contains(entry.getKey()))
        continue;

      float score = 0f;
      for (int i = 0; i < queryDims.length; i++) {
        if (queryWeights[i] == 0f)
          continue;
        final Float weight = vector.get(queryDims[i]);
        if (weight != null)
          score += queryWeights[i] * weight;
      }

      // A record sharing no dimension with the query scores 0, which is what the committed side reports for it
      // too: BMW never admits a document no cursor positioned on. Dropping it keeps the two sides symmetric.
      if (score != 0f)
        scored.add(new RidScore(entry.getKey(), score));
    }

    if (scored.isEmpty())
      return List.of();

    scored.sort(Comparator.comparingDouble((RidScore r) -> r.score()).reversed());
    return scored.size() <= k ? scored : scored.subList(0, k);
  }
}
