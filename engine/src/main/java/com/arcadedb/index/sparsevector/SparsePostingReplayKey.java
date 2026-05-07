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

/**
 * Typed marker for a single sparse-vector posting carried through the transaction commit replay
 * pipeline. Wraps the {@code (dim, rid, weight)} tuple that {@link LSMSparseVectorIndex} queues
 * onto {@code TransactionContext.addIndexOperation} so the wrapper's {@code put}/{@code remove}
 * overrides can identify a replay frame via {@code instanceof} instead of sniffing the shape
 * of the {@code Object[]} keys.
 * <p>
 * Implements {@link Comparable} so that
 * {@link com.arcadedb.database.TransactionIndexContext.ComparableKey#compareTo} can order
 * dedup-map entries (it routes through {@code BinaryComparator.compareTo}, which falls back to
 * {@code Comparable}).
 * <p>
 * <b>Dedup contract - intentionally weight-keyed, not operation-keyed.</b> {@code equals} /
 * {@code hashCode} are field-wise (the record contract): {@code (dim, rid, weight)} are all in.
 * Critically, the operation kind (ADD vs REMOVE) is <i>not</i> stored on the marker. This is
 * deliberate. {@code TransactionIndexContext} stores the operation on the IndexKey wrapper that
 * holds this marker, NOT in {@link #equals}. Same-{@code (dim, rid, weight)} put and remove in
 * the same transaction therefore collide into a single dedup-map entry, and HashMap.put
 * overwrite semantics mean the LAST operation wins:
 * <ul>
 *   <li>{@code put(d, r, 0.5)} then {@code remove(d, r)} (where remove pulls
 *       {@code weight=0.5} from the record's current value): the remove IndexKey overwrites
 *       the put IndexKey in the dedup map, so only REMOVE replays. Doc deleted. CORRECT.</li>
 *   <li>{@code remove(d, r)} then {@code put(d, r, 0.5)}: the put IndexKey overwrites the
 *       remove IndexKey, so only ADD replays. Doc inserted. CORRECT.</li>
 * </ul>
 * Including the operation kind in equality (or carrying a {@code tombstone} flag here) would
 * keep both entries distinct in the dedup map, and {@code TransactionIndexContext}'s
 * two-phase commit (REMOVEs first, then ADDs) would re-order them: the put-then-remove case
 * would end with the doc INSERTED instead of deleted. The current weight-keyed design relies on
 * dedup-map overwrite to preserve user intent.
 * <p>
 * <b>Different-weight cases.</b> Two puts with different weights on the same {@code (d, r)}
 * are distinct entries; both replay to the engine in order, and the memtable's
 * {@code ConcurrentSkipListMap.put} last-write-wins for the final value (cost: one extra replay
 * per duplicate-with-different-weight, bounded by per-tx posting volume). The realistic UPDATE
 * pattern (remove-OLD-then-put-NEW with different weights) goes through TransactionIndexContext's
 * REMOVE-then-ADD ordering correctly: remove of OLD weight, then put of NEW weight, ends with
 * the new weight in the memtable.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public record SparsePostingReplayKey(int dim, RID rid, float weight)
    implements Comparable<SparsePostingReplayKey> {

  @Override
  public int compareTo(final SparsePostingReplayKey o) {
    int c = Integer.compare(dim, o.dim);
    if (c != 0)
      return c;
    c = rid.compareTo(o.rid);
    if (c != 0)
      return c;
    return Float.compare(weight, o.weight);
  }
}
