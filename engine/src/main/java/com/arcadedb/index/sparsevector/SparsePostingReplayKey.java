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
 * Implements {@link Comparable} so the marker still orders deterministically wherever a comparison
 * is needed (it routes through {@code BinaryComparator.compareTo}, which falls back to
 * {@code Comparable}).
 * <p>
 * <b>Ordering contract - insertion order decides, nothing is deduplicated.</b> Because
 * {@link LSMSparseVectorIndex#isTransactionKeyOrderRequired()} returns {@code false}, these markers
 * ride {@code TransactionIndexContext}'s append-only lane and replay at commit in the exact order
 * they were queued (issue #5411). The last operation on a given {@code (dim, rid)} therefore wins by
 * construction:
 * <ul>
 *   <li>{@code put(d, r, 0.5)} then {@code remove(d, r)}: replays ADD then REMOVE, leaving a
 *       tombstone. Doc deleted. CORRECT.</li>
 *   <li>{@code remove(d, r)} then {@code put(d, r, 0.5)}: replays REMOVE then ADD, leaving the
 *       posting live. Doc inserted. CORRECT.</li>
 *   <li>The realistic UPDATE pattern (remove-OLD-then-put-NEW, possibly at a different weight)
 *       replays in that same order and ends with the new weight in the memtable.</li>
 * </ul>
 * That is strictly simpler than what the key-ordered lane needed for the same outcome: it collapsed
 * same-{@code (dim, rid, weight)} operations into one dedup-map slot and relied on HashMap overwrite,
 * while its two-phase commit (all REMOVEs, then all ADDs) re-ordered whatever did not collapse - so
 * the operation kind had to be kept OUT of {@code equals} for put-then-remove to end deleted. The
 * commit path no longer reads that equality at all; {@code equals}/{@code hashCode} stay field-wise
 * (the record default) purely so the marker behaves sanely in collections.
 * <p>
 * <b>Cost.</b> Repeated writes to the same {@code (d, r)} inside one transaction each replay
 * separately instead of collapsing, which the memtable absorbs with last-write-wins. The extra
 * replays are bounded by per-transaction posting volume and are far cheaper than the per-entry
 * ordered-map bookkeeping they replace.
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
