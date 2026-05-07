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
 * <b>Dedup contract.</b> {@code equals} / {@code hashCode} are field-wise (the record contract).
 * Weight participates in equality, so two puts on the same {@code (dim, rid)} with different
 * weights are distinct entries in the {@code TransactionIndexContext} dedup map and both replay
 * to the engine. This is intentional and preserves the prior MVP's behavior. The user-visible
 * outcome is correct in both directions:
 * <ul>
 *   <li>Same weight twice: the second IndexKey equals the first; the dedup map keeps one entry;
 *       one replay call. No wasted work.</li>
 *   <li>Different weights in one transaction (e.g. {@code put(d, r, 0.3)} then
 *       {@code put(d, r, 0.5)}): both entries survive the dedup map; both replay in order. The
 *       engine memtable is a {@code ConcurrentSkipListMap<RID, Float>} which last-write-wins on
 *       the {@code put}, so the final value at {@code (d, r)} is {@code 0.5} - the user's intent.
 *       Cost: one extra replay call per duplicate-with-different-weight, bounded by per-tx
 *       posting volume.</li>
 * </ul>
 * Keying dedup on {@code (dim, rid)} only would also work (HashMap.put overwrite would keep the
 * later entry), but at the cost of either subtle "first write wins" semantics if the dedup map
 * variant ever shifts (e.g. to {@code putIfAbsent}) or a custom equality contract that diverges
 * from the record auto-implementation. Field-wise equality plus the memtable's natural overwrite
 * is the simpler and more durable design.
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
