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

import com.arcadedb.database.Database;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.serializer.json.JSONObject;

/**
 * Builds a Studio-friendly snapshot of every {@link LSMSparseVectorIndex} live on a database:
 * memtable posting count, sealed segment count, total postings, and how many of those segments
 * are still untrusted. Operators read this on the Server tab to spot a memtable that is not
 * draining (compaction lag), a runaway segment count (size-tiered cascade jammed), or a segment
 * whose precedence predates the recency-epoch fix (issue #6379) - without log-grepping or a JMX
 * detour.
 * <p>
 * The shape returned is keyed by the user-facing {@link TypeIndex} name (e.g.
 * {@code Doc[tokens,weights]}) so Studio shows one row per logical index. Per-bucket physical
 * sub-indexes are summed under their parent {@code TypeIndex}, which is what an operator
 * thinks of as "the sparse-vector index". Values are pulled directly from the live engines -
 * no extra book-keeping; each counter is a cheap read bounded by the (small) active segment count.
 *
 * <pre>{
 *   "Doc[tokens,weights]":   { "memtablePostings": 12345, "segmentCount": 3, "totalPostings": 987654,
 *                               "untrustedSegments": 0 },
 *   "Other[tokens,weights]": { "memtablePostings": 0,     "segmentCount": 1, "totalPostings": 543210,
 *                               "untrustedSegments": 1 }
 * }</pre>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class LSMSparseVectorIndexMetrics {

  private LSMSparseVectorIndexMetrics() {
  }

  /**
   * Walks every index registered on the given database, collecting per-{@link LSMSparseVectorIndex}
   * stats and aggregating them under the parent {@link TypeIndex} name. Non-sparse indexes are
   * skipped. Returned object may be empty.
   */
  public static JSONObject buildJSON(final Database database) {
    final JSONObject out = new JSONObject();
    if (database == null)
      return out;
    for (final Index index : database.getSchema().getIndexes()) {
      if (!(index instanceof LSMSparseVectorIndex sparse))
        continue;
      // {@code getEngine()} can return null for an index that is mid-drop or whose engine
      // construction was deferred. The Studio card has no business reporting transient
      // intermediate state - skip and let the next scrape pick the row up once the index is
      // either fully gone or fully open.
      final PaginatedSparseVectorEngine engine = sparse.getEngine();
      if (engine == null)
        continue;
      final TypeIndex typeIndex = sparse.getTypeIndex();
      final String key = typeIndex != null ? typeIndex.getName() : sparse.getName();

      // One snapshot per engine for both counts, so a compaction landing between two separate reads cannot make a
      // single bucket's own pair inconsistent (PR #6720 review); the sum across buckets below is still only as
      // atomic as scraping several independent engines ever is, same as every other field aggregated here.
      final PaginatedSparseVectorEngine.SegmentTrustSnapshot trust = engine.segmentTrust();

      final JSONObject entry;
      if (out.has(key)) {
        entry = out.getJSONObject(key);
        entry.put("memtablePostings", entry.getLong("memtablePostings", 0L) + engine.memtablePostings());
        entry.put("segmentCount", entry.getInt("segmentCount", 0) + trust.total());
        entry.put("totalPostings", entry.getLong("totalPostings", 0L) + engine.totalPostings());
        entry.put("untrustedSegments", entry.getInt("untrustedSegments", 0) + trust.untrusted());
      } else {
        entry = new JSONObject();
        entry.put("memtablePostings", engine.memtablePostings());
        entry.put("segmentCount", trust.total());
        entry.put("totalPostings", engine.totalPostings());
        entry.put("untrustedSegments", trust.untrusted());
        out.put(key, entry);
      }
    }
    return out;
  }
}
