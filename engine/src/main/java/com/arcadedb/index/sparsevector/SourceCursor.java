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

import java.io.IOException;

/**
 * Forward-only cursor over the postings of one dim within a single source (sealed segment or
 * memtable). Used by {@link DimCursor} to merge multiple sources transparently.
 * <p>
 * Lifecycle:
 * <ol>
 *   <li>Caller invokes {@link #start()} once.</li>
 *   <li>Caller invokes {@link #advance()} repeatedly until it returns {@code false}.</li>
 *   <li>Optional: {@link #seekTo(RID)} skips forward; the cursor lands at the first posting
 *       whose RID is &gt;= the target.</li>
 *   <li>{@link #close()} releases backing resources. Idempotent.</li>
 * </ol>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public interface SourceCursor extends AutoCloseable {

  void start() throws IOException;

  boolean advance() throws IOException;

  /** Seek forward to the first posting whose RID is &gt;= {@code target}. Backwards seeks are no-ops. */
  boolean seekTo(RID target) throws IOException;

  RID currentRid();

  float currentWeight();

  boolean isTombstone();

  boolean isExhausted();

  /** Upper bound on the contribution of any remaining posting in this source for this dim. */
  float upperBoundRemaining();

  /**
   * Tight Block-Max WAND upper bound on this source's contribution to the document identified by
   * {@code rid}: the maximum weight of the block that would contain {@code rid} (i.e. the first
   * block at or after the current position whose last RID is &gt;= {@code rid}). Unlike
   * {@link #upperBoundRemaining()}, which is a suffix-max over every remaining block, this is a
   * per-block bound and is what lets BMW skip whole blocks that cannot beat the top-K threshold.
   * <p>
   * The default returns {@link #upperBoundRemaining()} - correct (an over-estimate is always safe
   * for an upper bound) but no tighter than plain WAND. Sources that maintain per-block maxima
   * (sealed segments) override this to expose the real block bound. Reading a block-max never
   * decodes a posting payload; only in-memory block headers are consulted.
   *
   * @return an upper bound on the contribution to {@code rid}, or {@code 0} if this source has no
   *         block that could contain {@code rid} (all its remaining postings are strictly before
   *         {@code rid}, or it is exhausted).
   */
  default float blockMaxAt(final RID rid) {
    return upperBoundRemaining();
  }

  /**
   * The last RID of the block that would contain {@code rid} (see {@link #blockMaxAt(RID)}). BMW
   * uses this as the right edge of the range that {@link #blockMaxAt(RID)} bounds, so it can skip
   * forward to just past it when the block cannot beat the threshold.
   * <p>
   * Returns {@code null} for sources that cannot bound a finite block boundary (e.g. an in-memory
   * memtable whose {@link #blockMaxAt(RID)} is a whole-dim max valid for every remaining posting).
   * A {@code null} boundary does not constrain the skip target: BMW only skips when at least one
   * source in the pivot prefix reports a finite boundary.
   */
  default RID blockEndAt(final RID rid) {
    return null;
  }

  @Override
  void close();
}
