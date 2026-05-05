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
 * In-memory representation of a single block header within a sealed segment. Equivalent to
 * the on-disk {@code block_header} layout described in the design doc.
 * <p>
 * Two "max"-shaped fields exist for distinct reasons:
 * <ul>
 *   <li>{@link #bmwUpperBound()} - the per-block max live weight, used by BlockMax-WAND DAAT to
 *       decide whether the block can possibly contribute to the top-K. Derived from live
 *       postings only (tombstoned postings do not contribute).</li>
 *   <li>{@link #weightMax()} - the upper end of the {@code [weightMin, weightMax]} range used
 *       to decode the int8-quantized per-posting weights inside this block. Independent of
 *       BMW pruning; matters only to the dequantizer.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class BlockHeader {
  private final RID     firstRid;
  private final RID     lastRid;
  private final int     postingCount;
  private final float   bmwUpperBound;
  private final float   weightMin;
  private final float   weightMax;
  private final boolean hasTombstones;

  public BlockHeader(final RID firstRid, final RID lastRid, final int postingCount, final float bmwUpperBound,
      final float weightMin, final float weightMax, final boolean hasTombstones) {
    this.firstRid = firstRid;
    this.lastRid = lastRid;
    this.postingCount = postingCount;
    this.bmwUpperBound = bmwUpperBound;
    this.weightMin = weightMin;
    this.weightMax = weightMax;
    this.hasTombstones = hasTombstones;
  }

  public RID firstRid() {
    return firstRid;
  }

  public RID lastRid() {
    return lastRid;
  }

  public int postingCount() {
    return postingCount;
  }

  /**
   * Per-block max live weight - the BlockMax-WAND pruning bound. A block whose
   * {@code bmwUpperBound} cannot push the running prefix-sum past the top-K threshold can be
   * skipped without decoding any postings.
   */
  public float bmwUpperBound() {
    return bmwUpperBound;
  }

  public float weightMin() {
    return weightMin;
  }

  /**
   * Upper end of the {@code [weightMin, weightMax]} range used to dequantize the int8 weights
   * stored in this block's payload. Not the BMW pruning bound; see {@link #bmwUpperBound()}.
   */
  public float weightMax() {
    return weightMax;
  }

  public boolean hasTombstones() {
    return hasTombstones;
  }
}
