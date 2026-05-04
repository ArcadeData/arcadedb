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
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class BlockHeader {
  private final RID     firstRid;
  private final RID     lastRid;
  private final int     postingCount;
  private final float   maxWeight;
  private final float   weightMin;
  private final float   weightMax;
  private final boolean hasTombstones;

  public BlockHeader(final RID firstRid, final RID lastRid, final int postingCount, final float maxWeight, final float weightMin,
      final float weightMax, final boolean hasTombstones) {
    this.firstRid = firstRid;
    this.lastRid = lastRid;
    this.postingCount = postingCount;
    this.maxWeight = maxWeight;
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

  public float maxWeight() {
    return maxWeight;
  }

  public float weightMin() {
    return weightMin;
  }

  public float weightMax() {
    return weightMax;
  }

  public boolean hasTombstones() {
    return hasTombstones;
  }
}
