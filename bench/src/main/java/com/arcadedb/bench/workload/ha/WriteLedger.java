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
package com.arcadedb.bench.workload.ha;

import java.util.Arrays;

/**
 * The writes of one repetition by outcome: an acknowledged id must be on every node afterwards; an unknown one (the write threw:
 * it may have landed or not) is counted and never looked for. Ids are kept in growable primitive arrays, not boxed sets, and need
 * not arrive in order. Not thread-safe: one per writer, merged with {@link #addAll} once the writers are done.
 */
public final class WriteLedger {
  private long[]  acked;
  private int     ackedCount;
  private long[]  unknown;
  private int     unknownCount;
  private boolean sorted = true;

  public WriteLedger(final int capacityHint) {
    acked = new long[Math.max(capacityHint, 16)];
    unknown = new long[16];
  }

  public void acked(final long id) {
    if (ackedCount == acked.length)
      acked = Arrays.copyOf(acked, ackedCount + (ackedCount >> 1) + 1);
    if (ackedCount > 0 && acked[ackedCount - 1] > id)
      sorted = false;
    acked[ackedCount++] = id;
  }

  public void unknown(final long id) {
    if (unknownCount == unknown.length)
      unknown = Arrays.copyOf(unknown, unknownCount + (unknownCount >> 1) + 1);
    unknown[unknownCount++] = id;
  }

  /** Appends every id of {@code other}, which is left as it was. */
  public void addAll(final WriteLedger other) {
    for (int i = 0; i < other.ackedCount; i++)
      acked(other.acked[i]);
    for (int i = 0; i < other.unknownCount; i++)
      unknown(other.unknown[i]);
  }

  public long ackedCount() {
    return ackedCount;
  }

  public long unknownCount() {
    return unknownCount;
  }

  /**
   * The acknowledged ids absent from {@code sortedIdsOnNode} (ascending), in ascending order. Unknown ids are never missing, and
   * ids on the node that were never acknowledged are not this method's business.
   */
  public long[] missingFrom(final long[] sortedIdsOnNode) {
    if (!sorted) {
      Arrays.sort(acked, 0, ackedCount);
      sorted = true;
    }
    long[] missing = null;
    int count = 0;
    int j = 0;
    for (int i = 0; i < ackedCount; i++) {
      final long id = acked[i];
      while (j < sortedIdsOnNode.length && sortedIdsOnNode[j] < id)
        j++;
      if (j < sortedIdsOnNode.length && sortedIdsOnNode[j] == id)
        continue;
      if (missing == null)
        missing = new long[16];
      else if (count == missing.length)
        missing = Arrays.copyOf(missing, count * 2);
      missing[count++] = id;
    }
    return missing == null ? new long[0] : Arrays.copyOf(missing, count);
  }
}
