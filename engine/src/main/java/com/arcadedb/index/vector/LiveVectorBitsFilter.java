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

import com.arcadedb.database.RID;
import io.github.jbellis.jvector.util.Bits;

import java.util.Set;

/**
 * Tells JVector which graph ordinals may enter a search result: the ones that still map to a live vector, optionally
 * narrowed to an allow-list of RIDs.
 * <p>
 * The liveness half is not an optimisation, it is what keeps a delete from emptying a result set (issue #5558). A
 * deleted vector stays in the HNSW graph until the next rebuild, and JVector's beam stops as soon as it holds
 * {@code rerankK} <i>acceptable</i> candidates. Accepting everything therefore lets a query aimed at a deleted
 * neighbourhood fill its beam with tombstones, declare itself finished, and hand back nodes that the caller's
 * post-filter then removes one by one - down to nothing. Rejecting them here instead makes the same beam keep walking
 * until it has found live neighbours: the tombstones are still traversed (JVector expands a rejected node, it just
 * does not collect it), so the surviving vectors behind the hole stay reachable.
 * <p>
 * The predicate is deliberately identical to the post-filter applied to the search output, so nothing that would have
 * survived it is dropped here.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LiveVectorBitsFilter implements Bits {
  private final Set<RID>            allowedRIDs;
  private final int[]               ordinalToVectorIdSnapshot;
  private final VectorLocationIndex vectorIndexSnapshot;

  /**
   * @param allowedRIDs               optional RID allow-list; {@code null} or empty means "every live vector"
   * @param ordinalToVectorIdSnapshot the ordinal map captured together with the vectors snapshot by the caller
   * @param vectorIndexSnapshot       the location map that answers whether a vector id is still live
   */
  LiveVectorBitsFilter(final Set<RID> allowedRIDs, final int[] ordinalToVectorIdSnapshot,
      final VectorLocationIndex vectorIndexSnapshot) {
    this.allowedRIDs = allowedRIDs != null && !allowedRIDs.isEmpty() ? allowedRIDs : null;
    this.ordinalToVectorIdSnapshot = ordinalToVectorIdSnapshot;
    this.vectorIndexSnapshot = vectorIndexSnapshot;
  }

  @Override
  public boolean get(final int ordinal) {
    if (ordinal < 0 || ordinal >= ordinalToVectorIdSnapshot.length)
      return false;

    final int vectorId = ordinalToVectorIdSnapshot[ordinal];

    final VectorLocationIndex.VectorLocation loc = vectorIndexSnapshot.getLocation(vectorId);
    if (loc == null || loc.deleted)
      return false;

    return allowedRIDs == null || allowedRIDs.contains(loc.rid);
  }
}
