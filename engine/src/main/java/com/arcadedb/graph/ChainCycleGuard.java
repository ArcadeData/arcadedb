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
package com.arcadedb.graph;

import com.arcadedb.database.RID;

import java.util.HashSet;
import java.util.Set;

/**
 * Detects a cycle of any length while walking a chunk chain, in constant memory and without allocating per hop
 * (Brent's algorithm): it remembers one checkpoint chunk, re-arms it at every power-of-two hop, and reports a cycle
 * when the walk comes back to it. A cycle is therefore caught within a few laps, whatever its length, where a
 * self-pointer comparison only catches a cycle of one (issue #8713). A legitimate chain never revisits a chunk, so
 * it never trips, however long it is.
 * <p>
 * Brent's detection reports the cycle a lap or so after the chain closes, so a walk that DESTROYS what it visits
 * (deleting a chunk, then following a pointer back to it) needs {@link #exact} instead: it remembers every chunk and
 * reports the first revisit, at the cost of one set entry per chunk on those walks only.
 * <p>
 * A cycle ENDS the walk, the policy every walker of the chain already applies to a chunk pointing at itself.
 * Because the detection happens a lap or so after the chain closes on itself, the chunks of the cycle may be
 * visited more than once before the walk ends.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class ChainCycleGuard {
  private final boolean exact;
  private Set<RID>      visited;
  private RID           checkpoint;
  private int           hops;
  private int           limit = 1;

  ChainCycleGuard(final RID head) {
    this(head, false);
  }

  private ChainCycleGuard(final RID head, final boolean exact) {
    this.checkpoint = head;
    this.exact = exact;
  }

  private ChainCycleGuard(final RID checkpoint, final int hops, final int limit) {
    this.exact = false;
    this.checkpoint = checkpoint;
    this.hops = hops;
    this.limit = limit;
  }

  /**
   * A guard that reports the FIRST revisit of a chunk, for the walks that delete what they visit. The set is only
   * allocated on the first hop, so a one-chunk chain costs nothing.
   */
  static ChainCycleGuard exact(final RID head) {
    return new ChainCycleGuard(head, true);
  }

  /**
   * A snapshot to resume from, for the iterators that rewind their position after a look-ahead walk.
   */
  ChainCycleGuard copy() {
    if (exact)
      throw new UnsupportedOperationException("An exact guard is not snapshotted");
    return new ChainCycleGuard(checkpoint, hops, limit);
  }

  /**
   * Registers the next chunk of the walk.
   *
   * @return true when {@code next} is the checkpoint, that is when the chain is a cycle
   */
  boolean revisits(final RID next) {
    if (exact) {
      if (visited == null) {
        visited = new HashSet<>();
        if (checkpoint != null)
          visited.add(checkpoint);
      }
      return !visited.add(next);
    }
    if (next.equals(checkpoint))
      return true;
    if (++hops == limit) {
      checkpoint = next;
      hops = 0;
      limit <<= 1;
    }
    return false;
  }
}
