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

/**
 * Detects a cycle of any length while walking a chunk chain, in constant memory and without allocating per hop
 * (Brent's algorithm): it remembers one checkpoint chunk, re-arms it at every power-of-two hop, and reports a cycle
 * when the walk comes back to it. A cycle is therefore caught within a few laps, whatever its length, where a
 * self-pointer comparison only catches a cycle of one (issue #8713). A legitimate chain never revisits a chunk, so
 * it never trips, however long it is.
 * <p>
 * A cycle ENDS the walk, the policy every walker of the chain already applies to a chunk pointing at itself.
 * Because the detection happens a lap or so after the chain closes on itself, the chunks of the cycle may be
 * visited more than once before the walk ends.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class ChainCycleGuard {
  private RID checkpoint;
  private int hops;
  private int limit = 1;

  ChainCycleGuard(final RID head) {
    this.checkpoint = head;
  }

  private ChainCycleGuard(final RID checkpoint, final int hops, final int limit) {
    this.checkpoint = checkpoint;
    this.hops = hops;
    this.limit = limit;
  }

  /**
   * A snapshot to resume from, for the iterators that rewind their position after a look-ahead walk.
   */
  ChainCycleGuard copy() {
    return new ChainCycleGuard(checkpoint, hops, limit);
  }

  /**
   * Registers the next chunk of the walk.
   *
   * @return true when {@code next} is the checkpoint, that is when the chain is a cycle
   */
  boolean revisits(final RID next) {
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
