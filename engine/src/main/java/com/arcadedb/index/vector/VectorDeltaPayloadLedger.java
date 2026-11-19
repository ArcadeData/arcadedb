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

import java.util.concurrent.atomic.AtomicLong;

/**
 * The heap the delta buffers of every vector index of ONE database hold in vector payloads (issue #12379).
 * <p>
 * A type has one vector index per bucket, so a schema with a few vector properties on bucketed types has dozens of
 * them, and {@code arcadedb.vectorIndex.deltaCacheMaxHeapPercent} bounds what they keep together. Summing the indexes
 * when one of them sizes itself gives that bound only up to the staleness of the sum: an index reuses its answer for a
 * second, so every index that sized itself in the same second could admit the room that was left, each on its own. A
 * payload is admitted here instead, by reserving its bytes against the total, so the room is handed out once whatever
 * the number of indexes that are filling at the same moment, and admitting costs one compare-and-set rather than a
 * walk of the schema.
 * <p>
 * <b>What keeps the total from drifting.</b> A counter that outlives what it counts is the risk of this shape, so:
 * <ul>
 *   <li>the ledger belongs to the database instance and goes with it;</li>
 *   <li>an index never adds to or subtracts from the total directly. It holds an {@link Account}, which remembers
 *       what it charged, so what it gives back is exactly that, once, however its own counters moved meanwhile;</li>
 *   <li>the account is closed when the index stops being the schema's (a close, a drop, a reload that published its
 *       successor), and a closed account charges nothing more.</li>
 * </ul>
 * A release that was missed would fail on the safe side - siblings keep fewer payloads than they could and read them
 * back from the pages - never on the side of the heap.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class VectorDeltaPayloadLedger {
  /** The budget of an index whose payloads are bounded by something other than the heap share: nothing is refused. */
  static final long NO_SHARED_BUDGET = Long.MAX_VALUE;

  private final AtomicLong bytes = new AtomicLong();

  /** @return the bytes the open accounts of this ledger hold together, never negative */
  public long bytes() {
    return bytes.get();
  }

  /**
   * One vector index's share of a {@link VectorDeltaPayloadLedger}.
   * <p>
   * Synchronized on itself. The index changes it under its own write lock, so the monitor is uncontended there; it is
   * what makes {@link #close()}, which arrives from the thread closing the database or reloading the schema without
   * that lock, atomic with a charge in flight.
   */
  static final class Account {
    private VectorDeltaPayloadLedger ledger;
    private long                     charged;
    private boolean                  closed;

    /**
     * Reserves the room of one more payload, unless the ledger is past {@code budgetBytes}.
     *
     * @param ledger       the ledger of the index's database
     * @param payloadBytes what the payload costs the heap
     * @param budgetBytes  what the indexes of the database may hold together, or {@link #NO_SHARED_BUDGET}
     *
     * @return whether the payload may be kept. Never for a closed account.
     */
    synchronized boolean tryCharge(final VectorDeltaPayloadLedger ledger, final long payloadBytes, final long budgetBytes) {
      if (closed)
        return false;
      this.ledger = ledger;

      final AtomicLong total = ledger.bytes;
      while (true) {
        final long current = total.get();
        // Written as a subtraction so NO_SHARED_BUDGET cannot overflow
        if (budgetBytes - current < payloadBytes)
          return false;
        if (total.compareAndSet(current, current + payloadBytes)) {
          charged += payloadBytes;
          return true;
        }
      }
    }

    /**
     * Sets what this account holds, whatever the budget: for the payloads that were admitted before and are being
     * counted again (a buffer a rebuild replaced, an entry an aborted replay puts back) and for those that left.
     */
    synchronized void set(final VectorDeltaPayloadLedger ledger, final long heldBytes) {
      if (closed)
        return;
      this.ledger = ledger;
      final long held = Math.max(0L, heldBytes);
      ledger.bytes.addAndGet(held - charged);
      charged = held;
    }

    /** Gives back everything this account charged. A closed account stays closed: its index is no longer the schema's. */
    synchronized void close() {
      if (closed)
        return;
      closed = true;
      if (ledger != null)
        ledger.bytes.addAndGet(-charged);
      charged = 0L;
    }

    /** @return the bytes this account is charging the ledger, {@code 0} once closed */
    synchronized long charged() {
      return charged;
    }
  }
}
