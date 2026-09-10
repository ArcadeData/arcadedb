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
package com.arcadedb.index;

import com.arcadedb.database.Database;
import com.arcadedb.log.LogManager;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

/**
 * One bulk load's hold on the speculative background maintenance of every index of a database, taken with
 * {@link #suspend(Database, String)} and lifted exactly once by {@link #close()}.
 * <p>
 * The maintenance that matters today is the LSM vector index's inactivity graph rebuild: it reads the index going
 * quiet as "the writer is done", which during a bulk load is produced by the load stalling on a compaction or a
 * flush burst, and the rebuild it starts then covers only what has been loaded so far and is superseded by the
 * rest of the load (issue #7357). Suspending is a pure deferral - the writes stay searchable through the index's
 * own delta path meanwhile - and lifting the last suspension is when the one rebuild the load is actually worth
 * gets scheduled.
 * <p>
 * Suspensions compose: {@link IndexInternal#suspendBackgroundMaintenance()} is reference-counted, so a loader
 * that opens several batches in a row holds one of these around the whole load and each batch nests its own
 * inside it, and the index stays quiet across the gaps between the batches as well as during them. That is the
 * shape {@code GraphImporter} has - a vertex batch, a topology pass with no batch open at all, then one edge
 * batch per edge type - and the gap between its two batches is exactly where a 4.2M-vector load saw the rebuild
 * fire and then run alongside the whole edge pass (issue #7432).
 * <p>
 * The list is a snapshot taken at {@link #suspend(Database, String)}, and {@link #close()} lifts exactly those:
 * an index dropped mid-load must still have its suspension lifted, and looking the list up again would silently
 * leak the count of one that is no longer in the schema. An index created while the suspension is held runs its
 * background maintenance as usual - an acceptable gap, since creating an index in the middle of a bulk load is
 * not a shape worth complicating the lifetime of these suspensions for, and the maintenance it would do is over
 * a corpus it has just been built on.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class IndexMaintenanceSuspension implements AutoCloseable {
  private static final IndexInternal[] NONE = new IndexInternal[0];

  private final String          owner;
  private final IndexInternal[] suspended;
  private final AtomicBoolean   lifted = new AtomicBoolean();

  private IndexMaintenanceSuspension(final String owner, final IndexInternal[] suspended) {
    this.owner = owner;
    this.suspended = suspended;
  }

  /**
   * Suspends the speculative background maintenance of every index of {@code database}.
   * <p>
   * A failure to suspend one index must not leave the load half-suspended and must not fail the load either - the
   * suspension is an optimization, not a correctness requirement - so an index that cannot be asked is skipped and
   * the rest are still suspended. Caught per index rather than around the loop for exactly that reason (PR #7360
   * review): the cast is the failure this is most likely to see, and one {@link Index} that does not implement
   * {@link IndexInternal} must not cost every OTHER index of the database its suspension.
   *
   * @param database the database whose indexes are to be held quiet
   * @param owner    who is asking, for the log line a failure produces (e.g. {@code "GraphBatch"})
   *
   * @return the suspension to {@link #close()} when the load is over; never null
   */
  public static IndexMaintenanceSuspension suspend(final Database database, final String owner) {
    final Index[] all = database.getSchema().getIndexes();
    if (all.length == 0)
      return new IndexMaintenanceSuspension(owner, NONE);

    final IndexInternal[] suspended = new IndexInternal[all.length];
    int count = 0;
    for (final Index idx : all) {
      try {
        // Every index type in the tree implements IndexInternal, so this cast does not fail today: the catch is
        // future-proofing for an implementation that does not, not cover for one that exists (PR #7360 review).
        final IndexInternal index = (IndexInternal) idx;
        index.suspendBackgroundMaintenance();
        suspended[count++] = index;
      } catch (final Exception e) {
        LogManager.instance().log(IndexMaintenanceSuspension.class, Level.WARNING,
            "%s: could not suspend the background maintenance of index %s for the bulk load: %s", e, owner,
            idx.getName(), e.getMessage());
      }
    }
    return new IndexMaintenanceSuspension(owner, count == suspended.length ? suspended : Arrays.copyOf(suspended, count));
  }

  /**
   * @return how many indexes this suspension holds
   */
  public int size() {
    return suspended.length;
  }

  /**
   * Lifts the suspensions {@link #suspend(Database, String)} took, once. Idempotent because a loader's abandon and
   * close paths can both reach it, and a second lift would decrement a count this suspension no longer holds -
   * handing another loader's suspension away with it.
   * <p>
   * Never throws: a load on its way out must lift every OTHER suspension it holds whatever one index does, and an
   * index dropped mid-load is the ordinary way one fails. Logged at WARNING, the same level a failed suspension
   * gets, and deliberately not lower (PR #7360 review): the two failures are not equally harmless. A suspension
   * that could not be taken costs an optimization; one that could not be LIFTED strands that index's background
   * maintenance off until the process reopens the database, and does it silently. That is worth seeing even when
   * the cause turns out to be an index dropped mid-load.
   */
  @Override
  public void close() {
    if (!lifted.compareAndSet(false, true))
      return;
    for (final IndexInternal index : suspended) {
      try {
        index.resumeBackgroundMaintenance();
      } catch (final Exception e) {
        LogManager.instance().log(IndexMaintenanceSuspension.class, Level.WARNING,
            "%s: could not resume the background maintenance of index %s, it stays suspended until this database "
                + "is reopened: %s", e, owner, index.getName(), e.getMessage());
      }
    }
  }
}
