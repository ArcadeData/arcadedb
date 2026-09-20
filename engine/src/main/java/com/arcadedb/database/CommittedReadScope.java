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
package com.arcadedb.database;

import com.arcadedb.log.LogManager;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

/**
 * Runs a block of engine work with the calling thread's transactions on one database SUSPENDED, so that every read
 * inside it sees committed state only (issue #7974).
 * <p>
 * The problem it solves is derived state. A vector index graph, like any index, describes the database as the last
 * COMMITTED transaction left it: it is persisted, it outlives the session that built it, and every other transaction
 * searches it. But an engine path that rebuilds such state can be reached from inside a user transaction - a
 * similarity search issued mid-transaction rebuilds the graph synchronously when the resident one is small - and
 * every record it reads on that thread then comes back through that transaction. An embedding written and not yet
 * committed was baked into the graph that way, and the rollback could not take it back: the record reverted, the
 * graph did not, and the index was left describing a row by a vector no committed transaction ever wrote.
 * <p>
 * <b>What suspension means here.</b> The thread's transaction stack for this database is set aside and replaced by a
 * single fresh, never-begun {@link TransactionContext}, exactly the state a thread that never opened a transaction is
 * in. Reads therefore resolve through {@code PageManager}, which serves committed pages, instead of through the
 * caller's record cache and modified pages. On {@link #close()} the original stack is put back, untouched: nothing is
 * committed on the caller's behalf and nothing is rolled back - which is the same contract issue #7058 established
 * for the graph persist, extended from the WRITE the rebuild performs to the READS it performs.
 * <p>
 * <b>It is not a savepoint and not a nested transaction.</b> A nested transaction would isolate the reads just as
 * well (an ArcadeDB nested transaction has its own caches and page map), but it would also consume one of the three
 * nesting levels a thread is allowed, on a path that already opens one of its own for the persist. Suspension costs
 * no nesting level at all, and it isolates a caller that is already nested just as completely as one that is not.
 * <p>
 * <b>Work begun inside the scope belongs to the scope.</b> A block that opens its own transaction (the graph persist
 * does) begins it on the fresh context, and {@link #close()} rolls back anything left active there rather than
 * leaking it onto the restored stack.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class CommittedReadScope implements AutoCloseable {
  /** Handed back when the calling thread has nothing to suspend, so the caller's try-with-resources is uniform. */
  private static final CommittedReadScope NOTHING_TO_SUSPEND = new CommittedReadScope(null, null);

  private final DatabaseContext.DatabaseContextTL context;
  private final List<TransactionContext>         suspended;

  private CommittedReadScope(final DatabaseContext.DatabaseContextTL context, final List<TransactionContext> suspended) {
    this.context = context;
    this.suspended = suspended;
  }

  /**
   * Suspends whatever transactions the calling thread holds on this database, and only when at least one of them is
   * ACTIVE: an inactive context on the stack is what an idle thread carries around between transactions, it answers
   * every read from committed state already, and replacing it would allocate for nothing.
   */
  public static CommittedReadScope open(final DatabaseInternal database) {
    final DatabaseContext.DatabaseContextTL context =
        DatabaseContext.INSTANCE.getContextIfExists(database.getDatabasePath());
    if (context == null)
      return NOTHING_TO_SUSPEND;

    boolean anyActive = false;
    for (int i = 0; i < context.transactions.size(); i++)
      if (context.transactions.get(i).isActive()) {
        anyActive = true;
        break;
      }

    if (!anyActive)
      return NOTHING_TO_SUSPEND;

    final List<TransactionContext> suspended = new ArrayList<>(context.transactions);
    context.transactions.clear();
    context.transactions.add(new TransactionContext(database.getWrappedDatabaseInstance()));
    return new CommittedReadScope(context, suspended);
  }

  @Override
  public void close() {
    if (suspended == null)
      return;

    // Restoring the caller's stack is the one guarantee this class makes, so it happens in a finally: a sweep
    // that dies on an Error would otherwise leave the thread holding the scope's own context and the caller's
    // transactions unreachable, with nothing left able to conclude them.
    try {
      // Anything the block began on the fresh context and did not conclude is ours to discard: it was never the
      // caller's, and leaving it active would restore the caller's stack underneath a live transaction object that
      // nothing would ever conclude.
      //
      // EVERY frame, innermost first, not just the top one. Today there is only ever the one context this scope
      // pushed - a block that opens a transaction of its own finds it inactive and re-begins it in place rather
      // than nesting (see LocalDatabase.begin()) - but the restore below replaces the whole list, so a block that
      // did nest would otherwise have its outer frames dropped still active, holding their file locks with nothing
      // left to release them. Making the sweep exhaustive states that invariant here instead of relying on a call
      // graph this class cannot see.
      for (int i = context.transactions.size() - 1; i >= 0; i--) {
        final TransactionContext own = context.transactions.get(i);
        if (own.isActive())
          try {
            own.rollback();
          } catch (final Exception e) {
            // A failed rollback must not cost the caller its own transactions, which the restore below gives back.
            LogManager.instance().log(this, Level.WARNING,
                "Error rolling back a transaction left open inside a committed-read scope: %s", e.getMessage());
          }
      }
    } finally {
      context.transactions.clear();
      context.transactions.addAll(suspended);
    }
  }
}
