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

import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.security.SecurityDatabaseUser;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

/**
 * Runs a maintenance operation that is not part of the caller's transaction (COMPACT INDEX, CHECK DATABASE, BACKUP DATABASE,
 * ALIGN DATABASE) without touching that transaction (issues #11580, #11781).
 * <p>
 * Such an operation manages transactions of its own (CHECK DATABASE commits its repairs in batches, COMPRESS a page batch at a
 * time), rewrites committed files, or reads the database as committed, and none of it can be rolled back with the caller's
 * work. Run on the caller's thread it used to roll that transaction back first, so the writes made before the statement were
 * lost and the next statement failed with "Transaction not begun", naming nothing. With a transaction open the operation runs on
 * a thread of its own, with the caller's user, and this thread waits for it: the caller's transaction, its pending changes,
 * locks and read snapshot are left exactly as they were. Without a transaction it runs inline. A caller holding explicit locks
 * (LOCK TYPE, LOCK BUCKET) is refused an operation that writes: its commits would wait for those locks while the caller waits for it.
 * <p>
 * An interrupt does not end the wait: the operation runs to its end, the caller returns once it has, with its interrupt flag set.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class OutsideCallerTransaction {
  private OutsideCallerTransaction() {
  }

  /**
   * Runs {@code operation} outside the caller's transaction, rethrowing what it throws.
   *
   * @param database      the database the operation works on, as the caller holds it
   * @param operationName names the thread the operation runs on when the caller has a transaction open
   * @param writes        whether the operation commits changes, which take the file locks a caller may hold explicitly: such an
   *                      operation is refused inside a transaction holding explicit locks, a read-only one is not
   */
  public static <T> T call(final DatabaseInternal database, final String operationName, final boolean writes,
      final Callable<T> operation) throws Exception {
    if (!database.isTransactionActive())
      return operation.call();

    if (writes && database.getTransaction().holdsExplicitLocks())
      // The operation's own commits would wait for the files this caller locked, while the caller waits for the operation: it
      // could only end with a lock timeout. Refused at once, naming the way out
      throw new CommandSemanticException(
          operationName + " cannot run inside a transaction that holds explicit locks (LOCK TYPE, LOCK BUCKET): commit or roll back "
              + "that transaction first");

    final DatabaseContext.DatabaseContextTL callerContext = DatabaseContext.INSTANCE.getContextIfExists(database.getDatabasePath());
    final SecurityDatabaseUser user = callerContext != null ? callerContext.getCurrentUser() : null;

    final FutureTask<T> task = new FutureTask<>(() -> {
      final DatabaseContext.DatabaseContextTL context = DatabaseContext.INSTANCE.init(database);
      // the operation is the caller's: its permission checks see the caller's user, not an unauthenticated thread
      context.setCurrentUser(user);
      try {
        return operation.call();
      } finally {
        DatabaseContext.INSTANCE.removeContext(database.getDatabasePath());
      }
    });
    new Thread(task, "ArcadeDB-" + operationName + "-" + database.getName()).start();

    // An interrupt does not end the wait: the operation runs to its end anyway, and the caller unwinding first would release
    // what it holds for the operation (a backup's maintenance slot, its progress entry) while the operation still uses the
    // database. The interrupt is kept for the caller to see once the operation is over
    boolean interrupted = false;
    try {
      while (true) {
        try {
          return task.get();
        } catch (final InterruptedException e) {
          interrupted = true;
        }
      }
    } catch (final ExecutionException e) {
      final Throwable cause = e.getCause();
      if (cause instanceof final Exception exception)
        throw exception;
      if (cause instanceof final Error error)
        throw error;
      throw e;
    } finally {
      if (interrupted)
        Thread.currentThread().interrupt();
    }
  }

  /**
   * As {@link #call}, for an operation that throws no checked exception: a checked one it throws all the same is wrapped in a
   * {@link DatabaseOperationException}.
   */
  public static <T> T execute(final DatabaseInternal database, final String operationName, final boolean writes,
      final Callable<T> operation) {
    try {
      return call(database, operationName, writes, operation);
    } catch (final RuntimeException e) {
      throw e;
    } catch (final Exception e) {
      throw new DatabaseOperationException("Error on executing " + operationName + " on database '" + database.getName() + "'", e);
    }
  }
}
