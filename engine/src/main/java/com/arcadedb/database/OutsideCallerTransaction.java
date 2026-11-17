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
 * locks and read snapshot are left exactly as they were. Without a transaction it runs inline.
 * <p>
 * An interrupted wait gives up waiting, not the operation: like a background one, it runs to its end.
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
   */
  public static <T> T call(final DatabaseInternal database, final String operationName, final Callable<T> operation)
      throws Exception {
    if (!database.isTransactionActive())
      return operation.call();

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

    try {
      return task.get();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw e;
    } catch (final ExecutionException e) {
      final Throwable cause = e.getCause();
      if (cause instanceof final Exception exception)
        throw exception;
      if (cause instanceof final Error error)
        throw error;
      throw e;
    }
  }

  /**
   * As {@link #call}, for an operation that throws no checked exception: a checked one it throws all the same (an interrupted
   * wait) is wrapped in a {@link DatabaseOperationException}.
   */
  public static <T> T execute(final DatabaseInternal database, final String operationName, final Callable<T> operation) {
    try {
      return call(database, operationName, operation);
    } catch (final RuntimeException e) {
      throw e;
    } catch (final Exception e) {
      throw new DatabaseOperationException("Error on executing " + operationName + " on database '" + database.getName() + "'", e);
    }
  }
}
