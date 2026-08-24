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
package com.arcadedb.database.async;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.security.SecurityDatabaseUser;

public class DatabaseAsyncTransaction implements DatabaseAsyncTask {
  public final Database.TransactionScope tx;
  public final  int           retries;
  private final OkCallback    onOkCallback;
  private final ErrorCallback onErrorCallback;
  // The principal that submitted the transaction, captured on the calling thread. It is bound onto the worker
  // thread's DatabaseContext before execution so the engine per-user permission gates enforce on this async path
  // too, the same as DatabaseAsyncCommand. Null in embedded/internal use, where no user is bound and the gates
  // are a no-op.
  public final SecurityDatabaseUser requestUser;

  public DatabaseAsyncTransaction(final Database.TransactionScope tx, final int retries, final OkCallback okCallback, final ErrorCallback errorCallback) {
    this(tx, retries, okCallback, errorCallback, null);
  }

  public DatabaseAsyncTransaction(final Database.TransactionScope tx, final int retries, final OkCallback okCallback,
      final ErrorCallback errorCallback, final SecurityDatabaseUser requestUser) {
    this.tx = tx;
    this.retries = retries;
    this.onOkCallback = okCallback;
    this.onErrorCallback = errorCallback;
    this.requestUser = requestUser;
  }

  @Override
  public boolean requiresActiveTx() {
    return false;
  }

  @Override
  public void execute(final DatabaseAsyncExecutorImpl.AsyncThread async, final DatabaseInternal database) {
    // Bind the submitting principal onto this thread's DatabaseContext so the engine permission gates
    // (LocalDatabase.checkPermissionsOnFile) enforce exactly as on the synchronous transports. Restore the
    // previous binding afterwards, since the thread and its DatabaseContext are reused across tasks submitted
    // by different users.
    final DatabaseContext.DatabaseContextTL dbContext = DatabaseContext.INSTANCE.getContextIfExists(database.getDatabasePath());
    final SecurityDatabaseUser previousUser = dbContext != null ? dbContext.getCurrentUser() : null;
    if (dbContext != null)
      dbContext.setCurrentUser(requestUser);

    try {
      executeTransaction(async, database);
    } finally {
      if (dbContext != null)
        dbContext.setCurrentUser(previousUser);
    }
  }

  private void executeTransaction(final DatabaseAsyncExecutorImpl.AsyncThread async, final DatabaseInternal database) {
    ConcurrentModificationException lastException = null;

    if (database.isTransactionActive())
      database.commit();

    for (int retry = 0; retry < retries + 1; ++retry) {
      try {
        database.begin();
        tx.execute();
        database.commit();

        lastException = null;

        if (onOkCallback != null)
          onOkCallback.call();

        // OK
        break;

      } catch (final ConcurrentModificationException e) {
        // RETRY
        lastException = e;
        if (database.isTransactionActive())
          database.rollback();

      } catch (final Exception e) {
        if (database.getTransaction().isActive())
          database.rollback();

        async.onError(e);

        if (onErrorCallback != null)
          onErrorCallback.call(e);

        throw e;
      }
    }

    if (lastException != null) {
      if (database.isTransactionActive())
        database.rollback();
      if (onErrorCallback != null)
        onErrorCallback.call(lastException);
      throw lastException;
    }
  }

  @Override
  public String toString() {
    return "Transaction(" + tx + ")";
  }
}
