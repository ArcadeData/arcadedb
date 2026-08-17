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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.security.SecurityDatabaseUser;

import java.util.Map;
import java.util.logging.Level;

public class DatabaseAsyncCommand implements DatabaseAsyncTask {
  public final boolean                idempotent;
  public final String                 language;
  public final String                 command;
  public final Object[]               parameters;
  public final Map<String, Object>    parametersMap;
  public final AsyncResultsetCallback userCallback;
  public final ContextConfiguration   configuration;
  // The principal that submitted the command, captured on the calling (e.g. HTTP) thread. It is bound onto the worker
  // thread's DatabaseContext before execution so the engine per-user permission gates enforce on the async path too
  // (GHSA-5j4x-3jfw-8xv3). Null in embedded/internal use, where no user is bound and the gates are a no-op.
  public final SecurityDatabaseUser   requestUser;

  public DatabaseAsyncCommand(final ContextConfiguration configuration, final boolean idempotent, final String language,
      final String command, final Object[] parameters,
      final AsyncResultsetCallback userCallback) {
    this(configuration, idempotent, language, command, parameters, null, userCallback, null);
  }

  public DatabaseAsyncCommand(final ContextConfiguration configuration, final boolean idempotent, final String language,
      final String command, final Map<String, Object> parametersMap,
      final AsyncResultsetCallback userCallback) {
    this(configuration, idempotent, language, command, null, parametersMap, userCallback, null);
  }

  public DatabaseAsyncCommand(final ContextConfiguration configuration, final boolean idempotent, final String language,
      final String command, final Object[] parameters,
      final AsyncResultsetCallback userCallback, final SecurityDatabaseUser requestUser) {
    this(configuration, idempotent, language, command, parameters, null, userCallback, requestUser);
  }

  public DatabaseAsyncCommand(final ContextConfiguration configuration, final boolean idempotent, final String language,
      final String command, final Map<String, Object> parametersMap,
      final AsyncResultsetCallback userCallback, final SecurityDatabaseUser requestUser) {
    this(configuration, idempotent, language, command, null, parametersMap, userCallback, requestUser);
  }

  private DatabaseAsyncCommand(final ContextConfiguration configuration, final boolean idempotent, final String language,
      final String command, final Object[] parameters, final Map<String, Object> parametersMap,
      final AsyncResultsetCallback userCallback, final SecurityDatabaseUser requestUser) {
    this.configuration = configuration;
    this.idempotent = idempotent;
    this.language = language;
    this.command = command;
    this.parameters = parameters;
    this.parametersMap = parametersMap;
    this.userCallback = userCallback;
    this.requestUser = requestUser;
  }

  /**
   * Runs the command, notifies {@code onComplete}, and CONTAINS its own failure - reporting it through
   * {@code onError} and rolling back what it dirtied.
   * <p>
   * Kept here rather than lifted into the caller when issue #6303 gave this task a second home: it now runs either
   * on an async worker (everything that is not DDL) or on {@link AsyncCommandPool} (the DDL that cannot), and only
   * this method is common to both. On the worker, the run loop's own catch reports to the EXECUTOR-wide error
   * callback and never to this command's, so lifting the reporting out would have silently stopped delivering
   * {@code onError} to the submitter of every non-DDL command.
   * <p>
   * The rollback is likewise right on both paths, for slightly different reasons: on a worker it keeps a failed
   * write from contaminating the SHARED batch transaction it was executed in, and on the pool it discards the
   * command's own. An idempotent query dirties nothing and must not roll back a batch it merely read from.
   */
  @Override
  public void execute(final DatabaseAsyncExecutorImpl.AsyncThread async, final DatabaseInternal database) {
    // Bind the submitting principal onto this thread's DatabaseContext so the engine permission gates
    // (LocalDatabase.checkPermissionsOnDatabase/checkPermissionsOnFile, and the polyglot scripting gate) enforce
    // exactly as on the synchronous transports. Restore the previous binding afterwards, since the thread and its
    // DatabaseContext are reused across tasks submitted by different users (GHSA-5j4x-3jfw-8xv3).
    final DatabaseContext.DatabaseContextTL dbContext = DatabaseContext.INSTANCE.getContextIfExists(database.getDatabasePath());
    final SecurityDatabaseUser previousUser = dbContext != null ? dbContext.getCurrentUser() : null;
    if (dbContext != null)
      dbContext.setCurrentUser(requestUser);

    try (final ResultSet resultset = idempotent ?
        parametersMap != null ?
            database.query(language, command, configuration, parametersMap) :
            database.query(language, command, configuration, parameters) :
        parametersMap != null ?
            database.command(language, command, configuration, parametersMap) :
            database.command(language, command, configuration, parameters)) {

      if (userCallback != null)
        userCallback.onComplete(resultset);

    } catch (final Exception e) {
      if (!idempotent && database.isTransactionActive()) {
        try {
          database.rollback();
        } catch (final Exception re) {
          LogManager.instance().log(this, Level.WARNING, "Error on rolling back active transaction", re);
        }
      }

      notifyError(e);
    } finally {
      if (dbContext != null)
        dbContext.setCurrentUser(previousUser);
    }
  }

  /**
   * Reports a failure to the submitter's callback. Called by {@link #execute} for anything the command itself raised,
   * and by the pool runner for the few failures raised around it (the transaction it opens and commits).
   *
   * @return whether anybody received it. {@code false} means the failure has nowhere to go but the log, which is what
   *     decides whether the executor logs it - a command whose submitter asked to be told is its own reporter, and
   *     duplicating that at SEVERE is how one failure becomes two entries in an operator's console.
   */
  boolean notifyError(final Throwable e) {
    if (userCallback == null)
      return false;
    try {
      userCallback.onError(e instanceof final Exception exception ? exception : new Exception(e));
    } catch (final Throwable callbackError) {
      // Never let the callback's own failure escape onto the pool thread: it would kill the worker and, worse, skip
      // the bookkeeping that releases anything waiting on this command.
      LogManager.instance().log(this, Level.WARNING, "Error on invoking the error callback of %s", callbackError, this);
    }
    return true;
  }

  @Override
  public String toString() {
    return (idempotent ? "Query" : "Command") + "(" + language + "," + command + ")";
  }
}
