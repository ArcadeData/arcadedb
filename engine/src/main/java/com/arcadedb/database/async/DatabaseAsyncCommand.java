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

  @Override
  public void execute(final DatabaseAsyncExecutorImpl.AsyncThread async, final DatabaseInternal database) {
    // Bind the submitting principal onto this worker thread's DatabaseContext so the engine permission gates
    // (LocalDatabase.checkPermissionsOnDatabase/checkPermissionsOnFile, and the polyglot scripting gate) enforce
    // exactly as on the synchronous transports. Restore the previous binding afterwards, since the worker thread and
    // its DatabaseContext are reused across tasks submitted by different users (GHSA-5j4x-3jfw-8xv3).
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
      // A failed write command leaves dirty pages in the shared batch transaction that would be
      // persisted at the next commit-every boundary. Roll back so the failure does not contaminate
      // the batch. Read queries (idempotent) produce no dirty pages, so they must not roll back the
      // pending writes of prior tasks in the same commit window.
      if (!idempotent && database.isTransactionActive()) {
        try {
          database.rollback();
        } catch (final Exception re) {
          LogManager.instance().log(this, Level.WARNING, "Error on rolling back active transaction", re);
        }
      }

      if (userCallback != null)
        userCallback.onError(e);
    } finally {
      if (dbContext != null)
        dbContext.setCurrentUser(previousUser);
    }
  }

  @Override
  public String toString() {
    return (idempotent ? "Query" : "Command") + "(" + language + "," + command + ")";
  }
}
