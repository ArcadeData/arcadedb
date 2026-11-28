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

import com.arcadedb.security.SecurityDatabaseUser;

import java.util.concurrent.Callable;

/**
 * Runs a task on another thread as the user who handed it over.
 * <p>
 * The permission checks of a database read the user bound to the thread that runs them, so a task that is handed to a pool of
 * the engine's own (the user code of the script and java engines) is given that user. The pool threads outlive the command, so
 * the user is bound for the duration of the task only and the one the thread had before is put back when the task ends, however
 * it ends.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
public final class CallerUser {
  private CallerUser() {
  }

  /**
   * Wraps {@code task} so that, wherever it runs, it runs as the user bound to the CALLING thread at the time of this call.
   * Called on the thread that owns the command, as the user is read from it here.
   *
   * @param database the database the task works on, as the caller holds it
   * @param task     the work to hand over
   *
   * @return the task to submit in place of {@code task}
   */
  public static <T> Callable<T> bind(final DatabaseInternal database, final Callable<T> task) {
    final String databasePath = database.getDatabasePath();
    final DatabaseContext.DatabaseContextTL callerContext = DatabaseContext.INSTANCE.getContextIfExists(databasePath);
    final SecurityDatabaseUser user = callerContext != null ? callerContext.getCurrentUser() : null;

    return () -> {
      // A THREAD THAT ALREADY HAS ITS CONTEXT KEEPS IT, TRANSACTION INCLUDED: A POOL THAT RUNS THE TASK ON THE SUBMITTING THREAD
      // (SATURATED) ENDS HERE, WITH THE SAME USER BOUND AS BEFORE
      DatabaseContext.DatabaseContextTL context = DatabaseContext.INSTANCE.getContextIfExists(databasePath);
      if (context == null) {
        if (user == null)
          // NOTHING BOUND ON THIS THREAD, NOTHING TO BIND
          return task.call();
        // A FIRST USE OF THE DATABASE ON THIS THREAD: THE CONTEXT STAYS WITH THE THREAD AFTER THE TASK, LIKE THE ONE ANY DATABASE
        // CALL MAKES, AND ONLY THE USER IS TAKEN OFF IT
        context = DatabaseContext.INSTANCE.init(database);
      }

      final SecurityDatabaseUser previous = context.getCurrentUser();
      context.setCurrentUser(user);
      try {
        return task.call();
      } finally {
        context.setCurrentUser(previous);
      }
    };
  }
}
