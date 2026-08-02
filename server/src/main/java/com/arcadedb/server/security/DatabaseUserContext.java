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
package com.arcadedb.server.security;

import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.security.SecurityDatabaseUser;

import java.util.function.Supplier;

/**
 * Binds an authenticated principal onto the current thread's {@link DatabaseContext} so the engine's per-user
 * permission gates ({@code LocalDatabase.checkPermissionsOnDatabase} / {@code checkPermissionsOnFile}) actually
 * enforce. Those gates are deliberately no-ops when no user is bound, which is the mechanism embedded and
 * HA-apply contexts use to skip checks, so a transport that fails to bind silently grants every caller
 * unrestricted access (GHSA-6x73-v3rc-f57c).
 * <p>
 * Every transport here runs on pooled worker threads, so a binding that is never undone leaks the principal
 * onto the next request the pool hands that thread. {@link #runAs} is the safe form: it restores whatever was
 * bound before, on every exit path.
 */
public class DatabaseUserContext {

  private DatabaseUserContext() {
  }

  /**
   * Binds the principal without restoring anything. The caller takes responsibility for clearing the thread's
   * contexts afterwards; prefer {@link #runAs} where the scope of the binding is a single call.
   */
  public static void bind(final DatabaseInternal database, final ServerSecurityUser user) {
    contextFor(database).setCurrentUser(user.getDatabaseUser(database));
  }

  /**
   * Runs an action with the principal bound, restoring the previous binding before returning or propagating.
   */
  public static <T> T runAs(final DatabaseInternal database, final ServerSecurityUser user,
      final Supplier<T> action) {
    final DatabaseContext.DatabaseContextTL context = contextFor(database);
    final SecurityDatabaseUser previous = context.getCurrentUser();
    context.setCurrentUser(user.getDatabaseUser(database));
    try {
      return action.get();
    } finally {
      context.setCurrentUser(previous);
    }
  }

  private static DatabaseContext.DatabaseContextTL contextFor(final DatabaseInternal database) {
    final DatabaseContext.DatabaseContextTL context = DatabaseContext.INSTANCE.getContextIfExists(
        database.getDatabasePath());
    return context != null ? context : DatabaseContext.INSTANCE.init(database);
  }
}
