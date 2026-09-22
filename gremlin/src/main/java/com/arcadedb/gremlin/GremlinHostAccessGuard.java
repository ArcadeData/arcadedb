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
package com.arcadedb.gremlin;

import com.arcadedb.database.BasicDatabase;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.security.SecurityDatabaseUser;

/**
 * Gate for the Gremlin capabilities whose reach is the host rather than the database: evaluating Groovy (arbitrary JVM
 * code, whatever the {@code SecureASTCustomizer} lets through) and the {@code io()} step (reads and writes a file at a
 * caller-chosen path). They cross every database boundary, so they are reserved to the server administrator
 * ({@link SecurityDatabaseUser#isServerAdministrator()}); no per-database grant authorizes them.
 * <p>
 * The check reads the principal bound to the calling thread, exactly like {@code LocalDatabase.checkPermissionsOnDatabase}:
 * it is a no-op when no user is bound (embedded mode, internal and system contexts), which is what keeps embedded
 * applications working unchanged.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class GremlinHostAccessGuard {
  private GremlinHostAccessGuard() {
  }

  /**
   * @param database   the database the Gremlin statement runs against
   * @param capability what the caller is about to do, for the error message (e.g. "evaluate Groovy")
   *
   * @throws SecurityException if a user is bound to the calling thread and is not the server administrator
   */
  public static void checkServerAdministrator(final BasicDatabase database, final String capability) {
    final DatabaseContext.DatabaseContextTL context = DatabaseContext.INSTANCE.getContextIfExists(database.getDatabasePath());
    if (context == null)
      return;

    final SecurityDatabaseUser user = context.getCurrentUser();
    if (user == null || user.isServerAdministrator())
      return;

    throw new SecurityException(
        "User '" + user.getName() + "' is not allowed to " + capability + " in Gremlin: it is reserved to the server administrator");
  }
}
