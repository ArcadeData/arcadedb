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
package com.arcadedb.engine;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;

/**
 * Where this server keeps its backups, as the engine sees it.
 * <p>
 * The resolution itself is the server's - {@code ServerControlPlane.resolveBackupDirectory()} is the only
 * implementation, and it consults the running auto-backup plugin's configuration first, then
 * {@code config/backup.json} on disk even when the schedule is off, then
 * {@link com.arcadedb.GlobalConfiguration#SERVER_BACKUP_DIRECTORY}. This interface is what the engine can name,
 * because the dependency runs from the server to the engine and not back, and the server binds an instance to
 * every database it opens under {@link #WRAPPER_NAME} - exactly as it binds {@link MaintenanceCoordinator}, and
 * for the same reason.
 * <p>
 * <b>Why the engine needs to name it at all.</b> {@code BACKUP DATABASE} is a SQL statement the engine executes,
 * reached over HTTP, Postgres, gRPC, Bolt or the console like any other. Issue #7392 gave every control-plane
 * backup command one definition of the directory and left the statement reading
 * {@code arcadedb.server.backupDirectory} directly, so on the very configuration #7392 was filed about - a
 * {@code config/backup.json} naming a different directory - the archive a {@code BACKUP DATABASE} wrote landed
 * where {@code list backups} does not look and {@code delete backup} and {@code restore backup} cannot reach:
 * invisible, undeletable and unrestorable through the API, and never retention-pruned (issue #7863).
 * <p>
 * A database with no resolver bound - an embedded process with no server in it, or a {@code RemoteDatabase},
 * whose statements execute on the server that does have one - falls back to the global setting, which is what
 * the statement did before. The answer is resolved per call rather than cached, so {@code set backup config}
 * changing the directory at run time is seen by the next statement.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@FunctionalInterface
public interface BackupDirectoryResolver {
  /** The {@link DatabaseInternal#setWrapper} name the server binds its resolver to. */
  String WRAPPER_NAME = "backup-directory-resolver";

  /**
   * The absolute directory this server writes backups into, WITHOUT the per-database sub-directory: every caller
   * appends the database name itself, which is the layout {@code ServerControlPlane} lists, deletes and restores
   * from.
   */
  String resolveBackupDirectory();

  /**
   * The resolver bound to this database, or {@code null} when there is none.
   * <p>
   * Every wrapper delegates {@link DatabaseInternal#getWrappers()} down to the embedded instance, so the answer
   * does not depend on which layer the caller happens to hold.
   */
  static BackupDirectoryResolver boundTo(final Database database) {
    if (database instanceof DatabaseInternal internal
        && internal.getWrappers().get(WRAPPER_NAME) instanceof BackupDirectoryResolver resolver)
      return resolver;
    return null;
  }

  /**
   * The backup directory for {@code database}, falling back to {@code globalSetting} when no server bound one -
   * or when the bound one answers nothing usable, which keeps a misconfigured server writing where it always did
   * rather than failing the backup outright.
   */
  static String resolveFor(final Database database, final String globalSetting) {
    final BackupDirectoryResolver resolver = boundTo(database);
    if (resolver == null)
      return globalSetting;

    final String resolved = resolver.resolveBackupDirectory();
    return resolved == null || resolved.isBlank() ? globalSetting : resolved;
  }
}
