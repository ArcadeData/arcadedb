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
package com.arcadedb.server.backup;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Admits one backup at a time per database across every backup entry point a server has, and names the archives they
 * produce.
 * <p>
 * Before this existed each entry point was on its own: the scheduler's periodic chain never overlapped itself, but an
 * immediate trigger was an independent submit on the same pool, and the HTTP "trigger backup" command ran its own
 * backup inline on the request thread. Any two of them could therefore back up one database at the same time - and
 * because both built the archive name from a second-precision timestamp, two starting in the same second resolved to
 * the same path (issue #6753).
 * <p>
 * There are two answers to that here, and both are needed. The names carry milliseconds, matching what
 * {@code BackupSettings} has always used for the default name of a CLI or SQL backup, so the three server-side
 * conventions agree instead of being coarser than the one they imitate. And an in-progress database is admitted only
 * once, which is what stops the second backup from being started at all: two full backups of one database running
 * together read and compress the same data twice for one usable archive, so the redundant one is refused rather than
 * queued - the caller is told, and a periodic schedule simply covers it on the next tick.
 * <p>
 * The admission is per server instance, not per JVM: an HA test - and a co-located pair of nodes - runs several
 * servers with the same database names in one process, and those backups are genuinely independent.
 * <p>
 * This is an admission policy, not the integrity guarantee. A backup started outside this server (the CLI, another
 * node writing into a shared directory) cannot be seen from here; what keeps THAT from corrupting an archive is
 * {@code FullBackupFormat} creating the target file atomically, so the loser of any race fails before it writes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class BackupCoordinator {
  private static final DateTimeFormatter ARCHIVE_TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmssSSS");
  /**
   * Matches the archives this class names, and the second-precision ones every release before it wrote: retention and
   * the backup listing run over directories that hold both, and a name they cannot parse is a file they silently stop
   * managing - it would never be listed and never be rotated out.
   */
  private static final Pattern           ARCHIVE_NAME_PATTERN     = Pattern.compile(".*-backup-(\\d{8})-(\\d{6}(?:\\d{3})?)\\.zip$");
  private static final DateTimeFormatter ARCHIVE_TIMESTAMP_PARSER = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss[SSS]");

  private final Set<String> inProgress = ConcurrentHashMap.newKeySet();

  /**
   * Reserves this database for a backup. Returns {@code false} when one is already running, in which case the caller
   * must not start a backup and must not call {@link #end(String)}.
   */
  public boolean begin(final String databaseName) {
    return inProgress.add(databaseName);
  }

  /**
   * Releases the reservation taken by a successful {@link #begin(String)}. Always call it from a {@code finally}: a
   * reservation leaked by a failed backup would block every later backup of that database until the server restarts.
   */
  public void end(final String databaseName) {
    inProgress.remove(databaseName);
  }

  public boolean isInProgress(final String databaseName) {
    return inProgress.contains(databaseName);
  }

  /**
   * The name of the archive a backup of this database starting now writes to.
   */
  public String newArchiveName(final String databaseName) {
    return databaseName + "-backup-" + LocalDateTime.now().format(ARCHIVE_TIMESTAMP_FORMAT) + ".zip";
  }

  /**
   * The instant encoded in a backup archive's name, or {@code null} when the name does not follow the convention
   * {@link #newArchiveName(String)} produces. Milliseconds are optional, so an archive written by an older release
   * still reads back.
   */
  public static LocalDateTime parseArchiveTimestamp(final String fileName) {
    final Matcher matcher = ARCHIVE_NAME_PATTERN.matcher(fileName);
    if (!matcher.matches())
      return null;

    try {
      return LocalDateTime.parse(matcher.group(1) + "-" + matcher.group(2), ARCHIVE_TIMESTAMP_PARSER);
    } catch (final RuntimeException e) {
      return null;
    }
  }
}
