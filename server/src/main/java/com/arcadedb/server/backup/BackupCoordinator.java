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
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
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
 * conventions agree instead of being coarser than the one they imitate - the convention is written down twice, since
 * the server cannot depend on the integration module, and
 * {@code BackupCoordinatorTest#theDefaultNameOfACliOrSqlBackupFollowsTheSameConvention} fails if the two drift. And
 * an in-progress database is admitted only
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
 * <h2>Restores and imports</h2>
 * The slot admits more than backups now. A restore is the one operation that <i>destroys</i> a database directory -
 * it restores into a temporary sibling and then drops the target and moves the temporary one over it - and it used to
 * take nothing at all, so two restores of one database both passed the existence pre-check and both reached the swap,
 * and a restore could drop the directory a backup was reading (issue #7384). Restores and imports therefore take the
 * same per-database slot, and {@link Operation} says which one holds it so the refusal can name it.
 * <p>
 * Two operations on one database conflict unless they are a backup and an import: those two coexist by construction,
 * because an import is ordinary transactions against a live database and backing a live database up is what the
 * auto-backup schedule does all day. Everything else is refused - see {@link Operation#conflictsWith}.
 * <p>
 * The class name predates restores and is kept so that the callers and tests written against
 * {@code getBackupCoordinator()} keep compiling; what it coordinates is now every whole-database maintenance
 * operation this server runs, not only backups.
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

  /**
   * The whole-database maintenance operations that share one per-database slot.
   * <p>
   * {@link #conflictsWith} is the whole admission policy: two operations on one database exclude each other unless
   * one is a {@link #BACKUP} and the other an {@link #IMPORT}. A restore excludes everything because it drops and
   * replaces the database directory; a backup excludes another backup because two full backups of one database read
   * and compress the same data twice for one usable archive (issue #6753); an import excludes another import because
   * an import creates the database it loads into, so the second one could not have created it anyway.
   */
  public enum Operation {
    BACKUP("back up", "a backup"), RESTORE("restore", "a restore"), IMPORT("import", "an import");

    private final String verb;
    private final String phrase;

    Operation(final String verb, final String phrase) {
      this.verb = verb;
      this.phrase = phrase;
    }

    /**
     * The operation as a verb, for "Cannot back up database 'x'". Carried rather than derived from the enum
     * constant, because the verb for a backup is two words.
     */
    public String verb() {
      return verb;
    }

    /**
     * The operation's name with its indefinite article, for "... a restore of it is already in progress". Carried
     * rather than derived, because "a import" is what deriving it produces.
     */
    public String phrase() {
      return phrase;
    }

    /**
     * Whether an operation of this kind, already running on a database, refuses one of kind {@code other} on the
     * same database.
     */
    public boolean conflictsWith(final Operation other) {
      return this == other || this == RESTORE || other == RESTORE;
    }
  }

  /**
   * The operations currently running per database. A value is never an empty set - the entry is removed instead -
   * so {@code containsKey} answers "is anything running on it".
   * <p>
   * {@link EnumSet} is exact here rather than merely convenient: every kind conflicts with itself, so at most one
   * operation of each kind is ever admitted for one database and a set needs no multiplicity.
   * <p>
   * Every value is replaced rather than mutated in place, so a set a reader has already been handed is never written
   * to by another thread: {@code EnumSet} is not thread-safe, and {@link #isInProgress(String, Operation)} reads one
   * outside the map's own per-entry lock.
   */
  private final Map<String, EnumSet<Operation>> inProgress = new ConcurrentHashMap<>();

  /**
   * Reserves this database for a backup. Returns {@code false} when a backup, restore or import of it is already
   * running, in which case the caller must not start a backup and must not call {@link #end(String)}.
   * <p>
   * The shorthand every backup entry point uses. {@link #begin(String, Operation)} is the same reservation for a
   * caller that wants to name the operation already holding the slot in its refusal.
   */
  public boolean begin(final String databaseName) {
    return begin(databaseName, Operation.BACKUP) == null;
  }

  /**
   * Reserves this database for {@code operation}.
   * <p>
   * When more than one operation is running - which only {@link Operation#BACKUP} and {@link Operation#IMPORT}
   * together can be - the one named is whichever the iteration reaches first, not a ranking: both refuse the caller
   * equally, and the message is true of either.
   *
   * @return {@code null} when the reservation was taken - the caller must then release it with
   * {@link #end(String, Operation)} from a {@code finally} - or the operation already running that refuses this one.
   */
  public Operation begin(final String databaseName, final Operation operation) {
    final AtomicReference<Operation> conflict = new AtomicReference<>();

    inProgress.compute(databaseName, (name, running) -> {
      if (running == null)
        return EnumSet.of(operation);

      for (final Operation active : running)
        if (active.conflictsWith(operation)) {
          conflict.set(active);
          return running;
        }

      final EnumSet<Operation> updated = EnumSet.copyOf(running);
      updated.add(operation);
      return updated;
    });

    return conflict.get();
  }

  /**
   * Releases the backup reservation taken by a successful {@link #begin(String)}. Always call it from a
   * {@code finally}: a reservation leaked by a failed backup would block every later backup of that database until
   * the server restarts.
   */
  public void end(final String databaseName) {
    end(databaseName, Operation.BACKUP);
  }

  /**
   * Releases the reservation a successful {@link #begin(String, Operation)} took. Always call it from a
   * {@code finally}: a leaked reservation blocks every later operation of that database until the server restarts.
   */
  public void end(final String databaseName, final Operation operation) {
    inProgress.computeIfPresent(databaseName, (name, running) -> {
      if (!running.contains(operation))
        return running;

      final EnumSet<Operation> updated = EnumSet.copyOf(running);
      updated.remove(operation);
      return updated.isEmpty() ? null : updated;
    });
  }

  /** Whether any backup, restore or import of this database is currently running. */
  public boolean isInProgress(final String databaseName) {
    return inProgress.containsKey(databaseName);
  }

  /** Whether an operation of this kind is currently running on this database. */
  public boolean isInProgress(final String databaseName, final Operation operation) {
    final EnumSet<Operation> running = inProgress.get(databaseName);
    return running != null && running.contains(operation);
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
