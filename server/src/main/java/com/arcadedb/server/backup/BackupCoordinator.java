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

import com.arcadedb.engine.MaintenanceCoordinator;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
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
 * Two operations on one database conflict unless one of them is something other than a restore and they are not
 * the same kind twice: a backup and an import coexist by construction, because an import is ordinary transactions
 * against a live database and backing a live database up is what the auto-backup schedule does all day. The one
 * exemption from "not the same kind twice" is {@link Operation#EXPORT}, added in issue #7450: a SQL
 * {@code EXPORT DATABASE} names its own target, so two exports of one database are two different archives rather
 * than two writers of one file, and both are admitted. That is why what is stored per database is a COUNT per kind
 * rather than a set of kinds - two concurrent exports each release only their own reservation, where a set let the
 * first one to finish free the database under the second. See {@link Operation#conflictsWith}.
 * <p>
 * An HA snapshot install takes {@link Operation#RESTORE} too, and for the same reason: it closes this node's copy of
 * the database, swaps its directory for the leader's snapshot and reopens it, which is a restore of this node's copy
 * whoever asked for it. That is what closes the one pair the per-server scoping above cannot see - a restore on the
 * leader replicates as an install entry, and a follower running its own scheduled backup of that database answers
 * the entry by replacing the directory the backup is reading (issue #7444). Unlike every other holder the install
 * cannot simply be refused, because it applies a committed Raft entry: it uses
 * {@link #begin(String, Operation, long)}, which waits out a conflicting operation for a bounded time first.
 * <p>
 * The class name predates restores and is kept so that the callers and tests written against
 * {@code getBackupCoordinator()} keep compiling; what it coordinates is now every whole-database maintenance
 * operation this server runs, not only backups.
 * <h2>The engine's view of it</h2>
 * {@link Operation} and the reservation contract live in {@link MaintenanceCoordinator}, in the engine, and this
 * class is its only implementation. {@code BACKUP DATABASE}, {@code IMPORT DATABASE} and {@code EXPORT DATABASE}
 * are SQL statements the ENGINE executes, so until issues #7443 and #7450 they could not reach this policy at all
 * and took nothing: a SQL backup ran unseen by a concurrent restore, a second SQL backup of one database was not
 * refused either, and a SQL export read a directory a restore was about to replace. The server binds this instance
 * to every database it opens (see {@code ServerDatabase}), which is how a statement finds it without the engine
 * depending on the server.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class BackupCoordinator implements MaintenanceCoordinator {
  private static final DateTimeFormatter ARCHIVE_TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmssSSS", Locale.ROOT);
  /**
   * Matches the archives this class names, and the second-precision ones every release before it wrote: retention and
   * the backup listing run over directories that hold both, and a name they cannot parse is a file they silently stop
   * managing - it would never be listed and never be rotated out.
   */
  private static final Pattern           ARCHIVE_NAME_PATTERN     = Pattern.compile(".*-backup-(\\d{8})-(\\d{6}(?:\\d{3})?)\\.zip$");
  private static final DateTimeFormatter ARCHIVE_TIMESTAMP_PARSER = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss[SSS]");

  /**
   * Cached because {@link Operation#values()} clones its array on every call, and {@link #begin(String, Operation)}
   * walks it on every reservation.
   */
  private static final Operation[] OPERATIONS = Operation.values();

  /**
   * How many reservations of each kind are currently running per database, indexed by {@link Operation#ordinal()}.
   * A value is never all-zero - the entry is removed instead - so {@code containsKey} answers "is anything running
   * on it".
   * <p>
   * A COUNT rather than the {@link java.util.EnumSet} this used to hold, because {@link Operation#EXPORT} does not
   * exclude a second of its own kind (issue #7450): two exports of one database are both admitted, and each has to
   * release only its own claim. With a set of kinds the first of the two to finish cleared the only entry, and a
   * restore walked in while the second export was still reading the directory. Every other kind conflicts with
   * itself, so its count never exceeds one.
   * <p>
   * An {@code int[]} rather than a map of boxed counters: it is four ints, it is copied on every write, and the
   * copy is what keeps a reader safe. Every value is REPLACED rather than mutated in place, so an array a reader
   * has already been handed is never written to by another thread - {@link #isInProgress(String, Operation)} reads
   * one outside the map's own per-entry lock.
   */
  private final Map<String, int[]> inProgress = new ConcurrentHashMap<>();

  /**
   * How many callers of {@link #begin(String, Operation, long)} are currently waiting for a {@link Operation#RESTORE}
   * on a database, indexed by database name. A value is never zero or less - the entry is removed instead.
   * <p>
   * Only {@code RESTORE} ever calls that overload (the HA snapshot install, applying a committed Raft entry), and
   * {@link Operation#RESTORE} already conflicts with everything, so this is only ever consulted to decide whether a
   * DIFFERENT kind of new reservation should queue behind it - see the guard in {@link #begin(String, Operation)}.
   * <p>
   * This is what closes issue #7646: {@link Operation#EXPORT} is the one kind {@link Operation#conflictsWith}
   * admits without limit, so a steady stream of exports arriving after a restore started waiting could keep the
   * per-database count above zero forever and the restore's bounded wait would expire every single time - starving
   * it rather than merely delaying it, and silently, because the timeout expiring looks identical to an ordinary
   * conflict that happened to outlast the wait. Once a restore is registered here, a fresh export is refused before
   * it is ever admitted, so the exports already running are the last ones the restore has to wait out.
   */
  private final Map<String, AtomicInteger> waitingRestores = new ConcurrentHashMap<>();

  /**
   * The monitor a bounded wait parks on, notified by every {@link #end(String, Operation)}.
   * <p>
   * One monitor for the whole coordinator rather than one per database: a server runs a handful of these
   * operations a day, so the spurious wakeups a release on an unrelated database causes cost a re-check of a
   * {@link ConcurrentHashMap} entry and nothing else, and a per-database monitor would need its own lifecycle to
   * avoid leaking an entry per database name ever waited on.
   */
  private final Object slotReleased = new Object();

  /**
   * Reserves this database for a backup. Returns {@code false} when a backup or a restore of it is already running -
   * the two kinds {@link Operation#BACKUP} conflicts with - in which case the caller must not start a backup and
   * must not call {@link #end(String)}. An import or an export running on the database does not refuse it.
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
   * When more than one operation is running, the one named is whichever the iteration reaches first, not a ranking:
   * every one of them refuses the caller equally, and the message is true of any of them.
   *
   * @return {@code null} when the reservation was taken - the caller must then release it with
   * {@link #end(String, Operation)} from a {@code finally} - or the operation already running that refuses this one.
   */
  @Override
  public Operation begin(final String databaseName, final Operation operation) {
    // A RESTORE ALREADY WAITING ON THIS DATABASE TAKES PRIORITY OVER EVERY NEW RESERVATION OF A DIFFERENT KIND,
    // EVEN WHEN NOTHING IS CURRENTLY IN PROGRESS (issue #7646): OTHERWISE A FRESH Operation.EXPORT - ADMITTED
    // WITHOUT LIMIT - CAN ALWAYS SLIP IN BETWEEN THE LAST EXPORT DRAINING AND THE WAITING RESTORE'S NEXT RE-CHECK,
    // AND THE COUNT NEVER REACHES ZERO. RESTORE ITSELF IS EXEMPT, OR THE WAITER'S OWN RE-CHECK - CALLED FROM
    // INSIDE ITS BOUNDED WAIT LOOP IN begin(String, Operation, long) - WOULD REFUSE ITSELF ON ITS OWN REGISTRATION.
    if (operation != Operation.RESTORE && isRestoreWaiting(databaseName))
      return Operation.RESTORE;

    final AtomicReference<Operation> conflict = new AtomicReference<>();

    inProgress.compute(databaseName, (name, running) -> {
      if (running == null) {
        final int[] started = new int[OPERATIONS.length];
        started[operation.ordinal()] = 1;
        return started;
      }

      for (final Operation active : OPERATIONS)
        if (running[active.ordinal()] > 0 && active.conflictsWith(operation)) {
          conflict.set(active);
          return running;
        }

      final int[] updated = running.clone();
      updated[operation.ordinal()]++;
      return updated;
    });

    return conflict.get();
  }

  /**
   * Reserves this database for {@code operation}, waiting up to {@code timeoutMs} for a conflicting operation to
   * finish rather than refusing straight away.
   * <p>
   * Every other caller may simply be refused: a scheduled backup is covered again on the next tick, and a restore or
   * an import is an operator command that can be retried. An HA snapshot install cannot - it applies a committed
   * Raft entry, and a follower that declines to apply one diverges from the cluster (issue #7444). So it needs a
   * third answer between taking the slot and giving up: wait for whatever is in the way, and take the slot the
   * moment it lets go.
   * <p>
   * The wait is bounded because the caller's own operation is: a timeout expiring means the caller proceeds without
   * the slot, loudly, which is the same outcome it had before this existed. Bounding it is also what keeps a caller
   * that already holds a conflicting reservation on this database from waiting on itself - these reservations are
   * not reentrant.
   *
   * @param timeoutMs how long to wait; zero or negative does not wait at all and is exactly
   *                  {@link #begin(String, Operation)}
   *
   * @return {@code null} when the reservation was taken - the caller must then release it with
   * {@link #end(String, Operation)} from a {@code finally} - or the operation that was still refusing this one when
   * the wait ran out. A non-null answer means nothing was reserved and {@link #end} must NOT be called.
   */
  public Operation begin(final String databaseName, final Operation operation, final long timeoutMs) {
    Operation conflict = begin(databaseName, operation);
    if (conflict == null || timeoutMs <= 0)
      return conflict;

    // REGISTERED BEFORE THE FIRST WAIT, NOT AFTER: A RESERVATION ARRIVING IN THE WINDOW BETWEEN THE INITIAL
    // begin() ABOVE AND THIS LINE STILL GETS ADMITTED, BUT EVERY ONE AFTER IT IS REFUSED BY THE GUARD IN
    // begin(String, Operation) - SO THE OPERATIONS ALREADY RUNNING (OR THIS ONE STRAGGLER) ARE THE LAST ONES THIS
    // CALLER HAS TO WAIT OUT (issue #7646). A NO-OP FOR ANY OPERATION OTHER THAN RESTORE: ONLY RESTORE CALLS THIS
    // OVERLOAD, BUT THE GUARD IT REGISTERS FOR ONLY EVER EXEMPTS RESTORE ITSELF, SO REGISTERING A DIFFERENT KIND
    // HERE WOULD MERELY COST A MAP ENTRY NO CALLER EVER CONSULTS.
    final boolean waitingAsRestore = operation == Operation.RESTORE;
    if (waitingAsRestore)
      restoreStartedWaiting(databaseName);
    try {
      final long deadline = System.currentTimeMillis() + timeoutMs;
      synchronized (slotReleased) {
        // Re-checked inside the monitor before every wait, and end() takes the same monitor to notify AFTER it has
        // updated the map: a release that lands between the check and the wait therefore cannot be missed.
        while ((conflict = begin(databaseName, operation)) != null) {
          final long remaining = deadline - System.currentTimeMillis();
          if (remaining <= 0)
            return conflict;

          try {
            slotReleased.wait(remaining);
          } catch (final InterruptedException e) {
            // Restore the flag and report the conflict: an interrupted caller took nothing and must not release.
            Thread.currentThread().interrupt();
            return conflict;
          }
        }
        return null;
      }
    } finally {
      if (waitingAsRestore)
        restoreStoppedWaiting(databaseName);
    }
  }

  /**
   * Registers a waiting {@link Operation#RESTORE} on {@code databaseName}. Pairs with {@link #restoreStoppedWaiting}.
   * <p>
   * The create-or-increment happens in ONE {@code compute}, not a {@code computeIfAbsent} followed by a separate
   * {@code incrementAndGet}: split across two steps, a second waiter's {@link #restoreStoppedWaiting} could land
   * between them - find the freshly created counter still at zero, decrement it to below zero and remove the map
   * entry - and this call's increment would then apply to a counter no longer in the map, silently losing the
   * registration the whole {@code waitingRestores} mechanism exists for (issue #7646, review of PR #7649). Two
   * waiting restores on one database are reachable: a second {@code RESTORE} is refused by {@code conflictsWith}
   * and then waits in {@link #begin(String, Operation, long)} exactly like the first.
   */
  private void restoreStartedWaiting(final String databaseName) {
    waitingRestores.compute(databaseName, (name, count) -> {
      if (count == null)
        return new AtomicInteger(1);
      count.incrementAndGet();
      return count;
    });
  }

  /**
   * Deregisters a waiting {@link Operation#RESTORE} on {@code databaseName}, whether it stopped waiting because it
   * was admitted, because it timed out or because it was interrupted - every exit out of the wait in
   * {@link #begin(String, Operation, long)} owes this call.
   */
  private void restoreStoppedWaiting(final String databaseName) {
    waitingRestores.computeIfPresent(databaseName, (name, count) -> count.decrementAndGet() > 0 ? count : null);
  }

  /** Whether at least one caller is currently waiting for a {@link Operation#RESTORE} on this database. */
  private boolean isRestoreWaiting(final String databaseName) {
    final AtomicInteger count = waitingRestores.get(databaseName);
    return count != null && count.get() > 0;
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
   * Releases the ONE reservation a successful {@link #begin(String, Operation)} took, not every reservation of that
   * kind - a second export of the same database may be holding one of its own. Always call it from a
   * {@code finally}: a leaked reservation blocks every later operation of that database until the server restarts.
   */
  @Override
  public void end(final String databaseName, final Operation operation) {
    inProgress.computeIfPresent(databaseName, (name, running) -> {
      // A CALLER THAT WAS REFUSED MUST NOT CALL end(), BUT IF IT DOES IT MUST NEITHER FREE SOMEBODY ELSE'S SLOT NOR
      // DRIVE A COUNT NEGATIVE - A NEGATIVE COUNT WOULD SWALLOW THE NEXT GENUINE RELEASE AND LEAVE THE DATABASE
      // BLOCKED UNTIL THE SERVER RESTARTS
      if (running[operation.ordinal()] == 0)
        return running;

      final int[] updated = running.clone();
      updated[operation.ordinal()]--;
      return isIdle(updated) ? null : updated;
    });
    // AFTER the map update, so a waiter that re-checks on waking sees the release that woke it. Unconditional: a
    // release that changed nothing costs one uncontended monitor and a notify nobody is parked on.
    synchronized (slotReleased) {
      slotReleased.notifyAll();
    }
  }

  /** Whether any backup, restore, import or export of this database is currently running. */
  public boolean isInProgress(final String databaseName) {
    return inProgress.containsKey(databaseName);
  }

  /**
   * Whether at least one operation of this kind is currently running on this database. There can be more than one
   * only for {@link Operation#EXPORT}, and a caller that needs to know the database is free asks
   * {@link #isInProgress(String)} rather than counting.
   */
  public boolean isInProgress(final String databaseName, final Operation operation) {
    final int[] running = inProgress.get(databaseName);
    return running != null && running[operation.ordinal()] > 0;
  }

  /** Whether every count in this snapshot is zero, in which case the map entry is dropped rather than kept. */
  private static boolean isIdle(final int[] running) {
    for (final int count : running)
      if (count > 0)
        return false;
    return true;
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
