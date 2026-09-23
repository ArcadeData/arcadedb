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
import com.arcadedb.exception.DatabaseOperationInProgressException;

/**
 * The admission policy for whole-database maintenance operations, as the engine sees it: one slot per database,
 * held for the duration of a backup, a restore, an import or an export, and handed to at most one operation of each
 * kind except {@link Operation#EXPORT}, which does not exclude itself.
 * <p>
 * The policy itself is the server's - {@code com.arcadedb.server.backup.BackupCoordinator} is the only
 * implementation, one instance per {@code ArcadeDBServer} - and the server binds it to every database it opens
 * under {@link #WRAPPER_NAME}. This interface is what the engine can name, because the dependency runs from the
 * server to the engine and not back.
 * <p>
 * <b>Why the engine needs to name it at all.</b> {@code BACKUP DATABASE}, {@code IMPORT DATABASE} and
 * {@code EXPORT DATABASE} are SQL statements executed by the engine, and a client reaches them over HTTP, Postgres,
 * gRPC, Bolt or the console exactly as it reaches {@code SELECT}. Until issue #7443 the first two took no slot, so
 * a SQL backup of a live database ran unseen by a concurrent {@code restore database} that was about to drop the
 * directory out from under it, and a second SQL backup of the same database was not refused either - two archives
 * named from the same timestamp writing into one file, which is the #6753 defect on this path. {@code EXPORT
 * DATABASE} reads the whole database off disk and writes an archive just as a backup does, and took nothing until
 * issue #7450.
 * <p>
 * A database with no coordinator bound - an embedded process with no server in it - reserves nothing and behaves
 * exactly as it did before. {@link #reserve} answers that case with a no-op reservation rather than with
 * {@code null}, so a caller has one shape to write.
 * <p>
 * The binding is per database instance, which makes the admission per server instance rather than per JVM: an HA
 * test, and a co-located pair of nodes, runs several servers with the same database names in one process, and
 * their backups are genuinely independent.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
public interface MaintenanceCoordinator {
  /** The {@link DatabaseInternal#setWrapper} name the server binds its coordinator to. */
  String WRAPPER_NAME = "maintenance-coordinator";

  /**
   * The whole-database maintenance operations that share one per-database slot.
   * <p>
   * {@link #conflictsWith} is the whole admission policy. A {@link #RESTORE} or a {@link #DROP} excludes everything
   * and is excluded by everything, because each destroys the database directory every other operation is reading
   * or writing - a restore by replacing it, a drop by deleting it outright with no replacement (issue #7641). Apart
   * from those two, the only refusal left is an operation of the same kind as one already running, and
   * {@link #EXPORT} is the one kind exempt from even that.
   * <p>
   * A {@link #BACKUP} excludes another backup because two full backups of one database read and compress the same
   * data twice for one usable archive, and both resolve their default name from a timestamp (issue #6753). An
   * {@link #IMPORT} excludes another import because an import creates the database it loads into, so the second one
   * could not have created it anyway. An {@link #EXPORT} excludes neither: two exports of one database write two
   * different files, and nothing makes them collide the way two backups do (issue #7450). That is why the
   * implementation counts reservations per kind rather than holding a set of kinds - two concurrent exports each
   * have to release only their own.
   */
  enum Operation {
    BACKUP("back up", "a backup"), RESTORE("restore", "a restore"), IMPORT("import", "an import"),
    /**
     * A SQL {@code EXPORT DATABASE}: reads the whole database off disk and writes an archive, like a backup, but
     * to a target the statement names, so two of them are legitimate and are admitted together (issue #7450).
     */
    EXPORT("export", "an export"),
    /**
     * {@code drop database}: deletes the database directory outright, unconditionally and with no replacement -
     * the same class of defect {@link #RESTORE} was given the slot for (#7384), on the one whole-database delete
     * that was still left unslotted (issue #7641).
     */
    DROP("drop", "a drop"),
    /**
     * {@code close database}: closes the open instance and deregisters it from the server. It leaves the directory
     * alone, so it is not a destroyer the way {@link #RESTORE} and {@link #DROP} are - but every operation in this
     * enum works THROUGH that instance, and closing it underneath one fails just as silently by another route: a
     * backup or an export loses the database it was streaming, half way into its archive. Enrolled with the same
     * conflict rule as {@link #DROP} (issue #7469, the residue left over when create and drop were enrolled).
     */
    CLOSE("close", "a close");

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
     * <p>
     * The relation is symmetric, which is what lets a caller ask it in whichever direction it holds the two
     * operations, and it is NOT reflexive: {@code EXPORT.conflictsWith(EXPORT)} is false.
     */
    public boolean conflictsWith(final Operation other) {
      if (excludesEverything(this) || excludesEverything(other))
        return true;
      // EVERY OTHER KIND EXCLUDES A SECOND OF ITS OWN - EXCEPT AN EXPORT, WHOSE TARGET THE STATEMENT NAMES, SO TWO
      // OF THEM ARE TWO DIFFERENT ARCHIVES RATHER THAN TWO WRITERS OF ONE (issue #7450)
      return this == other && this != EXPORT;
    }

    /**
     * The kinds that take the database away from every other operation: {@link #RESTORE} and {@link #DROP} replace
     * or delete its directory, {@link #CLOSE} closes the instance they all work through. Named rather than spelled
     * out inline so the next such kind is added in one place instead of two halves of one condition.
     */
    private static boolean excludesEverything(final Operation operation) {
      return operation == RESTORE || operation == DROP || operation == CLOSE;
    }
  }

  /**
   * Reserves {@code databaseName} for {@code operation}.
   * <p>
   * A kind that does not exclude itself - {@link Operation#EXPORT} - may hold several reservations of one database
   * at once, and each is released by its own {@link #end(String, Operation)}. Reservations are not reentrant: a
   * caller that already holds a conflicting one on this database would refuse itself.
   *
   * @return {@code null} when the reservation was taken - the caller must then release it with
   * {@link #end(String, Operation)} from a {@code finally} - or the operation already running that refuses this
   * one.
   */
  Operation begin(String databaseName, Operation operation);

  /**
   * Releases the ONE reservation a successful {@link #begin(String, Operation)} took - not every reservation of
   * that kind, which matters for {@link Operation#EXPORT}, where a second export of the same database may be
   * holding one of its own. Always call it from a {@code finally}: a leaked reservation blocks every later backup,
   * restore, import and export of that database until the server restarts.
   */
  void end(String databaseName, Operation operation);

  /**
   * The coordinator bound to this database, or {@code null} when there is none - an embedded process with no
   * server in it, or a {@code RemoteDatabase}, whose statements execute on the server that does have one.
   * <p>
   * Every wrapper delegates {@link DatabaseInternal#getWrappers()} down to the embedded instance, so the answer
   * does not depend on which layer the caller happens to hold.
   */
  static MaintenanceCoordinator boundTo(final Database database) {
    if (database instanceof DatabaseInternal internal
        && internal.getWrappers().get(WRAPPER_NAME) instanceof MaintenanceCoordinator coordinator)
      return coordinator;
    return null;
  }

  /**
   * The refusal message every entry point uses, so a client cannot tell a SQL {@code BACKUP DATABASE} refusal
   * apart from a {@code trigger backup} one. {@code ServerControlPlane} formats its own refusals through here too.
   */
  static String refusal(final Operation refused, final String databaseName, final Operation running) {
    return "Cannot " + refused.verb() + " database '" + databaseName + "': " + running.phrase()
        + " of it is already in progress";
  }

  /**
   * Takes the slot for {@code operation} on {@code database} and hands back the handle that releases it - designed
   * for a try-with-resources, since the release must happen on every path out.
   *
   * @throws DatabaseOperationInProgressException when a conflicting operation already holds the slot. Nothing is
   *                                              reserved in that case and {@link Reservation#close()} is never
   *                                              owed. Reserve as LATE as the caller can - after whatever
   *                                              validation may reject the operation outright - so a refused
   *                                              request never holds the slot for the time it takes to fail.
   */
  static Reservation reserve(final Database database, final Operation operation) {
    final MaintenanceCoordinator coordinator = boundTo(database);
    if (coordinator == null)
      return Reservation.NONE;

    final String databaseName = database.getName();
    final Operation running = coordinator.begin(databaseName, operation);
    if (running != null)
      throw new DatabaseOperationInProgressException(refusal(operation, databaseName, running));

    return new Reservation(coordinator, databaseName, operation);
  }

  /**
   * A held slot. {@link #close()} declares no checked exception, so a try-with-resources around it does not force
   * the caller to widen its own signature, and it is idempotent for {@link #NONE} - the reservation an
   * unco-ordinated database gets, which holds nothing and releases nothing.
   */
  final class Reservation implements AutoCloseable {
    /** The reservation of a database with no coordinator bound. Releasing it does nothing. */
    static final Reservation NONE = new Reservation(null, null, null);

    private final MaintenanceCoordinator coordinator;
    private final String                 databaseName;
    private final Operation              operation;

    private Reservation(final MaintenanceCoordinator coordinator, final String databaseName,
        final Operation operation) {
      this.coordinator = coordinator;
      this.databaseName = databaseName;
      this.operation = operation;
    }

    @Override
    public void close() {
      if (coordinator != null)
        coordinator.end(databaseName, operation);
    }
  }
}
