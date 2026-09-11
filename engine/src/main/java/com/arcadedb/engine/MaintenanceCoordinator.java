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
 * held for the duration of a backup, a restore or an import, and handed to at most one operation of each kind.
 * <p>
 * The policy itself is the server's - {@code com.arcadedb.server.backup.BackupCoordinator} is the only
 * implementation, one instance per {@code ArcadeDBServer} - and the server binds it to every database it opens
 * under {@link #WRAPPER_NAME}. This interface is what the engine can name, because the dependency runs from the
 * server to the engine and not back.
 * <p>
 * <b>Why the engine needs to name it at all.</b> {@code BACKUP DATABASE} and {@code IMPORT DATABASE} are SQL
 * statements executed by the engine, and a client reaches them over HTTP, Postgres, gRPC, Bolt or the console
 * exactly as it reaches {@code SELECT}. Until issue #7443 they took no slot, so a SQL backup of a live database
 * ran unseen by a concurrent {@code restore database} that was about to drop the directory out from under it, and
 * a second SQL backup of the same database was not refused either - two archives named from the same timestamp
 * writing into one file, which is the #6753 defect on this path.
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
   * {@link #conflictsWith} is the whole admission policy: two operations on one database exclude each other unless
   * one is a {@link #BACKUP} and the other an {@link #IMPORT}. A restore excludes everything because it drops and
   * replaces the database directory; a backup excludes another backup because two full backups of one database
   * read and compress the same data twice for one usable archive (issue #6753); an import excludes another import
   * because an import creates the database it loads into, so the second one could not have created it anyway.
   */
  enum Operation {
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
   * Reserves {@code databaseName} for {@code operation}.
   *
   * @return {@code null} when the reservation was taken - the caller must then release it with
   * {@link #end(String, Operation)} from a {@code finally} - or the operation already running that refuses this
   * one.
   */
  Operation begin(String databaseName, Operation operation);

  /**
   * Releases the reservation a successful {@link #begin(String, Operation)} took. Always call it from a
   * {@code finally}: a leaked reservation blocks every later backup, restore and import of that database until the
   * server restarts.
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
   *                                              owed.
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
