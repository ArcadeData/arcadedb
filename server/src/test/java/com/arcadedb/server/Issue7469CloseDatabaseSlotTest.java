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
package com.arcadedb.server;

import com.arcadedb.engine.MaintenanceCoordinator.Operation;
import com.arcadedb.server.backup.BackupCoordinator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7469, the residue left over when {@code create database} and {@code drop database} were enrolled in the
 * per-database exclusive-operation slot (#7441 and #7641). {@code ServerControlPlane.closeDatabase} took neither the
 * slot nor the restore name claim, so it could close the open instance out from under a backup, an export or an
 * import of the same database - every one of which reads or writes THROUGH that instance. The archive is half
 * written and the operation that was writing it loses its database, with nothing said to either caller: the same
 * silent failure the drop used to be, by another route.
 * <p>
 * Fixed by adding {@link Operation#CLOSE} to the enum and taking the slot in {@code closeDatabase}. It conflicts
 * with everything, as {@link Operation#DROP} does.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7469CloseDatabaseSlotTest {
  @RegisterExtension
  static final ServedDatabases SERVED = new ServedDatabases();

  private static final String DB_NAME = "close7469db";

  @Test
  void closeDatabaseIsRefusedWhileABackupIsRunning() {
    final BackupCoordinator coordinator = new BackupCoordinator();
    final FakeArcadeDBServer server = FakeArcadeDBServer.create();
    server.returns("getBackupCoordinator", coordinator);

    assertThat(coordinator.begin(DB_NAME, Operation.BACKUP)).isNull();
    try {
      final ServerControlPlane controlPlane = new ServerControlPlane(server);

      assertThatThrownBy(() -> controlPlane.closeDatabase(DB_NAME))
          .as("closing the instance a backup is streaming from must not be admitted")
          .isInstanceOf(ServerControlPlane.OperationInProgressException.class)
          .hasMessageContaining(DB_NAME);

      // NOT EVEN LOOKED UP: THE REFUSAL HAPPENS BEFORE ANYTHING IS TOUCHED
      assertThat(server.calls("getDatabase")).doesNotContain(List.of(DB_NAME));
      assertThat(server.calls("removeDatabase")).doesNotContain(List.of(DB_NAME));
    } finally {
      coordinator.end(DB_NAME, Operation.BACKUP);
    }
  }

  @Test
  void closeDatabaseIsRefusedWhileAnExportOrAnImportIsRunning() {
    for (final Operation running : new Operation[] { Operation.EXPORT, Operation.IMPORT, Operation.RESTORE }) {
      final BackupCoordinator coordinator = new BackupCoordinator();
      final FakeArcadeDBServer server = FakeArcadeDBServer.create();
      server.returns("getBackupCoordinator", coordinator);

      assertThat(coordinator.begin(DB_NAME, running)).isNull();
      try {
        assertThatThrownBy(() -> new ServerControlPlane(server).closeDatabase(DB_NAME))
            .as("a close must be refused while %s of the same database is running", running.phrase())
            .isInstanceOf(ServerControlPlane.OperationInProgressException.class);
      } finally {
        coordinator.end(DB_NAME, running);
      }
    }
  }

  /**
   * The other direction, which is the half that matters for the backup: a backup started while a close of the same
   * database is in flight must be refused too, or the slot would only be a one-way check.
   */
  @Test
  void anOperationIsRefusedWhileACloseHoldsTheSlot() {
    final BackupCoordinator coordinator = new BackupCoordinator();

    assertThat(coordinator.begin(DB_NAME, Operation.CLOSE)).isNull();
    assertThat(coordinator.begin(DB_NAME, Operation.BACKUP)).isEqualTo(Operation.CLOSE);
    assertThat(coordinator.begin(DB_NAME, Operation.EXPORT)).isEqualTo(Operation.CLOSE);
    assertThat(coordinator.begin(DB_NAME, Operation.CLOSE)).isEqualTo(Operation.CLOSE);

    coordinator.end(DB_NAME, Operation.CLOSE);
    assertThat(coordinator.isInProgress(DB_NAME)).isFalse();
  }

  /** The success path: the close proceeds and hands the slot back, so the database is genuinely free afterwards. */
  @Test
  void closeDatabaseSucceedsAndReleasesItsSlotAfterwards() {
    final BackupCoordinator coordinator = new BackupCoordinator();
    final FakeArcadeDBServer server = FakeArcadeDBServer.create();
    final ServerDatabase database = SERVED.open(DB_NAME);

    server.returns("getBackupCoordinator", coordinator);
    server.on("getDatabase", args -> Objects.equals(args[0], DB_NAME) ? database : null);

    new ServerControlPlane(server).closeDatabase(DB_NAME);

    assertThat(database.isOpen()).as("the close reached the database").isFalse();
    assertThat(server.calls("removeDatabase")).containsOnlyOnce(List.of(DB_NAME));

    assertThat(coordinator.begin(DB_NAME, Operation.BACKUP)).isNull();
    coordinator.end(DB_NAME, Operation.BACKUP);
    assertThat(coordinator.isInProgress(DB_NAME)).isFalse();
  }

  /**
   * A close that throws - most commonly because no database by that name is open - must still release its slot, or
   * one failed admin command would block every later backup, restore, import and export of that database until the
   * server restarts.
   */
  @Test
  void aFailedCloseStillReleasesItsSlot() {
    final BackupCoordinator coordinator = new BackupCoordinator();
    final FakeArcadeDBServer server = FakeArcadeDBServer.create();

    server.returns("getBackupCoordinator", coordinator);
    server.on("getDatabase", args -> {
      if (Objects.equals(args[0], DB_NAME))
        throw new IllegalArgumentException("Database '" + DB_NAME + "' not found");
      return null;
    });

    assertThatThrownBy(() -> new ServerControlPlane(server).closeDatabase(DB_NAME))
        .isInstanceOf(IllegalArgumentException.class);

    assertThat(coordinator.isInProgress(DB_NAME)).isFalse();
  }
}
