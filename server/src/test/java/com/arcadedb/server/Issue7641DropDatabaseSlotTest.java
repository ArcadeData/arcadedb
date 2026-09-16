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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.MaintenanceCoordinator.Operation;
import com.arcadedb.server.backup.BackupCoordinator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Issue #7641: {@code drop database} used to delete a whole database directory without consulting
 * {@link BackupCoordinator} at all - the one whole-database delete left unslotted after issue #7384 gave a
 * restore the same per-database reservation. A {@code trigger backup}, {@code restore database} or
 * {@code import database} running on the same database was no obstacle: the drop deleted the directory out from
 * under whichever of those was reading or writing it.
 * <p>
 * The fix gives {@code dropDatabase} the slot as the new {@link Operation#DROP}, which - like {@link Operation#RESTORE}
 * - conflicts with every other kind, because a drop destroys the directory unconditionally and with no replacement.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7641DropDatabaseSlotTest {

  private static final String DB_NAME = "drop7641db";

  @Test
  void dropDatabaseIsRefusedWhileABackupIsRunning() {
    final BackupCoordinator coordinator = new BackupCoordinator();
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.existsDatabase(DB_NAME)).thenReturn(true);
    when(server.getBackupCoordinator()).thenReturn(coordinator);

    assertThat(coordinator.begin(DB_NAME, Operation.BACKUP)).isNull();
    try {
      final ServerControlPlane controlPlane = new ServerControlPlane(server);

      assertThatThrownBy(() -> controlPlane.dropDatabase(DB_NAME))
          .as("a drop must not be admitted while a backup of the same database is running")
          .isInstanceOf(ServerControlPlane.OperationInProgressException.class)
          .hasMessageContaining(DB_NAME);

      // NOTHING WAS TOUCHED: THE DATABASE WAS NEVER EVEN LOOKED UP TO BE DROPPED
      verify(server, never()).getDatabase(DB_NAME);
      verify(server, never()).removeDatabase(DB_NAME);
    } finally {
      coordinator.end(DB_NAME, Operation.BACKUP);
    }
  }

  @Test
  void dropDatabaseIsRefusedWhileARestoreIsRunning() {
    final BackupCoordinator coordinator = new BackupCoordinator();
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.existsDatabase(DB_NAME)).thenReturn(true);
    when(server.getBackupCoordinator()).thenReturn(coordinator);

    assertThat(coordinator.begin(DB_NAME, Operation.RESTORE)).isNull();
    try {
      final ServerControlPlane controlPlane = new ServerControlPlane(server);

      assertThatThrownBy(() -> controlPlane.dropDatabase(DB_NAME))
          .isInstanceOf(ServerControlPlane.OperationInProgressException.class);

      verify(server, never()).getDatabase(DB_NAME);
    } finally {
      coordinator.end(DB_NAME, Operation.RESTORE);
    }
  }

  @Test
  void dropDatabaseIsRefusedWhileAnExportIsRunning() {
    final BackupCoordinator coordinator = new BackupCoordinator();
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.existsDatabase(DB_NAME)).thenReturn(true);
    when(server.getBackupCoordinator()).thenReturn(coordinator);

    // EXPORT IS THE ONE KIND ADMITTED WITHOUT LIMIT (issue #7450) - IT MUST STILL REFUSE A DROP
    assertThat(coordinator.begin(DB_NAME, Operation.EXPORT)).isNull();
    try {
      final ServerControlPlane controlPlane = new ServerControlPlane(server);

      assertThatThrownBy(() -> controlPlane.dropDatabase(DB_NAME))
          .isInstanceOf(ServerControlPlane.OperationInProgressException.class);
    } finally {
      coordinator.end(DB_NAME, Operation.EXPORT);
    }
  }

  /**
   * A drop conflicts with everything, so it is also refused when nothing but ANOTHER drop of the same database is
   * mid-flight - two concurrent drops racing each other are exactly the same hazard as a restore racing a drop.
   */
  @Test
  void twoConcurrentDropsOfOneDatabaseExcludeEachOther() {
    final BackupCoordinator coordinator = new BackupCoordinator();

    assertThat(coordinator.begin(DB_NAME, Operation.DROP)).isNull();
    assertThat(coordinator.begin(DB_NAME, Operation.DROP)).isEqualTo(Operation.DROP);

    coordinator.end(DB_NAME, Operation.DROP);
    assertThat(coordinator.isInProgress(DB_NAME)).isFalse();
  }

  /**
   * The success path: nothing else is running, so the drop proceeds and releases its own reservation afterwards -
   * the database is genuinely free again, not merely reported free.
   */
  @Test
  void dropDatabaseSucceedsAndReleasesItsSlotAfterwards() {
    final BackupCoordinator coordinator = new BackupCoordinator();
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    final ServerDatabase database = mock(ServerDatabase.class);
    final DatabaseInternal embedded = mock(DatabaseInternal.class);

    when(server.existsDatabase(DB_NAME)).thenReturn(true);
    when(server.getBackupCoordinator()).thenReturn(coordinator);
    when(server.getDatabase(DB_NAME)).thenReturn(database);
    // NOT AN HAReplicatedDatabase, SO dropDatabaseClusterWide TAKES THE NON-HA, LOCAL-DELETE BRANCH
    when(database.getWrappedDatabaseInstance()).thenReturn(embedded);
    when(database.getEmbedded()).thenReturn(embedded);

    final ServerControlPlane controlPlane = new ServerControlPlane(server);
    controlPlane.dropDatabase(DB_NAME);

    verify(embedded).drop();
    verify(server).removeDatabase(DB_NAME);

    // THE SLOT WAS RELEASED: A FRESH RESERVATION OF ANY KIND IS ADMITTED RIGHT AFTER
    assertThat(coordinator.begin(DB_NAME, Operation.DROP)).isNull();
    coordinator.end(DB_NAME, Operation.DROP);
    assertThat(coordinator.isInProgress(DB_NAME)).isFalse();
  }

  /**
   * A drop that throws while dropping must still release its slot - the failure is the caller's to see, not a
   * reservation leaked for the life of the server.
   */
  @Test
  void aFailedDropStillReleasesItsSlot() {
    final BackupCoordinator coordinator = new BackupCoordinator();
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    final ServerDatabase database = mock(ServerDatabase.class);
    final DatabaseInternal embedded = mock(DatabaseInternal.class);

    when(server.existsDatabase(DB_NAME)).thenReturn(true);
    when(server.getBackupCoordinator()).thenReturn(coordinator);
    when(server.getDatabase(DB_NAME)).thenReturn(database);
    when(database.getWrappedDatabaseInstance()).thenReturn(embedded);
    when(database.getEmbedded()).thenReturn(embedded);
    org.mockito.Mockito.doThrow(new RuntimeException("boom")).when(embedded).drop();

    final ServerControlPlane controlPlane = new ServerControlPlane(server);

    assertThatThrownBy(() -> controlPlane.dropDatabase(DB_NAME)).hasMessageContaining("boom");

    assertThat(coordinator.begin(DB_NAME, Operation.DROP)).isNull();
    coordinator.end(DB_NAME, Operation.DROP);
  }
}
