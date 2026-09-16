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
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
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
    doThrow(new RuntimeException("boom")).when(embedded).drop();

    final ServerControlPlane controlPlane = new ServerControlPlane(server);

    assertThatThrownBy(() -> controlPlane.dropDatabase(DB_NAME)).hasMessageContaining("boom");

    assertThat(coordinator.begin(DB_NAME, Operation.DROP)).isNull();
    coordinator.end(DB_NAME, Operation.DROP);
  }

  /**
   * Two real threads calling {@code dropDatabase} on one database at the same instant: exactly one delete happens,
   * the loser is told why rather than silently re-dropping, and neither leaks the slot. Either refusal is correct -
   * the loser may arrive while the winner still holds the slot
   * ({@link ServerControlPlane.OperationInProgressException}) or after it released ({@link IllegalArgumentException},
   * "does not exist").
   * <p>
   * WHAT THIS DOES NOT PIN, so nobody later reads more into a green run than is there: it does not discriminate the
   * existence check being INSIDE the reservation rather than before it (review of PR #7649). It was checked against
   * both orderings and passes on both. The interleaving only the reordering rules out - the loser reads
   * {@code existsDatabase() == true}, the winner then takes the slot, deletes and releases, and the loser goes on to
   * take the freed slot and drop an already-dropped database through a stale {@link ServerDatabase} handle - needs
   * the loser suspended between its check and its {@code beginExclusive}, and that point exists only in the code the
   * fix removed. Reaching it from a test means latching on {@code server.getBackupCoordinator()} and giving the
   * fixed ordering a timed escape so it does not simply deadlock, which pins this method's internal call sequence
   * rather than its behaviour. The reordering is argued at the call site instead; this test guards the invariants
   * above, which is what a later change to the slot or the check would break.
   */
  @Test
  @Timeout(30)
  void twoConcurrentDropDatabaseCallsDeleteItExactlyOnce() throws Exception {
    final BackupCoordinator coordinator = new BackupCoordinator();
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    final ServerDatabase database = mock(ServerDatabase.class);
    final DatabaseInternal embedded = mock(DatabaseInternal.class);

    // THE DATABASE STOPS EXISTING THE MOMENT IT IS DROPPED, WHICH IS WHAT THE LOSER MUST BE ABLE TO OBSERVE
    final AtomicBoolean exists = new AtomicBoolean(true);
    when(server.existsDatabase(DB_NAME)).thenAnswer(invocation -> exists.get());
    when(server.getBackupCoordinator()).thenReturn(coordinator);
    when(server.getDatabase(DB_NAME)).thenReturn(database);
    when(database.getWrappedDatabaseInstance()).thenReturn(embedded);
    when(database.getEmbedded()).thenReturn(embedded);
    doAnswer(invocation -> {
      assertThat(exists.getAndSet(false)).as("the directory was deleted twice").isTrue();
      return null;
    }).when(embedded).drop();

    final ServerControlPlane controlPlane = new ServerControlPlane(server);

    final CyclicBarrier startTogether = new CyclicBarrier(2);
    final List<Throwable> failures = new ArrayList<>();
    final List<Thread> droppers = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      final Thread dropper = new Thread(() -> {
        try {
          startTogether.await();
          controlPlane.dropDatabase(DB_NAME);
        } catch (final Throwable t) {
          synchronized (failures) {
            failures.add(t);
          }
        }
      }, "drop-racer-7641-" + i);
      droppers.add(dropper);
      dropper.start();
    }
    for (final Thread dropper : droppers)
      dropper.join(20_000);

    // EXACTLY ONE DELETE, AND THE OTHER CALLER WAS TOLD WHY RATHER THAN SILENTLY RE-DROPPING
    verify(embedded, times(1)).drop();
    verify(server, times(1)).removeDatabase(DB_NAME);
    assertThat(failures).hasSize(1);
    assertThat(failures.get(0)).isInstanceOfAny(ServerControlPlane.OperationInProgressException.class,
        IllegalArgumentException.class);

    // AND NO SLOT WAS LEAKED BY EITHER OF THEM
    assertThat(coordinator.isInProgress(DB_NAME)).isFalse();
  }
}
