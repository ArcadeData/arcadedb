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

import com.arcadedb.engine.MaintenanceCoordinator.Operation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The admission policy the per-database slot applies once it admits restores and imports as well as backups
 * (issue #7384).
 * <p>
 * A restore drops and replaces the database directory, so it has to exclude - and be excluded by - everything else
 * that touches it. A backup and an import are the one pair that coexists: an import is ordinary transactions against
 * a live database, and backing a live database up is what the auto-backup schedule does all day.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7384OperationAdmissionTest {

  @Test
  void aRestoreExcludesEveryOtherOperationOnTheSameDatabase() {
    for (final Operation other : Operation.values()) {
      final BackupCoordinator coordinator = new BackupCoordinator();

      assertThat(coordinator.begin("db", Operation.RESTORE)).isNull();
      assertThat(coordinator.begin("db", other)).isEqualTo(Operation.RESTORE);

      coordinator.end("db", Operation.RESTORE);
      assertThat(coordinator.isInProgress("db")).isFalse();
    }
  }

  @Test
  void everyOtherOperationExcludesARestoreOfTheSameDatabase() {
    for (final Operation holder : Operation.values()) {
      final BackupCoordinator coordinator = new BackupCoordinator();

      assertThat(coordinator.begin("db", holder)).isNull();
      assertThat(coordinator.begin("db", Operation.RESTORE)).isEqualTo(holder);

      coordinator.end("db", holder);
      assertThat(coordinator.begin("db", Operation.RESTORE)).isNull();
      coordinator.end("db", Operation.RESTORE);
    }
  }

  @Test
  void aBackupAndAnImportOfOneDatabaseRunTogether() {
    final BackupCoordinator coordinator = new BackupCoordinator();

    assertThat(coordinator.begin("db", Operation.BACKUP)).isNull();
    assertThat(coordinator.begin("db", Operation.IMPORT)).isNull();

    // AND NEITHER OF THEM LET A RESTORE IN WHILE BOTH ARE HELD
    assertThat(coordinator.begin("db", Operation.RESTORE)).isIn(Operation.BACKUP, Operation.IMPORT);

    // RELEASING ONE DOES NOT RELEASE THE OTHER: THE SLOT IS PER OPERATION, NOT A SINGLE FLAG
    coordinator.end("db", Operation.BACKUP);
    assertThat(coordinator.isInProgress("db")).isTrue();
    assertThat(coordinator.isInProgress("db", Operation.IMPORT)).isTrue();
    assertThat(coordinator.isInProgress("db", Operation.BACKUP)).isFalse();
    assertThat(coordinator.begin("db", Operation.RESTORE)).isEqualTo(Operation.IMPORT);

    coordinator.end("db", Operation.IMPORT);
    assertThat(coordinator.isInProgress("db")).isFalse();
    assertThat(coordinator.begin("db", Operation.RESTORE)).isNull();
  }

  @Test
  void anOperationAlwaysExcludesASecondOneOfItsOwnKind() {
    for (final Operation operation : Operation.values()) {
      final BackupCoordinator coordinator = new BackupCoordinator();

      assertThat(coordinator.begin("db", operation)).isNull();
      assertThat(coordinator.begin("db", operation)).isEqualTo(operation);

      coordinator.end("db", operation);
      assertThat(coordinator.begin("db", operation)).isNull();
      coordinator.end("db", operation);
    }
  }

  @Test
  void aDifferentDatabaseIsNeverHeldUpByARestore() {
    final BackupCoordinator coordinator = new BackupCoordinator();

    assertThat(coordinator.begin("db1", Operation.RESTORE)).isNull();
    assertThat(coordinator.begin("db2", Operation.RESTORE)).isNull();
    assertThat(coordinator.begin("db2", Operation.BACKUP)).isEqualTo(Operation.RESTORE);

    coordinator.end("db1", Operation.RESTORE);
    coordinator.end("db2", Operation.RESTORE);
    assertThat(coordinator.isInProgress("db1")).isFalse();
    assertThat(coordinator.isInProgress("db2")).isFalse();
  }

  @Test
  void releasingAnOperationThatIsNotHeldLeavesTheOthersAlone() {
    final BackupCoordinator coordinator = new BackupCoordinator();

    assertThat(coordinator.begin("db", Operation.RESTORE)).isNull();

    // A CALLER THAT WAS REFUSED MUST NOT CALL end(), BUT IF IT DOES IT MUST NOT FREE SOMEBODY ELSE'S SLOT
    coordinator.end("db", Operation.BACKUP);
    coordinator.end("db", Operation.IMPORT);

    assertThat(coordinator.isInProgress("db", Operation.RESTORE)).isTrue();
    assertThat(coordinator.begin("db", Operation.BACKUP)).isEqualTo(Operation.RESTORE);

    coordinator.end("db", Operation.RESTORE);
    assertThat(coordinator.isInProgress("db")).isFalse();
  }

  @Test
  void theLegacyBackupShorthandIsTheSameSlot() {
    final BackupCoordinator coordinator = new BackupCoordinator();

    // begin(db) IS begin(db, BACKUP): A RESTORE MUST SEE IT AND IT MUST SEE A RESTORE
    assertThat(coordinator.begin("db")).isTrue();
    assertThat(coordinator.begin("db", Operation.RESTORE)).isEqualTo(Operation.BACKUP);
    coordinator.end("db");

    assertThat(coordinator.begin("db", Operation.RESTORE)).isNull();
    assertThat(coordinator.begin("db")).isFalse();
    coordinator.end("db", Operation.RESTORE);
  }

  @Test
  // PLAIN HANG DETECTOR, NOT A LATENCY BOUND: EVERY CALLER DOES ONE ConcurrentHashMap.compute, SO ANY REAL RUN
  // FINISHES IN MICROSECONDS AND ONLY A DEADLOCK COULD REACH THIS
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void concurrentCallersNeverBothHoldConflictingOperations() throws Exception {
    final BackupCoordinator coordinator = new BackupCoordinator();
    final int callers = 12;
    final CountDownLatch startTogether = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(callers);
    final Set<Operation> admitted = ConcurrentHashMap.newKeySet();

    final ExecutorService executor = Executors.newFixedThreadPool(callers);
    try {
      for (int i = 0; i < callers; i++) {
        final Operation operation = Operation.values()[i % Operation.values().length];
        executor.submit(() -> {
          try {
            startTogether.await();
            if (coordinator.begin("db", operation) == null)
              admitted.add(operation);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            done.countDown();
          }
          return null;
        });
      }

      startTogether.countDown();
      done.await();
    } finally {
      executor.shutdownNow();
    }

    // WHATEVER WON, NO TWO CONFLICTING OPERATIONS ARE HOLDING THE DATABASE AT THE SAME TIME
    assertThat(admitted).isNotEmpty();
    for (final Operation one : admitted)
      for (final Operation two : admitted)
        if (one != two)
          assertThat(one.conflictsWith(two)).isFalse();

    // AND THE ONLY SET THAT CAN SURVIVE TOGETHER IS THE BACKUP/IMPORT PAIR
    assertThat(admitted).isIn(EnumSet.of(Operation.BACKUP), EnumSet.of(Operation.RESTORE), EnumSet.of(Operation.IMPORT),
        EnumSet.of(Operation.BACKUP, Operation.IMPORT));
  }
}
