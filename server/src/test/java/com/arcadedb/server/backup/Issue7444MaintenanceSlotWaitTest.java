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

import com.arcadedb.server.backup.BackupCoordinator.Operation;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The bounded-wait reservation an HA snapshot install needs (issue #7444).
 * <p>
 * Every other caller of the slot may simply be refused: a scheduled backup is covered again on the next tick, and a
 * restore or an import is an operator command that can be retried. A snapshot install cannot - it applies a committed
 * Raft entry, and a follower that declines to apply one diverges. So it needs a third answer between "take the slot"
 * and "give up": wait for the operation in the way, for a bounded time, and take the slot the moment it lets go.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7444MaintenanceSlotWaitTest {

  @Test
  void aFreeSlotIsTakenWithoutWaiting() {
    final BackupCoordinator coordinator = new BackupCoordinator();

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThat(coordinator.begin("db", Operation.RESTORE, 60_000)).isNull();
    stopwatch.assertGaveUpWithin(10_000, "taking a free slot from waiting for one");

    assertThat(coordinator.isInProgress("db", Operation.RESTORE)).isTrue();
  }

  @Test
  @Timeout(30)
  void theSlotIsTakenAsSoonAsTheOperationInTheWayReleasesIt() throws Exception {
    final BackupCoordinator coordinator = new BackupCoordinator();
    assertThat(coordinator.begin("db", Operation.BACKUP)).isNull();

    final CountDownLatch waiterStarted = new CountDownLatch(1);
    final AtomicReference<Operation> outcome = new AtomicReference<>(Operation.BACKUP);
    final Thread waiter = new Thread(() -> {
      waiterStarted.countDown();
      outcome.set(coordinator.begin("db", Operation.RESTORE, 20_000));
    }, "slot-waiter");
    waiter.start();

    assertThat(waiterStarted.await(10, TimeUnit.SECONDS)).isTrue();
    // The waiter must NOT have taken the slot while the backup still holds it.
    waiter.join(300);
    assertThat(waiter.isAlive()).as("the install waits instead of replacing the files under a running backup").isTrue();
    assertThat(coordinator.isInProgress("db", Operation.RESTORE)).isFalse();

    coordinator.end("db", Operation.BACKUP);

    waiter.join(20_000);
    assertThat(waiter.isAlive()).as("the waiter woke as soon as the backup released the slot").isFalse();
    assertThat(outcome.get()).as("the slot was taken, not refused").isNull();
    assertThat(coordinator.isInProgress("db", Operation.RESTORE)).isTrue();
  }

  @Test
  @Timeout(60)
  void theWaitIsBoundedAndNamesTheOperationStillHoldingTheSlot() {
    final BackupCoordinator coordinator = new BackupCoordinator();
    assertThat(coordinator.begin("db", Operation.BACKUP)).isNull();

    // A committed install entry has to be applied, so the wait can never be unbounded: it expires and the caller is
    // told which operation is still in the way so it can say so in its warning.
    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThat(coordinator.begin("db", Operation.RESTORE, 250)).isEqualTo(Operation.BACKUP);
    stopwatch.assertGaveUpWithin(30_000, "a bounded wait from an unbounded one");

    // The expired wait took nothing: the backup is still the only holder, and releasing the backup leaves the
    // database free rather than leaking a reservation the timed-out caller never made.
    assertThat(coordinator.isInProgress("db", Operation.RESTORE)).isFalse();
    coordinator.end("db", Operation.BACKUP);
    assertThat(coordinator.isInProgress("db")).isFalse();
  }

  @Test
  @Timeout(30)
  void aZeroWaitIsTheSameAnswerTheNonWaitingReservationGives() {
    final BackupCoordinator coordinator = new BackupCoordinator();
    assertThat(coordinator.begin("db", Operation.BACKUP)).isNull();

    assertThat(coordinator.begin("db", Operation.RESTORE, 0)).isEqualTo(Operation.BACKUP);
    assertThat(coordinator.begin("db", Operation.RESTORE, -1)).isEqualTo(Operation.BACKUP);
    assertThat(coordinator.begin("db", Operation.RESTORE)).isEqualTo(Operation.BACKUP);
  }

  @Test
  @Timeout(30)
  void aWaitOnOneDatabaseIsNotWokenByAnUnrelatedOne() throws Exception {
    final BackupCoordinator coordinator = new BackupCoordinator();
    assertThat(coordinator.begin("held", Operation.BACKUP)).isNull();
    assertThat(coordinator.begin("other", Operation.BACKUP)).isNull();

    final AtomicReference<Operation> outcome = new AtomicReference<>(Operation.BACKUP);
    final Thread waiter = new Thread(() -> outcome.set(coordinator.begin("held", Operation.RESTORE, 20_000)), "slot-waiter");
    waiter.start();

    // Releasing a DIFFERENT database wakes the waiter, which must go straight back to waiting rather than take a
    // slot it was never offered.
    coordinator.end("other", Operation.BACKUP);
    waiter.join(300);
    assertThat(waiter.isAlive()).isTrue();
    assertThat(coordinator.isInProgress("held", Operation.RESTORE)).isFalse();

    coordinator.end("held", Operation.BACKUP);
    waiter.join(20_000);
    assertThat(outcome.get()).isNull();
  }

  @Test
  @Timeout(30)
  void aBackupAndAnImportStillCoexistThroughTheWaitingForm() {
    final BackupCoordinator coordinator = new BackupCoordinator();
    assertThat(coordinator.begin("db", Operation.BACKUP)).isNull();

    // The waiting form is the same admission policy, not a stricter one: the one pair that coexists still does, and
    // without spending the timeout to find out.
    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThat(coordinator.begin("db", Operation.IMPORT, 60_000)).isNull();
    stopwatch.assertGaveUpWithin(10_000, "an admitted operation from one that waited out its timeout");

    assertThat(coordinator.isInProgress("db", Operation.IMPORT)).isTrue();
  }
}
