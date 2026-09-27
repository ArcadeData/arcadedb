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

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8452: since #8035 the replicated drop-database apply waits for the maintenance slot as {@link Operation#DROP}
 * through {@link BackupCoordinator#begin(String, Operation, long)}, the bounded wait the HA snapshot install uses as
 * {@link Operation#RESTORE}. The #7646 fairness guard only ever registered a waiting RESTORE, so a stream of
 * overlapping {@link Operation#EXPORT}s - admitted without limit - could keep the count above zero until the drop's
 * wait expired, and the drop then tore down the running exports anyway: the outcome #8035 fixed, reached by
 * starvation.
 * <p>
 * The guard now registers every waiter whose kind excludes everything, and a waiter's own re-check inside its bounded
 * wait is exempt from every registered waiter - so a waiting DROP and a waiting RESTORE on the same database cannot
 * refuse each other's re-check until both time out.
 */
class Issue8452DropStarvationTest {

  /**
   * The direct assertion of the fix: once a drop is waiting, a fresh export - or any other kind - is refused outright
   * rather than admitted, so the exports already running are the last ones the drop has to wait out.
   */
  @Test
  @Timeout(30)
  void aFreshExportIsRefusedWhileADropIsWaiting() throws Exception {
    final BackupCoordinator coordinator = new BackupCoordinator();

    assertThat(coordinator.begin("db", Operation.EXPORT)).isNull();

    final AtomicReference<Operation> outcome = new AtomicReference<>(Operation.EXPORT);
    final Thread waiter = new Thread(() -> outcome.set(coordinator.begin("db", Operation.DROP, 20_000)), "drop-waiter-8452");
    waiter.start();
    awaitRegisteredAsWaiting(coordinator, Operation.DROP);
    assertThat(waiter.isAlive()).as("the drop is waiting on the export already running").isTrue();

    // BEFORE THE FIX EVERY ONE OF THESE WAS ADMITTED, AND EACH KEPT THE COUNT ABOVE ZERO A LITTLE LONGER
    for (int i = 0; i < 10; i++)
      assertThat(coordinator.begin("db", Operation.EXPORT)).as("export #%d arriving after the drop is waiting", i)
          .isEqualTo(Operation.DROP);

    assertThat(coordinator.begin("db", Operation.BACKUP)).isEqualTo(Operation.DROP);
    assertThat(coordinator.begin("db", Operation.IMPORT)).isEqualTo(Operation.DROP);
    assertThat(coordinator.begin("db", Operation.RESTORE)).isEqualTo(Operation.DROP);
    assertThat(coordinator.begin("db", Operation.CLOSE)).isEqualTo(Operation.DROP);

    coordinator.end("db", Operation.EXPORT);

    waiter.join(20_000);
    assertThat(waiter.isAlive()).as("the waiter woke as soon as the sole export released").isFalse();
    assertThat(outcome.get()).as("the slot was taken, not refused after the timeout").isNull();
    assertThat(coordinator.isInProgress("db", Operation.DROP)).isTrue();

    coordinator.end("db", Operation.DROP);
    assertThat(coordinator.isInProgress("db")).isFalse();

    // THE REGISTRATION DID NOT OUTLIVE THE WAIT
    assertThat(coordinator.begin("db", Operation.EXPORT)).isNull();
    coordinator.end("db", Operation.EXPORT);
  }

  /** A drop that times out must still deregister itself, or it would refuse every later reservation forever. */
  @Test
  @Timeout(30)
  void aTimedOutDropStopsBlockingNewReservations() {
    final BackupCoordinator coordinator = new BackupCoordinator();

    assertThat(coordinator.begin("db", Operation.EXPORT)).isNull();
    assertThat(coordinator.begin("db", Operation.DROP, 200)).as("the export was never released").isEqualTo(Operation.EXPORT);

    assertThat(coordinator.begin("db", Operation.EXPORT)).as("a fresh export is admitted, not refused by a stale waiter")
        .isNull();

    coordinator.end("db", Operation.EXPORT);
    coordinator.end("db", Operation.EXPORT);
    assertThat(coordinator.isInProgress("db")).isFalse();
  }

  /**
   * A waiting RESTORE and a waiting DROP on the same database must not refuse each other's re-check: if each one's
   * registration refused the other, neither could ever take the slot and both would proceed without it once their
   * bounds ran out - two destroyers of one directory running at once. Both must be admitted, one after the other.
   */
  @Test
  @Timeout(60)
  void aWaitingRestoreAndAWaitingDropDoNotStarveEachOther() throws Exception {
    final BackupCoordinator coordinator = new BackupCoordinator();

    assertThat(coordinator.begin("db", Operation.EXPORT)).isNull();

    final AtomicReference<Operation> restoreOutcome = new AtomicReference<>(Operation.EXPORT);
    final AtomicReference<Operation> dropOutcome = new AtomicReference<>(Operation.EXPORT);

    final Thread restore = new Thread(() -> {
      restoreOutcome.set(coordinator.begin("db", Operation.RESTORE, 20_000));
      if (restoreOutcome.get() == null) {
        sleepQuietly(100);
        coordinator.end("db", Operation.RESTORE);
      }
    }, "restore-waiter-8452");
    final Thread drop = new Thread(() -> {
      dropOutcome.set(coordinator.begin("db", Operation.DROP, 20_000));
      if (dropOutcome.get() == null) {
        sleepQuietly(100);
        coordinator.end("db", Operation.DROP);
      }
    }, "drop-waiter-8452");

    restore.start();
    awaitRegisteredAsWaiting(coordinator, Operation.RESTORE);
    drop.start();
    awaitRegisteredAsWaiting(coordinator, Operation.DROP);

    coordinator.end("db", Operation.EXPORT);

    restore.join(30_000);
    drop.join(30_000);
    assertThat(restore.isAlive()).isFalse();
    assertThat(drop.isAlive()).isFalse();
    assertThat(restoreOutcome.get()).as("the restore took the slot rather than timing out").isNull();
    assertThat(dropOutcome.get()).as("the drop took the slot rather than timing out").isNull();
    assertThat(coordinator.isInProgress("db")).isFalse();
    assertThat(coordinator.begin("db", Operation.EXPORT)).isNull();
    coordinator.end("db", Operation.EXPORT);
  }

  private static void sleepQuietly(final long ms) {
    try {
      Thread.sleep(ms);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Blocks until a waiter of {@code kind} is REGISTERED. A started thread, or one still alive after a short join,
   * proves neither - it can be alive and not yet have registered (review of PR #7649) - so this polls the state
   * itself rather than probing with a reservation, which with two kinds waiting could not tell which one answered.
   */
  private static void awaitRegisteredAsWaiting(final BackupCoordinator coordinator, final Operation kind)
      throws InterruptedException {
    final long deadline = System.currentTimeMillis() + 10_000;
    while (!coordinator.isWaiting("db", kind)) {
      assertThat(System.currentTimeMillis()).as("the %s never registered as waiting", kind).isLessThan(deadline);
      Thread.sleep(5);
    }
  }
}
