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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7646: {@link Operation#EXPORT} is admitted without limit (issue #7450), so before this fix a stream of
 * overlapping exports of one database could keep {@link BackupCoordinator}'s per-database count above zero forever -
 * a {@link BackupCoordinator#begin(String, Operation, long)} waiter for {@link Operation#RESTORE} rechecks only on a
 * release, and a fresh export was always free to slip in between the last release and that recheck. The waiter's
 * bounded wait then expired every single time, indistinguishable from an ordinary conflict that happened to outlast
 * it, which is exactly what makes it starvation rather than a long wait: the caller - an HA snapshot install applying
 * a committed Raft entry (issue #7444) - proceeds without the slot, loudly, on every attempt.
 * <p>
 * The fix registers the waiting restore before the first wait, and {@link BackupCoordinator#begin(String, Operation)}
 * refuses every new reservation of a different kind while one is registered - so the exports already running (or one
 * unlucky straggler that starts in the small window before registration) are the last ones a waiting restore has to
 * wait out.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7646RestoreStarvationTest {

  /**
   * The direct assertion of the fix: once a restore is waiting, a fresh export is refused outright rather than
   * admitted, so it can never keep the database busy indefinitely.
   */
  @Test
  @Timeout(30)
  void aFreshExportIsRefusedWhileARestoreIsWaiting() throws Exception {
    final BackupCoordinator coordinator = new BackupCoordinator();

    // ONE EXPORT ALREADY RUNNING IS WHAT THE WAITER FINDS IN ITS WAY
    assertThat(coordinator.begin("db", Operation.EXPORT)).isNull();

    final CountDownLatch waiterStarted = new CountDownLatch(1);
    final AtomicReference<Operation> outcome = new AtomicReference<>(Operation.EXPORT);
    final Thread waiter = new Thread(() -> {
      waiterStarted.countDown();
      outcome.set(coordinator.begin("db", Operation.RESTORE, 20_000));
    }, "restore-waiter-7646");
    waiter.start();

    assertThat(waiterStarted.await(10, TimeUnit.SECONDS)).isTrue();
    // STILL BLOCKED AFTER A SHORT WHILE PROVES IT REACHED THE WAIT - AND, WITH IT, THE REGISTRATION THAT PRECEDES
    // THE WAIT IN begin(String, Operation, long) - RATHER THAN HAVING RETURNED ALREADY
    waiter.join(300);
    assertThat(waiter.isAlive()).as("the restore is waiting on the export already running").isTrue();

    // A STREAM OF NEW EXPORTS ARRIVING AFTER THE RESTORE STARTED WAITING MUST ALL BE REFUSED - THIS IS THE
    // STARVATION issue #7646 IS ABOUT: BEFORE THE FIX EVERY ONE OF THESE WAS ADMITTED
    for (int i = 0; i < 10; i++)
      assertThat(coordinator.begin("db", Operation.EXPORT)).as("export #%d arriving after the restore is waiting", i)
          .isEqualTo(Operation.RESTORE);

    // THE SAME GUARD REFUSES A FRESH BACKUP OR IMPORT TOO - A WAITING RESTORE OUTRANKS EVERY OTHER NEW RESERVATION
    assertThat(coordinator.begin("db", Operation.BACKUP)).isEqualTo(Operation.RESTORE);
    assertThat(coordinator.begin("db", Operation.IMPORT)).isEqualTo(Operation.RESTORE);

    // NOW THE ONLY EXPORT LEFT RELEASES, AND NOTHING NEW COULD HAVE JOINED IT
    coordinator.end("db", Operation.EXPORT);

    waiter.join(20_000);
    assertThat(waiter.isAlive()).as("the waiter woke as soon as the sole export released").isFalse();
    assertThat(outcome.get()).as("the slot was taken, not refused after the timeout").isNull();
    assertThat(coordinator.isInProgress("db", Operation.RESTORE)).isTrue();

    coordinator.end("db", Operation.RESTORE);
    assertThat(coordinator.isInProgress("db")).isFalse();
  }

  /**
   * The registration must not outlive the wait it guards: once the restore is done waiting - admitted here - a
   * fresh export must be admitted normally again, exactly as if nothing had ever waited.
   */
  @Test
  @Timeout(30)
  void theGuardIsClearedOnceTheRestoreStopsWaiting() throws Exception {
    final BackupCoordinator coordinator = new BackupCoordinator();

    assertThat(coordinator.begin("db", Operation.EXPORT)).isNull();

    final Thread waiter = new Thread(() -> coordinator.begin("db", Operation.RESTORE, 20_000), "restore-waiter-7646b");
    waiter.start();
    waiter.join(300);
    assertThat(waiter.isAlive()).isTrue();

    coordinator.end("db", Operation.EXPORT);
    waiter.join(20_000);
    assertThat(waiter.isAlive()).isFalse();
    assertThat(coordinator.isInProgress("db", Operation.RESTORE)).isTrue();

    coordinator.end("db", Operation.RESTORE);

    // THE DATABASE IS GENUINELY FREE AFTERWARDS, AND A NEW EXPORT IS NO LONGER REFUSED ON THE WAITING RESTORE'S
    // BEHALF - THERE IS NO WAITING RESTORE LEFT
    assertThat(coordinator.begin("db", Operation.EXPORT)).isNull();
    coordinator.end("db", Operation.EXPORT);
    assertThat(coordinator.isInProgress("db")).isFalse();
  }

  /**
   * A restore that times out anyway - because whatever it is waiting for genuinely never lets go within the bound -
   * must still deregister itself, or every later reservation on that database would be refused forever by a waiter
   * that is no longer waiting.
   */
  @Test
  @Timeout(30)
  void aTimedOutWaiterStopsBlockingNewReservations() throws Exception {
    final BackupCoordinator coordinator = new BackupCoordinator();

    assertThat(coordinator.begin("db", Operation.EXPORT)).isNull();

    // A SHORT BOUND THAT GENUINELY EXPIRES: THE EXPORT IS NEVER RELEASED IN THIS TEST
    final Operation outcome = coordinator.begin("db", Operation.RESTORE, 200);
    assertThat(outcome).as("the restore gave up, the export was never released").isEqualTo(Operation.EXPORT);

    // THE TIMED-OUT WAITER MUST NOT HAVE LEFT ITSELF REGISTERED
    assertThat(coordinator.begin("db", Operation.EXPORT)).as("a fresh export is admitted, not refused by a stale waiter")
        .isNull();

    coordinator.end("db", Operation.EXPORT);
    coordinator.end("db", Operation.EXPORT);
    assertThat(coordinator.isInProgress("db")).isFalse();
  }
}
