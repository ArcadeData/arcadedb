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
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The bounded-wait form of the per-database maintenance slot ({@link BackupCoordinator#begin(String, Operation, long)})
 * and the fairness guard that keeps a waiting restore or drop from being starved by a stream of new reservations.
 */
class BackupCoordinatorWaitTest {
  // Regression: #7444 an HA snapshot install waits, bounded, for the operation holding the slot instead of being refused
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
  @Nested
  class BoundedWait {

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

  // Regression: #7646 a stream of exports starved a waiting restore until its bounded wait expired
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
  @Nested
  class RestoreStarvation {

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
      awaitRestoreRegisteredAsWaiting(coordinator);
      assertThat(waiter.isAlive()).as("the restore is waiting on the export already running").isTrue();

      // A STREAM OF NEW EXPORTS ARRIVING AFTER THE RESTORE STARTED WAITING MUST ALL BE REFUSED - THIS IS THE
      // STARVATION issue #7646 IS ABOUT: BEFORE THE FIX EVERY ONE OF THESE WAS ADMITTED
      for (int i = 0; i < 10; i++)
        assertThat(coordinator.begin("db", Operation.EXPORT)).as("export #%d arriving after the restore is waiting", i)
            .isEqualTo(Operation.RESTORE);

      // THE SAME GUARD REFUSES EVERY OTHER KIND TOO - A WAITING RESTORE OUTRANKS EVERY NEW RESERVATION THAT IS NOT
      // ITSELF A RESTORE, INCLUDING A DROP (issue #7641's kind, which destroys the very directory the restore is
      // queued to replace, so it is the last one that should be let past a restore that is already waiting)
      assertThat(coordinator.begin("db", Operation.BACKUP)).isEqualTo(Operation.RESTORE);
      assertThat(coordinator.begin("db", Operation.IMPORT)).isEqualTo(Operation.RESTORE);
      assertThat(coordinator.begin("db", Operation.DROP)).isEqualTo(Operation.RESTORE);

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
      awaitRestoreRegisteredAsWaiting(coordinator);
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

    /**
     * Blocks until the waiting restore is actually REGISTERED, proved by the guard it registers for: a fresh export
     * answered with {@link Operation#RESTORE} can only come from the waiter guard, since no restore holds
     * the slot yet (the export the waiter is parked behind does).
     * <p>
     * A started thread, or one still alive after a short join, proves neither - the thread can be alive and not yet
     * have reached {@code startedWaiting}, which would make everything asserted after it either
     * scheduling-dependent or a false positive (review of PR #7649). This waits on the state itself instead.
     * <p>
     * The refusals it consumes are the point rather than a side effect: they ARE the behaviour under test, and every
     * one of them is a reservation that was refused, so none of them has to be released.
     */
    private static void awaitRestoreRegisteredAsWaiting(final BackupCoordinator coordinator) throws InterruptedException {
      final long deadline = System.currentTimeMillis() + 10_000;
      while (coordinator.begin("db", Operation.EXPORT) != Operation.RESTORE) {
        // Admitted rather than refused means the guard is not up yet - that export now holds a reservation of its
        // own, so give it straight back before looking again, or the waiter would be left waiting on this probe.
        coordinator.end("db", Operation.EXPORT);
        assertThat(System.currentTimeMillis()).as("the restore never registered as waiting").isLessThan(deadline);
        Thread.sleep(5);
      }
    }
  }

  // Regression: #8452 a stream of exports starved a waiting replicated drop, and a waiting restore and drop refused each other
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
  @Nested
  class DropStarvation {

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
}
