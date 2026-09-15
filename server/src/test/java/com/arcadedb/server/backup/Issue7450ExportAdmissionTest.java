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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The admission policy once the per-database slot admits exports as well (issue #7450).
 * <p>
 * {@link Operation#EXPORT} is the first constant that does NOT exclude a second of its own kind, and that is the
 * whole reason it could not simply be added in #7443. Every other operation conflicts with itself, and
 * {@link BackupCoordinator} used to rely on exactly that by holding the running operations of a database in an
 * {@code EnumSet}: "at most one of each kind" is what makes a set sufficient. Two exports of one database write two
 * different files and have no reason to refuse each other, so the set has to count reservations per kind instead -
 * otherwise the first of two concurrent exports to finish would release the second one's claim and let a restore in
 * under it.
 * <p>
 * What an export must still exclude, in both directions, is a restore: a restore drops and replaces the database
 * directory the export is reading off disk (issue #7384).
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7450ExportAdmissionTest {

  @Test
  void anExportAndARestoreOfOneDatabaseExcludeEachOther() {
    final BackupCoordinator coordinator = new BackupCoordinator();

    // A RESTORE IN FLIGHT REFUSES AN EXPORT ...
    assertThat(coordinator.begin("db", Operation.RESTORE)).isNull();
    assertThat(coordinator.begin("db", Operation.EXPORT)).isEqualTo(Operation.RESTORE);
    coordinator.end("db", Operation.RESTORE);

    // ... AND AN EXPORT IN FLIGHT REFUSES A RESTORE
    assertThat(coordinator.begin("db", Operation.EXPORT)).isNull();
    assertThat(coordinator.begin("db", Operation.RESTORE)).isEqualTo(Operation.EXPORT);

    coordinator.end("db", Operation.EXPORT);
    assertThat(coordinator.isInProgress("db")).isFalse();
    assertThat(coordinator.begin("db", Operation.RESTORE)).isNull();
    coordinator.end("db", Operation.RESTORE);
  }

  /**
   * The property the {@code EnumSet} could not express, and the one the issue is about: two exports of one database
   * are both admitted.
   */
  @Test
  void twoExportsOfOneDatabaseRunTogether() {
    final BackupCoordinator coordinator = new BackupCoordinator();

    assertThat(coordinator.begin("db", Operation.EXPORT)).isNull();
    assertThat(coordinator.begin("db", Operation.EXPORT)).isNull();

    assertThat(coordinator.isInProgress("db")).isTrue();
    assertThat(coordinator.isInProgress("db", Operation.EXPORT)).isTrue();
  }

  /**
   * The sharp half of the multiset. Releasing one of two concurrent exports must leave the other one's reservation
   * standing - with a set of kinds the first {@code end} cleared the only entry, and a restore walked in while the
   * second export was still reading the directory.
   */
  @Test
  void endingOneOfTwoExportsLeavesTheOtherHoldingTheDatabase() {
    final BackupCoordinator coordinator = new BackupCoordinator();

    assertThat(coordinator.begin("db", Operation.EXPORT)).isNull();
    assertThat(coordinator.begin("db", Operation.EXPORT)).isNull();

    coordinator.end("db", Operation.EXPORT);

    assertThat(coordinator.isInProgress("db")).as("the second export still holds the database").isTrue();
    assertThat(coordinator.isInProgress("db", Operation.EXPORT)).isTrue();
    assertThat(coordinator.begin("db", Operation.RESTORE)).isEqualTo(Operation.EXPORT);

    coordinator.end("db", Operation.EXPORT);

    assertThat(coordinator.isInProgress("db")).isFalse();
    assertThat(coordinator.begin("db", Operation.RESTORE)).isNull();
    coordinator.end("db", Operation.RESTORE);
  }

  /** The count is per kind, not one number per database: many exports do not hide, or outlive, one backup. */
  @Test
  void manyExportsAndOneBackupEachReleaseOnlyTheirOwnReservation() {
    final BackupCoordinator coordinator = new BackupCoordinator();

    for (int i = 0; i < 5; i++)
      assertThat(coordinator.begin("db", Operation.EXPORT)).isNull();
    assertThat(coordinator.begin("db", Operation.BACKUP)).isNull();

    // THE BACKUP IS STILL THE ONLY ONE OF ITS KIND, HOWEVER MANY EXPORTS ARE RUNNING
    assertThat(coordinator.begin("db", Operation.BACKUP)).isEqualTo(Operation.BACKUP);

    coordinator.end("db", Operation.BACKUP);
    assertThat(coordinator.isInProgress("db", Operation.BACKUP)).isFalse();
    assertThat(coordinator.isInProgress("db", Operation.EXPORT)).as("the exports are untouched by it").isTrue();

    for (int i = 0; i < 5; i++) {
      assertThat(coordinator.isInProgress("db", Operation.EXPORT)).isTrue();
      coordinator.end("db", Operation.EXPORT);
    }
    assertThat(coordinator.isInProgress("db")).isFalse();
  }

  /**
   * An export excludes only a restore. A backup and an import both read or write a live database, which is what an
   * export does, and the schedule already backs a live database up all day (issue #7384).
   */
  @Test
  void anExportRunsAlongsideABackupAndAnImport() {
    final BackupCoordinator coordinator = new BackupCoordinator();

    assertThat(coordinator.begin("db", Operation.BACKUP)).isNull();
    assertThat(coordinator.begin("db", Operation.IMPORT)).isNull();
    assertThat(coordinator.begin("db", Operation.EXPORT)).isNull();

    // AND NONE OF THE THREE LETS A RESTORE IN
    assertThat(coordinator.begin("db", Operation.RESTORE)).isIn(Operation.BACKUP, Operation.IMPORT, Operation.EXPORT);

    coordinator.end("db", Operation.BACKUP);
    coordinator.end("db", Operation.IMPORT);
    assertThat(coordinator.begin("db", Operation.RESTORE)).isEqualTo(Operation.EXPORT);

    coordinator.end("db", Operation.EXPORT);
    assertThat(coordinator.isInProgress("db")).isFalse();
  }

  /**
   * A caller that was refused must not call {@code end()}, but if it does it must not free somebody else's slot -
   * and with counted reservations it must not push a count below zero either, which would make the next genuine
   * release leave the database permanently "in progress".
   */
  @Test
  void releasingAnExportThatWasNeverTakenChangesNothing() {
    final BackupCoordinator coordinator = new BackupCoordinator();

    coordinator.end("db", Operation.EXPORT);
    assertThat(coordinator.isInProgress("db")).isFalse();

    assertThat(coordinator.begin("db", Operation.RESTORE)).isNull();
    coordinator.end("db", Operation.EXPORT);
    coordinator.end("db", Operation.EXPORT);
    assertThat(coordinator.isInProgress("db", Operation.RESTORE)).isTrue();
    assertThat(coordinator.begin("db", Operation.EXPORT)).isEqualTo(Operation.RESTORE);

    coordinator.end("db", Operation.RESTORE);
    assertThat(coordinator.isInProgress("db")).isFalse();

    // AND THE DATABASE IS GENUINELY FREE AFTERWARDS, NOT MERELY REPORTED FREE
    assertThat(coordinator.begin("db", Operation.EXPORT)).isNull();
    coordinator.end("db", Operation.EXPORT);
    assertThat(coordinator.isInProgress("db")).isFalse();
  }

  /** An export of one database never holds up an export, or anything else, of another. */
  @Test
  void aDifferentDatabaseIsNeverHeldUpByAnExport() {
    final BackupCoordinator coordinator = new BackupCoordinator();

    assertThat(coordinator.begin("db1", Operation.EXPORT)).isNull();
    assertThat(coordinator.begin("db2", Operation.RESTORE)).isNull();
    assertThat(coordinator.begin("db1", Operation.RESTORE)).isEqualTo(Operation.EXPORT);

    coordinator.end("db1", Operation.EXPORT);
    coordinator.end("db2", Operation.RESTORE);
    assertThat(coordinator.isInProgress("db1")).isFalse();
    assertThat(coordinator.isInProgress("db2")).isFalse();
  }

  /**
   * An HA snapshot install cannot be refused - it applies a committed Raft entry - so it waits for whatever is in
   * the way (issue #7444). An export is now one of the things that can be in the way, and the install has to wait
   * for BOTH of two concurrent exports rather than for the first one to finish: swapping the directory out from
   * under the second one is the same corruption the wait exists to avoid.
   */
  @Test
  @Timeout(30)
  void aSnapshotInstallWaitsForEveryRunningExport() throws Exception {
    final BackupCoordinator coordinator = new BackupCoordinator();

    assertThat(coordinator.begin("db", Operation.EXPORT)).isNull();
    assertThat(coordinator.begin("db", Operation.EXPORT)).isNull();

    final CountDownLatch waiterStarted = new CountDownLatch(1);
    final AtomicReference<Operation> outcome = new AtomicReference<>(Operation.EXPORT);
    final Thread waiter = new Thread(() -> {
      waiterStarted.countDown();
      outcome.set(coordinator.begin("db", Operation.RESTORE, 20_000));
    }, "install-waiter-7450");
    waiter.start();

    assertThat(waiterStarted.await(10, TimeUnit.SECONDS)).isTrue();
    waiter.join(300);
    assertThat(waiter.isAlive()).as("the install waits instead of replacing the files under a running export").isTrue();

    coordinator.end("db", Operation.EXPORT);
    waiter.join(300);
    assertThat(waiter.isAlive()).as("one of the two exports is still reading the directory").isTrue();
    assertThat(coordinator.isInProgress("db", Operation.RESTORE)).isFalse();

    coordinator.end("db", Operation.EXPORT);

    waiter.join(20_000);
    assertThat(waiter.isAlive()).as("the waiter woke as soon as the last export released the slot").isFalse();
    assertThat(outcome.get()).as("the slot was taken, not refused").isNull();
    assertThat(coordinator.isInProgress("db", Operation.RESTORE)).isTrue();
  }

  /**
   * Every export is admitted however many run at once, no reservation is lost when they release together, and no
   * restore ever gets in beside one.
   */
  @Test
  // PLAIN HANG DETECTOR, NOT A LATENCY BOUND: EVERY CALLER DOES ONE ConcurrentHashMap.compute, SO ANY REAL RUN
  // FINISHES IN MICROSECONDS AND ONLY A DEADLOCK COULD REACH THIS
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void concurrentExportsAreAllAdmittedAndAllReleaseTheirOwnReservation() throws Exception {
    final BackupCoordinator coordinator = new BackupCoordinator();
    final int exporters = 16;
    final CountDownLatch startTogether = new CountDownLatch(1);
    final CountDownLatch allAdmitted = new CountDownLatch(exporters);
    final CountDownLatch releaseTogether = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(exporters);
    final AtomicInteger admitted = new AtomicInteger();
    final Set<Operation> restoresAdmittedBeside = ConcurrentHashMap.newKeySet();

    final ExecutorService executor = Executors.newFixedThreadPool(exporters + 1);
    try {
      for (int i = 0; i < exporters; i++)
        executor.submit(() -> {
          try {
            startTogether.await();
            if (coordinator.begin("db", Operation.EXPORT) == null)
              admitted.incrementAndGet();
            allAdmitted.countDown();
            releaseTogether.await();
            coordinator.end("db", Operation.EXPORT);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            done.countDown();
          }
          return null;
        });

      startTogether.countDown();
      assertThat(allAdmitted.await(60, TimeUnit.SECONDS)).isTrue();

      // WHILE THEY ALL HOLD IT, NOTHING THAT DESTROYS THE DIRECTORY GETS IN
      if (coordinator.begin("db", Operation.RESTORE) == null)
        restoresAdmittedBeside.add(Operation.RESTORE);

      releaseTogether.countDown();
      assertThat(done.await(60, TimeUnit.SECONDS)).isTrue();
    } finally {
      executor.shutdownNow();
    }

    assertThat(admitted).hasValue(exporters);
    assertThat(restoresAdmittedBeside).isEmpty();

    // EVERY RESERVATION WAS RELEASED EXACTLY ONCE: A LOST DECREMENT LEAVES THE DATABASE BLOCKED FOREVER, AND AN
    // EXTRA ONE WOULD HAVE FREED IT WHILE AN EXPORT WAS STILL RUNNING
    assertThat(coordinator.isInProgress("db")).isFalse();
    assertThat(coordinator.begin("db", Operation.RESTORE)).isNull();
    coordinator.end("db", Operation.RESTORE);
  }
}
