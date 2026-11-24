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
import com.arcadedb.integration.backup.BackupSettings;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.EnumSet;
import java.util.Locale;
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
 * Admission and archive naming, the two halves of what keeps two backups of one database off the same file
 * (issue #6753).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BackupCoordinatorTest {

  @Test
  void onlyOneBackupPerDatabaseIsAdmittedAtATime() {
    final BackupCoordinator coordinator = new BackupCoordinator();

    assertThat(coordinator.begin("db")).isTrue();
    assertThat(coordinator.isInProgress("db")).isTrue();
    assertThat(coordinator.begin("db")).isFalse();

    coordinator.end("db");

    assertThat(coordinator.isInProgress("db")).isFalse();
    assertThat(coordinator.begin("db")).isTrue();
    coordinator.end("db");
  }

  @Test
  void aDifferentDatabaseIsNeverHeldUp() {
    final BackupCoordinator coordinator = new BackupCoordinator();

    assertThat(coordinator.begin("db1")).isTrue();
    assertThat(coordinator.begin("db2")).isTrue();

    coordinator.end("db1");
    coordinator.end("db2");
  }

  @Test
  // PLAIN HANG DETECTOR, NOT A LATENCY BOUND: THE CALLERS DO ONE Set.add EACH, SO ANY REAL RUN FINISHES IN
  // MICROSECONDS AND ONLY A DEADLOCK COULD REACH THIS
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void exactlyOneOfManyConcurrentCallersIsAdmitted() throws Exception {
    final BackupCoordinator coordinator = new BackupCoordinator();
    final int callers = 8;
    final CountDownLatch startTogether = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(callers);
    final AtomicInteger admitted = new AtomicInteger();

    final ExecutorService executor = Executors.newFixedThreadPool(callers);
    try {
      for (int i = 0; i < callers; i++)
        executor.submit(() -> {
          try {
            startTogether.await();
            if (coordinator.begin("db"))
              admitted.incrementAndGet();
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            done.countDown();
          }
          return null;
        });

      startTogether.countDown();
      done.await();
    } finally {
      executor.shutdownNow();
    }

    assertThat(admitted.get()).isEqualTo(1);
  }

  @Test
  void archiveNamesCarryMilliseconds() {
    final String name = new BackupCoordinator().newArchiveName("mydb");

    // 8 DIGITS OF DATE, 6 OF TIME AND 3 OF MILLISECONDS: THE SECOND-PRECISION NAME THIS REPLACES MADE TWO BACKUPS
    // STARTING IN THE SAME SECOND RESOLVE TO ONE PATH
    assertThat(name).matches("mydb-backup-\\d{8}-\\d{9}\\.zip");
  }

  @Test
  void archiveNamesAreReadBackByTheSameConvention() {
    final String name = new BackupCoordinator().newArchiveName("mydb");

    final LocalDateTime parsed = BackupCoordinator.parseArchiveTimestamp(name);

    // A ROUND TRIP RATHER THAN A COMPARISON AGAINST now(): WHAT HAS TO HOLD IS THAT THE PARSER AGREES WITH THE WRITER
    // DIGIT FOR DIGIT - ONE THAT DRIFTED WOULD DROP EVERY NEW ARCHIVE OUT OF THE RETENTION SET - AND THAT IS A
    // PROPERTY OF THE TWO SIDES, NOT OF WHAT THE CLOCK SAID IN BETWEEN
    assertThat(parsed).isNotNull();
    assertThat(name).isEqualTo(
        "mydb-backup-" + parsed.format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmssSSS")) + ".zip");
  }

  @Test
  void secondPrecisionArchivesFromOlderReleasesStillParse() {
    // RETENTION AND THE BACKUP LISTING RUN OVER DIRECTORIES HOLDING BOTH CONVENTIONS: A NAME THEY CANNOT READ IS A
    // FILE THEY WOULD NEVER LIST AND NEVER ROTATE OUT
    assertThat(BackupCoordinator.parseArchiveTimestamp("mydb-backup-20260826-143000.zip"))
        .isEqualTo(LocalDateTime.of(2026, 8, 26, 14, 30, 0));

    assertThat(BackupCoordinator.parseArchiveTimestamp("mydb-backup-20260826-143000456.zip"))
        .isEqualTo(LocalDateTime.of(2026, 8, 26, 14, 30, 0, 456_000_000));
  }

  /**
   * The convention exists twice - here, for everything the server writes, and as the default {@code BackupSettings}
   * assigns a CLI or SQL backup that names no target, because the server cannot depend on the integration module
   * (it reaches {@code Backup} reflectively so it stays buildable without it). Both write into the same directories,
   * and retention reads both through the parser above, so the day one of them changes and the other does not is the
   * day retention silently stops rotating half the archives. A comment asking the next editor to change both is not
   * enforcement; this is.
   */
  @Test
  void theDefaultNameOfACliOrSqlBackupFollowsTheSameConvention() {
    final BackupSettings settings = new BackupSettings();
    settings.databaseName = "mydb";
    settings.validateSettings();

    assertThat(settings.file).matches("mydb-backup-\\d{8}-\\d{9}\\.zip");
    assertThat(BackupCoordinator.parseArchiveTimestamp(settings.file)).isNotNull();
  }

  /**
   * Issue #8301: the CLI/SQL default name was built with a {@code SimpleDateFormat} that took the default locale's
   * calendar and digits, so under th-TH it carried the Buddhist year (543 years in the future to retention) and under
   * ar-EG or the Japanese imperial calendar a name retention could not read at all.
   */
  @Test
  void bothArchiveNamesAreLocaleIndependent() {
    final Locale saved = Locale.getDefault();
    try {
      for (final String tag : new String[] { "th-TH", "ar-EG", "ja-JP-u-ca-japanese", "hi-IN-u-nu-deva", "en-US" }) {
        Locale.setDefault(Locale.forLanguageTag(tag));

        final LocalDateTime before = LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS);
        final BackupSettings settings = new BackupSettings();
        settings.databaseName = "mydb";
        settings.validateSettings();
        final String serverName = new BackupCoordinator().newArchiveName("mydb");
        final LocalDateTime after = LocalDateTime.now();

        for (final String name : new String[] { settings.file, serverName }) {
          assertThat(name).as("locale %s", tag).matches("mydb-backup-\\d{8}-\\d{9}\\.zip");
          assertThat(BackupCoordinator.parseArchiveTimestamp(name)).as("locale %s: %s", tag, name).isBetween(before, after);
        }
      }
    } finally {
      Locale.setDefault(saved);
    }
  }

  @Test
  void aNameThatIsNotAnArchiveHasNoTimestamp() {
    assertThat(BackupCoordinator.parseArchiveTimestamp("mydb.zip")).isNull();
    assertThat(BackupCoordinator.parseArchiveTimestamp("mydb-backup-20260826-1430.zip")).isNull();
    assertThat(BackupCoordinator.parseArchiveTimestamp("mydb-backup-20260826-143000.zip.tmp")).isNull();
  }

  // Regression: #7384 a restore excludes, and is excluded by, every other operation on the same database
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
  @Nested
  class OperationAdmission {

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
        // EXPORT IS THE ONE KIND THAT DOES NOT EXCLUDE ITSELF, AND IT IS DELIBERATE: TWO EXPORTS OF ONE DATABASE NAME
        // TWO DIFFERENT TARGETS, WHERE TWO BACKUPS RESOLVE TO ONE ARCHIVE (#6753). ITS OWN ADMISSION IS PINNED BY
        // BackupCoordinatorTest.ExportAdmission, WHICH COVERS BOTH THE COEXISTENCE AND THE RESTORE IT STILL EXCLUDES (#7450)
        if (operation == Operation.EXPORT)
          continue;

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

      // AND A KIND THAT REFUSES EVERY OTHER KIND MUST HAVE BEEN ADMITTED ALONE: A RESTORE, A DROP OR A CLOSE TAKES
      // THE DATABASE AWAY FROM EVERYONE ELSE, WHILE BACKUP, IMPORT AND EXPORT COEXIST IN ANY COMBINATION.
      //
      // DERIVED FROM conflictsWith RATHER THAN LISTED AS A SET OF LEGAL EnumSets, BECAUSE THAT LIST HAD TO BE
      // EXTENDED BY HAND EVERY TIME A KIND WAS ADDED AND WENT STALE WHEN ONE WAS NOT: EXPORT JOINED IN #7450, DROP
      // IN #7641, AND CLOSE IN #7469 WITHOUT THE LIST FOLLOWING - WHICH LEFT THIS TEST FAILING ON main FOR THE ONE
      // OUTCOME THE LIST DID NOT NAME. WRITTEN THIS WAY IT NEEDS NO EDIT WHEN THE NEXT KIND IS ENROLLED.
      for (final Operation one : admitted)
        if (EnumSet.complementOf(EnumSet.of(one)).stream().allMatch(one::conflictsWith))
          assertThat(admitted).as("%s refuses every other kind, so it can only be admitted alone", one)
              .containsExactly(one);
    }
  }

  // Regression: #7450 exports of one database coexist with each other but still exclude a restore
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
  @Nested
  class ExportAdmission {

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
}
