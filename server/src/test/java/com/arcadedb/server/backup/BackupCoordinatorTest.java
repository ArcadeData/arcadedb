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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
  void adifferentDatabaseIsNeverHeldUp() {
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

  @Test
  void aNameThatIsNotAnArchiveHasNoTimestamp() {
    assertThat(BackupCoordinator.parseArchiveTimestamp("mydb.zip")).isNull();
    assertThat(BackupCoordinator.parseArchiveTimestamp("mydb-backup-20260826-1430.zip")).isNull();
    assertThat(BackupCoordinator.parseArchiveTimestamp("mydb-backup-20260826-143000.zip.tmp")).isNull();
  }
}
