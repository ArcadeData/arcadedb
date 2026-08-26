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

import java.time.LocalDateTime;
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
      assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
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
    final BackupCoordinator coordinator = new BackupCoordinator();
    final LocalDateTime before = LocalDateTime.now().minusSeconds(1);

    final LocalDateTime parsed = BackupCoordinator.parseArchiveTimestamp(coordinator.newArchiveName("mydb"));

    assertThat(parsed).isNotNull();
    assertThat(parsed).isAfterOrEqualTo(before);
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
