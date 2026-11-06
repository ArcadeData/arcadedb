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
package com.arcadedb.engine;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #11352: the gate that keeps a retired WAL file while a group commit with a queued record sits between its
 * two columnar marks was one database-wide counter, so under overlapping group-commit ingest the counter was never zero at a pass and
 * the inactive pool grew. The gate is now per retire epoch: a file is held back only by the commits that started before it was retired.
 */
class Issue11352QueuedColumnarMarksEpochTest extends TestHelper {

  private TransactionManager tm() {
    return ((DatabaseInternal) database).getTransactionManager();
  }

  private WALFile retire(final String name) throws IOException {
    final WALFile file = new WALFile(new File(database.getDatabasePath(), name).getAbsolutePath());
    tm().addInactiveWALFileForTesting(file);
    return file;
  }

  @Test
  void aCommitStartedBeforeTheRetirementHoldsTheFileUntilItEnds() throws Exception {
    final long epoch = tm().queuedColumnarMarksStarted();
    final File wal = new File(database.getDatabasePath(), "issue11352-a.wal");
    retire("issue11352-a.wal");

    assertThat(tm().cleanWALFilesForTesting(true, false, false)).as("the queued commit may still land in the file").isFalse();
    assertThat(wal.exists()).isTrue();

    tm().queuedColumnarMarksEnded(epoch);

    assertThat(tm().cleanWALFilesForTesting(true, false, false)).isTrue();
    assertThat(wal.exists()).isFalse();
  }

  @Test
  void aCommitStartedAfterTheRetirementDoesNotHoldTheFile() throws Exception {
    final File wal = new File(database.getDatabasePath(), "issue11352-b.wal");
    retire("issue11352-b.wal");

    final long epoch = tm().queuedColumnarMarksStarted();
    try {
      assertThat(tm().cleanWALFilesForTesting(true, false, false))
          .as("a record queued after the file left the active pool cannot land in it: always-overlapping commits must not pin it").isTrue();
      assertThat(wal.exists()).isFalse();
    } finally {
      tm().queuedColumnarMarksEnded(epoch);
    }
  }

  @Test
  void overlappingCommitsNeverPinAFileRetiredBeforeTheNewestOfThem() throws Exception {
    // c1 in flight, F1 retired, c2 starts, c1 ends: the count never reaches zero (the old gate held F1 forever), yet F1 is free
    final long c1 = tm().queuedColumnarMarksStarted();
    final File wal1 = new File(database.getDatabasePath(), "issue11352-c1.wal");
    retire("issue11352-c1.wal");
    final long c2 = tm().queuedColumnarMarksStarted();
    tm().queuedColumnarMarksEnded(c1);

    assertThat(tm().cleanWALFilesForTesting(true, false, false)).isTrue();
    assertThat(wal1.exists()).isFalse();

    // the same for the next file, with c3 overlapping c2
    final File wal2 = new File(database.getDatabasePath(), "issue11352-c2.wal");
    retire("issue11352-c2.wal");
    assertThat(tm().cleanWALFilesForTesting(true, false, false)).as("c2 started before F2 was retired").isFalse();
    final long c3 = tm().queuedColumnarMarksStarted();
    tm().queuedColumnarMarksEnded(c2);
    assertThat(tm().cleanWALFilesForTesting(true, false, false)).isTrue();
    assertThat(wal2.exists()).isFalse();
    tm().queuedColumnarMarksEnded(c3);
  }

  @Test
  void theInactivePoolDrainsUnderOverlappingConcurrentCommitters() throws Exception {
    final int committers = 4;
    final int rounds = 200;
    final ExecutorService pool = Executors.newFixedThreadPool(committers);
    final CountDownLatch go = new CountDownLatch(1);
    try {
      final List<Future<?>> futures = new ArrayList<>();
      for (int t = 0; t < committers; t++)
        futures.add(pool.submit(() -> {
          go.await();
          // each committer keeps one commit open while it starts the next: at every instant at least one is in flight
          long previous = tm().queuedColumnarMarksStarted();
          for (int i = 0; i < rounds; i++) {
            final long next = tm().queuedColumnarMarksStarted();
            tm().queuedColumnarMarksEnded(previous);
            previous = next;
          }
          tm().queuedColumnarMarksEnded(previous);
          return null;
        }));

      go.countDown();
      // rotations happen while the committers run
      for (int i = 0; i < 20; i++)
        retire("issue11352-d" + i + ".wal");
      for (final Future<?> f : futures)
        f.get(60, TimeUnit.SECONDS);
    } finally {
      pool.shutdownNow();
    }

    assertThat(tm().cleanWALFilesForTesting(true, false, false)).as("every committer ended: the whole inactive pool drains").isTrue();
    for (int i = 0; i < 20; i++)
      assertThat(new File(database.getDatabasePath(), "issue11352-d" + i + ".wal").exists()).isFalse();
  }
}
