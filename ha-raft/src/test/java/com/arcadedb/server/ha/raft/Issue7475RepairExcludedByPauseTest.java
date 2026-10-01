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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.timeseries.TimeSeriesCompactionPause;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.server.ha.raft.RaftLogEntryCodec.TsSealedBlob;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7475: a sealed-store repair on a type whose engine never loaded (issue #6356) ran with no lock at all, so
 * a backup or a snapshot ship taken across it could pair the mutable pages from before the clear WAL with the
 * repaired {@code .ts.sealed} from after the move - the duplicated-samples restore #7280 and #7337 each closed on
 * their own path. The per-shard compaction lock the other two rely on does not exist until the engine does, so
 * the repair is now excluded through the type's own lifecycle lock.
 * <p>
 * Same harness as {@code Issue6948MaintenanceAfterRepairTest}: a real {@link LocalDatabase}, a real (unstarted)
 * {@link ArcadeStateMachine}, no mocking. The test holds a {@link TimeSeriesCompactionPause}, drives the repair on
 * another thread, and asserts it does NOT complete while the pause is held - then that the very same repair
 * completes promptly once the pause is released, which is what stops the first assertion from passing because the
 * repair was broken for some other reason.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7475RepairExcludedByPauseTest {

  private static final String TYPE_NAME   = "Cpu";
  private static final String SEALED_FILE = TYPE_NAME + "_shard_0.ts.sealed";
  private static final int    ROWS        = 3_000;
  /** A wait that is EXPECTED to expire: it IS the assertion. A stall can only make it more true. */
  private static final long   BLOCKED_PROBE_MS = 1_500L;
  private static final long   PARK_WAIT_MS     = 30_000L;

  @TempDir
  private Path          serverDir;
  private LocalDatabase database;
  private String        databasePath;

  @BeforeEach
  void setUp() {
    databasePath = serverDir.resolve("db-ts").toString();
    database = (LocalDatabase) new DatabaseFactory(databasePath).create();
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.close();
  }

  @Test
  void aRepairWaitsForAHeldCompactionPauseAndRunsOnceItIsReleased() throws Exception {
    final byte[] leaderSealedBytes = buildTypeAndCaptureItsSealedFile();
    breakTheSealedFileAndReopen();

    final LocalTimeSeriesType broken = (LocalTimeSeriesType) database.getSchema().getType(TYPE_NAME);
    assertThat(broken.isEngineAvailable()).as("the #6356 state this test starts from").isFalse();

    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread repair;
    try (final TimeSeriesCompactionPause pause = TimeSeriesCompactionPause.acquire(database, 30_000L)) {
      repair = new Thread(() -> {
        try {
          new ArcadeStateMachine().applySealedBlobs(database,
              List.of(new TsSealedBlob(TYPE_NAME, 0, SEALED_FILE, leaderSealedBytes)));
        } catch (final Throwable e) {
          failure.set(e);
        }
      }, "issue7475-repair");
      repair.setDaemon(true);
      repair.start();

      awaitParked(repair);
      repair.join(BLOCKED_PROBE_MS);

      assertThat(repair.isAlive()).as("a repair must not complete inside a held pause: that is the torn copy").isTrue();
      assertThat(broken.isEngineAvailable()).as("and it must not have created the engine either").isFalse();
    }

    repair.join(60_000L);
    assertThat(repair.isAlive()).as("released, the same repair must go through promptly").isFalse();
    assertThat(failure.get()).isNull();
    assertThat(broken.isEngineAvailable()).isTrue();
  }

  private static void awaitParked(final Thread thread) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + PARK_WAIT_MS;
    while (System.currentTimeMillis() < deadline) {
      final Thread.State state = thread.getState();
      if (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING)
        for (final StackTraceElement frame : thread.getStackTrace())
          if (frame.getClassName().endsWith("ArcadeStateMachine") && frame.getMethodName().startsWith("repairEngineWithSealed"))
            return;
      if (state == Thread.State.TERMINATED)
        break;
      Thread.sleep(10);
    }
    throw new AssertionError("the repair thread never parked on the type's lock (state=" + thread.getState()
        + "); the assertion that follows would have been vacuous");
  }

  private byte[] buildTypeAndCaptureItsSealedFile() throws IOException {
    database.command("sql", "CREATE TIMESERIES TYPE " + TYPE_NAME
        + " TIMESTAMP ts TAGS (hostname STRING) FIELDS (usage DOUBLE) SHARDS 1");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType(TYPE_NAME)).getEngine();
    final long[] timestamps = new long[ROWS];
    final Object[][] columns = new Object[2][ROWS];
    for (int i = 0; i < ROWS; i++) {
      timestamps[i] = 1_700_000_000_000L + i * 1_000L;
      columns[0][i] = "host_" + (i % 7);
      columns[1][i] = (double) i;
    }
    engine.appendBatch(timestamps, columns);
    engine.compactAll();

    final File sealed = new File(databasePath, SEALED_FILE);
    assertThat(sealed).exists();
    return Files.readAllBytes(sealed.toPath());
  }

  /** Flips byte 0 of the sealed file, so the store can never open, and reopens the database (#6356). */
  private void breakTheSealedFileAndReopen() throws IOException {
    database.close();
    try (final RandomAccessFile raf = new RandomAccessFile(new File(databasePath, SEALED_FILE), "rw")) {
      raf.seek(0);
      final int b = raf.read();
      raf.seek(0);
      raf.write(b ^ 0x01);
    }
    database = (LocalDatabase) new DatabaseFactory(databasePath).open();
  }
}
