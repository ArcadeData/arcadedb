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
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.engine.timeseries.TimeSeriesMaintenanceScheduler;
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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6948, the remainder of #6839: a TimeSeries type that recovers through the HA sealed-store repair came back
 * readable and writable yet permanently UNMAINTAINED.
 * <p>
 * {@code schedule()} has exactly two call sites in main code - type creation, and
 * {@code LocalSchema.readConfiguration()} behind an {@code isEngineAvailable()} gate. The gate is skipped for
 * precisely the types this path repairs, because at schema load their engine is the thing that failed. So the
 * repair handed back a usable type with no maintenance task, and nothing downstream ever created one:
 * {@code compactAll()}, {@code applyRetention()} and {@code applyDownsampling()} have no caller outside the
 * recurring task, which means the mutable bucket grew unbounded ("Compaction is always scheduled to prevent
 * unbounded mutable-bucket growth", says the scheduler's own javadoc) and configured retention and downsampling
 * silently stopped being applied for that type on that node until the database was closed and reopened.
 * <p>
 * "It is a follower, it would skip the work anyway" does not cover it: the leader-only skip lives INSIDE the
 * recurring task, not in {@code schedule()}. A healthy follower keeps a ticking task that takes over the instant it
 * is elected; a repaired one had none, and never would. {@link #aHealthyTypeIsMaintainedAfterAPlainReopen()} is the
 * control that pins that asymmetry - it is what the repaired type has to be restored to parity with.
 * <p>
 * Same harness as {@code Issue6839TsSealedBlobRecoveryTest}: a real {@link LocalDatabase} and a real (unstarted)
 * {@link ArcadeStateMachine}, no mocking. The cluster adds nothing here - the defect is entirely in the apply path.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue6948MaintenanceAfterRepairTest {

  private static final String TYPE_NAME   = "Cpu";
  private static final String SEALED_FILE = sealedFileName(0);
  private static final int    ROWS        = 3_000;

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

  /**
   * The regression itself. Before the fix every assertion up to the repair passed and the last one failed: the
   * type was available again and maintained by nothing.
   */
  @Test
  void aRepairedTypeIsScheduledForMaintenanceAgain() throws Exception {
    final byte[] leaderSealedBytes = buildTypeAndCaptureItsSealedFile();

    breakTheSealedFileAndReopen();

    final LocalTimeSeriesType broken = (LocalTimeSeriesType) database.getSchema().getType(TYPE_NAME);
    assertThat(broken.isEngineAvailable()).as("the #6356 state this test starts from").isFalse();
    assertThat(scheduler().isScheduled(TYPE_NAME))
        .as("the schema-load gate skips an engine-unavailable type, so nothing is maintaining it yet").isFalse();

    new ArcadeStateMachine().applySealedBlobs(database,
        List.of(new TsSealedBlob(TYPE_NAME, 0, SEALED_FILE, leaderSealedBytes)));

    assertThat(broken.isEngineAvailable()).as("the #6839 repair still works").isTrue();
    assertThat(scheduler().isScheduled(TYPE_NAME))
        .as("a repaired type must be maintained again: compaction, retention and downsampling have no other caller")
        .isTrue();
  }

  /**
   * The parity the arm above is measured against, and the reason "it is only a follower" is not an answer: a type
   * whose files are intact is maintained from the moment the schema loads, whether or not this node leads. The
   * leader-only decision is taken per tick, inside the task.
   */
  @Test
  void aHealthyTypeIsMaintainedAfterAPlainReopen() throws Exception {
    buildTypeAndCaptureItsSealedFile();

    database.close();
    database = (LocalDatabase) new DatabaseFactory(databasePath).open();

    final LocalTimeSeriesType healthy = (LocalTimeSeriesType) database.getSchema().getType(TYPE_NAME);
    assertThat(healthy.isEngineAvailable()).isTrue();
    assertThat(scheduler().isScheduled(TYPE_NAME)).isTrue();
  }

  /**
   * A repair that did not repair anything must not claim the type is maintained. The blob here is bytes the sealed
   * store cannot open, so {@code initEngine()} fails again and the method returns before the new scheduling call -
   * which matters, because a task for a type whose engine is null would tick forever doing nothing while reporting
   * that the type is looked after.
   */
  @Test
  void aFailedRepairSchedulesNothing() throws Exception {
    buildTypeAndCaptureItsSealedFile();

    breakTheSealedFileAndReopen();

    final LocalTimeSeriesType broken = (LocalTimeSeriesType) database.getSchema().getType(TYPE_NAME);
    assertThat(broken.isEngineAvailable()).isFalse();

    new ArcadeStateMachine().applySealedBlobs(database,
        List.of(new TsSealedBlob(TYPE_NAME, 0, SEALED_FILE, new byte[512])));

    assertThat(broken.isEngineAvailable()).isFalse();
    assertThat(scheduler().isScheduled(TYPE_NAME))
        .as("nothing was repaired, so nothing may be scheduled").isFalse();
  }

  /**
   * Recovery is not one-shot: a type that is corrupted, repaired, and then corrupted AGAIN comes back maintained
   * the second time too.
   * <p>
   * The second corruption and reopen are what make this a genuine second repair rather than a repeat of the first.
   * {@code applySealedBlobs} only takes the repair branch when {@code tsType.getEngine() == null}; once the first
   * repair has succeeded the type has an engine, and a further blob for it is simply installed through the live
   * sealed store - see {@link #aRedundantBlobAfterARepairLeavesTheTaskInPlace()}, which pins that other branch. So
   * driving the type back to engine-unavailable is the only way to reach {@code scheduleMaintenanceAfterRepair}
   * twice, and the reopen is the only way to do it without reaching into the type's internals: {@code initEngine()}
   * returns immediately once the engine is non-null.
   */
  @Test
  void aSecondCorruptionIsRepairedAndScheduledAgain() throws Exception {
    final byte[] leaderSealedBytes = buildTypeAndCaptureItsSealedFile();

    breakTheSealedFileAndReopen();
    new ArcadeStateMachine().applySealedBlobs(database,
        List.of(new TsSealedBlob(TYPE_NAME, 0, SEALED_FILE, leaderSealedBytes)));
    assertThat(scheduler().isScheduled(TYPE_NAME)).as("the first repair schedules").isTrue();

    // Corrupt the file the first repair put in place, and come back up on it.
    breakTheSealedFileAndReopen();
    final LocalTimeSeriesType brokenAgain = (LocalTimeSeriesType) database.getSchema().getType(TYPE_NAME);
    assertThat(brokenAgain.isEngineAvailable()).isFalse();
    assertThat(scheduler().isScheduled(TYPE_NAME))
        .as("the reopened schema skips the gate again, so nothing is maintaining it").isFalse();

    new ArcadeStateMachine().applySealedBlobs(database,
        List.of(new TsSealedBlob(TYPE_NAME, 0, SEALED_FILE, leaderSealedBytes)));

    assertThat(brokenAgain.isEngineAvailable()).isTrue();
    assertThat(scheduler().isScheduled(TYPE_NAME)).as("and so does the second").isTrue();
  }

  /**
   * The other branch. A blob that arrives for a type whose engine is already healthy - a Raft entry replayed after
   * a restart, the leader re-shipping a store - never reaches {@code repairEngineWithSealedFile} at all: it is
   * installed through the live sealed store. What this pins is that the maintenance task the earlier repair
   * installed survives it, since a type losing its task on a routine install would be the same defect by another
   * route.
   */
  @Test
  void aRedundantBlobAfterARepairLeavesTheTaskInPlace() throws Exception {
    final byte[] leaderSealedBytes = buildTypeAndCaptureItsSealedFile();

    breakTheSealedFileAndReopen();

    final ArcadeStateMachine stateMachine = new ArcadeStateMachine();
    stateMachine.applySealedBlobs(database,
        List.of(new TsSealedBlob(TYPE_NAME, 0, SEALED_FILE, leaderSealedBytes)));

    final LocalTimeSeriesType repaired = (LocalTimeSeriesType) database.getSchema().getType(TYPE_NAME);
    assertThat(repaired.isEngineAvailable()).isTrue();
    assertThat(scheduler().isScheduled(TYPE_NAME)).isTrue();

    stateMachine.applySealedBlobs(database,
        List.of(new TsSealedBlob(TYPE_NAME, 0, SEALED_FILE, leaderSealedBytes)));

    assertThat(repaired.isEngineAvailable()).isTrue();
    assertThat(scheduler().isScheduled(TYPE_NAME)).as("a routine install must not unschedule the type").isTrue();
  }

  // ---- Helpers ----

  private TimeSeriesMaintenanceScheduler scheduler() {
    return database.getSchema().getEmbedded().getTimeSeriesMaintenanceScheduler();
  }

  private byte[] buildTypeAndCaptureItsSealedFile() throws IOException {
    database.command("sql", "CREATE TIMESERIES TYPE " + TYPE_NAME
        + " TIMESTAMP ts TAGS (hostname STRING) FIELDS (usage DOUBLE) SHARDS 1");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType(TYPE_NAME)).getEngine();
    appendRows(engine, 0, ROWS);
    engine.compactAll();

    final File sealed = new File(databasePath, SEALED_FILE);
    assertThat(sealed).exists();
    return Files.readAllBytes(sealed.toPath());
  }

  private static String sealedFileName(final int shardIndex) {
    return TYPE_NAME + "_shard_" + shardIndex + ".ts.sealed";
  }

  /**
   * Flips byte 0 of the sealed file - part of its MAGIC_VALUE, so the store can never open - and reopens the
   * database, which is the state {@code LocalSchema.readConfiguration()} registers rather than dropping (#6356).
   */
  private void breakTheSealedFileAndReopen() throws IOException {
    database.close();
    final File sealed = new File(databasePath, SEALED_FILE);
    try (final RandomAccessFile raf = new RandomAccessFile(sealed, "rw")) {
      raf.seek(0);
      final int b = raf.read();
      raf.seek(0);
      raf.write(b ^ 0x01);
    }
    database = (LocalDatabase) new DatabaseFactory(databasePath).open();
  }

  private void appendRows(final TimeSeriesEngine engine, final int from, final int rows) throws IOException {
    final long[] timestamps = new long[rows];
    final Object[][] columns = new Object[2][rows];
    for (int i = 0; i < rows; i++) {
      timestamps[i] = 1_700_000_000_000L + (from + i) * 1_000L;
      columns[0][i] = "host_" + ((from + i) % 7);
      columns[1][i] = (double) (from + i);
    }
    engine.appendBatch(timestamps, columns);
  }
}
