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
import com.arcadedb.engine.timeseries.TimeSeriesSealedStore;
import com.arcadedb.query.sql.executor.ResultSet;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #6839 (item 1): the "registered but engine-unavailable" TimeSeries state that #6356 made visible had no way
 * out, and the one payload that could repair it was the payload this path threw away.
 * <p>
 * {@code LocalSchema.readConfiguration()} justifies keeping such a type registered on the grounds that the file
 * that failed to open - "a {@code .ts.sealed} most commonly" - is rebuildable under HA from the replicated mutable
 * pages. But {@code applySealedBlobs} refused exactly that state ({@code tsType.getEngine() == null}) and logged
 * the blob away, and a Raft entry is applied once and never re-shipped, so every dropped blob was a permanent
 * divergence. Nothing else ever retried {@code initEngine()} - its only non-test callers are the schema-load site
 * and type creation - maintenance is gated off the type by {@code isEngineAvailable()}, and every read and write
 * throws through {@code requireEngine()}. The only escape was closing and reopening the whole database, i.e. the
 * restart #6356 set out to make unnecessary.
 * <p>
 * Two things had to change for a blob to actually repair the type, and the arms below pin both:
 * <ol>
 *   <li>the incoming bytes must land on disk BEFORE {@code initEngine()} runs. A bare retry re-opens the same
 *       corrupt {@code .ts.sealed} and fails again for the same reason, so "retry, then install" recovers
 *       nothing in the reported repro - the blob IS the repair, and it has to be in place first;</li>
 *   <li>{@code initEngine()} had to become genuinely retryable. {@code TimeSeriesShard}'s constructor closed the
 *       mutable bucket when the sealed store threw, and {@code PaginatedComponentFile.close()} sets
 *       {@code open=false}, which its own lazy-reopen then refuses ("was closed on purpose"). The component stays
 *       registered with the schema either way, so that close never prevented a leak - it only made the file
 *       permanently unreadable for the life of the process. A retry over it produced a type reporting
 *       {@code isEngineAvailable() == true} whose every read threw {@code FileNotFoundException}, which is why
 *       {@link #aSealedBlobRepairsATypeWhoseEngineFailedToInitialise()} reads AND writes after the repair rather
 *       than only asserting the flag.</li>
 * </ol>
 * Driven against a real {@link LocalDatabase} and a real (unstarted) {@link ArcadeStateMachine}, no mocking - the
 * same harness as {@code ArcadeStateMachineBootstrapDivergenceTest}. The cluster is not needed: the defect is
 * entirely inside the apply path, and a 3-node IT would only add the flakiness without adding evidence.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6839TsSealedBlobRecoveryTest {

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
   * The whole of item 1: a follower whose sealed store failed to open comes back on the next blob the leader ships
   * for it, with its data - the leader's, and its own mutable pages - intact and writable.
   */
  @Test
  void aSealedBlobRepairsATypeWhoseEngineFailedToInitialise() throws Exception {
    final byte[] leaderSealedBytes = buildTypeAndCaptureItsSealedFile();

    breakTheSealedFileAndReopen();

    final LocalTimeSeriesType broken = (LocalTimeSeriesType) database.getSchema().getType(TYPE_NAME);
    assertThat(broken.isEngineAvailable()).as("the #6356 state this test is about").isFalse();
    assertThat(broken.getEngineUnavailableReason()).contains(SEALED_FILE);

    new ArcadeStateMachine().applySealedBlobs(database,
        List.of(new TsSealedBlob(TYPE_NAME, 0, SEALED_FILE, leaderSealedBytes)));

    assertThat(broken.isEngineAvailable()).as("the blob must repair the type, not be discarded").isTrue();
    assertThat(broken.getEngineUnavailableReason()).as("a successful retry clears the stale reason").isNull();

    // The leader's blob is on disk and readable: the sealed blocks are back, and so are their samples. The count
    // is the unfiltered one on purpose - a tag-filtered count reflects only the sealed subset, which would pass
    // here for the wrong reason.
    assertThat(broken.getEngine().getShard(0).getSealedStore().getBlockCount()).isGreaterThan(0);
    assertThat(countSamples()).isEqualTo(ROWS);

    // And the MUTABLE half survived too. This is the arm that fails when initEngine() is merely retried over a
    // component whose file the first, failed attempt closed for good: the flag above flips to true either way,
    // and only touching the file says which of the two happened.
    appendRows(broken.getEngine(), ROWS, 100);
    assertThat(countSamples()).isEqualTo(ROWS + 100L);
  }

  /**
   * The repair is attempted for the engine-unavailable state alone. A blob naming a type that is not a TimeSeries
   * type at all is still refused and logged - there is nothing there an engine could be initialised over, and
   * writing the payload to a file named after it would be worse than dropping it.
   */
  @Test
  void aBlobForANonTimeSeriesTypeIsStillRefused() throws Exception {
    database.getSchema().createDocumentType("NotATimeSeries");

    new ArcadeStateMachine().applySealedBlobs(database,
        List.of(new TsSealedBlob("NotATimeSeries", 0, "NotATimeSeries_shard_0.ts.sealed", new byte[] { 1, 2, 3 })));

    assertThat(new File(databasePath, "NotATimeSeries_shard_0.ts.sealed"))
        .as("nothing may be written for a type that has no sealed store").doesNotExist();
  }

  /** A healthy type takes the path it always took: the blob is installed through the live sealed store. */
  @Test
  void aHealthyTypeStillInstallsTheBlobThroughItsLiveEngine() throws Exception {
    final byte[] leaderSealedBytes = buildTypeAndCaptureItsSealedFile();
    final LocalTimeSeriesType healthy = (LocalTimeSeriesType) database.getSchema().getType(TYPE_NAME);
    assertThat(healthy.isEngineAvailable()).isTrue();
    final TimeSeriesEngine engineBefore = healthy.getEngine();

    new ArcadeStateMachine().applySealedBlobs(database,
        List.of(new TsSealedBlob(TYPE_NAME, 0, SEALED_FILE, leaderSealedBytes)));

    assertThat(healthy.getEngine()).as("a healthy type is never re-initialised").isSameAs(engineBefore);
    assertThat(countSamples()).isEqualTo(ROWS);

    // The one coupling in the repair path with no compile-time enforcement: sealedFileNameFor() rebuilds the
    // <type>_shard_<index> half of the name that TimeSeriesShard's constructor owns, so a future rename there
    // would silently send the repair at a path nothing reads. Asserted against the live store's own answer, which
    // is derived from the path the shard actually opened.
    assertThat(TimeSeriesSealedStore.sealedFileNameFor(TYPE_NAME, 0))
        .as("the derived name must match the name the shard actually opened")
        .isEqualTo(healthy.getEngine().getShard(0).getSealedStore().getSealedFileName());
  }

  /**
   * The same recovery on a type with more than one shard, which is where a second copy of the very defect this PR
   * fixes lived: {@code TimeSeriesEngine}'s constructor closes every shard it had already built when a later one
   * throws, and {@code TimeSeriesShard.close()} closes the mutable bucket. So a corrupt {@code .ts.sealed} on
   * shard 1 took shard 0's mutable bucket down with it - permanently, since
   * {@code PaginatedComponentFile.close()} sets {@code open=false} - and no later repair could ever succeed,
   * however good the incoming blob was. Both other arms use SHARDS 1 and cannot see it.
   */
  @Test
  void aSealedBlobRepairsATypeWithMoreThanOneShard() throws Exception {
    final byte[] leaderSealedBytes = buildTypeAndCaptureItsSealedFile(2, 1);

    breakTheSealedFileAndReopen(1);

    final LocalTimeSeriesType broken = (LocalTimeSeriesType) database.getSchema().getType(TYPE_NAME);
    assertThat(broken.isEngineAvailable()).isFalse();
    assertThat(broken.getEngineUnavailableReason()).contains(sealedFileName(1));

    new ArcadeStateMachine().applySealedBlobs(database,
        List.of(new TsSealedBlob(TYPE_NAME, 1, sealedFileName(1), leaderSealedBytes)));

    assertThat(broken.isEngineAvailable()).as("shard 0's bucket must not have been poisoned by shard 1").isTrue();
    assertThat(broken.getEngineUnavailableReason()).isNull();

    // Every sample is back - the repaired shard's from the blob, shard 0's from its own untouched files - and both
    // shards still take writes, which is what a closed mutable bucket would refuse.
    assertThat(countSamples()).isEqualTo(ROWS);
    appendRows(broken.getEngine(), ROWS, 100);
    assertThat(countSamples()).isEqualTo(ROWS + 100L);
  }

  /**
   * The failure arm. A blob that is itself unusable cannot repair anything, and what matters is what the apply
   * path does about it: log it, leave the type exactly as unavailable as it was, and RETURN - not throw, because
   * the same entry may carry blobs for other types that are perfectly repairable.
   * <p>
   * It also pins the freshened reason. {@code initEngine()} now records why THIS attempt failed, so an operator
   * reading {@code CHECK DATABASE} after a failed repair sees the new cause rather than the original one - here,
   * a header the sealed store rejects, not the flipped magic byte that started it.
   */
  @Test
  void aBlobThatCannotBeOpenedLeavesTheTypeUnavailableWithoutThrowing() throws Exception {
    buildTypeAndCaptureItsSealedFile();

    breakTheSealedFileAndReopen();

    final LocalTimeSeriesType broken = (LocalTimeSeriesType) database.getSchema().getType(TYPE_NAME);
    assertThat(broken.isEngineAvailable()).isFalse();
    final String reasonBefore = broken.getEngineUnavailableReason();
    assertThat(reasonBefore).isNotNull();

    // A blob long enough to be written and read back, and nothing the sealed store can make sense of.
    final byte[] garbage = new byte[512];
    new ArcadeStateMachine().applySealedBlobs(database,
        List.of(new TsSealedBlob(TYPE_NAME, 0, SEALED_FILE, garbage)));

    assertThat(broken.isEngineAvailable()).as("an unusable blob must not appear to have repaired anything")
        .isFalse();
    // The reason now describes THIS attempt, not the one that started it: both are "invalid sealed store magic",
    // but the magic quoted is the zero-filled blob's rather than the flipped byte's. Before initEngine() recorded
    // its own failures an operator kept reading the original cause after every subsequent failed repair.
    assertThat(broken.getEngineUnavailableReason())
        .as("a failed retry must report why IT failed, not why the first attempt did")
        .isNotNull()
        .isNotEqualTo(reasonBefore);

    // The type stays loud rather than silently readable, and the second failed attempt is reachable again: the
    // recovery path is not one-shot, so a later, good blob can still repair it.
    assertThatThrownBy(broken::requireEngine).hasMessageContaining(TYPE_NAME);
  }

  // ---- Helpers ----

  /**
   * Creates the type, fills it and compacts, so the sealed file on disk holds real blocks. Returns its bytes,
   * which is exactly what the leader ships in a {@link TsSealedBlob}.
   */
  private byte[] buildTypeAndCaptureItsSealedFile() throws IOException {
    return buildTypeAndCaptureItsSealedFile(1, 0);
  }

  private byte[] buildTypeAndCaptureItsSealedFile(final int shardCount, final int shardIndex) throws IOException {
    database.command("sql", "CREATE TIMESERIES TYPE " + TYPE_NAME
        + " TIMESTAMP ts TAGS (hostname STRING) FIELDS (usage DOUBLE) SHARDS " + shardCount);
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType(TYPE_NAME)).getEngine();
    appendRows(engine, 0, ROWS);
    engine.compactAll();

    final File sealed = new File(databasePath, sealedFileName(shardIndex));
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
    breakTheSealedFileAndReopen(0);
  }

  private void breakTheSealedFileAndReopen(final int shardIndex) throws IOException {
    database.close();
    final File sealed = new File(databasePath, sealedFileName(shardIndex));
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

  private long countSamples() {
    try (final ResultSet resultSet = database.query("sql", "SELECT count(*) AS cnt FROM " + TYPE_NAME)) {
      return resultSet.next().<Number>getProperty("cnt").longValue();
    }
  }
}
