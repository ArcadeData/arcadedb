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
import com.arcadedb.server.ha.raft.RaftLogEntryCodec.TsSealedChunk;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.CRC32;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * How applySealedBlobs repairs an engine-less TimeSeries type and when it must refuse the entry (failed repair,
 * unknown or non-TimeSeries type), against real databases and an unstarted state machine.
 */
class SealedBlobRepairAndRefusalTest {

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
   */
  @Nested
  class EngineRepair {

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
    // Regression for #6839: a sealed blob repairs a type whose engine failed to initialise
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
     * type at all is still refused - there is nothing there an engine could be initialised over, and writing the
     * payload to a file named after it would be worse than dropping it.
     * <p>
     * It used to be refused by being logged and stepped over, which let the entry be checkpointed over a payload
     * that was consumed and will never be re-shipped; issue #8172 turned that into the same accumulate-then-throw
     * #8070 gave the failed repair below, so the refusal now reaches the caller.
     */
    // Regression for #6839: a blob for a non time series type is still refused
    @Test
    void aBlobForANonTimeSeriesTypeIsStillRefused() throws Exception {
      database.getSchema().createDocumentType("NotATimeSeries");

      assertThatThrownBy(() -> new ArcadeStateMachine().applySealedBlobs(database,
          List.of(new TsSealedBlob("NotATimeSeries", 0, "NotATimeSeries_shard_0.ts.sealed", new byte[] { 1, 2, 3 }))))
          .isInstanceOf(SealedStoreNotInstalledException.class)
          .hasMessageContaining("NotATimeSeries");

      assertThat(new File(databasePath, "NotATimeSeries_shard_0.ts.sealed"))
          .as("nothing may be written for a type that has no sealed store").doesNotExist();
    }

    /** A healthy type takes the path it always took: the blob is installed through the live sealed store. */
    // Regression for #6839: a healthy type still installs the blob through its live engine
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
    // Regression for #6839: a sealed blob repairs a type with more than one shard
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
     * path does about it: log it, leave the type exactly as unavailable as it was, and REFUSE THE ENTRY.
     * <p>
     * It used to return normally, and issue #8070 is what that cost: a Raft entry is applied once and never
     * re-shipped, so the entry was checkpointed as applied, the blob that WAS the repair was consumed, and the type
     * stayed engine-less for the life of the node with nothing left to resend. The refusal now reaches
     * {@code handleUnexpectedApplyError}, which quarantines the database and arms the targeted snapshot resync.
     * The "one unrepairable type must not abort the entry" contract is unchanged and pinned by
     * {@code SealedBlobRepairAndRefusalTest}: the throw happens after every blob has been attempted.
     * <p>
     * It also pins the freshened reason. {@code initEngine()} now records why THIS attempt failed, so an operator
     * reading {@code CHECK DATABASE} after a failed repair sees the new cause rather than the original one - here,
     * a header the sealed store rejects, not the flipped magic byte that started it.
     */
    // Regression for #6839: a blob that cannot be opened leaves the type unavailable and refuses the entry
    @Test
    void aBlobThatCannotBeOpenedLeavesTheTypeUnavailableAndRefusesTheEntry() throws Exception {
      buildTypeAndCaptureItsSealedFile();

      breakTheSealedFileAndReopen();

      final LocalTimeSeriesType broken = (LocalTimeSeriesType) database.getSchema().getType(TYPE_NAME);
      assertThat(broken.isEngineAvailable()).isFalse();
      final String reasonBefore = broken.getEngineUnavailableReason();
      assertThat(reasonBefore).isNotNull();

      // A blob long enough to be written and read back, and nothing the sealed store can make sense of.
      final byte[] garbage = new byte[512];
      assertThatThrownBy(() -> new ArcadeStateMachine().applySealedBlobs(database,
          List.of(new TsSealedBlob(TYPE_NAME, 0, SEALED_FILE, garbage))))
          .as("an entry whose repair did not take effect must not be recorded as applied (issue #8070)")
          .isInstanceOf(SealedStoreNotInstalledException.class)
          .hasMessageContaining(TYPE_NAME);

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

  /**
   * Issue #8070, the follow-up to #7602: an entry whose application did not actually take effect must not be
   * checkpointed as applied.
   * <p>
   * {@code applySealedBlobs} had exactly that shape for the engine-less repair of issue #6839. When
   * {@code repairEngineWithSealedBlob} failed it logged a SEVERE and returned {@code false}, the loop stepped over
   * it, and the entry completed normally - and the method's own comment states the cost: a Raft entry is applied
   * once and never re-shipped, so the blob that WAS the repair was consumed, the type stayed engine-less for the
   * life of the node, and nothing would ever send another. No quarantine, no resync, one log line.
   * <p>
   * The refusal is raised AFTER the loop, which is what keeps the contract the repair path was written to: one
   * unrepairable type must not abort the apply of an entry that may carry blobs for others.
   */
  @Nested
  class FailedRepairRefusal {

    private static final String BROKEN_TYPE  = "Cpu";
    private static final String HEALTHY_TYPE = "Mem";
    private static final int    ROWS         = 2_000;

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
     * The whole of the issue: a repair that fails refuses the entry rather than letting it be recorded as applied.
     */
    // Regression for #8070: a failed repair refuses the entry instead of consuming the blob
    @Test
    void aFailedRepairRefusesTheEntryInsteadOfConsumingTheBlob() throws Exception {
      buildType(BROKEN_TYPE);
      breakSealedFileAndReopen(BROKEN_TYPE);

      final LocalTimeSeriesType broken = (LocalTimeSeriesType) database.getSchema().getType(BROKEN_TYPE);
      assertThat(broken.isEngineAvailable()).as("the #6356 state the repair exists for").isFalse();

      assertThatThrownBy(() -> new ArcadeStateMachine().applySealedBlobs(database,
          List.of(new TsSealedBlob(BROKEN_TYPE, 0, sealedFileName(BROKEN_TYPE, 0), new byte[512]))))
          .isInstanceOf(SealedStoreNotInstalledException.class)
          .hasMessageContaining(BROKEN_TYPE)
          .hasMessageContaining("shard 0");

      assertThat(broken.isEngineAvailable()).as("nothing pretended the type was repaired").isFalse();
    }

    /**
     * The contract the accumulation exists for: the refusal is raised once, at the end, so a blob for a type that
     * CAN be installed is installed even though a sibling in the same entry cannot be.
     */
    // Regression for #8070: every other blob in the entry is still installed before the refusal
    @Test
    void everyOtherBlobInTheEntryIsStillInstalledBeforeTheRefusal() throws Exception {
      buildType(BROKEN_TYPE);
      final byte[] healthySealed = buildType(HEALTHY_TYPE);
      breakSealedFileAndReopen(BROKEN_TYPE);

      final LocalTimeSeriesType healthy = (LocalTimeSeriesType) database.getSchema().getType(HEALTHY_TYPE);
      assertThat(healthy.isEngineAvailable()).isTrue();
      // Empty the healthy type's live sealed store, so "the blob was installed" is observable rather than a
      // statement about bytes that were already identical.
      healthy.getEngine().getShard(0).getSealedStore().truncateBefore(Long.MAX_VALUE);
      assertThat(healthy.getEngine().getShard(0).getSealedStore().getBlockCount()).isZero();

      assertThatThrownBy(() -> new ArcadeStateMachine().applySealedBlobs(database, List.of(
          new TsSealedBlob(BROKEN_TYPE, 0, sealedFileName(BROKEN_TYPE, 0), new byte[512]),
          new TsSealedBlob(HEALTHY_TYPE, 0, sealedFileName(HEALTHY_TYPE, 0), healthySealed))))
          .isInstanceOf(SealedStoreNotInstalledException.class)
          .hasMessageContaining(BROKEN_TYPE)
          .as("the refusal names what failed and not what succeeded")
          .hasMessageNotContaining(HEALTHY_TYPE);

      assertThat(healthy.getEngine().getShard(0).getSealedStore().getBlockCount())
          .as("the sibling blob must be installed before the entry is refused").isGreaterThan(0);
    }

    /**
     * The sliced path (issue #4416) had the identical defect, and it is the worse half of it: the slices are
     * assembled, the assembled file is verified against the leader's length and CRC, and the last entry of the
     * sequence was then checkpointed over a repair that did not happen.
     */
    // Regression for #8070: the sliced path refuses the entry too
    @Test
    void theSlicedPathRefusesTheEntryToo() throws Exception {
      buildType(BROKEN_TYPE);
      breakSealedFileAndReopen(BROKEN_TYPE);

      final byte[] garbage = new byte[512];
      final CRC32 crc = new CRC32();
      crc.update(garbage);

      assertThatThrownBy(() -> new ArcadeStateMachine().applySealedChunks(database,
          List.of(new TsSealedChunk(BROKEN_TYPE, 0, sealedFileName(BROKEN_TYPE, 0), garbage.length, crc.getValue(),
              0L, garbage, true))))
          .isInstanceOf(SealedStoreNotInstalledException.class)
          .hasMessageContaining(BROKEN_TYPE);
    }

    // ---- Helpers ----

    /** Creates the type, fills it and compacts, so its sealed file on disk holds real blocks. Returns its bytes. */
    private byte[] buildType(final String typeName) throws IOException {
      database.command("sql", "CREATE TIMESERIES TYPE " + typeName
          + " TIMESTAMP ts TAGS (hostname STRING) FIELDS (usage DOUBLE) SHARDS 1");
      final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine();

      final long[] timestamps = new long[ROWS];
      final Object[][] columns = new Object[2][ROWS];
      for (int i = 0; i < ROWS; i++) {
        timestamps[i] = 1_700_000_000_000L + i * 1_000L;
        columns[0][i] = "host_" + (i % 7);
        columns[1][i] = (double) i;
      }
      engine.appendBatch(timestamps, columns);
      engine.compactAll();

      final File sealed = new File(databasePath, sealedFileName(typeName, 0));
      assertThat(sealed).exists();
      return Files.readAllBytes(sealed.toPath());
    }

    private static String sealedFileName(final String typeName, final int shardIndex) {
      return typeName + "_shard_" + shardIndex + ".ts.sealed";
    }

    /**
     * Flips byte 0 of the sealed file - part of its magic - and reopens the database, which is the "registered with
     * no engine" state {@code LocalSchema.readConfiguration()} keeps rather than dropping the type (#6356).
     */
    private void breakSealedFileAndReopen(final String typeName) throws IOException {
      database.close();
      final File sealed = new File(databasePath, sealedFileName(typeName, 0));
      try (final RandomAccessFile raf = new RandomAccessFile(sealed, "rw")) {
        raf.seek(0);
        final int b = raf.read();
        raf.seek(0);
        raf.write(b ^ 0x01);
      }
      database = (LocalDatabase) new DatabaseFactory(databasePath).open();
    }
  }

  /**
   * Issue #8172, the sibling of #8070 one branch above it: a sealed payload naming a type this node does not have,
   * or a type that exists but is not a TIMESERIES one, used to be logged at SEVERE and stepped over.
   * <p>
   * Stepping over it let the loop finish, {@code applyWithRetry} return normally and the entry be CHECKPOINTED -
   * and because a Raft entry is applied once and never re-shipped, the sealed store that entry carried was gone
   * for good and this node's copy of the type permanently behind the leader's, with one log line as the whole of
   * the evidence. That is the rule #7602 wrote down and #8070 enforced for the failed engine repair a few lines
   * below, so the same accumulate-then-throw applies here.
   * <p>
   * Both arms are guarded by a Raft-ordering invariant - the type-creation entry carries a lower index and is
   * applied first - so neither should happen. What this pins is what it costs if the invariant ever does not hold,
   * for instance during a rolling upgrade shipping a type this node's build cannot construct.
   */
  @Nested
  class UnknownTypeRefusal {

    private static final String ABSENT_TYPE   = "Cpu";
    private static final String DOCUMENT_TYPE = "Audit";
    private static final String HEALTHY_TYPE  = "Mem";
    private static final int    ROWS          = 2_000;

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
      new DatabaseFactory(databasePath).open().drop();
    }

    /** A blob for a type this node does not have refuses the entry instead of being logged away. */
    // Regression for #8172: a blob for an unknown type refuses the entry
    @Test
    void aBlobForAnUnknownTypeRefusesTheEntry() {
      assertThat(database.getSchema().existsType(ABSENT_TYPE)).isFalse();

      assertThatThrownBy(() -> new ArcadeStateMachine().applySealedBlobs(database,
          List.of(new TsSealedBlob(ABSENT_TYPE, 0, sealedFileName(ABSENT_TYPE, 0), new byte[512]))))
          .isInstanceOf(SealedStoreNotInstalledException.class)
          .hasMessageContaining(ABSENT_TYPE)
          .hasMessageContaining("shard 0")
          .hasMessageContaining("unknown type");
    }

    /** The same for a type that exists and is simply not a TIMESERIES one. */
    // Regression for #8172: a blob for a non time series type refuses the entry
    @Test
    void aBlobForANonTimeSeriesTypeRefusesTheEntry() {
      database.command("sql", "CREATE DOCUMENT TYPE " + DOCUMENT_TYPE);

      assertThatThrownBy(() -> new ArcadeStateMachine().applySealedBlobs(database,
          List.of(new TsSealedBlob(DOCUMENT_TYPE, 0, sealedFileName(DOCUMENT_TYPE, 0), new byte[512]))))
          .isInstanceOf(SealedStoreNotInstalledException.class)
          .hasMessageContaining(DOCUMENT_TYPE)
          .hasMessageContaining("not a TIMESERIES type");
    }

    /**
     * The contract the accumulation exists for, carried over from #8070: the refusal is raised once, after the
     * loop, so a blob for a type that CAN be installed is still installed.
     */
    // Regression for #8172: every installable blob in the entry is still installed before the refusal
    @Test
    void everyInstallableBlobInTheEntryIsStillInstalledBeforeTheRefusal() throws Exception {
      final byte[] healthySealed = buildType(HEALTHY_TYPE);

      final LocalTimeSeriesType healthy = (LocalTimeSeriesType) database.getSchema().getType(HEALTHY_TYPE);
      healthy.getEngine().getShard(0).getSealedStore().truncateBefore(Long.MAX_VALUE);
      assertThat(healthy.getEngine().getShard(0).getSealedStore().getBlockCount()).isZero();

      assertThatThrownBy(() -> new ArcadeStateMachine().applySealedBlobs(database, List.of(
          new TsSealedBlob(ABSENT_TYPE, 0, sealedFileName(ABSENT_TYPE, 0), new byte[512]),
          new TsSealedBlob(HEALTHY_TYPE, 0, sealedFileName(HEALTHY_TYPE, 0), healthySealed))))
          .isInstanceOf(SealedStoreNotInstalledException.class)
          .hasMessageContaining(ABSENT_TYPE)
          .as("the refusal names what failed and not what succeeded")
          .hasMessageNotContaining(HEALTHY_TYPE);

      assertThat(healthy.getEngine().getShard(0).getSealedStore().getBlockCount())
          .as("the installable blob must be installed before the entry is refused").isGreaterThan(0);
    }

    /** The sliced path carries the identical pair of arms and gets the identical treatment. */
    // Regression for #8172: a slice sequence for an unknown or non time series type refuses the entry
    @Test
    void aSliceSequenceForAnUnknownOrNonTimeSeriesTypeRefusesTheEntry() {
      database.command("sql", "CREATE DOCUMENT TYPE " + DOCUMENT_TYPE);

      final byte[] garbage = new byte[512];
      final CRC32 crc = new CRC32();
      crc.update(garbage);

      assertThatThrownBy(() -> new ArcadeStateMachine().applySealedChunks(database,
          List.of(new TsSealedChunk(ABSENT_TYPE, 0, sealedFileName(ABSENT_TYPE, 0), garbage.length, crc.getValue(),
              0L, garbage, true))))
          .isInstanceOf(SealedStoreNotInstalledException.class)
          .hasMessageContaining(ABSENT_TYPE)
          .hasMessageContaining("unknown type");

      assertThatThrownBy(() -> new ArcadeStateMachine().applySealedChunks(database,
          List.of(new TsSealedChunk(DOCUMENT_TYPE, 0, sealedFileName(DOCUMENT_TYPE, 0), garbage.length,
              crc.getValue(), 0L, garbage, true))))
          .isInstanceOf(SealedStoreNotInstalledException.class)
          .hasMessageContaining(DOCUMENT_TYPE)
          .hasMessageContaining("not a TIMESERIES type");
    }

    /**
     * Several slices of the same unknown (type, shard) in one entry name it ONCE in the refusal: an operator reads
     * the list of what is missing, not a list of how many payloads named it.
     */
    // Regression for #8172: repeated slices for the same unknown type are named once
    @Test
    void repeatedSlicesForTheSameUnknownTypeAreNamedOnce() {
      final byte[] garbage = new byte[256];
      final CRC32 crc = new CRC32();
      crc.update(garbage);

      assertThatThrownBy(() -> new ArcadeStateMachine().applySealedChunks(database, List.of(
          new TsSealedChunk(ABSENT_TYPE, 0, sealedFileName(ABSENT_TYPE, 0), garbage.length * 2L, crc.getValue(),
              0L, garbage, false),
          new TsSealedChunk(ABSENT_TYPE, 0, sealedFileName(ABSENT_TYPE, 0), garbage.length * 2L, crc.getValue(),
              garbage.length, garbage, true))))
          .isInstanceOf(SealedStoreNotInstalledException.class)
          .extracting(e -> countOccurrences(e.getMessage(), ABSENT_TYPE + " shard 0 (unknown type)"))
          .isEqualTo(1);
    }

    /** Nothing changed for the ordinary case: a blob for a healthy TIMESERIES type still installs silently. */
    // Regression for #8172: an installable blob still installs
    @Test
    void anInstallableBlobStillInstalls() throws Exception {
      final byte[] healthySealed = buildType(HEALTHY_TYPE);
      final LocalTimeSeriesType healthy = (LocalTimeSeriesType) database.getSchema().getType(HEALTHY_TYPE);
      healthy.getEngine().getShard(0).getSealedStore().truncateBefore(Long.MAX_VALUE);

      assertThatCode(() -> new ArcadeStateMachine().applySealedBlobs(database,
          List.of(new TsSealedBlob(HEALTHY_TYPE, 0, sealedFileName(HEALTHY_TYPE, 0), healthySealed))))
          .doesNotThrowAnyException();

      assertThat(healthy.getEngine().getShard(0).getSealedStore().getBlockCount()).isGreaterThan(0);
    }

    // ---- Helpers ----

    private static int countOccurrences(final String haystack, final String needle) {
      int count = 0;
      for (int at = haystack.indexOf(needle); at >= 0; at = haystack.indexOf(needle, at + needle.length()))
        count++;
      return count;
    }

    /** Creates the type, fills it and compacts, so its sealed file on disk holds real blocks. Returns its bytes. */
    private byte[] buildType(final String typeName) throws IOException {
      database.command("sql", "CREATE TIMESERIES TYPE " + typeName
          + " TIMESTAMP ts TAGS (hostname STRING) FIELDS (usage DOUBLE) SHARDS 1");
      final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine();

      final long[] timestamps = new long[ROWS];
      final Object[][] columns = new Object[2][ROWS];
      for (int i = 0; i < ROWS; i++) {
        timestamps[i] = 1_700_000_000_000L + i * 1_000L;
        columns[0][i] = "host_" + (i % 7);
        columns[1][i] = (double) i;
      }
      engine.appendBatch(timestamps, columns);
      engine.compactAll();

      final File sealed = new File(databasePath, sealedFileName(typeName, 0));
      assertThat(sealed).exists();
      return Files.readAllBytes(sealed.toPath());
    }

    private static String sealedFileName(final String typeName, final int shardIndex) {
      return typeName + "_shard_" + shardIndex + ".ts.sealed";
    }
  }
}
